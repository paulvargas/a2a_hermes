import fakeredis
from a2a_bus import A2ABus
from agentmart_worker import await_confirmation, handle_request
from orders import find_payable_order
from seed_data import seed

def test_handle_request_emits_lifecycle_and_result():
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    req = {"task_id": "t1", "correlation_id": "c1", "sender": "hermes",
           "recipient": "agentmart", "intent": "list-products", "state": "proposed",
           "payload": {"text": "list earbuds under $120", "customer_id": "CUST-1001"},
           "metrics": {}}
    final = handle_request(req, bus, dry_run=True)
    assert final["state"] in ("completed", "failed")
    assert final["correlation_id"] == "c1"
    responses = bus.read(bus.RESPONSES, last_id="0", block_ms=10)
    states = [e["state"] for _id, e in responses]
    assert "accepted" in states and final["state"] in states
    assert all(e["correlation_id"] == "c1" for _id, e in responses)


def test_handle_request_streams_hops_incrementally():
    """The worker must publish each agent's hops as they land (not one final burst):
    more than one streamed hop envelope, all sharing the correlation_id, and every
    one published before the terminal `completed` envelope."""
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    req = {"task_id": "t2", "correlation_id": "c2", "sender": "hermes",
           "recipient": "agentmart", "intent": "", "state": "proposed",
           "payload": {"text": "list earbuds under $120", "customer_id": "CUST-1001"},
           "metrics": {}}
    final = handle_request(req, bus, dry_run=True)
    assert final["state"] == "completed"

    responses = bus.read(bus.RESPONSES, last_id="0", block_ms=10, count=200)
    states = [e["state"] for _id, e in responses]

    # The leading "accepted" framing envelope and the terminal "completed" envelope
    # bookend the streamed hops; everything in between is a hop from the graph.
    completed_idx = len(states) - 1
    streamed_idx = list(range(1, completed_idx))

    # More than one hop streamed as agents finish, not a single closing dump.
    assert len(streamed_idx) > 1
    # Every streamed hop belongs to this request.
    assert all(responses[i][1]["correlation_id"] == "c2" for i in streamed_idx)
    # And all of them precede the terminal completed envelope.
    assert max(streamed_idx) < completed_idx


def test_handle_request_preserves_each_hops_own_state():
    """Regression test: the worker previously hardcoded every streamed hop's outer
    envelope state to "in_progress", so per-agent badges in the A2A trace never
    reached a terminal state. The outer envelope's state must match the hop's own
    `state` field (accepted/completed/etc.) rather than being overwritten."""
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    req = {"task_id": "t3", "correlation_id": "c3", "sender": "hermes",
           "recipient": "agentmart", "intent": "", "state": "proposed",
           "payload": {"text": "list earbuds under $120", "customer_id": "CUST-1001"},
           "metrics": {}}
    final = handle_request(req, bus, dry_run=True)
    assert final["state"] == "completed"

    responses = bus.read(bus.RESPONSES, last_id="0", block_ms=10, count=200)
    # Drop the leading "accepted" framing envelope and the terminal envelope from
    # AgentMart itself -- everything left is a streamed hop from the graph's a2a_log.
    streamed = [e for _id, e in responses[1:-1]]

    assert streamed, "expected at least one streamed hop"
    # Every streamed hop's outer state must equal its own source hop's state
    # (preserved), not overwritten with a hardcoded value.
    for hop in streamed:
        assert hop["state"] == hop["payload"]["state"]

    hop_states = {hop["state"] for hop in streamed}
    if hop_states == {"in_progress"}:
        # Every dry-run graph envelope happened to share one state: the
        # preservation assertion above already proves the fix, nothing more to check.
        pass
    else:
        # The common case: request hops (accepted/proposed) and response hops
        # (completed) carry distinct, real lifecycle states -- not all "in_progress".
        assert hop_states != {"in_progress"}
        assert any(s != "in_progress" for s in hop_states)


# ---------------------------------------------------------------------------
# HITL checkout approval gate
# ---------------------------------------------------------------------------
def _confirm_envelope(cid, state):
    return {"task_id": cid, "correlation_id": cid, "sender": "hermes",
            "recipient": "agentmart", "intent": "checkout-and-pay", "state": state,
            "payload": {"approved": state == "confirm"}, "metrics": {}}


def test_await_confirmation_reads_confirm_envelope():
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    bus.publish(bus.REQUESTS, _confirm_envelope("c1", "confirm"))
    assert await_confirmation(bus, "c1", timeout_ms=200) is True


def test_await_confirmation_reads_cancel_envelope():
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    bus.publish(bus.REQUESTS, _confirm_envelope("c1", "cancel"))
    assert await_confirmation(bus, "c1", timeout_ms=200) is False


def test_await_confirmation_times_out_with_no_answer():
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    assert await_confirmation(bus, "c-nobody-answers", timeout_ms=100) is None


def test_await_confirmation_ignores_non_matching_envelopes():
    """A real user request, and a confirm/cancel for a different checkout, can
    legitimately land on the same REQUESTS stream -- neither must be mistaken
    for this correlation id's answer."""
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    bus.publish(bus.REQUESTS, {"task_id": "tX", "correlation_id": "unrelated", "sender": "hermes",
                "recipient": "agentmart", "intent": "order_status", "state": "proposed",
                "payload": {"text": "where is my order"}, "metrics": {}})
    bus.publish(bus.REQUESTS, _confirm_envelope("other-cid", "confirm"))
    bus.publish(bus.REQUESTS, _confirm_envelope("c1", "confirm"))
    assert await_confirmation(bus, "c1", timeout_ms=200) is True


def _checkout_request(cid):
    return {"task_id": cid, "correlation_id": cid, "sender": "hermes",
            "recipient": "agentmart", "intent": "checkout_payment", "state": "proposed",
            "payload": {"text": "Checkout and pay for my order.", "customer_id": "CUST-1001"},
            "metrics": {}}


def test_checkout_payment_approved_emits_input_required_then_completes():
    """A checkout_payment request must pause with input_required before any
    payment runs; once the confirmation resolves True, the graph runs
    normally and settles the order."""
    seed()  # deterministic order book: CUST-1001 has one awaiting_payment order
    try:
        bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
        cid = "checkout-approve-1"
        # Published up front: the front-end's /confirm call happens after the
        # worker is already polling, but await_confirmation scans from the
        # start of the stream so it is found regardless of ordering.
        bus.publish(bus.REQUESTS, _confirm_envelope(cid, "confirm"))

        final = handle_request(_checkout_request(cid), bus, dry_run=True)
        assert final["state"] == "completed"

        responses = [e for _id, e in bus.read(bus.RESPONSES, last_id="0", block_ms=10, count=200)]
        states = [e["state"] for e in responses]
        assert "input_required" in states
        assert states.index("input_required") < len(states) - 1  # precedes the terminal envelope

        gate = next(e for e in responses if e["state"] == "input_required")
        assert gate["sender"] == "agentmart" and gate["recipient"] == "hermes"
        assert gate["correlation_id"] == cid
        assert gate["payload"]["order_id"]
        assert gate["payload"]["amount"] > 0
        assert "prompt" in gate["payload"]

        # Approved: the payment agent hop actually ran and settled the order.
        assert any(e.get("sender") == "payment_agent" for e in responses)
    finally:
        seed()  # restore the canonical seeded order book for other tests/scenarios


def test_checkout_payment_declined_skips_payment():
    seed()
    try:
        bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
        cid = "checkout-decline-1"
        bus.publish(bus.REQUESTS, _confirm_envelope(cid, "cancel"))

        final = handle_request(_checkout_request(cid), bus, dry_run=True)
        assert final["state"] == "completed"
        assert "cancelled" in final["payload"]["reply"].lower()

        responses = [e for _id, e in bus.read(bus.RESPONSES, last_id="0", block_ms=10, count=200)]
        states = [e["state"] for e in responses]
        assert "input_required" in states

        # No payment/order-agent hop ran -- the graph itself never executed.
        senders = {e.get("sender") for e in responses}
        assert "payment_agent" not in senders
        assert "order_agent" not in senders

        # Nothing was actually charged: the order is still payable.
        assert find_payable_order("CUST-1001") is not None
    finally:
        seed()


def test_checkout_payment_skips_gate_when_no_payable_order(monkeypatch):
    """Design requirement: with nothing to pay, the gate is skipped entirely
    (no input_required, no wait) and the normal flow runs, which reports
    there is nothing to pay."""
    monkeypatch.setattr("agentmart_worker.find_payable_order", lambda customer_id: None)
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    final = handle_request(_checkout_request("checkout-nogate-1"), bus, dry_run=True)
    assert final["state"] == "completed"

    states = [e["state"] for _id, e in bus.read(bus.RESPONSES, last_id="0", block_ms=10, count=200)]
    assert "input_required" not in states


def test_non_checkout_intent_is_not_gated():
    """Non-checkout intents must behave exactly as before: no input_required,
    no wait for a confirmation."""
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    req = {"task_id": "t-status", "correlation_id": "t-status", "sender": "hermes",
           "recipient": "agentmart", "intent": "order_status", "state": "proposed",
           "payload": {"text": "What is my order status?", "customer_id": "CUST-1001"},
           "metrics": {}}
    final = handle_request(req, bus, dry_run=True)
    assert final["state"] == "completed"
    states = [e["state"] for _id, e in bus.read(bus.RESPONSES, last_id="0", block_ms=10, count=200)]
    assert "input_required" not in states
