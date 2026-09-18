import logging, time, uuid
from datetime import datetime, timezone
from a2a_bus import A2ABus
from agentmart_ecosystem import (
    build_graph,
    classify_intent,
    load_hermes_a2a_config,
    load_product_listing,
)
from liveness import Liveness
from orders import find_payable_order

log = logging.getLogger("agentmart.worker")

# Shared across handle_request calls in this process so a single worker's
# liveness view accumulates over its lifetime rather than resetting per request.
liveness = Liveness()

def _publish_heartbeat(bus, agent):
    """Publish a lightweight heartbeat envelope for `agent` and mark it locally.

    Additive only: never touches hop/lifecycle publishing, correlation, metrics,
    or the HITL gate. One xadd per hop, so it stays cheap.
    """
    liveness.mark(agent)
    bus.publish(bus.HEARTBEATS, {
        "agent": agent,
        "ts": datetime.now(timezone.utc).isoformat(),
        "status": "healthy",
    })

def _env(state, sender, recipient, intent, payload, correlation_id, task_id, metrics=None):
    return {"task_id": task_id, "correlation_id": correlation_id, "sender": sender,
            "recipient": recipient, "intent": intent, "state": state,
            "protocol": "agentmart.a2a.v1", "payload": payload,
            "metrics": metrics or {}, "created_at": datetime.now(timezone.utc).isoformat()}

def await_confirmation(bus, correlation_id, timeout_ms=120000, poll_ms=200):
    """Block (by polling) for a `confirm`/`cancel` envelope on `bus.REQUESTS`
    that matches `correlation_id`, returning True/False/None (timeout).

    A real customer request -- or a confirm/cancel belonging to a *different*
    checkout -- can legitimately land on the same REQUESTS stream while this
    poll is running, so every envelope is matched strictly on
    correlation_id + state; anything else is skipped rather than treated as
    an answer.

    Scans from the start of the stream (not "$") so an envelope published
    before this call started (the common case: the front-end POSTs /confirm
    and only then does this poll observe it) is still found on the first read.
    """
    deadline = time.monotonic() + (timeout_ms / 1000.0)
    last_id = "0"
    while True:
        remaining_s = deadline - time.monotonic()
        if remaining_s <= 0:
            return None
        block_ms = max(1, min(poll_ms, int(remaining_s * 1000)))
        items = bus.read(bus.REQUESTS, last_id=last_id, block_ms=block_ms, count=100)
        if not items:
            # fakeredis (used in tests) returns immediately instead of actually
            # blocking for block_ms, so pace the loop ourselves to avoid a
            # tight spin while waiting for the real answer or the deadline.
            time.sleep(min(0.01, max(0.0, remaining_s)))
            continue
        for entry_id, item_env in items:
            last_id = entry_id
            if item_env.get("correlation_id") != correlation_id:
                continue
            state = item_env.get("state")
            if state == "confirm":
                return True
            if state == "cancel":
                return False
            # any other state for this correlation_id (e.g. a stray "proposed")
            # is not an answer -- keep polling.

def handle_request(env, bus, dry_run=True):
    cid = env.get("correlation_id") or env.get("task_id") or str(uuid.uuid4())
    intent = env.get("intent", "")
    payload = env.get("payload", {})
    text = payload.get("text", "")
    customer_id = payload.get("customer_id", "CUST-1001")
    task_id = env["task_id"]
    bus.publish(bus.RESPONSES, _env("accepted", "agentmart", "hermes", intent, {"ack": True}, cid, task_id))
    log.info("accepted cid=%s intent=%s", cid, intent)

    if intent == "checkout_payment":
        try:
            payable = find_payable_order(customer_id)
        except Exception:
            # No seeded order book, or some other lookup failure: skip the
            # gate and let the normal flow report "nothing to pay" as usual.
            log.exception("checkout gate: payable-order lookup failed cid=%s", cid)
            payable = None
        if payable:
            amount = payable["total_usd"]
            order_id = payable["order_id"]
            bus.publish(bus.RESPONSES, _env(
                "input_required", "agentmart", "hermes", intent,
                {"order_id": order_id, "amount": amount,
                 "prompt": f"Confirm purchase of ${amount:.2f}?"},
                cid, task_id,
            ))
            log.info("input_required cid=%s order_id=%s amount=%.2f", cid, order_id, amount)

            approved = await_confirmation(bus, cid, timeout_ms=120000)
            if approved is not True:
                reply = ("Checkout cancelled — no payment was made." if approved is False
                          else "Checkout timed out — no payment was made.")
                final = _env("completed", "agentmart", "hermes", intent,
                             {"transcript": [], "draft_order": None, "reply": reply},
                             cid, task_id)
                bus.publish(bus.RESPONSES, final)
                log.info("checkout %s cid=%s -- no payment run",
                          "declined" if approved is False else "timed out", cid)
                return final
            log.info("checkout approved cid=%s -- proceeding to payment", cid)
            # approved: fall through to the normal graph run below, which
            # settles the order via order_agent + payment_agent as usual.

    try:
        # Stream the graph rather than invoke-then-dump: with stream_mode="values"
        # LangGraph yields the full accumulated state after each superstep, so we
        # can publish each agent's a2a_log hops the moment they land instead of
        # holding them all back until the ~18s run finishes. That is what lets the
        # web client's SSE trace fill in real time instead of timing out on silence.
        inputs = {
            "customer_request": text,
            "channel": payload.get("channel", "webchat"),
            "dry_run": dry_run,
            "customer_id": customer_id,
            "intent": (env.get("intent") or None) or classify_intent(text),
            "hermes_a2a_config": load_hermes_a2a_config(),
            "product_listing": load_product_listing(),
            "transcript": [],
            "a2a_log": [],
        }
        app = build_graph()
        seen = 0
        last_state = {}
        for state in app.stream(inputs, stream_mode="values"):
            last_state = state
            hops = state.get("a2a_log", [])
            # Publish only the hops appended since the previous superstep, so each
            # agent's envelopes reach Redis as soon as that agent finishes.
            for hop in hops[seen:]:
                bus.publish(bus.RESPONSES, _env(hop.get("state", "in_progress"), hop.get("sender", "agentmart"),
                            hop.get("recipient", "hermes"), intent, hop, cid, task_id,
                            metrics=hop.get("metrics")))
                _publish_heartbeat(bus, hop.get("sender", "agentmart"))
            seen = len(hops)
        final = _env("completed", "agentmart", "hermes", intent,
                     {"transcript": last_state.get("transcript", []),
                      "draft_order": last_state.get("draft_order"),
                      "reply": last_state.get("customer_reply") or last_state.get("transcript", [])[-1:]},
                     cid, task_id)
        bus.publish(bus.RESPONSES, final)
        log.info("completed cid=%s hops=%d", cid, len(last_state.get("a2a_log", [])))
        return final
    except Exception as exc:  # surface failure as a failed envelope
        final = _env("failed", "agentmart", "hermes", intent, {"error": f"{type(exc).__name__}: {exc}"}, cid, task_id)
        bus.publish(bus.RESPONSES, final)
        log.exception("failed cid=%s", cid)
        return final

def run_worker(bus, dry_run=False):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    last_id = "$"
    log.info("worker up; consuming %s", bus.REQUESTS)
    while True:
        for entry_id, env in bus.read(bus.REQUESTS, last_id=last_id, block_ms=5000):
            last_id = entry_id
            handle_request(env, bus, dry_run=dry_run)

if __name__ == "__main__":
    import redis, os
    from dotenv import load_dotenv
    load_dotenv()
    client = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"),
                         port=int(os.getenv("REDIS_PORT", "6379")), decode_responses=True)
    run_worker(A2ABus(client=client), dry_run=not bool(os.getenv("OPENAI_API_KEY")))
