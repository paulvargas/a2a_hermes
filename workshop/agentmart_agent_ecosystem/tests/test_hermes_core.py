import fakeredis
from a2a_bus import A2ABus
from hermes_core import HermesCore

def test_start_request_publishes_proposed_and_returns_cid():
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    core = HermesCore(bus=bus)
    cid = core.start_request("find wireless earbuds under $120", customer_id="CUST-1001")
    assert cid
    reqs = bus.read(bus.REQUESTS, last_id="0", block_ms=10)
    assert len(reqs) == 1
    _id, env = reqs[0]
    assert env["state"] == "proposed" and env["sender"] == "hermes"
    assert env["correlation_id"] == cid

def test_finalize_reply_grounds_output():
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    core = HermesCore(bus=bus)
    safe, violations = core.finalize_reply("Buy AM-NOPE-0000 now")
    assert "AM-NOPE-0000" not in safe

def test_stream_events_does_not_stop_at_agent_hop_completed_state():
    """Regression guard: an agent hop (e.g. order_agent -> hermes_myshopper)
    reaches Redis with its own real lifecycle state, including "completed" for
    its response hop -- that is not the end of the conversation. stream_events
    must only stop at the TERMINAL framing envelope (sender="agentmart"), so a
    hop's own completed/failed state must not end the stream early and drop
    the real terminal envelope (and its transcript) that follows it."""
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    core = HermesCore(bus=bus)
    cid = "cid-hop-vs-terminal-stream-1"

    accepted = {
        "task_id": cid, "correlation_id": cid, "sender": "agentmart",
        "recipient": "hermes", "intent": "order_status", "state": "accepted",
        "protocol": "agentmart.a2a.v1", "payload": {"ack": True}, "metrics": {},
    }
    hop = {
        "task_id": "hop-1", "correlation_id": cid, "sender": "order_agent",
        "recipient": "hermes_myshopper", "intent": "order_status", "state": "completed",
        "protocol": "agentmart.a2a.v1",
        "payload": {"order_id": None, "draft_created": False}, "metrics": {},
    }
    terminal = {
        "task_id": cid, "correlation_id": cid, "sender": "agentmart",
        "recipient": "hermes", "intent": "order_status", "state": "completed",
        "protocol": "agentmart.a2a.v1",
        "payload": {
            "reply": None,
            "transcript": [{"agent": "order_agent", "message": "Your order ships tomorrow."}],
        },
        "metrics": {},
    }

    bus.publish(bus.RESPONSES, accepted)
    bus.publish(bus.RESPONSES, hop)
    bus.publish(bus.RESPONSES, terminal)

    events = list(core.stream_events(cid, timeout_ms=3000))

    assert events == [accepted, hop, terminal]
