import fakeredis
from a2a_bus import A2ABus
from agentmart_worker import handle_request

def test_handle_request_emits_lifecycle_and_result():
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    req = {"task_id": "t1", "correlation_id": "c1", "sender": "hermes",
           "recipient": "agentmart", "intent": "list-products", "state": "proposed",
           "payload": {"text": "list earbuds under $120", "customer_id": "AM-CUST-0001"},
           "metrics": {}}
    final = handle_request(req, bus, dry_run=True)
    assert final["state"] in ("completed", "failed")
    assert final["correlation_id"] == "c1"
    responses = bus.read(bus.RESPONSES, last_id="0", block_ms=10)
    states = [e["state"] for _id, e in responses]
    assert "accepted" in states and final["state"] in states
    assert all(e["correlation_id"] == "c1" for _id, e in responses)


def test_handle_request_streams_in_progress_incrementally():
    """The worker must publish each agent's hops as they land (not one final burst):
    more than one `in_progress` envelope, all sharing the correlation_id, and every
    one published before the terminal `completed` envelope."""
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    req = {"task_id": "t2", "correlation_id": "c2", "sender": "hermes",
           "recipient": "agentmart", "intent": "", "state": "proposed",
           "payload": {"text": "list earbuds under $120", "customer_id": "AM-CUST-0001"},
           "metrics": {}}
    final = handle_request(req, bus, dry_run=True)
    assert final["state"] == "completed"

    responses = bus.read(bus.RESPONSES, last_id="0", block_ms=10, count=200)
    states = [e["state"] for _id, e in responses]

    in_progress_idx = [i for i, s in enumerate(states) if s == "in_progress"]
    completed_idx = states.index("completed")

    # More than one hop streamed as agents finish, not a single closing dump.
    assert len(in_progress_idx) > 1
    # Every streamed hop belongs to this request.
    assert all(responses[i][1]["correlation_id"] == "c2" for i in in_progress_idx)
    # And all of them precede the terminal completed envelope.
    assert max(in_progress_idx) < completed_idx
