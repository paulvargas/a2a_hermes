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
