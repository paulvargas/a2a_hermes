import fakeredis
from a2a_bus import A2ABus, envelope_to_fields, fields_to_envelope

def make_bus():
    return A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))

def test_publish_then_read_roundtrips_envelope():
    bus = make_bus()
    env = {"task_id": "t1", "correlation_id": "c1", "sender": "hermes",
           "recipient": "agentmart", "intent": "list-products", "state": "proposed",
           "payload": {"text": "earbuds"}, "metrics": {"total_tokens": 0, "elapsed_ms": 0}}
    entry_id = bus.publish(bus.REQUESTS, env)
    assert entry_id
    items = bus.read(bus.REQUESTS, last_id="0", block_ms=10)
    assert len(items) == 1
    _id, got = items[0]
    assert got["correlation_id"] == "c1"
    assert got["payload"] == {"text": "earbuds"}   # JSON round-trip preserved
