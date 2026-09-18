import fakeredis
from redis import exceptions as redis_exceptions
from a2a_bus import A2ABus, envelope_to_fields, fields_to_envelope

def make_bus():
    return A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))


class _TimingOutClient:
    """Stub redis client whose blocking xread raises TimeoutError, as redis-py
    does when a blocking XREAD window elapses with no new entries. (fakeredis
    returns [] instead of raising, so this stub is used to exercise the fix.)"""
    def xread(self, *args, **kwargs):
        raise redis_exceptions.TimeoutError("Timeout reading from socket")


class _ConnErrClient:
    def xread(self, *args, **kwargs):
        raise redis_exceptions.ConnectionError("boom")


def test_read_returns_empty_on_idle_timeout():
    bus = A2ABus(client=_TimingOutClient())
    assert bus.read(bus.RESPONSES, last_id="$", block_ms=5000) == []


def test_read_does_not_swallow_connection_error():
    bus = A2ABus(client=_ConnErrClient())
    try:
        bus.read(bus.RESPONSES, last_id="$", block_ms=100)
        assert False, "ConnectionError should propagate"
    except redis_exceptions.ConnectionError:
        pass

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
