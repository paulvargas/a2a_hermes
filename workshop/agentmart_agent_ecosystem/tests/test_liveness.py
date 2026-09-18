import fakeredis
from a2a_bus import A2ABus
from agentmart_worker import handle_request
from liveness import Liveness


def test_mark_then_not_dead_within_ttl():
    live = Liveness()
    live.mark("pricing", now_ms=1000)
    assert "pricing" not in live.dead(now_ms=2000, ttl_ms=6000)


def test_mark_then_dead_after_ttl():
    live = Liveness()
    live.mark("pricing", now_ms=1000)
    assert "pricing" in live.dead(now_ms=9000, ttl_ms=6000)


def test_dead_boundary_is_exclusive_of_ttl_edge():
    """now - ttl exactly equal to last-seen is still within the TTL window
    (only strictly older than the cutoff counts as dead)."""
    live = Liveness()
    live.mark("pricing", now_ms=1000)
    assert "pricing" not in live.dead(now_ms=7000, ttl_ms=6000)
    assert "pricing" in live.dead(now_ms=7001, ttl_ms=6000)


def test_dead_ignores_agents_never_marked():
    live = Liveness()
    assert live.dead(now_ms=100000, ttl_ms=6000) == []


def test_snapshot_reflects_healthy_and_dead():
    live = Liveness()
    live.mark("pricing", now_ms=1000)
    live.mark("inventory", now_ms=8000)

    snap = live.snapshot(now_ms=9000, ttl_ms=6000)
    assert snap["pricing"] == "dead"       # last seen 8000ms ago
    assert snap["inventory"] == "healthy"  # last seen 1000ms ago


def test_snapshot_only_contains_known_agents():
    live = Liveness()
    live.mark("order_agent", now_ms=1000)
    snap = live.snapshot(now_ms=2000, ttl_ms=6000)
    assert set(snap.keys()) == {"order_agent"}


def test_mark_default_now_ms_uses_wall_clock():
    """Without an explicit now_ms, mark()/dead() still cooperate using real
    time -- a freshly marked agent must not appear dead immediately after."""
    live = Liveness()
    live.mark("order_agent")
    assert "order_agent" not in live.dead(ttl_ms=6000)


# ---------------------------------------------------------------------------
# Worker integration: each streamed hop must also publish a heartbeat.
# ---------------------------------------------------------------------------
def test_handle_request_publishes_heartbeats_for_each_hop():
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    req = {"task_id": "hb1", "correlation_id": "hb1", "sender": "hermes",
           "recipient": "agentmart", "intent": "", "state": "proposed",
           "payload": {"text": "list earbuds under $120", "customer_id": "CUST-1001"},
           "metrics": {}}
    final = handle_request(req, bus, dry_run=True)
    assert final["state"] == "completed"

    heartbeats = bus.read(bus.HEARTBEATS, last_id="0", block_ms=10, count=200)
    assert heartbeats, "expected at least one heartbeat published for the run's hops"
    for _id, env in heartbeats:
        assert env.get("status") == "healthy"
        assert env.get("agent")
        assert env.get("ts")
