import fakeredis
from fastapi.testclient import TestClient
from a2a_bus import A2ABus
from agentmart_ecosystem import OpenRouterHermesClient
from hermes_core import HermesCore
from hermes_web import create_app


def _offline_core(bus):
    """HermesCore whose LLM is a dry-run client, so compose_reply/_resolve take
    the deterministic fallback path with no network (mirrors production wiring
    but keeps these tests hermetic even when a real key is in .env)."""
    return HermesCore(bus=bus, llm=OpenRouterHermesClient(dry_run=True))


def _client():
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    app = create_app(_offline_core(bus))
    return TestClient(app)


def test_chat_endpoint_returns_correlation_id():
    client = _client()
    r = client.post("/chat", json={"text": "find earbuds under $120", "customer_id": "CUST-1001"})
    assert r.status_code == 200
    assert r.json().get("correlation_id")


def test_index_served():
    client = _client()
    r = client.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]
    body = r.text
    assert "Hermes" in body
    assert "EventSource" in body


def test_confirm_endpoint_ok():
    client = _client()
    r = client.post("/confirm", json={"cid": "c1", "task_id": "t1", "approved": True})
    assert r.status_code == 200
    assert r.json().get("ok") is True


def test_events_stream_grounds_completed_reply():
    """A completed reply containing an invented SKU must be redacted in the
    streamed SSE frame (grounding via core.finalize_reply is wired)."""
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    core = _offline_core(bus)
    client = TestClient(create_app(core))
    cid = "cid-ground-1"
    completed = {
        "task_id": cid, "correlation_id": cid, "sender": "agentmart",
        "recipient": "hermes", "intent": "product_advice", "state": "completed",
        "protocol": "agentmart.a2a.v1",
        "payload": {"reply": "Grab the AM-FAKE-9999 earbuds today", "transcript": []},
        "metrics": {"total_tokens": 10, "elapsed_ms": 5},
    }
    bus.publish(bus.RESPONSES, completed)

    r = client.get("/events/" + cid)
    assert r.status_code == 200
    body = r.text
    assert "data:" in body
    assert "AM-FAKE-9999" not in body
    assert "[unverified SKU]" in body


class _FakeStreamCore:
    """Stand-in for HermesCore that replays canned envelopes verbatim.

    HermesCore.stream_events(...) (hermes_core.py, not modified here) returns
    as soon as it reads ANY envelope whose state is "completed"/"failed" for
    the correlation id -- including a per-agent hop's own lifecycle state, not
    just the terminal agentmart envelope. That means publishing a hop followed
    by the terminal envelope on the real Redis-backed bus and reading through
    the real HermesCore would never even deliver the terminal envelope to this
    test (it stops at the hop). This fake reproduces exactly what a real
    worker run puts on the wire -- hop then terminal, both already published --
    so the test exercises the actual code under test here: hermes_web.py's
    /events generator, which must gate the customer-facing reply on the
    TERMINAL framing envelope (sender == "agentmart") and ignore agent hops.
    """

    def __init__(self, events):
        self._events = events
        self._pending = {}

    def stream_events(self, correlation_id, timeout_ms=120000):
        for env in self._events:
            yield env

    def finalize_reply(self, reply_text):
        return reply_text, []

    def compose_reply(self, cid, payload):
        from hermes_web import _normalize_reply
        return _normalize_reply((payload or {}).get("reply"), payload or {}), []

    def remember(self, *args, **kwargs):
        pass


def test_events_stream_ignores_agent_hop_completed_uses_terminal_transcript():
    """Regression guard for the "AgentMart has completed your request."
    placeholder bug: an agent-hop envelope (order_agent -> hermes_myshopper)
    carries state="completed" for its OWN sub-task, with a payload that has no
    transcript. The streamed customer reply must come from the TERMINAL
    framing envelope (sender="agentmart") that follows it, not from the hop."""
    cid = "cid-hop-then-terminal-1"
    hop = {
        "task_id": "hop-1", "correlation_id": cid, "sender": "order_agent",
        "recipient": "hermes_myshopper", "intent": "order_status", "state": "completed",
        "protocol": "agentmart.a2a.v1",
        "payload": {"order_id": None, "draft_created": False},
        "metrics": {},
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

    client = TestClient(create_app(_FakeStreamCore([hop, terminal])))
    r = client.get("/events/" + cid)

    assert r.status_code == 200
    body = r.text
    assert "Your order ships tomorrow." in body
    assert "AgentMart has completed your request." not in body


def test_events_stream_normalizes_list_reply():
    """When the worker emits reply as a 1-element transcript list, the stream
    must still surface a human-readable string (item 2 regression guard)."""
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    core = _offline_core(bus)
    client = TestClient(create_app(core))
    cid = "cid-list-1"
    completed = {
        "task_id": cid, "correlation_id": cid, "sender": "agentmart",
        "recipient": "hermes", "intent": "product_advice", "state": "completed",
        "protocol": "agentmart.a2a.v1",
        "payload": {"reply": [{"agent": "order_agent", "message": "Here are 3 earbud options."}],
                    "transcript": []},
        "metrics": {},
    }
    bus.publish(bus.RESPONSES, completed)

    r = client.get("/events/" + cid)
    assert r.status_code == 200
    assert "Here are 3 earbud options." in r.text
