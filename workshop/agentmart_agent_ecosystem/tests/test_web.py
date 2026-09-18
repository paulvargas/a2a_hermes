import fakeredis
from fastapi.testclient import TestClient
from a2a_bus import A2ABus
from hermes_core import HermesCore
from hermes_web import create_app


def _client():
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    app = create_app(HermesCore(bus=bus))
    return TestClient(app)


def test_chat_endpoint_returns_correlation_id():
    client = _client()
    r = client.post("/chat", json={"text": "find earbuds under $120", "customer_id": "AM-CUST-0001"})
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
