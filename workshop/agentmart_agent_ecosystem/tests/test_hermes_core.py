import fakeredis
from a2a_bus import A2ABus
from hermes_core import HermesCore

def test_start_request_publishes_proposed_and_returns_cid():
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    core = HermesCore(bus=bus)
    cid = core.start_request("find wireless earbuds under $120", customer_id="AM-CUST-0001")
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
