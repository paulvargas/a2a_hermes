import fakeredis
from a2a_bus import A2ABus
from agentmart_ecosystem import classify_intent
from hermes_core import HermesCore


class StubLLM:
    """Offline stand-in for OpenRouterHermesClient: no network, canned reply.

    Has neither a `dry_run` nor an `api_key` attribute, so HermesCore treats it
    as an available (usable) client and actually exercises it.
    """

    def __init__(self, reply="", error=False):
        self.reply = reply
        self.error = error
        self.calls = []

    def complete_with_metrics(self, agent_name, system_prompt, user_prompt):
        self.calls.append((agent_name, system_prompt, user_prompt))
        if self.error:
            raise RuntimeError("simulated LLM failure")
        return self.reply, {"prompt_tokens": 0, "completion_tokens": 0,
                            "total_tokens": 0, "elapsed_ms": 0}


def _core(llm=None):
    bus = A2ABus(client=fakeredis.FakeStrictRedis(decode_responses=True))
    return HermesCore(bus=bus, llm=llm if llm is not None else StubLLM())


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


# --------------------------------------------------------------------------
# Feature 1: conversation memory + context resolution
# --------------------------------------------------------------------------
_HISTORY = [
    {"role": "user", "content": "recommend a quiet tower fan"},
    {"role": "assistant", "content": "I recommend the Nimbus Air 2."},
]


def test_resolve_makes_followup_self_contained_with_valid_intent():
    stub = StubLLM(reply='{"intent": "checkout_payment", '
                         '"query": "Check out and pay for the Nimbus Air 2"}')
    core = _core(stub)
    intent, query = core._resolve("yes proceed", _HISTORY)
    assert intent == "checkout_payment"
    assert "Nimbus Air 2" in query
    assert stub.calls, "the LLM client should have been used to resolve context"


def test_resolve_falls_back_to_classify_intent_on_bad_json():
    stub = StubLLM(reply="sorry, I can't do JSON today")
    core = _core(stub)
    text = "show me the catalog"
    intent, query = core._resolve(text, _HISTORY)
    assert intent == classify_intent(text)
    assert query == text


def test_resolve_falls_back_when_intent_not_valid():
    stub = StubLLM(reply='{"intent": "not_a_real_intent", "query": "whatever"}')
    core = _core(stub)
    text = "where is my order"
    intent, query = core._resolve(text, _HISTORY)
    assert intent == classify_intent(text)
    assert query == text


def test_start_request_with_history_resolves_via_llm():
    stub = StubLLM(reply='{"intent": "checkout_payment", '
                         '"query": "Check out and pay for the Nimbus Air 2"}')
    core = _core(stub)
    core._history["conv-1"] = list(_HISTORY)
    cid = core.start_request("yes proceed", conversation_id="conv-1")
    reqs = core.bus.read(core.bus.REQUESTS, last_id="0", block_ms=10)
    _id, env = reqs[0]
    assert env["intent"] == "checkout_payment"
    assert "Nimbus Air 2" in env["payload"]["text"]
    assert env["payload"]["original_text"] == "yes proceed"
    assert env["payload"]["conversation_id"] == "conv-1"
    assert core._pending[cid]["question"] == "yes proceed"
    assert stub.calls


def test_start_request_without_conversation_id_skips_resolve():
    """Backward compatible: no conversation_id -> deterministic classify_intent,
    the resolved text is the (validated) input, and the LLM is not consulted."""
    stub = StubLLM(reply='{"intent": "order_status", "query": "hijacked"}')
    core = _core(stub)
    cid = core.start_request("find wireless earbuds under $120")
    assert cid
    assert stub.calls == []
    reqs = core.bus.read(core.bus.REQUESTS, last_id="0", block_ms=10)
    _id, env = reqs[0]
    assert env["payload"]["text"] == "find wireless earbuds under $120"
    assert env["payload"]["original_text"] == "find wireless earbuds under $120"


def test_remember_caps_history_to_last_six_messages():
    core = _core()
    for i in range(10):
        core.remember("conv-cap", f"u{i}", f"a{i}")
    hist = core._history["conv-cap"]
    assert len(hist) == 6
    assert hist[-1] == {"role": "assistant", "content": "a9"}
    assert hist[-2] == {"role": "user", "content": "u9"}


def test_remember_ignores_missing_conversation_id():
    core = _core()
    core.remember(None, "u", "a")
    assert core._history == {}


# --------------------------------------------------------------------------
# Feature 1: direct Hermes-voice reply (compose_reply)
# --------------------------------------------------------------------------
def test_compose_reply_returns_grounded_text_from_stub():
    stub = StubLLM(reply="Your Nimbus Air 2 ships free with standard delivery in 3 days.")
    core = _core(stub)
    cid = "cid-compose-1"
    core._pending[cid] = {"conversation_id": "conv", "question": "standard delivery for that?"}
    payload = {"reply": "Standard delivery: 3 days, free.", "transcript": []}
    safe, violations = core.compose_reply(cid, payload)
    assert "Nimbus Air 2" in safe
    assert violations == []
    assert stub.calls, "compose_reply should have used the LLM client"


def test_compose_reply_grounds_invented_sku_from_llm():
    stub = StubLLM(reply="Grab the AM-FAKE-9999 unit now.")
    core = _core(stub)
    cid = "cid-compose-2"
    core._pending[cid] = {"conversation_id": None, "question": "buy it"}
    safe, violations = core.compose_reply(cid, {"reply": "ok", "transcript": []})
    assert "AM-FAKE-9999" not in safe
    assert "[unverified SKU]" in safe


def test_compose_reply_falls_back_to_normalize_on_llm_error():
    stub = StubLLM(error=True)
    core = _core(stub)
    cid = "cid-compose-3"
    core._pending[cid] = {"conversation_id": None, "question": "where is my order"}
    payload = {"reply": "Your order ships tomorrow.", "transcript": []}
    safe, _violations = core.compose_reply(cid, payload)
    assert safe == "Your order ships tomorrow."


def test_compose_reply_falls_back_to_transcript_when_reply_empty():
    stub = StubLLM(error=True)
    core = _core(stub)
    cid = "cid-compose-4"
    core._pending[cid] = {"conversation_id": None, "question": "status?"}
    payload = {"reply": None,
               "transcript": [{"agent": "order_agent", "message": "Your order ships tomorrow."}]}
    safe, _violations = core.compose_reply(cid, payload)
    assert safe == "Your order ships tomorrow."
