"""Hermes core: the shared A2A client used by the web and Telegram adapters.

Loads the Hermes SOUL.md persona, validates and publishes customer requests
onto the A2A bus, streams AgentMart's responses back, resumes HITL
confirmations, and grounds outgoing replies against the real catalog.
"""

import os, uuid
from datetime import datetime, timezone
from a2a_bus import A2ABus
from agentmart_ecosystem import classify_intent
from guardrails import validate_input, ground_response, known_skus


def _read_soul(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return "You are Hermes/MyShopper. Route product/price/stock/delivery/order questions to AgentMart."


class HermesCore:
    def __init__(self, bus, soul_path="SOUL.md"):
        self.bus = bus
        self.soul = _read_soul(soul_path)
        self._skus = known_skus()

    def start_request(self, text, customer_id="AM-CUST-0001", channel="web"):
        clean, flags = validate_input(text)
        cid = str(uuid.uuid4())
        env = {"task_id": cid, "correlation_id": cid, "sender": "hermes",
               "recipient": "agentmart", "intent": classify_intent(clean), "state": "proposed",
               "protocol": "agentmart.a2a.v1",
               "payload": {"text": clean, "customer_id": customer_id, "channel": channel, "flags": flags},
               "metrics": {}, "created_at": datetime.now(timezone.utc).isoformat()}
        self.bus.publish(self.bus.REQUESTS, env)
        return cid

    def stream_events(self, correlation_id, timeout_ms=120000):
        # timeout_ms bounds INACTIVITY, not total run time: the worker now streams
        # hops as each agent finishes, and a real gpt-4o-mini run takes ~18s end to
        # end, so a total-time budget would still close mid-run. Reset the idle
        # counter on every read (matching this cid or not) so a long-but-active run
        # never times out; only a genuinely silent window closes the stream.
        last_id, waited = "0", 0
        while waited < timeout_ms:
            items = self.bus.read(self.bus.RESPONSES, last_id=last_id, block_ms=1000)
            if not items:
                waited += 1000
                continue
            waited = 0
            for entry_id, env in items:
                last_id = entry_id
                if env.get("correlation_id") != correlation_id:
                    continue
                yield env
                # Only the TERMINAL framing envelope (agentmart -> hermes) ends the
                # stream. Per-agent hops (e.g. order_agent -> hermes_myshopper) reuse
                # the same completed/failed lifecycle states for their own sub-task,
                # so terminating on ANY completed/failed envelope regardless of sender
                # would stop the stream at the first agent hop and drop the real
                # terminal envelope (and its transcript) that follows it. An
                # input_required envelope (any sender) must not terminate the stream
                # either -- it falls through here unaffected.
                if env.get("sender") == "agentmart" and env.get("state") in ("completed", "failed"):
                    return

    def confirm(self, correlation_id, task_id, approved):
        state = "confirm" if approved else "cancel"
        env = {"task_id": task_id, "correlation_id": correlation_id, "sender": "hermes",
               "recipient": "agentmart", "intent": "checkout-and-pay", "state": state,
               "protocol": "agentmart.a2a.v1", "payload": {"approved": approved},
               "metrics": {}, "created_at": datetime.now(timezone.utc).isoformat()}
        self.bus.publish(self.bus.REQUESTS, env)

    def finalize_reply(self, reply_text):
        return ground_response(reply_text, self._skus)
