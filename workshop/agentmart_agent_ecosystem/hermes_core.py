"""Hermes core: the shared A2A client used by the web and Telegram adapters.

Loads the Hermes SOUL.md persona, validates and publishes customer requests
onto the A2A bus, streams AgentMart's responses back, resumes HITL
confirmations, and grounds outgoing replies against the real catalog.

Hermes represents the customer. It keeps a small in-memory per-conversation
history so a context-dependent follow-up ("yes proceed", "standard delivery
for that") can be rewritten into a SELF-CONTAINED query before it is handed to
the stateless AgentMart ecosystem, and it composes a concise, direct reply in
the Hermes voice grounded only in AgentMart's result.
"""

import json, os, uuid
from datetime import datetime, timezone
from a2a_bus import A2ABus
from agentmart_ecosystem import classify_intent, INTENT_PATHS, OpenRouterHermesClient
from guardrails import validate_input, ground_response, known_skus


def _read_soul(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return "You are Hermes/MyShopper. Route product/price/stock/delivery/order questions to AgentMart."


def _text_from_item(item):
    """Pull a human-readable string out of a transcript/reply item."""
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        for k in ("reply", "text", "content", "message"):
            v = item.get(k)
            if isinstance(v, str) and v.strip():
                return v
    return None


def _normalize_reply(reply, payload):
    """Coerce a completed reply into a single human-readable string.

    The worker sets payload["reply"] to `customer_reply or transcript[-1:]`,
    so it may be a plain string, a 1-element list, a dict, or empty. Fall back
    to the last transcript entry, then to a generic completion message. This is
    the fallback path compose_reply uses when the LLM is unavailable or errors.
    """
    if isinstance(reply, str) and reply.strip():
        return reply
    if isinstance(reply, list) and reply:
        s = _text_from_item(reply[-1])
        if s:
            return s
    if isinstance(reply, dict):
        s = _text_from_item(reply)
        if s:
            return s
    transcript = (payload or {}).get("transcript") or []
    if transcript:
        s = _text_from_item(transcript[-1])
        if s:
            return s
    return "AgentMart has completed your request."


def _extract_json(raw):
    """Best-effort pull of the first JSON object out of an LLM reply.

    Models sometimes wrap strict JSON in prose or fenced code blocks; slice
    from the first "{" to the last "}" so json.loads has a fair chance. Raises
    (via json.loads) on anything that still isn't valid, which the callers
    catch and treat as a parse failure.
    """
    if not isinstance(raw, str):
        raise ValueError("non-string LLM reply")
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError("no JSON object found")
    return json.loads(raw[start:end + 1])


_HISTORY_CAP = 6  # keep only the last ~6 messages per conversation


class HermesCore:
    def __init__(self, bus, soul_path="SOUL.md", llm=None):
        self.bus = bus
        self.soul = _read_soul(soul_path)
        self._skus = known_skus()
        # LLM client used for context resolution + reply composition. Injectable
        # so tests can pass a stub; defaults to the shared OpenRouter client.
        self._llm = llm if llm is not None else OpenRouterHermesClient()
        # Per-conversation short-term memory and per-request bookkeeping.
        self._history: dict[str, list[dict]] = {}
        self._pending: dict[str, dict] = {}

    # ------------------------------------------------------------------ memory
    def remember(self, conversation_id, user_text, assistant_reply):
        """Append this turn to the conversation's history, capped to ~6 msgs."""
        if not conversation_id:
            return
        hist = self._history.setdefault(conversation_id, [])
        hist.append({"role": "user", "content": user_text})
        hist.append({"role": "assistant", "content": assistant_reply})
        if len(hist) > _HISTORY_CAP:
            del hist[:-_HISTORY_CAP]

    def _llm_available(self):
        """True when the LLM client can actually answer (not a keyless/dry-run).

        A real OpenRouterHermesClient with no API key (or dry_run) only echoes
        its prompt, so treat it as unavailable and fall back to deterministic
        behavior. Injected test stubs have neither attribute and count as
        available so they get exercised.
        """
        llm = self._llm
        if llm is None:
            return False
        if getattr(llm, "dry_run", False):
            return False
        if hasattr(llm, "api_key") and not llm.api_key:
            return False
        return True

    @staticmethod
    def _format_history(history):
        lines = []
        for turn in history or []:
            role = turn.get("role", "?")
            content = turn.get("content", "")
            who = "Customer" if role == "user" else "Hermes"
            lines.append(f"{who}: {content}")
        return "\n".join(lines)

    def _resolve(self, text, history):
        """Turn a (possibly context-dependent) message into a self-contained
        query + intent, using the LLM and the conversation history.

        Returns (intent, query). On ANY error -- LLM failure, bad JSON, an
        intent outside INTENT_PATHS, an empty query -- falls back to
        (classify_intent(text), text) so a follow-up never breaks the flow.
        """
        valid_intents = list(INTENT_PATHS)
        if not self._llm_available():
            return classify_intent(text), text
        try:
            convo = self._format_history(history)
            system = (
                "You are Hermes, MyShopper's shopping concierge, resolving a "
                "customer's latest message against the conversation so far. "
                "Rewrite that message into a SELF-CONTAINED request that names "
                "the product, order, or option it refers to (so a stateless "
                "backend needs no prior context), and pick the single best "
                "intent for it.\n"
                f"Valid intents (choose exactly one): {', '.join(valid_intents)}.\n"
                'Return STRICT JSON ONLY, no prose: '
                '{"intent": "<one of the valid intents>", '
                '"query": "<self-contained version of the customer\'s request>"}.\n'
                "Examples:\n"
                '- After you recommended "Nimbus Air 2", the customer says '
                '"yes proceed" -> {"intent": "checkout_payment", "query": '
                '"Check out and pay for the Nimbus Air 2"}. (Use '
                '"purchase_intent" instead if they only want to add it / draft '
                "the order.)\n"
                '- After discussing the Nimbus Air 2, "standard delivery for '
                'that" -> {"intent": "product_advice", "query": "What is the '
                'standard delivery option and time for the Nimbus Air 2?"}.'
            )
            user = (
                f"Conversation so far:\n{convo}\n\n"
                f"Latest customer message: {text}\n\nReturn the JSON now."
            )
            raw, _metrics = self._llm.complete_with_metrics("hermes_resolver", system, user)
            data = _extract_json(raw)
            intent = data.get("intent")
            query = data.get("query")
            if intent not in valid_intents or not isinstance(query, str) or not query.strip():
                return classify_intent(text), text
            return intent, query.strip()
        except Exception:
            return classify_intent(text), text

    # --------------------------------------------------------------- lifecycle
    def start_request(self, text, customer_id="CUST-1001", channel="web", conversation_id=None):
        clean, flags = validate_input(text)
        history = self._history.get(conversation_id) if conversation_id else None
        if history:
            intent, resolved = self._resolve(clean, history)
        else:
            intent, resolved = classify_intent(clean), clean
        cid = str(uuid.uuid4())
        env = {"task_id": cid, "correlation_id": cid, "sender": "hermes",
               "recipient": "agentmart", "intent": intent, "state": "proposed",
               "protocol": "agentmart.a2a.v1",
               "payload": {"text": resolved, "original_text": clean,
                           "customer_id": customer_id, "channel": channel,
                           "flags": flags, "conversation_id": conversation_id},
               "metrics": {}, "created_at": datetime.now(timezone.utc).isoformat()}
        self.bus.publish(self.bus.REQUESTS, env)
        # The question the reply step must answer is the customer's ORIGINAL
        # message, not the resolved query.
        self._pending[cid] = {"conversation_id": conversation_id, "question": clean}
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

    # ------------------------------------------------------------------- reply
    @staticmethod
    def _agentmart_context(payload):
        """Render AgentMart's result into a compact grounding block for the LLM.

        Uses only what AgentMart actually returned -- its transcript, reply and
        draft_order -- so the composed answer can never introduce products,
        SKUs or prices the backend did not surface.
        """
        payload = payload or {}
        parts = []
        reply = payload.get("reply")
        reply_str = _normalize_reply(reply, payload)
        if reply_str:
            parts.append(f"AgentMart reply: {reply_str}")
        transcript = payload.get("transcript") or []
        for item in transcript:
            s = _text_from_item(item)
            if s:
                agent = item.get("agent") if isinstance(item, dict) else None
                parts.append(f"- {agent + ': ' if agent else ''}{s}")
        draft = payload.get("draft_order")
        if draft:
            try:
                parts.append("Draft order: " + json.dumps(draft, ensure_ascii=False))
            except (TypeError, ValueError):
                parts.append(f"Draft order: {draft}")
        return "\n".join(parts) if parts else reply_str

    def compose_reply(self, cid, agentmart_payload):
        """Compose a concise, direct, customer-facing reply in the Hermes voice.

        Answers the customer's ORIGINAL question grounded ONLY in AgentMart's
        result, then runs the same grounding as finalize_reply. On ANY LLM
        error falls back to the current behavior (_normalize_reply + grounding).

        Returns (safe_text, violations).
        """
        payload = agentmart_payload or {}
        pending = self._pending.get(cid) or {}
        question = pending.get("question") or ""
        if self._llm_available():
            try:
                context = self._agentmart_context(payload)
                system = (
                    self.soul
                    + "\n\n---\n"
                    "You are now writing the customer-facing reply directly to "
                    "the customer. Answer their question in the second person, "
                    "concisely and directly -- lead with exactly what they "
                    "asked (e.g. if they asked about standard delivery, open "
                    "with that). Ground every fact ONLY in AgentMart's result "
                    "below; never invent SKUs, prices, stock, or delivery "
                    "details. Keep it short."
                )
                user = (
                    f"The customer asked: {question}\n\n"
                    f"AgentMart's result (the ONLY source of truth):\n{context}\n\n"
                    "Write the reply now."
                )
                raw, _metrics = self._llm.complete_with_metrics("hermes_reply", system, user)
                if isinstance(raw, str) and raw.strip():
                    return self.finalize_reply(raw.strip())
            except Exception:
                pass
        # Fallback: current behavior.
        reply_str = _normalize_reply(payload.get("reply"), payload)
        return self.finalize_reply(reply_str)

    def finalize_reply(self, reply_text):
        return ground_response(reply_text, self._skus)
