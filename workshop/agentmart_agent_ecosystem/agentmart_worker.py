import logging, uuid
from datetime import datetime, timezone
from a2a_bus import A2ABus
from agentmart_ecosystem import (
    build_graph,
    classify_intent,
    load_hermes_a2a_config,
    load_product_listing,
)

log = logging.getLogger("agentmart.worker")

def _env(state, sender, recipient, intent, payload, correlation_id, task_id, metrics=None):
    return {"task_id": task_id, "correlation_id": correlation_id, "sender": sender,
            "recipient": recipient, "intent": intent, "state": state,
            "protocol": "agentmart.a2a.v1", "payload": payload,
            "metrics": metrics or {}, "created_at": datetime.now(timezone.utc).isoformat()}

def handle_request(env, bus, dry_run=True):
    cid = env.get("correlation_id") or env.get("task_id") or str(uuid.uuid4())
    intent = env.get("intent", "")
    payload = env.get("payload", {})
    text = payload.get("text", "")
    customer_id = payload.get("customer_id", "AM-CUST-0001")
    bus.publish(bus.RESPONSES, _env("accepted", "agentmart", "hermes", intent, {"ack": True}, cid, env["task_id"]))
    log.info("accepted cid=%s intent=%s", cid, intent)
    try:
        # Stream the graph rather than invoke-then-dump: with stream_mode="values"
        # LangGraph yields the full accumulated state after each superstep, so we
        # can publish each agent's a2a_log hops the moment they land instead of
        # holding them all back until the ~18s run finishes. That is what lets the
        # web client's SSE trace fill in real time instead of timing out on silence.
        inputs = {
            "customer_request": text,
            "channel": payload.get("channel", "webchat"),
            "dry_run": dry_run,
            "customer_id": customer_id,
            "intent": (env.get("intent") or None) or classify_intent(text),
            "hermes_a2a_config": load_hermes_a2a_config(),
            "product_listing": load_product_listing(),
            "transcript": [],
            "a2a_log": [],
        }
        app = build_graph()
        seen = 0
        last_state = {}
        for state in app.stream(inputs, stream_mode="values"):
            last_state = state
            hops = state.get("a2a_log", [])
            # Publish only the hops appended since the previous superstep, so each
            # agent's envelopes reach Redis as soon as that agent finishes.
            for hop in hops[seen:]:
                bus.publish(bus.RESPONSES, _env("in_progress", hop.get("sender", "agentmart"),
                            hop.get("recipient", "hermes"), intent, hop, cid, env["task_id"],
                            metrics=hop.get("metrics")))
            seen = len(hops)
        final = _env("completed", "agentmart", "hermes", intent,
                     {"transcript": last_state.get("transcript", []),
                      "draft_order": last_state.get("draft_order"),
                      "reply": last_state.get("customer_reply") or last_state.get("transcript", [])[-1:]},
                     cid, env["task_id"])
        bus.publish(bus.RESPONSES, final)
        log.info("completed cid=%s hops=%d", cid, len(last_state.get("a2a_log", [])))
        return final
    except Exception as exc:  # surface failure as a failed envelope
        final = _env("failed", "agentmart", "hermes", intent, {"error": f"{type(exc).__name__}: {exc}"}, cid, env["task_id"])
        bus.publish(bus.RESPONSES, final)
        log.exception("failed cid=%s", cid)
        return final

def run_worker(bus, dry_run=False):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    last_id = "$"
    log.info("worker up; consuming %s", bus.REQUESTS)
    while True:
        for entry_id, env in bus.read(bus.REQUESTS, last_id=last_id, block_ms=5000):
            last_id = entry_id
            handle_request(env, bus, dry_run=dry_run)

if __name__ == "__main__":
    import redis, os
    from dotenv import load_dotenv
    load_dotenv()
    client = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"),
                         port=int(os.getenv("REDIS_PORT", "6379")), decode_responses=True)
    run_worker(A2ABus(client=client), dry_run=not bool(os.getenv("OPENAI_API_KEY")))
