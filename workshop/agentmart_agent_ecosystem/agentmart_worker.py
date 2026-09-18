import logging, uuid
from datetime import datetime, timezone
from a2a_bus import A2ABus
from agentmart_ecosystem import run_agentmart

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
        result = run_agentmart(text, channel=payload.get("channel", "webchat"),
                               dry_run=dry_run, customer_id=customer_id,
                               intent=env.get("intent") or None)
        for hop in result.get("a2a_log", []):
            bus.publish(bus.RESPONSES, _env("in_progress", hop.get("sender", "agentmart"),
                        hop.get("recipient", "hermes"), intent, hop, cid, env["task_id"],
                        metrics=hop.get("metrics")))
        final = _env("completed", "agentmart", "hermes", intent,
                     {"transcript": result.get("transcript", []),
                      "draft_order": result.get("draft_order"),
                      "reply": result.get("customer_reply") or result.get("transcript", [])[-1:]},
                     cid, env["task_id"])
        bus.publish(bus.RESPONSES, final)
        log.info("completed cid=%s hops=%d", cid, len(result.get("a2a_log", [])))
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
