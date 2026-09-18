"""Hermes web client: FastAPI app serving the MyShopper chat UI + live A2A trace.

Exposes a `create_app(core)` factory so tests can inject a fakeredis-backed
`HermesCore`. Routes:
  GET  /                    -> serves static/index.html
  POST /chat {text, ...}    -> {correlation_id}
  GET  /events/{cid}        -> SSE stream of A2A envelopes (grounded reply)
  POST /confirm {cid, ...}  -> resumes a HITL confirmation
"""

import json, os
from fastapi import FastAPI
from fastapi.responses import StreamingResponse, FileResponse
from pydantic import BaseModel

from liveness import Liveness


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
    to the last transcript entry, then to a generic completion message.
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


class ChatIn(BaseModel):
    text: str
    customer_id: str = "CUST-1001"


class ConfirmIn(BaseModel):
    cid: str
    task_id: str
    approved: bool


def create_app(core) -> FastAPI:
    app = FastAPI(title="Hermes / MyShopper")
    here = os.path.dirname(os.path.abspath(__file__))

    # Liveness state for the /agents endpoint. Reading the whole heartbeat
    # stream on every poll would get more expensive as it grows, so this
    # instance persists across requests and only reads entries newer than
    # the last one it has already seen.
    agent_liveness = Liveness()
    liveness_cursor = {"last_id": "0"}

    @app.get("/")
    def index():
        return FileResponse(os.path.join(here, "static", "index.html"))

    @app.get("/agents")
    def agents(ttl_ms: int = 6000):
        # block_ms=None -> non-blocking XREAD (returns immediately with whatever
        # is already on the stream); passing 0 would block the request forever
        # waiting for a new heartbeat instead of just polling the current state.
        for entry_id, env in core.bus.read(
            core.bus.HEARTBEATS, last_id=liveness_cursor["last_id"], block_ms=None, count=1000
        ):
            liveness_cursor["last_id"] = entry_id
            agent = env.get("agent")
            if agent:
                agent_liveness.mark(agent)
        return agent_liveness.snapshot(ttl_ms=ttl_ms)

    @app.post("/chat")
    def chat(body: ChatIn):
        cid = core.start_request(body.text, customer_id=body.customer_id, channel="web")
        return {"correlation_id": cid}

    @app.get("/events/{cid}")
    def events(cid: str):
        def gen():
            for env in core.stream_events(cid):
                state = env.get("state")
                # Only the TERMINAL framing envelope (agentmart -> hermes) carries the
                # real customer-facing reply/transcript. Per-agent hops (e.g.
                # order_agent -> hermes_myshopper) reuse the same lifecycle states
                # (accepted/completed/failed) for their own sub-task, but their
                # payload has no transcript -- rendering them as the chat reply is
                # what produced the "AgentMart has completed your request." placeholder.
                # Those hops still stream through unmodified for the live trace.
                if env.get("sender") == "agentmart" and state in ("completed", "failed"):
                    payload = env.get("payload") or {}
                    if state == "completed":
                        reply_str = _normalize_reply(payload.get("reply"), payload)
                        safe, _violations = core.finalize_reply(reply_str)
                        env.setdefault("payload", {})["reply"] = safe
                    else:  # failed
                        err = payload.get("error") or "Sorry, something went wrong while handling that request."
                        env.setdefault("payload", {})["reply"] = str(err)
                yield f"data: {json.dumps(env)}\n\n"
            yield "event: done\ndata: {}\n\n"

        return StreamingResponse(gen(), media_type="text/event-stream")

    @app.post("/confirm")
    def confirm(body: ConfirmIn):
        core.confirm(body.cid, body.task_id, body.approved)
        return {"ok": True}

    return app


def main():
    from dotenv import load_dotenv
    load_dotenv()
    import redis, uvicorn
    from a2a_bus import A2ABus
    from hermes_core import HermesCore

    client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True,
    )
    app = create_app(HermesCore(bus=A2ABus(client=client)))
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
