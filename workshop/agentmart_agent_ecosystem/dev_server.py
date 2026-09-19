"""Single-process local dev launcher.

Runs the AgentMart worker (in a background thread) and the Hermes web app
together in ONE process, against the host Redis, so a simple `python dev_server.py`
serves the whole app on port 8000. Use `docker compose up` for the full
multi-container delivery; this file is purely for the local dev-server preview.
"""
import os
import threading

# Resolve paths relative to this file (so .env, static/, data/ load regardless of cwd).
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

load_dotenv()

import redis
import uvicorn

from a2a_bus import A2ABus
from agentmart_worker import run_worker
from hermes_core import HermesCore
from hermes_web import create_app


def _client() -> "redis.Redis":
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True,
    )


def _start_worker() -> None:
    run_worker(A2ABus(client=_client()), dry_run=not bool(os.getenv("OPENAI_API_KEY")))


def main() -> None:
    # AgentMart worker consumes A2A requests in the background.
    threading.Thread(target=_start_worker, daemon=True).start()
    # Hermes web app (chat + live A2A trace).
    app = create_app(HermesCore(bus=A2ABus(client=_client())))
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))


if __name__ == "__main__":
    main()
