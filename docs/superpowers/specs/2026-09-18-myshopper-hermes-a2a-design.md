# MyShopper / Hermes — Redis-Streams A2A Buying Agent
**Design & Build Guide (Day-3 Project)**

- **Date:** 2026-09-18
- **Author:** paul@vargas.im (with Claude)
- **Status:** Design — pending user review
- **Repo (working):** clone of `kenken64/Building-Autonomous-AI-Agent`; deliverable will be a fresh repo under the user's account.

---

## 1. Goal

Build **MyShopper** (a.k.a. **Hermes**), a customer-owned buying agent that talks to the **AgentMart** back-office ecosystem over **A2A (Agent-to-Agent)**. The customer chats with Hermes through **Telegram**; Hermes coordinates AgentMart's five LangGraph agents to search, price, check stock, arrange fulfillment, and place an order — never letting the customer touch the back office directly.

This project satisfies a graded **submission brief** while staying faithful to the **Day-3 lecture**, whose A2A thesis is **Redis Streams** (async, stream-native), not HTTP.

### Submission requirements → where they are met

| # | Requirement | How this design meets it |
|---|-------------|--------------------------|
| 1 | Configure the Hermes Agent (MyShopper) | Hermes is its own process: Telegram front door + Redis A2A client + `SOUL.md` persona (§4.3) |
| 2 | A2A connectivity Hermes ↔ AgentMart | Real async A2A over **Redis Streams** — envelopes with lifecycle + correlation (§5) |
| 3 | Implement 5 agents in LangGraph | Existing graph reused unchanged: Shopping, Pricing, Inventory, Fulfillment, Order (+ Payment) (§4.2) |
| 4 | Capture token usage + elapsed time | Instrumented in the LLM client; attached to every A2A envelope; aggregated + displayed (§6) |
| 5 | Logging evidence of agent↔graph comms | Structured `logging` on both processes + `a2a_audit.jsonl` + the Redis stream is itself replayable evidence (§7) |
| 6 | API provider: OpenAI or OpenRouter | **OpenAI `gpt-4o-mini`** (code already prefers `OPENAI_*` vars) (§8) |
| + | Web chat client (primary channel) | Claude-style page: chat + live A2A trace, over the shared Hermes core (§4.3, §4.4) |
| + | Telegram integration (user requirement) | Second channel adapter over the same Hermes core; added when the bot token is available (§4.3) |
| + | Human-in-the-loop (design choice) | Checkout pauses in Telegram for "Confirm $X? Yes/No" before Order/Payment (§4.3) |
| + | Heartbeats / liveness (lecture Part 4) | Agents emit heartbeats to a Redis stream; Hermes detects dead agents (§9) |
| + | Guardrails (lecture Part 5 / OWASP) | Grounding (no invented SKUs/prices) + prompt-injection/input validation (§9.5) |

---

## 2. Design principles

1. **Reuse the working scaffold.** The LangGraph graph, the 5 agents, the payment node, the SQLite data layer, the A2A envelope dataclass, and `SOUL.md` already work and are covered by `test_scenarios.py`. We wrap and connect them; we do not rewrite them.
2. **Keep the test suite green.** `test_scenarios.py` asserts exact agent paths incl. the `hermes_myshopper` intake node. We do **not** rename graph nodes. The graph runs identically inside the new worker process.
3. **Never delete the user's files.** The HTTP `a2a_server.py` is kept as a secondary interface, not deleted.
4. **Simulated money only.** `orders.py` already generates `sim_` payment refs. No real payment credentials ever enter the code or repo.
5. **Secrets never in git.** `.env` is git-ignored; keys live only there.

---

## 3. Prerequisites & environment

| Need | Status on this machine | Action |
|------|------------------------|--------|
| Python 3.10+ | ✅ **installed** (3.12.10 via winget, host dev) | venv per §12 |
| Docker (for Redis + delivery compose) | ✅ installed + daemon running (27.2.0) | Redis container for dev; full compose for delivery |
| OpenAI API key | ⏳ user provides | Paste into `.env` as `OPENAI_API_KEY` |
| Telegram bot token | ⏳ user provides | Create via **@BotFather** → `.env` `TELEGRAM_BOT_TOKEN` |
| GitHub repo | ⏳ user decides name/visibility | New repo under user's account; push only on confirmation |
| Screen recorder | user's own tool | For the demo video (manual) |

New Python deps to add to `requirements.txt`: `redis>=5.0`, `python-telegram-bot>=21`, `fastapi>=0.110`, `uvicorn>=0.29`. (Existing: `langgraph`, `openai`, `python-dotenv`.)

---

## 4. Architecture

Four cooperating pieces around a Redis message bus.

```
  Web chat (primary) ─┐
  Telegram (later) ───┤ channel adapters
                      ▼
┌─────────────────────┐                      ┌──────────────────────┐
│ HERMES core          │   XADD proposed      │   Redis Streams      │
│ • SOUL.md persona    │ ───envelope───────▶  │  a2a:requests        │
│ • Redis producer/    │                      │  a2a:responses       │
│   consumer           │ ◀─progress/completed─│  a2a:heartbeats      │
│ • HITL approval gate │                      └──────────┬───────────┘
└──────────┬───────────┘                                 │ XREADGROUP
           │ SSE (chat + trace)                          ▼
           ▼                                  ┌──────────────────────┐
┌──────────────────────────┐                 │  AgentMart worker     │
│  WEB CLIENT (FastAPI+SSE) │                 │  consume request →    │
│  • left: chat w/ Hermes   │ ◀──tails────────│  run LangGraph graph  │
│    (+ HITL Yes/No buttons)│    streams      │  (5 agents + payment) │
│  • right: live A2A trace  │                 │  → emit progress/done │
│    (tokens + elapsed)     │                 └──────────────────────┘
└──────────────────────────┘
```

### 4.1 Redis (message bus)
A single Redis 7 container. Streams used:
- `a2a:requests` — Hermes → AgentMart request envelopes (consumer group `agentmart`).
- `a2a:responses` — AgentMart → Hermes progress + result envelopes.
- `a2a:heartbeats` — agents' liveness pings.
- Correlation is by `correlation_id`; the dashboard and Hermes filter on it.

### 4.2 AgentMart worker (`agentmart_worker.py`, new — wraps existing graph)
- Joins consumer group on `a2a:requests`, reads request envelopes.
- For each request: emits `accepted`, runs the **existing LangGraph graph** (`hermes_myshopper → shopping → {pricing, inventory, fulfillment} → order → payment`), emitting an `in_progress` envelope per node hop and a final `completed` (or `failed`) envelope to `a2a:responses`.
- Emits a heartbeat per agent on each superstep (§9).
- **The graph itself is unchanged** — the worker is a thin Redis adapter around `run_agentmart()`.

### 4.3 Hermes core + channel adapters (`hermes_core.py` + adapters, new)
Hermes is split into a shared **core** and thin **channel adapters** (the lecture's "channels as adapters"):
- **`hermes_core.py`** — the reusable A2A client + brains:
  - Loads **`SOUL.md`** as system-prompt slot one (identity + the routing rule: always call AgentMart, relay SKUs verbatim, never answer from memory).
  - **A2A client:** `XADD` a `proposed` request envelope to `a2a:requests`; consume correlated envelopes from `a2a:responses`; expose them as a stream of progress events.
  - **HITL approval gate:** on buy/checkout, when AgentMart returns a draft order awaiting payment, the core raises an "approval needed ($X)" event; on approve it emits a follow-up `confirm` envelope (Order → Payment), on decline it cancels. (Autonomous reads, gated write.)
- **Web adapter** (primary) — see §4.4.
- **Telegram adapter** (`hermes_telegram.py`, added when the bot token exists) — `python-telegram-bot` long-polling; same core; HITL rendered as inline Yes/No buttons.

### 4.4 Web client — chat + live trace (`hermes_web.py`, new — FastAPI + SSE) — PRIMARY channel
A single Claude-style page, no Telegram token required:
- **Left: chat with Hermes** — customer types here; messages + Hermes replies as bubbles; the HITL gate renders as **[Yes] / [No]** buttons inline.
- **Right: live A2A trace** — each hop: which agent, request, response, next hop, **token count**, **elapsed ms**, lifecycle badge; running totals.
- Backend: `POST /chat` drives `hermes_core`; an SSE endpoint streams both chat replies and trace events (it tails the Redis streams).
- This one screen is the visual proof of reqs #2/#4/#5 and the video's money-shot.

---

## 5. A2A envelope schema (Redis Streams)

Reuse the existing `A2AEnvelope` shape, serialized as a Redis stream entry (fields flattened / JSON payload):

```jsonc
{
  "envelope_id": "uuid",
  "correlation_id": "uuid",        // one shopping session
  "sender": "hermes" | "shopping" | "pricing" | ... ,
  "recipient": "agentmart" | "hermes" | "<agent>",
  "task_id": "uuid",
  "lifecycle": "proposed|accepted|in_progress|completed|failed",
  "intent": "list-products|buy-this|checkout-and-pay|order-status|product-advice|...",
  "payload": { /* message / shortlist / draft_order / result */ },
  "metrics": { "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "elapsed_ms": 0 },
  "created_at": "ISO-8601"
}
```

- **Lifecycle** entries are appended in order → the stream is a replayable transcript.
- **Replay / resume** (lecture Slide 39–40): re-read a correlation from stream offset 0 to reconstruct the whole conversation, or resume a consumer after a crash from its last-acked ID.

---

## 6. Token usage + elapsed time (req #4)

- Modify the LLM client's `complete()` to (a) wrap the call in `time.perf_counter()` and (b) read `response.usage` (`prompt_tokens`, `completion_tokens`, `total_tokens`). Return a small `metrics` object alongside `content`.
- Each agent node attaches its `metrics` to the `in_progress`/`completed` envelope it emits.
- The AgentMart worker aggregates per-correlation totals; Hermes shows a summary line in Telegram ("🧾 1,240 tokens · 3.8s"); the dashboard shows per-hop + totals.
- Also written to `a2a_audit.jsonl` for offline evidence.

---

## 7. Logging evidence (req #5)

Three overlapping layers of evidence:
1. **Structured `logging`** on both Hermes and the AgentMart worker (one line per envelope in/out, with correlation_id, sender→recipient, lifecycle, tokens, ms).
2. **`a2a_audit.jsonl`** — append every envelope; a durable, greppable audit trail.
3. **The Redis streams themselves** — replayable proof that agents communicated through the graph.

The demo video will show the console logs + the audit file + the dashboard side by side.

---

## 8. Model configuration (req #6)

- Default to **OpenAI `gpt-4o-mini`**. The existing client already prefers `OPENAI_*` over `OPENROUTER_*`.
- `.env`: `OPENAI_API_KEY=...`, `OPENAI_MODEL=gpt-4o-mini`, `OPENAI_BASE_URL=https://api.openai.com/v1`.
- Replace the fictional `qwen/qwen3.7-flash` default in `.env.example`, `hermes_a2a_config.json`, and the `--check-model` path.
- Verify connectivity with `python agentmart_ecosystem.py --check-model` once the key is in.

---

## 9. Heartbeats & liveness (lecture Part 4, opt-in — included)

- Each AgentMart agent `XADD`s a heartbeat `{agent, ts, status: healthy}` to `a2a:heartbeats` on each activation (and optionally on a timer).
- Hermes (or a small monitor in the worker) tracks last-seen per agent; if no heartbeat within `N × interval`, it flags the agent **dead** and surfaces it on the dashboard + a Telegram warning.
- Demonstrates the lecture's dead-agent detection; the video can kill an agent and show detection.

---

## 9.5 Guardrails (safety core — `guardrails.py`, new)

Two focused guardrails, applied in Hermes core and the AgentMart worker:

1. **Grounding guard — no invented SKUs/prices.** Before Hermes relays any agent output to the customer, extract SKUs (`AM-XXX-0000` pattern, already in `SKU_PATTERN`) and prices; verify each against the seeded catalog (`query_products`). Any SKU/price not in the catalog is flagged and stripped, and the hop is marked `grounding_violation` in the trace + audit. Mirrors the repo's "invented-SKU" metric.
2. **Prompt-injection / input validation.** Customer input and product/catalog text are **data, not instructions**. Validate customer input (length cap, control-char strip); wrap untrusted text so embedded directives ("ignore your instructions", "reveal the system prompt") cannot override `SOUL.md` routing. Suspicious inputs are flagged in the trace and answered with a safe refusal, never executed.

Both emit a `guardrail` event to the trace/audit so the video can show them firing. (Money/robustness/rate-limit guardrails are documented as future work — see §16.)

## 10. Data flow — the "buy-then-checkout" scenario (the video money-shot)

1. Customer (Telegram): *"buy wireless earbuds under $120"*.
2. Hermes classifies via SOUL rule → `XADD` `proposed` envelope to `a2a:requests`.
3. AgentMart worker: `accepted` → runs graph: `hermes_myshopper → shopping → {pricing, inventory, fulfillment}(parallel) → order(draft)`; emits an `in_progress` envelope per hop (with metrics).
4. Worker emits `completed` with the **draft order**.
5. Hermes shows the draft in Telegram + **"Confirm $X? [Yes][No]"**.
6. On **Yes** → Hermes `XADD`s `confirm` → worker runs `order → payment` (simulated `sim_` ref) → `completed` with confirmation.
7. Hermes replies with the order confirmation + a **token/time summary**.
8. Throughout: the dashboard streams every hop live; every envelope is logged + audited.

---

## 11. Implementation plan (phased)

Each phase ends green (tests pass, something demoable). TDD where practical.

- **Phase 0 — Environment.** (Python 3.12.10 already installed.) Create venv; `pip install -r requirements.txt` (+ new deps); `Copy-Item .env.example .env` and fill secrets; `python seed_data.py`; start Redis container; `--check-model` passes. *Exit:* existing `test_scenarios.py` dry-run passes.
- **Phase 1 — Redis A2A envelope layer.** Add `a2a_bus.py`: publish/consume envelopes on the streams; envelope (de)serialization; audit-log writer. Unit tests with a Redis test container / fakeredis. *Exit:* round-trip an envelope through Redis.
- **Phase 2 — AgentMart worker.** `agentmart_worker.py` wraps `run_agentmart()`; consumes `a2a:requests`, emits lifecycle + result envelopes; graph unchanged. *Exit:* send a request envelope by hand, get a correct `completed` envelope; `test_scenarios.py` still green.
- **Phase 3 — Instrumentation.** Token + elapsed capture in the LLM client; attach to envelopes; aggregate; write audit. *Exit:* metrics visible in envelopes + audit file.
- **Phase 3b — Guardrails module.** `guardrails.py`: grounding check (SKUs/prices vs catalog) + input validation / prompt-injection wrap; emit `guardrail` trace events. *Exit:* unit tests prove invented SKUs are stripped and injection attempts are neutralized.
- **Phase 4 — Hermes core + Web chat client (primary).** `hermes_core.py` (SOUL.md load, A2A request/response, progress events) + `hermes_web.py` (FastAPI `POST /chat` + SSE, chat bubbles). *Exit:* full Q&A with Hermes through the browser, no Telegram token needed.
- **Phase 5 — HITL gate.** Approval event in the core; **[Yes]/[No]** buttons in the web chat before Order/Payment. *Exit:* checkout pauses in the browser and resumes on Yes.
- **Phase 6 — Live trace panel + metrics.** Right-hand SSE timeline: per-hop agent/request/response/next + **tokens + elapsed** + running totals. *Exit:* the one-screen Claude-style chat+trace mirrors a live session.
- **Phase 7 — Heartbeats/liveness.** Heartbeat emit + dead-agent detection + a status indicator in the web UI. *Exit:* killing an agent is detected and shown.
- **Phase 7b — Telegram adapter (when token available).** `hermes_telegram.py` over the same core; HITL as inline buttons. *Exit:* same flow works in Telegram. (Non-blocking; can slot in anytime after Phase 5.)
- **Phase 8 — Dockerize + docs + repo.** Add Dockerfiles for the 3 app services (worker, hermes, dashboard) + a `docker-compose.yml` (those 3 + Redis, secrets via `env_file: .env`, Redis on a named volume). Verify `docker compose up` brings the whole ecosystem up. Update README with the run guide; write the video script; create the new GitHub repo; push on confirmation. **Dev stays on host Python (fast loop); delivery is `docker compose up`.**

---

## 12. How to run (final state)

```powershell
# one-time
cd workshop\agentmart_agent_ecosystem
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env        # then edit: OPENAI_API_KEY, OPENAI_MODEL, TELEGRAM_BOT_TOKEN
python seed_data.py
docker run -d -p 6379:6379 --name myshopper-redis redis:7

# verify model
python agentmart_ecosystem.py --check-model

# run (terminals, or a launcher script)
python agentmart_worker.py       # AgentMart consumers
python hermes_web.py             # Web chat + live trace at http://127.0.0.1:8000  (PRIMARY)
# optional, only when a bot token is in keyvault.env:
python hermes_telegram.py        # Telegram adapter (same Hermes core)
# keep an eye on a2a_audit.jsonl / logs for evidence
```

### Delivery (the video's opening shot): everything in Docker
```powershell
# .env already filled with OPENAI_API_KEY, OPENAI_MODEL, TELEGRAM_BOT_TOKEN
docker compose up --build       # Redis + agentmart-worker + hermes-web (+ hermes-telegram if token set)
# open http://127.0.0.1:8000 → chat with Hermes + watch the live A2A trace
```
Dev loop stays on host Python (above) for speed; the compose stack is the shipped artifact.

Offline/no-key demo still works via the existing dry-run path.

---

## 13. Demo video checklist (deliverable)

Show, in order: (1) `docker compose up` bringing the ecosystem up; (2) the **web client** — chat with Hermes for a product search, with the **live A2A trace** streaming each agent hop (tokens + elapsed) on the right; (3) a **buy-then-checkout** with the **HITL Yes/No** buttons in the chat; (4) the **logs + `a2a_audit.jsonl`** as communication evidence; (5) optionally, killing an agent to show **heartbeat/dead-agent detection**; (6) a closing token/time summary; (7) optionally, the **same flow in Telegram** if the bot token is ready.

---

## 14. Testing

- `test_scenarios.py` must stay green (9/9 dry-run) — run before/after every phase.
- New tests: envelope round-trip through Redis (Phase 1), worker produces correct lifecycle (Phase 2), metrics present (Phase 3), HITL gate logic (Phase 5), heartbeat/dead-detection (Phase 7). Telegram + dashboard smoke-tested manually + with mocked handlers.

---

## 15. Risks & mitigations

| Risk | Mitigation |
|------|------------|
| Redis not running during demo | Health-check on startup; clear error; Docker daemon confirmed present |
| OpenAI rate limits / cost during video | `gpt-4o-mini` is cheap; pre-warm; dry-run fallback |
| Telegram long-polling vs webhook | Long-polling (no public URL needed) for local demo |
| Graph refactor breaks tests | Worker wraps graph without editing nodes; run tests each phase |
| Secrets leak to repo | `.env` git-ignored; audit before first push |
| Scope creep (NanoClaw/voice/skills from lecture) | Explicitly out of scope; documented as future work |

---

## 16. Out of scope (documented future work)
WhatsApp/voice channels, NanoClaw edge deployment, skill-writing/sandboxing labs, real payments, the external "Hermes gateway" product, HTTP A2A as the primary path (kept only as a secondary interface). **Guardrails deferred:** configurable spend-limit gate, A2A envelope schema/size validation, secret/PII log redaction, and per-session rate limiting (only the grounding + prompt-injection safety core is in scope — §9.5).
