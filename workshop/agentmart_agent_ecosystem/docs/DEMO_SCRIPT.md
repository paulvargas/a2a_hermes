# MyShopper / Hermes — demo video shot list

Before recording, restore the seeded order book so there's a fresh order
sitting in `awaiting_payment` for the checkout shot:

```bash
python seed_data.py
```

## Shots

1. **Boot the stack.**
   `docker compose up --build`. Let it settle (Redis healthy, worker and web
   front-end up). Optionally show `docker compose ps`.

2. **Web chat — product search + live trace.**
   Open **http://localhost:8000**. Ask:
   > find wireless earbuds under $120

   Watch the right-hand A2A trace panel fill in real time as each agent
   (Shopping → Pricing → Inventory → Fulfillment → Order) reports in, with its
   own token in/out/total and elapsed-ms badge. Click one card to expand its
   drill-down and show the raw envelope JSON (`correlation_id`, `state`,
   `sender`/`recipient`, `metrics`). Point out the running "Total tokens" /
   "Total elapsed" counters at the top of the panel.

3. **Order status.**
   Ask:
   > what is my order status?

   Show the reply resolving against a real seeded order (not an invented
   one), and note in the trace that only the Order Agent woke up for this
   intent.

4. **Buy → checkout → HITL gate → payment.**
   Ask something like:
   > I want to buy this AM-EAR-1002

   then:
   > checkout and pay for my order

   Show the Approve/Decline prompt appearing (the `input_required` envelope)
   before any payment runs. Click **Approve** and show the flow continuing
   into the Order and Payment agents, ending with a settled order and a
   `sim_`-prefixed payment reference in the reply.

5. **Evidence: logs + audit trail.**
   Show the worker's structured log lines (`accepted cid=... intent=...`,
   `completed cid=... hops=N`) and then open `a2a_audit.jsonl` and scroll to
   the entries matching that `correlation_id` — one JSON line per envelope
   published to Redis, in order, with any secret fields redacted. Optionally
   run `redis-cli -p 6379 XRANGE a2a:responses - +` from inside the redis
   container to show the same envelopes live on the stream.

6. **Optional: heartbeat / dead-agent detection.**
   Stop the worker container (`docker compose stop agentmart-worker`) and
   point out the agent status dots above the chat panel (polled from
   `GET /agents`) flipping from healthy to dead once the TTL elapses. Restart
   it (`docker compose start agentmart-worker`) to show it recover.

7. **Optional: same flow on Telegram.**
   `docker compose --profile telegram up --build` (needs `TELEGRAM_BOT_TOKEN`
   in `.env`). Open the bot (`@your_bot`) and repeat a shortened version of
   steps 2–4: a product question, then a checkout with the inline
   Approve/Decline keyboard.

## Reset between takes

Re-run `python seed_data.py` any time you need a clean order book again
(e.g. before re-recording the checkout shot) — it resets orders back to their
seeded state, including one order left in `awaiting_payment`.
