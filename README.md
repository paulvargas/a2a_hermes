# 🛍️ MyShopper (Hermes) — your personal AI shopping assistant

MyShopper is a friendly AI shopping assistant you can chat with — in your **web browser** or on **Telegram**. Behind the scenes it quietly coordinates a small team of specialist "agents" (product search, pricing, stock, delivery, and ordering) to find products, check your orders, and place a purchase for you.

Before it ever "buys" anything, it **stops and asks you to approve** — so you're always in control.

> 🧪 **This is a demo.** All payments are **simulated** — no real money is ever charged, and no real payment company is contacted.

---

## What you'll need (one‑time setup)

Just three things:

1. **Docker Desktop** — the free program that runs the app for you.
   Download it here → https://www.docker.com/products/docker-desktop/
   Install it, open it, and wait until it says it's **running**.

2. **An OpenAI key** — this is the "brain" that powers the chat.
   Get one here → https://platform.openai.com/api-keys
   Sign in, click **Create new secret key**, and copy it. It looks like `sk-...`.
   *(This may require adding a small amount of billing credit to your OpenAI account.)*

3. **(Only if you want Telegram)** A Telegram bot — takes 2 minutes, steps are in the **Telegram** section below.

---

## Install & run (about 5 minutes)

**Step 1 — Get the code.**
Click the green **Code** button at the top of this page → **Download ZIP**, then unzip it.
*(Or, if you use git: `git clone https://github.com/paulvargas/a2a_hermes.git`)*

**Step 2 — Open a terminal in the app folder.**
The app lives in the folder **`workshop/agentmart_agent_ecosystem`**.
- **Windows:** open **PowerShell**, then move into that folder, e.g.
  `cd path\to\a2a_hermes\workshop\agentmart_agent_ecosystem`
- **Mac:** open **Terminal**, then `cd path/to/a2a_hermes/workshop/agentmart_agent_ecosystem`

**Step 3 — Add your OpenAI key.**
In that folder there's a file called **`.env.example`**. Make a **copy** of it and name the copy **`.env`**.
Open `.env` in any text editor and set these two lines:
```
OPENAI_API_KEY=sk-...paste-your-key-here...
OPENAI_MODEL=gpt-4o-mini
```
Save the file. *(You'll add a Telegram line later, only if you want Telegram.)*

**Step 4 — Start the app (one command).**
Make sure Docker Desktop is open, then run:
```
docker compose up --build
```
The **first** run takes a few minutes while it downloads and sets everything up. When the text stops scrolling and you see a line mentioning **"Uvicorn running"**, it's ready.

**Step 5 — Open it in your browser:**
👉 **http://localhost:8000**

**To stop the app:** press **Ctrl + C** in the terminal (or run `docker compose down`).

---

## Try it in your browser

Type any of these into the chat box on the **left**, and watch the **right‑hand panel** show the agents working together in real time:

- `find wireless earbuds under $120 with good battery life`
- `what is my order status?`
- `I want to buy this AM-EAR-1002`
- `Checkout and pay for my order.`
  → MyShopper will pause and ask **"Confirm purchase of $93.00?"** — click **Approve** to finish, or **Decline** to cancel. (Remember: it's a simulated payment.)

---

## Test it on Telegram (chat from your phone)

**Step 1 — Create your own bot (one‑time, ~2 minutes).**
- In Telegram, search for **@BotFather** (the official one with a blue checkmark) and open the chat.
- Send **`/newbot`** and follow the prompts: choose a display name, then a username that ends in **`bot`**.
- BotFather replies with a **token** that looks like `123456789:AAE...`. **Copy it.**

**Step 2 — Give the app your token.**
Open your **`.env`** file (from Step 3 above) and add this line:
```
TELEGRAM_BOT_TOKEN=123456789:AAE...paste-your-token-here...
```
Save it.

**Step 3 — Start the app with Telegram switched on:**
```
docker compose --profile telegram up --build
```

**Step 4 — Chat with your bot.**
- Open your bot in Telegram (BotFather gave you a `t.me/your_bot_name` link), press **Start**, and try:
  - `find wireless earbuds under $120`
  - `what is my order status?`
  - `I want to buy this AM-EAR-1002`
  - `Checkout and pay for my order.` → the bot shows **Approve / Decline** buttons — tap **Approve** to complete the (simulated) payment.

Same assistant, now on your phone. 📱

---

## Good to know

- 💳 **No real charges.** Every payment is simulated for this demo.
- 🔒 **Your keys stay on your computer.** They live only in your `.env` file and are never uploaded or shared.
- 🔁 **Want to redo the checkout?** Once the sample order is paid, refresh the demo data with:
  ```
  docker compose exec agentmart-worker python seed_data.py
  ```
- ❓ **Nothing loads at localhost:8000?** Give it another minute on first run, make sure Docker Desktop is running, and check the terminal for a line saying "Uvicorn running".

---

## For developers

Full technical documentation — architecture diagram, how the A2A / Redis Streams / LangGraph pieces fit together, the submission‑requirement mapping, tests, and the demo recording script — lives here:

- **Technical README:** [`workshop/agentmart_agent_ecosystem/README.md`](workshop/agentmart_agent_ecosystem/README.md)
- **Demo script:** [`workshop/agentmart_agent_ecosystem/docs/DEMO_SCRIPT.md`](workshop/agentmart_agent_ecosystem/docs/DEMO_SCRIPT.md)

*Built on the "Building Autonomous AI Agents" workshop (Day 3).*
