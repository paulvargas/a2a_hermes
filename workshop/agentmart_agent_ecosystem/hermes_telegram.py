"""Hermes Telegram adapter: a second front-end over the shared Hermes core.

Mirrors hermes_web.py's flow (start_request -> stream_events -> finalize_reply)
but drives it through python-telegram-bot's async `Application` instead of
FastAPI/SSE. A HITL `input_required` envelope becomes an inline
Approve/Decline keyboard; the resulting callback query resumes the flow via
`core.confirm(...)`.

`HermesCore.stream_events` is a blocking generator (it long-polls Redis), so
each message handler drains it on a worker thread via
`loop.run_in_executor(...)` and hands each envelope back to the bot's event
loop with `asyncio.run_coroutine_threadsafe(...)`, keeping the event loop
free to keep answering other chats/callbacks meanwhile.
"""

import asyncio
import logging
import os
import uuid

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logger = logging.getLogger("hermes.telegram")

DEFAULT_CUSTOMER_ID = "AM-CUST-0001"
_CB_PREFIX = "hitl"
_TERMINAL_STATES = ("completed", "failed")
_PENDING_KEY = "pending_confirmations"


def _stash_confirmation(bot_data: dict, cid, task_id) -> str:
    """Store a pending (cid, task_id) pair under a short token and return it.

    Telegram caps InlineKeyboardButton.callback_data at 64 bytes; cid and
    task_id are UUID4s (36 chars each), so embedding both directly (e.g.
    "hitl:{cid}:{task_id}:1") runs ~80 bytes and Telegram rejects the
    sendMessage outright (BadRequest: Button_data_invalid). Keeping only a
    short key in callback_data and the real ids in bot_data keeps every
    button well under the limit.
    """
    pending = bot_data.setdefault(_PENDING_KEY, {})
    key = uuid.uuid4().hex[:8]
    while key in pending:
        key = uuid.uuid4().hex[:8]
    pending[key] = (cid, task_id)
    return key


def _pop_confirmation(bot_data: dict, key: str):
    """Recover and remove a pending (cid, task_id) pair by its short token."""
    pending = bot_data.get(_PENDING_KEY) or {}
    return pending.pop(key, None)


def _text_from_item(item):
    """Pull a human-readable string out of a transcript/reply item.

    Local copy of the same normalization `hermes_web._text_from_item` does:
    that helper is a private module function (not exported), and
    hermes_web.py must not be modified, so a small duplicate lives here
    instead of importing a private name.
    """
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        for k in ("reply", "text", "content", "message"):
            v = item.get(k)
            if isinstance(v, str) and v.strip():
                return v
    return None


def _normalize_reply(reply, payload):
    """Coerce a completed envelope's reply into a single human-readable string.

    Mirrors hermes_web._normalize_reply: the worker sets payload["reply"] to
    `customer_reply or transcript[-1:]`, so it may be a plain string, a
    1-element list, a dict, or empty.
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


def _hop_note(env, seen_senders):
    """Return a short progress note the first time a new agent hop appears.

    Keeps HITL/completion/failure states silent here (those get their own
    messages) and dedupes per-sender so a multi-hop run doesn't spam the chat
    with one line per envelope.
    """
    state = env.get("state")
    sender = env.get("sender")
    if state in _TERMINAL_STATES or state == "input_required":
        return None
    if not sender or sender == "hermes" or sender in seen_senders:
        return None
    seen_senders.add(sender)
    return f"→ {sender} is on it…"


async def _send_text(message, text):
    """Send `text`, preferring Markdown but falling back to plain text.

    Telegram's Markdown parser rejects a lot of ordinary punctuation (stray
    `_`/`*`/`[`), and AgentMart's replies are free-form LLM text, so a failed
    parse must not drop the reply -- just resend it unformatted.
    """
    try:
        await message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
    except Exception:
        await message.reply_text(text)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Hi, I'm Hermes \U0001F44B — MyShopper's assistant. Ask me about "
        "products, prices, stock, delivery, or an existing order and I'll "
        "route it to AgentMart for you."
    )


async def _handle_envelope(message, core, env: dict, seen_senders: set, bot_data: dict) -> None:
    """React to one streamed envelope: HITL prompt, final reply, failure, or hop note."""
    state = env.get("state")
    payload = env.get("payload") or {}

    if state == "input_required":
        cid = env.get("correlation_id")
        task_id = env.get("task_id")
        summary = (
            payload.get("summary")
            or payload.get("text")
            or payload.get("reply")
            or "Hermes needs your confirmation before proceeding with this action."
        )
        approve_key = _stash_confirmation(bot_data, cid, task_id)
        decline_key = _stash_confirmation(bot_data, cid, task_id)
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("Approve", callback_data=f"{_CB_PREFIX}:{approve_key}:1"),
                    InlineKeyboardButton("Decline", callback_data=f"{_CB_PREFIX}:{decline_key}:0"),
                ]
            ]
        )
        await message.reply_text(str(summary), reply_markup=keyboard)
        return

    # Only the TERMINAL framing envelope (sender "agentmart") carries the real
    # customer-facing reply. Per-agent hops (e.g. order_agent -> hermes_myshopper)
    # reuse the same completed/failed lifecycle states for their own sub-task but
    # never carry a transcript -- sending those as the final reply is what
    # produced the "AgentMart has completed your request." placeholder.
    is_terminal_frame = env.get("sender") == "agentmart" and state in _TERMINAL_STATES

    if is_terminal_frame and state == "completed":
        reply_str = _normalize_reply(payload.get("reply"), payload)
        safe, _violations = core.finalize_reply(reply_str)
        await _send_text(message, safe)
        return

    if is_terminal_frame and state == "failed":
        err = payload.get("error") or payload.get("reply") or (
            "Sorry, something went wrong while handling that request."
        )
        await _send_text(message, str(err))
        return

    note = _hop_note(env, seen_senders)
    if note:
        await message.reply_text(note)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    core = context.bot_data["core"]
    text = (update.message.text or "").strip()
    if not text:
        return

    customer_id = context.bot_data.get("customer_id", DEFAULT_CUSTOMER_ID)
    cid = core.start_request(text, customer_id=customer_id, channel="telegram")

    message = update.message
    bot_data = context.bot_data
    await message.reply_text("Working on it…")

    loop = asyncio.get_running_loop()

    def _drain():
        saw_terminal = False
        seen_senders = set()
        for env in core.stream_events(cid):
            # Only the terminal agentmart envelope counts as "we heard back" --
            # an agent hop's own completed/failed lifecycle state is not the
            # real answer (see the sender gate in _handle_envelope above).
            if env.get("sender") == "agentmart" and env.get("state") in _TERMINAL_STATES:
                saw_terminal = True
            asyncio.run_coroutine_threadsafe(
                _handle_envelope(message, core, env, seen_senders, bot_data), loop
            ).result()
        if not saw_terminal:
            asyncio.run_coroutine_threadsafe(
                message.reply_text(
                    "I didn't hear back from AgentMart in time -- please try again."
                ),
                loop,
            ).result()

    try:
        await loop.run_in_executor(None, _drain)
    except Exception:
        logger.exception("hermes_telegram: error streaming events for cid=%s", cid)
        await message.reply_text("Sorry, something went wrong on my end. Please try again.")


async def handle_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    core = context.bot_data["core"]
    query = update.callback_query
    await query.answer()

    try:
        _prefix, short_key, approved_flag = (query.data or "").split(":", 2)
    except ValueError:
        return

    entry = _pop_confirmation(context.bot_data, short_key)
    if entry is None:
        # Unknown or already-used token (e.g. stale button after a restart);
        # nothing left to resolve.
        return
    cid, task_id = entry

    approved = approved_flag == "1"
    core.confirm(cid, task_id, approved)

    ack = "Approved ✓ — completing your order…" if approved else "Declined ✗"
    try:
        await query.edit_message_text(ack)
    except Exception:
        await query.message.reply_text(ack)


def build_application(core, token: str) -> Application:
    """Build the PTB Application wired to `core`. Does not start polling."""
    application = Application.builder().token(token).build()
    application.bot_data["core"] = core
    application.bot_data["customer_id"] = DEFAULT_CUSTOMER_ID

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CallbackQueryHandler(handle_confirmation, pattern=rf"^{_CB_PREFIX}:"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    return application


def main() -> None:
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    # httpx (used by python-telegram-bot for the Bot API HTTP calls) and the
    # telegram/telegram.ext loggers log each request at INFO with the full
    # request URL, which embeds the bot token (https://api.telegram.org/bot
    # <TOKEN>/...). Keep those at WARNING so the token never hits the log
    # stream; hermes.* stays at INFO for our own operational logging.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("telegram.ext").setLevel(logging.WARNING)

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        logger.warning(
            "TELEGRAM_BOT_TOKEN is not set (check .env) -- Telegram adapter will not start."
        )
        return

    import redis
    from a2a_bus import A2ABus
    from hermes_core import HermesCore

    client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True,
    )
    core = HermesCore(bus=A2ABus(client=client))
    application = build_application(core, token)
    logger.info("Hermes Telegram adapter starting long polling")
    application.run_polling()


if __name__ == "__main__":
    main()
