"""Offline unit tests for the Telegram adapter's core wiring.

No network access and no bot token: HermesCore is stubbed with a fake that
records calls and replays canned envelopes, and Telegram's Update/Context are
stubbed with plain mocks. Async handlers are driven with asyncio.run (no
pytest-asyncio dependency in this project).
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from hermes_telegram import (
    build_application,
    handle_confirmation,
    handle_message,
    start_command,
)


class FakeCore:
    """Stand-in for HermesCore: records calls, replays canned envelopes."""

    def __init__(self, events=None):
        self.start_request_calls = []
        self.confirm_calls = []
        self.events = events or []

    def start_request(self, text, customer_id="AM-CUST-0001", channel="web"):
        self.start_request_calls.append(
            {"text": text, "customer_id": customer_id, "channel": channel}
        )
        return "cid-1"

    def stream_events(self, correlation_id, timeout_ms=120000):
        for env in self.events:
            yield env

    def finalize_reply(self, reply_text):
        return reply_text, []

    def confirm(self, correlation_id, task_id, approved):
        self.confirm_calls.append((correlation_id, task_id, approved))


def _fake_update(text):
    update = MagicMock()
    update.message.text = text
    update.message.reply_text = AsyncMock()
    return update


def _fake_context(core, customer_id="AM-CUST-0001"):
    context = MagicMock()
    context.bot_data = {"core": core, "customer_id": customer_id}
    return context


def test_handle_message_calls_start_request_with_message_text():
    core = FakeCore(events=[
        {"state": "completed", "correlation_id": "cid-1", "task_id": "t1",
         "sender": "agentmart", "payload": {"reply": "Here you go"}},
    ])
    update = _fake_update("find wireless earbuds under $120")
    context = _fake_context(core)

    asyncio.run(handle_message(update, context))

    assert core.start_request_calls == [
        {"text": "find wireless earbuds under $120", "customer_id": "AM-CUST-0001", "channel": "telegram"}
    ]


def test_handle_message_sends_grounded_final_reply():
    core = FakeCore(events=[
        {"state": "completed", "correlation_id": "cid-1", "task_id": "t1",
         "sender": "agentmart", "payload": {"reply": "Your order is on its way"}},
    ])
    update = _fake_update("where is my order")
    context = _fake_context(core)

    asyncio.run(handle_message(update, context))

    sent = [c.args[0] for c in update.message.reply_text.await_args_list if c.args]
    assert any("Your order is on its way" in t for t in sent)


def test_handle_message_normalizes_list_reply():
    """Same normalization guard as hermes_web: a 1-element transcript list
    must still surface as readable text."""
    core = FakeCore(events=[
        {"state": "completed", "correlation_id": "cid-1", "task_id": "t1",
         "sender": "agentmart",
         "payload": {"reply": [{"agent": "order_agent", "message": "Here are 3 earbud options."}],
                     "transcript": []}},
    ])
    update = _fake_update("show me earbuds")
    context = _fake_context(core)

    asyncio.run(handle_message(update, context))

    sent = [c.args[0] for c in update.message.reply_text.await_args_list if c.args]
    assert any("Here are 3 earbud options." in t for t in sent)


def test_handle_message_sends_inline_keyboard_on_input_required():
    core = FakeCore(events=[
        {"state": "input_required", "correlation_id": "cid-1", "task_id": "t1",
         "sender": "order_agent", "payload": {"summary": "Confirm checkout of AM-ORD-0001?"}},
    ])
    update = _fake_update("checkout")
    context = _fake_context(core)

    asyncio.run(handle_message(update, context))

    keyboard_calls = [c.kwargs for c in update.message.reply_text.await_args_list if "reply_markup" in c.kwargs]
    assert keyboard_calls, "expected an inline Approve/Decline keyboard for input_required"


def test_handle_message_notes_timeout_when_stream_never_terminates():
    """If stream_events ends without completed/failed (inactivity timeout),
    the user should still hear something rather than silence."""
    core = FakeCore(events=[
        {"state": "accepted", "correlation_id": "cid-1", "task_id": "t1",
         "sender": "agentmart", "payload": {}},
    ])
    update = _fake_update("find earbuds")
    context = _fake_context(core)

    asyncio.run(handle_message(update, context))

    sent = [c.args[0] for c in update.message.reply_text.await_args_list if c.args]
    assert any("try again" in t.lower() for t in sent)


def test_handle_confirmation_calls_core_confirm_with_parsed_ids():
    core = FakeCore()
    query = MagicMock()
    query.data = "hitl:cid-1:t1:1"
    query.answer = AsyncMock()
    query.edit_message_text = AsyncMock()
    update = MagicMock()
    update.callback_query = query
    context = _fake_context(core)

    asyncio.run(handle_confirmation(update, context))

    assert core.confirm_calls == [("cid-1", "t1", True)]
    query.answer.assert_awaited_once()
    query.edit_message_text.assert_awaited_once()


def test_handle_confirmation_maps_decline_to_approved_false():
    core = FakeCore()
    query = MagicMock()
    query.data = "hitl:cid-2:t2:0"
    query.answer = AsyncMock()
    query.edit_message_text = AsyncMock()
    update = MagicMock()
    update.callback_query = query
    context = _fake_context(core)

    asyncio.run(handle_confirmation(update, context))

    assert core.confirm_calls == [("cid-2", "t2", False)]


def test_start_command_replies_with_intro():
    update = MagicMock()
    update.message.reply_text = AsyncMock()
    context = MagicMock()

    asyncio.run(start_command(update, context))

    update.message.reply_text.assert_awaited_once()
    assert "Hermes" in update.message.reply_text.await_args.args[0]


def test_build_application_wires_core_and_handlers_without_network():
    core = FakeCore()
    app = build_application(core, "123456:FAKE-TEST-TOKEN")

    assert app.bot_data["core"] is core
    all_handlers = [h for group in app.handlers.values() for h in group]
    assert len(all_handlers) >= 3
