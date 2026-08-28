"""Bootstrap gate shared by all interactive Telegram handlers.

Kurigram must connect before its dispatcher exists, so a tiny connection-to-
ready window is unavoidable.  Database preparation is completed before that
connection, and these highest-priority handlers reject anything received in
the remaining window instead of allowing normal handlers to observe partially
initialized state.
"""

import asyncio
import logging

from pyrogram import Client, filters
from pyrogram.types import CallbackQuery, ChatMemberUpdated, Message

from database.redis_client import redis_state
from plugins.callbacks import answer_callback_safely
from plugins.telegram_retry import INTERACTIVE_RETRY, telegram_call


logger = logging.getLogger(__name__)
_ready = asyncio.Event()


def mark_ready() -> None:
    _ready.set()


def mark_not_ready() -> None:
    _ready.clear()


def is_ready() -> bool:
    return _ready.is_set()


@Client.on_message(filters.all, group=-10_000)
async def readiness_message_gate(client: Client, message: Message):
    if is_ready():
        claimed = await redis_state.claim_once(
            "telegram-update-message", f"{message.chat.id}:{message.id}", 120
        )
        if claimed:
            return
        message.stop_propagation()
        return
    logger.info("Dropped update during bootstrap chat=%s", getattr(message.chat, "id", None))
    await telegram_call(
        lambda: message.reply_text(
            "⏳ The bot is starting. Please try again in a few seconds.",
            reply_parameters=None,
        ),
        route="bootstrap_message",
        policy=INTERACTIVE_RETRY,
        retry_safe=False,
    )
    message.stop_propagation()


@Client.on_callback_query(group=-10_000)
async def readiness_callback_gate(client: Client, callback: CallbackQuery):
    # Telegram's callback deadline is stricter than any database operation:
    # ACK before even the Redis cross-replica deduplication lookup.
    await answer_callback_safely(callback)
    if is_ready():
        claimed = await redis_state.claim_once(
            "telegram-update-callback", callback.id, 120
        )
        if claimed:
            return
        callback.stop_propagation()
        return
    await answer_callback_safely(
        callback,
        "The bot is still starting. Please retry shortly.",
        show_alert=True,
    )
    callback.stop_propagation()


@Client.on_chat_member_updated(group=-10_000)
async def readiness_member_gate(client: Client, update: ChatMemberUpdated):
    if is_ready():
        return
    update.stop_propagation()
