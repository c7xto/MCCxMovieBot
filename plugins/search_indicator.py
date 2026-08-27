"""Reusable animated search indicator for DM, deep-link and group searches."""

import asyncio
import logging

from pyrogram.enums import ParseMode
from plugins.telegram_retry import INTERACTIVE_RETRY, telegram_call

logger = logging.getLogger(__name__)

_SEARCH_STICKER_SET = "AnimatedEmojies"
_SEARCH_EMOJIS = ("🔎", "🔍")
_sticker_file_id = None


def _select_search_sticker(stickers):
    """Choose Telegram's animated magnifying-glass sticker from the set."""
    for emoji in _SEARCH_EMOJIS:
        for sticker in stickers:
            if getattr(sticker, "emoji", "") == emoji and getattr(
                sticker, "is_animated", False
            ):
                return sticker
    return None


async def _resolve_search_sticker(client):
    """Resolve once per process, then reuse Telegram's native sticker ID."""
    global _sticker_file_id
    if _sticker_file_id:
        return _sticker_file_id

    stickers = await asyncio.wait_for(
        telegram_call(
            lambda: client.get_stickers(_SEARCH_STICKER_SET),
            route="search_indicator_resolve",
            policy=INTERACTIVE_RETRY,
            retry_safe=True,
            idempotency_key="search-sticker-set",
        ),
        timeout=10,
    )
    sticker = _select_search_sticker(stickers)
    if not sticker:
        raise LookupError("Animated magnifying-glass sticker was not found")

    _sticker_file_id = sticker.file_id
    return _sticker_file_id


async def show_search_indicator(client, chat_id):
    """Show a native animated sticker, falling back to editable text."""
    try:
        sticker_id = await _resolve_search_sticker(client)
        message = await asyncio.wait_for(
            telegram_call(
                lambda: client.send_sticker(chat_id=chat_id, sticker=sticker_id),
                route="search_indicator_send",
                policy=INTERACTIVE_RETRY,
                retry_safe=True,
                idempotency_key=f"indicator:{chat_id}",
            ),
            timeout=10,
        )
        if not message or not getattr(message, "sticker", None):
            if message:
                try:
                    await message.delete()
                except Exception:
                    pass
            raise ValueError("Telegram did not return a sticker message")
        return message
    except Exception as exc:
        logger.warning("Animated search sticker unavailable; using text: %s", exc)
        return await telegram_call(
            lambda: client.send_message(
                chat_id,
                "🔎 <b>Searching your library…</b>\n<i>Finding the best matches</i>",
                parse_mode=ParseMode.HTML,
            ),
            route="search_indicator_fallback",
            policy=INTERACTIVE_RETRY,
            retry_safe=True,
            idempotency_key=f"indicator-fallback:{chat_id}",
        )


async def remove_search_indicator(message):
    if not message:
        return
    try:
        await message.delete()
    except Exception:
        pass
