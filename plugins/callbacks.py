"""Telegram callback helpers that keep expired button clicks out of error logs."""

import logging

from pyrogram.errors import QueryIdInvalid


logger = logging.getLogger(__name__)


async def answer_callback_safely(callback, text=None, *, show_alert=False) -> bool:
    """Answer promptly; an expired Telegram callback is harmless user input."""
    try:
        await callback.answer(text, show_alert=show_alert)
        return True
    except QueryIdInvalid:
        logger.info("Ignored expired Telegram callback data=%s", callback.data)
        return False
