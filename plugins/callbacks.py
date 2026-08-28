"""Telegram callback helpers that keep expired button clicks out of error logs."""

import logging
import time
from collections import OrderedDict

from pyrogram.errors import QueryIdInvalid


logger = logging.getLogger(__name__)
_answered_callback_ids = OrderedDict()
_MAX_ANSWERED_CALLBACKS = 4096


async def answer_callback_safely(callback, text=None, *, show_alert=False) -> bool:
    """Answer promptly; an expired Telegram callback is harmless user input."""
    # A callback query can be answered only once.  Mark it locally so nested
    # policy/verification helpers do not make a second Telegram request and
    # create QUERY_ID_INVALID noise after the UI lock has already been freed.
    telegram_callback_id = getattr(callback, "id", "")
    callback_key = f"telegram:{telegram_callback_id}" if telegram_callback_id else None
    if getattr(callback, "_mccx_answered", False) or (
        callback_key is not None and callback_key in _answered_callback_ids
    ):
        if text and show_alert and getattr(callback, "message", None) is not None:
            # Telegram cannot show a second popup after the early ACK. Preserve
            # important error feedback as a normal chat message instead.
            await callback.message.reply_text(str(text), reply_parameters=None)
        return True
    if callback_key is not None:
        _answered_callback_ids[callback_key] = None
        _answered_callback_ids.move_to_end(callback_key)
        while len(_answered_callback_ids) > _MAX_ANSWERED_CALLBACKS:
            _answered_callback_ids.popitem(last=False)
    started = time.monotonic()
    try:
        await callback.answer(text, show_alert=show_alert)
        try:
            from plugins.workload import record_workload_metric

            elapsed_ms = int((time.monotonic() - started) * 1000)
            record_workload_metric("callback_ack_count")
            record_workload_metric("callback_ack_latency_ms", elapsed_ms)
            if elapsed_ms >= 1000:
                record_workload_metric("callback_ack_slow")
        except Exception:
            pass
        try:
            setattr(callback, "_mccx_answered", True)
        except Exception:
            pass
        return True
    except QueryIdInvalid:
        logger.info("Ignored expired Telegram callback data=%s", callback.data)
        try:
            setattr(callback, "_mccx_answered", True)
        except Exception:
            pass
        return False
    except Exception:
        # A transport error may occur before Telegram receives the answer;
        # permit an explicit retry instead of treating it as acknowledged.
        if callback_key is not None:
            _answered_callback_ids.pop(callback_key, None)
        raise
