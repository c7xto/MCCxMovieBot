"""Telegram callback helpers that keep expired button clicks out of error logs."""

import logging
import time

from pyrogram.errors import QueryIdInvalid
from database.redis_client import redis_state
from plugins.telegram_retry import INTERACTIVE_RETRY, telegram_call


logger = logging.getLogger(__name__)
_CALLBACK_DEDUP_TTL = 30


async def answer_callback_safely(callback, text=None, *, show_alert=False, url=None) -> bool:
    """Answer promptly; an expired Telegram callback is harmless user input."""
    # A callback query can be answered only once.  Mark it locally so nested
    # policy/verification helpers do not make a second Telegram request and
    # create QUERY_ID_INVALID noise after the UI lock has already been freed.
    telegram_callback_id = getattr(callback, "id", "")
    callback_key = f"telegram:{telegram_callback_id}" if telegram_callback_id else None
    already_answered = getattr(callback, "_mccx_answered", False)
    if already_answered:
        if text and show_alert and getattr(callback, "message", None) is not None:
            # Telegram cannot show a second popup after the early ACK. Preserve
            # important error feedback as a normal chat message instead.
            await callback.message.reply_text(str(text), reply_parameters=None)
        return True
    started = time.monotonic()
    try:
        answer_kwargs = {"show_alert": show_alert}
        if url:
            answer_kwargs["url"] = url
        await telegram_call(
            lambda: callback.answer(text, **answer_kwargs),
            route="callback_answer",
            policy=INTERACTIVE_RETRY,
            retry_safe=False,
        )
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
        if callback_key is not None:
            await redis_state.set_json(
                "answered-callback", callback_key, True, _CALLBACK_DEDUP_TTL
            )
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
            await redis_state.delete("answered-callback", callback_key)
        raise
