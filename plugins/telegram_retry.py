"""Central bounded FloodWait retry policy for Telegram API operations."""

import asyncio
import logging
import random
import secrets
import time
from collections import Counter
from dataclasses import dataclass

from pyrogram.errors import FloodWait


logger = logging.getLogger(__name__)
_metrics = Counter()


@dataclass(frozen=True)
class TelegramRetryPolicy:
    max_attempts: int
    max_elapsed: float
    max_wait: float


INTERACTIVE_RETRY = TelegramRetryPolicy(2, 10.0, 8.0)
DELIVERY_RETRY = TelegramRetryPolicy(3, 90.0, 60.0)
BACKGROUND_RETRY = TelegramRetryPolicy(4, 300.0, 120.0)
BROADCAST_RETRY = TelegramRetryPolicy(3, 180.0, 120.0)


async def telegram_call(operation, *, route: str, policy: TelegramRetryPolicy,
                        retry_safe: bool, idempotency_key: str | None = None,
                        sleep=asyncio.sleep, jitter=random.uniform):
    """Retry only explicit FloodWait responses within a hard time budget.

    Cancellation is never swallowed. Send-like operations must opt in with
    ``retry_safe`` and an operation-specific idempotency key; other exceptions
    are returned to the caller immediately because their outcome may be
    ambiguous.
    """
    if retry_safe and not idempotency_key:
        raise ValueError("retry_safe Telegram operations require an idempotency key")

    started = time.monotonic()
    attempts = 0
    while True:
        attempts += 1
        try:
            result = await operation()
            _metrics[f"completed:{route}"] += 1
            return result
        except asyncio.CancelledError:
            _metrics[f"cancelled:{route}"] += 1
            raise
        except FloodWait as exc:
            wait_seconds = max(1.0, float(getattr(exc, "value", 1)))
            _metrics[f"floodwait_count:{route}"] += 1
            _metrics[f"floodwait_seconds:{route}"] += int(wait_seconds)
            elapsed = time.monotonic() - started
            may_retry = (
                retry_safe
                and attempts < policy.max_attempts
                and wait_seconds <= policy.max_wait
                and elapsed + wait_seconds <= policy.max_elapsed
            )
            if not may_retry:
                _metrics[f"exhausted:{route}"] += 1
                logger.warning(
                    "telegram_retry_exhausted route=%s attempts=%s wait=%.1f "
                    "elapsed=%.1f key=%s",
                    route,
                    attempts,
                    wait_seconds,
                    elapsed,
                    idempotency_key or "none",
                )
                raise
            delay = wait_seconds + jitter(0.0, min(1.0, wait_seconds * 0.1))
            logger.info(
                "telegram_floodwait route=%s attempt=%s delay=%.2f",
                route,
                attempts,
                delay,
            )
            await sleep(delay)


def telegram_retry_snapshot() -> dict:
    return dict(_metrics)


async def send_message_resilient(client, *args, route="interactive_send", **kwargs):
    operation_id = secrets.token_hex(12)
    return await telegram_call(
        lambda: client.send_message(*args, **kwargs),
        route=route,
        policy=INTERACTIVE_RETRY,
        retry_safe=True,
        idempotency_key=f"{route}:{operation_id}",
    )


async def edit_message_text_resilient(client, *args, route="interactive_edit", **kwargs):
    operation_id = secrets.token_hex(12)
    return await telegram_call(
        lambda: client.edit_message_text(*args, **kwargs),
        route=route,
        policy=INTERACTIVE_RETRY,
        retry_safe=True,
        idempotency_key=f"{route}:{operation_id}",
    )


async def copy_message_resilient(client, *args, route="interactive_copy", **kwargs):
    operation_id = secrets.token_hex(12)
    return await telegram_call(
        lambda: client.copy_message(*args, **kwargs),
        route=route,
        policy=DELIVERY_RETRY,
        retry_safe=True,
        idempotency_key=f"{route}:{operation_id}",
    )


async def reply_text_resilient(message, *args, route="interactive_reply", **kwargs):
    operation_id = secrets.token_hex(12)
    return await telegram_call(
        lambda: message.reply_text(*args, **kwargs),
        route=route,
        policy=INTERACTIVE_RETRY,
        retry_safe=True,
        idempotency_key=f"{route}:{operation_id}",
    )


async def edit_text_resilient(message, *args, route="interactive_edit", **kwargs):
    operation_id = secrets.token_hex(12)
    return await telegram_call(
        lambda: message.edit_text(*args, **kwargs),
        route=route,
        policy=INTERACTIVE_RETRY,
        retry_safe=True,
        idempotency_key=f"{route}:{operation_id}",
    )
