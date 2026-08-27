"""Small async retry primitive with bounded exponential backoff and jitter."""

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar


T = TypeVar("T")


async def retry_with_backoff(
    operation: Callable[[], Awaitable[T]],
    *,
    attempts: int = 4,
    base_delay: float = 0.5,
    max_delay: float = 8.0,
    jitter: float = 0.25,
    should_retry: Callable[[Exception], bool] | None = None,
    on_retry: Callable[[Exception, int, float], None] | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    predicate = should_retry or (lambda _exc: True)

    for attempt in range(1, attempts + 1):
        try:
            return await operation()
        except Exception as exc:
            if attempt >= attempts or not predicate(exc):
                raise
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            delay += random.uniform(0.0, max(0.0, jitter))
            if on_retry:
                on_retry(exc, attempt, delay)
            await sleep(delay)

    raise RuntimeError("retry loop exhausted without a result")
