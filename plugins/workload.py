"""Shared workload admission controls for public search and delivery routes."""

import asyncio
import logging
import os
import time
from collections import Counter
from contextlib import asynccontextmanager
from functools import wraps

from database.db import db
from database.redis_client import redis_state


logger = logging.getLogger(__name__)

MAX_QUERY_CHARS = 128
MAX_QUERY_TOKENS = 12
SEARCH_QUEUE_TIMEOUT_SECONDS = 2.0
DELIVERY_LEASE_SECONDS = 10 * 60

_SEARCH_CONCURRENCY = max(8, int(os.getenv("SEARCH_CONCURRENCY_PER_PROCESS", "32")))
_GLOBAL_SEARCH_CONCURRENCY = max(
    _SEARCH_CONCURRENCY,
    int(os.getenv("GLOBAL_SEARCH_CONCURRENCY", "128")),
)
_search_semaphore = asyncio.Semaphore(_SEARCH_CONCURRENCY)
_metrics = Counter()
_search_waiting = 0
_search_active = 0
_interactive_active = 0
_interactive_lock = asyncio.Lock()


class WorkloadRejected(RuntimeError):
    """A safe, expected rejection which may be shown to the end user."""

    def __init__(self, message: str, retry_after: int = 0):
        self.public_message = message
        self.retry_after = max(0, int(retry_after))
        super().__init__(message)


@asynccontextmanager
async def interactive_slot(route: str):
    """Advertise latency-sensitive work to cooperative background workers."""
    global _interactive_active
    started = time.monotonic()
    async with _interactive_lock:
        _interactive_active += 1
        _metrics[f"interactive_started:{route}"] += 1
        _metrics["interactive_high_water"] = max(
            _metrics["interactive_high_water"], _interactive_active
        )
    shared_incremented = False
    try:
        await redis_state.increment_counter("interactive-active", ttl=30)
        shared_incremented = True
        yield
        _metrics[f"interactive_completed:{route}"] += 1
    finally:
        _metrics[f"interactive_latency_ms:{route}"] += int(
            (time.monotonic() - started) * 1000
        )
        async with _interactive_lock:
            _interactive_active = max(0, _interactive_active - 1)
        if shared_incremented:
            await redis_state.decrement_counter("interactive-active", ttl=30)


async def background_turn(route: str, max_defer: float = 2.0) -> bool:
    """Yield to active users, but let one background batch through eventually.

    Returns True when an idle window was observed and False when the bounded
    wait elapsed. The timeout prevents a busy bot from starving the indexer.
    """
    if await redis_state.get_counter("interactive-active") == 0:
        return True
    _metrics[f"background_deferred:{route}"] += 1
    try:
        deadline = time.monotonic() + max(0.0, max_defer)
        while time.monotonic() < deadline:
            await asyncio.sleep(0.025)
            if await redis_state.get_counter("interactive-active") == 0:
                return True
    except asyncio.CancelledError:
        raise
    _metrics[f"background_fairness_release:{route}"] += 1
    return False


def interactive_callback(route: str):
    """Acknowledge a Telegram callback first and advertise its full lifetime."""
    def decorate(function):
        @wraps(function)
        async def wrapped(client, callback, *args, **kwargs):
            from plugins.callbacks import answer_callback_safely

            await answer_callback_safely(callback)
            async with interactive_slot(route):
                return await function(client, callback, *args, **kwargs)

        return wrapped

    return decorate


def validate_search_query(query: str) -> str:
    normalized = " ".join(str(query or "").split())
    if not normalized:
        raise WorkloadRejected("Please enter a movie or series title.")
    if len(normalized) > MAX_QUERY_CHARS:
        raise WorkloadRejected(
            f"Searches are limited to {MAX_QUERY_CHARS} characters. Try only the title."
        )
    if len(normalized.split()) > MAX_QUERY_TOKENS:
        raise WorkloadRejected(
            f"Searches are limited to {MAX_QUERY_TOKENS} words. Try only the title."
        )
    return normalized


async def _consume(scope, key, limit, window_seconds, repository):
    allowed, retry_after = await repository.consume_rate_limit(
        scope, str(key), limit, window_seconds
    )
    if allowed:
        return
    _metrics[f"rate_limit_rejections:{scope}"] += 1
    logger.warning(
        "workload_rejected kind=rate_limit scope=%s key=%s retry_after=%s",
        scope,
        key,
        retry_after,
    )
    raise WorkloadRejected(
        f"Too many requests. Please try again in {retry_after} seconds.",
        retry_after,
    )


async def enforce_search_rate_limits(user_id: int, group_id=None, repository=None):
    repository = repository or db
    await _consume("search_user", user_id, 6, 30, repository)
    if group_id is not None:
        await _consume("search_group", group_id, 20, 30, repository)


@asynccontextmanager
async def search_slot(route: str):
    """Bound total DB search fan-out and expose queue/latency telemetry."""
    global _search_active, _search_waiting
    started = time.monotonic()
    acquired = False
    entered = False
    distributed_token = None
    _search_waiting += 1
    _metrics["search_queue_high_water"] = max(
        _metrics["search_queue_high_water"], _search_waiting
    )
    try:
        try:
            await asyncio.wait_for(
                _search_semaphore.acquire(), timeout=SEARCH_QUEUE_TIMEOUT_SECONDS
            )
            acquired = True
            deadline = started + SEARCH_QUEUE_TIMEOUT_SECONDS
            while distributed_token is None and time.monotonic() < deadline:
                distributed_token = await redis_state.acquire_semaphore(
                    "search",
                    _GLOBAL_SEARCH_CONCURRENCY,
                    lease_seconds=15,
                )
                if distributed_token is None:
                    await asyncio.sleep(0.025)
            if distributed_token is None:
                raise TimeoutError
        except TimeoutError as exc:
            _metrics[f"search_queue_timeouts:{route}"] += 1
            logger.warning(
                "workload_rejected kind=search_queue route=%s waiting=%s active=%s",
                route,
                _search_waiting,
                _search_active,
            )
            raise WorkloadRejected(
                "Search is busy right now. Please try again in a few seconds.", 3
            ) from exc
        finally:
            _search_waiting -= 1

        _search_active += 1
        entered = True
        _metrics[f"search_started:{route}"] += 1
        async with interactive_slot(route):
            yield
            _metrics[f"search_completed:{route}"] += 1
    finally:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        _metrics[f"search_latency_ms:{route}"] += elapsed_ms
        if entered:
            _search_active -= 1
        if acquired:
            _search_semaphore.release()
        if distributed_token is not None:
            await redis_state.release_semaphore("search", distributed_token)
        if elapsed_ms >= 5000:
            logger.warning(
                "workload_latency route=%s elapsed_ms=%s waiting=%s active=%s",
                route,
                elapsed_ms,
                _search_waiting,
                _search_active,
            )


@asynccontextmanager
async def delivery_guard(user_id: int, file_obj_id: str, repository=None):
    """Rate-limit delivery and reject duplicate concurrent sends."""
    repository = repository or db
    async with interactive_slot("file_delivery"):
        await _consume("delivery_user", user_id, 5, 60, repository)
        lease_key = f"{user_id}:{file_obj_id}"
        owner = await repository.acquire_action_lease(
            "file_delivery", lease_key, DELIVERY_LEASE_SECONDS
        )
        if owner is None:
            _metrics["delivery_duplicate_rejections"] += 1
            logger.warning(
                "workload_rejected kind=duplicate_delivery user_id=%s file=%s",
                user_id,
                file_obj_id,
            )
            raise WorkloadRejected(
                "This file is already being sent. Please wait for it to finish."
            )

        started = time.monotonic()
        _metrics["delivery_started"] += 1
        try:
            yield
            _metrics["delivery_completed"] += 1
        finally:
            _metrics["delivery_latency_ms"] += int((time.monotonic() - started) * 1000)
            await repository.release_action_lease("file_delivery", lease_key, owner)


async def workload_snapshot() -> dict:
    """Return process metrics for health/log integrations and tests."""
    return {
        **dict(_metrics),
        "search_queue_depth": _search_waiting,
        "search_active": _search_active,
        "search_capacity": _SEARCH_CONCURRENCY,
        "global_search_capacity": _GLOBAL_SEARCH_CONCURRENCY,
        "interactive_active": _interactive_active,
        "interactive_active_shared": await redis_state.get_counter("interactive-active"),
    }


def record_workload_metric(name: str, amount: int = 1):
    """Record a privacy-safe operational counter from another subsystem."""
    _metrics[str(name)] += int(amount)


def set_workload_gauge(name: str, value: int):
    _metrics[str(name)] = int(value)
