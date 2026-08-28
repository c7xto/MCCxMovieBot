"""In-memory movie-shard health routing; reads continue to fan out."""

from __future__ import annotations

import time
from dataclasses import dataclass


CAPACITY_LIMIT_MB = 450.0
NEAR_LIMIT_MB = 420.0


def is_capacity_error(error: BaseException) -> bool:
    text = str(error).casefold()
    details = getattr(error, "details", None) or {}
    write_errors = details.get("writeErrors", []) if isinstance(details, dict) else []
    return (
        details.get("code") == 8000
        or any(item.get("code") == 8000 for item in write_errors)
        or "space quota" in text
        or "over your space" in text
        or "writes are blocked" in text
    )


@dataclass
class ShardHealth:
    state: str = "healthy"
    size_mb: float | None = None
    changed_at: float = 0.0
    retry_at: float = 0.0
    reason: str = "startup"


class ShardRouter:
    def __init__(self, count: int):
        self._health = [ShardHealth(changed_at=time.monotonic()) for _ in range(count)]

    def candidates(self) -> list[int]:
        now = time.monotonic()
        candidates = []
        for index, health in enumerate(self._health):
            if health.state == "full":
                continue
            if health.state == "unavailable" and health.retry_at > now:
                continue
            candidates.append(index)
        return candidates

    def record_size(self, index: int, size_mb: float):
        if size_mb == float("inf"):
            self.mark_unavailable(index, "capacity_unknown")
        elif size_mb >= CAPACITY_LIMIT_MB:
            self._set(index, "full", size_mb, "capacity_limit", retry_after=300)
        elif size_mb >= NEAR_LIMIT_MB:
            self._set(index, "near_limit", size_mb, "near_capacity")
        else:
            self._set(index, "healthy", size_mb, "capacity_ok")

    def mark_error(self, index: int, error: BaseException):
        if is_capacity_error(error):
            self._set(index, "full", self._health[index].size_mb, "quota_write_error", retry_after=300)
        else:
            self.mark_unavailable(index, type(error).__name__)

    def mark_unavailable(self, index: int, reason: str):
        self._set(index, "unavailable", self._health[index].size_mb, reason, retry_after=30)

    def _set(self, index: int, state: str, size_mb, reason: str, retry_after: int = 0):
        health = self._health[index]
        now = time.monotonic()
        if health.state != state:
            health.changed_at = now
        health.state = state
        health.size_mb = size_mb
        health.reason = reason
        health.retry_at = now + retry_after if retry_after else 0.0

    def snapshot(self) -> list[dict]:
        return [
            {
                "cluster": index + 1,
                "state": health.state,
                "size_mb": health.size_mb,
                "reason": health.reason,
            }
            for index, health in enumerate(self._health)
        ]
