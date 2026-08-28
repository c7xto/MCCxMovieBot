"""In-memory movie-shard health routing for reads and writes."""

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
    state: str = "checking"
    size_mb: float | None = None
    changed_at: float = 0.0
    retry_at: float = 0.0
    reason: str = "startup"


class ShardRouter:
    def __init__(self, count: int):
        self._health = [ShardHealth(changed_at=time.monotonic()) for _ in range(count)]

    def candidates(self) -> list[int]:
        """Return shards that may accept a write.

        Full, quarantined and currently unavailable shards are excluded.
        An unavailable shard becomes probe-eligible after ``retry_at`` but is
        not sent normal traffic until a health probe marks it reachable.
        """
        candidates = []
        for index, health in enumerate(self._health):
            if health.state in {"full", "quarantined", "capacity_unknown"}:
                continue
            if health.state == "unavailable":
                continue
            candidates.append(index)
        return candidates

    def read_candidates(self) -> list[int]:
        """Return shards safe for user-facing reads.

        Full shards remain readable. Unavailable and quarantined shards are
        kept out of request fan-out so one failed Atlas cluster cannot add a
        server-selection timeout to every search, callback or admin screen.
        """
        return [
            index
            for index, health in enumerate(self._health)
            if health.state not in {"unavailable", "quarantined"}
        ]

    def probe_candidates(self) -> list[int]:
        """Return shards whose connectivity should be tested now."""
        now = time.monotonic()
        return [
            index
            for index, health in enumerate(self._health)
            if health.state != "quarantined"
            and (health.state != "unavailable" or health.retry_at <= now)
        ]

    def record_size(self, index: int, size_mb: float):
        if size_mb == float("inf"):
            self._set(index, "capacity_unknown", None, "capacity_unknown", retry_after=30)
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

    def mark_reachable(self, index: int, reason: str = "connection_ok"):
        """Restore reads without accidentally clearing a known full state."""
        health = self._health[index]
        if health.state in {"checking", "unavailable"}:
            self._set(index, "healthy", health.size_mb, reason)

    def mark_quarantined(self, index: int, reason: str):
        """Exclude a shard until an operator resolves a data-safety issue."""
        self._set(index, "quarantined", self._health[index].size_mb, reason)

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

    def apply_snapshot(self, snapshot: list[dict]) -> None:
        """Merge a Redis-published health snapshot from another process."""
        if not isinstance(snapshot, list):
            return
        for item in snapshot:
            try:
                index = int(item["cluster"]) - 1
                state = str(item["state"])
            except (KeyError, TypeError, ValueError):
                continue
            if not 0 <= index < len(self._health):
                continue
            if state not in {
                "checking",
                "healthy",
                "near_limit",
                "full",
                "capacity_unknown",
                "unavailable",
                "quarantined",
            }:
                continue
            size_mb = item.get("size_mb")
            if size_mb is not None:
                try:
                    size_mb = float(size_mb)
                except (TypeError, ValueError):
                    size_mb = None
            self._set(index, state, size_mb, str(item.get("reason") or "shared_state"))

    def unavailable(self) -> list[int]:
        """Return one-based cluster numbers that cannot provide complete reads."""
        return [
            index + 1
            for index, health in enumerate(self._health)
            if health.state in {"unavailable", "quarantined", "checking"}
        ]
