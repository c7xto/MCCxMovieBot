"""Small, deterministic helpers for the bulk indexer's live status UI."""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field


def readable_duration(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "Calculating…"
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def progress_bar(percentage: float, length: int = 10) -> str:
    bounded = max(0.0, min(100.0, float(percentage)))
    filled = int((bounded / 100.0) * length)
    return "▰" * filled + "▱" * (length - filled)


@dataclass
class IndexProgress:
    first_id: int
    last_id: int
    now: callable = time.monotonic
    checkpoint: int = field(init=False)
    scanned: int = 0
    media: int = 0
    saved: int = 0
    duplicates: int = 0
    skipped: int = 0
    retries: int = 0
    _started: float = field(init=False)
    _samples: deque = field(init=False)

    def __post_init__(self):
        self.first_id = max(1, int(self.first_id))
        self.last_id = max(self.first_id, int(self.last_id))
        self.checkpoint = self.first_id - 1
        self._started = self.now()
        self._samples = deque([(self._started, 0)], maxlen=120)

    @property
    def total(self) -> int:
        return self.last_id - self.first_id + 1

    @property
    def percentage(self) -> float:
        return min(100.0, (self.scanned / max(1, self.total)) * 100.0)

    @property
    def elapsed(self) -> float:
        return max(0.0, self.now() - self._started)

    @property
    def rolling_speed(self) -> float:
        current = self.now()
        while len(self._samples) > 2 and current - self._samples[0][0] > 60:
            self._samples.popleft()
        first_time, first_count = self._samples[0]
        last_time, last_count = self._samples[-1]
        duration = last_time - first_time
        if duration <= 0:
            return 0.0
        return max(0.0, (last_count - first_count) / duration)

    @property
    def eta(self) -> float | None:
        speed = self.rolling_speed
        if speed <= 0:
            return None
        return max(0, self.total - self.scanned) / speed

    def record_batch(
        self,
        *,
        end_id: int,
        media: int,
        saved: int,
        duplicates: int,
        skipped: int,
    ) -> None:
        bounded_end = min(self.last_id, max(self.checkpoint, int(end_id)))
        processed = max(0, bounded_end - self.checkpoint)
        self.checkpoint = bounded_end
        self.scanned = min(self.total, self.scanned + processed)
        self.media += max(0, int(media))
        self.saved += max(0, int(saved))
        self.duplicates += max(0, int(duplicates))
        self.skipped += max(0, int(skipped))
        self._samples.append((self.now(), self.scanned))

    def render(self, state: str = "running") -> str:
        titles = {
            "running": "⚡ Indexer • Running",
            "paused": "⏸ Indexer • Paused",
            "stopping": "⏹ Indexer • Stopping safely",
            "stopped": "⏹ Indexer • Stopped safely",
            "complete": "✅ Indexer • Complete",
        }
        speed = self.rolling_speed
        eta = "Paused" if state == "paused" else readable_duration(self.eta)
        return (
            f"{titles.get(state, titles['running'])}\n"
            f"{progress_bar(self.percentage)}  `{self.percentage:.1f}%`\n\n"
            f"📨 Scanned  `{self.scanned:,} / {self.total:,}`\n"
            f"🎞 Media    `{self.media:,}`\n"
            f"✅ Saved    `{self.saved:,}`\n"
            f"♻️ Existing `{self.duplicates:,}`\n"
            f"⏭ Skipped  `{self.skipped:,}`\n\n"
            f"⚙️ Speed    `{speed:.1f} msg/s`\n"
            f"⏱ Elapsed  `{readable_duration(self.elapsed)}`\n"
            f"⌛ ETA      `{eta}`\n"
            f"💾 Saved through message `{self.checkpoint:,}`"
        )
