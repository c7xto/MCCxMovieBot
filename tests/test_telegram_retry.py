import asyncio
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock


try:
    import rapidfuzz  # noqa: F401
except ImportError:
    rapidfuzz_stub = types.ModuleType("rapidfuzz")
    rapidfuzz_stub.fuzz = SimpleNamespace(WRatio=lambda *_args, **_kwargs: 0)
    rapidfuzz_stub.process = SimpleNamespace(extract=lambda *_args, **_kwargs: [])
    sys.modules["rapidfuzz"] = rapidfuzz_stub

from pyrogram.errors import FloodWait

from database.db import Database
from plugins.telegram_retry import (
    TelegramRetryPolicy,
    telegram_call,
    telegram_retry_snapshot,
)


ROOT = Path(__file__).resolve().parents[1]


class TelegramRetryTests(unittest.TestCase):
    def test_floodwait_retries_with_jitter_and_metrics(self):
        calls = 0
        delays = []

        async def operation():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise FloodWait(2)
            return "sent"

        async def no_sleep(delay):
            delays.append(delay)

        result = asyncio.run(telegram_call(
            operation,
            route="test_retry",
            policy=TelegramRetryPolicy(2, 10, 5),
            retry_safe=True,
            idempotency_key="message:1",
            sleep=no_sleep,
            jitter=lambda _start, _end: 0.5,
        ))
        self.assertEqual(result, "sent")
        self.assertEqual(calls, 2)
        self.assertEqual(delays, [2.5])
        self.assertGreaterEqual(
            telegram_retry_snapshot()["floodwait_count:test_retry"], 1
        )

    def test_wait_beyond_budget_is_not_slept_or_retried(self):
        sleep = AsyncMock()

        async def operation():
            raise FloodWait(20)

        with self.assertRaises(FloodWait):
            asyncio.run(telegram_call(
                operation,
                route="test_budget",
                policy=TelegramRetryPolicy(3, 10, 5),
                retry_safe=True,
                idempotency_key="message:2",
                sleep=sleep,
            ))
        sleep.assert_not_awaited()

    def test_send_retries_require_operation_idempotency_key(self):
        async def operation():
            return None

        with self.assertRaisesRegex(ValueError, "idempotency key"):
            asyncio.run(telegram_call(
                operation,
                route="missing_key",
                policy=TelegramRetryPolicy(2, 10, 5),
                retry_safe=True,
            ))

    def test_optional_announcements_are_durable(self):
        database = Database.__new__(Database)
        database.announcement_col = SimpleNamespace(
            find_one=AsyncMock(return_value=None),
            count_documents=AsyncMock(return_value=0),
            update_one=AsyncMock(),
        )
        job_id = asyncio.run(database.enqueue_announcement("Movie.mkv", 3))
        self.assertTrue(job_id.startswith("announcement:"))
        update = database.announcement_col.update_one.await_args.args[1]
        self.assertEqual(update["$set"]["payload"]["file_name"], "Movie.mkv")
        self.assertIn("expires_at", update["$set"])

    def test_core_routes_use_shared_retry_and_no_memory_post_queue(self):
        direct_retry_routes = (
            "plugins/filter.py",
            "plugins/group_connect.py",
            "plugins/request.py",
            "plugins/realtime_indexer.py",
        )
        for relative_path in direct_retry_routes:
            source = (ROOT / relative_path).read_text(encoding="utf-8")
            self.assertIn("telegram_call(", source, relative_path)
        for relative_path in ("plugins/start.py", "plugins/req_fsub.py"):
            source = (ROOT / relative_path).read_text(encoding="utf-8")
            self.assertIn("deliver_cached_file", source, relative_path)
        shared_delivery = (ROOT / "plugins/filter.py").read_text(encoding="utf-8")
        self.assertIn("policy=DELIVERY_RETRY", shared_delivery)
        realtime = (ROOT / "plugins/realtime_indexer.py").read_text(encoding="utf-8")
        self.assertNotIn("asyncio.Queue", realtime)
        self.assertIn("enqueue_announcement", realtime)


if __name__ == "__main__":
    unittest.main()
