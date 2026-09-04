import asyncio
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace


try:
    import rapidfuzz  # noqa: F401
except ImportError:
    rapidfuzz_stub = types.ModuleType("rapidfuzz")
    rapidfuzz_stub.fuzz = SimpleNamespace(WRatio=lambda *_args, **_kwargs: 0)
    rapidfuzz_stub.process = SimpleNamespace(extract=lambda *_args, **_kwargs: [])
    sys.modules["rapidfuzz"] = rapidfuzz_stub

from database.db import Database, MAX_NOTIFICATION_OUTBOX_JOBS


ROOT = Path(__file__).resolve().parents[1]


class NotificationCollection:
    def __init__(self, depth=0, existing=None):
        self.depth = depth
        self.existing = existing
        self.updates = []

    async def find_one(self, *_args, **_kwargs):
        return self.existing

    async def count_documents(self, *_args, **_kwargs):
        return self.depth

    async def update_one(self, query, update, **kwargs):
        self.updates.append((query, update, kwargs))
        return SimpleNamespace()


class RealtimePipelineTests(unittest.TestCase):
    def test_jobs_coalesce_by_normalized_title(self):
        database = Database.__new__(Database)
        database.announcement_col = NotificationCollection()

        async def run():
            first = await database.enqueue_announcement("Movie 2026 1080p.mkv")
            second = await database.enqueue_announcement("Movie 2026 720p.mkv")
            return first, second

        first, second = asyncio.run(run())
        self.assertEqual(first, second)
        self.assertEqual(len(database.announcement_col.updates), 2)
        update = database.announcement_col.updates[0][1]
        self.assertEqual(update["$inc"], {"revision": 1})

    def test_full_outbox_uses_explicit_drop_new_policy(self):
        database = Database.__new__(Database)
        database.announcement_col = NotificationCollection(
            depth=MAX_NOTIFICATION_OUTBOX_JOBS
        )
        job_id = asyncio.run(database.enqueue_announcement("New Movie.mkv"))
        self.assertIsNone(job_id)
        self.assertEqual(database.announcement_col.updates, [])

    def test_realtime_work_uses_one_durable_supervised_pipeline(self):
        source = (ROOT / "plugins" / "realtime_indexer.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("asyncio.Queue", source)
        self.assertNotIn("request-fulfillment:", source)
        self.assertIn("claim_due_notification", source)
        live_source = (ROOT / "plugins" / "live_library.py").read_text(encoding="utf-8")
        self.assertIn("enqueue_request_fulfillment", live_source)
        self.assertIn("enqueue_candidate", live_source)
        self.assertIn("async for match in db.iter_matching_requests", source)
        bot_source = (ROOT / "bot.py").read_text(encoding="utf-8")
        self.assertIn('key="worker:announcement-outbox"', bot_source)

    def test_request_match_iterator_has_hard_cap_and_page_size(self):
        source = (ROOT / "database" / "db.py").read_text(encoding="utf-8")
        self.assertIn("MAX_REQUEST_MATCHES_PER_JOB = 100", source)
        self.assertIn(".batch_size(max(1, min(int(page_size), 50)))", source)


if __name__ == "__main__":
    unittest.main()
