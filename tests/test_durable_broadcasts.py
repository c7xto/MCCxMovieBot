import asyncio
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


try:
    import rapidfuzz  # noqa: F401
except ImportError:
    rapidfuzz_stub = types.ModuleType("rapidfuzz")
    rapidfuzz_stub.fuzz = SimpleNamespace(
        ratio=lambda *_args, **_kwargs: 100,
        WRatio=lambda *_args, **_kwargs: 100,
        token_sort_ratio=lambda *_args, **_kwargs: 100,
    )
    rapidfuzz_stub.process = SimpleNamespace(extract=lambda *_args, **_kwargs: [])
    sys.modules["rapidfuzz"] = rapidfuzz_stub

from database.db import Database
from plugins import broadcast


ROOT = Path(__file__).resolve().parents[1]


class FakeBroadcastCollection:
    def __init__(self):
        self.inserted = None

    async def count_documents(self, *_args, **_kwargs):
        return 0

    async def insert_one(self, document):
        self.inserted = document
        return SimpleNamespace(inserted_id="broadcast-id")


class FakeBroadcastDatabase:
    def __init__(self):
        self.checkpoints = []
        self.phase_calls = []
        self.completed = []
        self.after_id = None

    async def iter_broadcast_groups_after(self, after_id):
        self.after_id = after_id
        for group_id in (20, 30):
            yield group_id

    async def checkpoint_broadcast(
        self, job_id, token, audience, recipient_id, outcome
    ):
        self.checkpoints.append(
            (job_id, token, audience, recipient_id, outcome)
        )
        return True

    async def complete_broadcast_phase(self, job_id, token, audience):
        self.phase_calls.append((job_id, token, audience))
        return True

    async def complete_broadcast(self, job_id, token):
        self.completed.append((job_id, token))
        return True


class FakeClient:
    def __init__(self):
        self.copied = []
        self.edited = []

    async def copy_message(self, **kwargs):
        self.copied.append(kwargs)
        return SimpleNamespace()

    async def edit_message_text(self, *args):
        self.edited.append(args)


async def direct_telegram_call(operation, **_kwargs):
    return await operation()


class DurableBroadcastTests(unittest.TestCase):
    def test_schedule_persists_only_message_coordinates_and_status(self):
        collection = FakeBroadcastCollection()
        database = Database.__new__(Database)
        database.broadcast_col = collection
        job_id = asyncio.run(database.enqueue_broadcast(
            source_chat_id=1,
            source_message_id=2,
            admin_chat_id=3,
            status_message_id=4,
            created_by=5,
            due_at=9_999_999_999,
            target="groups",
            do_pin=False,
            do_delete=False,
        ))
        self.assertEqual(job_id, "broadcast-id")
        self.assertEqual(collection.inserted["status"], "pending")
        self.assertEqual(collection.inserted["source_chat_id"], 1)
        self.assertEqual(collection.inserted["source_message_id"], 2)
        self.assertNotIn("message", collection.inserted)

    def test_group_resume_streams_after_checkpoint_and_checkpoints_each_result(self):
        database = FakeBroadcastDatabase()
        client = FakeClient()
        job = {
            "_id": "job",
            "lock_token": "lease",
            "source_chat_id": 1,
            "source_message_id": 2,
            "admin_chat_id": 3,
            "status_message_id": 4,
            "target": "groups",
            "do_pin": False,
            "do_delete": False,
            "users_done": True,
            "groups_done": False,
            "group_cursor": 10,
        }
        with (
            patch.object(broadcast, "db", database),
            patch.object(broadcast, "telegram_call", direct_telegram_call),
            patch.object(broadcast.asyncio, "sleep", AsyncMock()),
        ):
            asyncio.run(broadcast._run_broadcast_job(client, job))

        self.assertEqual(database.after_id, 10)
        self.assertEqual(
            [call["chat_id"] for call in client.copied], [20, 30]
        )
        self.assertEqual(
            [checkpoint[3] for checkpoint in database.checkpoints], [20, 30]
        )
        self.assertEqual(database.completed, [("job", "lease")])

    def test_no_volatile_scheduled_sleep_or_group_materialization_remains(self):
        source = (ROOT / "plugins" / "broadcast.py").read_text(encoding="utf-8")
        self.assertNotIn("await db.get_all_groups()", source)
        self.assertNotIn("await asyncio.sleep(delay_seconds)", source)
        self.assertNotIn("supervisor.spawn", source)
        self.assertIn("await db.enqueue_broadcast(", source)
        database_source = (ROOT / "database" / "db.py").read_text(encoding="utf-8")
        self.assertIn("async def iter_broadcast_groups_after", database_source)
        self.assertIn("async def checkpoint_broadcast", database_source)


if __name__ == "__main__":
    unittest.main()
