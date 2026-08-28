import asyncio
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bson.objectid import ObjectId


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


class FakeControlCollection:
    def __init__(self, status="running"):
        self.document = {
            "_id": ObjectId(),
            "status": status,
            "lock_token": "lease",
        }

    def _matches(self, query):
        for key, expected in query.items():
            actual = self.document.get(key)
            if isinstance(expected, dict) and "$in" in expected:
                if actual not in expected["$in"]:
                    return False
            elif actual != expected:
                return False
        return True

    def _apply(self, update):
        self.document.update(update.get("$set", {}))
        for key in update.get("$unset", {}):
            self.document.pop(key, None)

    async def find_one(self, query, *_args, **_kwargs):
        return dict(self.document) if self._matches(query) else None

    async def find_one_and_update(self, query, update, **_kwargs):
        if not self._matches(query):
            return None
        self._apply(update)
        return dict(self.document)

    async def update_one(self, query, update):
        if not self._matches(query):
            return SimpleNamespace(matched_count=0)
        self._apply(update)
        return SimpleNamespace(matched_count=1)


class FakeBroadcastDatabase:
    def __init__(self):
        self.checkpoints = []
        self.phase_calls = []
        self.completed = []
        self.after_id = None
        self.control_requested = None
        self.control_calls = []
        self.deletions = []

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

    async def get_broadcast_control(self, job_id, token):
        self.control_calls.append((job_id, token))
        return {"status": "running", "control_requested": self.control_requested}

    async def apply_broadcast_control(self, job_id, token, action):
        self.control_calls.append((job_id, token, action))
        return True

    async def schedule_deletion(self, chat_id, message_id, delay):
        self.deletions.append((chat_id, message_id, delay))
        return True


class FakeCopiedMessage:
    def __init__(self, message_id):
        self.chat = SimpleNamespace(id=99)
        self.id = message_id
        self.pin_calls = []

    async def pin(self, **kwargs):
        self.pin_calls.append(kwargs)


class FakeClient:
    def __init__(self):
        self.copied = []
        self.copied_messages = []
        self.edited = []

    async def copy_message(self, **kwargs):
        self.copied.append(kwargs)
        copied = FakeCopiedMessage(len(self.copied))
        self.copied_messages.append(copied)
        return copied

    async def edit_message_text(self, *args, **kwargs):
        self.edited.append((args, kwargs))


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
            total_users=0,
            total_groups=12,
        ))
        self.assertEqual(job_id, "broadcast-id")
        self.assertEqual(collection.inserted["status"], "pending")
        self.assertEqual(collection.inserted["source_chat_id"], 1)
        self.assertEqual(collection.inserted["source_message_id"], 2)
        self.assertEqual(collection.inserted["total_groups"], 12)
        self.assertEqual(collection.inserted["total_recipients"], 12)
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
            "total_groups": 2,
            "total_recipients": 2,
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
        self.assertEqual(job["status"], "completed")

    def test_group_delivery_applies_pin_and_durable_auto_delete(self):
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
            "do_pin": True,
            "do_delete": True,
            "users_done": True,
            "groups_done": False,
            "group_cursor": 10,
            "total_groups": 2,
            "total_recipients": 2,
        }
        with (
            patch.object(broadcast, "db", database),
            patch.object(broadcast, "telegram_call", direct_telegram_call),
            patch.object(broadcast.asyncio, "sleep", AsyncMock()),
        ):
            asyncio.run(broadcast._run_broadcast_job(client, job))

        self.assertEqual(len(client.copied_messages), 2)
        self.assertEqual(
            client.copied_messages[0].pin_calls,
            [{"both_sides": False, "disable_notification": True}],
        )
        self.assertEqual(database.deletions, [(99, 1, 86400), (99, 2, 86400)])

    def test_live_status_contains_clean_progress_speed_and_eta(self):
        text = broadcast._broadcast_status_text(
            {
                "_id": "job",
                "status": "running",
                "target": "users",
                "total_recipients": 100,
                "sent_users": 40,
                "blocked_users": 2,
                "failed_users": 1,
                "skipped_banned": 2,
                "_runtime_speed": 5,
                "started_at": 900,
                "do_pin": False,
                "do_delete": False,
            },
            now=910,
        )
        self.assertIn("Broadcast • Running", text)
        self.assertIn("45.0%", text)
        self.assertIn("45 / 100", text)
        self.assertIn("5.0/s", text)
        self.assertIn("ETA  **11s**", text)

    def test_safe_pause_is_applied_before_a_recipient_is_sent(self):
        database = FakeBroadcastDatabase()
        database.control_requested = "pause"
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
        }
        with (
            patch.object(broadcast, "db", database),
            patch.object(broadcast, "telegram_call", direct_telegram_call),
        ):
            result = asyncio.run(broadcast._run_broadcast_job(client, job))

        self.assertEqual(result, "paused")
        self.assertEqual(client.copied, [])
        self.assertIn(("job", "lease", "pause"), database.control_calls)

    def test_database_pause_resume_and_safe_stop_are_durable(self):
        collection = FakeControlCollection(status="running")
        database = Database.__new__(Database)
        database.broadcast_col = collection
        job_id = str(collection.document["_id"])

        paused_request = asyncio.run(database.request_broadcast_control(job_id, "pause"))
        self.assertEqual(paused_request["control_requested"], "pause")
        self.assertTrue(
            asyncio.run(
                database.apply_broadcast_control(
                    collection.document["_id"], "lease", "pause"
                )
            )
        )
        self.assertEqual(collection.document["status"], "paused")
        self.assertNotIn("lock_token", collection.document)

        resumed = asyncio.run(database.request_broadcast_control(job_id, "resume"))
        self.assertEqual(resumed["status"], "pending")
        stopped = asyncio.run(database.request_broadcast_control(job_id, "stop"))
        self.assertEqual(stopped["status"], "stopped")
        self.assertIn("finished_at", stopped)

    def test_no_volatile_scheduled_sleep_or_group_materialization_remains(self):
        source = (ROOT / "plugins" / "broadcast.py").read_text(encoding="utf-8")
        database_source = (ROOT / "database" / "db.py").read_text(encoding="utf-8")
        self.assertNotIn("await db.get_all_groups()", source)
        self.assertNotIn("await asyncio.sleep(delay_seconds)", source)
        self.assertNotIn("message.reply_to_message.copy", source)
        self.assertNotIn("supervisor.spawn", source)
        self.assertIn("await db.enqueue_broadcast(", source)
        self.assertIn("await answer_callback_safely(callback", source)
        self.assertIn("async def broadcast_status_handler", source)
        self.assertIn("async def request_broadcast_control", database_source)
        self.assertIn("BROADCAST_LEASE_SECONDS = 360", database_source)
        self.assertIn("async def iter_broadcast_groups_after", database_source)
        self.assertIn("async def checkpoint_broadcast", database_source)


if __name__ == "__main__":
    unittest.main()
