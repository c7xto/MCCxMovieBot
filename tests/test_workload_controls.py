import asyncio
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import patch


try:
    import rapidfuzz  # noqa: F401
except ImportError:
    rapidfuzz_stub = types.ModuleType("rapidfuzz")
    rapidfuzz_stub.fuzz = SimpleNamespace(
        ratio=lambda left, right: 100 if left == right else 80,
        WRatio=lambda left, right: 100 if left == right else 80,
        token_sort_ratio=lambda left, right: 100 if left == right else 80,
    )
    rapidfuzz_stub.process = SimpleNamespace(extract=lambda *_args, **_kwargs: [])
    sys.modules["rapidfuzz"] = rapidfuzz_stub

from plugins.workload import (
    WorkloadRejected,
    delivery_guard,
    enforce_search_rate_limits,
    search_slot,
    validate_search_query,
    workload_snapshot,
)
from database.db import Database, search_tokens_for_name
from database.redis_client import RedisConfigurationError, RedisState


ROOT = Path(__file__).resolve().parents[1]


class FakeRepository:
    def __init__(self, allowed=True, owner="owner"):
        self.allowed = allowed
        self.owner = owner
        self.consumed = []
        self.released = []

    async def consume_rate_limit(self, scope, key, limit, window):
        self.consumed.append((scope, key, limit, window))
        return self.allowed, 7

    async def acquire_action_lease(self, scope, key, ttl):
        return self.owner

    async def release_action_lease(self, scope, key, owner):
        self.released.append((scope, key, owner))


class WorkloadControlTests(unittest.TestCase):
    def test_all_in_one_can_use_process_local_ephemeral_state(self):
        with patch.dict(
            "os.environ",
            {"SERVICE_ROLE": "all-in-one", "REDIS_URL": ""},
            clear=False,
        ):
            state = RedisState()
            asyncio.run(state.start())
            self.assertTrue(state.configured)
            self.assertFalse(state.shared)

    def test_split_worker_still_requires_redis(self):
        with patch.dict(
            "os.environ",
            {"SERVICE_ROLE": "worker-indexer", "REDIS_URL": ""},
            clear=False,
        ):
            state = RedisState()
            with self.assertRaises(RedisConfigurationError):
                asyncio.run(state.start())

    def test_normalized_query_limits_length_and_tokens(self):
        self.assertEqual(validate_search_query("  War   Machine  "), "War Machine")
        with self.assertRaises(WorkloadRejected):
            validate_search_query("x" * 129)
        with self.assertRaises(WorkloadRejected):
            validate_search_query(" ".join(str(i) for i in range(13)))

    def test_search_tokens_are_index_safe(self):
        self.assertEqual(
            search_tokens_for_name("Movie.Movie.2026.MKV"),
            ["movie", "2026", "mkv"],
        )

    def test_database_rate_limit_uses_atomic_update_pipeline(self):
        database = Database.__new__(Database)
        database.rate_limits_col = SimpleNamespace(
            find_one_and_update=AsyncMock(return_value={
                "allowed": True, "tokens": 2.5
            })
        )
        allowed, retry_after = asyncio.run(
            database.consume_rate_limit("search_user", "10", 6, 30)
        )
        self.assertTrue(allowed)
        self.assertEqual(retry_after, 1)
        call = database.rate_limits_col.find_one_and_update.await_args
        self.assertEqual(call.args[0], {"_id": "search_user:10"})
        self.assertIsInstance(call.args[1], list)

    def test_group_search_consumes_user_and_group_buckets(self):
        repository = FakeRepository()
        asyncio.run(enforce_search_rate_limits(10, -10020, repository))
        self.assertEqual(
            [call[0] for call in repository.consumed],
            ["search_user", "search_group"],
        )

    def test_rate_limit_rejection_is_safe_and_bounded(self):
        repository = FakeRepository(allowed=False)
        with self.assertRaisesRegex(WorkloadRejected, "7 seconds"):
            asyncio.run(enforce_search_rate_limits(10, repository=repository))

    def test_delivery_lease_is_released(self):
        repository = FakeRepository()

        async def run():
            async with delivery_guard(10, "abc", repository):
                pass

        asyncio.run(run())
        self.assertEqual(
            repository.released, [("file_delivery", "10:abc", "owner")]
        )

    def test_duplicate_delivery_is_rejected(self):
        repository = FakeRepository(owner=None)

        async def run():
            async with delivery_guard(10, "abc", repository):
                pass

        with self.assertRaisesRegex(WorkloadRejected, "already being sent"):
            asyncio.run(run())

    def test_search_slot_records_route_metrics(self):
        async def run():
            async with search_slot("test_route"):
                await asyncio.sleep(0)

        asyncio.run(run())
        snapshot = asyncio.run(workload_snapshot())
        self.assertGreaterEqual(snapshot["search_completed:test_route"], 1)
        self.assertEqual(snapshot["search_queue_depth"], 0)

    def test_public_route_sources_use_shared_admission_controls(self):
        expected = {
            "plugins/filter.py": ("search_slot(", "delivery_guard("),
            "plugins/group_connect.py": ("search_slot(",),
            "plugins/start.py": ("search_slot(", "deliver_cached_file"),
            "plugins/req_fsub.py": ("deliver_cached_file",),
        }
        for relative_path, controls in expected.items():
            source = (ROOT / relative_path).read_text(encoding="utf-8")
            for control in controls:
                self.assertIn(control, source, f"{relative_path} lacks {control}")


if __name__ == "__main__":
    unittest.main()
