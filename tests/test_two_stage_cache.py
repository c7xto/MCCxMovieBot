import asyncio
import sys
import types
import unittest
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

from database.db import Database, TWO_STAGE_VERIFY_INTERVAL
from plugins import req_fsub
from verification import VerificationResult, VerificationStatus


class FakeGateDatabase:
    def __init__(self, gate_status):
        self.gate_status = gate_status
        self.marked = []

    async def get_config(self):
        return {
            "fsub_channels": [],
            "req_fsub_channels": [],
            "two_stage_channels": [
                {"id": -1001, "link": "https://t.me/one"},
                {"id": -1002, "link": "https://t.me/two"},
            ],
        }

    async def get_two_stage_gate_status(self, _user_id):
        return self.gate_status

    async def mark_two_stage_verified(self, user_id):
        self.marked.append(user_id)
        return True


class FakeClient:
    async def get_chat(self, _channel_id):
        return SimpleNamespace(title="Gate")


class FakeUsersCollection:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error

    async def find_one(self, *_args, **_kwargs):
        if self.error:
            raise self.error
        return self.result


class TwoStageCacheTests(unittest.TestCase):
    def evaluate(self, fake_db, membership_result):
        membership = AsyncMock(return_value=membership_result)
        with (
            patch.object(req_fsub, "db", fake_db),
            patch.object(
                req_fsub,
                "get_subscription_status_by_id",
                AsyncMock(return_value=VerificationResult.allow("no_main_gate")),
            ),
            patch.object(req_fsub, "_requested_or_joined_status", membership),
        ):
            result = asyncio.run(
                req_fsub._collect_outstanding_gates(FakeClient(), 42)
            )
        return result, membership

    def test_first_pass_is_denied_until_both_channels_are_joined(self):
        fake_db = FakeGateDatabase(VerificationResult.deny("two_stage_due"))
        evaluation, membership = self.evaluate(
            fake_db, VerificationResult.deny("not_joined")
        )
        self.assertEqual(evaluation.result.status, VerificationStatus.DENY)
        self.assertEqual(len(evaluation.missing), 2)
        self.assertEqual(evaluation.passed_due_gates, frozenset())
        self.assertEqual(membership.await_count, 2)

    def test_callback_pass_is_explicitly_marked_and_persisted(self):
        fake_db = FakeGateDatabase(VerificationResult.deny("two_stage_due"))
        evaluation, _ = self.evaluate(
            fake_db, VerificationResult.allow("joined")
        )
        self.assertEqual(evaluation.result.status, VerificationStatus.PASS)
        self.assertEqual(evaluation.passed_due_gates, {"two_stage"})
        with patch.object(req_fsub, "db", fake_db):
            asyncio.run(
                req_fsub._persist_passed_gate_state(
                    42, evaluation.passed_due_gates
                )
            )
        self.assertEqual(fake_db.marked, [42])

    def test_cached_pass_skips_telegram_membership_checks(self):
        fake_db = FakeGateDatabase(VerificationResult.allow("two_stage_cached"))
        evaluation, membership = self.evaluate(
            fake_db, VerificationResult.deny("must_not_be_called")
        )
        self.assertEqual(evaluation.result.status, VerificationStatus.PASS)
        self.assertEqual(evaluation.passed_due_gates, frozenset())
        membership.assert_not_awaited()

    def test_cache_expiry_becomes_due(self):
        database = Database.__new__(Database)
        database.users_col = FakeUsersCollection({
            "two_stage_verified_at": 10_000 - TWO_STAGE_VERIFY_INTERVAL - 1
        })
        with patch("database.db.time.time", return_value=10_000):
            result = asyncio.run(database.get_two_stage_gate_status(42))
        self.assertEqual(result.status, VerificationStatus.DENY)

    def test_database_failure_is_indeterminate(self):
        database = Database.__new__(Database)
        database.users_col = FakeUsersCollection(error=ConnectionError("offline"))
        result = asyncio.run(database.get_two_stage_gate_status(42))
        self.assertEqual(result.status, VerificationStatus.INDETERMINATE)


if __name__ == "__main__":
    unittest.main()
