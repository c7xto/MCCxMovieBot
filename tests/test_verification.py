import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace

from pyrogram.enums import ChatMemberStatus

from verification import (
    VerificationStatus,
    check_channel_membership,
)


ROOT = Path(__file__).resolve().parents[1]


class FakeClient:
    def __init__(self, outcome):
        self.outcome = outcome

    async def get_chat_member(self, _channel_id, _user_id):
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return SimpleNamespace(status=self.outcome)


class VerificationTests(unittest.TestCase):
    def check(self, outcome):
        return asyncio.run(check_channel_membership(FakeClient(outcome), -1001, 7))

    def test_confirmed_member_passes(self):
        self.assertEqual(
            self.check(ChatMemberStatus.MEMBER).status,
            VerificationStatus.PASS,
        )

    def test_confirmed_non_member_is_denied(self):
        self.assertEqual(
            self.check(ChatMemberStatus.LEFT).status,
            VerificationStatus.DENY,
        )

    def test_dependency_error_is_indeterminate_not_pass(self):
        self.assertEqual(
            self.check(ConnectionError("offline")).status,
            VerificationStatus.INDETERMINATE,
        )

    def test_old_fail_open_patterns_are_absent(self):
        req_source = (ROOT / "plugins/req_fsub.py").read_text(encoding="utf-8")
        filter_source = (ROOT / "plugins/filter.py").read_text(encoding="utf-8")
        self.assertNotIn("True if isinstance(r, BaseException)", req_source)
        self.assertNotIn("False if isinstance(r, BaseException)", filter_source)


if __name__ == "__main__":
    unittest.main()
