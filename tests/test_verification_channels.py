import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace

from pyrogram.enums import ChatMemberStatus

from plugins.verification_channels import (
    ChannelConfigurationError,
    parse_request_fsub_input,
    resolve_request_fsub_channel,
)


ROOT = Path(__file__).resolve().parents[1]


class FakeClient:
    def __init__(self, *, username=None, bot_status=ChatMemberStatus.ADMINISTRATOR):
        self.me = SimpleNamespace(id=99)
        self.chat = SimpleNamespace(id=-100123, username=username, title="Gate")
        self.bot_status = bot_status

    async def get_chat(self, _identifier):
        return self.chat

    async def get_chat_member(self, _chat_id, _user_id):
        return SimpleNamespace(status=self.bot_status)

    async def create_chat_invite_link(self, _chat_id, **_kwargs):
        return SimpleNamespace(invite_link="https://t.me/+verified")


class VerificationChannelTests(unittest.TestCase):
    def test_private_link_alone_is_rejected(self):
        with self.assertRaises(ChannelConfigurationError):
            parse_request_fsub_input("https://t.me/+unverifiable")

    def test_private_channel_stores_numeric_id_and_generated_link(self):
        result = asyncio.run(
            resolve_request_fsub_channel(
                FakeClient(), "-100123 https://t.me/+operator-link"
            )
        )
        self.assertEqual(result.chat_id, -100123)
        self.assertEqual(result.link, "https://t.me/+verified")

    def test_public_username_is_resolved_to_numeric_id(self):
        result = asyncio.run(
            resolve_request_fsub_channel(FakeClient(username="publicgate"), "@publicgate")
        )
        self.assertEqual(result.chat_id, -100123)
        self.assertEqual(result.link, "https://t.me/publicgate")

    def test_non_admin_bot_is_rejected(self):
        with self.assertRaises(ChannelConfigurationError):
            asyncio.run(
                resolve_request_fsub_channel(
                    FakeClient(bot_status=ChatMemberStatus.MEMBER), "@publicgate"
                )
            )

    def test_request_interval_is_only_marked_after_a_pass(self):
        source = (ROOT / "plugins/req_fsub.py").read_text(encoding="utf-8")
        self.assertNotIn("mark_req_fsub_shown", source)
        self.assertIn("mark_req_fsub_verified", source)

    def test_storage_requires_numeric_identity_and_separate_link(self):
        source = (ROOT / "database/db.py").read_text(encoding="utf-8")
        self.assertIn("async def add_req_fsub_channel(self, channel_id, link", source)
        self.assertIn('entry = {"id": channel_id, "link": link', source)


if __name__ == "__main__":
    unittest.main()
