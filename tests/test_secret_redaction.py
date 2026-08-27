import asyncio
import os
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

from database.db import (
    Database,
    PRIVATE_INVITE_REDACTION,
    redact_private_invites,
    validate_config_restore,
)
from plugins.config_backup import decrypt_config_export, encrypt_config_export


class SecretRedactionTests(unittest.TestCase):
    def test_private_invites_are_redacted_recursively(self):
        data = {
            "direct": "https://t.me/+secret",
            "embedded": "Join t.me/joinchat/another now",
            "nested": [{"link": "tg://join?invite=third"}],
            "public": "https://t.me/public_name",
        }
        redacted = redact_private_invites(data)
        rendered = repr(redacted)
        self.assertNotIn("+secret", rendered)
        self.assertNotIn("another", rendered)
        self.assertNotIn("invite=third", rendered)
        self.assertEqual(redacted["public"], "https://t.me/public_name")
        self.assertIn(PRIVATE_INVITE_REDACTION, rendered)

    def test_ordinary_export_is_redacted_but_explicit_secret_export_is_not(self):
        config = {
            "update_channel": "https://t.me/+updates-secret",
            "main_group": "https://t.me/public_group",
            "fsub_channels": [{"id": -1001, "link": "https://t.me/+gate-secret"}],
        }
        database = Database.__new__(Database)
        database.get_config = AsyncMock(return_value=config)

        ordinary = asyncio.run(database.export_config())
        secret = asyncio.run(database.export_config(include_private_invites=True))

        self.assertEqual(ordinary["update_channel"], PRIVATE_INVITE_REDACTION)
        self.assertEqual(ordinary["fsub_channels"], [{"id": -1001}])
        self.assertEqual(secret["update_channel"], config["update_channel"])
        self.assertEqual(secret["fsub_channels"][0]["link"], "https://t.me/+gate-secret")
        self.assertEqual(validate_config_restore(ordinary), {"main_group": "https://t.me/public_group"})

    def test_environment_migration_logs_never_include_values(self):
        secret = "https://t.me/+must-not-appear"
        collection = SimpleNamespace(
            find_one=AsyncMock(return_value={"_id": "bot_config"}),
            update_one=AsyncMock(),
        )
        database = Database.__new__(Database)
        database.config_col = collection
        environment = {
            "LOG_CHANNEL_ID": "",
            "DATABASE_CHANNEL_ID": "",
            "UPDATE_CHANNEL": "",
            "UPDATE_CHANNEL_LINK": secret,
            "MAIN_GROUP_LINK": "",
        }
        with (
            patch.dict(os.environ, environment, clear=False),
            self.assertLogs("database.db", level="INFO") as captured,
        ):
            asyncio.run(database.sync_config())
        self.assertNotIn(secret, "\n".join(captured.output))
        collection.update_one.assert_awaited_once()

    def test_secret_export_is_separate_authenticated_encryption_path(self):
        admin_source = Path("plugins/admin.py").read_text(encoding="utf-8")
        crypto_source = Path("plugins/config_backup.py").read_text(encoding="utf-8")
        self.assertIn("admin_export_secrets", admin_source)
        self.assertIn("include_private_invites=True", admin_source)
        self.assertIn("AESGCM", crypto_source)
        self.assertIn("Scrypt", crypto_source)

    def test_encrypted_secret_export_round_trip_and_tamper_detection(self):
        passphrase = "correct horse battery staple"
        config = {"private": "https://t.me/+encrypted-secret"}
        encrypted = encrypt_config_export(config, passphrase)
        self.assertNotIn(b"encrypted-secret", encrypted)
        self.assertEqual(decrypt_config_export(encrypted, passphrase), config)
        with self.assertRaises(Exception):
            decrypt_config_export(encrypted, "incorrect password value")


if __name__ == "__main__":
    unittest.main()
