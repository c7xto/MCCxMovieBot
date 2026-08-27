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
    rapidfuzz_stub.fuzz = SimpleNamespace(
        ratio=lambda *_args, **_kwargs: 100,
        WRatio=lambda *_args, **_kwargs: 100,
        token_sort_ratio=lambda *_args, **_kwargs: 100,
    )
    rapidfuzz_stub.process = SimpleNamespace(extract=lambda *_args, **_kwargs: [])
    sys.modules["rapidfuzz"] = rapidfuzz_stub

from database.db import Database, validate_config_restore


ROOT = Path(__file__).resolve().parents[1]


class FakeConfigCollection:
    def __init__(self):
        self.updates = []

    async def update_one(self, query, update, **kwargs):
        self.updates.append((query, update, kwargs))


class ConfigRestoreSafetyTests(unittest.TestCase):
    def test_restore_requires_bounded_mapping_schema(self):
        with self.assertRaisesRegex(ValueError, "JSON object"):
            validate_config_restore(["not", "a", "mapping"])
        with self.assertRaisesRegex(ValueError, "too many keys"):
            validate_config_restore({str(index): index for index in range(51)})
        with self.assertRaisesRegex(ValueError, "nesting"):
            validate_config_restore({"x": {"x": {"x": {"x": {"x": {}}}}}})
        with self.assertRaisesRegex(ValueError, "invalid type"):
            validate_config_restore({"maintenance_mode": 1})

    def test_restore_accepts_only_typed_allowlisted_fields(self):
        safe = validate_config_restore({
            "welcome_text": "Welcome",
            "maintenance_mode": False,
            "admin_id": 999,
            "unexpected": {"value": "ignored"},
        })
        self.assertEqual(
            safe,
            {"welcome_text": "Welcome", "maintenance_mode": False},
        )

    def test_restore_is_one_validated_database_update(self):
        collection = FakeConfigCollection()
        database = Database.__new__(Database)
        database.config_col = collection

        result = asyncio.run(database.restore_config({"auto_delete_time": 600}))

        self.assertTrue(result)
        self.assertEqual(len(collection.updates), 1)
        self.assertEqual(
            collection.updates[0],
            (
                {"_id": "bot_config"},
                {"$set": {"auto_delete_time": 600}},
                {"upsert": True},
            ),
        )

    def test_handler_checks_size_before_download_and_requires_confirmation(self):
        source = (ROOT / "plugins" / "admin.py").read_text(encoding="utf-8")
        handler = source[source.index("async def handle_config_restore_file"):]
        self.assertLess(handler.index("file_size"), handler.index("download_media"))
        self.assertIn("await asyncio.to_thread(_parse_config_backup, raw)", handler)
        self.assertIn('callback_data="admin_restore_apply"', handler)
        self.assertIn("async def apply_config_restore", handler)


if __name__ == "__main__":
    unittest.main()
