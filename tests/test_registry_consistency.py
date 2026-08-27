import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock


try:
    import rapidfuzz  # noqa: F401
except ImportError:
    rapidfuzz_stub = types.ModuleType("rapidfuzz")
    rapidfuzz_stub.fuzz = SimpleNamespace(WRatio=lambda *_args, **_kwargs: 0)
    rapidfuzz_stub.process = SimpleNamespace(extract=lambda *_args, **_kwargs: [])
    sys.modules["rapidfuzz"] = rapidfuzz_stub

from bson import ObjectId
from pymongo.errors import DuplicateKeyError

from database.db import Database


def bare_database(registry, physical):
    database = Database.__new__(Database)
    database.registry_col = registry
    database.file_cols = [physical]
    database.dbs = [object()]
    database.get_db_size = AsyncMock(return_value=0)
    database._release_registry_ids = AsyncMock()
    database._invalidate_file_count = Mock()
    return database


def media():
    return SimpleNamespace(
        file_id="telegram-file",
        file_name="Movie.mkv",
        file_size=100,
        mime_type="video/x-matroska",
    )


class RegistryConsistencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_metadata_failure_does_not_retry_or_release_physical_insert(self):
        registry = SimpleNamespace(
            insert_one=AsyncMock(),
            update_one=AsyncMock(side_effect=ConnectionError("metadata offline")),
        )
        physical = SimpleNamespace(
            insert_one=AsyncMock(return_value=SimpleNamespace(inserted_id=ObjectId()))
        )
        database = bare_database(registry, physical)

        saved, _message = await database.save_file(media())

        self.assertTrue(saved)
        physical.insert_one.assert_awaited_once()
        database._release_registry_ids.assert_not_awaited()

    async def test_failed_physical_insert_releases_only_unstored_reservation(self):
        registry = SimpleNamespace(insert_one=AsyncMock())
        physical = SimpleNamespace(
            insert_one=AsyncMock(side_effect=ConnectionError("shard offline"))
        )
        database = bare_database(registry, physical)

        saved, _message = await database.save_file(media())

        self.assertFalse(saved)
        database._release_registry_ids.assert_awaited_once_with(["telegram-file"])

    async def test_reservation_failure_never_writes_physical_row(self):
        registry = SimpleNamespace(
            insert_one=AsyncMock(side_effect=ConnectionError("registry offline"))
        )
        physical = SimpleNamespace(insert_one=AsyncMock())
        database = bare_database(registry, physical)

        with self.assertRaises(ConnectionError):
            await database.save_file(media())
        physical.insert_one.assert_not_awaited()

    async def test_existing_reservation_is_reported_as_duplicate(self):
        registry = SimpleNamespace(
            insert_one=AsyncMock(side_effect=DuplicateKeyError("duplicate"))
        )
        physical = SimpleNamespace(insert_one=AsyncMock())
        database = bare_database(registry, physical)

        saved, message_text = await database.save_file(media())

        self.assertFalse(saved)
        self.assertEqual(message_text, "Duplicate")
        physical.insert_one.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
