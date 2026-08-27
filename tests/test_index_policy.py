import asyncio
import unittest
from unittest.mock import AsyncMock

from database.index_policy import (
    RequiredIndexError,
    ensure_required_unique_index,
    has_exact_unique_index,
)


class IndexPolicyTests(unittest.TestCase):
    def test_exact_unique_index_is_required(self):
        self.assertTrue(
            has_exact_unique_index(
                {"file_id_1": {"key": [("file_id", 1)], "unique": True}},
                "file_id",
            )
        )
        self.assertFalse(
            has_exact_unique_index(
                {"file_id_1": {"key": [("file_id", 1)]}},
                "file_id",
            )
        )

    def test_non_unique_conflict_fails_with_repair_instruction(self):
        collection = AsyncMock()
        collection.index_information.return_value = {
            "file_id_1": {"key": [("file_id", 1)]}
        }
        with self.assertRaisesRegex(RequiredIndexError, "repair_registry_index"):
            asyncio.run(
                ensure_required_unique_index(
                    collection, "file_id", "file_registry.file_id"
                )
            )
        collection.create_index.assert_not_awaited()

    def test_created_index_is_re_read_and_verified(self):
        collection = AsyncMock()
        collection.index_information.side_effect = [
            {"_id_": {"key": [("_id", 1)]}},
            {
                "_id_": {"key": [("_id", 1)]},
                "file_id_1": {"key": [("file_id", 1)], "unique": True},
            },
        ]
        asyncio.run(
            ensure_required_unique_index(
                collection, "file_id", "file_registry.file_id"
            )
        )
        collection.create_index.assert_awaited_once_with("file_id", unique=True)


if __name__ == "__main__":
    unittest.main()
