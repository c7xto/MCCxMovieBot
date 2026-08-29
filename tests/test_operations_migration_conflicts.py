from types import SimpleNamespace

import pytest
from pymongo.errors import BulkWriteError

from database.db import Database


class _BulkCollection:
    def __init__(self, outcome):
        self.outcome = outcome
        self.calls = 0

    async def bulk_write(self, operations, ordered=False):
        self.calls += 1
        assert operations == ["one", "two"]
        assert ordered is False
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


@pytest.mark.asyncio
async def test_operations_migration_batch_returns_normal_upserts():
    collection = _BulkCollection(SimpleNamespace(upserted_count=2))

    inserted, conflicts = await Database._write_operations_migration_batch(
        collection,
        ["one", "two"],
        collection="users",
        source="legacy",
    )

    assert (inserted, conflicts) == (2, 0)


@pytest.mark.asyncio
async def test_registry_duplicate_keys_are_preserved_as_merge_conflicts():
    error = BulkWriteError(
        {
            "nUpserted": 1,
            "writeErrors": [
                {"index": 1, "code": 11000, "errmsg": "duplicate file_id"}
            ],
        }
    )
    collection = _BulkCollection(error)

    inserted, conflicts = await Database._write_operations_migration_batch(
        collection,
        ["one", "two"],
        collection="file_registry",
        source="legacy",
    )

    assert (inserted, conflicts) == (1, 1)


@pytest.mark.asyncio
async def test_non_registry_duplicate_key_remains_fatal():
    error = BulkWriteError(
        {
            "nUpserted": 0,
            "writeErrors": [{"index": 0, "code": 11000, "errmsg": "duplicate"}],
        }
    )
    collection = _BulkCollection(error)

    with pytest.raises(BulkWriteError):
        await Database._write_operations_migration_batch(
            collection,
            ["one", "two"],
            collection="users",
            source="legacy",
        )


@pytest.mark.asyncio
async def test_non_duplicate_registry_write_error_remains_fatal():
    error = BulkWriteError(
        {
            "nUpserted": 0,
            "writeErrors": [{"index": 0, "code": 121, "errmsg": "validation"}],
        }
    )
    collection = _BulkCollection(error)

    with pytest.raises(BulkWriteError):
        await Database._write_operations_migration_batch(
            collection,
            ["one", "two"],
            collection="file_registry",
            source="legacy",
        )
