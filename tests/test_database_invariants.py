from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId
from pymongo.errors import BulkWriteError

from database.db import Database, deduplicate_file_batch, normalize_file_name
from utils import callback_data


class AsyncListCursor:
    def __init__(self, docs):
        self.docs = docs

    async def to_list(self, length=None):
        return self.docs[:length]


class AsyncIterator:
    def __init__(self, items):
        self._items = iter(items)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration


class PartialInsertCollection:
    def __init__(self):
        self.received = []
        self.stored = []

    async def insert_many(self, docs, ordered=False):
        self.received.append(list(docs))
        first = dict(docs[0])
        first["_id"] = ObjectId()
        self.stored.append(first)
        raise BulkWriteError({
            "writeErrors": [{"index": 1, "code": 121, "errmsg": "validation failed"}],
            "writeConcernErrors": [],
            "nInserted": 1,
        })

    def find(self, query, projection):
        wanted = set(query["file_id"]["$in"])
        return AsyncListCursor([doc for doc in self.stored if doc["file_id"] in wanted])


class SuccessfulInsertCollection:
    def __init__(self):
        self.received = []

    async def insert_many(self, docs, ordered=False):
        self.received.append(list(docs))
        return SimpleNamespace(inserted_ids=[ObjectId() for _ in docs])


def bare_database():
    database = Database.__new__(Database)
    database.registry_col = object()
    database._file_count_cache = (0.0, 0)
    return database


def test_batch_deduplication_keeps_first_document():
    files, duplicates = deduplicate_file_batch([
        {"file_id": "a", "file_name": "first"},
        {"file_id": "a", "file_name": "duplicate"},
        {"file_id": "b", "file_name": "second"},
        {"file_name": "missing id"},
    ])
    assert [item["file_name"] for item in files] == ["first", "second"]
    assert duplicates == 2


def test_filename_normalization_removes_promotional_noise():
    assert normalize_file_name("[Site]_Movie.Name.2026_@channel.mkv") == "Site Movie Name 2026 mkv"


def test_callback_data_respects_telegram_utf8_byte_limit():
    payload = callback_data("reqmovie#", "മലയാളം സിനിമ" * 10)
    assert len(payload.encode("utf-8")) <= 64
    assert payload.startswith("reqmovie#")


@pytest.mark.asyncio
async def test_bulk_partial_success_retries_only_failed_documents():
    database = bare_database()
    partial = PartialInsertCollection()
    successful = SuccessfulInsertCollection()
    database.file_cols = [partial, successful]
    database.dbs = [object(), object()]
    database.get_db_size = AsyncMock(return_value=0)
    database._registry_bulk_reserve = AsyncMock(return_value=(["a", "b"], 0))
    database._mark_registry_locations = AsyncMock()

    saved, duplicates = await database.save_files_bulk([
        {"file_id": "a", "file_name": "A"},
        {"file_id": "b", "file_name": "B"},
    ])

    assert saved == 2
    assert duplicates == 0
    assert [doc["file_id"] for doc in partial.received[0]] == ["a", "b"]
    assert [doc["file_id"] for doc in successful.received[0]] == ["b"]


@pytest.mark.asyncio
async def test_delete_by_file_id_releases_registry_after_all_clusters():
    database = bare_database()
    database.file_cols = [
        SimpleNamespace(delete_many=AsyncMock(return_value=SimpleNamespace(deleted_count=1))),
        SimpleNamespace(delete_many=AsyncMock(return_value=SimpleNamespace(deleted_count=0))),
    ]
    database._release_registry_ids = AsyncMock()

    assert await database.delete_file_by_id("file-a") is True
    database._release_registry_ids.assert_awaited_once_with(["file-a"])


@pytest.mark.asyncio
async def test_reset_drops_registry_and_rebuilds_indexes():
    database = bare_database()
    database.dbs = [SimpleNamespace(command=AsyncMock())]
    database.users_col = SimpleNamespace(drop=AsyncMock())
    database.banned_col = SimpleNamespace(drop=AsyncMock())
    database.file_cols = [SimpleNamespace(drop=AsyncMock()), SimpleNamespace(drop=AsyncMock())]
    database.registry_col = SimpleNamespace(drop=AsyncMock())
    database.ensure_indexes = AsyncMock()

    assert await database.reset_database() is True
    database.registry_col.drop.assert_awaited_once()
    database.ensure_indexes.assert_awaited_once()


@pytest.mark.asyncio
async def test_empty_registry_with_existing_files_requires_migration():
    database = bare_database()
    database.registry_col = SimpleNamespace(count_documents=AsyncMock(return_value=0))
    database.file_cols = [SimpleNamespace(find_one=AsyncMock(return_value={"_id": ObjectId()}))]

    assert await database.registry_needs_migration() is True


@pytest.mark.asyncio
async def test_search_preserves_relevance_tiers():
    database = bare_database()
    database._regex_search = AsyncMock(side_effect=[
        [{"file_id": "exact"}],
        [{"file_id": "loose"}],
        [{"file_id": "word-a"}],
        [{"file_id": "word-b"}],
    ])

    results = await database.get_search_results("alpha beta", max_results=10)
    assert [doc["file_id"] for doc in results] == ["exact", "loose", "word-a", "word-b"]


@pytest.mark.asyncio
async def test_capacity_uses_whole_cluster_and_ignores_system_databases():
    database = bare_database()
    database._db_size_cache = {}
    one_mb = 1024 * 1024
    client = SimpleNamespace(list_databases=AsyncMock(return_value=AsyncIterator([
        {"name": "admin", "sizeOnDisk": 50 * one_mb},
        {"name": "MCCxBot_Cluster_1", "sizeOnDisk": 200 * one_mb},
        {"name": "another_app", "sizeOnDisk": 251 * one_mb},
    ])))
    cluster_db = SimpleNamespace(client=client)

    assert await database.get_db_size(cluster_db) == 451


@pytest.mark.asyncio
async def test_existing_shard_indexes_are_verified_without_writes():
    database = bare_database()
    collection = SimpleNamespace(
        index_information=AsyncMock(return_value={
            "_id_": {"key": [("_id", 1)]},
            "file_name_1": {"key": [("file_name", 1)]},
            "file_id_1": {"key": [("file_id", 1)]},
        }),
        create_index=AsyncMock(),
    )
    database.file_cols = [collection]
    database.main_db = None
    database.deletion_col = None
    database.groups_col = None
    database.registry_col = None

    await database.ensure_indexes()

    collection.create_index.assert_not_awaited()
