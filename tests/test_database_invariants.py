from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId
from pymongo.errors import BulkWriteError

from database.db import (
    Database,
    _SearchCache,
    deduplicate_file_batch,
    deduplicate_search_results,
    normalize_file_name,
    normalized_search_identity,
    primary_search_identity,
    rank_search_results,
    search_tokens_for_name,
    suggest_search_titles,
)
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


def test_batch_deduplication_uses_stable_content_identity_when_available():
    files, duplicates = deduplicate_file_batch([
        {"file_id": "bot-a", "file_unique_id": "same-content", "file_name": "A"},
        {"file_id": "bot-b", "file_unique_id": "same-content", "file_name": "A copy"},
        {"file_id": "bot-c", "file_unique_id": "different", "file_name": "C"},
    ])
    assert [item["file_id"] for item in files] == ["bot-a", "bot-c"]
    assert duplicates == 1


def test_filename_normalization_removes_promotional_noise():
    assert normalize_file_name("[Site]_Movie.Name.2026_@channel.mkv") == "Site Movie Name 2026 mkv"


def test_search_identity_is_casefolded_and_extension_free():
    assert normalized_search_identity("[Site]_Movie.Name.2026_@channel.MKV") == (
        "site movie name 2026"
    )


def test_search_tokens_are_bounded_casefolded_and_unique():
    assert search_tokens_for_name("Movie.Movie.2026.MKV") == [
        "movie", "2026", "mkv"
    ]


def test_search_deduplication_keeps_real_size_variants():
    files = [
        {"file_id": "a", "file_name": "Movie.Name.mkv", "file_size": 100},
        {"file_id": "b", "file_name": "Movie_Name.mp4", "file_size": 100},
        {"file_id": "c", "file_name": "Movie Name mkv", "file_size": 200},
        {"file_id": "a", "file_name": "unrelated", "file_size": 300},
    ]
    assert [doc["file_id"] for doc in deduplicate_search_results(files)] == ["a", "c"]


def test_primary_title_ignores_hidden_episode_title():
    assert primary_search_identity(
        "On Call S01E07 War Machine 1080p WEB-DL.mkv"
    ) == "on call"
    assert primary_search_identity(
        "War Machine 2026 1080p WEB-DL.mkv"
    ) == "war machine"


def test_fuzzy_title_ranking_removes_episode_title_false_positive():
    files = [
        {"file_id": "episode", "file_name": "On Call S01E07 War Machine 720p.mkv"},
        {"file_id": "movie", "file_name": "War Machine 2026 1080p.mkv"},
    ]
    assert [doc["file_id"] for doc in rank_search_results(
        "war machine", files, 40
    )] == ["movie"]


def test_catalog_suggestions_correct_typos_without_short_substring_noise():
    choices = ("war machine", "a", "ash", "on call")
    assert suggest_search_titles("war mashine", choices=choices) == ["war machine"]


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
async def test_multiword_search_uses_indexed_tokens():
    database = bare_database()
    database._indexed_token_search = AsyncMock(return_value=[
        {"file_id": "exact", "file_name": "Reacher S01E03 2022 1080p"},
    ])

    results = await database.get_search_results("reacher 2022", max_results=10)
    assert [doc["file_id"] for doc in results] == ["exact"]
    database._indexed_token_search.assert_awaited_once()
    assert database._indexed_token_search.await_args.args[0] == [["reacher", "2022"]]


@pytest.mark.asyncio
async def test_multiword_search_does_not_relax_title_words(monkeypatch):
    monkeypatch.setattr("database.db._SEARCH_TITLE_CATALOG", ())
    database = bare_database()
    database._indexed_token_search = AsyncMock(return_value=[])

    results = await database.get_search_results("reacher nonsense", max_results=10)
    assert results == []
    database._indexed_token_search.assert_awaited_once()


@pytest.mark.asyncio
async def test_missing_metadata_retries_with_reference_title_search():
    database = bare_database()
    database._indexed_token_search = AsyncMock(side_effect=[
        [],
        [{"file_id": "series", "file_name": "Reacher S01E01 720p"}],
    ])

    results = await database.get_search_results("reacher 2022", max_results=10)
    assert [doc["file_id"] for doc in results] == ["series"]
    assert database._indexed_token_search.await_count == 2
    assert database._indexed_token_search.await_args_list[1].args[0] == [["reacher"]]


@pytest.mark.asyncio
async def test_reference_search_passes_result_limit_and_offset_to_database():
    database = bare_database()
    database._indexed_token_search = AsyncMock(return_value=[])

    await database.get_search_results("reacher 2022", max_results=10, offset=20)
    assert database._indexed_token_search.await_args.args[1:] == (80, 20)


@pytest.mark.asyncio
async def test_strict_search_uses_an_exact_index_token():
    database = bare_database()
    database._indexed_token_search = AsyncMock(return_value=[
        {"file_id": "exact", "file_name": "Reacher S01E01 720p"}
    ])

    await database.get_search_results("reacher", max_results=1)

    assert database._indexed_token_search.await_args.args[0] == [["reacher"]]


@pytest.mark.asyncio
async def test_legacy_library_uses_compatible_reference_search_without_migration():
    database = bare_database()
    database._search_tokens_complete = False
    database._legacy_search_results = AsyncMock(return_value=[
        {"file_id": "legacy", "file_name": "Aavesham 2024 Malayalam"}
    ])
    database._indexed_token_search = AsyncMock()

    results = await database.get_search_results("aavesham 2024")

    assert [doc["file_id"] for doc in results] == ["legacy"]
    database._legacy_search_results.assert_awaited_once()
    database._indexed_token_search.assert_not_awaited()


@pytest.mark.asyncio
async def test_repeated_query_uses_short_lived_result_cache():
    database = bare_database()
    database._query_cache = _SearchCache(maxsize=4, default_ttl=120)
    database._indexed_token_search = AsyncMock(return_value=[
        {"file_id": "cached", "file_name": "KGF Chapter 1 2018 720p"}
    ])

    first = await database.get_search_results("kgf")
    second = await database.get_search_results("KGF")

    assert first == second
    database._indexed_token_search.assert_awaited_once()


@pytest.mark.asyncio
async def test_catalog_typo_correction_runs_precise_corrected_search(monkeypatch):
    monkeypatch.setattr("database.db._SEARCH_TITLE_CATALOG", ("war machine",))
    database = bare_database()
    database._indexed_token_search = AsyncMock(side_effect=[
        [],
        [{"file_id": "movie", "file_name": "War Machine 2026 1080p"}],
    ])

    results = await database.get_search_results("war mashine", max_results=10)
    assert [doc["file_id"] for doc in results] == ["movie"]
    assert database._indexed_token_search.await_count == 2
    assert database._indexed_token_search.await_args_list[1].args[0] == [
        ["war", "machine"]
    ]


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
async def test_existing_shard_indexes_are_verified_without_shard_writes():
    database = bare_database()
    collection = SimpleNamespace(
        index_information=AsyncMock(return_value={
            "_id_": {"key": [("_id", 1)]},
            "file_name_1": {"key": [("file_name", 1)]},
            "file_id_1": {"key": [("file_id", 1)]},
            "search_tokens_1": {"key": [("search_tokens", 1)]},
        }),
        create_index=AsyncMock(),
    )
    database.file_cols = [collection]
    database.main_db = None
    database.deletion_col = None
    database.deletion_dead_letter_col = None
    database.broadcast_col = None
    database.groups_col = None
    database.registry_col = SimpleNamespace(
        index_information=AsyncMock(return_value={
            "file_id_1": {"key": [("file_id", 1)], "unique": True},
            "movie_id_1": {"key": [("movie_id", 1)]},
        }),
        create_index=AsyncMock(),
    )
    database.rate_limits_col = SimpleNamespace(create_index=AsyncMock())
    database.action_leases_col = SimpleNamespace(create_index=AsyncMock())
    database.announcement_col = SimpleNamespace(create_index=AsyncMock())

    await database.ensure_indexes()

    collection.create_index.assert_not_awaited()
