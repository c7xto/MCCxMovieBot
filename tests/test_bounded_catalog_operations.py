import asyncio
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from pyrogram.file_id import FileId, FileType


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
    MAX_DUPLICATE_GROUPS,
    MAX_SEARCH_CATALOG_TITLES,
    _initialize_duplicate_spool,
    _initialize_verified_cleanup_spool,
    _classify_verified_duplicate_batch,
    _clear_cleanup_registry_ids,
    _read_cleanup_registry_ids,
    _read_duplicate_groups,
    _read_duplicate_report,
    _stage_cleanup_registry_ids,
    _spool_duplicate_batch,
    deduplicate_file_batch,
    normalize_duplicate_name,
    probable_duplicate_name,
    registry_identity_document,
    telegram_file_identity,
)


ROOT = Path(__file__).resolve().parents[1]


class _FakeCursor:
    def __init__(self, documents):
        self._documents = list(documents)

    def batch_size(self, _size):
        return self

    def sort(self, key, direction):
        self._documents.sort(
            key=lambda document: document.get(key), reverse=direction < 0
        )
        return self

    def __aiter__(self):
        self._iterator = iter(self._documents)
        return self

    async def __anext__(self):
        try:
            return next(self._iterator)
        except StopIteration as error:
            raise StopAsyncIteration from error


class _FakeCollection:
    def __init__(self, documents):
        self._documents = documents

    def find(self, query=None, *_args, **_kwargs):
        query = query or {}

        def matches(document):
            for key, condition in query.items():
                value = document.get(key)
                if isinstance(condition, dict) and "$in" in condition:
                    if value not in condition["$in"]:
                        return False
                elif isinstance(condition, dict):
                    if condition.get("$exists") and key not in document:
                        return False
                    if "$ne" in condition and value == condition["$ne"]:
                        return False
                elif value != condition:
                    return False
            return True

        return _FakeCursor([doc for doc in self._documents if matches(doc)])

    async def delete_many(self, query):
        object_ids = set(query.get("_id", {}).get("$in", []))
        before = len(self._documents)
        self._documents[:] = [
            document
            for document in self._documents
            if document.get("_id") not in object_ids
        ]
        return SimpleNamespace(deleted_count=before - len(self._documents))


class _FakeRegistry:
    def __init__(self):
        self.deleted_queries = []
        self.repairs = []

    async def delete_many(self, query):
        self.deleted_queries.append(query)
        return SimpleNamespace(deleted_count=1)

    async def bulk_write(self, operations, ordered=False):
        self.repairs.extend(operations)
        return SimpleNamespace(modified_count=len(operations))


class BoundedCatalogTests(unittest.TestCase):
    def test_duplicate_normalization_runs_in_bounded_disk_spool(self):
        with tempfile.TemporaryDirectory() as directory:
            database_path = str(Path(directory) / "duplicates.sqlite3")
            _initialize_duplicate_spool(database_path)
            _spool_duplicate_batch(database_path, [
                {"_id": "1", "file_id": "same", "file_name": "Alpha 2026 1080p.mkv"},
                {"_id": "2", "file_id": "same", "file_name": "Alpha 2026 720p.mkv"},
                {"_id": "3", "file_id": "three", "file_name": "Alpha 480p.mkv", "file_size": 99},
                {"_id": "4", "file_id": "four", "file_name": "Alpha.480p.mkv", "file_size": 99},
            ])
            groups = _read_duplicate_groups(database_path)
            report = _read_duplicate_report(database_path)
            spool_bytes = Path(database_path).read_bytes()

        self.assertLessEqual(len(groups), MAX_DUPLICATE_GROUPS)
        exact = next(group for group in groups if group["type"] == "exact")
        fuzzy = next(group for group in groups if group["type"] == "probable")
        self.assertEqual(exact["count"], 2)
        self.assertEqual(exact["ids"], [])
        self.assertEqual(fuzzy["count"], 2)
        self.assertFalse(fuzzy["truncated"])
        self.assertTrue(report["summary"]["report_only"])
        self.assertEqual(
            report["summary"]["storage"], "compact_fingerprints"
        )
        self.assertNotIn(b"Alpha", spool_bytes)
        self.assertNotIn(b"same", spool_bytes)

    def test_duplicate_phases_use_independent_compact_tables(self):
        documents = [
            {
                "file_id": "one",
                "file_name": "Movie 2026 1080p.mkv",
                "file_size": 100,
            },
            {
                "file_id": "two",
                "file_name": "Movie.2026.1080p.mp4",
                "file_size": 100,
            },
        ]
        with tempfile.TemporaryDirectory() as directory:
            exact_path = str(Path(directory) / "exact.sqlite3")
            probable_path = str(Path(directory) / "probable.sqlite3")
            _initialize_duplicate_spool(exact_path, "exact")
            _spool_duplicate_batch(exact_path, documents, mode="exact")
            _initialize_duplicate_spool(probable_path, "probable")
            _spool_duplicate_batch(probable_path, documents, mode="probable")

            exact_report = _read_duplicate_report(exact_path)
            probable_report = _read_duplicate_report(probable_path)

        self.assertEqual(exact_report["summary"]["exact_groups"], 0)
        self.assertEqual(exact_report["summary"]["probable_groups"], 0)
        self.assertEqual(probable_report["summary"]["exact_groups"], 0)
        self.assertEqual(probable_report["summary"]["probable_groups"], 1)

    def test_telegram_media_identity_ignores_refreshable_file_reference(self):
        first = FileId(
            file_type=FileType.VIDEO,
            dc_id=4,
            file_reference=b"first-reference",
            media_id=987654321,
            access_hash=111,
        ).encode()
        refreshed = FileId(
            file_type=FileType.VIDEO,
            dc_id=4,
            file_reference=b"new-reference",
            media_id=987654321,
            access_hash=222,
        ).encode()

        self.assertNotEqual(first, refreshed)
        self.assertEqual(
            telegram_file_identity(first), telegram_file_identity(refreshed)
        )

        registry_doc = registry_identity_document(first, "unique-content")
        self.assertEqual(registry_doc["telegram_identity"], telegram_file_identity(first))
        self.assertEqual(registry_doc["file_unique_id"], "unique-content")

        files, duplicate_count = deduplicate_file_batch(
            [{"file_id": first}, {"file_id": refreshed}]
        )
        self.assertEqual([item["file_id"] for item in files], [first])
        self.assertEqual(duplicate_count, 1)

    def test_verified_cleanup_classifies_only_same_telegram_media(self):
        def file_id(media_id, reference):
            return FileId(
                file_type=FileType.VIDEO,
                dc_id=4,
                file_reference=reference,
                media_id=media_id,
                access_hash=media_id * 10,
            ).encode()

        documents = [
            {"_id": "keep", "file_id": file_id(10, b"new")},
            {"_id": "remove", "file_id": file_id(10, b"old")},
            {"_id": "different", "file_id": file_id(11, b"other")},
        ]
        with tempfile.TemporaryDirectory() as directory:
            database_path = str(Path(directory) / "cleanup.sqlite3")
            _initialize_verified_cleanup_spool(database_path)
            duplicates = _classify_verified_duplicate_batch(
                database_path, documents
            )
            _stage_cleanup_registry_ids(database_path, ["one", "two", "one"])
            pending = _read_cleanup_registry_ids(database_path)
            _clear_cleanup_registry_ids(database_path, ["one", "two"])
            cleared = _read_cleanup_registry_ids(database_path)

        self.assertEqual([item["_id"] for item in duplicates], ["remove"])
        self.assertEqual(set(pending), {"one", "two"})
        self.assertEqual(cleared, [])

    def test_verified_cleanup_keeps_newer_row_and_repairs_registry(self):
        def file_id(media_id, reference):
            return FileId(
                file_type=FileType.VIDEO,
                dc_id=4,
                file_reference=reference,
                media_id=media_id,
                access_hash=media_id * 10,
            ).encode()

        shared_file_id = file_id(25, b"shared")
        old_copy = {
            "_id": "1",
            "file_id": shared_file_id,
            "file_name": "Movie 2026.mkv",
            "file_size": 100,
            "mime_type": "video/x-matroska",
        }
        newest_copy = {
            "_id": "3",
            "file_id": shared_file_id,
            "file_name": "Movie 2026.mkv",
            "file_size": 100,
            "mime_type": "video/x-matroska",
        }
        different = {
            "_id": "2",
            "file_id": file_id(26, b"different"),
            "file_name": "Other 2026.mkv",
            "file_size": 200,
            "mime_type": "video/x-matroska",
        }
        older_cluster = _FakeCollection([old_copy])
        newer_cluster = _FakeCollection([different, newest_copy])
        database = Database.__new__(Database)
        database.file_cols = [older_cluster, newer_cluster]
        database.registry_col = _FakeRegistry()
        database.get_total_files = AsyncMock(side_effect=[3, 2])
        progress = []

        async def collect_progress(update):
            progress.append(update)

        with tempfile.TemporaryDirectory() as directory, patch(
            "database.db._DUPLICATE_SCAN_DIR", Path(directory)
        ):
            result = asyncio.run(
                database.delete_verified_duplicates(collect_progress)
            )

        self.assertEqual(result["deleted"], 1)
        self.assertEqual(result["remaining"], 2)
        self.assertEqual(older_cluster._documents, [])
        self.assertEqual(len(newer_cluster._documents), 2)
        self.assertTrue(database.registry_col.repairs)
        self.assertEqual(progress[-1]["deleted"], 1)

    def test_full_duplicate_scan_reports_live_phases_and_cleans_spools(self):
        documents = [
            {
                "file_id": "same",
                "file_name": "Alpha 2026 1080p.mkv",
                "file_size": 100,
            },
            {
                "file_id": "same",
                "file_name": "Alpha 2026 720p.mkv",
                "file_size": 90,
            },
            {
                "file_id": "three",
                "file_name": "Beta 2026 1080p.mkv",
                "file_size": 200,
            },
            {
                "file_id": "four",
                "file_name": "Beta.2026.1080p.mp4",
                "file_size": 200,
            },
        ]
        database = Database.__new__(Database)
        database.file_cols = [_FakeCollection(documents)]
        database.get_total_files = AsyncMock(return_value=len(documents))
        progress = []

        async def collect_progress(update):
            progress.append(update)

        with tempfile.TemporaryDirectory() as directory, patch(
            "database.db._DUPLICATE_SCAN_DIR", Path(directory)
        ):
            report = asyncio.run(
                database.scan_duplicate_report(collect_progress)
            )
            leftover_spools = list(Path(directory).glob("*.sqlite3"))

        self.assertEqual(report["summary"]["scanned"], len(documents))
        self.assertEqual(report["summary"]["exact_groups"], 1)
        self.assertEqual(report["summary"]["probable_groups"], 1)
        self.assertTrue(any(group["name"].startswith("Alpha") for group in report["groups"]))
        self.assertTrue(any(group["name"].startswith("Beta") for group in report["groups"]))
        self.assertEqual({item["phase"] for item in progress}, {"exact", "probable", "labels"})
        self.assertFalse(leftover_spools)

    def test_probable_identity_preserves_release_variants(self):
        self.assertNotEqual(
            probable_duplicate_name("Movie 2026 1080p Malayalam.mkv"),
            probable_duplicate_name("Movie 2026 720p Tamil.mkv"),
        )

    def test_normalized_duplicate_key_removes_release_metadata(self):
        self.assertEqual(
            normalize_duplicate_name("Movie.2026.1080p.WEB-DL.mkv"), "movie"
        )

    def test_rapidfuzz_is_dispatched_to_worker_thread(self):
        database = Database.__new__(Database)
        database._fuzzy_worker_semaphore = asyncio.Semaphore(1)
        with patch("database.db.suggest_search_titles", return_value=["movie"]) as ranker:
            result = asyncio.run(database.suggest_search_titles("mvoie", limit=1))
        self.assertEqual(result, ["movie"])
        ranker.assert_called_once_with("mvoie", 1)

    def test_catalog_and_duplicate_memory_have_explicit_caps(self):
        self.assertLessEqual(MAX_SEARCH_CATALOG_TITLES, 250_000)
        self.assertEqual(MAX_DUPLICATE_GROUPS, 100)
        source = (ROOT / "database" / "db.py").read_text(encoding="utf-8")
        self.assertNotIn("fuzzy_data = {}", source)
        self.assertIn("asyncio.to_thread(\n                lambda: tuple(sorted(titles))", source)
        self.assertIn("tempfile.mkstemp", source)


if __name__ == "__main__":
    unittest.main()
