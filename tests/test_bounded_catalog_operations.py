import asyncio
import sys
import tempfile
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
    MAX_DUPLICATE_GROUPS,
    MAX_SEARCH_CATALOG_TITLES,
    _initialize_duplicate_spool,
    _read_duplicate_groups,
    _read_duplicate_report,
    _spool_duplicate_batch,
    normalize_duplicate_name,
    probable_duplicate_name,
)


ROOT = Path(__file__).resolve().parents[1]


class _FakeCursor:
    def __init__(self, documents):
        self._documents = list(documents)

    def batch_size(self, _size):
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

    def find(self, *_args, **_kwargs):
        return _FakeCursor(self._documents)


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
