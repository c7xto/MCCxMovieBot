import asyncio
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


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

        self.assertLessEqual(len(groups), MAX_DUPLICATE_GROUPS)
        exact = next(group for group in groups if group["type"] == "exact")
        fuzzy = next(group for group in groups if group["type"] == "probable")
        self.assertEqual(exact["ids"], ["1:1", "1:2"])
        self.assertEqual(fuzzy["count"], 2)
        self.assertFalse(fuzzy["truncated"])
        self.assertTrue(report["summary"]["report_only"])

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
