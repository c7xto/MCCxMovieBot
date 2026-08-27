import asyncio
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace

from bson import ObjectId


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

from database.db import Database


ROOT = Path(__file__).resolve().parents[1]


class FakeCollection:
    def __init__(self, result=None):
        self.result = result
        self.find_calls = []
        self.updates = []

    async def find_one(self, query, *_args, **kwargs):
        self.find_calls.append((query, kwargs))
        await asyncio.sleep(0)
        return self.result

    async def update_one(self, query, update, **kwargs):
        self.updates.append((query, update, kwargs))
        return SimpleNamespace(matched_count=1)


class RegistryRoutingTests(unittest.TestCase):
    def test_registry_location_routes_directly_to_one_shard(self):
        movie_id = ObjectId()
        movie = {"_id": movie_id, "file_id": "telegram-file"}
        registry = FakeCollection({
            "file_id": "telegram-file",
            "movie_id": str(movie_id),
            "cluster": 2,
        })
        first = FakeCollection()
        second = FakeCollection(movie)
        database = Database.__new__(Database)
        database.registry_col = registry
        database.file_cols = [first, second]

        result = asyncio.run(database.get_file(str(movie_id)))

        self.assertEqual(result, movie)
        self.assertEqual(len(registry.find_calls), 1)
        self.assertEqual(registry.find_calls[0][0], {"movie_id": str(movie_id)})
        self.assertEqual(first.find_calls, [])
        self.assertEqual(len(second.find_calls), 1)

    def test_legacy_parallel_fallback_repairs_registry_location(self):
        movie_id = ObjectId()
        movie = {"_id": movie_id, "file_id": "legacy-file"}
        registry = FakeCollection()
        first = FakeCollection()
        second = FakeCollection(movie)
        database = Database.__new__(Database)
        database.registry_col = registry
        database.file_cols = [first, second]

        result = asyncio.run(database.get_file(str(movie_id)))

        self.assertEqual(result, movie)
        self.assertEqual(len(first.find_calls), 1)
        self.assertEqual(len(second.find_calls), 1)
        self.assertEqual(
            registry.updates[0][1]["$set"],
            {"cluster": 2, "movie_id": str(movie_id)},
        )

    def test_registry_movie_id_lookup_has_required_index(self):
        source = (ROOT / "database" / "db.py").read_text(encoding="utf-8")
        self.assertIn('"file_registry.movie_id"', source)
        self.assertNotIn("for col in self.file_cols:\n            doc = await col.find_one({\"_id\"", source)


if __name__ == "__main__":
    unittest.main()
