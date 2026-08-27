import time
import unittest
from unittest.mock import AsyncMock

from database.db import Database, _SearchCache


class CacheAndIndexerHeartbeatTests(unittest.IsolatedAsyncioTestCase):
    def test_cache_metrics_track_hits_misses_and_evictions(self):
        cache = _SearchCache(maxsize=1, default_ttl=60)
        cache.set("first", 1)
        self.assertEqual(cache.get("first"), 1)
        self.assertIsNone(cache.get("missing"))
        cache.set("second", 2)
        self.assertEqual(
            cache.snapshot(),
            {"size": 1, "maxsize": 1, "hits": 1, "misses": 1, "evictions": 1},
        )

    async def test_checkpoint_refreshes_task_heartbeat(self):
        database = object.__new__(Database)
        settings = AsyncMock()
        settings.update_one.return_value.acknowledged = True
        main_db = {"settings": settings}
        indexer = AsyncMock()
        indexer.update_one.return_value.acknowledged = True
        database.main_db = main_db
        database.indexer_col = indexer

        before = time.time()
        await database.set_index_progress(-100, 250)
        indexer.update_one.assert_awaited_once()
        update = indexer.update_one.await_args.args[1]["$set"]
        self.assertEqual(update["checkpoint"], 250)
        self.assertGreaterEqual(update["updated"], before)


if __name__ == "__main__":
    unittest.main()
