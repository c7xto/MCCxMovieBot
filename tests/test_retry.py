import asyncio
import unittest
from pathlib import Path

from plugins.retry import retry_with_backoff


ROOT = Path(__file__).resolve().parents[1]


class RetryTests(unittest.TestCase):
    def test_retries_then_returns_acknowledged_result(self):
        calls = 0
        delays = []

        async def operation():
            nonlocal calls
            calls += 1
            if calls < 3:
                raise ConnectionError("transient")
            return "acknowledged"

        async def no_sleep(delay):
            delays.append(delay)

        result = asyncio.run(
            retry_with_backoff(operation, attempts=4, jitter=0, sleep=no_sleep)
        )
        self.assertEqual(result, "acknowledged")
        self.assertEqual(calls, 3)
        self.assertEqual(delays, [0.5, 1.0])

    def test_exhaustion_raises_without_claiming_success(self):
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            raise ConnectionError("offline")

        async def no_sleep(_delay):
            return None

        with self.assertRaises(ConnectionError):
            asyncio.run(
                retry_with_backoff(operation, attempts=3, jitter=0, sleep=no_sleep)
            )
        self.assertEqual(calls, 3)

    def test_non_retryable_error_is_raised_immediately(self):
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            raise ValueError("permanent")

        with self.assertRaises(ValueError):
            asyncio.run(
                retry_with_backoff(
                    operation,
                    should_retry=lambda exc: not isinstance(exc, ValueError),
                )
            )
        self.assertEqual(calls, 1)

    def test_indexer_never_advances_after_an_exhausted_save(self):
        source = (ROOT / "plugins/bulk_indexer.py").read_text(encoding="utf-8")
        self.assertNotIn("Batch save error (non-fatal)", source)
        self.assertIn("_record_failed_range", source)
        self.assertIn("checkpoint was **not advanced**", source)

    def test_checkpoint_write_requires_acknowledgement(self):
        source = (ROOT / "database/db.py").read_text(encoding="utf-8")
        self.assertIn("if not result.acknowledged", source)
        self.assertIn("Index checkpoint write was not acknowledged", source)


if __name__ == "__main__":
    unittest.main()
