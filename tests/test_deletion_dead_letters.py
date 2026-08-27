import asyncio
import sys
import types
import unittest
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

from plugins import health_monitor


def named_error(name):
    return type(name, (Exception,), {})(name)


def fake_database():
    return SimpleNamespace(
        complete_deletion=AsyncMock(),
        retry_deletion=AsyncMock(),
        dead_letter_deletion=AsyncMock(return_value="dead-letter-id"),
    )


class DeletionDeadLetterTests(unittest.TestCase):
    def test_errors_are_classified_and_backoff_is_exponential(self):
        self.assertEqual(
            health_monitor.classify_deletion_error(named_error("MessageIdInvalid")),
            "already_gone",
        )
        self.assertEqual(
            health_monitor.classify_deletion_error(
                named_error("MessageDeleteForbidden")
            ),
            "permanent",
        )
        self.assertEqual(
            health_monitor.classify_deletion_error(ConnectionError("offline")),
            "transient",
        )
        self.assertEqual(health_monitor.deletion_retry_delay(0), 30)
        self.assertEqual(health_monitor.deletion_retry_delay(3), 240)

    def run_job(self, error, attempts=0):
        database = fake_database()
        job = {
            "_id": "job-id",
            "chat_id": 123,
            "message_id": 456,
            "attempts": attempts,
            "due_at": 1,
        }
        with (
            patch.object(health_monitor, "db", database),
            patch.object(
                health_monitor, "telegram_call", AsyncMock(side_effect=error)
            ),
            patch.object(health_monitor, "send_smart_log", AsyncMock()) as alert,
        ):
            result = asyncio.run(
                health_monitor.process_deletion_job(SimpleNamespace(), job)
            )
        return result, database, alert

    def test_transient_failure_is_retried_without_discarding_job(self):
        result, database, alert = self.run_job(ConnectionError("offline"))
        self.assertEqual(result, "retry_scheduled")
        database.retry_deletion.assert_awaited_once_with("job-id", 30)
        database.complete_deletion.assert_not_awaited()
        database.dead_letter_deletion.assert_not_awaited()
        alert.assert_not_awaited()

    def test_exhausted_transient_failure_is_retained_and_alerted(self):
        result, database, alert = self.run_job(
            ConnectionError("offline"),
            attempts=health_monitor.MAX_DELETION_ATTEMPTS - 1,
        )
        self.assertEqual(result, "dead_lettered")
        database.dead_letter_deletion.assert_awaited_once()
        database.complete_deletion.assert_not_awaited()
        alert.assert_awaited_once()
        markup = alert.await_args.kwargs["reply_markup"]
        self.assertIn(
            "retry_deletion#dead-letter-id",
            markup.inline_keyboard[0][0].callback_data,
        )

    def test_permanent_failure_is_dead_lettered_immediately(self):
        error = named_error("ChatAdminRequired")
        result, database, _ = self.run_job(error)
        self.assertEqual(result, "dead_lettered")
        self.assertTrue(database.dead_letter_deletion.await_args.args[2])

    def test_already_absent_message_completes_successfully(self):
        result, database, _ = self.run_job(named_error("MessageIdInvalid"))
        self.assertEqual(result, "completed_absent")
        database.complete_deletion.assert_awaited_once_with("job-id")
        database.dead_letter_deletion.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
