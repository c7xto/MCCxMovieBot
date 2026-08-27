import io
import logging
import unittest
from pathlib import Path
from types import SimpleNamespace

from plugins.log_safety import SecretRedactionFilter
from utils import _html, html_user_mention, public_error_message, report_internal_error


ROOT = Path(__file__).resolve().parents[1]


class MessageSafetyTests(unittest.TestCase):
    def test_html_helpers_neutralize_user_markup(self):
        self.assertEqual(_html("<b>x & y</b>"), "&lt;b&gt;x &amp; y&lt;/b&gt;")
        mention = html_user_mention(
            SimpleNamespace(id=42, first_name='<a href="https://evil">Admin</a>')
        )
        self.assertIn("tg://user?id=42", mention)
        self.assertNotIn('<a href="https://evil">', mention)
        self.assertIn("&lt;a href=", mention)

    def test_structured_errors_return_stable_code_and_redact_logs(self):
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.addFilter(SecretRedactionFilter())
        test_logger = logging.getLogger("tests.message_safety")
        test_logger.handlers = [handler]
        test_logger.propagate = False
        test_logger.setLevel(logging.ERROR)
        secret = "mongodb://user:password@db.example/test"

        reference = report_internal_error(
            test_logger,
            "request_ticket",
            RuntimeError(f"connection failed at {secret}"),
            source="https://t.me/+private-link",
        )

        logged = stream.getvalue()
        self.assertNotIn("user:password", logged)
        self.assertNotIn("private-link", logged)
        self.assertIn("[REDACTED]", logged)
        public = public_error_message(reference)
        self.assertIn(reference, public)
        self.assertNotIn("connection failed", public)

    def test_exception_tracebacks_are_redacted_by_process_filter(self):
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.addFilter(SecretRedactionFilter())
        test_logger = logging.getLogger("tests.message_traceback")
        test_logger.handlers = [handler]
        test_logger.propagate = False
        test_logger.setLevel(logging.ERROR)
        try:
            raise RuntimeError("token=super-secret-value")
        except RuntimeError:
            test_logger.exception("backend failure")
        self.assertNotIn("super-secret-value", stream.getvalue())
        self.assertIn("token=[REDACTED]", stream.getvalue())

    def test_audited_user_content_paths_use_html_and_no_raw_errors(self):
        request_source = (ROOT / "plugins" / "request.py").read_text(encoding="utf-8")
        filter_source = (ROOT / "plugins" / "filter.py").read_text(encoding="utf-8")
        group_source = (ROOT / "plugins" / "group_connect.py").read_text(encoding="utf-8")
        self.assertNotIn("Failed to send request: {e}", request_source)
        self.assertNotIn("Could not PM user (they might have blocked the bot). Error", request_source)
        self.assertIn("html_user_mention(user)", request_source)
        self.assertIn("parse_mode=ParseMode.HTML", request_source)
        self.assertIn("🎬 <code>{_html(query)}</code>", filter_source)
        self.assertIn("🎬 <code>{_html(query)}</code>", group_source)


if __name__ == "__main__":
    unittest.main()
