"""Process-wide redaction for credentials that may appear in exception text."""

import logging
import re


_SECRET_PATTERNS = (
    (
        re.compile(r"(?i)(mongodb(?:\+srv)?://)[^\s/@]+(?::[^\s/@]*)?@"),
        r"\1[REDACTED]@",
    ),
    (
        re.compile(
            r"(?i)(?:(?:https?://)?(?:t|telegram)\.me/(?:\+|joinchat/)|"
            r"tg://join\?invite=)[^\s\"'<>]+"
        ),
        "[REDACTED_PRIVATE_INVITE]",
    ),
    (re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b"), "[REDACTED_BOT_TOKEN]"),
    (
        re.compile(r"(?i)\b(token|api[_-]?key|password|secret)=([^\s&]+)"),
        r"\1=[REDACTED]",
    ),
)


def redact_log_secrets(value) -> str:
    redacted = str(value)
    for pattern, replacement in _SECRET_PATTERNS:
        redacted = pattern.sub(replacement, redacted)
    return redacted


class SecretRedactionFilter(logging.Filter):
    """Redact messages, arguments, and formatted exception tracebacks."""

    def filter(self, record):
        rendered = redact_log_secrets(record.getMessage())
        if record.exc_info:
            traceback_text = logging.Formatter().formatException(record.exc_info)
            rendered = f"{rendered}\n{redact_log_secrets(traceback_text)}"
            record.exc_info = None
            record.exc_text = None
        record.msg = rendered
        record.args = ()
        return True


def install_log_redaction():
    root = logging.getLogger()
    for handler in root.handlers:
        if not any(isinstance(item, SecretRedactionFilter) for item in handler.filters):
            handler.addFilter(SecretRedactionFilter())
