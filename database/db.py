import os
import re
import time
import json
import gzip
import asyncio
import logging
import secrets
import sqlite3
import tempfile
import hashlib
import ipaddress
from pathlib import Path
from datetime import datetime, timezone
from collections import OrderedDict
from contextlib import closing
from functools import lru_cache
from urllib.parse import parse_qsl, unquote
from bson.objectid import ObjectId
import certifi
from pymongo import AsyncMongoClient, InsertOne, ReturnDocument, UpdateOne
from pymongo.errors import AutoReconnect, BulkWriteError, ConnectionFailure, DuplicateKeyError
from dotenv import load_dotenv
from rapidfuzz import fuzz, process
from pyrogram.file_id import DOCUMENT_TYPES, FileId
from database.index_policy import (
    RequiredIndexError,
    ensure_required_compound_index,
    ensure_required_index,
    ensure_required_unique_index,
)
from database.shard_router import ShardRouter, is_capacity_error
from database.redis_client import redis_state, stable_cache_key
from plugins.duplicate_safety import stable_duplicate_key
from verification import VerificationResult, VerificationStatus

try:
    import dns  # noqa: F401 — dnspython; required for mongodb+srv:// URIs.
    # Not used directly: pymongo/Motor import it internally to resolve the
    # SRV/TXT records that mongodb+srv:// connection strings depend on.
    # Imported here only so a missing install fails loudly and immediately
    # with an actionable message, instead of surfacing later as a cryptic
    # pymongo.errors.ConfigurationError deep inside a connection attempt.
except ImportError as e:
    raise ImportError(
        "dnspython is required for mongodb+srv:// connection strings "
        "(pip install dnspython — see requirements.txt)."
    ) from e

load_dotenv()

logger = logging.getLogger(__name__)

_SEARCH_CATALOG_PATH = Path(__file__).resolve().parents[1] / "runtime" / "search_titles.txt.gz"
_DUPLICATE_SCAN_DIR = Path(__file__).resolve().parents[1] / "runtime" / "duplicate_scans"
_SEARCH_TITLE_CATALOG: tuple[str, ...] = ()
MAX_SEARCH_CATALOG_TITLES = min(250_000, max(10_000, int(os.getenv("MAX_SEARCH_CATALOG_TITLES", "100000"))))
MAX_DUPLICATE_GROUPS = 100
MAX_DUPLICATE_IDS_PER_GROUP = 1000
_DUPLICATE_HASH_BYTES = 12
_VERIFIED_CLEANUP_BATCH_SIZE = 2000
MAX_NOTIFICATION_OUTBOX_JOBS = 5000
MAX_REQUEST_MATCHES_PER_JOB = 100
MAX_CONFIG_BACKUP_KEYS = 50
MAX_CONFIG_BACKUP_DEPTH = 4
BROADCAST_LEASE_SECONDS = 360
MOVIE_MONGO_SELECTION_TIMEOUT_MS = max(
    2000, min(15000, int(os.getenv("MOVIE_MONGO_SELECTION_TIMEOUT_MS", "5000")))
)
MOVIE_MONGO_SOCKET_TIMEOUT_MS = max(
    5000, min(30000, int(os.getenv("MOVIE_MONGO_SOCKET_TIMEOUT_MS", "15000")))
)
MONGO_WAIT_QUEUE_TIMEOUT_MS = 5000
MONGO_MAX_CONNECTING = 10
SHARD_OPERATION_TIMEOUT_SECONDS = max(
    2.0, min(15.0, float(os.getenv("SHARD_OPERATION_TIMEOUT_SECONDS", "6")))
)
ANALYTICS_LANGUAGES = (
    "Malayalam",
    "Tamil",
    "Telugu",
    "Hindi",
    "English",
    "Kannada",
    "Dual Audio",
    "Multi Audio",
)


def language_tags_for_name(file_name: str) -> list[str]:
    """Return stable language labels stored at write time for analytics."""
    normalized = " ".join(re.findall(r"[a-z0-9]+", str(file_name).casefold()))
    padded = f" {normalized} "
    return [
        language
        for language in ANALYTICS_LANGUAGES
        if f" {' '.join(re.findall(r'[a-z0-9]+', language.casefold()))} " in padded
    ]

_RESTORABLE_CONFIG_TYPES = {
    "start_media": str,
    "welcome_text": str,
    "auto_delete_time": int,
    "maintenance_mode": bool,
    "maintenance_message": str,
    "file_caption_template": str,
    "update_channel": str,
    "main_group": str,
    "group_whitelist_mode": str,
    "req_fsub_interval_hours": int,
}
PRIVATE_INVITE_REDACTION = "[REDACTED_PRIVATE_INVITE]"
_PRIVATE_INVITE_RE = re.compile(
    r"(?i)(?:(?:https?://)?(?:t|telegram)\.me/(?:\+|joinchat/)|"
    r"tg://join\?invite=)[^\s\"'<>]+"
)


def redact_private_invites(value):
    """Recursively redact Telegram bearer invite URLs from export data."""
    if isinstance(value, str):
        return _PRIVATE_INVITE_RE.sub(PRIVATE_INVITE_REDACTION, value)
    if isinstance(value, dict):
        return {key: redact_private_invites(child) for key, child in value.items()}
    if isinstance(value, list):
        return [redact_private_invites(child) for child in value]
    if isinstance(value, tuple):
        return tuple(redact_private_invites(child) for child in value)
    return value


def validate_config_restore(data: object) -> dict:
    """Validate an untrusted config backup and return its restorable subset."""
    if type(data) is not dict:
        raise ValueError("backup root must be a JSON object")

    key_count = 0

    def _check_shape(value, depth=0):
        nonlocal key_count
        if depth > MAX_CONFIG_BACKUP_DEPTH:
            raise ValueError("backup nesting is too deep")
        if isinstance(value, dict):
            key_count += len(value)
            if key_count > MAX_CONFIG_BACKUP_KEYS:
                raise ValueError("backup contains too many keys")
            for key, child in value.items():
                if not isinstance(key, str):
                    raise ValueError("backup keys must be strings")
                _check_shape(child, depth + 1)
        elif isinstance(value, list):
            if len(value) > MAX_CONFIG_BACKUP_KEYS:
                raise ValueError("backup array is too large")
            for child in value:
                _check_shape(child, depth + 1)

    _check_shape(data)
    safe_data = {}
    for key, expected_type in _RESTORABLE_CONFIG_TYPES.items():
        if key not in data:
            continue
        value = data[key]
        if type(value) is not expected_type:
            raise ValueError(f"{key} has an invalid type")
        if isinstance(value, str) and PRIVATE_INVITE_REDACTION in value:
            continue
        safe_data[key] = value

    if "auto_delete_time" in safe_data and not 60 <= safe_data["auto_delete_time"] <= 86400:
        raise ValueError("auto_delete_time must be between 60 and 86400")
    if "req_fsub_interval_hours" in safe_data and not 1 <= safe_data["req_fsub_interval_hours"] <= 720:
        raise ValueError("req_fsub_interval_hours must be between 1 and 720")
    if safe_data.get("group_whitelist_mode") not in (None, "whitelist", "blacklist"):
        raise ValueError("group_whitelist_mode must be whitelist or blacklist")

    string_limits = {
        "start_media": 512,
        "welcome_text": 4096,
        "maintenance_message": 4096,
        "file_caption_template": 4096,
        "update_channel": 512,
        "main_group": 512,
        "group_whitelist_mode": 16,
    }
    for key, limit in string_limits.items():
        if key in safe_data and len(safe_data[key]) > limit:
            raise ValueError(f"{key} exceeds its {limit}-character limit")
    return safe_data


class InsecureMongoURIError(ValueError):
    """Raised when a non-loopback MongoDB endpoint does not enforce TLS."""


def _mongo_uri_hosts(uri: str) -> list[str]:
    lower_uri = uri.casefold()
    if lower_uri.startswith("mongodb+srv://"):
        remainder = uri[len("mongodb+srv://") :]
    elif lower_uri.startswith("mongodb://"):
        remainder = uri[len("mongodb://") :]
    else:
        raise ValueError("MongoDB URI must use mongodb:// or mongodb+srv://")
    authority = remainder.split("/", 1)[0].split("?", 1)[0]
    hosts = authority.rsplit("@", 1)[-1]
    parsed_hosts = []
    for entry in hosts.split(","):
        entry = unquote(entry.strip())
        if entry.startswith("[") and "]" in entry:
            host = entry[1 : entry.index("]")]
        elif entry.count(":") == 1:
            host = entry.rsplit(":", 1)[0]
        else:
            host = entry
        if host:
            parsed_hosts.append(host.casefold())
    if not parsed_hosts:
        raise ValueError("MongoDB URI contains no host")
    return parsed_hosts


def _is_loopback_mongo_host(host: str) -> bool:
    if host == "localhost" or host.endswith(".localhost") or host.endswith(".sock"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def mongo_tls_options(
    uri: str,
    *,
    allow_insecure_development: bool = False,
    ca_file: str | None = None,
) -> dict:
    """Enforce certificate-validated TLS for every non-loopback Mongo URI."""
    uri_lower = uri.casefold()
    hosts = _mongo_uri_hosts(uri)
    remote = not all(_is_loopback_mongo_host(host) for host in hosts)
    query = uri.split("?", 1)[1] if "?" in uri else ""
    options = {key.casefold(): value.casefold() for key, value in parse_qsl(query, keep_blank_values=True)}
    tls_value = options.get("tls", options.get("ssl"))
    tls_enabled = (
        uri_lower.startswith("mongodb+srv://") if tls_value is None else tls_value in {"1", "true", "yes"}
    )
    certificate_validation_disabled = any(
        options.get(key) in {"1", "true", "yes"} for key in ("tlsallowinvalidcertificates", "tlsinsecure")
    )

    if remote and (not tls_enabled or certificate_validation_disabled):
        if allow_insecure_development:
            logger.critical("Development override permits insecure remote MongoDB transport")
            return {}
        reason = (
            "certificate validation is disabled" if certificate_validation_disabled else "TLS is not enabled"
        )
        raise InsecureMongoURIError(
            f"Remote MongoDB URI rejected because {reason}; use mongodb+srv:// "
            "or add tls=true with certificate validation"
        )

    if not tls_enabled:
        return {}
    resolved_ca = ca_file or certifi.where()
    if not Path(resolved_ca).is_file():
        raise ValueError(f"MongoDB TLS CA file does not exist: {resolved_ca}")
    return {"tls": True, "tlsCAFile": resolved_ca}


class AllClustersFullError(Exception):
    """Raised by save_files_bulk() when every cluster is at/above its 450MB
    safety margin and none of the batch's files could be stored anywhere.
    Distinguishes "no cluster had room" from a normal (0 saved, N
    duplicates) result — without this, callers can't tell a full database
    apart from a batch that was simply all duplicates."""

    def __init__(self, unsaved_count: int, duplicates: int):
        self.unsaved_count = unsaved_count
        self.duplicates = duplicates
        super().__init__(f"All clusters full — {unsaved_count} file(s) could not be stored")


_config_cache = None
_config_cache_ts = 0.0
_last_valid_config = None
_CONFIG_TTL = 60

# Fixed per-user re-verification window for the Two-Stage Verification gate
# (database.get_two_stage_due / mark_two_stage_verified below). Deliberately
# a hardcoded constant rather than an admin-configurable field like
# req_fsub_interval_hours — the product requirement is specifically a fixed
# 30-minute window, not a tunable one.
TWO_STAGE_VERIFY_INTERVAL = 1800


# Search's separator class ([\s.+-_]) doesn't cover brackets, @tags, hashes,
# etc. — a raw filename like "[TamilBlasters.cc]_Balan_(2024)_1080p.mkv"
# can silently fail to match even a correct query because "]" or "(" sits
# where a recognized separator was expected. Normalizing every stored
# file_name to plain space-separated words (same idea as DreamXBotz's
# save_file) removes that whole class of missed matches at the source,
# instead of trying to special-case every possible symbol in the regex.
_JUNK_TAG_RE = re.compile(r"http\S+|www\.\S+|@\w+", re.IGNORECASE)
_SYMBOL_RE = re.compile(r"[_\-\.#+$%^&*()!~`,;:\"'?/<>\[\]{}=|\\]")
_WS_COLLAPSE_RE = re.compile(r"\s+")
_SEARCH_EXTENSION_TOKENS = {"mkv", "mp4", "avi", "mov", "zip", "srt"}
_OPTIONAL_SEARCH_TOKENS = {
    "malayalam",
    "tamil",
    "telugu",
    "hindi",
    "english",
    "kannada",
    "dual",
    "multi",
    "audio",
    "dubbed",
    "sub",
    "subs",
    "esub",
    "esubs",
    "2160p",
    "1080p",
    "720p",
    "480p",
    "4k",
    "uhd",
    "hdrip",
    "webrip",
    "web",
    "dl",
    "webdl",
    "bluray",
    "predvd",
    "cam",
    "hevc",
    "x265",
    "x264",
}
_SEARCH_SERIES_MARKER_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:S(?:EASON)?\s*\d{1,2}|"
    r"E(?:P(?:ISODE)?)?\s*\d{1,3})",
    re.IGNORECASE,
)
_SEARCH_BRACKET_GROUP_RE = re.compile(r"[\[({][^\])}]{1,80}[\])}]")
_STRONG_METADATA_RE = re.compile(
    r"^(?:2160p|1080p|720p|480p|360p|4k|uhd|hdrip|webrip|webdl|"
    r"bluray|predvd|cam|hevc|avc|x26[45]|h26[45]|aac\d*|ddp?\d*|"
    r"eac3|ac3|10bit|amzn|nf|hq|yts|mkv|mp4|avi|mov)$",
    re.IGNORECASE,
)


def normalize_file_name(name: str) -> str:
    """Strip promotional junk (URLs, @mentions) and collapse every
    separator/symbol character to a single space, so every stored
    file_name is consistently space-separated. Falls back to a stripped
    copy of the original if cleaning would otherwise empty it out."""
    if not name:
        return name
    cleaned = _JUNK_TAG_RE.sub("", name)
    cleaned = _SYMBOL_RE.sub(" ", cleaned)
    cleaned = _WS_COLLAPSE_RE.sub(" ", cleaned).strip()
    return cleaned or name.strip()


def search_tokens_for_name(name: str) -> list[str]:
    """Create a bounded, stable token array suitable for a multikey index."""
    tokens = (token[:64] for token in normalize_file_name(name).casefold().split() if token)
    return list(dict.fromkeys(tokens))[:32]


@lru_cache(maxsize=16384)
def normalized_search_identity(name: str) -> str:
    """Stable, compact identity used for ranking and duplicate suppression.

    It is derived from the existing normalized filename, so legacy records
    need no migration and no second large indexed field is stored in Atlas.
    """
    tokens = normalize_file_name(name).casefold().split()
    while tokens and tokens[-1] in _SEARCH_EXTENSION_TOKENS:
        tokens.pop()
    return " ".join(tokens)


def deduplicate_search_results(files: list) -> list:
    """Remove identical file IDs and identical name/size records stably."""
    unique = []
    seen_file_ids = set()
    seen_content = set()
    for file_doc in files:
        file_id = file_doc.get("file_id")
        identity = normalized_search_identity(file_doc.get("file_name", ""))
        signature = (identity, int(file_doc.get("file_size", 0) or 0))
        if file_id and file_id in seen_file_ids:
            continue
        if identity and signature in seen_content:
            continue
        if file_id:
            seen_file_ids.add(file_id)
        if identity:
            seen_content.add(signature)
        unique.append(file_doc)
    return unique


@lru_cache(maxsize=4096)
def compile_regex(pattern: str):
    return re.compile(pattern, re.IGNORECASE)


def _reference_search_pattern(words: list[str]) -> str:
    """DreamXBotz-style ordered-word matching with safe boundaries."""
    separator = r"[\s\.\+\-_]"
    if len(words) > 1:
        return (
            r"(?:^|"
            + separator
            + r")"
            + (r".*" + separator).join(re.escape(word) for word in words)
            + r"(?:$|"
            + separator
            + r")"
        )
    return r"(\b|[\.\+\-_])" + re.escape(words[0]) + r"(\b|[\.\+\-_])"


def _is_optional_search_token(token: str) -> bool:
    return bool(re.fullmatch(r"(?:19|20)\d{2}", token)) or (token.casefold() in _OPTIONAL_SEARCH_TOKENS)


def primary_search_identity(name: str) -> str:
    """Extract the title users see, excluding hidden episode-title text."""
    without_tags = _SEARCH_BRACKET_GROUP_RE.sub(" ", name or "")
    normalized = normalize_file_name(without_tags)
    tokens = normalized.split()
    while tokens and tokens[-1].casefold() in _SEARCH_EXTENSION_TOKENS:
        tokens.pop()
    if not tokens:
        return ""

    normalized = " ".join(tokens)
    series_marker = _SEARCH_SERIES_MARKER_RE.search(normalized)
    if series_marker:
        return normalized[: series_marker.start()].strip().casefold()

    year_positions = [index for index, token in enumerate(tokens) if re.fullmatch(r"(?:19|20)\d{2}", token)]
    if year_positions:
        # For numeric titles such as ``1917 2019``, the final year is release
        # metadata while the earlier number remains part of the title.
        cut_at = year_positions[-1]
        title_tokens = tokens[:cut_at]
        if title_tokens:
            return " ".join(title_tokens).casefold()

    for index, token in enumerate(tokens[1:], 1):
        if _STRONG_METADATA_RE.fullmatch(token):
            return " ".join(tokens[:index]).casefold()
    return " ".join(tokens).casefold()


def _fuzzy_query_identity(query: str) -> str:
    tokens = normalized_search_identity(query).split()
    title_tokens = [token for token in tokens if not _is_optional_search_token(token)]
    return " ".join(title_tokens or tokens)


def rank_search_results(query: str, files: list, max_results: int) -> list:
    """Keep candidates whose visible title fuzzily matches the query.

    Scoring a bounded Mongo result set makes typo tolerance inexpensive while
    ensuring a phrase found only in an episode title cannot outrank a movie
    with that actual title.
    """
    query_identity = _fuzzy_query_identity(query)
    if not query_identity or not files:
        return []

    scored = []
    for position, file_doc in enumerate(files):
        identity = primary_search_identity(file_doc.get("file_name", ""))
        if not identity:
            continue
        direct_score = fuzz.ratio(query_identity, identity)
        weighted_score = fuzz.WRatio(query_identity, identity)
        score = (0.7 * direct_score) + (0.3 * weighted_score)
        if identity == query_identity:
            score = 100.0
        elif identity.startswith(query_identity + " "):
            score = max(score, 96.0)
        elif len(query_identity.split()) == len(identity.split()):
            # A user may reverse two title words. Apply the order-independent
            # score only when neither side contains extra words.
            score = max(
                score,
                fuzz.token_sort_ratio(query_identity, identity) * 0.9,
            )
        scored.append((score, position, file_doc))

    if not scored:
        return []
    best_score = max(item[0] for item in scored)
    if best_score < 75.0:
        return []
    cutoff = max(75.0, best_score - 10.0)
    ranked = sorted(
        (item for item in scored if item[0] >= cutoff),
        key=lambda item: (-item[0], item[1]),
    )
    return [item[2] for item in ranked[:max_results]]


def load_search_catalog() -> int:
    """Load the compact, local unique-title catalog when it exists."""
    global _SEARCH_TITLE_CATALOG
    if not _SEARCH_CATALOG_PATH.exists():
        _SEARCH_TITLE_CATALOG = ()
        return 0
    try:
        with gzip.open(_SEARCH_CATALOG_PATH, "rt", encoding="utf-8") as handle:
            titles = []
            for line in handle:
                title = line.strip()
                if title:
                    titles.append(title)
                if len(titles) >= MAX_SEARCH_CATALOG_TITLES:
                    break
            _SEARCH_TITLE_CATALOG = tuple(titles)
    except (OSError, UnicodeError) as exc:
        logger.warning("Could not load fuzzy-search title catalog: %s", exc)
        _SEARCH_TITLE_CATALOG = ()
    return len(_SEARCH_TITLE_CATALOG)


def suggest_search_titles(query: str, limit: int = 3, choices=None) -> list[str]:
    """Return the closest known visible titles using RapidFuzz."""
    query_identity = _fuzzy_query_identity(query)
    catalog = _SEARCH_TITLE_CATALOG if choices is None else choices
    if not query_identity or not catalog:
        return []
    # WRatio intentionally rewards a short substring inside a long string;
    # that is useful for release filenames but harmful for a title catalog
    # (``avsham`` could rank a one-letter title highly). Plain edit similarity
    # handles spelling, while token-sort similarity handles reversed words.
    matches = process.extract(
        query_identity,
        catalog,
        scorer=fuzz.ratio,
        score_cutoff=72.0,
        limit=limit,
    )
    reordered = process.extract(
        query_identity,
        catalog,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=80.0,
        limit=limit,
    )
    best = {}
    for title, score, _ in matches + reordered:
        best[title] = max(score, best.get(title, 0.0))
    return [
        title for title, _ in sorted(best.items(), key=lambda item: (-item[1], len(item[0]), item[0]))[:limit]
    ]


def _catalog_identities(file_names: list[str]) -> list[str]:
    return [primary_search_identity(name) for name in file_names]


def deduplicate_file_batch(files_list):
    """Keep the first Telegram/content identity and count rejected rows."""
    unique_files = []
    seen_file_ids = set()
    seen_unique_ids = set()
    seen_telegram_ids = set()
    duplicate_count = 0
    for file_doc in files_list:
        fid = file_doc.get("file_id")
        unique_id = file_doc.get("file_unique_id") or ""
        telegram_identity = telegram_file_identity(fid)
        if (
            not fid
            or fid in seen_file_ids
            or (unique_id and unique_id in seen_unique_ids)
            or (telegram_identity and telegram_identity in seen_telegram_ids)
        ):
            duplicate_count += 1
            continue
        seen_file_ids.add(fid)
        if unique_id:
            seen_unique_ids.add(unique_id)
        if telegram_identity:
            seen_telegram_ids.add(telegram_identity)
        unique_files.append(file_doc)
    return unique_files, duplicate_count


# Precompiled once at import time — find_duplicate_files()._normalize() runs
# once per document across every file in every cluster during a full scan
# (potentially hundreds of thousands of calls), so these are hoisted out of
# the per-call closure instead of being rebuilt from pattern strings each time.
_DUPE_EXT_RE = re.compile(r"\.(mkv|mp4|avi|mov|zip|srt)$", re.IGNORECASE)
_DUPE_JUNK_RE = re.compile(
    r"\b(1080p|720p|480p|360p|4k|2160p|hdrip|hd.rip|webrip|web-dl|webdl|"
    r"bluray|predvd|cam|hdcam|tsrip|dvdrip|x264|x265|hevc|aac|esub|hsub|"
    r"10bit|hq|nf|amzn|dual.audio|multi.audio|malayalam|tamil|telugu|hindi|"
    r"english|kannada|1xbet|tamilblasters|tamilmv|moviezwap)\b",
    re.IGNORECASE,
)
_DUPE_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
_DUPE_BRACKET_RE = re.compile(r"[\[\(].*?[\]\)]")
_DUPE_SEP_RE = re.compile(r"[._\-]")
_DUPE_WS_RE = re.compile(r"\s+")
_PROBABLE_DUPE_EXT_RE = re.compile(r"\.(mkv|mp4|avi|mov|m4v|webm|zip|srt)$", re.IGNORECASE)


def normalize_duplicate_name(name: str) -> str:
    """Legacy broad title normalizer retained for search compatibility only."""
    if not name:
        return ""
    normalized = _DUPE_EXT_RE.sub("", name)
    normalized = _DUPE_JUNK_RE.sub("", normalized)
    normalized = _DUPE_YEAR_RE.sub("", normalized)
    normalized = _DUPE_BRACKET_RE.sub("", normalized)
    normalized = _DUPE_SEP_RE.sub(" ", normalized)
    return _DUPE_WS_RE.sub(" ", normalized).strip().casefold()


def probable_duplicate_name(name: str) -> str:
    """Normalize spelling separators without removing release metadata.

    Language, year, quality, codec, season and episode tokens remain part of
    the identity. This is intentionally conservative: related releases are
    useful variants, not duplicates.
    """
    normalized = _PROBABLE_DUPE_EXT_RE.sub("", str(name or ""))
    normalized = _DUPE_SEP_RE.sub(" ", normalized)
    return _DUPE_WS_RE.sub(" ", normalized).strip().casefold()


def telegram_file_identity(file_id: str) -> str:
    """Derive Telegram's stable document identity from a reusable file ID.

    For documents, videos and audio, Telegram's own ``file_unique_id`` is
    based on this media ID. File references and access hashes may change when
    the same media is encountered again, so comparing the complete file_id
    incorrectly treats those exact copies as different files.
    """
    if not file_id:
        return ""
    try:
        decoded = FileId.decode(file_id)
    except Exception:
        # Legacy/imported rows may contain Bot API IDs from older layouts or
        # malformed text. Falling back keeps the scan stable and conservative.
        return ""
    if decoded.file_type not in DOCUMENT_TYPES or decoded.media_id is None:
        return ""
    return f"telegram-document:{int(decoded.media_id)}"


def registry_identity_document(file_id: str, file_unique_id: str = "") -> dict:
    """Build every stable identity protected by the central registry."""
    document = {"file_id": file_id, "location_pending": True}
    if file_unique_id:
        document["file_unique_id"] = file_unique_id
    telegram_identity = telegram_file_identity(file_id)
    if telegram_identity:
        document["telegram_identity"] = telegram_identity
    return document


def _duplicate_fingerprints(document: dict) -> tuple[bytes | None, bytes | None]:
    """Return compact exact/probable identities without retaining user data."""
    file_id = str(document.get("file_id", "") or "")
    unique_id = str(document.get("file_unique_id", "") or "")
    telegram_identity = telegram_file_identity(file_id)
    exact_value = telegram_identity or (
        f"u:{unique_id}" if unique_id else (f"f:{file_id}" if file_id else "")
    )
    if not exact_value:
        return None, None

    exact_key = hashlib.blake2b(
        exact_value.encode("utf-8", "surrogatepass"),
        digest_size=_DUPLICATE_HASH_BYTES,
        person=b"mccx-exact-v1",
    ).digest()
    name = probable_duplicate_name(str(document.get("file_name", "")))
    file_size = int(document.get("file_size", 0) or 0)
    if not name or file_size <= 0:
        return exact_key, None
    mime_type = str(document.get("mime_type", "") or "").split(";", 1)[0].casefold()
    probable_value = f"{name}\x1f{file_size}\x1f{mime_type}"
    probable_key = hashlib.blake2b(
        probable_value.encode("utf-8", "surrogatepass"),
        digest_size=_DUPLICATE_HASH_BYTES,
        person=b"mccx-prob-v1",
    ).digest()
    return exact_key, probable_key


def _configure_duplicate_spool(connection: sqlite3.Connection):
    """Tune a disposable report database for low disk usage."""
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute("PRAGMA temp_store=MEMORY")
    connection.execute("PRAGMA cache_size=-4096")


def _initialize_duplicate_spool(database_path: str, mode: str = "both"):
    if mode not in {"exact", "probable", "both"}:
        raise ValueError(f"Unknown duplicate spool mode: {mode}")
    with closing(sqlite3.connect(database_path, timeout=30)) as connection:
        _configure_duplicate_spool(connection)
        with connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS scan_meta ("
                "key TEXT PRIMARY KEY, value INTEGER NOT NULL) WITHOUT ROWID"
            )
            connection.execute("INSERT OR IGNORE INTO scan_meta (key, value) VALUES ('scanned', 0)")
            if mode in {"exact", "both"}:
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS exact_counts ("
                    "key BLOB PRIMARY KEY, copies INTEGER NOT NULL) WITHOUT ROWID"
                )
            if mode in {"probable", "both"}:
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS probable_members ("
                    "group_key BLOB NOT NULL, exact_key BLOB NOT NULL, "
                    "PRIMARY KEY (group_key, exact_key)) WITHOUT ROWID"
                )


def _spool_duplicate_batch(
    database_path: str,
    documents: list[dict],
    cluster_index: int = 1,
    mode: str = "both",
):
    """Aggregate one bounded batch using fingerprints, never raw file data."""
    del cluster_index  # Kept in the signature for compatibility with older callers.
    if mode not in {"exact", "probable", "both"}:
        raise ValueError(f"Unknown duplicate spool mode: {mode}")

    exact_counts: dict[bytes, int] = {}
    probable_members: set[tuple[bytes, bytes]] = set()
    for document in documents:
        exact_key, probable_key = _duplicate_fingerprints(document)
        if exact_key is not None and mode in {"exact", "both"}:
            exact_counts[exact_key] = exact_counts.get(exact_key, 0) + 1
        if exact_key is not None and probable_key is not None and mode in {"probable", "both"}:
            probable_members.add((probable_key, exact_key))

    with closing(sqlite3.connect(database_path, timeout=30)) as connection:
        _configure_duplicate_spool(connection)
        with connection:
            if exact_counts:
                connection.executemany(
                    "INSERT INTO exact_counts (key, copies) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET copies = copies + excluded.copies",
                    exact_counts.items(),
                )
            if probable_members:
                connection.executemany(
                    "INSERT OR IGNORE INTO probable_members (group_key, exact_key) VALUES (?, ?)",
                    probable_members,
                )
            connection.execute(
                "UPDATE scan_meta SET value = value + ? WHERE key = 'scanned'",
                (len(documents),),
            )


def _sqlite_table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
        ).fetchone()
        is not None
    )


def _initialize_verified_cleanup_spool(database_path: str):
    """Create the compact survivor set used by strict duplicate cleanup."""
    with closing(sqlite3.connect(database_path, timeout=30)) as connection:
        _configure_duplicate_spool(connection)
        with connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS exact_survivors (key BLOB PRIMARY KEY) WITHOUT ROWID"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS pending_registry (file_id TEXT PRIMARY KEY) WITHOUT ROWID"
            )


def _classify_verified_duplicate_batch(database_path: str, documents: list[dict]) -> list[dict]:
    """Return rows whose exact Telegram identity already has a survivor."""
    duplicates = []
    with closing(sqlite3.connect(database_path, timeout=30)) as connection:
        _configure_duplicate_spool(connection)
        with connection:
            for document in documents:
                exact_key, _probable_key = _duplicate_fingerprints(document)
                if exact_key is None:
                    continue
                inserted = connection.execute(
                    "INSERT OR IGNORE INTO exact_survivors (key) VALUES (?)",
                    (exact_key,),
                ).rowcount
                if not inserted:
                    duplicates.append(document)
    return duplicates


def _stage_cleanup_registry_ids(database_path: str, file_ids: list[str]):
    rows = [(file_id,) for file_id in dict.fromkeys(file_ids) if file_id]
    if not rows:
        return
    with closing(sqlite3.connect(database_path, timeout=30)) as connection:
        _configure_duplicate_spool(connection)
        with connection:
            connection.executemany(
                "INSERT OR IGNORE INTO pending_registry (file_id) VALUES (?)",
                rows,
            )


def _read_cleanup_registry_ids(database_path: str, limit: int = 500) -> list[str]:
    with closing(sqlite3.connect(database_path, timeout=30)) as connection:
        _configure_duplicate_spool(connection)
        if not _sqlite_table_exists(connection, "pending_registry"):
            return []
        return [
            str(row[0])
            for row in connection.execute(
                "SELECT file_id FROM pending_registry LIMIT ?", (int(limit),)
            ).fetchall()
        ]


def _clear_cleanup_registry_ids(database_path: str, file_ids: list[str]):
    rows = [(file_id,) for file_id in dict.fromkeys(file_ids) if file_id]
    if not rows:
        return
    with closing(sqlite3.connect(database_path, timeout=30)) as connection:
        _configure_duplicate_spool(connection)
        with connection:
            connection.executemany("DELETE FROM pending_registry WHERE file_id = ?", rows)


def _read_duplicate_report(database_path: str) -> dict:
    """Read a bounded report from compact aggregate tables."""
    results = []
    exact_summary = (0, 0)
    probable_summary = (0, 0)
    with closing(sqlite3.connect(database_path, timeout=30)) as connection:
        _configure_duplicate_spool(connection)
        scanned_row = connection.execute("SELECT value FROM scan_meta WHERE key = 'scanned'").fetchone()
        scanned = int(scanned_row[0]) if scanned_row else 0

        if _sqlite_table_exists(connection, "exact_counts"):
            exact_summary = connection.execute(
                "SELECT COUNT(*), COALESCE(SUM(copies - 1), 0) FROM exact_counts WHERE copies > 1"
            ).fetchone()
            exact_groups = connection.execute(
                "SELECT key, copies FROM exact_counts WHERE copies > 1 ORDER BY copies DESC, key LIMIT ?",
                (MAX_DUPLICATE_GROUPS,),
            ).fetchall()
            for identity_bytes, count in exact_groups:
                identity = bytes(identity_bytes).hex()
                results.append(
                    {
                        "key": stable_duplicate_key("exact", identity),
                        "identity": identity,
                        "name": f"Exact file {identity[:8]}",
                        "count": int(count),
                        "ids": [],
                        "type": "exact",
                        "truncated": False,
                    }
                )

        remaining = max(0, MAX_DUPLICATE_GROUPS - len(results))
        if _sqlite_table_exists(connection, "probable_members"):
            probable_summary = connection.execute(
                "SELECT COUNT(*), COALESCE(SUM(members - 1), 0) FROM ("
                "SELECT COUNT(*) AS members FROM probable_members "
                "GROUP BY group_key HAVING COUNT(*) > 1)"
            ).fetchone()
            if remaining:
                probable_groups = connection.execute(
                    "SELECT group_key, COUNT(*) AS members FROM probable_members "
                    "GROUP BY group_key HAVING COUNT(*) > 1 "
                    "ORDER BY members DESC, group_key LIMIT ?",
                    (remaining,),
                ).fetchall()
                for identity_bytes, count in probable_groups:
                    identity = bytes(identity_bytes).hex()
                    results.append(
                        {
                            "key": stable_duplicate_key("probable", identity),
                            "identity": identity,
                            "name": f"Possible match {identity[:8]}",
                            "count": int(count),
                            "ids": [],
                            "type": "probable",
                            "truncated": False,
                        }
                    )

    return {
        "summary": {
            "scanned": scanned,
            "exact_groups": int(exact_summary[0]),
            "exact_extras": int(exact_summary[1]),
            "probable_groups": int(probable_summary[0]),
            "probable_matches": int(probable_summary[1]),
            "shown_groups": len(results),
            "report_only": True,
            "storage": "compact_fingerprints",
        },
        "groups": results,
    }


def _read_duplicate_groups(database_path: str) -> list[dict]:
    """Compatibility wrapper for callers that only need the bounded groups."""
    return _read_duplicate_report(database_path)["groups"]


class _SearchCache:
    """In-process, memory-bounded TTL cache for short-lived search-pagination
    sessions. Replaces the old MongoDB-backed search_cache collection —
    sessions auto-expire within minutes and don't need to survive a restart
    or be shared across processes (the bot already enforces single-instance
    execution via a flock lock), so externalizing them to Mongo only cost a
    network round-trip on every pagination click and file-send tap."""

    def __init__(self, maxsize=2000, default_ttl=600):
        self.maxsize = maxsize
        self.default_ttl = default_ttl
        self._data = OrderedDict()  # session_id -> (inserted_at, payload)
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def set(self, session_id, payload):
        self._data.pop(session_id, None)
        if len(self._data) >= self.maxsize:
            self._data.popitem(last=False)  # evict least-recently-inserted
            self.evictions += 1
        self._data[session_id] = (time.time(), payload)

    def get(self, session_id):
        entry = self._data.get(session_id)
        if not entry:
            self.misses += 1
            return None
        inserted_at, payload = entry
        if time.time() - inserted_at > self.default_ttl:
            del self._data[session_id]
            self.misses += 1
            return None
        self.hits += 1
        return payload

    def purge(self, older_than_seconds):
        cutoff = time.time() - older_than_seconds
        expired = [k for k, (inserted_at, _) in self._data.items() if inserted_at < cutoff]
        for k in expired:
            del self._data[k]

    def clear(self):
        self._data.clear()

    def snapshot(self):
        return {
            "size": len(self._data),
            "maxsize": self.maxsize,
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
        }


class Database:
    def __init__(self):
        self.uris = [
            os.getenv("DATABASE_URI"),
            os.getenv("DATABASE_URI_2"),
            os.getenv("DATABASE_URI_3"),
            os.getenv("DATABASE_URI_4"),
            os.getenv("DATABASE_URI_5"),
        ]

        self.clients = []
        self.dbs = []
        self.file_cols = []
        self.operations_client = None
        self.operations_db = None
        self.operations_uri = os.getenv("OPERATIONS_DATABASE_URI") or None
        allow_insecure_mongo = os.getenv("ALLOW_INSECURE_MONGODB_FOR_DEVELOPMENT", "false").casefold() in {
            "1",
            "true",
            "yes",
        }
        mongo_ca_file = os.getenv("MONGODB_TLS_CA_FILE") or None

        for i, uri in enumerate(self.uris):
            if uri:
                tls_options = mongo_tls_options(
                    uri,
                    allow_insecure_development=allow_insecure_mongo,
                    ca_file=mongo_ca_file,
                )
                try:
                    client = AsyncMongoClient(
                        uri,
                        serverSelectionTimeoutMS=MOVIE_MONGO_SELECTION_TIMEOUT_MS,
                        connectTimeoutMS=MOVIE_MONGO_SELECTION_TIMEOUT_MS,
                        socketTimeoutMS=MOVIE_MONGO_SOCKET_TIMEOUT_MS,
                        retryWrites=True,
                        retryReads=True,
                        # Bounded explicitly rather than left at the driver default
                        # (100) — up to 5 clusters x asyncio.gather fan-outs on
                        # every search means an unbounded pool could push total
                        # concurrent connections toward Atlas's per-tier ceiling
                        # (500 on M0) under heavy load. minPoolSize=0 avoids
                        # holding idle connections open on a quiet bot.
                        maxPoolSize=50,
                        minPoolSize=0,
                        waitQueueTimeoutMS=MONGO_WAIT_QUEUE_TIMEOUT_MS,
                        maxConnecting=MONGO_MAX_CONNECTING,
                        **tls_options,
                    )
                    self.clients.append(client)
                    db_instance = client[f"MCCxBot_Cluster_{i + 1}"]
                    self.dbs.append(db_instance)
                    self.file_cols.append(db_instance["movies"])
                except Exception as e:
                    logger.error(f"Cluster {i + 1} init failed: {e}")

        self.users_col = None
        self.banned_col = None
        self.config_col = None
        self.indexer_col = None
        self.index_failures_col = None
        self.rate_limits_col = None
        self.action_leases_col = None
        self.announcement_col = None
        self.registry_col = None
        self.deletion_col = None
        self.deletion_dead_letter_col = None
        self.broadcast_col = None
        self.groups_col = None
        self.verification_cache_col = None
        self.analytics_col = None
        self.main_db = None
        self.legacy_main_db = None
        self.legacy_operations_db = None
        self._db_size_cache = {}  # id(db_instance) -> (fetched_at, size_mb)
        self.shard_router = ShardRouter(len(self.dbs))
        self._file_count_cache = (0.0, 0)
        self._cluster_file_count_cache = {}
        self._language_count_cache = (0.0, {})
        self._operations_stats_cache = {"users": 0, "banned": 0, "groups": 0}
        self._catalog_building = False
        self._catalog_worker_semaphore = asyncio.Semaphore(1)
        self._fuzzy_worker_semaphore = asyncio.Semaphore(2)
        self._duplicate_scan_lock = asyncio.Lock()
        catalog_size = load_search_catalog()
        if catalog_size:
            logger.info("✅ Loaded fuzzy-search catalog: %s titles", catalog_size)

        if self.dbs:
            # Movie shards retain their original numbered databases. The
            # operational control plane is intentionally never placed on a
            # movie shard: a full catalogue shard must not disable sessions,
            # registry writes, rate limits, or durable jobs.
            self.legacy_main_db = self.dbs[0]
            self.legacy_operations_db = self.dbs[1] if len(self.dbs) > 1 else self.legacy_main_db
            _ops_db = None
            if self.operations_uri:
                tls_options = mongo_tls_options(
                    self.operations_uri,
                    allow_insecure_development=allow_insecure_mongo,
                    ca_file=mongo_ca_file,
                )
                self.operations_client = AsyncMongoClient(
                    self.operations_uri,
                    serverSelectionTimeoutMS=30000,
                    connectTimeoutMS=30000,
                    socketTimeoutMS=30000,
                    retryWrites=True,
                    retryReads=True,
                    maxPoolSize=50,
                    minPoolSize=0,
                    waitQueueTimeoutMS=MONGO_WAIT_QUEUE_TIMEOUT_MS,
                    maxConnecting=MONGO_MAX_CONNECTING,
                    **tls_options,
                )
                self.clients.append(self.operations_client)
                self.operations_db = self.operations_client["MCCxBot_Operations"]
                _ops_db = self.operations_db
            self.main_db = _ops_db
            if _ops_db is None:
                return
            self.users_col = _ops_db["users"]
            self.banned_col = _ops_db["banned_users"]
            self.config_col = _ops_db["bot_config"]
            self.indexer_col = _ops_db["indexer_tasks"]
            self.index_failures_col = _ops_db["index_failures"]
            self.rate_limits_col = _ops_db["rate_limits"]
            self.action_leases_col = _ops_db["action_leases"]
            self.announcement_col = _ops_db["announcement_outbox"]
            # Centralized cross-cluster identity registry — single source of
            # truth for file_id uniqueness across all sharded clusters, since
            # the per-cluster unique index on `movies.file_id` only protects
            # within one cluster's collection.
            # Derived/high-churn operational data belongs on the operations
            # database.  Keeping it off the primary movie/config cluster lets
            # an installation whose first Atlas M0 cluster is read-only at its
            # quota continue to register files and schedule message cleanup.
            self.registry_col = _ops_db["file_registry"]
            self.deletion_col = _ops_db["scheduled_deletions"]
            self.deletion_dead_letter_col = _ops_db["scheduled_deletion_dead_letters"]
            self.broadcast_col = _ops_db["broadcast_jobs"]
            self.groups_col = _ops_db["connected_groups"]
            self.verification_cache_col = _ops_db["verification_cache"]
            self.analytics_col = _ops_db["analytics_counters"]

    async def migrate_legacy_control_data(self):
        """Copy the small writable control plane off legacy cluster 1 once.

        Movie collections are deliberately excluded. Existing target records
        win, so a restart can never overwrite newer settings or counters with
        stale values from the read-only legacy database.
        """
        if self.main_db is None or self.legacy_main_db is None:
            return

        if self.operations_db is not None:
            await self._migrate_to_dedicated_operations()
            return
        if self.main_db is self.legacy_main_db:
            return

        marker_col = self.main_db["_migrations"]
        marker_id = "control_plane_to_operations_v1"
        if await marker_col.find_one({"_id": marker_id}, {"_id": 1}):
            return

        collections = (
            "bot_config",
            "users",
            "banned_users",
            "connected_groups",
            "missed_searches",
            "pending_requests",
            "settings",
            "duplicate_scan_results",
        )
        copied = 0
        for name in collections:
            source = self.legacy_main_db[name]
            target = self.main_db[name]
            ops = []
            async for document in source.find({}):
                document_id = document.pop("_id")
                payload = document or {"_legacy_migrated": True}
                ops.append(
                    UpdateOne(
                        {"_id": document_id},
                        {"$setOnInsert": payload},
                        upsert=True,
                    )
                )
                if len(ops) >= 500:
                    result = await target.bulk_write(ops, ordered=False)
                    copied += result.upserted_count
                    ops = []
            if ops:
                result = await target.bulk_write(ops, ordered=False)
                copied += result.upserted_count

        await marker_col.update_one(
            {"_id": marker_id},
            {"$set": {"completed_at": time.time(), "copied_documents": copied}},
            upsert=True,
        )
        logger.info("✅ Migrated %s legacy control-plane document(s) to operations DB.", copied)

    async def _migrate_to_dedicated_operations(self):
        """Resumably copy the legacy control plane without deleting its data."""
        marker_col = self.main_db["_migrations"]
        marker_id = "dedicated_operations_database_v1"
        if await marker_col.find_one({"_id": marker_id}, {"_id": 1}):
            return

        # Establish the uniqueness boundary before copying the registry. A
        # restart repeats only missing inserts because every write is upserted.
        await ensure_required_unique_index(
            self.main_db["file_registry"], "file_id", "file_registry.file_id"
        )
        await self.main_db["file_registry"].create_index(
            "file_unique_id", unique=True, sparse=True, name="file_unique_id_unique"
        )

        collections = (
            "bot_config",
            "users",
            "banned_users",
            "connected_groups",
            "missed_searches",
            "pending_requests",
            "settings",
            "duplicate_scan_results",
            "indexer_tasks",
            "index_failures",
            "rate_limits",
            "action_leases",
            "announcement_outbox",
            "file_registry",
            "scheduled_deletions",
            "scheduled_deletion_dead_letters",
            "broadcast_jobs",
            "verification_cache",
        )
        sources = []
        for source in (self.legacy_operations_db, self.legacy_main_db):
            if source is not None and all(source is not existing for existing in sources):
                sources.append(source)

        copied = 0
        validated = {}
        checkpoint_col = self.main_db["_migration_checkpoints"]
        for source in sources:
            for name in collections:
                source_col = source[name]
                target_col = self.main_db[name]
                checkpoint_id = (
                    f"{marker_id}:"
                    f"{hashlib.sha256(f'{source.name}:{name}'.encode()).hexdigest()[:20]}"
                )
                checkpoint = await checkpoint_col.find_one(
                    {"_id": checkpoint_id}, {"last_id": 1, "complete": 1}
                )
                if checkpoint and checkpoint.get("complete"):
                    validated[f"{source.name}:{name}"] = await source_col.count_documents({})
                    continue
                query = {}
                if checkpoint and "last_id" in checkpoint:
                    query = {"_id": {"$gt": checkpoint["last_id"]}}
                ops = []
                batch_last_id = None
                collection_copied = 0
                async for original in source_col.find(query).sort("_id", 1):
                    document = dict(original)
                    document_id = document.pop("_id")
                    batch_last_id = document_id
                    update = (
                        {"$set": document or {"_legacy_migrated": True}}
                        if name == "bot_config" and source is self.legacy_operations_db
                        else {"$setOnInsert": document or {"_legacy_migrated": True}}
                    )
                    ops.append(
                        UpdateOne(
                            {"_id": document_id},
                            update,
                            upsert=True,
                        )
                    )
                    if len(ops) >= 2000:
                        result = await target_col.bulk_write(ops, ordered=False)
                        copied += result.upserted_count
                        collection_copied += result.upserted_count
                        await checkpoint_col.update_one(
                            {"_id": checkpoint_id},
                            {
                                "$set": {
                                    "source": source.name,
                                    "collection": name,
                                    "last_id": batch_last_id,
                                    "updated_at": datetime.now(timezone.utc),
                                }
                            },
                            upsert=True,
                        )
                        if collection_copied and collection_copied % 20_000 < 2000:
                            logger.info(
                                "Operations migration %s/%s: %s new documents",
                                source.name,
                                name,
                                f"{collection_copied:,}",
                            )
                        ops = []
                if ops:
                    result = await target_col.bulk_write(ops, ordered=False)
                    copied += result.upserted_count
                    collection_copied += result.upserted_count
                    await checkpoint_col.update_one(
                        {"_id": checkpoint_id},
                        {
                            "$set": {
                                "source": source.name,
                                "collection": name,
                                "last_id": batch_last_id,
                                "updated_at": datetime.now(timezone.utc),
                            }
                        },
                        upsert=True,
                    )

                source_count = await source_col.count_documents({})
                target_count = await target_col.count_documents({})
                if target_count < source_count:
                    raise RuntimeError(
                        f"Operations migration validation failed for {name}: "
                        f"source={source_count}, target={target_count}"
                    )
                validated[f"{source.name}:{name}"] = source_count
                await checkpoint_col.update_one(
                    {"_id": checkpoint_id},
                    {
                        "$set": {
                            "complete": True,
                            "source_count": source_count,
                            "target_count": target_count,
                            "completed_at": datetime.now(timezone.utc),
                        }
                    },
                    upsert=True,
                )

        await marker_col.update_one(
            {"_id": marker_id},
            {
                "$set": {
                    "completed_at": time.time(),
                    "copied_documents": copied,
                    "validated_source_counts": validated,
                    "legacy_data_retained": True,
                }
            },
            upsert=True,
        )
        logger.info(
            "✅ Dedicated operations migration complete: %s new document(s); legacy retained.",
            copied,
        )

    async def migrate_access_gates(self):
        """Idempotently add the canonical schema while retaining old fields."""
        if self.config_col is None:
            return 0
        config = await self.config_col.find_one({"_id": "bot_config"}) or {}
        if int(config.get("access_gates_schema_version", 0) or 0) >= 1:
            return 0
        from plugins.access_gates import ACCESS_GATES_SCHEMA_VERSION, legacy_access_gates

        gates = legacy_access_gates(config)
        await self.config_col.update_one(
            {"_id": "bot_config"},
            {
                "$set": {
                    "access_gates": gates,
                    "access_gates_schema_version": ACCESS_GATES_SCHEMA_VERSION,
                    "access_gates_migrated_at": datetime.now(timezone.utc),
                }
            },
            upsert=True,
        )
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0
        return len(gates)

    async def consume_rate_limit(
        self, scope: str, key: str, limit: int, window_seconds: int
    ) -> tuple[bool, int]:
        """Atomically consume a continuously refilled MongoDB token bucket."""
        if self.rate_limits_col is None:
            logger.error("Rate-limit store unavailable; rejecting %s:%s", scope, key)
            return False, max(1, int(window_seconds))
        now = datetime.now(timezone.utc)
        refill_per_second = float(limit) / float(window_seconds)
        document_id = f"{scope}:{key}"
        elapsed_seconds = {
            "$divide": [
                {
                    "$max": [
                        0,
                        {"$subtract": [now, {"$ifNull": ["$updated_at", now]}]},
                    ]
                },
                1000,
            ]
        }
        available_tokens = {
            "$min": [
                float(limit),
                {
                    "$add": [
                        {"$ifNull": ["$tokens", float(limit)]},
                        {"$multiply": [elapsed_seconds, refill_per_second]},
                    ]
                },
            ]
        }
        try:
            document = await self.rate_limits_col.find_one_and_update(
                {"_id": document_id},
                [
                    {"$set": {"_available_tokens": available_tokens}},
                    {
                        "$set": {
                            "scope": scope,
                            "key": str(key),
                            "allowed": {"$gte": ["$_available_tokens", 1.0]},
                            "tokens": {
                                "$cond": [
                                    {"$gte": ["$_available_tokens", 1.0]},
                                    {"$subtract": ["$_available_tokens", 1.0]},
                                    "$_available_tokens",
                                ]
                            },
                            "updated_at": now,
                            "expires_at": datetime.fromtimestamp(
                                now.timestamp() + (window_seconds * 2), timezone.utc
                            ),
                        }
                    },
                    {"$unset": "_available_tokens"},
                ],
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
            allowed = bool(document and document.get("allowed"))
            remaining = float((document or {}).get("tokens", 0.0))
        except Exception as exc:
            logger.error(
                "Rate-limit store failure scope=%s error_type=%s",
                scope,
                type(exc).__name__,
            )
            allowed = False
            remaining = 0.0
        retry_after = max(1, int((max(0.0, 1.0 - remaining) / refill_per_second) + 0.999))
        return allowed, retry_after

    async def acquire_action_lease(self, scope: str, key: str, ttl_seconds: int) -> str | None:
        """Acquire a distributed idempotency lease, failing closed on errors."""
        if self.action_leases_col is None:
            return None
        now = datetime.now(timezone.utc)
        document_id = f"{scope}:{key}"
        owner = secrets.token_urlsafe(18)
        try:
            await self.action_leases_col.delete_one({"_id": document_id, "expires_at": {"$lte": now}})
            await self.action_leases_col.insert_one(
                {
                    "_id": document_id,
                    "owner": owner,
                    "expires_at": datetime.fromtimestamp(now.timestamp() + ttl_seconds, timezone.utc),
                }
            )
            return owner
        except DuplicateKeyError:
            return None
        except Exception as exc:
            logger.error(
                "Action-lease store failure scope=%s error_type=%s",
                scope,
                type(exc).__name__,
            )
            return None

    async def release_action_lease(self, scope: str, key: str, owner: str):
        if self.action_leases_col is None:
            return
        try:
            await self.action_leases_col.delete_one({"_id": f"{scope}:{key}", "owner": owner})
        except Exception as exc:
            logger.warning(
                "Action-lease cleanup deferred scope=%s error_type=%s",
                scope,
                type(exc).__name__,
            )

    async def search_tokens_need_migration(self) -> bool:
        """Return whether any legacy movie row lacks the indexed token field."""
        for index in self.readable_shard_indices():
            try:
                async with asyncio.timeout(SHARD_OPERATION_TIMEOUT_SECONDS):
                    if await self.file_cols[index].find_one(
                        {"search_tokens": {"$exists": False}}, {"_id": 1}
                    ):
                        return True
            except Exception as exc:
                self.mark_shard_error(index, exc)
                logger.warning(
                    "Search-token readiness skipped Cluster %s error_type=%s",
                    index + 1,
                    type(exc).__name__,
                )
        return False

    async def ensure_indexes(self):
        if self.registry_col is None:
            raise RequiredIndexError("Required file_registry collection is unavailable; refusing readiness.")
        await ensure_required_unique_index(
            self.registry_col,
            "file_id",
            "file_registry.file_id",
        )
        await ensure_required_index(
            self.registry_col,
            "movie_id",
            "file_registry.movie_id",
        )
        await ensure_required_index(
            self.registry_col,
            "location_pending",
            "file_registry.location_pending",
        )
        try:
            indexes = await self.registry_col.index_information()
            for field, name in (
                ("file_unique_id", "file_unique_id_unique"),
                ("telegram_identity", "telegram_identity_unique"),
            ):
                identity_ready = any(
                    spec.get("key") == [(field, 1)] and spec.get("unique") is True
                    for spec in indexes.values()
                )
                if not identity_ready:
                    await self.registry_col.create_index(
                        field,
                        unique=True,
                        sparse=True,
                        name=name,
                    )
        except Exception as exc:
            raise RequiredIndexError(
                "Could not create a required file content identity index: "
                f"{exc}. Run the duplicate report before retrying startup."
            ) from exc
        logger.info("✅ Required unique index verified on file_registry.file_id")

        # Registry uniqueness is the readiness boundary. Remaining indexes
        # improve performance and may degrade without weakening data safety.
        for i, col in enumerate(self.file_cols):
            try:
                indexes = await col.index_information()
                search_token_indexes = [
                    spec for spec in indexes.values() if ("search_tokens", 1) in spec.get("key", [])
                ]
                if not search_token_indexes:
                    # Existing libraries can contain hundreds of thousands of
                    # pre-token rows.  A sparse index keeps those rows out of
                    # the index until they are rewritten by normal indexing,
                    # avoiding a free-tier storage spike during startup.
                    await col.create_index("search_tokens", sparse=True)
                file_id_indexes = [spec for spec in indexes.values() if ("file_id", 1) in spec.get("key", [])]
                if not file_id_indexes:
                    await col.create_index("file_id", unique=True)
                elif not any(spec.get("unique") for spec in file_id_indexes):
                    # Do not attempt an in-place unique-index replacement on
                    # every startup. The centralized registry already enforces
                    # cross-cluster uniqueness, and an existing non-unique
                    # index is still useful for lookups.
                    logger.info(
                        "ℹ️ Cluster %s uses a legacy non-unique file_id index; "
                        "central registry enforces uniqueness.",
                        i + 1,
                    )
                logger.info(f"✅ Indexes verified on Cluster {i + 1}")
            except Exception as e:
                raise RequiredIndexError(
                    f"Could not verify required movie indexes on Cluster {i + 1}: {e}"
                ) from e
        if self.main_db is not None:
            try:
                await self.main_db["missed_searches"].create_index([("count", -1)])
                await ensure_required_compound_index(
                    self.main_db["pending_requests"],
                    [("user_id", 1), ("movie_name", 1)],
                    "pending_requests.user_movie",
                    unique=True,
                )
                await ensure_required_compound_index(
                    self.main_db["indexer_tasks"],
                    [("state", 1), ("updated", 1)],
                    "indexer_tasks.state_updated",
                )
                # TTL cleanup — additive only, never touches existing
                # documents. Both fields are populated going forward
                # (log_missed_search() / save_pending_request());
                # documents written before this change simply lack the
                # field and are skipped by MongoDB's TTL monitor until
                # they're naturally written to again (re-searched /
                # re-requested).
                await self.main_db["missed_searches"].create_index(
                    "last_searched_at",
                    expireAfterSeconds=90 * 24 * 3600,  # 90 days of no repeat searches
                )
                await self.main_db["pending_requests"].create_index(
                    "requested_at",
                    expireAfterSeconds=180
                    * 24
                    * 3600,  # 180 days — generous so a slow-to-fulfill request still gets auto-notified
                )
            except Exception as e:
                raise RequiredIndexError(f"Could not ensure operations indexes: {e}") from e
        if self.deletion_col is not None:
            try:
                await self.deletion_col.create_index("due_at")
                await ensure_required_compound_index(
                    self.deletion_col,
                    [("chat_id", 1), ("message_id", 1)],
                    "scheduled_deletions.chat_message",
                    unique=True,
                )
            except Exception as e:
                raise RequiredIndexError(f"Could not ensure scheduled-deletion indexes: {e}") from e
        if self.deletion_dead_letter_col is not None:
            try:
                await self.deletion_dead_letter_col.create_index("failed_at")
            except Exception as e:
                logger.warning("Could not ensure deletion dead-letter index: %s", e)
        if self.broadcast_col is not None:
            try:
                await self.broadcast_col.create_index([("status", 1), ("due_at", 1)])
            except Exception as e:
                logger.warning("Could not ensure broadcast-job index: %s", e)
        if self.groups_col is not None:
            try:
                await self.groups_col.create_index("search_count")
            except Exception as e:
                logger.warning("Could not ensure connected-groups index: %s", e)
        if hasattr(self, "verification_cache_col"):
            if self.verification_cache_col is None:
                raise RequiredIndexError("Required verification cache is unavailable; refusing readiness.")
            try:
                await self.verification_cache_col.create_index(
                    [("user_id", 1), ("gate_key", 1)],
                    unique=True,
                    name="verification_user_gate_unique",
                )
                await self.verification_cache_col.create_index(
                    "grace_until", expireAfterSeconds=0, name="verification_grace_ttl"
                )
            except Exception as exc:
                raise RequiredIndexError(
                    f"Could not create required verification-cache indexes: {exc}"
                ) from exc

        for collection, label in (
            (self.rate_limits_col, "rate-limit"),
            (self.action_leases_col, "action-lease"),
        ):
            if collection is None:
                raise RequiredIndexError(f"Required {label} collection is unavailable; refusing readiness.")
            try:
                await collection.create_index("expires_at", expireAfterSeconds=0)
            except Exception as exc:
                raise RequiredIndexError(
                    f"Could not create required TTL index for {label} controls: {exc}"
                ) from exc
        if self.announcement_col is None:
            raise RequiredIndexError("Required announcement outbox is unavailable; refusing readiness.")
        try:
            await self.announcement_col.create_index("due_at")
            await self.announcement_col.create_index("expires_at", expireAfterSeconds=0)
        except Exception as exc:
            raise RequiredIndexError(f"Could not create announcement outbox indexes: {exc}") from exc
    async def sync_config(self):
        if self.config_col is None:
            return
        config = await self.config_col.find_one({"_id": "bot_config"})
        migrations = {
            "log_channel": int(os.getenv("LOG_CHANNEL_ID", 0) or 0),
            "db_channel": int(os.getenv("DATABASE_CHANNEL_ID", 0) or 0),
            "update_channel_id": int(os.getenv("UPDATE_CHANNEL", 0) or 0),
            "update_channel": os.getenv("UPDATE_CHANNEL_LINK", ""),
            "main_group": os.getenv("MAIN_GROUP_LINK", ""),
            "request_channel_id": int(os.getenv("REQUEST_CHANNEL_ID", 0) or 0),
        }
        fields_to_set = {}
        for key, env_val in migrations.items():
            if config is None or key not in config:
                if env_val:
                    fields_to_set[key] = env_val
                    logger.info(
                        "  📥 Migrating '%s' from .env (value_type=%s; value redacted)",
                        key,
                        type(env_val).__name__,
                    )
        if fields_to_set:
            await self.config_col.update_one({"_id": "bot_config"}, {"$set": fields_to_set}, upsert=True)
            logger.info(f"✅ Config sync complete — {len(fields_to_set)} field(s) migrated.")
        else:
            logger.info("✅ Config sync complete — nothing to migrate.")

    async def save_user(self, user_id, first_name):
        if self.users_col is None:
            return False
        try:
            user = await self.users_col.find_one({"_id": user_id})
            if not user:
                await self.users_col.insert_one(
                    {"_id": user_id, "first_name": first_name, "joined": time.time()}
                )
                return True
            return False
        except Exception:
            return False

    async def get_all_users(self):
        if self.users_col is None:
            return []
        cursor = self.users_col.find({})
        return [doc["_id"] async for doc in cursor]

    async def get_user_count(self):
        """Return a count without materialising the complete user collection."""
        if self.users_col is None:
            return 0
        return await self.users_col.count_documents({})

    async def iter_user_ids(self, batch_size=500):
        """Stream recipient IDs for broadcasts instead of loading all users."""
        if self.users_col is None:
            return
        cursor = self.users_col.find({}, {"_id": 1}).batch_size(batch_size)
        async for doc in cursor:
            yield doc["_id"]

    async def iter_user_ids_after(self, after_id=None, batch_size=500):
        """Stream broadcast recipients after a durable numeric checkpoint."""
        if self.users_col is None:
            return
        query = {"_id": {"$gt": after_id}} if after_id is not None else {}
        cursor = (
            self.users_col.find(query, {"_id": 1})
            .sort("_id", 1)
            .batch_size(max(1, min(int(batch_size), 1000)))
        )
        async for doc in cursor:
            yield doc["_id"]

    async def delete_user(self, user_id):
        if self.users_col is None:
            return
        await self.users_col.delete_one({"_id": user_id})

    async def ban_user(self, user_id):
        if self.banned_col is None:
            return
        await self.banned_col.update_one({"_id": user_id}, {"$set": {"_id": user_id}}, upsert=True)

    async def unban_user(self, user_id):
        if self.banned_col is None:
            return
        await self.banned_col.delete_one({"_id": user_id})

    async def is_banned(self, user_id):
        if self.banned_col is None:
            return False
        doc = await self.banned_col.find_one({"_id": user_id})
        return doc is not None

    async def get_banned_users(self):
        if self.banned_col is None:
            return []
        cursor = self.banned_col.find({})
        return [doc["_id"] async for doc in cursor]

    async def add_group(self, group_id, group_title):
        if self.groups_col is None:
            return False
        group = await self.groups_col.find_one({"_id": group_id})
        if not group:
            await self.groups_col.insert_one(
                {
                    "_id": group_id,
                    "title": group_title,
                    "added": time.time(),
                    "whitelisted": False,
                    "banned": False,
                    "search_count": 0,
                    "settings": {},
                }
            )
            return True
        return False

    async def get_all_groups(self):
        if self.groups_col is None:
            return []
        cursor = self.groups_col.find({})
        return [doc async for doc in cursor]

    async def iter_broadcast_groups_after(self, after_id=None, batch_size=250):
        """Stream non-banned groups after a durable numeric checkpoint."""
        if self.groups_col is None:
            return
        query = {"banned": {"$ne": True}}
        if after_id is not None:
            query["_id"] = {"$gt": after_id}
        cursor = (
            self.groups_col.find(query, {"_id": 1})
            .sort("_id", 1)
            .batch_size(max(1, min(int(batch_size), 1000)))
        )
        async for doc in cursor:
            yield doc["_id"]

    async def get_group_count(self):
        if self.groups_col is None:
            return 0
        return await self.groups_col.count_documents({})

    async def get_broadcast_group_count(self):
        """Return only groups that are eligible to receive broadcasts."""
        if self.groups_col is None:
            return 0
        return await self.groups_col.count_documents({"banned": {"$ne": True}})

    async def get_group(self, group_id):
        if self.groups_col is None:
            return None
        return await self.groups_col.find_one({"_id": group_id})

    async def update_group(self, group_id, fields: dict):
        if self.groups_col is None:
            return
        await self.groups_col.update_one({"_id": group_id}, {"$set": fields}, upsert=True)

    async def ban_group(self, group_id):
        if self.groups_col is None:
            return
        await self.groups_col.update_one({"_id": group_id}, {"$set": {"banned": True}}, upsert=True)

    async def unban_group(self, group_id):
        if self.groups_col is None:
            return
        await self.groups_col.update_one({"_id": group_id}, {"$set": {"banned": False}})

    async def is_group_banned(self, group_id):
        if self.groups_col is None:
            return False
        doc = await self.groups_col.find_one({"_id": group_id})
        return doc.get("banned", False) if doc else False

    async def is_group_whitelisted(self, group_id):
        if self.groups_col is None:
            return True
        doc = await self.groups_col.find_one({"_id": group_id})
        return doc.get("whitelisted", False) if doc else False

    async def increment_group_search(self, group_id):
        if self.groups_col is None:
            return
        await self.groups_col.update_one({"_id": group_id}, {"$inc": {"search_count": 1}}, upsert=True)

    async def get_top_groups(self, limit=10):
        if self.groups_col is None:
            return []
        cursor = self.groups_col.find({}).sort("search_count", -1).limit(limit)
        return [doc async for doc in cursor]

    _DB_SIZE_TTL = 30  # seconds

    async def get_db_size(self, db_instance):
        """Return cached Atlas usage for the whole physical cluster.

        An Atlas M0 quota applies across every database on that cluster, not
        only this bot's ``MCCxBot_Cluster_N`` database. Using ``dbstats`` for
        one database made a 514MB cluster look like 249MB and repeatedly sent
        writes to a server that Atlas had already made read-only.
        """
        now = time.time()
        key = id(db_instance)
        hit = self._db_size_cache.get(key)
        if hit and (now - hit[0]) < self._DB_SIZE_TTL:
            return hit[1]
        try:
            total_bytes = 0
            async for info in await db_instance.client.list_databases():
                if info.get("name") not in {"admin", "local", "config"}:
                    total_bytes += info.get("sizeOnDisk", 0)
            size = total_bytes / (1024 * 1024)
        except Exception as cluster_error:
            try:
                stats = await db_instance.command("dbstats")
                size = (
                    max(stats.get("storageSize", 0), stats.get("dataSize", 0)) + stats.get("indexSize", 0)
                ) / (1024 * 1024)
            except Exception as db_error:
                # Unknown usage must fail closed: skipping a temporarily
                # unavailable shard is safer than reserving IDs for writes
                # that cannot possibly be acknowledged.
                logger.warning(
                    "Could not determine cluster capacity (%s; fallback: %s)",
                    cluster_error,
                    db_error,
                )
                size = float("inf")
        self._db_size_cache[key] = (now, size)
        try:
            self.shard_router.record_size(self.dbs.index(db_instance), size)
        except (AttributeError, ValueError):
            pass
        return size

    def shard_health_snapshot(self):
        router = getattr(self, "shard_router", None)
        return router.snapshot() if router is not None else []

    def readable_shard_indices(self) -> list[int]:
        """Return shards that can serve reads without known outage delays."""
        router = getattr(self, "shard_router", None)
        if router is None:
            return list(range(len(self.file_cols)))
        return router.read_candidates()

    def unavailable_shards(self) -> list[int]:
        router = getattr(self, "shard_router", None)
        return router.unavailable() if router is not None else []

    def mark_shard_reachable(self, index: int, reason: str = "connection_ok"):
        router = getattr(self, "shard_router", None)
        if router is not None:
            router.mark_reachable(index, reason)

    def mark_shard_error(self, index: int, error: BaseException):
        """Remove a shard from reads only for connectivity/capacity failures.

        A Mongo ``maxTimeMS`` query timeout or an invalid admin pattern is an
        operation failure, not proof that the whole cluster is offline.
        """
        router = getattr(self, "shard_router", None)
        if router is not None and (
            is_capacity_error(error)
            or isinstance(error, (AutoReconnect, ConnectionFailure, OSError))
            or type(error).__name__
            in {"NetworkTimeout", "ServerSelectionTimeoutError", "TLSHandshakeError"}
        ):
            router.mark_error(index, error)

    async def hydrate_shard_health(self):
        router = getattr(self, "shard_router", None)
        if router is None:
            return []
        shared = await redis_state.get_json("shard-health", "movie-shards")
        if shared:
            router.apply_snapshot(shared)
        return router.snapshot()

    async def publish_shard_health(self):
        snapshot = self.shard_health_snapshot()
        await redis_state.set_json("shard-health", "movie-shards", snapshot, ttl=120)
        return snapshot

    async def probe_shards(self, *, force: bool = False):
        """Refresh read/write routing concurrently with bounded probes."""
        router = getattr(self, "shard_router", None)
        if router is None:
            return []

        async def _probe(index: int):
            db_instance = self.dbs[index]
            try:
                async with asyncio.timeout(SHARD_OPERATION_TIMEOUT_SECONDS):
                    await db_instance.command("ping")
                router.mark_reachable(index, "health_probe_ok")
                self._db_size_cache.pop(id(db_instance), None)
                async with asyncio.timeout(SHARD_OPERATION_TIMEOUT_SECONDS):
                    size = await self.get_db_size(db_instance)
                router.record_size(index, size)
            except Exception as exc:
                router.mark_error(index, exc)

        candidates = (
            [
                index
                for index, health in enumerate(router.snapshot())
                if health.get("state") != "quarantined"
            ]
            if force
            else router.probe_candidates()
        )
        if candidates:
            await asyncio.gather(*[_probe(index) for index in candidates])
        return await self.publish_shard_health()

    async def get_total_files(self):
        cached_at, cached_count = self._file_count_cache
        if time.time() - cached_at < 30:
            return cached_count

        async def _count(index: int):
            col = self.file_cols[index]
            try:
                async with asyncio.timeout(SHARD_OPERATION_TIMEOUT_SECONDS):
                    count = await col.estimated_document_count()
                self._cluster_file_count_cache[index] = count
                self.mark_shard_reachable(index, "count_ok")
                return count
            except TimeoutError:
                logger.warning(
                    "File count timed out on Cluster %s; using last-known value",
                    index + 1,
                )
                return int(self._cluster_file_count_cache.get(index, 0))
            except Exception as exc:
                self.mark_shard_error(index, exc)
                logger.warning(
                    "File count using last-known value for Cluster %s error_type=%s",
                    index + 1,
                    type(exc).__name__,
                )
                return int(self._cluster_file_count_cache.get(index, 0))

        readable = self.readable_shard_indices()
        counts_by_index = {
            index: count
            for index, count in zip(readable, await asyncio.gather(*[_count(index) for index in readable]))
        }
        counts = [
            counts_by_index.get(index, int(self._cluster_file_count_cache.get(index, 0)))
            for index in range(len(self.file_cols))
        ]
        total = sum(counts)
        self._file_count_cache = (time.time(), total)
        return total

    def _invalidate_file_count(self):
        self._file_count_cache = (0.0, 0)

    def purge_caches(self):
        """Redis TTLs expire ephemeral caches without process-local sweeps."""
        return None

    def cache_metrics(self):
        return {
            "backend": "redis",
            "sessions": {"ttl_seconds": 600},
            "queries": {"ttl_seconds": 120},
        }

    async def ensure_search_catalog(self, force=False):
        """Build a compact unique-title catalog without duplicating files.

        The scan runs as a background startup task. Exact searches remain
        available throughout; typo correction activates when the generated
        gzip catalog is atomically installed.
        """
        global _SEARCH_TITLE_CATALOG
        if self._catalog_building:
            return len(_SEARCH_TITLE_CATALOG)
        if not force and _SEARCH_TITLE_CATALOG and _SEARCH_CATALOG_PATH.exists():
            age = time.time() - _SEARCH_CATALOG_PATH.stat().st_mtime
            if age < 7 * 24 * 3600:
                return len(_SEARCH_TITLE_CATALOG)

        worker_semaphore = self._catalog_worker_semaphore
        await worker_semaphore.acquire()
        self._catalog_building = True
        titles = set(_SEARCH_TITLE_CATALOG[:MAX_SEARCH_CATALOG_TITLES])
        scanned = 0
        started = time.monotonic()
        logger.info("🔤 Building fuzzy-search title catalog in background...")
        try:
            for shard_index in self.readable_shard_indices():
                cluster_number = shard_index + 1
                collection = self.file_cols[shard_index]
                cluster_count = 0
                name_batch = []
                try:
                    cursor = collection.find({}, {"file_name": 1, "_id": 0}).batch_size(2000)
                    async for file_doc in cursor:
                        name_batch.append(file_doc.get("file_name", ""))
                        cluster_count += 1
                        scanned += 1
                        if len(name_batch) >= 2000:
                            identities = await asyncio.to_thread(_catalog_identities, name_batch)
                            name_batch = []
                            for identity in identities:
                                if identity:
                                    titles.add(identity)
                                if len(titles) >= MAX_SEARCH_CATALOG_TITLES:
                                    break
                        if len(titles) >= MAX_SEARCH_CATALOG_TITLES:
                            break
                        if scanned % 100000 == 0:
                            logger.info(
                                "🔤 Search catalog: %s files → %s titles",
                                f"{scanned:,}",
                                f"{len(titles):,}",
                            )
                    if name_batch and len(titles) < MAX_SEARCH_CATALOG_TITLES:
                        identities = await asyncio.to_thread(_catalog_identities, name_batch)
                        for identity in identities:
                            if identity:
                                titles.add(identity)
                            if len(titles) >= MAX_SEARCH_CATALOG_TITLES:
                                break
                except Exception as exc:
                    self.mark_shard_error(shard_index, exc)
                    logger.warning(
                        "Search-catalog scan skipped remainder of cluster %s: %s",
                        cluster_number,
                        exc,
                    )
                logger.info(
                    "🔤 Search catalog cluster %s complete: %s files",
                    cluster_number,
                    f"{cluster_count:,}",
                )
                if len(titles) >= MAX_SEARCH_CATALOG_TITLES:
                    logger.info(
                        "Search catalog reached configured cap of %s titles",
                        f"{MAX_SEARCH_CATALOG_TITLES:,}",
                    )
                    break

            if not titles:
                return 0
            sorted_titles = await asyncio.to_thread(
                lambda: tuple(sorted(titles))
            )

            def _write_catalog():
                _SEARCH_CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
                temporary = _SEARCH_CATALOG_PATH.with_suffix(".tmp.gz")
                with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as handle:
                    handle.write("\n".join(sorted_titles))
                    handle.write("\n")
                os.replace(temporary, _SEARCH_CATALOG_PATH)

            await asyncio.to_thread(_write_catalog)
            _SEARCH_TITLE_CATALOG = sorted_titles
            self._invalidate_file_count()
            logger.info(
                "✅ Fuzzy-search catalog ready: %s titles from %s files in %.1fs",
                f"{len(sorted_titles):,}",
                f"{scanned:,}",
                time.monotonic() - started,
            )
            return len(sorted_titles)
        finally:
            self._catalog_building = False
            worker_semaphore.release()

    async def registry_needs_migration(self):
        """Detect the dangerous upgrade case: old files but no registry."""
        if self.registry_col is None:
            return False
        if await self.registry_col.count_documents({}, limit=1):
            return False

        async def _has_file(col):
            try:
                return await col.find_one({}, {"_id": 1}) is not None
            except Exception:
                return False

        return any(await asyncio.gather(*[_has_file(col) for col in self.file_cols]))

    async def save_file(self, media):
        file_id = getattr(media, "file_id", "")
        file_unique_id = getattr(media, "file_unique_id", "") or ""
        file_name = normalize_file_name(getattr(media, "file_name", ""))
        file_size = getattr(media, "file_size", 0)
        mime_type = getattr(media, "mime_type", "")
        if not file_id or not file_name:
            return False, "Invalid media"

        await self.hydrate_shard_health()
        # Centralized registry is the single source of truth for cross-cluster
        # uniqueness — one atomic insert instead of an O(clusters) fan-out.
        # A DuplicateKeyError here means the file_id is already claimed,
        # whether by another cluster or a concurrent save_file() call.
        reservation_created = False
        physical_present = False
        if self.registry_col is not None:
            try:
                reservation = registry_identity_document(file_id, file_unique_id)
                await self.registry_col.insert_one(reservation)
                reservation_created = True
            except DuplicateKeyError:
                return False, "Duplicate"

        file_doc = {
            "file_id": file_id,
            "file_name": file_name,
            "search_tokens": search_tokens_for_name(file_name),
            "language_tags": language_tags_for_name(file_name),
            "file_size": file_size,
            "mime_type": mime_type,
        }
        if file_unique_id:
            file_doc["file_unique_id"] = file_unique_id
        try:
            router = getattr(self, "shard_router", None)
            candidates = router.candidates() if router is not None else range(len(self.file_cols))
            for i in candidates:
                col = self.file_cols[i]
                try:
                    size = await self.get_db_size(self.dbs[i])
                except Exception as exc:
                    logger.warning(
                        "Cluster %s capacity check failed for %s: %s",
                        i + 1,
                        file_id,
                        type(exc).__name__,
                    )
                    continue
                if size >= 450:
                    if router is not None:
                        router.record_size(i, size)
                        await self.publish_shard_health()
                    continue

                try:
                    result = await col.insert_one(file_doc)
                except DuplicateKeyError:
                    existing = await col.find_one({"file_id": file_id}, {"_id": 1})
                    if not existing:
                        continue
                    physical_present = True
                    if self.registry_col is not None:
                        try:
                            await self.registry_col.update_one(
                                {"file_id": file_id},
                                {
                                    "$set": {
                                        "cluster": i + 1,
                                        "movie_id": str(existing["_id"]),
                                        "location_pending": False,
                                    }
                                },
                            )
                        except Exception as exc:
                            logger.warning(
                                "Registry location repair deferred for %s: %s",
                                file_id,
                                type(exc).__name__,
                            )
                    return False, "Duplicate"
                except Exception as exc:
                    if router is not None:
                        router.mark_error(i, exc)
                        await self.publish_shard_health()
                    logger.warning(
                        "Cluster %s insert failed for %s: %s",
                        i + 1,
                        file_id,
                        type(exc).__name__,
                    )
                    continue

                physical_present = True
                if self.registry_col is not None:
                    try:
                        await self.registry_col.update_one(
                            {"file_id": file_id},
                            {
                                "$set": {
                                    "cluster": i + 1,
                                    "movie_id": str(result.inserted_id),
                                    "location_pending": False,
                                }
                            },
                        )
                    except Exception as exc:
                        logger.warning(
                            "Registry location update deferred for %s: %s",
                            file_id,
                            type(exc).__name__,
                        )
                self._invalidate_file_count()
                await self.increment_language_counters([file_doc], 1)
                return True, f"Saved to Cluster {i + 1}"
            return False, "All clusters full"
        finally:
            if reservation_created and not physical_present:
                await self._release_registry_ids([file_id])

    async def reconcile_registry_locations(self, limit=250):
        """Repair registry locations with one bounded query per movie shard.

        The former per-row fan-out issued up to ``limit × shards`` MongoDB
        lookups every health cycle. A 250-row repair over four shards meant
        roughly 1,000 small queries. This batches the same non-destructive
        repair into one registry read, one lookup per readable shard and one
        unordered registry write.
        """
        if self.registry_col is None:
            return {"checked": 0, "repaired": 0, "unresolved": 0}
        cursor = self.registry_col.find(
            {"location_pending": True},
            {"_id": 1, "file_id": 1},
        ).limit(max(1, int(limit)))
        registry_docs = [document async for document in cursor]
        checked = len(registry_docs)
        file_ids = list(
            dict.fromkeys(
                document.get("file_id") for document in registry_docs if document.get("file_id")
            )
        )
        if not file_ids:
            return {"checked": checked, "repaired": 0, "unresolved": checked}

        async def _lookup(index: int):
            try:
                async with asyncio.timeout(SHARD_OPERATION_TIMEOUT_SECONDS):
                    movie_cursor = self.file_cols[index].find(
                        {"file_id": {"$in": file_ids}},
                        {"_id": 1, "file_id": 1},
                    )
                    documents = [document async for document in movie_cursor]
                self.mark_shard_reachable(index, "registry_reconcile_ok")
                return index + 1, documents
            except TimeoutError:
                logger.warning("Registry batch lookup timed out on Cluster %s", index + 1)
                return index + 1, []
            except Exception as error:
                self.mark_shard_error(index, error)
                logger.warning(
                    "Registry batch lookup skipped Cluster %s error_type=%s",
                    index + 1,
                    type(error).__name__,
                )
                return index + 1, []

        readable = self.readable_shard_indices()
        lookup_results = await asyncio.gather(*[_lookup(index) for index in readable])
        locations = {}
        for cluster_number, movie_docs in lookup_results:
            for movie_doc in movie_docs:
                file_id = movie_doc.get("file_id")
                if file_id and file_id not in locations:
                    locations[file_id] = (cluster_number, str(movie_doc["_id"]))

        repairs = []
        for registry_doc in registry_docs:
            location = locations.get(registry_doc.get("file_id"))
            if not location:
                continue
            cluster_number, movie_id = location
            repairs.append(
                UpdateOne(
                    {"_id": registry_doc["_id"], "location_pending": True},
                    {
                        "$set": {
                            "cluster": cluster_number,
                            "movie_id": movie_id,
                            "location_pending": False,
                        }
                    },
                )
            )
        repaired = 0
        if repairs:
            result = await self.registry_col.bulk_write(repairs, ordered=False)
            repaired = int(result.modified_count)
        return {
            "checked": checked,
            "repaired": repaired,
            "unresolved": max(0, checked - repaired),
        }

    async def _registry_bulk_reserve(self, files: list) -> tuple:
        """Attempts to atomically reserve every file_id in the centralized
        registry via one ordered=False bulk insert. Returns (accepted_ids,
        duplicate_count) — duplicates are file_ids already claimed by another
        cluster or a prior call, and are silently excluded rather than
        treated as an error."""
        if not files:
            return [], 0
        if isinstance(files[0], dict):
            file_ids = [item["file_id"] for item in files]
            registry_docs = []
            for item in files:
                registry_docs.append(
                    registry_identity_document(
                        item["file_id"], item.get("file_unique_id") or ""
                    )
                )
        else:
            file_ids = list(files)
            registry_docs = [registry_identity_document(fid) for fid in file_ids]
        if self.registry_col is None:
            return file_ids, 0

        ops = [InsertOne(document) for document in registry_docs]
        try:
            await self.registry_col.bulk_write(ops, ordered=False)
            return file_ids, 0
        except BulkWriteError as bwe:
            write_errors = bwe.details.get("writeErrors", [])
            concern_errors = bwe.details.get("writeConcernErrors", [])
            if concern_errors:
                # The server may have applied some reservations, but did not
                # acknowledge them reliably. Remove only claims with no
                # physical file, then force the indexer to retry later.
                await self._release_registry_ids(file_ids)
                raise RuntimeError(f"Registry write concern failed: {concern_errors[:1]}") from bwe
            dup_indexes = {e["index"] for e in write_errors if e.get("code") == 11000}
            other_errors = [e for e in write_errors if e.get("code") != 11000]
            if other_errors:
                # Continuing would store files without a uniqueness claim.
                # Roll back reservations that did succeed, then fail so the
                # caller can retry the complete batch safely.
                failed_indexes = {e.get("index") for e in write_errors}
                reserved = [fid for idx, fid in enumerate(file_ids) if idx not in failed_indexes]
                if reserved:
                    try:
                        await self.registry_col.delete_many({"file_id": {"$in": reserved}})
                    except Exception as rollback_error:
                        logger.error(
                            "Registry reservation rollback failed for %s IDs: %s",
                            len(reserved),
                            rollback_error,
                        )
                raise RuntimeError(
                    f"Registry reservation failed for {len(other_errors)} file(s): {other_errors[:1]}"
                ) from bwe
            accepted = [fid for idx, fid in enumerate(file_ids) if idx not in dup_indexes]
            return accepted, len(dup_indexes)

    async def _mark_registry_locations(self, docs: list, cluster_index: int):
        if not docs or self.registry_col is None:
            return
        ops = [
            UpdateOne(
                {"file_id": doc["file_id"]},
                {
                    "$set": {
                        "cluster": cluster_index + 1,
                        "movie_id": str(doc["_id"]),
                        "location_pending": False,
                    }
                },
            )
            for doc in docs
            if doc.get("file_id") and doc.get("_id")
        ]
        if ops:
            try:
                await self.registry_col.bulk_write(ops, ordered=False)
            except Exception as e:
                # Location is repairable metadata; the file_id reservation is
                # the actual uniqueness invariant and must remain claimed.
                logger.warning("Could not update registry location metadata: %s", e)

    async def save_files_bulk(self, files_list):
        if not files_list:
            return 0, 0
        await self.hydrate_shard_health()

        # Normalize defensively here too. The bulk indexer already cleans its
        # input, but keeping the database boundary authoritative guarantees
        # every caller produces the same searchable identity.
        normalized_files = []
        for incoming in files_list:
            file_doc = dict(incoming)
            file_doc["file_name"] = normalize_file_name(file_doc.get("file_name", ""))
            file_doc["search_tokens"] = search_tokens_for_name(file_doc["file_name"])
            file_doc["language_tags"] = language_tags_for_name(file_doc["file_name"])
            if not file_doc.get("file_unique_id"):
                file_doc.pop("file_unique_id", None)
            normalized_files.append(file_doc)

        # De-duplicate the incoming batch before reserving IDs. Filtering via
        # an accepted-id set after reservation reintroduced duplicate entries.
        unique_files, internal_duplicates = deduplicate_file_batch(normalized_files)

        accepted_ids, duplicates = await self._registry_bulk_reserve(unique_files)
        duplicates += internal_duplicates
        accepted_set = set(accepted_ids)
        new_files = [f for f in unique_files if f["file_id"] in accepted_set]

        if not new_files:
            return 0, duplicates

        saved_total = 0
        saved_documents = []
        remaining = new_files[:]
        router = getattr(self, "shard_router", None)
        candidates = router.candidates() if router is not None else range(len(self.file_cols))
        for i in candidates:
            col = self.file_cols[i]
            if not remaining:
                break
            size = await self.get_db_size(self.dbs[i])
            if size >= 450:
                if router is not None:
                    router.record_size(i, size)
                continue
            try:
                result = await col.insert_many(remaining, ordered=False)
                inserted = []
                for doc, inserted_id in zip(remaining, result.inserted_ids):
                    stored = dict(doc)
                    stored["_id"] = inserted_id
                    inserted.append(stored)
                await self._mark_registry_locations(inserted, i)
                saved_total += len(inserted)
                saved_documents.extend(inserted)
                remaining = []
            except BulkWriteError as bwe:
                if router is not None and is_capacity_error(bwe):
                    router.mark_error(i, bwe)
                errors = bwe.details.get("writeErrors", [])
                failed_indexes = {err.get("index") for err in errors}
                successful = [doc for idx, doc in enumerate(remaining) if idx not in failed_indexes]

                # PyMongo normally adds generated _ids to input documents
                # before the write. Query only as a fallback (also useful for
                # alternate/fake collection implementations).
                if successful:
                    stored_docs = [doc for doc in successful if doc.get("_id")]
                    if len(stored_docs) != len(successful):
                        try:
                            stored_docs = await col.find(
                                {"file_id": {"$in": [doc["file_id"] for doc in successful]}},
                                {"_id": 1, "file_id": 1},
                            ).to_list(length=len(successful))
                        except Exception as e:
                            logger.warning("Could not read inserted IDs for registry metadata: %s", e)
                            stored_docs = []
                    await self._mark_registry_locations(stored_docs, i)
                    saved_total += len(successful)
                    saved_documents.extend(successful)

                retry = []
                for err in errors:
                    idx = err.get("index")
                    if idx is None or idx >= len(remaining):
                        continue
                    doc = remaining[idx]
                    if err.get("code") == 11000:
                        # The row already exists physically; repair registry
                        # metadata and treat it as a duplicate, not an unsaved file.
                        existing = await col.find_one({"file_id": doc["file_id"]}, {"_id": 1, "file_id": 1})
                        if existing:
                            await self._mark_registry_locations([existing], i)
                            duplicates += 1
                            continue
                    retry.append(doc)
                remaining = retry
                logger.warning(
                    f"Cluster {i + 1} bulk insert: {len(successful)} saved, "
                    f"{len(retry)} retryable, {len(errors) - len(retry)} duplicate(s)"
                )
            except Exception as e:
                if router is not None:
                    router.mark_error(i, e)
                if "space quota" in str(e).lower() or "over your space" in str(e).lower():
                    logger.error(f"Cluster {i + 1} FULL — add DATABASE_URI_{i + 2} to .env")
                else:
                    logger.warning(f"Cluster {i + 1} bulk insert partial failure: {e}")

        if remaining and self.registry_col is not None:
            # Nothing left could actually be stored — roll back their
            # registry reservations so these file_ids aren't permanently
            # (and incorrectly) marked as taken.
            try:
                await self.registry_col.delete_many({"file_id": {"$in": [f["file_id"] for f in remaining]}})
            except Exception as e:
                logger.warning(f"Registry rollback failed for {len(remaining)} unsaved files: {e}")

        if remaining:
            # Every cluster was either at its safety margin or rejected the
            # insert — this batch is genuinely unstorable right now. Raise
            # instead of returning (0, duplicates) so callers can't mistake
            # "database full" for "everything was a duplicate".
            await self.publish_shard_health()
            raise AllClustersFullError(len(remaining), duplicates)

        if saved_total:
            self._invalidate_file_count()
            await self.increment_language_counters(saved_documents, 1)
        await self.publish_shard_health()
        return saved_total, duplicates

    async def _release_registry_ids(self, file_ids, batch_size=500):
        """Release registry claims only when no physical row remains.

        This keeps deletes, purges, duplicate cleanup and automatic broken-file
        removal consistent with the cross-cluster uniqueness registry.
        """
        if self.registry_col is None:
            return
        ids = list(dict.fromkeys(fid for fid in file_ids if fid))
        for start in range(0, len(ids), batch_size):
            chunk = ids[start : start + batch_size]
            present_sets = await asyncio.gather(
                *[col.distinct("file_id", {"file_id": {"$in": chunk}}) for col in self.file_cols],
                return_exceptions=True,
            )
            errors = [value for value in present_sets if isinstance(value, Exception)]
            if errors:
                # Never release a uniqueness claim when an unavailable cluster
                # prevents us from proving that no physical copy remains.
                logger.warning(
                    "Registry cleanup deferred for %s file(s): %s cluster check(s) failed",
                    len(chunk),
                    len(errors),
                )
                continue
            present = {fid for values in present_sets for fid in values}
            orphaned = [fid for fid in chunk if fid not in present]
            if orphaned:
                await self.registry_col.delete_many({"file_id": {"$in": orphaned}})

    async def admin_search_files(self, query, limit=20):
        words = search_tokens_for_name(query)[:12]
        if not words:
            return []
        mongo_query = {"search_tokens": {"$all": words}}

        async def _search_cluster(i, col):
            try:
                docs = []
                async with asyncio.timeout(5.0):
                    cursor = col.find(mongo_query).limit(limit).max_time_ms(4500)
                    async for doc in cursor:
                        doc["_cluster"] = i + 1
                        docs.append(doc)
                self.mark_shard_reachable(i, "admin_search_ok")
                return docs
            except TimeoutError:
                logger.warning("Admin search skipped slow Cluster %s", i + 1)
                return []
            except Exception as exc:
                self.mark_shard_error(i, exc)
                logger.warning(
                    "Admin search skipped Cluster %s error_type=%s",
                    i + 1,
                    type(exc).__name__,
                )
                return []

        readable = self.readable_shard_indices()
        cluster_results = await asyncio.gather(
            *[_search_cluster(i, self.file_cols[i]) for i in readable]
        )
        results = [doc for docs in cluster_results for doc in docs]
        return results[:limit]

    async def delete_file_by_obj_id(self, file_obj_id):
        try:
            obj_id = ObjectId(file_obj_id)
        except Exception:
            return False
        for index in self.readable_shard_indices():
            col = self.file_cols[index]
            try:
                doc = await col.find_one({"_id": obj_id}, {"file_id": 1, "file_name": 1})
            except Exception as e:
                logger.warning("File lookup failed during delete: %s", e)
                continue
            if not doc:
                continue
            try:
                result = await col.delete_one({"_id": obj_id})
            except Exception as e:
                logger.warning("File delete failed: %s", e)
                continue
            if result.deleted_count > 0:
                await self._release_registry_ids([doc.get("file_id")])
                await self.increment_language_counters([doc], -1)
                self._invalidate_file_count()
                return True
        return False

    async def update_file_name(self, file_obj_id, new_name):
        try:
            obj_id = ObjectId(file_obj_id)
        except Exception:
            return False
        normalized_name = normalize_file_name(new_name)
        for index in self.readable_shard_indices():
            col = self.file_cols[index]
            try:
                existing = await col.find_one({"_id": obj_id}, {"file_name": 1})
                result = await col.update_one(
                    {"_id": obj_id},
                    {
                        "$set": {
                            "file_name": normalized_name,
                            "search_tokens": search_tokens_for_name(normalized_name),
                            "language_tags": language_tags_for_name(normalized_name),
                        }
                    },
                )
            except Exception as exc:
                self.mark_shard_error(index, exc)
                continue
            if result.matched_count > 0:
                if existing:
                    await self.increment_language_counters([existing], -1)
                await self.increment_language_counters(
                    [{"file_name": normalized_name}], 1
                )
                self._invalidate_file_count()
                return True
        return False

    async def get_files_by_language(self):
        if self.analytics_col is None:
            raise RuntimeError("Analytics counters collection is unavailable")
        document = await self.analytics_col.find_one({"_id": "files_by_language"})
        counts = (document or {}).get("counts", {})
        return {language: max(0, int(counts.get(language, 0))) for language in ANALYTICS_LANGUAGES}

    async def analytics_counters_ready(self) -> bool:
        return bool(
            self.analytics_col is not None
            and await self.analytics_col.find_one(
                {"_id": "files_by_language"}, {"_id": 1}
            )
        )

    async def increment_language_counters(self, documents, direction: int):
        """Atomically maintain language totals as file rows change."""
        if getattr(self, "analytics_col", None) is None or not documents or not direction:
            return
        deltas = {}
        for document in documents:
            tags = document.get("language_tags")
            if tags is None:
                tags = language_tags_for_name(document.get("file_name", ""))
            for tag in set(tags):
                if tag in ANALYTICS_LANGUAGES:
                    deltas[f"counts.{tag}"] = deltas.get(f"counts.{tag}", 0) + int(direction)
        if not deltas:
            return
        try:
            await self.analytics_col.update_one(
                {"_id": "files_by_language"},
                {
                    "$inc": deltas,
                    "$set": {"updated_at": datetime.now(timezone.utc)},
                    "$setOnInsert": {"created_at": datetime.now(timezone.utc)},
                },
                upsert=True,
            )
        except Exception as exc:
            # A completed movie-row write must not be reported as failed just
            # because its derived analytics counter could not be updated.
            # Mark the projection dirty so maintenance can rebuild it with
            # tools/migrate_search_tokens.py --apply.
            logger.warning("Language analytics counter update deferred: %s", exc)
            try:
                await redis_state.set_json(
                    "analytics:files_by_language:dirty",
                    {
                        "dirty": True,
                        "recorded_at": datetime.now(timezone.utc).isoformat(),
                    },
                    ttl=86400,
                )
            except Exception:
                logger.debug("Could not record the analytics dirty marker", exc_info=True)

    async def replace_language_counters(self, counts: dict):
        if self.analytics_col is None:
            raise RuntimeError("Analytics counters collection is unavailable")
        normalized = {
            language: max(0, int(counts.get(language, 0)))
            for language in ANALYTICS_LANGUAGES
        }
        await self.analytics_col.replace_one(
            {"_id": "files_by_language"},
            {
                "_id": "files_by_language",
                "counts": normalized,
                "rebuilt_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            },
            upsert=True,
        )

    async def scan_duplicate_report(self, progress_callback=None):
        """Build a conservative, low-disk report without deleting any data."""
        scan_lock = getattr(self, "_duplicate_scan_lock", None)
        if scan_lock is None:
            scan_lock = asyncio.Lock()
            self._duplicate_scan_lock = scan_lock

        async with scan_lock:
            await self.probe_shards()
            unavailable = self.unavailable_shards()
            if unavailable:
                cluster_list = ", ".join(str(number) for number in unavailable)
                raise RuntimeError(
                    "A complete duplicate report cannot start while MongoDB "
                    f"Cluster {cluster_list} is unavailable. Partial scans are "
                    "blocked because they can hide cross-cluster duplicates. "
                    "Retry after the cluster health check is green."
                )
            total = await self.get_total_files()
            started = time.monotonic()
            total_work = max(1, total * 3)
            await asyncio.to_thread(_DUPLICATE_SCAN_DIR.mkdir, parents=True, exist_ok=True)

            def _remove_stale_spools():
                cutoff = time.time() - 86400
                for candidate in _DUPLICATE_SCAN_DIR.glob("mccx-duplicates-*.sqlite3"):
                    try:
                        if candidate.is_file() and candidate.stat().st_mtime < cutoff:
                            candidate.unlink(missing_ok=True)
                    except OSError:
                        logger.debug("Could not remove stale duplicate spool %s", candidate)

            await asyncio.to_thread(_remove_stale_spools)

            async def _show_phase_progress(phase, phase_scanned, cluster, phase_started, work_offset):
                if not progress_callback:
                    return
                try:
                    await progress_callback(
                        {
                            "phase": phase,
                            "scanned": phase_scanned,
                            "total": total,
                            "overall_scanned": min(total_work, work_offset + phase_scanned),
                            "overall_total": total_work,
                            "cluster": cluster,
                            "clusters": len(self.file_cols),
                            "elapsed": time.monotonic() - started,
                            "phase_elapsed": time.monotonic() - phase_started,
                        }
                    )
                except Exception as error:
                    logger.warning(
                        "Duplicate progress update skipped error_type=%s",
                        type(error).__name__,
                    )

            async def _scan_aggregate_phase(mode: str, work_offset: int):
                phase_started = time.monotonic()
                descriptor, spool_path = await asyncio.to_thread(
                    tempfile.mkstemp,
                    suffix=".sqlite3",
                    prefix="mccx-duplicates-",
                    dir=str(_DUPLICATE_SCAN_DIR),
                )
                await asyncio.to_thread(os.close, descriptor)
                phase_scanned = 0
                active_cluster = 0
                try:
                    await asyncio.to_thread(_initialize_duplicate_spool, spool_path, mode)
                    await _show_phase_progress(mode, 0, 1, phase_started, work_offset)
                    for cluster_index, col in enumerate(self.file_cols, 1):
                        active_cluster = cluster_index
                        batch = []
                        cursor = col.find(
                            {"file_name": {"$exists": True, "$ne": ""}},
                            {
                                "file_name": 1,
                                "file_id": 1,
                                "file_unique_id": 1,
                                "file_size": 1,
                                "mime_type": 1,
                            },
                        ).batch_size(1000)
                        async for document in cursor:
                            batch.append(document)
                            if len(batch) >= 1000:
                                await asyncio.to_thread(
                                    _spool_duplicate_batch,
                                    spool_path,
                                    batch,
                                    cluster_index,
                                    mode,
                                )
                                phase_scanned += len(batch)
                                await _show_phase_progress(
                                    mode,
                                    phase_scanned,
                                    cluster_index,
                                    phase_started,
                                    work_offset,
                                )
                                batch = []
                        if batch:
                            await asyncio.to_thread(
                                _spool_duplicate_batch,
                                spool_path,
                                batch,
                                cluster_index,
                                mode,
                            )
                            phase_scanned += len(batch)
                            await _show_phase_progress(
                                mode,
                                phase_scanned,
                                cluster_index,
                                phase_started,
                                work_offset,
                            )
                    return await asyncio.to_thread(_read_duplicate_report, spool_path)
                except sqlite3.OperationalError as error:
                    if "full" in str(error).casefold():
                        raise RuntimeError(
                            "The host storage is full. Free at least 150 MB of "
                            "server disk space, then run the duplicate report again."
                        ) from error
                    raise
                except RuntimeError:
                    raise
                except Exception as error:
                    if active_cluster:
                        self.mark_shard_error(active_cluster - 1, error)
                    raise RuntimeError(
                        "The complete duplicate report stopped because MongoDB "
                        f"Cluster {active_cluster or '?'} became unavailable "
                        f"({type(error).__name__}). Nothing was deleted."
                    ) from error
                finally:
                    await asyncio.to_thread(Path(spool_path).unlink, missing_ok=True)

            exact_report = await _scan_aggregate_phase("exact", 0)
            probable_report = await _scan_aggregate_phase("probable", total)

            exact_groups = exact_report["groups"]
            remaining = max(0, MAX_DUPLICATE_GROUPS - len(exact_groups))
            groups = exact_groups + probable_report["groups"][:remaining]
            summary = {
                "scanned": max(
                    exact_report["summary"]["scanned"],
                    probable_report["summary"]["scanned"],
                ),
                "exact_groups": exact_report["summary"]["exact_groups"],
                "exact_extras": exact_report["summary"]["exact_extras"],
                "probable_groups": probable_report["summary"]["probable_groups"],
                "probable_matches": probable_report["summary"]["probable_matches"],
                "shown_groups": len(groups),
                "report_only": True,
                "storage": "compact_fingerprints",
            }

            if groups:
                exact_targets = {
                    bytes.fromhex(group["identity"]) for group in groups if group["type"] == "exact"
                }
                probable_targets = {
                    bytes.fromhex(group["identity"]) for group in groups if group["type"] == "probable"
                }
                labels: dict[tuple[str, bytes], str] = {}
                phase_started = time.monotonic()
                phase_scanned = 0
                await _show_phase_progress("labels", 0, 1, phase_started, total * 2)
                for cluster_index, col in enumerate(self.file_cols, 1):
                    batch = []
                    cursor = col.find(
                        {"file_name": {"$exists": True, "$ne": ""}},
                        {
                            "file_name": 1,
                            "file_id": 1,
                            "file_unique_id": 1,
                            "file_size": 1,
                            "mime_type": 1,
                        },
                    ).batch_size(1000)
                    async for document in cursor:
                        batch.append(document)
                        if len(batch) >= 1000:
                            for item in batch:
                                exact_key, probable_key = _duplicate_fingerprints(item)
                                name = str(item.get("file_name", "") or "Unknown")
                                if exact_key in exact_targets:
                                    labels.setdefault(("exact", exact_key), name)
                                if probable_key in probable_targets:
                                    labels.setdefault(("probable", probable_key), name)
                            phase_scanned += len(batch)
                            await _show_phase_progress(
                                "labels",
                                phase_scanned,
                                cluster_index,
                                phase_started,
                                total * 2,
                            )
                            batch = []
                    if batch:
                        for item in batch:
                            exact_key, probable_key = _duplicate_fingerprints(item)
                            name = str(item.get("file_name", "") or "Unknown")
                            if exact_key in exact_targets:
                                labels.setdefault(("exact", exact_key), name)
                            if probable_key in probable_targets:
                                labels.setdefault(("probable", probable_key), name)
                        phase_scanned += len(batch)
                        await _show_phase_progress(
                            "labels",
                            phase_scanned,
                            cluster_index,
                            phase_started,
                            total * 2,
                        )

                for group in groups:
                    identity = bytes.fromhex(group["identity"])
                    group["name"] = labels.get((group["type"], identity), group["name"])

            report = {"summary": summary, "groups": groups}

        for group in report["groups"]:
            await redis_state.set_json("duplicate-group", group["key"], group, ttl=900)
        return report

    async def find_duplicate_files(self):
        """Compatibility wrapper returning the report's bounded group list."""
        return (await self.scan_duplicate_report())["groups"]

    async def get_duplicate_group(self, group_key: str):
        group = await redis_state.get_json("duplicate-group", group_key)
        if group is None:
            groups = await self.find_duplicate_files()
            group = next((item for item in groups if item.get("key") == group_key), None)
        return group

    async def delete_duplicate_group(self, group_key: str):
        raise RuntimeError("Individual deletion is disabled. Use verified exact cleanup.")

    async def _reconcile_registry_file_ids(self, file_ids: list[str]):
        """Remove stale claims and point surviving claims at a real movie row."""
        if self.registry_col is None:
            return
        unique_ids = list(dict.fromkeys(file_id for file_id in file_ids if file_id))
        if not unique_ids:
            return

        last_error = None
        for attempt in range(1, 4):
            try:
                locations = {}
                for cluster_index, col in enumerate(self.file_cols, 1):
                    cursor = col.find(
                        {"file_id": {"$in": unique_ids}},
                        {"_id": 1, "file_id": 1},
                    )
                    async for document in cursor:
                        file_id = document.get("file_id")
                        if file_id and file_id not in locations:
                            locations[file_id] = (
                                cluster_index,
                                str(document["_id"]),
                            )

                stale_ids = [file_id for file_id in unique_ids if file_id not in locations]
                if stale_ids:
                    await self.registry_col.delete_many({"file_id": {"$in": stale_ids}})
                repairs = [
                    UpdateOne(
                        {"file_id": file_id},
                    {
                        "$set": {
                            "cluster": cluster,
                            "movie_id": movie_id,
                            "location_pending": False,
                        }
                    },
                        upsert=True,
                    )
                    for file_id, (cluster, movie_id) in locations.items()
                ]
                if repairs:
                    await self.registry_col.bulk_write(repairs, ordered=False)
                return
            except Exception as error:
                last_error = error
                if attempt < 3:
                    await asyncio.sleep(attempt)
        raise RuntimeError("Registry repair failed after verified movie rows were processed") from last_error

    async def _recover_duplicate_cleanup_registry(self):
        """Finish registry repairs left by an interrupted exact cleanup."""
        await asyncio.to_thread(_DUPLICATE_SCAN_DIR.mkdir, parents=True, exist_ok=True)
        for candidate in _DUPLICATE_SCAN_DIR.glob("mccx-cleanup-*.sqlite3"):
            try:
                while True:
                    file_ids = await asyncio.to_thread(_read_cleanup_registry_ids, str(candidate), 500)
                    if not file_ids:
                        break
                    await self._reconcile_registry_file_ids(file_ids)
                    await asyncio.to_thread(_clear_cleanup_registry_ids, str(candidate), file_ids)
                await asyncio.to_thread(candidate.unlink, missing_ok=True)
            except Exception as error:
                logger.warning(
                    "Deferred duplicate-cleanup registry recovery for %s: %s",
                    candidate.name,
                    type(error).__name__,
                )
                raise

    async def delete_verified_duplicates(self, progress_callback=None):
        """Delete only rows with an already-preserved exact Telegram identity.

        Higher-numbered clusters and newer ObjectIds are scanned first, so
        the freshest usable row survives. Metadata-only probable matches are
        never considered by this cleanup.
        """
        cleanup_lock = getattr(self, "_verified_cleanup_lock", None)
        if cleanup_lock is None:
            cleanup_lock = asyncio.Lock()
            self._verified_cleanup_lock = cleanup_lock

        async with cleanup_lock:
            await self._recover_duplicate_cleanup_registry()
            total = await self.get_total_files()
            started = time.monotonic()
            scanned = deleted = 0
            descriptor, spool_path = await asyncio.to_thread(
                tempfile.mkstemp,
                suffix=".sqlite3",
                prefix="mccx-cleanup-",
                dir=str(_DUPLICATE_SCAN_DIR),
            )
            await asyncio.to_thread(os.close, descriptor)
            try:
                await asyncio.to_thread(_initialize_verified_cleanup_spool, spool_path)
            except Exception:
                await asyncio.to_thread(Path(spool_path).unlink, missing_ok=True)
                raise
            keep_spool = False

            async def _process_batch(col, batch, cluster_index):
                nonlocal scanned, deleted, keep_spool
                duplicates = await asyncio.to_thread(_classify_verified_duplicate_batch, spool_path, batch)
                staged_ids = [str(document.get("file_id", "") or "") for document in duplicates]
                if duplicates:
                    await asyncio.to_thread(_stage_cleanup_registry_ids, spool_path, staged_ids)
                    keep_spool = True
                    object_ids = [document["_id"] for document in duplicates]
                    result = await col.delete_many({"_id": {"$in": object_ids}})
                    if result.deleted_count == len(object_ids):
                        confirmed_deleted = duplicates
                    else:
                        remaining = {
                            document["_id"]
                            async for document in col.find({"_id": {"$in": object_ids}}, {"_id": 1})
                        }
                        confirmed_deleted = [
                            document for document in duplicates if document["_id"] not in remaining
                        ]
                    await self._reconcile_registry_file_ids(staged_ids)
                    await asyncio.to_thread(_clear_cleanup_registry_ids, spool_path, staged_ids)
                    keep_spool = False
                    await self.increment_language_counters(confirmed_deleted, -1)
                    deleted += len(confirmed_deleted)

                scanned += len(batch)
                if progress_callback:
                    elapsed = time.monotonic() - started
                    await progress_callback(
                        {
                            "phase": "verified_cleanup",
                            "scanned": scanned,
                            "total": total,
                            "deleted": deleted,
                            "cluster": cluster_index,
                            "clusters": len(self.file_cols),
                            "elapsed": elapsed,
                        }
                    )

            try:
                for cluster_index in range(len(self.file_cols), 0, -1):
                    col = self.file_cols[cluster_index - 1]
                    batch = []
                    cursor = (
                        col.find(
                            {
                                "file_name": {"$exists": True, "$ne": ""},
                                "file_id": {"$exists": True, "$ne": ""},
                            },
                            {
                                "file_name": 1,
                                "file_id": 1,
                                "file_unique_id": 1,
                                "file_size": 1,
                                "mime_type": 1,
                            },
                        )
                        .sort("_id", -1)
                        .batch_size(_VERIFIED_CLEANUP_BATCH_SIZE)
                    )
                    async for document in cursor:
                        batch.append(document)
                        if len(batch) >= _VERIFIED_CLEANUP_BATCH_SIZE:
                            await _process_batch(col, batch, cluster_index)
                            batch = []
                    if batch:
                        await _process_batch(col, batch, cluster_index)
            finally:
                try:
                    pending = await asyncio.to_thread(_read_cleanup_registry_ids, spool_path, 1)
                except Exception:
                    pending = ["unknown"]
                if not pending and not keep_spool:
                    await asyncio.to_thread(Path(spool_path).unlink, missing_ok=True)
                else:
                    logger.warning("Retained cleanup recovery spool %s", spool_path)

            self._invalidate_file_count()
            return {
                "scanned": scanned,
                "deleted": deleted,
                "remaining": await self.get_total_files(),
                "report_only_matches_untouched": True,
            }

    async def delete_duplicates_all(self, progress_callback=None):
        """Compatibility wrapper for strict Telegram-identity cleanup only."""
        return await self.delete_verified_duplicates(progress_callback)

    async def purge_by_pattern(self, pattern):
        deleted_total = 0
        deleted_ids = []
        for col in self.file_cols:
            try:
                deleted_documents = []
                cursor = col.find(
                    {"file_name": {"$regex": pattern, "$options": "i"}},
                    {"file_id": 1, "file_name": 1, "language_tags": 1},
                )
                async for doc in cursor:
                    deleted_documents.append(doc)
                    if doc.get("file_id"):
                        deleted_ids.append(doc["file_id"])
                result = await col.delete_many({"file_name": {"$regex": pattern, "$options": "i"}})
                deleted_total += result.deleted_count
                if result.deleted_count:
                    await self.increment_language_counters(
                        deleted_documents[: result.deleted_count], -1
                    )
            except Exception as e:
                logger.warning("Pattern purge skipped an unavailable cluster: %s", e)
        await self._release_registry_ids(deleted_ids)
        if deleted_total:
            self._invalidate_file_count()
        return deleted_total

    async def count_by_pattern(self, pattern):
        total = 0
        for col in self.file_cols:
            try:
                total += await col.count_documents({"file_name": {"$regex": pattern, "$options": "i"}})
            except Exception:
                pass
        return total

    async def migrate_cluster(self, from_idx: int, to_idx: int, batch_size=100):
        """Moves every document from cluster from_idx to cluster to_idx.
        Each batch is only deleted from the source *after* it's confirmed
        copied to the destination — otherwise a file's file_id (already
        claimed in file_registry from when it was first saved) would end up
        physically duplicated across two clusters instead of moved, with no
        way to tell which copy is the "real" one."""
        if from_idx >= len(self.file_cols) or to_idx >= len(self.file_cols):
            return 0, 0
        to_size = await self.get_db_size(self.dbs[to_idx])
        if to_size >= 450:
            return 0, -1

        migrated, skipped = 0, 0

        async def _flush(batch):
            nonlocal migrated, skipped
            if not batch:
                return
            docs = [entry["doc"] for entry in batch]
            try:
                await self.file_cols[to_idx].insert_many(docs, ordered=False)
                copied_src_ids = [entry["_id"] for entry in batch]
            except BulkWriteError as bwe:
                write_errors = bwe.details.get("writeErrors", [])
                failed_idx = {e["index"] for e in write_errors}
                copied_src_ids = [entry["_id"] for idx, entry in enumerate(batch) if idx not in failed_idx]
                skipped += len(failed_idx)
                if len(failed_idx) < len(write_errors):
                    logger.warning(
                        f"Migration batch: {len(write_errors)} write errors, {len(failed_idx)} unique docs failed"
                    )
            except Exception as e:
                logger.warning(f"Migration batch error: {e}")
                skipped += len(batch)
                return

            if copied_src_ids:
                copied_id_set = set(copied_src_ids)
                copied_file_ids = [
                    entry["doc"].get("file_id")
                    for entry in batch
                    if entry["_id"] in copied_id_set and entry["doc"].get("file_id")
                ]
                if copied_file_ids:
                    stored_docs = (
                        await self.file_cols[to_idx]
                        .find(
                            {"file_id": {"$in": copied_file_ids}},
                            {"_id": 1, "file_id": 1},
                        )
                        .to_list(length=len(copied_file_ids))
                    )
                    await self._mark_registry_locations(stored_docs, to_idx)
                try:
                    result = await self.file_cols[from_idx].delete_many({"_id": {"$in": copied_src_ids}})
                    migrated += result.deleted_count
                except Exception as e:
                    # Docs are now confirmed present in the destination but
                    # couldn't be removed from the source — they exist in
                    # both clusters until a retry cleans up the source.
                    logger.error(
                        f"Migration source-cleanup failed for {len(copied_src_ids)} doc(s) — now duplicated across clusters {from_idx + 1}/{to_idx + 1}: {e}"
                    )

        batch = []
        async for doc in self.file_cols[from_idx].find({}):
            src_id = doc.pop("_id")
            batch.append({"_id": src_id, "doc": doc})
            if len(batch) >= batch_size:
                await _flush(batch)
                batch = []
        await _flush(batch)

        if migrated:
            self._invalidate_file_count()
        return migrated, skipped

    async def _indexed_token_search(self, token_groups, max_results, cursor=None):
        """Search indexed tokens with a per-shard keyset cursor.

        No user query scans ``file_name`` and no Mongo cursor calls ``skip``.
        Each continuation supplies the last ``_id`` observed on every shard,
        so work remains proportional to the requested page instead of the
        number of earlier results.
        """
        filters = [{"search_tokens": {"$all": list(dict.fromkeys(group))}} for group in token_groups if group]
        if not filters:
            return [], None
        base_filter = filters[0] if len(filters) == 1 else {"$or": filters}
        limit = max_results + 1
        cursor = cursor if isinstance(cursor, dict) else {}

        async def _search_cluster(index, col):
            try:
                filter_mongo = dict(base_filter)
                raw_after = cursor.get(str(index))
                if raw_after:
                    try:
                        filter_mongo["_id"] = {"$lt": ObjectId(str(raw_after))}
                    except Exception:
                        logger.warning("Ignored malformed search cursor for Cluster %s", index + 1)
                # A cold/unindexed Atlas shard must not make every healthy
                # shard wait five or thirty seconds. Mongo gets its own time
                # budget and asyncio provides a hard client-side ceiling.
                # The reference implementation does not impose a MongoDB
                # deadline. Keep a bounded budget for fault isolation, but
                # allow enough time for the largest Atlas shard to complete
                # an ordered multi-word scan (measured at roughly 4 seconds).
                async with asyncio.timeout(4.0):
                    mongo_cursor = (
                        col.find(filter_mongo).sort("_id", -1).limit(limit).max_time_ms(3000)
                    )
                    documents = [doc async for doc in mongo_cursor]
                    self.mark_shard_reachable(index, "search_ok")
                    return index, documents
            except TimeoutError:
                logger.warning(
                    "Indexed search Cluster %s skipped after the 4s latency budget",
                    index + 1,
                )
                return index, []
            except Exception as e:
                # A secondary cluster outage should degrade result coverage,
                # not take the entire search feature down.
                self.mark_shard_error(index, e)
                logger.warning(
                    "Indexed search Cluster %s failed error_type=%s",
                    index + 1,
                    type(e).__name__,
                )
                return index, []

        await self.hydrate_shard_health()
        readable = self.readable_shard_indices()
        cluster_results = await asyncio.gather(
            *[_search_cluster(index, self.file_cols[index]) for index in readable]
        )
        merged = sorted(
            (
                (document["_id"], index, document)
                for index, shard_documents in cluster_results
                for document in shard_documents
            ),
            key=lambda item: item[0],
            reverse=True,
        )
        consumed = merged[:max_results]
        next_cursor = dict(cursor)
        for _, index, document in consumed:
            next_cursor[str(index)] = str(document["_id"])
        files = deduplicate_search_results([document for _, _, document in consumed])
        has_more = len(merged) > len(consumed) or any(
            len(shard_documents) > max_results
            for _, shard_documents in cluster_results
        )
        return files[:max_results], next_cursor if has_more else None

    async def _legacy_regex_search_page(self, pattern, max_results, cursor=None):
        """Bounded keyset search for rows created before ``search_tokens``.

        This compatibility route is used only while legacy rows remain.  It
        deliberately preserves the cursor guarantees of the indexed engine:
        every shard has a hard deadline and pagination never calls ``skip``.
        New and re-indexed files continue to use the multikey token index.
        """
        limit = max_results + 1
        cursor = cursor if isinstance(cursor, dict) else {}

        async def _search_cluster(index, col):
            filter_mongo = {"file_name": pattern}
            raw_after = cursor.get(str(index))
            if raw_after:
                try:
                    filter_mongo["_id"] = {"$lt": ObjectId(str(raw_after))}
                except Exception:
                    logger.warning("Ignored malformed legacy cursor for Cluster %s", index + 1)
            try:
                async with asyncio.timeout(4.0):
                    mongo_cursor = (
                        col.find(filter_mongo).sort("_id", -1).limit(limit).max_time_ms(3000)
                    )
                    documents = [doc async for doc in mongo_cursor]
                self.mark_shard_reachable(index, "legacy_search_ok")
                return index, documents
            except TimeoutError:
                logger.warning(
                    "Legacy search Cluster %s skipped after the 4s latency budget",
                    index + 1,
                )
                return index, []
            except Exception as exc:
                self.mark_shard_error(index, exc)
                logger.warning(
                    "Legacy search Cluster %s failed error_type=%s",
                    index + 1,
                    type(exc).__name__,
                )
                return index, []

        await self.hydrate_shard_health()
        readable = self.readable_shard_indices()
        cluster_results = await asyncio.gather(
            *[_search_cluster(index, self.file_cols[index]) for index in readable]
        )
        merged = sorted(
            (
                (document["_id"], index, document)
                for index, shard_documents in cluster_results
                for document in shard_documents
            ),
            key=lambda item: item[0],
            reverse=True,
        )
        consumed = merged[:max_results]
        next_cursor = dict(cursor)
        for _, index, document in consumed:
            next_cursor[str(index)] = str(document["_id"])
        files = deduplicate_search_results([document for _, _, document in consumed])
        has_more = len(merged) > len(consumed) or any(
            len(shard_documents) > max_results
            for _, shard_documents in cluster_results
        )
        return files[:max_results], next_cursor if has_more else None

    async def _legacy_search_page(self, query, max_results=40, cursor=None):
        """Search old rows safely without expanding nearly-full movie shards."""
        if isinstance(query, list):
            raw_pattern = "|".join(
                re.escape(str(item).strip()) for item in query if str(item).strip()
            )
            if not raw_pattern:
                return {"results": [], "next_cursor": None}
            results, next_cursor = await self._legacy_regex_search_page(
                compile_regex(raw_pattern), max_results, cursor
            )
            return {"results": results, "next_cursor": next_cursor}

        query = str(query).strip()
        words = [word for word in query.split() if word][:12]
        if not words:
            return {"results": [], "next_cursor": None}
        candidate_limit = max(80, max_results * 2)
        candidates, next_cursor = await self._legacy_regex_search_page(
            compile_regex(_reference_search_pattern(words)), candidate_limit, cursor
        )
        ranked = rank_search_results(query, candidates, max_results)
        if ranked or cursor:
            return {"results": ranked, "next_cursor": next_cursor}

        title_words = [word for word in words if not _is_optional_search_token(word)]
        if title_words and title_words != words:
            candidates, next_cursor = await self._legacy_regex_search_page(
                compile_regex(_reference_search_pattern(title_words)), candidate_limit, None
            )
            ranked = rank_search_results(query, candidates, max_results)
            if ranked:
                return {"results": ranked, "next_cursor": next_cursor}

        query_identity = _fuzzy_query_identity(query)
        for suggestion in await self.suggest_search_titles(query, limit=3):
            if suggestion == query_identity:
                continue
            suggestion_words = suggestion.split()[:12]
            if not suggestion_words:
                continue
            candidates, next_cursor = await self._legacy_regex_search_page(
                compile_regex(_reference_search_pattern(suggestion_words)),
                candidate_limit,
                None,
            )
            ranked = rank_search_results(query, candidates, max_results)
            if ranked:
                return {"results": ranked, "next_cursor": next_cursor}
        return {"results": [], "next_cursor": None}

    async def _get_search_page_uncached(self, query, max_results=40, cursor=None):
        """Use indexed Mongo candidates followed by fuzzy title ranking."""
        if not getattr(self, "_search_tokens_complete", True):
            return await self._legacy_search_page(query, max_results, cursor)
        if isinstance(query, list):
            token_groups = [search_tokens_for_name(str(item))[:12] for item in query if str(item).strip()]
            results, next_cursor = await self._indexed_token_search(
                token_groups, max_results, cursor
            )
            return {"results": results, "next_cursor": next_cursor}

        query = query.strip()
        if not query:
            return {"results": [], "next_cursor": None}
        words = search_tokens_for_name(query)[:12]
        if not words:
            return {"results": [], "next_cursor": None}
        candidate_limit = max(80, max_results * 2)

        candidates, next_cursor = await self._indexed_token_search(
            [words], candidate_limit, cursor
        )
        ranked = rank_search_results(query, candidates, max_results)
        if ranked or cursor:
            return {"results": ranked, "next_cursor": next_cursor}

        # Real libraries are inconsistent about release metadata. The
        # reference query is always attempted first; if it misses, retry the
        # same ordered search after removing only optional year/language/
        # quality tokens. Title, season and episode words are never relaxed.
        title_words = [word for word in words if not _is_optional_search_token(word)]
        if title_words and title_words != words:
            candidates, next_cursor = await self._indexed_token_search(
                [title_words], candidate_limit, cursor
            )
            ranked = rank_search_results(query, candidates, max_results)
            if ranked:
                return {"results": ranked, "next_cursor": next_cursor}

        # Typo fallback: RapidFuzz compares against the compact catalog of
        # unique visible titles, then Mongo performs the normal precise lookup
        # for the corrected title. No fuzzy scan ever touches all file rows.
        query_identity = _fuzzy_query_identity(query)
        for suggestion in await self.suggest_search_titles(query, limit=3):
            if suggestion == query_identity:
                continue
            suggestion_words = suggestion.split()[:12]
            if not suggestion_words:
                continue
            candidates, next_cursor = await self._indexed_token_search(
                [suggestion_words], candidate_limit, None
            )
            ranked = rank_search_results(query, candidates, max_results)
            if ranked:
                return {"results": ranked, "next_cursor": next_cursor}
        return {"results": [], "next_cursor": None}

    async def suggest_search_titles(self, query, limit=3):
        """Run bounded-catalog RapidFuzz work outside the event loop."""
        semaphore = getattr(self, "_fuzzy_worker_semaphore", None)
        if semaphore is None:
            return await asyncio.to_thread(suggest_search_titles, query, limit)
        async with semaphore:
            return await asyncio.to_thread(suggest_search_titles, query, limit)

    async def get_search_page(self, query, max_results=40, cursor=None):
        """Return a Redis-cached keyset page shared by all bot replicas."""
        if isinstance(query, list):
            normalized_query = tuple(str(item).strip().lower() for item in query)
        else:
            normalized_query = str(query).strip().lower()
        cache_key = stable_cache_key(normalized_query, int(max_results), cursor or {})
        cached = await redis_state.get_json("search-query", cache_key)
        if cached is not None:
            return cached
        page = await self._get_search_page_uncached(query, max_results, cursor)
        await redis_state.set_json("search-query", cache_key, page, ttl=120)
        return page

    async def get_search_results(self, query, max_results=40, offset=0):
        """Compatibility API implemented with keyset pages, never ``skip``."""
        remaining = max(0, int(offset))
        cursor = None
        while True:
            page_size = max(int(max_results), min(100, remaining + int(max_results)))
            page = await self.get_search_page(query, page_size, cursor)
            results = page["results"]
            if remaining < len(results):
                return results[remaining : remaining + int(max_results)]
            remaining -= len(results)
            cursor = page.get("next_cursor")
            if not cursor or not results:
                return []

    async def get_prefix_suggestions(self, query, limit=3):
        return await self.suggest_search_titles(query, limit=limit)

    async def get_file(self, file_obj_id):
        try:
            obj_id = ObjectId(file_obj_id)
        except Exception:
            return None

        async def _bounded_find(collection, query, timeout_seconds, shard_index=None):
            try:
                async with asyncio.timeout(timeout_seconds):
                    document = await collection.find_one(
                        query, max_time_ms=max(100, int(timeout_seconds * 800))
                    )
                if shard_index is not None:
                    self.mark_shard_reachable(shard_index, "file_lookup_ok")
                return document
            except TimeoutError:
                return None
            except Exception as exc:
                if shard_index is not None:
                    self.mark_shard_error(shard_index, exc)
                logger.warning(
                    "Bounded file lookup failed error_type=%s",
                    type(exc).__name__,
                )
                return None

        registry_doc = None
        if self.registry_col is not None:
            registry_doc = await _bounded_find(self.registry_col, {"movie_id": str(obj_id)}, 1.5)
        if registry_doc:
            cluster_number = registry_doc.get("cluster")
            if isinstance(cluster_number, int) and 1 <= cluster_number <= len(self.file_cols):
                shard_index = cluster_number - 1
                if shard_index not in self.readable_shard_indices():
                    doc = None
                else:
                    doc = await _bounded_find(
                        self.file_cols[shard_index], {"_id": obj_id}, 2.5, shard_index
                    )
                if doc:
                    return doc

        async def _fallback(shard_index, collection):
            document = await _bounded_find(collection, {"_id": obj_id}, 3.0, shard_index)
            return shard_index + 1, document

        readable = self.readable_shard_indices()
        lookups = await asyncio.gather(
            *[_fallback(index, self.file_cols[index]) for index in readable]
        )
        found = next(
            (
                (cluster, document)
                for cluster, document in lookups
                if document
            ),
            None,
        )
        if found is None:
            return None

        cluster_number, document = found
        if self.registry_col is not None and document.get("file_id"):
            try:
                async with asyncio.timeout(2.0):
                    await self.registry_col.update_one(
                        {"file_id": document["file_id"]},
                        {
                            "$set": {
                                "cluster": cluster_number,
                                "movie_id": str(document["_id"]),
                            }
                        },
                    )
            except Exception as exc:
                logger.warning(
                    "Registry lookup repair deferred file_id=%s error_type=%s",
                    document.get("file_id"),
                    type(exc).__name__,
                )
        return document

    async def delete_file_by_id(self, file_id):
        deleted = 0
        for col in self.file_cols:
            documents = []
            try:
                if hasattr(col, "find"):
                    documents = [
                        document
                        async for document in col.find(
                            {"file_id": file_id},
                            {"file_name": 1, "language_tags": 1},
                        )
                    ]
            except Exception as error:
                logger.warning(
                    "Language-counter lookup failed before file delete: %s",
                    type(error).__name__,
                )
            try:
                result = await col.delete_many({"file_id": file_id})
                deleted += result.deleted_count
                if result.deleted_count and documents:
                    await self.increment_language_counters(
                        documents[: result.deleted_count], -1
                    )
            except Exception as e:
                logger.warning("File-id delete skipped an unavailable cluster: %s", e)
        if deleted:
            await self._release_registry_ids([file_id])
            self._invalidate_file_count()
        return deleted > 0

    async def log_missed_search(self, query: str):
        if self.main_db is None:
            return False
        col = self.main_db["missed_searches"]
        cleaned = re.sub(r"[^a-zA-Z0-9 ]", "", query.lower()).strip()
        if not cleaned:
            return False
        now = time.time()
        cooldown = 3600
        existing = await col.find_one({"_id": cleaned})
        should_alert = existing is None or (now - existing.get("last_alerted", 0)) > cooldown
        # last_searched_at (a real BSON date) drives the TTL index in
        # ensure_indexes() — last_searched (epoch float) is kept as-is for
        # any existing reader of that field; MongoDB TTL only acts on Date
        # fields, so the two coexist rather than replacing one with the other.
        update = {
            "$inc": {"count": 1},
            "$set": {"last_searched": now, "original": query, "last_searched_at": datetime.now(timezone.utc)},
        }
        if should_alert:
            update["$set"]["last_alerted"] = now
        try:
            await col.update_one({"_id": cleaned}, update, upsert=True)
        except Exception:
            pass
        return should_alert

    async def get_top_missed(self, limit=15):
        if self.main_db is None:
            return []
        cursor = self.main_db["missed_searches"].find({}).sort("count", -1).limit(limit)
        return [doc async for doc in cursor]

    async def clear_missed_search(self, query_id: str):
        if self.main_db is None:
            return
        await self.main_db["missed_searches"].delete_one({"_id": query_id})

    async def get_bot_stats(self):
        async def _banned_count():
            return await self.banned_col.count_documents({}) if self.banned_col is not None else 0

        async def _control_count(label, awaitable):
            cache = getattr(self, "_operations_stats_cache", {})
            try:
                async with asyncio.timeout(SHARD_OPERATION_TIMEOUT_SECONDS):
                    value = int(await awaitable)
                cache[label] = value
                self._operations_stats_cache = cache
                return value
            except Exception as exc:
                logger.warning(
                    "Analytics using last-known %s count error_type=%s",
                    label,
                    type(exc).__name__,
                )
                return int(cache.get(label, 0))

        async def _cluster_stats(i, db_instance):
            if i not in self.readable_shard_indices():
                snapshot = self.shard_health_snapshot()
                cached_size = snapshot[i].get("size_mb") if i < len(snapshot) else None
                count_cache = getattr(self, "_cluster_file_count_cache", {})
                return i + 1, int(count_cache.get(i, 0)), cached_size
            try:
                async with asyncio.timeout(SHARD_OPERATION_TIMEOUT_SECONDS):
                    files_in_db = await self.file_cols[i].estimated_document_count()
                count_cache = getattr(self, "_cluster_file_count_cache", {})
                count_cache[i] = files_in_db
                self._cluster_file_count_cache = count_cache
                async with asyncio.timeout(SHARD_OPERATION_TIMEOUT_SECONDS):
                    size = await self.get_db_size(db_instance)
                self.mark_shard_reachable(i, "analytics_ok")
                return i + 1, files_in_db, size
            except TimeoutError:
                snapshot = self.shard_health_snapshot()
                cached_size = snapshot[i].get("size_mb") if i < len(snapshot) else None
                count_cache = getattr(self, "_cluster_file_count_cache", {})
                logger.warning("Analytics timed out on Cluster %s; using last-known values", i + 1)
                return i + 1, int(count_cache.get(i, 0)), cached_size
            except Exception as exc:
                self.mark_shard_error(i, exc)
                snapshot = self.shard_health_snapshot()
                cached_size = snapshot[i].get("size_mb") if i < len(snapshot) else None
                count_cache = getattr(self, "_cluster_file_count_cache", {})
                return i + 1, int(count_cache.get(i, 0)), cached_size

        total_users, total_banned, total_groups, *cluster_stats = await asyncio.gather(
            _control_count("users", self.get_user_count()),
            _control_count("banned", _banned_count()),
            _control_count("groups", self.get_group_count()),
            *[_cluster_stats(i, db_instance) for i, db_instance in enumerate(self.dbs)],
        )

        total_files = sum(files for _, files, _ in cluster_stats)
        self._file_count_cache = (time.time(), total_files)
        db_sizes = [(idx, size) for idx, _, size in cluster_stats]
        return total_users, total_banned, total_files, db_sizes, total_groups

    async def reset_database(self):
        # Avoid starting a cross-cluster destructive operation when one of the
        # targets is already unreachable.
        await asyncio.gather(*[db_instance.command("ping") for db_instance in self.dbs])
        if self.users_col is not None:
            await self.users_col.drop()
        if self.banned_col is not None:
            await self.banned_col.drop()
        for col in self.file_cols:
            await col.drop()
        if self.registry_col is not None:
            await self.registry_col.drop()
        if getattr(self, "analytics_col", None) is not None:
            await self.analytics_col.delete_one({"_id": "files_by_language"})
        # Recreate uniqueness indexes immediately so a reset cannot leave a
        # running bot accepting duplicate file IDs.
        await self.ensure_indexes()
        self._invalidate_file_count()
        return True

    async def get_config(self):
        global _config_cache, _config_cache_ts, _last_valid_config
        now = time.time()
        if _config_cache is not None and (now - _config_cache_ts) < _CONFIG_TTL:
            return _config_cache
        if self.config_col is None:
            return _last_valid_config or {}
        try:
            config = await self.config_col.find_one({"_id": "bot_config"})
        except Exception as exc:
            if _last_valid_config is not None:
                logger.warning(
                    "Config refresh failed; using last valid snapshot error_type=%s",
                    type(exc).__name__,
                )
                return _last_valid_config
            raise
        if not config:
            config = {
                "_id": "bot_config",
                "start_media": "https://files.catbox.moe/wvdeci.mp4",
                "fsub_channels": [],
                "db_channels": [],
                "auto_delete_time": 300,
                "maintenance_mode": False,
                "maintenance_message": "🔧 Bot is under maintenance. Back soon!",
                "file_caption_template": "",
                "request_channel_id": 0,
            }
            await self.config_col.insert_one(config)
        _config_cache = config
        _last_valid_config = config
        _config_cache_ts = now
        return config

    async def update_config(self, key, value):
        global _config_cache, _config_cache_ts, _last_valid_config
        if self.config_col is None:
            return False
        await self.config_col.update_one({"_id": "bot_config"}, {"$set": {key: value}}, upsert=True)
        updated = dict(_last_valid_config or _config_cache or {"_id": "bot_config"})
        updated[key] = value
        _last_valid_config = updated
        _config_cache = updated
        _config_cache_ts = time.time()
        if key in {
            "fsub_channels",
            "req_fsub_channels",
            "req_fsub_interval_hours",
            "two_stage_channels",
        }:
            await self._sync_access_gates_from_legacy()
        return True

    async def update_config_fields(self, values: dict):
        """Atomically update related settings and refresh the shared cache."""
        global _config_cache, _config_cache_ts, _last_valid_config
        if self.config_col is None or not values:
            return False
        clean_values = {str(key): value for key, value in values.items()}
        await self.config_col.update_one(
            {"_id": "bot_config"}, {"$set": clean_values}, upsert=True
        )
        updated = dict(_last_valid_config or _config_cache or {"_id": "bot_config"})
        updated.update(clean_values)
        _last_valid_config = updated
        _config_cache = updated
        _config_cache_ts = time.time()
        return True

    async def _sync_access_gates_from_legacy(self):
        """Keep legacy admin actions reflected in the canonical gate list."""
        if self.config_col is None:
            return
        from plugins.access_gates import ACCESS_GATES_SCHEMA_VERSION, legacy_access_gates

        config = await self.config_col.find_one({"_id": "bot_config"}) or {}
        await self.config_col.update_one(
            {"_id": "bot_config"},
            {
                "$set": {
                    "access_gates": legacy_access_gates(config),
                    "access_gates_schema_version": ACCESS_GATES_SCHEMA_VERSION,
                    "access_gates_updated_at": datetime.now(timezone.utc),
                }
            },
            upsert=True,
        )
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0

    async def get_verification_cache(self, user_id: int, gate_keys: list[str]) -> dict:
        if self.verification_cache_col is None or not gate_keys:
            return {}
        documents = await self.verification_cache_col.find(
            {"user_id": int(user_id), "gate_key": {"$in": list(gate_keys)}}
        ).to_list(length=len(gate_keys))
        return {str(document.get("gate_key")): document for document in documents}

    async def mark_gate_verified(
        self,
        user_id: int,
        gate_key: str,
        interval_seconds: int,
        grace_seconds: int = 900,
    ):
        if self.verification_cache_col is None:
            return False
        now = datetime.now(timezone.utc)
        valid_until = datetime.fromtimestamp(now.timestamp() + max(60, int(interval_seconds)), timezone.utc)
        grace_until = datetime.fromtimestamp(valid_until.timestamp() + max(0, int(grace_seconds)), timezone.utc)
        await self.verification_cache_col.update_one(
            {"user_id": int(user_id), "gate_key": str(gate_key)},
            {
                "$set": {
                    "verified_at": now,
                    "valid_until": valid_until,
                    "grace_until": grace_until,
                }
            },
            upsert=True,
        )
        return {"valid_until": valid_until, "grace_until": grace_until}

    async def invalidate_gate_verification(self, user_id: int, gate_key: str):
        if self.verification_cache_col is None:
            return False
        await self.verification_cache_col.delete_one(
            {"user_id": int(user_id), "gate_key": str(gate_key)}
        )
        return True

    async def update_access_gate_link(self, gate_key: str, link: str):
        if self.config_col is None:
            return False
        await self.config_col.update_one(
            {"_id": "bot_config", "access_gates.key": str(gate_key)},
            {"$set": {"access_gates.$.link": str(link)}},
        )
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0
        return True

    async def export_config(self, *, include_private_invites=False):
        config = await self.get_config()
        exclude = {
            "_id",
            "log_channel",
            "request_channel_id",
            "admin_id",
            "db_channels",
            "update_channel_id",
            "db_channel",
        }
        safe = {k: v for k, v in config.items() if k not in exclude}
        if include_private_invites:
            return safe

        # Ordinary backups are shareable configuration artifacts, not secret
        # containers. Strip cached gate links and recursively redact any
        # private invite embedded in another configurable string.
        for channel_key in ("fsub_channels", "req_fsub_channels", "two_stage_channels"):
            if channel_key in safe:
                safe[channel_key] = [
                    ({"id": e.get("id")} if isinstance(e, dict) else e) for e in safe[channel_key]
                ]
        return redact_private_invites(safe)

    async def restore_config(self, data: dict):
        global _config_cache, _config_cache_ts
        safe_data = validate_config_restore(data)
        if not safe_data or self.config_col is None:
            return False
        await self.config_col.update_one({"_id": "bot_config"}, {"$set": safe_data}, upsert=True)
        _config_cache = None
        _config_cache_ts = 0.0
        return True

    async def add_fsub_channel(self, channel_id, link=None):
        if self.config_col is None:
            return False
        entry = {"id": channel_id}
        if isinstance(link, str) and link.startswith("https://t.me/"):
            entry["link"] = link
        await self.config_col.update_one(
            {"_id": "bot_config"}, {"$pull": {"fsub_channels": {"id": channel_id}}}
        )
        await self.config_col.update_one({"_id": "bot_config"}, {"$pull": {"fsub_channels": channel_id}})
        await self.config_col.update_one(
            {"_id": "bot_config"}, {"$push": {"fsub_channels": entry}}, upsert=True
        )
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0
        await self._sync_access_gates_from_legacy()
        return True

    async def update_fsub_channel_link(self, channel_id, link):
        if self.config_col is None:
            return
        config = await self.config_col.find_one({"_id": "bot_config"})
        if not config:
            return
        channels = config.get("fsub_channels", [])
        updated = []
        for entry in channels:
            if isinstance(entry, dict) and entry.get("id") == channel_id:
                entry["link"] = link
            updated.append(entry)
        await self.config_col.update_one({"_id": "bot_config"}, {"$set": {"fsub_channels": updated}})
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0
        await self._sync_access_gates_from_legacy()

    async def remove_fsub_channel(self, channel_id):
        if self.config_col is None:
            return False
        dict_result = await self.config_col.update_one(
            {"_id": "bot_config"}, {"$pull": {"fsub_channels": {"id": channel_id}}}
        )
        scalar_result = await self.config_col.update_one(
            {"_id": "bot_config"}, {"$pull": {"fsub_channels": channel_id}}
        )
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0
        await self._sync_access_gates_from_legacy()
        return bool(dict_result.modified_count or scalar_result.modified_count)

    async def add_db_channel(self, channel_id):
        if self.config_col is None:
            return False
        await self.config_col.update_one(
            {"_id": "bot_config"}, {"$addToSet": {"db_channels": channel_id}}, upsert=True
        )
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0
        return True

    async def remove_db_channel(self, channel_id):
        if self.config_col is None:
            return False
        await self.config_col.update_one({"_id": "bot_config"}, {"$pull": {"db_channels": channel_id}})
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0
        return True

    async def add_req_fsub_channel(self, channel_id, link, title=""):
        if self.config_col is None:
            return False, "No DB"
        try:
            channel_id = int(channel_id)
        except (TypeError, ValueError):
            return False, "A verified numeric channel ID is required"
        if not isinstance(link, str) or not link.startswith("https://t.me/"):
            return False, "A verified Telegram invite/public link is required"
        config = await self.config_col.find_one({"_id": "bot_config"})
        existing = config.get("req_fsub_channels", []) if config else []
        for e in existing:
            eid = e.get("id") if isinstance(e, dict) else e
            if str(eid) == str(channel_id):
                return False, "That channel is already configured"
        if len(existing) >= 5:
            return False, "Remove one timed channel before adding another (maximum 5)"
        entry = {"id": channel_id, "link": link, "title": str(title)[:100]}
        await self.config_col.update_one(
            {"_id": "bot_config"},
            {"$push": {"req_fsub_channels": entry}},
            upsert=True,
        )
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0
        await self._sync_access_gates_from_legacy()
        return True, "Added"

    async def remove_req_fsub_channel(self, channel_id):
        if self.config_col is None:
            return False
        dict_result = await self.config_col.update_one(
            {"_id": "bot_config"}, {"$pull": {"req_fsub_channels": {"id": channel_id}}}
        )
        scalar_result = await self.config_col.update_one(
            {"_id": "bot_config"}, {"$pull": {"req_fsub_channels": channel_id}}
        )
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0
        await self._sync_access_gates_from_legacy()
        return bool(dict_result.modified_count or scalar_result.modified_count)

    async def update_req_fsub_link(self, channel_id, link):
        if self.config_col is None:
            return
        config = await self.config_col.find_one({"_id": "bot_config"})
        if not config:
            return
        channels = config.get("req_fsub_channels", [])
        updated = []
        for entry in channels:
            if isinstance(entry, dict) and str(entry.get("id")) == str(channel_id):
                entry["link"] = link
            updated.append(entry)
        await self.config_col.update_one({"_id": "bot_config"}, {"$set": {"req_fsub_channels": updated}})
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0
        await self._sync_access_gates_from_legacy()

    async def set_two_stage_channel(self, slot: int, channel_id, link=None) -> bool:
        """slot is 1 or 2 — a fixed 2-slot list (unlike fsub_channels/
        req_fsub_channels, which are appendable pools), since the Two-Stage
        Verification gate is specifically a sequential Channel-1-then-
        Channel-2 flow, not "join any N of these"."""
        if self.config_col is None:
            return False
        config = await self.config_col.find_one({"_id": "bot_config"})
        channels = list(config.get("two_stage_channels", [])) if config else []
        while len(channels) < 2:
            channels.append(None)
        entry = {"id": channel_id}
        if isinstance(link, str) and link.startswith("https://t.me/"):
            entry["link"] = link
        channels[slot - 1] = entry
        await self.config_col.update_one(
            {"_id": "bot_config"}, {"$set": {"two_stage_channels": channels}}, upsert=True
        )
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0
        await self._sync_access_gates_from_legacy()
        return True

    async def remove_two_stage_channel(self, slot: int) -> bool:
        if self.config_col is None:
            return False
        config = await self.config_col.find_one({"_id": "bot_config"})
        channels = list(config.get("two_stage_channels", [])) if config else []
        while len(channels) < 2:
            channels.append(None)
        channels[slot - 1] = None
        await self.config_col.update_one(
            {"_id": "bot_config"}, {"$set": {"two_stage_channels": channels}}, upsert=True
        )
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0
        await self._sync_access_gates_from_legacy()
        return True

    async def update_two_stage_channel_link(self, channel_id, link):
        if self.config_col is None:
            return
        config = await self.config_col.find_one({"_id": "bot_config"})
        if not config:
            return
        channels = config.get("two_stage_channels", [])
        updated = []
        for entry in channels:
            if isinstance(entry, dict) and str(entry.get("id")) == str(channel_id):
                entry["link"] = link
            updated.append(entry)
        await self.config_col.update_one({"_id": "bot_config"}, {"$set": {"two_stage_channels": updated}})
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0
        await self._sync_access_gates_from_legacy()

    async def get_two_stage_gate_status(self, user_id: int) -> VerificationResult:
        """Return PASS when cached verification is valid, DENY when due."""
        if self.users_col is None:
            return VerificationResult.indeterminate("users_collection_unavailable")
        try:
            doc = await self.users_col.find_one({"_id": user_id}, {"two_stage_verified_at": 1})
            last = doc.get("two_stage_verified_at", 0) if doc else 0
            if (time.time() - last) >= TWO_STAGE_VERIFY_INTERVAL:
                return VerificationResult.deny("two_stage_due")
            return VerificationResult.allow("two_stage_cached")
        except Exception as exc:
            logger.warning(
                "verification_indeterminate gate=two_stage_due error_type=%s",
                type(exc).__name__,
            )
            return VerificationResult.indeterminate("two_stage_state_unavailable")

    async def check_two_stage_due(self, user_id: int) -> bool:
        """Compatibility wrapper; indeterminate state is treated as due."""
        result = await self.get_two_stage_gate_status(user_id)
        return result.status is not VerificationStatus.PASS

    async def mark_two_stage_verified(self, user_id: int):
        if self.users_col is None:
            logger.warning("two_stage_cache_write_failed reason=users_collection_unavailable")
            return False
        try:
            await self.users_col.update_one(
                {"_id": user_id}, {"$set": {"two_stage_verified_at": time.time()}}, upsert=True
            )
            return True
        except Exception as exc:
            logger.warning("two_stage_cache_write_failed error_type=%s", type(exc).__name__)
            return False

    async def get_req_fsub_interval(self):
        config = await self.get_config()
        return int(config.get("req_fsub_interval_hours", 24)) * 3600

    async def get_req_fsub_gate_status(self, user_id: int) -> VerificationResult:
        if self.users_col is None:
            return VerificationResult.indeterminate("users_collection_unavailable")
        try:
            doc = await self.users_col.find_one({"_id": user_id}, {"req_fsub_last": 1})
            last = doc.get("req_fsub_last", 0) if doc else 0
            interval = await self.get_req_fsub_interval()
            if (time.time() - last) >= interval:
                return VerificationResult.deny("request_fsub_due")
            return VerificationResult.allow("request_fsub_cached")
        except Exception as exc:
            logger.warning(
                "verification_indeterminate gate=request_fsub_due error_type=%s",
                type(exc).__name__,
            )
            return VerificationResult.indeterminate("request_fsub_state_unavailable")

    async def check_req_fsub_due(self, user_id: int) -> bool:
        """Compatibility wrapper; indeterminate state is treated as due."""
        result = await self.get_req_fsub_gate_status(user_id)
        return result.status is not VerificationStatus.PASS

    async def mark_req_fsub_verified(self, user_id: int):
        if self.users_col is None:
            return
        try:
            await self.users_col.update_one(
                {"_id": user_id}, {"$set": {"req_fsub_last": time.time()}}, upsert=True
            )
        except Exception:
            pass

    async def get_user_language(self, user_id: int) -> str:
        """Persisted UI language preference — additive field on the existing
        users doc, same pattern as the old data_saver flag. Defaults to
        "en"; plugins/start.py's LANG_STRINGS defines what's translated."""
        if self.users_col is None:
            return "en"
        try:
            doc = await self.users_col.find_one({"_id": user_id}, {"language": 1})
            return doc.get("language", "en") if doc else "en"
        except Exception:
            return "en"

    async def set_user_language(self, user_id: int, lang: str):
        if self.users_col is None:
            return
        try:
            await self.users_col.update_one({"_id": user_id}, {"$set": {"language": lang}}, upsert=True)
        except Exception:
            pass

    async def save_pending_request(self, user_id, movie_name):
        if self.main_db is None:
            return
        requests_col = self.main_db["pending_requests"]
        # requested_at (a real BSON date) drives the TTL index in
        # ensure_indexes() — timestamp (epoch float) is kept as-is for any
        # existing reader of that field, same coexistence approach as
        # log_missed_search()'s last_searched_at above.
        await requests_col.update_one(
            {"user_id": user_id, "movie_name": movie_name.lower().strip()},
            {
                "$set": {
                    "user_id": user_id,
                    "movie_name": movie_name.lower().strip(),
                    "original_name": movie_name,
                    "timestamp": time.time(),
                    "requested_at": datetime.now(timezone.utc),
                }
            },
            upsert=True,
        )

    async def iter_matching_requests(self, file_name, page_size=25, max_matches=MAX_REQUEST_MATCHES_PER_JOB):
        if self.main_db is None:
            return
        requests_col = self.main_db["pending_requests"]
        clean = re.sub(r"[^a-zA-Z0-9 ]", " ", file_name)
        words = [w for w in clean.split() if len(w) >= 5 and not w.isdigit()]
        if not words:
            return
        conditions = [{"movie_name": {"$regex": word[:5], "$options": "i"}} for word in words[:3]]
        cursor = (
            requests_col.find({"$or": conditions})
            .limit(max(1, min(int(max_matches), MAX_REQUEST_MATCHES_PER_JOB)))
            .batch_size(max(1, min(int(page_size), 50)))
        )
        async for doc in cursor:
            yield {"user_id": doc["user_id"], "movie_name": doc["original_name"]}

    async def find_matching_requests(self, file_name, limit=MAX_REQUEST_MATCHES_PER_JOB):
        """Compatibility wrapper with a hard cap; prefer the streaming iterator."""
        return [match async for match in self.iter_matching_requests(file_name, max_matches=limit)]

    async def delete_pending_request(self, user_id, movie_name):
        if self.main_db is None:
            return
        await self.main_db["pending_requests"].delete_one(
            {"user_id": user_id, "movie_name": movie_name.lower().strip()}
        )

    async def pending_request_exists(self, user_id, movie_name) -> bool:
        """Used by the manual "Mark Uploaded" admin ticket flow to check
        whether _fulfill_matching_requests() already auto-fulfilled (and
        deleted) this same request before the admin got to it — avoids a
        duplicate "your movie is ready" ping to the user."""
        if self.main_db is None:
            return False
        doc = await self.main_db["pending_requests"].find_one(
            {"user_id": user_id, "movie_name": movie_name.lower().strip()}
        )
        return doc is not None

    async def set_index_progress(self, chat_id, msg_id):
        if self.main_db is None:
            raise RuntimeError("Operations database is unavailable")
        result = await self.main_db["settings"].update_one(
            {"_id": "index_progress"},
            {"$set": {str(chat_id): msg_id}},
            upsert=True,
        )
        if not result.acknowledged:
            raise RuntimeError("Index checkpoint write was not acknowledged")
        if self.indexer_col is not None:
            heartbeat = await self.indexer_col.update_one(
                {"_id": str(chat_id)},
                {
                    "$set": {
                        "updated": time.time(),
                        "checkpoint": int(msg_id),
                    }
                },
            )
            if not heartbeat.acknowledged:
                raise RuntimeError("Index heartbeat write was not acknowledged")
        return True

    async def record_index_failure(
        self,
        chat_id: int,
        start_id: int,
        end_id: int,
        stage: str,
        error: Exception,
        attempts: int,
    ):
        if self.index_failures_col is None:
            raise RuntimeError("Index failure collection is unavailable")
        failure_id = f"{chat_id}:{start_id}:{end_id}"
        await self.index_failures_col.update_one(
            {"_id": failure_id},
            {
                "$set": {
                    "chat_id": chat_id,
                    "start_id": start_id,
                    "end_id": end_id,
                    "stage": stage,
                    "error_type": type(error).__name__,
                    "attempts": attempts,
                    "status": "unresolved",
                    "updated_at": datetime.now(timezone.utc),
                },
                "$setOnInsert": {"created_at": datetime.now(timezone.utc)},
            },
            upsert=True,
        )

    async def resolve_index_failure(self, chat_id: int, start_id: int, end_id: int):
        if self.index_failures_col is not None:
            await self.index_failures_col.delete_one({"_id": f"{chat_id}:{start_id}:{end_id}"})

    async def get_index_progress(self, chat_id):
        if self.main_db is None:
            return 0
        data = await self.main_db["settings"].find_one({"_id": "index_progress"})
        return data.get(str(chat_id), 0) if data else 0

    async def clear_index_progress(self, chat_id=None):
        if self.main_db is None:
            return
        try:
            settings = self.main_db["settings"]
            if chat_id is None:
                await settings.delete_one({"_id": "index_progress"})
            else:
                await settings.update_one({"_id": "index_progress"}, {"$unset": {str(chat_id): ""}})
        except Exception as e:
            logger.warning(f"clear_index_progress failed: {e}")

    async def set_index_task(self, chat_id, state):
        if self.indexer_col is None:
            raise RuntimeError("Indexer task collection is unavailable")
        update = {"$set": {"state": state, "updated": time.time()}}
        if state == "queued":
            update["$unset"] = {"lock_token": "", "locked_until": ""}
        result = await self.indexer_col.update_one(
            {"_id": str(chat_id)},
            update,
            upsert=True,
        )
        if not result.acknowledged:
            raise RuntimeError("Indexer task update was not acknowledged")

    async def enqueue_index_task(
        self,
        chat_id: int,
        last_msg_id: int,
        start_id: int,
        admin_chat_id: int,
        status_message_id: int,
        requested_by: int,
    ):
        if self.indexer_col is None:
            raise RuntimeError("Indexer task collection is unavailable")
        now = time.time()
        await self.indexer_col.update_one(
            {"_id": str(chat_id)},
            {
                "$set": {
                    "chat_id": int(chat_id),
                    "last_msg_id": int(last_msg_id),
                    "start_id": int(start_id),
                    "admin_chat_id": int(admin_chat_id),
                    "status_message_id": int(status_message_id),
                    "requested_by": int(requested_by),
                    "state": "queued",
                    "updated": now,
                },
                "$setOnInsert": {"created": now},
                "$unset": {"lock_token": "", "locked_until": ""},
            },
            upsert=True,
        )

    async def claim_index_task(self, lease_seconds=90):
        if self.indexer_col is None:
            raise RuntimeError("Indexer task collection is unavailable")
        now = time.time()
        token = secrets.token_urlsafe(24)
        return await self.indexer_col.find_one_and_update(
            {
                "state": {"$in": ["queued", "running"]},
                "$or": [
                    {"locked_until": {"$exists": False}},
                    {"locked_until": {"$lte": now}},
                ],
            },
            {
                "$set": {
                    "state": "running",
                    "lock_token": token,
                    "locked_until": now + max(30, int(lease_seconds)),
                    "updated": now,
                }
            },
            sort=[("updated", 1)],
            return_document=ReturnDocument.AFTER,
        )

    async def renew_index_task(self, chat_id: int, lock_token: str, lease_seconds=90) -> bool:
        if self.indexer_col is None:
            return False
        result = await self.indexer_col.update_one(
            {"_id": str(chat_id), "lock_token": lock_token, "state": "running"},
            {
                "$set": {
                    "locked_until": time.time() + max(30, int(lease_seconds)),
                    "updated": time.time(),
                }
            },
        )
        return result.matched_count == 1

    async def release_index_task(self, chat_id: int, lock_token: str, state: str):
        if self.indexer_col is None:
            return False
        result = await self.indexer_col.update_one(
            {"_id": str(chat_id), "lock_token": lock_token},
            {
                "$set": {"state": state, "updated": time.time()},
                "$unset": {"lock_token": "", "locked_until": ""},
            },
        )
        return result.matched_count == 1

    async def get_index_task(self, chat_id):
        if self.indexer_col is None:
            return None
        try:
            doc = await self.indexer_col.find_one({"_id": str(chat_id)})
            return doc["state"] if doc else None
        except Exception as e:
            logger.warning(f"get_index_task failed: {e}")
            return None  # treat transient DB errors as "not stopped" — loop keeps trying

    async def get_index_task_document(self, chat_id):
        if self.indexer_col is None:
            return None
        return await self.indexer_col.find_one({"_id": str(chat_id)})

    async def clear_index_task(self, chat_id):
        if self.indexer_col is None:
            return
        await self.indexer_col.delete_one({"_id": str(chat_id)})

    async def clear_all_index_tasks(self):
        if self.indexer_col is None:
            return
        await self.indexer_col.delete_many({})

    async def recover_index_tasks_on_startup(self):
        """Requeue only expired worker leases while retaining checkpoints."""
        if self.indexer_col is None:
            return 0
        result = await self.indexer_col.update_many(
            {
                "state": "running",
                "$or": [
                    {"locked_until": {"$exists": False}},
                    {"locked_until": {"$lte": time.time()}},
                ],
            },
            {
                "$set": {
                    "state": "queued",
                    "recovered_at": time.time(),
                    "recovery_reason": "process_restart",
                },
                "$unset": {"lock_token": "", "locked_until": ""},
            },
        )
        return int(result.modified_count)

    async def get_stale_index_tasks(self, older_than_seconds=7200):
        if self.indexer_col is None:
            return []
        cutoff = time.time() - older_than_seconds
        cursor = self.indexer_col.find({"state": "running", "updated": {"$lt": cutoff}})
        return [doc async for doc in cursor]

    async def save_search(self, session_id, data):
        await redis_state.set_json("search-session", session_id, data, ttl=600)

    async def get_search(self, session_id):
        return await redis_state.get_json("search-session", session_id)

    async def clear_old_searches(self, expiry_seconds=600):
        return 0

    async def enqueue_notification_job(self, kind: str, coalesce_key: str, payload: dict, delay_seconds=0):
        """Persist a coalesced, bounded notification-pipeline job."""
        if self.announcement_col is None:
            raise RuntimeError("Announcement outbox is unavailable")
        now = time.time()
        digest = hashlib.sha256(coalesce_key.encode("utf-8")).hexdigest()[:32]
        job_id = f"{kind}:{digest}"
        existing = await self.announcement_col.find_one({"_id": job_id}, {"_id": 1})
        if existing is None:
            depth = await self.announcement_col.count_documents({}, limit=MAX_NOTIFICATION_OUTBOX_JOBS)
            if depth >= MAX_NOTIFICATION_OUTBOX_JOBS:
                logger.warning(
                    "notification_outbox_full kind=%s policy=drop_new depth=%s",
                    kind,
                    depth,
                )
                return None
        due_at = now + max(0.0, float(delay_seconds))
        await self.announcement_col.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "kind": kind,
                    "payload": dict(payload),
                    "expires_at": datetime.fromtimestamp(now + (7 * 24 * 3600), timezone.utc),
                },
                "$setOnInsert": {"created_at": now, "attempts": 0},
                "$inc": {"revision": 1},
                "$min": {"due_at": due_at},
            },
            upsert=True,
        )
        return job_id

    async def enqueue_announcement(self, file_name: str, delay_seconds=0):
        title_key = primary_search_identity(file_name) or normalized_search_identity(file_name)
        return await self.enqueue_notification_job(
            "announcement",
            title_key,
            {"file_name": str(file_name)},
            delay_seconds,
        )

    async def enqueue_request_fulfillment(self, file_name: str, delay_seconds=0):
        title_key = primary_search_identity(file_name) or normalized_search_identity(file_name)
        return await self.enqueue_notification_job(
            "request_fulfillment",
            title_key,
            {"file_name": str(file_name)},
            delay_seconds,
        )

    async def claim_due_notification(self, lease_seconds=300):
        if self.announcement_col is None:
            return None
        now = time.time()
        return await self.announcement_col.find_one_and_update(
            {
                "due_at": {"$lte": now},
                "$or": [
                    {"locked_until": {"$exists": False}},
                    {"locked_until": {"$lte": now}},
                ],
            },
            {
                "$set": {"locked_until": now + lease_seconds},
                "$inc": {"attempts": 1},
            },
            sort=[("due_at", 1)],
            return_document=ReturnDocument.AFTER,
        )

    async def claim_due_announcement(self, lease_seconds=300):
        return await self.claim_due_notification(lease_seconds)

    async def complete_announcement(self, job_id, revision=None):
        if self.announcement_col is not None:
            query = {"_id": job_id}
            if revision is not None:
                query["revision"] = revision
            result = await self.announcement_col.delete_one(query)
            if revision is not None and result.deleted_count == 0:
                await self.announcement_col.update_one(
                    {"_id": job_id},
                    {
                        "$min": {"due_at": time.time()},
                        "$unset": {"locked_until": ""},
                    },
                )

    async def retry_announcement(self, job_id, delay_seconds, revision=None):
        if self.announcement_col is not None:
            query = {"_id": job_id}
            if revision is not None:
                query["revision"] = revision
            result = await self.announcement_col.update_one(
                query,
                {
                    "$set": {
                        "due_at": time.time() + max(1.0, float(delay_seconds)),
                    },
                    "$unset": {"locked_until": ""},
                },
            )
            if revision is not None and result.matched_count == 0:
                await self.announcement_col.update_one(
                    {"_id": job_id},
                    {
                        "$min": {"due_at": time.time()},
                        "$unset": {"locked_until": ""},
                    },
                )

    async def notification_outbox_depth(self):
        if self.announcement_col is None:
            return 0
        return await self.announcement_col.count_documents({}, limit=MAX_NOTIFICATION_OUTBOX_JOBS + 1)

    async def enqueue_broadcast(
        self,
        *,
        source_chat_id: int,
        source_message_id: int,
        admin_chat_id: int,
        status_message_id: int,
        created_by: int,
        due_at: float,
        target: str,
        do_pin: bool,
        do_delete: bool,
        total_users: int = 0,
        total_groups: int = 0,
    ):
        """Persist a scheduled broadcast without retaining Telegram objects."""
        if self.broadcast_col is None or target not in {"users", "groups", "both"}:
            return None
        active = await self.broadcast_col.count_documents(
            {"status": {"$in": ["pending", "running", "paused"]}}, limit=101
        )
        if active >= 100:
            return None
        now = time.time()
        document = {
            "source_chat_id": int(source_chat_id),
            "source_message_id": int(source_message_id),
            "admin_chat_id": int(admin_chat_id),
            "status_message_id": int(status_message_id),
            "created_by": int(created_by),
            "created_at": now,
            "due_at": max(now, float(due_at)),
            "target": target,
            "do_pin": bool(do_pin),
            "do_delete": bool(do_delete),
            "total_users": max(0, int(total_users)),
            "total_groups": max(0, int(total_groups)),
            "total_recipients": max(0, int(total_users)) + max(0, int(total_groups)),
            "status": "pending",
            "attempts": 0,
            "user_cursor": None,
            "group_cursor": None,
            "users_done": target == "groups",
            "groups_done": target == "users",
            "sent_users": 0,
            "failed_users": 0,
            "blocked_users": 0,
            "skipped_banned": 0,
            "sent_groups": 0,
            "failed_groups": 0,
        }
        result = await self.broadcast_col.insert_one(document)
        return result.inserted_id

    async def claim_due_broadcast(self, lease_seconds=BROADCAST_LEASE_SECONDS):
        if self.broadcast_col is None:
            return None
        now = time.time()
        lock_token = secrets.token_hex(16)
        return await self.broadcast_col.find_one_and_update(
            {
                "status": {"$in": ["pending", "running"]},
                "due_at": {"$lte": now},
                "$or": [
                    {"status": "pending"},
                    {"locked_until": {"$lte": now}},
                    {"locked_until": {"$exists": False}},
                ],
            },
            {
                "$set": {
                    "status": "running",
                    "lock_token": lock_token,
                    "locked_until": now + max(30, int(lease_seconds)),
                    "last_started_at": now,
                },
                "$min": {"started_at": now},
                "$unset": {"control_requested": ""},
            },
            sort=[("due_at", 1)],
            return_document=ReturnDocument.AFTER,
        )

    async def checkpoint_broadcast(
        self, job_id, lock_token: str, audience: str, recipient_id: int, outcome: str
    ) -> bool:
        if self.broadcast_col is None:
            return False
        counters = {
            "sent_user": "sent_users",
            "failed_user": "failed_users",
            "blocked_user": "blocked_users",
            "skipped_banned": "skipped_banned",
            "sent_group": "sent_groups",
            "failed_group": "failed_groups",
        }
        if audience not in {"user", "group"} or outcome not in counters:
            raise ValueError("invalid broadcast checkpoint")
        cursor_field = f"{audience}_cursor"
        result = await self.broadcast_col.update_one(
            {
                "_id": job_id,
                "status": "running",
                "lock_token": lock_token,
                "$or": [
                    {cursor_field: {"$lt": recipient_id}},
                    {cursor_field: None},
                    {cursor_field: {"$exists": False}},
                ],
            },
            {
                "$set": {
                    cursor_field: recipient_id,
                    "locked_until": time.time() + BROADCAST_LEASE_SECONDS,
                },
                "$inc": {counters[outcome]: 1},
            },
        )
        if result.matched_count == 1:
            return True
        # An ambiguous network result may have committed the checkpoint even
        # though the client did not observe the acknowledgement. Treat the
        # already-advanced cursor as success without incrementing again.
        current = await self.broadcast_col.find_one(
            {"_id": job_id, "status": "running", "lock_token": lock_token},
            {cursor_field: 1},
        )
        return bool(
            current and current.get(cursor_field) is not None and current[cursor_field] >= recipient_id
        )

    async def complete_broadcast_phase(self, job_id, lock_token: str, audience: str) -> bool:
        if self.broadcast_col is None or audience not in {"users", "groups"}:
            return False
        result = await self.broadcast_col.update_one(
            {"_id": job_id, "status": "running", "lock_token": lock_token},
            {
                "$set": {
                    f"{audience}_done": True,
                    "locked_until": time.time() + BROADCAST_LEASE_SECONDS,
                }
            },
        )
        return result.matched_count == 1

    async def complete_broadcast(self, job_id, lock_token: str) -> bool:
        if self.broadcast_col is None:
            return False
        result = await self.broadcast_col.update_one(
            {"_id": job_id, "status": "running", "lock_token": lock_token},
            {
                "$set": {"status": "completed", "finished_at": time.time()},
                "$unset": {
                    "lock_token": "",
                    "locked_until": "",
                    "control_requested": "",
                },
            },
        )
        return result.matched_count == 1

    @staticmethod
    def _broadcast_job_id(job_id):
        try:
            return ObjectId(job_id) if isinstance(job_id, str) else job_id
        except Exception:
            return None

    async def get_broadcast(self, job_id):
        if self.broadcast_col is None:
            return None
        parsed_id = self._broadcast_job_id(job_id)
        if parsed_id is None:
            return None
        return await self.broadcast_col.find_one({"_id": parsed_id})

    async def list_recent_broadcasts(self, limit=8):
        if self.broadcast_col is None:
            return []
        cursor = (
            self.broadcast_col.find({})
            .sort("created_at", -1)
            .limit(max(1, min(int(limit), 20)))
        )
        return [document async for document in cursor]

    async def get_broadcast_control(self, job_id, lock_token: str):
        if self.broadcast_col is None:
            return None
        return await self.broadcast_col.find_one(
            {"_id": job_id, "status": "running", "lock_token": lock_token},
            {"status": 1, "control_requested": 1},
        )

    async def request_broadcast_control(self, job_id, action: str):
        """Request a safe pause, resume or stop and return the latest job."""
        if self.broadcast_col is None or action not in {"pause", "resume", "stop"}:
            return None
        parsed_id = self._broadcast_job_id(job_id)
        if parsed_id is None:
            return None
        for _ in range(3):
            now = time.time()
            job = await self.broadcast_col.find_one({"_id": parsed_id}, {"status": 1})
            if not job:
                return None
            status = job.get("status")
            query = {"_id": parsed_id, "status": status}
            if action == "pause" and status == "pending":
                update = {
                    "$set": {"status": "paused", "paused_at": now},
                    "$unset": {"lock_token": "", "locked_until": "", "control_requested": ""},
                }
            elif action == "pause" and status == "running":
                update = {"$set": {"control_requested": "pause"}}
            elif action == "resume" and status == "paused":
                update = {
                    "$set": {"status": "pending", "due_at": now},
                    "$unset": {
                        "paused_at": "",
                        "lock_token": "",
                        "locked_until": "",
                        "control_requested": "",
                    },
                }
            elif action == "stop" and status in {"pending", "paused"}:
                update = {
                    "$set": {"status": "stopped", "finished_at": now},
                    "$unset": {"lock_token": "", "locked_until": "", "control_requested": ""},
                }
            elif action == "stop" and status == "running":
                update = {"$set": {"control_requested": "stop"}}
            else:
                return await self.broadcast_col.find_one({"_id": parsed_id})
            updated = await self.broadcast_col.find_one_and_update(
                query,
                update,
                return_document=ReturnDocument.AFTER,
            )
            if updated:
                return updated
        return await self.broadcast_col.find_one({"_id": parsed_id})

    async def apply_broadcast_control(self, job_id, lock_token: str, action: str) -> bool:
        """Atomically release a running job after its current recipient."""
        if self.broadcast_col is None or action not in {"pause", "stop"}:
            return False
        now = time.time()
        terminal_status = "paused" if action == "pause" else "stopped"
        fields = {"status": terminal_status}
        fields["paused_at" if action == "pause" else "finished_at"] = now
        result = await self.broadcast_col.update_one(
            {
                "_id": job_id,
                "status": "running",
                "lock_token": lock_token,
                "control_requested": action,
            },
            {
                "$set": fields,
                "$unset": {"lock_token": "", "locked_until": "", "control_requested": ""},
            },
        )
        return result.matched_count == 1

    async def retry_broadcast(self, job_id, lock_token: str, error: Exception, delay_seconds: int) -> bool:
        if self.broadcast_col is None:
            return False
        result = await self.broadcast_col.update_one(
            {"_id": job_id, "status": "running", "lock_token": lock_token},
            {
                "$set": {
                    "status": "pending",
                    "due_at": time.time() + max(5, int(delay_seconds)),
                    "last_error_type": type(error).__name__,
                    "last_error": str(error)[:500],
                },
                "$inc": {"attempts": 1},
                "$unset": {"lock_token": "", "locked_until": "", "control_requested": ""},
            },
        )
        return result.matched_count == 1

    async def fail_broadcast(self, job_id, lock_token: str, error: Exception) -> bool:
        if self.broadcast_col is None:
            return False
        result = await self.broadcast_col.update_one(
            {"_id": job_id, "status": "running", "lock_token": lock_token},
            {
                "$set": {
                    "status": "failed",
                    "finished_at": time.time(),
                    "last_error_type": type(error).__name__,
                    "last_error": str(error)[:500],
                },
                "$unset": {"lock_token": "", "locked_until": "", "control_requested": ""},
            },
        )
        return result.matched_count == 1

    async def schedule_deletion(self, chat_id: int, message_id: int, delay_seconds: int):
        """Persist an auto-delete job so it survives bot restarts."""
        if self.deletion_col is None:
            return False
        due_at = datetime.now(timezone.utc).timestamp() + max(0, int(delay_seconds))
        await self.deletion_col.update_one(
            {"chat_id": chat_id, "message_id": message_id},
            {"$set": {"due_at": due_at, "attempts": 0}},
            upsert=True,
        )
        return True

    async def get_due_deletions(self, limit=100):
        if self.deletion_col is None:
            return []
        cursor = (
            self.deletion_col.find({"due_at": {"$lte": datetime.now(timezone.utc).timestamp()}})
            .sort("due_at", 1)
            .limit(limit)
        )
        return [doc async for doc in cursor]

    async def complete_deletion(self, job_id):
        if self.deletion_col is not None:
            await self.deletion_col.delete_one({"_id": job_id})

    async def retry_deletion(self, job_id, delay_seconds=60):
        if self.deletion_col is not None:
            await self.deletion_col.update_one(
                {"_id": job_id},
                {
                    "$inc": {"attempts": 1},
                    "$set": {"due_at": datetime.now(timezone.utc).timestamp() + delay_seconds},
                },
            )

    async def dead_letter_deletion(self, job: dict, error: Exception, permanent: bool):
        """Retain an undeletable job for operator inspection and retry."""
        if self.deletion_dead_letter_col is None or self.deletion_col is None:
            raise RuntimeError("deletion dead-letter storage is unavailable")
        failed_at = datetime.now(timezone.utc).timestamp()
        await self.deletion_dead_letter_col.update_one(
            {"_id": job["_id"]},
            {
                "$set": {
                    "chat_id": job["chat_id"],
                    "message_id": job["message_id"],
                    "attempts": int(job.get("attempts", 0)) + 1,
                    "original_due_at": job.get("due_at"),
                    "failed_at": failed_at,
                    "error_type": type(error).__name__,
                    "error": str(error)[:500],
                    "permanent": bool(permanent),
                }
            },
            upsert=True,
        )
        await self.deletion_col.delete_one({"_id": job["_id"]})
        return job["_id"]

    async def retry_dead_letter_deletion(self, job_id) -> bool:
        if self.deletion_dead_letter_col is None or self.deletion_col is None:
            return False
        try:
            parsed_id = ObjectId(job_id) if isinstance(job_id, str) else job_id
        except Exception:
            return False
        job = await self.deletion_dead_letter_col.find_one({"_id": parsed_id})
        if not job:
            return False
        await self.deletion_col.update_one(
            {"chat_id": job["chat_id"], "message_id": job["message_id"]},
            {
                "$set": {
                    "due_at": datetime.now(timezone.utc).timestamp(),
                    "attempts": 0,
                }
            },
            upsert=True,
        )
        await self.deletion_dead_letter_col.delete_one({"_id": parsed_id})
        return True

    async def close(self):
        """Close all MongoDB clients during a graceful shutdown."""
        for client in self.clients:
            await client.close()


db = Database()
