import os
import re
import time
import secrets
import asyncio
import logging
from urllib.parse import quote
from dotenv import load_dotenv
from pyrogram.errors import (
    MessageNotModified,
    FileIdInvalid,
    FileReferenceEmpty,
    FileReferenceExpired,
    FileReferenceInvalid,
    MediaEmpty,
    MediaInvalid,
)
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from database.db import db
from plugins.access_policy import enforce_user_action
from plugins.callbacks import answer_callback_safely
from plugins.req_fsub import check_verification_gates
from plugins.search_indicator import show_search_indicator, remove_search_indicator
from plugins.workload import (
    WorkloadRejected,
    delivery_guard,
    enforce_search_rate_limits,
    interactive_slot,
    interactive_callback,
    search_slot,
    validate_search_query,
)
from plugins.telegram_retry import DELIVERY_RETRY, INTERACTIVE_RETRY, telegram_call
from plugins.task_supervisor import TaskConflict, supervisor
from utils import (
    _no_preview,
    _html,
    callback_data,
    html_user_mention,
)

load_dotenv()
logger = logging.getLogger(__name__)

IGNORE_WORDS = {"hi", "hello", "bro", "pls", "plz", "bot", "help", "admin", "sir"}

LANGUAGES = ["Malayalam", "Tamil", "Telugu", "Hindi", "English", "Kannada", "Dual Audio", "Multi Audio"]
QUALITIES = ["4K", "1080p", "720p", "480p", "HDRip", "WEB-DL", "WEBRip", "BluRay", "PreDVD", "CAM", "HD Rip"]

LANG_EMOJI = {
    "Malayalam": "🌴",
    "Tamil": "🎭",
    "Telugu": "⭐",
    "Hindi": "🇮🇳",
    "English": "🌍",
    "Kannada": "🏵",
    "Dual Audio": "🎧",
    "Multi Audio": "🎵",
    "Other": "🌐",
}


async def send_smart_log(client, text, reply_markup=None, parse_mode=None):
    try:
        config = await db.get_config()
        log_chat = config.get("log_channel", 0)
        if log_chat:
            options = _no_preview()
            if parse_mode is not None:
                options["parse_mode"] = parse_mode
            await client.send_message(
                log_chat,
                text,
                reply_markup=reply_markup,
                **options,
            )
    except Exception:
        pass


# Precompiled once at import time rather than rebuilt from pattern strings on
# every incoming search/result-render — clean_query() runs on every message
# and extract_attributes() runs once per file on every page render.
_STOP_WORD_RES = [
    re.compile(p, re.IGNORECASE)
    for p in (
        r"\bplease\b",
        r"\bsend\b",
        r"\bme\b",
        r"\bmovie\b",
        r"\bseries\b",
        r"\bhd\b",
        r"\bprint\b",
        r"\bdownload\b",
        r"\blink\b",
        r"\bbro\b",
        r"\bcan\b",
        r"\byou\b",
        r"\bprovide\b",
        r"\bi\b",
        r"\bneed\b",
        r"\bwant\b",
    )
]
_WS_RE = re.compile(r"\s+")


def clean_query(query):
    # Treat filename-style separators as spaces. Users often paste indexed
    # names such as ``aavesham_2024-malayalam``; those should search exactly
    # like normal words, without carrying visual noise into result headings.
    q = re.sub(r"[_.\-–—]+", " ", query.lower())
    for pattern in _STOP_WORD_RES:
        q = pattern.sub("", q)
    return _WS_RE.sub(" ", q).strip()


def _attribute_regex(value: str):
    parts = [re.escape(part) for part in re.split(r"[\s._\-–—]+", value) if part]
    return re.compile(r"\b" + r"\s*".join(parts) + r"\b", re.IGNORECASE)


_LANG_RES = [(language, _attribute_regex(language)) for language in LANGUAGES]
_QUAL_RES = [(quality, _attribute_regex(quality)) for quality in QUALITIES]


def extract_attributes(filename):
    normalized = re.sub(r"[._\-–—]+", " ", filename)
    lang = next((l for l, pat in _LANG_RES if pat.search(normalized)), "Other")
    qual = next((q for q, pat in _QUAL_RES if pat.search(normalized)), "Other")
    if qual.lower() == "hdrip":
        qual = "HD Rip"
    return lang, qual


def _extract_codec(filename: str) -> str:
    normalized = re.sub(r"[._\-–—]+", " ", filename)
    if re.search(r"\b(?:hevc|x265|h\s*265)\b", normalized, re.IGNORECASE):
        return "HEVC"
    if re.search(r"\b(?:avc|x264|h\s*264)\b", normalized, re.IGNORECASE):
        return "H.264"
    if re.search(r"\bav1\b", normalized, re.IGNORECASE):
        return "AV1"
    return ""


# ── Display-only title cleanup ───────────────────────────────────────────────
# Never touches the stored file_name — purely how a filename is *rendered*
# in a button label or delivered-file caption.
_EXT_RE = re.compile(r"(?:\.|\b)(mkv|mp4|avi|mov|zip|srt)\s*$", re.IGNORECASE)
_YEAR_RE = re.compile(r"^(19\d{2}|20\d{2})$")
_PAREN_YEAR_RE = re.compile(r"[\(\[](\d{4})[\)\]]")
_BRACKET_GROUP_RE = re.compile(r"[\[\(].*?[\]\)]")
_TITLE_SEP_RE = re.compile(r"[._\-–—]+")
_LISTING_SEP_RE = re.compile(r"[._\-–—#+|]+")
_TITLE_WS_RE = re.compile(r"\s+")
_STRAY_BRACKET_RE = re.compile(r"[\[\]\(\)\{\}]+")
_PROMO_RE = re.compile(
    r"(?:https?://\S+|www\.\S+|(?:t|telegram)\.me/\S+|@[A-Za-z0-9_]+)",
    re.IGNORECASE,
)

# Codec/subtitle/release-group tags that aren't in LANGUAGES/QUALITIES but
# still need to be recognized as "metadata, not title" when walking backward
# through a filename's tokens (see _display_title below).
_JUNK_WORDS = {
    "x264",
    "x265",
    "h264",
    "h265",
    "hevc",
    "avc",
    "aac",
    "aac2",
    "ac3",
    "eac3",
    "ddp",
    "ddp5",
    "dd5",
    "esub",
    "esubs",
    "hsub",
    "hsubs",
    "10bit",
    "hdcam",
    "tsrip",
    "dvdrip",
    "hq",
    "nf",
    "amzn",
    "brrip",
    "bdrip",
    "webrip",
    "webdl",
    "bluray",
    "proper",
    "repack",
    "telly",
    "collective",
    "etrg",
    "yts",
    "true",
    "vbr",
    "bigil",
    "ddh",
    "dd",
    "mkv",
    "mp4",
    "avi",
    "mov",
    "zip",
    "srt",
}


def _metadata_key(value: str) -> str:
    return re.sub(r"[\s._\-–—]+", " ", value.lower()).strip()


_QUALITY_LOWER = {_metadata_key(q) for q in QUALITIES}
_LANGUAGE_LOWER = {_metadata_key(l) for l in LANGUAGES}
_TECH_TOKEN_RE = re.compile(
    r"^(?:2160p|1080p|720p|480p|360p|\d+(?:\.\d+)?(?:mb|gb)|"
    r"\d{3,4}k(?:bps)?|\d(?:\.\d)?ch|"
    r"dd\+?\d*|ddp?\d(?:\.\d)?|aac\d?|ac3|eac3|h26[45]|x26[45]|hevc|avc|10bit)$",
    re.IGNORECASE,
)


def _is_year_token(tok: str) -> bool:
    return bool(_YEAR_RE.match(tok))


def _is_metadata_word(tok: str) -> bool:
    """Quality/language/codec-junk classification — deliberately excludes
    year tokens, which _display_title handles separately (a filename can
    legitimately contain two 4-digit-looking tokens — a numeric title like
    "1917"/"2012" AND its real release year — and only one of them should
    ever be consumed as metadata)."""
    low = _metadata_key(tok)
    if low in _QUALITY_LOWER or low in _LANGUAGE_LOWER or low in _JUNK_WORDS:
        return True
    return bool(_TECH_TOKEN_RE.fullmatch(low))


def _display_title(filename: str) -> tuple:
    """Derives a clean display title + year from a raw indexed filename, for
    presentation only (button labels / delivered-file captions) — never
    touches the stored file_name.

    Walks the filename's tokens backward from the end, classifying each
    trailing token (or 2-token phrase, for "Dual Audio"/"HD Rip"-style
    entries) as quality/language/year/codec metadata, and stops at the
    first token (from the end) that isn't recognized — everything up to
    and including that token is the real title. This checks the whole
    trailing run rather than "does a marker appear anywhere in the
    filename", so a title that merely *contains* a language/quality word
    ("The Malayalam Movie") is never truncated mid-title — a cut only
    happens once the marker is genuinely part of an unbroken metadata tail
    at the end of the string. Season/episode markers (S01E01 etc.) are
    deliberately never classified as metadata, so they stay part of the
    title for series files — otherwise every episode of a show would
    render an identical caption/label with no way to tell them apart.
    """
    # Promotional handles and URLs are never useful to a user choosing a
    # file. Remove them before the extension so ``movie.mkv @channel`` also
    # loses its extension correctly.
    name = _PROMO_RE.sub(" ", filename).strip()
    name = _EXT_RE.sub("", name)

    # A parenthesized/bracketed 4-digit year is unambiguous regardless of
    # what the rest of the title looks like (a numeric title like "300" or
    # "2012" is never itself wrapped in its own parens).
    paren_match = _PAREN_YEAR_RE.search(name)
    paren_year = paren_match.group(1) if paren_match else None

    # Strip every complete bracket group (site tags like [TamilMV], the
    # year annotation just captured above, etc.) — replace with a space,
    # not empty, so two words butting up against a bracket don't fuse.
    name = _BRACKET_GROUP_RE.sub(" ", name)
    name = _STRAY_BRACKET_RE.sub(" ", name)
    # Normalize remaining separators to spaces before tokenizing —
    # underscore is a \w character, so naive \b-based regex matching would
    # silently fail against underscore-separated filenames otherwise.
    norm = _TITLE_SEP_RE.sub(" ", name)
    tokens = [t for t in _TITLE_WS_RE.split(norm) if t]

    i = len(tokens)
    year_consumed = False
    metadata_consumed = False
    while i > 0:
        if i >= 2:
            phrase = _metadata_key(f"{tokens[i - 2]} {tokens[i - 1]}")
            if phrase in _QUALITY_LOWER or phrase in _LANGUAGE_LOWER:
                i -= 2
                metadata_consumed = True
                continue
            if re.fullmatch(r"h 26[45]", phrase):
                i -= 2
                metadata_consumed = True
                continue
            # Audio layouts are frequently written as ``DDP5.1`` and become
            # two tokens after separator cleanup. Consume the pair together
            # so the trailing ``1`` never leaks into the movie title.
            if re.fullmatch(r"(?:ddp?|aac)\d+ \d+", phrase):
                i -= 2
                metadata_consumed = True
                continue
        tok = tokens[i - 1]
        if _is_year_token(tok):
            if year_consumed:
                break  # a 2nd year-shaped token further back is the title, not metadata
            year_consumed = True
            i -= 1
            metadata_consumed = True
            continue
        if _is_metadata_word(tok):
            i -= 1
            metadata_consumed = True
            continue
        # Some indexes repeat a short title acronym immediately before the
        # language metadata (for example ``K G F Chapter 1 2018 KGF Tamil``).
        # Remove only an all-uppercase duplicate after genuine metadata was
        # already consumed, keeping normal title words untouched.
        compact = re.sub(r"[^A-Za-z0-9]", "", tok)
        previous_tokens = tokens[: i - 1]
        joined_head = "".join(previous_tokens[:4]).lower()
        if (
            metadata_consumed
            and 2 <= len(compact) <= 8
            and compact.isupper()
            and (
                compact.lower() in {p.lower() for p in previous_tokens}
                or joined_head.startswith(compact.lower())
            )
        ):
            i -= 1
            continue
        break

    title = " ".join(tokens[:i]).strip(" |")
    title = re.sub(
        r"\b(?:[A-Z]\s+){1,}[A-Z]\b",
        lambda match: match.group(0).replace(" ", ""),
        title,
    )
    tail_tokens = tokens[i:]

    if not title:
        title = " ".join(tokens)  # nothing recognized — no safe cut point

    if paren_year:
        year = paren_year
    else:
        year = next((t for t in tail_tokens if _is_year_token(t)), "")

    clean_fallback = _TITLE_WS_RE.sub(" ", _TITLE_SEP_RE.sub(" ", name)).strip(" |")
    return (title or clean_fallback or filename.strip(), year)


def _variant_label(f, show_title: bool, title=None, year=None) -> str:
    """Builds one file's button label as Title (Year) · Language · Quality ·
    Size — or, when show_title is False, just Language · Quality · Size.
    The latter is used for the single-result hero card's metadata line,
    where the title is already the bolded caption headline just above it."""
    if title is None:
        title, year = _display_title(f["file_name"])
    f_lang, f_qual = extract_attributes(f["file_name"])
    codec = _extract_codec(f["file_name"])
    size_str = _fmt_size(f)

    parts = []
    if show_title:
        parts.append(f"{title} ({year})" if year else title)
    if f_qual not in ("Other", ""):
        parts.append(f_qual)
    if codec:
        parts.append(codec)
    if f_lang not in ("Other", ""):
        parts.append(f_lang)
    parts.append(size_str)

    label = " • ".join(parts)
    return label if len(label) <= 52 else label[:51] + "…"


async def _auto_delete_search(status_msg, original_msg, manual_query):
    await db.schedule_deletion(status_msg.chat.id, status_msg.id, 300)
    if not manual_query:
        await db.schedule_deletion(original_msg.chat.id, original_msg.id, 300)


async def _auto_delete_file(sent_msg, file_name, bot_username, delete_seconds=300):
    """Compatibility wrapper that now schedules a durable deletion job."""
    await db.schedule_deletion(sent_msg.chat.id, sent_msg.id, delete_seconds)


def _fmt_size(file_doc):
    size_mb = file_doc.get("file_size", 0) / (1024 * 1024)
    if size_mb >= 1000:
        return f"{size_mb / 1024:.2f} GB"
    return f"{size_mb:.2f} MB"


_EPISODE_TAG_RE = re.compile(
    r"(?<![A-Za-z0-9])S\s*(\d{1,2})\s*E\s*(\d{1,3})(?![A-Za-z0-9])",
    re.IGNORECASE,
)


def _listing_name(filename: str) -> tuple[str, str]:
    """Return a cleaned full filename and its normalized episode tag.

    Useful release details stay visible; only promotional content, the file
    extension, bracket characters and filename separators are removed.
    """
    name = _PROMO_RE.sub(" ", filename or "")
    name = _EXT_RE.sub("", name.strip())

    episode_match = _EPISODE_TAG_RE.search(name)
    episode = ""
    if episode_match:
        episode = f"S{int(episode_match.group(1)):02d}E{int(episode_match.group(2)):02d}"
        name = _EPISODE_TAG_RE.sub(" ", name, count=1)

    name = _STRAY_BRACKET_RE.sub(" ", name)
    name = _LISTING_SEP_RE.sub(" ", name)
    name = _TITLE_WS_RE.sub(" ", name).strip(" |")
    return name or "Unnamed file", episode


def _smart_metadata(filename: str) -> list[str]:
    """Return stable, non-duplicated metadata fields for a button label."""
    language, quality = extract_attributes(filename)
    codec = _extract_codec(filename)
    fields = []
    for value in (language, quality, codec):
        if value in ("", "Other") or value in fields:
            continue
        fields.append(value)
    return fields


def _series_identity(filename: str) -> tuple[str, str]:
    """Extract only the series name and SxxExx marker.

    Text between the episode marker and technical metadata is normally the
    episode title. Excluding it prevents labels such as ``Reacher Welcome to
    Margrave`` from looking like two unrelated titles.
    """
    marker = _SERIES_RE.search(filename)
    prefix = filename[: marker.start()] if marker else filename
    title, year = _display_title(prefix)
    if not title:
        title = "Series"
    identity = f"{title} ({year})" if year else title

    season_match = _SEASON_NUM_RE.search(filename)
    episode_match = _EPISODE_NUM_RE.search(filename)
    season = int(season_match.group(1)) if season_match else (1 if episode_match else 0)
    episode = int(episode_match.group(1)) if episode_match else 0
    if episode:
        marker_text = f"S{season:02d}E{episode:02d}"
    elif season:
        marker_text = f"S{season:02d}"
    else:
        marker_text = "EP"
    return identity, marker_text


def _compose_aligned_label(prefix: str, identity: str, metadata: list[str], max_length: int) -> str:
    """Keep the fixed fields visible and trim only the variable title."""
    suffix = "" if not metadata else " • " + " • ".join(metadata)
    available = max_length - len(prefix) - len(suffix) - 1
    if available < 8 and metadata:
        # Prefer the identity over the least important trailing metadata when
        # Telegram's 64-character button limit is especially tight.
        metadata = metadata[:-1]
        return _compose_aligned_label(prefix, identity, metadata, max_length)
    if available < 2:
        return f"{prefix} {identity}"[:max_length]
    if len(identity) > available:
        identity = identity[: available - 1].rstrip() + "…"
    return f"{prefix} {identity}{suffix}"


def _flat_file_label(file_doc: dict, max_length: int = 64) -> str:
    """Build a consistent smart label for movie and episodic files."""
    filename = file_doc.get("file_name", "")
    prefix = f"[{_fmt_size(file_doc)}]"
    if _is_series(filename):
        identity, episode = _series_identity(filename)
        prefix += f" [{episode}]"
    else:
        title, year = _display_title(filename)
        identity = f"{title} ({year})" if year else title
    return _compose_aligned_label(prefix, identity or "Unnamed file", _smart_metadata(filename), max_length)


def _build_results_caption(query: str, total: int, page: int, total_pages: int, first_name: str = "") -> str:
    """Build the identical results heading used in DMs and groups."""
    lines = [f"🔎 <b>Results Found For {_html(query)}</b>"]
    if first_name:
        lines.append(f"👤 <b>{_html(first_name)}</b>")
    lines.extend(
        [
            "",
            f"📁 <b>Files:</b> {total}  •  📚 <b>Page:</b> {page + 1} / {total_pages}",
        ]
    )
    return "\n".join(lines)


_SERIES_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:S(?:EASON)?[\s._-]*\d{1,2}|"
    r"E(?:P(?:ISODE)?)?[\s._-]*\d{1,3})",
    re.IGNORECASE,
)
_SEASON_NUM_RE = re.compile(r"(?<![A-Za-z0-9])S(?:EASON)?[\s._-]*0*(\d{1,2})", re.IGNORECASE)
_EPISODE_NUM_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:S(?:EASON)?[\s._-]*\d{1,2}[\s._-]*)?"
    r"E(?:P(?:ISODE)?)?[\s._-]*0*(\d{1,3})",
    re.IGNORECASE,
)


def _is_series(filename: str) -> bool:
    return bool(_SERIES_RE.search(filename))


def _has_series_content(results: list) -> bool:
    return any(_is_series(f.get("file_name", "")) for f in results)


def _series_sort_key(f):
    name = f.get("file_name", "")
    s = _SEASON_NUM_RE.search(name)
    e = _EPISODE_NUM_RE.search(name)
    season = int(s.group(1)) if s else (1 if e else 0)
    episode = int(e.group(1)) if e else 0
    size = int(f.get("file_size", 0) or 0)
    if s or e:
        return (0, season, episode, size, name.casefold())
    return (1, 0, 0, -size, name.casefold())


def _sort_results(results: list) -> list:
    """Movies sort large-to-small; series sort by season and episode.

    A mixed search keeps episodic matches first in chronological order and
    puts any related movie/non-series matches afterwards by descending size.
    """
    if not results:
        return []
    unique_results = []
    seen_labels = set()
    for file_doc in results:
        # Use the untruncated smart label plus exact byte size as the semantic
        # identity. This catches duplicate releases with different Telegram
        # file IDs or harmless source-prefix differences while retaining real
        # variants (different episode, size, language, quality or codec).
        signature = (
            int(file_doc.get("file_size", 0) or 0),
            _flat_file_label(file_doc, max_length=512).casefold(),
        )
        if signature in seen_labels:
            continue
        seen_labels.add(signature)
        unique_results.append(file_doc)

    if _has_series_content(unique_results):
        return sorted(unique_results, key=_series_sort_key)
    return sorted(
        unique_results,
        key=lambda f: (
            -int(f.get("file_size", 0) or 0),
            f.get("file_name", "").casefold(),
        ),
    )


def _apply_result_filters(results: list, data: dict) -> list:
    """Apply the active presentation filters without changing stored files."""
    active_lang = data.get("filter_language")
    active_quality = data.get("filter_quality")
    if not active_lang and not active_quality:
        return results

    filtered = []
    for file_doc in results:
        language, quality = extract_attributes(file_doc.get("file_name", ""))
        if active_lang and language != active_lang:
            continue
        if active_quality and quality != active_quality:
            continue
        filtered.append(file_doc)
    return filtered


def _result_filter_row(session_id: str, data: dict):
    language = data.get("filter_language") or "Language"
    quality = data.get("filter_quality") or "Quality"
    return [
        InlineKeyboardButton(f"🌐 {language}", callback_data=f"filtermenu#{session_id}#language"),
        InlineKeyboardButton(f"🎞 {quality}", callback_data=f"filtermenu#{session_id}#quality"),
    ]


def _available_filter_values(results: list, kind: str) -> list:
    found = set()
    for file_doc in results:
        language, quality = extract_attributes(file_doc.get("file_name", ""))
        value = language if kind == "language" else quality
        if value not in ("", "Other"):
            found.add(value)
    ordered = LANGUAGES if kind == "language" else QUALITIES
    return [value for value in ordered if value in found]


def _build_caption(config, file_data, delete_minutes, bot_username):
    raw_filename = file_data.get("file_name", "")
    title, year = _display_title(raw_filename)
    clean_filename = f"{title} ({year})" if year else title
    template = config.get("file_caption_template", "")
    if template:
        f_lang, f_qual = extract_attributes(raw_filename)
        codec = _extract_codec(raw_filename)
        size_mb = file_data.get("file_size", 0) / (1024 * 1024)
        size_str = f"{size_mb / 1024:.2f}GB" if size_mb > 1024 else f"{size_mb:.0f}MB"
        try:
            return template.format(
                filename=_html(clean_filename),
                raw_filename=_html(raw_filename),
                size=size_str,
                quality=f_qual or "Unknown",
                codec=codec or "Unknown",
                lang=f_lang or "Unknown",
                username=bot_username,
                delete_minutes=delete_minutes,
            )
        except (KeyError, ValueError):
            pass

    f_lang, f_qual = extract_attributes(raw_filename)
    codec = _extract_codec(raw_filename)
    size_mb = file_data.get("file_size", 0) / (1024 * 1024)
    size_str = f"{size_mb / 1024:.2f} GB" if size_mb >= 1024 else f"{size_mb:.0f} MB"

    lang_emojis = {
        "Malayalam": "🌴",
        "Tamil": "🎭",
        "Telugu": "⭐",
        "Hindi": "🇮🇳",
        "English": "🌍",
        "Kannada": "🏵",
        "Dual Audio": "🎧",
        "Multi Audio": "🎵",
    }
    meta_parts = []
    if f_lang not in ["Other", ""]:
        meta_parts.append(f"{lang_emojis.get(f_lang, '🎬')} {f_lang}")
    if f_qual not in ["Other", ""]:
        meta_parts.append(f"🎞 {f_qual}")
    if codec:
        meta_parts.append(f"🧬 {codec}")
    meta_parts.append(f"📦 {size_str}")
    meta_line = "  •  ".join(meta_parts)

    title_line = f"🎬 <b>{_html(title)}{f' ({year})' if year else ''}</b>"

    return (
        f"{title_line}\n"
        f"<blockquote>{meta_line}</blockquote>\n"
        f"<i>⏳ Available for {delete_minutes} min  •  Forward it to keep it</i>"
    )


def _build_result_buttons(results: list, session_id: str, page: int, per_page: int = 10):
    """Build the shared, ungrouped file rows plus simple pagination."""
    total = len(results)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, total_pages - 1))
    start_idx = page * per_page
    page_files = results[start_idx : start_idx + per_page]

    buttons = []
    for f in page_files:
        btn_text = _flat_file_label(f)
        buttons.append([InlineKeyboardButton(btn_text, callback_data=f"sendfile#{f['_id']}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅ PREV", callback_data=f"page#{session_id}#{page - 1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("NEXT ➡", callback_data=f"page#{session_id}#{page + 1}"))
    if nav:
        buttons.append(nav)

    return buttons, page, total_pages


# Keep the older public helper name used by tests/extensions, but route it to
# the same flat builder so movie and series results cannot diverge again.
def _build_movie_result_buttons(results: list, session_id: str, page: int, per_page: int = 10):
    return _build_result_buttons(results, session_id, page, per_page)


async def _render_results_view(client, message, session_id: str, page: int, data: dict, user_id=None):
    """Render every successful DM search with the shared flat list UI."""
    results = data["results"]
    query = data["query"]
    total = len(results)
    buttons, page, total_pages = _build_result_buttons(results, session_id, page)
    caption = _build_results_caption(query, total, page, total_pages, data.get("first_name", ""))
    markup = InlineKeyboardMarkup(buttons)

    # Results render as plain text only now — there's no TMDB poster to
    # upgrade a status message into a photo with. The media-message branch
    # is just a safe fallback for a session started just before this deploy.
    is_media_msg = bool(
        getattr(message, "photo", None)
        or getattr(message, "video", None)
        or getattr(message, "animation", None)
        or getattr(message, "document", None)
        or getattr(message, "sticker", None)
    )

    try:
        if is_media_msg:
            chat_id = message.chat.id
            await message.delete()
            return await telegram_call(
                lambda: client.send_message(
                    chat_id, caption, reply_markup=markup, parse_mode=ParseMode.HTML, **_no_preview()
                ),
                route="private_search_results_send",
                policy=INTERACTIVE_RETRY,
                retry_safe=True,
                idempotency_key=f"search-results:{chat_id}:{session_id}:{page}",
            )
        else:
            await telegram_call(
                lambda: message.edit_text(text=caption, reply_markup=markup, parse_mode=ParseMode.HTML),
                route="private_search_results_edit",
                policy=INTERACTIVE_RETRY,
                retry_safe=True,
                idempotency_key=f"search-edit:{message.chat.id}:{message.id}:{page}",
            )
            return message
    except MessageNotModified:
        return message
    except Exception as e:
        logger.error(f"_render_results_view error: {e}")
        return message


async def show_results(client, message, session_id, page):
    data = await db.get_search(session_id)
    if not data:
        try:
            await message.edit_text("⚠️ Session expired. Search again.")
        except Exception:
            pass
        return
    return await _render_results_view(client, message, session_id, page, data)


route_menu = show_results


@Client.on_message(
    filters.text
    & filters.private
    & ~filters.command(
        [
            "start",
            "help",
            "admin",
            "broadcast",
            "ban",
            "unban",
            "reset_db",
            "update",
            "request",
            "filesearch",
            "stats",
            "cancel",
            "reset_index_progress",
            "confirm_reset",
        ]
    )
)
async def auto_filter(client: Client, message: Message, manual_query=None):
    user_id = message.from_user.id

    access = await enforce_user_action(message, "search")
    if not access.allowed:
        return
    config = access.config

    if manual_query:
        query = manual_query
    else:
        raw = message.text
        query = clean_query(raw)
        if len(raw.strip()) < 3 or raw.strip().lower() in IGNORE_WORDS:
            main_group = config.get("main_group", "")
            return await message.reply_text(
                f"<b>Type a movie or series name to search!</b>",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📝 Request Here", url=main_group)]])
                if main_group
                else None,
                reply_parameters=None,
                parse_mode=ParseMode.HTML,
            )

    if not query:
        return

    try:
        query = validate_search_query(query)
        await enforce_search_rate_limits(user_id)
    except WorkloadRejected as exc:
        return await message.reply_text(exc.public_message, reply_parameters=None)

    indicator = await show_search_indicator(client, message.chat.id)
    start_time = time.time()
    try:
        async with search_slot("private_search"):
            results = await db.get_search_results(query)
    except WorkloadRejected as exc:
        await remove_search_indicator(indicator)
        return await message.reply_text(exc.public_message, reply_parameters=None)
    except Exception:
        await remove_search_indicator(indicator)
        raise

    if not results:
        await remove_search_indicator(indicator)
        google_url = f"https://www.google.com/search?q={quote(query)}"
        safe_query = query[:40]

        should_alert = await db.log_missed_search(query)
        if should_alert:
            try:
                supervisor.spawn(
                    send_smart_log(
                        client,
                        "❌ <b>#MissedSearch</b>\n\n"
                        f"🎬 <code>{_html(query)}</code>\n"
                        f"👤 {html_user_mention(message.from_user)}\n"
                        "📍 Private Chat",
                        parse_mode=ParseMode.HTML,
                    ),
                    key=f"log:missed-private:{user_id}:{time.monotonic_ns()}",
                    owner="filter",
                    drain_on_shutdown=True,
                )
            except TaskConflict:
                logger.info("Missed-search log skipped during shutdown")

        suggestions = await db.get_prefix_suggestions(query, limit=3)
        sug_row = []
        for sug in suggestions:
            safe_sug = re.sub(r"[^a-zA-Z0-9]", "_", sug)[:40]
            suggestion_title, _ = _display_title(sug)
            sug_row.append(
                InlineKeyboardButton(
                    f"💡 {suggestion_title[:20]}",
                    url=f"https://t.me/{client.me.username}?start=search_{safe_sug}",
                )
            )

        sug_buttons = [sug_row] if sug_row else []
        sug_buttons += [
            [
                InlineKeyboardButton(
                    "📝 Request This Movie", callback_data=callback_data("reqmovie#", safe_query)
                ),
                InlineKeyboardButton("🔎 Google", url=google_url),
            ],
            [InlineKeyboardButton("⌂ Back to Home", callback_data="start_home")],
        ]

        no_results_msg = await message.reply_text(
            f"🔎 <b>No files found</b>\n\n"
            f"We couldn't find <code>{_html(query)}</code>.\n"
            f"<blockquote>Try searching only the title, removing the year or "
            f"language, or checking the spelling.</blockquote>\n"
            f"You can also request it and receive an update when it is added.",
            reply_markup=InlineKeyboardMarkup(sug_buttons),
            parse_mode=ParseMode.HTML,
            **_no_preview(),
        )
        # Previously left in chat forever — no cleanup path existed for a
        # PM no-results screen. Reuses the same auto-delete/ manual_query
        # convention as a successful search so the two behave consistently.
        await _auto_delete_search(no_results_msg, message, manual_query)
        return

    time_taken = time.time() - start_time
    await db.clear_old_searches()

    session_id = secrets.token_urlsafe(9)

    session_data = {
        "results": _sort_results(results),
        "query": query,
        "speed": f"{time_taken:.3f}s",
        "time": time.time(),
        "auto_delete_time": int(config.get("auto_delete_time", 300)),
        "user_id": user_id,
        "first_name": message.from_user.first_name or "",
    }
    await db.save_search(session_id, session_data)

    result_msg = await show_results(client, indicator, session_id, 0)
    await _auto_delete_search(result_msg or indicator, message, manual_query)


@Client.on_callback_query(filters.regex(r"^page#"))
@interactive_callback("search_pagination")
async def handle_pagination(client: Client, callback: CallbackQuery):
    if not (await enforce_user_action(callback, "search_navigation")).allowed:
        return
    parts = callback.data.split("#")
    session_id = parts[1]
    page = int(parts[2])

    data = await db.get_search(session_id)
    if not data:
        await answer_callback_safely(callback, "⚠️ Session expired.", show_alert=True)
        return
    if data.get("user_id") not in (None, callback.from_user.id):
        await answer_callback_safely(callback, "This search belongs to another user.", show_alert=True)
        return

    await _render_results_view(
        client, callback.message, session_id, page, data, user_id=callback.from_user.id
    )
    await answer_callback_safely(callback)


@Client.on_callback_query(filters.regex(r"^expandseries#"))
@interactive_callback("series_expansion")
async def handle_expand_series(client: Client, callback: CallbackQuery):
    if not (await enforce_user_action(callback, "search_navigation")).allowed:
        return
    session_id = callback.data.split("#", 1)[1]

    data = await db.get_search(session_id)
    if not data:
        return await answer_callback_safely(callback, "⚠️ Session expired.", show_alert=True)
    if data.get("user_id") not in (None, callback.from_user.id):
        return await answer_callback_safely(callback, "This search belongs to another user.", show_alert=True)

    data["series_expanded"] = True
    await db.save_search(session_id, data)

    await answer_callback_safely(callback)
    await _render_results_view(client, callback.message, session_id, 0, data, user_id=callback.from_user.id)


@Client.on_callback_query(filters.regex(r"^filtermenu#"))
@interactive_callback("search_filter_menu")
async def show_filter_menu(client: Client, callback: CallbackQuery):
    if not (await enforce_user_action(callback, "search_navigation")).allowed:
        return
    _, session_id, kind = callback.data.split("#", 2)
    if kind not in {"language", "quality"}:
        return await answer_callback_safely(callback, "Invalid filter.", show_alert=True)

    data = await db.get_search(session_id)
    if not data:
        return await answer_callback_safely(callback, "⚠️ Session expired.", show_alert=True)
    if data.get("user_id") not in (None, callback.from_user.id):
        return await answer_callback_safely(callback, "This search belongs to another user.", show_alert=True)

    values = _available_filter_values(data["results"], kind)
    field = f"filter_{kind}"
    active = data.get(field)
    plural = "Languages" if kind == "language" else "Qualities"
    buttons = [
        [
            InlineKeyboardButton(
                f"{'✓ ' if not active else ''}All {plural}",
                callback_data=f"applyfilter#{session_id}#{kind}#all",
            )
        ]
    ]
    for index in range(0, len(values), 2):
        row = []
        for value in values[index : index + 2]:
            row.append(
                InlineKeyboardButton(
                    f"{'✓ ' if active == value else ''}{value}",
                    callback_data=f"applyfilter#{session_id}#{kind}#{value}",
                )
            )
        buttons.append(row)
    buttons.append([InlineKeyboardButton("‹ Back to Results", callback_data=f"page#{session_id}#0")])

    icon = "🌐" if kind == "language" else "🎞"
    await callback.message.edit_text(
        f"{icon} <b>Choose {kind}</b>\n\nOnly options available in this search are shown.",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode=ParseMode.HTML,
    )
    await answer_callback_safely(callback)


@Client.on_callback_query(filters.regex(r"^applyfilter#"))
@interactive_callback("search_filter_apply")
async def apply_result_filter(client: Client, callback: CallbackQuery):
    if not (await enforce_user_action(callback, "search_navigation")).allowed:
        return
    _, session_id, kind, value = callback.data.split("#", 3)
    if kind not in {"language", "quality"}:
        return await answer_callback_safely(callback, "Invalid filter.", show_alert=True)

    data = await db.get_search(session_id)
    if not data:
        return await answer_callback_safely(callback, "⚠️ Session expired.", show_alert=True)
    if data.get("user_id") not in (None, callback.from_user.id):
        return await answer_callback_safely(callback, "This search belongs to another user.", show_alert=True)

    data[f"filter_{kind}"] = None if value == "all" else value
    await db.save_search(session_id, data)
    await _render_results_view(client, callback.message, session_id, 0, data, user_id=callback.from_user.id)
    await answer_callback_safely(callback, "Filter updated")


@Client.on_callback_query(filters.regex(r"^clearfilters#"))
@interactive_callback("search_filter_clear")
async def clear_result_filters(client: Client, callback: CallbackQuery):
    if not (await enforce_user_action(callback, "search_navigation")).allowed:
        return
    session_id = callback.data.split("#", 1)[1]
    data = await db.get_search(session_id)
    if not data:
        return await answer_callback_safely(callback, "⚠️ Session expired.", show_alert=True)
    if data.get("user_id") not in (None, callback.from_user.id):
        return await answer_callback_safely(callback, "This search belongs to another user.", show_alert=True)

    data["filter_language"] = None
    data["filter_quality"] = None
    await db.save_search(session_id, data)
    await _render_results_view(client, callback.message, session_id, 0, data, user_id=callback.from_user.id)
    await answer_callback_safely(callback, "Filters cleared")


@Client.on_callback_query(filters.regex(r"^ignore$"))
async def handle_ignore(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)


@Client.on_callback_query(filters.regex(r"^sendfile#"))
async def send_movie_file(client: Client, callback: CallbackQuery):
    # Absolute first awaitable: release Telegram's callback spinner before
    # policy, MongoDB, verification, or delivery work begins.
    await answer_callback_safely(callback)
    if not (await enforce_user_action(callback, "file_delivery")).allowed:
        return
    _, file_obj_id = callback.data.split("#")
    async with interactive_slot("file_delivery_callback"):
        file_data = await db.get_file(file_obj_id)

        if not file_data:
            await callback.message.reply_text("⚠️ File no longer available. Please search again.")
            return

        if not await check_verification_gates(client, callback, file_obj_id):
            return

        access = await enforce_user_action(callback, "file_delivery")
        if not access.allowed:
            return

        guard = delivery_guard(callback.from_user.id, file_obj_id)
        try:
            await guard.__aenter__()
        except WorkloadRejected as exc:
            await callback.message.reply_text(exc.public_message)
            return

        config = access.config
        delete_seconds = int(config.get("auto_delete_time", 300))
        delete_minutes = delete_seconds // 60

        try:
            sent = await telegram_call(
                lambda: client.send_cached_media(
                    chat_id=callback.message.chat.id,
                    file_id=file_data["file_id"],
                    caption=_build_caption(config, file_data, delete_minutes, client.me.username),
                    parse_mode=ParseMode.HTML,
                ),
                route="file_delivery_callback",
                policy=DELIVERY_RETRY,
                retry_safe=True,
                idempotency_key=f"{callback.from_user.id}:{file_obj_id}",
            )
            await _auto_delete_file(sent, file_data["file_name"], client.me.username, delete_seconds)
        except (
            FileIdInvalid,
            FileReferenceEmpty,
            FileReferenceExpired,
            FileReferenceInvalid,
            MediaEmpty,
            MediaInvalid,
        ) as e:
            await db.delete_file_by_id(file_data["file_id"])
            unavailable_title, unavailable_year = _display_title(file_data["file_name"])
            unavailable_name = (
                f"{unavailable_title} ({unavailable_year})" if unavailable_year else unavailable_title
            )
            await callback.message.reply_text(
                f"❌ <b>File unavailable</b>\n\n"
                f"{_html(unavailable_name)} is no longer valid. "
                f"It was removed from the index; please search again.",
                parse_mode=ParseMode.HTML,
            )
            logger.warning("Removed invalid cached file %s: %s", file_data["file_id"], e)
        except Exception as e:
            await callback.message.reply_text("❌ Could not send this file right now. Please try again.")
            logger.error(f"send_cached_media failed: {e}")
        finally:
            await guard.__aexit__(None, None, None)


@Client.on_callback_query(filters.regex(r"^check_fsub#"))
async def check_fsub_callback(client: Client, callback: CallbackQuery):
    """One-release bridge for old buttons; always use the unified gate path."""
    await answer_callback_safely(callback)
    if not (await enforce_user_action(callback, "verification")).allowed:
        return
    file_part = callback.data.split("#", 1)[1]
    pending_file_id = file_part if file_part != "none" else None
    chat_id = callback.message.chat.id
    if not pending_file_id:
        try:
            await callback.message.delete()
        except Exception:
            pass
        await client.send_message(
            chat_id,
            "🔎 <b>Search is ready</b>\n\nType a movie or series name. Access is checked when you choose a file.",
            parse_mode=ParseMode.HTML,
        )
        return

    passed = await check_verification_gates(client, callback, pending_file_id)
    try:
        await callback.message.delete()
    except Exception:
        pass
    if not passed:
        return

    from plugins.req_fsub import _deliver_file

    await _deliver_file(client, chat_id, callback.from_user.id, pending_file_id)
