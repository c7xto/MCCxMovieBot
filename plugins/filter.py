import os
import re
import time
import secrets
import asyncio
import logging
from collections import OrderedDict
from urllib.parse import quote
from dotenv import load_dotenv
from pyrogram.errors import (
    MessageNotModified, FloodWait, UserNotParticipant,
    FileIdInvalid, FileReferenceEmpty, FileReferenceExpired,
    FileReferenceInvalid, MediaEmpty, MediaInvalid,
)
from pyrogram import Client, filters
from pyrogram.enums import ParseMode, ChatAction
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from database.db import db
from plugins.req_fsub import check_verification_gates
from utils import (
    is_subscribed, is_subscribed_join_only, send_fsub_message, _parse_fsub_entry,
    ADMIN_ID, _no_preview, _html, callback_data,
)

load_dotenv()
logger = logging.getLogger(__name__)

IGNORE_WORDS = {"hi", "hello", "bro", "pls", "plz", "bot", "help", "admin", "sir"}

_COOLDOWN_MAX = 10000
USER_SEARCH_COOLDOWN = OrderedDict()  # LRU: oldest entry is first
COOLDOWN_TIME = 2

LANGUAGES = ["Malayalam", "Tamil", "Telugu", "Hindi", "English", "Kannada", "Dual Audio", "Multi Audio"]
QUALITIES  = ["4K", "1080p", "720p", "480p", "HDRip", "WEB-DL", "WEBRip", "BluRay", "PreDVD", "CAM", "HD Rip"]

LANG_EMOJI = {
    "Malayalam": "🌴", "Tamil": "🎭", "Telugu": "⭐",
    "Hindi": "🇮🇳", "English": "🌍", "Kannada": "🏵",
    "Dual Audio": "🎧", "Multi Audio": "🎵", "Other": "🌐"
}

async def send_smart_log(client, text):
    try:
        config   = await db.get_config()
        log_chat = config.get("log_channel", 0)
        if log_chat:
            await client.send_message(log_chat, text, **_no_preview())
    except Exception:
        pass


# Precompiled once at import time rather than rebuilt from pattern strings on
# every incoming search/result-render — clean_query() runs on every message
# and extract_attributes() runs once per file on every page render.
_STOP_WORD_RES = [
    re.compile(p, re.IGNORECASE) for p in (
        r'\bplease\b', r'\bsend\b', r'\bme\b', r'\bmovie\b',
        r'\bseries\b', r'\bhd\b', r'\bprint\b', r'\bdownload\b', r'\blink\b',
        r'\bbro\b', r'\bcan\b', r'\byou\b', r'\bprovide\b', r'\bi\b',
        r'\bneed\b', r'\bwant\b'
    )
]
_WS_RE = re.compile(r'\s+')


def clean_query(query):
    q = query.lower()
    for pattern in _STOP_WORD_RES:
        q = pattern.sub('', q)
    return _WS_RE.sub(' ', q).strip()


_LANG_RES = [(l, re.compile(r'\b' + l + r'\b', re.IGNORECASE)) for l in LANGUAGES]
_QUAL_RES = [(q, re.compile(r'\b' + q.replace(' ', r'\s*') + r'\b', re.IGNORECASE)) for q in QUALITIES]


def extract_attributes(filename):
    lang = next((l for l, pat in _LANG_RES if pat.search(filename)), "Other")
    qual = next((q for q, pat in _QUAL_RES if pat.search(filename)), "Other")
    if qual.lower() == "hdrip":
        qual = "HD Rip"
    return lang, qual


# ── Display-only title cleanup ───────────────────────────────────────────────
# Never touches the stored file_name — purely how a filename is *rendered*
# in a button label or delivered-file caption.
_EXT_RE = re.compile(r'\.(mkv|mp4|avi|mov|zip|srt)$', re.IGNORECASE)
_YEAR_RE = re.compile(r'^(19\d{2}|20\d{2})$')
_PAREN_YEAR_RE = re.compile(r'[\(\[](\d{4})[\)\]]')
_BRACKET_GROUP_RE = re.compile(r'[\[\(].*?[\]\)]')
_TITLE_SEP_RE = re.compile(r'[._]')
_TITLE_WS_RE = re.compile(r'\s+')

# Codec/subtitle/release-group tags that aren't in LANGUAGES/QUALITIES but
# still need to be recognized as "metadata, not title" when walking backward
# through a filename's tokens (see _display_title below).
_JUNK_WORDS = {"x264", "x265", "hevc", "aac", "esub", "hsub", "10bit",
               "hdcam", "tsrip", "dvdrip", "hq", "nf", "amzn", "brrip", "bdrip"}
_QUALITY_LOWER = {q.lower() for q in QUALITIES}
_LANGUAGE_LOWER = {l.lower() for l in LANGUAGES}


def _is_year_token(tok: str) -> bool:
    return bool(_YEAR_RE.match(tok))


def _is_metadata_word(tok: str) -> bool:
    """Quality/language/codec-junk classification — deliberately excludes
    year tokens, which _display_title handles separately (a filename can
    legitimately contain two 4-digit-looking tokens — a numeric title like
    "1917"/"2012" AND its real release year — and only one of them should
    ever be consumed as metadata)."""
    low = tok.lower()
    if low in _QUALITY_LOWER or low in _LANGUAGE_LOWER or low in _JUNK_WORDS:
        return True
    if '-' in low:
        parts = [p for p in low.split('-') if p]
        if parts and all(p in _JUNK_WORDS or p in _QUALITY_LOWER for p in parts):
            return True
    return False


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
    name = _EXT_RE.sub('', filename)

    # A parenthesized/bracketed 4-digit year is unambiguous regardless of
    # what the rest of the title looks like (a numeric title like "300" or
    # "2012" is never itself wrapped in its own parens).
    paren_match = _PAREN_YEAR_RE.search(name)
    paren_year = paren_match.group(1) if paren_match else None

    # Strip every complete bracket group (site tags like [TamilMV], the
    # year annotation just captured above, etc.) — replace with a space,
    # not empty, so two words butting up against a bracket don't fuse.
    name = _BRACKET_GROUP_RE.sub(' ', name)
    # Normalize remaining separators to spaces before tokenizing —
    # underscore is a \w character, so naive \b-based regex matching would
    # silently fail against underscore-separated filenames otherwise.
    norm = _TITLE_SEP_RE.sub(' ', name)
    tokens = [t for t in _TITLE_WS_RE.split(norm) if t]

    i = len(tokens)
    year_consumed = False
    while i > 0:
        if i >= 2:
            phrase = f"{tokens[i-2]} {tokens[i-1]}".lower()
            if phrase in _QUALITY_LOWER or phrase in _LANGUAGE_LOWER:
                i -= 2
                continue
        tok = tokens[i-1]
        if _is_year_token(tok):
            if year_consumed:
                break  # a 2nd year-shaped token further back is the title, not metadata
            year_consumed = True
            i -= 1
            continue
        if _is_metadata_word(tok):
            i -= 1
            continue
        break

    title = " ".join(tokens[:i]).strip()
    tail_tokens = tokens[i:]

    if not title:
        title = " ".join(tokens)  # nothing recognized — no safe cut point

    if paren_year:
        year = paren_year
    else:
        year = next((t for t in tail_tokens if _is_year_token(t)), "")

    return (title or filename.strip(), year)


def _variant_label(f, show_title: bool, title=None, year=None) -> str:
    """Builds one file's button label as Title (Year) · Language · Quality ·
    Size — or, when show_title is False, just Language · Quality · Size.
    The latter is used for the single-result hero card's metadata line,
    where the title is already the bolded caption headline just above it."""
    if title is None:
        title, year = _display_title(f["file_name"])
    f_lang, f_qual = extract_attributes(f["file_name"])
    size_str = _fmt_size(f)

    parts = []
    if show_title:
        parts.append(f"{title} ({year})" if year else title)
    if f_lang not in ("Other", ""):
        parts.append(f_lang)
    if f_qual not in ("Other", ""):
        parts.append(f_qual)
    parts.append(size_str)

    label = " · ".join(parts)
    return label if len(label) <= 52 else label[:51] + "…"


def _build_movie_result_buttons(results: list, session_id: str, page: int, per_page: int = 8):
    """Movie-search button builder — groups same-title files (different
    quality/language variants) next to each other instead of leaving them
    scattered by the size-based sort, and shows a clean Title (Year) ·
    Language · Quality · Size label instead of the raw indexed filename.
    Every button (grouped or not) is independently tappable and self-
    labeled — no separate non-clickable "header" row, since a keyboard
    button that does nothing on tap is worse UX than a bit of repeated
    title text. Grouping is purely a rendering-order choice: pagination
    math (page/total_pages) and the sendfile# callback on every button are
    unchanged, so this never adds a tap versus the un-grouped list. Series
    results use the separate, untouched _build_result_buttons() below
    instead — grouping by title would otherwise put every episode of a
    series next to each other with an identical label."""
    total = len(results)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, total_pages - 1))
    start_idx = page * per_page
    page_files = results[start_idx: start_idx + per_page]

    groups = OrderedDict()
    for f in page_files:
        title, year = _display_title(f["file_name"])
        key = title.lower()
        if key not in groups:
            groups[key] = {"title": title, "year": year, "files": []}
        groups[key]["files"].append(f)

    buttons = []
    for group in groups.values():
        for f in group["files"]:
            label = _variant_label(f, show_title=True, title=group["title"], year=group["year"])
            buttons.append([InlineKeyboardButton(label, callback_data=f"sendfile#{f['_id']}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("‹ Previous", callback_data=f"page#{session_id}#{page-1}"))
    if total_pages > 1:
        nav.append(InlineKeyboardButton(f"{page+1} / {total_pages}", callback_data="ignore"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ›", callback_data=f"page#{session_id}#{page+1}"))
    if nav:
        buttons.append(nav)

    return buttons, page, total_pages


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
    return f"{size_mb:.0f} MB"


_SERIES_RE = re.compile(r'\b[Ss]\d{1,2}[Ee]\d{1,2}\b|\b[Ss]eason\s*\d+\b|\b[Ee]pisode\s*\d+\b', re.IGNORECASE)
_SEASON_NUM_RE = re.compile(r'[Ss](\d{1,2})')
_EPISODE_NUM_RE = re.compile(r'[Ee](\d{1,2})')


def _is_series(filename: str) -> bool:
    return bool(_SERIES_RE.search(filename))


def _has_series_content(results: list) -> bool:
    return any(_is_series(f.get("file_name", "")) for f in results)


def _series_sort_key(f):
    name = f.get("file_name", "")
    s = _SEASON_NUM_RE.search(name)
    e = _EPISODE_NUM_RE.search(name)
    season  = int(s.group(1)) if s else 0
    episode = int(e.group(1)) if e else 0
    return (season, episode)


def _sort_results(results: list) -> list:
    """"Smart" sort — the pre-existing default: series-aware chronological
    order if any series content is detected, else size descending."""
    if not results:
        return results
    has_series = any(_is_series(f.get("file_name", "")) for f in results)
    if has_series:
        return sorted(results, key=_series_sort_key)
    return sorted(results, key=lambda f: f.get("file_size", 0), reverse=True)


def _build_caption(config, file_data, delete_minutes, bot_username):
    template = config.get("file_caption_template", "")
    if template:
        f_lang, f_qual = extract_attributes(file_data.get("file_name", ""))
        size_mb  = file_data.get("file_size", 0) / (1024 * 1024)
        size_str = f"{size_mb / 1024:.2f}GB" if size_mb > 1024 else f"{size_mb:.0f}MB"
        try:
            return template.format(
                filename=file_data.get("file_name", ""),
                size=size_str,
                quality=f_qual or "Unknown",
                lang=f_lang or "Unknown",
                username=bot_username,
                delete_minutes=delete_minutes
            )
        except (KeyError, ValueError):
            pass

    title, year = _display_title(file_data.get("file_name", ""))
    f_lang, f_qual = extract_attributes(file_data.get("file_name", ""))
    size_mb  = file_data.get("file_size", 0) / (1024 * 1024)
    size_str = f"{size_mb / 1024:.2f} GB" if size_mb >= 1024 else f"{size_mb:.0f} MB"

    lang_emojis = {
        "Malayalam": "🌴", "Tamil": "🎭", "Telugu": "⭐",
        "Hindi": "🇮🇳", "English": "🌍", "Kannada": "🏵",
        "Dual Audio": "🎧", "Multi Audio": "🎵"
    }
    meta_parts = []
    if f_lang not in ["Other", ""]:
        meta_parts.append(f"{lang_emojis.get(f_lang, '🎬')} {f_lang}")
    if f_qual not in ["Other", ""]:
        meta_parts.append(f"🎞 {f_qual}")
    meta_parts.append(f"📦 {size_str}")
    meta_line = "  ·  ".join(meta_parts)

    title_line = f"🎬 <b>{_html(title)}{f' ({year})' if year else ''}</b>"

    return (
        f"{title_line}\n"
        f"<blockquote>{meta_line}</blockquote>\n"
        f"<i>⏳ Available for {delete_minutes} min · Forward it to keep it</i>"
    )


def _build_result_buttons(results: list, session_id: str, page: int, per_page: int = 8):
    """Builds the file-button rows plus Prev/Next row for one page of
    results. Used for the plain movie-results view and the expanded
    (per-episode) series view — they render identically once expanded."""
    total = len(results)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, total_pages - 1))
    start_idx = page * per_page
    page_files = results[start_idx: start_idx + per_page]

    buttons = []
    for f in page_files:
        f_lang, f_qual = extract_attributes(f["file_name"])
        size_str = _fmt_size(f)
        name = re.sub(r'\s+', ' ', f["file_name"]).strip()

        meta_parts = []
        if f_qual not in ["Other", ""]:
            meta_parts.append(f_qual)
        if f_lang not in ["Other", ""]:
            meta_parts.append(f_lang)

        meta     = " | ".join(meta_parts)
        size_tag = f"[{size_str}]"

        if meta:
            available = 48 - len(size_tag) - len(meta) - 4
            truncated = name[:max(10, available)] + ("…" if len(name) > max(10, available) else "")
            btn_text  = f"{size_tag} {truncated} | {meta}"
        else:
            available = 52 - len(size_tag) - 1
            truncated = name[:available] + ("…" if len(name) > available else "")
            btn_text  = f"{size_tag} {truncated}"

        buttons.append([InlineKeyboardButton(btn_text, callback_data=f"sendfile#{f['_id']}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("‹ Previous", callback_data=f"page#{session_id}#{page-1}"))
    if total_pages > 1:
        nav.append(InlineKeyboardButton(f"{page+1} / {total_pages}", callback_data="ignore"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ›", callback_data=f"page#{session_id}#{page+1}"))
    if nav:
        buttons.append(nav)

    return buttons, page, total_pages


async def _render_results_view(client, message, session_id: str, page: int, data: dict, user_id=None):
    """Single shared renderer for every results screen — the initial
    search render, Prev/Next pagination, and the series expand toggle all
    funnel through this one function so they can never drift out of sync
    with each other (the old code duplicated this whole block between
    show_results and handle_pagination).
    """
    results = data["results"]
    query   = data["query"]
    series_expanded = data.get("series_expanded", False)

    total     = len(results)
    _del_secs = int(data.get("auto_delete_time", 300))
    _del_mins = max(1, _del_secs // 60)

    has_series = _has_series_content(results)

    # A single, unambiguous match gets a direct "hero" card instead of the
    # list view — still exactly one tap to the file, same sendfile#
    # callback and verification-gate path as every other result.
    if total == 1 and not has_series:
        f = results[0]
        title, year = _display_title(f["file_name"])
        meta = _variant_label(f, show_title=False)
        caption = (
            f"🎬 <b>{_html(title)}{f' ({year})' if year else ''}</b>\n"
            f"<blockquote>{meta}</blockquote>\n"
            f"<i>⏳ Available for {_del_mins} min</i>"
        )
        buttons = [
            [InlineKeyboardButton("⬇ Get This File", callback_data=f"sendfile#{f['_id']}")],
            [InlineKeyboardButton("⌂ Home", callback_data="start_home")],
        ]
        markup = InlineKeyboardMarkup(buttons)
    else:
        buttons = []

        if has_series and not series_expanded:
            page, total_pages = 0, 1
        elif has_series:
            file_buttons, page, total_pages = _build_result_buttons(results, session_id, page)
            buttons.extend(file_buttons)
        else:
            file_buttons, page, total_pages = _build_movie_result_buttons(results, session_id, page)
            buttons.extend(file_buttons)

        caption = (
            f"<b>🎬 {_html(query.title())}</b>\n"
            f"<blockquote>{total} matches  •  Results expire in {_del_mins} min</blockquote>\n"
        )

        if has_series and not series_expanded:
            caption += "Episodes are grouped to keep this list tidy."
            buttons.append([InlineKeyboardButton(
                f"📺 Browse All {total} Episodes",
                callback_data=f"expandseries#{session_id}"
            )])
        else:
            caption += "<i>Choose a file by size, language and quality.</i>"

        buttons.append([InlineKeyboardButton("⌂ Back to Home", callback_data="start_home")])
        markup = InlineKeyboardMarkup(buttons)

    # Results render as plain text only now — there's no TMDB poster to
    # upgrade a status message into a photo with. The media-message branch
    # is just a safe fallback for a session started just before this deploy.
    is_media_msg = bool(
        getattr(message, "photo", None) or getattr(message, "video", None) or
        getattr(message, "animation", None) or getattr(message, "document", None)
    )

    try:
        if is_media_msg:
            chat_id = message.chat.id
            await message.delete()
            await client.send_message(
                chat_id, caption, reply_markup=markup,
                parse_mode=ParseMode.HTML, **_no_preview()
            )
        else:
            await message.edit_text(text=caption, reply_markup=markup, parse_mode=ParseMode.HTML)
    except MessageNotModified:
        pass
    except Exception as e:
        logger.error(f"_render_results_view error: {e}")


async def show_results(client, message, session_id, page):
    data = await db.get_search(session_id)
    if not data:
        try:
            await message.edit_text("⚠️ Session expired. Search again.")
        except Exception:
            pass
        return
    await _render_results_view(client, message, session_id, page, data)


route_menu = show_results


@Client.on_message(
    filters.text & filters.private &
    ~filters.command([
        "start", "help", "admin", "broadcast", "ban", "unban", "reset_db", "update",
        "request", "filesearch", "stats", "cancel", "reset_index_progress", "confirm_reset"
    ])
)
async def auto_filter(client: Client, message: Message, manual_query=None):
    user_id = message.from_user.id

    if await db.is_banned(user_id):
        return await message.reply_text("🚫 **You are banned from using this bot.**", reply_parameters=None)

    config = await db.get_config()
    if config.get("maintenance_mode") and user_id not in ADMIN_ID:
        return await message.reply_text(
            config.get("maintenance_message", "🔧 Bot is under maintenance. Back soon!"),
            reply_parameters=None
        )

    if not await is_subscribed_join_only(client, message):
        await send_fsub_message(client, message)
        return

    current_time = time.time()
    if user_id in USER_SEARCH_COOLDOWN:
        passed = current_time - USER_SEARCH_COOLDOWN[user_id]
        USER_SEARCH_COOLDOWN.move_to_end(user_id)
        if passed < COOLDOWN_TIME:
            warning = await message.reply_text(
                f"⏳ Wait `{int(COOLDOWN_TIME - passed) + 1}s` before searching again.",
                reply_parameters=None
            )
            await asyncio.sleep(2)
            try:
                await warning.delete()
            except Exception:
                pass
            return

    if len(USER_SEARCH_COOLDOWN) >= _COOLDOWN_MAX:
        USER_SEARCH_COOLDOWN.popitem(last=False)  # evict least-recently-used
    USER_SEARCH_COOLDOWN[user_id] = current_time
    USER_SEARCH_COOLDOWN.move_to_end(user_id)

    if manual_query:
        query = manual_query
    else:
        raw   = message.text
        query = clean_query(raw)
        if len(raw.strip()) < 3 or raw.strip().lower() in IGNORE_WORDS:
            main_group = config.get("main_group", "")
            return await message.reply_text(
                f"<b>Type a movie or series name to search!</b>",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("📝 Request Here", url=main_group)]]
                ) if main_group else None,
                reply_parameters=None, parse_mode=ParseMode.HTML
            )

    if not query:
        return

    try:
        await client.send_chat_action(message.chat.id, ChatAction.TYPING)
    except Exception:
        pass

    start_time = time.time()
    results    = await db.get_search_results(query)

    if not results:
        google_url = f"https://www.google.com/search?q={quote(query)}"
        safe_query = query[:40]

        should_alert = await db.log_missed_search(query)
        if should_alert:
            asyncio.create_task(send_smart_log(
                client,
                f"❌ **#MissedSearch**\n\n🎬 `{query}`\n"
                f"👤 {message.from_user.mention}\n📍 Private Chat"
            ))

        suggestions = await db.get_prefix_suggestions(query, limit=3)
        sug_row = []
        for sug in suggestions:
            safe_sug = re.sub(r"[^a-zA-Z0-9]", "_", sug)[:40]
            sug_row.append(InlineKeyboardButton(
                f"💡 {sug[:20]}",
                url=f"https://t.me/{client.me.username}?start=search_{safe_sug}"
            ))

        sug_buttons = [sug_row] if sug_row else []
        sug_buttons += [
            [InlineKeyboardButton("📝 Request This Movie", callback_data=callback_data("reqmovie#", safe_query)),
             InlineKeyboardButton("🔎 Google", url=google_url)],
            [InlineKeyboardButton("⌂ Back to Home", callback_data="start_home")]
        ]

        no_results_msg = await message.reply_text(
            f"🔍 Nothing found for <code>{_html(query)}</code>\n\n"
            f"It's probably not uploaded yet — try a suggestion below, or "
            f"request it and we'll notify you the moment it's added.",
            reply_markup=InlineKeyboardMarkup(sug_buttons),
            parse_mode=ParseMode.HTML, **_no_preview()
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
        "results":          _sort_results(results),
        "query":            query,
        "speed":            f"{time_taken:.3f}s",
        "time":             time.time(),
        "auto_delete_time": int(config.get("auto_delete_time", 300)),
        "user_id":          user_id,
        "first_name":       message.from_user.first_name or "",
    }
    await db.save_search(session_id, session_data)

    status_msg = await message.reply_text(
        "🔎 <b>Searching your library…</b>", reply_parameters=None, parse_mode=ParseMode.HTML
    )
    await show_results(client, status_msg, session_id, 0)
    await _auto_delete_search(status_msg, message, manual_query)


@Client.on_callback_query(filters.regex(r"^page#"))
async def handle_pagination(client: Client, callback: CallbackQuery):
    parts      = callback.data.split("#")
    session_id = parts[1]
    page       = int(parts[2])

    data = await db.get_search(session_id)
    if not data:
        await callback.answer("⚠️ Session expired.", show_alert=True)
        return
    if data.get("user_id") not in (None, callback.from_user.id):
        await callback.answer("This search belongs to another user.", show_alert=True)
        return

    await _render_results_view(client, callback.message, session_id, page, data, user_id=callback.from_user.id)
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^expandseries#"))
async def handle_expand_series(client: Client, callback: CallbackQuery):
    session_id = callback.data.split("#", 1)[1]

    data = await db.get_search(session_id)
    if not data:
        return await callback.answer("⚠️ Session expired.", show_alert=True)
    if data.get("user_id") not in (None, callback.from_user.id):
        return await callback.answer("This search belongs to another user.", show_alert=True)

    data["series_expanded"] = True
    await db.save_search(session_id, data)

    await callback.answer()
    await _render_results_view(client, callback.message, session_id, 0, data, user_id=callback.from_user.id)


@Client.on_callback_query(filters.regex(r"^ignore$"))
async def handle_ignore(client: Client, callback: CallbackQuery):
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^sendfile#"))
async def send_movie_file(client: Client, callback: CallbackQuery):
    _, file_obj_id = callback.data.split("#")
    file_data = await db.get_file(file_obj_id)

    if not file_data:
        return await callback.answer("⚠️ File no longer available.", show_alert=True)

    if not await check_verification_gates(client, callback, file_obj_id):
        return

    await callback.answer("📤 Sending file...", show_alert=False)

    config         = await db.get_config()
    delete_seconds = int(config.get("auto_delete_time", 300))
    delete_minutes = delete_seconds // 60

    try:
        sent = await client.send_cached_media(
            chat_id=callback.message.chat.id,
            file_id=file_data["file_id"],
            caption=_build_caption(config, file_data, delete_minutes, client.me.username),
            parse_mode=ParseMode.HTML
        )
        await _auto_delete_file(sent, file_data["file_name"], client.me.username, delete_seconds)
    except (FileIdInvalid, FileReferenceEmpty, FileReferenceExpired,
            FileReferenceInvalid, MediaEmpty, MediaInvalid) as e:
        await db.delete_file_by_id(file_data["file_id"])
        await callback.message.reply_text(
            f"❌ <b>File unavailable</b>\n\n"
            f"{_html(file_data['file_name'])} is no longer valid. "
            f"It was removed from the index; please search again.",
            parse_mode=ParseMode.HTML
        )
        logger.warning("Removed invalid cached file %s: %s", file_data["file_id"], e)
    except Exception as e:
        await callback.message.reply_text("❌ Could not send this file right now. Please try again.")
        logger.error(f"send_cached_media failed: {e}")


async def _fsub_needs_join(client, channel_id, user_id) -> bool:
    """True if this user still needs to join channel_id — same per-channel
    try/except semantics check_fsub_callback always used: KICKED/BANNED/LEFT
    or UserNotParticipant means still needs to join; any other error is
    treated as already-joined (fail open, consistent with
    utils.is_subscribed elsewhere)."""
    try:
        ch = int(channel_id) if str(channel_id).lstrip('-').isdigit() else str(channel_id)
        member = await client.get_chat_member(ch, user_id)
        return member.status.name in ["KICKED", "BANNED", "LEFT"]
    except UserNotParticipant:
        return True
    except Exception as e:
        logger.warning(f"FSub check error on channel {channel_id}: {e}")
        return False


@Client.on_callback_query(filters.regex(r"^check_fsub#"))
async def check_fsub_callback(client: Client, callback: CallbackQuery):
    file_part       = callback.data.split("#")[1]
    pending_file_id = file_part if file_part != "none" else None

    config   = await db.get_config()
    channels = config.get("fsub_channels", [])
    user_id  = callback.from_user.id

    # Parse first, preserving each channel's original 1-based position (i)
    # in `channels` for the "Channel {i}" fallback name below, exactly as
    # the old sequential loop did.
    valid = []
    for i, entry in enumerate(channels, 1):
        channel_id, _ = _parse_fsub_entry(entry)
        if channel_id:
            valid.append((i, entry, channel_id))

    # Membership checks are independent Telegram API calls — run them
    # concurrently instead of one at a time.
    raw_flags = await asyncio.gather(
        *[_fsub_needs_join(client, channel_id, user_id) for _, _, channel_id in valid],
        return_exceptions=True
    ) if valid else []
    # _fsub_needs_join already fails open (returns False = "doesn't need to
    # join") on any error internally — same defense-in-depth reasoning as
    # req_fsub.py's gather calls: this should never actually see an
    # exception object, but guards against a future change silently
    # breaking the fail-open guarantee.
    needs_join_flags = [False if isinstance(r, BaseException) else r for r in raw_flags]

    remaining = []
    for (i, entry, channel_id), needs_join in zip(valid, needs_join_flags):
        if not needs_join:
            continue  # already joined

        # Same join-link resolution as utils.send_fsub_message.
        try:
            ch_str      = str(channel_id).strip()
            stored_link = entry.get("link") if isinstance(entry, dict) else None
            if ch_str.startswith("@"):
                link = f"https://t.me/{ch_str[1:]}"
            elif stored_link and not stored_link.startswith("tg://"):
                link = stored_link
            elif ch_str.startswith("-100"):
                link = await client.export_chat_invite_link(int(ch_str))
                await db.update_fsub_channel_link(channel_id, link)
            elif ch_str.startswith("http"):
                link = ch_str
            else:
                link = f"https://t.me/{ch_str}"
        except Exception as e:
            logger.warning(f"Could not build FSub join link for {channel_id}: {e}")
            continue

        try:
            chat    = await client.get_chat(int(ch_str) if ch_str.lstrip('-').isdigit() else ch_str)
            ch_name = (getattr(chat, "title", None) or f"Channel {i}")[:30]
        except Exception:
            ch_name = f"Channel {i}"
        remaining.append([InlineKeyboardButton(f"📢 Join {ch_name}", url=link)])

    if remaining:
        remaining.append([InlineKeyboardButton(
            "✅ I've Joined — Check Now",
            callback_data=f"check_fsub#{file_part}"
        )])
        await callback.answer(
            f"❌ Still need to join {len(remaining)-1} channel(s).",
            show_alert=True
        )
        try:
            await callback.message.edit_reply_markup(InlineKeyboardMarkup(remaining))
        except Exception:
            pass
        return

    await callback.answer("✅ Verified! Sending your file...", show_alert=False)
    chat_id = callback.message.chat.id
    try:
        await callback.message.delete()
    except Exception:
        pass

    if pending_file_id:
        file_data = await db.get_file(pending_file_id)
        if not file_data:
            await client.send_message(chat_id, "✅ Verified! But the file is no longer available.")
            return

        cfg            = await db.get_config()
        delete_seconds = int(cfg.get("auto_delete_time", 300))
        delete_minutes = delete_seconds // 60

        try:
            sent = await client.send_cached_media(
                chat_id=chat_id,
                file_id=file_data["file_id"],
                caption=_build_caption(cfg, file_data, delete_minutes, client.me.username),
                parse_mode=ParseMode.HTML
            )
            await _auto_delete_file(sent, file_data["file_name"], client.me.username, delete_seconds)
        except (FileIdInvalid, FileReferenceEmpty, FileReferenceExpired,
                FileReferenceInvalid, MediaEmpty, MediaInvalid) as e:
            await db.delete_file_by_id(file_data["file_id"])
            await client.send_message(chat_id, "❌ File unavailable. Please search again.")
            logger.warning("Removed invalid cached file %s: %s", file_data["file_id"], e)
        except Exception as e:
            await client.send_message(chat_id, "❌ Could not send file right now. Try again.")
            logger.error("FSub delivery failed: %s", e)
    else:
        await client.send_message(
            chat_id,
            "✅ **Verification Successful!**\n\n"
            "<blockquote>You're all set! Type any movie name to search.</blockquote>",
            parse_mode=ParseMode.HTML
        )
