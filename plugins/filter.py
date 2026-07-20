import os
import re
import time
import random
import string
import asyncio
import logging
from collections import OrderedDict
from urllib.parse import quote
from dotenv import load_dotenv
from pyrogram.errors import MessageNotModified, FloodWait, UserNotParticipant
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
try:
    from pyrogram.types import LinkPreviewOptions
    def _no_preview(): return {"link_preview_options": LinkPreviewOptions(is_disabled=True)}
except ImportError:
    LinkPreviewOptions = None
    def _no_preview(): return {"disable_web_page_preview": True}
from database.db import db
from plugins.req_fsub import check_and_show_req_fsub, check_and_show_two_stage
from utils import is_subscribed, is_subscribed_join_only, send_fsub_message, _parse_fsub_entry, ADMIN_ID
from tmdb import get_movie_data

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

# ── Sort modes for the results-view "sort toggle" row. "smart" is the
# existing series-aware/size-desc default (_sort_results, unchanged).
_SORT_LABELS = {
    "smart": "✨ Smart",
    "size":  "📦 Size",
    "new":   "🆕 Newest",
}


def _html(text: str) -> str:
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


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
        r'\bplease\b', r'\bsend\b', r'\bme\b', r'\bthe\b', r'\bmovie\b',
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


async def _auto_delete_search(status_msg, original_msg, manual_query):
    await asyncio.sleep(300)
    try:
        await status_msg.delete()
    except Exception:
        pass
    if not manual_query:
        try:
            await original_msg.delete()
        except Exception:
            pass


async def _auto_delete_file(sent_msg, file_name, bot_username, delete_seconds=300):
    await asyncio.sleep(delete_seconds)
    try:
        await sent_msg.delete()
    except Exception:
        pass


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


def _apply_sort(results: list, mode: str) -> list:
    """Applies one of the "sort toggle" modes to a full results list.
    "new" sorts by the raw ObjectId (monotonically time-ordered, same trick
    already used elsewhere in this codebase — e.g. db.py's duplicate
    cleanup uses ObjectId.generation_time) rather than needing a separate
    upload-timestamp field."""
    if mode == "size":
        return sorted(results, key=lambda f: f.get("file_size", 0), reverse=True)
    if mode == "new":
        return sorted(results, key=lambda f: f.get("_id"), reverse=True)
    return _sort_results(results)


def _sort_row(session_id: str, current_mode: str) -> list:
    row = []
    for mode, label in _SORT_LABELS.items():
        text = f"✅ {label}" if mode == current_mode else label
        row.append(InlineKeyboardButton(text, callback_data=f"sort#{session_id}#{mode}"))
    return row


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

    f_lang, f_qual = extract_attributes(file_data.get("file_name", ""))
    size_mb  = file_data.get("file_size", 0) / (1024 * 1024)
    size_str = f"{size_mb / 1024:.2f} GB" if size_mb >= 1024 else f"{size_mb:.0f} MB"

    lang_emojis = {
        "Malayalam": "🌴", "Tamil": "🎭", "Telugu": "⭐",
        "Hindi": "🇮🇳", "English": "🌍", "Kannada": "🏵",
        "Dual Audio": "🎧", "Multi Audio": "🎵"
    }
    lang_icon = lang_emojis.get(f_lang, "🎬")
    tag_parts = []
    if f_lang not in ["Other", ""]:
        tag_parts.append(f"{lang_icon} {f_lang}")
    if f_qual not in ["Other", ""]:
        tag_parts.append(f"🎞 {f_qual}")
    tags_line = f"{'  •  '.join(tag_parts)}\n" if tag_parts else ""

    return (
        f"📥 <b>{_html(file_data['file_name'])}</b>\n"
        f"📦 Size: {size_str}\n"
        f"{tags_line}\n"
        f"👇 Use the links below to join or download...\n\n"
        f"<blockquote>⚠️ <b>Files will be deleted in {delete_minutes} minutes. If you want to keep "
        f"this file, kindly forward it to any chat (Saved Messages) and start the download...</b></blockquote>\n"
        f"📢 @{bot_username}"
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
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"page#{session_id}#{page-1}"))
    if total_pages > 1:
        nav.append(InlineKeyboardButton(f"🟢 {page+1}/{total_pages} 🟢", callback_data="ignore"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"page#{session_id}#{page+1}"))
    if nav:
        buttons.append(nav)

    return buttons, page, total_pages


async def _render_results_view(client, message, session_id: str, page: int, data: dict, user_id=None):
    """Single shared renderer for every results screen — the initial
    search render, Prev/Next pagination, sort-mode switches, the series
    expand toggle, and the Data Saver toggle all funnel through this one
    function so they can never drift out of sync with each other (the old
    code duplicated this whole block between show_results and
    handle_pagination).
    """
    results   = data["results"]
    query     = data["query"]
    tmdb      = data.get("tmdb")
    sort_mode = data.get("sort_mode", "smart")
    series_expanded = data.get("series_expanded", False)

    total     = len(results)
    _del_secs = int(data.get("auto_delete_time", 300))
    _del_mins = max(1, _del_secs // 60)

    uid = user_id if user_id is not None else data.get("user_id")
    data_saver = await db.get_data_saver(uid) if uid is not None else False
    first_name = data.get("first_name", "")

    has_series = _has_series_content(results)
    buttons = []

    if has_series and not series_expanded:
        page, total_pages = 0, 1
    else:
        file_buttons, page, total_pages = _build_result_buttons(results, session_id, page)
        buttons.extend(file_buttons)

    caption = (
        f"🔍 <b>Results Found For {_html(query)}</b>\n"
        f"🗣️ : {_html(first_name)}\n\n"
        f"📁 <b>Files: {total} - 📚 Page: {page+1}/{total_pages}</b>\n\n"
    )

    if tmdb:
        title_display = _html(tmdb["title"])
        caption += f"🎬 <b>{title_display}</b>\n"
        if tmdb.get("overview"):
            # tap-to-reveal — keeps plot details out of the way until the
            # user actually wants them (also a mild spoiler guard).
            caption += f"<blockquote><tg-spoiler>{_html(tmdb['overview'])}</tg-spoiler></blockquote>\n"
        if tmdb.get("rating"):
            caption += f"⭐ <b>{tmdb['rating']}/10</b>\n"
        caption += "\n"

    caption += f"🗑 Auto-deletes in {_del_mins} mins\n\n"

    if has_series and not series_expanded:
        caption += "👇 Grouped as a series — tap to see every episode:"
        buttons.append([InlineKeyboardButton(
            f"📺 View All Episodes ({total} found)",
            callback_data=f"expandseries#{session_id}"
        )])
    else:
        caption += "👇 Tap a file to receive it in your PM:"

    buttons.append(_sort_row(session_id, sort_mode))

    utility_row = [InlineKeyboardButton("🔍 New Search", callback_data="new_search_hint")]
    if uid is not None:
        saver_label = "📶 Data Saver: ON" if data_saver else "📶 Data Saver: OFF"
        utility_row.append(InlineKeyboardButton(saver_label, callback_data=f"datasaver#{session_id}"))
    buttons.append(utility_row)

    buttons.append([InlineKeyboardButton("🏠 Home", callback_data="start_home")])
    markup = InlineKeyboardMarkup(buttons)

    has_poster = bool(tmdb and tmdb.get("poster"))
    is_media_msg = bool(
        getattr(message, "photo", None) or getattr(message, "video", None) or
        getattr(message, "animation", None) or getattr(message, "document", None)
    )

    try:
        if is_media_msg:
            if data_saver:
                # Downgrade: the session was already showing the poster and
                # the user just enabled Data Saver — Telegram can't turn a
                # photo message into a text message in place, so replace it.
                chat_id = message.chat.id
                await message.delete()
                await client.send_message(
                    chat_id, caption, reply_markup=markup,
                    parse_mode=ParseMode.HTML, **_no_preview()
                )
            else:
                await message.edit_caption(caption=caption, reply_markup=markup, parse_mode=ParseMode.HTML)
        elif page == 0 and has_poster and not data_saver:
            # First render of this session with a poster available and Data
            # Saver off — upgrade the plain status message into a photo.
            chat_id = message.chat.id
            await message.delete()
            await client.send_photo(
                chat_id=chat_id, photo=tmdb["poster"], caption=caption,
                reply_markup=markup, parse_mode=ParseMode.HTML
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
        "start", "help", "about", "admin", "broadcast", "ban", "unban", "purge_cams", "reset_db", "update",
        "request", "filesearch", "stats", "cancel", "reset_index_progress", "confirm_reset"
    ])
)
async def auto_filter(client: Client, message: Message, manual_query=None):
    user_id = message.from_user.id

    if await db.is_banned(user_id):
        return await message.reply_text("🚫 **You are banned from using this bot.**", quote=True)

    config = await db.get_config()
    if config.get("maintenance_mode") and user_id not in ADMIN_ID:
        return await message.reply_text(
            config.get("maintenance_message", "🔧 Bot is under maintenance. Back soon!"),
            quote=True
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
                quote=True
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
                quote=True, parse_mode=ParseMode.HTML
            )

    if not query:
        return

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
        sug_buttons = [
            [InlineKeyboardButton("🔎 Correct Spelling (Google)", url=google_url)]
        ]
        for sug in suggestions:
            safe_sug = re.sub(r"[^a-zA-Z0-9]", "_", sug)[:40]
            sug_buttons.append([InlineKeyboardButton(
                f"💡 {sug[:30]}",
                url=f"https://t.me/{client.me.username}?start=search_{safe_sug}"
            )])

        sug_buttons += [
            [InlineKeyboardButton("📝 Request This Movie", callback_data=f"reqmovie#{safe_query}")],
            [InlineKeyboardButton("🏠 Home", callback_data="start_home")]
        ]

        first_name = _html(message.from_user.first_name or "")
        return await message.reply_text(
            f"<b>Hey {first_name} something Is Wrong ❌</b>\n\n"
            f"🔹 Check IMDb and check the OTT release or Join the Channel below\n"
            f"🔹 Click spelling button below to correct the spelling of the movie And More Information 📥\n\n"
            f"📢 Join Our Community",
            reply_markup=InlineKeyboardMarkup(sug_buttons),
            parse_mode=ParseMode.HTML, **_no_preview()
        )

    time_taken = time.time() - start_time
    await db.clear_old_searches()

    # Only searches that actually returned something are worth surfacing as
    # a "trending" suggestion on the home panel — see _TrendingCache's
    # docstring in db.py.
    await db.log_trending_search(query)

    session_id = "".join(random.choices(string.ascii_letters + string.digits, k=6))

    # Fetch TMDB data if API key is set
    tmdb_data = None
    try:
        best_name = results[0]["file_name"]
        clean_tmdb = re.sub(
            r"(1080p|720p|480p|4K|HDRip|WEB-DL|WEBRip|BluRay|PreDVD|CAM|"
            r"HD.Rip|x264|x265|HEVC|Dual.Audio|Multi.Audio|"
            r"Malayalam|Tamil|Telugu|Hindi|English|Kannada)",
            "", best_name, flags=re.IGNORECASE
        )
        clean_tmdb = re.sub(r"[\(\[].*?[\)\]]", "", clean_tmdb)
        clean_tmdb = re.sub(r"[^a-zA-Z0-9\s]", " ", clean_tmdb).strip()
        if len(clean_tmdb) > 2:
            tmdb_data = await get_movie_data(clean_tmdb)
        if not tmdb_data:
            tmdb_data = await get_movie_data(query)
    except Exception:
        tmdb_data = None

    session_data = {
        "results":          _sort_results(results),
        "query":            query,
        "tmdb":             tmdb_data,
        "speed":            f"{time_taken:.3f}s",
        "time":             time.time(),
        "auto_delete_time": int(config.get("auto_delete_time", 300)),
        "user_id":          user_id,
        "first_name":       message.from_user.first_name or "",
        "sort_mode":        "smart",
    }
    await db.save_search(session_id, session_data)

    status_msg = await message.reply_text("🔍", quote=True)
    await show_results(client, status_msg, session_id, 0)
    asyncio.create_task(_auto_delete_search(status_msg, message, manual_query))


@Client.on_callback_query(filters.regex(r"^page#"))
async def handle_pagination(client: Client, callback: CallbackQuery):
    parts      = callback.data.split("#")
    session_id = parts[1]
    page       = int(parts[2])

    data = await db.get_search(session_id)
    if not data:
        await callback.answer("⚠️ Session expired.", show_alert=True)
        return

    await _render_results_view(client, callback.message, session_id, page, data, user_id=callback.from_user.id)
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^sort#"))
async def handle_sort(client: Client, callback: CallbackQuery):
    parts = callback.data.split("#")
    if len(parts) < 3:
        return await callback.answer()
    session_id, mode = parts[1], parts[2]
    if mode not in _SORT_LABELS:
        return await callback.answer()

    data = await db.get_search(session_id)
    if not data:
        return await callback.answer("⚠️ Session expired.", show_alert=True)

    data["results"]   = _apply_sort(data["results"], mode)
    data["sort_mode"] = mode
    await db.save_search(session_id, data)

    await callback.answer(f"Sorted: {_SORT_LABELS[mode]}")
    await _render_results_view(client, callback.message, session_id, 0, data, user_id=callback.from_user.id)


@Client.on_callback_query(filters.regex(r"^expandseries#"))
async def handle_expand_series(client: Client, callback: CallbackQuery):
    session_id = callback.data.split("#", 1)[1]

    data = await db.get_search(session_id)
    if not data:
        return await callback.answer("⚠️ Session expired.", show_alert=True)

    data["series_expanded"] = True
    await db.save_search(session_id, data)

    await callback.answer()
    await _render_results_view(client, callback.message, session_id, 0, data, user_id=callback.from_user.id)


@Client.on_callback_query(filters.regex(r"^datasaver#"))
async def handle_data_saver_toggle(client: Client, callback: CallbackQuery):
    session_id = callback.data.split("#", 1)[1]

    data = await db.get_search(session_id)
    if not data:
        return await callback.answer("⚠️ Session expired.", show_alert=True)

    user_id   = callback.from_user.id
    new_value = not await db.get_data_saver(user_id)
    await db.set_data_saver(user_id, new_value)

    await callback.answer(f"📶 Data Saver {'ON' if new_value else 'OFF'}", show_alert=False)
    # No page number travels in this callback's data — reset to page 0,
    # same as the sort and expand-series toggles.
    await _render_results_view(client, callback.message, session_id, 0, data, user_id=user_id)


@Client.on_callback_query(filters.regex(r"^new_search_hint$"))
async def new_search_hint_callback(client: Client, callback: CallbackQuery):
    await callback.answer()
    prompt_text = "🔍 <b>Type any movie or series name to search!</b>"
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Home", callback_data="start_home")]])
    msg = callback.message
    try:
        if getattr(msg, "photo", None) or getattr(msg, "video", None) or \
           getattr(msg, "animation", None) or getattr(msg, "document", None):
            await msg.edit_caption(caption=prompt_text, reply_markup=markup, parse_mode=ParseMode.HTML)
        else:
            await msg.edit_text(text=prompt_text, reply_markup=markup, parse_mode=ParseMode.HTML)
    except Exception:
        pass


@Client.on_callback_query(filters.regex(r"^ignore$"))
async def handle_ignore(client: Client, callback: CallbackQuery):
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^sendfile#"))
async def send_movie_file(client: Client, callback: CallbackQuery):
    _, file_obj_id = callback.data.split("#")
    file_data = await db.get_file(file_obj_id)

    if not file_data:
        return await callback.answer("⚠️ File no longer available.", show_alert=True)

    if not await is_subscribed_join_only(client, callback):
        await callback.answer("🔐 Join our channel first!", show_alert=False)
        await send_fsub_message(client, callback, pending_file_id=file_obj_id)
        return

    if not await check_and_show_req_fsub(client, callback, file_obj_id):
        return

    if not await check_and_show_two_stage(client, callback, file_obj_id):
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
        asyncio.create_task(_auto_delete_file(sent, file_data["file_name"], client.me.username, delete_seconds))
    except Exception as e:
        err = str(e).lower()
        if any(k in err for k in ["file_reference", "invalid", "not found", "media"]):
            await db.delete_file_by_id(file_data["file_id"])
            await callback.message.reply_text(
                f"❌ **File Unavailable**\n\n"
                f"{_html(file_data['file_name'])} has expired.\n"
                f"Removed from DB. Please search again.",
                parse_mode=ParseMode.HTML
            )
        else:
            await callback.message.reply_text("❌ Could not send file. Try again.")
        logger.error(f"send_cached_media failed: {e}")


@Client.on_callback_query(filters.regex(r"^check_fsub#"))
async def check_fsub_callback(client: Client, callback: CallbackQuery):
    file_part       = callback.data.split("#")[1]
    pending_file_id = file_part if file_part != "none" else None

    config   = await db.get_config()
    channels = config.get("fsub_channels", [])
    user_id  = callback.from_user.id

    remaining = []
    for i, entry in enumerate(channels, 1):
        channel_id, _ = _parse_fsub_entry(entry)
        if not channel_id:
            continue

        # Same membership check as utils.is_subscribed, per-channel so we can
        # report exactly which channels are still outstanding.
        try:
            ch = int(channel_id) if str(channel_id).lstrip('-').isdigit() else str(channel_id)
            member = await client.get_chat_member(ch, user_id)
            if member.status.name not in ["KICKED", "BANNED", "LEFT"]:
                continue  # already joined
        except UserNotParticipant:
            pass
        except Exception as e:
            logger.warning(f"FSub check error on channel {channel_id}: {e}")
            continue  # treat as joined on error, consistent with is_subscribed

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
            asyncio.create_task(_auto_delete_file(sent, file_data["file_name"], client.me.username, delete_seconds))
        except Exception as e:
            err = str(e).lower()
            if any(k in err for k in ["file_reference", "invalid", "not found", "media"]):
                await db.delete_file_by_id(file_data["file_id"])
                await client.send_message(chat_id, "❌ File unavailable. Please search again.")
            else:
                await client.send_message(chat_id, "❌ Could not send file. Try again.")
    else:
        await client.send_message(
            chat_id,
            "✅ **Verification Successful!**\n\n"
            "<blockquote>You're all set! Type any movie name to search.</blockquote>",
            parse_mode=ParseMode.HTML
        )
