import os
import re
import time
import random
import string
import asyncio
import logging
from urllib.parse import quote
from dotenv import load_dotenv
from pyrogram.errors import MessageNotModified, FloodWait
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
from plugins.req_fsub import check_and_show_req_fsub
from utils import is_subscribed, is_subscribed_join_only, send_fsub_message
from tmdb import get_movie_data

load_dotenv()
logger = logging.getLogger(__name__)
_ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

MISSED_CACHE = set()
IGNORE_WORDS = {"hi", "hello", "bro", "pls", "plz", "bot", "help", "admin", "sir"}

_COOLDOWN_MAX = 10000
USER_SEARCH_COOLDOWN = {}
COOLDOWN_TIME = 2

LANGUAGES = ["Malayalam", "Tamil", "Telugu", "Hindi", "English", "Kannada", "Dual Audio", "Multi Audio"]
QUALITIES  = ["4K", "1080p", "720p", "480p", "HDRip", "WEB-DL", "WEBRip", "BluRay", "PreDVD", "CAM", "HD Rip"]

LANG_EMOJI = {
    "Malayalam": "🌴", "Tamil": "🎭", "Telugu": "⭐",
    "Hindi": "🇮🇳", "English": "🌍", "Kannada": "🏵",
    "Dual Audio": "🎧", "Multi Audio": "🎵", "Other": "🌐"
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


def clean_query(query):
    stop_words = [
        r'\bplease\b', r'\bsend\b', r'\bme\b', r'\bthe\b', r'\bmovie\b',
        r'\bseries\b', r'\bhd\b', r'\bprint\b', r'\bdownload\b', r'\blink\b',
        r'\bbro\b', r'\bcan\b', r'\byou\b', r'\bprovide\b', r'\bi\b',
        r'\bneed\b', r'\bwant\b'
    ]
    q = query.lower()
    for w in stop_words:
        q = re.sub(w, '', q, flags=re.IGNORECASE)
    return re.sub(r'\s+', ' ', q).strip()


def extract_attributes(filename):
    lang = next(
        (l for l in LANGUAGES if re.search(r'\b' + l + r'\b', filename, re.IGNORECASE)),
        "Other"
    )
    qual = next(
        (q for q in QUALITIES if re.search(r'\b' + q.replace(' ', r'\s*') + r'\b', filename, re.IGNORECASE)),
        "Other"
    )
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


def _is_series(filename: str) -> bool:
    return bool(re.search(r'\b[Ss]\d{1,2}[Ee]\d{1,2}\b|\b[Ss]eason\s*\d+\b|\b[Ee]pisode\s*\d+\b', filename, re.IGNORECASE))


def _series_sort_key(f):
    name = f.get("file_name", "")
    s = re.search(r'[Ss](\d{1,2})', name)
    e = re.search(r'[Ee](\d{1,2})', name)
    season  = int(s.group(1)) if s else 0
    episode = int(e.group(1)) if e else 0
    return (season, episode)


def _sort_results(results: list) -> list:
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

    f_lang, f_qual = extract_attributes(file_data.get("file_name", ""))
    size_mb  = file_data.get("file_size", 0) / (1024 * 1024)
    size_str = f"{size_mb / 1024:.2f} GB" if size_mb >= 1024 else f"{size_mb:.0f} MB"

    lang_emojis = {
        "Malayalam": "🌴", "Tamil": "🎭", "Telugu": "⭐",
        "Hindi": "🇮🇳", "English": "🌍", "Kannada": "🏵",
        "Dual Audio": "🎧", "Multi Audio": "🎵"
    }
    lang_icon = lang_emojis.get(f_lang, "🎬")
    parts     = []
    if f_lang not in ["Other", ""]:
        parts.append(f"{lang_icon} {f_lang}")
    if f_qual not in ["Other", ""]:
        parts.append(f"🎞 {f_qual}")
    parts.append(f"💿 {size_str}")
    meta_line = "  •  ".join(parts)

    return (
        f"🍿 <b>{_html(file_data['file_name'])}</b>\n"
        f"<blockquote>{meta_line}\n\n"
        f"⏳ Auto-deletes in <b>{delete_minutes} mins</b> — forward to save!</blockquote>\n"
        f"📢 @{bot_username}"
    )


async def show_results(client, message, session_id, page):
    data = await db.get_search(session_id)
    if not data:
        try:
            await message.edit_text("⚠️ Session expired. Search again.")
        except Exception:
            pass
        return

    results     = data["results"]
    query       = data["query"]
    tmdb        = data.get("tmdb")
    per_page    = 8
    total       = len(results)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page        = max(0, min(page, total_pages - 1))
    start_idx   = page * per_page
    page_files  = results[start_idx: start_idx + per_page]

    _del_secs = int(data.get("auto_delete_time", 300))
    _del_mins = max(1, _del_secs // 60)

    title_display = _html(tmdb["title"] if tmdb else query.title())

    caption = (
        f"🎬 <b>{title_display}</b>\n"
    )

    if tmdb:
        if tmdb.get("overview"):
            caption += f"<blockquote>{_html(tmdb['overview'])}</blockquote>\n"
        if tmdb.get("rating"):
            caption += f"⭐ <b>{tmdb['rating']}/10</b>  •  "
        caption += f"📦 {total} files  •  ⚡ {data.get('speed', '')}\n"
        caption += f"🗑 Auto-deletes in {_del_mins} mins\n\n"
    else:
        caption += (
            f"<blockquote>📦 {total} files found  •  ⚡ {data.get('speed', '')}\n"
            f"🗑 Auto-deletes in {_del_mins} mins</blockquote>\n\n"
        )

    caption += "👇 Tap a file to receive it in your PM:"

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
        nav.append(InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="ignore"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"page#{session_id}#{page+1}"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton("🏠 Home", callback_data="start_home")])
    markup = InlineKeyboardMarkup(buttons)

    # Page 0 with TMDB poster → send as photo replacing the 🔍 placeholder
    if page == 0 and tmdb and tmdb.get("poster"):
        try:
            chat_id = message.chat.id
            await message.delete()
            await client.send_photo(
                chat_id=chat_id,
                photo=tmdb["poster"],
                caption=caption,
                reply_markup=markup,
                parse_mode=ParseMode.HTML
            )
            return
        except Exception:
            pass  # fall through to text if photo fails

    try:
        await message.edit_text(text=caption, reply_markup=markup, parse_mode=ParseMode.HTML)
    except MessageNotModified:
        pass
    except Exception as e:
        logger.error(f"show_results error: {e}")


route_menu = show_results


@Client.on_message(
    filters.text & filters.private &
    ~filters.command(["start", "help", "about", "admin", "broadcast", "ban", "unban", "purge_cams", "reset_db", "update"])
)
async def auto_filter(client: Client, message: Message, manual_query=None):
    user_id = message.from_user.id

    if await db.is_banned(user_id):
        return await message.reply_text("🚫 **You are banned from using this bot.**", quote=True)

    config = await db.get_config()
    if config.get("maintenance_mode") and user_id != _ADMIN_ID:
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
        USER_SEARCH_COOLDOWN.clear()
    USER_SEARCH_COOLDOWN[user_id] = current_time

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
        sug_buttons = []
        for sug in suggestions:
            safe_sug = re.sub(r"[^a-zA-Z0-9]", "_", sug)[:40]
            sug_buttons.append([InlineKeyboardButton(
                f"🔎 {sug[:30]}",
                url=f"https://t.me/{client.me.username}?start=search_{safe_sug}"
            )])

        sug_buttons += [
            [InlineKeyboardButton("🔍 Search on Google", url=google_url)],
            [InlineKeyboardButton("📝 Request This Movie", callback_data=f"reqmovie#{safe_query}")],
            [InlineKeyboardButton("🏠 Home", callback_data="start_home")]
        ]

        hint = "\n💡 <b>Did you mean one of these?</b>" if suggestions else ""
        return await message.reply_text(
            f"🔍 <b>No results for</b> <code>{query}</code>\n\n"
            f"<blockquote>Not in our database yet.\n"
            f"Check spelling or tap Request below.{hint}</blockquote>",
            reply_markup=InlineKeyboardMarkup(sug_buttons),
            parse_mode=ParseMode.HTML, **_no_preview()
        )

    time_taken = time.time() - start_time
    await db.clear_old_searches()
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
        "auto_delete_time": int(config.get("auto_delete_time", 300))
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

    results     = data["results"]
    tmdb        = data.get("tmdb")
    per_page    = 8
    total       = len(results)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page        = max(0, min(page, total_pages - 1))
    start_idx   = page * per_page
    page_files  = results[start_idx: start_idx + per_page]

    _del_secs = int(data.get("auto_delete_time", 300))
    _del_mins = max(1, _del_secs // 60)

    title_display = _html(tmdb["title"] if tmdb else data["query"].title())
    caption = f"🎬 <b>{title_display}</b>\n"

    if tmdb:
        if tmdb.get("overview"):
            caption += f"<blockquote>{_html(tmdb['overview'])}</blockquote>\n"
        if tmdb.get("rating"):
            caption += f"⭐ <b>{tmdb['rating']}/10</b>  •  "
        caption += f"📦 {total} files  •  ⚡ {data.get('speed', '')}\n"
        caption += f"🗑 Auto-deletes in {_del_mins} mins\n\n"
    else:
        caption += (
            f"<blockquote>📦 {total} files  •  ⚡ {data.get('speed', '')}\n"
            f"🗑 Auto-deletes in {_del_mins} mins</blockquote>\n\n"
        )
    caption += "👇 Tap a file to receive it in your PM:"

    buttons = []
    for f in page_files:
        f_lang, f_qual = extract_attributes(f["file_name"])
        size_str = _fmt_size(f)
        name     = re.sub(r'\s+', ' ', f["file_name"]).strip()
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
        nav.append(InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="ignore"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"page#{session_id}#{page+1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton("🏠 Home", callback_data="start_home")])
    markup = InlineKeyboardMarkup(buttons)

    msg = callback.message
    try:
        if getattr(msg, "photo", None) or getattr(msg, "document", None):
            await msg.edit_caption(caption=caption, reply_markup=markup, parse_mode=ParseMode.HTML)
        else:
            await msg.edit_text(text=caption, reply_markup=markup, parse_mode=ParseMode.HTML)
    except MessageNotModified:
        pass
    except Exception as e:
        logger.error(f"Pagination error: {e}")

    await callback.answer()


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
                f"{_html(file