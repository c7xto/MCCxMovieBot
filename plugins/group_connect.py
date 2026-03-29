import re
import time
import random
import string
import asyncio
import logging
from urllib.parse import quote
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import MessageNotModified, FloodWait
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
from plugins.filter import (
    send_smart_log, extract_attributes, _html, _fmt_size,
    _sort_results, clean_query, LANG_EMOJI
)
from tmdb import get_movie_data

logger = logging.getLogger(__name__)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _build_group_buttons(page_files, client_username, session_id, page,
                          total, total_pages):
    """
    Build the same file-button rows as filter.py, but each button is a URL
    that opens the bot DM and sends the file (via ?start=file_<id>).
    """
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

        bot_url = f"https://t.me/{client_username}?start=file_{f['_id']}"
        buttons.append([InlineKeyboardButton(btn_text, url=bot_url)])

    # Navigation row — pagination goes back to DM full search for group
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(
            "⬅️ Prev",
            callback_data=f"grppage#{session_id}#{page - 1}"
        ))
    if total_pages > 1:
        nav.append(InlineKeyboardButton(
            f"📄 {page + 1}/{total_pages}", callback_data="ignore"
        ))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(
            "Next ➡️",
            callback_data=f"grppage#{session_id}#{page + 1}"
        ))
    if nav:
        buttons.append(nav)

    return buttons


def _build_caption(query, tmdb, total, speed, del_mins):
    """Identical caption style as filter.py show_results."""
    title_display = _html(tmdb["title"] if tmdb else query.title())
    caption = f"🎬 <b>{title_display}</b>\n"

    if tmdb:
        if tmdb.get("overview"):
            caption += f"<blockquote>{_html(tmdb['overview'])}</blockquote>\n"
        if tmdb.get("rating"):
            caption += f"⭐ <b>{tmdb['rating']}/10</b>  •  "
        caption += f"📦 {total} files  •  ⚡ {speed}\n"
        caption += f"🗑 Auto-deletes in {del_mins} mins\n\n"
    else:
        caption += (
            f"<blockquote>📦 {total} files found  •  ⚡ {speed}\n"
            f"🗑 Auto-deletes in {del_mins} mins</blockquote>\n\n"
        )

    caption += "👇 Tap a file to receive it in your PM:"
    return caption


# ─── Bot added to group ───────────────────────────────────────────────────────

@Client.on_message(filters.group & filters.new_chat_members)
async def auto_connect_group(client: Client, message: Message):
    bot_id = client.me.id
    for member in message.new_chat_members:
        if member.id != bot_id:
            continue

        if await db.is_group_banned(message.chat.id):
            try:
                await client.leave_chat(message.chat.id)
            except Exception:
                pass
            return

        await db.add_group(message.chat.id, message.chat.title)

        try:
            total_members = await client.get_chat_members_count(message.chat.id)
            added_by = message.from_user.mention if message.from_user else "Unknown"
            asyncio.create_task(send_smart_log(client,
                f"🏘 **#NewGroup**\n\n📌 {message.chat.title}\n"
                f"🆔 `{message.chat.id}`\n👥 `{total_members}` members\n"
                f"👤 Added by: {added_by}"
            ))
        except Exception as e:
            logger.error(f"New group log failed: {e}")

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🤖 Search Movies", url=f"https://t.me/{client.me.username}")]
        ])
        await message.reply_text(
            f"🎬 <b>MCCxBot connected to {message.chat.title}!</b>\n\n"
            f"Type any movie or series name to search.",
            reply_markup=keyboard, quote=True, parse_mode=ParseMode.HTML
        )
        break


# ─── Group text search ────────────────────────────────────────────────────────

@Client.on_message(filters.group & filters.text & ~filters.command(["start", "help", "connect"]))
async def group_search(client: Client, message: Message):
    if not message.from_user:
        return

    if await db.is_banned(message.from_user.id):
        return

    if await db.is_group_banned(message.chat.id):
        try:
            await client.leave_chat(message.chat.id)
        except Exception:
            pass
        return

    config = await db.get_config()
    if config.get("maintenance_mode"):
        return

    raw_query = message.text.strip()
    query     = clean_query(raw_query)

    chat_words = {"hi", "hello", "hey", "bro", "thanks", "thank you",
                  "pls", "admin", "help", "ok", "okay", "good", "morning", "night"}
    if raw_query.lower() in chat_words or len(raw_query) < 3:
        try:
            await message.delete()
            warning = await message.reply_text(
                f"⚠️ {message.from_user.mention}, <b>No Chatting Allowed.</b>\n"
                f"<blockquote>Type a Movie or Series name only.</blockquote>",
                parse_mode=ParseMode.HTML
            )
            await asyncio.sleep(5)
            await warning.delete()
        except Exception:
            pass
        return

    if not query:
        return

    start_time = time.time()
    results    = await db.get_search_results(query)
    asyncio.create_task(db.increment_group_search(message.chat.id))

    # ── No results ────────────────────────────────────────────────────────────
    if not results:
        should_alert = await db.log_missed_search(query)
        if should_alert:
            asyncio.create_task(send_smart_log(client,
                f"❌ **#MissedSearch**\n\n🎬 `{query}`\n"
                f"👤 {message.from_user.mention}\n📍 Group Chat"
            ))

        safe_query = re.sub(r'[^a-zA-Z0-9]', '_', query)[:40]
        google_url = f"https://www.google.com/search?q={quote(query)}"
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("📝 Request This Movie",
             url=f"https://t.me/{client.me.username}?start=req_{safe_query}")],
            [InlineKeyboardButton("🔍 Search on Google", url=google_url)]
        ])
        not_found_msg = await message.reply_text(
            f"🔍 <b>No results for</b> <code>{query}</code>\n\n"
            f"<blockquote>Not in our database yet.\n"
            f"Check spelling or tap Request below.</blockquote>",
            reply_markup=markup, quote=True, parse_mode=ParseMode.HTML,
            **_no_preview()
        )
        await asyncio.sleep(15)
        try:
            await not_found_msg.delete()
        except Exception:
            pass
        return

    # ── Results found ─────────────────────────────────────────────────────────
    time_taken = time.time() - start_time
    await db.clear_old_searches()

    # Fetch TMDB data (same logic as filter.py)
    tmdb_data = None
    try:
        best_name  = results[0]["file_name"]
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

    session_id   = "".join(random.choices(string.ascii_letters + string.digits, k=6))
    sorted_files = _sort_results(results)

    _del_secs = int(config.get("auto_delete_time", 300))
    _del_mins = max(1, _del_secs // 60)
    speed     = f"{time_taken:.3f}s"

    session_data = {
        "results":          sorted_files,
        "query":            query,
        "tmdb":             tmdb_data,
        "speed":            speed,
        "time":             time.time(),
        "auto_delete_time": _del_secs,
        "is_group":         True,
        "group_chat_id":    message.chat.id,
    }
    await db.save_search(session_id, session_data)

    # Build page 0
    per_page    = 8
    total       = len(sorted_files)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page_files  = sorted_files[:per_page]

    caption = _build_caption(query, tmdb_data, total, speed, _del_mins)
    buttons = _build_group_buttons(
        page_files, client.me.username, session_id, 0, total, total_pages
    )
    buttons.append([InlineKeyboardButton(
        "🤖 Open Bot", url=f"https://t.me/{client.me.username}"
    )])
    markup = InlineKeyboardMarkup(buttons)

    # Page 0 with TMDB poster → send as photo (same as filter.py)
    status_msg = await message.reply_text("🔍", quote=True)
    if tmdb_data and tmdb_data.get("poster"):
        try:
            await status_msg.delete()
            result_msg = await message.reply_photo(
                photo=tmdb_data["poster"],
                caption=caption,
                reply_markup=markup,
                parse_mode=ParseMode.HTML
            )
        except Exception:
            result_msg = await message.reply_text(
                text=caption,
                reply_markup=markup,
                parse_mode=ParseMode.HTML
            )
            try:
                await status_msg.delete()
            except Exception:
                pass
    else:
        try:
            await status_msg.edit_text(
                text=caption, reply_markup=markup, parse_mode=ParseMode.HTML
            )
            result_msg = status_msg
        except Exception:
            result_msg = status_msg

    # Auto-delete the result message after auto_delete_time
    async def _auto_delete(msg):
        await asyncio.sleep(_del_secs)
        try:
            await msg.delete()
        except Exception:
            pass

    asyncio.create_task(_auto_delete(result_msg))


# ─── Group pagination callback ────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^grppage#"))
async def handle_group_pagination(client: Client, callback: CallbackQuery):
    parts      = callback.data.split("#")
    session_id = parts[1]
    page       = int(parts[2])

    data = await db.get_search(session_id)
    if not data:
        await callback.answer("⚠️ Session expired. Search again.", show_alert=True)
        return

    results     = data["results"]
    tmdb        = data.get("tmdb")
    query       = data["query"]
    per_page    = 8
    total       = len(results)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page        = max(0, min(page, total_pages - 1))
    start_idx   = page * per_page
    page_files  = results[start_idx: start_idx + per_page]

    _del_secs = int(data.get("auto_delete_time", 300))
    _del_mins = max(1, _del_secs // 60)

    caption = _build_caption(query, tmdb, total, data.get("speed", ""), _del_mins)
    buttons = _build_group_buttons(
        page_files, client.me.username, session_id, page, total, total_pages
    )
    buttons.append([InlineKeyboardButton(
        "🤖 Open Bot", url=f"https://t.me/{client.me.username}"
    )])
    markup = InlineKeyboardMarkup(buttons)

    msg = callback.message
    try:
        if getattr(msg, "photo", None):
            await msg.edit_caption(caption=caption, reply_markup=markup, parse_mode=ParseMode.HTML)
        else:
            await msg.edit_text(text=caption, reply_markup=markup, parse_mode=ParseMode.HTML)
    except MessageNotModified:
        pass
    except Exception as e:
        logger.error(f"Group pagination error: {e}")

    await callback.answer()