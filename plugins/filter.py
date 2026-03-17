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
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, LinkPreviewOptions
)
from database.db import db
from utils import is_subscribed, is_subscribed_join_only, send_fsub_message
from tmdb import get_movie_data

load_dotenv()

logger = logging.getLogger(__name__)

# MISSED_CACHE removed — db.log_missed_search() now handles deduplication
# with a per-query 1-hour cooldown stored in MongoDB.
MISSED_CACHE = set()  # kept as empty set — group_connect.py imports it

IGNORE_WORDS = {"hi", "hello", "bro", "pls", "plz", "bot", "help", "admin", "sir"}

# FIX #16: USER_SEARCH_COOLDOWN capped to prevent unbounded RAM growth.
_COOLDOWN_MAX = 10000
USER_SEARCH_COOLDOWN = {}
COOLDOWN_TIME = 2

# Replaces deprecated disable_web_page_preview throughout this file
_NO_PREVIEW = LinkPreviewOptions(is_disabled=True)


async def send_smart_log(client, text):
    try:
        config = await db.get_config()
        log_chat = config.get("log_channel", 0)
        if log_chat:
            # FIX deprecated: replaced disable_web_page_preview with link_preview_options
            await client.send_message(
                log_chat, text,
                link_preview_options=_NO_PREVIEW
            )
    except FloodWait:
        pass
    except Exception:
        pass


LANGUAGES = ["Malayalam", "Tamil", "Telugu", "Hindi", "English", "Kannada", "Dual Audio", "Multi Audio"]
QUALITIES = ["4K", "1080p", "720p", "480p", "HDRip", "WEB-DL", "WEBRip", "BluRay", "PreDVD", "CAM", "HD Rip"]

LANG_EMOJI = {
    "Malayalam": "🌴", "Tamil": "🎭", "Telugu": "⭐",
    "Hindi": "🇮🇳", "English": "🌍", "Kannada": "🏵",
    "Dual Audio": "🎧", "Multi Audio": "🎵", "Other": "🌐"
}


def clean_query(query):
    stop_words = [
        r'\bplease\b', r'\bsend\b', r'\bme\b', r'\bthe\b', r'\bmovie\b',
        r'\bseries\b', r'\bhd\b', r'\bprint\b', r'\bdownload\b', r'\blink\b',
        r'\bbro\b', r'\bcan\b', r'\byou\b', r'\bprovide\b', r'\bi\b',
        r'\bneed\b', r'\bwant\b'
    ]
    query_clean = query.lower()
    for word in stop_words:
        query_clean = re.sub(word, '', query_clean, flags=re.IGNORECASE)
    return re.sub(r'\s+', ' ', query_clean).strip()


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


# FIX #3 & #4: Background tasks for auto-delete — no longer block the event loop.
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
    """
    Auto-deletes a sent file after delete_seconds.
    Shows a 1-minute warning before deletion using the actual configured time.
    """
    # Sleep for (total - 60) seconds, then warn, then delete after 60 more
    warn_sleep = max(delete_seconds - 60, 30)
    await asyncio.sleep(warn_sleep)
    try:
        await sent_msg.edit_caption(
            caption=(
                f"🍿 <b>{file_name}</b>\n\n"
                f"<blockquote>⚠️ Deleting in <b>1 minute</b> — save it now!\n"
                f"Forward to your <b>Saved Messages</b> to keep it.</blockquote>\n\n"
                f"📢 More movies → @{bot_username}"
            ),
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass
    await asyncio.sleep(60)
    try:
        await sent_msg.delete()
    except Exception:
        pass


# ── PRIVATE SEARCH HANDLER ────────────────────────────────────────────────────

@Client.on_message(
    filters.text & filters.private &
    ~filters.command(["start", "help", "about", "admin", "broadcast", "ban", "unban", "purge_cams", "reset_db"])
)
async def auto_filter(client: Client, message: Message, manual_query=None):
    user_id = message.from_user.id

    # FIX #20: Ban enforcement — was completely missing before
    if await db.is_banned(user_id):
        return await message.reply_text("🚫 **You are banned from using this bot.**", quote=True)

    # C1: Maintenance mode — admin exempt
    _cm_config = await db.get_config()
    if _cm_config.get("maintenance_mode") and user_id != int(os.getenv("ADMIN_ID", 0)):
        return await message.reply_text(
            _cm_config.get("maintenance_message", "🔧 Bot is under maintenance. Back soon!"),
            quote=True
        )

    # Gate 1: join channels only — user must join public/private channels to search
    if not await is_subscribed_join_only(client, message):
        await send_fsub_message(client, message)
        return

    # Anti-Spam Rate Limiter
    current_time = time.time()
    if user_id in USER_SEARCH_COOLDOWN:
        time_passed = current_time - USER_SEARCH_COOLDOWN[user_id]
        if time_passed < COOLDOWN_TIME:
            warning = await message.reply_text(
                f"⏳ **Anti-Spam:** Please wait `{int(COOLDOWN_TIME - time_passed) + 1}s` before searching again.",
                quote=True
            )
            await asyncio.sleep(2)
            try:
                await warning.delete()
            except Exception:
                pass
            return

    # FIX #16: Cap dict to prevent unbounded RAM growth
    if len(USER_SEARCH_COOLDOWN) >= _COOLDOWN_MAX:
        USER_SEARCH_COOLDOWN.clear()
    USER_SEARCH_COOLDOWN[user_id] = current_time

    start_time = time.time()

    if manual_query:
        query = manual_query
    else:
        original_text = message.text
        query = clean_query(original_text)
        clean_content = original_text.lower().strip()

        if len(clean_content) < 3 or clean_content in IGNORE_WORDS:
            config = await db.get_config()
            main_group = config.get("main_group", "")
            await message.reply_text(
                f"<b>🙋 ʜᴇʏ {message.from_user.first_name} 😍 ,\n\n"
                f"𝒀𝒐𝒖 𝒄𝒂𝒏 𝒔𝒆𝒂𝒓𝒄ʜ 𝒇𝒐𝒓 𝒎𝒐𝒗𝒊𝒆𝒔 𝒐𝒏𝒍𝒚 𝒐𝒏 𝒐𝒖𝒓 𝑴𝒐𝒗𝒊𝒆 𝑮𝒓𝒐𝒖𝒑.</b>",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("📝 ʀᴇǫᴜᴇsᴛ ʜᴇʀᴇ", url=main_group)]]
                ) if main_group else None,
                quote=True,
                parse_mode=ParseMode.HTML
            )
            return

    # S4: Extract quality/language preset from raw query
    raw_query_text = manual_query or (message.text if message.text else "")
    preset_qual = None
    preset_lang = None
    quality_map = {
        "4k": "4K", "2160p": "4K", "1080p": "1080p", "720p": "720p",
        "480p": "480p", "360p": "360p", "hdrip": "HDRip", "hd": "HD"
    }
    for kw, qual_val in quality_map.items():
        if kw in raw_query_text.lower():
            preset_qual = qual_val
            query = re.sub(re.escape(kw), "", query, flags=re.IGNORECASE).strip()
            break
    for lang in LANGUAGES:
        if lang.lower() in raw_query_text.lower():
            preset_lang = lang
            query = re.sub(re.escape(lang), "", query, flags=re.IGNORECASE).strip()
            break
    if not query:
        return

    results = await db.get_search_results(query)

    if not results:
        google_url = f"https://www.google.com/search?q={quote(query)}"
        safe_query = query[:40]

        # F9: Log to MongoDB — returns True if log channel alert should be sent
        # (deduplicates alerts with 1-hour cooldown, replaces old MISSED_CACHE RAM set)
        should_alert = await db.log_missed_search(query)
        if should_alert:
            asyncio.create_task(send_smart_log(
                client,
                f"❌ **#MissedSearch**\n\n🎬 **Movie:** `{query}`\n"
                f"👤 **Requested by:** {message.from_user.mention}\n📍 **Where:** Private Chat\n"
            ))

        # S10: Spell correction — suggest similar titles via prefix match
        suggestions = await db.get_prefix_suggestions(query, limit=3)
        suggestion_buttons = []
        for sug in suggestions:
            safe_sug = re.sub(r"[^a-zA-Z0-9]", "_", sug)[:40]
            suggestion_buttons.append([InlineKeyboardButton(
                f"🔎 {sug[:30]}",
                url=f"https://t.me/{client.me.username}?start=search_{safe_sug}"
            )])

        suggestion_buttons += [
            [InlineKeyboardButton("🔍 Check Spelling on Google", url=google_url)],
            [InlineKeyboardButton("📝 Request This Movie", callback_data=f"reqmovie#{safe_query}")],
            [InlineKeyboardButton("🏠 Home", callback_data="start_home")]
        ]

        spell_hint = "\n💡 <b>Did you mean?</b> Tap a suggestion below." if suggestions else ""
        text = (
            f"🔍 <b>No results for</b> <code>{query}</code>\n\n"
            f"<blockquote>This title isn't in our database yet.\n"
            f"Check the spelling or tap Request below.{spell_hint}</blockquote>"
        )
        return await message.reply_text(
            text, reply_markup=InlineKeyboardMarkup(suggestion_buttons),
            link_preview_options=_NO_PREVIEW,
            parse_mode=ParseMode.HTML
        )

    time_taken = time.time() - start_time
    await db.clear_old_searches()
    session_id = ''.join(random.choices(string.ascii_letters + string.digits, k=6))

    session_data = {
        "results": results,
        "query": query,
        "speed": f"{time_taken:.3f}s",
        "time": time.time(),
        "preset_lang": preset_lang,
        "preset_qual": preset_qual
    }
    await db.save_search(session_id, session_data)

    status_msg = await message.reply_text("🔍", quote=True)
    # S4: pass presets. If only quality is preset (no lang), use MIXED for lang
    # so route_menu skips the language step and goes straight to quality-filtered results.
    # If only lang is preset, pass ALL for qual so quality step shows for that lang.
    # If both are preset, pass both and go straight to file list.
    lang_arg = preset_lang if preset_lang else ("MIXED" if preset_qual else "ALL")
    qual_arg = preset_qual if preset_qual else "ALL"
    await route_menu(client, status_msg, session_id, lang_arg, qual_arg, 0)

    # FIX #4: Fire-and-forget instead of blocking handler for 5 minutes
    asyncio.create_task(_auto_delete_search(status_msg, message, manual_query))


# ── SEARCH RESULT MENU ────────────────────────────────────────────────────────

async def route_menu(client, message, session_id, lang, qual, page):
    data = await db.get_search(session_id)
    if not data:
        return await message.edit_text("⚠️ **Search Session Expired.** Please search for the movie again.")

    all_files = data["results"]
    tmdb = data.get("tmdb")

    filtered_files = []
    for f in all_files:
        f_lang, f_qual = extract_attributes(f["file_name"])
        if lang not in ["ALL", "MIXED"] and f_lang != lang:
            continue
        if qual not in ["ALL", "MIXED"] and f_qual != qual:
            continue
        filtered_files.append((f, f_lang, f_qual))

    available_langs = set(f[1] for f in filtered_files)
    available_quals = set(f[2] for f in filtered_files)

    title_display = tmdb['title'] if tmdb else data['query']
    title_display = (
        title_display.replace("&", "&amp;")
                     .replace("<", "&lt;")
                     .replace(">", "&gt;")
    )

    caption = (
        f"🎬 <b>{title_display.upper()}</b>\n"
        f"<blockquote>📦 {len(filtered_files)} files found  •  ⚡ {data['speed']}\n"
        f"🗑 Auto-deletes in 5 mins</blockquote>\n\n"
    )

    buttons = []

    if lang == "ALL" and len(available_langs) > 1:
        caption += "🌐 <b>Step 1 of 2</b> — Choose your language:"
        lang_counts = {}
        for f, f_lang, _ in filtered_files:
            lang_counts[f_lang] = lang_counts.get(f_lang, 0) + 1

        row = []
        for l in sorted(available_langs):
            count = lang_counts.get(l, 0)
            l_emoji = LANG_EMOJI.get(l, "🔊")
            row.append(InlineKeyboardButton(
                f"{l_emoji} {l} ({count})",
                callback_data=f"lang#{session_id}#{l}"
            ))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([InlineKeyboardButton("🌍 Show All Languages", callback_data=f"lang#{session_id}#MIXED")])
        buttons.append([InlineKeyboardButton("🏠 Home", callback_data="start_home")])

    elif qual == "ALL" and len(available_quals) > 1:
        display_lang = lang if lang not in ["ALL", "MIXED"] else "All Languages"
        caption += f"🎞 <b>Step 2 of 2</b> — Choose quality for {display_lang}:"
        qual_counts = {}
        for f, _, f_qual in filtered_files:
            qual_counts[f_qual] = qual_counts.get(f_qual, 0) + 1

        row = []
        for q in sorted(available_quals):
            count = qual_counts.get(q, 0)
            row.append(InlineKeyboardButton(
                f"{q} ({count})",
                callback_data=f"qual#{session_id}#{lang}#{q}"
            ))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([InlineKeyboardButton("🎞 Show All Qualities", callback_data=f"qual#{session_id}#{lang}#MIXED")])
        buttons.append([
            InlineKeyboardButton("◀️ Back", callback_data=f"lang#{session_id}#ALL"),
            InlineKeyboardButton("🏠 Home", callback_data="start_home")
        ])

    else:
        lang_tag = f"  {LANG_EMOJI.get(lang, '🔊')} {lang}" if lang not in ["ALL", "MIXED"] else ""
        qual_tag = f"  {qual}" if qual not in ["ALL", "MIXED"] else ""
        caption += f"📂 <b>{len(filtered_files)} files</b>{lang_tag}{qual_tag}\n👇 Tap a file to receive it in your PM:"

        per_page = 10
        total_pages = (len(filtered_files) + per_page - 1) // per_page
        start_idx = page * per_page
        end_idx = start_idx + per_page
        page_files = filtered_files[start_idx:end_idx]

        for f, f_lang, f_qual in page_files:
            size_mb = f.get('file_size', 0) / (1024 * 1024)
            size_str = f"{size_mb / 1024:.2f} GB" if size_mb >= 1024 else f"{size_mb:.0f} MB"

            display_name = f['file_name']
            if lang not in ["ALL", "MIXED"]:
                display_name = re.sub(rf'\b{lang}\b', '', display_name, flags=re.IGNORECASE)
            if qual not in ["ALL", "MIXED"]:
                display_name = re.sub(rf'\b{qual}\b', '', display_name, flags=re.IGNORECASE)
            display_name = re.sub(r'\s+', ' ', display_name).strip(" -_")

            btn_text = f"[{size_str}] {display_name[:28]}"
            buttons.append([InlineKeyboardButton(btn_text, callback_data=f"sendfile#{f['_id']}")])

        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(
                "⬅️ Prev", callback_data=f"page#{session_id}#{lang}#{qual}#{page-1}"
            ))
        if total_pages > 1:
            nav_buttons.append(InlineKeyboardButton(
                f"📄 {page+1}/{total_pages}", callback_data="ignore"
            ))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton(
                "Next ➡️", callback_data=f"page#{session_id}#{lang}#{qual}#{page+1}"
            ))

        if nav_buttons:
            buttons.append(nav_buttons)
        buttons.append([
            InlineKeyboardButton("◀️ Back", callback_data=f"lang#{session_id}#ALL"),
            InlineKeyboardButton("🏠 Home", callback_data="start_home")
        ])

    markup = InlineKeyboardMarkup(buttons)
    try:
        await message.edit_text(text=caption, reply_markup=markup, parse_mode=ParseMode.HTML)
    except MessageNotModified:
        pass
    except Exception as e:
        logger.error(f"UI Route Error: {e}")


# ── CALLBACK HANDLERS ─────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^lang#"))
async def handle_language(client: Client, callback: CallbackQuery):
    _, session_id, lang = callback.data.split("#")
    await route_menu(client, callback.message, session_id, lang, "ALL", 0)
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^qual#"))
async def handle_quality(client: Client, callback: CallbackQuery):
    _, session_id, lang, qual = callback.data.split("#")
    await route_menu(client, callback.message, session_id, lang, qual, 0)
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^page#"))
async def handle_pagination(client: Client, callback: CallbackQuery):
    _, session_id, lang, qual, page = callback.data.split("#")
    await route_menu(client, callback.message, session_id, lang, qual, int(page))
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^ignore$"))
async def handle_ignore(client: Client, callback: CallbackQuery):
    await callback.answer()


def _build_caption(config, file_data, delete_minutes, bot_username):
    """C6: Builds file caption from template or falls back to default."""
    template = config.get("file_caption_template", "")
    if template:
        f_lang, f_qual = extract_attributes(file_data.get("file_name", ""))
        size_mb = file_data.get("file_size", 0) / (1024 * 1024)
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
            pass  # Bad template — fall through to default
    return (
        f"🍿 <b>{file_data['file_name']}</b>\n\n"
        f"<blockquote>⏳ Auto-deletes in <b>{delete_minutes} minutes</b>\n"
        f"⚠️ Forward to your <b>Saved Messages</b> to keep it!</blockquote>\n\n"
        f"📢 More movies → @{bot_username}"
    )


@Client.on_callback_query(filters.regex(r"^sendfile#"))
async def send_movie_file(client: Client, callback: CallbackQuery):
    _, file_obj_id = callback.data.split("#")
    file_data = await db.get_file(file_obj_id)

    if not file_data:
        return await callback.answer(
            "⚠️ This file is no longer available in the database.", show_alert=True
        )

    # Gate 1: join channels — catches users coming from group buttons who bypassed PM search
    if not await is_subscribed_join_only(client, callback):
        await callback.answer("🔐 Please join our channel first!", show_alert=False)
        await send_fsub_message(client, callback.message, pending_file_id=file_obj_id)
        return


    await callback.answer("📤 Sending file... Please wait!", show_alert=False)

    config = await db.get_config()
    delete_seconds = int(config.get("auto_delete_time", 300))
    delete_minutes = delete_seconds // 60

    try:
        sent_message = await client.send_cached_media(
            chat_id=callback.message.chat.id,
            file_id=file_data["file_id"],
            caption=_build_caption(config, file_data, delete_minutes, client.me.username),
            parse_mode=ParseMode.HTML
        )
        asyncio.create_task(_auto_delete_file(sent_message, file_data['file_name'], client.me.username, delete_seconds))
    except Exception as e:
        err_str = str(e).lower()
        if any(k in err_str for k in ["file_reference", "invalid", "not found", "media"]):
            await db.delete_file_by_id(file_data["file_id"])
            await callback.message.reply_text(
                f"❌ **File Unavailable**\n\n"
                f"`{file_data['file_name']}` has expired or been deleted from Telegram's servers.\n"
                f"It has been removed from our database. Please search again for an updated copy.",
                parse_mode=ParseMode.HTML
            )
        else:
            await callback.message.reply_text(
                "❌ **Could not send file.** Please try again in a moment."
            )
        logger.error(f"send_cached_media failed for {file_data['file_id']}: {e}")


@Client.on_callback_query(filters.regex(r"^check_fsub#"))
async def check_fsub_callback(client: Client, callback: CallbackQuery):
    """
    Handles FSub verification. callback_data format: check_fsub#<file_id_or_none>
    If a file_id is present, the file is sent automatically after successful verification
    so the user doesn't have to tap the button again.
    """
    file_part = callback.data.split("#")[1]
    pending_file_id = file_part if file_part != "none" else None

    join_ok = await is_subscribed_join_only(client, callback)
    if not join_ok:
        await callback.answer(
            "❌ You haven't joined all channels yet! Please join and try again.",
            show_alert=True
        )
        return

    # All gates passed — delete the FSub prompt
    await callback.message.delete()

    if pending_file_id:
        # User was trying to get a file — send it now automatically
        file_data = await db.get_file(pending_file_id)
        if not file_data:
            await callback.message.reply_text(
                "✅ **Verified!** But the file is no longer available. Please search again."
            )
            return

        config = await db.get_config()
        delete_seconds = int(config.get("auto_delete_time", 300))
        delete_minutes = delete_seconds // 60

        try:
            sent_msg = await client.send_cached_media(
                chat_id=callback.message.chat.id,
                file_id=file_data["file_id"],
                caption=(
                    f"🍿 <b>{file_data['file_name']}</b>\n\n"
                    f"<blockquote>⏳ Auto-deletes in <b>{delete_minutes} minutes</b>\n"
                    f"⚠️ Forward to your <b>Saved Messages</b> to keep it!</blockquote>\n\n"
                    f"📢 More movies → @{client.me.username}"
                ),
                parse_mode=ParseMode.HTML
            )
            asyncio.create_task(_auto_delete_file(sent_msg, file_data['file_name'], client.me.username, delete_seconds))
        except Exception as e:
            err_str = str(e).lower()
            if any(k in err_str for k in ["file_reference", "invalid", "not found", "media"]):
                await db.delete_file_by_id(file_data["file_id"])
                await callback.message.reply_text(
                    "❌ **File Unavailable** — it has expired. Please search again.",
                    parse_mode=ParseMode.HTML
                )
            else:
                await callback.message.reply_text("❌ Could not send file. Please try again.")
    else:
        # No pending file — just confirm they can now search
        await callback.message.reply_text(
            "✅ **Verification Successful!**\n\n"
            "<blockquote>You're all set! Type any movie or series name to search.</blockquote>",
            parse_mode=ParseMode.HTML
        )


# ── ISSUE #15 FIX ─────────────────────────────────────────────────────────────
# Removed group_filter_handler from this file entirely.
# group_connect.py already handles all group message logic (chat word filtering,
# search, not-found, request button). Having it here too caused double MongoDB
# queries on every single group message. group_connect.py is the single source of truth.
# ─────────────────────────────────────────────────────────────────────────────


# FIX #9: Duplicate reqmovie# handler also removed.
# Canonical handler with full ticket system lives in request.py.
