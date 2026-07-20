import os
import re
import asyncio
import urllib.parse
import time
import random
import string
from dotenv import load_dotenv
from plugins.filter import route_menu
from utils import is_subscribed_join_only, send_fsub_message
from tmdb import get_movie_data
from pyrogram import Client, filters
from pyrogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
try:
    from pyrogram.types import LinkPreviewOptions
except ImportError:
    LinkPreviewOptions = None

def _no_preview():
    if LinkPreviewOptions is not None:
        return {"link_preview_options": LinkPreviewOptions(is_disabled=True)}
    return {"disable_web_page_preview": True}
from pyrogram.types import Message
from pyrogram.enums import ParseMode
from database.db import db

load_dotenv()

# ── Brand kit ─────────────────────────────────────────────────────────────
# Shared icon set for this file — keep in sync with plugins/filter.py's
# LANG_EMOJI and the rest of the bot's emoji usage. Centralizing this into a
# dedicated branding module is a reasonable follow-up once more files need
# it; scoped to start.py for now since this phase only touches this file.
ICON_SEARCH   = "🔍"
ICON_MOVIE    = "🎬"
ICON_TRENDING = "🔥"
ICON_UPDATES  = "📢"
ICON_SUCCESS  = "✅"
ICON_FAIL     = "❌"
ICON_REQUEST  = "📝"


def _html(text: str) -> str:
    """Escapes a string for safe use inside Telegram HTML-mode messages."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

# All config (log channel, media, links) is read from MongoDB inside each
# handler via db.get_config() — no module-level env reads needed here.


def _build_start_ui(config, mention, total_files, bot_username, update_link, group_link,
                     is_new=False, trending=None):
    """Shared welcome UI builder — used by /start and start_home callback."""
    default_welcome = (
        "<b>👋 Hey {mention}!</b>\n\n"
        "🎬 <b>{total_files:,}</b> files ready to deliver.\n"
        "Malayalam • Tamil • Telugu • Hindi &amp; more\n\n"
        "Just type any movie name to search.\n"
        "<i>Files land in your PM instantly.</i>"
    )
    raw = config.get("welcome_text", default_welcome)
    try:
        text = raw.format(mention=mention, total_files=total_files)
    except Exception:
        text = raw

    # Clean onboarding: first-time users get a short "how this works" block
    # appended after the (possibly admin-customized) welcome text; returning
    # users just see the normal welcome — no repeated hand-holding.
    if is_new:
        text += (
            "\n\n<blockquote>🆕 <b>New here? It's simple:</b>\n"
            "1️⃣ Type any movie or series name\n"
            "2️⃣ Tap the file you want\n"
            "3️⃣ It lands in this chat instantly</blockquote>"
        )

    text += f"\n\n👮‍♂️ @{bot_username}"

    buttons = []

    if trending:
        # Two per row so a full set of 6 only takes 3 rows instead of 6.
        trend_buttons = [
            InlineKeyboardButton(f"{ICON_TRENDING} {t[:20]}", callback_data=f"trend#{t[:40]}")
            for t in trending
        ]
        for i in range(0, len(trend_buttons), 2):
            buttons.append(trend_buttons[i:i + 2])

    top_row = []
    if update_link:
        top_row.append(InlineKeyboardButton(f"{ICON_UPDATES} Updates", url=update_link))
    if group_link:
        top_row.append(InlineKeyboardButton("👥 Group", url=group_link))
    if top_row:
        buttons.append(top_row)
    buttons.append([InlineKeyboardButton("➕ Add to Group", url=f"https://t.me/{bot_username}?startgroup=true")])
    buttons.append([InlineKeyboardButton("ℹ️ Help", callback_data="help_menu")])
    return text, InlineKeyboardMarkup(buttons)


async def _execute_search(client, status_msg, query: str, config: dict, user_id=None):
    """Runs a search and renders page 0 of results into status_msg.

    Shared by the /start deep-link search path and the new "🔥 trending"
    quick-search buttons on the home panel, so both stay in sync with one
    implementation instead of drifting apart.
    """
    results = await db.get_search_results(query)
    tmdb_data = None

    if results:
        best_filename = results[0]["file_name"]
        clean_tmdb_query = re.sub(
            r'(1080p|720p|480p|4K|HDRip|WEB-DL|WEBRip|BluRay|PreDVD|CAM|HD Rip|'
            r'x264|x265|HEVC|Dual Audio|Multi Audio|'
            r'Malayalam|Tamil|Telugu|Hindi|English|Kannada)',
            '', best_filename, flags=re.IGNORECASE
        )
        clean_tmdb_query = re.sub(r'[\(\[].*?[\)\]]', '', clean_tmdb_query)
        clean_tmdb_query = re.sub(r'[^a-zA-Z0-9\s]', ' ', clean_tmdb_query).strip()
        if len(clean_tmdb_query) > 2:
            tmdb_data = await get_movie_data(clean_tmdb_query)
        if not tmdb_data:
            tmdb_data = await get_movie_data(query)

    if not results:
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{ICON_REQUEST} Request This Movie", callback_data=f"reqmovie#{query[:40]}")]
        ])
        return await status_msg.edit_text(
            f"😔 <b>Sorry!</b> I couldn't find any files for <code>{_html(query)}</code> right now.\n\n"
            f"The admin might still be uploading it, or there was a typo in the name!",
            reply_markup=markup, parse_mode=ParseMode.HTML
        )

    # Only searches that actually returned something are worth surfacing as
    # a "trending" suggestion — see _TrendingCache's docstring in db.py.
    await db.log_trending_search(query)

    await db.clear_old_searches()
    session_id = ''.join(random.choices(string.ascii_letters + string.digits, k=6))
    session_data = {
        "results":          results,
        "tmdb":             tmdb_data,
        "query":            query,
        "speed":            "0.001s",
        "time":             time.time(),
        "auto_delete_time": int(config.get("auto_delete_time", 300)),
        "user_id":          user_id,
        "sort_mode":        "smart",
    }
    await db.save_search(session_id, session_data)
    return await route_menu(client, status_msg, session_id, 0)


async def _handle_file_link(client, message, file_obj_id: str):
    """Deep-link payload: file_<obj_id> — direct file delivery, gated by
    the main FSub check."""
    file_data = await db.get_file(file_obj_id)
    if not file_data:
        return await message.reply_text(
            f"{ICON_FAIL} <b>Sorry!</b> This file was deleted or is no longer available.",
            parse_mode=ParseMode.HTML
        )

    if not await is_subscribed_join_only(client, message):
        await send_fsub_message(client, message, pending_file_id=file_obj_id)
        return

    config = await db.get_config()
    delete_seconds = int(config.get("auto_delete_time", 300))
    delete_minutes = delete_seconds // 60

    from plugins.filter import _auto_delete_file
    sent = await client.send_cached_media(
        chat_id=message.chat.id,
        file_id=file_data["file_id"],
        caption=(
            f"{ICON_MOVIE} <b>{_html(file_data['file_name'])}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"⏳ Deletes in <b>{delete_minutes} mins</b>  •  Forward to keep\n"
            f"{ICON_UPDATES} @{client.me.username}"
        ),
        parse_mode=ParseMode.HTML
    )
    asyncio.create_task(_auto_delete_file(sent, file_data['file_name'], client.me.username, delete_seconds))


async def _handle_request_link(message, raw_query: str):
    """Deep-link payload: req_<query> — pre-fills a request confirmation."""
    movie_name = urllib.parse.unquote(raw_query).replace("_", " ")
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{ICON_SUCCESS} Confirm Request", callback_data=f"reqmovie#{movie_name[:40]}")]
    ])
    return await message.reply_text(
        f"{ICON_REQUEST} <b>Movie Request Ticket</b>\n\n"
        f"You are requesting: <code>{_html(movie_name)}</code>\n\n"
        f"Tap below to send this directly to the admins!",
        reply_markup=markup, parse_mode=ParseMode.HTML, quote=True
    )


async def _handle_search_payload(client, message, config, payload: str):
    """Deep-link payload: search_<query>, or a bare payload treated as a
    search term (legacy / QR-code links)."""
    if payload.startswith("search_"):
        raw_query = payload.split("search_", 1)[1]
    else:
        raw_query = payload
    query = urllib.parse.unquote(raw_query).replace("_", " ")
    status_msg = await message.reply_text(
        f"{ICON_SEARCH} <b>Searching databases...</b>", parse_mode=ParseMode.HTML, quote=True
    )
    return await _execute_search(client, status_msg, query, config, user_id=message.from_user.id)


@Client.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    # 1. Fetch live config from Database
    config = await db.get_config()
    START_MEDIA = config.get("start_media", "https://files.catbox.moe/wvdeci.mp4")
    UPDATE_CHANNEL_LINK = config.get("update_channel", "")
    MAIN_GROUP_LINK = config.get("main_group", "")
    LOG_CHANNEL_ID = config.get("log_channel", 0)

    # Live file count across all clusters
    total_files = await db.get_total_files()

    # 2. Silently log the user to the database
    is_new = await db.save_user(message.from_user.id, message.from_user.first_name)

    if is_new and LOG_CHANNEL_ID:
        try:
            users = await db.get_all_users()
            await client.send_message(
                LOG_CHANNEL_ID,
                f"🆕 **New User Alert**\n\n"
                f"👤 **User:** {message.from_user.mention}\n"
                f"🆔 **ID:** `{message.from_user.id}`\n"
                f"📊 **Total Users:** `{len(users)}`"
            )
        except Exception:
            pass

    # Catch Group Search Deep-Links
    if len(message.command) > 1 and message.command[1].startswith("search_"):
        query = message.command[1].replace("search_", "").replace("_", " ")
        from plugins.filter import auto_filter
        return await auto_filter(client, message, manual_query=query)

    # 3. Deep-link dispatch — one clearly separated handler per payload type.
    if len(message.command) > 1:
        payload = message.command[1]
        if payload.startswith("file_"):
            return await _handle_file_link(client, message, payload.split("file_", 1)[1])
        elif payload.startswith("req_"):
            return await _handle_request_link(message, payload.split("req_", 1)[1])
        else:
            return await _handle_search_payload(client, message, config, payload)

    # No payload — render the home panel, with trending searches attached.
    trending = await db.get_trending_searches(limit=6)
    caption_text, reply_markup = _build_start_ui(
        config, message.from_user.mention, total_files, client.me.username,
        UPDATE_CHANNEL_LINK, MAIN_GROUP_LINK, is_new=is_new, trending=trending
    )

    try:
        media_lower = START_MEDIA.lower()
        if media_lower.endswith((".mp4", ".mkv", ".mov")):
            await message.reply_video(video=START_MEDIA, caption=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, quote=True)
        elif media_lower.endswith((".gif")):
            await message.reply_animation(animation=START_MEDIA, caption=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, quote=True)
        else:
            await message.reply_photo(photo=START_MEDIA, caption=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, quote=True)
    except Exception:
        await message.reply_text(text=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, quote=True, **_no_preview())


@Client.on_callback_query(filters.regex(r"^help_menu$"))
async def help_menu_callback(client: Client, callback: CallbackQuery):
    help_text = (
        "<blockquote>"
        "1. Type a movie or series name\n"
        "2. Select your language\n"
        "3. Pick your preferred quality\n"
        "4. Tap the file — it's sent to your PM"
        "</blockquote>\n\n"
        "<i>Can't find it? Use the Request button and we'll upload it within 24h.</i>"
    )
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("◀️ Back", callback_data="start_home")]
    ])

    try:
        if getattr(callback.message, "video", None) or getattr(callback.message, "photo", None) or getattr(callback.message, "animation", None):
            await callback.message.edit_caption(caption=help_text, reply_markup=markup, parse_mode=ParseMode.HTML)
        else:
            await callback.message.edit_text(text=help_text, reply_markup=markup, parse_mode=ParseMode.HTML)
    except Exception:
        pass
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^start_home$"))
async def start_home_callback(client: Client, callback: CallbackQuery):
    config = await db.get_config()
    UPDATE_CHANNEL_LINK = config.get("update_channel", "")
    MAIN_GROUP_LINK = config.get("main_group", "")

    total_files = await db.get_total_files()
    trending = await db.get_trending_searches(limit=6)

    caption_text, reply_markup = _build_start_ui(
        config, callback.from_user.mention, total_files, client.me.username,
        UPDATE_CHANNEL_LINK, MAIN_GROUP_LINK, is_new=False, trending=trending
    )

    try:
        if getattr(callback.message, "video", None) or getattr(callback.message, "photo", None) or getattr(callback.message, "animation", None):
            await callback.message.edit_caption(caption=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        else:
            await callback.message.edit_text(text=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception:
        pass
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^trend#"))
async def trend_search_callback(client: Client, callback: CallbackQuery):
    """Tap-to-search on a "🔥 Trending" button from the home panel."""
    query = callback.data.split("#", 1)[1] if "#" in callback.data else ""
    if not query:
        return await callback.answer()

    await callback.answer(f"{ICON_SEARCH} Searching...")
    config = await db.get_config()

    # Always send a fresh message rather than editing the home panel in
    # place — the home panel is very often a video/photo message
    # (start_media), and route_menu()/show_results() expect a plain
    # text-editable status message, the same contract every other search
    # entry point in this file already relies on.
    status_msg = await callback.message.reply_text(
        f"{ICON_SEARCH} <b>Searching for</b> <code>{_html(query)}</code>...",
        parse_mode=ParseMode.HTML
    )
    await _execute_search(client, status_msg, query, config, user_id=callback.from_user.id)
