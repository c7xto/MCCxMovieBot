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


def _html(text: str) -> str:
    """Escapes a string for safe use inside Telegram HTML-mode messages."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

# All config (log channel, media, links) is read from MongoDB inside each
# handler via db.get_config() — no module-level env reads needed here.



def _build_start_ui(config, mention, total_files, bot_username, update_link, group_link):
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
    text += f"\n\n👮‍♂️ @{bot_username}"

    buttons = []
    top_row = []
    if update_link:
        top_row.append(InlineKeyboardButton("📢 Updates", url=update_link))
    if group_link:
        top_row.append(InlineKeyboardButton("👥 Group", url=group_link))
    if top_row:
        buttons.append(top_row)
    buttons.append([InlineKeyboardButton("➕ Add to Group", url=f"https://t.me/{bot_username}?startgroup=true")])
    buttons.append([InlineKeyboardButton("ℹ️ Help", callback_data="help_menu")])
    return text, InlineKeyboardMarkup(buttons)


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
        except Exception as e:
            pass
    
    # Catch Group Search Deep-Links
    if len(message.command) > 1 and message.command[1].startswith("search_"):
        query = message.command[1].replace("search_", "").replace("_", " ")
        from plugins.filter import auto_filter
        return await auto_filter(client, message, manual_query=query)
    
    # 3. Check for Deep Links
    if len(message.command) > 1:
        payload = message.command[1]
        
        if payload.startswith("file_"):
            file_obj_id = payload.split("file_")[1]
            file_data = await db.get_file(file_obj_id)

            if not file_data:
                return await message.reply_text("❌ **Sorry!** This file was deleted or is no longer available.")

            # Gate 1: join channels — must be joined to receive any file
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
                    f"🎬 <b>{_html(file_data['file_name'])}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━\n"
                    f"⏳ Deletes in <b>{delete_minutes} mins</b>  •  Forward to keep\n"
                    f"📢 @{client.me.username}"
                ),
                parse_mode=ParseMode.HTML
            )
            asyncio.create_task(_auto_delete_file(sent, file_data['file_name'], client.me.username, delete_seconds))
            return
                
        elif payload.startswith("req_"):
            raw_query = payload.split("req_", 1)[1]
            movie_name = urllib.parse.unquote(raw_query).replace("_", " ")
            
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Confirm Request", callback_data=f"reqmovie#{movie_name[:40]}")]
            ])
            return await message.reply_text(
                f"📝 **Movie Request Ticket**\n\n"
                f"You are requesting: `{movie_name}`\n\n"
                f"Click the button below to send this directly to the admins!",
                reply_markup=markup,
                quote=True
            )

        else:
            if payload.startswith("search_"):
                raw_query = payload.split("search_", 1)[1]
            else:
                raw_query = payload 
                
            query = urllib.parse.unquote(raw_query).replace("_", " ")
            status_msg = await message.reply_text("🔍 **Searching databases...**", quote=True)
            
            results = await db.get_search_results(query)
            tmdb_data = None
            search_term = query

            if results:
                best_filename = results[0]['file_name']
                clean_tmdb_query = re.sub(r'(1080p|720p|480p|4K|HDRip|WEB-DL|WEBRip|BluRay|PreDVD|CAM|HD Rip|x264|x265|HEVC|Dual Audio|Multi Audio|Malayalam|Tamil|Telugu|Hindi|English|Kannada)', '', best_filename, flags=re.IGNORECASE)
                clean_tmdb_query = re.sub(r'[\(\[].*?[\)\]]', '', clean_tmdb_query)
                clean_tmdb_query = re.sub(r'[^a-zA-Z0-9\s]', ' ', clean_tmdb_query).strip()
                
                if len(clean_tmdb_query) > 2:
                     tmdb_data = await get_movie_data(clean_tmdb_query)
                if not tmdb_data:
                     tmdb_data = await get_movie_data(query)

            if not results:
                markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📝 Request This Movie", callback_data=f"reqmovie#{query[:40]}")]
                ])
                return await status_msg.edit_text(
                    f"😔 **Sorry!** I couldn't find any files for `{query}` right now.\n\nThe admin might still be uploading it, or there was a typo in the name!",
                    reply_markup=markup
                )

            await db.clear_old_searches()
            session_id = ''.join(random.choices(string.ascii_letters + string.digits, k=6))
            session_data = {
                "results": results,
                "tmdb": tmdb_data,
                "query": search_term,
                "speed": "0.001s",
                "time": time.time(),
                "auto_delete_time": int(config.get("auto_delete_time", 300))
            }
            await db.save_search(session_id, session_data)
            return await route_menu(client, status_msg, session_id, "ALL", "ALL", 0)

    caption_text, reply_markup = _build_start_ui(
        config, message.from_user.mention, total_files, client.me.username,
        UPDATE_CHANNEL_LINK, MAIN_GROUP_LINK
    )

    try:
        media_lower = START_MEDIA.lower()
        if media_lower.endswith((".mp4", ".mkv", ".mov")):
            await message.reply_video(video=START_MEDIA, caption=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, quote=True)
        elif media_lower.endswith((".gif")):
            await message.reply_animation(animation=START_MEDIA, caption=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, quote=True)
        else:
            await message.reply_photo(photo=START_MEDIA, caption=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, quote=True)
    except Exception as e:
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

    caption_text, reply_markup = _build_start_ui(
        config, callback.from_user.mention, total_files, client.me.username,
        UPDATE_CHANNEL_LINK, MAIN_GROUP_LINK
    )
    
    try:
        if getattr(callback.message, "video", None) or getattr(callback.message, "photo", None) or getattr(callback.message, "animation", None):
            await callback.message.edit_caption(caption=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        else:
            await callback.message.edit_text(text=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception:
        pass
    await callback.answer()