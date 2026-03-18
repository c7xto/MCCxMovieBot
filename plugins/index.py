import os
import re
import time
import asyncio
import logging
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram import ContinuePropagation, StopPropagation
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from database.db import db

load_dotenv()

# Import our unified smart logger!
from plugins.filter import send_smart_log

logger = logging.getLogger(__name__)
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

# --- GLOBAL STATE DICTIONARY ---

def get_progress_bar(percentage, length=12):
    filled = int((percentage / 100) * length)
    return '🟩' * filled + '⬜️' * (length - filled)

def get_readable_time(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{int(h)}h {int(m)}m {int(s)}s"
    elif m > 0: return f"{int(m)}m {int(s)}s"
    return f"{int(s)}s"

# --- THE BACKGROUND WORKER ---
async def run_indexer(client: Client, status_message: Message, chat_id: int, last_msg_id: int, start_id: int):
    await db.set_index_task(chat_id, "running")
    current_id = start_id
    batch_size = 50 # Safe size to prevent API limits
    
    saved_files, duplicates, deleted_empty = 0, 0, 0
    start_time = time.time()
    loops = 0

    while current_id <= last_msg_id:
        # 1. Check the Control State
        state = await db.get_index_task(chat_id)
        if state == "stopped":
            await status_message.edit_text("⏹ **Indexing Stopped manually by Admin.**")
            elapsed = get_readable_time(time.time() - start_time)
            asyncio.create_task(send_smart_log(client,
                f"⏹ **#IndexStopped**\n\n"
                f"📦 **Channel:** `{chat_id}`\n"
                f"✨ **Saved:** `{saved_files}`\n"
                f"⚠️ **Duplicates:** `{duplicates}`\n"
                f"⏱ **Ran for:** `{elapsed}`\n"
                f"_(Stopped manually by admin)_"
            ))
            break
        if state == "paused":
            await asyncio.sleep(2)
            continue # Loop silently until resumed

        end_id = min(current_id + batch_size - 1, last_msg_id)
        message_ids = list(range(current_id, end_id + 1))
        batch_files = []

        try:
            messages = await client.get_messages(chat_id, message_ids)
            
            if not isinstance(messages, list):
                messages = [messages]

            for msg in messages:
                if getattr(msg, "empty", False) or not msg:
                    deleted_empty += 1
                    continue

                media = msg.document or msg.video
                if media and hasattr(media, "file_id"):
                    raw_name = getattr(media, "file_name", "")
                    if not raw_name: continue

                    # 🧹 Regex Name Purifier
                    clean_name = re.sub(r'http\S+|www\.\S+|@\w+|\[Join.*?\]', '', raw_name, flags=re.IGNORECASE)
                    clean_name = clean_name.replace('_', ' ').replace('.', ' ')
                    clean_name = re.sub(r'\s+', ' ', clean_name).strip()

                    batch_files.append({
                        "file_id": media.file_id,
                        "file_name": clean_name,
                        "file_size": getattr(media, "file_size", 0),
                        "mime_type": getattr(media, "mime_type", "")
                    })

        except FloodWait as e:
            await asyncio.sleep(e.value)
            continue
            
        except Exception as e:
            # --- 1. UPDATE THE USER IN THE CHAT ---
            try:
                await status_message.edit_text(
                    f"❌ **Indexing Failed!**\n\n"
                    f"**Reason:** Telegram blocked access or a fatal error occurred.\n"
                    f"**Fix:** Please ensure the bot is an **Admin** in the Database Channel with 'Read Messages' permission."
                )
            except Exception:
                pass

            # --- FEATURE 5: SYSTEM ERRORS LOG (Optimized) ---
            logger.error(f"❌ Indexer Blocked! Error: {e}")
            
            error_log = (
                f"⚠️ **#SystemError** (Indexer)\n\n"
                f"📦 **Channel:** `{chat_id}`\n"
                f"🛑 **Error:** `{e}`\n\n"
                f"*(Please check bot Admin permissions in this channel)*"
            )
            asyncio.create_task(send_smart_log(client, error_log))
            
            # We still send a direct PM to the Admin just in case they miss the log!
            if ADMIN_ID:
                try:
                    await client.send_message(
                        ADMIN_ID, 
                        f"❌ **Indexer Crashed!**\n\nI cannot read the Database Channel (`{chat_id}`). Please make sure I am an **Administrator** with permission to read message history!\n\n**Error Details:** `{e}`"
                    )
                except Exception:
                    pass
                
            # Clear the task state so it isn't stuck "running" internally
            await db.clear_index_task(chat_id)
                
            return    

        # 2. Bulk Execute & Save Progress
        if batch_files:
            new_saves, dups = await db.save_files_bulk(batch_files)
            saved_files += new_saves
            duplicates += dups

        await db.set_index_progress(chat_id, end_id)

        # 3. Throttle UI Updates
        current_id += batch_size
        loops += 1

        if loops % 5 == 0 or current_id > last_msg_id:
            percentage = min((current_id / last_msg_id) * 100, 100)
            elapsed_time = time.time() - start_time
            msgs_per_sec = (current_id - start_id) / elapsed_time if elapsed_time > 0 else 1
            eta_seconds = (last_msg_id - current_id) / msgs_per_sec if msgs_per_sec > 0 else 0

            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("⏸ Pause", callback_data=f"pause_idx#{chat_id}"),
                 InlineKeyboardButton("⏹ Stop", callback_data=f"stop_idx#{chat_id}")]
            ])

            text = (
                f"⚡ **MCCxBot Super-Indexer [RUNNING]**\n"
                f"{get_progress_bar(percentage)} `{percentage:.1f}%`\n\n"
                f"📈 **Progress:** `{current_id} / {last_msg_id}`\n"
                f"⏱ **Elapsed:** `{get_readable_time(elapsed_time)}` | ⏳ **ETA:** `{get_readable_time(eta_seconds)}`\n"
                f"🚀 **Speed:** `{int(msgs_per_sec)} msgs/sec`\n\n"
                f"✅ **Cleaned & Saved:** `{saved_files}` | ⚠️ **Dups:** `{duplicates}`"
            )
            try:
                await status_message.edit_text(text, reply_markup=markup)
            except MessageNotModified:
                pass
            except FloodWait as e:
                await asyncio.sleep(e.value)

        # 🛡️ THE STEALTH BRAKE: 3-second mandatory pause between API calls
        await asyncio.sleep(3)

    # 4. Finish Execution
    if await db.get_index_task(chat_id) == "running":
        total_time = time.time() - start_time
        await db.clear_index_task(chat_id)
        await status_message.edit_text(
            f"🎉 **SUPER-INDEX COMPLETE!**\n\n"
            f"⏱ **Total Time:** `{get_readable_time(total_time)}`\n"
            f"✨ Saved: `{saved_files}`\n"
            f"⚠️ Duplicates: `{duplicates}`\n"
            f"🗑 Junk Ignored: `{deleted_empty}`"
        )
        # Smart index notification — sends full summary to log channel
        total_in_db = await db.get_total_files()
        asyncio.create_task(send_smart_log(client,
            f"📦 **#IndexComplete**\n\n"
            f"📺 **Channel:** `{chat_id}`\n"
            f"✨ **Files Saved:** `{saved_files}`\n"
            f"⚠️ **Duplicates Skipped:** `{duplicates}`\n"
            f"🗑 **Junk Ignored:** `{deleted_empty}`\n"
            f"⏱ **Time Taken:** `{get_readable_time(total_time)}`\n"
            f"📁 **Total Files in DB Now:** `{total_in_db:,}`"
        ))


# --- UI HANDLERS & COMMANDS ---
@Client.on_message(filters.private & filters.forwarded & filters.user(ADMIN_ID))
async def forward_indexer(client: Client, message: Message):
    chat_id = None
    last_msg_id = None
    
    # Try all known Pyrogram attribute paths for forwarded channel messages.
    # Pyrogram 2.0+ uses forward_origin (MessageOriginChannel).
    # Older versions use forward_from_chat + forward_from_message_id.
    try:
        # Path 1: Pyrogram 2.0+ forward_origin
        if hasattr(message, "forward_origin") and message.forward_origin:
            origin = message.forward_origin
            # MessageOriginChannel has .chat and .message_id
            if hasattr(origin, "chat") and origin.chat:
                chat_id = origin.chat.id
            # Also try .sender_chat for some Pyrogram builds
            elif hasattr(origin, "sender_chat") and origin.sender_chat:
                chat_id = origin.sender_chat.id
            last_msg_id = getattr(origin, "message_id", None)

        # Path 2: Older Pyrogram — forward_from_chat
        if not chat_id and hasattr(message, "forward_from_chat") and message.forward_from_chat:
            chat_id = message.forward_from_chat.id
            last_msg_id = getattr(message, "forward_from_message_id", None)

        # Path 3: Fallback — message.id is the forwarded message ID
        if chat_id and not last_msg_id:
            last_msg_id = message.id
    except Exception as detect_err:
        logger.warning(f"Forward detection error: {detect_err}")

    if not chat_id or not last_msg_id:
        return await message.reply_text("⚠️ Could not detect the channel ID. Please forward directly from a channel.", quote=True)

    # --- PRE-FLIGHT CHECK ---
    try:
        # Ask Telegram if we have access to this channel BEFORE showing the menu
        await client.get_chat(chat_id)
    except Exception as e:
        return await message.reply_text(
            f"❌ **Indexing Blocked!**\n\n"
            f"I cannot read messages from `{chat_id}`.\n"
            f"**Error:** `{e}`\n\n"
            f"⚠️ **Fix:** Make sure you add me as an **Administrator** to the channel first!",
            quote=True
        )
    # ------------------------

    saved_progress = await db.get_index_progress(chat_id)
    resume_text = f"🔄 **Resuming from message {saved_progress}**\n" if saved_progress > 0 else ""

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Start Super-Index", callback_data=f"bulkindex#{chat_id}#{last_msg_id}")],
        [InlineKeyboardButton("❌ Cancel", callback_data="close_data")]
    ])

    await message.reply_text(
        f"⚡ **MCCxBot Super-Indexer Ready**\n\n"
        f"📦 **Target Channel ID:** `{chat_id}`\n"
        f"🎯 **Total Messages:** `{last_msg_id}`\n"
        f"{resume_text}\n"
        f"Ready to run in Stealth Mode?",
        reply_markup=markup,
        quote=True
    )
    raise StopPropagation  # prevent forwarded message leaking into auto_filter

@Client.on_callback_query(filters.regex(r"^bulkindex#") & filters.user(ADMIN_ID))
async def start_bulk_index(client: Client, callback: CallbackQuery):
    try:
        _, chat_id_str, last_msg_id_str = callback.data.split("#")
        chat_id = int(chat_id_str)
        last_msg_id = int(last_msg_id_str)
    except (ValueError, IndexError):
        return await callback.answer("❌ Malformed callback data.", show_alert=True)

    # Fetch state from MongoDB instead of the deleted RAM dictionary
    state = await db.get_index_task(chat_id)
    if state == "running":
        return await callback.answer("⚠️ Indexer is already running for this channel!", show_alert=True)

    start_id = await db.get_index_progress(chat_id)
    if start_id == 0: start_id = 1

    status_msg = await callback.message.edit_text("⏳ **Spinning up background worker...**")
    
    # --- FEATURE 4: INDEXING REQUESTS LOG ---
    log_text = (
        f"🚀 **#IndexRequest**\n\n"
        f"👤 **Admin:** {callback.from_user.mention}\n"
        f"📦 **Target Channel:** `{chat_id}`\n"
        f"🎯 **Total Messages:** `{last_msg_id}`\n"
        f"🔄 **Starting From:** `{start_id}`"
    )
    asyncio.create_task(send_smart_log(client, log_text))
    # ----------------------------------------
    
    # Offload the massive task to the background
    asyncio.create_task(run_indexer(client, status_msg, chat_id, last_msg_id, start_id))
    await callback.answer()

# --- CONTROL BUTTON CALLBACKS ---
@Client.on_callback_query(filters.regex(r"^pause_idx#") & filters.user(ADMIN_ID))
async def pause_index(client: Client, callback: CallbackQuery):
    try:
        chat_id = int(callback.data.split("#")[1])
    except (ValueError, IndexError):
        return await callback.answer("❌ Malformed callback.", show_alert=True)
    await db.set_index_task(chat_id, "paused")
    
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Resume", callback_data=f"resume_idx#{chat_id}"),
         InlineKeyboardButton("⏹ Stop", callback_data=f"stop_idx#{chat_id}")]
    ])
    await callback.message.edit_reply_markup(reply_markup=markup)
    await callback.answer("Indexer Paused ⏸")

@Client.on_callback_query(filters.regex(r"^resume_idx#") & filters.user(ADMIN_ID))
async def resume_index(client: Client, callback: CallbackQuery):
    try:
        chat_id = int(callback.data.split("#")[1])
    except (ValueError, IndexError):
        return await callback.answer("❌ Malformed callback.", show_alert=True)
    await db.set_index_task(chat_id, "running")
    
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏸ Pause", callback_data=f"pause_idx#{chat_id}"),
         InlineKeyboardButton("⏹ Stop", callback_data=f"stop_idx#{chat_id}")]
    ])
    await callback.message.edit_reply_markup(reply_markup=markup)
    await callback.answer("Indexer Resumed ▶️")

@Client.on_callback_query(filters.regex(r"^stop_idx#") & filters.user(ADMIN_ID))
async def stop_index(client: Client, callback: CallbackQuery):
    try:
        chat_id = int(callback.data.split("#")[1])
    except (ValueError, IndexError):
        return await callback.answer("❌ Malformed callback.", show_alert=True)
    await db.set_index_task(chat_id, "stopped")
    await callback.answer("Stopping Indexer... ⏹", show_alert=True)

# FIX #8: close_data handler removed from this file.
# The canonical handler lives in admin.py — having it here too caused
# Pyrogram to register it twice, leading to unpredictable double-fire behavior.