import os
import re
import time
import asyncio
from collections import OrderedDict
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import InputUserDeactivated, UserIsBlocked
from database.db import db

# Per-user request cooldown — same bounded-LRU pattern and naming style as
# filter.py's USER_SEARCH_COOLDOWN/_COOLDOWN_MAX/COOLDOWN_TIME, applied to
# /request and the "Request This Movie" button so both entry points share
# one spam guard (enforced once, inside send_request_ticket() below,
# instead of duplicated in each handler).
_COOLDOWN_MAX = 10000
USER_REQUEST_COOLDOWN = OrderedDict()  # LRU: oldest entry is first
COOLDOWN_TIME = 30  # seconds — long enough to stop rapid-fire spam, short
                    # enough that requesting a few different titles in one
                    # sitting is never blocked


async def _delayed_delete(msg, delay=2):
    """Background auto-delete for the transient cooldown warning — fire-
    and-forget via create_task (see call site), matching every other
    auto-delete in this codebase, instead of an inline blocking sleep that
    would hold the handler open for 2 extra seconds doing nothing useful."""
    await asyncio.sleep(delay)
    try:
        await msg.delete()
    except Exception:
        pass

# ── /request command ──────────────────────────────────────────────────────────
@Client.on_message(filters.command("request") & filters.private)
async def request_cmd(client: Client, message: Message):
    if len(message.command) < 2:
        return await message.reply_text("**Usage:** `/request [Movie Name]`\nExample: `/request Oppenheimer`")

    movie_name = message.text.split(" ", 1)[1][:40]
    await send_request_ticket(client, message.from_user, movie_name, message)

# ── "Request This Movie" button (from filter.py's no-results screen) ──────────
@Client.on_callback_query(filters.regex(r"^reqmovie#"))
async def handle_movie_request(client: Client, callback: CallbackQuery):
    movie_name = callback.data.split("#", 1)[1]
    await callback.answer("📤 Sending request...", show_alert=False)
    await send_request_ticket(client, callback.from_user, movie_name, callback.message, is_callback=True)

# ── Ticket generator ────────────────────────────────────────────────────────────
async def send_request_ticket(client, user, movie_name, message_obj, is_callback=False):
    current_time = time.time()
    if user.id in USER_REQUEST_COOLDOWN:
        passed = current_time - USER_REQUEST_COOLDOWN[user.id]
        USER_REQUEST_COOLDOWN.move_to_end(user.id)
        if passed < COOLDOWN_TIME:
            wait_msg = await message_obj.reply_text(
                f"⏳ Wait `{int(COOLDOWN_TIME - passed) + 1}s` before submitting another request.",
                quote=not is_callback
            )
            asyncio.create_task(_delayed_delete(wait_msg))
            return

    if len(USER_REQUEST_COOLDOWN) >= _COOLDOWN_MAX:
        USER_REQUEST_COOLDOWN.popitem(last=False)  # evict least-recently-used
    USER_REQUEST_COOLDOWN[user.id] = current_time
    USER_REQUEST_COOLDOWN.move_to_end(user.id)

    config = await db.get_config()
    log_channel = config.get("log_channel", 0)
    
    if not log_channel:
        return await message_obj.reply_text("Request system is currently offline — log channel not set.")

    ticket_text = (
        f"🎫 **New Movie Request**\n\n"
        f"User: {user.mention} (`{user.id}`)\n"
        f"Movie: `{movie_name}`"
    )

    # The button the admin will click when they upload it
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Mark Uploaded & Notify User", callback_data=f"reqdone#{user.id}#{movie_name}")]
    ])

    try:
        await client.send_message(chat_id=log_channel, text=ticket_text, reply_markup=markup)
        await db.save_pending_request(user.id, movie_name)
        success_msg = f"**Request sent**\n\nYour request for **{movie_name}** has been sent to the admins — we'll notify you here once it's uploaded."

        user_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 Back to Home", callback_data="start_home")]
        ])

        if is_callback:
            await message_obj.edit_text(success_msg, reply_markup=user_markup)
        else:
            await message_obj.reply_text(success_msg, reply_markup=user_markup)
    except Exception as e:
        await message_obj.reply_text(f"Failed to send request: {e}")

# ── Admin taps "Mark Uploaded & Notify User" ───────────────────────────────────
@Client.on_callback_query(filters.regex(r"^reqdone#"))
async def mark_request_done(client: Client, callback: CallbackQuery):
    try:
        parts = callback.data.split("#", 2)
        if len(parts) < 3:
            return await callback.answer("❌ Malformed data.", show_alert=True)
        _, user_id, movie_name = parts
    except Exception:
        return await callback.answer("❌ Malformed data.", show_alert=True)

    # _fulfill_matching_requests() (realtime_indexer.py) may have already
    # auto-notified this user and deleted the ticket between when it was
    # created and when an admin got around to tapping this button — check
    # first so we never send the same "your movie is ready" message twice.
    if not await db.pending_request_exists(int(user_id), movie_name):
        await callback.answer("Already fulfilled automatically — no duplicate sent.", show_alert=True)
        resolved_text = callback.message.text + (
            f"\n\nℹ️ **Already auto-fulfilled** — acknowledged by: {callback.from_user.mention}"
        )
        await callback.message.edit_text(resolved_text)
        return

    await callback.answer("Notifying user...", show_alert=False)

    # Notify the User
    try:
        # Deep-link payload can't contain spaces — swap them for underscores
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', movie_name)

        notify_text = (
            f"🎉 **Good news**\n\n"
            f"The movie you requested — **{movie_name}** — has just been uploaded.\n\n"
            f"Tap below to fetch it instantly."
        )
        
        # The button now triggers the search_ payload we built in start.py!
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Fetch Movie Now", url=f"https://t.me/{client.me.username}?start=search_{safe_name}")]
        ])
        
        await client.send_message(chat_id=int(user_id), text=notify_text, reply_markup=markup)
        # Clear the ticket now that the user's been notified — otherwise it
        # lingers in pending_requests and a later matching upload could
        # trigger _fulfill_matching_requests() to notify them a second time.
        await db.delete_pending_request(int(user_id), movie_name)
    except (InputUserDeactivated, UserIsBlocked):
        await db.delete_user(int(user_id))
        await db.delete_pending_request(int(user_id), movie_name)
        resolved_text = callback.message.text + f"\n\n⚠️ **User blocked bot — removed from database by:** {callback.from_user.mention}"
        await callback.message.edit_text(resolved_text)
        return await callback.answer("⚠️ User has blocked the bot — removed from database.", show_alert=True)
    except Exception as e:
        return await callback.answer(f"❌ Could not PM user (they might have blocked the bot). Error: {e}", show_alert=True)

    # Update the Admin Ticket so you know it's done
    resolved_text = callback.message.text + f"\n\n✅ **Completed by:** {callback.from_user.mention}"
    await callback.message.edit_text(resolved_text)