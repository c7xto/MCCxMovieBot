import os
import re
import time
import logging
from collections import OrderedDict
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import InputUserDeactivated, UserIsBlocked
from pyrogram.enums import ParseMode
from database.db import db
from plugins.access_policy import authorize_user_action
from plugins.callbacks import answer_callback_safely
from plugins.telegram_retry import BACKGROUND_RETRY, telegram_call
from utils import (
    ADMIN_ID,
    _html,
    callback_data,
    html_user_mention,
    public_error_message,
    report_internal_error,
)


logger = logging.getLogger(__name__)

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
    """Queue transient-message cleanup without retaining a sleeping task."""
    await db.schedule_deletion(msg.chat.id, msg.id, delay)


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
    await answer_callback_safely(callback, "📤 Sending request...", show_alert=False)
    await send_request_ticket(client, callback.from_user, movie_name, callback.message, is_callback=True)


# ── Ticket generator ────────────────────────────────────────────────────────────
async def send_request_ticket(client, user, movie_name, message_obj, is_callback=False):
    access = await authorize_user_action(user.id, "request")
    if not access.allowed:
        return await message_obj.reply_text(access.message or "Action denied.")
    # Store the same bounded title carried by the admin callback. Telegram's
    # 64-byte callback limit is easy to exceed with multibyte titles.
    request_prefix = f"reqdone#{user.id}#"
    movie_name = callback_data(request_prefix, movie_name)[len(request_prefix) :].strip()
    if not movie_name:
        return await message_obj.reply_text("Please include a valid movie title.")

    current_time = time.time()
    if user.id in USER_REQUEST_COOLDOWN:
        passed = current_time - USER_REQUEST_COOLDOWN[user.id]
        USER_REQUEST_COOLDOWN.move_to_end(user.id)
        if passed < COOLDOWN_TIME:
            wait_msg = await message_obj.reply_text(
                f"⏳ Wait `{int(COOLDOWN_TIME - passed) + 1}s` before submitting another request.",
                reply_parameters=None,
            )
            await _delayed_delete(wait_msg)
            return

    if len(USER_REQUEST_COOLDOWN) >= _COOLDOWN_MAX:
        USER_REQUEST_COOLDOWN.popitem(last=False)  # evict least-recently-used
    USER_REQUEST_COOLDOWN[user.id] = current_time
    USER_REQUEST_COOLDOWN.move_to_end(user.id)

    config = access.config
    log_channel = config.get("log_channel", 0)

    if not log_channel:
        return await message_obj.reply_text("Request system is currently offline — log channel not set.")

    ticket_text = (
        "🎫 <b>New Movie Request</b>\n\n"
        f"User: {html_user_mention(user)} (<code>{user.id}</code>)\n"
        f"Movie: <code>{_html(movie_name)}</code>"
    )

    # The button the admin will click when they upload it
    markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Mark Uploaded & Notify User",
                    callback_data=callback_data(request_prefix, movie_name),
                )
            ]
        ]
    )

    try:
        access = await authorize_user_action(user.id, "request")
        if not access.allowed:
            return await message_obj.reply_text(access.message or "Action denied.")
        log_channel = access.config.get("log_channel", 0)
        if not log_channel:
            return await message_obj.reply_text("Request system is currently offline — log channel not set.")
        await telegram_call(
            lambda: client.send_message(
                chat_id=log_channel,
                text=ticket_text,
                reply_markup=markup,
                parse_mode=ParseMode.HTML,
            ),
            route="request_ticket",
            policy=BACKGROUND_RETRY,
            retry_safe=True,
            idempotency_key=f"request-ticket:{user.id}:{movie_name.casefold()}",
        )
        await db.save_pending_request(user.id, movie_name)
        success_msg = (
            "<b>Request sent</b>\n\nYour request for "
            f"<b>{_html(movie_name)}</b> has been sent to the admins — "
            "we'll notify you here once it's uploaded."
        )

        user_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("🏠 Back to Home", callback_data="start_home")]]
        )

        if is_callback:
            await message_obj.edit_text(success_msg, reply_markup=user_markup, parse_mode=ParseMode.HTML)
        else:
            await message_obj.reply_text(success_msg, reply_markup=user_markup, parse_mode=ParseMode.HTML)
    except Exception as error:
        reference = report_internal_error(logger, "request_ticket", error, user_id=user.id)
        await message_obj.reply_text(public_error_message(reference))


# ── Admin taps "Mark Uploaded & Notify User" ───────────────────────────────────
@Client.on_callback_query(filters.regex(r"^reqdone#") & filters.user(ADMIN_ID))
async def mark_request_done(client: Client, callback: CallbackQuery):
    try:
        parts = callback.data.split("#", 2)
        if len(parts) < 3:
            return await answer_callback_safely(callback, "❌ Malformed data.", show_alert=True)
        _, user_id, movie_name = parts
    except Exception:
        return await answer_callback_safely(callback, "❌ Malformed data.", show_alert=True)

    # _fulfill_matching_requests() (realtime_indexer.py) may have already
    # auto-notified this user and deleted the ticket between when it was
    # created and when an admin got around to tapping this button — check
    # first so we never send the same "your movie is ready" message twice.
    if not await db.pending_request_exists(int(user_id), movie_name):
        await answer_callback_safely(
            callback, "Already fulfilled automatically — no duplicate sent.", show_alert=True
        )
        resolved_text = _html(callback.message.text) + (
            f"\n\nℹ️ <b>Already auto-fulfilled</b> — acknowledged by: {html_user_mention(callback.from_user)}"
        )
        await callback.message.edit_text(resolved_text, parse_mode=ParseMode.HTML)
        return

    await answer_callback_safely(callback, "Notifying user...", show_alert=False)

    # Notify the User
    try:
        # Deep-link payload can't contain spaces — swap them for underscores
        safe_name = re.sub(r"[^a-zA-Z0-9]", "_", movie_name)

        notify_text = (
            "🎉 <b>Good news</b>\n\n"
            f"The movie you requested — <b>{_html(movie_name)}</b> — "
            "has just been uploaded.\n\n"
            "Tap below to fetch it instantly."
        )

        # The button now triggers the search_ payload we built in start.py!
        markup = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🔍 Fetch Movie Now",
                        url=f"https://t.me/{client.me.username}?start=search_{safe_name}",
                    )
                ]
            ]
        )

        await telegram_call(
            lambda: client.send_message(
                chat_id=int(user_id),
                text=notify_text,
                reply_markup=markup,
                parse_mode=ParseMode.HTML,
            ),
            route="request_manual_fulfillment",
            policy=BACKGROUND_RETRY,
            retry_safe=True,
            idempotency_key=f"request:{user_id}:{movie_name.casefold()}",
        )
        # Clear the ticket now that the user's been notified — otherwise it
        # lingers in pending_requests and a later matching upload could
        # trigger _fulfill_matching_requests() to notify them a second time.
        await db.delete_pending_request(int(user_id), movie_name)
    except (InputUserDeactivated, UserIsBlocked):
        await db.delete_user(int(user_id))
        await db.delete_pending_request(int(user_id), movie_name)
        resolved_text = _html(callback.message.text) + (
            "\n\n⚠️ <b>User blocked bot — removed from database by:</b> "
            f"{html_user_mention(callback.from_user)}"
        )
        await callback.message.edit_text(resolved_text, parse_mode=ParseMode.HTML)
        return await answer_callback_safely(
            callback, "⚠️ User has blocked the bot — removed from database.", show_alert=True
        )
    except Exception as error:
        reference = report_internal_error(logger, "request_notify", error, user_id=user_id)
        return await answer_callback_safely(callback, public_error_message(reference), show_alert=True)

    # Update the Admin Ticket so you know it's done
    resolved_text = _html(callback.message.text) + (
        f"\n\n✅ <b>Completed by:</b> {html_user_mention(callback.from_user)}"
    )
    await callback.message.edit_text(resolved_text, parse_mode=ParseMode.HTML)
