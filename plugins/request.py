import os
import re
import logging
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from database.db import db

load_dotenv()

logger = logging.getLogger(__name__)


@Client.on_message(filters.command("request") & filters.private)
async def request_cmd(client: Client, message: Message):
    if len(message.command) < 2:
        return await message.reply_text(
            "⚠️ **Usage:** `/request [Movie Name]`\nExample: `/request Oppenheimer`"
        )
    movie_name = message.text.split(" ", 1)[1][:40]
    await send_request_ticket(client, message.from_user, movie_name, message)


@Client.on_callback_query(filters.regex(r"^reqmovie#"))
async def handle_movie_request(client: Client, callback: CallbackQuery):
    movie_name = callback.data.split("#", 1)[1]
    await callback.answer("📤 Sending request...", show_alert=False)
    await send_request_ticket(
        client, callback.from_user, movie_name, callback.message, is_callback=True
    )


async def send_request_ticket(client, user, movie_name, message_obj, is_callback=False):
    config = await db.get_config()
    log_channel = config.get("log_channel", 0)

    if not log_channel:
        reply = "⚠️ Request system is currently offline (Log channel not set)."
        await message_obj.reply_text(reply)
        return

    ticket_text = (
        f"🎫 **New Movie Request**\n\n"
        f"👤 **User:** {user.mention} (`{user.id}`)\n"
        f"🎬 **Movie:** `{movie_name}`"
    )
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "✅ Mark Uploaded & Notify User",
            callback_data=f"reqdone#{user.id}#{movie_name}"
        )]
    ])

    try:
        await client.send_message(chat_id=log_channel, text=ticket_text, reply_markup=markup)

        # Save to pending requests so auto-fulfillment can notify when uploaded
        await db.save_pending_request(user.id, movie_name)

        success_msg = (
            f"✅ **Request Sent!**\n\n"
            f"Your request for **{movie_name}** has been sent to the admins.\n"
            f"We will notify you right here as soon as it is uploaded!"
        )
        user_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 Back to Home", callback_data="start_home")]
        ])

        if is_callback:
            await message_obj.edit_text(success_msg, reply_markup=user_markup)
        else:
            await message_obj.reply_text(success_msg, reply_markup=user_markup)

    except Exception as e:
        logger.error(f"Request ticket error: {e}")
        await message_obj.reply_text("❌ Failed to send request. Please try again later.")


@Client.on_callback_query(filters.regex(r"^reqdone#"))
async def mark_request_done(client: Client, callback: CallbackQuery):
    parts = callback.data.split("#", 2)
    if len(parts) < 3:
        await callback.answer("❌ Malformed callback data.", show_alert=True)
        return
    user_id = parts[1]
    movie_name = parts[2]
    await callback.answer("Notifying user...", show_alert=False)

    try:
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', movie_name)
        notify_text = (
            f"🎉 **Good News!**\n\n"
            f"The movie you requested (**{movie_name}**) has just been uploaded!\n\n"
            f"👇 Click below to fetch it instantly."
        )
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "🔍 Fetch Movie Now",
                url=f"https://t.me/{client.me.username}?start=search_{safe_name}"
            )]
        ])
        await client.send_message(chat_id=int(user_id), text=notify_text, reply_markup=markup)

    except Exception as e:
        return await callback.answer(
            f"❌ Could not PM user (they may have blocked the bot). Error: {e}",
            show_alert=True
        )

    try:
        resolved_text = (
            callback.message.text +
            f"\n\n✅ **Completed by:** {callback.from_user.mention}"
        )
        await callback.message.edit_text(resolved_text)
    except Exception:
        pass
