import os
import re
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from database.db import db

# --- 1. THE COMMAND HANDLER (/request) ---
@Client.on_message(filters.command("request") & filters.private)
async def request_cmd(client: Client, message: Message):
    if len(message.command) < 2:
        return await message.reply_text("⚠️ **Usage:** `/request [Movie Name]`\nExample: `/request Oppenheimer`")
        
    movie_name = message.text.split(" ", 1)[1][:40]
    await send_request_ticket(client, message.from_user, movie_name, message)

# --- 2. THE BUTTON HANDLER (From filter.py) ---
@Client.on_callback_query(filters.regex(r"^reqmovie#"))
async def handle_movie_request(client: Client, callback: CallbackQuery):
    movie_name = callback.data.split("#", 1)[1]
    await callback.answer("📤 Sending request...", show_alert=False)
    await send_request_ticket(client, callback.from_user, movie_name, callback.message, is_callback=True)

# --- 3. THE TICKET GENERATOR ---
async def send_request_ticket(client, user, movie_name, message_obj, is_callback=False):
    config = await db.get_config()
    log_channel = config.get("log_channel", 0)
    
    if not log_channel:
        error_msg = "⚠️ Request system is currently offline (Log channel not set)."
        if is_callback: return await message_obj.reply_text(error_msg)
        else: return await message_obj.reply_text(error_msg)
        
    ticket_text = (
        f"🎫 **New Movie Request**\n\n"
        f"👤 **User:** {user.mention} (`{user.id}`)\n"
        f"🎬 **Movie:** `{movie_name}`"
    )
    
    # The button the admin will click when they upload it
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Mark Uploaded & Notify User", callback_data=f"reqdone#{user.id}#{movie_name}")]
    ])
    
    try:
        await client.send_message(chat_id=log_channel, text=ticket_text, reply_markup=markup)
        await db.save_pending_request(user.id, movie_name)
        success_msg = f"✅ **Request Sent!**\n\nYour request for **{movie_name}** has been sent to the admins. We will notify you right here as soon as it is uploaded!"
        
        # --- THE FIX: ADD HOME BUTTON TO REQUEST SCREEN ---
        user_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 Back to Home", callback_data="start_home")]
        ])
        
        if is_callback:
            await message_obj.edit_text(success_msg, reply_markup=user_markup)
        else:
            await message_obj.reply_text(success_msg, reply_markup=user_markup)
    except Exception as e:
        await message_obj.reply_text(f"❌ Failed to send request: {e}")

# --- 4. THE ADMIN COMPLETION HANDLER ---
@Client.on_callback_query(filters.regex(r"^reqdone#"))
async def mark_request_done(client: Client, callback: CallbackQuery):
    try:
        parts = callback.data.split("#", 2)
        if len(parts) < 3:
            return await callback.answer("❌ Malformed data.", show_alert=True)
        _, user_id, movie_name = parts
    except Exception:
        return await callback.answer("❌ Malformed data.", show_alert=True)
    await callback.answer("Notifying user...", show_alert=False)
    
    # Notify the User
    try:
        # --- THE FIX: Create a Magic Deep Link ---
        # Replaces spaces with underscores so Telegram accepts it as a command
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', movie_name)
        
        notify_text = (
            f"🎉 **Good News!**\n\n"
            f"The movie you requested (**{movie_name}**) has just been uploaded to our database!\n\n"
            f"👇 Click the button below to fetch it instantly."
        )
        
        # The button now triggers the search_ payload we built in start.py!
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Fetch Movie Now", url=f"https://t.me/{client.me.username}?start=search_{safe_name}")]
        ])
        
        await client.send_message(chat_id=int(user_id), text=notify_text, reply_markup=markup)
    except Exception as e:
        return await callback.answer(f"❌ Could not PM user (they might have blocked the bot). Error: {e}", show_alert=True)
        
    # Update the Admin Ticket so you know it's done
    resolved_text = callback.message.text + f"\n\n✅ **Completed by:** {callback.from_user.mention}"
    await callback.message.edit_text(resolved_text)