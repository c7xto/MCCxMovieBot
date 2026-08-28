import logging
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, ChatMemberUpdated
)
from pyrogram.enums import ChatMemberStatus, ParseMode
from database.db import db
from utils import _no_preview, html_user_mention

load_dotenv()
logger = logging.getLogger(__name__)


async def _auto_delete_welcome(msg, delay=300):
    """Persist cleanup so it still occurs if the bot restarts."""
    await db.schedule_deletion(msg.chat.id, msg.id, delay)


@Client.on_message(filters.new_chat_members | filters.left_chat_member, group=1)
async def delete_service_messages(client: Client, message: Message):
    try:
        await message.delete()
    except Exception:
        pass


@Client.on_chat_member_updated(filters.group, group=2)
async def welcome_new_member(client: Client, update: ChatMemberUpdated):
    old = update.old_chat_member
    new = update.new_chat_member

    if not new or new.status not in [ChatMemberStatus.MEMBER, ChatMemberStatus.RESTRICTED]:
        return
    if old and old.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.RESTRICTED]:
        return
    if new.user.id == client.me.id:
        return

    config = await db.get_config()
    update_channel = config.get("update_channel") or config.get("update_channel_link", "")
    group = await db.get_group(update.chat.id)
    custom_delete = (group.get("settings", {}) if group else {}).get("auto_delete_time")
    auto_delete_secs = int(custom_delete or config.get("auto_delete_time", 300))
    auto_delete_mins = max(1, auto_delete_secs // 60)

    buttons = []
    if update_channel and update_channel not in ["", "Not Set"]:
        buttons.append([InlineKeyboardButton("📢 Updates Channel", url=update_channel)])
    buttons.append([InlineKeyboardButton(
        "🤖 Search Movies",
        url=f"https://t.me/{client.me.username}?start=start"
    )])
    markup = InlineKeyboardMarkup(buttons)

    welcome_text = (
        f"👋 Welcome, {html_user_mention(new.user)}!\n\n"
        f"<blockquote>"
        f"Type any movie or series name here to search.\n"
        f"Files are sent directly to your PM.\n\n"
        f"Example: <code>Leo</code> or <code>Aadujeevitham 2024</code>"
        f"</blockquote>\n\n"
        f"<i>🗑 This message auto-deletes in {auto_delete_mins} minutes.</i>"
    )

    try:
        welcome_msg = await client.send_message(
            chat_id=update.chat.id,
            text=welcome_text,
            reply_markup=markup,
            parse_mode=ParseMode.HTML,
            **_no_preview()
        )
        await _auto_delete_welcome(welcome_msg, auto_delete_secs)
    except Exception as e:
        logger.error(f"Welcome message error: {e}")
