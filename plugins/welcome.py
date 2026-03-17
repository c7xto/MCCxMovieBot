import asyncio
import logging
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    ChatMemberUpdated, LinkPreviewOptions
)
from pyrogram.enums import ChatMemberStatus, ParseMode
from database.db import db

load_dotenv()

logger = logging.getLogger(__name__)
_NO_PREVIEW = LinkPreviewOptions(is_disabled=True)


async def _auto_delete_welcome(msg, delay=300):
    """Fire-and-forget — was previously blocking the handler for 5 minutes inline."""
    await asyncio.sleep(delay)
    try:
        await msg.delete()
    except Exception:
        pass


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

    # All config from MongoDB — no .env reads
    config = await db.get_config()
    update_channel = config.get("update_channel", "")

    # Live file count — reflects actual DB state at the moment of joining
    total_files = await db.get_total_files()

    buttons = []
    if update_channel and update_channel not in ["", "Not Set"]:
        buttons.append([InlineKeyboardButton("📢 Updates Channel", url=update_channel)])
    buttons.append([
        InlineKeyboardButton(
            "🤖 Search Movies",
            url=f"https://t.me/{client.me.username}?start=start"
        )
    ])
    markup = InlineKeyboardMarkup(buttons)

    welcome_text = (
        f"👋 Welcome, {new.user.mention}!\n\n"
        f"<blockquote>"
        f"Type any movie or series name here to search.\n"
        f"Files are sent directly to your PM.\n\n"
        f"📁 <b>{total_files:,}</b> files available — updated live.\n\n"
        f"Example: <code>Leo</code> or <code>Aadujeevitham 2024</code>"
        f"</blockquote>\n\n"
        f"<i>🗑 This message auto-deletes in 5 minutes.</i>"
    )

    try:
        welcome_msg = await client.send_message(
            chat_id=update.chat.id,
            text=welcome_text,
            reply_markup=markup,
            link_preview_options=_NO_PREVIEW,
            parse_mode=ParseMode.HTML
        )
        asyncio.create_task(_auto_delete_welcome(welcome_msg, 300))
    except Exception as e:
        logger.error(f"Welcome message error: {e}")
