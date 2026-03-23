import re
import asyncio
import logging
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode
from database.db import db
from plugins.filter import send_smart_log, extract_attributes, LANG_EMOJI

logger = logging.getLogger(__name__)


@Client.on_message(filters.group & filters.new_chat_members)
async def auto_connect_group(client: Client, message: Message):
    bot_id = client.me.id
    for member in message.new_chat_members:
        if member.id != bot_id:
            continue

        if await db.is_group_banned(message.chat.id):
            try:
                await client.leave_chat(message.chat.id)
            except Exception:
                pass
            return

        await db.add_group(message.chat.id, message.chat.title)

        try:
            total_members = await client.get_chat_members_count(message.chat.id)
            added_by = message.from_user.mention if message.from_user else "Unknown"
            asyncio.create_task(send_smart_log(client,
                f"🏘 **#NewGroup**\n\n📌 {message.chat.title}\n"
                f"🆔 `{message.chat.id}`\n👥 `{total_members}` members\n"
                f"👤 Added by: {added_by}"
            ))
        except Exception as e:
            logger.error(f"New group log failed: {e}")

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🤖 Search Movies", url=f"https://t.me/{client.me.username}")]
        ])
        await message.reply_text(
            f"🎬 <b>MCCxBot connected to {message.chat.title}!</b>\n\n"
            f"Type any movie or series name to search.",
            reply_markup=keyboard, quote=True, parse_mode=ParseMode.HTML
        )
        break


@Client.on_message(filters.group & filters.text & ~filters.command(["start", "help", "connect"]))
async def group_search(client: Client, message: Message):
    # Guard: anonymous admin posts have no from_user
    if not message.from_user:
        return

    if await db.is_banned(message.from_user.id):
        return

    if await db.is_group_banned(message.chat.id):
        try:
            await client.leave_chat(message.chat.id)
        except Exception:
            pass
        return

    config = await db.get_config()
    if config.get("maintenance_mode"):
        return

    query = message.text.strip()
    clean_content = query.lower()

    chat_words = ["hi", "hello", "hey", "bro", "thanks", "thank you",
                  "pls", "admin", "help", "ok", "okay", "good", "morning", "night"]
    if clean_content in chat_words:
        try:
            await message.delete()
            warning = await message.reply_text(
                f"⚠️ {message.from_user.mention}, <b>No Chatting Allowed.</b>\n"
                f"<blockquote>Type a Movie or Series name only.</blockquote>",
                parse_mode=ParseMode.HTML
            )
            await asyncio.sleep(5)
            await warning.delete()
        except Exception:
            pass
        return

    if len(query) < 3:
        return

    results = await db.get_search_results(query)
    asyncio.create_task(db.increment_group_search(message.chat.id))

    if not results:
        should_alert = await db.log_missed_search(query)
        if should_alert:
            asyncio.create_task(send_smart_log(client,
                f"❌ **#MissedSearch**\n\n🎬 `{query}`\n"
                f"👤 {message.from_user.mention}\n📍 Group Chat"
            ))

        safe_query = re.sub(r'[^a-zA-Z0-9]', '_', query)[:40]
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("📝 Request This Movie",
             url=f"https://t.me/{client.me.username}?start=req_{safe_query}")]
        ])
        not_found_msg = await message.reply_text(
            f"😔 <b>No results for</b> <code>{query}</code>\n\n"
            f"<blockquote>Tap below to request it.</blockquote>",
            reply_markup=markup, quote=True, parse_mode=ParseMode.HTML
        )
        await asyncio.sleep(15)
        try:
            await not_found_msg.delete()
        except Exception:
            pass
        return

    buttons = []
    for file in results[:5]:
        size_mb = file.get("file_size", 0) / (1024 * 1024)
        size_str = f"{size_mb / 1024:.2f} GB" if size_mb >= 1024 else f"{size_mb:.0f} MB"
        f_lang, f_qual = extract_attributes(file["file_name"])
        qual_label = f_qual if f_qual not in ["Other", ""] else ""
        lang_label = f_lang if f_lang not in ["Other", ""] else ""
        parts = [p for p in [qual_label, lang_label, size_str] if p]
        btn_text = "🎬 " + "  •  ".join(parts)
        bot_url = f"https://t.me/{client.me.username}?start=file_{file['_id']}"
        buttons.append([InlineKeyboardButton(btn_text, url=bot_url)])

    if len(results) > 5:
        safe_query = re.sub(r'[^a-zA-Z0-9]', '_', query)[:40]
        buttons.append([InlineKeyboardButton(
            f"🔍 See all {len(results)} results",
            url=f"https://t.me/{client.me.username}?start=search_{safe_query}"
        )])

    result_msg = await message.reply_text(
        f"🎬 <b>{query.title()}</b>  —  {len(results)} files\n"
        f"<blockquote>Tap a result to get it in your PM.</blockquote>",
        reply_markup=InlineKeyboardMarkup(buttons),
        quote=True, parse_mode=ParseMode.HTML
    )

    async def _auto_delete(msg):
        await asyncio.sleep(120)
        try:
            await msg.delete()
        except Exception:
            pass
    asyncio.create_task(_auto_delete(result_msg))
