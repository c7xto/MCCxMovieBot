import os
import re
import asyncio
import logging
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode
from database.db import db
from plugins.filter import send_smart_log, extract_attributes, LANG_EMOJI

# load_dotenv() needed so any remaining os.getenv calls work correctly at import time
load_dotenv()

logger = logging.getLogger(__name__)


@Client.on_message(filters.group & filters.new_chat_members)
async def auto_connect_group(client: Client, message: Message):
    bot_id = client.me.id

    for member in message.new_chat_members:
        if member.id == bot_id:
            # G3: If the group was previously banned, leave immediately
            if await db.is_group_banned(message.chat.id):
                try:
                    await client.send_message(
                        message.chat.id,
                        "⚠️ This bot has been removed from this group by the administrator."
                    )
                    await client.leave_chat(message.chat.id)
                except Exception:
                    pass
                return

            await db.add_group(message.chat.id, message.chat.title)

            try:
                total_members = await client.get_chat_members_count(message.chat.id)
                log_text = (
                    f"🏘 **#NewGroup Alert**\n\n"
                    f"📌 **Name:** {message.chat.title}\n"
                    f"🆔 **ID:** `{message.chat.id}`\n"
                    f"👥 **Members:** `{total_members}`\n"
                    f"👤 **Added by:** {message.from_user.mention}"
                )
                # send_smart_log reads log_channel from db.get_config() internally
                asyncio.create_task(send_smart_log(client, log_text))
            except Exception as e:
                logger.error(f"Failed to send New Group log: {e}")

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🤖 Message Bot Privately",
                                      url=f"https://t.me/{client.me.username}")]
            ])

            await message.reply_text(
                f"🎬 <b>Thanks for adding me to {message.chat.title}!</b>\n\n"
                f"I am automatically connected to my master database.\n"
                f"Just type the name of any movie or TV show here, and I will find it for you!",
                reply_markup=keyboard,
                quote=True,
                parse_mode=ParseMode.HTML
            )
            break


@Client.on_message(filters.group & filters.text & ~filters.command(["start", "help", "connect"]))
async def group_search(client: Client, message: Message):
    # Ban check — banned users cannot search in groups either
    if not message.from_user:
        return  # anonymous admin / channel post — skip
    if await db.is_banned(message.from_user.id):
        return

    # G3: Group ban check — bot should not be responding in banned groups
    if await db.is_group_banned(message.chat.id):
        try:
            await client.leave_chat(message.chat.id)
        except Exception:
            pass
        return

    # G1: Whitelist/blacklist mode
    config_gm = await db.get_config()
    gm_mode = config_gm.get("group_whitelist_mode", "blacklist")
    if gm_mode == "whitelist":
        if not await db.is_group_whitelisted(message.chat.id):
            return  # silently ignore non-whitelisted groups

    # C1: Maintenance mode
    if config_gm.get("maintenance_mode"):
        return

    query = message.text.strip()
    clean_content = query.lower()

    chat_words = [
        "hi", "hello", "hey", "bro", "thanks", "thank you",
        "pls", "admin", "help", "ok", "okay", "good", "morning", "night"
    ]
    if clean_content in chat_words:
        try:
            await message.delete()
            warning = await message.reply_text(
                f"⚠️ {message.from_user.mention}, <b>No Chatting Allowed.</b>\n"
                f"<blockquote>Please type a Movie or Series name only.</blockquote>",
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

    if not results:
        # F9: Log to MongoDB — returns True if log channel alert should fire
        should_alert = await db.log_missed_search(query)
        if should_alert:
            asyncio.create_task(send_smart_log(
                client,
                f"❌ **#MissedSearch**\n\n"
                f"🎬 **Movie:** `{query}`\n"
                f"👤 **Requested by:** {message.from_user.mention}\n"
                f"📍 **Where:** Group Chat\n"
            ))

        from urllib.parse import quote as url_quote
        safe_query = re.sub(r'[^a-zA-Z0-9]', '_', query)[:40]
        google_url = f"https://www.google.com/search?q={url_quote(query)}"
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "🔍 Check Spelling on Google", url=google_url
            )],
            [InlineKeyboardButton(
                "📝 Request This Movie",
                url=f"https://t.me/{client.me.username}?start=req_{safe_query}"
            )]
        ])

        not_found_msg = await message.reply_text(
            f"😔 <b>No results for</b> <code>{query}</code>\n\n"
            f"<blockquote>Tap below to check the spelling or send a request.</blockquote>",
            reply_markup=markup,
            quote=True,
            parse_mode=ParseMode.HTML
        )

        await asyncio.sleep(15)
        try:
            await not_found_msg.delete()
        except Exception:
            pass
        return

    # G5: Increment group search counter atomically
    asyncio.create_task(db.increment_group_search(message.chat.id))

    buttons = []
    for file in results[:5]:
        size_mb = file.get('file_size', 0) / (1024 * 1024)
        size_str = f"{size_mb / 1024:.2f}GB" if size_mb > 1024 else f"{size_mb:.0f}MB"

        f_lang, f_qual = extract_attributes(file['file_name'])
        l_emoji = LANG_EMOJI.get(f_lang, "")

        button_text = f"🎬 [{size_str}] {file['file_name'][:20]}... {l_emoji} {f_qual}"
        bot_url = f"https://t.me/{client.me.username}?start=file_{file['_id']}"
        buttons.append([InlineKeyboardButton(button_text, url=bot_url)])

    if len(results) > 5:
        safe_query = re.sub(r'[^a-zA-Z0-9]', '_', query)[:40]
        buttons.append([InlineKeyboardButton(
            f"🔍 See All {len(results)} Results",
            url=f"https://t.me/{client.me.username}?start=search_{safe_query}"
        )])

    markup = InlineKeyboardMarkup(buttons)

    await message.reply_text(
        f"🎬 <b>{query.title()}</b>  —  {len(results)} files found\n"
        f"<blockquote>Tap a result below to get it in your PM.</blockquote>",
        reply_markup=markup,
        quote=True,
        parse_mode=ParseMode.HTML
    )
