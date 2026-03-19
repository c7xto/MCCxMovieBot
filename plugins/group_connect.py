import os
import asyncio
import logging
import urllib.parse
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode
from database.db import db

# Notice: QUAL_EMOJI has been completely removed from this import line!
from plugins.filter import send_smart_log, MISSED_CACHE, extract_attributes, LANG_EMOJI

logger = logging.getLogger(__name__)
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", 0))

@Client.on_message(filters.group & filters.new_chat_members)
async def auto_connect_group(client: Client, message: Message):
    bot_id = client.me.id
    
    for member in message.new_chat_members:
        if member.id == bot_id:
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
                asyncio.create_task(send_smart_log(client, log_text))
            except Exception as e:
                logger.error(f"Failed to send New Group log: {e}")
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🤖 Message Bot Privately", url=f"https://t.me/{client.me.username}")]
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
    query = message.text.strip()
    clean_content = query.lower()
    
    chat_words = ["hi", "hello", "hey", "bro", "thanks", "thank you", "pls", "admin", "help", "ok", "okay", "good", "morning", "night"]
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
        if clean_content not in MISSED_CACHE:
            MISSED_CACHE.add(clean_content)
            asyncio.create_task(send_smart_log(client, f"❌ **#MissedSearch**\n\n🎬 **Movie:** `{query}`\n👤 **Requested by:** {message.from_user.mention}\n📍 **Where:** Group Chat\n"))
        
        import re
        safe_query = re.sub(r'[^a-zA-Z0-9]', '_', query)[:40]
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("📝 Request This Movie", url=f"https://t.me/{client.me.username}?start=req_{safe_query}")]
        ])
        
        not_found_msg = await message.reply_text(
            f"😔 <b>No results for</b> <code>{query}</code>\n\n"
            f"<blockquote>Tap below to send a request and we'll upload it for you.</blockquote>",
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

    buttons = []
    for file in results[:5]: 
        size_mb = file.get('file_size', 0) / (1024 * 1024)
        size_str = f"{size_mb / 1024:.2f}GB" if size_mb > 1024 else f"{size_mb:.0f}MB"
        
        f_lang, f_qual = extract_attributes(file['file_name'])
        l_emoji = LANG_EMOJI.get(f_lang, "")
        
        # Put the size right after the clapperboard!
        button_text = f"🎬 [{size_str}] {file['file_name'][:20]}... {l_emoji} {f_qual}"
        bot_url = f"https://t.me/{client.me.username}?start=file_{file['_id']}"
        buttons.append([InlineKeyboardButton(button_text, url=bot_url)])
    if len(results) > 5:
        import re
        safe_query = re.sub(r'[^a-zA-Z0-9]', '_', query)[:40]
        buttons.append([InlineKeyboardButton(
            f"🔍 See All {len(results)} Results",
            url=f"https://t.me/{client.me.username}?start=search_{safe_query}"
        )])

    markup = InlineKeyboardMarkup(buttons)

    result_msg = await message.reply_text(
        f"🎬 <b>{query.title()}</b>  —  {len(results)} files found\n"
        f"<blockquote>Tap a result below to get it in your PM.</blockquote>",
        reply_markup=markup,
        quote=True,
        parse_mode=ParseMode.HTML
    )

    # Auto-delete result after 2 minutes to keep group clean
    async def _auto_delete_result(msg):
        await asyncio.sleep(120)
        try:
            await msg.delete()
        except Exception:
            pass
    asyncio.create_task(_auto_delete_result(result_msg))