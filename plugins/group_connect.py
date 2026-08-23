import re
import time
import secrets
import asyncio
import logging
from pyrogram import Client, filters
from pyrogram.enums import ParseMode, ChatAction
from pyrogram.errors import MessageNotModified, FloodWait
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from database.db import db
from plugins.filter import (
    send_smart_log, _sort_results, clean_query,
    _flat_file_label, _build_results_caption
)
from plugins.health_monitor import _log_task_crash
from utils import _no_preview, _html

logger = logging.getLogger(__name__)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _build_group_buttons(page_files, client_username, session_id, page,
                          total, total_pages):
    """Build the same flat rows as DM, using deep links for delivery."""
    buttons = []
    for f in page_files:
        bot_url = f"https://t.me/{client_username}?start=file_{f['_id']}"
        buttons.append([InlineKeyboardButton(_flat_file_label(f), url=bot_url)])

    # Navigation row — pagination goes back to DM full search for group
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(
            "⬅ PREV",
            callback_data=f"grppage#{session_id}#{page - 1}"
        ))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(
            "NEXT ➡",
            callback_data=f"grppage#{session_id}#{page + 1}"
        ))
    if nav:
        buttons.append(nav)

    return buttons


def _build_caption(query, total, page, total_pages, first_name=""):
    return _build_results_caption(query, total, page, total_pages, first_name)


def _is_whitelist_ok(config: dict, group_doc) -> bool:
    """True if this group may interact with the bot under the current
    whitelist/blacklist mode. In blacklist mode (default) every non-banned
    group is allowed; in whitelist mode only groups explicitly marked
    `whitelisted` in connected_groups are."""
    if config.get("group_whitelist_mode", "blacklist") != "whitelist":
        return True
    return bool(group_doc and group_doc.get("whitelisted", False))


# ─── Bot added to group ───────────────────────────────────────────────────────

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

        config = await db.get_config()
        if not await db.is_group_whitelisted(message.chat.id) and \
                config.get("group_whitelist_mode", "blacklist") == "whitelist":
            try:
                await client.leave_chat(message.chat.id)
            except Exception:
                pass
            asyncio.create_task(send_smart_log(client,
                f"🔒 **#WhitelistBlocked**\n\n📌 {message.chat.title}\n"
                f"🆔 `{message.chat.id}`\n\n"
                f"Whitelist mode is ON and this group isn't approved — left automatically. "
                f"Whitelist it in Group Manager, then re-add the bot."
            ))
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
            f"🎬 <b>MCCx Movie Bot is ready</b>\n\n"
            f"Send a movie or series title in this group to search the library.",
            reply_markup=keyboard, parse_mode=ParseMode.HTML
        )
        break


# ─── Group text search ────────────────────────────────────────────────────────

@Client.on_message(filters.group & filters.text & ~filters.command(["start", "help"]))
async def group_search(client: Client, message: Message):
    if not message.from_user:
        return
    # Commands belong to command handlers (or other bots), never to the
    # movie-search pipeline. This also covers /command@other_bot forms that a
    # fixed command exclusion list cannot enumerate safely.
    if (message.text or "").lstrip().startswith("/"):
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
    group  = await db.get_group(message.chat.id)
    if not _is_whitelist_ok(config, group):
        try:
            await client.leave_chat(message.chat.id)
        except Exception:
            pass
        return

    if config.get("maintenance_mode"):
        return

    raw_query = message.text.strip()
    query     = clean_query(raw_query)

    chat_words = {"hi", "hello", "hey", "bro", "thanks", "thank you",
                  "pls", "admin", "help", "ok", "okay", "good", "morning", "night"}
    if raw_query.lower() in chat_words or len(raw_query) < 3:
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

    if not query:
        return

    try:
        await client.send_chat_action(message.chat.id, ChatAction.TYPING)
    except Exception:
        pass

    start_time = time.time()
    results    = await db.get_search_results(query)
    # increment_group_search has no internal try/except, unlike the other
    # fire-and-forget calls in this file — a transient Mongo error here would
    # otherwise vanish into asyncio's default "Task exception was never
    # retrieved" log instead of reaching the admin log channel.
    count_task = asyncio.create_task(db.increment_group_search(message.chat.id))
    count_task.add_done_callback(lambda t: _log_task_crash(t, client, "increment_group_search"))

    # ── No results ────────────────────────────────────────────────────────────
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
             url=f"https://t.me/{client.me.username}?start=req_{safe_query}"),
             InlineKeyboardButton("🔍 Search Guide", url=f"https://t.me/{client.me.username}?start=help")]
        ])
        not_found_msg = await message.reply_text(
            f"🔎 <b>No files found</b>\n\n"
            f"We couldn't find <code>{_html(query)}</code>.\n"
            f"<blockquote>Try only the title, remove the year or language, "
            f"or check the spelling.</blockquote>",
            reply_markup=markup, parse_mode=ParseMode.HTML,
            **_no_preview()
        )
        await asyncio.sleep(15)
        try:
            await not_found_msg.delete()
        except Exception:
            pass
        return

    # ── Results found ─────────────────────────────────────────────────────────
    time_taken = time.time() - start_time
    await db.clear_old_searches()

    session_id   = secrets.token_urlsafe(9)
    sorted_files = _sort_results(results)

    # Per-group override takes priority over the global default — set via
    # Group Manager -> Group Settings -> Set Auto-Delete.
    custom_del = (group.get("settings", {}) if group else {}).get("auto_delete_time")
    _del_secs  = int(custom_del) if custom_del else int(config.get("auto_delete_time", 300))
    speed     = f"{time_taken:.3f}s"

    session_data = {
        "results":          sorted_files,
        "query":            query,
        "speed":            speed,
        "time":             time.time(),
        "auto_delete_time": _del_secs,
        "is_group":         True,
        "group_chat_id":    message.chat.id,
        "user_id":          message.from_user.id,
        "first_name":       message.from_user.first_name or "",
    }
    await db.save_search(session_id, session_data)

    # Build page 0
    per_page    = 10
    total       = len(sorted_files)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page_files  = sorted_files[:per_page]

    caption = _build_caption(
        query, total, 0, total_pages, message.from_user.first_name or ""
    )
    buttons = _build_group_buttons(
        page_files, client.me.username, session_id, 0, total, total_pages
    )
    markup = InlineKeyboardMarkup(buttons)

    # Send the finished card directly.  Kurigram can wait indefinitely when
    # editing a just-sent group message on some MTProto sessions; that left
    # users staring at "Searching…" forever even though results were ready.
    # A bounded direct send avoids that edit round-trip entirely.
    try:
        result_msg = await asyncio.wait_for(
            message.reply_text(
                text=caption,
                reply_markup=markup,
                parse_mode=ParseMode.HTML,
            ),
            timeout=20,
        )
    except Exception as exc:
        # Retry with a smaller, callback-free keyboard that stays well below
        # Telegram's markup limits and avoids pagination state entirely.
        logger.exception("Full group result-card send failed for %r: %s", query, exc)
        compact_buttons = []
        for file_doc in page_files:
            compact_label = _flat_file_label(file_doc)
            compact_buttons.append([InlineKeyboardButton(
                compact_label,
                url=f"https://t.me/{client.me.username}?start=file_{file_doc['_id']}",
            )])
        try:
            result_msg = await asyncio.wait_for(
                message.reply_text(
                    text=caption,
                    reply_markup=InlineKeyboardMarkup(compact_buttons),
                    parse_mode=ParseMode.HTML,
                ),
                timeout=20,
            )
        except Exception as fallback_exc:
            logger.exception(
                "Compact group result-card send also failed for %r: %s",
                query,
                fallback_exc,
            )
            result_msg = await message.reply_text(
                (
                    f"🎬 <b>{_html(query.title())}</b>\n"
                    f"<blockquote>{total} matches found.</blockquote>\n"
                    f"Open @{client.me.username} privately to get the files."
                ),
                parse_mode=ParseMode.HTML,
            )

    await db.schedule_deletion(result_msg.chat.id, result_msg.id, _del_secs)


# ─── Group pagination callback ────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^grppage#"))
async def handle_group_pagination(client: Client, callback: CallbackQuery):
    parts      = callback.data.split("#")
    session_id = parts[1]
    page       = int(parts[2])

    data = await db.get_search(session_id)
    if not data:
        await callback.answer("⚠️ Session expired. Search again.", show_alert=True)
        return
    if data.get("user_id") != callback.from_user.id:
        await callback.answer("Only the person who searched can change this page.", show_alert=True)
        return

    results     = data["results"]
    query       = data["query"]
    per_page    = 10
    total       = len(results)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page        = max(0, min(page, total_pages - 1))
    start_idx   = page * per_page
    page_files  = results[start_idx: start_idx + per_page]

    caption = _build_caption(
        query, total, page, total_pages, data.get("first_name", "")
    )
    buttons = _build_group_buttons(
        page_files, client.me.username, session_id, page, total, total_pages
    )
    markup = InlineKeyboardMarkup(buttons)

    try:
        await callback.message.edit_text(text=caption, reply_markup=markup, parse_mode=ParseMode.HTML)
    except MessageNotModified:
        pass
    except Exception as e:
        logger.error(f"Group pagination error: {e}")

    await callback.answer()
