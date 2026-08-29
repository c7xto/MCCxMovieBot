import re
import time
import secrets
import asyncio
import logging
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import MessageNotModified
from pyrogram.types import Message, InlineKeyboardButton, CallbackQuery
from plugins.mobile_ui import MobileInlineKeyboardMarkup as InlineKeyboardMarkup
from database.db import db
from plugins.access_policy import authorize_user_action, enforce_user_action
from plugins.callbacks import answer_callback_safely
from plugins.filter import (
    send_smart_log,
    _sort_results,
    clean_query,
    _file_button_label,
    _build_results_caption,
)
from plugins.search_indicator import show_search_indicator, remove_search_indicator
from plugins.telegram_retry import INTERACTIVE_RETRY, telegram_call
from plugins.task_supervisor import TaskConflict, supervisor
from plugins.workload import (
    WorkloadRejected,
    enforce_search_rate_limits,
    interactive_callback,
    search_slot,
    validate_search_query,
)
from utils import _no_preview, _html, html_user_mention

logger = logging.getLogger(__name__)


def _spawn_group_task(coroutine, key):
    try:
        supervisor.spawn(
            coroutine,
            key=key,
            owner="group_connect",
            drain_on_shutdown=True,
        )
    except TaskConflict:
        logger.info("Group background task skipped key=%s", key)


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _build_group_buttons(
    page_files,
    client_username,
    session_id,
    page,
    total,
    total_pages,
    delete_seconds=None,
):
    """Build the same compact callback file rows used in private search."""
    buttons = []
    for file_doc in page_files:
        delete_value = int(delete_seconds) if delete_seconds else 0
        buttons.append(
            [
                InlineKeyboardButton(
                    _file_button_label(file_doc),
                    callback_data=f"grpfile#{file_doc['_id']}#{delete_value}",
                )
            ]
        )
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅ PREV", callback_data=f"grppage#{session_id}#{page - 1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("NEXT ➡", callback_data=f"grppage#{session_id}#{page + 1}"))
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
        if (
            not await db.is_group_whitelisted(message.chat.id)
            and config.get("group_whitelist_mode", "blacklist") == "whitelist"
        ):
            try:
                await client.leave_chat(message.chat.id)
            except Exception:
                pass
            _spawn_group_task(
                send_smart_log(
                    client,
                    f"🔒 **#WhitelistBlocked**\n\n📌 {message.chat.title}\n"
                    f"🆔 `{message.chat.id}`\n\n"
                    f"Whitelist mode is ON and this group isn't approved — left automatically. "
                    f"Whitelist it in Group Manager, then re-add the bot.",
                ),
                f"log:whitelist:{message.chat.id}:{time.monotonic_ns()}",
            )
            return

        await db.add_group(message.chat.id, message.chat.title)

        try:
            total_members = await client.get_chat_members_count(message.chat.id)
            added_by = html_user_mention(message.from_user) if message.from_user else "Unknown"
            _spawn_group_task(
                send_smart_log(
                    client,
                    "🏘 <b>#NewGroup</b>\n\n"
                    f"📌 {_html(message.chat.title)}\n"
                    f"🆔 <code>{message.chat.id}</code>\n"
                    f"👥 <code>{total_members}</code> members\n"
                    f"👤 Added by: {added_by}",
                    parse_mode=ParseMode.HTML,
                ),
                f"log:new-group:{message.chat.id}:{time.monotonic_ns()}",
            )
        except Exception as e:
            logger.error(f"New group log failed: {e}")

        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("🤖 Search Movies", url=f"https://t.me/{client.me.username}")]]
        )
        await message.reply_text(
            f"🎬 <b>MCCx Movie Bot is ready</b>\n\n"
            f"Send a movie or series title in this group to search the library.",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML,
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

    access = await authorize_user_action(message.from_user.id, "group_search")
    if not access.allowed:
        return

    if await db.is_group_banned(message.chat.id):
        try:
            await client.leave_chat(message.chat.id)
        except Exception:
            pass
        return

    config = access.config
    group = await db.get_group(message.chat.id)
    if not _is_whitelist_ok(config, group):
        try:
            await client.leave_chat(message.chat.id)
        except Exception:
            pass
        return

    raw_query = message.text.strip()
    query = clean_query(raw_query)

    chat_words = {
        "hi",
        "hello",
        "hey",
        "bro",
        "thanks",
        "thank you",
        "pls",
        "admin",
        "help",
        "ok",
        "okay",
        "good",
        "morning",
        "night",
    }
    if raw_query.lower() in chat_words or len(raw_query) < 3:
        try:
            await message.delete()
            warning = await message.reply_text(
                f"⚠️ {html_user_mention(message.from_user)}, "
                f"<b>No Chatting Allowed.</b>\n"
                f"<blockquote>Type a Movie or Series name only.</blockquote>",
                parse_mode=ParseMode.HTML,
            )
            await db.schedule_deletion(warning.chat.id, warning.id, 5)
        except Exception:
            pass
        return

    if not query:
        return

    try:
        query = validate_search_query(query)
        await enforce_search_rate_limits(message.from_user.id, message.chat.id)
    except WorkloadRejected as exc:
        return await message.reply_text(exc.public_message)

    indicator = await show_search_indicator(client, message.chat.id)
    start_time = time.time()
    try:
        async with search_slot("group_search"):
            results = await db.get_search_results(query)
    except WorkloadRejected as exc:
        await remove_search_indicator(indicator)
        return await message.reply_text(exc.public_message)
    except Exception:
        await remove_search_indicator(indicator)
        raise
    # increment_group_search has no internal try/except, unlike the other
    # fire-and-forget calls in this file — a transient Mongo error here would
    # otherwise vanish into asyncio's default "Task exception was never
    # retrieved" log instead of reaching the admin log channel.
    _spawn_group_task(
        db.increment_group_search(message.chat.id),
        f"metric:group-search:{message.chat.id}:{message.id}",
    )

    # ── No results ────────────────────────────────────────────────────────────
    if not results:
        await remove_search_indicator(indicator)
        should_alert = await db.log_missed_search(query)
        if should_alert:
            _spawn_group_task(
                send_smart_log(
                    client,
                    "❌ <b>#MissedSearch</b>\n\n"
                    f"🎬 <code>{_html(query)}</code>\n"
                    f"👤 {html_user_mention(message.from_user)}\n"
                    "📍 Group Chat",
                    parse_mode=ParseMode.HTML,
                ),
                f"log:missed-group:{message.chat.id}:{time.monotonic_ns()}",
            )

        safe_query = re.sub(r"[^a-zA-Z0-9]", "_", query)[:40]
        markup = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "📝 Request This Movie",
                        url=f"https://t.me/{client.me.username}?start=req_{safe_query}",
                    ),
                    InlineKeyboardButton(
                        "🔍 Search Guide", url=f"https://t.me/{client.me.username}?start=help"
                    ),
                ]
            ]
        )
        not_found_msg = await message.reply_text(
            f"🔎 <b>No files found</b>\n\n"
            f"We couldn't find <code>{_html(query)}</code>.\n"
            f"<blockquote>Try only the title, remove the year or language, "
            f"or check the spelling.</blockquote>",
            reply_markup=markup,
            parse_mode=ParseMode.HTML,
            **_no_preview(),
        )
        await db.schedule_deletion(not_found_msg.chat.id, not_found_msg.id, 15)
        return

    # ── Results found ─────────────────────────────────────────────────────────
    time_taken = time.time() - start_time
    await db.clear_old_searches()

    session_id = secrets.token_urlsafe(9)
    sorted_files = _sort_results(results)

    # Per-group override takes priority over the global default — set via
    # Group Manager -> Group Settings -> Set Auto-Delete.
    custom_del = (group.get("settings", {}) if group else {}).get("auto_delete_time")
    _del_secs = int(custom_del) if custom_del else int(config.get("auto_delete_time", 300))
    speed = f"{time_taken:.3f}s"

    session_data = {
        "results": sorted_files,
        "query": query,
        "speed": speed,
        "time": time.time(),
        "auto_delete_time": _del_secs,
        "is_group": True,
        "group_chat_id": message.chat.id,
        "user_id": message.from_user.id,
        "first_name": message.from_user.first_name or "",
    }
    await db.save_search(session_id, session_data)

    # Build page 0
    per_page = 10
    total = len(sorted_files)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page_files = sorted_files[:per_page]

    caption = _build_caption(query, total, 0, total_pages, message.from_user.first_name or "")
    buttons = _build_group_buttons(
        page_files,
        client.me.username,
        session_id,
        0,
        total,
        total_pages,
        delete_seconds=_del_secs,
    )
    markup = InlineKeyboardMarkup(buttons)

    await remove_search_indicator(indicator)

    # Send the finished card directly.  Kurigram can wait indefinitely when
    # editing a just-sent group message on some MTProto sessions; that left
    # users staring at "Searching…" forever even though results were ready.
    # A bounded direct send avoids that edit round-trip entirely.
    try:
        result_msg = await asyncio.wait_for(
            telegram_call(
                lambda: message.reply_text(
                    text=caption,
                    reply_markup=markup,
                    parse_mode=ParseMode.HTML,
                    **_no_preview(),
                ),
                route="group_search_results",
                policy=INTERACTIVE_RETRY,
                retry_safe=True,
                idempotency_key=f"group-results:{message.chat.id}:{message.id}",
            ),
            timeout=20,
        )
    except Exception as exc:
        # Retry with file rows only, avoiding pagination/session controls.
        logger.exception("Full group result-card send failed for %r: %s", query, exc)
        compact_buttons = [
            [
                InlineKeyboardButton(
                    _file_button_label(file_doc),
                    callback_data=f"grpfile#{file_doc['_id']}#{_del_secs}",
                )
            ]
            for file_doc in page_files
        ]
        try:
            result_msg = await asyncio.wait_for(
                telegram_call(
                    lambda: message.reply_text(
                        text=caption,
                        reply_markup=InlineKeyboardMarkup(compact_buttons),
                        parse_mode=ParseMode.HTML,
                        **_no_preview(),
                    ),
                    route="group_search_results_compact",
                    policy=INTERACTIVE_RETRY,
                    retry_safe=True,
                    idempotency_key=f"group-compact:{message.chat.id}:{message.id}",
                ),
                timeout=20,
            )
        except Exception as fallback_exc:
            logger.exception(
                "Compact group result-card send also failed for %r: %s",
                query,
                fallback_exc,
            )
            result_msg = await telegram_call(
                lambda: message.reply_text(
                    (
                        f"🎬 <b>{_html(query.title())}</b>\n"
                        f"<blockquote>{total} matches found.</blockquote>\n"
                        f"Open @{client.me.username} privately to get the files."
                    ),
                    parse_mode=ParseMode.HTML,
                ),
                route="group_search_results_text",
                policy=INTERACTIVE_RETRY,
                retry_safe=True,
                idempotency_key=f"group-text:{message.chat.id}:{message.id}",
            )

    await db.schedule_deletion(result_msg.chat.id, result_msg.id, _del_secs)


# ─── Group pagination callback ────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^grpfile#"))
async def open_group_file_in_private(client: Client, callback: CallbackQuery):
    """Open a selected group result in the bot's private delivery flow."""
    parts = callback.data.split("#", 2)
    if len(parts) != 3 or not parts[1]:
        await answer_callback_safely(callback, "This file button is invalid.", show_alert=True)
        return

    file_obj_id, raw_delete_seconds = parts[1], parts[2]
    try:
        delete_seconds = max(0, int(raw_delete_seconds))
    except (TypeError, ValueError):
        delete_seconds = 0
    delete_suffix = f"_d{delete_seconds}" if delete_seconds else ""
    deep_link = (
        f"https://t.me/{client.me.username}?start="
        f"file_{file_obj_id}{delete_suffix}"
    )
    await answer_callback_safely(callback, url=deep_link)


@Client.on_callback_query(filters.regex(r"^grppage#"))
@interactive_callback("group_search_pagination")
async def handle_group_pagination(client: Client, callback: CallbackQuery):
    if not (await enforce_user_action(callback, "search_navigation")).allowed:
        return
    parts = callback.data.split("#")
    session_id = parts[1]
    page = int(parts[2])

    data = await db.get_search(session_id)
    if not data:
        await answer_callback_safely(callback, "⚠️ Session expired. Search again.", show_alert=True)
        return
    if data.get("user_id") != callback.from_user.id:
        await answer_callback_safely(
            callback, "Only the person who searched can change this page.", show_alert=True
        )
        return

    results = data["results"]
    query = data["query"]
    per_page = 10
    total = len(results)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, total_pages - 1))
    start_idx = page * per_page
    page_files = results[start_idx : start_idx + per_page]

    caption = _build_caption(query, total, page, total_pages, data.get("first_name", ""))
    buttons = _build_group_buttons(
        page_files,
        client.me.username,
        session_id,
        page,
        total,
        total_pages,
        delete_seconds=data.get("auto_delete_time"),
    )
    markup = InlineKeyboardMarkup(buttons)

    try:
        await telegram_call(
            lambda: callback.message.edit_text(
                text=caption,
                reply_markup=markup,
                parse_mode=ParseMode.HTML,
                **_no_preview(),
            ),
            route="group_search_pagination",
            policy=INTERACTIVE_RETRY,
            retry_safe=True,
            idempotency_key=(f"group-page:{callback.message.chat.id}:{callback.message.id}:{page}"),
        )
    except MessageNotModified:
        pass
    except Exception as e:
        logger.error(f"Group pagination error: {e}")

    await answer_callback_safely(callback)
