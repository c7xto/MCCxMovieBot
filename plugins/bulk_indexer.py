import os
import re
import time
import json
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram import ContinuePropagation, StopPropagation
from pyrogram.enums import ParseMode
from pyrogram.errors import MessageNotModified
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from database.db import db, AllClustersFullError, normalize_file_name
from utils import ADMIN_ID, html_user_mention, report_internal_error

load_dotenv()

# Import our unified smart logger!
from plugins.filter import send_smart_log
from plugins.callbacks import answer_callback_safely
from plugins.index_progress import IndexProgress
from plugins.retry import retry_with_backoff
from plugins.telegram_retry import BACKGROUND_RETRY, INTERACTIVE_RETRY, telegram_call
from plugins.task_supervisor import TaskConflict, supervisor
from plugins.workload import background_turn

logger = logging.getLogger(__name__)
_active_indexes = {}


def _spawn_index_log(coroutine, chat_id, label):
    try:
        supervisor.spawn(
            coroutine,
            key=f"log:index:{chat_id}:{label}:{time.monotonic_ns()}",
            owner="bulk_indexer",
            drain_on_shutdown=True,
        )
    except TaskConflict:
        logger.info("Indexer log skipped during shutdown label=%s", label)
_LOCAL_INDEX_FAILURES = (
    Path(__file__).resolve().parents[1] / "runtime" / "index_failures.jsonl"
)


def _append_local_index_failure(record: dict):
    _LOCAL_INDEX_FAILURES.parent.mkdir(parents=True, exist_ok=True)
    with _LOCAL_INDEX_FAILURES.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


async def _record_failed_range(chat_id, start_id, end_id, stage, error, attempts):
    """Persist a dead-letter record, with runtime-volume fallback."""
    try:
        await db.record_index_failure(
            chat_id, start_id, end_id, stage, error, attempts
        )
        return
    except Exception as persistence_error:
        logger.error(
            "Mongo dead-letter write failed for %s:%s-%s: %s",
            chat_id,
            start_id,
            end_id,
            type(persistence_error).__name__,
        )
    await asyncio.to_thread(
        _append_local_index_failure,
        {
            "chat_id": chat_id,
            "start_id": start_id,
            "end_id": end_id,
            "stage": stage,
            "error_type": type(error).__name__,
            "attempts": attempts,
            "timestamp": time.time(),
        },
    )

# --- GLOBAL STATE DICTIONARY ---

def _index_controls(chat_id: int, state: str = "running"):
    first = (
        InlineKeyboardButton("▶️ Resume", callback_data=f"resume_idx#{chat_id}")
        if state == "paused"
        else InlineKeyboardButton("⏸ Pause", callback_data=f"pause_idx#{chat_id}")
    )
    return InlineKeyboardMarkup([[
        first,
        InlineKeyboardButton("⏹ Safe stop", callback_data=f"stop_idx#{chat_id}"),
    ]])


async def _edit_live_status(chat_id: int, state: str):
    active = _active_indexes.get(chat_id)
    if not active:
        return
    progress, status_message = active
    markup = None if state in {"stopped", "complete"} else _index_controls(chat_id, state)
    try:
        await telegram_call(
            lambda: status_message.edit_text(
                progress.render(state), reply_markup=markup
            ),
            route="bulk_index_progress",
            policy=INTERACTIVE_RETRY,
            retry_safe=True,
            idempotency_key=f"index-progress:{chat_id}:{state}:{progress.checkpoint}",
        )
    except MessageNotModified:
        pass

# --- THE BACKGROUND WORKER ---
async def run_indexer(
    client: Client,
    status_message: Message,
    chat_id: int,
    last_msg_id: int,
    start_id: int,
    lock_token: str,
):
    current_id = max(1, int(start_id))
    batch_size = 50
    progress = IndexProgress(current_id, last_msg_id)
    _active_indexes[chat_id] = (progress, status_message)
    await _edit_live_status(chat_id, "running")
    next_ui_update = time.monotonic() + 3.0

    def note_retry(exc, attempt, delay):
        progress.retries += 1
        logger.warning(
            "Indexer retry channel=%s range=%s-%s attempt=%s delay=%.2fs error=%s",
            chat_id, current_id, end_id, attempt, delay, type(exc).__name__,
        )

    try:
        while current_id <= last_msg_id:
            task = await db.get_index_task_document(chat_id)
            state = (task or {}).get("state")
            if not task or task.get("lock_token") != lock_token:
                logger.warning("Indexer lease lost for channel %s; worker exiting", chat_id)
                return
            if state == "stopped":
                await _edit_live_status(chat_id, "stopped")
                _spawn_index_log(
                    send_smart_log(
                        client,
                        f"⏹ **#IndexStopped**\n\n"
                        f"📦 **Channel:** `{chat_id}`\n"
                        f"✅ **Saved:** `{progress.saved:,}`\n"
                        f"♻️ **Existing:** `{progress.duplicates:,}`\n"
                        f"💾 **Checkpoint:** `{progress.checkpoint:,}`",
                    ),
                    chat_id,
                    "stopped",
                )
                await db.release_index_task(chat_id, lock_token, "stopped")
                return
            if state == "paused":
                await _edit_live_status(chat_id, "paused")
                await db.release_index_task(chat_id, lock_token, "paused")
                return

            if not await db.renew_index_task(chat_id, lock_token):
                logger.warning("Indexer lease renewal failed for channel %s", chat_id)
                return

            end_id = min(current_id + batch_size - 1, last_msg_id)
            message_ids = list(range(current_id, end_id + 1))
            batch_files = []
            try:
                await background_turn("bulk_index_telegram_read")
                messages = await telegram_call(
                    lambda: client.get_messages(chat_id, message_ids),
                    route="bulk_index_messages",
                    policy=BACKGROUND_RETRY,
                    retry_safe=True,
                    idempotency_key=f"index-read:{chat_id}:{current_id}:{end_id}",
                )
                if not isinstance(messages, list):
                    messages = [messages]
                for msg in messages:
                    if not msg or getattr(msg, "empty", False):
                        continue
                    media = msg.document or msg.video or msg.audio
                    raw_name = getattr(media, "file_name", "") if media else ""
                    if not media or not getattr(media, "file_id", "") or not raw_name:
                        continue
                    batch_files.append({
                        "file_id": media.file_id,
                        "file_unique_id": getattr(media, "file_unique_id", ""),
                        "file_name": normalize_file_name(raw_name),
                        "file_size": getattr(media, "file_size", 0),
                        "mime_type": getattr(media, "mime_type", ""),
                        "source_chat_id": chat_id,
                        "source_message_id": getattr(msg, "id", None),
                        "indexed_at": time.time(),
                    })
            except Exception as error:
                reference = report_internal_error(
                    logger, "bulk_indexer_read", error, channel_id=chat_id
                )
                await _record_failed_range(
                    chat_id, current_id, end_id, "telegram_read", error, 1
                )
                await db.release_index_task(chat_id, lock_token, "paused")
                await status_message.edit_text(
                    f"❌ **Indexer paused safely**\n\n"
                    f"Telegram could not read messages `{current_id:,}-{end_id:,}`.\n"
                    f"Nothing in this range was marked complete.\n"
                    f"Check the bot's channel access, then tap Start again.\n\n"
                    f"Reference: `{reference}`"
                )
                return

            new_saves = dups = 0
            try:
                if batch_files:
                    await background_turn("bulk_index_database_write")
                    new_saves, dups = await retry_with_backoff(
                        lambda: db.save_files_bulk(batch_files),
                        attempts=4,
                        base_delay=0.75,
                        max_delay=6.0,
                        jitter=0.5,
                        should_retry=lambda exc: not isinstance(
                            exc, AllClustersFullError
                        ),
                        on_retry=note_retry,
                    )
            except AllClustersFullError as error:
                progress.duplicates += error.duplicates
                await _record_failed_range(
                    chat_id, current_id, end_id, "cluster_capacity", error, 1
                )
                await db.release_index_task(chat_id, lock_token, "paused")
                next_slot = len(db.file_cols) + 1
                await status_message.edit_text(
                    f"🛑 **Indexer paused • Database full**\n\n"
                    f"`{error.unsaved_count}` files in messages "
                    f"`{current_id:,}-{end_id:,}` were not saved.\n"
                    f"The checkpoint was not moved. Add `DATABASE_URI_{next_slot}`, "
                    f"restart, then tap Start to retry safely."
                )
                return
            except Exception as error:
                await _record_failed_range(
                    chat_id, current_id, end_id, "batch_save", error, 4
                )
                await db.release_index_task(chat_id, lock_token, "paused")
                await status_message.edit_text(
                    f"❌ **Indexer paused safely**\n\n"
                    f"Database saving failed after retries for messages "
                    f"`{current_id:,}-{end_id:,}`.\n"
                    f"The checkpoint was **not advanced**, so Start will retry this range."
                )
                return

            try:
                await background_turn("bulk_index_checkpoint")
                await retry_with_backoff(
                    lambda: db.set_index_progress(chat_id, end_id),
                    attempts=3,
                    base_delay=0.5,
                    max_delay=3.0,
                    jitter=0.25,
                    on_retry=note_retry,
                )
            except Exception as error:
                await _record_failed_range(
                    chat_id, current_id, end_id, "checkpoint", error, 3
                )
                await db.release_index_task(chat_id, lock_token, "paused")
                await status_message.edit_text(
                    f"❌ **Checkpoint not confirmed**\n\n"
                    f"Messages `{current_id:,}-{end_id:,}` may already be saved. "
                    f"The worker stopped so this range can be replayed safely."
                )
                return

            try:
                await db.resolve_index_failure(chat_id, current_id, end_id)
            except Exception as error:
                logger.warning(
                    "Could not clear resolved index failure %s-%s: %s",
                    current_id, end_id, type(error).__name__,
                )

            progress.record_batch(
                end_id=end_id,
                media=len(batch_files),
                saved=new_saves,
                duplicates=dups,
                skipped=len(message_ids) - len(batch_files),
            )
            current_id = end_id + 1
            if time.monotonic() >= next_ui_update or current_id > last_msg_id:
                await _edit_live_status(chat_id, "running")
                next_ui_update = time.monotonic() + 3.0
            await asyncio.sleep(0.35)

        await _edit_live_status(chat_id, "complete")
        await db.release_index_task(chat_id, lock_token, "complete")
        total_in_db = await db.get_total_files()
        _spawn_index_log(
            send_smart_log(
                client,
                f"📦 **#IndexComplete**\n\n"
                f"📺 **Channel:** `{chat_id}`\n"
                f"✅ **Files saved:** `{progress.saved:,}`\n"
                f"♻️ **Already indexed:** `{progress.duplicates:,}`\n"
                f"⏭ **Skipped:** `{progress.skipped:,}`\n"
                f"📁 **Library total:** `{total_in_db:,}`",
            ),
            chat_id,
            "complete",
        )
    except asyncio.CancelledError:
        await db.release_index_task(chat_id, lock_token, "queued")
        raise
    finally:
        _active_indexes.pop(chat_id, None)


async def run_indexer_worker(client: Client):
    """Claim resumable indexing jobs from MongoDB across worker replicas."""
    while True:
        task = await db.claim_index_task()
        if not task:
            await asyncio.sleep(1)
            continue
        chat_id = int(task["chat_id"])
        lock_token = str(task["lock_token"])
        try:
            status_message = await telegram_call(
                lambda: client.get_messages(
                    int(task["admin_chat_id"]), int(task["status_message_id"])
                ),
                route="indexer_status_lookup",
                policy=BACKGROUND_RETRY,
                retry_safe=True,
                idempotency_key=f"index-status:{chat_id}:{task['status_message_id']}",
            )
            if not status_message or getattr(status_message, "empty", False):
                status_message = await telegram_call(
                    lambda: client.send_message(
                        int(task["admin_chat_id"]),
                        f"⏳ Resuming index for `{chat_id}`…",
                    ),
                    route="indexer_status_recreate",
                    policy=BACKGROUND_RETRY,
                    retry_safe=False,
                )
            start_id = max(
                int(task.get("start_id", 1)),
                (await db.get_index_progress(chat_id)) + 1,
            )
            await run_indexer(
                client,
                status_message,
                chat_id,
                int(task["last_msg_id"]),
                start_id,
                lock_token,
            )
        except asyncio.CancelledError:
            await db.release_index_task(chat_id, lock_token, "queued")
            raise
        except Exception as error:
            reference = report_internal_error(
                logger, "indexer_worker", error, channel_id=chat_id
            )
            await db.release_index_task(chat_id, lock_token, "queued")
            logger.error("Indexer job requeued reference=%s", reference)


# --- UI HANDLERS & COMMANDS ---
@Client.on_message(filters.private & filters.forwarded & filters.user(ADMIN_ID))
async def forward_indexer(client: Client, message: Message):
    chat_id = None
    last_msg_id = None
    
    # Try all known Pyrogram attribute paths for forwarded channel messages.
    # Pyrogram 2.0+ uses forward_origin (MessageOriginChannel).
    # Older versions use forward_from_chat + forward_from_message_id.
    try:
        # Path 1: Pyrogram 2.0+ forward_origin
        if hasattr(message, "forward_origin") and message.forward_origin:
            origin = message.forward_origin
            # MessageOriginChannel has .chat and .message_id
            if hasattr(origin, "chat") and origin.chat:
                chat_id = origin.chat.id
            # Also try .sender_chat for some Pyrogram builds
            elif hasattr(origin, "sender_chat") and origin.sender_chat:
                chat_id = origin.sender_chat.id
            last_msg_id = getattr(origin, "message_id", None)

        # Path 2: Older Pyrogram — forward_from_chat
        if not chat_id and hasattr(message, "forward_from_chat") and message.forward_from_chat:
            chat_id = message.forward_from_chat.id
            last_msg_id = getattr(message, "forward_from_message_id", None)

        # Path 3: Fallback — message.id is the forwarded message ID
        if chat_id and not last_msg_id:
            last_msg_id = message.id
    except Exception as detect_err:
        report_internal_error(logger, "forward_detection", detect_err)

    if not chat_id or not last_msg_id:
        return await message.reply_text("⚠️ Could not detect the channel ID. Please forward directly from a channel.", reply_parameters=None)

    # --- PRE-FLIGHT CHECK ---
    try:
        # Ask Telegram if we have access to this channel BEFORE showing the menu
        await client.get_chat(chat_id)
    except Exception as error:
        reference = report_internal_error(
            logger, "bulk_index_preflight", error, channel_id=chat_id
        )
        return await message.reply_text(
            f"❌ **Indexing Blocked!**\n\n"
            f"I cannot read messages from `{chat_id}`.\n"
            f"**Reference:** `{reference}`\n\n"
            f"⚠️ **Fix:** Make sure you add me as an **Administrator** to the channel first!",
            reply_parameters=None
        )
    # ------------------------

    saved_progress = await db.get_index_progress(chat_id)
    next_message = saved_progress + 1 if saved_progress > 0 else 1
    resume_text = (
        f"🔄 **Next message:** `{next_message:,}`\n"
        if saved_progress > 0 else ""
    )

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Start Super-Index", callback_data=f"bulkindex#{chat_id}#{last_msg_id}")],
        [InlineKeyboardButton("🔄 Reset & Start Fresh", callback_data=f"resetidx#{chat_id}#{last_msg_id}")],
        [InlineKeyboardButton("❌ Cancel", callback_data="close_data")]
    ])

    await message.reply_text(
        f"⚡ **Fast Indexer Ready**\n\n"
        f"📦 **Channel:** `{chat_id}`\n"
        f"🎯 **Last message:** `{last_msg_id:,}`\n"
        f"{resume_text}\n"
        f"Progress is saved after every batch. You can pause, restart or stop safely.",
        reply_markup=markup,
        reply_parameters=None
    )
    raise StopPropagation  # prevent forwarded message leaking into auto_filter

@Client.on_callback_query(filters.regex(r"^bulkindex#") & filters.user(ADMIN_ID))
async def start_bulk_index(client: Client, callback: CallbackQuery):
    try:
        _, chat_id_str, last_msg_id_str = callback.data.split("#")
        chat_id = int(chat_id_str)
        last_msg_id = int(last_msg_id_str)
    except (ValueError, IndexError):
        return await answer_callback_safely(
            callback, "❌ Malformed callback data.", show_alert=True
        )

    await answer_callback_safely(callback, "⏳ Starting indexer…")

    # Fetch state from MongoDB instead of the deleted RAM dictionary
    state = await db.get_index_task(chat_id)
    if state == "running":
        return await callback.message.edit_text(
            "⚠️ **This channel is already being indexed.**"
        )

    saved_progress = await db.get_index_progress(chat_id)
    start_id = max(1, saved_progress + 1)
    if start_id > last_msg_id:
        return await callback.message.edit_text(
            "✅ **Nothing new to index.**\n\n"
            f"The saved checkpoint is already at message `{saved_progress:,}`."
        )

    status_msg = await callback.message.edit_text("⏳ **Spinning up background worker...**")
    
    # --- FEATURE 4: INDEXING REQUESTS LOG ---
    log_text = (
        "🚀 <b>#IndexRequest</b>\n\n"
        f"👤 <b>Admin:</b> {html_user_mention(callback.from_user)}\n"
        f"📦 <b>Target Channel:</b> <code>{chat_id}</code>\n"
        f"🎯 <b>Total Messages:</b> <code>{last_msg_id}</code>\n"
        f"🔄 <b>Starting From:</b> <code>{start_id}</code>"
    )
    _spawn_index_log(
        send_smart_log(client, log_text, parse_mode=ParseMode.HTML),
        chat_id,
        "requested",
    )
    # ----------------------------------------
    
    await db.enqueue_index_task(
        chat_id,
        last_msg_id,
        start_id,
        callback.message.chat.id,
        status_msg.id,
        callback.from_user.id,
    )
    await status_msg.edit_text(
        "✅ **Indexer queued**\n\nThe dedicated indexer worker will begin shortly."
    )


@Client.on_callback_query(filters.regex(r"^resetidx#") & filters.user(ADMIN_ID))
async def reset_and_index(client: Client, callback: CallbackQuery):
    """Clears saved progress then starts indexing from message 1."""
    try:
        _, chat_id_str, last_msg_id_str = callback.data.split("#")
        chat_id = int(chat_id_str)
        last_msg_id = int(last_msg_id_str)
    except (ValueError, IndexError):
        return await answer_callback_safely(
            callback, "❌ Malformed callback.", show_alert=True
        )

    await answer_callback_safely(callback, "🔄 Resetting progress…")
    await db.clear_index_progress(chat_id)
    status_msg = await callback.message.edit_text("⏳ **Progress reset. Starting from message 1...**")
    await db.enqueue_index_task(
        chat_id,
        last_msg_id,
        1,
        callback.message.chat.id,
        status_msg.id,
        callback.from_user.id,
    )
    await status_msg.edit_text(
        "✅ **Fresh index queued**\n\nThe dedicated indexer worker will begin shortly."
    )
    _spawn_index_log(send_smart_log(client,
        f"🔄 **#IndexReset**\n\n📦 Channel: `{chat_id}`\n"
        f"🎯 Total: `{last_msg_id}`\nStarting fresh from message 1."
    ), chat_id, "reset")


# --- CONTROL BUTTON CALLBACKS ---
@Client.on_callback_query(filters.regex(r"^pause_idx#") & filters.user(ADMIN_ID))
async def pause_index(client: Client, callback: CallbackQuery):
    try:
        chat_id = int(callback.data.split("#")[1])
    except (ValueError, IndexError):
        return await answer_callback_safely(
            callback, "❌ Malformed callback.", show_alert=True
        )
    await answer_callback_safely(callback, "Indexer paused ⏸")
    await db.set_index_task(chat_id, "paused")
    await _edit_live_status(chat_id, "paused")

@Client.on_callback_query(filters.regex(r"^resume_idx#") & filters.user(ADMIN_ID))
async def resume_index(client: Client, callback: CallbackQuery):
    try:
        chat_id = int(callback.data.split("#")[1])
    except (ValueError, IndexError):
        return await answer_callback_safely(
            callback, "❌ Malformed callback.", show_alert=True
        )
    await answer_callback_safely(callback, "Indexer resumed ▶️")
    await db.set_index_task(chat_id, "queued")
    await _edit_live_status(chat_id, "queued")

@Client.on_callback_query(filters.regex(r"^stop_idx#") & filters.user(ADMIN_ID))
async def stop_index(client: Client, callback: CallbackQuery):
    try:
        chat_id = int(callback.data.split("#")[1])
    except (ValueError, IndexError):
        return await answer_callback_safely(
            callback, "❌ Malformed callback.", show_alert=True
        )
    await answer_callback_safely(
        callback, "Stopping after the current batch…", show_alert=True
    )
    await db.set_index_task(chat_id, "stopped")
    await _edit_live_status(chat_id, "stopping")

# No close_data handler in this file — the canonical one lives in admin.py.
# Having it here too caused Pyrogram to register it twice, leading to
# unpredictable double-fire behavior.
