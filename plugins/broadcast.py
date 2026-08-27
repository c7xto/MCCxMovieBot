import os
import re as _re
import asyncio
import logging
import time
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import InputUserDeactivated, UserIsBlocked
from database.db import db
from utils import ADMIN_ID
from plugins.callbacks import answer_callback_safely
from plugins.telegram_retry import BROADCAST_RETRY, telegram_call

load_dotenv()

logger = logging.getLogger(__name__)

# Stores pending broadcast params while admin reviews the preview
_pending_broadcasts = {}


async def _auto_delete_broadcast(msg, delay=86400):
    await db.schedule_deletion(msg.chat.id, msg.id, delay)


_BROADCAST_COUNTER_FIELDS = {
    "sent_user": "sent_users",
    "failed_user": "failed_users",
    "blocked_user": "blocked_users",
    "skipped_banned": "skipped_banned",
    "sent_group": "sent_groups",
    "failed_group": "failed_groups",
}


async def _checkpoint(job, audience, recipient_id, outcome):
    saved = await db.checkpoint_broadcast(job["_id"], job["lock_token"], audience, recipient_id, outcome)
    if not saved:
        raise RuntimeError("broadcast lease was lost while checkpointing")
    field = _BROADCAST_COUNTER_FIELDS[outcome]
    job[field] = int(job.get(field, 0)) + 1


async def _copy_broadcast(client, job, recipient_id, audience):
    return await telegram_call(
        lambda: client.copy_message(
            chat_id=recipient_id,
            from_chat_id=job["source_chat_id"],
            message_id=job["source_message_id"],
        ),
        route=f"broadcast_{audience}",
        policy=BROADCAST_RETRY,
        retry_safe=True,
        idempotency_key=f"broadcast:{job['_id']}:{audience}:{recipient_id}",
    )


def _broadcast_result_text(job):
    lines = ["✅ **Broadcast Complete!**\n"]
    if job["target"] in ("users", "both"):
        lines += [
            f"👥 **Users Sent:** `{job.get('sent_users', 0)}`",
            f"🚫 **Blocked/Deleted:** `{job.get('blocked_users', 0)}` • Removed from database",
            f"⛔ **Skipped (Banned):** `{job.get('skipped_banned', 0)}`",
            f"❌ **Failed:** `{job.get('failed_users', 0)}`",
        ]
    if job["target"] in ("groups", "both"):
        lines += [
            f"🏘 **Groups Sent:** `{job.get('sent_groups', 0)}`",
            f"❌ **Groups Failed:** `{job.get('failed_groups', 0)}`",
        ]
    lines += [
        f"📌 **Pinned:** `{'Yes' if job['do_pin'] else 'No'}`",
        f"🗑️ **Auto-Delete (24h):** `{'Yes' if job['do_delete'] else 'No'}`",
    ]
    return "\n".join(lines)


async def _run_broadcast_job(client, job):
    banned_users = set()
    if job["target"] in ("users", "both") and not job.get("users_done"):
        banned_users = set(await db.get_banned_users())
        async for user_id in db.iter_user_ids_after(job.get("user_cursor")):
            if user_id in banned_users:
                await _checkpoint(job, "user", user_id, "skipped_banned")
                continue
            try:
                broadcast_message = await _copy_broadcast(client, job, user_id, "user")
                if job["do_pin"]:
                    try:
                        await broadcast_message.pin(both_sides=True, disable_notification=True)
                    except Exception:
                        pass
                if job["do_delete"]:
                    await _auto_delete_broadcast(broadcast_message, 86400)
                outcome = "sent_user"
            except (InputUserDeactivated, UserIsBlocked):
                outcome = "blocked_user"
                await db.delete_user(user_id)
            except Exception:
                outcome = "failed_user"
            await _checkpoint(job, "user", user_id, outcome)
            await asyncio.sleep(0.05)
        if not await db.complete_broadcast_phase(job["_id"], job["lock_token"], "users"):
            raise RuntimeError("broadcast lease was lost after user phase")
        job["users_done"] = True

    if job["target"] in ("groups", "both") and not job.get("groups_done"):
        async for group_id in db.iter_broadcast_groups_after(job.get("group_cursor")):
            try:
                await _copy_broadcast(client, job, group_id, "group")
                outcome = "sent_group"
            except Exception:
                outcome = "failed_group"
            await _checkpoint(job, "group", group_id, outcome)
            await asyncio.sleep(0.1)
        if not await db.complete_broadcast_phase(job["_id"], job["lock_token"], "groups"):
            raise RuntimeError("broadcast lease was lost after group phase")
        job["groups_done"] = True

    if not await db.complete_broadcast(job["_id"], job["lock_token"]):
        raise RuntimeError("broadcast lease was lost at completion")
    try:
        await client.edit_message_text(
            job["admin_chat_id"],
            job["status_message_id"],
            _broadcast_result_text(job),
        )
    except Exception:
        logger.warning("Could not edit completed broadcast status message")


async def run_broadcast_worker(client):
    """Resume due broadcast jobs from durable recipient checkpoints."""
    while True:
        job = await db.claim_due_broadcast()
        if not job:
            await asyncio.sleep(5)
            continue
        try:
            await _run_broadcast_job(client, job)
        except Exception as error:
            attempts = int(job.get("attempts", 0)) + 1
            if attempts >= 8:
                await db.fail_broadcast(job["_id"], job["lock_token"], error)
                logger.error(
                    "Broadcast failed permanently job=%s error_type=%s",
                    job.get("_id"),
                    type(error).__name__,
                )
            else:
                delay = min(3600, 30 * (2 ** min(attempts - 1, 7)))
                await db.retry_broadcast(job["_id"], job["lock_token"], error, delay)


@Client.on_callback_query(filters.regex(r"^bc_confirm$") & filters.user(ADMIN_ID))
async def bc_confirm(client: Client, callback: CallbackQuery):
    pending = _pending_broadcasts.pop(callback.message.chat.id, None)
    if not pending:
        await answer_callback_safely(callback, "⚠️ Broadcast expired. Run /broadcast again.", show_alert=True)
        return
    await callback.message.edit_reply_markup(None)
    await answer_callback_safely(callback, "✅ Broadcast queued!")
    delay_seconds = pending["delay_seconds"]
    status_msg = await callback.message.reply_text("⏳ **Saving broadcast job...**")
    job_id = await db.enqueue_broadcast(
        source_chat_id=pending["source_chat_id"],
        source_message_id=pending["source_message_id"],
        admin_chat_id=status_msg.chat.id,
        status_message_id=status_msg.id,
        created_by=callback.from_user.id,
        due_at=time.time() + delay_seconds,
        target=pending["target"],
        do_pin=pending["do_pin"],
        do_delete=pending["do_del"],
    )
    if job_id is None:
        await status_msg.edit_text(
            "⚠️ **Broadcast was not queued.** The durable queue is unavailable or full."
        )
        return
    if delay_seconds > 0:
        time_str = f"{delay_seconds // 3600}h" if delay_seconds >= 3600 else f"{delay_seconds // 60}m"
        await status_msg.edit_text(
            f"⏰ **Broadcast Scheduled!**\n\nWill send in **{time_str}**.\n"
            f"Job: `{job_id}` — it will resume after a restart."
        )
    else:
        await status_msg.edit_text(f"⏳ **Broadcast queued and starting shortly.**\nJob: `{job_id}`")


@Client.on_callback_query(filters.regex(r"^bc_cancel$") & filters.user(ADMIN_ID))
async def bc_cancel(client: Client, callback: CallbackQuery):
    _pending_broadcasts.pop(callback.message.chat.id, None)
    await callback.message.edit_text("❌ **Broadcast cancelled.**")
    await answer_callback_safely(callback)


@Client.on_message(filters.command("broadcast") & filters.private & filters.user(ADMIN_ID))
async def broadcast_handler(client: Client, message: Message):
    if not message.reply_to_message:
        return await message.reply_text(
            "⚠️ **Usage:** Reply to any message with `/broadcast`\n\n"
            "**Flags:**\n"
            "├ `-pin`           : Pin for each user\n"
            "├ `-del`           : Auto-delete after 24h\n"
            "├ `-users`         : Send to users (default)\n"
            "├ `-groups`        : Send to connected groups\n"
            "├ `-users -groups` : Send to both\n"
            "└ `-schedule Xh/Xm`: Delay broadcast\n\n"
            "**Examples:**\n"
            "`/broadcast -pin -del`\n"
            "`/broadcast -groups`\n"
            "`/broadcast -users -groups -schedule 2h`",
            reply_parameters=None,
        )

    flags = message.text.lower()
    do_pin = "-pin" in flags
    do_del = "-del" in flags

    schedule_match = _re.search(r"-schedule\s+(\d+)([hm])", flags)
    delay_seconds = 0
    if schedule_match:
        amount = int(schedule_match.group(1))
        unit = schedule_match.group(2)
        delay_seconds = amount * 3600 if unit == "h" else amount * 60

    do_groups = "-groups" in flags
    do_users_flag = "-users" in flags
    if do_groups and do_users_flag:
        target = "both"
    elif do_groups:
        target = "groups"
    else:
        target = "users"

    user_count = await db.get_user_count() if target in ("users", "both") else 0
    group_count = await db.get_group_count() if target in ("groups", "both") else 0
    total = user_count + group_count

    est_seconds = total * 0.05
    est_str = (
        f"{int(est_seconds)}s" if est_seconds < 60 else f"{int(est_seconds // 60)}m {int(est_seconds % 60)}s"
    )

    if delay_seconds >= 3600:
        sched_str = f"⏰ Scheduled in: `{delay_seconds // 3600}h`"
    elif delay_seconds > 0:
        sched_str = f"⏰ Scheduled in: `{delay_seconds // 60}m`"
    else:
        sched_str = "📤 Send: Immediately"

    preview_lines = ["📢 **Broadcast Preview**\n"]
    if target in ("users", "both"):
        preview_lines.append(f"👥 Users: `{user_count:,}`")
    if target in ("groups", "both"):
        preview_lines.append(f"🏘 Groups: `{group_count:,}`")
    preview_lines += [
        f"📊 Total: `{total:,}`",
        f"⏱ Est. time: `{est_str}`",
        sched_str,
        f"📌 Pin: `{'Yes' if do_pin else 'No'}`",
        f"🗑 Auto-delete 24h: `{'Yes' if do_del else 'No'}`",
        "\nThis is what recipients will receive 👆",
    ]

    await message.reply_to_message.copy(chat_id=message.chat.id)

    confirm_markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Confirm & Send", callback_data="bc_confirm"),
                InlineKeyboardButton("❌ Cancel", callback_data="bc_cancel"),
            ]
        ]
    )
    await message.reply_text("\n".join(preview_lines), reply_markup=confirm_markup, reply_parameters=None)

    _pending_broadcasts[message.chat.id] = {
        "source_chat_id": message.reply_to_message.chat.id,
        "source_message_id": message.reply_to_message.id,
        "do_pin": do_pin,
        "do_del": do_del,
        "delay_seconds": delay_seconds,
        "target": target,
    }
