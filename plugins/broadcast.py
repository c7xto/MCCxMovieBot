import asyncio
import logging
import re as _re
import time
from collections import deque

from pyrogram import Client, filters
from pyrogram.errors import InputUserDeactivated, MessageNotModified, UserIsBlocked
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from database.db import db
from plugins.callbacks import answer_callback_safely
from plugins.telegram_retry import BACKGROUND_RETRY, BROADCAST_RETRY, INTERACTIVE_RETRY, telegram_call
from utils import ADMIN_ID


logger = logging.getLogger(__name__)

# Preview state is intentionally short-lived. Confirmed jobs are persisted in MongoDB.
_pending_broadcasts = {}
_PROGRESS_EDIT_SECONDS = 5.0
_CONTROL_CHECK_SECONDS = 2.0

_BROADCAST_COUNTER_FIELDS = {
    "sent_user": "sent_users",
    "failed_user": "failed_users",
    "blocked_user": "blocked_users",
    "skipped_banned": "skipped_banned",
    "sent_group": "sent_groups",
    "failed_group": "failed_groups",
}

_STATUS_LABELS = {
    "pending": ("⏳", "Queued"),
    "running": ("📣", "Running"),
    "paused": ("⏸", "Paused"),
    "stopped": ("⏹", "Stopped"),
    "completed": ("✅", "Complete"),
    "failed": ("⚠️", "Failed"),
}


def _format_duration(seconds):
    if seconds is None:
        return "Calculating…"
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _audience_label(target):
    return {"users": "Users", "groups": "Groups", "both": "Users + Groups"}.get(
        target, "Unknown"
    )


def _processed_count(job):
    return sum(int(job.get(field, 0) or 0) for field in _BROADCAST_COUNTER_FIELDS.values())


def _total_count(job):
    stored = int(job.get("total_recipients", 0) or 0)
    if stored:
        return stored
    split_total = int(job.get("total_users", 0) or 0) + int(job.get("total_groups", 0) or 0)
    if split_total:
        return split_total
    # Old completed jobs predate persisted totals. Their processed count is authoritative.
    return _processed_count(job) if job.get("status") in {"completed", "stopped"} else 0


def _job_speed(job, now=None):
    runtime_speed = float(job.get("_runtime_speed", 0.0) or 0.0)
    if runtime_speed > 0:
        return runtime_speed
    started_at = job.get("started_at")
    if not started_at:
        return 0.0
    end_at = job.get("finished_at") or now or time.time()
    elapsed = max(0.0, float(end_at) - float(started_at))
    return _processed_count(job) / elapsed if elapsed > 0 else 0.0


def _broadcast_status_text(job, now=None):
    now = time.time() if now is None else now
    status = job.get("status", "pending")
    icon, label = _STATUS_LABELS.get(status, ("📣", status.title()))
    processed = _processed_count(job)
    total = _total_count(job)
    percent = min(100.0, processed * 100 / total) if total else 0.0
    if status == "completed":
        percent = 100.0
    filled = min(10, int(percent / 10))
    progress_bar = "▰" * filled + "▱" * (10 - filled)
    delivered = int(job.get("sent_users", 0) or 0) + int(job.get("sent_groups", 0) or 0)
    failed = int(job.get("failed_users", 0) or 0) + int(job.get("failed_groups", 0) or 0)
    blocked = int(job.get("blocked_users", 0) or 0)
    skipped = int(job.get("skipped_banned", 0) or 0)
    speed = _job_speed(job, now=now)
    started_at = job.get("started_at")
    elapsed = None if not started_at else (job.get("finished_at") or now) - float(started_at)
    remaining = max(0, total - processed) if total else 0
    eta = remaining / speed if status == "running" and total and speed > 0 else None
    total_text = f"{total:,}" if total else "Unknown"

    if status == "pending" and float(job.get("due_at", 0) or 0) > now + 1:
        label = "Scheduled"
    elif status == "running" and job.get("control_requested") == "pause":
        label = "Pausing"
    elif status == "running" and job.get("control_requested") == "stop":
        label = "Stopping"

    lines = [
        f"{icon} **Broadcast • {label}**",
        f"`{progress_bar}`  **{percent:.1f}%**" if total else f"Processed  **{processed:,}**",
        "",
        f"👥 Audience  **{_audience_label(job.get('target'))}**",
        f"📨 Progress  **{processed:,} / {total_text}**",
        f"✅ Delivered  **{delivered:,}**",
        f"🚫 Blocked  **{blocked:,}**",
        f"⛔ Skipped  **{skipped:,}**",
        f"❌ Failed  **{failed:,}**",
    ]
    if status == "running" and job.get("control_requested") not in {"pause", "stop"}:
        lines += [
            f"⚡ Speed  **{speed:.1f}/s**" if speed else "⚡ Speed  **Calculating…**",
            f"⏱ Elapsed  **{_format_duration(elapsed)}**",
            f"🕒 ETA  **{_format_duration(eta)}**",
        ]
    elif status == "pending":
        due_at = float(job.get("due_at", now) or now)
        lines.append(
            f"🕒 Starts in  **{_format_duration(due_at - now)}**"
            if due_at > now + 1
            else "🕒 Starting shortly"
        )
    elif status == "paused":
        lines.append("ℹ️ Resume continues from the last saved recipient.")
    elif status == "stopped":
        lines.append("ℹ️ Already delivered messages were kept.")
    elif status == "failed":
        error_type = str(job.get("last_error_type") or "Unknown error")[:80]
        lines.append(f"Reason  `{error_type}`")

    lines += [
        "",
        f"📌 Pin  **{'On' if job.get('do_pin') else 'Off'}**  •  "
        f"🗑 24h delete  **{'On' if job.get('do_delete') else 'Off'}**",
        f"Job  `{str(job.get('_id', 'pending'))}`",
    ]
    return "\n".join(lines)


def _broadcast_controls(job):
    status = job.get("status", "pending")
    job_id = str(job.get("_id", ""))
    rows = []
    if status == "running" and job.get("control_requested") not in {"pause", "stop"}:
        rows.append(
            [
                InlineKeyboardButton("⏸ Pause", callback_data=f"bc_pause#{job_id}"),
                InlineKeyboardButton("⏹ Safe Stop", callback_data=f"bc_stop#{job_id}"),
            ]
        )
    elif status == "paused":
        rows.append(
            [
                InlineKeyboardButton("▶️ Resume", callback_data=f"bc_resume#{job_id}"),
                InlineKeyboardButton("⏹ Safe Stop", callback_data=f"bc_stop#{job_id}"),
            ]
        )
    elif status == "pending":
        rows.append([InlineKeyboardButton("⏹ Safe Stop", callback_data=f"bc_stop#{job_id}")])
    rows.append(
        [
            InlineKeyboardButton("🔄 Refresh", callback_data=f"bc_status#{job_id}"),
            InlineKeyboardButton("📚 History", callback_data="bc_history"),
        ]
    )
    return InlineKeyboardMarkup(rows)


async def _edit_callback_message(callback, text, reply_markup=None):
    """Refresh an admin view without surfacing harmless no-change errors."""
    try:
        await callback.message.edit_text(text, reply_markup=reply_markup)
    except MessageNotModified:
        return


async def _edit_broadcast_status(client, job, *, force=False, policy=INTERACTIVE_RETRY):
    now_mono = time.monotonic()
    if not force and now_mono - float(job.get("_last_status_edit", 0.0)) < _PROGRESS_EDIT_SECONDS:
        return
    job["_last_status_edit"] = now_mono
    try:
        await telegram_call(
            lambda: client.edit_message_text(
                chat_id=job["admin_chat_id"],
                message_id=job["status_message_id"],
                text=_broadcast_status_text(job),
                reply_markup=_broadcast_controls(job),
            ),
            route="broadcast_status_edit",
            policy=policy,
            retry_safe=True,
            idempotency_key=f"broadcast-status:{job['_id']}:{_processed_count(job)}:{job.get('status')}",
        )
    except MessageNotModified:
        return
    except Exception as error:
        logger.warning(
            "Broadcast status update deferred job=%s error_type=%s",
            job.get("_id"),
            type(error).__name__,
        )


async def _auto_delete_broadcast(message, delay=86400):
    await db.schedule_deletion(message.chat.id, message.id, delay)


async def _apply_delivery_options(message, job, audience):
    if job.get("do_pin"):
        try:
            await message.pin(
                both_sides=audience == "user",
                disable_notification=True,
            )
        except Exception as error:
            logger.info(
                "Broadcast pin skipped audience=%s error_type=%s",
                audience,
                type(error).__name__,
            )
    if job.get("do_delete"):
        try:
            await _auto_delete_broadcast(message, 86400)
        except Exception as error:
            logger.warning(
                "Broadcast auto-delete scheduling deferred error_type=%s",
                type(error).__name__,
            )


def _record_progress_sample(job):
    samples = job.setdefault("_progress_samples", deque(maxlen=60))
    samples.append((time.monotonic(), _processed_count(job)))
    if len(samples) >= 2:
        elapsed = samples[-1][0] - samples[0][0]
        delta = samples[-1][1] - samples[0][1]
        if elapsed > 0 and delta > 0:
            job["_runtime_speed"] = delta / elapsed


async def _checkpoint(job, audience, recipient_id, outcome):
    saved = await db.checkpoint_broadcast(job["_id"], job["lock_token"], audience, recipient_id, outcome)
    if not saved:
        raise RuntimeError("broadcast lease was lost while checkpointing")
    field = _BROADCAST_COUNTER_FIELDS[outcome]
    job[field] = int(job.get(field, 0)) + 1
    _record_progress_sample(job)


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


async def _apply_control_if_requested(client, job, *, force=False):
    now_mono = time.monotonic()
    if not force and now_mono - float(job.get("_last_control_check", 0.0)) < _CONTROL_CHECK_SECONDS:
        return None
    job["_last_control_check"] = now_mono
    control = await db.get_broadcast_control(job["_id"], job["lock_token"])
    if control is None:
        raise RuntimeError("broadcast lease was lost while checking controls")
    action = control.get("control_requested")
    if action not in {"pause", "stop"}:
        return None
    if not await db.apply_broadcast_control(job["_id"], job["lock_token"], action):
        raise RuntimeError("broadcast control could not be applied")
    job["status"] = "paused" if action == "pause" else "stopped"
    if action == "pause":
        job["paused_at"] = time.time()
    else:
        job["finished_at"] = time.time()
    await _edit_broadcast_status(client, job, force=True)
    return action


async def _run_broadcast_job(client, job):
    job["status"] = "running"
    job.setdefault("started_at", time.time())
    job["_progress_samples"] = deque([(time.monotonic(), _processed_count(job))], maxlen=60)
    await _edit_broadcast_status(client, job, force=True)
    if await _apply_control_if_requested(client, job, force=True):
        return job["status"]

    if job["target"] in ("users", "both") and not job.get("users_done"):
        banned_users = set(await db.get_banned_users())
        async for user_id in db.iter_user_ids_after(job.get("user_cursor")):
            if user_id in banned_users:
                outcome = "skipped_banned"
            else:
                try:
                    broadcast_message = await _copy_broadcast(client, job, user_id, "user")
                    await _apply_delivery_options(broadcast_message, job, "user")
                    outcome = "sent_user"
                except (InputUserDeactivated, UserIsBlocked):
                    outcome = "blocked_user"
                    await db.delete_user(user_id)
                except Exception as error:
                    outcome = "failed_user"
                    logger.info("User broadcast failed error_type=%s", type(error).__name__)
            await _checkpoint(job, "user", user_id, outcome)
            await _edit_broadcast_status(client, job)
            if await _apply_control_if_requested(client, job):
                return job["status"]
            await asyncio.sleep(0.05)
        if not await db.complete_broadcast_phase(job["_id"], job["lock_token"], "users"):
            raise RuntimeError("broadcast lease was lost after user phase")
        job["users_done"] = True

    if await _apply_control_if_requested(client, job, force=True):
        return job["status"]

    if job["target"] in ("groups", "both") and not job.get("groups_done"):
        async for group_id in db.iter_broadcast_groups_after(job.get("group_cursor")):
            try:
                broadcast_message = await _copy_broadcast(client, job, group_id, "group")
                await _apply_delivery_options(broadcast_message, job, "group")
                outcome = "sent_group"
            except Exception as error:
                outcome = "failed_group"
                logger.info("Group broadcast failed error_type=%s", type(error).__name__)
            await _checkpoint(job, "group", group_id, outcome)
            await _edit_broadcast_status(client, job)
            if await _apply_control_if_requested(client, job):
                return job["status"]
            await asyncio.sleep(0.1)
        if not await db.complete_broadcast_phase(job["_id"], job["lock_token"], "groups"):
            raise RuntimeError("broadcast lease was lost after group phase")
        job["groups_done"] = True

    if not await db.complete_broadcast(job["_id"], job["lock_token"]):
        raise RuntimeError("broadcast lease was lost at completion")
    job["status"] = "completed"
    job["finished_at"] = time.time()
    await _edit_broadcast_status(client, job, force=True, policy=BACKGROUND_RETRY)
    return "completed"


async def run_broadcast_worker(client):
    """Resume due broadcast jobs from durable recipient checkpoints."""
    while True:
        job = await db.claim_due_broadcast()
        if not job:
            await asyncio.sleep(5)
            continue
        try:
            await _run_broadcast_job(client, job)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            attempts = int(job.get("attempts", 0)) + 1
            if attempts >= 8:
                await db.fail_broadcast(job["_id"], job["lock_token"], error)
                job["status"] = "failed"
                job["finished_at"] = time.time()
                job["last_error_type"] = type(error).__name__
                await _edit_broadcast_status(client, job, force=True, policy=BACKGROUND_RETRY)
                logger.error(
                    "Broadcast failed permanently job=%s error_type=%s",
                    job.get("_id"),
                    type(error).__name__,
                )
            else:
                delay = min(3600, 30 * (2 ** min(attempts - 1, 7)))
                await db.retry_broadcast(job["_id"], job["lock_token"], error, delay)
                job["status"] = "pending"
                job["due_at"] = time.time() + delay
                job["attempts"] = attempts
                await _edit_broadcast_status(client, job, force=True)


def _recent_broadcasts_text(jobs):
    if not jobs:
        return "📣 **Broadcast Center**\n\nNo broadcasts have been created yet."
    lines = ["📣 **Broadcast Center**", "", "Recent jobs:"]
    for job in jobs:
        icon, label = _STATUS_LABELS.get(job.get("status"), ("•", "Unknown"))
        processed = _processed_count(job)
        total = _total_count(job)
        total_text = f"/{total:,}" if total else ""
        lines.append(
            f"{icon} **{label}**  `{processed:,}{total_text}`  •  {_audience_label(job.get('target'))}"
        )
    lines += ["", "Use `/broadcast_status JOB_ID` for a direct lookup."]
    return "\n".join(lines)


def _recent_broadcasts_markup(jobs):
    rows = []
    for job in jobs:
        icon, label = _STATUS_LABELS.get(job.get("status"), ("•", "Unknown"))
        short_id = str(job.get("_id"))[-8:]
        rows.append(
            [
                InlineKeyboardButton(
                    f"{icon} {label} • {short_id}",
                    callback_data=f"bc_status#{job['_id']}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("‹ Preferences", callback_data="admin_cat_settings")])
    return InlineKeyboardMarkup(rows)


@Client.on_callback_query(filters.regex(r"^broadcast_jobs_menu$") & filters.user(ADMIN_ID))
async def broadcast_jobs_menu(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    jobs = await db.list_recent_broadcasts(8)
    await _edit_callback_message(
        callback,
        _recent_broadcasts_text(jobs),
        _recent_broadcasts_markup(jobs),
    )


@Client.on_callback_query(filters.regex(r"^bc_history$") & filters.user(ADMIN_ID))
async def bc_history(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    jobs = await db.list_recent_broadcasts(8)
    await _edit_callback_message(
        callback,
        _recent_broadcasts_text(jobs),
        _recent_broadcasts_markup(jobs),
    )


@Client.on_callback_query(filters.regex(r"^bc_status#") & filters.user(ADMIN_ID))
async def bc_status(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    job = await db.get_broadcast(callback.data.split("#", 1)[1])
    if not job:
        await _edit_callback_message(
            callback,
            "⚠️ **Broadcast not found.**",
            InlineKeyboardMarkup(
                [[InlineKeyboardButton("📚 History", callback_data="bc_history")]]
            ),
        )
        return
    await _edit_callback_message(
        callback,
        _broadcast_status_text(job),
        _broadcast_controls(job),
    )


@Client.on_callback_query(filters.regex(r"^bc_(pause|resume)#") & filters.user(ADMIN_ID))
async def bc_control(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback, "Updating broadcast…")
    action, job_id = callback.data[3:].split("#", 1)
    job = await db.request_broadcast_control(job_id, action)
    if not job:
        await callback.message.edit_text("⚠️ **Broadcast not found.**")
        return
    await _edit_callback_message(
        callback,
        _broadcast_status_text(job),
        _broadcast_controls(job),
    )


@Client.on_callback_query(filters.regex(r"^bc_stop#") & filters.user(ADMIN_ID))
async def bc_stop_prompt(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    job_id = callback.data.split("#", 1)[1]
    job = await db.get_broadcast(job_id)
    if not job:
        await callback.message.edit_text("⚠️ **Broadcast not found.**")
        return
    if job.get("status") not in {"pending", "running", "paused"}:
        await _edit_callback_message(
            callback,
            _broadcast_status_text(job),
            _broadcast_controls(job),
        )
        return
    await _edit_callback_message(
        callback,
        "⏹ **Safe Stop Broadcast?**\n\n"
        "Sending stops after the current recipient. Already delivered messages remain, "
        "and the saved progress is kept.",
        InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⏹ Confirm Safe Stop", callback_data=f"bc_stop_confirm#{job_id}")],
                [InlineKeyboardButton("‹ Keep Running", callback_data=f"bc_status#{job_id}")],
            ]
        ),
    )


@Client.on_callback_query(filters.regex(r"^bc_stop_confirm#") & filters.user(ADMIN_ID))
async def bc_stop_confirm(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback, "Requesting safe stop…")
    job_id = callback.data.split("#", 1)[1]
    job = await db.request_broadcast_control(job_id, "stop")
    if not job:
        await callback.message.edit_text("⚠️ **Broadcast not found.**")
        return
    await _edit_callback_message(
        callback,
        _broadcast_status_text(job),
        _broadcast_controls(job),
    )


@Client.on_callback_query(filters.regex(r"^bc_confirm$") & filters.user(ADMIN_ID))
async def bc_confirm(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback, "Saving broadcast…")
    pending = _pending_broadcasts.pop(callback.message.chat.id, None)
    if not pending:
        await callback.message.edit_text("⚠️ **Broadcast preview expired.** Run `/broadcast` again.")
        return
    await callback.message.edit_text("⏳ **Saving broadcast job…**")
    delay_seconds = pending["delay_seconds"]
    job_id = await db.enqueue_broadcast(
        source_chat_id=pending["source_chat_id"],
        source_message_id=pending["source_message_id"],
        admin_chat_id=callback.message.chat.id,
        status_message_id=callback.message.id,
        created_by=callback.from_user.id,
        due_at=time.time() + delay_seconds,
        target=pending["target"],
        do_pin=pending["do_pin"],
        do_delete=pending["do_del"],
        total_users=pending["user_count"],
        total_groups=pending["group_count"],
    )
    if job_id is None:
        await callback.message.edit_text(
            "⚠️ **Broadcast was not queued.** The durable queue is unavailable or full."
        )
        return
    job = {
        "_id": job_id,
        "admin_chat_id": callback.message.chat.id,
        "status_message_id": callback.message.id,
        "status": "pending",
        "due_at": time.time() + delay_seconds,
        "target": pending["target"],
        "do_pin": pending["do_pin"],
        "do_delete": pending["do_del"],
        "total_users": pending["user_count"],
        "total_groups": pending["group_count"],
        "total_recipients": pending["user_count"] + pending["group_count"],
    }
    await callback.message.edit_text(
        _broadcast_status_text(job),
        reply_markup=_broadcast_controls(job),
    )


@Client.on_callback_query(filters.regex(r"^bc_cancel$") & filters.user(ADMIN_ID))
async def bc_cancel(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback, "Broadcast cancelled")
    _pending_broadcasts.pop(callback.message.chat.id, None)
    await callback.message.edit_text("✕ **Broadcast cancelled.**")


@Client.on_message(filters.command("broadcast_status") & filters.private & filters.user(ADMIN_ID))
async def broadcast_status_handler(client: Client, message: Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) == 2:
        job = await db.get_broadcast(parts[1].strip())
        if not job:
            await message.reply_text("⚠️ **Broadcast not found.**", reply_parameters=None)
            return
        await message.reply_text(
            _broadcast_status_text(job),
            reply_markup=_broadcast_controls(job),
            reply_parameters=None,
        )
        return
    jobs = await db.list_recent_broadcasts(8)
    await message.reply_text(
        _recent_broadcasts_text(jobs),
        reply_markup=_recent_broadcasts_markup(jobs),
        reply_parameters=None,
    )


@Client.on_message(filters.command("broadcast") & filters.private & filters.user(ADMIN_ID))
async def broadcast_handler(client: Client, message: Message):
    if not message.reply_to_message:
        await message.reply_text(
            "📣 **Broadcast**\n\n"
            "Reply to the message you want to send, then use `/broadcast`.\n\n"
            "**Options**\n"
            "`-pin`  Pin delivered messages\n"
            "`-del`  Delete delivered messages after 24 hours\n"
            "`-groups`  Send to connected groups\n"
            "`-users -groups`  Send to users and groups\n"
            "`-schedule 2h` or `-schedule 30m`  Send later\n\n"
            "Use `/broadcast_status` to view active and recent jobs.",
            reply_parameters=None,
        )
        return

    flags = message.text.lower()
    do_pin = "-pin" in flags
    do_del = "-del" in flags
    schedule_match = _re.search(r"-schedule\s+(\d+)([hm])", flags)
    delay_seconds = 0
    if schedule_match:
        amount = int(schedule_match.group(1))
        delay_seconds = amount * 3600 if schedule_match.group(2) == "h" else amount * 60

    do_groups = "-groups" in flags
    do_users_flag = "-users" in flags
    target = "both" if do_groups and do_users_flag else ("groups" if do_groups else "users")

    user_count = await db.get_user_count() if target in ("users", "both") else 0
    group_count = await db.get_broadcast_group_count() if target in ("groups", "both") else 0
    total = user_count + group_count

    send_timing = f"In {_format_duration(delay_seconds)}" if delay_seconds > 0 else "Immediately"
    preview_lines = [
        "📣 **Broadcast Preview**",
        "",
        f"👥 Audience  **{_audience_label(target)}**",
        f"📨 Recipients  **{total:,}**",
        f"🕒 Send  **{send_timing}**",
        f"📌 Pin  **{'On' if do_pin else 'Off'}**",
        f"🗑 24h delete  **{'On' if do_del else 'Off'}**",
        "",
        "The replied message will be copied exactly.",
        "ETA is calculated live after sending starts.",
    ]
    if delay_seconds:
        preview_lines.append("Keep the source message available until this job finishes.")

    confirm_markup = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("▶️ Start Broadcast", callback_data="bc_confirm")],
            [InlineKeyboardButton("✕ Cancel", callback_data="bc_cancel")],
        ]
    )
    await message.reply_text(
        "\n".join(preview_lines),
        reply_markup=confirm_markup,
        reply_parameters=None,
    )
    _pending_broadcasts[message.chat.id] = {
        "source_chat_id": message.reply_to_message.chat.id,
        "source_message_id": message.reply_to_message.id,
        "do_pin": do_pin,
        "do_del": do_del,
        "delay_seconds": delay_seconds,
        "target": target,
        "user_count": user_count,
        "group_count": group_count,
    }
