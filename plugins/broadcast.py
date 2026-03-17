import os
import re as _re
import asyncio
import logging
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, InputUserDeactivated, UserIsBlocked
from database.db import db

load_dotenv()

logger = logging.getLogger(__name__)
ADMIN_ID = [int(x.strip()) for x in os.getenv("ADMIN_ID", "0").split(",") if x.strip()]

# B2: Stores pending broadcast params while admin reviews the preview
_pending_broadcasts = {}


async def _auto_delete_broadcast(msg, delay=86400):
    await asyncio.sleep(delay)
    try:
        await msg.delete()
    except Exception:
        pass


async def _run_broadcast(client, message, do_pin, do_del, status_msg, target="users"):
    sent_users = failed_users = blocked_users = skipped_banned = 0
    sent_groups = failed_groups = 0

    if target in ("users", "both"):
        users = await db.get_all_users()
        for user_id in users:
            if await db.is_banned(user_id):
                skipped_banned += 1
                continue
            try:
                b_msg = await message.reply_to_message.copy(chat_id=user_id)
                sent_users += 1
                if do_pin:
                    try:
                        await b_msg.pin(both_sides=True, disable_notification=True)
                    except Exception:
                        pass
                if do_del:
                    asyncio.create_task(_auto_delete_broadcast(b_msg, 86400))
            except FloodWait as e:
                await asyncio.sleep(e.value)
                try:
                    b_msg = await message.reply_to_message.copy(chat_id=user_id)
                    sent_users += 1
                    if do_del:
                        asyncio.create_task(_auto_delete_broadcast(b_msg, 86400))
                except Exception:
                    failed_users += 1
            except (InputUserDeactivated, UserIsBlocked):
                blocked_users += 1
                await db.delete_user(user_id)
            except Exception:
                failed_users += 1
            await asyncio.sleep(0.05)

    if target in ("groups", "both"):
        groups = await db.get_all_groups()
        for group in groups:
            if group.get("banned"):
                continue
            try:
                await message.reply_to_message.copy(chat_id=group["_id"])
                sent_groups += 1
            except FloodWait as e:
                await asyncio.sleep(e.value)
                try:
                    await message.reply_to_message.copy(chat_id=group["_id"])
                    sent_groups += 1
                except Exception:
                    failed_groups += 1
            except Exception:
                failed_groups += 1
            await asyncio.sleep(0.1)

    lines = ["✅ **Broadcast Complete!**\n"]
    if target in ("users", "both"):
        lines += [
            f"👥 **Users Sent:** `{sent_users}`",
            f"🚫 **Blocked/Deleted:** `{blocked_users}` _(Removed from DB)_",
            f"⛔ **Skipped (Banned):** `{skipped_banned}`",
            f"❌ **Failed:** `{failed_users}`",
        ]
    if target in ("groups", "both"):
        lines += [
            f"🏘 **Groups Sent:** `{sent_groups}`",
            f"❌ **Groups Failed:** `{failed_groups}`",
        ]
    lines += [
        f"📌 **Pinned:** `{'Yes' if do_pin else 'No'}`",
        f"🗑️ **Auto-Delete (24h):** `{'Yes' if do_del else 'No'}`",
    ]
    await status_msg.edit_text("\n".join(lines))


@Client.on_callback_query(filters.regex(r"^bc_confirm$") & filters.user(ADMIN_ID))
async def bc_confirm(client: Client, callback: CallbackQuery):
    pending = _pending_broadcasts.pop(callback.message.chat.id, None)
    if not pending:
        await callback.answer("⚠️ Broadcast expired. Run /broadcast again.", show_alert=True)
        return
    await callback.message.edit_reply_markup(None)
    await callback.answer("✅ Broadcast started!")
    message = pending["message"]
    delay_seconds = pending["delay_seconds"]
    target = pending["target"]
    status_msg = await callback.message.reply_text("⏳ **Broadcast starting...**")
    if delay_seconds > 0:
        time_str = f"{delay_seconds // 3600}h" if delay_seconds >= 3600 else f"{delay_seconds // 60}m"
        await status_msg.edit_text(
            f"⏰ **Broadcast Scheduled!**\n\nWill send in **{time_str}**.\n"
            f"⚠️ Will cancel if bot restarts."
        )
        async def _scheduled():
            await asyncio.sleep(delay_seconds)
            await status_msg.edit_text("⏳ **Scheduled broadcast starting now...**")
            await _run_broadcast(client, message, pending["do_pin"], pending["do_del"], status_msg, target=target)
        asyncio.create_task(_scheduled())
    else:
        await _run_broadcast(client, message, pending["do_pin"], pending["do_del"], status_msg, target=target)


@Client.on_callback_query(filters.regex(r"^bc_cancel$") & filters.user(ADMIN_ID))
async def bc_cancel(client: Client, callback: CallbackQuery):
    _pending_broadcasts.pop(callback.message.chat.id, None)
    await callback.message.edit_text("❌ **Broadcast cancelled.**")
    await callback.answer()


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
            quote=True
        )

    flags = message.text.lower()
    do_pin = "-pin" in flags
    do_del = "-del" in flags

    schedule_match = _re.search(r'-schedule\s+(\d+)([hm])', flags)
    delay_seconds = 0
    if schedule_match:
        amount = int(schedule_match.group(1))
        unit = schedule_match.group(2)
        delay_seconds = amount * 3600 if unit == 'h' else amount * 60

    do_groups = "-groups" in flags
    do_users_flag = "-users" in flags
    if do_groups and do_users_flag:
        target = "both"
    elif do_groups:
        target = "groups"
    else:
        target = "users"

    user_count = len(await db.get_all_users()) if target in ("users", "both") else 0
    group_count = await db.get_group_count() if target in ("groups", "both") else 0
    total = user_count + group_count

    est_seconds = total * 0.05
    est_str = f"{int(est_seconds)}s" if est_seconds < 60 else f"{int(est_seconds // 60)}m {int(est_seconds % 60)}s"

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
        "\nThis is what recipients will receive 👆"
    ]

    await message.reply_to_message.copy(chat_id=message.chat.id)

    confirm_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Confirm & Send", callback_data="bc_confirm"),
         InlineKeyboardButton("❌ Cancel", callback_data="bc_cancel")]
    ])
    await message.reply_text("\n".join(preview_lines), reply_markup=confirm_markup, quote=True)

    _pending_broadcasts[message.chat.id] = {
        "message": message,
        "do_pin": do_pin,
        "do_del": do_del,
        "delay_seconds": delay_seconds,
        "target": target,
    }
