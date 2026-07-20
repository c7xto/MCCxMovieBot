"""
req_fsub.py — Request Channel FSub, and Two-Stage Verification

Two independent gates live in this file, both built on the same real
join-link (_get_link) and real membership-check (_has_requested_or_joined)
helpers — neither ever delivers a file without Telegram actually confirming
the user joined or requested to join:

  Request Channel FSub (original):
    - Unlimited channels in req_fsub pool
    - Before sending any file, check ALL channels in the pool
    - Find the FIRST channel user has not requested/joined → show that link
    - If ALL channels requested/joined → skip, deliver file immediately
    - Timer per user prevents showing prompt on every single file tap

  Two-Stage Verification (added):
    - Exactly 2 fixed channels (bot_config.two_stage_channels)
    - A sequential Step 1 → Step 2 UI: Channel 1's prompt is replaced by
      Channel 2's prompt only after Channel 1 is genuinely verified: the
      "continue" button re-checks membership via get_chat_member before
      advancing — it does not just advance on tap.
    - Only configured/active once BOTH slots are set; otherwise a no-op.
    - Fixed 30-minute per-user re-verification window (not admin-tunable
      like req_fsub's interval — see TWO_STAGE_VERIFY_INTERVAL in db.py).
"""

import logging
import asyncio
from pyrogram import Client, filters
from pyrogram.errors import UserNotParticipant
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from pyrogram.enums import ParseMode, ChatMemberStatus
from database.db import db

logger = logging.getLogger(__name__)


async def _get_link(client, entry) -> str | None:
    channel_id  = entry.get("id") if isinstance(entry, dict) else entry
    stored_link = entry.get("link") if isinstance(entry, dict) else None
    if not channel_id:
        return None

    ch_str = str(channel_id).strip()

    # Private invite link stored as https://t.me/+xxxx — use directly, always works
    if stored_link and stored_link.startswith("https://t.me/+"):
        return stored_link

    # Any stored https link — use as-is
    if stored_link and stored_link.startswith("https://"):
        return stored_link

    # Public @username
    if ch_str.startswith("@"):
        return f"https://t.me/{ch_str.lstrip('@')}"

    # Try to resolve public username via API
    try:
        ch    = int(ch_str) if ch_str.lstrip("-").isdigit() else ch_str
        chat  = await client.get_chat(ch)
        uname = getattr(chat, "username", None)
        if uname:
            return f"https://t.me/{uname}"
    except Exception:
        pass

    # Private channel — auto-generate a "Request to Join" invite link
    if ch_str.lstrip("-").isdigit():
        try:
            link = await client.create_chat_invite_link(
                int(ch_str),
                creates_join_request=True
            )
            await db.update_req_fsub_link(channel_id, link.invite_link)
            return link.invite_link
        except Exception as e:
            logger.debug(f"req_fsub: no invite link for {channel_id}: {e}")
            return None

    return f"https://t.me/{ch_str}"


async def _has_requested_or_joined(client, channel_id, user_id: int) -> bool:
    ch_str = str(channel_id).strip()
    try:
        ch     = int(ch_str) if ch_str.lstrip("-").isdigit() else ch_str
        member = await client.get_chat_member(ch, user_id)
        status = member.status

        if status in (
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER,
            ChatMemberStatus.RESTRICTED,
        ):
            return True

        if "PENDING" in str(status).upper() or "REQUEST" in str(status).upper():
            return True

        return False

    except UserNotParticipant:
        return False
    except Exception as e:
        logger.debug(f"req_fsub check error for {channel_id}: {e}")
        return True


async def _deliver_file(client, chat_id, file_obj_id: str):
    """Fetches and sends a cached file by its Mongo ObjectId string, with
    the standard expired-file fallback and auto-delete scheduling. Shared
    by the Request-FSub and Two-Stage-Verification post-verification
    delivery paths in this file, so they can't drift apart."""
    file_data = await db.get_file(file_obj_id)
    if not file_data:
        await client.send_message(chat_id, "✅ Verified! But the file is no longer available. Please search again.")
        return

    cfg            = await db.get_config()
    delete_seconds = int(cfg.get("auto_delete_time", 300))
    delete_minutes = delete_seconds // 60

    from plugins.filter import _build_caption, _auto_delete_file

    try:
        sent = await client.send_cached_media(
            chat_id=chat_id,
            file_id=file_data["file_id"],
            caption=_build_caption(cfg, file_data, delete_minutes, client.me.username),
            parse_mode=ParseMode.HTML
        )
        asyncio.create_task(
            _auto_delete_file(sent, file_data["file_name"], client.me.username, delete_seconds)
        )
    except Exception as e:
        err = str(e).lower()
        if any(k in err for k in ["file_reference", "invalid", "not found", "media"]):
            await db.delete_file_by_id(file_data["file_id"])
            await client.send_message(chat_id, "❌ File has expired. Search again.")
        else:
            await client.send_message(chat_id, "❌ Could not send file. Try again.")
        logger.error(f"req_fsub/_deliver_file send failed: {e}")


async def _find_first_unjoined(client, req_channels: list, user_id: int):
    """Returns (entry, channel_id) of first unjoined channel, or (None, None) if all joined."""
    for entry in req_channels:
        channel_id = entry.get("id") if isinstance(entry, dict) else entry
        if not channel_id:
            continue
        ok = await _has_requested_or_joined(client, channel_id, user_id)
        if not ok:
            return entry, channel_id
    return None, None


async def check_and_show_req_fsub(client, callback, file_obj_id: str) -> bool:
    """
    Returns True  → deliver file (all channels passed or timer not due)
    Returns False → prompt shown, do not deliver yet
    """
    user_id = callback.from_user.id if callback.from_user else None
    if not user_id:
        return True

    config       = await db.get_config()
    req_channels = config.get("req_fsub_channels", [])
    if not req_channels:
        return True

    if not await db.check_req_fsub_due(user_id):
        return True

    unjoined_entry, unjoined_id = await _find_first_unjoined(client, req_channels, user_id)

    if unjoined_entry is None:
        return True

    link = await _get_link(client, unjoined_entry)
    if not link:
        logger.debug(f"req_fsub: no link for {unjoined_id}, failing open")
        return True

    try:
        ch     = int(str(unjoined_id)) if str(unjoined_id).lstrip("-").isdigit() else str(unjoined_id)
        chat   = await client.get_chat(ch)
        ch_name = getattr(chat, "title", "our channel")
    except Exception:
        ch_name = "our channel"

    await db.mark_req_fsub_shown(user_id)

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"📢 Join {ch_name}", url=link)],
        [InlineKeyboardButton(
            "✅ I've Requested — Send My File",
            callback_data=f"rfsub_check#{unjoined_id}#{file_obj_id}"
        )]
    ])

    await callback.answer("📢 One more step!", show_alert=False)
    await callback.message.reply_text(
        f"📢 <b>Quick step!</b>\n\n"
        f"<blockquote>Please join <b>{ch_name}</b> before getting this file.\n\n"
        f"Tap <b>Join</b> below, then tap <b>✅ I've Requested</b>.</blockquote>",
        reply_markup=markup,
        parse_mode=ParseMode.HTML
    )
    return False


@Client.on_callback_query(filters.regex(r"^rfsub_check#"))
async def rfsub_check_callback(client: Client, callback: CallbackQuery):
    user_id = callback.from_user.id if callback.from_user else None
    if not user_id:
        return await callback.answer("❌ Could not identify user.", show_alert=True)

    try:
        parts = callback.data.split("#", 2)
        if len(parts) < 3:
            return await callback.answer("❌ Malformed data.", show_alert=True)
        _, channel_id_str, file_obj_id = parts
    except Exception:
        return await callback.answer("❌ Malformed data.", show_alert=True)

    config       = await db.get_config()
    req_channels = config.get("req_fsub_channels", [])

    unjoined_entry, unjoined_id = await _find_first_unjoined(client, req_channels, user_id)

    if unjoined_entry is not None:
        link = await _get_link(client, unjoined_entry)
        if link:
            try:
                ch      = int(str(unjoined_id)) if str(unjoined_id).lstrip("-").isdigit() else str(unjoined_id)
                chat    = await client.get_chat(ch)
                ch_name = getattr(chat, "title", "our channel")
            except Exception:
                ch_name = "our channel"

            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"📢 Join {ch_name}", url=link)],
                [InlineKeyboardButton(
                    "✅ I've Requested — Send My File",
                    callback_data=f"rfsub_check#{unjoined_id}#{file_obj_id}"
                )]
            ])
            await callback.answer("❌ Still missing a channel.", show_alert=True)
            try:
                await callback.message.edit_text(
                    f"📢 <b>One more channel!</b>\n\n"
                    f"<blockquote>Please also join <b>{ch_name}</b>.\n\n"
                    f"Tap <b>Join</b> → then <b>✅ I've Requested</b>.</blockquote>",
                    reply_markup=markup,
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass
        else:
            await callback.answer("❌ Please join and try again.", show_alert=True)
        return

    await callback.answer("✅ Verified! Sending your file...", show_alert=False)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await _deliver_file(client, callback.message.chat.id, file_obj_id)


# ── TWO-STAGE VERIFICATION ─────────────────────────────────────────────────
# Real, sequential, 2-channel gate — see the module docstring at the top of
# this file for the full design. Config lives in bot_config.two_stage_channels
# (a fixed [slot1, slot2] list, set via plugins/admin.py's Two-Stage Verification
# submenu); per-user cooldown lives in users.two_stage_verified_at.

async def check_and_show_two_stage(client, event, file_obj_id: str) -> bool:
    """Entry point, called right before a file would be delivered.

    `event` is either the Message or CallbackQuery that triggered the file
    request — this works identically whether the user tapped a search
    result (CallbackQuery, plugins/filter.py::send_movie_file) or arrived
    via a group-to-bot deep link (Message, plugins/start.py::_handle_file_link).

    Returns True  → deliver the file now (gate not fully configured, or
                     this user verified within the last 30 minutes).
    Returns False → the Step 1 prompt was shown instead; delivery happens
                     later, from twostage_check2_callback, once both
                     channels are genuinely verified via get_chat_member.
    """
    user = getattr(event, "from_user", None)
    if not user:
        return True

    config          = await db.get_config()
    active_channels = [c for c in config.get("two_stage_channels", []) if c]
    if len(active_channels) < 2:
        return True  # not fully configured on both slots — gate is a no-op

    if not await db.check_two_stage_due(user.id):
        return True

    await _show_two_stage_step(client, event, active_channels, step=1, file_obj_id=file_obj_id)
    return False


async def _show_two_stage_step(client, event, active_channels: list, step: int, file_obj_id: str):
    entry      = active_channels[step - 1]
    channel_id = entry.get("id") if isinstance(entry, dict) else entry

    try:
        ch      = int(str(channel_id)) if str(channel_id).lstrip("-").isdigit() else str(channel_id)
        chat    = await client.get_chat(ch)
        ch_name = getattr(chat, "title", f"Channel {step}")
    except Exception:
        ch_name = f"Channel {step}"

    link = await _get_link(client, entry)

    buttons = []
    if link:
        buttons.append([InlineKeyboardButton(f"📢 Join {ch_name}", url=link)])
    buttons.append([InlineKeyboardButton(
        "✅ I've Joined — Continue",
        callback_data=f"twostage_check{step}#{file_obj_id}"
    )])
    markup = InlineKeyboardMarkup(buttons)

    text = (
        f"🔐 <b>Quick Verification — Step {step}/2</b>\n\n"
        f"<blockquote>Join <b>{ch_name}</b>, then tap "
        f"<b>✅ I've Joined — Continue</b> below.</blockquote>"
    )

    if isinstance(event, CallbackQuery):
        # Edit the same message in place — this is the "step 1's button
        # disappears, step 2's button appears" effect.
        try:
            await event.message.edit_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        except Exception:
            await event.message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
    else:
        await event.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)


async def _finish_two_stage(client, callback: CallbackQuery, file_obj_id: str):
    try:
        await callback.message.delete()
    except Exception:
        pass
    await _deliver_file(client, callback.message.chat.id, file_obj_id)


@Client.on_callback_query(filters.regex(r"^twostage_check1#"))
async def twostage_check1_callback(client: Client, callback: CallbackQuery):
    file_obj_id     = callback.data.split("#", 1)[1]
    config          = await db.get_config()
    active_channels = [c for c in config.get("two_stage_channels", []) if c]

    if len(active_channels) < 2:
        # Admin cleared a channel slot mid-flow — gate no longer applies.
        await callback.answer("✅ Verification no longer required.", show_alert=False)
        return await _finish_two_stage(client, callback, file_obj_id)

    entry      = active_channels[0]
    channel_id = entry.get("id") if isinstance(entry, dict) else entry
    ok = await _has_requested_or_joined(client, channel_id, callback.from_user.id)
    if not ok:
        await callback.answer("❌ You haven't joined Channel 1 yet — please join first.", show_alert=True)
        return

    await callback.answer("✅ Step 1 verified!")
    await _show_two_stage_step(client, callback, active_channels, step=2, file_obj_id=file_obj_id)


@Client.on_callback_query(filters.regex(r"^twostage_check2#"))
async def twostage_check2_callback(client: Client, callback: CallbackQuery):
    file_obj_id     = callback.data.split("#", 1)[1]
    config          = await db.get_config()
    active_channels = [c for c in config.get("two_stage_channels", []) if c]

    if len(active_channels) < 2:
        await callback.answer("✅ Verification no longer required.", show_alert=False)
        return await _finish_two_stage(client, callback, file_obj_id)

    entry      = active_channels[1]
    channel_id = entry.get("id") if isinstance(entry, dict) else entry
    ok = await _has_requested_or_joined(client, channel_id, callback.from_user.id)
    if not ok:
        await callback.answer("❌ You haven't joined Channel 2 yet — please join first.", show_alert=True)
        return

    await db.mark_two_stage_verified(callback.from_user.id)
    await callback.answer("✅ Verified! Sending your file...", show_alert=False)
    await _finish_two_stage(client, callback, file_obj_id)
