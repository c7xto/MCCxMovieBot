"""
req_fsub.py — Request Channel FSub Plugin
==========================================
Shows a "Request to Join" prompt before file delivery, once per configured interval.
Checks if the user has sent a join request (status = REQUESTED) OR is already a member.
Either passes. No approval — just verifies the request exists.

Bot must be admin in the channel with "Manage Invites" permission.
"""
import random
import logging
import asyncio
from pyrogram import Client, filters
from pyrogram.errors import UserNotParticipant
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
)
from pyrogram.enums import ParseMode, ChatMemberStatus
from database.db import db

logger = logging.getLogger(__name__)


async def _get_channel_link(client, entry: dict) -> str:
    """
    Build join link for a req_fsub channel.
    Private channels (-100xxx): use stored invite link, or generate once and save.
    Public channels (@username): direct https://t.me/username link.
    Never calls get_chat_member here — avoids the permission issue.
    """
    channel_id = entry.get("id") if isinstance(entry, dict) else entry
    if not channel_id:
        return None

    ch_str = str(channel_id).strip()
    stored_link = entry.get("link") if isinstance(entry, dict) else None

    # Public @username
    if ch_str.startswith("@"):
        return f"https://t.me/{ch_str[1:]}"

    # Already have a stored https invite link
    if stored_link and stored_link.startswith("https://"):
        return stored_link

    # Private -100xxx — generate invite link once and store it
    if ch_str.startswith("-100"):
        try:
            link = await client.create_chat_invite_link(
                int(ch_str),
                creates_join_request=True   # shows "Request to Join" button
            )
            await db.update_req_fsub_link(channel_id, link.invite_link)
            return link.invite_link
        except Exception as e:
            logger.warning(f"req_fsub: could not create invite link for {channel_id}: {e}")
            return None

    # Fallback: treat as username
    return f"https://t.me/{ch_str}"


async def _check_user_requested_or_joined(client, channel_id, user_id: int) -> bool:
    """
    Returns True if user has sent a join request OR is already a member.
    Pyrogram ChatMemberStatus.PENDING = join request submitted.
    """
    ch_str = str(channel_id).strip()
    try:
        ch = int(ch_str) if ch_str.lstrip("-").isdigit() else ch_str
        member = await client.get_chat_member(ch, user_id)
        status = member.status
        # Accept: already member, admin, creator, or pending (requested to join)
        if status in [
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER,
            ChatMemberStatus.RESTRICTED,
        ]:
            return True
        # PENDING = join request submitted — this is what we want
        if hasattr(ChatMemberStatus, "PENDING") and status == ChatMemberStatus.PENDING:
            return True
        # Some Pyrogram versions expose it differently
        if str(status).upper() in ["PENDING", "MEMBER", "ADMINISTRATOR", "OWNER", "RESTRICTED"]:
            return True
        return False
    except UserNotParticipant:
        return False
    except Exception as e:
        logger.warning(f"req_fsub check error for {channel_id}: {e}")
        # Fail open — don't block user if check itself errors
        return True


async def check_and_show_req_fsub(client, callback: CallbackQuery, file_obj_id: str) -> bool:
    """
    Called from sendfile handler AFTER main FSub passes.
    Returns True if file should be delivered (req_fsub passed or not due).
    Returns False if we showed the prompt (caller should stop, not deliver).
    """
    user_id = callback.from_user.id if callback.from_user else None
    if not user_id:
        return True

    config = await db.get_config()
    req_channels = config.get("req_fsub_channels", [])
    if not req_channels:
        return True  # feature not configured — pass through

    # Check if timer is due for this user
    if not await db.check_req_fsub_due(user_id):
        return True  # not time yet — deliver file normally

    # Pick one random channel from the pool
    chosen = random.choice(req_channels)
    channel_id = chosen.get("id") if isinstance(chosen, dict) else chosen

    link = await _get_channel_link(client, chosen)
    if not link:
        logger.warning(f"req_fsub: no link for channel {channel_id}, skipping")
        return True  # can't build link — fail open

    # Record that we showed the prompt NOW (before showing, so even if they dismiss
    # without tapping, the timer resets — prevents spamming on every file)
    await db.mark_req_fsub_shown(user_id)

    # Build prompt
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Request to Join", url=link)],
        [InlineKeyboardButton("✅ I've Requested — Give Me The File",
                              callback_data=f"rfsub_check#{channel_id}#{file_obj_id}")]
    ])

    await callback.answer("📢 One more step!", show_alert=False)
    await callback.message.reply_text(
        "📢 <b>Quick step!</b>\n\n"
        "<blockquote>Tap below to request to join our channel.\n"
        "Once you've sent the request, tap <b>✅ I've Requested</b> to get your file.</blockquote>",
        reply_markup=markup,
        parse_mode=ParseMode.HTML
    )
    return False  # don't deliver yet


@Client.on_callback_query(filters.regex(r"^rfsub_check#"))
async def rfsub_check_callback(client: Client, callback: CallbackQuery):
    """
    User tapped 'I've Requested'. Check if they have a pending request or membership.
    If yes — deliver the file. If no — tell them to request first.
    """
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

    # Check if user has requested or joined
    passed = await _check_user_requested_or_joined(client, channel_id_str, user_id)

    if not passed:
        await callback.answer(
            "❌ We couldn't find your request yet.\n"
            "Please tap 'Request to Join' first, then try again.",
            show_alert=True
        )
        return

    # Passed — delete the prompt and deliver the file
    try:
        await callback.message.delete()
    except Exception:
        pass

    # Fetch and send the file
    file_data = await db.get_file(file_obj_id)
    if not file_data:
        await client.send_message(
            callback.message.chat.id,
            "✅ Verified! But the file is no longer available. Please search again."
        )
        return

    config = await db.get_config()
    delete_seconds = int(config.get("auto_delete_time", 300))
    delete_minutes = delete_seconds // 60

    # Reuse caption builder from filter.py
    from plugins.filter import _build_caption, _auto_delete_file
    import asyncio as _asyncio

    try:
        sent = await client.send_cached_media(
            chat_id=callback.message.chat.id,
            file_id=file_data["file_id"],
            caption=_build_caption(config, file_data, delete_minutes, client.me.username),
            parse_mode=ParseMode.HTML
        )
        _asyncio.create_task(
            _auto_delete_file(sent, file_data["file_name"], client.me.username, delete_seconds)
        )
        await callback.answer("✅ Here's your file!", show_alert=False)
    except Exception as e:
        err = str(e).lower()
        if any(k in err for k in ["file_reference", "invalid", "not found", "media"]):
            await db.delete_file_by_id(file_data["file_id"])
            await client.send_message(
                callback.message.chat.id,
                "❌ File has expired from Telegram servers. Please search again."
            )
        else:
            await client.send_message(
                callback.message.chat.id,
                "❌ Could not send file. Please try again."
            )
        logger.error(f"rfsub file send failed: {e}")
