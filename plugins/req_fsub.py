"""
req_fsub.py — Request-FSub and Two-Stage Verification config/admin support,
plus the unified Verification Gates entry point that enforces both of them
(alongside Main FSub) before a file is delivered.

  Request-FSub:
    - Unlimited channels in a pool (bot_config.req_fsub_channels)
    - Timer per user (req_fsub_interval_hours) — not re-checked on every tap

  Two-Stage Verification:
    - Exactly 2 fixed channels (bot_config.two_stage_channels), only active
      once both slots are set
    - Fixed 30-minute per-user re-verification window (TWO_STAGE_VERIFY_INTERVAL
      in db.py) — not admin-tunable like req_fsub's interval

  Both gates, and Main FSub, are enforced through ONE path —
  check_verification_gates() at the bottom of this file — built on the same
  real join-link (_get_link) and real membership-check
  (_has_requested_or_joined) primitives, so a file is never delivered
  without Telegram actually confirming membership. When more than one gate
  is outstanding for a user, they see a single combined "join these N
  channels, then Continue" screen instead of separate sequential prompts —
  this replaced an earlier design where each gate had its own standalone
  check-and-show flow and users could face up to 3 sequential join prompts
  for one file.
"""

import logging
import asyncio
from pyrogram import Client, filters
from pyrogram.errors import UserNotParticipant
from pyrogram.errors import (
    FileIdInvalid, FileReferenceEmpty, FileReferenceExpired,
    FileReferenceInvalid, MediaEmpty, MediaInvalid,
)
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from pyrogram.enums import ParseMode, ChatMemberStatus
from database.db import db
from utils import is_subscribed_by_id, _parse_fsub_entry

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
    the standard expired-file fallback and auto-delete scheduling. Called
    from vgate_check_callback once every outstanding gate has passed."""
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
        await _auto_delete_file(sent, file_data["file_name"], client.me.username, delete_seconds)
    except (FileIdInvalid, FileReferenceEmpty, FileReferenceExpired,
            FileReferenceInvalid, MediaEmpty, MediaInvalid) as e:
        await db.delete_file_by_id(file_data["file_id"])
        await client.send_message(chat_id, "❌ File has expired. Search again.")
        logger.warning("Removed invalid cached file %s: %s", file_data["file_id"], e)
    except Exception as e:
        await client.send_message(chat_id, "❌ Could not send file right now. Try again.")
        logger.error(f"req_fsub/_deliver_file send failed: {e}")


# ── VERIFICATION GATES ───────────────────────────────────────────────────────
# See module docstring. This is the entry point filter.py and start.py call
# before delivering a file — Main FSub, Request-FSub, and Two-Stage are all
# evaluated here and combined into one join screen when more than one is
# outstanding, reusing _get_link/_has_requested_or_joined/_deliver_file above.

async def _channel_display_name(client, channel_id) -> str:
    try:
        ch = int(str(channel_id)) if str(channel_id).lstrip("-").isdigit() else str(channel_id)
        chat = await client.get_chat(ch)
        return getattr(chat, "title", "this channel")
    except Exception:
        return "this channel"


async def _collect_outstanding_gates(client, user_id: int):
    """Evaluates all three gates against their exact existing pass/fail
    rules and returns (missing, mark_fns):
      missing  — [{"label", "link"}, ...] channels still to join, across
                 every gate currently outstanding for this user.
      mark_fns — zero-arg async callables to run once the user has passed
                 everything (records gate-satisfied state, same calls each
                 gate's own standalone flow already makes).
    """
    config = await db.get_config()
    missing, mark_fns = [], []

    # Main FSub — all-or-nothing pass/fail (same as utils.is_subscribed);
    # no per-channel granularity, so on failure every configured channel is
    # listed, matching send_fsub_message()'s existing standalone behavior.
    fsub_channels = config.get("fsub_channels", [])
    if fsub_channels and not await is_subscribed_by_id(client, user_id):
        for i, entry in enumerate(fsub_channels, 1):
            channel_id, _ = _parse_fsub_entry(entry)
            if not channel_id:
                continue
            link = await _get_link(client, entry if isinstance(entry, dict) else {"id": channel_id})
            if link:
                missing.append({"label": f"Main Channel {i}", "link": link})

    # Request-FSub — every unjoined channel in the pool, not just the first,
    # so they all fit on one combined screen instead of a sequential chain.
    req_channels = config.get("req_fsub_channels", [])
    if req_channels and await db.check_req_fsub_due(user_id):
        # Membership checks are independent Telegram API calls — run them
        # concurrently instead of one at a time, same result, lower latency
        # on the file-delivery critical path. Falsy channel_id entries are
        # filtered out first so they're never awaited, matching the
        # original loop's "if channel_id and not await ..." short-circuit.
        candidates = [
            (entry, entry.get("id") if isinstance(entry, dict) else entry)
            for entry in req_channels
        ]
        candidates = [(entry, cid) for entry, cid in candidates if cid]
        raw_flags = await asyncio.gather(
            *[_has_requested_or_joined(client, cid, user_id) for _, cid in candidates],
            return_exceptions=True
        ) if candidates else []
        # _has_requested_or_joined already fails open (returns True) on any
        # error internally, so raw_flags should never actually contain an
        # exception object — return_exceptions=True plus this fallback is
        # defense-in-depth so a future change to that function can't turn
        # one bad channel into an unhandled exception that aborts the whole
        # gate check instead of failing open like every other error path here.
        joined_flags = [True if isinstance(r, BaseException) else r for r in raw_flags]
        unjoined = [
            (entry, cid) for (entry, cid), joined in zip(candidates, joined_flags)
            if not joined
        ]
        if unjoined:
            # Resets the due-interval the moment the gate is shown, so a
            # user who ignores it isn't re-prompted again mid-interval.
            await db.mark_req_fsub_shown(user_id)
            for entry, channel_id in unjoined:
                link = await _get_link(client, entry)
                if link:
                    name = await _channel_display_name(client, channel_id)
                    missing.append({"label": name, "link": link})

    # Two-Stage — whichever of the (exactly 2) configured channels aren't
    # joined yet; both at once rather than gated Step 1 -> Step 2.
    active_two_stage = [c for c in config.get("two_stage_channels", []) if c]
    if len(active_two_stage) >= 2 and await db.check_two_stage_due(user_id):
        # Same concurrency treatment as the Request-FSub block above.
        stage_ids = [
            entry.get("id") if isinstance(entry, dict) else entry
            for entry in active_two_stage
        ]
        raw_flags = await asyncio.gather(
            *[_has_requested_or_joined(client, cid, user_id) for cid in stage_ids],
            return_exceptions=True
        )
        # Same defense-in-depth fallback as the Request-FSub block above.
        joined_flags = [True if isinstance(r, BaseException) else r for r in raw_flags]
        unjoined_stage = [
            entry for entry, joined in zip(active_two_stage, joined_flags) if not joined
        ]
        if unjoined_stage:
            for entry in unjoined_stage:
                channel_id = entry.get("id") if isinstance(entry, dict) else entry
                link = await _get_link(client, entry)
                if link:
                    name = await _channel_display_name(client, channel_id)
                    missing.append({"label": name, "link": link})
            mark_fns.append(lambda uid=user_id: db.mark_two_stage_verified(uid))

    return missing, mark_fns


def _gates_markup(missing: list, file_obj_id: str) -> InlineKeyboardMarkup:
    buttons = [[InlineKeyboardButton(f"📢 Join {m['label']}", url=m["link"])] for m in missing]
    buttons.append([InlineKeyboardButton(
        "✅ I've Joined Everything — Continue",
        callback_data=f"vgate_check#{file_obj_id}"
    )])
    return InlineKeyboardMarkup(buttons)


async def check_verification_gates(client, event, file_obj_id: str) -> bool:
    """Entry point called right before a file would be delivered.

    Returns True  → deliver the file now (nothing outstanding).
    Returns False → the combined join screen was shown; delivery happens
                     later, from vgate_check_callback, once every
                     outstanding gate is genuinely re-verified.
    """
    user = getattr(event, "from_user", None)
    if not user:
        return True

    missing, _ = await _collect_outstanding_gates(client, user.id)
    if not missing:
        return True

    text = (
        f"🔐 <b>Almost there — join {len(missing)} channel(s) to unlock this file</b>\n\n"
        f"<blockquote>Tap each one below, then hit "
        f"<b>✅ Continue</b>.</blockquote>"
    )
    markup = _gates_markup(missing, file_obj_id)

    if isinstance(event, CallbackQuery):
        try:
            await event.message.edit_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        except Exception:
            await event.message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
    else:
        await event.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)

    return False


@Client.on_callback_query(filters.regex(r"^vgate_check#"))
async def vgate_check_callback(client: Client, callback: CallbackQuery):
    file_obj_id = callback.data.split("#", 1)[1]
    user_id     = callback.from_user.id

    missing, mark_fns = await _collect_outstanding_gates(client, user_id)
    if missing:
        await callback.answer("❌ Still missing some channels.", show_alert=True)
        try:
            await callback.message.edit_text(
                f"🔐 <b>Still missing {len(missing)} channel(s)</b>\n\n"
                f"<blockquote>Tap each one below, then hit "
                f"<b>✅ Continue</b>.</blockquote>",
                reply_markup=_gates_markup(missing, file_obj_id),
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass
        return

    for fn in mark_fns:
        await fn()

    await callback.answer("✅ Verified! Sending your file...", show_alert=False)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await _deliver_file(client, callback.message.chat.id, file_obj_id)
