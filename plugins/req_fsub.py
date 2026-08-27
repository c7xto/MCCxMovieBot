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
  (_requested_or_joined_status) primitives, so a file is never delivered
  without Telegram actually confirming membership. When more than one gate
  is outstanding for a user, they see a single combined "join these N
  channels, then Continue" screen instead of separate sequential prompts —
  this replaced an earlier design where each gate had its own standalone
  check-and-show flow and users could face up to 3 sequential join prompts
  for one file.
"""

import logging
import asyncio
from dataclasses import dataclass
from pyrogram import Client, filters
from pyrogram.errors import (
    FileIdInvalid, FileReferenceEmpty, FileReferenceExpired,
    FileReferenceInvalid, MediaEmpty, MediaInvalid,
)
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from pyrogram.enums import ParseMode
from database.db import db
from plugins.access_policy import authorize_user_action, enforce_user_action
from plugins.workload import WorkloadRejected, delivery_guard
from plugins.telegram_retry import DELIVERY_RETRY, telegram_call
from utils import get_subscription_status_by_id, _parse_fsub_entry
from verification import (
    VerificationResult,
    VerificationStatus,
    check_channel_membership,
    verification_unavailable_message,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GateEvaluation:
    result: VerificationResult
    missing: tuple[dict, ...] = ()
    passed_due_gates: frozenset[str] = frozenset()


def _gate_evaluation(result, missing=(), passed_due_gates=()):
    return GateEvaluation(result, tuple(missing), frozenset(passed_due_gates))


async def _persist_passed_gate_state(user_id: int, passed_due_gates):
    if "request_fsub" in passed_due_gates:
        await db.mark_req_fsub_verified(user_id)
    if "two_stage" in passed_due_gates:
        cached = await db.mark_two_stage_verified(user_id)
        if cached is False:
            logger.warning("Two-stage verification passed but cache persistence failed")


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


async def _requested_or_joined_status(
    client, channel_id, user_id: int
) -> VerificationResult:
    return await check_channel_membership(
        client, channel_id, user_id, allow_pending_request=True
    )


async def _deliver_file(client, chat_id, user_id: int, file_obj_id: str):
    """Fetches and sends a cached file by its Mongo ObjectId string, with
    the standard expired-file fallback and auto-delete scheduling. Called
    from vgate_check_callback once every outstanding gate has passed."""
    access = await authorize_user_action(user_id, "file_delivery")
    if not access.allowed:
        await client.send_message(chat_id, access.message or "Action denied.")
        return
    file_data = await db.get_file(file_obj_id)
    if not file_data:
        await client.send_message(chat_id, "✅ Verified! But the file is no longer available. Please search again.")
        return

    access = await authorize_user_action(user_id, "file_delivery")
    if not access.allowed:
        await client.send_message(chat_id, access.message or "Action denied.")
        return
    cfg            = access.config
    delete_seconds = int(cfg.get("auto_delete_time", 300))
    delete_minutes = delete_seconds // 60

    from plugins.filter import _build_caption, _auto_delete_file

    guard = delivery_guard(user_id, file_obj_id)
    try:
        await guard.__aenter__()
    except WorkloadRejected as exc:
        await client.send_message(chat_id, exc.public_message)
        return

    try:
        sent = await telegram_call(
            lambda: client.send_cached_media(
                chat_id=chat_id,
                file_id=file_data["file_id"],
                caption=_build_caption(
                    cfg, file_data, delete_minutes, client.me.username
                ),
                parse_mode=ParseMode.HTML,
            ),
            route="file_delivery_verification",
            policy=DELIVERY_RETRY,
            retry_safe=True,
            idempotency_key=f"{user_id}:{file_obj_id}",
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
    finally:
        await guard.__aexit__(None, None, None)


# ── VERIFICATION GATES ───────────────────────────────────────────────────────
# See module docstring. This is the entry point filter.py and start.py call
# before delivering a file — Main FSub, Request-FSub, and Two-Stage are all
# evaluated here and combined into one join screen when more than one is
# outstanding, reusing _get_link/_requested_or_joined_status/_deliver_file above.

async def _channel_display_name(client, channel_id) -> str:
    try:
        ch = int(str(channel_id)) if str(channel_id).lstrip("-").isdigit() else str(channel_id)
        chat = await client.get_chat(ch)
        return getattr(chat, "title", "this channel")
    except Exception:
        return "this channel"


async def _collect_outstanding_gates(client, user_id: int):
    """Return aggregate state plus due gates conclusively passed this check."""
    try:
        config = await db.get_config()
    except Exception as exc:
        logger.warning(
            "verification_indeterminate gate=config error_type=%s",
            type(exc).__name__,
        )
        return _gate_evaluation(VerificationResult.indeterminate("config_unavailable"))

    missing, passed_due_gates = [], set()

    fsub_channels = config.get("fsub_channels", [])
    main_status = await get_subscription_status_by_id(client, user_id, config)
    if main_status.status is VerificationStatus.INDETERMINATE:
        return _gate_evaluation(main_status)
    if main_status.status is VerificationStatus.DENY:
        for i, entry in enumerate(fsub_channels, 1):
            channel_id, _ = _parse_fsub_entry(entry)
            if not channel_id:
                return _gate_evaluation(VerificationResult.indeterminate(
                    "missing_main_fsub_channel_id"
                ))
            link = await _get_link(
                client,
                entry if isinstance(entry, dict) else {"id": channel_id},
            )
            if not link:
                return _gate_evaluation(VerificationResult.indeterminate(
                    "main_fsub_link_unavailable"
                ))
            missing.append({"label": f"Main Channel {i}", "link": link})

    req_channels = config.get("req_fsub_channels", [])
    if not isinstance(req_channels, list):
        return _gate_evaluation(VerificationResult.indeterminate(
            "invalid_request_fsub_configuration"
        ))
    if req_channels:
        req_due = await db.get_req_fsub_gate_status(user_id)
        if req_due.status is VerificationStatus.INDETERMINATE:
            return _gate_evaluation(req_due)
        if req_due.status is VerificationStatus.DENY:
            candidates = [
                (entry, entry.get("id") if isinstance(entry, dict) else entry)
                for entry in req_channels
            ]
            if any(not channel_id for _, channel_id in candidates):
                return _gate_evaluation(VerificationResult.indeterminate(
                    "missing_request_fsub_channel_id"
                ))
            membership_results = await asyncio.gather(
                *[
                    _requested_or_joined_status(client, channel_id, user_id)
                    for _, channel_id in candidates
                ],
                return_exceptions=True,
            )
            for result in membership_results:
                if isinstance(result, BaseException):
                    return _gate_evaluation(VerificationResult.indeterminate(
                        "request_fsub_task_error"
                    ))
                if result.status is VerificationStatus.INDETERMINATE:
                    return _gate_evaluation(result)

            unjoined = [
                pair
                for pair, result in zip(candidates, membership_results)
                if result.status is VerificationStatus.DENY
            ]
            if unjoined:
                for entry, channel_id in unjoined:
                    link = await _get_link(client, entry)
                    if not link:
                        return _gate_evaluation(VerificationResult.indeterminate(
                            "request_fsub_link_unavailable"
                        ))
                    name = await _channel_display_name(client, channel_id)
                    missing.append({"label": name, "link": link})
            else:
                passed_due_gates.add("request_fsub")

    two_stage_channels = config.get("two_stage_channels", [])
    if not isinstance(two_stage_channels, list):
        return _gate_evaluation(VerificationResult.indeterminate(
            "invalid_two_stage_configuration"
        ))
    active_two_stage = [channel for channel in two_stage_channels if channel]
    if len(active_two_stage) >= 2:
        two_stage_due = await db.get_two_stage_gate_status(user_id)
        if two_stage_due.status is VerificationStatus.INDETERMINATE:
            return _gate_evaluation(two_stage_due)
        if two_stage_due.status is VerificationStatus.DENY:
            stage_ids = [
                entry.get("id") if isinstance(entry, dict) else entry
                for entry in active_two_stage
            ]
            if any(not channel_id for channel_id in stage_ids):
                return _gate_evaluation(VerificationResult.indeterminate(
                    "missing_two_stage_channel_id"
                ))
            membership_results = await asyncio.gather(
                *[
                    _requested_or_joined_status(client, channel_id, user_id)
                    for channel_id in stage_ids
                ],
                return_exceptions=True,
            )
            for result in membership_results:
                if isinstance(result, BaseException):
                    return _gate_evaluation(VerificationResult.indeterminate(
                        "two_stage_task_error"
                    ))
                if result.status is VerificationStatus.INDETERMINATE:
                    return _gate_evaluation(result)

            unjoined_stage = [
                entry
                for entry, result in zip(active_two_stage, membership_results)
                if result.status is VerificationStatus.DENY
            ]
            if unjoined_stage:
                for entry in unjoined_stage:
                    channel_id = entry.get("id") if isinstance(entry, dict) else entry
                    link = await _get_link(client, entry)
                    if not link:
                        return _gate_evaluation(VerificationResult.indeterminate(
                            "two_stage_link_unavailable"
                        ))
                    name = await _channel_display_name(client, channel_id)
                    missing.append({"label": name, "link": link})
            else:
                passed_due_gates.add("two_stage")

    if missing:
        return _gate_evaluation(
            VerificationResult.deny("verification_channels_missing"),
            missing,
            passed_due_gates,
        )
    return _gate_evaluation(
        VerificationResult.allow("all_gates_passed"),
        passed_due_gates=passed_due_gates,
    )


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
    if not (await enforce_user_action(event, "verification")).allowed:
        return False
    user = getattr(event, "from_user", None)
    if not user:
        return True

    evaluation = await _collect_outstanding_gates(client, user.id)
    gate_result, missing = evaluation.result, evaluation.missing
    if gate_result.status is VerificationStatus.INDETERMINATE:
        message = verification_unavailable_message(gate_result)
        if isinstance(event, CallbackQuery):
            await event.answer(message, show_alert=True)
        else:
            await event.reply_text(message, reply_parameters=None)
        return False
    if gate_result.status is VerificationStatus.PASS:
        await _persist_passed_gate_state(user.id, evaluation.passed_due_gates)
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
    if not (await enforce_user_action(callback, "verification")).allowed:
        return
    file_obj_id = callback.data.split("#", 1)[1]
    user_id     = callback.from_user.id

    evaluation = await _collect_outstanding_gates(client, user_id)
    gate_result, missing = evaluation.result, evaluation.missing
    if gate_result.status is VerificationStatus.INDETERMINATE:
        await callback.answer(
            verification_unavailable_message(gate_result), show_alert=True
        )
        return
    if gate_result.status is VerificationStatus.DENY:
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

    await _persist_passed_gate_state(user_id, evaluation.passed_due_gates)

    await callback.answer("✅ Verified! Sending your file...", show_alert=False)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await _deliver_file(client, callback.message.chat.id, user_id, file_obj_id)
