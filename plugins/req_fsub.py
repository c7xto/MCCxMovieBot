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
import time
from collections import OrderedDict
from dataclasses import dataclass
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, CallbackQuery, Message
from plugins.mobile_ui import MobileInlineKeyboardMarkup as InlineKeyboardMarkup
from pyrogram.enums import ParseMode
from database.db import db
from plugins.access_policy import authorize_user_action, enforce_user_action
from plugins.callbacks import answer_callback_safely
from plugins.workload import interactive_slot, record_workload_metric
from plugins.access_gates import GRACE_SECONDS, get_access_gates
from utils import _parse_fsub_entry, get_subscription_status_by_id  # legacy import compatibility
from verification import (
    VerificationResult,
    VerificationStatus,
    check_channel_membership,
    verification_unavailable_message,
)

logger = logging.getLogger(__name__)
_memory_gate_cache = OrderedDict()
_MEMORY_GATE_CACHE_MAX = 10_000
_VERIFICATION_IO_TIMEOUT = 2.0


@dataclass(frozen=True)
class GateEvaluation:
    result: VerificationResult
    missing: tuple[dict, ...] = ()
    passed_due_gates: frozenset[str] = frozenset()


def _gate_evaluation(result, missing=(), passed_due_gates=()):
    return GateEvaluation(result, tuple(missing), frozenset(passed_due_gates))


async def _persist_passed_gate_state(user_id: int, passed_due_gates):
    writes = []
    if "request_fsub" in passed_due_gates:
        writes.append(db.mark_req_fsub_verified(user_id))
    if "two_stage" in passed_due_gates:
        writes.append(db.mark_two_stage_verified(user_id))
    if not writes:
        return
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*writes, return_exceptions=True),
            timeout=_VERIFICATION_IO_TIMEOUT,
        )
        if any(result is False or isinstance(result, BaseException) for result in results):
            logger.warning("Legacy verification cache persistence was incomplete")
    except TimeoutError:
        logger.warning("Legacy verification cache persistence timed out")


async def _get_link(client, entry) -> str | None:
    channel_id = entry.get("id") if isinstance(entry, dict) else entry
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
        ch = int(ch_str) if ch_str.lstrip("-").isdigit() else ch_str
        chat = await client.get_chat(ch)
        uname = getattr(chat, "username", None)
        if uname:
            return f"https://t.me/{uname}"
    except Exception:
        pass

    # Private channel — generate a direct join link. A request-to-join link
    # cannot satisfy a required membership gate until an admin approves it.
    if ch_str.lstrip("-").isdigit():
        try:
            link = await asyncio.wait_for(
                client.create_chat_invite_link(int(ch_str), creates_join_request=False),
                timeout=_VERIFICATION_IO_TIMEOUT,
            )
            source = entry.get("source") if isinstance(entry, dict) else None
            gate_key = entry.get("key") if isinstance(entry, dict) else None
            try:
                if source == "main_fsub":
                    update = db.update_fsub_channel_link(channel_id, link.invite_link)
                elif source == "two_stage":
                    update = db.update_two_stage_channel_link(channel_id, link.invite_link)
                elif source == "request_fsub":
                    update = db.update_req_fsub_link(channel_id, link.invite_link)
                elif gate_key:
                    update = db.update_access_gate_link(gate_key, link.invite_link)
                else:
                    update = None
                if update is not None:
                    await asyncio.wait_for(update, timeout=_VERIFICATION_IO_TIMEOUT)
            except Exception as exc:
                logger.warning(
                    "Could not persist generated access-gate link error_type=%s",
                    type(exc).__name__,
                )
            return link.invite_link
        except Exception as e:
            logger.debug(f"req_fsub: no invite link for {channel_id}: {e}")
            return None

    return f"https://t.me/{ch_str}"


async def _requested_or_joined_status(
    client,
    channel_id,
    user_id: int,
    allow_pending_request: bool = True,
) -> VerificationResult:
    return await check_channel_membership(
        client,
        channel_id,
        user_id,
        allow_pending_request=allow_pending_request,
    )


async def _deliver_file(client, chat_id, user_id: int, file_obj_id: str):
    """Fetches and sends a cached file by its Mongo ObjectId string, with
    the standard expired-file fallback and auto-delete scheduling. Called
    from vgate_check_callback once every outstanding gate has passed."""
    file_data = await db.get_file(file_obj_id)
    if not file_data:
        await client.send_message(
            chat_id, "✅ Verified! But the file is no longer available. Please search again."
        )
        return

    access = await authorize_user_action(user_id, "file_delivery")
    if not access.allowed:
        await client.send_message(chat_id, access.message or "Action denied.")
        return
    cfg = access.config
    from plugins.filter import deliver_cached_file

    await deliver_cached_file(
        client,
        chat_id=chat_id,
        user_id=user_id,
        file_obj_id=file_obj_id,
        file_data=file_data,
        config=cfg,
        route="file_delivery_verification",
    )


# ── VERIFICATION GATES ───────────────────────────────────────────────────────
# See module docstring. This is the entry point filter.py and start.py call
# before delivering a file — Main FSub, Request-FSub, and Two-Stage are all
# evaluated here and combined into one join screen when more than one is
# outstanding, reusing _get_link/_requested_or_joined_status/_deliver_file above.


async def _channel_display_name(client, channel_id) -> str:
    try:
        ch = int(str(channel_id)) if str(channel_id).lstrip("-").isdigit() else str(channel_id)
        chat = await asyncio.wait_for(client.get_chat(ch), timeout=_VERIFICATION_IO_TIMEOUT)
        title = str(getattr(chat, "title", None) or "this channel").strip()
        return title[:48] or "this channel"
    except Exception:
        return "this channel"


async def _collect_outstanding_gates(client, user_id: int):
    """Evaluate canonical gates with bounded cache reads and a grace window."""
    try:
        config = await asyncio.wait_for(db.get_config(), timeout=2.0)
    except Exception as exc:
        logger.warning(
            "verification_indeterminate gate=config error_type=%s",
            type(exc).__name__,
        )
        return _gate_evaluation(VerificationResult.indeterminate("config_unavailable"))

    gates = get_access_gates(config)
    if not gates:
        return _gate_evaluation(VerificationResult.allow("no_access_gates"))
    if any(not gate.get("id") for gate in gates):
        return _gate_evaluation(VerificationResult.indeterminate("missing_access_gate_channel_id"))

    now = time.time()
    keys = [gate["key"] for gate in gates]
    persistent_cache = {}
    if hasattr(db, "get_verification_cache"):
        try:
            persistent_cache = await asyncio.wait_for(
                db.get_verification_cache(user_id, keys), timeout=2.0
            )
        except Exception as exc:
            record_workload_metric("verification_cache_read_failure")
            logger.warning(
                "verification_cache_unavailable error_type=%s",
                type(exc).__name__,
            )

    cache = {}
    for gate in gates:
        cache_key = (id(db), int(user_id), gate["key"])
        record = persistent_cache.get(gate["key"]) or _memory_gate_cache.get(cache_key)
        if record:
            cache[gate["key"]] = record
            _memory_gate_cache[cache_key] = record
            _memory_gate_cache.move_to_end(cache_key)
    while len(_memory_gate_cache) > _MEMORY_GATE_CACHE_MAX:
        _memory_gate_cache.popitem(last=False)

    def _timestamp(value):
        if hasattr(value, "timestamp"):
            return value.timestamp()
        try:
            return float(value or 0)
        except (TypeError, ValueError):
            return 0.0

    legacy_status = {}
    sources = {gate.get("source") for gate in gates}
    legacy_readers = {}
    if "request_fsub" in sources and hasattr(db, "get_req_fsub_gate_status"):
        legacy_readers["request_fsub"] = db.get_req_fsub_gate_status(user_id)
    if "two_stage" in sources and hasattr(db, "get_two_stage_gate_status"):
        legacy_readers["two_stage"] = db.get_two_stage_gate_status(user_id)
    if legacy_readers:
        try:
            values = await asyncio.wait_for(
                asyncio.gather(*legacy_readers.values(), return_exceptions=True),
                timeout=_VERIFICATION_IO_TIMEOUT,
            )
            for source, value in zip(legacy_readers, values):
                if isinstance(value, VerificationResult):
                    legacy_status[source] = value
                elif isinstance(value, BaseException):
                    record_workload_metric("verification_legacy_cache_read_failure")
        except TimeoutError:
            record_workload_metric("verification_legacy_cache_read_timeout")

    due = []
    for gate in gates:
        record = cache.get(gate["key"], {})
        if _timestamp(record.get("valid_until")) > now:
            continue
        old_status = legacy_status.get(gate.get("source"))
        if old_status and old_status.status is VerificationStatus.PASS:
            continue
        due.append(gate)
    if not due:
        return _gate_evaluation(VerificationResult.allow("verification_cache_valid"))

    try:
        membership_results = await asyncio.wait_for(
            asyncio.gather(
                *[
                    _requested_or_joined_status(
                        client,
                        gate["id"],
                        user_id,
                        allow_pending_request=False,
                    )
                    for gate in due
                ],
                return_exceptions=True,
            ),
            timeout=_VERIFICATION_IO_TIMEOUT,
        )
    except TimeoutError:
        if all(
            _timestamp(cache.get(gate["key"], {}).get("grace_until")) > now
            for gate in due
        ):
            record_workload_metric("verification_grace_pass")
            logger.warning("verification_grace_pass reason=telegram_membership_timeout")
            return _gate_evaluation(VerificationResult.allow("verification_timeout_grace"))
        return _gate_evaluation(VerificationResult.indeterminate("telegram_membership_timeout"))
    missing, passed_due_gates = [], set()
    for gate, result in zip(due, membership_results):
        if isinstance(result, BaseException):
            result = VerificationResult.indeterminate("access_gate_task_error")
        record = cache.get(gate["key"], {})
        if result.status is VerificationStatus.INDETERMINATE:
            if _timestamp(record.get("grace_until")) > now:
                record_workload_metric("verification_grace_pass")
                logger.warning(
                    "verification_grace_pass gate_key=%s reason=%s",
                    gate["key"],
                    result.reason,
                )
                continue
            return _gate_evaluation(result)
        if result.status is VerificationStatus.DENY:
            _memory_gate_cache.pop((id(db), int(user_id), gate["key"]), None)
            try:
                await asyncio.wait_for(
                    db.invalidate_gate_verification(user_id, gate["key"]), timeout=2.0
                )
            except Exception:
                record_workload_metric("verification_cache_invalidation_failure")
            link = await _get_link(client, gate)
            if not link:
                return _gate_evaluation(
                    VerificationResult.indeterminate("access_gate_link_unavailable")
                )
            name = await _channel_display_name(client, gate["id"])
            missing.append({"label": name, "link": link})
            continue

        interval = int(gate.get("interval_seconds", 900))
        valid_until = now + interval
        grace_until = valid_until + GRACE_SECONDS
        memory_record = {"valid_until": valid_until, "grace_until": grace_until}
        cache_key = (id(db), int(user_id), gate["key"])
        _memory_gate_cache[cache_key] = memory_record
        try:
            persisted = await asyncio.wait_for(
                db.mark_gate_verified(user_id, gate["key"], interval, GRACE_SECONDS),
                timeout=2.0,
            )
            if persisted:
                _memory_gate_cache[cache_key] = persisted
        except Exception:
            record_workload_metric("verification_cache_write_failure")
        passed_due_gates.add(str(gate.get("source") or "access_gates"))

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
    buttons.append(
        [
            InlineKeyboardButton(
                "✅ I've Joined Everything — Continue", callback_data=f"vgate_check#{file_obj_id}"
            )
        ]
    )
    buttons.append([InlineKeyboardButton("‹ Back to Results", callback_data="vgate_back")])
    return InlineKeyboardMarkup(buttons)


def _retry_markup(file_obj_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔄 Retry Verification", callback_data=f"vgate_check#{file_obj_id}")],
            [InlineKeyboardButton("‹ Back to Results", callback_data="vgate_back")],
        ]
    )


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

    async with interactive_slot("verification"):
        evaluation = await _collect_outstanding_gates(client, user.id)
    gate_result, missing = evaluation.result, evaluation.missing
    if gate_result.status is VerificationStatus.INDETERMINATE:
        message = verification_unavailable_message(gate_result)
        if isinstance(event, CallbackQuery):
            await event.message.reply_text(message, reply_markup=_retry_markup(file_obj_id))
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
        # Keep the results visible behind this actionable verification card.
        await event.message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
    else:
        await event.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)

    return False


@Client.on_callback_query(filters.regex(r"^vgate_check#"))
async def vgate_check_callback(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    if not (await enforce_user_action(callback, "verification")).allowed:
        return
    file_obj_id = callback.data.split("#", 1)[1]
    user_id = callback.from_user.id

    async with interactive_slot("verification_callback"):
        evaluation = await _collect_outstanding_gates(client, user_id)
    gate_result, missing = evaluation.result, evaluation.missing
    if gate_result.status is VerificationStatus.INDETERMINATE:
        try:
            await callback.message.edit_text(
                verification_unavailable_message(gate_result),
                reply_markup=_retry_markup(file_obj_id),
            )
        except Exception:
            pass
        return
    if gate_result.status is VerificationStatus.DENY:
        try:
            await callback.message.edit_text(
                f"🔐 <b>Still missing {len(missing)} channel(s)</b>\n\n"
                f"<blockquote>Tap each one below, then hit "
                f"<b>✅ Continue</b>.</blockquote>",
                reply_markup=_gates_markup(missing, file_obj_id),
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
        return

    await _persist_passed_gate_state(user_id, evaluation.passed_due_gates)

    try:
        await callback.message.delete()
    except Exception:
        pass
    await _deliver_file(client, callback.message.chat.id, user_id, file_obj_id)


@Client.on_callback_query(filters.regex(r"^vgate_back$"))
async def vgate_back_callback(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    try:
        await callback.message.delete()
    except Exception:
        pass
