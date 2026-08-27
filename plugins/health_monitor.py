"""
Background tasks started from bot.py:
  - Health monitor — pings clusters every 10 min, alerts on issues
"""
import asyncio
import datetime
import logging
import os
import time
from pathlib import Path

from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from database.db import db
from plugins.filter import send_smart_log
from plugins.workload import workload_snapshot
from plugins.telegram_retry import (
    BACKGROUND_RETRY,
    INTERACTIVE_RETRY,
    telegram_call,
    telegram_retry_snapshot,
)
from utils import ADMIN_ID, report_internal_error

logger = logging.getLogger(__name__)

# Tracks last alert per issue so we don't spam the log channel
_last_alert = {}
_ALERT_COOLDOWN = 3600  # 1 hour between repeat alerts for same issue
MAX_DELETION_ATTEMPTS = 8
_DELETION_ALREADY_GONE_ERRORS = {
    "MessageEmpty",
    "MessageIdInvalid",
    "MsgIdInvalid",
}
_DELETION_PERMANENT_ERRORS = {
    "ChannelPrivate",
    "ChatAdminRequired",
    "MessageDeleteForbidden",
    "MessageTooOld",
    "PeerIdInvalid",
}


def classify_deletion_error(error: Exception) -> str:
    names = {cls.__name__ for cls in type(error).__mro__}
    if names & _DELETION_ALREADY_GONE_ERRORS:
        return "already_gone"
    if names & _DELETION_PERMANENT_ERRORS:
        return "permanent"
    return "transient"


def deletion_retry_delay(attempts: int, retry_after: int | None = None) -> int:
    delay = min(6 * 3600, 30 * (2 ** min(max(0, attempts), 10)))
    return max(delay, max(0, int(retry_after or 0)))


async def _should_alert(key: str) -> bool:
    """Returns True only if we haven't alerted about this issue recently."""
    last = _last_alert.get(key, 0)
    if time.time() - last > _ALERT_COOLDOWN:
        _last_alert[key] = time.time()
        return True
    return False


async def _clear_alert(key: str):
    """Clears an alert key so recovery is reported even within cooldown."""
    _last_alert.pop(key, None)


async def check_all_channels(client, config):
    """
    Shared channel health check — called by the admin panel's manual
    Channel Health Check button (the automatic 10-min run_health_monitor()
    below checks cluster/indexer health instead, not channels).

    Returns a list of {"label", "ok", "fix", "text"} dicts rather than
    plain strings, so the caller can build a "🔧 Fix" button straight to
    the right admin.py edit flow for anything that isn't OK, instead of
    just reporting the failure and making the admin go find the fix
    themselves. "ok" is True/False/None (None = not configured at all,
    treated the same as a failure for "should this get a fix button").
    """
    results = []

    async def _check(label, ch_id, fix=None):
        if not ch_id or ch_id in [0, "", None]:
            return {"label": label, "ok": None, "fix": fix,
                    "text": f"{label}: ⚪ Not configured"}
        try:
            ch = int(ch_id) if str(ch_id).lstrip('-').isdigit() else str(ch_id)
            async def _read_channel():
                await client.get_chat(ch)
                return await client.get_chat_member(ch, client.me.id)

            member = await telegram_call(
                _read_channel,
                route="channel_health",
                policy=INTERACTIVE_RETRY,
                retry_safe=True,
                idempotency_key=f"channel-health:{ch}",
            )
            status = member.status.name
            if status == "ADMINISTRATOR":
                return {"label": label, "ok": True, "fix": fix,
                        "text": f"{label}: ✅ Admin — `{ch_id}`"}
            elif status == "MEMBER":
                return {"label": label, "ok": False, "fix": fix,
                        "text": f"{label}: ⚠️ Member only — `{ch_id}`"}
            else:
                return {"label": label, "ok": False, "fix": fix,
                        "text": f"{label}: ❓ Status `{status}` — `{ch_id}`"}
        except FloodWait as e:
            return {"label": label, "ok": False, "fix": fix,
                    "text": f"{label}: ⏳ Rate-limited ({e.value}s), retry later — `{ch_id}`"}
        except Exception as error:
            reference = report_internal_error(
                logger, "channel_health", error, channel_id=ch_id
            )
            return {"label": label, "ok": False, "fix": fix,
                    "text": f"{label}: ❌ No access — `{ch_id}`\n  Reference: `{reference}`"}

    results.append(await _check("📡 Log Channel",    config.get("log_channel"), fix="edit_logchannel"))
    results.append(await _check("📢 Update Channel", config.get("update_channel_id"), fix="edit_updatechid"))

    for i, ch in enumerate(config.get("db_channels", []), 1):
        results.append(await _check(f"📚 DB Channel {i}", ch, fix="db_chan_menu"))

    for i, entry in enumerate(config.get("fsub_channels", []), 1):
        ch_id = entry.get("id") if isinstance(entry, dict) else entry
        results.append(await _check(f"🔐 FSub {i}", ch_id, fix="fsub_menu"))

    return results


async def check_known_issues(client, config):
    """
    Live operational checks for the admin panel's "⚠️ Known Issues" tile —
    unlike run_health_monitor() (which only alerts the log channel when
    something breaks), this is pull-based: an admin can check current
    status any time instead of waiting to be alerted or discovering a
    problem by accident. Returns a list of {"label", "ok", "text"} dicts,
    "ok" is True/False/None (None = informational, not a problem).
    """
    findings = []

    # ── Cluster capacity ──────────────────────────────────────────────────────
    for i, db_instance in enumerate(db.dbs):
        try:
            size_mb = await db.get_db_size(db_instance)
        except Exception:
            continue
        if size_mb >= 450:
            findings.append({
                "label": f"Cluster {i+1}", "ok": False,
                "text": f"🛑 Cluster {i+1} is at its 450MB safety margin (`{size_mb:.0f} MB`) "
                        f"— new saves will skip it. Add `DATABASE_URI_{i+2}`."
            })
        elif size_mb >= 400:
            findings.append({
                "label": f"Cluster {i+1}", "ok": False,
                "text": f"⚠️ Cluster {i+1} is approaching its safety margin (`{size_mb:.0f} MB` / 450 MB)."
            })

    # ── Whitelist mode with nothing whitelisted ───────────────────────────────
    if config.get("group_whitelist_mode", "blacklist") == "whitelist":
        try:
            groups = await db.get_all_groups()
            if not any(g.get("whitelisted") for g in groups):
                findings.append({
                    "label": "Group Whitelist", "ok": False,
                    "text": "⚠️ Whitelist mode is ON but no group is whitelisted — the bot "
                            "will leave every group it's added to. Approve groups in Group Manager."
                })
        except Exception:
            pass

    # ── Verification gate stacking ────────────────────────────────────────────
    gates_active = []
    if config.get("fsub_channels"):
        gates_active.append("Main FSub")
    if config.get("req_fsub_channels"):
        gates_active.append("Request-FSub")
    if len([c for c in config.get("two_stage_channels", []) if c]) >= 2:
        gates_active.append("Two-Stage Verification")
    if len(gates_active) >= 2:
        findings.append({
            "label": "Verification Gates", "ok": None,
            "text": f"ℹ️ {len(gates_active)} verification gates are active at once "
                    f"({', '.join(gates_active)}) — a new user may face multiple "
                    f"join-and-confirm steps for one file."
        })

    # ── Stale indexer tasks ───────────────────────────────────────────────────
    try:
        stale = await db.get_stale_index_tasks(older_than_seconds=7200)
        if stale:
            stale_ids = [s["_id"] for s in stale]
            findings.append({
                "label": "Indexer", "ok": False,
                "text": f"⚠️ Indexer task(s) stuck \"running\" for 2h+: `{stale_ids}` — may have crashed."
            })
    except Exception:
        pass

    # ── TMDB key ───────────────────────────────────────────────────────────────
    if not (os.getenv("TMDB_BEARER_TOKEN") or os.getenv("TMDB_API_READ_TOKEN")):
        findings.append({
            "label": "TMDB", "ok": None,
            "text": "ℹ️ `TMDB_BEARER_TOKEN` is not set — new-upload announcements will post without a poster/rating."
        })

    if not findings:
        findings.append({"label": "All clear", "ok": True, "text": "✅ No known issues detected."})

    return findings


async def run_health_monitor(client):
    """
    Runs every 10 minutes.
    Checks:
      1. Each MongoDB cluster is reachable
      2. No indexer task has been stuck in 'running' for >2 hours
    Sends green heartbeat every 6 hours when all is well.
    """
    while True:
        await asyncio.sleep(600)  # 10 minutes

        ready_file = Path(os.getenv("SESSION_WORKDIR", "runtime")) / "ready"
        try:
            await asyncio.to_thread(ready_file.touch)
        except OSError as exc:
            logger.warning("Could not refresh readiness marker: %s", type(exc).__name__)

        issues = []
        logger.info("workload_metrics %s", workload_snapshot())
        logger.info("telegram_retry_metrics %s", telegram_retry_snapshot())
        try:
            logger.info(
                "notification_outbox_depth %s",
                await db.notification_outbox_depth(),
            )
        except Exception as exc:
            logger.warning(
                "Could not read notification outbox depth: %s",
                type(exc).__name__,
            )

        # ── 1. Ping all clusters ──────────────────────────────────────────────
        for i, db_instance in enumerate(db.dbs):
            key = f"cluster_{i+1}_down"
            try:
                await db_instance.command("ping")
                # If we previously alerted about this cluster, send recovery notice
                if key in _last_alert:
                    await _clear_alert(key)
                    await send_smart_log(
                        client,
                        f"✅ **#ClusterRecovered**\n\nCluster {i+1} is back online."
                    )
            except Exception as error:
                reference = report_internal_error(
                    logger, "cluster_health", error, cluster=i + 1
                )
                issues.append(f"Cluster {i+1} unreachable ({reference})")
                if await _should_alert(key):
                    await send_smart_log(
                        client,
                        f"🚨 **#HealthAlert — Cluster {i+1} Down**\n\n"
                        f"MongoDB Cluster {i+1} is not responding.\n"
                        f"Reference: `{reference}`"
                    )

        # ── 2. Check for stale indexer tasks ──────────────────────────────────
        key = "stale_indexer"
        try:
            stale = await db.get_stale_index_tasks(older_than_seconds=7200)
            if stale:
                stale_ids = [s["_id"] for s in stale]
                issues.append(f"Stale indexer tasks: {stale_ids}")
                if await _should_alert(key):
                    await send_smart_log(
                        client,
                        f"⚠️ **#HealthAlert — Stale Indexer**\n\n"
                        f"The following indexer tasks have been running for >2 hours "
                        f"and may have crashed:\n`{stale_ids}`\n\n"
                        f"Use /admin to stop them manually."
                    )
            else:
                _last_alert.pop(key, None)
        except Exception:
            pass

        # Repair registry location metadata independently of ingestion. A
        # successful physical insert must never be repeated merely because
        # its best-effort location update failed.
        key = "registry_reconciliation"
        try:
            registry_stats = await db.reconcile_registry_locations(limit=250)
            if registry_stats["repaired"]:
                logger.info(
                    "Registry reconciliation repaired %s/%s checked entries",
                    registry_stats["repaired"],
                    registry_stats["checked"],
                )
            if registry_stats["unresolved"]:
                issues.append(
                    f"Registry entries unresolved: {registry_stats['unresolved']}"
                )
                if await _should_alert(key):
                    await send_smart_log(
                        client,
                        f"⚠️ **#HealthAlert — Registry Reconciliation**\n\n"
                        f"`{registry_stats['unresolved']}` of "
                        f"`{registry_stats['checked']}` checked registry entries "
                        f"could not be matched to a physical file.",
                    )
            else:
                _last_alert.pop(key, None)
        except Exception as exc:
            logger.warning(
                "Registry reconciliation failed: %s", type(exc).__name__
            )

        # Heartbeat removed — was noise in log channel


async def run_cache_reaper():
    """
    Sweeps the in-process search-session cache every 5 minutes. _SearchCache
    normally evicts lazily (on read) or via the LRU cap, so a burst of unique
    searches followed by a long quiet period would otherwise leave stale
    sessions resident in memory until something else happens to touch the
    cache. This is the timer-driven backstop for that gap.
    """
    while True:
        await asyncio.sleep(300)
        try:
            db.purge_caches()
            logger.info("cache_metrics %s", db.cache_metrics())
        except Exception as e:
            logger.warning(f"Cache reaper error: {e}")


async def _dead_letter_deletion(client, job, error, *, permanent):
    dead_letter_id = await db.dead_letter_deletion(job, error, permanent)
    logger.error(
        "Deletion dead-lettered job=%s chat=%s message=%s error_type=%s",
        dead_letter_id,
        job.get("chat_id"),
        job.get("message_id"),
        type(error).__name__,
    )
    await send_smart_log(
        client,
        "🚨 **#DeletionDeadLetter**\n\n"
        f"Chat: `{job.get('chat_id')}`\n"
        f"Message: `{job.get('message_id')}`\n"
        f"Attempts: `{int(job.get('attempts', 0)) + 1}`\n"
        f"Error: `{type(error).__name__}`\n\n"
        "The job was retained and can be retried below.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "Retry deletion",
                callback_data=f"retry_deletion#{dead_letter_id}",
            )
        ]]),
    )
    return "dead_lettered"


async def process_deletion_job(client, job):
    """Process one durable deletion job and retain every terminal failure."""
    try:
        await telegram_call(
            lambda: client.delete_messages(job["chat_id"], job["message_id"]),
            route="scheduled_deletion",
            policy=BACKGROUND_RETRY,
            retry_safe=True,
            idempotency_key=f"delete:{job['chat_id']}:{job['message_id']}",
        )
        await db.complete_deletion(job["_id"])
        return "completed"
    except FloodWait as error:
        attempts = int(job.get("attempts", 0))
        if attempts + 1 >= MAX_DELETION_ATTEMPTS:
            return await _dead_letter_deletion(
                client, job, error, permanent=False
            )
        await db.retry_deletion(
            job["_id"], deletion_retry_delay(attempts, error.value)
        )
        return "retry_scheduled"
    except Exception as error:
        classification = classify_deletion_error(error)
        if classification == "already_gone":
            await db.complete_deletion(job["_id"])
            return "completed_absent"
        attempts = int(job.get("attempts", 0))
        if classification == "permanent" or attempts + 1 >= MAX_DELETION_ATTEMPTS:
            return await _dead_letter_deletion(
                client,
                job,
                error,
                permanent=classification == "permanent",
            )
        await db.retry_deletion(job["_id"], deletion_retry_delay(attempts))
        return "retry_scheduled"


@Client.on_callback_query(
    filters.regex(r"^retry_deletion#") & filters.user(ADMIN_ID)
)
async def retry_dead_letter_callback(client: Client, callback: CallbackQuery):
    job_id = callback.data.split("#", 1)[1]
    retried = await db.retry_dead_letter_deletion(job_id)
    if not retried:
        await callback.answer("Deletion job is missing or was already retried.", show_alert=True)
        return
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.answer("Deletion queued for retry.", show_alert=True)


async def run_deletion_worker(client):
    """Process durable Telegram message deletions in bounded batches."""
    while True:
        try:
            jobs = await db.get_due_deletions(limit=100)
            if not jobs:
                await asyncio.sleep(5)
                continue
            for job in jobs:
                try:
                    await process_deletion_job(client, job)
                except Exception as error:
                    logger.error(
                        "Deletion job processing failed without state loss job=%s: %s",
                        job.get("_id"),
                        error,
                    )
                    try:
                        await db.retry_deletion(
                            job["_id"],
                            deletion_retry_delay(int(job.get("attempts", 0))),
                        )
                    except Exception:
                        logger.exception(
                            "Could not defer deletion job after processing failure"
                        )
        except Exception as e:
            logger.error("Deletion worker iteration failed: %s", e)
            await asyncio.sleep(10)
