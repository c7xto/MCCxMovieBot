"""
Background tasks started from bot.py:
  - Health monitor — pings clusters every 10 min, alerts on issues
"""
import os
import asyncio
import logging
import time
import datetime
from pyrogram.errors import FloodWait
from database.db import db
from plugins.filter import send_smart_log

logger = logging.getLogger(__name__)

# Tracks last alert per issue so we don't spam the log channel
_last_alert = {}
_ALERT_COOLDOWN = 3600  # 1 hour between repeat alerts for same issue


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


def _log_task_crash(task: asyncio.Task, client, label: str):
    """Attach via task.add_done_callback(...) on any fire-and-forget
    asyncio.create_task(...) so an unhandled exception is surfaced to the
    log channel instead of dying silently — by default it's only visible
    via asyncio's own "Task exception was never retrieved" stderr log,
    which nobody is watching."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc:
        logger.error(f"{label} crashed: {exc}")
        asyncio.create_task(send_smart_log(
            client,
            f"💥 **#BackgroundTaskCrashed**\n\n🏷 **Task:** `{label}`\n🛑 **Error:** `{exc}`"
        ))


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
            await client.get_chat(ch)
            member = await client.get_chat_member(ch, client.me.id)
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
            await asyncio.sleep(e.value)
            return {"label": label, "ok": False, "fix": fix,
                    "text": f"{label}: ⏳ Rate-limited, retry health check — `{ch_id}`"}
        except Exception as e:
            return {"label": label, "ok": False, "fix": fix,
                    "text": f"{label}: ❌ No access — `{ch_id}`\n  _({str(e)[:60]})_"}

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
    if not os.getenv("TMDB_API_KEY"):
        findings.append({
            "label": "TMDB", "ok": None,
            "text": "ℹ️ `TMDB_API_KEY` is not set — new-upload announcements will post without a poster/rating."
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

        issues = []

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
            except Exception as e:
                issues.append(f"Cluster {i+1} unreachable: {e}")
                if await _should_alert(key):
                    await send_smart_log(
                        client,
                        f"🚨 **#HealthAlert — Cluster {i+1} Down**\n\n"
                        f"MongoDB Cluster {i+1} is not responding.\n"
                        f"Error: `{str(e)[:100]}`"
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
            db._search_cache.purge(db._search_cache.default_ttl)
        except Exception as e:
            logger.warning(f"Cache reaper error: {e}")


