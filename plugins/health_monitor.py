"""
Background tasks started from bot.py:
  - A10: Health monitor — pings clusters every 10 min, alerts on issues
"""
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
    Shared channel health check — called by both the automatic monitor
    and the admin panel's manual Channel Health Check button.
    Returns a list of status strings.
    """
    results = []

    async def _check(label, ch_id):
        if not ch_id or ch_id in [0, "", None]:
            return f"{label}: ⚪ Not configured"
        try:
            ch = int(ch_id) if str(ch_id).lstrip('-').isdigit() else str(ch_id)
            await client.get_chat(ch)
            member = await client.get_chat_member(ch, client.me.id)
            status = member.status.name
            if status == "ADMINISTRATOR":
                return f"{label}: ✅ Admin — `{ch_id}`"
            elif status == "MEMBER":
                return f"{label}: ⚠️ Member only — `{ch_id}`"
            else:
                return f"{label}: ❓ Status `{status}` — `{ch_id}`"
        except FloodWait as e:
            await asyncio.sleep(e.value)
            return f"{label}: ⏳ Rate-limited, retry health check — `{ch_id}`"
        except Exception as e:
            return f"{label}: ❌ No access — `{ch_id}`\n  _({str(e)[:60]})_"

    results.append(await _check("📡 Log Channel",    config.get("log_channel")))
    results.append(await _check("📢 Update Channel", config.get("update_channel_id")))

    for i, ch in enumerate(config.get("db_channels", []), 1):
        results.append(await _check(f"📚 DB Channel {i}", ch))

    for i, entry in enumerate(config.get("fsub_channels", []), 1):
        ch_id = entry.get("id") if isinstance(entry, dict) else entry
        results.append(await _check(f"🔐 FSub {i}", ch_id))

    return results


async def run_health_monitor(client):
    """
    A10: Runs every 10 minutes.
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


