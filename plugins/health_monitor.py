"""
Background tasks started from bot.py:
  - A10: Health monitor — pings clusters, checks stale indexers, sends alerts
  - B5: Birthday broadcast — checks daily for users with birthday today
"""
import asyncio
import logging
import time
import datetime
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
    last_heartbeat = 0.0

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

        # ── 3. Green heartbeat every 6 hours when everything is fine ──────────
        now = time.time()
        if not issues and (now - last_heartbeat) >= 21600:
            last_heartbeat = now
            cluster_count = len(db.dbs)
            await send_smart_log(
                client,
                f"✅ **#Heartbeat — All Systems Normal**\n\n"
                f"🟢 Clusters online: `{cluster_count}/{cluster_count}`\n"
                f"⏱ Next check: 10 minutes"
            )


async def run_birthday_check(client):
    """
    B5: Runs once daily at the configured time (default midnight UTC).
    Checks for users with birthday today and sends them a greeting.
    """
    while True:
        # Calculate seconds until next midnight UTC
        now = datetime.datetime.utcnow()
        next_midnight = (now + datetime.timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        sleep_seconds = (next_midnight - now).total_seconds()
        await asyncio.sleep(sleep_seconds)

        # Check configured birthday time — default midnight, configurable
        try:
            config = await db.get_config()
            birthday_hour = int(config.get("birthday_hour", 0))
            birthday_minute = int(config.get("birthday_minute", 0))
        except Exception:
            birthday_hour, birthday_minute = 0, 0

        # Sleep extra if birthday time is not midnight
        extra = birthday_hour * 3600 + birthday_minute * 60
        if extra > 0:
            await asyncio.sleep(extra)

        # Find users with birthday today
        today = datetime.datetime.utcnow()
        today_mm_dd = f"{today.month:02d}-{today.day:02d}"

        try:
            if db.users_col is None:
                continue

            cursor = db.users_col.find({"birthday": {"$regex": f"-{today_mm_dd}$"}})
            birthday_users = [doc["_id"] async for doc in cursor]

            if not birthday_users:
                continue

            logger.info(f"🎂 Birthday check: {len(birthday_users)} user(s) today")

            sent, failed = 0, 0
            for user_id in birthday_users:
                try:
                    await client.send_message(
                        chat_id=user_id,
                        text=(
                            "🎂 **Happy Birthday!**\n\n"
                            "Wishing you a wonderful day from all of us at MCCxBot! 🎉\n\n"
                            "Enjoy some movies today 🍿"
                        )
                    )
                    sent += 1
                    await asyncio.sleep(0.05)  # Respect rate limits
                except Exception:
                    failed += 1

            await send_smart_log(
                client,
                f"🎂 **#BirthdayBroadcast**\n\n"
                f"Sent birthday greetings to `{sent}` users.\n"
                f"Failed: `{failed}`"
            )

        except Exception as e:
            logger.error(f"Birthday check error: {e}")
