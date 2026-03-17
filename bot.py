import os
import asyncio
import logging
from pyrogram import Client
from dotenv import load_dotenv
from database.db import db
from plugins.health_monitor import run_health_monitor, run_birthday_check

# load_dotenv() in bot.py covers API_ID, API_HASH, BOT_TOKEN at module level.
# db.py has its own load_dotenv() for DATABASE_URI — both are needed.
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")


class AutoFilterBot(Client):
    def __init__(self):
        super().__init__(
            name="MCCxBot",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            plugins=dict(root="plugins"),
            sleep_threshold=60,
            max_concurrent_transmissions=3
        )

    async def start(self):
        await super().start()
        me = await self.get_me()
        logger.info(f"🚀 Bot started successfully as @{me.username}!")

        # ── 1. MONGODB CONNECTION VALIDATOR ───────────────────────────────────
        logger.info("🔌 Validating MongoDB connections...")
        if not db.dbs:
            logger.critical(
                "💥 FATAL: No MongoDB clusters connected! "
                "Check DATABASE_URI in your .env is a single unbroken line."
            )
        else:
            for i, db_instance in enumerate(db.dbs):
                try:
                    await db_instance.command("ping")
                    logger.info(f"  ✅ Cluster {i+1} — connected OK")
                except Exception as e:
                    logger.error(f"  ❌ Cluster {i+1} — FAILED: {e}")

        # ── 2. SYNC .env VALUES → MONGODB (one-time migration) ────────────────
        # Writes LOG_CHANNEL_ID, DATABASE_CHANNEL_ID, UPDATE_CHANNEL,
        # UPDATE_CHANNEL_LINK, MAIN_GROUP_LINK from .env into MongoDB
        # only if they don't already exist there. Never overwrites DB values.
        logger.info("🔄 Syncing .env config → MongoDB...")
        await db.sync_config()

        # ── 3. CLEAR STALE INDEXER STATES ────────────────────────────────────
        # If the bot crashed mid-index, the state stays "running" in MongoDB
        # forever and blocks new index attempts. Wipe them on every clean start.
        logger.info("🧹 Clearing stale indexer tasks...")
        await db.clear_all_index_tasks()

        # ── 4. WIPE EXPIRED SEARCH SESSIONS ──────────────────────────────────
        # Sessions from before a restart are useless — the inline buttons in
        # those old messages will show "Session Expired" anyway.
        logger.info("🧹 Clearing old search sessions...")
        await db.clear_old_searches(expiry_seconds=0)

        # ── 5. ENSURE FILE_NAME INDEXES ON ALL CLUSTERS ───────────────────────
        logger.info("📑 Ensuring database indexes...")
        await db.ensure_indexes()

        logger.info("✅ Bot fully ready.")

        # ── 6. START BACKGROUND TASKS ─────────────────────────────────────────
        # A10: Health monitor — pings clusters every 10 min, alerts on issues
        asyncio.create_task(run_health_monitor(self))
        # B5: Birthday broadcast — checks once daily
        asyncio.create_task(run_birthday_check(self))
        logger.info("✅ Background tasks started (health monitor + birthday check).")

    async def stop(self, *args):
        await super().stop()
        logger.info("🛑 Bot stopped.")


if __name__ == "__main__":
    bot = AutoFilterBot()
    bot.run()
