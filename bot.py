import os
import sys
import fcntl
import asyncio
import logging
from pyrogram import Client
from dotenv import load_dotenv
from database.db import db
from plugins.health_monitor import run_health_monitor

load_dotenv()

# ── Single instance lock — kills the startup if another copy is already running
_lock_file = open("/tmp/mccxbot.lock", "w")
try:
    fcntl.flock(_lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
except IOError:
    print("❌ Another instance of MCCxBot is already running. Exiting.")
    sys.exit(1)

# ── Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Suppress noisy third-party logs
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pyrogram.session.session").setLevel(logging.ERROR)
logging.getLogger("pyrogram.connection.connection").setLevel(logging.ERROR)
logging.getLogger("pyrogram.session.auth").setLevel(logging.ERROR)
logging.getLogger("motor").setLevel(logging.ERROR)
logging.getLogger("pymongo").setLevel(logging.ERROR)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)

API_ID    = int(os.getenv("API_ID", 0))
API_HASH  = os.getenv("API_HASH")
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
        logger.info(f"🚀 Bot started as @{me.username}")

        logger.info("🔌 Validating MongoDB connections...")
        if not db.dbs:
            logger.critical("💥 No MongoDB clusters connected! Check DATABASE_URI in .env")
        else:
            for i, db_instance in enumerate(db.dbs):
                try:
                    await db_instance.command("ping")
                    logger.info(f"  ✅ Cluster {i+1} — OK")
                except Exception as e:
                    logger.error(f"  ❌ Cluster {i+1} — FAILED: {e}")

        logger.info("🔄 Syncing .env config → MongoDB...")
        await db.sync_config()

        logger.info("🧹 Clearing stale indexer tasks...")
        await db.clear_all_index_tasks()

        logger.info("🧹 Clearing old search sessions...")
        await db.clear_old_searches(expiry_seconds=0)

        logger.info("📑 Ensuring database indexes...")
        await db.ensure_indexes()

        logger.info("✅ Bot fully ready.")

        asyncio.create_task(run_health_monitor(self))
        logger.info("✅ Health monitor started.")

    async def stop(self, *args):
        await super().stop()
        logger.info("🛑 Bot stopped.")


if __name__ == "__main__":
    bot = AutoFilterBot()
    bot.run()
