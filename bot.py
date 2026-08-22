import os
import sys
import asyncio
import logging
import tempfile
from dotenv import load_dotenv

load_dotenv()

# ── Logging setup — configured before anything else runs so the startup
# verification block below has somewhere to log to.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def _verify_environment():
    """Fail-fast startup checks that must pass before Pyrogram is imported
    or any database cluster connection is attempted.

    Runs two checks:
      1. Python 3.14 asyncio event loop patch — Python 3.14 removed the
         implicit loop-creation fallback that asyncio.get_event_loop() used
         to rely on, so calling it on a fresh thread with no running loop
         now raises RuntimeError instead of silently creating one. Pyrogram
         calls get_event_loop() at import time, so on 3.14 `import pyrogram`
         crashes unless a loop has already been registered first. This is
         empirically confirmed against this host's Python 3.14 install, not
         assumed.
      2. Cryptographic / CA bundle sanity — certifi.where() must resolve to
         a real, non-empty CA bundle file, since database/db.py uses it as
         tlsCAFile for secured MongoDB cluster connections. A missing or
         empty bundle would surface later as an opaque TLS handshake failure
         deep inside the first MongoDB connection attempt instead of here.

    Any failure logs a diagnostic and exits immediately (sys.exit(1)) rather
    than letting the bot partially boot into an unstable state.
    """
    logger.info("🔎 Verifying startup environment (event loop + TLS/CA bundle)...")

    # 1. Python 3.14 asyncio event loop patch — must happen before `import
    # pyrogram` anywhere below.
    try:
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())
        logger.info("  ✅ asyncio event loop — OK")
    except Exception as e:
        logger.error(f"  ❌ Failed to patch the asyncio event loop for Python 3.14: {e}")
        sys.exit(1)

    # 2. Cryptographic & CA bundle sanity check.
    try:
        import certifi
        ca_path = certifi.where()
        if not os.path.isfile(ca_path) or os.path.getsize(ca_path) == 0:
            raise RuntimeError(f"certifi CA bundle missing or empty at {ca_path}")
        logger.info(f"  ✅ certifi CA bundle — OK ({ca_path})")
    except Exception as e:
        logger.error(
            f"  ❌ TLS/CA bundle environment check failed: {e}\n"
            f"     mongodb+srv:// connections require a valid CA bundle "
            f"(pip install --upgrade certifi)."
        )
        sys.exit(1)

    required = {
        "API_ID": os.getenv("API_ID"),
        "API_HASH": os.getenv("API_HASH"),
        "BOT_TOKEN": os.getenv("BOT_TOKEN"),
        "DATABASE_URI": os.getenv("DATABASE_URI"),
        "ADMIN_ID": os.getenv("ADMIN_ID"),
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        logger.critical("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)
    try:
        int(required["API_ID"])
        [int(value.strip()) for value in required["ADMIN_ID"].split(",") if value.strip()]
    except ValueError:
        logger.critical("API_ID and every ADMIN_ID value must be numeric.")
        sys.exit(1)
    logger.info("✅ Startup environment verified.")


_verify_environment()

from pyrogram import Client
from database.db import db
from plugins.health_monitor import (
    run_health_monitor, run_cache_reaper, run_deletion_worker, _log_task_crash,
)

# ── Cross-platform single-instance lock ──────────────────────────────────────
_lock_path = os.path.join(tempfile.gettempdir(), "mccxbot.lock")
_lock_file = open(_lock_path, "a+")
_lock_file.seek(0)
_lock_file.write("0")
_lock_file.flush()
try:
    if os.name == "nt":
        import msvcrt
        _lock_file.seek(0)
        msvcrt.locking(_lock_file.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl
        fcntl.flock(_lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
except OSError:
    logger.critical("Another MCCxBot instance is already running.")
    sys.exit(1)

# Suppress noisy third-party logs
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pyrogram.session.session").setLevel(logging.ERROR)
logging.getLogger("pyrogram.connection.connection").setLevel(logging.ERROR)
logging.getLogger("pyrogram.session.auth").setLevel(logging.ERROR)
logging.getLogger("pymongo").setLevel(logging.ERROR)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)

API_ID    = int(os.getenv("API_ID", 0))
API_HASH  = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")


class AutoFilterBot(Client):
    def __init__(self):
        self.background_tasks = []
        super().__init__(
            name="MCCxBot",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            plugins=dict(root="plugins"),
            sleep_threshold=60,
            max_concurrent_transmissions=3
        )

    async def start(self, *args, **kwargs):
        """Start through Kurigram while preserving its runtime options.

        ``Client.run()`` forwards keyword arguments such as ``use_qr`` and
        ``except_ids`` to ``start()``.  Accepting and forwarding them keeps
        this override compatible with current Kurigram releases.
        """
        await super().start(*args, **kwargs)
        me = await self.get_me()
        logger.info(f"🚀 Bot started as @{me.username}")

        logger.info("🔌 Validating MongoDB connections...")
        if not db.dbs:
            await super().stop()
            raise RuntimeError("No MongoDB clusters configured. Check DATABASE_URI.")
        else:
            for i, db_instance in enumerate(db.dbs):
                try:
                    await db_instance.command("ping")
                    logger.info(f"  ✅ Cluster {i+1} — OK")
                except Exception as e:
                    if i == 0:
                        await super().stop()
                        raise RuntimeError(f"Primary MongoDB cluster is unavailable: {e}") from e
                    logger.warning(f"  ⚠️ Optional cluster {i+1} unavailable: {e}")

        logger.info("🔄 Migrating legacy control data → operations database...")
        await db.migrate_legacy_control_data()

        logger.info("🔄 Syncing .env config → MongoDB...")
        await db.sync_config()

        logger.info("🧹 Clearing stale indexer tasks...")
        await db.clear_all_index_tasks()

        logger.info("🧹 Clearing old search sessions...")
        await db.clear_old_searches(expiry_seconds=0)

        logger.info("📑 Ensuring database indexes...")
        await db.ensure_indexes()

        if await db.registry_needs_migration():
            await super().stop()
            raise RuntimeError(
                "Existing movie files were found but file_registry is empty. "
                "Run `python tools/migrate_registry.py` once before starting the bot."
            )

        logger.info("✅ Bot fully ready.")

        health_task = asyncio.create_task(run_health_monitor(self))
        health_task.add_done_callback(lambda t: _log_task_crash(t, self, "run_health_monitor"))
        self.background_tasks.append(health_task)
        logger.info("✅ Health monitor started.")

        reaper_task = asyncio.create_task(run_cache_reaper())
        reaper_task.add_done_callback(lambda t: _log_task_crash(t, self, "run_cache_reaper"))
        self.background_tasks.append(reaper_task)
        logger.info("✅ Search-cache reaper started.")

        deletion_task = asyncio.create_task(run_deletion_worker(self))
        deletion_task.add_done_callback(lambda t: _log_task_crash(t, self, "run_deletion_worker"))
        self.background_tasks.append(deletion_task)
        logger.info("✅ Durable deletion worker started.")

    async def stop(self, *args):
        for task in self.background_tasks:
            task.cancel()
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
            self.background_tasks.clear()
        await super().stop()
        await db.close()
        logger.info("🛑 Bot stopped.")


if __name__ == "__main__":
    bot = AutoFilterBot()
    bot.run()
