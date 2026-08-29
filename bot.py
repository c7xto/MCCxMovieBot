import os
import sys
import asyncio
import logging
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Logging setup — configured before anything else runs so the startup
# verification block below has somewhere to log to.
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
from plugins.log_safety import install_log_redaction

install_log_redaction()
logger = logging.getLogger(__name__)


def _verify_environment():
    """Fail-fast startup checks that must pass before Pyrogram is imported
    or any database cluster connection is attempted.

    Runs two checks:
      1. Python 3.13+ asyncio event loop patch — register a loop before
         importing Pyrogram without calling the deprecated synchronous
         asyncio.get_event_loop() fallback.
      2. Cryptographic / CA bundle sanity — certifi.where() must resolve to
         a real, non-empty CA bundle file, since database/db.py uses it as
         tlsCAFile for secured MongoDB cluster connections. A missing or
         empty bundle would surface later as an opaque TLS handshake failure
         deep inside the first MongoDB connection attempt instead of here.

    Any failure logs a diagnostic and exits immediately (sys.exit(1)) rather
    than letting the bot partially boot into an unstable state.
    """
    logger.info("🔎 Verifying startup environment (event loop + TLS/CA bundle)...")

    # 1. Python 3.13+ asyncio event loop patch — must happen before `import
    # pyrogram` anywhere below. get_running_loop() is warning-free when no
    # loop has been registered yet, unlike get_event_loop() on Python 3.13.
    try:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())
        logger.info("  ✅ asyncio event loop — OK")
    except Exception as e:
        logger.error(f"  ❌ Failed to prepare the asyncio event loop: {e}")
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

    service_role = os.getenv("SERVICE_ROLE", "all-in-one").strip().casefold()
    required = {
        "API_ID": os.getenv("API_ID"),
        "API_HASH": os.getenv("API_HASH"),
        "BOT_TOKEN": os.getenv("BOT_TOKEN"),
        "DATABASE_URI": os.getenv("DATABASE_URI"),
        "OPERATIONS_DATABASE_URI": os.getenv("OPERATIONS_DATABASE_URI"),
        "ADMIN_ID": os.getenv("ADMIN_ID"),
    }
    if service_role != "all-in-one":
        required["REDIS_URL"] = os.getenv("REDIS_URL")
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

PROJECT_ROOT = Path(__file__).resolve().parent
from plugins.process_lock import (
    AlreadyRunningError,
    ProcessLockError,
    acquire_process_lock,
    prepare_private_runtime_dir,
)

try:
    SESSION_WORKDIR = prepare_private_runtime_dir(
        Path(os.getenv("SESSION_WORKDIR") or PROJECT_ROOT / "runtime")
    )
    _lock_file = acquire_process_lock(SESSION_WORKDIR)
except AlreadyRunningError:
    logger.critical("Another MCCxBot instance is already running.")
    sys.exit(1)
except ProcessLockError as error:
    logger.critical("Secure runtime lock initialization failed: %s", error)
    sys.exit(1)

from pyrogram import Client
from database.db import db
from database.index_policy import RequiredIndexError
from database.redis_client import redis_state
from plugins.health_monitor import (
    run_health_monitor,
    run_cache_reaper,
    run_deletion_worker,
)
from plugins.task_supervisor import supervisor
from tmdb import close_tmdb_client, start_tmdb_client

# Suppress noisy third-party logs
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pyrogram.session.session").setLevel(logging.ERROR)
logging.getLogger("pyrogram.connection.connection").setLevel(logging.ERROR)
logging.getLogger("pyrogram.session.auth").setLevel(logging.ERROR)
logging.getLogger("pymongo").setLevel(logging.ERROR)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)

API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
SERVICE_ROLE = os.getenv("SERVICE_ROLE", "all-in-one").strip().casefold()
SERVICE_ROLES = {
    "all-in-one",
    "bot-interactive",
    "worker-broadcast",
    "worker-indexer",
    "worker-maintenance",
}
if SERVICE_ROLE not in SERVICE_ROLES:
    logger.critical("Unknown SERVICE_ROLE=%s", SERVICE_ROLE)
    sys.exit(1)
READY_FILE = SESSION_WORKDIR / "ready"


def _write_ready_marker():
    READY_FILE.parent.mkdir(parents=True, exist_ok=True)
    READY_FILE.write_text(str(time.time()), encoding="ascii")


def _remove_ready_marker():
    READY_FILE.unlink(missing_ok=True)


class AutoFilterBot(Client):
    def __init__(self):
        SESSION_WORKDIR.mkdir(parents=True, exist_ok=True)
        super().__init__(
            name=f"MCCxBot-{SERVICE_ROLE}",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            plugins=dict(root="plugins")
            if SERVICE_ROLE in {"all-in-one", "bot-interactive"}
            else None,
            no_updates=SERVICE_ROLE not in {"all-in-one", "bot-interactive"},
            sleep_threshold=10,
            max_concurrent_transmissions=3,
            workdir=str(SESSION_WORKDIR),
        )

    async def start(self, *args, use_qr=False, except_ids=None, **kwargs):
        """Start through Kurigram while preserving its runtime options.

        Kurigram 2.2.25 forwards ``use_qr`` and ``except_ids`` from run() to
        start(). Keep them explicit so an outdated deployed entry file is
        immediately visible and future keyword options still pass through.
        """
        from plugins.readiness import mark_not_ready, mark_ready

        mark_not_ready()
        await redis_state.start()
        if redis_state.shared:
            logger.info("✅ Redis shared state plane ready.")
        else:
            logger.info(
                "✅ Local temporary state ready (single-process mode; Redis is optional)."
            )
        logger.info("🔌 Validating MongoDB connections before Telegram startup...")
        if not db.dbs:
            raise RuntimeError("No MongoDB clusters configured. Check DATABASE_URI.")
        else:
            for i, db_instance in enumerate(db.dbs):
                try:
                    await db_instance.command("ping")
                    db.mark_shard_reachable(i, "startup_ping_ok")
                    logger.info(f"  ✅ Cluster {i + 1} — OK")
                except Exception as e:
                    db.mark_shard_error(i, e)
                    if i == 0:
                        raise RuntimeError(f"Primary MongoDB cluster is unavailable: {e}") from e
                    logger.warning(f"  ⚠️ Optional cluster {i + 1} unavailable: {e}")

            # Capture capacity and finish read/write routing before any user
            # traffic or background catalog scan starts. Probes run in
            # parallel, so one optional outage costs only its bounded timeout.
            await db.probe_shards()
            logger.info("🧭 MongoDB shard routing ready: %s", db.shard_health_snapshot())

        if db.operations_db is not None:
            try:
                await db.operations_db.command("ping")
                logger.info("  ✅ Dedicated operations database — OK")
            except Exception as e:
                raise RuntimeError(f"Dedicated operations database is unavailable: {e}") from e
        else:
            raise RuntimeError("OPERATIONS_DATABASE_URI is required and was not initialized.")

        logger.info("🔄 Migrating legacy control data → operations database...")
        await db.migrate_legacy_control_data()

        migrated_gates = await db.migrate_access_gates()
        if migrated_gates:
            logger.info("✅ Unified %s access gate(s); legacy fields retained.", migrated_gates)

        logger.info("🔄 Syncing .env config → MongoDB...")
        await db.sync_config()
        await db.get_config()  # Pin a last-known-good snapshot before accepting users.

        logger.info("🧭 Recovering interrupted indexer tasks...")
        recovered_indexes = await db.recover_index_tasks_on_startup()
        if recovered_indexes:
            logger.warning(
                "Requeued %s expired indexer lease(s); checkpoints were retained.",
                recovered_indexes,
            )

        logger.info("🧹 Clearing old search sessions...")
        await db.clear_old_searches(expiry_seconds=0)

        logger.info("📑 Ensuring database indexes...")
        try:
            await db.ensure_indexes()
        except RequiredIndexError:
            raise

        if await db.registry_needs_migration():
            raise RuntimeError(
                "Existing movie files were found but file_registry is empty. "
                "Run `python tools/migrate_registry.py` once before starting the bot."
            )

        try:
            await db._recover_duplicate_cleanup_registry()
        except Exception as error:
            logger.warning(
                "Interrupted duplicate-cleanup registry repair was deferred: %s",
                type(error).__name__,
            )

        db._search_tokens_complete = not await db.search_tokens_need_migration()
        if not db._search_tokens_complete:
            logger.warning(
                "Legacy movie rows are missing search_tokens; using the bounded "
                "keyset compatibility search. New files use the indexed engine."
            )
        if not await db.analytics_counters_ready():
            logger.warning(
                "Language analytics counters are not initialized. Run "
                "`python tools/migrate_search_tokens.py --apply` while the bot remains online."
            )

        # Only now connect the Kurigram client.  All required persistence and
        # indexes are ready before the dispatcher can receive an update.
        await super().start(
            *args,
            use_qr=use_qr,
            except_ids=except_ids if except_ids is not None else [],
            **kwargs,
        )
        me = await self.get_me()
        logger.info("🚀 %s started as @%s", SERVICE_ROLE, me.username)
        if SERVICE_ROLE in {"all-in-one", "bot-interactive", "worker-maintenance"}:
            await start_tmdb_client()

        logger.info("✅ Bot fully ready.")
        await asyncio.to_thread(_write_ready_marker)
        mark_ready()

        supervisor.start_accepting()
        supervisor.spawn(self._ready_heartbeat(), key="worker:ready", owner=SERVICE_ROLE)

        if SERVICE_ROLE in {"all-in-one", "worker-broadcast"}:
            from plugins.broadcast import run_broadcast_worker

            supervisor.spawn(run_broadcast_worker(self), key="worker:broadcast", owner=SERVICE_ROLE)
            logger.info("✅ Durable broadcast worker started.")
        if SERVICE_ROLE in {"all-in-one", "worker-indexer"}:
            from plugins.bulk_indexer import run_indexer_worker

            supervisor.spawn(run_indexer_worker(self), key="worker:indexer", owner=SERVICE_ROLE)
            logger.info("✅ Durable indexer worker started.")
        if SERVICE_ROLE in {"all-in-one", "worker-maintenance"}:
            from plugins.realtime_indexer import run_notification_worker

            supervisor.spawn(
                db.ensure_search_catalog(),
                key="worker:search_catalog",
                owner=SERVICE_ROLE,
                resources=("search-catalog",),
                drain_on_shutdown=True,
            )
            supervisor.spawn(run_health_monitor(self), key="worker:health", owner=SERVICE_ROLE)
            supervisor.spawn(run_cache_reaper(), key="worker:cache_reaper", owner=SERVICE_ROLE)
            supervisor.spawn(run_deletion_worker(self), key="worker:deletion", owner=SERVICE_ROLE)
            supervisor.spawn(
                run_notification_worker(self),
                key="worker:announcement-outbox",
                owner=SERVICE_ROLE,
            )
            logger.info("✅ Maintenance workers started.")

    async def _ready_heartbeat(self):
        while True:
            await asyncio.to_thread(_write_ready_marker)
            await asyncio.sleep(60)

    async def stop(self, *args, **kwargs):
        from plugins.readiness import mark_not_ready

        mark_not_ready()
        await asyncio.to_thread(_remove_ready_marker)
        await supervisor.shutdown(drain_timeout=10, cancel_timeout=10)
        await close_tmdb_client()
        await super().stop(*args, **kwargs)
        await db.close()
        await redis_state.close()
        _lock_file.close()
        logger.info("🛑 Bot stopped.")


if __name__ == "__main__":
    bot = AutoFilterBot()
    bot.run()
