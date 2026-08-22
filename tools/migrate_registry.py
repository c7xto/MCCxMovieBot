"""
tools/migrate_registry.py

One-time backfill migration for the centralized `file_registry` collection.

Background: `database/db.py`'s save_file()/save_files_bulk() now treat
`file_registry` (on the operations cluster when available) as the single source of truth for
cross-cluster file_id uniqueness, replacing the old per-cluster existence
checks. That only works going forward unless every file_id already sitting
in the ~1.5M existing documents across all configured clusters is backfilled
into the registry first — this script does that backfill.

Design notes:
  - Idempotent & safe to re-run: registry inserts use the same unique index
    save_file() relies on, and duplicate-key errors here are expected, not
    failures — a file_id already registered (by an earlier run of this
    script, or by a fresh save_file() call that landed concurrently) is
    silently skipped.
  - Safe to run against a live bot: this script only reads from the movies
    collections and writes additively to file_registry. It never touches
    existing file documents, so it can run alongside normal bot traffic
    with no downtime and no locking.
  - Memory-efficient: each cluster is scanned with a single streaming async
    cursor projected down to {"file_id": 1}, and only a bounded batch (up to
    BATCH_SIZE file_ids) is ever held in memory per cluster at once — not
    the full ~1.5M-document result set.
  - Concurrent: all configured clusters are scanned in parallel via
    asyncio.gather, each with its own cursor and its own batching, so total
    wall-clock time is bounded by the slowest cluster, not the sum of all of
    them.

Usage:
    python tools/migrate_registry.py
"""

import os
import sys
import time
import asyncio
import logging

# Allow running as `python tools/migrate_registry.py` from any working
# directory by putting the repo root (one level above this file) on
# sys.path, so `from database.db import db` resolves correctly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import InsertOne
from pymongo.errors import BulkWriteError

from database.db import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("migrate_registry")

BATCH_SIZE      = 5000    # file_ids per bulk_write to file_registry
PROGRESS_EVERY  = 100_000  # log a progress line at least this often
MAX_BATCH_RETRIES = 5


async def _flush_batch(batch: list) -> tuple:
    """Bulk-inserts a batch of file_ids into file_registry.
    Returns (inserted_count, duplicate_count). Duplicate-key errors (code
    11000) are expected — both from cross-cluster collisions this backfill
    is meant to surface, and from re-running this script — and are not
    treated as failures. Any other error is logged and the batch is simply
    not counted, safely retryable on a future run."""
    if not batch:
        return 0, 0

    ops = [InsertOne({"file_id": fid}) for fid in batch]
    for attempt in range(1, MAX_BATCH_RETRIES + 1):
        try:
            result = await db.registry_col.bulk_write(ops, ordered=False)
            return result.inserted_count, 0
        except BulkWriteError as bwe:
            write_errors = bwe.details.get("writeErrors", [])
            dup_count = sum(1 for e in write_errors if e.get("code") == 11000)
            other_errors = [e for e in write_errors if e.get("code") != 11000]
            if not other_errors:
                return bwe.details.get("nInserted", 0), dup_count
            error = RuntimeError(
                f"{len(other_errors)} non-duplicate write error(s); "
                f"sample: {other_errors[0].get('errmsg', '')[:150]}"
            )
        except Exception as exc:
            error = exc

        if attempt == MAX_BATCH_RETRIES:
            logger.error(
                "Batch flush failed after %s attempts (%s ids): %s",
                MAX_BATCH_RETRIES,
                len(batch),
                error,
            )
            raise error

        delay = min(2 ** (attempt - 1), 15)
        logger.warning(
            "Batch flush attempt %s/%s failed (%s ids); retrying in %ss: %s",
            attempt,
            MAX_BATCH_RETRIES,
            len(batch),
            delay,
            error,
        )
        await asyncio.sleep(delay)

    raise RuntimeError("unreachable batch retry state")


async def _scan_cluster(cluster_idx: int, col, progress: dict, lock: asyncio.Lock, total_estimate: int, start: float):
    """Streams every file_id out of one cluster's movies collection,
    batching and flushing into file_registry as it goes. Runs concurrently
    with the other clusters via asyncio.gather in main()."""
    batch = []

    cursor = col.find({"file_id": {"$exists": True, "$ne": ""}}, {"_id": 0, "file_id": 1})
    async for doc in cursor:
        fid = doc.get("file_id")
        if not fid:
            continue
        batch.append(fid)

        if len(batch) >= BATCH_SIZE:
            await _drain(batch, progress, lock, total_estimate, start)
            batch = []

    if batch:
        await _drain(batch, progress, lock, total_estimate, start)

    logger.info(f"✅ Cluster {cluster_idx + 1} scan complete.")


async def _drain(batch: list, progress: dict, lock: asyncio.Lock, total_estimate: int, start: float):
    inserted, duplicates = await _flush_batch(batch)
    n = len(batch)

    async with lock:
        prev_scanned = progress["scanned"]
        progress["scanned"]    += n
        progress["inserted"]   += inserted
        progress["duplicates"] += duplicates
        scanned = progress["scanned"]

        # Log whenever we cross a PROGRESS_EVERY boundary, regardless of
        # which cluster's batch happened to trigger it.
        if scanned // PROGRESS_EVERY > prev_scanned // PROGRESS_EVERY:
            _log_progress(progress, total_estimate, start)


def _log_progress(progress: dict, total_estimate: int, start: float):
    elapsed = time.time() - start
    rate    = progress["scanned"] / elapsed if elapsed > 0 else 0
    logger.info(
        f"Processed {progress['scanned']:,} / {total_estimate:,} files... "
        f"({progress['inserted']:,} newly registered, {progress['duplicates']:,} duplicates, "
        f"{rate:,.0f} docs/sec)"
    )


async def _build_and_verify_registry_index():
    """Builds the unique index on file_registry.file_id and explicitly
    verifies it actually took — via index_information(), not just trusting
    create_index() not to have raised — before any cursor/batch work
    starts. This is a pre-migration lock: at this point file_registry is
    either empty or only holds data from a prior run of this exact script,
    so index creation cannot itself fail on a duplicate-key conflict here.
    What this guards against is the index silently not being what we think
    it is (e.g. a same-named index that already exists with different
    options, which create_index() would reject) — discovering that after
    streaming a large fraction of ~1.5M documents would be a far more
    expensive way to find out than checking it up front."""
    logger.info("📑 Building unique index on file_registry.file_id...")
    try:
        await db.registry_col.create_index("file_id", unique=True)
    except Exception as e:
        logger.error(
            f"❌ Could not build the unique index on file_registry.file_id: {e}\n"
            f"   This usually means an index with the same name already exists with "
            f"different options. Inspect it with db.file_registry.getIndexes() in the "
            f"Mongo shell, drop/fix it, then re-run this script."
        )
        sys.exit(1)

    indexes = await db.registry_col.index_information()
    is_unique = any(
        spec.get("unique") and ("file_id", 1) in spec.get("key", [])
        for spec in indexes.values()
    )
    if not is_unique:
        logger.error(
            "❌ Index verification failed — file_registry.file_id does not have a "
            "unique index after create_index() returned. Aborting before any batch "
            "inserts start, rather than risk silently accepting cross-cluster "
            "duplicates for the entire run."
        )
        sys.exit(1)

    logger.info("✅ Verified: file_registry.file_id has a unique index. Safe to proceed.")


async def main():
    if not db.file_cols:
        logger.error("No database clusters configured (DATABASE_URI / DATABASE_URI_2..5) — aborting.")
        sys.exit(1)

    if db.registry_col is None:
        logger.error("No main_db configured — file_registry unavailable — aborting.")
        sys.exit(1)

    # Pre-migration indexing lock — must complete and verify successfully
    # before the asynchronous cursor batch loops below are allowed to start.
    await _build_and_verify_registry_index()

    logger.info(f"🔢 Counting documents across {len(db.file_cols)} cluster(s)...")
    counts = await asyncio.gather(*[col.count_documents({}) for col in db.file_cols])
    total_estimate = sum(counts)
    for i, c in enumerate(counts):
        logger.info(f"   Cluster {i + 1}: {c:,} documents")
    logger.info(f"📊 Estimated {total_estimate:,} total documents to scan.")

    progress = {"scanned": 0, "inserted": 0, "duplicates": 0}
    lock     = asyncio.Lock()
    start    = time.time()

    await asyncio.gather(*[
        _scan_cluster(i, col, progress, lock, total_estimate, start)
        for i, col in enumerate(db.file_cols)
    ])

    _log_progress(progress, total_estimate, start)
    elapsed = time.time() - start
    logger.info(
        f"🎉 Migration complete in {elapsed:.1f}s — "
        f"{progress['scanned']:,} scanned, "
        f"{progress['inserted']:,} newly registered, "
        f"{progress['duplicates']:,} already-registered duplicates skipped "
        f"(cross-cluster collisions + files already covered by a prior run)."
    )


if __name__ == "__main__":
    async def _run():
        try:
            await main()
        finally:
            await db.close()

    asyncio.run(_run())
