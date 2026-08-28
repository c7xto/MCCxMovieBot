"""
tools/verify_db_performance.py

Standalone read-only verification for database connectivity, indexes,
search and bounded cache behavior. Run it from the project's Python 3.13
environment so it exercises the same imports used by the bot.

Usage:
    python tools/verify_db_performance.py
"""

import os
import sys
import time
import asyncio
import logging

# Windows consoles default to a non-UTF-8 codepage, which mangles the
# status emoji below. Force UTF-8 on stdout/stderr where supported (Python
# 3.7+) so output is readable regardless of host terminal configuration.
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(encoding="utf-8")
        except Exception:
            pass

# Allow running as `python tools/verify_db_performance.py` from any working
# directory by putting the repo root (one level above this file) on
# sys.path, so `from database.db import db` resolves correctly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

EXPECTED_LANGUAGES = [
    "Malayalam", "Tamil", "Telugu", "Hindi",
    "English", "Kannada", "Dual Audio", "Multi Audio"
]

from database.db import db  # noqa: E402
from database.redis_client import redis_state  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("verify_db_performance")

RESULTS = []  # (section, name, status, detail) — status in {"PASS", "FAIL", "SKIP"}


def record(section, name, status, detail=""):
    RESULTS.append((section, name, status, detail))
    icon = {"PASS": "✅", "FAIL": "❌", "SKIP": "⏭️"}[status]
    logger.info(f"{icon} [{section}] {name}" + (f" — {detail}" if detail else ""))


def _pf(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


def _section(title):
    logger.info("")
    logger.info(f"=== {title} ===")


# ── 1. Cache Mechanics Validation ────────────────────────────────────────

async def test_cache_mechanics():
    section = "Cache Mechanics"
    _section(section)
    try:
        await redis_state.start()
        sample_payload = {
            "results": [{"file_id": "f1", "file_name": "Movie.mkv"}],
            "query": "test movie",
        }
        await redis_state.set_json("verify:search-session", sample_payload, ttl=30)
        restored = await redis_state.get_json("verify:search-session")
        record(
            section,
            "Redis search-session JSON round-trips",
            _pf(restored == sample_payload),
        )
        first_claim = await redis_state.claim_once("verify:callback", ttl=30)
        second_claim = await redis_state.claim_once("verify:callback", ttl=30)
        record(
            section,
            "Redis callback deduplication is atomic",
            _pf(first_claim and not second_claim),
        )
        await redis_state.delete("verify:search-session", "verify:callback")
    except Exception as exc:
        record(section, "Redis ephemeral-state backend reachable", "FAIL", str(exc)[:300])


# ── 2. Aggregation Pipeline Health ───────────────────────────────────────

async def test_aggregation_pipeline_health():
    section = "Aggregation Pipeline"
    _section(section)

    if db.analytics_col is None:
        record(section, "get_files_by_language()", "SKIP", "operations database not configured")
        return

    try:
        start = time.time()
        results = await db.get_files_by_language()
        elapsed = time.time() - start

        expected_keys = set(EXPECTED_LANGUAGES)
        got_keys = set(results.keys())
        record(
            section, "get_files_by_language() returns every expected language key",
            _pf(got_keys == expected_keys),
            f"missing={expected_keys - got_keys or None}, unexpected={got_keys - expected_keys or None}"
        )
        record(
            section, "get_files_by_language() values are non-negative ints",
            _pf(all(isinstance(v, int) and v >= 0 for v in results.values())),
            f"{elapsed:.3f}s — {results}"
        )
    except Exception as e:
        record(section, "get_files_by_language() completes without error", "FAIL", str(e)[:300])


# ── 3. Registry & Traversal Sanity Check ────────────────────────────────

async def test_search_traversal():
    section = "Registry & Search Traversal"
    _section(section)

    if not db.file_cols:
        record(section, "get_search_results()", "SKIP", "no clusters configured")
        return

    # 3a. Shards need an efficient file_id lookup index, while the centralized
    # registry owns uniqueness across every shard. Legacy shard indexes may be
    # non-unique and should not be rebuilt during normal startup.
    for i, col in enumerate(db.file_cols):
        try:
            indexes = await col.index_information()
            has_file_id_index = any(
                ("file_id", 1) in spec.get("key", [])
                for spec in indexes.values()
            )
            record(
                section, f"Cluster {i+1}: file_id lookup index present",
                _pf(has_file_id_index),
                f"indexes={list(indexes.keys())}"
            )
        except Exception as e:
            record(section, f"Cluster {i+1}: index_information() reachable", "FAIL", str(e)[:200])

    if db.registry_col is not None:
        try:
            registry_indexes = await db.registry_col.index_information()
            has_unique_registry = any(
                spec.get("unique") and ("file_id", 1) in spec.get("key", [])
                for spec in registry_indexes.values()
            )
            record(
                section,
                "central file registry has a unique file_id index",
                _pf(has_unique_registry),
                f"indexes={list(registry_indexes.keys())}",
            )

            registry_count = await db.registry_col.estimated_document_count()
            shard_count = sum(
                await asyncio.gather(
                    *(col.estimated_document_count() for col in db.file_cols)
                )
            )
            record(
                section,
                "central registry covers every sharded movie document",
                _pf(registry_count == shard_count),
                f"registry={registry_count:,}, shards={shard_count:,}",
            )
        except Exception as e:
            record(section, "central registry verification", "FAIL", str(e)[:300])
    else:
        record(section, "central registry configured", "FAIL")

    # 3b. Confirm the bounded natural-order compatibility query runs cleanly
    # per cluster and returns well-formed documents.
    for i, col in enumerate(db.file_cols):
        try:
            docs = await col.find({}).sort("$natural", -1).limit(3).to_list(length=3)
            shape_ok = all(("file_id" in d and "file_name" in d) for d in docs) if docs else True
            record(
                section, f"Cluster {i+1}: natural-order probe query",
                _pf(shape_ok),
                f"{len(docs)} doc(s) sampled"
            )
        except Exception as e:
            record(section, f"Cluster {i+1}: natural-order probe query", "FAIL", str(e)[:200])

    # 3c. Run the real get_search_results() end-to-end with a mock query and
    # validate the returned payload shape/types.
    try:
        start = time.time()
        results = await db.get_search_results("test", max_results=5)
        elapsed = time.time() - start
        shape_ok = isinstance(results, list) and all(
            isinstance(r, dict) and "file_id" in r and "file_name" in r for r in results
        )
        record(
            section, "get_search_results() returns a well-formed payload",
            _pf(shape_ok),
            f"{len(results)} result(s) in {elapsed:.3f}s"
        )
    except Exception as e:
        record(section, "get_search_results() completes without error", "FAIL", str(e)[:300])


# ── Main ──────────────────────────────────────────────────────────────────

async def main():
    logger.info("Running read-only database and search checks.")
    logger.info(f"Configured clusters: {len(db.file_cols)}")

    if db.file_cols:
        _section("Cluster Connectivity")
        for i, db_instance in enumerate(db.dbs):
            try:
                await db_instance.command("ping")
                record("Cluster Connectivity", f"Cluster {i+1} reachable", "PASS")
            except Exception as e:
                record("Cluster Connectivity", f"Cluster {i+1} reachable", "FAIL", str(e)[:200])
    else:
        logger.warning(
            "No DATABASE_URI[/_2../_5] configured in this environment — "
            "cache mechanics will still be verified, but aggregation and "
            "search checks will be reported as SKIP, not PASS."
        )

    await test_cache_mechanics()
    await test_aggregation_pipeline_health()
    await test_search_traversal()

    for client in db.clients:
        try:
            await client.close()
        except Exception:
            pass
    await redis_state.close()

    _section("SUMMARY")
    passed  = sum(1 for _, _, s, _ in RESULTS if s == "PASS")
    failed  = sum(1 for _, _, s, _ in RESULTS if s == "FAIL")
    skipped = sum(1 for _, _, s, _ in RESULTS if s == "SKIP")
    for sec, name, s, detail in RESULTS:
        if s == "FAIL":
            logger.info(f"  ❌ [{sec}] {name} — {detail}")
    logger.info(f"{passed} passed, {failed} failed, {skipped} skipped ({len(RESULTS)} total checks).")
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
    except Exception:
        logger.exception("verify_db_performance.py crashed before completing.")
        exit_code = 2
    sys.exit(exit_code)
