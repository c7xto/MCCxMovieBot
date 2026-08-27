"""Dry-run-first repair for the required file_registry.file_id unique index.

Usage:
    python tools/repair_registry_index.py
    python tools/repair_registry_index.py --apply

Only the derived registry collection is changed. Movie rows are never deleted.
"""

import argparse
import asyncio
import logging
import os
import sys


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db import db
from database.index_policy import ensure_required_unique_index


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("repair_registry_index")


async def repair_registry_index(apply: bool):
    if db.registry_col is None:
        raise RuntimeError("file_registry is unavailable; check DATABASE_URI settings")

    pipeline = [
        {"$match": {"file_id": {"$exists": True, "$ne": ""}}},
        {"$group": {
            "_id": "$file_id",
            "count": {"$sum": 1},
            "claims": {"$push": {
                "_id": "$_id",
                "cluster": "$cluster",
                "movie_id": "$movie_id",
            }},
        }},
        {"$match": {"count": {"$gt": 1}}},
    ]
    duplicate_groups = duplicate_claims = 0
    cursor = await db.registry_col.aggregate(pipeline, allowDiskUse=True)
    async for group in cursor:
        duplicate_groups += 1
        claims = group["claims"]
        duplicate_claims += len(claims) - 1
        if apply:
            keep = max(
                claims,
                key=lambda claim: (
                    bool(claim.get("cluster") and claim.get("movie_id")),
                    -claim["_id"].generation_time.timestamp(),
                ),
            )
            await db.registry_col.delete_many({
                "_id": {"$in": [
                    claim["_id"] for claim in claims if claim["_id"] != keep["_id"]
                ]}
            })

    indexes = await db.registry_col.index_information()
    conflicting = [
        name
        for name, spec in indexes.items()
        if spec.get("key") == [("file_id", 1)] and spec.get("unique") is not True
    ]
    logger.info(
        "Registry diagnosis: %s duplicate group(s), %s extra claim(s), conflicts=%s",
        duplicate_groups,
        duplicate_claims,
        conflicting,
    )
    if not apply:
        logger.info("Dry run only. Re-run with --apply to repair the derived registry.")
        return

    for index_name in conflicting:
        await db.registry_col.drop_index(index_name)
    await ensure_required_unique_index(
        db.registry_col,
        "file_id",
        "file_registry.file_id",
    )
    stats = await db.reconcile_registry_locations(limit=10_000)
    logger.info("Repair complete; reconciliation=%s", stats)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply repairs. Without this flag the command is read-only.",
    )
    args = parser.parse_args()
    try:
        await repair_registry_index(args.apply)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
