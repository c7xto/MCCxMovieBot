"""Backfill indexed search tokens without loading movie libraries into memory.

Usage:
    python tools/migrate_search_tokens.py          # dry-run counts
    python tools/migrate_search_tokens.py --apply  # update and create indexes
"""

import argparse
import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import UpdateOne

from database.db import db, search_tokens_for_name


BATCH_SIZE = 1000
logger = logging.getLogger("migrate_search_tokens")


async def migrate_cluster(number, collection, apply_changes):
    query = {"search_tokens": {"$exists": False}}
    if not apply_changes:
        missing = await collection.count_documents(query)
        logger.info("Cluster %s: %s rows require migration", number, f"{missing:,}")
        return missing

    changed = 0
    operations = []
    cursor = collection.find(query, {"file_name": 1}).batch_size(BATCH_SIZE)
    async for document in cursor:
        operations.append(UpdateOne(
            {"_id": document["_id"], "search_tokens": {"$exists": False}},
            {"$set": {
                "search_tokens": search_tokens_for_name(document.get("file_name", ""))
            }},
        ))
        if len(operations) >= BATCH_SIZE:
            result = await collection.bulk_write(operations, ordered=False)
            changed += result.modified_count
            operations = []
            logger.info("Cluster %s: migrated %s rows", number, f"{changed:,}")
    if operations:
        result = await collection.bulk_write(operations, ordered=False)
        changed += result.modified_count
    await collection.create_index("search_tokens")
    logger.info("Cluster %s complete: %s rows migrated", number, f"{changed:,}")
    return changed


async def main(apply_changes):
    if not db.file_cols:
        raise RuntimeError("No DATABASE_URI values are configured.")
    counts = await asyncio.gather(*[
        migrate_cluster(number, collection, apply_changes)
        for number, collection in enumerate(db.file_cols, 1)
    ])
    total = sum(counts)
    if apply_changes:
        if await db.search_tokens_need_migration():
            raise RuntimeError("Migration ended with unprocessed movie rows; rerun it.")
        logger.info("Migration verified complete: %s rows updated", f"{total:,}")
    else:
        logger.info(
            "Dry run: %s rows need updates. Rerun with --apply to migrate.",
            f"{total:,}",
        )
    for client in db.clients:
        await client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(main(args.apply))
