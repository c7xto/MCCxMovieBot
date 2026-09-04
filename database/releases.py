"""Durable live ingestion and release publications, isolated from movie shards.

No TTL is applied to publication identities: a later encode must edit the same
post even months later. Claims carry fencing tokens; expired sends are held for
review rather than automatically replayed.
"""

import hashlib
import time
import uuid

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError


def digest(value):
    return hashlib.sha256(str(value).encode()).hexdigest()[:32]


class ReleaseStore:
    def __init__(self, operations):
        self.sources = operations["live_sources"]
        self.receipts = operations["live_receipts"]
        self.candidates = operations["release_candidates"]
        self.posts = operations["release_posts"]
        self.members = operations["release_files"]

    async def ensure_indexes(self):
        for collection in (self.receipts, self.candidates, self.posts):
            await collection.create_index([("state", 1), ("due", 1), ("lease_until", 1)])
        await self.receipts.create_index([("source", 1), ("message", 1)], unique=True)
        await self.members.create_index([("release", 1), ("file_key", 1)], unique=True)
        await self.members.create_index([("release", 1), ("order", 1)])
        await self.candidates.create_index([("identity", 1), ("state", 1)])
        await self.sources.create_index("lease_until")

    async def watch(self, source):
        await self.sources.update_one(
            {"_id": source},
            {
                "$setOnInsert": {
                    "activated": time.time(),
                    "checkpoint": None,
                    "frontier": None,
                    "lease_until": 0,
                    "last_seen": None,
                }
            },
            upsert=True,
        )

    async def receive(self, source, message, media, *, uploaded_at, live=True):
        await self.watch(source)
        watch = await self.sources.find_one({"_id": source})
        announce = live and uploaded_at >= watch["activated"]
        now = time.time()
        receipt_id = f"{source}:{message}"
        await self.receipts.update_one(
            {"_id": receipt_id},
            {
                "$setOnInsert": {
                    "source": source,
                    "message": message,
                    "media": media,
                    "announce": announce,
                    "state": "pending" if media else "ignored",
                    "due": now,
                    "lease_until": 0,
                    "attempts": 0,
                    "received": now,
                }
            },
            upsert=True,
        )
        # Set the activation baseline once. Never infer an entire channel's
        # older history from the first arrival, or announce the old library.
        await self.sources.update_one(
            {"_id": source, "checkpoint": None},
            {
                "$set": {"checkpoint": message - 1},
            },
        )
        await self.sources.update_one(
            {"_id": source},
            {
                "$max": {"frontier": message},
                **({"$set": {"last_seen": now}} if live else {}),
            },
        )
        return receipt_id

    async def claim(self, collection, states, *, extra=None, seconds=180):
        now = time.time()
        query = {"state": {"$in": states}, "due": {"$lte": now}, "lease_until": {"$lte": now}}
        query.update(extra or {})
        return await collection.find_one_and_update(
            query,
            {
                "$set": {"lease_until": now + seconds, "claim": uuid.uuid4().hex},
                "$inc": {"attempts": 1},
            },
            sort=[("due", 1)],
            return_document=ReturnDocument.AFTER,
        )

    async def finish(self, collection, job, fields):
        return await collection.update_one(
            {"_id": job["_id"], "claim": job["claim"]},
            {
                "$set": {**fields, "lease_until": 0},
            },
        )

    async def retry(self, collection, job, error, delay=None):
        delay = delay if delay is not None else min(3600, 5 * 2 ** min(job.get("attempts", 1), 9))
        await self.finish(
            collection,
            job,
            {
                "due": time.time() + delay,
                "error": type(error).__name__,
            },
        )

    async def enqueue_candidate(self, receipt, parsed, file_ref):
        # One candidate per physical file, not per channel message. A retry
        # after save-before-enqueue repairs the handoff without adding a copy.
        key = digest(f"{parsed['identity']}:{file_ref['file_key']}")
        await self.candidates.update_one(
            {"_id": key},
            {
                "$setOnInsert": {
                    "identity": parsed["identity"],
                    "parsed": parsed,
                    "file": file_ref,
                    "state": "pending",
                    "due": time.time(),
                    "lease_until": 0,
                    "attempts": 0,
                    "arrived": receipt["received"],
                }
            },
            upsert=True,
        )

    async def attach(self, candidate, metadata, destination):
        parsed = candidate["parsed"]
        season = parsed.get("season")
        identity = f"{destination}:{metadata['kind']}:{metadata['id']}:{season}"
        key = digest(identity)
        now = time.time()
        await self.posts.update_one(
            {"_id": key},
            {
                "$setOnInsert": {
                    "destination": destination,
                    "metadata": metadata,
                    "season": season,
                    "state": "pending",
                    "created": now,
                    "last_arrival": now,
                    "due": now + 90,
                    "lease_until": 0,
                    "revision": 0,
                    "message_id": None,
                    "last_edit": 0,
                    "attempts": 0,
                }
            },
            upsert=True,
        )
        member_id = digest(f"{key}:{candidate['file']['file_key']}")
        episode = min(parsed["episodes"], default=0)
        size = min(999999999999, max(0, candidate["file"].get("file_size", 0)))
        await self.members.update_one(
            {"_id": member_id},
            {
                "$setOnInsert": {
                    "release": key,
                    **candidate["file"],
                    "order": f"{episode:04d}:{999999999999 - size:012d}:{member_id}",
                    "languages": parsed["languages"],
                    "qualities": parsed["qualities"],
                    "episodes": parsed["episodes"],
                }
            },
            upsert=True,
        )
        # Replaying this update can cause a harmless no-op edit attempt. It
        # cannot inflate file counts (membership is unique) or create a post.
        await self.posts.update_one(
            {"_id": key},
            [
                {
                    "$set": {
                        "revision": {"$add": ["$revision", 1]},
                        "last_arrival": now,
                        "due": {
                            "$cond": [
                                {"$ne": ["$message_id", None]},
                                {"$max": [now, {"$add": ["$last_edit", 300]}]},
                                {"$min": [now + 90, {"$add": ["$created", 300]}]},
                            ]
                        },
                    }
                },
            ],
        )
        await self.finish(self.candidates, candidate, {"state": "resolved", "release": key, "error": ""})
        return key

    async def summary(self, key):
        languages, qualities, episodes = set(), set(), set()
        count = 0
        async for item in self.members.find(
            {"release": key},
            {
                "languages": 1,
                "qualities": 1,
                "episodes": 1,
            },
        ).batch_size(200):
            count += 1
            languages.update(item.get("languages", []))
            qualities.update(item.get("qualities", []))
            episodes.update(item.get("episodes", []))
        return {
            "languages": sorted(languages),
            "qualities": sorted(qualities),
            "episodes": sorted(episodes),
            "count": count,
        }

    async def begin_send(self, job):
        result = await self.posts.update_one(
            {
                "_id": job["_id"],
                "claim": job["claim"],
                "state": "pending",
            },
            {"$set": {"state": "sending"}},
        )
        return result.modified_count == 1

    async def published(self, job, message_id, photo, caption_hash):
        now = time.time()
        # A concurrent arrival must remain due; never overwrite its revision.
        await self.posts.update_one(
            {"_id": job["_id"], "claim": job["claim"]},
            [
                {
                    "$set": {
                        "state": "posted",
                        "message_id": message_id,
                        "photo": photo,
                        "caption_hash": caption_hash,
                        "last_edit": now,
                        "lease_until": 0,
                        "published_revision": job["revision"],
                        "error": "",
                        "due": {
                            "$cond": [{"$eq": ["$revision", job["revision"]]}, now + 365 * 86400, now + 300]
                        },
                    }
                },
            ],
        )

    async def recover_uncertain(self):
        await self.posts.update_many(
            {"state": "sending", "lease_until": {"$lte": time.time()}},
            {
                "$set": {"state": "uncertain", "error": "SendOutcomeUnknown"},
            },
        )

    async def claim_source(self, allowed):
        now = time.time()
        return await self.sources.find_one_and_update(
            {
                "_id": {"$in": allowed},
                "frontier": {"$ne": None},
                "lease_until": {"$lte": now},
                "$expr": {"$lt": ["$checkpoint", "$frontier"]},
            },
            {"$set": {"lease_until": now + 180, "claim": uuid.uuid4().hex}},
            return_document=ReturnDocument.AFTER,
        )

    async def status(self, sources):
        result = {"sources": [row async for row in self.sources.find({"_id": {"$in": sources}})]}
        for name, states, collection in (
            ("indexed", ["done"], self.receipts),
            ("pending", ["pending"], self.receipts),
            ("review", ["review"], self.candidates),
            ("queued", ["pending"], self.posts),
            ("uncertain", ["uncertain", "blocked"], self.posts),
        ):
            result[name] = await collection.count_documents({"state": {"$in": states}})
        result["retrying"] = await self.receipts.count_documents(
            {"state": "pending", "error": {"$exists": True}}
        )
        return result
