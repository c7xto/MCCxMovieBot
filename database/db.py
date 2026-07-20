import os
import re
import time
import json
import asyncio
import logging
from collections import OrderedDict
from functools import lru_cache
from bson.objectid import ObjectId
import certifi
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import InsertOne
from pymongo.errors import DuplicateKeyError, BulkWriteError
from dotenv import load_dotenv

try:
    import dns  # noqa: F401 — dnspython; required for mongodb+srv:// URIs.
    # Not used directly: pymongo/Motor import it internally to resolve the
    # SRV/TXT records that mongodb+srv:// connection strings depend on.
    # Imported here only so a missing install fails loudly and immediately
    # with an actionable message, instead of surfacing later as a cryptic
    # pymongo.errors.ConfigurationError deep inside a connection attempt.
except ImportError as e:
    raise ImportError(
        "dnspython is required for mongodb+srv:// connection strings "
        "(pip install dnspython — see requirements.txt)."
    ) from e

load_dotenv()

logger = logging.getLogger(__name__)

_config_cache = None
_config_cache_ts = 0.0
_CONFIG_TTL = 60


@lru_cache(maxsize=4096)
def compile_regex(pattern):
    return re.compile(pattern, re.IGNORECASE)


# Precompiled once at import time — find_duplicate_files()._normalize() runs
# once per document across every file in every cluster during a full scan
# (potentially hundreds of thousands of calls), so these are hoisted out of
# the per-call closure instead of being rebuilt from pattern strings each time.
_DUPE_EXT_RE = re.compile(r"\.(mkv|mp4|avi|mov|zip|srt)$", re.IGNORECASE)
_DUPE_JUNK_RE = re.compile(
    r"\b(1080p|720p|480p|360p|4k|2160p|hdrip|hd.rip|webrip|web-dl|webdl|"
    r"bluray|predvd|cam|hdcam|tsrip|dvdrip|x264|x265|hevc|aac|esub|hsub|"
    r"10bit|hq|nf|amzn|dual.audio|multi.audio|malayalam|tamil|telugu|hindi|"
    r"english|kannada|1xbet|tamilblasters|tamilmv|moviezwap)\b",
    re.IGNORECASE
)
_DUPE_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
_DUPE_BRACKET_RE = re.compile(r"[\[\(].*?[\]\)]")
_DUPE_SEP_RE = re.compile(r"[._\-]")
_DUPE_WS_RE = re.compile(r"\s+")


class _SearchCache:
    """In-process, memory-bounded TTL cache for short-lived search-pagination
    sessions. Replaces the old MongoDB-backed search_cache collection —
    sessions auto-expire within minutes and don't need to survive a restart
    or be shared across processes (the bot already enforces single-instance
    execution via a flock lock), so externalizing them to Mongo only cost a
    network round-trip on every pagination click and file-send tap."""

    def __init__(self, maxsize=2000, default_ttl=600):
        self.maxsize = maxsize
        self.default_ttl = default_ttl
        self._data = OrderedDict()  # session_id -> (inserted_at, payload)

    def set(self, session_id, payload):
        self._data.pop(session_id, None)
        if len(self._data) >= self.maxsize:
            self._data.popitem(last=False)  # evict least-recently-inserted
        self._data[session_id] = (time.time(), payload)

    def get(self, session_id):
        entry = self._data.get(session_id)
        if not entry:
            return None
        inserted_at, payload = entry
        if time.time() - inserted_at > self.default_ttl:
            del self._data[session_id]
            return None
        return payload

    def purge(self, older_than_seconds):
        cutoff = time.time() - older_than_seconds
        expired = [k for k, (inserted_at, _) in self._data.items() if inserted_at < cutoff]
        for k in expired:
            del self._data[k]


class _TrendingCache:
    """In-process, memory-bounded rolling counter backing the home-panel
    'Trending Searches' widget. Deliberately NOT a new Mongo collection —
    trending is a short-window, best-effort popularity signal, not durable
    data, so this mirrors _SearchCache's in-process design instead of
    adding schema. Also deliberately NOT sourced from missed_searches:
    those are queries that returned zero results, and surfacing one as a
    "trending" suggestion would make tapping it a guaranteed dead end —
    only queries that actually returned results should ever be recorded."""

    def __init__(self, maxsize=500, window_seconds=86400):
        self.maxsize = maxsize
        self.window_seconds = window_seconds
        self._data = OrderedDict()  # normalized_query -> [count, last_seen_ts, display_text]

    def record(self, query: str):
        norm = self._normalize(query)
        if not norm:
            return
        self._prune()
        if norm in self._data:
            entry = self._data[norm]
            entry[0] += 1
            entry[1] = time.time()
            self._data.move_to_end(norm)
        else:
            if len(self._data) >= self.maxsize:
                self._data.popitem(last=False)  # evict least-recently-inserted
            self._data[norm] = [1, time.time(), query.strip()]

    def top(self, limit=6) -> list:
        self._prune()
        ranked = sorted(self._data.values(), key=lambda entry: entry[0], reverse=True)
        return [entry[2] for entry in ranked[:limit]]

    def _prune(self):
        cutoff = time.time() - self.window_seconds
        expired = [k for k, entry in self._data.items() if entry[1] < cutoff]
        for k in expired:
            del self._data[k]

    @staticmethod
    def _normalize(query: str) -> str:
        q = re.sub(r"[^a-zA-Z0-9 ]", "", query.lower()).strip()
        return re.sub(r"\s+", " ", q)


class Database:
    def __init__(self):
        self.uris = [
            os.getenv("DATABASE_URI"),
            os.getenv("DATABASE_URI_2"),
            os.getenv("DATABASE_URI_3"),
            os.getenv("DATABASE_URI_4"),
            os.getenv("DATABASE_URI_5")
        ]

        self.clients = []
        self.dbs = []
        self.file_cols = []

        for i, uri in enumerate(self.uris):
            if uri:
                try:
                    client = AsyncIOMotorClient(
                        uri,
                        tls=True,
                        tlsCAFile=certifi.where(),
                        serverSelectionTimeoutMS=30000,
                        connectTimeoutMS=30000,
                        socketTimeoutMS=30000,
                        retryWrites=True,
                        retryReads=True,
                        # Bounded explicitly rather than left at Motor's default
                        # (100) — up to 5 clusters x asyncio.gather fan-outs on
                        # every search means an unbounded pool could push total
                        # concurrent connections toward Atlas's per-tier ceiling
                        # (500 on M0) under heavy load. minPoolSize=0 avoids
                        # holding idle connections open on a quiet bot.
                        maxPoolSize=50,
                        minPoolSize=0,
                    )
                    self.clients.append(client)
                    db_instance = client[f"MCCxBot_Cluster_{i+1}"]
                    self.dbs.append(db_instance)
                    self.file_cols.append(db_instance["movies"])
                except Exception as e:
                    logger.error(f"Cluster {i+1} init failed: {e}")

        self.users_col = None
        self.banned_col = None
        self.config_col = None
        self.indexer_col = None
        self.registry_col = None
        self.main_db = None
        self._search_cache = _SearchCache(maxsize=2000, default_ttl=600)
        self._trending_cache = _TrendingCache(maxsize=500, window_seconds=86400)

        if self.dbs:
            self.main_db = self.dbs[0]
            self.users_col = self.main_db["users"]
            self.banned_col = self.main_db["banned_users"]
            self.config_col = self.main_db["bot_config"]
            _ops_db = self.dbs[1] if len(self.dbs) > 1 else self.main_db
            self.indexer_col = _ops_db["indexer_tasks"]
            # Centralized cross-cluster identity registry — single source of
            # truth for file_id uniqueness across all sharded clusters, since
            # the per-cluster unique index on `movies.file_id` only protects
            # within one cluster's collection.
            self.registry_col = self.main_db["file_registry"]

    async def ensure_indexes(self):
        for i, col in enumerate(self.file_cols):
            try:
                await col.create_index("file_name")
                await col.create_index("file_id", unique=True)
                logger.info(f"✅ Index ensured on Cluster {i+1}")
            except Exception as e:
                logger.warning(f"⚠️ Could not create index on Cluster {i+1}: {e}")
        if self.main_db is not None:
            try:
                await self.main_db["missed_searches"].create_index([("count", -1)])
                await self.main_db["pending_requests"].create_index("movie_name")
                await self.main_db["connected_groups"].create_index("search_count")
            except Exception:
                pass
        if self.registry_col is not None:
            try:
                await self.registry_col.create_index("file_id", unique=True)
                logger.info("✅ Index ensured on file_registry")
            except Exception as e:
                logger.warning(f"⚠️ Could not create index on file_registry: {e}")

    async def sync_config(self):
        if self.config_col is None:
            return
        config = await self.config_col.find_one({"_id": "bot_config"})
        migrations = {
            "log_channel":       int(os.getenv("LOG_CHANNEL_ID", 0) or 0),
            "db_channel":        int(os.getenv("DATABASE_CHANNEL_ID", 0) or 0),
            "update_channel_id": int(os.getenv("UPDATE_CHANNEL", 0) or 0),
            "update_channel":    os.getenv("UPDATE_CHANNEL_LINK", ""),
            "main_group":        os.getenv("MAIN_GROUP_LINK", ""),
        }
        fields_to_set = {}
        for key, env_val in migrations.items():
            if config is None or key not in config:
                if env_val:
                    fields_to_set[key] = env_val
                    logger.info(f"  📥 Migrating '{key}' from .env → MongoDB: {env_val}")
        if fields_to_set:
            await self.config_col.update_one(
                {"_id": "bot_config"},
                {"$set": fields_to_set},
                upsert=True
            )
            logger.info(f"✅ Config sync complete — {len(fields_to_set)} field(s) migrated.")
        else:
            logger.info("✅ Config sync complete — nothing to migrate.")

    async def save_user(self, user_id, first_name):
        if self.users_col is None:
            return False
        try:
            user = await self.users_col.find_one({"_id": user_id})
            if not user:
                await self.users_col.insert_one({"_id": user_id, "first_name": first_name, "joined": time.time()})
                return True
            return False
        except Exception:
            return False

    async def get_all_users(self):
        if self.users_col is None:
            return []
        cursor = self.users_col.find({})
        return [doc["_id"] async for doc in cursor]

    async def delete_user(self, user_id):
        if self.users_col is None:
            return
        await self.users_col.delete_one({"_id": user_id})

    async def ban_user(self, user_id):
        if self.banned_col is None:
            return
        await self.banned_col.update_one({"_id": user_id}, {"$set": {"_id": user_id}}, upsert=True)

    async def unban_user(self, user_id):
        if self.banned_col is None:
            return
        await self.banned_col.delete_one({"_id": user_id})

    async def is_banned(self, user_id):
        if self.banned_col is None:
            return False
        doc = await self.banned_col.find_one({"_id": user_id})
        return doc is not None

    async def get_banned_users(self):
        if self.banned_col is None:
            return []
        cursor = self.banned_col.find({})
        return [doc["_id"] async for doc in cursor]

    async def add_group(self, group_id, group_title):
        if self.main_db is None:
            return False
        groups_col = self.main_db["connected_groups"]
        group = await groups_col.find_one({"_id": group_id})
        if not group:
            await groups_col.insert_one({
                "_id": group_id,
                "title": group_title,
                "added": time.time(),
                "whitelisted": False,
                "banned": False,
                "search_count": 0,
                "settings": {}
            })
            return True
        return False

    async def get_all_groups(self):
        if self.main_db is None:
            return []
        cursor = self.main_db["connected_groups"].find({})
        return [doc async for doc in cursor]

    async def get_group_count(self):
        if self.main_db is None:
            return 0
        return await self.main_db["connected_groups"].count_documents({})

    async def get_group(self, group_id):
        if self.main_db is None:
            return None
        return await self.main_db["connected_groups"].find_one({"_id": group_id})

    async def update_group(self, group_id, fields: dict):
        if self.main_db is None:
            return
        await self.main_db["connected_groups"].update_one({"_id": group_id}, {"$set": fields}, upsert=True)

    async def ban_group(self, group_id):
        if self.main_db is None:
            return
        await self.main_db["connected_groups"].update_one({"_id": group_id}, {"$set": {"banned": True}}, upsert=True)

    async def unban_group(self, group_id):
        if self.main_db is None:
            return
        await self.main_db["connected_groups"].update_one({"_id": group_id}, {"$set": {"banned": False}})

    async def is_group_banned(self, group_id):
        if self.main_db is None:
            return False
        doc = await self.main_db["connected_groups"].find_one({"_id": group_id})
        return doc.get("banned", False) if doc else False

    async def is_group_whitelisted(self, group_id):
        if self.main_db is None:
            return True
        doc = await self.main_db["connected_groups"].find_one({"_id": group_id})
        return doc.get("whitelisted", False) if doc else False

    async def increment_group_search(self, group_id):
        if self.main_db is None:
            return
        await self.main_db["connected_groups"].update_one(
            {"_id": group_id}, {"$inc": {"search_count": 1}}, upsert=True
        )

    async def get_top_groups(self, limit=10):
        if self.main_db is None:
            return []
        cursor = self.main_db["connected_groups"].find({}).sort("search_count", -1).limit(limit)
        return [doc async for doc in cursor]

    async def get_db_size(self, db_instance):
        try:
            stats = await db_instance.command("dbstats")
            return max(stats.get("storageSize", 0), stats.get("dataSize", 0)) / (1024 * 1024)
        except Exception:
            return 0

    async def get_total_files(self):
        async def _count(col):
            try:
                return await col.count_documents({})
            except Exception:
                return 0
        counts = await asyncio.gather(*[_count(col) for col in self.file_cols])
        return sum(counts)

    async def save_file(self, media):
        file_id   = getattr(media, "file_id", "")
        file_name = getattr(media, "file_name", "")
        file_size = getattr(media, "file_size", 0)
        mime_type = getattr(media, "mime_type", "")
        if not file_id or not file_name:
            return False, "Invalid media"

        # Centralized registry is the single source of truth for cross-cluster
        # uniqueness — one atomic insert instead of an O(clusters) fan-out.
        # A DuplicateKeyError here means the file_id is already claimed,
        # whether by another cluster or a concurrent save_file() call.
        if self.registry_col is not None:
            try:
                await self.registry_col.insert_one({"file_id": file_id})
            except DuplicateKeyError:
                return False, "Duplicate"

        file_doc = {"file_id": file_id, "file_name": file_name, "file_size": file_size, "mime_type": mime_type}
        for i, col in enumerate(self.file_cols):
            size = await self.get_db_size(self.dbs[i])
            if size >= 450:
                continue
            try:
                await col.insert_one(file_doc)
                return True, f"Saved to Cluster {i+1}"
            except DuplicateKeyError:
                return False, "Duplicate"

        # Nothing could actually be stored — roll back the registry
        # reservation so this file_id isn't permanently (and incorrectly)
        # marked as taken for a file that was never actually saved anywhere.
        if self.registry_col is not None:
            try:
                await self.registry_col.delete_one({"file_id": file_id})
            except Exception as e:
                logger.warning(f"Registry rollback failed for {file_id}: {e}")
        return False, "All clusters full"

    async def _registry_bulk_reserve(self, file_ids: list) -> tuple:
        """Attempts to atomically reserve every file_id in the centralized
        registry via one ordered=False bulk insert. Returns (accepted_ids,
        duplicate_count) — duplicates are file_ids already claimed by another
        cluster or a prior call, and are silently excluded rather than
        treated as an error."""
        if not file_ids or self.registry_col is None:
            return file_ids, 0

        ops = [InsertOne({"file_id": fid}) for fid in file_ids]
        try:
            await self.registry_col.bulk_write(ops, ordered=False)
            return file_ids, 0
        except BulkWriteError as bwe:
            write_errors = bwe.details.get("writeErrors", [])
            dup_indexes  = {e["index"] for e in write_errors if e.get("code") == 11000}
            other_errors = [e for e in write_errors if e.get("code") != 11000]
            if other_errors:
                logger.warning(f"{len(other_errors)} non-duplicate-key registry errors: {other_errors[:1]}")
            accepted = [fid for idx, fid in enumerate(file_ids) if idx not in dup_indexes]
            return accepted, len(dup_indexes)

    async def save_files_bulk(self, files_list):
        if not files_list:
            return 0, 0

        incoming_ids = [f["file_id"] for f in files_list]
        accepted_ids, duplicates = await self._registry_bulk_reserve(incoming_ids)
        accepted_set = set(accepted_ids)
        new_files    = [f for f in files_list if f["file_id"] in accepted_set]

        if not new_files:
            return 0, duplicates

        saved_total = 0
        remaining   = new_files[:]
        for i, col in enumerate(self.file_cols):
            if not remaining:
                break
            size = await self.get_db_size(self.dbs[i])
            if size >= 450:
                continue
            try:
                await col.insert_many(remaining, ordered=False)
                saved_total += len(remaining)
                remaining    = []
            except Exception as e:
                if "space quota" in str(e).lower() or "over your space" in str(e).lower():
                    logger.error(f"Cluster {i+1} FULL — add DATABASE_URI_{i+2} to .env")
                else:
                    logger.warning(f"Cluster {i+1} bulk insert partial failure: {e}")

        if remaining and self.registry_col is not None:
            # Nothing left could actually be stored — roll back their
            # registry reservations so these file_ids aren't permanently
            # (and incorrectly) marked as taken.
            try:
                await self.registry_col.delete_many(
                    {"file_id": {"$in": [f["file_id"] for f in remaining]}}
                )
            except Exception as e:
                logger.warning(f"Registry rollback failed for {len(remaining)} unsaved files: {e}")

        return saved_total, duplicates

    async def admin_search_files(self, query, limit=20):
        clean = re.sub(r"[^a-zA-Z0-9]", " ", query.strip())
        words = [w for w in clean.split() if w]
        if not words:
            return []
        conditions  = [{"file_name": {"$regex": f"(?:^|[\\W_]){re.escape(w)}(?:[\\W_]|$)", "$options": "i"}} for w in words]
        mongo_query = {"$and": conditions}

        async def _search_cluster(i, col):
            docs = []
            cursor = col.find(mongo_query).limit(limit)
            async for doc in cursor:
                doc["_cluster"] = i + 1
                docs.append(doc)
            return docs

        cluster_results = await asyncio.gather(
            *[_search_cluster(i, col) for i, col in enumerate(self.file_cols)]
        )
        results = [doc for docs in cluster_results for doc in docs]
        return results[:limit]

    async def delete_file_by_obj_id(self, file_obj_id):
        try:
            obj_id = ObjectId(file_obj_id)
        except Exception:
            return False
        for col in self.file_cols:
            result = await col.delete_one({"_id": obj_id})
            if result.deleted_count > 0:
                return True
        return False

    async def update_file_name(self, file_obj_id, new_name):
        try:
            obj_id = ObjectId(file_obj_id)
        except Exception:
            return False
        for col in self.file_cols:
            result = await col.update_one({"_id": obj_id}, {"$set": {"file_name": new_name}})
            if result.matched_count > 0:
                return True
        return False

    async def get_files_by_language(self):
        from plugins.filter import LANGUAGES

        facet_stage = {
            lang: [
                {"$match": {"file_name": {"$regex": rf"\b{re.escape(lang)}\b", "$options": "i"}}},
                {"$count": "n"}
            ]
            for lang in LANGUAGES
        }

        async def _cluster_counts(col):
            try:
                cursor = col.aggregate([{"$facet": facet_stage}], allowDiskUse=True)
                docs = await cursor.to_list(length=1)
                return docs[0] if docs else {}
            except Exception as e:
                logger.warning(f"get_files_by_language facet error: {e}")
                return {}

        per_cluster = await asyncio.gather(*[_cluster_counts(col) for col in self.file_cols])

        results = {lang: 0 for lang in LANGUAGES}
        for cluster_doc in per_cluster:
            for lang in LANGUAGES:
                bucket = cluster_doc.get(lang, [])
                results[lang] += bucket[0]["n"] if bucket else 0
        return results

    async def find_duplicate_files(self):
        def _normalize(name):
            if not name:
                return ""
            n = _DUPE_EXT_RE.sub("", name)
            n = _DUPE_JUNK_RE.sub("", n)
            n = _DUPE_YEAR_RE.sub("", n)
            n = _DUPE_BRACKET_RE.sub("", n)
            n = _DUPE_SEP_RE.sub(" ", n)
            return _DUPE_WS_RE.sub(" ", n).strip().lower()

        exact_data = {}
        fuzzy_data = {}

        for col in self.file_cols:
            try:
                pipeline = [
                    {"$match": {"file_id": {"$exists": True, "$ne": ""}}},
                    {"$group": {"_id": "$file_id", "count": {"$sum": 1}, "ids": {"$push": {"$toString": "$_id"}}, "name": {"$first": "$file_name"}}},
                    {"$match": {"count": {"$gt": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 200}
                ]
                async for doc in col.aggregate(pipeline, allowDiskUse=True):
                    fid = doc["_id"]
                    if not fid:
                        continue
                    if fid in exact_data:
                        exact_data[fid]["count"] += doc["count"]
                        exact_data[fid]["ids"].extend(doc["ids"])
                    else:
                        exact_data[fid] = {"count": doc["count"], "ids": doc["ids"], "name": doc.get("name", "Unknown")}
            except Exception as e:
                logger.warning(f"Exact duplicate scan error: {e}")
            try:
                cursor = col.find({"file_name": {"$exists": True, "$ne": ""}}, {"file_name": 1, "_id": 1})
                async for doc in cursor:
                    raw  = doc.get("file_name", "")
                    norm = _normalize(raw)
                    if not norm or len(norm) < 4:
                        continue
                    oid = str(doc["_id"])
                    if norm in fuzzy_data:
                        fuzzy_data[norm]["count"] += 1
                        fuzzy_data[norm]["ids"].append(oid)
                    else:
                        fuzzy_data[norm] = {"count": 1, "ids": [oid], "original_name": raw}
            except Exception as e:
                logger.warning(f"Fuzzy duplicate scan error: {e}")

        results      = []
        exact_id_set = set(i for d in exact_data.values() for i in d["ids"])

        for fid, data in exact_data.items():
            results.append({"name": data["name"], "count": data["count"], "ids": data["ids"], "type": "exact"})

        for norm, data in fuzzy_data.items():
            if data["count"] < 2:
                continue
            uncovered = [i for i in data["ids"] if i not in exact_id_set]
            if len(uncovered) < 2:
                continue
            results.append({"name": data["original_name"], "count": data["count"], "ids": data["ids"], "type": "fuzzy"})

        results.sort(key=lambda x: (x["type"] == "fuzzy", -x["count"]))
        results = results[:100]

        try:
            if self.main_db is not None:
                dupes_col = self.main_db["duplicate_scan_results"]
                await dupes_col.drop()
                if results:
                    await dupes_col.insert_many(results)
        except Exception:
            pass

        return results

    async def delete_duplicates_all(self):
        """Drops every duplicate document reported by find_duplicate_files(),
        keeping only the oldest (by ObjectId generation time) in each group."""
        groups = await self.find_duplicate_files()
        deleted = 0
        for group in groups:
            ids = group.get("ids", [])
            if len(ids) < 2:
                continue
            try:
                keep = min(ids, key=lambda oid: ObjectId(oid).generation_time)
            except Exception:
                continue
            for oid in ids:
                if oid == keep:
                    continue
                if await self.delete_file_by_obj_id(oid):
                    deleted += 1
        return deleted

    async def purge_by_pattern(self, pattern):
        deleted_total = 0
        for col in self.file_cols:
            result = await col.delete_many({"file_name": {"$regex": pattern, "$options": "i"}})
            deleted_total += result.deleted_count
        return deleted_total

    async def count_by_pattern(self, pattern):
        total = 0
        for col in self.file_cols:
            try:
                total += await col.count_documents({"file_name": {"$regex": pattern, "$options": "i"}})
            except Exception:
                pass
        return total

    async def migrate_cluster(self, from_idx: int, to_idx: int, batch_size=100):
        if from_idx >= len(self.file_cols) or to_idx >= len(self.file_cols):
            return 0, 0
        to_size = await self.get_db_size(self.dbs[to_idx])
        if to_size >= 450:
            return 0, -1
        migrated, skipped = 0, 0
        batch = []
        async for doc in self.file_cols[from_idx].find({}):
            doc.pop("_id", None)
            batch.append(doc)
            if len(batch) >= batch_size:
                try:
                    await self.file_cols[to_idx].insert_many(batch, ordered=False)
                    migrated += len(batch)
                except Exception as e:
                    logger.warning(f"Migration batch error: {e}")
                    skipped += len(batch)
                batch = []
        if batch:
            try:
                await self.file_cols[to_idx].insert_many(batch, ordered=False)
                migrated += len(batch)
            except Exception:
                skipped += len(batch)
        return migrated, skipped

    async def get_search_results(self, query, max_results=40, offset=0):
        """DreamXBotz-style search: single ordered regex (words must appear in
        order, separated by [\\s.+-_]), sorted by _id (indexed, newest-first),
        fanned across all clusters."""
        if isinstance(query, list):
            raw_pattern = "|".join(re.escape(q.strip()) for q in query if q and q.strip())
            if not raw_pattern:
                return []
            regex = compile_regex(raw_pattern)
        else:
            query = query.strip()
            if not query:
                return []
            if " " in query:
                words = [re.escape(w) for w in query.split() if w]
                raw_pattern = (r".*[\s\.\+\-_]".join(words) if words else r".")
            else:
                raw_pattern = r"(\b|[\.\+\-_])" + re.escape(query) + r"(\b|[\.\+\-_])"
            try:
                regex = compile_regex(raw_pattern)
            except re.error:
                return []

        filter_mongo = {"file_name": regex}
        limit = max_results + 1

        async def _search_cluster(col):
            cursor = col.find(filter_mongo).sort("_id", -1).skip(offset).limit(limit)
            return [doc async for doc in cursor]

        cluster_results = await asyncio.gather(*[_search_cluster(col) for col in self.file_cols])

        seen_ids = set()
        files = []
        for docs in cluster_results:
            for doc in docs:
                fid = doc.get("file_id")
                if fid in seen_ids:
                    continue
                seen_ids.add(fid)
                files.append(doc)

        return files[:max_results]

    async def get_prefix_suggestions(self, query, limit=3):
        clean = re.sub(r"[^a-zA-Z0-9]", " ", query.strip())
        words = [w for w in clean.split() if len(w) >= 4]
        if not words:
            return []
        # Use the longest significant word, not just the first one — the first word
        # in a query is often noise the stop-word stripper didn't catch.
        anchor = max(words, key=len)
        prefix = anchor[:5]

        async def _search_cluster(col):
            cursor = col.find(
                {"file_name": {"$regex": f"(?:^|[\\W_]){re.escape(prefix)}", "$options": "i"}},
                {"file_name": 1}
            ).limit(15)
            return [doc.get("file_name", "") async for doc in cursor]

        cluster_results = await asyncio.gather(*[_search_cluster(col) for col in self.file_cols])

        suggestions, seen = [], set()
        for names in cluster_results:
            for name in names:
                title = " ".join(name.split()[:4])
                if title.lower() not in seen:
                    seen.add(title.lower())
                    suggestions.append(title)
                if len(suggestions) >= limit:
                    return suggestions[:limit]
        return suggestions[:limit]

    async def get_file(self, file_obj_id):
        try:
            obj_id = ObjectId(file_obj_id)
        except Exception:
            return None
        for col in self.file_cols:
            doc = await col.find_one({"_id": obj_id})
            if doc:
                return doc
        return None

    async def delete_file_by_id(self, file_id):
        for col in self.file_cols:
            result = await col.delete_one({"file_id": file_id})
            if result.deleted_count > 0:
                return True
        return False

    async def purge_cams(self):
        return await self.purge_by_pattern(r"cam|predvd|hdcam|tsrip|1xbet")

    async def log_missed_search(self, query: str):
        if self.main_db is None:
            return False
        col     = self.main_db["missed_searches"]
        cleaned = re.sub(r"[^a-zA-Z0-9 ]", "", query.lower()).strip()
        if not cleaned:
            return False
        now          = time.time()
        cooldown     = 3600
        existing     = await col.find_one({"_id": cleaned})
        should_alert = existing is None or (now - existing.get("last_alerted", 0)) > cooldown
        update = {"$inc": {"count": 1}, "$set": {"last_searched": now, "original": query}}
        if should_alert:
            update["$set"]["last_alerted"] = now
        try:
            await col.update_one({"_id": cleaned}, update, upsert=True)
        except Exception:
            pass
        return should_alert

    async def get_top_missed(self, limit=15):
        if self.main_db is None:
            return []
        cursor = self.main_db["missed_searches"].find({}).sort("count", -1).limit(limit)
        return [doc async for doc in cursor]

    async def clear_missed_search(self, query_id: str):
        if self.main_db is None:
            return
        await self.main_db["missed_searches"].delete_one({"_id": query_id})

    async def get_bot_stats(self):
        async def _count(col):
            return await col.count_documents({}) if col is not None else 0

        async def _cluster_stats(i, db_instance):
            files_in_db = await self.file_cols[i].count_documents({})
            size = await self.get_db_size(db_instance)
            return i + 1, files_in_db, size

        total_users, total_banned, total_groups, *cluster_stats = await asyncio.gather(
            _count(self.users_col),
            _count(self.banned_col),
            self.get_group_count(),
            *[_cluster_stats(i, db_instance) for i, db_instance in enumerate(self.dbs)]
        )

        total_files = sum(files_in_db for _, files_in_db, _ in cluster_stats)
        db_sizes    = [(idx, size) for idx, _, size in cluster_stats]
        return total_users, total_banned, total_files, db_sizes, total_groups

    async def reset_database(self):
        if self.users_col is not None:
            await self.users_col.drop()
        if self.banned_col is not None:
            await self.banned_col.drop()
        for col in self.file_cols:
            await col.drop()
        return True

    async def get_config(self):
        global _config_cache, _config_cache_ts
        now = time.time()
        if _config_cache is not None and (now - _config_cache_ts) < _CONFIG_TTL:
            return _config_cache
        if self.config_col is None:
            return {}
        config = await self.config_col.find_one({"_id": "bot_config"})
        if not config:
            config = {
                "_id": "bot_config",
                "start_media": "https://files.catbox.moe/wvdeci.mp4",
                "fsub_channels": [],
                "db_channels": [],
                "auto_delete_time": 300,
                "maintenance_mode": False,
                "maintenance_message": "🔧 Bot is under maintenance. Back soon!",
                "group_whitelist_enabled": False,
                "file_caption_template": "",
            }
            await self.config_col.insert_one(config)
        _config_cache    = config
        _config_cache_ts = now
        return config

    async def update_config(self, key, value):
        global _config_cache, _config_cache_ts
        if self.config_col is None:
            return False
        await self.config_col.update_one({"_id": "bot_config"}, {"$set": {key: value}}, upsert=True)
        _config_cache    = None
        _config_cache_ts = 0.0
        return True

    async def export_config(self):
        config  = await self.get_config()
        exclude = {"_id", "log_channel", "admin_id", "db_channels", "update_channel_id", "db_channel"}
        safe    = {k: v for k, v in config.items() if k not in exclude}
        # Invite links are bearer credentials — export channel IDs only, never
        # the cached link string, so a shared/leaked backup can't be used to
        # join private FSub channels.
        if "fsub_channels" in safe:
            safe["fsub_channels"] = [
                ({"id": e.get("id")} if isinstance(e, dict) else e) for e in safe["fsub_channels"]
            ]
        return safe

    async def restore_config(self, data: dict):
        protected = {"_id", "log_channel", "admin_id", "db_channels", "update_channel_id", "db_channel", "fsub_channels"}
        safe_data = {k: v for k, v in data.items() if k not in protected}
        if not safe_data:
            return False
        for key, value in safe_data.items():
            await self.update_config(key, value)
        return True

    async def add_fsub_channel(self, channel_id):
        if self.config_col is None:
            return False
        entry = {"id": channel_id}
        await self.config_col.update_one({"_id": "bot_config"}, {"$pull": {"fsub_channels": {"id": channel_id}}})
        await self.config_col.update_one({"_id": "bot_config"}, {"$pull": {"fsub_channels": channel_id}})
        await self.config_col.update_one({"_id": "bot_config"}, {"$push": {"fsub_channels": entry}}, upsert=True)
        global _config_cache, _config_cache_ts
        _config_cache = None; _config_cache_ts = 0.0
        return True

    async def update_fsub_channel_link(self, channel_id, link):
        if self.config_col is None:
            return
        config = await self.config_col.find_one({"_id": "bot_config"})
        if not config:
            return
        channels = config.get("fsub_channels", [])
        updated  = []
        for entry in channels:
            if isinstance(entry, dict) and entry.get("id") == channel_id:
                entry["link"] = link
            updated.append(entry)
        await self.config_col.update_one({"_id": "bot_config"}, {"$set": {"fsub_channels": updated}})
        global _config_cache, _config_cache_ts
        _config_cache = None; _config_cache_ts = 0.0

    async def remove_fsub_channel(self, channel_id):
        if self.config_col is None:
            return False
        await self.config_col.update_one({"_id": "bot_config"}, {"$pull": {"fsub_channels": {"id": channel_id}}})
        await self.config_col.update_one({"_id": "bot_config"}, {"$pull": {"fsub_channels": channel_id}})
        global _config_cache, _config_cache_ts
        _config_cache = None; _config_cache_ts = 0.0
        return True

    async def add_db_channel(self, channel_id):
        if self.config_col is None:
            return False
        await self.config_col.update_one({"_id": "bot_config"}, {"$addToSet": {"db_channels": channel_id}}, upsert=True)
        global _config_cache, _config_cache_ts
        _config_cache = None; _config_cache_ts = 0.0
        return True

    async def remove_db_channel(self, channel_id):
        if self.config_col is None:
            return False
        await self.config_col.update_one({"_id": "bot_config"}, {"$pull": {"db_channels": channel_id}})
        global _config_cache, _config_cache_ts
        _config_cache = None; _config_cache_ts = 0.0
        return True

    async def add_req_fsub_channel(self, channel_id):
        if self.config_col is None:
            return False, "No DB"
        config   = await self.config_col.find_one({"_id": "bot_config"})
        existing = config.get("req_fsub_channels", []) if config else []
        if len(existing) >= 5:
            return False, "Max 5 reached"
        for e in existing:
            eid = e.get("id") if isinstance(e, dict) else e
            if str(eid) == str(channel_id):
                return False, "Already exists"
        await self.config_col.update_one({"_id": "bot_config"}, {"$push": {"req_fsub_channels": {"id": channel_id}}}, upsert=True)
        global _config_cache, _config_cache_ts
        _config_cache = None; _config_cache_ts = 0.0
        return True, "Added"

    async def remove_req_fsub_channel(self, channel_id):
        if self.config_col is None:
            return False
        await self.config_col.update_one({"_id": "bot_config"}, {"$pull": {"req_fsub_channels": {"id": channel_id}}})
        await self.config_col.update_one({"_id": "bot_config"}, {"$pull": {"req_fsub_channels": channel_id}})
        global _config_cache, _config_cache_ts
        _config_cache = None; _config_cache_ts = 0.0
        return True

    async def update_req_fsub_link(self, channel_id, link):
        if self.config_col is None:
            return
        config = await self.config_col.find_one({"_id": "bot_config"})
        if not config:
            return
        channels = config.get("req_fsub_channels", [])
        updated  = []
        for entry in channels:
            if isinstance(entry, dict) and str(entry.get("id")) == str(channel_id):
                entry["link"] = link
            updated.append(entry)
        await self.config_col.update_one({"_id": "bot_config"}, {"$set": {"req_fsub_channels": updated}})
        global _config_cache, _config_cache_ts
        _config_cache = None; _config_cache_ts = 0.0

    async def get_req_fsub_interval(self):
        config = await self.get_config()
        return int(config.get("req_fsub_interval_hours", 24)) * 3600

    async def check_req_fsub_due(self, user_id: int) -> bool:
        if self.users_col is None:
            return False
        try:
            doc      = await self.users_col.find_one({"_id": user_id}, {"req_fsub_last": 1})
            last     = doc.get("req_fsub_last", 0) if doc else 0
            interval = await self.get_req_fsub_interval()
            return (time.time() - last) >= interval
        except Exception:
            return False

    async def mark_req_fsub_shown(self, user_id: int):
        if self.users_col is None:
            return
        try:
            await self.users_col.update_one({"_id": user_id}, {"$set": {"req_fsub_last": time.time()}}, upsert=True)
        except Exception:
            pass

    async def get_data_saver(self, user_id: int) -> bool:
        """Mobile Data Saver preference — an additive field on the existing
        users doc, not a new collection. A single find_one on an indexed
        _id is cheap enough to call on every result-view render, unlike the
        multi-cluster gather() fan-outs elsewhere in this class."""
        if self.users_col is None:
            return False
        try:
            doc = await self.users_col.find_one({"_id": user_id}, {"data_saver": 1})
            return bool(doc.get("data_saver", False)) if doc else False
        except Exception:
            return False

    async def set_data_saver(self, user_id: int, enabled: bool):
        if self.users_col is None:
            return
        try:
            await self.users_col.update_one({"_id": user_id}, {"$set": {"data_saver": enabled}}, upsert=True)
        except Exception:
            pass

    async def save_pending_request(self, user_id, movie_name):
        if self.main_db is None:
            return
        requests_col = self.main_db["pending_requests"]
        await requests_col.update_one(
            {"user_id": user_id, "movie_name": movie_name.lower().strip()},
            {"$set": {"user_id": user_id, "movie_name": movie_name.lower().strip(), "original_name": movie_name, "timestamp": time.time()}},
            upsert=True
        )

    async def find_matching_requests(self, file_name):
        if self.main_db is None:
            return []
        requests_col = self.main_db["pending_requests"]
        clean = re.sub(r"[^a-zA-Z0-9 ]", " ", file_name)
        words = [w for w in clean.split() if len(w) >= 5 and not w.isdigit()]
        if not words:
            return []
        conditions = [{"movie_name": {"$regex": word[:5], "$options": "i"}} for word in words[:3]]
        cursor     = requests_col.find({"$or": conditions})
        matches    = []
        async for doc in cursor:
            matches.append({"user_id": doc["user_id"], "movie_name": doc["original_name"]})
        return matches

    async def delete_pending_request(self, user_id, movie_name):
        if self.main_db is None:
            return
        await self.main_db["pending_requests"].delete_one({"user_id": user_id, "movie_name": movie_name.lower().strip()})

    async def set_index_progress(self, chat_id, msg_id):
        if self.main_db is None:
            return
        try:
            await self.main_db["settings"].update_one({"_id": "index_progress"}, {"$set": {str(chat_id): msg_id}}, upsert=True)
        except Exception as e:
            logger.warning(f"set_index_progress failed: {e}")

    async def get_index_progress(self, chat_id):
        if self.main_db is None:
            return 0
        data = await self.main_db["settings"].find_one({"_id": "index_progress"})
        return data.get(str(chat_id), 0) if data else 0

    async def clear_index_progress(self, chat_id=None):
        if self.main_db is None:
            return
        try:
            settings = self.main_db["settings"]
            if chat_id is None:
                await settings.delete_one({"_id": "index_progress"})
            else:
                await settings.update_one({"_id": "index_progress"}, {"$unset": {str(chat_id): ""}})
        except Exception as e:
            logger.warning(f"clear_index_progress failed: {e}")

    async def set_index_task(self, chat_id, state):
        if self.indexer_col is None:
            return
        try:
            await self.indexer_col.update_one({"_id": str(chat_id)}, {"$set": {"state": state, "updated": time.time()}}, upsert=True)
        except Exception as e:
            logger.warning(f"set_index_task failed: {e}")

    async def get_index_task(self, chat_id):
        if self.indexer_col is None:
            return None
        try:
            doc = await self.indexer_col.find_one({"_id": str(chat_id)})
            return doc["state"] if doc else None
        except Exception as e:
            logger.warning(f"get_index_task failed: {e}")
            return None  # treat transient DB errors as "not stopped" — loop keeps trying

    async def clear_index_task(self, chat_id):
        if self.indexer_col is None:
            return
        await self.indexer_col.delete_one({"_id": str(chat_id)})

    async def clear_all_index_tasks(self):
        if self.indexer_col is None:
            return
        await self.indexer_col.delete_many({})

    async def get_stale_index_tasks(self, older_than_seconds=7200):
        if self.indexer_col is None:
            return []
        cutoff = time.time() - older_than_seconds
        cursor = self.indexer_col.find({"state": "running", "updated": {"$lt": cutoff}})
        return [doc async for doc in cursor]

    async def save_search(self, session_id, data):
        self._search_cache.set(session_id, data)

    async def get_search(self, session_id):
        return self._search_cache.get(session_id)

    async def clear_old_searches(self, expiry_seconds=600):
        self._search_cache.purge(expiry_seconds)

    async def log_trending_search(self, query: str):
        """Records a query that actually returned results, for the home-
        panel Trending Searches widget. Call only on the success path —
        see _TrendingCache's docstring for why misses are excluded."""
        self._trending_cache.record(query)

    async def get_trending_searches(self, limit: int = 6) -> list:
        return self._trending_cache.top(limit)


db = Database()