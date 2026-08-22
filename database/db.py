import os
import re
import time
import json
import asyncio
import logging
from datetime import datetime, timezone
from collections import OrderedDict
from functools import lru_cache
from bson.objectid import ObjectId
import certifi
from pymongo import AsyncMongoClient, InsertOne, UpdateOne
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


class AllClustersFullError(Exception):
    """Raised by save_files_bulk() when every cluster is at/above its 450MB
    safety margin and none of the batch's files could be stored anywhere.
    Distinguishes "no cluster had room" from a normal (0 saved, N
    duplicates) result — without this, callers can't tell a full database
    apart from a batch that was simply all duplicates."""
    def __init__(self, unsaved_count: int, duplicates: int):
        self.unsaved_count = unsaved_count
        self.duplicates = duplicates
        super().__init__(f"All clusters full — {unsaved_count} file(s) could not be stored")


_config_cache = None
_config_cache_ts = 0.0
_CONFIG_TTL = 60

# Fixed per-user re-verification window for the Two-Stage Verification gate
# (database.get_two_stage_due / mark_two_stage_verified below). Deliberately
# a hardcoded constant rather than an admin-configurable field like
# req_fsub_interval_hours — the product requirement is specifically a fixed
# 30-minute window, not a tunable one.
TWO_STAGE_VERIFY_INTERVAL = 1800


@lru_cache(maxsize=4096)
def compile_regex(pattern):
    return re.compile(pattern, re.IGNORECASE)


# Search's separator class ([\s.+-_]) doesn't cover brackets, @tags, hashes,
# etc. — a raw filename like "[TamilBlasters.cc]_Balan_(2024)_1080p.mkv"
# can silently fail to match even a correct query because "]" or "(" sits
# where a recognized separator was expected. Normalizing every stored
# file_name to plain space-separated words (same idea as DreamXBotz's
# save_file) removes that whole class of missed matches at the source,
# instead of trying to special-case every possible symbol in the regex.
_JUNK_TAG_RE = re.compile(r"http\S+|www\.\S+|@\w+", re.IGNORECASE)
_SYMBOL_RE = re.compile(r"[_\-\.#+$%^&*()!~`,;:\"'?/<>\[\]{}=|\\]")
_WS_COLLAPSE_RE = re.compile(r"\s+")
_SEARCH_FALLBACK_STOP = {
    "the", "and", "for", "with", "from", "movie", "film", "part",
    "season", "episode", "full", "official",
}


def normalize_file_name(name: str) -> str:
    """Strip promotional junk (URLs, @mentions) and collapse every
    separator/symbol character to a single space, so every stored
    file_name is consistently space-separated. Falls back to a stripped
    copy of the original if cleaning would otherwise empty it out."""
    if not name:
        return name
    cleaned = _JUNK_TAG_RE.sub("", name)
    cleaned = _SYMBOL_RE.sub(" ", cleaned)
    cleaned = _WS_COLLAPSE_RE.sub(" ", cleaned).strip()
    return cleaned or name.strip()


def deduplicate_file_batch(files_list):
    """Keep the first document for each valid file_id and count rejects."""
    unique_files = []
    seen = set()
    duplicate_count = 0
    for file_doc in files_list:
        fid = file_doc.get("file_id")
        if not fid or fid in seen:
            duplicate_count += 1
            continue
        seen.add(fid)
        unique_files.append(file_doc)
    return unique_files, duplicate_count


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
                    uri_lower = uri.lower()
                    tls_options = {}
                    if uri_lower.startswith("mongodb+srv://") or "tls=true" in uri_lower or "ssl=true" in uri_lower:
                        tls_options = {"tls": True, "tlsCAFile": certifi.where()}
                    client = AsyncMongoClient(
                        uri,
                        serverSelectionTimeoutMS=30000,
                        connectTimeoutMS=30000,
                        socketTimeoutMS=30000,
                        retryWrites=True,
                        retryReads=True,
                        # Bounded explicitly rather than left at the driver default
                        # (100) — up to 5 clusters x asyncio.gather fan-outs on
                        # every search means an unbounded pool could push total
                        # concurrent connections toward Atlas's per-tier ceiling
                        # (500 on M0) under heavy load. minPoolSize=0 avoids
                        # holding idle connections open on a quiet bot.
                        maxPoolSize=50,
                        minPoolSize=0,
                        **tls_options,
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
        self.deletion_col = None
        self.main_db = None
        self._search_cache = _SearchCache(maxsize=2000, default_ttl=600)
        self._db_size_cache = {}  # id(db_instance) -> (fetched_at, size_mb)
        self._file_count_cache = (0.0, 0)

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
            # Derived/high-churn operational data belongs on the operations
            # database.  Keeping it off the primary movie/config cluster lets
            # an installation whose first Atlas M0 cluster is read-only at its
            # quota continue to register files and schedule message cleanup.
            self.registry_col = _ops_db["file_registry"]
            self.deletion_col = _ops_db["scheduled_deletions"]

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
                # TTL cleanup — additive only, never touches existing
                # documents. Both fields are populated going forward
                # (log_missed_search() / save_pending_request());
                # documents written before this change simply lack the
                # field and are skipped by MongoDB's TTL monitor until
                # they're naturally written to again (re-searched /
                # re-requested).
                await self.main_db["missed_searches"].create_index(
                    "last_searched_at", expireAfterSeconds=90 * 24 * 3600  # 90 days of no repeat searches
                )
                await self.main_db["pending_requests"].create_index(
                    "requested_at", expireAfterSeconds=180 * 24 * 3600  # 180 days — generous so a slow-to-fulfill request still gets auto-notified
                )
            except Exception as e:
                logger.warning("Could not ensure auxiliary indexes: %s", e)
        if self.deletion_col is not None:
            try:
                await self.deletion_col.create_index("due_at")
            except Exception as e:
                logger.warning("Could not ensure scheduled-deletion index: %s", e)
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

    async def get_user_count(self):
        """Return a count without materialising the complete user collection."""
        if self.users_col is None:
            return 0
        return await self.users_col.count_documents({})

    async def iter_user_ids(self, batch_size=500):
        """Stream recipient IDs for broadcasts instead of loading all users."""
        if self.users_col is None:
            return
        cursor = self.users_col.find({}, {"_id": 1}).batch_size(batch_size)
        async for doc in cursor:
            yield doc["_id"]

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

    _DB_SIZE_TTL = 30  # seconds

    async def get_db_size(self, db_instance):
        """Cached wrapper around the `dbstats` command. save_file() and
        save_files_bulk() call this once per candidate cluster on every
        single write — once earlier clusters fill up, that becomes a
        sequential dbstats round-trip on each of them before ever reaching
        one with room. A short TTL keeps the "is this cluster full" check
        fresh enough (worst case a few seconds stale right at the 450MB
        boundary) while collapsing bursts of saves into one real query."""
        now  = time.time()
        key  = id(db_instance)
        hit  = self._db_size_cache.get(key)
        if hit and (now - hit[0]) < self._DB_SIZE_TTL:
            return hit[1]
        try:
            stats = await db_instance.command("dbstats")
            size  = max(stats.get("storageSize", 0), stats.get("dataSize", 0)) / (1024 * 1024)
        except Exception:
            size = 0
        self._db_size_cache[key] = (now, size)
        return size

    async def get_total_files(self):
        cached_at, cached_count = self._file_count_cache
        if time.time() - cached_at < 30:
            return cached_count

        async def _count(col):
            try:
                return await col.count_documents({})
            except Exception:
                return 0
        counts = await asyncio.gather(*[_count(col) for col in self.file_cols])
        total = sum(counts)
        self._file_count_cache = (time.time(), total)
        return total

    def _invalidate_file_count(self):
        self._file_count_cache = (0.0, 0)

    async def registry_needs_migration(self):
        """Detect the dangerous upgrade case: old files but no registry."""
        if self.registry_col is None:
            return False
        if await self.registry_col.count_documents({}, limit=1):
            return False

        async def _has_file(col):
            try:
                return await col.find_one({}, {"_id": 1}) is not None
            except Exception:
                return False

        return any(await asyncio.gather(*[_has_file(col) for col in self.file_cols]))

    async def save_file(self, media):
        file_id   = getattr(media, "file_id", "")
        file_name = normalize_file_name(getattr(media, "file_name", ""))
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
                result = await col.insert_one(file_doc)
                if self.registry_col is not None:
                    await self.registry_col.update_one(
                        {"file_id": file_id},
                        {"$set": {"cluster": i + 1, "movie_id": str(result.inserted_id)}},
                    )
                self._invalidate_file_count()
                return True, f"Saved to Cluster {i+1}"
            except DuplicateKeyError:
                # A physical row already exists. Keep the reservation and
                # repair its location metadata instead of releasing it.
                existing = await col.find_one({"file_id": file_id}, {"_id": 1})
                if existing and self.registry_col is not None:
                    await self.registry_col.update_one(
                        {"file_id": file_id},
                        {"$set": {"cluster": i + 1, "movie_id": str(existing["_id"])}},
                    )
                return False, "Duplicate"
            except Exception as e:
                logger.warning(f"Cluster {i+1} insert failed for {file_id}: {e}")
                continue

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
            concern_errors = bwe.details.get("writeConcernErrors", [])
            if concern_errors:
                # The server may have applied some reservations, but did not
                # acknowledge them reliably. Remove only claims with no
                # physical file, then force the indexer to retry later.
                await self._release_registry_ids(file_ids)
                raise RuntimeError(
                    f"Registry write concern failed: {concern_errors[:1]}"
                ) from bwe
            dup_indexes  = {e["index"] for e in write_errors if e.get("code") == 11000}
            other_errors = [e for e in write_errors if e.get("code") != 11000]
            if other_errors:
                # Continuing would store files without a uniqueness claim.
                # Roll back reservations that did succeed, then fail so the
                # caller can retry the complete batch safely.
                failed_indexes = {e.get("index") for e in write_errors}
                reserved = [
                    fid for idx, fid in enumerate(file_ids)
                    if idx not in failed_indexes
                ]
                if reserved:
                    try:
                        await self.registry_col.delete_many({"file_id": {"$in": reserved}})
                    except Exception as rollback_error:
                        logger.error(
                            "Registry reservation rollback failed for %s IDs: %s",
                            len(reserved), rollback_error,
                        )
                raise RuntimeError(
                    f"Registry reservation failed for {len(other_errors)} file(s): "
                    f"{other_errors[:1]}"
                ) from bwe
            accepted = [fid for idx, fid in enumerate(file_ids) if idx not in dup_indexes]
            return accepted, len(dup_indexes)

    async def _mark_registry_locations(self, docs: list, cluster_index: int):
        if not docs or self.registry_col is None:
            return
        ops = [
            UpdateOne(
                {"file_id": doc["file_id"]},
                {"$set": {"cluster": cluster_index + 1, "movie_id": str(doc["_id"])}},
            )
            for doc in docs if doc.get("file_id") and doc.get("_id")
        ]
        if ops:
            try:
                await self.registry_col.bulk_write(ops, ordered=False)
            except Exception as e:
                # Location is repairable metadata; the file_id reservation is
                # the actual uniqueness invariant and must remain claimed.
                logger.warning("Could not update registry location metadata: %s", e)

    async def save_files_bulk(self, files_list):
        if not files_list:
            return 0, 0

        # De-duplicate the incoming batch before reserving IDs. Filtering via
        # an accepted-id set after reservation reintroduced duplicate entries.
        unique_files, internal_duplicates = deduplicate_file_batch(files_list)

        incoming_ids = [f["file_id"] for f in unique_files]
        accepted_ids, duplicates = await self._registry_bulk_reserve(incoming_ids)
        duplicates += internal_duplicates
        accepted_set = set(accepted_ids)
        new_files    = [f for f in unique_files if f["file_id"] in accepted_set]

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
                result = await col.insert_many(remaining, ordered=False)
                inserted = []
                for doc, inserted_id in zip(remaining, result.inserted_ids):
                    stored = dict(doc)
                    stored["_id"] = inserted_id
                    inserted.append(stored)
                await self._mark_registry_locations(inserted, i)
                saved_total += len(inserted)
                remaining    = []
            except BulkWriteError as bwe:
                errors = bwe.details.get("writeErrors", [])
                failed_indexes = {err.get("index") for err in errors}
                successful = [doc for idx, doc in enumerate(remaining) if idx not in failed_indexes]

                # PyMongo normally adds generated _ids to input documents
                # before the write. Query only as a fallback (also useful for
                # alternate/fake collection implementations).
                if successful:
                    stored_docs = [doc for doc in successful if doc.get("_id")]
                    if len(stored_docs) != len(successful):
                        try:
                            stored_docs = await col.find(
                                {"file_id": {"$in": [doc["file_id"] for doc in successful]}},
                                {"_id": 1, "file_id": 1},
                            ).to_list(length=len(successful))
                        except Exception as e:
                            logger.warning("Could not read inserted IDs for registry metadata: %s", e)
                            stored_docs = []
                    await self._mark_registry_locations(stored_docs, i)
                    saved_total += len(successful)

                retry = []
                for err in errors:
                    idx = err.get("index")
                    if idx is None or idx >= len(remaining):
                        continue
                    doc = remaining[idx]
                    if err.get("code") == 11000:
                        # The row already exists physically; repair registry
                        # metadata and treat it as a duplicate, not an unsaved file.
                        existing = await col.find_one({"file_id": doc["file_id"]}, {"_id": 1, "file_id": 1})
                        if existing:
                            await self._mark_registry_locations([existing], i)
                            duplicates += 1
                            continue
                    retry.append(doc)
                remaining = retry
                logger.warning(
                    f"Cluster {i+1} bulk insert: {len(successful)} saved, "
                    f"{len(retry)} retryable, {len(errors) - len(retry)} duplicate(s)"
                )
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

        if remaining:
            # Every cluster was either at its safety margin or rejected the
            # insert — this batch is genuinely unstorable right now. Raise
            # instead of returning (0, duplicates) so callers can't mistake
            # "database full" for "everything was a duplicate".
            raise AllClustersFullError(len(remaining), duplicates)

        if saved_total:
            self._invalidate_file_count()
        return saved_total, duplicates

    async def _release_registry_ids(self, file_ids, batch_size=500):
        """Release registry claims only when no physical row remains.

        This keeps deletes, purges, duplicate cleanup and automatic broken-file
        removal consistent with the cross-cluster uniqueness registry.
        """
        if self.registry_col is None:
            return
        ids = list(dict.fromkeys(fid for fid in file_ids if fid))
        for start in range(0, len(ids), batch_size):
            chunk = ids[start:start + batch_size]
            present_sets = await asyncio.gather(*[
                col.distinct("file_id", {"file_id": {"$in": chunk}})
                for col in self.file_cols
            ], return_exceptions=True)
            errors = [value for value in present_sets if isinstance(value, Exception)]
            if errors:
                # Never release a uniqueness claim when an unavailable cluster
                # prevents us from proving that no physical copy remains.
                logger.warning(
                    "Registry cleanup deferred for %s file(s): %s cluster check(s) failed",
                    len(chunk), len(errors),
                )
                continue
            present = {fid for values in present_sets for fid in values}
            orphaned = [fid for fid in chunk if fid not in present]
            if orphaned:
                await self.registry_col.delete_many({"file_id": {"$in": orphaned}})

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
            try:
                doc = await col.find_one({"_id": obj_id}, {"file_id": 1})
            except Exception as e:
                logger.warning("File lookup failed during delete: %s", e)
                continue
            if not doc:
                continue
            try:
                result = await col.delete_one({"_id": obj_id})
            except Exception as e:
                logger.warning("File delete failed: %s", e)
                continue
            if result.deleted_count > 0:
                await self._release_registry_ids([doc.get("file_id")])
                self._invalidate_file_count()
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
                cursor = await col.aggregate([{"$facet": facet_stage}], allowDiskUse=True)
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
                cursor = await col.aggregate(pipeline, allowDiskUse=True)
                async for doc in cursor:
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
        deleted_ids = []
        for col in self.file_cols:
            try:
                cursor = col.find(
                    {"file_name": {"$regex": pattern, "$options": "i"}},
                    {"file_id": 1},
                )
                async for doc in cursor:
                    if doc.get("file_id"):
                        deleted_ids.append(doc["file_id"])
                result = await col.delete_many({"file_name": {"$regex": pattern, "$options": "i"}})
                deleted_total += result.deleted_count
            except Exception as e:
                logger.warning("Pattern purge skipped an unavailable cluster: %s", e)
        await self._release_registry_ids(deleted_ids)
        if deleted_total:
            self._invalidate_file_count()
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
        """Moves every document from cluster from_idx to cluster to_idx.
        Each batch is only deleted from the source *after* it's confirmed
        copied to the destination — otherwise a file's file_id (already
        claimed in file_registry from when it was first saved) would end up
        physically duplicated across two clusters instead of moved, with no
        way to tell which copy is the "real" one."""
        if from_idx >= len(self.file_cols) or to_idx >= len(self.file_cols):
            return 0, 0
        to_size = await self.get_db_size(self.dbs[to_idx])
        if to_size >= 450:
            return 0, -1

        migrated, skipped = 0, 0

        async def _flush(batch):
            nonlocal migrated, skipped
            if not batch:
                return
            docs = [entry["doc"] for entry in batch]
            try:
                await self.file_cols[to_idx].insert_many(docs, ordered=False)
                copied_src_ids = [entry["_id"] for entry in batch]
            except BulkWriteError as bwe:
                write_errors  = bwe.details.get("writeErrors", [])
                failed_idx    = {e["index"] for e in write_errors}
                copied_src_ids = [entry["_id"] for idx, entry in enumerate(batch) if idx not in failed_idx]
                skipped       += len(failed_idx)
                if len(failed_idx) < len(write_errors):
                    logger.warning(f"Migration batch: {len(write_errors)} write errors, {len(failed_idx)} unique docs failed")
            except Exception as e:
                logger.warning(f"Migration batch error: {e}")
                skipped += len(batch)
                return

            if copied_src_ids:
                copied_id_set = set(copied_src_ids)
                copied_file_ids = [
                    entry["doc"].get("file_id") for entry in batch
                    if entry["_id"] in copied_id_set and entry["doc"].get("file_id")
                ]
                if copied_file_ids:
                    stored_docs = await self.file_cols[to_idx].find(
                        {"file_id": {"$in": copied_file_ids}},
                        {"_id": 1, "file_id": 1},
                    ).to_list(length=len(copied_file_ids))
                    await self._mark_registry_locations(stored_docs, to_idx)
                try:
                    result = await self.file_cols[from_idx].delete_many({"_id": {"$in": copied_src_ids}})
                    migrated += result.deleted_count
                except Exception as e:
                    # Docs are now confirmed present in the destination but
                    # couldn't be removed from the source — they exist in
                    # both clusters until a retry cleans up the source.
                    logger.error(f"Migration source-cleanup failed for {len(copied_src_ids)} doc(s) — now duplicated across clusters {from_idx+1}/{to_idx+1}: {e}")

        batch = []
        async for doc in self.file_cols[from_idx].find({}):
            src_id = doc.pop("_id")
            batch.append({"_id": src_id, "doc": doc})
            if len(batch) >= batch_size:
                await _flush(batch)
                batch = []
        await _flush(batch)

        if migrated:
            self._invalidate_file_count()
        return migrated, skipped

    async def _regex_search(self, regex, max_results, offset=0):
        """Run a single compiled regex against file_name across every
        cluster, dedup by file_id, sorted newest-first. Shared by every
        tier of get_search_results()."""
        filter_mongo = {"file_name": regex}
        limit = max_results + 1

        async def _search_cluster(col):
            try:
                cursor = (
                    col.find(filter_mongo)
                    .sort("_id", -1)
                    .skip(offset)
                    .limit(limit)
                    .max_time_ms(5000)
                )
                return [doc async for doc in cursor]
            except Exception as e:
                # A secondary cluster outage should degrade result coverage,
                # not take the entire search feature down.
                logger.warning(f"Search cluster failed: {e}")
                return []

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

    async def get_search_results(self, query, max_results=40, offset=0):
        """Every search now always checks three match levels and merges
        them by relevance — nothing is gated behind an earlier level
        coming back completely empty, so a single-word hit is never
        silently dropped just because another level found *something*:

        Level 1: words in order, separated by [\\s.+-_] (best match).
        Level 2: same words present anywhere, any order.
        Level 3: EVERY single word, searched independently — "balan the
        boy" always checks "balan" and "the" and "boy" on their own, and
        any file matching even one of them is included.

        Results are merged in that priority order (level-1 hits first,
        then new level-2 hits, then new level-3 hits), deduped by
        file_id, capped at max_results. Pagination (offset) only walks
        level 1 — it's the exact-match set, so "page 2" of a search means
        page 2 of the best matches, not the loose fallback ones.
        """
        if isinstance(query, list):
            raw_pattern = "|".join(re.escape(q.strip()) for q in query if q and q.strip())
            if not raw_pattern:
                return []
            try:
                regex = compile_regex(raw_pattern)
            except re.error:
                return []
            return await self._regex_search(regex, max_results, offset)

        query = query.strip()
        if not query:
            return []
        words = [w for w in query.split() if w][:8]
        if not words:
            return []

        seen_ids = set()
        merged = []

        def _add(docs):
            for doc in docs:
                fid = doc.get("file_id")
                if fid in seen_ids:
                    continue
                seen_ids.add(fid)
                merged.append(doc)

        # Level 1: strict, in-order match (original behavior).
        if len(words) > 1:
            pattern1 = r".*[\s\.\+\-_]".join(re.escape(w) for w in words)
        else:
            pattern1 = r"(\b|[\.\+\-_])" + re.escape(words[0]) + r"(\b|[\.\+\-_])"
        try:
            regex1 = compile_regex(pattern1)
            level1 = await self._regex_search(regex1, max_results, offset)
        except re.error:
            level1 = []
        _add(level1)

        if offset:
            # Paginating an existing search — stick to level 1 only.
            return merged[:max_results]

        if len(words) == 1 or len(merged) >= max_results:
            return merged[:max_results]

        # Level 2: all words present, any order, anywhere in the name.
        try:
            pattern2 = "".join(f"(?=.*{re.escape(w)})" for w in words)
            regex2 = compile_regex(pattern2)
            level2 = await self._regex_search(regex2, max_results)
        except re.error:
            level2 = []
        _add(level2)

        if len(merged) >= max_results:
            return merged[:max_results]

        # Level 3: EVERY word, checked independently — always runs, not
        # just when levels 1/2 came back empty.
        async def _word_hits(word):
            try:
                regex_w = compile_regex(r"(\b|[\.\+\-_])" + re.escape(word) + r"(\b|[\.\+\-_])")
            except re.error:
                return []
            return await self._regex_search(regex_w, max_results)

        fallback_words = [
            w for w in words
            if len(w) >= 3 and w.lower() not in _SEARCH_FALLBACK_STOP
        ]
        if not fallback_words:
            fallback_words = words[:1]
        per_word = await asyncio.gather(*[_word_hits(w) for w in fallback_words])
        for docs in per_word:
            _add(docs)

        # Keep level 1 → level 2 → level 3 insertion order. Sorting the final
        # merged list by ObjectId made fresh but weak matches outrank exact ones.
        return merged[:max_results]

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
        deleted = 0
        for col in self.file_cols:
            try:
                result = await col.delete_many({"file_id": file_id})
                deleted += result.deleted_count
            except Exception as e:
                logger.warning("File-id delete skipped an unavailable cluster: %s", e)
        if deleted:
            await self._release_registry_ids([file_id])
            self._invalidate_file_count()
        return deleted > 0

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
        # last_searched_at (a real BSON date) drives the TTL index in
        # ensure_indexes() — last_searched (epoch float) is kept as-is for
        # any existing reader of that field; MongoDB TTL only acts on Date
        # fields, so the two coexist rather than replacing one with the other.
        update = {
            "$inc": {"count": 1},
            "$set": {"last_searched": now, "original": query, "last_searched_at": datetime.now(timezone.utc)},
        }
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

        total_users, total_banned, total_groups, total_files, *cluster_stats = await asyncio.gather(
            self.get_user_count(),
            _count(self.banned_col),
            self.get_group_count(),
            self.get_total_files(),
            *[_cluster_stats(i, db_instance) for i, db_instance in enumerate(self.dbs)]
        )

        db_sizes    = [(idx, size) for idx, _, size in cluster_stats]
        return total_users, total_banned, total_files, db_sizes, total_groups

    async def reset_database(self):
        # Avoid starting a cross-cluster destructive operation when one of the
        # targets is already unreachable.
        await asyncio.gather(*[db_instance.command("ping") for db_instance in self.dbs])
        if self.users_col is not None:
            await self.users_col.drop()
        if self.banned_col is not None:
            await self.banned_col.drop()
        for col in self.file_cols:
            await col.drop()
        if self.registry_col is not None:
            await self.registry_col.drop()
        # Recreate uniqueness indexes immediately so a reset cannot leave a
        # running bot accepting duplicate file IDs.
        await self.ensure_indexes()
        self._invalidate_file_count()
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
        for channel_key in ("fsub_channels", "req_fsub_channels", "two_stage_channels"):
            if channel_key in safe:
                safe[channel_key] = [
                    ({"id": e.get("id")} if isinstance(e, dict) else e)
                    for e in safe[channel_key]
                ]
        return safe

    async def restore_config(self, data: dict):
        # Backups are admin-supplied but still untrusted input. Restore only
        # documented presentation/preferences fields with expected types.
        allowed = {
            "start_media": str,
            "welcome_text": str,
            "auto_delete_time": int,
            "maintenance_mode": bool,
            "maintenance_message": str,
            "file_caption_template": str,
            "update_channel": str,
            "main_group": str,
            "group_whitelist_mode": str,
            "req_fsub_interval_hours": int,
        }
        safe_data = {
            key: value for key, value in data.items()
            if key in allowed and type(value) is allowed[key]
        }
        if "auto_delete_time" in safe_data:
            safe_data["auto_delete_time"] = max(60, min(safe_data["auto_delete_time"], 86400))
        if "req_fsub_interval_hours" in safe_data:
            safe_data["req_fsub_interval_hours"] = max(1, min(safe_data["req_fsub_interval_hours"], 720))
        if safe_data.get("group_whitelist_mode") not in (None, "whitelist", "blacklist"):
            safe_data.pop("group_whitelist_mode", None)
        for key in ("start_media", "welcome_text", "maintenance_message", "file_caption_template"):
            if key in safe_data:
                safe_data[key] = safe_data[key][:4096]
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

    async def set_two_stage_channel(self, slot: int, channel_id) -> bool:
        """slot is 1 or 2 — a fixed 2-slot list (unlike fsub_channels/
        req_fsub_channels, which are appendable pools), since the Two-Stage
        Verification gate is specifically a sequential Channel-1-then-
        Channel-2 flow, not "join any N of these"."""
        if self.config_col is None:
            return False
        config   = await self.config_col.find_one({"_id": "bot_config"})
        channels = list(config.get("two_stage_channels", [])) if config else []
        while len(channels) < 2:
            channels.append(None)
        channels[slot - 1] = {"id": channel_id}
        await self.config_col.update_one({"_id": "bot_config"}, {"$set": {"two_stage_channels": channels}}, upsert=True)
        global _config_cache, _config_cache_ts
        _config_cache = None; _config_cache_ts = 0.0
        return True

    async def remove_two_stage_channel(self, slot: int) -> bool:
        if self.config_col is None:
            return False
        config   = await self.config_col.find_one({"_id": "bot_config"})
        channels = list(config.get("two_stage_channels", [])) if config else []
        while len(channels) < 2:
            channels.append(None)
        channels[slot - 1] = None
        await self.config_col.update_one({"_id": "bot_config"}, {"$set": {"two_stage_channels": channels}}, upsert=True)
        global _config_cache, _config_cache_ts
        _config_cache = None; _config_cache_ts = 0.0
        return True

    async def update_two_stage_channel_link(self, channel_id, link):
        if self.config_col is None:
            return
        config = await self.config_col.find_one({"_id": "bot_config"})
        if not config:
            return
        channels = config.get("two_stage_channels", [])
        updated  = []
        for entry in channels:
            if isinstance(entry, dict) and str(entry.get("id")) == str(channel_id):
                entry["link"] = link
            updated.append(entry)
        await self.config_col.update_one({"_id": "bot_config"}, {"$set": {"two_stage_channels": updated}})
        global _config_cache, _config_cache_ts
        _config_cache = None; _config_cache_ts = 0.0

    async def check_two_stage_due(self, user_id: int) -> bool:
        """True if this user needs to go through Two-Stage Verification
        again — i.e. hasn't completed it within the last
        TWO_STAGE_VERIFY_INTERVAL (30 min)."""
        if self.users_col is None:
            return False
        try:
            doc  = await self.users_col.find_one({"_id": user_id}, {"two_stage_verified_at": 1})
            last = doc.get("two_stage_verified_at", 0) if doc else 0
            return (time.time() - last) >= TWO_STAGE_VERIFY_INTERVAL
        except Exception:
            return False

    async def mark_two_stage_verified(self, user_id: int):
        if self.users_col is None:
            return
        try:
            await self.users_col.update_one({"_id": user_id}, {"$set": {"two_stage_verified_at": time.time()}}, upsert=True)
        except Exception:
            pass

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

    async def get_user_language(self, user_id: int) -> str:
        """Persisted UI language preference — additive field on the existing
        users doc, same pattern as the old data_saver flag. Defaults to
        "en"; plugins/start.py's LANG_STRINGS defines what's translated."""
        if self.users_col is None:
            return "en"
        try:
            doc = await self.users_col.find_one({"_id": user_id}, {"language": 1})
            return doc.get("language", "en") if doc else "en"
        except Exception:
            return "en"

    async def set_user_language(self, user_id: int, lang: str):
        if self.users_col is None:
            return
        try:
            await self.users_col.update_one({"_id": user_id}, {"$set": {"language": lang}}, upsert=True)
        except Exception:
            pass

    async def save_pending_request(self, user_id, movie_name):
        if self.main_db is None:
            return
        requests_col = self.main_db["pending_requests"]
        # requested_at (a real BSON date) drives the TTL index in
        # ensure_indexes() — timestamp (epoch float) is kept as-is for any
        # existing reader of that field, same coexistence approach as
        # log_missed_search()'s last_searched_at above.
        await requests_col.update_one(
            {"user_id": user_id, "movie_name": movie_name.lower().strip()},
            {"$set": {
                "user_id": user_id, "movie_name": movie_name.lower().strip(),
                "original_name": movie_name, "timestamp": time.time(),
                "requested_at": datetime.now(timezone.utc),
            }},
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

    async def pending_request_exists(self, user_id, movie_name) -> bool:
        """Used by the manual "Mark Uploaded" admin ticket flow to check
        whether _fulfill_matching_requests() already auto-fulfilled (and
        deleted) this same request before the admin got to it — avoids a
        duplicate "your movie is ready" ping to the user."""
        if self.main_db is None:
            return False
        doc = await self.main_db["pending_requests"].find_one(
            {"user_id": user_id, "movie_name": movie_name.lower().strip()}
        )
        return doc is not None

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

    async def schedule_deletion(self, chat_id: int, message_id: int, delay_seconds: int):
        """Persist an auto-delete job so it survives bot restarts."""
        if self.deletion_col is None:
            return False
        due_at = datetime.now(timezone.utc).timestamp() + max(0, int(delay_seconds))
        await self.deletion_col.update_one(
            {"chat_id": chat_id, "message_id": message_id},
            {"$set": {"due_at": due_at, "attempts": 0}},
            upsert=True,
        )
        return True

    async def get_due_deletions(self, limit=100):
        if self.deletion_col is None:
            return []
        cursor = self.deletion_col.find(
            {"due_at": {"$lte": datetime.now(timezone.utc).timestamp()}}
        ).sort("due_at", 1).limit(limit)
        return [doc async for doc in cursor]

    async def complete_deletion(self, job_id):
        if self.deletion_col is not None:
            await self.deletion_col.delete_one({"_id": job_id})

    async def retry_deletion(self, job_id, delay_seconds=60):
        if self.deletion_col is not None:
            await self.deletion_col.update_one(
                {"_id": job_id},
                {
                    "$inc": {"attempts": 1},
                    "$set": {"due_at": datetime.now(timezone.utc).timestamp() + delay_seconds},
                },
            )

    async def close(self):
        """Close all MongoDB clients during a graceful shutdown."""
        for client in self.clients:
            await client.close()


db = Database()
