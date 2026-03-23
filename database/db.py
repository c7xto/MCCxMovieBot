import os
import re
import time
import json
import asyncio
import logging
from bson.objectid import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# load_dotenv() MUST be here — Database() is instantiated at the bottom of this
# file at import time, before bot.py's load_dotenv() has a chance to run.
load_dotenv()

logger = logging.getLogger(__name__)

# ── CONFIG CACHE ──────────────────────────────────────────────────────────────
# get_config() is called on every search, every file send, every FSub check.
# At scale this hammers MongoDB. Cache with 60-second TTL — invalidated on
# every update_config() call so the panel always shows fresh data.
_config_cache = None
_config_cache_ts = 0.0
_CONFIG_TTL = 60  # seconds


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
                client = AsyncIOMotorClient(uri)
                self.clients.append(client)
                db_instance = client[f"MCCxBot_Cluster_{i+1}"]
                self.dbs.append(db_instance)
                self.file_cols.append(db_instance["movies"])

        self.users_col = None
        self.banned_col = None
        self.config_col = None
        self.cache_col = None
        self.indexer_col = None
        self.main_db = None

        if self.dbs:
            self.main_db = self.dbs[0]
            self.users_col = self.main_db["users"]
            self.banned_col = self.main_db["banned_users"]
            self.config_col = self.main_db["bot_config"]
            _ops_db = self.dbs[1] if len(self.dbs) > 1 else self.main_db
            self.cache_col = _ops_db["search_cache"]
            self.indexer_col = _ops_db["indexer_tasks"]

    # ── STARTUP TASKS ─────────────────────────────────────────────────────────

    async def ensure_indexes(self):
        """Creates file_name index on all clusters. Idempotent — safe every startup."""
        for i, col in enumerate(self.file_cols):
            try:
                await col.create_index("file_name")
                logger.info(f"✅ Index ensured on Cluster {i+1}")
            except Exception as e:
                logger.warning(f"⚠️ Could not create index on Cluster {i+1}: {e}")

        # Index missed_searches by count for fast top-N queries
        if self.main_db is not None:
            try:
                await self.main_db["missed_searches"].create_index(
                    [("count", -1)]
                )
            except Exception:
                pass

    async def sync_config(self):
        """
        One-time migration: reads values from .env and writes any missing ones
        to MongoDB. Safe to call every startup — never overwrites existing DB values.
        """
        if self.config_col is None:
            return

        config = await self.config_col.find_one({'_id': 'bot_config'})

        migrations = {
            'log_channel':       int(os.getenv("LOG_CHANNEL_ID", 0) or 0),
            'db_channel':        int(os.getenv("DATABASE_CHANNEL_ID", 0) or 0),
            'update_channel_id': int(os.getenv("UPDATE_CHANNEL", 0) or 0),
            'update_channel':    os.getenv("UPDATE_CHANNEL_LINK", ""),
            'main_group':        os.getenv("MAIN_GROUP_LINK", ""),
        }

        fields_to_set = {}
        for key, env_val in migrations.items():
            if config is None or key not in config:
                if env_val:
                    fields_to_set[key] = env_val
                    logger.info(f"  📥 Migrating '{key}' from .env → MongoDB: {env_val}")

        if fields_to_set:
            await self.config_col.update_one(
                {'_id': 'bot_config'},
                {'$set': fields_to_set},
                upsert=True
            )
            logger.info(f"✅ Config sync complete — {len(fields_to_set)} field(s) migrated.")
        else:
            logger.info("✅ Config sync complete — nothing to migrate.")

    # ── USER & BAN SYSTEM ─────────────────────────────────────────────────────

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
        await self.banned_col.update_one(
            {"_id": user_id}, {"$set": {"_id": user_id}}, upsert=True
        )

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

    # ── GROUP SYSTEM ──────────────────────────────────────────────────────────

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
        groups_col = self.main_db["connected_groups"]
        cursor = groups_col.find({})
        return [doc async for doc in cursor]

    async def get_group_count(self):
        if self.main_db is None:
            return 0
        groups_col = self.main_db["connected_groups"]
        return await groups_col.count_documents({})

    async def get_group(self, group_id):
        if self.main_db is None:
            return None
        groups_col = self.main_db["connected_groups"]
        return await groups_col.find_one({"_id": group_id})

    async def update_group(self, group_id, fields: dict):
        """Updates specific fields on a group document."""
        if self.main_db is None:
            return
        groups_col = self.main_db["connected_groups"]
        await groups_col.update_one(
            {"_id": group_id},
            {"$set": fields},
            upsert=True
        )

    async def ban_group(self, group_id):
        if self.main_db is None:
            return
        groups_col = self.main_db["connected_groups"]
        await groups_col.update_one(
            {"_id": group_id},
            {"$set": {"banned": True}},
            upsert=True
        )

    async def unban_group(self, group_id):
        if self.main_db is None:
            return
        groups_col = self.main_db["connected_groups"]
        await groups_col.update_one(
            {"_id": group_id},
            {"$set": {"banned": False}}
        )

    async def is_group_banned(self, group_id):
        if self.main_db is None:
            return False
        groups_col = self.main_db["connected_groups"]
        doc = await groups_col.find_one({"_id": group_id})
        return doc.get("banned", False) if doc else False

    async def is_group_whitelisted(self, group_id):
        if self.main_db is None:
            return True  # fail open if DB unavailable
        groups_col = self.main_db["connected_groups"]
        doc = await groups_col.find_one({"_id": group_id})
        return doc.get("whitelisted", False) if doc else False

    async def increment_group_search(self, group_id):
        """Atomically increments the search counter for a group."""
        if self.main_db is None:
            return
        groups_col = self.main_db["connected_groups"]
        await groups_col.update_one(
            {"_id": group_id},
            {"$inc": {"search_count": 1}},
            upsert=True
        )

    async def get_top_groups(self, limit=10):
        """Returns top N groups by search volume."""
        if self.main_db is None:
            return []
        groups_col = self.main_db["connected_groups"]
        cursor = groups_col.find({}).sort("search_count", -1).limit(limit)
        return [doc async for doc in cursor]

    # ── FILE SYSTEM ───────────────────────────────────────────────────────────

    async def get_db_size(self, db_instance):
        try:
            stats = await db_instance.command("dbstats")
            return max(stats.get("storageSize", 0), stats.get("dataSize", 0)) / (1024 * 1024)
        except Exception:
            return 0

    async def get_total_files(self):
        total = 0
        for col in self.file_cols:
            try:
                total += await col.count_documents({})
            except Exception:
                pass
        return total

    async def save_file(self, media):
        file_id = getattr(media, "file_id", "")
        file_name = getattr(media, "file_name", "")
        file_size = getattr(media, "file_size", 0)
        mime_type = getattr(media, "mime_type", "")

        if not file_id or not file_name:
            return False, "Invalid media"

        file_doc = {
            "file_id": file_id,
            "file_name": file_name,
            "file_size": file_size,
            "mime_type": mime_type
        }

        for col in self.file_cols:
            if await col.find_one({"file_id": file_id}):
                return False, "Duplicate"

        for i, col in enumerate(self.file_cols):
            size = await self.get_db_size(self.dbs[i])
            if size < 450:
                await col.insert_one(file_doc)
                return True, f"Saved to Cluster {i+1}"

        return False, "All clusters full"

    async def save_files_bulk(self, files_list):
        if not files_list:
            return 0, 0

        incoming_ids = [f["file_id"] for f in files_list]
        existing_ids = set()
        for col in self.file_cols:
            cursor = col.find({"file_id": {"$in": incoming_ids}}, {"file_id": 1})
            async for doc in cursor:
                existing_ids.add(doc["file_id"])

        new_files = [f for f in files_list if f["file_id"] not in existing_ids]
        duplicates = len(files_list) - len(new_files)

        if not new_files:
            return 0, duplicates

        saved_total = 0
        remaining = new_files[:]

        for i, col in enumerate(self.file_cols):
            if not remaining:
                break
            size = await self.get_db_size(self.dbs[i])
            if size >= 450:
                continue
            try:
                await col.insert_many(remaining, ordered=False)
                saved_total += len(remaining)
                remaining = []
            except Exception as e:
                if "space quota" in str(e).lower() or "over your space" in str(e).lower():
                    logger.error(f"Cluster {i+1} FULL — add DATABASE_URI_{i+2} to .env")
                else:
                    logger.warning(f"Cluster {i+1} bulk insert partial failure: {e}")

        return saved_total, duplicates

    async def admin_search_files(self, query, limit=20):
        """
        Admin file search — same regex as user search but returns raw docs
        with full metadata so admin can see file_id, cluster, etc.
        """
        results = []
        clean = re.sub(r'[^a-zA-Z0-9]', ' ', query.strip())
        words = [w for w in clean.split() if w]
        if not words:
            return []

        conditions = []
        for word in words:
            conditions.append({
                "file_name": {
                    "$regex": f"(?:^|[\\W_]){re.escape(word)}(?:[\\W_]|$)",
                    "$options": "i"
                }
            })
        mongo_query = {"$and": conditions}

        for i, col in enumerate(self.file_cols):
            cursor = col.find(mongo_query).limit(limit)
            async for doc in cursor:
                doc["_cluster"] = i + 1  # tag with cluster number
                results.append(doc)
            if len(results) >= limit:
                break

        return results[:limit]

    async def delete_file_by_obj_id(self, file_obj_id):
        """Deletes a file by its MongoDB ObjectId — used by admin file manager."""
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
        """Updates the stored name of a file — admin can fix typos."""
        try:
            obj_id = ObjectId(file_obj_id)
        except Exception:
            return False
        for col in self.file_cols:
            result = await col.update_one(
                {"_id": obj_id},
                {"$set": {"file_name": new_name}}
            )
            if result.matched_count > 0:
                return True
        return False

    async def get_files_by_language(self):
        """
        Returns file counts per language across all clusters.
        Used for the language breakdown in admin stats panel.
        """
        from plugins.filter import LANGUAGES
        results = {}
        for lang in LANGUAGES:
            count = 0
            regex = {"file_name": {"$regex": rf"\b{lang}\b", "$options": "i"}}
            for col in self.file_cols:
                try:
                    count += await col.count_documents(regex)
                except Exception:
                    pass
            results[lang] = count
        return results

    async def find_duplicate_files(self):
        """
        Two-pass duplicate scanner:
        Pass 1 — exact: group by file_id (same file uploaded multiple times, 100% reliable)
        Pass 2 — fuzzy: normalize filename and group by cleaned title
        Both passes use allowDiskUse=True so they work on large collections.
        """
        import re as _re

        def _normalize(name):
            if not name:
                return ""
            n = _re.sub(r'\.(mkv|mp4|avi|mov|zip|srt)$', '', name, flags=_re.IGNORECASE)
            n = _re.sub(
                r'\b(1080p|720p|480p|360p|4k|2160p|hdrip|hd.rip|webrip|web-dl|webdl|'
                r'bluray|predvd|cam|hdcam|tsrip|dvdrip|x264|x265|hevc|aac|esub|hsub|'
                r'10bit|hq|nf|amzn|dual.audio|multi.audio|malayalam|tamil|telugu|hindi|'
                r'english|kannada|1xbet|tamilblasters|tamilmv|moviezwap)\b',
                '', n, flags=_re.IGNORECASE)
            n = _re.sub(r'\b(19|20)\d{2}\b', '', n)
            n = _re.sub(r'[\[\(].*?[\]\)]', '', n)
            n = _re.sub(r'[._\-]', ' ', n)
            n = _re.sub(r'\s+', ' ', n).strip().lower()
            return n

        exact_data = {}
        fuzzy_data = {}

        for col in self.file_cols:
            # Pass 1: exact duplicates by file_id
            try:
                pipeline = [
                    {"$match": {"file_id": {"$exists": True, "$ne": ""}}},
                    {"$group": {
                        "_id": "$file_id",
                        "count": {"$sum": 1},
                        "ids": {"$push": {"$toString": "$_id"}},
                        "name": {"$first": "$file_name"}
                    }},
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
                        exact_data[fid] = {
                            "count": doc["count"],
                            "ids": doc["ids"],
                            "name": doc.get("name", "Unknown")
                        }
            except Exception as e:
                logger.warning(f"Exact duplicate scan error: {e}")

            # Pass 2: fuzzy duplicates by normalized filename
            try:
                cursor = col.find(
                    {"file_name": {"$exists": True, "$ne": ""}},
                    {"file_name": 1, "_id": 1}
                )
                async for doc in cursor:
                    raw = doc.get("file_name", "")
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

        results = []
        exact_id_set = set(i for d in exact_data.values() for i in d["ids"])

        for fid, data in exact_data.items():
            results.append({"name": data["name"], "count": data["count"],
                            "ids": data["ids"], "type": "exact"})

        for norm, data in fuzzy_data.items():
            if data["count"] < 2:
                continue
            uncovered = [i for i in data["ids"] if i not in exact_id_set]
            if len(uncovered) < 2:
                continue
            results.append({"name": data["original_name"], "count": data["count"],
                            "ids": data["ids"], "type": "fuzzy"})

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
    async def purge_by_pattern(self, pattern):
        """
        Deletes all files matching a regex pattern across all clusters.
        Used by admin bulk delete (F10). Same logic as purge_cams but parameterized.
        """
        deleted_total = 0
        for col in self.file_cols:
            result = await col.delete_many(
                {"file_name": {"$regex": pattern, "$options": "i"}}
            )
            deleted_total += result.deleted_count
        return deleted_total

    async def count_by_pattern(self, pattern):
        """Counts files matching a pattern — used for dry-run preview before bulk delete."""
        total = 0
        for col in self.file_cols:
            try:
                total += await col.count_documents(
                    {"file_name": {"$regex": pattern, "$options": "i"}}
                )
            except Exception:
                pass
        return total

    async def migrate_cluster(self, from_idx: int, to_idx: int, batch_size=100):
        """
        Moves all files from one cluster to another.
        Returns (migrated, skipped) counts.
        Checks destination size before starting.
        """
        if from_idx >= len(self.file_cols) or to_idx >= len(self.file_cols):
            return 0, 0

        from_col = self.file_cols[from_idx]
        to_db = self.dbs[to_idx]
        to_size = await self.get_db_size(to_db)

        if to_size >= 450:
            return 0, -1  # -1 signals destination full

        migrated, skipped = 0, 0
        cursor = from_col.find({})

        batch = []
        async for doc in cursor:
            doc.pop("_id", None)  # remove old _id so MongoDB generates a new one
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
            except Exception as e:
                skipped += len(batch)

        return migrated, skipped

    async def get_search_results(self, query):
        """Strict $and regex search across all clusters with natural sort."""
        results = []
        clean = re.sub(r'[^a-zA-Z0-9]', ' ', query.strip())
        words = [w for w in clean.split() if w]

        if not words:
            return []

        search_conditions = []
        for word in words:
            strict_regex = f"(?:^|[\\W_]){re.escape(word)}(?:[\\W_]|$)"
            search_conditions.append({"file_name": {"$regex": strict_regex, "$options": "i"}})

        mongo_query = {"$and": search_conditions}

        for col in self.file_cols:
            cursor = col.find(mongo_query).limit(40)
            async for doc in cursor:
                if not any(r["file_id"] == doc["file_id"] for r in results):
                    results.append(doc)
                if len(results) >= 40:
                    break
            if len(results) >= 40:
                break

        def natural_sort_key(file_doc):
            return [int(text) if text.isdigit() else text.lower()
                    for text in re.split(r'(\d+)', file_doc['file_name'])]

        results.sort(key=natural_sort_key)
        return results[:40]

    async def get_prefix_suggestions(self, query, limit=3):
        """
        S10: Spell correction — when strict search finds nothing, try prefix
        match on the first meaningful word (5+ chars) to suggest alternatives.
        """
        clean = re.sub(r'[^a-zA-Z0-9]', ' ', query.strip())
        words = [w for w in clean.split() if len(w) >= 5]
        if not words:
            return []

        prefix = words[0][:5]
        suggestions = []
        seen_titles = set()

        for col in self.file_cols:
            cursor = col.find(
                {"file_name": {"$regex": f"^{re.escape(prefix)}", "$options": "i"}},
                {"file_name": 1}
            ).limit(10)
            async for doc in cursor:
                name = doc.get("file_name", "")
                # Extract just the movie title (first few words)
                title_words = name.split()[:4]
                title = " ".join(title_words)
                if title.lower() not in seen_titles:
                    seen_titles.add(title.lower())
                    suggestions.append(title)
                if len(suggestions) >= limit:
                    break
            if len(suggestions) >= limit:
                break

        return suggestions[:limit]

    async def get_file(self, file_obj_id):
        try:
            obj_id = ObjectId(file_obj_id)
        except Exception:
            return None
        for col in self.file_cols:
            file_data = await col.find_one({"_id": obj_id})
            if file_data:
                return file_data
        return None

    async def delete_file_by_id(self, file_id):
        """Removes a dead/expired Telegram file from the database."""
        for col in self.file_cols:
            result = await col.delete_one({"file_id": file_id})
            if result.deleted_count > 0:
                return True
        return False

    async def purge_cams(self):
        """Kept for backward compat — delegates to purge_by_pattern."""
        return await self.purge_by_pattern(r"cam|predvd|hdcam|tsrip|1xbet")

    # ── MISSED SEARCH TRACKER (F9) ────────────────────────────────────────────

    async def log_missed_search(self, query: str):
        """
        Increments the counter for a missed search query.
        Also returns True if this query should trigger a log channel alert
        (i.e. it hasn't been alerted in the last hour).
        This replaces the in-RAM MISSED_CACHE set — MongoDB is the single source.
        """
        if self.main_db is None:
            return False
        col = self.main_db["missed_searches"]
        cleaned = re.sub(r'[^a-zA-Z0-9 ]', '', query.lower()).strip()
        if not cleaned:
            return False

        now = time.time()
        cooldown = 3600  # 1 hour between log channel alerts for same query

        existing = await col.find_one({"_id": cleaned})
        should_alert = (
            existing is None or
            (now - existing.get("last_alerted", 0)) > cooldown
        )

        update = {
            "$inc": {"count": 1},
            "$set": {"last_searched": now, "original": query}
        }
        if should_alert:
            update["$set"]["last_alerted"] = now

        try:
            await col.update_one({"_id": cleaned}, update, upsert=True)
        except Exception:
            pass
        return should_alert

    async def get_top_missed(self, limit=15):
        """Returns top N most searched-for missing titles."""
        if self.main_db is None:
            return []
        col = self.main_db["missed_searches"]
        cursor = col.find({}).sort("count", -1).limit(limit)
        return [doc async for doc in cursor]

    async def clear_missed_search(self, query_id: str):
        """Removes a fulfilled entry from the missed searches list."""
        if self.main_db is None:
            return
        col = self.main_db["missed_searches"]
        await col.delete_one({"_id": query_id})

    # ── STATS ─────────────────────────────────────────────────────────────────

    async def get_bot_stats(self):
        """Full stats — users, banned, files per cluster, sizes, groups."""
        total_users = await self.users_col.count_documents({}) if self.users_col is not None else 0
        total_banned = await self.banned_col.count_documents({}) if self.banned_col is not None else 0
        total_groups = await self.get_group_count()

        total_files = 0
        db_sizes = []

        for i, db_instance in enumerate(self.dbs):
            files_in_db = await self.file_cols[i].count_documents({})
            total_files += files_in_db
            size = await self.get_db_size(db_instance)
            db_sizes.append((i + 1, size))

        return total_users, total_banned, total_files, db_sizes, total_groups

    async def reset_database(self):
        if self.users_col is not None:
            await self.users_col.drop()
        if self.banned_col is not None:
            await self.banned_col.drop()
        for col in self.file_cols:
            await col.drop()
        return True

    # ── CONFIG SYSTEM (with cache) ────────────────────────────────────────────

    async def get_config(self):
        """
        Fetches live bot settings from MongoDB with 60-second TTL cache.
        Cache is invalidated on every update_config() call.
        """
        global _config_cache, _config_cache_ts

        now = time.time()
        if _config_cache is not None and (now - _config_cache_ts) < _CONFIG_TTL:
            return _config_cache

        if self.config_col is None:
            return {}

        config = await self.config_col.find_one({'_id': 'bot_config'})
        if not config:
            config = {
                '_id': 'bot_config',
                'start_media': "https://files.catbox.moe/wvdeci.mp4",
                'fsub_channels': [],
                'db_channels': [],
                'auto_delete_time': 300,
                'maintenance_mode': False,
                'maintenance_message': "🔧 Bot is under maintenance. Back soon!",
                'group_whitelist_enabled': False,
                'file_caption_template': "",
            }
            await self.config_col.insert_one(config)

        _config_cache = config
        _config_cache_ts = now
        return config

    async def update_config(self, key, value):
        """Updates a config field and invalidates the cache immediately."""
        global _config_cache, _config_cache_ts

        if self.config_col is None:
            return False
        await self.config_col.update_one(
            {'_id': 'bot_config'},
            {'$set': {key: value}},
            upsert=True
        )
        # Invalidate cache so next read fetches fresh data
        _config_cache = None
        _config_cache_ts = 0.0
        return True

    async def export_config(self):
        """
        C9: Returns config as a JSON-serialisable dict, excluding sensitive fields.
        Safe to send to admin as a backup file.
        """
        config = await self.get_config()
        # Fields to exclude from export (security/infrastructure)
        exclude = {
            '_id', 'log_channel', 'admin_id', 'db_channels',
            'update_channel_id', 'db_channel'
        }
        return {k: v for k, v in config.items() if k not in exclude}

    async def restore_config(self, data: dict):
        """
        C10: Restores config from a dict. Never overwrites infrastructure fields.
        Only restores UI/display settings.
        """
        # Fields that must never be restored from backup
        protected = {
            '_id', 'log_channel', 'admin_id', 'db_channels',
            'update_channel_id', 'db_channel', 'fsub_channels'
        }
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

        await self.config_col.update_one(
            {'_id': 'bot_config'},
            {'$pull': {'fsub_channels': {'id': channel_id}}},
        )
        await self.config_col.update_one(
            {'_id': 'bot_config'},
            {'$pull': {'fsub_channels': channel_id}},
        )
        await self.config_col.update_one(
            {'_id': 'bot_config'},
            {'$push': {'fsub_channels': entry}},
            upsert=True
        )
        # Invalidate cache
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0
        return True

    async def update_fsub_channel_link(self, channel_id, link):
        if self.config_col is None:
            return
        config = await self.config_col.find_one({'_id': 'bot_config'})
        if not config:
            return
        channels = config.get('fsub_channels', [])
        updated = []
        for entry in channels:
            if isinstance(entry, dict) and entry.get('id') == channel_id:
                entry['link'] = link
            updated.append(entry)
        await self.config_col.update_one(
            {'_id': 'bot_config'},
            {'$set': {'fsub_channels': updated}}
        )
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0

    async def remove_fsub_channel(self, channel_id):
        if self.config_col is None:
            return False
        await self.config_col.update_one(
            {'_id': 'bot_config'},
            {'$pull': {'fsub_channels': {'id': channel_id}}}
        )
        await self.config_col.update_one(
            {'_id': 'bot_config'},
            {'$pull': {'fsub_channels': channel_id}}
        )
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0
        return True

    async def add_db_channel(self, channel_id):
        if self.config_col is None:
            return False
        await self.config_col.update_one(
            {'_id': 'bot_config'},
            {'$addToSet': {'db_channels': channel_id}},
            upsert=True
        )
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0
        return True

    async def remove_db_channel(self, channel_id):
        if self.config_col is None:
            return False
        await self.config_col.update_one(
            {'_id': 'bot_config'},
            {'$pull': {'db_channels': channel_id}}
        )
        global _config_cache, _config_cache_ts
        _config_cache = None
        _config_cache_ts = 0.0
        return True

    # ── PENDING REQUESTS ──────────────────────────────────────────────────────

    # ── REQUEST FSub CHANNELS ─────────────────────────────────────────────────
    # Separate pool from main FSub. Up to 5 private channels.
    # One random channel shown per prompt. Timer tracked per user.

    async def add_req_fsub_channel(self, channel_id):
        if self.config_col is None:
            return False, "No DB"
        config = await self.config_col.find_one({"_id": "bot_config"})
        existing = config.get("req_fsub_channels", []) if config else []
        if len(existing) >= 5:
            return False, "Max 5 reached"
        for e in existing:
            eid = e.get("id") if isinstance(e, dict) else e
            if str(eid) == str(channel_id):
                return False, "Already exists"
        await self.config_col.update_one(
            {"_id": "bot_config"},
            {"$push": {"req_fsub_channels": {"id": channel_id}}},
            upsert=True
        )
        global _config_cache, _config_cache_ts
        _config_cache = None; _config_cache_ts = 0.0
        return True, "Added"

    async def remove_req_fsub_channel(self, channel_id):
        if self.config_col is None:
            return False
        await self.config_col.update_one(
            {"_id": "bot_config"},
            {"$pull": {"req_fsub_channels": {"id": channel_id}}}
        )
        await self.config_col.update_one(
            {"_id": "bot_config"},
            {"$pull": {"req_fsub_channels": channel_id}}
        )
        global _config_cache, _config_cache_ts
        _config_cache = None; _config_cache_ts = 0.0
        return True

    async def update_req_fsub_link(self, channel_id, link):
        """Store generated invite link for a req_fsub private channel."""
        if self.config_col is None:
            return
        config = await self.config_col.find_one({"_id": "bot_config"})
        if not config:
            return
        channels = config.get("req_fsub_channels", [])
        updated = []
        for entry in channels:
            if isinstance(entry, dict) and str(entry.get("id")) == str(channel_id):
                entry["link"] = link
            updated.append(entry)
        await self.config_col.update_one(
            {"_id": "bot_config"},
            {"$set": {"req_fsub_channels": updated}}
        )
        global _config_cache, _config_cache_ts
        _config_cache = None; _config_cache_ts = 0.0

    async def get_req_fsub_interval(self):
        """Returns configured req_fsub interval in seconds (default 24h)."""
        config = await self.get_config()
        hours = int(config.get("req_fsub_interval_hours", 24))
        return hours * 3600

    async def check_req_fsub_due(self, user_id: int) -> bool:
        """True if enough time has passed since last req_fsub prompt for this user."""
        if self.users_col is None:
            return False
        try:
            doc = await self.users_col.find_one({"_id": user_id}, {"req_fsub_last": 1})
            last = doc.get("req_fsub_last", 0) if doc else 0
            interval = await self.get_req_fsub_interval()
            import time as _t
            return (_t.time() - last) >= interval
        except Exception:
            return False

    async def mark_req_fsub_shown(self, user_id: int):
        """Record that we just showed req_fsub prompt to this user."""
        if self.users_col is None:
            return
        try:
            import time as _t
            await self.users_col.update_one(
                {"_id": user_id},
                {"$set": {"req_fsub_last": _t.time()}},
                upsert=True
            )
        except Exception:
            pass


    async def save_pending_request(self, user_id, movie_name):
        if self.main_db is None:
            return
        requests_col = self.main_db["pending_requests"]
        await requests_col.update_one(
            {"user_id": user_id, "movie_name": movie_name.lower().strip()},
            {"$set": {
                "user_id": user_id,
                "movie_name": movie_name.lower().strip(),
                "original_name": movie_name,
                "timestamp": time.time()
            }},
            upsert=True
        )

    async def find_matching_requests(self, file_name):
        if self.main_db is None:
            return []
        requests_col = self.main_db["pending_requests"]
        clean = re.sub(r'[^a-zA-Z0-9 ]', ' ', file_name)
        words = [w for w in clean.split() if len(w) >= 5 and not w.isdigit()]
        if not words:
            return []
        conditions = [
            {"movie_name": {"$regex": word[:5], "$options": "i"}}
            for word in words[:3]
        ]
        cursor = requests_col.find({"$or": conditions})
        matches = []
        async for doc in cursor:
            matches.append({
                "user_id": doc["user_id"],
                "movie_name": doc["original_name"]
            })
        return matches

    async def delete_pending_request(self, user_id, movie_name):
        if self.main_db is None:
            return
        requests_col = self.main_db["pending_requests"]
        await requests_col.delete_one({
            "user_id": user_id,
            "movie_name": movie_name.lower().strip()
        })

    # ── INDEXER PROGRESS ──────────────────────────────────────────────────────

    async def set_index_progress(self, chat_id, msg_id):
        if self.main_db is None:
            return
        try:
            settings = self.main_db["settings"]
            await settings.update_one({"_id": "index_progress"}, {"$set": {str(chat_id): msg_id}}, upsert=True)
        except Exception as e:
            logger.warning(f"set_index_progress failed: {e}")

    async def get_index_progress(self, chat_id):
        if self.main_db is None:
            return 0
        settings = self.main_db["settings"]
        data = await settings.find_one({"_id": "index_progress"})
        if data and str(chat_id) in data:
            return data[str(chat_id)]
        return 0

    async def clear_index_progress(self, chat_id=None):
        """
        Clears saved index progress.
        If chat_id given — clears only that channel's progress.
        If chat_id is None — wipes ALL saved progress (full reset).
        """
        if self.main_db is None:
            return
        try:
            settings = self.main_db["settings"]
            if chat_id is None:
                await settings.delete_one({"_id": "index_progress"})
            else:
                await settings.update_one(
                    {"_id": "index_progress"},
                    {"$unset": {str(chat_id): ""}}
                )
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
        doc = await self.indexer_col.find_one({"_id": str(chat_id)})
        return doc["state"] if doc else None

    async def clear_index_task(self, chat_id):
        if self.indexer_col is None:
            return
        await self.indexer_col.delete_one({"_id": str(chat_id)})

    async def clear_all_index_tasks(self):
        if self.indexer_col is None:
            return
        await self.indexer_col.delete_many({})

    async def get_stale_index_tasks(self, older_than_seconds=7200):
        """A10: Returns indexer tasks that have been running for too long (likely crashed)."""
        if self.indexer_col is None:
            return []
        cutoff = time.time() - older_than_seconds
        cursor = self.indexer_col.find({
            "state": "running",
            "updated": {"$lt": cutoff}
        })
        return [doc async for doc in cursor]

    # ── SEARCH SESSION CACHE ──────────────────────────────────────────────────

    async def save_search(self, session_id, data):
        if self.cache_col is None:
            return
        try:
            await self.cache_col.update_one({"_id": session_id}, {"$set": data}, upsert=True)
        except Exception as e:
            logger.warning(f"save_search failed: {e}")

    async def get_search(self, session_id):
        if self.cache_col is None:
            return None
        try:
            return await self.cache_col.find_one({"_id": session_id})
        except Exception:
            return None

    async def clear_old_searches(self, expiry_seconds=600):
        if self.cache_col is None:
            return
        try:
            cutoff = time.time() - expiry_seconds
            await self.cache_col.delete_many({"time": {"$lt": cutoff}})
        except Exception:
            pass


db = Database()
