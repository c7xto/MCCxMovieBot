"""Live channel receipts, bounded recovery and permanent release posts."""

import asyncio
import logging
import os
import time
from types import SimpleNamespace

from bson import ObjectId
from pyrogram.enums import ChatType, ChatMemberStatus, ParseMode
from pyrogram.errors import FloodWait, BadRequest, Forbidden, MessageNotModified
from pyrogram.types import InlineKeyboardButton

from database.db import db
from database.releases import ReleaseStore, digest
from plugins.mobile_ui import MobileInlineKeyboardMarkup
from plugins.release_identity import parse_release, render_release
from plugins.telegram_retry import INTERACTIVE_RETRY, telegram_call
from tmdb import release_metadata

logger = logging.getLogger(__name__)


def store():
    if db.operations_db is None:
        raise RuntimeError("Operations database is unavailable")
    return ReleaseStore(db.operations_db)


def source_ids(config):
    values = list(config.get("db_channels", [])) + [os.getenv("DATABASE_CHANNEL_ID", "0")]
    return sorted({int(value) for value in values if str(value).lstrip("-").isdigit() and int(value) < 0})


def media_snapshot(message):
    media = message.document or message.video or message.audio
    if not media or not getattr(media, "file_name", None):
        return None
    return {
        key: getattr(media, key, None)
        for key in (
            "file_id",
            "file_unique_id",
            "file_name",
            "file_size",
            "mime_type",
        )
    }


async def capture_source_message(client, message):
    """Called before readiness and ephemeral dedupe; never replies in sources."""
    if getattr(message.chat, "type", None) != ChatType.CHANNEL:
        return False
    config = await db.get_config()
    if message.chat.id not in source_ids(config):
        return False
    timestamp = message.date.timestamp() if message.date else time.time()
    await store().receive(message.chat.id, message.id, media_snapshot(message), uploaded_at=timestamp)
    return True


async def validate_source(client, source):
    chat = await telegram_call(
        lambda: client.get_chat(source), route="live_source_check", policy=INTERACTIVE_RETRY, retry_safe=False
    )
    if chat.type != ChatType.CHANNEL:
        raise ValueError("Select a Telegram channel as the database source")
    member = await telegram_call(
        lambda: client.get_chat_member(source, client.me.id),
        route="live_source_membership",
        policy=INTERACTIVE_RETRY,
        retry_safe=False,
    )
    if member.status not in {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER}:
        raise ValueError("Make the Movie Bot an administrator in the source channel")
    return chat


async def validate_destination(client, destination):
    await validate_source(client, destination)
    member = await telegram_call(
        lambda: client.get_chat_member(destination, client.me.id),
        route="release_destination_check",
        policy=INTERACTIVE_RETRY,
        retry_safe=False,
    )
    privileges = getattr(member, "privileges", None)
    if member.status != ChatMemberStatus.OWNER and not getattr(privileges, "can_post_messages", False):
        raise ValueError("Grant Post Messages permission in New Releases")


async def find_indexed_file(media):
    """A registry reservation alone is not proof a file is searchable."""
    query = {"file_id": media["file_id"]}
    if media.get("file_unique_id"):
        query = {"$or": [query, {"file_unique_id": media["file_unique_id"]}]}
    registry = await db.registry_col.find_one(query)
    if registry and registry.get("movie_id") and registry.get("cluster"):
        index = int(registry["cluster"]) - 1
        if 0 <= index < len(db.file_cols):
            document = await db.file_cols[index].find_one({"_id": ObjectId(registry["movie_id"])})
            if document:
                return document, index
    # Bounded fallback repairs the save-before-registry-location crash window.
    for index in db.readable_shard_indices():
        async with asyncio.timeout(4):
            document = await db.file_cols[index].find_one(query, max_time_ms=3000)
        if document:
            return document, index
    raise RuntimeError("File save is not yet confirmed in a movie shard")


async def ingest_receipt(receipt, persistence):
    media = receipt["media"]
    saved, reason = await db.save_file(SimpleNamespace(**media))
    if not saved and reason != "Duplicate":
        raise RuntimeError(reason)
    document, shard = await find_indexed_file(media)
    if receipt.get("announce"):
        parsed = parse_release(media["file_name"])
        await persistence.enqueue_candidate(
            receipt,
            parsed,
            {
                "file_key": media.get("file_unique_id") or digest(document["file_id"]),
                "movie_id": str(document["_id"]),
                "shard": shard,
                "file_size": int(document.get("file_size") or 0),
            },
        )
        # Keep existing user-request fulfilment, independently of public posts.
        job = await db.enqueue_request_fulfillment(media["file_name"])
        if job is None:
            raise RuntimeError("Request notification queue is full")
    result = await persistence.finish(
        persistence.receipts,
        receipt,
        {
            "state": "done",
            "indexed_at": time.time(),
            "existing": not saved,
            "error": "",
        },
    )
    if result.matched_count:
        await persistence.receipts.update_one(
            {"_id": receipt["_id"], "state": "done"},
            {
                "$unset": {"media": ""},
            },
        )


async def recover_source(client, persistence, allowed):
    source = await persistence.claim_source(allowed)
    if not source:
        return
    try:
        start = source["checkpoint"] + 1
        end = min(source["frontier"], start + 49)
        messages = await telegram_call(
            lambda: client.get_messages(source["_id"], list(range(start, end + 1))),
            route="live_gap_recovery",
            policy=INTERACTIVE_RETRY,
            retry_safe=False,
        )
        by_id = {message.id: message for message in messages}
        checkpoint = source["checkpoint"]
        blocked = False
        for message_id in range(start, end + 1):
            receipt_id = f"{source['_id']}:{message_id}"
            receipt = await persistence.receipts.find_one({"_id": receipt_id})
            if receipt is None:
                message = by_id.get(message_id)
                # Successful ID lookup can return MessageEmpty for deleted IDs.
                media = media_snapshot(message) if message and not getattr(message, "empty", False) else None
                uploaded = message.date.timestamp() if message and message.date else 0
                await persistence.receive(source["_id"], message_id, media, uploaded_at=uploaded, live=False)
                receipt = await persistence.receipts.find_one({"_id": receipt_id})
            if receipt["state"] not in {"done", "ignored"}:
                blocked = True
            if not blocked:
                checkpoint = message_id
        await persistence.sources.update_one(
            {"_id": source["_id"], "claim": source["claim"]},
            {
                "$max": {"checkpoint": checkpoint},
                "$set": {"lease_until": 0, "error": ""},
            },
        )
    except Exception as error:
        await persistence.sources.update_one(
            {"_id": source["_id"], "claim": source["claim"]},
            {
                "$set": {"lease_until": time.time() + 60, "error": type(error).__name__},
            },
        )
        raise


async def run_live_indexer(client):
    persistence = store()
    next_recovery = 0
    while True:
        receipt = None
        indexed = False
        try:
            config = await db.get_config()
            allowed = source_ids(config)
            receipt = await persistence.claim(
                persistence.receipts, ["pending"], extra={"source": {"$in": allowed}}
            )
            if receipt:
                async with asyncio.timeout(90):
                    await ingest_receipt(receipt, persistence)
                indexed = True
            if time.monotonic() >= next_recovery:
                next_recovery = time.monotonic() + 10
                async with asyncio.timeout(90):
                    await recover_source(client, persistence, allowed)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning("Live indexing deferred: %s", type(error).__name__)
            if receipt and not indexed:
                try:
                    await persistence.retry(persistence.receipts, receipt, error)
                except Exception as retry_error:
                    logger.warning(
                        "Receipt lease retained during database outage: %s", type(retry_error).__name__
                    )
        await asyncio.sleep(0.1 if receipt else 1)


async def resolve_candidate(candidate, persistence, destination):
    parsed = dict(candidate["parsed"])
    # Durable manual choices take precedence. Auto resolutions are not reused
    # for different filenames until those filenames pass the same strict check.
    chosen = await persistence.candidates.find_one(
        {
            "identity": candidate["identity"],
            "confirmed_id": {"$exists": True},
        }
    )
    if chosen:
        parsed.update(kind=chosen["parsed"]["kind"], season=chosen["parsed"]["season"])
        candidate = {**candidate, "parsed": parsed}
    if not parsed["title"] or (parsed["kind"] == "tv" and parsed["season"] is None):
        await persistence.finish(
            persistence.candidates, candidate, {"state": "review", "error": "TitleOrSeasonMissing"}
        )
        return
    metadata = await release_metadata(parsed, confirmed_id=(chosen or {}).get("confirmed_id"))
    if metadata is None:
        await persistence.finish(
            persistence.candidates, candidate, {"state": "review", "error": "AmbiguousMetadata"}
        )
        return
    await persistence.attach(candidate, metadata, destination)


def release_markup(client, key):
    return MobileInlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "View Available Files",
                    url=f"https://t.me/{client.me.username}?start=release_{key}",
                )
            ]
        ]
    )


async def publish_release(client, job, persistence):
    summary = await persistence.summary(job["_id"])
    if not summary["count"]:
        raise RuntimeError("Release has no indexed files")
    caption = render_release(job, summary)
    caption_hash = digest(caption)
    if job.get("message_id") and job.get("caption_hash") == caption_hash:
        await persistence.published(job, job["message_id"], job.get("photo", False), caption_hash)
        return
    markup = release_markup(client, job["_id"])
    if not job.get("message_id"):
        if not await persistence.begin_send(job):
            return
        try:
            options = {"chat_id": job["destination"], "reply_markup": markup, "parse_mode": ParseMode.HTML}
            if job["metadata"].get("poster"):
                operation = lambda: client.send_photo(
                    photo=job["metadata"]["poster"], caption=caption, **options
                )
            else:
                operation = lambda: client.send_message(text=caption, **options)
            sent = await telegram_call(
                operation, route="release_publish", policy=INTERACTIVE_RETRY, retry_safe=False
            )
            await persistence.published(job, sent.id, bool(job["metadata"].get("poster")), caption_hash)
        except FloodWait as error:
            await persistence.finish(
                persistence.posts, job, {"state": "pending", "due": time.time() + error.value + 2}
            )
        except (BadRequest, Forbidden) as error:
            # Explicit RPC rejection means nothing was sent. Keep visible for
            # repair; URL/permission errors must not loop in the background.
            if job["metadata"].get("poster") and getattr(error, "ID", "") in {
                "WEBPAGE_CURL_FAILED",
                "WEBPAGE_MEDIA_EMPTY",
                "PHOTO_INVALID_DIMENSIONS",
                "PHOTO_EXT_INVALID",
                "EXTERNAL_URL_INVALID",
                "MEDIA_EMPTY",
                "MEDIA_INVALID",
            }:
                await persistence.finish(
                    persistence.posts,
                    job,
                    {
                        "state": "pending",
                        "metadata.poster": None,
                        "due": time.time() + 5,
                        "error": "PosterUnavailableUsingText",
                    },
                )
            else:
                await persistence.finish(
                    persistence.posts, job, {"state": "blocked", "error": type(error).__name__}
                )
        except BaseException:
            # Includes cancellation and database failure after a successful
            # send. No blind resend: expired sending claims become uncertain.
            raise
        return
    try:
        options = {
            "chat_id": job["destination"],
            "message_id": job["message_id"],
            "reply_markup": markup,
            "parse_mode": ParseMode.HTML,
        }
        if job.get("photo"):
            operation = lambda: client.edit_message_caption(caption=caption, **options)
        else:
            operation = lambda: client.edit_message_text(text=caption, **options)
        await telegram_call(operation, route="release_edit", policy=INTERACTIVE_RETRY, retry_safe=False)
    except MessageNotModified:
        logger.debug("Release caption was already current")
    except (BadRequest, Forbidden) as error:
        await persistence.finish(persistence.posts, job, {"state": "blocked", "error": type(error).__name__})
        return
    await persistence.published(job, job["message_id"], job.get("photo", False), caption_hash)


async def run_release_worker(client):
    persistence = store()
    while True:
        job = None
        collection = None
        try:
            config = await db.get_config()
            destination = int(config.get("update_channel_id") or 0)
            await persistence.recover_uncertain()
            if destination:
                collection = persistence.candidates
                job = await persistence.claim(collection, ["pending"])
                if job:
                    async with asyncio.timeout(60):
                        await resolve_candidate(job, persistence, destination)
                job = None
                if config.get("release_posts_enabled", False):
                    collection = persistence.posts
                    job = await persistence.claim(
                        collection, ["pending", "posted"], extra={"destination": destination}
                    )
                    if job:
                        async with asyncio.timeout(60):
                            await publish_release(client, job, persistence)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning("Release work deferred: %s", type(error).__name__)
            if job:
                # retry() preserves state; 'sending' cannot be reclaimed for
                # another send and will be marked uncertain on the next pass.
                delay = error.value + 2 if isinstance(error, FloodWait) else None
                try:
                    await persistence.retry(collection, job, error, delay)
                except Exception as retry_error:
                    logger.warning(
                        "Release lease retained during database outage: %s", type(retry_error).__name__
                    )
        await asyncio.sleep(1)


async def release_file_page(key, after="", limit=40):
    """Exact membership, not fuzzy title search; keyset for large seasons."""
    persistence = store()
    query = {"release": key}
    if after:
        query["order"] = {"$gt": after}
    members = [row async for row in persistence.members.find(query).sort("order", 1).limit(limit + 1)]
    more = len(members) > limit
    page = members[:limit]
    by_shard = {}
    for row in page:
        by_shard.setdefault(row["shard"], []).append(ObjectId(row["movie_id"]))
    results = []
    for shard, ids in by_shard.items():
        if 0 <= shard < len(db.file_cols):
            async with asyncio.timeout(5):
                results.extend([row async for row in db.file_cols[shard].find({"_id": {"$in": ids}})])
    order = {row["movie_id"]: index for index, row in enumerate(page)}
    results.sort(key=lambda row: order[str(row["_id"])])
    return results, page[-1]["order"] if more else None
