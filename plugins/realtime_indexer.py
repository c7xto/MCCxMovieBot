import os
import re
import time
import random
import asyncio
import logging
from collections import OrderedDict
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode
from database.db import db
from tmdb import get_movie_data
from pyrogram.errors import FloodWait, InputUserDeactivated, UserIsBlocked
from plugins.filter import send_smart_log
from plugins.telegram_retry import BACKGROUND_RETRY, telegram_call
from plugins.task_supervisor import TaskConflict, supervisor
from utils import _html, report_internal_error

# load_dotenv() needed so DATABASE_CHANNEL_ID env fallback works correctly
load_dotenv()

logger = logging.getLogger(__name__)

# Cap to prevent unbounded RAM growth — LRU: oldest entry is first
RECENT_POSTS = OrderedDict()
_RECENT_POSTS_MAX = 1000
POST_COOLDOWN = 300


# ── DURABLE POST QUEUE ────────────────────────────────────────────────────────
async def run_notification_worker(client: Client):
    """Claims durable announcements at a safe rate of 1 post per 3 seconds."""
    while True:
        job = None
        try:
            job = await db.claim_due_notification()
            if not job:
                await asyncio.sleep(3)
                continue
            payload = job.get("payload", {})
            file_name = payload.get("file_name") or job.get("file_name", "")
            if job.get("kind", "announcement") == "request_fulfillment":
                await _fulfill_matching_requests(client, file_name)
            else:
                await _do_post(client, file_name)
            await db.complete_announcement(job["_id"], job.get("revision"))
        except asyncio.CancelledError:
            raise
        except FloodWait as exc:
            if job:
                await db.retry_announcement(
                    job["_id"],
                    max(5, exc.value) + random.uniform(0.0, 2.0),
                    job.get("revision"),
                )
        except Exception as e:
            logger.error("Post queue worker error: %s", e)
            if job:
                delay = min(3600, 30 * (2 ** min(job.get("attempts", 1), 7)))
                await db.retry_announcement(job["_id"], delay, job.get("revision"))
        await asyncio.sleep(3)


async def _ensure_queue_worker(client: Client):
    """Starts the queue worker once on first use."""
    try:
        supervisor.spawn(
            run_notification_worker(client),
            key="worker:announcement-outbox",
            owner="realtime_indexer",
        )
    except TaskConflict:
        pass


# ── FILE INFO PARSER ──────────────────────────────────────────────────────────


def parse_file_info(filename):
    filename = re.sub(r"\.(mkv|mp4|avi|mov|zip)$", "", filename, flags=re.IGNORECASE)

    year_match = re.search(r"\b(19\d{2}|20\d{2})\b", filename)
    year = year_match.group(1) if year_match else None

    qualities = [
        "4K",
        "1080p",
        "720p",
        "480p",
        "HDRip",
        "WEB-DL",
        "WEBRip",
        "BluRay",
        "PreDVD",
        "CAM",
        "HD Rip",
    ]
    quality = next(
        (q for q in qualities if re.search(r"\b" + q.replace(" ", r"\s*") + r"\b", filename, re.IGNORECASE)),
        None,
    )
    if quality and quality.lower() == "hdrip":
        quality = "HD Rip"

    languages = ["Malayalam", "Tamil", "Telugu", "Hindi", "English", "Kannada", "Dual Audio", "Multi Audio"]
    language = next((l for l in languages if re.search(r"\b" + l + r"\b", filename, re.IGNORECASE)), None)

    is_series = bool(re.search(r"(S\d+|Season \d+|E\d+|Episode \d+)", filename, re.IGNORECASE))

    clean_title = filename
    if year:
        clean_title = clean_title.split(year)[0]

    clean_title = re.sub(r"[._\-]", " ", clean_title)

    junk = [
        "sample\\s*of",
        "www",
        "1tamilmv",
        "tamilblasters",
        "moviezwap",
        "tamilyogi",
        "life",
        "hq",
        "esub",
        "combined",
        "10bit",
        "org",
    ]
    for j in junk:
        clean_title = re.sub(rf"\b{j}\b", "", clean_title, flags=re.IGNORECASE)

    match = re.search(
        r"\b(19\d{2}|20\d{2}|S\d{1,2}|Season \d+|1080p|720p|480p|4k|2160p)\b", clean_title, re.IGNORECASE
    )
    if match:
        clean_title = clean_title[: match.start()]

    clean_title = re.sub(r"\[.*?\]|\(.*?\)", "", clean_title)
    clean_title = re.sub(r"\s+", " ", clean_title).strip()
    clean_title = re.sub(r"^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$", "", clean_title)

    return clean_title, year, quality, language, is_series


# ── ACTUAL POST LOGIC ─────────────────────────────────────────────────────────


async def _do_post(client: Client, filename: str):
    """
    Performs the actual Telegram post. Always reads update_channel and
    main_group from MongoDB (via db.get_config()) so admin panel changes
    take effect immediately without a restart.
    """
    config = await db.get_config()
    update_channel = config.get("update_channel_id", 0)
    main_group = config.get("main_group", "")

    if not update_channel:
        return

    clean_title, year, quality, language, is_series = parse_file_info(filename)
    if not clean_title or len(clean_title) < 2:
        return

    current_time = time.time()
    title_key = clean_title.lower()
    last_posted = RECENT_POSTS.get(title_key, 0)
    if current_time - last_posted < POST_COOLDOWN:
        return

    tmdb_data = await get_movie_data(clean_title)
    display_title = tmdb_data["title"] if tmdb_data else clean_title.title()

    metadata = []
    if year:
        metadata.append(f"<code>{year}</code>")
    if language:
        metadata.append(f"#{language.replace(' ', '')}")
    if quality:
        metadata.append(quality)
    meta_string = "  •  ".join(metadata)

    caption = (
        f"🎬 <b>{_html(display_title)}</b>\n"
        f"{meta_string}\n\n"
        f"<blockquote>Tap the button below to get this file instantly.</blockquote>"
    )

    safe_title = re.sub(r"[^a-zA-Z0-9\s\-]", " ", display_title)
    safe_query = re.sub(r"\s+", "_", safe_title.strip())[:45]
    bot_url = f"https://t.me/{client.me.username}?start=search_{safe_query}"

    btn_text = "📥 Get Series" if is_series else "📥 Get Movie"

    buttons = [[InlineKeyboardButton(btn_text, url=bot_url)]]
    if main_group:
        buttons[0].append(InlineKeyboardButton("💬 Request Group", url=main_group))

    markup = InlineKeyboardMarkup(buttons)

    if tmdb_data and tmdb_data.get("poster"):
        await telegram_call(
            lambda: client.send_photo(
                chat_id=update_channel,
                photo=tmdb_data["poster"],
                caption=caption,
                reply_markup=markup,
                parse_mode=ParseMode.HTML,
            ),
            route="update_channel_photo",
            policy=BACKGROUND_RETRY,
            retry_safe=True,
            idempotency_key=f"announcement:{title_key}",
        )
    else:
        await telegram_call(
            lambda: client.send_message(
                chat_id=update_channel,
                text=caption,
                reply_markup=markup,
                parse_mode=ParseMode.HTML,
            ),
            route="update_channel_text",
            policy=BACKGROUND_RETRY,
            retry_safe=True,
            idempotency_key=f"announcement:{title_key}",
        )

    if title_key in RECENT_POSTS:
        RECENT_POSTS.move_to_end(title_key)
    elif len(RECENT_POSTS) >= _RECENT_POSTS_MAX:
        RECENT_POSTS.popitem(last=False)
    RECENT_POSTS[title_key] = current_time


# ── NEW FILE HANDLER ──────────────────────────────────────────────────────────


@Client.on_message(filters.channel & (filters.document | filters.video | filters.audio))
async def index_new_files(client: Client, message: Message):
    # Always read db_channels from MongoDB so admin panel additions work instantly
    config = await db.get_config()
    db_channels = list(config.get("db_channels", []))

    # Also honour the .env DATABASE_CHANNEL_ID as a permanent fallback
    env_db = int(os.getenv("DATABASE_CHANNEL_ID", 0) or 0)
    if env_db and env_db not in db_channels:
        db_channels.append(env_db)

    if message.chat.id not in db_channels:
        return

    media = message.document or message.video or message.audio
    if not media or not hasattr(media, "file_name") or not media.file_name:
        return

    success, return_msg = await db.save_file(media)

    # Log to the channel stored in MongoDB — stays in sync with admin panel changes
    log_channel = config.get("log_channel", 0)
    if log_channel and success:
        try:
            await telegram_call(
                lambda: client.send_message(
                    log_channel,
                    f"✅ **Successfully Indexed**\n\n"
                    f"🎬 **File:** `{media.file_name}`\n"
                    f"💿 **Size:** `{media.file_size / (1024 * 1024):.2f} MB`",
                ),
                route="realtime_index_log",
                policy=BACKGROUND_RETRY,
                retry_safe=True,
                idempotency_key=f"index-log:{media.file_id}",
            )
        except Exception:
            pass
    elif return_msg == "All clusters full":
        # Real-time ingestion has no other failure path — without this alert
        # every new upload silently vanishes from the index the moment all
        # clusters hit their 450MB safety margin.
        try:
            supervisor.spawn(
                send_smart_log(
                    client,
                    f"🛑 **#DatabaseFull**\n\n"
                    f"Real-time indexing failed — every configured cluster is at "
                    f"its 450MB safety margin.\n"
                    f"🎬 **File:** `{media.file_name}`\n"
                    f"**Fix:** Add a new `DATABASE_URI` cluster and restart the bot.\n\n"
                    f"⚠️ This file was **not indexed** and will not appear in search.",
                ),
                key=f"log:realtime-full:{media.file_id}",
                owner="realtime_indexer",
                drain_on_shutdown=True,
            )
        except TaskConflict:
            logger.info("Realtime capacity log skipped during shutdown")

    if success:
        await _ensure_queue_worker(client)
        await db.enqueue_announcement(media.file_name, delay_seconds=random.uniform(1.0, 3.0))
        await db.enqueue_request_fulfillment(media.file_name, delay_seconds=random.uniform(1.0, 3.0))


async def _fulfill_matching_requests(client, file_name: str):
    """
    After a new file is indexed, check pending movie requests for matches.
    If found, notify the user automatically and remove the fulfilled request.
    This runs entirely in the background — any failure is silent and safe.
    """
    try:
        safe_name = re.sub(r"[^a-zA-Z0-9]", "_", file_name)[:45]
        matched = False
        transient_failure = None
        async for match in db.iter_matching_requests(file_name):
            matched = True
            user_id = match["user_id"]
            movie_name = match["movie_name"]
            try:
                notify_text = (
                    "🎉 <b>Great News!</b>\n\n"
                    f"The movie you requested — <b>{_html(movie_name)}</b> — "
                    "has just been uploaded to our database!\n\n"
                    "👇 Tap below to fetch it instantly."
                )
                markup = InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "🔍 Get It Now",
                                url=f"https://t.me/{client.me.username}?start=search_{safe_name}",
                            )
                        ]
                    ]
                )
                await telegram_call(
                    lambda: client.send_message(
                        chat_id=user_id,
                        text=notify_text,
                        reply_markup=markup,
                        parse_mode=ParseMode.HTML,
                    ),
                    route="request_auto_fulfillment",
                    policy=BACKGROUND_RETRY,
                    retry_safe=True,
                    idempotency_key=f"request:{user_id}:{movie_name.casefold()}",
                )
                await db.delete_pending_request(user_id, movie_name)
                logger.info("Auto-fulfilled request user_id=%s movie=%r", user_id, movie_name)
            except (InputUserDeactivated, UserIsBlocked):
                await db.delete_user(user_id)
                await db.delete_pending_request(user_id, movie_name)
                logger.info(f"User {user_id} blocked/deactivated — cleaned up")
            except FloodWait:
                raise
            except Exception as e:
                logger.warning(f"Could not notify user {user_id} for request '{movie_name}': {e}")
                transient_failure = e
        if not matched:
            return
        if transient_failure is not None:
            raise RuntimeError("One or more request notifications failed") from transient_failure
    except FloodWait:
        raise
    except Exception as e:
        logger.warning(f"Request fulfillment check failed for '{file_name}': {e}")
        raise
