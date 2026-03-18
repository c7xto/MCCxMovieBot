import os
import re
import time
import random
import asyncio
import logging
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode
from database.db import db
from tmdb import get_movie_data
from pyrogram.errors import FloodWait

# load_dotenv() needed so DATABASE_CHANNEL_ID env fallback works correctly
load_dotenv()

logger = logging.getLogger(__name__)


def _html(text: str) -> str:
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

# Cap to prevent unbounded RAM growth
RECENT_POSTS = {}
_RECENT_POSTS_MAX = 1000
POST_COOLDOWN = 300

# ── POST QUEUE ────────────────────────────────────────────────────────────────
# A global asyncio queue so rapid bulk-indexing of many files doesn't fire
# 50 Telegram messages at once and trigger a FloodWait on the update channel.
# Posts drain at one every 3 seconds regardless of how many arrive.
_post_queue: asyncio.Queue = asyncio.Queue()
_queue_worker_started = False


async def _post_queue_worker(client: Client):
    """Drains the post queue at a safe rate of 1 post per 3 seconds."""
    while True:
        try:
            filename = await _post_queue.get()
            await _do_post(client, filename)
            _post_queue.task_done()
        except Exception as e:
            logger.error(f"Post queue worker error: {e}")
        await asyncio.sleep(3)


async def _ensure_queue_worker(client: Client):
    """Starts the queue worker once on first use."""
    global _queue_worker_started
    if not _queue_worker_started:
        _queue_worker_started = True
        asyncio.create_task(_post_queue_worker(client))


# ── FILE INFO PARSER ──────────────────────────────────────────────────────────

def parse_file_info(filename):
    filename = re.sub(r'\.(mkv|mp4|avi|mov|zip)$', '', filename, flags=re.IGNORECASE)

    year_match = re.search(r'\b(19\d{2}|20\d{2})\b', filename)
    year = year_match.group(1) if year_match else None

    qualities = ["4K", "1080p", "720p", "480p", "HDRip", "WEB-DL", "WEBRip",
                 "BluRay", "PreDVD", "CAM", "HD Rip"]
    quality = next(
        (q for q in qualities if re.search(
            r'\b' + q.replace(' ', r'\s*') + r'\b', filename, re.IGNORECASE
        )), None
    )
    if quality and quality.lower() == "hdrip":
        quality = "HD Rip"

    languages = ["Malayalam", "Tamil", "Telugu", "Hindi", "English",
                 "Kannada", "Dual Audio", "Multi Audio"]
    language = next(
        (l for l in languages if re.search(r'\b' + l + r'\b', filename, re.IGNORECASE)),
        None
    )

    is_series = bool(re.search(
        r'(S\d+|Season \d+|E\d+|Episode \d+)', filename, re.IGNORECASE
    ))

    clean_title = filename
    if year:
        clean_title = clean_title.split(year)[0]

    clean_title = re.sub(r'[._\-]', ' ', clean_title)

    junk = ['sample\\s*of', 'www', '1tamilmv', 'tamilblasters', 'moviezwap',
            'tamilyogi', 'life', 'hq', 'esub', 'combined', '10bit', 'org']
    for j in junk:
        clean_title = re.sub(fr'\b{j}\b', '', clean_title, flags=re.IGNORECASE)

    match = re.search(
        r'\b(19\d{2}|20\d{2}|S\d{1,2}|Season \d+|1080p|720p|480p|4k|2160p)\b',
        clean_title, re.IGNORECASE
    )
    if match:
        clean_title = clean_title[:match.start()]

    clean_title = re.sub(r'\[.*?\]|\(.*?\)', '', clean_title)
    clean_title = re.sub(r'\s+', ' ', clean_title).strip()
    clean_title = re.sub(r'^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$', '', clean_title)

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
    last_posted = RECENT_POSTS.get(clean_title.lower(), 0)
    if current_time - last_posted < POST_COOLDOWN:
        return

    if len(RECENT_POSTS) >= _RECENT_POSTS_MAX:
        RECENT_POSTS.clear()
    RECENT_POSTS[clean_title.lower()] = current_time

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

    safe_title = re.sub(r'[^a-zA-Z0-9\s\-]', ' ', display_title)
    safe_query = re.sub(r'\s+', '_', safe_title.strip())[:45]
    bot_url = f"https://t.me/{client.me.username}?start=search_{safe_query}"

    btn_text = "📥 Get Series" if is_series else "📥 Get Movie"

    buttons = [[InlineKeyboardButton(btn_text, url=bot_url)]]
    if main_group:
        buttons[0].append(InlineKeyboardButton("👥 Join Group", url=main_group))

    markup = InlineKeyboardMarkup(buttons)

    try:
        if tmdb_data and tmdb_data.get("poster"):
            await client.send_photo(
                chat_id=update_channel,
                photo=tmdb_data["poster"],
                caption=caption,
                reply_markup=markup,
                parse_mode=ParseMode.HTML
            )
        else:
            await client.send_message(
                chat_id=update_channel,
                text=caption,
                reply_markup=markup,
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        logger.error(f"Failed to auto-post '{clean_title}': {e}")


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
            await client.send_message(
                log_channel,
                f"✅ **Successfully Indexed**\n\n"
                f"🎬 **File:** `{media.file_name}`\n"
                f"💿 **Size:** `{media.file_size / (1024 * 1024):.2f} MB`"
            )
        except Exception:
            pass

    if success:
        # Ensure queue worker is running
        await _ensure_queue_worker(client)
        # Fire-and-forget: delay then enqueue so handler returns immediately
        async def _delayed_enqueue(fname):
            await asyncio.sleep(random.uniform(1.0, 3.0))
            await _post_queue.put(fname)
        asyncio.create_task(_delayed_enqueue(media.file_name))

        # Auto request fulfillment — check if any pending requests match this file
        # Runs in background so it never delays indexing
        asyncio.create_task(_fulfill_matching_requests(client, media.file_name))


async def _fulfill_matching_requests(client, file_name: str):
    """
    After a new file is indexed, check pending movie requests for matches.
    If found, notify the user automatically and remove the fulfilled request.
    This runs entirely in the background — any failure is silent and safe.
    """
    try:
        matches = await db.find_matching_requests(file_name)
        if not matches:
            return

        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', file_name)[:45]

        for match in matches:
            user_id = match["user_id"]
            movie_name = match["movie_name"]
            try:
                notify_text = (
                    f"🎉 **Great News!**\n\n"
                    f"The movie you requested — **{movie_name}** — "
                    f"has just been uploaded to our database!\n\n"
                    f"👇 Tap below to fetch it instantly."
                )
                markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton(
                        "🔍 Get It Now",
                        url=f"https://t.me/{client.me.username}?start=search_{safe_name}"
                    )]
                ])
                await client.send_message(
                    chat_id=user_id,
                    text=notify_text,
                    reply_markup=markup
                )
                await db.delete_pending_request(user_id, movie_name)
                logger.info(f"Auto-fulfilled request for user {user_id}: {movie_name}")
            except Exception as e:
                logger.warning(f"Could not notify user {user_id} for request '{movie_name}': {e}")
    except Exception as e:
        logger.warning(f"Request fulfillment check failed for '{file_name}': {e}")
