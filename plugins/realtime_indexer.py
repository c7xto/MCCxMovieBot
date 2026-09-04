import re
import random
import asyncio
import logging
from pyrogram import Client
from pyrogram.types import InlineKeyboardButton
from plugins.mobile_ui import MobileInlineKeyboardMarkup as InlineKeyboardMarkup
from pyrogram.enums import ParseMode
from database.db import db
from pyrogram.errors import FloodWait, InputUserDeactivated, UserIsBlocked
from plugins.telegram_retry import BACKGROUND_RETRY, telegram_call
from utils import _html

logger = logging.getLogger(__name__)

# ── DURABLE POST QUEUE ────────────────────────────────────────────────────────
async def run_notification_worker(client: Client):
    """Existing opt-in request notifications; public posts use live_library."""
    while True:
        job = None
        try:
            job = await db.claim_due_notification(kind="request_fulfillment")
            if not job:
                await asyncio.sleep(3)
                continue
            payload = job.get("payload", {})
            file_name = payload.get("file_name") or job.get("file_name", "")
            await _fulfill_matching_requests(client, file_name)
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


# Source arrivals are captured durably by the readiness gate. The dedicated
# live_library worker performs indexing and smart release aggregation.



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
