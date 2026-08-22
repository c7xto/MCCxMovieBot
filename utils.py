import os
import time
import asyncio
import logging
from dotenv import load_dotenv
from pyrogram.errors import UserNotParticipant
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
try:
    from pyrogram.types import LinkPreviewOptions
    def _no_preview(): return {"link_preview_options": LinkPreviewOptions(is_disabled=True)}
except ImportError:
    LinkPreviewOptions = None
    def _no_preview(): return {"disable_web_page_preview": True}
from pyrogram.enums import ParseMode
from database.db import db

load_dotenv()

logger = logging.getLogger(__name__)


def _html(text) -> str:
    """Escapes a string for safe use inside Telegram HTML-mode messages.
    Single shared definition — every plugin that builds HTML captions
    imports this instead of redefining it locally."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def callback_data(prefix: str, value, max_bytes: int = 64) -> str:
    """Build callback data without splitting UTF-8 or exceeding Telegram's limit."""
    prefix_bytes = prefix.encode("utf-8")
    if len(prefix_bytes) >= max_bytes:
        raise ValueError("Callback prefix leaves no room for a value")
    value_bytes = str(value).encode("utf-8")[:max_bytes - len(prefix_bytes)]
    return prefix + value_bytes.decode("utf-8", errors="ignore")

# Single shared source of truth for admin IDs — parsed as a comma-separated
# list so multi-admin setups (ADMIN_ID=123,456) work everywhere, not just
# in broadcast.py. Import this everywhere instead of re-parsing os.getenv.
ADMIN_ID = [int(x.strip()) for x in os.getenv("ADMIN_ID", "0").split(",") if x.strip()]

# Canonical help copy — single source both /help (admin.py) and the "ℹ️ Help"
# button (start.py) build from, so the two surfaces can't silently diverge
# again. English only — start.py's Malayalam translation is maintained
# separately since it's hand-translated, not derived from this.
HELP_STEPS_EN = [
    "Type a movie or series name",
    "Tap the file you want",
    "It's sent straight to your PM",
]
HELP_FOOTER_EN = "Can't find it? Use the <b>Request</b> button and we'll upload it within 24h."

# TTL-cached wrapper around db.get_total_files() — send_fsub_message() calls
# this on every message from a not-yet-subscribed user, and a live count is a
# full fan-out across every configured cluster just to render cosmetic
# "unlock X files" copy.
_total_files_cache = None
_total_files_cache_ts = 0.0
_TOTAL_FILES_TTL = 60  # seconds
# Guards the refresh path so a cold/expired cache under concurrent traffic
# triggers exactly one db.get_total_files() call — every other caller
# waiting on the lock sees the now-fresh cache on re-check instead of firing
# its own redundant cross-cluster fan-out.
_total_files_lock = asyncio.Lock()


async def _get_total_files_cached() -> int:
    global _total_files_cache, _total_files_cache_ts
    now = time.time()
    if _total_files_cache is not None and (now - _total_files_cache_ts) < _TOTAL_FILES_TTL:
        return _total_files_cache

    async with _total_files_lock:
        # Re-check — another coroutine may have already refreshed the cache
        # while this one was waiting for the lock.
        now = time.time()
        if _total_files_cache is not None and (now - _total_files_cache_ts) < _TOTAL_FILES_TTL:
            return _total_files_cache

        total = await db.get_total_files()
        # db.get_total_files() swallows per-cluster failures into a 0
        # contribution instead of raising, so a 0 here is ambiguous between
        # "genuinely empty library" and "every cluster just failed". A drop
        # from a previously-known-good non-zero count to 0 within one
        # refresh cycle is a failure artifact, not real data (a library
        # doesn't lose everything in under a minute) — so a 0 is only
        # trusted (and cached) when there's no prior good value to protect:
        # the very first fetch, or the cache already held 0. Otherwise the
        # stale-but-plausible cached value is returned and the timestamp is
        # left untouched, so the next call retries instead of serving a
        # wrong number for the rest of the TTL window.
        if total > 0 or not _total_files_cache:
            _total_files_cache = total
            _total_files_cache_ts = now
            return total
        return _total_files_cache


def _parse_fsub_entry(entry):
    """Normalises a stored FSub entry to (channel_id). All channels are join type."""
    if isinstance(entry, dict):
        return entry.get("id"), "join"  # always join — request type removed
    try:
        return int(entry), "join"
    except (ValueError, TypeError):
        return str(entry), "join"


async def is_subscribed_by_id(client, user_id: int) -> bool:
    """Core Main-FSub membership check against every configured channel,
    by user_id directly — the reusable half of is_subscribed() below, and
    also used by req_fsub.py's unified verification-gates checker so both
    entry points share one exact set of pass/fail semantics. Deny only on
    KICKED/BANNED/LEFT/UserNotParticipant; fail-open on any other error
    (documented tradeoff — see BOT_BLUEPRINT.md's fail-open FSub note)."""
    config = await db.get_config()
    fsub_channels = config.get("fsub_channels", [])
    if not fsub_channels:
        return True

    for entry in fsub_channels:
        channel_id, _ = _parse_fsub_entry(entry)
        if not channel_id:
            continue
        try:
            ch = int(channel_id) if str(channel_id).lstrip('-').isdigit() else str(channel_id)
            member = await client.get_chat_member(ch, user_id)
            if member.status.name in ["KICKED", "BANNED", "LEFT"]:
                return False
        except UserNotParticipant:
            return False
        except Exception as e:
            logger.warning(f"FSub check error on channel {channel_id}: {e}")
            continue

    return True


async def is_subscribed(client, message_or_callback):
    """Checks if the user has joined all FSub channels."""
    if not message_or_callback.from_user:
        return True  # anonymous admin — let through
    return await is_subscribed_by_id(client, message_or_callback.from_user.id)


# Keep is_subscribed_join_only as alias for backward compat with filter.py and start.py
is_subscribed_join_only = is_subscribed


async def send_fsub_message(client, message, pending_file_id=None):
    """
    Sends the FSub join prompt.
    Join channels:
      - Public @username → https://t.me/username (never expires)
      - Private -100xxx  → stored invite link (generated once, reused)
    """
    config = await db.get_config()
    fsub_channels = config.get("fsub_channels", [])

    buttons = []

    for i, entry in enumerate(fsub_channels, 1):
        channel_id, _ = _parse_fsub_entry(entry)
        if not channel_id:
            continue

        try:
            ch_str = str(channel_id).strip()
            stored_link = entry.get("link") if isinstance(entry, dict) else None

            if ch_str.startswith("@"):
                # Public channel — username link never expires
                link = f"https://t.me/{ch_str[1:]}"
            elif stored_link and not stored_link.startswith("tg://"):
                # Stored https:// invite link — use directly
                link = stored_link
            elif ch_str.startswith("-100"):
                # Private channel — generate once and save
                try:
                    generated = await client.export_chat_invite_link(int(ch_str))
                    link = generated
                    await db.update_fsub_channel_link(channel_id, generated)
                except Exception as gen_err:
                    logger.warning(f"Could not generate invite link for {channel_id}: {gen_err}")
                    continue
            elif ch_str.startswith("http"):
                link = ch_str
            else:
                link = f"https://t.me/{ch_str}"

            buttons.append([InlineKeyboardButton(f"📢 Join Channel {i}", url=link)])

        except Exception as e:
            logger.warning(f"Could not build FSub button for {channel_id}: {e}")
            continue

    if not buttons:
        logger.warning("send_fsub_message: no buttons built")
        return

    file_part = pending_file_id if pending_file_id else "none"
    buttons.append([
        InlineKeyboardButton("✅ Done — Let Me In", callback_data=f"check_fsub#{file_part}")
    ])
    markup = InlineKeyboardMarkup(buttons)

    mention = message.from_user.mention if message.from_user else "there"
    # Get file count for unlock framing — cached, see _get_total_files_cached above
    try:
        total_files = await _get_total_files_cached()
        files_str = f"{total_files:,}"
    except Exception:
        files_str = "millions of"

    text = (
        f"🔐 <b>One step away!</b>\n\n"
        f"Join our channel to unlock {files_str} files, free forever.\n\n"
        f"<blockquote>Tap join, then tap <b>✅ Done — Let Me In</b></blockquote>"
    )
    await message.reply_text(
        text, reply_markup=markup, quote=True, parse_mode=ParseMode.HTML
    )
