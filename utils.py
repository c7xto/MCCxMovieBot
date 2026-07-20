import os
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

# Single shared source of truth for admin IDs — parsed as a comma-separated
# list so multi-admin setups (ADMIN_ID=123,456) work everywhere, not just
# in broadcast.py. Import this everywhere instead of re-parsing os.getenv.
ADMIN_ID = [int(x.strip()) for x in os.getenv("ADMIN_ID", "0").split(",") if x.strip()]


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
    # Get file count for unlock framing
    try:
        total_files = await db.get_total_files()
        files_str = f"{total_files:,}"
    except Exception:
        files_str = "millions of"

    text = (
        f"🔐 <b>One step away!</b>\n\n"
        f"Join our channel to unlock {files_str} files — free forever.\n\n"
        f"<blockquote>Tap join, then tap <b>✅ Done — Let Me In</b></blockquote>"
    )
    await message.reply_text(
        text, reply_markup=markup, quote=True, parse_mode=ParseMode.HTML
    )
