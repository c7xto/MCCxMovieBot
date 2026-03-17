import os
import logging
from dotenv import load_dotenv
from pyrogram.errors import UserNotParticipant
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, LinkPreviewOptions
from pyrogram.enums import ParseMode
from database.db import db

load_dotenv()

logger = logging.getLogger(__name__)


def _parse_fsub_entry(entry):
    """
    Normalises a stored FSub entry to (channel_id, channel_type).
    Old format:  -1003551418912 or "@MCCxRequest"  → (id, "join")
    New format:  {"id": -100x, "type": "request", "link": "..."}
    """
    if isinstance(entry, dict):
        return entry.get("id"), entry.get("type", "join")
    try:
        return int(entry), "join"
    except (ValueError, TypeError):
        return str(entry), "join"


async def _check_single_channel(client, user_id, channel_id, channel_type):
    """
    Returns True if user passes this channel's requirement, False if not.
    Join    — must be MEMBER or ADMINISTRATOR
    Request — MEMBER, ADMINISTRATOR, or RESTRICTED (= request pending = passes)
    """
    try:
        ch = int(channel_id) if str(channel_id).lstrip('-').isdigit() else str(channel_id)
        member = await client.get_chat_member(ch, user_id)
        status = member.status.name
        if channel_type == "request":
            return status not in ["KICKED", "BANNED", "LEFT"]
        else:
            return status not in ["KICKED", "BANNED", "LEFT"]
    except UserNotParticipant:
        return False
    except Exception as e:
        logger.warning(f"FSub check error on channel {channel_id}: {e}")
        return True  # fail open on errors so users aren't locked out by API issues


async def is_subscribed(client, message_or_callback):
    """
    Full FSub check — ALL channels (join + request) must pass.
    Used for PM search access.
    """
    config = await db.get_config()
    fsub_channels = config.get("fsub_channels", [])
    if not fsub_channels:
        return True

    user_id = message_or_callback.from_user.id
    for entry in fsub_channels:
        channel_id, channel_type = _parse_fsub_entry(entry)
        if not channel_id:
            continue
        if not await _check_single_channel(client, user_id, channel_id, channel_type):
            return False
    return True


async def is_subscribed_join_only(client, message_or_callback):
    """
    Checks ONLY join-type channels (not request channels).
    Used as the first gate — user must join public/private channels
    before they can even search. Request channel is checked separately
    at file delivery time.
    """
    config = await db.get_config()
    fsub_channels = config.get("fsub_channels", [])
    if not fsub_channels:
        return True

    user_id = message_or_callback.from_user.id
    for entry in fsub_channels:
        channel_id, channel_type = _parse_fsub_entry(entry)
        if not channel_id:
            continue
        if channel_type == "request":
            continue  # skip request channels at this stage
        if not await _check_single_channel(client, user_id, channel_id, channel_type):
            return False
    return True


async def is_subscribed_request_only(client, message_or_callback):
    """
    Checks ONLY request-type channels.
    Used as the second gate just before file delivery.
    """
    config = await db.get_config()
    fsub_channels = config.get("fsub_channels", [])
    if not fsub_channels:
        return True

    user_id = message_or_callback.from_user.id
    has_request_channels = False

    for entry in fsub_channels:
        channel_id, channel_type = _parse_fsub_entry(entry)
        if not channel_id or channel_type != "request":
            continue
        has_request_channels = True
        if not await _check_single_channel(client, user_id, channel_id, channel_type):
            return False

    return True  # passes if no request channels configured


async def send_fsub_message(client, message, pending_file_id=None, request_only=False):
    """
    Sends the FSub prompt.
    request_only=True  → shows only request-type channel buttons (pre-file gate)
    request_only=False → shows all channel buttons (full gate)

    Join channels:
      - Public (@username) → direct t.me link, never expires
      - Private (-100xxx)  → stored invite link (generated once, reused)

    Request channels → stored https://t.me/+xxxx link (set by admin)
    """
    config = await db.get_config()
    fsub_channels = config.get("fsub_channels", [])

    buttons = []

    for i, entry in enumerate(fsub_channels, 1):
        channel_id, channel_type = _parse_fsub_entry(entry)
        if not channel_id:
            continue

        # Filter based on mode
        if request_only and channel_type != "request":
            continue
        if not request_only and channel_type == "request":
            # In full mode, show request channels too but after join channels
            pass

        try:
            ch_str = str(channel_id).strip()
            stored_link = entry.get("link") if isinstance(entry, dict) else None

            if ch_str.startswith("@"):
                # Public @username channel — ALWAYS use direct link, never a stored
                # invite link. The refresh function may have stored a private invite
                # link here which can expire. Username links never expire.
                link = f"https://t.me/{ch_str[1:]}"

            elif stored_link:
                # Private channel with a stored invite link — use it directly
                link = stored_link

            elif ch_str.startswith("-100"):
                if channel_type == "request":
                    # Request channel with no stored link — cannot generate safely
                    logger.warning(
                        f"Request FSub channel {channel_id} has no stored link. "
                        f"Remove and re-add via /admin > Add Request Channel."
                    )
                    continue
                else:
                    # Private join channel — generate once and save permanently
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

            label = f"📨 Send Join Request {i}" if channel_type == "request" else f"📢 Join Channel {i}"
            buttons.append([InlineKeyboardButton(label, url=link)])

        except Exception as e:
            logger.warning(f"Could not build button for channel {channel_id}: {e}")
            continue

    if not buttons:
        # No buttons could be built — don't send an empty prompt
        logger.warning("send_fsub_message: no buttons built, skipping prompt")
        return

    file_part = pending_file_id if pending_file_id else "none"
    check_label = "✅ I've Requested — Check Now" if request_only else "✅ I've Joined — Check Now"
    buttons.append([
        InlineKeyboardButton(check_label, callback_data=f"check_fsub#{file_part}")
    ])
    markup = InlineKeyboardMarkup(buttons)

    if request_only:
        text = (
            f"👋 <b>One more step, {message.from_user.mention}!</b>\n\n"
            f"<blockquote>To receive files, please send a join request "
            f"to our channel below.\n"
            f"Then tap <b>✅ I've Requested</b> to get your file.</blockquote>"
        )
    else:
        text = (
            f"👋 <b>Hello {message.from_user.mention}!</b>\n\n"
            f"<blockquote>To use this bot, please join our channel(s) below.\n"
            f"Then tap <b>✅ I've Joined</b> to continue.</blockquote>"
        )

    await message.reply_text(
        text, reply_markup=markup, quote=True, parse_mode=ParseMode.HTML
    )
