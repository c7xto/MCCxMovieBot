"""Clean Telegram admin controls for filename branding."""

import logging
import os

from pyrogram import Client, ContinuePropagation, StopPropagation, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from database.db import db
from plugins.callbacks import answer_callback_safely
from plugins.file_branding import build_branded_file_name, normalize_brand_text
from plugins.state import clear_state, get_state
from plugins.ui_helpers import begin_prompt, delete_prompt_input, finish_prompt, restore_prompt
from utils import ADMIN_ID, report_internal_error


logger = logging.getLogger(__name__)


@Client.on_callback_query(filters.regex(r"^file_branding_menu$") & filters.user(ADMIN_ID))
async def file_branding_menu(client, callback, *, answer=True):
    clear_state(callback.from_user.id)
    if answer:
        await answer_callback_safely(callback)
    config = await db.get_config()
    enabled = bool(config.get("file_branding_enabled"))
    channel_id = int(config.get("file_branding_channel_id", 0) or 0)
    brand = normalize_brand_text(config.get("file_branding_text", ""), client.me.username)
    try:
        stats = await db.file_branding_stats()
        queue_text = (
            f"Waiting: `{stats['pending']:,}` • Working: `{stats['running']:,}`\n"
            f"Completed: `{stats['complete']:,}` • Failed: `{stats['failed']:,}`"
        )
        active_progress = (stats.get("active") or {}).get("progress") or {}
        total = int(active_progress.get("total", 0) or 0)
        current = int(active_progress.get("current", 0) or 0)
        if total:
            percent = min(100, max(0, round((current / total) * 100)))
            queue_text += (
                f"\n{active_progress.get('stage', 'Working')}: "
                f"`{percent}%`"
            )
    except Exception:
        queue_text = "Queue status is temporarily unavailable."
    text = (
        "🏷 **File Branding**\n\n"
        f"Status: {'🟢 Enabled' if enabled else '⚫ Disabled'}\n"
        f"Brand: `{brand or 'Not set'}`\n"
        f"Cache channel: `{channel_id}`\n\n"
        f"{queue_text}\n\n"
        "New source-channel uploads are downloaded once, renamed, uploaded "
        "to the private cache channel and then delivered instantly from Telegram.\n\n"
        "Existing library files are not re-uploaded by this setting. Turning "
        "branding off stops new jobs; files already waiting finish safely."
    )
    markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
            "⏸ Stop New Branding" if enabled else "▶ Enable Branding",
                    callback_data="file_branding_toggle",
                )
            ],
            [InlineKeyboardButton("📦 Set Cache Channel", callback_data="file_branding_set_channel")],
        [InlineKeyboardButton("✏ Set Brand Text", callback_data="file_branding_set_text")],
        [InlineKeyboardButton("👁 Filename Preview", callback_data="file_branding_preview")],
        [InlineKeyboardButton("↻ Refresh Status", callback_data="file_branding_menu")],
            [InlineKeyboardButton("‹ Appearance", callback_data="admin_cat_appearance")],
        ]
    )
    await callback.message.edit_text(text, reply_markup=markup)


@Client.on_callback_query(filters.regex(r"^file_branding_toggle$") & filters.user(ADMIN_ID))
async def file_branding_toggle(client, callback):
    await answer_callback_safely(callback)
    config = await db.get_config()
    enabled = bool(config.get("file_branding_enabled"))
    if not enabled and not int(config.get("file_branding_channel_id", 0) or 0):
        return await answer_callback_safely(callback, "Set a private cache channel first.", show_alert=True)
    await db.update_config("file_branding_enabled", not enabled)
    await answer_callback_safely(
        callback, "File branding enabled" if not enabled else "File branding disabled"
    )
    await file_branding_menu(client, callback, answer=False)


@Client.on_callback_query(filters.regex(r"^file_branding_set_channel$") & filters.user(ADMIN_ID))
async def file_branding_set_channel(client, callback):
    await begin_prompt(
        callback,
        "branding_channel",
        "📦 **Branded Cache Channel**\n\n"
        "Send the numeric ID of a private channel where renamed files should be stored.\n\n"
        "The bot must be an administrator there, and it must be different from every source channel.\n\n"
        "Example: `-1001234567890`",
    )


@Client.on_callback_query(filters.regex(r"^file_branding_set_text$") & filters.user(ADMIN_ID))
async def file_branding_set_text(client, callback):
    await begin_prompt(
        callback,
        "branding_text",
        "✏ **Brand Text**\n\n"
        "Send the text appended to every new filename.\n\n"
        f"Example: `@{client.me.username}`",
    )


@Client.on_callback_query(filters.regex(r"^file_branding_preview$") & filters.user(ADMIN_ID))
async def file_branding_preview(client, callback):
    await answer_callback_safely(callback)
    config = await db.get_config()
    brand = normalize_brand_text(config.get("file_branding_text", ""), client.me.username)
    movie = build_branded_file_name("Aavesham.2024.Malayalam.1080p.HEVC.mkv", brand)
    episode = build_branded_file_name("Reacher.S01E03.English.1080p.WEB-DL.mkv", brand)
    await callback.message.edit_text(
        f"👁 **Filename Preview**\n\nMovie\n`{movie}`\n\nSeries\n`{episode}`",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("‹ File Branding", callback_data="file_branding_menu")]]
        ),
    )


@Client.on_message(
    filters.private & filters.text & filters.user(ADMIN_ID) & ~filters.command(["start", "admin", "cancel"]),
    group=-1,
)
async def file_branding_input(client, message):
    admin_id = message.from_user.id
    state = get_state(admin_id)
    if state not in {"branding_channel", "branding_text"}:
        raise ContinuePropagation

    if message.text.casefold() in {"cancel", "/cancel"}:
        await restore_prompt(client, admin_id, fallback_message=message)
        await delete_prompt_input(message)
        raise StopPropagation

    if state == "branding_text":
        brand = normalize_brand_text(message.text, client.me.username)
        if not brand:
            brand = f"@{client.me.username}"
        await db.update_config("file_branding_text", brand)
        await finish_prompt(
            client,
            admin_id,
            f"✅ **Brand Text Updated**\n\n`{brand}`",
            back_callback="file_branding_menu",
            back_label="‹ File Branding",
            fallback_message=message,
        )
        raise StopPropagation

    try:
        channel_id = int(message.text.strip())
        config = await db.get_config()
        source_channels = {int(value) for value in config.get("db_channels", [])}
        env_source = int(os.getenv("DATABASE_CHANNEL_ID", 0) or 0)
        if env_source:
            source_channels.add(env_source)
        if channel_id in source_channels:
            raise ValueError("The cache channel cannot also be a source channel")
        chat = await client.get_chat(channel_id)
        member = await client.get_chat_member(channel_id, client.me.id)
        if member.status.name not in {"ADMINISTRATOR", "OWNER", "CREATOR"}:
            raise ValueError("Make the bot an administrator in that channel first")
        await db.update_config("file_branding_channel_id", channel_id)
        await finish_prompt(
            client,
            admin_id,
            f"✅ **Cache Channel Updated**\n\n{getattr(chat, 'title', 'Private channel')}\n`{channel_id}`",
            back_callback="file_branding_menu",
            back_label="‹ File Branding",
            fallback_message=message,
        )
    except ValueError as exc:
        await finish_prompt(
            client,
            admin_id,
            f"❌ **Cache Channel Not Updated**\n\n{str(exc)}",
            back_callback="file_branding_menu",
            back_label="‹ File Branding",
            fallback_message=message,
        )
    except Exception as exc:
        reference = report_internal_error(logger, "file_branding_channel", exc, admin_id=admin_id)
        await finish_prompt(
            client,
            admin_id,
            "❌ **Cache Channel Not Updated**\n\n"
            "I could not access that channel. Add the bot as an administrator "
            "and try again.\n\n"
            f"Reference: `{reference}`",
            back_callback="file_branding_menu",
            back_label="‹ File Branding",
            fallback_message=message,
        )
    raise StopPropagation
