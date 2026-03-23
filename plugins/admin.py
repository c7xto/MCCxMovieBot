import os
import io
import json
import asyncio
import logging
from dotenv import load_dotenv
from pyrogram import ContinuePropagation, StopPropagation
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery
)
try:
    from pyrogram.types import LinkPreviewOptions
    def _no_preview(): return {"link_preview_options": LinkPreviewOptions(is_disabled=True)}
except ImportError:
    LinkPreviewOptions = None
    def _no_preview(): return {"disable_web_page_preview": True}
from database.db import db
from plugins.state import get_state as _get_state_fn, set_state as _set_state_fn, clear_state as _clear_state_fn

load_dotenv()

logger = logging.getLogger(__name__)
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

_BACK_BTN = InlineKeyboardMarkup([
    [InlineKeyboardButton("🔙 Back to Admin Panel", callback_data="back_to_admin")]
])


def _get_state(admin_id):
    return _get_state_fn(admin_id)

def _set_state(admin_id, state):
    _set_state_fn(admin_id, state)

def _clear_state(admin_id):
    _clear_state_fn(admin_id)


async def get_admin_menu_data():
    config = await db.get_config()
    total_users, _, total_files, _, total_groups = await db.get_bot_stats()

    fsub_count = len(config.get("fsub_channels", []))
    fsub_status = f"✅ Active ({fsub_count} ch)" if fsub_count > 0 else "⚫ Disabled"

    log_val = config.get("log_channel")
    log_status = "✅ Set" if log_val not in [None, 0, ""] else "❌ Missing"

    text = (
        f"🛠 **MCCxBot Admin**\n\n"
        f"👤 `{total_users}` users  •  📁 `{total_files:,}` files  •  🏘 `{total_groups}` groups\n"
        f"🔐 FSub: {fsub_status}  •  📡 Log: {log_status}\n\n"
        f"_Tap a module below_"
    )

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Analytics",                  callback_data="admin_stats")],
        [InlineKeyboardButton("📚 Manage Database Channels",   callback_data="db_chan_menu")],
        [InlineKeyboardButton("🔐 Manage FSub Channels",       callback_data="fsub_menu")],
        [InlineKeyboardButton("📢 Req Channel FSub",            callback_data="req_fsub_menu")],
        [InlineKeyboardButton("⚙️ Change Main Group Link",     callback_data="edit_maingroup")],
        [InlineKeyboardButton("⚙️ Change Update Link",         callback_data="edit_update")],
        [InlineKeyboardButton("📡 Set Log Channel ID",         callback_data="edit_logchannel"),
         InlineKeyboardButton("👤 Set Admin ID",               callback_data="edit_adminid")],
        [InlineKeyboardButton("⏱ Set Auto-Delete Time",       callback_data="edit_autodeletetime")],
        [InlineKeyboardButton("🖼 Change Welcome Media",       callback_data="edit_media")],
        [InlineKeyboardButton("📝 Edit Welcome Text",          callback_data="edit_welcometext")],
        [InlineKeyboardButton("🔍 Channel Health Check",       callback_data="channel_health_check")],
        [InlineKeyboardButton("📁 File Manager",               callback_data="file_manager_menu"),
         InlineKeyboardButton("🏘 Group Manager",              callback_data="group_manager_menu")],
        [InlineKeyboardButton("🔧 Maintenance Mode",           callback_data="admin_toggle_maintenance")],
        [InlineKeyboardButton("✏️ Caption Template",           callback_data="edit_captiontemplate")],
        [InlineKeyboardButton("📥 Export Config",              callback_data="admin_export_config"),
         InlineKeyboardButton("📤 Restore Config",             callback_data="admin_restore_config")],
        [InlineKeyboardButton("🔄 Update Bot",                 callback_data="upd_start")],
        [InlineKeyboardButton("❌ Close Panel",                callback_data="close_data")]
    ])
    return text, markup


@Client.on_message(filters.command("admin") & filters.private & filters.user(ADMIN_ID))
async def admin_panel(client: Client, message: Message):
    text, markup = await get_admin_menu_data()
    await message.reply_text(text=text, reply_markup=markup, quote=True, **_no_preview())


@Client.on_callback_query(filters.regex(r"^back_to_admin$") & filters.user(ADMIN_ID))
async def back_to_admin(client: Client, callback: CallbackQuery):
    text, markup = await get_admin_menu_data()
    try:
        await callback.message.edit_text(text=text, reply_markup=markup, **_no_preview())
    except Exception:
        await callback.message.reply_text(text=text, reply_markup=markup, **_no_preview())
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^admin_stats$") & filters.user(ADMIN_ID))
async def show_stats(client: Client, callback: CallbackQuery):
    await callback.message.edit_text("⏳ **Loading analytics...**")
    await callback.answer()

    total_users, total_banned, total_files, db_sizes, total_groups = await db.get_bot_stats()

    cluster_text = ""
    for db_num, size in db_sizes:
        fill = int((size / 512) * 10)
        bar = "█" * fill + "░" * (10 - fill)
        cluster_text += f"├ Cluster {db_num}: [{bar}] `{size:.1f} MB`\n"

    try:
        lang_counts = await db.get_files_by_language()
        lang_lines = ""
        lang_emojis = {
            "Malayalam": "🌴", "Tamil": "🎭", "Telugu": "⭐",
            "Hindi": "🇮🇳", "English": "🌍", "Kannada": "🏵",
            "Dual Audio": "🎧", "Multi Audio": "🎵"
        }
        for lang, count in sorted(lang_counts.items(), key=lambda x: x[1], reverse=True):
            if count == 0:
                continue
            emoji = lang_emojis.get(lang, "🔊")
            pct = (count / total_files * 100) if total_files > 0 else 0
            bar_f = int(pct / 10)
            bar = "█" * bar_f + "░" * (10 - bar_f)
            lang_lines += f"{emoji} {lang:<12} [{bar}] `{count:,}`\n"
    except Exception:
        lang_lines = "_Language data unavailable_\n"

    try:
        top_groups = await db.get_top_groups(limit=5)
        group_lines = ""
        for i, g in enumerate(top_groups, 1):
            group_lines += f"{i}. {g.get('title','?')[:25]} — `{g.get('search_count',0)}` searches\n"
        if not group_lines:
            group_lines = "_No group activity yet_\n"
    except Exception:
        group_lines = "_Group data unavailable_\n"

    stats_text = (
        f"📊 **MCCxBot Analytics**\n\n"
        f"👥 Users: `{total_users:,}`  🚫 Banned: `{total_banned}`\n"
        f"📁 Files: `{total_files:,}`  🏘 Groups: `{total_groups}`\n\n"
        f"💾 **Storage:**\n{cluster_text}\n"
        f"🌐 **Files by Language:**\n{lang_lines}\n"
        f"🏆 **Top Active Groups:**\n{group_lines}"
    )

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to Menu", callback_data="back_to_admin")]
    ])
    try:
        await callback.message.edit_text(stats_text, reply_markup=markup)
    except Exception:
        await callback.message.edit_text(stats_text[:4000], reply_markup=markup)


@Client.on_callback_query(filters.regex(r"^edit_") & filters.user(ADMIN_ID))
async def handle_edit_buttons(client: Client, callback: CallbackQuery):
    action = callback.data.split("_", 1)[1]
    _set_state(callback.from_user.id, action)

    prompts = {
        "maingroup": "🔗 **Send me the new Main Group Link.**\n_/cancel to abort._",
        "update":    "🔗 **Send me the new Updates Channel Link.**\n_/cancel to abort._",
        "adddb":     "➕ **Send the Channel ID** to add to the Auto-Indexer.\nExample: `-1001234567890`\n_/cancel to abort._",
        "remdb":     "➖ **Send the Channel ID** to remove from the Auto-Indexer.\n_/cancel to abort._",
        "media":     "🖼 **Send the Catbox link** for your Welcome Media (.mp4, .gif, or image).\n_/cancel to abort._",
        "addfsub":   "➕ **Send the Channel ID** to add as FSub.\nBot must be **Admin** in that channel.\n_/cancel to abort._",
        "remfsub":   "➖ **Send the Channel ID or @username** to remove from FSub.\n_/cancel to abort._",
        "welcometext": (
            "📝 **Send the new Welcome Message.**\n\n"
            "Use HTML tags: `<b>`, `<i>`, `<blockquote>`\n"
            "Use `{mention}` for the user's name.\n_/cancel to abort._"
        ),
        "logchannel": (
            "📡 **Send the Log Channel ID.**\n"
            "Numeric ID like `-100123456789`.\n"
            "Bot must be **Admin** in that channel.\n_/cancel to abort._"
        ),
        "adminid": (
            "👤 **Send the new Admin User ID.**\n"
            "Numeric Telegram user ID like `123456789`.\n"
            "⚠️ Also update `.env` for permanent change.\n_/cancel to abort._"
        ),
        "autodeletetime": (
            "⏱ **Send the Auto-Delete Time in minutes.**\n"
            "Number between `1` and `60`.\n_/cancel to abort._"
        ),
        "captiontemplate": (
            "✏️ **Send the new Caption Template.**\n\n"
            "Variables: `{filename}` `{size}` `{quality}` `{lang}` `{username}` `{delete_minutes}`\n\n"
            "Send `clear` to reset to default.\n_/cancel to abort._"
        ),
        "restore_config": "📤 **Send the config .json file as a document.**\n_/cancel to abort._",
    }

    prompt = prompts.get(action)
    if not prompt:
        await callback.answer("⚠️ Unknown action.", show_alert=True)
        return

    await callback.message.reply_text(prompt)
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^fsub_menu$") & filters.user(ADMIN_ID))
async def show_fsub_menu(client: Client, callback: CallbackQuery):
    config = await db.get_config()
    channels = config.get("fsub_channels", [])

    text = "🔐 **FSub Manager**\n\n"
    if not channels:
        text += "⚫ Disabled — no channels set.\n"
    else:
        text += f"✅ Active — `{len(channels)}` channel(s)\n\n"
        for i, ch in enumerate(channels, 1):
            ch_id = ch.get("id", "?") if isinstance(ch, dict) else ch
            text += f" {i}. `{ch_id}` — 📢 Join\n"
    text += "\n_Users must join ALL listed channels._"

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Channel",       callback_data="edit_addfsub")],
        [InlineKeyboardButton("➖ Remove Channel",    callback_data="edit_remfsub")],
        [InlineKeyboardButton("♻️ Refresh Links",    callback_data="fsub_refresh_links")],
        [InlineKeyboardButton("🔙 Back",              callback_data="back_to_admin")]
    ])
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^req_fsub_menu$") & filters.user(ADMIN_ID))
async def show_req_fsub_menu(client: Client, callback: CallbackQuery):
    config = await db.get_config()
    channels = config.get("req_fsub_channels", [])
    interval = int(config.get("req_fsub_interval_hours", 24))

    text = (
        f"📢 **Request Channel FSub**\n\n"
        f"Users are prompted to join one random channel before file delivery.\n"
        f"Once every **{interval}h** per user.\n\n"
        f"**Channels ({len(channels)}/5):**\n"
    )
    for i, entry in enumerate(channels, 1):
        ch_id = entry.get("id") if isinstance(entry, dict) else entry
        text += f"`{i}.` `{ch_id}`\n"
    if not channels:
        text += "_None configured yet_\n"

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Channel",  callback_data="req_fsub_add"),
         InlineKeyboardButton("➖ Remove",        callback_data="req_fsub_remove")],
        [InlineKeyboardButton("⏱ Set Interval",  callback_data="req_fsub_interval")],
        [InlineKeyboardButton("🔙 Back",          callback_data="back_to_admin")]
    ])
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^req_fsub_add$") & filters.user(ADMIN_ID))
async def req_fsub_add_prompt(client: Client, callback: CallbackQuery):
    _set_state(callback.from_user.id, "req_fsub_add")
    await callback.message.reply_text(
        "➕ **Send the Channel ID** to add to the Req FSub pool.\n\n"
        "Bot must be **Admin with Invite permission**.\n"
        "Example: `-1001234567890`\n\n_/cancel to abort._"
    )
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^req_fsub_remove$") & filters.user(ADMIN_ID))
async def req_fsub_remove_prompt(client: Client, callback: CallbackQuery):
    _set_state(callback.from_user.id, "req_fsub_remove")
    await callback.message.reply_text("➖ **Send the Channel ID** to remove.\n\n_/cancel to abort._")
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^req_fsub_interval$") & filters.user(ADMIN_ID))
async def req_fsub_interval_prompt(client: Client, callback: CallbackQuery):
    config = await db.get_config()
    current = int(config.get("req_fsub_interval_hours", 24))
    _set_state(callback.from_user.id, "req_fsub_interval")
    await callback.message.reply_text(
        f"⏱ **Send interval in hours** between prompts per user.\n\n"
        f"Current: `{current}h` — Example: `24` = once per day.\n\n_/cancel to abort._"
    )
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^db_chan_menu$") & filters.user(ADMIN_ID))
async def show_db_chan_menu(client: Client, callback: CallbackQuery):
    config = await db.get_config()
    channels = config.get("db_channels", [])

    text = "📚 **Auto-Indexer Channels**\n\nFiles uploaded to these channels are auto-indexed:\n\n"
    if not channels:
        text += "🔸 No channels set (only checking .env).\n"
    else:
        for i, ch in enumerate(channels, 1):
            text += f" {i}. `{ch}`\n"

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add DB Channel", callback_data="edit_adddb"),
         InlineKeyboardButton("➖ Remove DB",       callback_data="edit_remdb")],
        [InlineKeyboardButton("🔙 Back",            callback_data="back_to_admin")]
    ])
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer()


@Client.on_message(
    filters.private & filters.text & filters.user(ADMIN_ID) &
    ~filters.command([
        "start", "admin", "ban", "unban", "reset_db",
        "reset_index_progress", "broadcast", "cancel",
        "stats", "help", "update"
    ])
)
async def catch_admin_input(client: Client, message: Message):
    admin_id = message.from_user.id
    state = _get_state(admin_id)

    if not state:
        raise ContinuePropagation

    if message.text.lower() in ("/cancel", "cancel"):
        _clear_state(admin_id)
        await message.reply_text("🚫 **Action Cancelled.**", reply_markup=_BACK_BTN)
        raise StopPropagation

    if state == "maingroup":
        await db.update_config("main_group", message.text.strip())
        await message.reply_text("✅ **Main Group Link Updated!**", reply_markup=_BACK_BTN)

    elif state == "update":
        await db.update_config("update_channel", message.text.strip())
        await message.reply_text("✅ **Update Channel Link Updated!**", reply_markup=_BACK_BTN)

    elif state == "adddb":
        try:
            ch_val = int(message.text.strip())
            await db.add_db_channel(ch_val)
            await message.reply_text(
                f"✅ **DB Channel `{ch_val}` Added!**\nFiles uploaded there will be auto-indexed.",
                reply_markup=_BACK_BTN
            )
        except Exception as e:
            await message.reply_text(f"❌ **Failed!**\n`{e}`", reply_markup=_BACK_BTN)

    elif state == "remdb":
        try:
            ch_val = int(message.text.strip())
            await db.remove_db_channel(ch_val)
            await message.reply_text(f"✅ **Channel `{ch_val}` Removed.**", reply_markup=_BACK_BTN)
        except ValueError:
            await message.reply_text("❌ Invalid ID. Must be a number.", reply_markup=_BACK_BTN)

    elif state == "media":
        await db.update_config("start_media", message.text.strip())
        await message.reply_text("✅ **Welcome Media Updated!**", reply_markup=_BACK_BTN)

    elif state == "welcometext":
        await db.update_config("welcome_text", message.text)
        await message.reply_text("✅ **Welcome Text Updated!** Type /start to preview.", reply_markup=_BACK_BTN)

    elif state == "addfsub":
        try:
            raw = message.text.strip()
            ch_val = int(raw) if raw.lstrip("-").isdigit() else raw
            member = await client.get_chat_member(ch_val, client.me.id)
            if member.status.name not in ["ADMINISTRATOR", "CREATOR"]:
                await message.reply_text(
                    f"❌ Bot is not Admin in `{ch_val}`. Make bot Admin first.",
                    reply_markup=_BACK_BTN
                )
            else:
                await db.add_fsub_channel(ch_val)
                await message.reply_text(
                    f"✅ **FSub Channel `{ch_val}` Added!**\nUsers must join to use the bot.",
                    reply_markup=_BACK_BTN
                )
        except Exception as e:
            await message.reply_text(f"❌ **Failed!**\n`{e}`", reply_markup=_BACK_BTN)

    elif state == "remfsub":
        raw = message.text.strip()
        try:
            ch_val = int(raw)
        except ValueError:
            ch_val = raw
        await db.remove_fsub_channel(ch_val)
        await message.reply_text(f"✅ **Channel `{ch_val}` Removed from FSub.**", reply_markup=_BACK_BTN)

    elif state == "autodeletetime":
        try:
            minutes = int(message.text.strip())
            if not 1 <= minutes <= 60:
                await message.reply_text("❌ Must be between 1 and 60 minutes.", reply_markup=_BACK_BTN)
            else:
                await db.update_config("auto_delete_time", minutes * 60)
                await message.reply_text(
                    f"✅ **Auto-Delete set to {minutes} minute(s).**", reply_markup=_BACK_BTN
                )
        except ValueError:
            await message.reply_text("❌ Send a plain number like `5`.", reply_markup=_BACK_BTN)

    elif state == "captiontemplate":
        raw = message.text.strip()
        if raw.lower() == "clear":
            await db.update_config("file_caption_template", "")
            await message.reply_text("✅ **Caption Template Cleared.** Using default.", reply_markup=_BACK_BTN)
        else:
            await db.update_config("file_caption_template", raw)
            await message.reply_text("✅ **Caption Template Updated.**", reply_markup=_BACK_BTN)

    elif state == "restore_config":
        await message.reply_text(
            "⚠️ Please send the JSON file as a **document**, not as text.",
            reply_markup=_BACK_BTN
        )

    elif state == "logchannel":
        raw = message.text.strip()
        try:
            ch_val = int(raw)
            await db.update_config("log_channel", ch_val)
            await message.reply_text(
                f"✅ **Log Channel set to `{ch_val}`.**", reply_markup=_BACK_BTN
            )
        except ValueError:
            await message.reply_text("❌ Must be a numeric ID like `-100123456789`.", reply_markup=_BACK_BTN)
        except Exception as e:
            await message.reply_text(f"❌ **Failed!**\n`{e}`", reply_markup=_BACK_BTN)

    elif state == "adminid":
        try:
            new_admin_id = int(message.text.strip())
            await db.update_config("admin_id", new_admin_id)
            await message.reply_text(
                f"✅ **Admin ID set to `{new_admin_id}` in DB.**\n"
                f"⚠️ Also update `.env` for permanent change.",
                reply_markup=_BACK_BTN
            )
        except ValueError:
            await message.reply_text("❌ Must be a numeric user ID.", reply_markup=_BACK_BTN)

    elif state == "req_fsub_add":
        raw = message.text.strip()
        try:
            ch_val = int(raw)
        except ValueError:
            ch_val = raw
        ok, msg_r = await db.add_req_fsub_channel(ch_val)
        if ok:
            await message.reply_text(
                f"✅ Channel `{ch_val}` added to R