import os
import json
import asyncio
import logging
from dotenv import load_dotenv
from pyrogram import ContinuePropagation, StopPropagation
from pyrogram import Client, filters
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

# load_dotenv() here so ADMIN_ID is populated before module-level filter decorators run
load_dotenv()

logger = logging.getLogger(__name__)

ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

# Reusable "Back to Panel" button — avoids repeating it everywhere
_BACK_BTN = InlineKeyboardMarkup([
    [InlineKeyboardButton("🔙 Back to Admin Panel", callback_data="back_to_admin")]
])

# link_preview_options replacement for deprecated disable_web_page_preview
# _NO_PREVIEW replaced with _no_preview() compat function above


def _get_state(admin_id):
    return _get_state_fn(admin_id)

def _set_state(admin_id, state):
    _set_state_fn(admin_id, state)

def _clear_state(admin_id):
    _clear_state_fn(admin_id)


async def get_admin_menu_data():
    """Fetches live config and builds the dynamic admin panel."""
    config = await db.get_config()
    total_users, _, total_files, _, total_groups = await db.get_bot_stats()

    fsub_count = len(config.get('fsub_channels', []))
    fsub_status = f"✅ Active ({fsub_count} ch)" if fsub_count > 0 else "⚫ Disabled"

    # BUG FIX #1 + #7: Unified single read per field.
    # Previously the status check used config.get('log_channel') which returns 0 for
    # unset, and 0 is falsy — so it showed ❌ Missing even after saving a valid ID.
    # Now we explicitly check for None/0/"" so a real channel ID always shows ✅.
    log_val = config.get('log_channel')
    log_display = f"`{log_val}`" if log_val not in [None, 0, ""] else "`Not Set`"
    log_status = "✅ Set" if log_val not in [None, 0, ""] else "❌ Missing"

    main_group = config.get('main_group', '')
    update_ch = config.get('update_channel', '')
    group_status = "✅ Set" if main_group else "❌ Missing"
    update_status = "✅ Set" if update_ch else "❌ Missing"

    db_admin = config.get('admin_id')
    admin_display = f"`{db_admin}`" if db_admin not in [None, 0, ""] else "`From .env`"

    text = (
        f"🛠 **MCCxBot Admin**\n\n"
        f"👤 `{total_users}` users  •  📁 `{total_files:,}` files  •  🏘 `{total_groups}` groups\n"
        f"🔐 FSub: {fsub_status}  •  📡 Log: {log_status}\n\n"
        f"_Tap a module below_"
    )

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Analytics (Stats + Languages + Groups)", callback_data="admin_stats")],
        [InlineKeyboardButton("📚 Manage Database Channels",  callback_data="db_chan_menu")],
        [InlineKeyboardButton("🔐 Manage FSub Channels",      callback_data="fsub_menu")],
        [InlineKeyboardButton("📢 Req Channel FSub",           callback_data="req_fsub_menu")],
        [InlineKeyboardButton("⚙️ Change Main Group Link",    callback_data="edit_maingroup")],
        [InlineKeyboardButton("⚙️ Change Update Link",        callback_data="edit_update")],
        [InlineKeyboardButton("📡 Set Log Channel ID",        callback_data="edit_logchannel"),
         InlineKeyboardButton("👤 Set Admin ID",              callback_data="edit_adminid")],
        [InlineKeyboardButton("⏱ Set Auto-Delete Time",      callback_data="edit_autodeletetime")],
        [InlineKeyboardButton("🖼 Change Welcome Media",      callback_data="edit_media")],
        [InlineKeyboardButton("📝 Edit Welcome Text",         callback_data="edit_welcometext")],
        [InlineKeyboardButton("🔍 Channel Health Check",      callback_data="channel_health_check")],
        [InlineKeyboardButton("📁 File Manager",              callback_data="file_manager_menu"),
         InlineKeyboardButton("🏘 Group Manager",             callback_data="group_manager_menu")],
        [InlineKeyboardButton("🔧 Maintenance Mode",          callback_data="admin_toggle_maintenance")],
        [InlineKeyboardButton("✏️ Caption Template",          callback_data="edit_captiontemplate")],
        [InlineKeyboardButton("📥 Export Config",             callback_data="admin_export_config"),
         InlineKeyboardButton("📤 Restore Config",            callback_data="admin_restore_config")],
        [InlineKeyboardButton("🔄 Update Bot",                callback_data="upd_start")],
        [InlineKeyboardButton("❌ Close Panel",               callback_data="close_data")]
    ])
    return text, markup


# ── DASHBOARD ────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("admin") & filters.private & filters.user(ADMIN_ID))
async def admin_panel(client: Client, message: Message):
    text, markup = await get_admin_menu_data()
    # FIX: replaced deprecated disable_web_page_preview with link_preview_options
    await message.reply_text(
        text=text, reply_markup=markup, quote=True, **_no_preview()
    )


@Client.on_callback_query(filters.regex(r"^back_to_admin$") & filters.user(ADMIN_ID))
async def back_to_admin(client: Client, callback: CallbackQuery):
    text, markup = await get_admin_menu_data()
    try:
        await callback.message.edit_text(
            text=text, reply_markup=markup, **_no_preview()
        )
    except Exception:
        # If the message is a media type (photo/video) we can't edit_text — send fresh
        await callback.message.reply_text(
            text=text, reply_markup=markup, **_no_preview()
        )
    await callback.answer()


# ── STATS ─────────────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^admin_stats$") & filters.user(ADMIN_ID))
async def show_stats(client: Client, callback: CallbackQuery):
    """Unified analytics — users, files, clusters, language breakdown, top groups."""
    await callback.message.edit_text("⏳ **Loading analytics...**")
    await callback.answer()

    total_users, total_banned, total_files, db_sizes, total_groups = await db.get_bot_stats()

    # Cluster bars
    cluster_text = ""
    for db_num, size in db_sizes:
        fill = int((size / 512) * 10)
        bar = "█" * fill + "░" * (10 - fill)
        cluster_text += f"├ Cluster {db_num}: [{bar}] `{size:.1f} MB`\n"

    # Language breakdown
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

    # Top 5 groups
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


# ── EDIT BUTTON DISPATCHER ───────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^edit_") & filters.user(ADMIN_ID))
async def handle_edit_buttons(client: Client, callback: CallbackQuery):
    # split("_", 1) so names like "logchannel", "adminid" never break
    action = callback.data.split("_", 1)[1]
    _set_state(callback.from_user.id, action)

    prompts = {
        "maingroup": (
            "🔗 **Send me the new Main Group Link** (Public or Private).\n"
            "*Type /cancel to abort.*"
        ),
        "update": (
            "🔗 **Send me the new Updates Channel Link** (Public or Private).\n"
            "*Type /cancel to abort.*"
        ),
        "adddb": (
            "➕ **Send me the Channel ID** (e.g., `-100123...`) to add to the Auto-Indexer.\n"
            "*Type /cancel to abort.*"
        ),
        "remdb": (
            "➖ **Send me the Channel ID** to remove from the Auto-Indexer.\n"
            "*Type /cancel to abort.*"
        ),
        "media": (
            "🖼 **Send me the new Catbox Link** for your Welcome Media (.mp4, .gif, or image).\n"
            "*Type /cancel to abort.*"
        ),
        "addfsub": (
            "➕ **Send the channel link, @username, or ID.**\n\n"
            "Accepted formats:\n"
            "`https://t.me/yourchannel`\n"
            "`@yourchannel`\n"
            "`-100123456789`\n\n"
            "Bot must be **Admin** in that channel.\n\n"
            "*Type /cancel to abort.*"
        ),

        "remfsub": (
            "➖ **Send me the Channel ID or Username** to remove from FSub.\n"
            "*Type /cancel to abort.*"
        ),
        "welcometext": (
            "📝 **Send me the new Welcome Message.**\n\n"
            "**Tip:** You can use standard Telegram HTML tags (`<b>`, `<i>`, `<blockquote>`).\n"
            "Type `{mention}` wherever you want the user's name to appear!\n\n"
            "*Type /cancel to abort.*"
        ),
        "logchannel": (
            "📡 **Send me the new Log Channel ID.**\n\n"
            "This is a numeric ID like `-100123456789`.\n"
            "Make sure the bot is an **Admin** in that channel first!\n\n"
            "*Type /cancel to abort.*"
        ),
        "adminid": (
            "👤 **Send me the new Admin User ID.**\n\n"
            "This is a numeric Telegram user ID like `123456789`.\n"
            "⚠️ This updates the live DB record. The `.env` value is still the "
            "**fallback** used on restart — update that too if you want it permanent.\n\n"
            "*Type /cancel to abort.*"
        ),
        "autodeletetime": (
            "⏱ **Send me the new Auto-Delete Time in minutes.**\n\n"
            "This is how long files stay before being deleted after sending.\n"
            "Must be a number between `1` and `60`.\n"
            "Current default: `5` minutes.\n\n"
            "*Type /cancel to abort.*"
        ),
        "captiontemplate": (
            "✏️ **Send me the new File Caption Template.**\n\n"
            "Available variables:\n"
            "`{filename}` — Full file name\n"
            "`{size}` — File size (e.g. 1.2GB)\n"
            "`{quality}` — Quality (e.g. 1080p)\n"
            "`{lang}` — Language (e.g. Malayalam)\n"
            "`{username}` — Bot username\n"
            "`{delete_minutes}` — Auto-delete minutes\n\n"
            "**Example:**\n"
            "`🍿 {filename}\n⏳ Deletes in {delete_minutes} min — @{username}`\n\n"
            "Send `clear` to reset to the default caption.\n\n"
            "*Type /cancel to abort.*"
        ),
        "restore_config": (
            "📤 **Send me the config backup JSON file.**\n\n"
            "This must be a `.json` file exported via the Export Config button.\n\n"
            "*Type /cancel to abort.*"
        ),
    }

    prompt = prompts.get(action)
    if not prompt:
        await callback.answer("⚠️ Unknown action.", show_alert=True)
        return

    await callback.message.reply_text(prompt)
    await callback.answer()


# ── FSUB MANAGER ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^fsub_menu$") & filters.user(ADMIN_ID))
async def show_fsub_menu(client: Client, callback: CallbackQuery):
    config = await db.get_config()
    channels = config.get("fsub_channels", [])

    text = "🔐 **Global FSub (Force Subscribe) Manager**\n\n"
    if not channels:
        text += "🔸 **Status:** ⚫ Disabled (No channels set).\n"
    else:
        text += f"🔸 **Status:** ✅ Active — `{len(channels)}` channel(s) enforced\n\n"
        text += "📋 **Currently Enforced Channels:**\n"
        for i, ch in enumerate(channels, 1):
            # Handle both old int format and new dict format
            if isinstance(ch, dict):
                ch_id = ch.get("id", "?")
                ch_type = ch.get("type", "join")
                type_icon = "📢 Join"
                text += f" {i}. `{ch_id}`  —  {type_icon}\n"
            else:
                text += f" {i}. `{ch}`  —  📢 Join\n"
    text += "\n*Users must satisfy ALL listed channels to use the bot.*"

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Join Channel",      callback_data="edit_addfsub")],
        [InlineKeyboardButton("➖ Remove Channel",        callback_data="edit_remfsub")],
        [InlineKeyboardButton("♻️ Refresh Join Links",   callback_data="fsub_refresh_links")],
        [InlineKeyboardButton("🔙 Back to Main Menu",     callback_data="back_to_admin")]
    ])
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer()


# ── DATABASE CHANNELS MANAGER ─────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^req_fsub_menu$") & filters.user(ADMIN_ID))
async def show_req_fsub_menu(client: Client, callback: CallbackQuery):
    config = await db.get_config()
    channels = config.get("req_fsub_channels", [])
    interval = int(config.get("req_fsub_interval_hours", 24))
    text = (
        f"📢 **Request Channel FSub**\n\n"
        f"Users are prompted to join one random channel before file delivery.\n"
        f"Only once every **{interval}h** per user.\n\n"
        f"**Channels ({len(channels)}/5):**\n"
    )
    for i, entry in enumerate(channels, 1):
        ch_id = entry.get("id") if isinstance(entry, dict) else entry
        text += f"`{i}.` `{ch_id}`\n"
    if not channels:
        text += "_None configured yet_\n"
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Channel",   callback_data="req_fsub_add"),
         InlineKeyboardButton("➖ Remove",         callback_data="req_fsub_remove")],
        [InlineKeyboardButton("⏱ Set Interval",   callback_data="req_fsub_interval")],
        [InlineKeyboardButton("🔙 Back",           callback_data="back_to_admin")]
    ])
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^req_fsub_add$") & filters.user(ADMIN_ID))
async def req_fsub_add_prompt(client: Client, callback: CallbackQuery):
    _set_state(callback.from_user.id, "req_fsub_add")
    await callback.message.reply_text(
        "➕ **Add a Req FSub Channel**\n\n"
        "Send the channel in any format:\n"
        "• `https://t.me/+xxxxxxx` — private invite link ✅\n"
        "• `https://t.me/username` — public channel link\n"
        "• `@username` — public username\n"
        "• `-1001234567890` — numeric channel ID\n\n"
        "_/cancel to abort._"
    )
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^req_fsub_remove$") & filters.user(ADMIN_ID))
async def req_fsub_remove_prompt(client: Client, callback: CallbackQuery):
    _set_state(callback.from_user.id, "req_fsub_remove")
    await callback.message.reply_text(
        "➖ **Send the Channel ID** to remove.\n\n_Type /cancel to abort._"
    )
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^req_fsub_interval$") & filters.user(ADMIN_ID))
async def req_fsub_interval_prompt(client: Client, callback: CallbackQuery):
    config = await db.get_config()
    current = int(config.get("req_fsub_interval_hours", 24))
    _set_state(callback.from_user.id, "req_fsub_interval")
    await callback.message.reply_text(
        f"⏱ **Send interval in hours** between prompts per user.\n\n"
        f"Current: `{current}h` — Example: `24` = once per day.\n\n"
        "_Type /cancel to abort._"
    )
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^db_chan_menu$") & filters.user(ADMIN_ID))
async def show_db_chan_menu(client: Client, callback: CallbackQuery):
    config = await db.get_config()
    channels = config.get("db_channels", [])

    text = "📚 **Auto-Indexer Channels**\n\nThe bot will automatically absorb files uploaded to these channels:\n\n"
    if not channels:
        text += "🔸 **Status:** No extra channels set (Only checking .env).\n"
    else:
        for i, ch in enumerate(channels, 1):
            text += f" {i}. `{ch}`\n"

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add DB Channel", callback_data="edit_adddb"),
         InlineKeyboardButton("➖ Remove DB",       callback_data="edit_remdb")],
        [InlineKeyboardButton("🔙 Back to Main Menu", callback_data="back_to_admin")]
    ])
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer()


# ── INPUT CATCHER ─────────────────────────────────────────────────────────────

@Client.on_message(
    filters.private & filters.text & filters.user(ADMIN_ID) &
    ~filters.command(["start", "admin", "ban", "unban", "reset_db", "reset_index_progress", "broadcast", "cancel"])
)
async def catch_admin_input(client: Client, message: Message):
    admin_id = message.from_user.id
    state = _get_state(admin_id)

    if not state:
        raise ContinuePropagation

    # DEAD END FIX #3: /cancel now returns a back button instead of a dead end
    if message.text.lower() in ("/cancel", "cancel"):
        _clear_state(admin_id)
        await message.reply_text(
            "🚫 **Action Cancelled.**",
            reply_markup=_BACK_BTN
        )
        raise StopPropagation

    # ── STATE HANDLERS ────────────────────────────────────────────────────────

    if state == "maingroup":
        await db.update_config("main_group", message.text.strip())
        # DEAD END FIX #2: back button on every success reply
        await message.reply_text(
            "✅ **Main Group Link Successfully Updated!**",
            reply_markup=_BACK_BTN
        )

    elif state == "update":
        await db.update_config("update_channel", message.text.strip())
        await message.reply_text(
            "✅ **Updates Channel Link Successfully Updated!**",
            reply_markup=_BACK_BTN
        )

    elif state == "adddb":
        try:
            ch_val = int(message.text.strip())
            await client.get_chat(ch_val)
            await db.add_db_channel(ch_val)
            await message.reply_text(
                f"✅ **Database Channel `{ch_val}` Added!**\n"
                f"Any movie uploaded there will now be auto-indexed.",
                reply_markup=_BACK_BTN
            )
        except Exception as e:
            # DEAD END FIX #4: back button on error replies too
            await message.reply_text(
                f"❌ **Failed!** Make sure I am an Admin in that channel.\nError: `{e}`",
     