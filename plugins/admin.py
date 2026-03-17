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
        f"╔══ 🛠 **MCCxBot Control Panel** ══╗\n\n"
        f"📊 **Live Stats:**\n"
        f"┣ 👤 Users: `{total_users}`\n"
        f"┣ 📁 Files: `{total_files}`\n"
        f"┣ 🏘 Groups: `{total_groups}`\n"
        f"┣ 📡 Log Channel: {log_status}\n"
        f"┗ 🔐 FSub: {fsub_status}\n\n"
        f"⚙️ **Core Settings:**\n"
        f"┣ 🔸 Log Channel: {log_display}\n"
        f"┣ 🔸 Admin ID: {admin_display}\n"
        f"┣ 🔹 Main Group: {group_status}\n"
        f"┗ 🔹 Update Channel: {update_status}\n\n"
        f"╚══ _Select a module below_ ══╝"
    )

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Analytics (Stats + Languages + Groups)", callback_data="admin_stats")],
        [InlineKeyboardButton("📚 Manage Database Channels",  callback_data="db_chan_menu")],
        [InlineKeyboardButton("🔐 Manage FSub Channels",      callback_data="fsub_menu")],
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
            "➕ **Send me the Channel ID** (e.g., `-100123456789`) to add as a Join FSub.\n"
            "Make sure the bot is an **Admin** in that channel first!\n\n"
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
    ~filters.command(["start", "admin", "ban", "unban", "reset_db", "broadcast", "cancel"])
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
                reply_markup=_BACK_BTN
            )

    elif state == "remdb":
        try:
            ch_val = int(message.text.strip())
            await db.remove_db_channel(ch_val)
            await message.reply_text(
                f"✅ **Channel `{ch_val}` Removed.**",
                reply_markup=_BACK_BTN
            )
        except ValueError:
            await message.reply_text(
                "❌ Invalid channel ID. Must be a number like `-100123456789`.",
                reply_markup=_BACK_BTN
            )

    elif state == "media":
        await db.update_config("start_media", message.text.strip())
        await message.reply_text(
            "✅ **Welcome Media Successfully Updated!**",
            reply_markup=_BACK_BTN
        )

    elif state == "welcometext":
        await db.update_config("welcome_text", message.text)
        await message.reply_text(
            "✅ **Welcome Text Successfully Updated!**\n\nType /start to see it live.",
            reply_markup=_BACK_BTN
        )

    elif state == "addfsub":
        # Join channel: admin sends the channel ID — verify bot is admin there
        try:
            raw = message.text.strip()
            ch_val = int(raw) if raw.lstrip('-').isdigit() else raw
            # Verify bot is an admin in the channel
            member = await client.get_chat_member(ch_val, client.me.id)
            if member.status.name not in ["ADMINISTRATOR", "CREATOR"]:
                await message.reply_text(
                    f"❌ **Bot is not an Admin in `{ch_val}`.**\n"
                    f"Please make the bot an Admin first, then try again.",
                    reply_markup=_BACK_BTN
                )
            else:
                await db.add_fsub_channel(ch_val)
                await message.reply_text(
                    f"✅ **Join Channel `{ch_val}` Added to FSub!**\n\n"
                    f"Users must join this channel to use the bot.",
                    reply_markup=_BACK_BTN
                )
        except Exception as e:
            await message.reply_text(
                f"❌ **Failed!** Make sure I am an Admin in that channel.\nError: `{e}`",
                reply_markup=_BACK_BTN
            )

    elif state == "remfsub":
        raw = message.text.strip()
        try:
            ch_val = int(raw)
        except ValueError:
            ch_val = raw  # treat as @username
        await db.remove_fsub_channel(ch_val)
        await message.reply_text(
            f"✅ **Channel `{ch_val}` Successfully Removed from FSub!**",
            reply_markup=_BACK_BTN
        )

    elif state == "autodeletetime":
        raw = message.text.strip()
        try:
            minutes = int(raw)
            if not 1 <= minutes <= 60:
                await message.reply_text(
                    "❌ **Invalid value.** Must be between 1 and 60 minutes.",
                    reply_markup=_BACK_BTN
                )
            else:
                await db.update_config("auto_delete_time", minutes * 60)
                await message.reply_text(
                    f"✅ **Auto-Delete Time Updated!**\n\n"
                    f"Files will now be deleted **{minutes} minute(s)** after sending.",
                    reply_markup=_BACK_BTN
                )
        except ValueError:
            await message.reply_text(
                "❌ **Invalid format.** Send a plain number like `5`.",
                reply_markup=_BACK_BTN
            )

    elif state == "captiontemplate":
        raw = message.text.strip()
        if raw.lower() == "clear":
            await db.update_config("file_caption_template", "")
            await message.reply_text(
                "✅ **Caption Template Cleared!**\n\nFiles will use the default caption.",
                reply_markup=_BACK_BTN
            )
        else:
            await db.update_config("file_caption_template", raw)
            await message.reply_text(
                f"✅ **Caption Template Updated!**\n\nTemplate saved. "
                f"New files will use your custom caption.",
                reply_markup=_BACK_BTN
            )

    elif state == "restore_config":
        await message.reply_text(
            "⚠️ To restore config, please send the JSON file as a document attachment, "
            "not as text. Use the restore callback button and attach your backup file.",
            reply_markup=_BACK_BTN
        )

    elif state == "logchannel":
        raw = message.text.strip()
        try:
            ch_val = int(raw)
            await client.get_chat(ch_val)
            await db.update_config("log_channel", ch_val)
            await message.reply_text(
                f"✅ **Log Channel Updated!**\n\n"
                f"New Log Channel ID: `{ch_val}`\n"
                f"All system logs and user alerts will now be sent there.",
                reply_markup=_BACK_BTN
            )
        except ValueError:
            await message.reply_text(
                "❌ **Invalid format!** Log Channel ID must be a number like `-100123456789`.",
                reply_markup=_BACK_BTN
            )
        except Exception as e:
            await message.reply_text(
                f"❌ **Cannot access that channel!**\n"
                f"Make sure the bot is an **Admin** in `{raw}` first.\nError: `{e}`",
                reply_markup=_BACK_BTN
            )

    elif state == "adminid":
        raw = message.text.strip()
        try:
            new_admin_id = int(raw)
            await db.update_config("admin_id", new_admin_id)
            await message.reply_text(
                f"✅ **Admin ID Updated in Database!**\n\n"
                f"New Admin ID: `{new_admin_id}`\n\n"
                f"⚠️ **Important:** The bot still uses `ADMIN_ID` from your `.env` "
                f"for command-level access until you restart. Update your `.env` too "
                f"if you want this change permanent across restarts.",
                reply_markup=_BACK_BTN
            )
        except ValueError:
            await message.reply_text(
                "❌ **Invalid format!** Admin ID must be a plain numeric Telegram user ID.\n"
                "Example: `123456789`",
                reply_markup=_BACK_BTN
            )

    _clear_state(admin_id)
    raise StopPropagation


# ── EXPLICIT /cancel COMMAND ─────────────────────────────────────────────────
# Handles /cancel as a real command so it NEVER leaks to filter.py as a search query.
# This is registered with group=0 (highest priority) so it fires first.

@Client.on_message(filters.command("cancel") & filters.private & filters.user(ADMIN_ID), group=0)
async def cancel_cmd(client: Client, message: Message):
    from plugins.state import clear_state as _cs
    _cs(message.from_user.id)
    await message.reply_text("🚫 **Cancelled.**", reply_markup=_BACK_BTN)
    raise StopPropagation


# ── SECURITY COMMANDS ─────────────────────────────────────────────────────────

@Client.on_message(filters.command("ban") & filters.private & filters.user(ADMIN_ID))
async def ban_user_cmd(client: Client, message: Message):
    if len(message.command) < 2:
        return await message.reply_text("⚠️ **Usage:** `/ban [user_id]`")
    try:
        user_id = int(message.command[1])
    except ValueError:
        return await message.reply_text("❌ Invalid user ID. Must be a number like `123456789`.")
    await db.ban_user(user_id)
    await message.reply_text(f"✅ **User `{user_id}` has been permanently banned.**")


@Client.on_message(filters.command("unban") & filters.private & filters.user(ADMIN_ID))
async def unban_user_cmd(client: Client, message: Message):
    if len(message.command) < 2:
        return await message.reply_text("⚠️ **Usage:** `/unban [user_id]`")
    try:
        user_id = int(message.command[1])
    except ValueError:
        return await message.reply_text("❌ Invalid user ID. Must be a number like `123456789`.")
    await db.unban_user(user_id)
    await message.reply_text(f"✅ **User `{user_id}` has been unbanned.**")


# ── DELETION COMMANDS ─────────────────────────────────────────────────────────

# /purge_cams removed — use File Manager → Bulk Delete with pattern "CAM|PreDVD|HDCAM"
# A "Quick Purge CAMs" shortcut is available in the File Manager for one-tap CAM purge.


@Client.on_message(filters.command("reset_db") & filters.private & filters.user(ADMIN_ID))
async def reset_db_cmd(client: Client, message: Message):
    await message.reply_text(
        "⚠️ **WARNING: NUCLEAR OPTION** ⚠️\n\n"
        "Are you absolutely sure you want to completely wipe ALL files, users, and bans across all 5 clusters?\n\n"
        "To confirm, reply to this message with: `/confirm_reset`",
        quote=True
    )


@Client.on_message(filters.command("confirm_reset") & filters.private & filters.user(ADMIN_ID))
async def confirm_reset_cmd(client: Client, message: Message):
    status = await message.reply_text("☢️ **Nuking the database...**")
    await db.reset_database()
    await status.edit_text("✅ **Database has been completely wiped.** You now have a clean slate.")




# ── /stats COMMAND ────────────────────────────────────────────────────────────
# Quick stats for admin — works instantly on mobile without opening the full panel

@Client.on_message(filters.command("stats") & filters.private & filters.user(ADMIN_ID))
async def stats_cmd(client: Client, message: Message):
    msg = await message.reply_text("⏳ Fetching live stats...", quote=True)
    total_users, total_banned, total_files, db_sizes, total_groups = await db.get_bot_stats()

    text = (
        f"📊 **MCCxBot Quick Stats**\n\n"
        f"👥 **Users:** `{total_users:,}`\n"
        f"🏘 **Groups:** `{total_groups:,}`\n"
        f"📁 **Total Files:** `{total_files:,}`\n"
        f"🚫 **Banned:** `{total_banned}`\n\n"
        f"💾 **Clusters:**\n"
    )
    for db_num, size in db_sizes:
        fill = int((size / 512) * 10)
        bar = "█" * fill + "░" * (10 - fill)
        text += f"├ Cluster {db_num}: [{bar}] `{size:.1f} MB`\n"

    await msg.edit_text(text)


# ── /help COMMAND ─────────────────────────────────────────────────────────────
# Works in both PM and groups. In groups it auto-deletes after 30 seconds.

@Client.on_message(filters.command("help"))
async def help_cmd(client: Client, message: Message):
    help_text = (
        f"📖 **How to use MCCxBot**\n\n"
        f"<blockquote>"
        f"1. Type a movie or series name\n"
        f"2. Select your language\n"
        f"3. Pick your preferred quality\n"
        f"4. Tap the file — it's sent to your PM instantly"
        f"</blockquote>\n\n"
        f"🎬 <b>Examples:</b>\n"
        f"<code>Leo</code>  •  <code>Aadujeevitham</code>  •  <code>KGF 2</code>\n\n"
        f"❓ Can\'t find it? Use the <b>Request</b> button and we\'ll upload "
        f"it within 24 hours."
    )

    is_group = message.chat.type.name in ["GROUP", "SUPERGROUP"]

    markup = None
    if is_group:
        markup = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "🤖 Search in PM",
                url=f"https://t.me/{client.me.username}?start=start"
            )
        ]])

    help_msg = await message.reply_text(
        help_text,
        reply_markup=markup,
        parse_mode=ParseMode.HTML,
        quote=True
    )

    # Auto-delete in groups after 30 seconds — fire-and-forget so the
    # handler returns immediately instead of blocking for 30 seconds
    if is_group:
        async def _delete_help(hm, om):
            await asyncio.sleep(30)
            try:
                await hm.delete()
                await om.delete()
            except Exception:
                pass
        asyncio.create_task(_delete_help(help_msg, message))
# ── FSub REFRESH JOIN LINKS ──────────────────────────────────────────────────
# Forces regeneration of join channel invite links.
# Use this if a join channel's link has expired or been manually revoked.
# Note: this will revoke the current stored link and create a new one.

@Client.on_callback_query(filters.regex(r"^fsub_refresh_links$") & filters.user(ADMIN_ID))
async def fsub_refresh_links(client: Client, callback: CallbackQuery):
    await callback.message.edit_text("♻️ **Refreshing join channel links...**")
    await callback.answer()

    config = await db.get_config()
    channels = config.get("fsub_channels", [])

    refreshed, skipped = 0, 0
    for entry in channels:
        if isinstance(entry, dict):
            ch_id = entry.get("id")
            ch_type = entry.get("type", "join")
        else:
            ch_id = entry
            ch_type = "join"

        if ch_type != "join" or not ch_id:
            skipped += 1
            continue

        ch_str = str(ch_id).strip()

        # Public @username channels never need invite links — skip them.
        # export_chat_invite_link on a public channel stores a private invite
        # link which can expire, replacing the working @username link.
        if ch_str.startswith("@") or not ch_str.startswith("-100"):
            skipped += 1
            continue

        try:
            new_link = await client.export_chat_invite_link(int(ch_str))
            await db.update_fsub_channel_link(ch_id, new_link)
            refreshed += 1
        except Exception as e:
            logger.warning(f"Could not refresh link for {ch_id}: {e}")
            skipped += 1

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to FSub Menu", callback_data="fsub_menu")]
    ])
    await callback.message.edit_text(
        f"♻️ **Links Refreshed!**\n\n"
        f"✅ Refreshed: `{refreshed}` join channel(s)\n"
        f"⏭ Skipped: `{skipped}` (request channels or errors)\n\n"
        f"All old FSub prompt messages are now invalid — users will need "
        f"a fresh prompt to get working buttons.",
        reply_markup=markup
    )


# ── CHANNEL HEALTH CHECK ──────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^channel_health_check$") & filters.user(ADMIN_ID))
async def channel_health_check(client: Client, callback: CallbackQuery):
    """Uses shared check_all_channels() from health_monitor — no duplicate logic."""
    from plugins.health_monitor import check_all_channels
    await callback.message.edit_text("🔍 **Running channel health check...**")
    await callback.answer()

    config = await db.get_config()
    results = await check_all_channels(client, config)

    report_text = "🔍 **Channel Health Report**\n\n" + "\n".join(results)
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Menu", callback_data="back_to_admin")]])

    try:
        await callback.message.edit_text(report_text, reply_markup=markup)
    except Exception:
        await callback.message.edit_text(report_text[:4000] + "\n...", reply_markup=markup)


# ── CLOSE PANEL ───────────────────────────────────────────────────────────────
# FIX #8: Only ONE close_data handler exists now — the duplicate in index.py is removed.

# ── C1: MAINTENANCE MODE TOGGLE ──────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^admin_toggle_maintenance$") & filters.user(ADMIN_ID))
async def toggle_maintenance(client: Client, callback: CallbackQuery):
    config = await db.get_config()
    current = config.get("maintenance_mode", False)
    new_val = not current
    await db.update_config("maintenance_mode", new_val)
    status = "🔧 **Maintenance Mode ON**\n\nUsers will see the maintenance message."         if new_val else "✅ **Maintenance Mode OFF**\n\nBot is live again."
    await callback.answer(f"{'ON' if new_val else 'OFF'}", show_alert=False)
    await callback.message.reply_text(status, reply_markup=_BACK_BTN)


# ── C9: EXPORT CONFIG ─────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^admin_export_config$") & filters.user(ADMIN_ID))
async def export_config(client: Client, callback: CallbackQuery):
    await callback.answer()
    config_data = await db.export_config()
    config_json = json.dumps(config_data, indent=2, default=str)
    import io
    buf = io.BytesIO(config_json.encode())
    buf.name = "mccxbot_config_backup.json"
    await callback.message.reply_document(
        document=buf,
        caption="📥 **MCCxBot Config Backup**\n\nStore this safely. "
                "Use Restore Config to apply it."
    )


# ── C10: RESTORE CONFIG ───────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^admin_restore_config$") & filters.user(ADMIN_ID))
async def restore_config_prompt(client: Client, callback: CallbackQuery):
    _set_state(callback.from_user.id, "restore_config_file")
    await callback.message.reply_text(
        "📤 **Restore Config**\n\n"
        "Send me the `.json` backup file as a **document attachment**.\n\n"
        "⚠️ This will overwrite your current settings (except log channel, "
        "admin ID, and DB channels which are always protected).\n\n"
        "*Type /cancel to abort.*"
    )
    await callback.answer()


@Client.on_message(
    filters.private & filters.document & filters.user(ADMIN_ID)
)
async def handle_config_restore_file(client: Client, message: Message):
    state = _get_state(message.from_user.id)
    if state != "restore_config_file":
        return

    if not message.document.file_name.endswith(".json"):
        return await message.reply_text(
            "❌ Please send a `.json` file.",
            reply_markup=_BACK_BTN
        )

    _clear_state(message.from_user.id)
    try:
        file_bytes = await client.download_media(message.document, in_memory=True)
        config_data = json.loads(file_bytes.getvalue().decode())
        success = await db.restore_config(config_data)
        if success:
            await message.reply_text(
                f"✅ **Config Restored!**\n\n"
                f"Restored `{len(config_data)}` settings from backup.\n"
                f"Protected fields (log channel, admin ID, DB channels) were not changed.",
                reply_markup=_BACK_BTN
            )
        else:
            await message.reply_text(
                "❌ No safe settings found to restore. File may be empty or invalid.",
                reply_markup=_BACK_BTN
            )
    except Exception as e:
        await message.reply_text(
            f"❌ Failed to parse backup file: `{e}`",
            reply_markup=_BACK_BTN
        )


# ── CLOSE PANEL ───────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^close_data$") & filters.user(ADMIN_ID))
async def close_callback(client: Client, callback: CallbackQuery):
    await callback.message.delete()
