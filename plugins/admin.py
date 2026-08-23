import os
import json
import asyncio
import logging
import time
from dotenv import load_dotenv
from pyrogram import ContinuePropagation, StopPropagation
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.enums import ParseMode
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery
)
from database.db import db
from plugins.state import get_state as _get_state_fn, set_state as _set_state_fn, clear_state as _clear_state_fn
from utils import ADMIN_ID, _no_preview, HELP_STEPS_EN, HELP_FOOTER_EN

# load_dotenv() here so ADMIN_ID is populated before module-level filter decorators run
load_dotenv()

logger = logging.getLogger(__name__)
_reset_confirmations = {}

# Reusable "Back to Panel" button — avoids repeating it everywhere
_BACK_BTN = InlineKeyboardMarkup([
    [InlineKeyboardButton("‹ Control Center", callback_data="back_to_admin")]
])


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
    fsub_status = f"✅ {fsub_count} channel{'s' if fsub_count != 1 else ''}" if fsub_count > 0 else "⚫ Disabled"

    # config.get('log_channel') returns 0 when unset, and 0 is falsy — a naive
    # truthy check would show "Missing" even after saving a valid channel ID,
    # so explicitly check for None/0/"" instead.
    log_val = config.get('log_channel')
    log_status = "✅ Set" if log_val not in [None, 0, ""] else "❌ Missing"

    text = (
        "🛠 **MCCx Control Center**\n\n"
        "🟢 **Status**  Online\n"
        f"📚 **Library**  `{total_files:,}` files\n"
        f"👥 **Audience**  `{total_users:,}` users  •  `{total_groups:,}` groups\n"
        f"🔐 **Access**  {fsub_status}\n"
        f"📡 **Logging**  {log_status}\n\n"
        "_Choose an area to manage._"
    )

    # Two-tier dashboard: the root panel only shows the 4 category tiles
    # (plus Close) — each opens a submenu built from _CATEGORY_MENUS below.
    # Same underlying callbacks as before, just better information
    # architecture instead of one flat 19-button wall.
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("📚 Library", callback_data="admin_cat_library"),
         InlineKeyboardButton("🎨 Appearance", callback_data="admin_cat_appearance")],
        [InlineKeyboardButton("👥 Access", callback_data="admin_cat_users"),
         InlineKeyboardButton("⚙ Preferences", callback_data="admin_cat_settings")],
        [InlineKeyboardButton("📊 Analytics", callback_data="admin_stats"),
         InlineKeyboardButton("🩺 System", callback_data="admin_cat_health")],
        [InlineKeyboardButton("✕ Close", callback_data="close_data")]
    ])
    return text, markup


# ── TWO-TIER DASHBOARD: CATEGORY SUBMENUS ─────────────────────────────────────
# Every entry here is one of the exact same callback_data strings the old
# flat menu used — this only changes how they're grouped and navigated to,
# not what they do or how they're handled.

_CATEGORY_MENUS = {
    "library": ("📚 **Library**", [
        ("📥 Source Channels",          "db_chan_menu"),
        ("🗂 File Manager",             "file_manager_menu"),
    ]),
    "appearance": ("🎨 **Appearance**", [
        ("✏ File Captions",             "edit_captiontemplate"),
        ("🖼 Welcome Media",            "edit_media"),
        ("💬 Welcome Message",          "edit_welcometext"),
    ]),
    "users": ("👥 **Users, Groups & Access**", [
        ("🔐 Required Channels",        "fsub_menu"),
        ("🛡 Access Gates",             "verification_gates_menu"),
        ("🏘 Group Manager",            "group_manager_menu"),
    ]),
    "settings": ("⚙ **Preferences & Backup**", [
        ("💬 Request Group",            "edit_maingroup"),
        ("📢 Public Updates",           "edit_update"),
        ("📡 Log Channel",              "edit_logchannel"),
        ("📣 Announcement Channel",     "edit_updatechid"),
        ("⏱ Auto-Delete",              "edit_autodeletetime"),
        ("⬇ Export Backup",            "admin_export_config"),
        ("⬆ Restore Backup",           "admin_restore_config"),
        ("🔄 Safe Update",              "upd_start"),
    ]),
    "health": ("🩺 **Health & System**", [
        ("📊 Analytics",                "admin_stats"),
        ("🔎 Channel Check",            "channel_health_check"),
        ("🧪 Diagnostics",              "known_issues_check"),
        ("🛠 Maintenance",              "admin_toggle_maintenance"),
    ]),
}

_CATEGORY_BACK_BTN = InlineKeyboardMarkup([
    [InlineKeyboardButton("‹ Control Center", callback_data="back_to_admin")]
])


@Client.on_callback_query(filters.regex(r"^admin_cat_(library|appearance|users|settings|health)$") & filters.user(ADMIN_ID))
async def show_category_menu(client: Client, callback: CallbackQuery):
    key = callback.data.split("_", 2)[2]
    title, items = _CATEGORY_MENUS[key]

    text = f"{title}\n\n_Select an action._"
    buttons = []
    for index in range(0, len(items), 2):
        buttons.append([
            InlineKeyboardButton(label, callback_data=cb)
            for label, cb in items[index:index + 2]
        ])
    buttons.append([InlineKeyboardButton("‹ Control Center", callback_data="back_to_admin")])

    try:
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
    except Exception:
        await callback.message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))
    await callback.answer()


# ── DASHBOARD ────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("admin") & filters.private & filters.user(ADMIN_ID))
async def admin_panel(client: Client, message: Message):
    text, markup = await get_admin_menu_data()
    await message.reply_text(
        text=text, reply_markup=markup, reply_parameters=None, **_no_preview()
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
    # split("_", 1) so names like "logchannel", "autodeletetime" never break
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
        "twostage1": (
            "➕ **Set Two-Stage Channel 1**\n\n"
            "Send the channel in any format:\n"
            "`https://t.me/+xxxxxxx` — private invite link ✅\n"
            "`https://t.me/username` — public channel link\n"
            "`@username` — public username\n"
            "`-1001234567890` — numeric channel ID\n\n"
            "Bot must be **Admin** in that channel.\n\n"
            "*Type /cancel to abort.*"
        ),
        "twostage2": (
            "➕ **Set Two-Stage Channel 2**\n\n"
            "Send the channel in any format:\n"
            "`https://t.me/+xxxxxxx` — private invite link ✅\n"
            "`https://t.me/username` — public channel link\n"
            "`@username` — public username\n"
            "`-1001234567890` — numeric channel ID\n\n"
            "Bot must be **Admin** in that channel.\n\n"
            "*Type /cancel to abort.*"
        ),
        "welcometext": (
            "📝 **Send me the new Welcome Message.**\n\n"
            "**Tip:** You can use standard Telegram HTML tags (`<b>`, `<i>`, `<blockquote>`).\n"
            "Type `{mention}` for an @-tag, `{first_name}` for their plain first name, "
            "or `{total_files:,}` for the live file count!\n\n"
            "*Type /cancel to abort.*"
        ),
        "logchannel": (
            "📡 **Send me the new Log Channel ID.**\n\n"
            "This is a numeric ID like `-100123456789`.\n"
            "Make sure the bot is an **Admin** in that channel first!\n\n"
            "*Type /cancel to abort.*"
        ),
        "updatechid": (
            "📢 **Send me the new Update Channel ID.**\n\n"
            "This is a numeric ID like `-100123456789` — the channel new-upload "
            "announcements are posted to.\n"
            "Make sure the bot is an **Admin** in that channel first!\n\n"
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
            "`{filename}` — Clean display name\n"
            "`{raw_filename}` — Original indexed name\n"
            "`{size}` — File size (e.g. 1.2GB)\n"
            "`{quality}` — Quality (e.g. 1080p)\n"
            "`{codec}` — Codec (e.g. HEVC)\n"
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


@Client.on_callback_query(filters.regex(r"^verification_gates_menu$") & filters.user(ADMIN_ID))
async def show_verification_gates_menu(client: Client, callback: CallbackQuery):
    """Request-FSub and Two-Stage Verification are two independently-built
    gates that both run before file delivery (on top of Main FSub, managed
    separately under "Manage FSub Channels") — grouped here under one
    submenu instead of two separate top-level entries so their cumulative
    effect on a new user is visible in one place. Each still has its own
    config/admin screen below; see plugins/req_fsub.py's
    check_verification_gates() for how they're combined into one join
    screen on the user-facing side."""
    config = await db.get_config()
    req_count = len(config.get("req_fsub_channels", []))
    two_stage_active = len([c for c in config.get("two_stage_channels", []) if c]) >= 2

    text = (
        f"🔐🔐 **Verification Gates**\n\n"
        f"Extra join-gates on top of Main FSub, checked before a file is delivered. "
        f"When more than one is active, the user sees a single combined join screen, "
        f"not separate sequential prompts.\n\n"
        f"📢 **Request-FSub:** {req_count} channel(s) configured\n"
        f"🔐🔐 **Two-Stage:** {'✅ Active' if two_stage_active else '⚫ Incomplete'}"
    )
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Request-FSub", callback_data="req_fsub_menu")],
        [InlineKeyboardButton("🔐🔐 Two-Stage Verification", callback_data="two_stage_menu")],
        [InlineKeyboardButton("🔙 Back", callback_data="back_to_admin")]
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
        [InlineKeyboardButton("🔙 Back",           callback_data="verification_gates_menu")]
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


# ── TWO-STAGE VERIFICATION MANAGER ────────────────────────────────────────────
# Real, sequential 2-channel gate — see plugins/req_fsub.py's module
# docstring for the full design. Both slots must be set for the gate to be
# active; leaving either one unset makes it a no-op (fails open), matching
# how every other optional channel-gate in this bot behaves.

def _fmt_two_stage_slot(entry):
    if not entry:
        return "_Not set_"
    cid = entry.get("id") if isinstance(entry, dict) else entry
    return f"`{cid}`"


@Client.on_callback_query(filters.regex(r"^two_stage_menu$") & filters.user(ADMIN_ID))
async def show_two_stage_menu(client: Client, callback: CallbackQuery):
    config   = await db.get_config()
    channels = config.get("two_stage_channels", [])
    ch1 = channels[0] if len(channels) > 0 else None
    ch2 = channels[1] if len(channels) > 1 else None
    active = bool(ch1 and ch2)

    text = (
        f"🔐🔐 **Two-Stage Verification**\n\n"
        f"Requires joining 2 channels in sequence — each step is a real "
        f"membership check via Telegram, not just a tap — before a file is "
        f"delivered. Verified users are cached for **30 minutes** so they "
        f"aren't re-prompted on every file in that window.\n\n"
        f"🔸 **Status:** {'✅ Active' if active else '⚫ Incomplete — both channels needed'}\n\n"
        f"**Channel 1:** {_fmt_two_stage_slot(ch1)}\n"
        f"**Channel 2:** {_fmt_two_stage_slot(ch2)}"
    )
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Set Channel 1", callback_data="edit_twostage1"),
         InlineKeyboardButton("✏️ Set Channel 2", callback_data="edit_twostage2")],
        [InlineKeyboardButton("🗑 Remove Channel 1", callback_data="twostage_remove1"),
         InlineKeyboardButton("🗑 Remove Channel 2", callback_data="twostage_remove2")],
        [InlineKeyboardButton("🔙 Back", callback_data="verification_gates_menu")]
    ])
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^twostage_remove(1|2)$") & filters.user(ADMIN_ID))
async def two_stage_remove(client: Client, callback: CallbackQuery):
    slot = int(callback.data[-1])
    await db.remove_two_stage_channel(slot)
    await callback.answer(f"✅ Channel {slot} removed.", show_alert=False)
    await show_two_stage_menu(client, callback)


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

# The exact set of state strings this handler's own if/elif chain below
# recognizes. plugins/admin.py registers first in plugin load order (see
# bot.py's plugins=dict(root="plugins") -> Pyrogram sorts plugin files
# alphabetically), so — unlike file_manager.py's "fm_" and group_manager.py's
# "gm_" prefixed states, which correctly ContinuePropagate past each other —
# this handler used to fall through any *unrecognized* truthy state straight
# to an unconditional StopPropagation, silently swallowing every fm_*/gm_*
# admin input before file_manager.py or group_manager.py ever saw it. Keep
# this set in sync with the state strings the elif chain below checks.
_OWN_STATES = {
    "maingroup", "update", "adddb", "remdb", "media", "addfsub", "remfsub",
    "twostage1", "twostage2", "welcometext", "logchannel", "updatechid",
    "autodeletetime", "captiontemplate", "restore_config",
    "req_fsub_add", "req_fsub_remove", "req_fsub_interval",
}


@Client.on_message(
    filters.private & filters.text & filters.user(ADMIN_ID) &
    ~filters.command(["start", "admin", "ban", "unban", "reset_db", "reset_index_progress", "broadcast", "cancel"]),
    group=-1,  # must win the race against filter.py's auto_filter (default group 0),
               # which matches any plain text and never ContinuePropagates — see
               # file_manager.py's / group_manager.py's / updater.py's matching state
               # catch-alls, all pinned to the same group for the same reason.
)
async def catch_admin_input(client: Client, message: Message):
    admin_id = message.from_user.id
    state = _get_state(admin_id)

    if not state or state not in _OWN_STATES:
        raise ContinuePropagation

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
        import re as _re
        raw = message.text.strip()

        # Resolve input → channel identifier Pyrogram can look up
        # Accepted: https://t.me/username  |  @username  |  -100xxxxxxx
        link_to_store = None
        ch_input = raw  # what we pass to get_chat / get_chat_member

        # Extract username from t.me link
        tme_match = _re.match(r'https?://t\.me/([a-zA-Z0-9_]+)', raw)
        if tme_match:
            username   = tme_match.group(1)
            ch_input   = f"@{username}"
            link_to_store = raw  # store the original https link

        elif raw.startswith("@"):
            ch_input   = raw
            link_to_store = f"https://t.me/{raw.lstrip('@')}"

        # bare numeric ID — no link to store (private channel, generate invite later)
        elif raw.lstrip("-").isdigit():
            ch_input = int(raw)

        else:
            await message.reply_text(
                "❌ **Invalid format.**\n\n"
                "Send one of:\n"
                "• `https://t.me/yourchannel`\n"
                "• `@yourchannel`\n"
                "• `-100123456789`",
                reply_markup=_BACK_BTN
            )
            _clear_state(admin_id)
            raise StopPropagation

        try:
            # Resolve to actual channel object to get numeric ID + title
            chat = await client.get_chat(ch_input)
            ch_id    = chat.id
            ch_title = getattr(chat, "title", str(ch_id))

            # If public channel and no link stored yet, build from username
            if not link_to_store:
                uname = getattr(chat, "username", None)
                if uname:
                    link_to_store = f"https://t.me/{uname}"
                # private channel: generate invite link
                else:
                    try:
                        link_to_store = await client.export_chat_invite_link(ch_id)
                    except Exception:
                        link_to_store = None

            # Check bot is admin
            try:
                member = await client.get_chat_member(ch_id, client.me.id)
                is_admin = member.status.name in ["ADMINISTRATOR", "CREATOR"]
            except Exception:
                is_admin = False

            if not is_admin:
                await message.reply_text(
                    f"❌ **Bot is not Admin in** `{ch_title}`.\n\n"
                    f"Make the bot an Admin first, then add it again.",
                    reply_markup=_BACK_BTN
                )
                _clear_state(admin_id)
                raise StopPropagation

            # Save: store numeric ID + link together
            await db.add_fsub_channel(ch_id)
            if link_to_store:
                await db.update_fsub_channel_link(ch_id, link_to_store)

            await message.reply_text(
                f"✅ **FSub Channel Added!**\n\n"
                f"📢 **{ch_title}**\n"
                f"🆔 `{ch_id}`\n"
                f"🔗 `{link_to_store or 'No link (set manually)'}`\n\n"
                f"Users must join this channel to use the bot.",
                reply_markup=_BACK_BTN
            )

        except StopPropagation:
            raise
        except Exception as e:
            await message.reply_text(
                f"❌ **Could not resolve channel.**\n\n`{e}`\n\n"
                f"Make sure the bot is a member/admin in that channel.",
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

    elif state in ("twostage1", "twostage2"):
        import re as _re
        raw  = message.text.strip()
        slot = 1 if state == "twostage1" else 2

        # Same input-resolution logic as addfsub — accepts a t.me link,
        # @username, or a bare numeric ID.
        link_to_store = None
        ch_input = raw

        tme_match = _re.match(r'https?://t\.me/([a-zA-Z0-9_]+)', raw)
        if tme_match:
            username = tme_match.group(1)
            ch_input = f"@{username}"
            link_to_store = raw
        elif raw.startswith("@"):
            ch_input = raw
            link_to_store = f"https://t.me/{raw.lstrip('@')}"
        elif raw.lstrip("-").isdigit():
            ch_input = int(raw)
        else:
            await message.reply_text(
                "❌ **Invalid format.**\n\n"
                "Send one of:\n"
                "• `https://t.me/yourchannel`\n"
                "• `@yourchannel`\n"
                "• `-100123456789`",
                reply_markup=_BACK_BTN
            )
            _clear_state(admin_id)
            raise StopPropagation

        try:
            chat = await client.get_chat(ch_input)
            ch_id    = chat.id
            ch_title = getattr(chat, "title", str(ch_id))

            if not link_to_store:
                uname = getattr(chat, "username", None)
                if uname:
                    link_to_store = f"https://t.me/{uname}"
                else:
                    # Private channel — a "request to join" link, matching
                    # the "Request to Join" wording this gate uses, not a
                    # direct-join link like the main FSub's addfsub flow.
                    try:
                        invite = await client.create_chat_invite_link(ch_id, creates_join_request=True)
                        link_to_store = invite.invite_link
                    except Exception:
                        link_to_store = None

            try:
                member = await client.get_chat_member(ch_id, client.me.id)
                is_admin = member.status.name in ["ADMINISTRATOR", "CREATOR"]
            except Exception:
                is_admin = False

            if not is_admin:
                await message.reply_text(
                    f"❌ **Bot is not Admin in** `{ch_title}`.\n\n"
                    f"Make the bot an Admin first, then add it again.",
                    reply_markup=_BACK_BTN
                )
                _clear_state(admin_id)
                raise StopPropagation

            await db.set_two_stage_channel(slot, ch_id)
            if link_to_store:
                await db.update_two_stage_channel_link(ch_id, link_to_store)

            await message.reply_text(
                f"✅ **Two-Stage Channel {slot} Set!**\n\n"
                f"📢 **{ch_title}**\n"
                f"🆔 `{ch_id}`\n"
                f"🔗 `{link_to_store or 'No link (set manually)'}`",
                reply_markup=_BACK_BTN
            )

        except StopPropagation:
            raise
        except Exception as e:
            await message.reply_text(
                f"❌ **Could not resolve channel.**\n\n`{e}`\n\n"
                f"Make sure the bot is a member/admin in that channel.",
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

    elif state == "updatechid":
        raw = message.text.strip()
        try:
            ch_val = int(raw)
            await client.get_chat(ch_val)
            await db.update_config("update_channel_id", ch_val)
            await message.reply_text(
                f"✅ **Update Channel Updated!**\n\n"
                f"New Update Channel ID: `{ch_val}`\n"
                f"New upload announcements will now post there.",
                reply_markup=_BACK_BTN
            )
        except ValueError:
            await message.reply_text(
                "❌ **Invalid format!** Update Channel ID must be a number like `-100123456789`.",
                reply_markup=_BACK_BTN
            )
        except Exception as e:
            await message.reply_text(
                f"❌ **Cannot access that channel!**\n"
                f"Make sure the bot is an **Admin** in `{raw}` first.\nError: `{e}`",
                reply_markup=_BACK_BTN
            )

    elif state == "req_fsub_add":
        import re as _re
        raw = message.text.strip()

        # Private invite link — https://t.me/+xxxx — store as-is, use as the link
        if raw.startswith("https://t.me/+"):
            ok, msg_r = await db.add_req_fsub_channel(raw)
            if ok:
                await message.reply_text(
                    f"✅ Private channel link added to Req FSub pool.\n"
                    f"🔗 `{raw}`\n\n"
                    f"Users will see this as the join link.",
                    reply_markup=_BACK_BTN
                )
            else:
                await message.reply_text(f"❌ Failed: {msg_r}", reply_markup=_BACK_BTN)

        # Public https://t.me/username
        elif raw.startswith("https://t.me/"):
            uname  = raw.rstrip("/").split("/")[-1]
            ch_val = f"@{uname}"
            ok, msg_r = await db.add_req_fsub_channel(ch_val)
            if ok:
                await message.reply_text(
                    f"✅ `{ch_val}` added to Req FSub pool.",
                    reply_markup=_BACK_BTN
                )
            else:
                await message.reply_text(f"❌ Failed: {msg_r}", reply_markup=_BACK_BTN)

        # @username or numeric ID
        else:
            try:
                ch_val = int(raw)
            except ValueError:
                ch_val = raw
            ok, msg_r = await db.add_req_fsub_channel(ch_val)
            if ok:
                await message.reply_text(
                    f"✅ `{ch_val}` added to Req FSub pool.",
                    reply_markup=_BACK_BTN
                )
            else:
                await message.reply_text(f"❌ Failed: {msg_r}", reply_markup=_BACK_BTN)

    elif state == "req_fsub_remove":
        raw = message.text.strip()
        try:
            ch_val = int(raw)
        except ValueError:
            ch_val = raw
        await db.remove_req_fsub_channel(ch_val)
        await message.reply_text(
            f"✅ Channel `{ch_val}` removed from Req FSub pool.",
            reply_markup=_BACK_BTN
        )

    elif state == "req_fsub_interval":
        try:
            hours = int(message.text.strip())
            if hours < 1:
                raise ValueError
            await db.update_config("req_fsub_interval_hours", hours)
            await message.reply_text(
                f"✅ Interval set to `{hours}h`.\n"
                f"Users will see the prompt once every {hours} hour(s).",
                reply_markup=_BACK_BTN
            )
        except ValueError:
            await message.reply_text("❌ Send a number like `24`.", reply_markup=_BACK_BTN)

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


@Client.on_message(filters.command("reset_index_progress") & filters.private & filters.user(ADMIN_ID))
async def reset_index_progress_cmd(client: Client, message: Message):
    """
    /reset_index_progress         - clears ALL channels saved progress
    /reset_index_progress <id>    - clears only that channel
    Use before re-indexing a channel you already indexed,
    or after wiping MongoDB so the bot starts from message 1.
    """
    args = message.command
    if len(args) > 1:
        try:
            chat_id = int(args[1])
        except ValueError:
            return await message.reply_text(
                "Invalid channel ID. Use a numeric ID like -1001234567890.",
                reply_parameters=None
            )
        await db.clear_index_progress(chat_id)
        await message.reply_text(
            "Index progress cleared for that channel.\n\nIndexing will start from message 1 next time.",
            reply_parameters=None
        )
    else:
        await db.clear_index_progress(chat_id=None)
        await message.reply_text(
            "All index progress cleared.\n\nIndexing will start from message 1 for all channels next time.",
            reply_parameters=None
        )


@Client.on_message(filters.command("reset_db") & filters.private & filters.user(ADMIN_ID))
async def reset_db_cmd(client: Client, message: Message):
    _reset_confirmations[message.from_user.id] = time.monotonic() + 60
    await message.reply_text(
        "⚠️ **WARNING: NUCLEAR OPTION** ⚠️\n\n"
        "Are you absolutely sure you want to completely wipe ALL files, users, and bans across all 5 clusters?\n\n"
        "To confirm, reply to this message with: `/confirm_reset`",
        reply_parameters=None
    )


@Client.on_message(filters.command("confirm_reset") & filters.private & filters.user(ADMIN_ID))
async def confirm_reset_cmd(client: Client, message: Message):
    expires_at = _reset_confirmations.pop(message.from_user.id, 0)
    if time.monotonic() > expires_at:
        return await message.reply_text(
            "Reset confirmation is missing or expired. Run /reset_db first."
        )
    status = await message.reply_text("☢️ **Nuking the database...**")
    try:
        await db.reset_database()
    except Exception as e:
        logger.exception("Database reset failed")
        return await status.edit_text(f"❌ **Reset aborted or incomplete:** `{e}`")
    await status.edit_text("✅ **Database has been completely wiped.** You now have a clean slate.")




# ── /stats COMMAND ────────────────────────────────────────────────────────────
# Quick stats for admin — works instantly on mobile without opening the full panel

@Client.on_message(filters.command("stats") & filters.private & filters.user(ADMIN_ID))
async def stats_cmd(client: Client, message: Message):
    msg = await message.reply_text("⏳ Fetching live stats...", reply_parameters=None)
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
    steps = "\n".join(f"{i}. {s}" for i, s in enumerate(HELP_STEPS_EN, 1))
    help_text = (
        f"📖 <b>How to use MCCxBot</b>\n\n"
        f"<blockquote>{steps}</blockquote>\n\n"
        f"🎬 <b>Examples:</b>\n"
        f"<code>Leo</code>  •  <code>Aadujeevitham</code>  •  <code>KGF 2</code>\n\n"
        f"❓ {HELP_FOOTER_EN}"
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
        reply_parameters=None
    )

    # Persist group cleanup so it survives a restart without a sleeping task.
    if is_group:
        await db.schedule_deletion(help_msg.chat.id, help_msg.id, 30)
        await db.schedule_deletion(message.chat.id, message.id, 30)
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
        except FloodWait as e:
            await asyncio.sleep(e.value)
            try:
                new_link = await client.export_chat_invite_link(int(ch_str))
                await db.update_fsub_channel_link(ch_id, new_link)
                refreshed += 1
            except Exception as e2:
                logger.warning(f"Could not refresh link for {ch_id} after FloodWait: {e2}")
                skipped += 1
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
    checks = await check_all_channels(client, config)

    report_text = "🔍 **Channel Health Report**\n\n" + "\n".join(c["text"] for c in checks)

    # One "Fix" button per failing/unconfigured channel — jumps straight into
    # the relevant edit flow instead of making the admin navigate back
    # through the menu to find it. Multiple DB/FSub channel failures all
    # point at the same manager screen, so that button is only offered once.
    buttons = []
    seen_fix_targets = set()
    for c in checks:
        fix = c.get("fix")
        if c["ok"] is not True and fix and fix not in seen_fix_targets:
            seen_fix_targets.add(fix)
            buttons.append([InlineKeyboardButton(f"🔧 Fix: {c['label']}", callback_data=fix)])

    buttons.append([InlineKeyboardButton("🔙 Back to Menu", callback_data="back_to_admin")])
    markup = InlineKeyboardMarkup(buttons)

    try:
        await callback.message.edit_text(report_text, reply_markup=markup)
    except Exception:
        await callback.message.edit_text(report_text[:4000] + "\n...", reply_markup=markup)


# ── KNOWN ISSUES ──────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^known_issues_check$") & filters.user(ADMIN_ID))
async def known_issues_check(client: Client, callback: CallbackQuery):
    """Live status tile — cluster capacity, whitelist-mode misconfiguration,
    verification-gate stacking, stuck indexer tasks, missing TMDB key.
    Uses shared check_known_issues() from health_monitor."""
    from plugins.health_monitor import check_known_issues
    await callback.message.edit_text("🔍 **Checking for known issues...**")
    await callback.answer()

    config   = await db.get_config()
    findings = await check_known_issues(client, config)
    report_text = "⚠️ **Known Issues**\n\n" + "\n\n".join(f["text"] for f in findings)

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to Menu", callback_data="back_to_admin")]
    ])
    try:
        await callback.message.edit_text(report_text, reply_markup=markup)
    except Exception:
        await callback.message.edit_text(report_text[:4000] + "\n...", reply_markup=markup)


# ── CLOSE PANEL ───────────────────────────────────────────────────────────────
# Only one close_data handler exists — the duplicate in bulk_indexer.py is removed.

# ── MAINTENANCE MODE TOGGLE ──────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^admin_toggle_maintenance$") & filters.user(ADMIN_ID))
async def toggle_maintenance(client: Client, callback: CallbackQuery):
    config = await db.get_config()
    current = config.get("maintenance_mode", False)
    new_val = not current
    await db.update_config("maintenance_mode", new_val)
    status = "🔧 **Maintenance Mode ON**\n\nUsers will see the maintenance message."         if new_val else "✅ **Maintenance Mode OFF**\n\nBot is live again."
    await callback.answer(f"{'ON' if new_val else 'OFF'}", show_alert=False)
    await callback.message.reply_text(status, reply_markup=_BACK_BTN)


# ── EXPORT CONFIG ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^admin_export_config$") & filters.user(ADMIN_ID))
async def export_config(client: Client, callback: CallbackQuery):
    await callback.answer("📥 Preparing export...", show_alert=False)
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


# ── RESTORE CONFIG ────────────────────────────────────────────────────────────

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
        # Not a config restore — let the message propagate to other handlers (e.g. forward_indexer)
        raise ContinuePropagation

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
    await callback.answer()
