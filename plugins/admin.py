import os
import json
import asyncio
import logging
import time
from dotenv import load_dotenv
from pyrogram import ContinuePropagation, StopPropagation
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import MessageNotModified
from pyrogram.types import Message, InlineKeyboardButton, CallbackQuery
from plugins.mobile_ui import MobileInlineKeyboardMarkup as InlineKeyboardMarkup
from database.db import db, validate_config_restore
from database.redis_client import redis_state
from plugins.state import (
    clear_state as _clear_state_fn,
    get_state as _get_state_fn,
    set_state as _set_state_fn,
)
from plugins.verification_channels import (
    ChannelConfigurationError,
    resolve_channel_id,
    resolve_request_fsub_channel,
)
from plugins.config_backup import encrypt_config_export
from plugins.callbacks import answer_callback_safely
from plugins.access_gates import access_gate_health, get_access_gates
from plugins.workload import workload_snapshot
from plugins.ui_helpers import begin_prompt, delete_prompt_input, finish_prompt, restore_prompt
from utils import (
    ADMIN_ID,
    _no_preview,
    HELP_STEPS_EN,
    HELP_FOOTER_EN,
    _html,
    report_internal_error,
)

# load_dotenv() here so ADMIN_ID is populated before module-level filter decorators run
load_dotenv()

logger = logging.getLogger(__name__)
MAX_CONFIG_BACKUP_BYTES = 64 * 1024
CONFIG_RESTORE_CONFIRM_SECONDS = 120

# Reusable "Back to Panel" button — avoids repeating it everywhere
_BACK_BTN = InlineKeyboardMarkup([[InlineKeyboardButton("‹ Control Center", callback_data="back_to_admin")]])


async def _get_state(admin_id):
    return await _get_state_fn(admin_id)


async def _set_state(admin_id, state):
    await _set_state_fn(admin_id, state)


async def _clear_state(admin_id):
    await _clear_state_fn(admin_id)


def _parse_config_backup(raw: bytes) -> dict:
    parsed = json.loads(raw)
    return validate_config_restore(parsed)


def _format_config_value(value, limit=72):
    rendered = json.dumps(value, ensure_ascii=False, default=str)
    return rendered if len(rendered) <= limit else f"{rendered[: limit - 3]}..."


def _format_restore_diff(current: dict, changes: dict) -> str:
    lines = ["Config restore preview", ""]
    for key in sorted(changes):
        lines.extend(
            [
                f"**{key}**",
                f"Before: `{_format_config_value(current.get(key))}`",
                f"After: `{_format_config_value(changes[key])}`",
                "",
            ]
        )
    lines.append("Apply these changes?")
    return "\n".join(lines)


async def get_admin_menu_data():
    """Fetches live config and builds the dynamic admin panel."""
    config = await db.get_config()
    total_users, _, total_files, _, total_groups = await db.get_bot_stats()

    gate_count = len(get_access_gates(config))
    fsub_status = (
        f"✅ {gate_count} gate{'s' if gate_count != 1 else ''}" if gate_count > 0 else "⚫ Disabled"
    )

    # config.get('log_channel') returns 0 when unset, and 0 is falsy — a naive
    # truthy check would show "Missing" even after saving a valid channel ID,
    # so explicitly check for None/0/"" instead.
    log_val = config.get("log_channel")
    log_status = "✅ Set" if log_val not in [None, 0, ""] else "❌ Missing"

    text = (
        "🛠 **MCCx Control Center**\n\n"
        "🟢 **Status**  Online\n"
        f"📚 **Library**  `{total_files:,}` files\n"
        f"👥 **Audience**  `{total_users:,}` users  •  `{total_groups:,}` groups\n"
        f"🔐 **Access**  {fsub_status}\n"
        f"📡 **Logging**  {log_status}\n\n"
        "Choose an area to manage."
    )

    # Two-tier dashboard: the root panel only shows the 4 category tiles
    # (plus Close) — each opens a submenu built from _CATEGORY_MENUS below.
    # Same underlying callbacks as before, just better information
    # architecture instead of one flat 19-button wall.
    markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📚 Library", callback_data="admin_cat_library"),
                InlineKeyboardButton("🎨 Appearance", callback_data="admin_cat_appearance"),
            ],
            [
                InlineKeyboardButton("👥 Access", callback_data="admin_cat_users"),
                InlineKeyboardButton("⚙ Preferences", callback_data="admin_cat_settings"),
            ],
            [
                InlineKeyboardButton("📊 Analytics", callback_data="admin_stats"),
                InlineKeyboardButton("🩺 System", callback_data="admin_cat_health"),
            ],
            [InlineKeyboardButton("✕ Close", callback_data="close_data")],
        ]
    )
    return text, markup


# ── TWO-TIER DASHBOARD: CATEGORY SUBMENUS ─────────────────────────────────────
# Every entry here is one of the exact same callback_data strings the old
# flat menu used — this only changes how they're grouped and navigated to,
# not what they do or how they're handled.

_CATEGORY_MENUS = {
    "library": (
        "📚 **Library**",
        "Manage source channels and every indexed file.",
        [
            ("📥 Source Channels", "db_chan_menu"),
            ("🗂 File Manager", "file_manager_menu"),
        ],
    ),
    "appearance": (
        "🎨 **Appearance**",
        "Control the home screen and delivered-file text.",
        [
            ("🏠 Home Screen", "home_screen_menu"),
            ("📝 Delivery Caption", "edit_captiontemplate"),
        ],
    ),
    "users": (
        "👥 **Users, Groups & Access**",
        "Manage verification requirements and connected groups.",
        [
            ("🛡 Access Gates", "verification_gates_menu"),
            ("🏘 Group Manager", "group_manager_menu"),
        ],
    ),
    "settings": (
        "⚙ **Preferences & Backup**",
        "Manage channels, deletion rules, backups and deployment.",
        [
            ("📣 Broadcasts", "broadcast_jobs_menu"),
            ("💬 Request Group", "edit_maingroup"),
            ("📰 New Releases Channel", "releases_channel_menu"),
            ("🎫 Request Inbox", "edit_requestchannel"),
            ("🛠 System Log Channel", "edit_logchannel"),
            ("⏱ Default Auto-Delete", "edit_autodeletetime"),
            ("⬇ Export Backup", "admin_export_config"),
            ("🔒 Secret Backup", "admin_export_secrets"),
            ("⬆ Restore Backup", "admin_restore_config"),
            ("🚀 Deployment Guide", "upd_start"),
        ],
    ),
    "health": (
        "🩺 **Health & System**",
        "Check Telegram connections and internal system health.",
        [
            ("📡 Telegram Channels", "channel_health_check"),
            ("🧪 System Diagnostics", "known_issues_check"),
            ("🛠 Maintenance", "admin_toggle_maintenance"),
        ],
    ),
}

_CATEGORY_BACK_BTN = InlineKeyboardMarkup(
    [[InlineKeyboardButton("‹ Control Center", callback_data="back_to_admin")]]
)


@Client.on_callback_query(
    filters.regex(r"^admin_cat_(library|appearance|users|settings|health)$") & filters.user(ADMIN_ID)
)
async def show_category_menu(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    await _clear_state(callback.from_user.id)
    key = callback.data.split("_", 2)[2]
    title, description, items = _CATEGORY_MENUS[key]

    text = f"{title}\n\n{description}"
    buttons = []
    for index in range(0, len(items), 2):
        buttons.append(
            [InlineKeyboardButton(label, callback_data=cb) for label, cb in items[index : index + 2]]
        )
    buttons.append([InlineKeyboardButton("‹ Control Center", callback_data="back_to_admin")])

    try:
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
    except Exception:
        await callback.message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))


@Client.on_callback_query(filters.regex(r"^home_screen_menu$") & filters.user(ADMIN_ID))
async def show_home_screen_menu(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    config = await db.get_config()
    text = (
        "🏠 **Home Screen**\n\n"
        f"Home text: `{'Custom' if config.get('welcome_text') else 'Default'}`\n"
        f"Home media: `{'Configured' if config.get('start_media') else 'Not set'}`\n\n"
        "These settings control the `/start` screen in private chat. "
        "Group welcome messages are separate system messages."
    )
    markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("💬 Edit Home Text", callback_data="edit_welcometext"),
                InlineKeyboardButton("🖼 Edit Home Media", callback_data="edit_media"),
            ],
            [InlineKeyboardButton("‹ Appearance", callback_data="admin_cat_appearance")],
        ]
    )
    await callback.message.edit_text(text, reply_markup=markup)


@Client.on_callback_query(filters.regex(r"^releases_channel_menu$") & filters.user(ADMIN_ID))
async def show_releases_channel_menu(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    config = await db.get_config()
    channel_id = int(config.get("update_channel_id", 0) or 0)
    link_set = bool(config.get("update_channel"))
    complete = bool(channel_id and link_set)
    text = (
        "📰 **New Releases Channel**\n\n"
        f"Status: `{'Ready' if complete else 'Incomplete'}`\n"
        f"Channel ID: `{'Set' if channel_id else 'Missing'}`\n"
        f"Public/join link: `{'Set' if link_set else 'Missing'}`\n\n"
        "One channel is used for both automatic new-upload announcements "
        "and the New Releases button shown to users."
    )
    markup = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✏ Set Channel", callback_data="edit_releaseschannel")],
            [InlineKeyboardButton("✕ Clear Channel", callback_data="releases_channel_clear")],
            [InlineKeyboardButton("‹ Preferences", callback_data="admin_cat_settings")],
        ]
    )
    await callback.message.edit_text(text, reply_markup=markup)


@Client.on_callback_query(filters.regex(r"^releases_channel_clear$") & filters.user(ADMIN_ID))
async def clear_releases_channel(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback, "New Releases channel cleared")
    await db.update_config_fields({"update_channel_id": 0, "update_channel": ""})
    await show_releases_channel_menu(client, callback)


# ── DASHBOARD ────────────────────────────────────────────────────────────────


@Client.on_message(filters.command("admin") & filters.private & filters.user(ADMIN_ID))
async def admin_panel(client: Client, message: Message):
    text, markup = await get_admin_menu_data()
    await message.reply_text(text=text, reply_markup=markup, reply_parameters=None, **_no_preview())


@Client.on_callback_query(filters.regex(r"^back_to_admin$") & filters.user(ADMIN_ID))
async def back_to_admin(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    await _clear_state(callback.from_user.id)
    text, markup = await get_admin_menu_data()
    try:
        await callback.message.edit_text(text=text, reply_markup=markup, **_no_preview())
    except Exception:
        # If the message is a media type (photo/video) we can't edit_text — send fresh
        await callback.message.reply_text(text=text, reply_markup=markup, **_no_preview())


# ── STATS ─────────────────────────────────────────────────────────────────────


def _analytics_markup(active="overview"):
    labels = {
        "overview": "📊 Overview",
        "library": "🌐 Library",
        "activity": "🏆 Activity",
        "health": "🩺 Health",
    }
    callbacks = {
        "overview": "admin_stats",
        "library": "admin_stats_library",
        "activity": "admin_stats_activity",
        "health": "admin_stats_health",
    }
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(labels["overview"], callback_data=callbacks["overview"]),
                InlineKeyboardButton(labels["library"], callback_data=callbacks["library"]),
            ],
            [
                InlineKeyboardButton(labels["activity"], callback_data=callbacks["activity"]),
                InlineKeyboardButton(labels["health"], callback_data=callbacks["health"]),
            ],
            [
                InlineKeyboardButton("↻ Refresh", callback_data=callbacks[active]),
                InlineKeyboardButton("‹ Control Center", callback_data="back_to_admin"),
            ],
        ]
    )


def _cluster_status_line(health: dict) -> str:
    state = str(health.get("state") or "checking")
    size = health.get("size_mb")
    state_ui = {
        "healthy": ("🟢", "Healthy"),
        "near_limit": ("🟠", "Near limit"),
        "full": ("🔴", "Full"),
        "unavailable": ("🔴", "Offline"),
        "capacity_unknown": ("🟡", "Capacity unknown"),
        "quarantined": ("🔒", "Needs migration"),
        "checking": ("🟡", "Checking"),
    }
    icon, label = state_ui.get(state, ("🟡", state.replace("_", " ").title()))
    if isinstance(size, (int, float)) and size != float("inf"):
        fill = max(0, min(10, round((float(size) / 450.0) * 10)))
        bar = "▰" * fill + "▱" * (10 - fill)
        usage = f"`{float(size):.1f} MB`"
    else:
        bar = "▱" * 10
        usage = "`Unavailable`"
    return (
        f"{icon} **Cluster {health.get('cluster')}** • {label}\n"
        f"`{bar}`  {usage}"
    )


async def _show_analytics_page(callback, text, active):
    try:
        await callback.message.edit_text(text[:4000], reply_markup=_analytics_markup(active))
    except MessageNotModified:
        # A quick refresh can legitimately produce the same live snapshot.
        # The callback was already acknowledged, so this is a successful no-op.
        pass


@Client.on_callback_query(filters.regex(r"^admin_stats$") & filters.user(ADMIN_ID))
async def show_stats(client: Client, callback: CallbackQuery):
    """Fast overview; expensive breakdowns live on their own pages."""
    await answer_callback_safely(callback, "Refreshing analytics…")
    total_users, total_banned, total_files, _, total_groups = await db.get_bot_stats()
    health = db.shard_health_snapshot()
    unavailable = db.unavailable_shards()
    coverage = "Complete" if not unavailable else f"Last-known • C{', C'.join(map(str, unavailable))} offline"
    cache = db.cache_metrics().get("queries", {})
    hits = int(cache.get("hits", 0))
    misses = int(cache.get("misses", 0))
    hit_rate = (hits * 100 / (hits + misses)) if hits + misses else 0.0
    workload = await workload_snapshot()
    ops_note = (
        "🟢 Dedicated operations database"
        if db.operations_db is not None
        else "🟠 Operations share a movie cluster"
    )
    cluster_lines = "\n".join(_cluster_status_line(item) for item in health)
    text = (
        "📊 **MCCxBot Analytics**\n"
        "Live, compact operational snapshot\n\n"
        "👥 **Audience**\n"
        f"Users  `{total_users:,}`   Groups  `{total_groups:,}`\n"
        f"Banned  `{total_banned:,}`\n\n"
        "📚 **Library**\n"
        f"Files  `{total_files:,}`\n"
        f"Coverage  `{coverage}`\n\n"
        f"💾 **Movie Shards**\n{cluster_lines or 'No movie shards configured'}\n\n"
        "⚡ **Live Performance**\n"
        f"Searches active  `{int(workload.get('search_active', 0))}` / "
        f"`{int(workload.get('search_capacity', 0))}`\n"
        f"Queue  `{int(workload.get('search_queue_depth', 0))}`   "
        f"Cache hit  `{hit_rate:.0f}%`\n"
        f"{ops_note}"
    )
    await _show_analytics_page(callback, text, "overview")


@Client.on_callback_query(filters.regex(r"^admin_stats_library$") & filters.user(ADMIN_ID))
async def show_stats_library(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback, "Loading library breakdown…")
    await callback.message.edit_text("🌐 **Library Analytics**\n\nCalculating language totals…")
    total_files = await db.get_total_files()
    lang_counts = await db.get_files_by_language()
    largest = max(lang_counts.values(), default=0)
    lines = []
    for language, count in sorted(lang_counts.items(), key=lambda item: item[1], reverse=True):
        if not count:
            continue
        fill = max(1, round((count / largest) * 8)) if largest else 0
        bar = "▰" * fill + "▱" * (8 - fill)
        percentage = count * 100 / total_files if total_files else 0.0
        lines.append(f"{bar}  **{language}**\n`{count:,}` files • `{percentage:.1f}%`")
    coverage = (
        "Complete across all connected shards."
        if not db.unavailable_shards()
        else "Partial: unavailable shards are excluded until they recover."
    )
    text = (
        "🌐 **Library Analytics**\n"
        f"`{total_files:,}` files currently known\n\n"
        + ("\n\n".join(lines) if lines else "No language tags detected.")
        + f"\n\nℹ️ {coverage}"
    )
    await _show_analytics_page(callback, text, "library")


@Client.on_callback_query(filters.regex(r"^admin_stats_activity$") & filters.user(ADMIN_ID))
async def show_stats_activity(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback, "Loading activity…")
    groups = await db.get_top_groups(limit=10)
    lines = []
    for position, group in enumerate(groups, 1):
        title = str(group.get("title") or "").strip()
        if not title or title == "?":
            title = "Unavailable group"
        lines.append(
            f"**{position}. {title[:32]}**\n`{int(group.get('search_count', 0)):,}` searches"
        )
    text = (
        "🏆 **Search Activity**\n"
        "Most active connected groups\n\n"
        + ("\n\n".join(lines) if lines else "No group search activity yet.")
        + "\n\nℹ️ No user search text or private data is shown."
    )
    await _show_analytics_page(callback, text, "activity")


@Client.on_callback_query(filters.regex(r"^admin_stats_health$") & filters.user(ADMIN_ID))
async def show_stats_health(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback, "Checking every MongoDB cluster…")
    await callback.message.edit_text("🩺 **System Health**\n\nRunning live connection checks…")
    health = await db.probe_shards(force=True)
    cluster_lines = "\n".join(_cluster_status_line(item) for item in health)
    workload = await workload_snapshot()
    cache = db.cache_metrics()
    operations = (
        "🟢 Dedicated and isolated"
        if db.operations_db is not None
        else "🔴 Unavailable • readiness violation"
    )
    text = (
        "🩺 **System Health**\n"
        "Fresh checks, not a stale snapshot\n\n"
        f"💾 **MongoDB**\n{cluster_lines}\n\n"
        f"🗂 **Operations Data**\n{operations}\n\n"
        "⚡ **Runtime**\n"
        f"Search queue  `{int(workload.get('search_queue_depth', 0))}`\n"
        f"Active searches  `{int(workload.get('search_active', 0))}`\n"
        f"Event-loop lag  `{int(workload.get('event_loop_lag_latest_ms', 0))} ms`\n"
        f"Search cache  `Redis • {int(cache.get('queries', {}).get('ttl_seconds', 0))}s TTL`"
    )
    await _show_analytics_page(callback, text, "health")


# ── EDIT BUTTON DISPATCHER ───────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^edit_") & filters.user(ADMIN_ID))
async def handle_edit_buttons(client: Client, callback: CallbackQuery):
    # split("_", 1) so names like "logchannel", "autodeletetime" never break
    action = callback.data.split("_", 1)[1]
    prompts = {
        "maingroup": (
            "💬 **Request Group**\n\n"
            "Send the public or private Telegram group link shown as the "
            "Request Group button. In-bot request tickets use Request Inbox instead.\n\n"
            "Example: `https://t.me/MCCxRequests`"
        ),
        "update": (
            "📰 **New Releases Channel**\n\n"
            "Send one channel link, @username or numeric ID. The bot must be an administrator."
        ),
        "releaseschannel": (
            "📰 **New Releases Channel**\n\n"
            "Send one channel link, @username or numeric ID. The bot will use the same "
            "channel for automatic announcements and the user-facing New Releases button.\n\n"
            "For a private channel you may send: `-1001234567890 https://t.me/+xxxx`"
        ),
        "adddb": ("➕ **Add Source Channel**\n\nSend the channel ID.\n\nExample: `-100123456789`"),
        "remdb": ("➖ **Remove Source Channel**\n\nSend the channel ID."),
        "media": ("🖼 **Home Media**\n\nSend the Catbox link for an image, GIF or MP4."),
        "addfsub": (
            "➕ **Add Required Channel**\n\n"
            "Accepted formats:\n"
            "`https://t.me/yourchannel`\n"
            "`@yourchannel`\n"
            "`-100123456789`\n"
            "`-100123456789 https://t.me/+xxxx` — private chat\n\n"
            "Bot must be **Admin** in that channel."
        ),
        "remfsub": ("➖ **Remove Required Channel**\n\nSend the channel ID or username."),
        "twostage1": (
            "➕ **Set Two-Stage Channel 1**\n\n"
            "Send the channel in one of these formats:\n"
            "`https://t.me/username` — public channel link\n"
            "`@username` — public username\n"
            "`-1001234567890` — numeric channel ID\n"
            "`-1001234567890 https://t.me/+xxxx` — private ID and link\n\n"
            "Bot must be **Admin** in that channel."
        ),
        "twostage2": (
            "➕ **Set Two-Stage Channel 2**\n\n"
            "Send the channel in one of these formats:\n"
            "`https://t.me/username` — public channel link\n"
            "`@username` — public username\n"
            "`-1001234567890` — numeric channel ID\n"
            "`-1001234567890 https://t.me/+xxxx` — private ID and link\n\n"
            "Bot must be **Admin** in that channel."
        ),
        "welcometext": (
            "📝 **Send me the new Home Message.**\n\n"
            "**Tip:** You can use standard Telegram HTML tags (`<b>`, `<i>`, `<blockquote>`).\n"
            "Type `{mention}` for an @-tag, `{first_name}` for their plain first name, "
            "or `{total_files:,}` for the live file count."
        ),
        "logchannel": (
            "🛠 **System Log Channel**\n\n"
            "This is a numeric ID like `-100123456789`.\n"
            "Make sure the bot is an **Admin** in that channel first."
        ),
        "requestchannel": (
            "🎫 **Request Inbox**\n\n"
            "Send the numeric channel ID where in-bot movie request tickets should arrive.\n\n"
            "The bot must be an administrator. If this is not set, the System Log Channel "
            "remains the compatibility fallback."
        ),
        "updatechid": (
            "📰 **New Releases Channel**\n\n"
            "Send one channel link, @username or numeric ID. The bot must be an administrator."
        ),
        "autodeletetime": (
            "⏱ **Default Auto-Delete Time**\n\n"
            "This is how long delivered files stay before deletion. Individual groups "
            "can override this default in Group Manager.\n"
            "Must be a number between `1` and `60`.\n"
            "Current default: `5` minutes."
        ),
        "captiontemplate": (
            "📝 **Delivery Caption**\n\n"
            "This changes the text below every delivered file; it does not rename the Telegram file.\n\n"
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
            "Send `clear` to reset to the default caption."
        ),
        "restore_config": (
            "📤 **Send me the config backup JSON file.**\n\n"
            "This must be a `.json` file exported with Export Backup."
        ),
    }

    prompt = prompts.get(action)
    if not prompt:
        await answer_callback_safely(callback, "⚠️ Unknown action.", show_alert=True)
        return
    await begin_prompt(callback, action, prompt)


# ── FSUB MANAGER ─────────────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^fsub_menu$") & filters.user(ADMIN_ID))
async def show_fsub_menu(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    config = await db.get_config()
    channels = config.get("fsub_channels", [])

    text = "🔐 **Required Access Channels**\n\n"
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
    text += "\nUsers must join every listed channel before receiving files."

    markup = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ Add Join Channel", callback_data="edit_addfsub")],
            [InlineKeyboardButton("➖ Remove Channel", callback_data="edit_remfsub")],
            [InlineKeyboardButton("♻️ Refresh Join Links", callback_data="fsub_refresh_links")],
            [InlineKeyboardButton("‹ Control Center", callback_data="back_to_admin")],
        ]
    )
    await callback.message.edit_text(text, reply_markup=markup)


# ── DATABASE CHANNELS MANAGER ─────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^verification_gates_menu$") & filters.user(ADMIN_ID))
async def show_verification_gates_menu(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    config = await db.get_config()
    gates = get_access_gates(config)
    required = sum(gate["mode"] == "required" for gate in gates)
    timed = len(gates) - required
    lines = []
    for index, gate in enumerate(gates, 1):
        healthy, reason = access_gate_health(gate)
        icon = "✅" if healthy else "⚠️"
        mode = "Required" if gate["mode"] == "required" else "Timed"
        interval = "15 min" if gate["mode"] == "required" else f"{gate['interval_seconds'] // 3600 or 1}h"
        lines.append(
            f"{icon} `{index}.` {mode} • `{gate.get('id') or 'Missing ID'}` • {interval}"
            + ("" if healthy else f"\n   Fix: {reason}")
        )
    text = (
        "🛡 **Access Gates**\n\n"
        "One clear access system is used before every file delivery.\n"
        f"Required: `{required}` • Timed: `{timed}`\n"
        "Successful checks are cached, with a short grace period during Telegram outages.\n\n"
        + ("\n".join(lines) if lines else "⚫ No access gates are active.")
    )
    markup = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔐 Required Channels", callback_data="fsub_menu")],
            [InlineKeyboardButton("⏱ Timed Channels", callback_data="req_fsub_menu")],
            [InlineKeyboardButton("‹ Control Center", callback_data="back_to_admin")],
        ]
    )
    await callback.message.edit_text(text, reply_markup=markup)


@Client.on_callback_query(filters.regex(r"^req_fsub_menu$") & filters.user(ADMIN_ID))
async def show_req_fsub_menu(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    config = await db.get_config()
    channels = config.get("req_fsub_channels", [])
    interval = int(config.get("req_fsub_interval_hours", 24))
    text = (
        f"⏱ **Timed Access Channels**\n\n"
        f"Users verify all configured timed channels once every **{interval}h**.\n"
        f"Required channels remain separate and are checked more often.\n\n"
        f"**Channels ({len(channels)}/5):**\n"
    )
    for i, entry in enumerate(channels, 1):
        ch_id = entry.get("id") if isinstance(entry, dict) else entry
        text += f"`{i}.` `{ch_id}`\n"
    if not channels:
        text += "None configured yet.\n"
    markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("➕ Add Channel", callback_data="req_fsub_add"),
                InlineKeyboardButton("➖ Remove", callback_data="req_fsub_remove"),
            ],
            [InlineKeyboardButton("⏱ Set Interval", callback_data="req_fsub_interval")],
            [InlineKeyboardButton("‹ Verification", callback_data="verification_gates_menu")],
        ]
    )
    await callback.message.edit_text(text, reply_markup=markup)


@Client.on_callback_query(filters.regex(r"^req_fsub_add$") & filters.user(ADMIN_ID))
async def req_fsub_add_prompt(client: Client, callback: CallbackQuery):
    await begin_prompt(
        callback,
        "req_fsub_add",
        "➕ **Add Timed Access Channel**\n\n"
        "Use a public link, @username, numeric ID, or private ID and link:\n"
        "`https://t.me/username`\n"
        "`@username`\n"
        "`-1001234567890`\n"
        "`-1001234567890 https://t.me/+xxxx`\n\n"
        "The bot must already be an administrator. A private invite link alone "
        "does not reveal its chat ID.",
    )


@Client.on_callback_query(filters.regex(r"^req_fsub_remove$") & filters.user(ADMIN_ID))
async def req_fsub_remove_prompt(client: Client, callback: CallbackQuery):
    await begin_prompt(callback, "req_fsub_remove", "➖ **Remove Channel**\n\nSend the channel ID.")


@Client.on_callback_query(filters.regex(r"^req_fsub_interval$") & filters.user(ADMIN_ID))
async def req_fsub_interval_prompt(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    config = await db.get_config()
    current = int(config.get("req_fsub_interval_hours", 24))
    await begin_prompt(
        callback,
        "req_fsub_interval",
        f"⏱ **Send interval in hours** between prompts per user.\n\n"
        f"Current: `{current}h` • Example: `24` means once per day.",
    )


# ── TWO-STAGE VERIFICATION MANAGER ────────────────────────────────────────────
# Real, sequential 2-channel gate — see plugins/req_fsub.py's module
# docstring for the full design. Both slots must be set for the gate to be
# active; leaving either one unset makes it a no-op (fails open), matching
# how every other optional channel-gate in this bot behaves.


def _fmt_two_stage_slot(entry):
    if not entry:
        return "Not set"
    cid = entry.get("id") if isinstance(entry, dict) else entry
    return f"`{cid}`"


@Client.on_callback_query(filters.regex(r"^two_stage_menu$") & filters.user(ADMIN_ID))
async def show_two_stage_menu(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    config = await db.get_config()
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
    markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✏️ Set Channel 1", callback_data="edit_twostage1"),
                InlineKeyboardButton("✏️ Set Channel 2", callback_data="edit_twostage2"),
            ],
            [
                InlineKeyboardButton("🗑 Remove Channel 1", callback_data="twostage_remove1"),
                InlineKeyboardButton("🗑 Remove Channel 2", callback_data="twostage_remove2"),
            ],
            [InlineKeyboardButton("‹ Verification", callback_data="verification_gates_menu")],
        ]
    )
    await callback.message.edit_text(text, reply_markup=markup)


@Client.on_callback_query(filters.regex(r"^twostage_remove(1|2)$") & filters.user(ADMIN_ID))
async def two_stage_remove(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    slot = int(callback.data[-1])
    await db.remove_two_stage_channel(slot)
    await answer_callback_safely(callback, f"✅ Channel {slot} removed.", show_alert=False)
    await show_two_stage_menu(client, callback)


@Client.on_callback_query(filters.regex(r"^db_chan_menu$") & filters.user(ADMIN_ID))
async def show_db_chan_menu(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    config = await db.get_config()
    channels = config.get("db_channels", [])
    env_channel = int(os.getenv("DATABASE_CHANNEL_ID", "0") or 0)

    text = "📚 **Source Channels**\n\nNew media posted in these channels is indexed automatically:\n\n"
    if env_channel:
        text += f"Environment fallback: `{env_channel}`\n"
    if channels:
        text += "Control Center channels:\n"
        for i, ch in enumerate(channels, 1):
            text += f" {i}. `{ch}`\n"
    if not env_channel and not channels:
        text += "⚠️ No source channel is configured.\n"
    text += (
        "\nThe environment fallback cannot be removed here; change "
        "`DATABASE_CHANNEL_ID` in your hosting panel."
    )

    markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("➕ Add Channel", callback_data="edit_adddb"),
                InlineKeyboardButton("➖ Remove Channel", callback_data="edit_remdb"),
            ],
            [InlineKeyboardButton("‹ Control Center", callback_data="back_to_admin")],
        ]
    )
    await callback.message.edit_text(text, reply_markup=markup)
    await answer_callback_safely(callback)


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
    "maingroup",
    "update",
    "releaseschannel",
    "requestchannel",
    "adddb",
    "remdb",
    "media",
    "addfsub",
    "remfsub",
    "twostage1",
    "twostage2",
    "welcometext",
    "logchannel",
    "updatechid",
    "autodeletetime",
    "captiontemplate",
    "restore_config",
    "req_fsub_add",
    "req_fsub_remove",
    "req_fsub_interval",
}


@Client.on_message(
    filters.private
    & filters.text
    & filters.user(ADMIN_ID)
    & ~filters.command(
        [
            "start",
            "admin",
            "ban",
            "unban",
            "reset_db",
            "reset_index_progress",
            "broadcast",
            "broadcast_status",
            "cancel",
        ]
    ),
    group=-1,  # must win the race against filter.py's auto_filter (default group 0),
    # which matches any plain text and never ContinuePropagates — see
    # file_manager.py's / group_manager.py's / updater.py's matching state
    # catch-alls, all pinned to the same group for the same reason.
)
async def catch_admin_input(client: Client, message: Message):
    admin_id = message.from_user.id
    state = await _get_state(admin_id)

    if not state or state not in _OWN_STATES:
        raise ContinuePropagation

    async def respond(text, **kwargs):
        """Finish the active prompt in place instead of stacking a new bubble."""
        await finish_prompt(
            client,
            admin_id,
            text,
            back_callback="back_to_admin",
            back_label="‹ Control Center",
            fallback_message=message,
            reply_markup=kwargs.pop("reply_markup", _BACK_BTN),
            parse_mode=kwargs.pop("parse_mode", None),
        )

    if message.text.lower() in ("/cancel", "cancel"):
        await restore_prompt(client, admin_id, fallback_message=message)
        await delete_prompt_input(message)
        raise StopPropagation

    # ── STATE HANDLERS ────────────────────────────────────────────────────────

    if state == "maingroup":
        await db.update_config("main_group", message.text.strip())
        await respond("✅ **Main Group Link Successfully Updated!**", reply_markup=_BACK_BTN)

    elif state in {"update", "updatechid", "releaseschannel"}:
        raw = message.text.strip()
        try:
            verified = await resolve_request_fsub_channel(client, raw)
            await db.update_config_fields(
                {
                    "update_channel_id": verified.chat_id,
                    "update_channel": verified.link,
                }
            )
            await respond(
                "✅ <b>New Releases Channel Updated</b>\n\n"
                f"📰 <b>{_html(verified.title)}</b>\n"
                f"🆔 <code>{verified.chat_id}</code>\n\n"
                "Automatic announcements and user buttons now use this one channel.",
                parse_mode=ParseMode.HTML,
                reply_markup=_BACK_BTN,
            )
        except ChannelConfigurationError as error:
            await respond(
                "❌ <b>New Releases Channel Not Updated</b>\n\n"
                f"{_html(str(error))}",
                parse_mode=ParseMode.HTML,
                reply_markup=_BACK_BTN,
            )

    elif state == "adddb":
        try:
            ch_val = int(message.text.strip())
            await client.get_chat(ch_val)
            await db.add_db_channel(ch_val)
            await respond(
                f"✅ **Database Channel `{ch_val}` Added!**\n"
                f"Any movie uploaded there will now be auto-indexed.",
                reply_markup=_BACK_BTN,
            )
        except ValueError:
            await respond(
                "❌ **Invalid Channel ID**\n\nSend a number like `-100123456789`.",
                reply_markup=_BACK_BTN,
            )
        except Exception as error:
            reference = report_internal_error(logger, "admin_add_db_channel", error)
            await respond(
                f"❌ **Failed!** Make sure I am an Admin in that channel.\nReference: `{reference}`",
                reply_markup=_BACK_BTN,
            )

    elif state == "remdb":
        try:
            ch_val = int(message.text.strip())
            env_channel = int(os.getenv("DATABASE_CHANNEL_ID", "0") or 0)
            if ch_val == env_channel:
                await respond(
                    "⚠️ **Environment Source Not Removed**\n\n"
                    "This channel comes from `DATABASE_CHANNEL_ID`. Remove or change it "
                    "in your hosting panel, then restart the bot.",
                    reply_markup=_BACK_BTN,
                )
                return
            await db.remove_db_channel(ch_val)
            await respond(f"✅ **Channel `{ch_val}` Removed.**", reply_markup=_BACK_BTN)
        except ValueError:
            await respond(
                "❌ Invalid channel ID. Must be a number like `-100123456789`.", reply_markup=_BACK_BTN
            )

    elif state == "media":
        await db.update_config("start_media", message.text.strip())
        await respond("✅ **Home Media Updated!**", reply_markup=_BACK_BTN)

    elif state == "welcometext":
        await db.update_config("welcome_text", message.text)
        await respond(
            "✅ **Home Text Updated!**\n\nType /start to see it live.", reply_markup=_BACK_BTN
        )

    elif state == "addfsub":
        raw = message.text.strip()
        try:
            verified = await resolve_request_fsub_channel(client, raw)
            await db.add_fsub_channel(verified.chat_id, verified.link)

            await respond(
                "✅ <b>Required Channel Added</b>\n\n"
                f"📢 <b>{_html(verified.title)}</b>\n"
                f"🆔 <code>{verified.chat_id}</code>\n"
                f"🔗 <code>{_html(verified.link)}</code>\n\n"
                "Users must join this channel to use the bot.",
                parse_mode=ParseMode.HTML,
                reply_markup=_BACK_BTN,
            )
        except ChannelConfigurationError as error:
            logger.info("Required access channel rejected: %s", error)
            await respond(
                "❌ <b>Required Channel Not Added</b>\n\n"
                f"{_html(str(error))}",
                parse_mode=ParseMode.HTML,
                reply_markup=_BACK_BTN,
            )
        except Exception as error:
            reference = report_internal_error(logger, "admin_add_fsub", error)
            await respond(
                "❌ **Could not resolve channel.**\n\n"
                "Make sure the bot is a member/admin in that channel.\n"
                f"Reference: `{reference}`",
                reply_markup=_BACK_BTN,
            )

    elif state == "remfsub":
        raw = message.text.strip()
        try:
            ch_val = await resolve_channel_id(client, raw)
            removed = await db.remove_fsub_channel(ch_val)
            if removed:
                await respond(f"✅ **Required channel `{ch_val}` removed.**", reply_markup=_BACK_BTN)
            else:
                await respond(
                    f"ℹ️ **Channel `{ch_val}` was not in Required Access.**",
                    reply_markup=_BACK_BTN,
                )
        except ChannelConfigurationError as error:
            await respond(f"❌ **Channel not removed.**\n\n{str(error)}", reply_markup=_BACK_BTN)

    elif state in ("twostage1", "twostage2"):
        raw = message.text.strip()
        slot = 1 if state == "twostage1" else 2
        try:
            verified = await resolve_request_fsub_channel(client, raw)
            await db.set_two_stage_channel(slot, verified.chat_id, verified.link)

            await respond(
                f"✅ <b>Two-Stage Channel {slot} Set</b>\n\n"
                f"📢 <b>{_html(verified.title)}</b>\n"
                f"🆔 <code>{verified.chat_id}</code>\n"
                f"🔗 <code>{_html(verified.link)}</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=_BACK_BTN,
            )
        except ChannelConfigurationError as error:
            logger.info("Two-stage channel rejected: %s", error)
            await respond(
                f"❌ <b>Two-Stage Channel {slot} Not Set</b>\n\n{_html(str(error))}",
                parse_mode=ParseMode.HTML,
                reply_markup=_BACK_BTN,
            )
        except Exception as error:
            reference = report_internal_error(logger, "admin_set_two_stage", error)
            await respond(
                "❌ **Could not resolve channel.**\n\n"
                "Make sure the bot is a member/admin in that channel.\n"
                f"Reference: `{reference}`",
                reply_markup=_BACK_BTN,
            )

    elif state == "autodeletetime":
        raw = message.text.strip()
        try:
            minutes = int(raw)
            if not 1 <= minutes <= 60:
                await respond(
                    "❌ **Invalid value.** Must be between 1 and 60 minutes.", reply_markup=_BACK_BTN
                )
            else:
                await db.update_config("auto_delete_time", minutes * 60)
                await respond(
                    f"✅ **Default Auto-Delete Updated!**\n\n"
                    f"Files will now be deleted **{minutes} minute(s)** after sending "
                    "unless a group override is configured.",
                    reply_markup=_BACK_BTN,
                )
        except ValueError:
            await respond("❌ **Invalid format.** Send a plain number like `5`.", reply_markup=_BACK_BTN)

    elif state == "captiontemplate":
        raw = message.text.strip()
        if raw.lower() == "clear":
            await db.update_config("file_caption_template", "")
            await respond(
                "✅ **Delivery Caption Reset**\n\nAll delivered files will use the default caption.",
                reply_markup=_BACK_BTN,
            )
        else:
            await db.update_config("file_caption_template", raw)
            await respond(
                "✅ **Delivery Caption Updated**\n\n"
                "The template now applies whenever any existing or new file is delivered.",
                reply_markup=_BACK_BTN,
            )

    elif state == "restore_config":
        await respond(
            "⚠️ To restore config, please send the JSON file as a document attachment, "
            "not as text. Use the restore callback button and attach your backup file.",
            reply_markup=_BACK_BTN,
        )

    elif state == "logchannel":
        raw = message.text.strip()
        try:
            ch_val = int(raw)
            await client.get_chat(ch_val)
            await db.update_config("log_channel", ch_val)
            await respond(
                f"✅ **Log Channel Updated!**\n\n"
                f"New Log Channel ID: `{ch_val}`\n"
                f"All system logs and user alerts will now be sent there.",
                reply_markup=_BACK_BTN,
            )
        except ValueError:
            await respond(
                "❌ **Invalid format!** Log Channel ID must be a number like `-100123456789`.",
                reply_markup=_BACK_BTN,
            )
        except Exception as error:
            reference = report_internal_error(logger, "admin_set_log_channel", error)
            await respond(
                "❌ **Cannot access that channel!**\n"
                "Make sure the bot is an **Admin** there first.\n"
                f"Reference: `{reference}`",
                reply_markup=_BACK_BTN,
            )

    elif state == "requestchannel":
        raw = message.text.strip()
        try:
            ch_val = int(raw)
            await client.get_chat(ch_val)
            member = await client.get_chat_member(ch_val, client.me.id)
            if member.status.name not in {"ADMINISTRATOR", "OWNER", "CREATOR"}:
                raise ValueError("Make the bot an administrator in that channel first")
            await db.update_config("request_channel_id", ch_val)
            await respond(
                f"✅ **Request Inbox Updated!**\n\n"
                f"Movie request tickets will now arrive in `{ch_val}`.",
                reply_markup=_BACK_BTN,
            )
        except ValueError:
            await respond(
                "❌ **Request Inbox Not Updated**\n\n"
                "Send a numeric channel ID and make the bot an administrator there.",
                reply_markup=_BACK_BTN,
            )
        except Exception as error:
            reference = report_internal_error(logger, "admin_set_request_channel", error)
            await respond(
                "❌ **Cannot access that channel!**\n"
                "Make sure the bot is an **Admin** there first.\n"
                f"Reference: `{reference}`",
                reply_markup=_BACK_BTN,
            )

    elif state == "req_fsub_add":
        raw = message.text.strip()
        try:
            verified = await resolve_request_fsub_channel(client, raw)
            ok, msg_r = await db.add_req_fsub_channel(
                verified.chat_id,
                verified.link,
                verified.title,
            )
            if not ok:
                raise ChannelConfigurationError(msg_r)
            await respond(
                "✅ <b>Timed Access Channel Added</b>\n\n"
                f"📢 <b>{_html(verified.title)}</b>\n"
                f"🆔 <code>{verified.chat_id}</code>\n"
                f"🔗 <code>{_html(verified.link)}</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=_BACK_BTN,
            )
        except ChannelConfigurationError as exc:
            logger.info("Timed access channel rejected: %s", exc)
            await respond(
                f"❌ <b>Timed Access Channel Not Added</b>\n\n{_html(str(exc))}",
                parse_mode=ParseMode.HTML,
                reply_markup=_BACK_BTN,
            )

    elif state == "req_fsub_remove":
        raw = message.text.strip()
        try:
            ch_val = await resolve_channel_id(client, raw)
            removed = await db.remove_req_fsub_channel(ch_val)
            if removed:
                await respond(f"✅ Timed access channel `{ch_val}` removed.", reply_markup=_BACK_BTN)
            else:
                await respond(
                    f"ℹ️ **Channel `{ch_val}` was not in Timed Access.**",
                    reply_markup=_BACK_BTN,
                )
        except ChannelConfigurationError as error:
            await respond(f"❌ **Channel not removed.**\n\n{str(error)}", reply_markup=_BACK_BTN)

    elif state == "req_fsub_interval":
        try:
            hours = int(message.text.strip())
            if hours < 1:
                raise ValueError
            await db.update_config("req_fsub_interval_hours", hours)
            await respond(
                f"✅ Interval set to `{hours}h`.\nUsers will see the prompt once every {hours} hour(s).",
                reply_markup=_BACK_BTN,
            )
        except ValueError:
            await respond("❌ Send a number like `24`.", reply_markup=_BACK_BTN)

    await restore_prompt(client, admin_id)
    raise StopPropagation


# ── EXPLICIT /cancel COMMAND ─────────────────────────────────────────────────
# Handles /cancel as a real command so it NEVER leaks to filter.py as a search query.
# This is registered with group=0 (highest priority) so it fires first.


@Client.on_message(filters.command("cancel") & filters.private & filters.user(ADMIN_ID), group=0)
async def cancel_cmd(client: Client, message: Message):
    restored = await restore_prompt(client, message.from_user.id, fallback_message=message)
    if not restored:
        await _clear_state(message.from_user.id)
    try:
        await message.delete()
    except Exception:
        pass
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
                "Invalid channel ID. Use a numeric ID like -1001234567890.", reply_parameters=None
            )
        await db.clear_index_progress(chat_id)
        await message.reply_text(
            "Index progress cleared for that channel.\n\nIndexing will start from message 1 next time.",
            reply_parameters=None,
        )
    else:
        await db.clear_index_progress(chat_id=None)
        await message.reply_text(
            "All index progress cleared.\n\nIndexing will start from message 1 for all channels next time.",
            reply_parameters=None,
        )


@Client.on_message(filters.command("reset_db") & filters.private & filters.user(ADMIN_ID))
async def reset_db_cmd(client: Client, message: Message):
    await redis_state.set_json("admin-reset-confirm", message.from_user.id, {"ready": True}, 60)
    await message.reply_text(
        "⚠️ **WARNING: NUCLEAR OPTION** ⚠️\n\n"
        "Are you absolutely sure you want to completely wipe ALL files, users, and bans across all 5 clusters?\n\n"
        "To confirm, reply to this message with: `/confirm_reset`",
        reply_parameters=None,
    )


@Client.on_message(filters.command("confirm_reset") & filters.private & filters.user(ADMIN_ID))
async def confirm_reset_cmd(client: Client, message: Message):
    confirmation = await redis_state.get_json("admin-reset-confirm", message.from_user.id)
    await redis_state.delete("admin-reset-confirm", message.from_user.id)
    if not confirmation:
        return await message.reply_text("Reset confirmation is missing or expired. Run /reset_db first.")
    status = await message.reply_text("☢️ **Nuking the database...**")
    try:
        await db.reset_database()
    except Exception as error:
        reference = report_internal_error(logger, "admin_reset_database", error)
        return await status.edit_text(f"❌ **Reset aborted or incomplete.** Reference: `{reference}`")
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
        if isinstance(size, (int, float)) and size != float("inf"):
            fill = max(0, min(10, int((size / 450) * 10)))
            bar = "█" * fill + "░" * (10 - fill)
            text += f"├ Cluster {db_num}: [{bar}] `{size:.1f} MB`\n"
        else:
            text += f"├ Cluster {db_num}: [{('░' * 10)}] `Unavailable`\n"

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
        markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("🤖 Search in PM", url=f"https://t.me/{client.me.username}?start=start")]]
        )

    help_msg = await message.reply_text(
        help_text, reply_markup=markup, parse_mode=ParseMode.HTML, reply_parameters=None
    )

    # Persist group cleanup so it survives a restart without a sleeping task.
    if is_group:
        await db.schedule_deletion(help_msg.chat.id, help_msg.id, 30)
        await db.schedule_deletion(message.chat.id, message.id, 30)


# ── FSub REFRESH JOIN LINKS ──────────────────────────────────────────────────
# Re-resolves public links and creates fresh direct-join links for private
# chats. Existing valid links are not revoked as a side effect.


@Client.on_callback_query(filters.regex(r"^fsub_refresh_links$") & filters.user(ADMIN_ID))
async def fsub_refresh_links(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    await callback.message.edit_text("♻️ **Refreshing join channel links...**")

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

        try:
            chat = await client.get_chat(ch_id)
            username = getattr(chat, "username", None)
            if username:
                new_link = f"https://t.me/{username}"
            else:
                invite = await client.create_chat_invite_link(
                    int(chat.id),
                    creates_join_request=False,
                )
                new_link = invite.invite_link
            await db.update_fsub_channel_link(ch_id, new_link)
            refreshed += 1
        except Exception as error:
            report_internal_error(logger, "admin_refresh_invite", error, channel_id=ch_id)
            skipped += 1

    markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("‹ Subscription", callback_data="fsub_menu")]]
    )
    await callback.message.edit_text(
        f"♻️ **Links Refreshed!**\n\n"
        f"✅ Refreshed: `{refreshed}` join channel(s)\n"
        f"⏭ Skipped: `{skipped}` (invalid entries or errors)\n\n"
        f"New prompts will use the refreshed links. Existing valid links were not revoked.",
        reply_markup=markup,
    )


# ── CHANNEL HEALTH CHECK ──────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^channel_health_check$") & filters.user(ADMIN_ID))
async def channel_health_check(client: Client, callback: CallbackQuery):
    """Uses shared check_all_channels() from health_monitor — no duplicate logic."""
    await answer_callback_safely(callback)
    from plugins.health_monitor import check_all_channels

    await callback.message.edit_text("🔍 **Running channel health check...**")

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

    buttons.append(
        [InlineKeyboardButton("‹ Control Center", callback_data="back_to_admin")]
    )
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
    await answer_callback_safely(callback)
    from plugins.health_monitor import check_known_issues

    await callback.message.edit_text("🔍 **Checking for known issues...**")

    config = await db.get_config()
    findings = await check_known_issues(client, config)
    report_text = "⚠️ **Known Issues**\n\n" + "\n\n".join(f["text"] for f in findings)

    markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("‹ Control Center", callback_data="back_to_admin")]]
    )
    try:
        await callback.message.edit_text(report_text, reply_markup=markup)
    except Exception:
        await callback.message.edit_text(report_text[:4000] + "\n...", reply_markup=markup)


# ── CLOSE PANEL ───────────────────────────────────────────────────────────────
# Only one close_data handler exists — the duplicate in bulk_indexer.py is removed.

# ── MAINTENANCE MODE TOGGLE ──────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^admin_toggle_maintenance$") & filters.user(ADMIN_ID))
async def toggle_maintenance(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    config = await db.get_config()
    current = config.get("maintenance_mode", False)
    new_val = not current
    await db.update_config("maintenance_mode", new_val)
    status = (
        "🔧 **Maintenance Mode ON**\n\nUsers will see the maintenance message."
        if new_val
        else "✅ **Maintenance Mode OFF**\n\nBot is live again."
    )
    await answer_callback_safely(callback, f"{'ON' if new_val else 'OFF'}", show_alert=False)
    await callback.message.reply_text(status, reply_markup=_BACK_BTN)


# ── EXPORT CONFIG ─────────────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^admin_export_config$") & filters.user(ADMIN_ID))
async def export_config(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback, "📥 Preparing export...", show_alert=False)
    config_data = await db.export_config()
    config_json = json.dumps(config_data, indent=2, default=str)
    import io

    buf = io.BytesIO(config_json.encode())
    buf.name = "mccxbot_config_backup.json"
    await callback.message.reply_document(
        document=buf,
        caption="📥 **MCCxBot Config Backup**\n\nPrivate invite links are redacted. "
        "Use Restore Config to apply the remaining settings.",
    )


@Client.on_callback_query(filters.regex(r"^admin_export_secrets$") & filters.user(ADMIN_ID))
async def export_encrypted_config(client: Client, callback: CallbackQuery):
    passphrase = os.getenv("CONFIG_EXPORT_PASSPHRASE", "")
    if len(passphrase) < 16:
        await answer_callback_safely(
            callback,
            "Set CONFIG_EXPORT_PASSPHRASE (16+ characters) and restart first.",
            show_alert=True,
        )
        return
    await answer_callback_safely(callback, "Encrypting secret backup...", show_alert=False)
    config_data = await db.export_config(include_private_invites=True)
    encrypted = await asyncio.to_thread(encrypt_config_export, config_data, passphrase)
    import io

    buf = io.BytesIO(encrypted)
    buf.name = "mccxbot_config_secrets.enc.json"
    await callback.message.reply_document(
        document=buf,
        caption=(
            "🔒 **Encrypted MCCxBot Secret Backup**\n\n"
            "AES-256-GCM encrypted with CONFIG_EXPORT_PASSPHRASE. "
            "Keep both the file and passphrase protected."
        ),
    )


# ── RESTORE CONFIG ────────────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^admin_restore_config$") & filters.user(ADMIN_ID))
async def restore_config_prompt(client: Client, callback: CallbackQuery):
    await begin_prompt(
        callback,
        "restore_config_file",
        "📤 **Restore Config**\n\n"
        "Send me the `.json` backup file as a **document attachment**.\n\n"
        "⚠️ This will overwrite your current settings (except log channel, "
        "admin ID, and DB channels which are always protected).",
    )


@Client.on_message(filters.private & filters.document & filters.user(ADMIN_ID))
async def handle_config_restore_file(client: Client, message: Message):
    state = await _get_state(message.from_user.id)
    if state != "restore_config_file":
        # Not a config restore — let the message propagate to other handlers (e.g. forward_indexer)
        raise ContinuePropagation

    file_name = message.document.file_name or ""
    if not file_name.casefold().endswith(".json"):
        return await message.reply_text("❌ Please send a `.json` file.", reply_markup=_BACK_BTN)

    file_size = message.document.file_size
    if file_size is None or file_size > MAX_CONFIG_BACKUP_BYTES:
        return await message.reply_text(
            f"Backup must report a size no larger than {MAX_CONFIG_BACKUP_BYTES // 1024} KiB.",
            reply_markup=_BACK_BTN,
        )

    await _clear_state(message.from_user.id)
    file_bytes = None
    try:
        file_bytes = await client.download_media(message.document, in_memory=True)
        raw = file_bytes.getvalue()
        if len(raw) > MAX_CONFIG_BACKUP_BYTES:
            raise ValueError("downloaded backup exceeds the size limit")
        config_data = await asyncio.to_thread(_parse_config_backup, raw)
        if not config_data:
            raise ValueError("backup contains no restorable settings")

        current = await db.get_config()
        changes = {key: value for key, value in config_data.items() if current.get(key) != value}
        if not changes:
            return await message.reply_text(
                "No configuration changes were found in this backup.",
                reply_markup=_BACK_BTN,
            )

        await redis_state.set_json(
            "admin-restore-confirm",
            message.from_user.id,
            {"changes": changes},
            CONFIG_RESTORE_CONFIRM_SECONDS,
        )
        await message.reply_text(
            _format_restore_diff(current, changes),
            parse_mode=ParseMode.DISABLED,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("Apply restore", callback_data="admin_restore_apply"),
                        InlineKeyboardButton("Cancel", callback_data="admin_restore_cancel"),
                    ]
                ]
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        await message.reply_text(f"Invalid config backup: {exc}", reply_markup=_BACK_BTN)
    except Exception:
        logger.exception("Config restore preview failed")
        await message.reply_text(
            "Config restore preview failed. No settings were changed.",
            reply_markup=_BACK_BTN,
        )
    finally:
        if file_bytes is not None:
            file_bytes.close()


@Client.on_callback_query(filters.regex(r"^admin_restore_apply$") & filters.user(ADMIN_ID))
async def apply_config_restore(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    pending = await redis_state.get_json("admin-restore-confirm", callback.from_user.id)
    await redis_state.delete("admin-restore-confirm", callback.from_user.id)
    if not pending:
        await answer_callback_safely(callback, "Restore preview expired. Upload it again.", show_alert=True)
        return

    try:
        success = await db.restore_config(pending["changes"])
    except ValueError as exc:
        await answer_callback_safely(callback, f"Restore rejected: {exc}", show_alert=True)
        return
    if not success:
        await answer_callback_safely(callback, "No safe settings were restored.", show_alert=True)
        return
    await callback.message.edit_text(
        f"Config restored: {len(pending['changes'])} setting(s) changed.\nProtected fields were not changed.",
        reply_markup=_BACK_BTN,
    )
    await answer_callback_safely(callback, "Config restored")


@Client.on_callback_query(filters.regex(r"^admin_restore_cancel$") & filters.user(ADMIN_ID))
async def cancel_config_restore(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    await redis_state.delete("admin-restore-confirm", callback.from_user.id)
    await callback.message.edit_text("Config restore cancelled.", reply_markup=_BACK_BTN)
    await answer_callback_safely(callback)


# ── CLOSE PANEL ───────────────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^close_data$") & filters.user(ADMIN_ID))
async def close_callback(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    await callback.message.delete()
    await answer_callback_safely(callback)
