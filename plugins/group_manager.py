import os
import asyncio
import logging
from dotenv import load_dotenv
from pyrogram import Client, filters, ContinuePropagation, StopPropagation
from pyrogram.types import Message, InlineKeyboardButton, CallbackQuery
from plugins.mobile_ui import MobileInlineKeyboardMarkup as InlineKeyboardMarkup
from pyrogram.enums import ParseMode
from database.db import db
from plugins.state import get_state, set_state, clear_state
from plugins.callbacks import answer_callback_safely
from plugins.ui_helpers import begin_prompt, delete_prompt_input, finish_prompt, restore_prompt
from utils import ADMIN_ID, _html

load_dotenv()

logger = logging.getLogger(__name__)

_BACK_BTN = InlineKeyboardMarkup(
    [[InlineKeyboardButton("‹ Group Manager", callback_data="group_manager_menu")]]
)


# ── GROUP MANAGER MENU ────────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^group_manager_menu$") & filters.user(ADMIN_ID))
async def group_manager_menu(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    await clear_state(callback.from_user.id)
    config = await db.get_config()
    whitelist_mode = config.get("group_whitelist_mode", "blacklist")  # "whitelist" or "blacklist"

    mode_label = (
        "🔒 Whitelist Mode (only approved groups)"
        if whitelist_mode == "whitelist"
        else "🔓 Blacklist Mode (all groups except banned)"
    )
    toggle_label = "Switch to Blacklist Mode" if whitelist_mode == "whitelist" else "Switch to Whitelist Mode"

    markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("⚙ Manage Groups", callback_data="gm_settings_prompt"),
                InlineKeyboardButton("🔎 Find", callback_data="gm_find"),
            ],
            [InlineKeyboardButton(f"🔄 {toggle_label}", callback_data="gm_toggle_mode")],
            [InlineKeyboardButton("‹ Control Center", callback_data="back_to_admin")],
        ]
    )

    text = f"🏘 **Group Manager**\n\n{mode_label}\nConnected groups: `{await db.get_group_count():,}`"
    try:
        await callback.message.edit_text(text, reply_markup=markup)
    except Exception:
        await callback.message.reply_text(text, reply_markup=markup)


# ── LIST & TOP GROUPS ─────────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^gm_list$") & filters.user(ADMIN_ID))
async def gm_list_groups(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    await callback.message.edit_text("📋 **Fetching group list...**")

    groups = await db.get_all_groups()
    if not groups:
        await callback.message.edit_text("📋 No groups connected yet.", reply_markup=_BACK_BTN)
        return

    text = f"📋 **All Connected Groups** ({len(groups)} total)\n\n"
    for g in groups[:20]:
        status = "🚫" if g.get("banned") else ("✅" if g.get("whitelisted") else "⚪")
        count = g.get("search_count", 0)
        text += f"{status} `{g['_id']}` — {g.get('title', '?')[:25]} ({count} searches)\n"

    if len(groups) > 20:
        text += f"\n_...and {len(groups) - 20} more_"

    await callback.message.edit_text(text, reply_markup=_BACK_BTN)


# ── BAN/UNBAN GROUP ───────────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^gm_ban_prompt$") & filters.user(ADMIN_ID))
async def gm_ban_prompt(client: Client, callback: CallbackQuery):
    await begin_prompt(
        callback,
        "gm_ban",
        "🚫 **Ban a Group**\n\n"
        "Send me the **Group ID** to ban.\n"
        "The bot will send a farewell message and leave the group.",
    )


@Client.on_callback_query(filters.regex(r"^gm_unban_prompt$") & filters.user(ADMIN_ID))
async def gm_unban_prompt(client: Client, callback: CallbackQuery):
    await begin_prompt(callback, "gm_unban", "✅ **Unban a Group**\n\nSend me the **Group ID** to unban.")


# ── PER-GROUP SETTINGS ────────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^gm_settings_prompt$") & filters.user(ADMIN_ID))
async def gm_settings_prompt(client: Client, callback: CallbackQuery):
    """Shows all connected groups as inline buttons — no text input needed."""
    await answer_callback_safely(callback)
    groups = await db.get_all_groups()
    if not groups:
        await callback.message.edit_text("⚙️ No groups connected yet.", reply_markup=_BACK_BTN)
        return

    buttons = []
    for g in groups[:20]:
        status = "🚫" if g.get("banned") else ("✅" if g.get("whitelisted") else "⚪")
        title = g.get("title", "Unknown")[:22]
        buttons.append(
            [InlineKeyboardButton(f"{status} {title}", callback_data=f"gm_view_settings#{g['_id']}")]
        )

    if len(groups) > 20:
        buttons.append(
            [
                InlineKeyboardButton(
                    f"...and {len(groups) - 20} more (use Find Group)", callback_data="gm_find"
                )
            ]
        )

    buttons.append(
        [InlineKeyboardButton("‹ Group Manager", callback_data="group_manager_menu")]
    )

    await callback.message.edit_text(
        f"⚙️ **Select a group to configure:**\n\nTotal: `{len(groups)}` groups",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


@Client.on_callback_query(filters.regex(r"^gm_view_settings#") & filters.user(ADMIN_ID))
async def gm_view_settings(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    try:
        group_id = int(callback.data.split("#")[1])
    except (ValueError, IndexError):
        return await answer_callback_safely(callback, "❌ Malformed callback.", show_alert=True)
    group = await db.get_group(group_id)
    if not group:
        await answer_callback_safely(callback, "Group not found in DB.", show_alert=True)
        return

    settings = group.get("settings", {})
    auto_del = settings.get("auto_delete_time")
    auto_del_label = f"{max(1, int(auto_del) // 60)} min override" if auto_del else "Global default"
    whitelisted = group.get("whitelisted", False)
    banned = group.get("banned", False)

    text = (
        f"⚙️ **Settings for:** {group.get('title', 'Unknown')}\n"
        f"🆔 `{group_id}`\n\n"
        f"🚫 Banned: `{banned}`\n"
        f"✅ Whitelisted: `{whitelisted}`\n"
        f"⏱ Auto-delete: `{auto_del_label}`\n"
        f"🔍 Total searches: `{group.get('search_count', 0)}`"
    )

    access_button = (
        InlineKeyboardButton("✅ Unban", callback_data=f"gm_unban_confirm#{group_id}")
        if banned
        else InlineKeyboardButton("🚫 Ban", callback_data=f"gm_ban_confirm#{group_id}")
    )
    markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⚪ Remove Whitelist" if whitelisted else "✅ Whitelist",
                    callback_data=f"gm_whitelist#{group_id}",
                ),
                access_button,
            ],
            [
                InlineKeyboardButton(
                    "⏱ Auto-Delete Override", callback_data=f"gm_set_autodel#{group_id}"
                )
            ],
            [InlineKeyboardButton("‹ Group Manager", callback_data="group_manager_menu")],
        ]
    )
    await callback.message.edit_text(text, reply_markup=markup)


@Client.on_callback_query(filters.regex(r"^gm_whitelist#") & filters.user(ADMIN_ID))
async def gm_whitelist_toggle(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback, "Updating group access…")
    try:
        group_id = int(callback.data.split("#")[1])
    except (ValueError, IndexError):
        return await answer_callback_safely(callback, "❌ Malformed callback.", show_alert=True)
    group = await db.get_group(group_id)
    if not group:
        await answer_callback_safely(callback, "Group not found.", show_alert=True)
        return
    new_val = not group.get("whitelisted", False)
    await db.update_group(group_id, {"whitelisted": new_val})
    # Refresh settings view
    callback.data = f"gm_view_settings#{group_id}"
    await gm_view_settings(client, callback)


@Client.on_callback_query(filters.regex(r"^gm_ban_confirm#") & filters.user(ADMIN_ID))
async def gm_ban_confirm(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback, "Updating group access…")
    try:
        group_id = int(callback.data.split("#")[1])
    except (ValueError, IndexError):
        return await answer_callback_safely(callback, "❌ Malformed callback.", show_alert=True)
    await _ban_group(client, callback.message, group_id)


@Client.on_callback_query(filters.regex(r"^gm_unban_confirm#") & filters.user(ADMIN_ID))
async def gm_unban_confirm(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback, "Updating group access…")
    try:
        group_id = int(callback.data.split("#")[1])
    except (ValueError, IndexError):
        return await answer_callback_safely(callback, "❌ Malformed callback.", show_alert=True)
    await db.unban_group(group_id)
    callback.data = f"gm_view_settings#{group_id}"
    await gm_view_settings(client, callback)


@Client.on_callback_query(filters.regex(r"^gm_set_autodel#") & filters.user(ADMIN_ID))
async def gm_set_autodel_prompt(client: Client, callback: CallbackQuery):
    try:
        group_id = int(callback.data.split("#")[1])
    except (ValueError, IndexError):
        return await answer_callback_safely(callback, "❌ Malformed callback.", show_alert=True)
    await begin_prompt(
        callback,
        f"gm_autodel#{group_id}",
        f"⏱ **Auto-Delete Override for group `{group_id}`**\n\n"
        f"Send the number of minutes (1–60).\n"
        f"Send `0` to use the global default.",
    )


# ── WHITELIST MODE TOGGLE ─────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^gm_toggle_mode$") & filters.user(ADMIN_ID))
async def gm_toggle_mode(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    config = await db.get_config()
    current = config.get("group_whitelist_mode", "blacklist")
    new_mode = "whitelist" if current == "blacklist" else "blacklist"
    await db.update_config("group_whitelist_mode", new_mode)
    mode_label = "🔒 Whitelist Mode" if new_mode == "whitelist" else "🔓 Blacklist Mode"
    await answer_callback_safely(callback, f"Switched to {mode_label}", show_alert=True)
    # Refresh menu
    await group_manager_menu(client, callback)


# ── BROADCAST TO GROUPS ───────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^gm_broadcast_prompt$") & filters.user(ADMIN_ID))
async def gm_broadcast_prompt(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    await callback.message.edit_text(
        "📢 **Broadcast to Groups**\n\n"
        "Reply to any message with `/broadcast -groups` to send it to all connected groups.\n\n"
        "Or use `/broadcast -users -groups` to send to both users and groups.",
        reply_markup=_BACK_BTN,
    )


# ── FIND GROUP ────────────────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^gm_find$") & filters.user(ADMIN_ID))
async def gm_find_prompt(client: Client, callback: CallbackQuery):
    await begin_prompt(
        callback, "gm_find", "🔍 **Find Group**\n\nSend a **Group ID** or part of a **group name**."
    )


# ── SHARED BAN HELPER ─────────────────────────────────────────────────────────


async def _ban_group(client, reply_to_msg, group_id: int):
    """Bans a group: marks in DB, sends farewell, leaves."""
    await db.ban_group(group_id)
    try:
        await client.send_message(
            group_id, "⚠️ This bot has been removed from this group by the administrator."
        )
        await client.leave_chat(group_id)
    except Exception as e:
        logger.warning(f"Could not leave group {group_id}: {e}")

    text = f"✅ **Group `{group_id}` banned and left.**\nBot will not rejoin this group."
    if reply_to_msg is not None:
        await reply_to_msg.reply_text(text, reply_markup=_BACK_BTN)
    return text


# ── INPUT HANDLER for group manager states ────────────────────────────────────


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
            "broadcast",
            "broadcast_status",
            "filesearch",
            "cancel",
        ]
    ),
    group=-1,  # must win the race against filter.py's auto_filter — see admin.py's
    # matching catch_admin_input handler for the full explanation. Without
    # this, auto_filter (default group 0, registered earlier since
    # group_manager.py loads after filter.py alphabetically) always
    # consumes plain admin text first, so this handler never fires.
)
async def gm_input_handler(client: Client, message: Message):
    admin_id = message.from_user.id
    state = await get_state(admin_id)

    if not state or not state.startswith("gm_"):
        raise ContinuePropagation

    async def respond(text, *, parse_mode=None):
        await finish_prompt(
            client,
            admin_id,
            text,
            back_callback="group_manager_menu",
            back_label="‹ Group Manager",
            fallback_message=message,
            parse_mode=parse_mode,
        )

    if message.text.lower() in ("/cancel", "cancel"):
        await restore_prompt(client, admin_id, fallback_message=message)
        await delete_prompt_input(message)
        raise StopPropagation

    if state == "gm_ban":
        try:
            group_id = int(message.text.strip())
            await respond(await _ban_group(client, None, group_id))
        except ValueError:
            await respond("❌ Invalid Group ID. Must be a number.")

    elif state == "gm_unban":
        try:
            group_id = int(message.text.strip())
            await db.unban_group(group_id)
            await respond(f"✅ **Group `{group_id}` unbanned.**")
        except ValueError:
            await respond("❌ Invalid Group ID.")

    elif state == "gm_find":
        query = message.text.strip()
        groups = await db.get_all_groups()

        # Match by ID or partial title
        matched = []
        for g in groups:
            if query == str(g["_id"]) or query.lower() in g.get("title", "").lower():
                matched.append(g)

        if not matched:
            await respond(
                f"❌ No groups found matching <code>{_html(query)}</code>.",
                parse_mode=ParseMode.HTML,
            )
            raise StopPropagation

        text = f"🔍 <b>Found {len(matched)} group(s):</b>\n\n"
        for g in matched[:10]:
            status = "🚫" if g.get("banned") else ("✅" if g.get("whitelisted") else "⚪")
            text += f"{status} <code>{g['_id']}</code> — {_html(g.get('title', '?')[:30])}\n"

        await respond(text, parse_mode=ParseMode.HTML)

    elif state.startswith("gm_autodel#"):
        try:
            group_id = int(state.split("#")[1])
        except (ValueError, IndexError):
            await respond("❌ Session error. Please try again.")
            raise StopPropagation
        try:
            minutes = int(message.text.strip())
            if minutes == 0:
                # Remove per-group override — use global default
                group = await db.get_group(group_id)
                settings = group.get("settings", {}) if group else {}
                settings.pop("auto_delete_time", None)
                await db.update_group(group_id, {"settings": settings})
                await respond(f"✅ Group `{group_id}` will now use the **global auto-delete setting**.")
            elif 1 <= minutes <= 60:
                group = await db.get_group(group_id)
                settings = group.get("settings", {}) if group else {}
                settings["auto_delete_time"] = minutes * 60
                await db.update_group(group_id, {"settings": settings})
                await respond(f"✅ Auto-delete for group `{group_id}` set to **{minutes} minute(s)**.")
            else:
                await respond("❌ Value must be 0–60 minutes.")
        except ValueError:
            await respond("❌ Send a plain number like `5`.")

    else:
        raise ContinuePropagation
    raise StopPropagation
