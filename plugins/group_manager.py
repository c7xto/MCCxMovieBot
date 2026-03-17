import os
import asyncio
import logging
from dotenv import load_dotenv
from pyrogram import Client, filters, ContinuePropagation, StopPropagation
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import ParseMode
from database.db import db
from plugins.state import get_state, set_state, clear_state

load_dotenv()

logger = logging.getLogger(__name__)
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

_BACK_BTN = InlineKeyboardMarkup([
    [InlineKeyboardButton("🔙 Back to Group Manager", callback_data="group_manager_menu")]
])


# ── GROUP MANAGER MENU ────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^group_manager_menu$") & filters.user(ADMIN_ID))
async def group_manager_menu(client: Client, callback: CallbackQuery):
    config = await db.get_config()
    whitelist_enabled = config.get("group_whitelist_enabled", False)
    whitelist_mode = config.get("group_whitelist_mode", "blacklist")  # "whitelist" or "blacklist"

    mode_label = "🔒 Whitelist Mode (only approved groups)" if whitelist_mode == "whitelist" \
        else "🔓 Blacklist Mode (all groups except banned)"
    toggle_label = "Switch to Blacklist Mode" if whitelist_mode == "whitelist" \
        else "Switch to Whitelist Mode"

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 List All Groups",        callback_data="gm_list")],
        [InlineKeyboardButton("📊 Top Groups by Activity", callback_data="admin_stats")],  # now in unified Analytics
        [InlineKeyboardButton("🔍 Find Group",             callback_data="gm_find")],
        [InlineKeyboardButton("🚫 Ban a Group",            callback_data="gm_ban_prompt")],
        [InlineKeyboardButton("✅ Unban a Group",           callback_data="gm_unban_prompt")],
        [InlineKeyboardButton("⚙️ Group Settings",         callback_data="gm_settings_prompt")],
        [InlineKeyboardButton("📢 Broadcast to Groups",    callback_data="gm_broadcast_prompt")],
        [InlineKeyboardButton(f"🔄 {toggle_label}",        callback_data="gm_toggle_mode")],
        [InlineKeyboardButton("🔙 Back to Admin Panel",    callback_data="back_to_admin")]
    ])

    text = (
        f"🏘 **Group Manager**\n\n"
        f"**Current Mode:** {mode_label}\n\n"
        f"Total connected groups: `{await db.get_group_count()}`"
    )
    try:
        await callback.message.edit_text(text, reply_markup=markup)
    except Exception:
        await callback.message.reply_text(text, reply_markup=markup)
    await callback.answer()


# ── G5: LIST & TOP GROUPS ─────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^gm_list$") & filters.user(ADMIN_ID))
async def gm_list_groups(client: Client, callback: CallbackQuery):
    await callback.message.edit_text("📋 **Fetching group list...**")
    await callback.answer()

    groups = await db.get_all_groups()
    if not groups:
        await callback.message.edit_text(
            "📋 No groups connected yet.", reply_markup=_BACK_BTN
        )
        return

    text = f"📋 **All Connected Groups** ({len(groups)} total)\n\n"
    for g in groups[:20]:
        status = "🚫" if g.get("banned") else ("✅" if g.get("whitelisted") else "⚪")
        count = g.get("search_count", 0)
        text += f"{status} `{g['_id']}` — {g.get('title','?')[:25]} ({count} searches)\n"

    if len(groups) > 20:
        text += f"\n_...and {len(groups) - 20} more_"

    await callback.message.edit_text(text, reply_markup=_BACK_BTN)


@Client.on_callback_query(filters.regex(r"^gm_top$") & filters.user(ADMIN_ID))
async def gm_top_groups(client: Client, callback: CallbackQuery):
    await callback.answer()
    top = await db.get_top_groups(limit=10)

    if not top:
        await callback.message.edit_text("📊 No group activity data yet.", reply_markup=_BACK_BTN)
        return

    text = "📊 **Top 10 Groups by Search Volume**\n\n"
    for i, g in enumerate(top, 1):
        count = g.get("search_count", 0)
        title = g.get("title", "Unknown")[:30]
        text += f"{i}. `{g['_id']}` — {title}\n   🔍 `{count}` searches\n"

    await callback.message.edit_text(text, reply_markup=_BACK_BTN)


# ── G3: BAN/UNBAN GROUP ───────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^gm_ban_prompt$") & filters.user(ADMIN_ID))
async def gm_ban_prompt(client: Client, callback: CallbackQuery):
    set_state(callback.from_user.id, "gm_ban")
    await callback.message.reply_text(
        "🚫 **Ban a Group**\n\n"
        "Send me the **Group ID** to ban.\n"
        "The bot will send a farewell message and leave the group.\n\n"
        "*Type /cancel to abort.*"
    )
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^gm_unban_prompt$") & filters.user(ADMIN_ID))
async def gm_unban_prompt(client: Client, callback: CallbackQuery):
    set_state(callback.from_user.id, "gm_unban")
    await callback.message.reply_text(
        "✅ **Unban a Group**\n\n"
        "Send me the **Group ID** to unban.\n\n"
        "*Type /cancel to abort.*"
    )
    await callback.answer()


# ── G2: PER-GROUP SETTINGS ────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^gm_settings_prompt$") & filters.user(ADMIN_ID))
async def gm_settings_prompt(client: Client, callback: CallbackQuery):
    """Shows all connected groups as inline buttons — no text input needed."""
    await callback.answer()
    groups = await db.get_all_groups()
    if not groups:
        await callback.message.edit_text(
            "⚙️ No groups connected yet.",
            reply_markup=_BACK_BTN
        )
        return

    buttons = []
    for g in groups[:20]:
        status = "🚫" if g.get("banned") else ("✅" if g.get("whitelisted") else "⚪")
        title = g.get("title", "Unknown")[:22]
        buttons.append([InlineKeyboardButton(
            f"{status} {title}",
            callback_data=f"gm_view_settings#{g['_id']}"
        )])

    if len(groups) > 20:
        buttons.append([InlineKeyboardButton(
            f"...and {len(groups)-20} more (use Find Group)",
            callback_data="gm_find"
        )])

    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="group_manager_menu")])

    await callback.message.edit_text(
        f"⚙️ **Select a group to configure:**\n\n"
        f"Total: `{len(groups)}` groups",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


@Client.on_callback_query(filters.regex(r"^gm_view_settings#") & filters.user(ADMIN_ID))
async def gm_view_settings(client: Client, callback: CallbackQuery):
    try:
        group_id = int(callback.data.split("#")[1])
    except (ValueError, IndexError):
        return await callback.answer("❌ Malformed callback.", show_alert=True)
    group = await db.get_group(group_id)
    if not group:
        await callback.answer("Group not found in DB.", show_alert=True)
        return

    settings = group.get("settings", {})
    auto_del = settings.get("auto_delete_time", "default")
    whitelisted = group.get("whitelisted", False)
    banned = group.get("banned", False)

    text = (
        f"⚙️ **Settings for:** {group.get('title', 'Unknown')}\n"
        f"🆔 `{group_id}`\n\n"
        f"🚫 Banned: `{banned}`\n"
        f"✅ Whitelisted: `{whitelisted}`\n"
        f"⏱ Auto-delete: `{auto_del}` seconds\n"
        f"🔍 Total searches: `{group.get('search_count', 0)}`"
    )

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Whitelist",  callback_data=f"gm_whitelist#{group_id}"),
         InlineKeyboardButton("🚫 Ban",        callback_data=f"gm_ban_confirm#{group_id}")],
        [InlineKeyboardButton("⏱ Set Auto-Delete", callback_data=f"gm_set_autodel#{group_id}")],
        [InlineKeyboardButton("🔙 Back", callback_data="group_manager_menu")]
    ])
    await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^gm_whitelist#") & filters.user(ADMIN_ID))
async def gm_whitelist_toggle(client: Client, callback: CallbackQuery):
    try:
        group_id = int(callback.data.split("#")[1])
    except (ValueError, IndexError):
        return await callback.answer("❌ Malformed callback.", show_alert=True)
    group = await db.get_group(group_id)
    if not group:
        await callback.answer("Group not found.", show_alert=True)
        return
    new_val = not group.get("whitelisted", False)
    await db.update_group(group_id, {"whitelisted": new_val})
    status = "✅ Whitelisted" if new_val else "⚪ Removed from whitelist"
    await callback.answer(f"{status}", show_alert=True)
    # Refresh settings view
    callback.data = f"gm_view_settings#{group_id}"
    await gm_view_settings(client, callback)


@Client.on_callback_query(filters.regex(r"^gm_ban_confirm#") & filters.user(ADMIN_ID))
async def gm_ban_confirm(client: Client, callback: CallbackQuery):
    try:
        group_id = int(callback.data.split("#")[1])
    except (ValueError, IndexError):
        return await callback.answer("❌ Malformed callback.", show_alert=True)
    await _ban_group(client, callback.message, group_id)
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^gm_set_autodel#") & filters.user(ADMIN_ID))
async def gm_set_autodel_prompt(client: Client, callback: CallbackQuery):
    try:
        group_id = int(callback.data.split("#")[1])
    except (ValueError, IndexError):
        return await callback.answer("❌ Malformed callback.", show_alert=True)
    set_state(callback.from_user.id, f"gm_autodel#{group_id}")
    await callback.message.reply_text(
        f"⏱ **Set Auto-Delete for group `{group_id}`**\n\n"
        f"Send the number of minutes (1–60).\n"
        f"Send `0` to use the global default.\n\n"
        f"*Type /cancel to abort.*"
    )
    await callback.answer()


# ── G1: WHITELIST MODE TOGGLE ─────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^gm_toggle_mode$") & filters.user(ADMIN_ID))
async def gm_toggle_mode(client: Client, callback: CallbackQuery):
    config = await db.get_config()
    current = config.get("group_whitelist_mode", "blacklist")
    new_mode = "whitelist" if current == "blacklist" else "blacklist"
    await db.update_config("group_whitelist_mode", new_mode)
    mode_label = "🔒 Whitelist Mode" if new_mode == "whitelist" else "🔓 Blacklist Mode"
    await callback.answer(f"Switched to {mode_label}", show_alert=True)
    # Refresh menu
    await group_manager_menu(client, callback)


# ── G4: BROADCAST TO GROUPS ───────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^gm_broadcast_prompt$") & filters.user(ADMIN_ID))
async def gm_broadcast_prompt(client: Client, callback: CallbackQuery):
    await callback.message.reply_text(
        "📢 **Broadcast to Groups**\n\n"
        "Reply to any message with `/broadcast -groups` to send it to all connected groups.\n\n"
        "Or use `/broadcast -users -groups` to send to both users and groups."
    )
    await callback.answer()


# ── FIND GROUP ────────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^gm_find$") & filters.user(ADMIN_ID))
async def gm_find_prompt(client: Client, callback: CallbackQuery):
    set_state(callback.from_user.id, "gm_find")
    await callback.message.reply_text(
        "🔍 **Find Group**\n\n"
        "Send me a **Group ID** or part of a **group name** to search.\n\n"
        "*Type /cancel to abort.*"
    )
    await callback.answer()


# ── SHARED BAN HELPER ─────────────────────────────────────────────────────────

async def _ban_group(client, reply_to_msg, group_id: int):
    """Bans a group: marks in DB, sends farewell, leaves."""
    await db.ban_group(group_id)
    try:
        await client.send_message(
            group_id,
            "⚠️ This bot has been removed from this group by the administrator."
        )
        await client.leave_chat(group_id)
    except Exception as e:
        logger.warning(f"Could not leave group {group_id}: {e}")

    await reply_to_msg.reply_text(
        f"✅ **Group `{group_id}` banned and left.**\n"
        f"Bot will not rejoin this group.",
        reply_markup=_BACK_BTN
    )


# ── INPUT HANDLER for group manager states ────────────────────────────────────

@Client.on_message(
    filters.private & filters.text & filters.user(ADMIN_ID) &
    ~filters.command(["start", "admin", "ban", "unban", "purge_cams",
                      "reset_db", "broadcast", "filesearch", "cancel"])
)
async def gm_input_handler(client: Client, message: Message):
    admin_id = message.from_user.id
    state = get_state(admin_id)

    if not state or not state.startswith("gm_"):
        raise ContinuePropagation

    if message.text.lower() in ("/cancel", "cancel"):
        clear_state(admin_id)
        await message.reply_text(
            "🚫 **Cancelled.**",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back to Group Manager", callback_data="group_manager_menu")]
            ])
        )
        raise StopPropagation

    if state == "gm_ban":
        clear_state(admin_id)
        try:
            group_id = int(message.text.strip())
            await _ban_group(client, message, group_id)
        except ValueError:
            await message.reply_text("❌ Invalid Group ID. Must be a number.", reply_markup=_BACK_BTN)

    elif state == "gm_unban":
        clear_state(admin_id)
        try:
            group_id = int(message.text.strip())
            await db.unban_group(group_id)
            await message.reply_text(
                f"✅ **Group `{group_id}` unbanned.**",
                reply_markup=_BACK_BTN
            )
        except ValueError:
            await message.reply_text("❌ Invalid Group ID.", reply_markup=_BACK_BTN)

    elif state == "gm_find":
        clear_state(admin_id)
        query = message.text.strip()
        groups = await db.get_all_groups()

        # Match by ID or partial title
        matched = []
        for g in groups:
            if query == str(g["_id"]) or query.lower() in g.get("title", "").lower():
                matched.append(g)

        if not matched:
            await message.reply_text(
                f"❌ No groups found matching `{query}`.",
                reply_markup=_BACK_BTN
            )
            return

        text = f"🔍 **Found {len(matched)} group(s):**\n\n"
        for g in matched[:10]:
            status = "🚫" if g.get("banned") else ("✅" if g.get("whitelisted") else "⚪")
            text += f"{status} `{g['_id']}` — {g.get('title', '?')[:30]}\n"

        await message.reply_text(text, reply_markup=_BACK_BTN)

    elif state.startswith("gm_autodel#"):
        try:
            group_id = int(state.split("#")[1])
        except (ValueError, IndexError):
            clear_state(admin_id)
            await message.reply_text("❌ Session error. Please try again.", reply_markup=_BACK_BTN)
            raise StopPropagation
        clear_state(admin_id)
        try:
            minutes = int(message.text.strip())
            if minutes == 0:
                # Remove per-group override — use global default
                group = await db.get_group(group_id)
                settings = group.get("settings", {}) if group else {}
                settings.pop("auto_delete_time", None)
                await db.update_group(group_id, {"settings": settings})
                await message.reply_text(
                    f"✅ Group `{group_id}` will now use the **global auto-delete setting**.",
                    reply_markup=_BACK_BTN
                )
            elif 1 <= minutes <= 60:
                group = await db.get_group(group_id)
                settings = group.get("settings", {}) if group else {}
                settings["auto_delete_time"] = minutes * 60
                await db.update_group(group_id, {"settings": settings})
                await message.reply_text(
                    f"✅ Auto-delete for group `{group_id}` set to **{minutes} minute(s)**.",
                    reply_markup=_BACK_BTN
                )
            else:
                await message.reply_text(
                    "❌ Value must be 0–60 minutes.", reply_markup=_BACK_BTN
                )
        except ValueError:
            await message.reply_text("❌ Send a plain number like `5`.", reply_markup=_BACK_BTN)

    else:
        raise ContinuePropagation
    raise StopPropagation
