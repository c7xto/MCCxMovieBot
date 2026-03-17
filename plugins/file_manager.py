import os
import asyncio
import logging
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import ParseMode
from database.db import db
from plugins.state import get_state, set_state, clear_state

load_dotenv()

logger = logging.getLogger(__name__)
ADMIN_ID = [int(x.strip()) for x in os.getenv("ADMIN_ID", "0").split(",") if x.strip()]

_BACK_BTN = InlineKeyboardMarkup([
    [InlineKeyboardButton("🔙 Back to File Manager", callback_data="file_manager_menu")]
])

# In-memory cache for duplicate scan results (TTL not needed — re-scan resets it)
_cached_dupes = []


# ── FILE MANAGER MENU ─────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^file_manager_menu$") & filters.user(ADMIN_ID))
async def file_manager_menu(client: Client, callback: CallbackQuery):
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 Search & Delete Files",   callback_data="fm_search")],
        [InlineKeyboardButton("🔄 Find Duplicates",         callback_data="fm_duplicates")],
        [InlineKeyboardButton("🗑 Bulk Delete by Pattern",  callback_data="fm_bulkdelete")],
        [InlineKeyboardButton("⚡ Quick Purge CAMs/PreDVD",  callback_data="fm_quickpurgecam")],
        [InlineKeyboardButton("✏️ Edit File Name",          callback_data="fm_editname")],
        [InlineKeyboardButton("📦 Migrate Cluster",         callback_data="fm_migrate")],
        [InlineKeyboardButton("📊 Files by Language",       callback_data="admin_stats")],  # now in unified Analytics
        [InlineKeyboardButton("📋 Top Missing Files",       callback_data="fm_missing")],
        [InlineKeyboardButton("🔙 Back to Admin Panel",     callback_data="back_to_admin")]
    ])
    text = (
        "📁 **File Manager**\n\n"
        "Select an operation below."
    )
    try:
        await callback.message.edit_text(text, reply_markup=markup)
    except Exception:
        await callback.message.reply_text(text, reply_markup=markup)
    await callback.answer()


# ── F1: SEARCH & DELETE ───────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^fm_search$") & filters.user(ADMIN_ID))
async def fm_search_prompt(client: Client, callback: CallbackQuery):
    set_state(callback.from_user.id, "fm_search")
    await callback.message.reply_text(
        "🔍 **File Search**\n\nSend me the movie/file name to search.\n*Type /cancel to abort.*"
    )
    await callback.answer()


@Client.on_message(filters.command("filesearch") & filters.private & filters.user(ADMIN_ID))
async def filesearch_cmd(client: Client, message: Message):
    if len(message.command) < 2:
        return await message.reply_text("⚠️ **Usage:** `/filesearch [query]`")
    query = message.text.split(" ", 1)[1]
    await _do_file_search(client, message, query)


async def _do_file_search(client, message_obj, query):
    status = await message_obj.reply_text(f"🔍 Searching for `{query}`...")
    results = await db.admin_search_files(query, limit=15)

    if not results:
        await status.edit_text(
            f"❌ No files found for `{query}`.",
            reply_markup=_BACK_BTN
        )
        return

    await status.delete()

    for file_doc in results:
        size_mb = file_doc.get("file_size", 0) / (1024 * 1024)
        size_str = f"{size_mb:.1f} MB"
        cluster = file_doc.get("_cluster", "?")
        name = file_doc.get("file_name", "Unknown")
        obj_id = str(file_doc["_id"])

        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑 Delete This File", callback_data=f"fm_del#{obj_id}"),
             InlineKeyboardButton("✏️ Rename", callback_data=f"fm_rename#{obj_id}")]
        ])

        await message_obj.reply_text(
            f"📄 `{name}`\n"
            f"💿 Size: `{size_str}`  •  Cluster: `{cluster}`\n"
            f"🆔 `{obj_id}`",
            reply_markup=markup,
            parse_mode=ParseMode.MARKDOWN
        )


@Client.on_callback_query(filters.regex(r"^fm_del#") & filters.user(ADMIN_ID))
async def fm_delete_file(client: Client, callback: CallbackQuery):
    obj_id = callback.data.split("#")[1]
    deleted = await db.delete_file_by_obj_id(obj_id)
    if deleted:
        await callback.answer("✅ File deleted from database.", show_alert=True)
        await callback.message.delete()
    else:
        await callback.answer("❌ File not found — may already be deleted.", show_alert=True)


# ── F6: RENAME ────────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^fm_rename#") & filters.user(ADMIN_ID))
async def fm_rename_prompt(client: Client, callback: CallbackQuery):
    obj_id = callback.data.split("#")[1]
    set_state(callback.from_user.id, f"fm_rename#{obj_id}")
    await callback.message.reply_text(
        f"✏️ **Rename File**\n\n"
        f"File ID: `{obj_id}`\n\n"
        f"Send me the new file name.\n*Type /cancel to abort.*"
    )
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^fm_editname$") & filters.user(ADMIN_ID))
async def fm_editname_prompt(client: Client, callback: CallbackQuery):
    set_state(callback.from_user.id, "fm_editname_id")
    await callback.message.reply_text(
        "✏️ **Edit File Name**\n\n"
        "Send me the **File Object ID** (the long hex string from a file search result).\n"
        "*Type /cancel to abort.*"
    )
    await callback.answer()


# ── F3: FIND DUPLICATES ───────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^fm_duplicates$") & filters.user(ADMIN_ID))
async def fm_duplicates(client: Client, callback: CallbackQuery):
    await callback.message.edit_text(
        "🔄 **Scanning for duplicates...**\n\n"
        "_(This scans all clusters and may take a minute for large databases)_"
    )
    await callback.answer()

    # Run in background so it doesn't timeout
    asyncio.create_task(_run_duplicate_scan(client, callback.message))


@Client.on_callback_query(filters.regex(r"^fm_dupes_page#") & filters.user(ADMIN_ID))
async def fm_dupes_page(client: Client, callback: CallbackQuery):
    """Handles pagination for the duplicate files list."""
    page = int(callback.data.split("#")[1])
    await callback.answer()
    if not _cached_dupes:
        await callback.message.edit_text(
            "⚠️ Scan results expired. Please re-run duplicate scan.",
            reply_markup=_BACK_BTN
        )
        return
    await _show_dupes_page(callback, _cached_dupes, page=page)


@Client.on_callback_query(filters.regex(r"^fm_del_dupes#") & filters.user(ADMIN_ID))
async def fm_del_dupes(client: Client, callback: CallbackQuery):
    """Deletes all copies of a duplicate EXCEPT the first (oldest) one."""
    ids_raw = callback.data.split("#")[1]
    ids = [i for i in ids_raw.split(",") if i]
    if len(ids) <= 1:
        await callback.answer("Nothing to delete — only 1 copy.", show_alert=True)
        return
    # Keep first, delete the rest
    to_delete = ids[1:]
    deleted = 0
    for obj_id in to_delete:
        if await db.delete_file_by_obj_id(obj_id):
            deleted += 1
    await callback.answer(f"✅ Deleted {deleted} duplicate(s).", show_alert=True)
    # Remove from cached list
    for i, d in enumerate(_cached_dupes):
        if ids[0] in d.get("ids", []):
            _cached_dupes.pop(i)
            break
    # Refresh page
    if _cached_dupes:
        await _show_dupes_page(callback, _cached_dupes, page=0)
    else:
        await callback.message.edit_text(
            "✅ **All duplicates resolved!** Database is clean.",
            reply_markup=_BACK_BTN
        )


async def _run_duplicate_scan(client, status_msg):
    try:
        dupes = await db.find_duplicate_files()

        if not dupes:
            await status_msg.edit_text(
                "✅ **No duplicates found!** Your database is clean.",
                reply_markup=_BACK_BTN
            )
            return

        global _cached_dupes
        _cached_dupes = dupes
        await _show_dupes_page(status_msg, dupes, page=0)

    except Exception as e:
        await status_msg.edit_text(
            f"❌ Duplicate scan failed: `{e}`",
            reply_markup=_BACK_BTN
        )


async def _show_dupes_page(msg_or_callback, dupes, page=0):
    """Shows duplicate files as paginated inline buttons with delete-all option."""
    per_page = 8
    total = len(dupes)
    start = page * per_page
    end = min(start + per_page, total)
    page_dupes = dupes[start:end]

    text = f"🔄 **Duplicate Files: {total} groups**\n_(Page {page+1}/{(total+per_page-1)//per_page})_\n\n"

    buttons = []
    for dupe in page_dupes:
        name = dupe['name'][:35]
        count = dupe['count']
        ids_joined = ",".join(dupe['ids'][:10])  # max 10 IDs in callback
        text += f"• `{name}` — **{count} copies**\n"
        buttons.append([
            InlineKeyboardButton(
                f"🗑 Delete extras ({count-1}) — {name[:20]}",
                callback_data=f"fm_del_dupes#{ids_joined}"
            )
        ])

    # Pagination
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"fm_dupes_page#{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"fm_dupes_page#{page+1}"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton("🔙 Back to File Manager", callback_data="file_manager_menu")])

    try:
        if hasattr(msg_or_callback, 'edit_text'):
            await msg_or_callback.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
        else:
            await msg_or_callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
    except Exception:
        pass


# ── F10: BULK DELETE BY PATTERN ───────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^fm_quickpurgecam$") & filters.user(ADMIN_ID))
async def fm_quickpurge_cam(client: Client, callback: CallbackQuery):
    """One-tap CAM/PreDVD purge — replaces the old /purge_cams command."""
    pattern = r"cam|predvd|hdcam|tsrip|1xbet"
    await callback.answer()
    count = await db.count_by_pattern(pattern)
    if count == 0:
        await callback.message.edit_text(
            "✅ No CAM/PreDVD files found. Database is clean.",
            reply_markup=_BACK_BTN
        )
        return
    await callback.message.edit_text(
        f"⚡ **Quick Purge CAMs/PreDVD**\n\n"
        f"Found: **{count:,}** low-quality files\n"
        f"Pattern: `CAM | PreDVD | HDCAM | TSRip | 1xBet`\n\n"
        f"Confirm delete?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"🗑 Delete {count:,} files", callback_data=f"fm_bulkconfirm#{pattern}"),
             InlineKeyboardButton("❌ Cancel", callback_data="file_manager_menu")]
        ])
    )


@Client.on_callback_query(filters.regex(r"^fm_bulkdelete$") & filters.user(ADMIN_ID))
async def fm_bulkdelete_prompt(client: Client, callback: CallbackQuery):
    set_state(callback.from_user.id, "fm_bulkdelete")
    await callback.message.reply_text(
        "🗑 **Bulk Delete by Pattern**\n\n"
        "Send me a keyword or pattern to match file names.\n\n"
        "**Examples:**\n"
        "`HDCAM` — deletes all files with HDCAM in name\n"
        "`480p Tamil` — deletes all 480p Tamil files\n\n"
        "⚠️ You will see a **preview count** before anything is deleted.\n"
        "*Type /cancel to abort.*"
    )
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^fm_bulkconfirm#") & filters.user(ADMIN_ID))
async def fm_bulk_confirm(client: Client, callback: CallbackQuery):
    pattern = callback.data.split("#")[1]
    status = await callback.message.edit_text(f"🗑 Deleting files matching `{pattern}`...")
    await callback.answer()

    deleted = await db.purge_by_pattern(pattern)
    await status.edit_text(
        f"✅ **Bulk Delete Complete!**\n\n"
        f"🗑 Deleted: `{deleted}` files matching `{pattern}`",
        reply_markup=_BACK_BTN
    )


# ── F7: CLUSTER MIGRATION ─────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^fm_migrate$") & filters.user(ADMIN_ID))
async def fm_migrate_prompt(client: Client, callback: CallbackQuery):
    set_state(callback.from_user.id, "fm_migrate")
    await callback.message.reply_text(
        "📦 **Cluster Migration**\n\n"
        "Send me the source and destination cluster numbers on one line.\n\n"
        "**Format:** `from_cluster to_cluster`\n"
        "**Example:** `1 2` (moves all files from Cluster 1 → Cluster 2)\n\n"
        "⚠️ Destination cluster must have enough free space.\n"
        "*Type /cancel to abort.*"
    )
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^fm_migrate_confirm#") & filters.user(ADMIN_ID))
async def fm_migrate_confirm(client: Client, callback: CallbackQuery):
    parts = callback.data.split("#")
    from_idx = int(parts[1]) - 1
    to_idx = int(parts[2]) - 1
    status = await callback.message.edit_text(
        f"📦 **Migrating Cluster {from_idx+1} → Cluster {to_idx+1}...**\n\n"
        f"_(This runs in background — you'll be notified when done)_"
    )
    await callback.answer()
    asyncio.create_task(_run_migration(client, status, from_idx, to_idx))


async def _run_migration(client, status_msg, from_idx, to_idx):
    try:
        migrated, skipped = await db.migrate_cluster(from_idx, to_idx)
        if skipped == -1:
            await status_msg.edit_text(
                f"❌ **Migration Failed!**\n\nCluster {to_idx+1} is full (>450MB). "
                f"Choose a cluster with more free space.",
                reply_markup=_BACK_BTN
            )
        else:
            await status_msg.edit_text(
                f"✅ **Migration Complete!**\n\n"
                f"📦 Moved: `{migrated}` files\n"
                f"⚠️ Skipped: `{skipped}` (duplicates or errors)\n\n"
                f"Cluster {from_idx+1} → Cluster {to_idx+1}",
                reply_markup=_BACK_BTN
            )
    except Exception as e:
        await status_msg.edit_text(f"❌ Migration error: `{e}`", reply_markup=_BACK_BTN)


# ── F4: FILES BY LANGUAGE ─────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^fm_bylang$") & filters.user(ADMIN_ID))
async def fm_by_language(client: Client, callback: CallbackQuery):
    await callback.message.edit_text("📊 **Counting files by language...**")
    await callback.answer()

    try:
        lang_counts = await db.get_files_by_language()
        total = sum(lang_counts.values())

        lang_emojis = {
            "Malayalam": "🌴", "Tamil": "🎭", "Telugu": "⭐",
            "Hindi": "🇮🇳", "English": "🌍", "Kannada": "🏵",
            "Dual Audio": "🎧", "Multi Audio": "🎵"
        }

        text = "📊 **Files by Language**\n\n"
        for lang, count in sorted(lang_counts.items(), key=lambda x: x[1], reverse=True):
            if count == 0:
                continue
            emoji = lang_emojis.get(lang, "🔊")
            pct = (count / total * 100) if total > 0 else 0
            bar_filled = int(pct / 10)
            bar = "█" * bar_filled + "░" * (10 - bar_filled)
            text += f"{emoji} {lang:<12} [{bar}] `{count:,}`\n"

        text += f"\n📁 **Total Tagged:** `{total:,}`"

        await callback.message.edit_text(text, reply_markup=_BACK_BTN)
    except Exception as e:
        await callback.message.edit_text(f"❌ Error: `{e}`", reply_markup=_BACK_BTN)


# ── F9: TOP MISSING FILES ─────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^fm_missing$") & filters.user(ADMIN_ID))
async def fm_missing_files(client: Client, callback: CallbackQuery):
    await callback.message.edit_text("📋 **Fetching top missing searches...**")
    await callback.answer()

    try:
        missed = await db.get_top_missed(limit=15)
        if not missed:
            await callback.message.edit_text(
                "📋 **Top Missing Files**\n\nNo missed searches recorded yet.",
                reply_markup=_BACK_BTN
            )
            return

        buttons = []
        text = "📋 **Top Missing Files** _(most searched, not in DB)_\n\n"
        for i, entry in enumerate(missed, 1):
            text += f"{i}. `{entry.get('original', entry['_id'])}` — **{entry['count']}x**\n"
            buttons.append([
                InlineKeyboardButton(
                    f"✅ Mark Fulfilled — {entry.get('original', entry['_id'])[:20]}",
                    callback_data=f"fm_clear_missed#{entry['_id']}"
                )
            ])

        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="file_manager_menu")])
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        await callback.message.edit_text(f"❌ Error: `{e}`", reply_markup=_BACK_BTN)


@Client.on_callback_query(filters.regex(r"^fm_clear_missed#") & filters.user(ADMIN_ID))
async def fm_clear_missed(client: Client, callback: CallbackQuery):
    query_id = callback.data.split("#")[1]
    await db.clear_missed_search(query_id)
    await callback.answer("✅ Removed from missing list.", show_alert=False)
    # Refresh the list
    await fm_missing_files(client, callback)


# ── INPUT HANDLER for file manager states ────────────────────────────────────
# This catches admin text input for fm_* states.
# It is a separate handler from admin.py's catch_admin_input — uses ContinuePropagation
# so if state doesn't match here it passes through to admin.py.

from pyrogram import ContinuePropagation, StopPropagation

@Client.on_message(
    filters.private & filters.text & filters.user(ADMIN_ID) &
    ~filters.command(["start", "admin", "ban", "unban", "purge_cams", "reset_db",
                      "broadcast", "filesearch", "cancel"])
)
async def fm_input_handler(client: Client, message: Message):
    admin_id = message.from_user.id
    state = get_state(admin_id)

    if not state or not state.startswith("fm_"):
        raise ContinuePropagation

    if message.text.lower() in ("/cancel", "cancel"):
        clear_state(admin_id)
        await message.reply_text(
            "🚫 **Cancelled.**",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back to File Manager", callback_data="file_manager_menu")]
            ])
        )
        raise StopPropagation

    # fm_search
    if state == "fm_search":
        clear_state(admin_id)
        await _do_file_search(client, message, message.text.strip())

    # fm_bulkdelete — show dry-run preview first
    elif state == "fm_bulkdelete":
        clear_state(admin_id)
        pattern = message.text.strip()
        count = await db.count_by_pattern(pattern)
        if count == 0:
            await message.reply_text(
                f"ℹ️ No files match `{pattern}`. Nothing to delete.",
                reply_markup=_BACK_BTN
            )
            return
        await message.reply_text(
            f"⚠️ **Dry Run Preview**\n\n"
            f"Pattern: `{pattern}`\n"
            f"Files that would be deleted: **{count:,}**\n\n"
            f"Confirm?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"🗑 Delete {count:,} files", callback_data=f"fm_bulkconfirm#{pattern}"),
                 InlineKeyboardButton("❌ Cancel", callback_data="file_manager_menu")]
            ])
        )

    # fm_migrate — parse "from to" format
    elif state == "fm_migrate":
        clear_state(admin_id)
        parts = message.text.strip().split()
        if len(parts) != 2 or not all(p.isdigit() for p in parts):
            await message.reply_text(
                "❌ Invalid format. Send two numbers like `1 2`.",
                reply_markup=_BACK_BTN
            )
            return
        from_c, to_c = int(parts[0]), int(parts[1])
        total_clusters = len(db.file_cols)
        if not (1 <= from_c <= total_clusters) or not (1 <= to_c <= total_clusters):
            await message.reply_text(
                f"❌ Cluster numbers must be between 1 and {total_clusters}.",
                reply_markup=_BACK_BTN
            )
            return
        if from_c == to_c:
            await message.reply_text("❌ Source and destination cannot be the same.", reply_markup=_BACK_BTN)
            return
        # Check destination has space
        dest_size = await db.get_db_size(db.dbs[to_c - 1])
        src_count = await db.file_cols[from_c - 1].count_documents({})
        await message.reply_text(
            f"📦 **Migration Preview**\n\n"
            f"From: Cluster {from_c} (`{src_count:,}` files)\n"
            f"To: Cluster {to_c} (`{dest_size:.1f} MB` used / 512 MB)\n\n"
            f"Confirm?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Start Migration", callback_data=f"fm_migrate_confirm#{from_c}#{to_c}"),
                 InlineKeyboardButton("❌ Cancel", callback_data="file_manager_menu")]
            ])
        )

    # fm_editname_id — first step: admin sent the file object ID
    elif state == "fm_editname_id":
        obj_id = message.text.strip()
        # Validate it looks like a MongoDB ObjectId (24 hex chars)
        if len(obj_id) != 24 or not all(c in "0123456789abcdefABCDEF" for c in obj_id):
            await message.reply_text(
                "❌ That doesn't look like a valid File ID. "
                "Get the ID from a `/filesearch` result.\n\n"
                "*Try again or type /cancel.*"
            )
            return  # keep state alive
        set_state(admin_id, f"fm_rename#{obj_id}")
        await message.reply_text(
            f"✏️ File ID: `{obj_id}`\n\nNow send the **new file name**.\n*Type /cancel to abort.*"
        )

    # fm_rename#<obj_id> — second step: admin sent new name
    elif state.startswith("fm_rename#"):
        obj_id = state.split("#")[1]
        new_name = message.text.strip()
        clear_state(admin_id)
        success = await db.update_file_name(obj_id, new_name)
        if success:
            await message.reply_text(
                f"✅ **File Renamed!**\n\nNew name: `{new_name}`",
                reply_markup=_BACK_BTN
            )
        else:
            await message.reply_text(
                "❌ File not found. It may have been deleted already.",
                reply_markup=_BACK_BTN
            )

    else:
        raise ContinuePropagation
    raise StopPropagation
