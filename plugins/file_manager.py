import os
import logging
import time
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import ParseMode
from database.db import db
from plugins.state import get_state, get_state_context, set_state, clear_state
from plugins.task_supervisor import TaskConflict, supervisor
from plugins.callbacks import answer_callback_safely
from plugins.workload import background_turn
from plugins.ui_helpers import (
    begin_prompt,
    cancel_button,
    delete_prompt_input,
    finish_prompt,
    restore_prompt,
)
from utils import ADMIN_ID, _html, report_internal_error

load_dotenv()

logger = logging.getLogger(__name__)

_BACK_BTN = InlineKeyboardMarkup(
    [[InlineKeyboardButton("‹ File Manager", callback_data="file_manager_menu")]]
)

# In-memory cache for duplicate scan results, keyed by admin user_id so
# concurrent admin sessions don't overwrite each other's scan results.
_cached_dupes = {}


# ── FILE MANAGER MENU ─────────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^file_manager_menu$") & filters.user(ADMIN_ID))
async def file_manager_menu(client: Client, callback: CallbackQuery):
    clear_state(callback.from_user.id)
    await answer_callback_safely(callback)
    total_files = await db.get_total_files()
    markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔎 Find Files", callback_data="fm_search"),
                InlineKeyboardButton("🧬 Duplicates", callback_data="fm_duplicates"),
            ],
            [
                InlineKeyboardButton("🧹 Pattern Cleanup", callback_data="fm_bulkdelete"),
                InlineKeyboardButton("⚡ CAM Cleanup", callback_data="fm_quickpurgecam"),
            ],
            [InlineKeyboardButton("📋 Missing Requests", callback_data="fm_missing")],
            [InlineKeyboardButton("‹ Control Center", callback_data="back_to_admin")],
        ]
    )
    text = (
        "🗂 **File Manager**\n\n"
        f"📚 `{total_files:,}` indexed files\n"
        "Search, repair and clean your library safely."
    )
    try:
        await callback.message.edit_text(text, reply_markup=markup)
    except Exception:
        await callback.message.reply_text(text, reply_markup=markup)


# ── SEARCH & DELETE ───────────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^fm_search$") & filters.user(ADMIN_ID))
async def fm_search_prompt(client: Client, callback: CallbackQuery):
    await begin_prompt(
        callback,
        "fm_search",
        "🔍 **File Search**\n\nSend the movie or file name to search.",
    )


@Client.on_message(filters.command("filesearch") & filters.private & filters.user(ADMIN_ID))
async def filesearch_cmd(client: Client, message: Message):
    if len(message.command) < 2:
        return await message.reply_text("⚠️ **Usage:** `/filesearch [query]`")
    query = message.text.split(" ", 1)[1]
    await _do_file_search(client, message, query)


async def _do_file_search(client, message_obj, query):
    status = await message_obj.reply_text(
        f"🔍 Searching for <code>{_html(query)}</code>...",
        parse_mode=ParseMode.HTML,
    )
    results = await db.admin_search_files(query, limit=15)

    if not results:
        await status.edit_text(
            f"❌ No files found for <code>{_html(query)}</code>.",
            parse_mode=ParseMode.HTML,
            reply_markup=_BACK_BTN,
        )
        return

    await status.delete()

    for file_doc in results:
        size_mb = file_doc.get("file_size", 0) / (1024 * 1024)
        size_str = f"{size_mb:.1f} MB"
        cluster = file_doc.get("_cluster", "?")
        name = file_doc.get("file_name", "Unknown")
        obj_id = str(file_doc["_id"])

        markup = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("🗑 Delete", callback_data=f"fm_del#{obj_id}"),
                    InlineKeyboardButton("✏ Edit Search Name", callback_data=f"fm_rename#{obj_id}"),
                ]
            ]
        )

        await message_obj.reply_text(
            f"📄 <code>{_html(name)}</code>\n"
            f"💿 Size: <code>{size_str}</code>  •  Cluster: <code>{cluster}</code>\n"
            f"🆔 <code>{obj_id}</code>",
            reply_markup=markup,
            parse_mode=ParseMode.HTML,
        )


@Client.on_callback_query(filters.regex(r"^fm_del#") & filters.user(ADMIN_ID))
async def fm_delete_file(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    obj_id = callback.data.split("#")[1]
    deleted = await db.delete_file_by_obj_id(obj_id)
    if deleted:
        await answer_callback_safely(callback, "✅ File deleted from database.", show_alert=True)
        await callback.message.delete()
    else:
        await answer_callback_safely(callback, "❌ File not found — may already be deleted.", show_alert=True)


# ── RENAME ────────────────────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^fm_rename#") & filters.user(ADMIN_ID))
async def fm_rename_prompt(client: Client, callback: CallbackQuery):
    obj_id = callback.data.split("#")[1]
    await begin_prompt(
        callback,
        f"fm_rename#{obj_id}",
        f"✏️ **Edit Search Name**\n\nFile ID: `{obj_id}`\n\n"
        "Send the new name used by search and result labels. This does not rename "
        "the Telegram file users download.",
    )


@Client.on_callback_query(filters.regex(r"^fm_editname$") & filters.user(ADMIN_ID))
async def fm_editname_prompt(client: Client, callback: CallbackQuery):
    await begin_prompt(
        callback,
        "fm_editname_id",
        "✏️ **Edit Search Name**\n\nSend the **File Object ID** from a file search result.",
    )


# ── FIND DUPLICATES ───────────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^fm_duplicates$") & filters.user(ADMIN_ID))
async def fm_duplicates(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback, "🔎 Starting safe scan…")
    await callback.message.edit_text(
        "🧬 **Duplicate Report • Starting**\n\nThis is a report-only scan. Nothing will be deleted."
    )

    # Run in background so it doesn't timeout
    try:
        supervisor.spawn(
            _run_duplicate_scan(client, callback.message, callback.from_user.id),
            key="maintenance:duplicate-scan",
            owner=f"admin:{callback.from_user.id}",
            resources=("movie-catalog",),
            drain_on_shutdown=True,
        )
    except TaskConflict as exc:
        await callback.message.edit_text(f"⚠️ **Scan not started:** `{exc}`")


@Client.on_callback_query(filters.regex(r"^fm_dupes_page#") & filters.user(ADMIN_ID))
async def fm_dupes_page(client: Client, callback: CallbackQuery):
    """Handles pagination for the duplicate files list."""
    try:
        page = int(callback.data.split("#")[1])
    except (ValueError, IndexError):
        return await answer_callback_safely(callback, "❌ Malformed callback.", show_alert=True)
    await answer_callback_safely(callback)
    report = _cached_dupes.get(callback.from_user.id)
    if not report:
        await callback.message.edit_text(
            "⚠️ Scan results expired. Please re-run duplicate scan.", reply_markup=_BACK_BTN
        )
        return
    await _show_dupes_page(callback, report, page=page)


@Client.on_callback_query(filters.regex(r"^fm_dupe_review#") & filters.user(ADMIN_ID))
async def fm_review_duplicate_group(client: Client, callback: CallbackQuery):
    await answer_callback_safely(
        callback,
        "Yellow matches are never deleted because metadata alone is not proof.",
        show_alert=True,
    )


@Client.on_callback_query(filters.regex(r"^fm_dupe_delete#") & filters.user(ADMIN_ID))
async def fm_delete_duplicate_group(client: Client, callback: CallbackQuery):
    await answer_callback_safely(
        callback,
        "Use verified exact cleanup. Yellow matches remain protected.",
        show_alert=True,
    )


@Client.on_callback_query(filters.regex(r"^fm_dupes_cleanup$") & filters.user(ADMIN_ID))
async def fm_verified_cleanup_prompt(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    report = _cached_dupes.get(callback.from_user.id)
    if not report:
        await callback.message.edit_text(
            "⚠️ Scan results expired. Run the duplicate report again.",
            reply_markup=_BACK_BTN,
        )
        return
    exact_extras = int(report["summary"].get("exact_extras", 0))
    if exact_extras <= 0:
        await callback.message.edit_text(
            "✅ No Telegram-verified exact copies were found.",
            reply_markup=_BACK_BTN,
        )
        return
    await callback.message.edit_text(
        "🧹 **Verified Exact Cleanup**\n\n"
        f"Telegram proved `{exact_extras:,}` rows point to media already stored "
        "elsewhere in your database.\n\n"
        "✅ One database row will be kept for every Telegram file.\n"
        "✅ Only extra MongoDB rows will be removed.\n"
        "✅ Telegram channel files and messages will not be touched.\n"
        "🔒 Yellow metadata-only matches will not be touched.\n\n"
        "Continue with the strict cleanup?",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"✅ Remove {exact_extras:,} exact copies",
                        callback_data="fm_dupes_cleanup_confirm",
                    )
                ],
                [InlineKeyboardButton("‹ Cancel", callback_data="file_manager_menu")],
            ]
        ),
    )


@Client.on_callback_query(filters.regex(r"^fm_dupes_cleanup_confirm$") & filters.user(ADMIN_ID))
async def fm_verified_cleanup_confirm(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback, "🧹 Starting verified cleanup…")
    status = await callback.message.edit_text(
        "🧹 **Verified Exact Cleanup • Starting**\n\n"
        "Preparing the survivor list. Yellow matches remain protected."
    )
    try:
        supervisor.spawn(
            _run_verified_cleanup(status, callback.from_user.id),
            key="maintenance:verified-duplicate-cleanup",
            owner=f"admin:{callback.from_user.id}",
            resources=("movie-catalog",),
            drain_on_shutdown=True,
        )
    except TaskConflict as exc:
        await status.edit_text(f"⚠️ **Cleanup not started:** `{exc}`")


async def _run_verified_cleanup(status_msg, admin_id):
    last_update = 0.0
    latest = {"scanned": 0, "deleted": 0, "total": 0}

    async def show_progress(progress):
        nonlocal last_update, latest
        latest = progress
        now = time.monotonic()
        if now - last_update < 3.0:
            return
        last_update = now
        scanned = int(progress.get("scanned", 0))
        total = max(scanned, int(progress.get("total", 0)))
        deleted = int(progress.get("deleted", 0))
        elapsed = max(0.1, float(progress.get("elapsed", 0.1)))
        speed = scanned / elapsed
        remaining = max(0, total - scanned)
        eta = int(remaining / speed) if speed > 0 else 0
        percent = min(100.0, scanned * 100 / max(1, total))
        filled = min(10, int(percent / 10))
        bar = "▰" * filled + "▱" * (10 - filled)
        eta_text = "Calculating…" if speed <= 0 else f"{eta // 60}m {eta % 60}s"
        await status_msg.edit_text(
            "🧹 **Verified Exact Cleanup • Running**\n\n"
            f"`{bar}`  **{percent:.1f}%**\n"
            f"📂 Cluster `{progress.get('cluster', 1)}/"
            f"{progress.get('clusters', 1)}`\n"
            f"🔎 Checked  `{scanned:,} / {total:,}`\n"
            f"🗑 Removed  `{deleted:,}` verified copies\n"
            f"⚡ Speed    `{speed:,.0f} files/s`\n"
            f"⌛ ETA      `{eta_text}`\n\n"
            "🔒 One copy is kept • yellow matches are untouched"
        )

    try:
        result = await db.delete_verified_duplicates(show_progress)
        _cached_dupes.pop(admin_id, None)
        await status_msg.edit_text(
            "✅ **Verified Exact Cleanup Complete**\n\n"
            f"🗑 Removed: `{result['deleted']:,}` exact copies\n"
            f"📚 Remaining: `{result['remaining']:,}` files\n\n"
            "🔒 Metadata-only possible matches were not touched.\n"
            "📢 Telegram channel messages were not touched.",
            reply_markup=_BACK_BTN,
        )
    except Exception as error:
        reference = report_internal_error(logger, "verified_duplicate_cleanup", error)
        await status_msg.edit_text(
            "❌ **Verified Cleanup Stopped Safely**\n\n"
            f"Checked: `{int(latest.get('scanned', 0)):,}`\n"
            f"Removed before stop: `{int(latest.get('deleted', 0)):,}`\n"
            "A verified survivor was always kept. Run the report again to resume.\n\n"
            f"Reference: `{reference}`",
            reply_markup=_BACK_BTN,
        )


async def _run_duplicate_scan(client, status_msg, admin_id):
    last_update = 0.0

    async def show_progress(progress):
        nonlocal last_update
        # The scan yields every bounded database batch while interactive
        # searches or callbacks are active. UI edits remain throttled below,
        # but the workload gate is consulted on every progress callback.
        await background_turn("duplicate_scan")
        now = time.monotonic()
        if now - last_update < 3.0 and int(progress.get("scanned", 0)) > 0:
            return
        last_update = now
        scanned = int(progress.get("scanned", 0))
        total = max(scanned, int(progress.get("total", 0)))
        overall_scanned = int(progress.get("overall_scanned", scanned))
        overall_total = max(overall_scanned, int(progress.get("overall_total", total)))
        elapsed = max(0.1, float(progress.get("elapsed", 0.1)))
        phase_elapsed = max(0.1, float(progress.get("phase_elapsed", elapsed)))
        speed = scanned / phase_elapsed
        overall_speed = overall_scanned / elapsed
        remaining = max(0, overall_total - overall_scanned)
        eta = int(remaining / overall_speed) if overall_speed > 0 else 0
        phase_key = progress.get("phase", "exact")
        phase_names = {
            "exact": ("Checking exact copies", 1),
            "probable": ("Checking possible matches", 2),
            "labels": ("Preparing clean file names", 3),
        }
        phase, phase_number = phase_names.get(phase_key, ("Scanning safely", 1))
        percent = min(100.0, overall_scanned * 100 / max(1, overall_total))
        filled = min(10, int(percent / 10))
        progress_bar = "▰" * filled + "▱" * (10 - filled)
        if not remaining:
            eta_text = "Finishing…"
        elif overall_speed <= 0:
            eta_text = "Calculating…"
        else:
            eta_text = f"About {eta // 60}m {eta % 60}s"
        await status_msg.edit_text(
            f"🧬 **Duplicate Report • {phase}**\n\n"
            f"`{progress_bar}`  **{percent:.1f}%**\n"
            f"📂 Phase `{phase_number}/3` • Cluster "
            f"`{progress.get('cluster', 1)}/{progress.get('clusters', 1)}`\n"
            f"📨 This phase  `{scanned:,} / {total:,}`\n"
            f"⚡ Live speed  `{speed:,.0f} files/s`\n"
            f"⌛ Time left   `{eta_text}`\n\n"
            f"🔒 Report only • no files will be deleted"
        )

    try:
        report = await db.scan_duplicate_report(show_progress)
        dupes = report["groups"]
        summary = report["summary"]

        if not summary["exact_groups"] and not summary["probable_groups"]:
            await status_msg.edit_text(
                f"✅ **Duplicate Report Complete**\n\n"
                f"📨 Scanned: `{summary['scanned']:,}` files\n"
                f"🧬 No exact or probable duplicates found.\n\n"
                f"🔒 No files were changed.",
                reply_markup=_BACK_BTN,
            )
            return

        _cached_dupes[admin_id] = report
        await _show_dupes_page(status_msg, report, page=0)

    except RuntimeError as error:
        public_runtime_errors = (
            "host storage is full",
            "complete duplicate report cannot start",
        )
        if any(marker in str(error).casefold() for marker in public_runtime_errors):
            await status_msg.edit_text(
                f"❌ **Duplicate Report Paused**\n\n{error}\n\n🔒 Nothing was deleted or changed.",
                reply_markup=_BACK_BTN,
            )
            return
        reference = report_internal_error(logger, "duplicate_scan", error)
        await status_msg.edit_text(
            f"❌ Duplicate scan failed. Reference: `{reference}`",
            reply_markup=_BACK_BTN,
        )
    except Exception as error:
        reference = report_internal_error(logger, "duplicate_scan", error)
        await status_msg.edit_text(
            f"❌ Duplicate scan failed. Reference: `{reference}`", reply_markup=_BACK_BTN
        )


async def _show_dupes_page(msg_or_callback, report, page=0):
    """Show a paginated, report-only duplicate summary."""
    dupes = report["groups"]
    summary = report["summary"]
    per_page = 8
    total = len(dupes)
    start = page * per_page
    end = min(start + per_page, total)
    page_dupes = dupes[start:end]

    total_pages = max(1, (total + per_page - 1) // per_page)
    text = (
        f"🧬 **Duplicate Report • Page {page + 1}/{total_pages}**\n\n"
        f"📨 Scanned: `{summary['scanned']:,}` files\n"
        f"🟢 Confirmed exact copies: `{summary['exact_groups']:,}` groups • "
        f"`{summary['exact_extras']:,}` extra copies\n"
        f"🟡 Review only, not duplicates: `{summary['probable_groups']:,}` groups • "
        f"`{summary['probable_matches']:,}` matches\n\n"
        f"🔒 **Nothing was deleted during this report.**\n"
        f"Only green Telegram-verified copies can be cleaned.\n"
        f"Large yellow counts are expected; they are similar labels, not "
        f"confirmed copies, and remain protected.\n\n"
    )

    buttons = []
    for dupe in page_dupes:
        name = dupe["name"][:35]
        count = dupe["count"]
        dtype = dupe.get("type", "exact")
        type_icon = "🟢" if dtype == "exact" else "🟡"
        size_mb = dupe.get("size", 0) / (1024 * 1024)
        size_str = f"{size_mb:.0f}MB" if size_mb > 0 else ""
        label = "copies" if dtype == "exact" else "possible matches"
        text += f"{type_icon} `{name[:30]}` — {count} {label}  {size_str}\n"
        if dupe.get("truncated"):
            text += "  ℹ️ Only part of this large group is shown.\n"

    # Pagination
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"fm_dupes_page#{page - 1}"))
    if end < total:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"fm_dupes_page#{page + 1}"))
    if nav:
        buttons.append(nav)

    if int(summary.get("exact_extras", 0)) > 0:
        buttons.append(
            [
                InlineKeyboardButton(
                    f"🧹 Remove {summary['exact_extras']:,} verified copies",
                    callback_data="fm_dupes_cleanup",
                )
            ]
        )

    buttons.append(
        [InlineKeyboardButton("‹ File Manager", callback_data="file_manager_menu")]
    )

    try:
        if hasattr(msg_or_callback, "edit_text"):
            await msg_or_callback.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
        else:
            await msg_or_callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
    except Exception:
        pass


@Client.on_callback_query(filters.regex(r"^fm_delete_all_dupes$") & filters.user(ADMIN_ID))
async def fm_review_all_duplicates(client: Client, callback: CallbackQuery):
    await answer_callback_safely(
        callback,
        "Deletion is disabled in report-only mode. No files were changed.",
        show_alert=True,
    )


@Client.on_callback_query(filters.regex(r"^fm_delete_all_dupes_confirm$") & filters.user(ADMIN_ID))
async def fm_delete_all_dupes(client: Client, callback: CallbackQuery):
    await answer_callback_safely(
        callback,
        "Deletion is disabled in report-only mode. No files were changed.",
        show_alert=True,
    )


# ── BULK DELETE BY PATTERN ────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^fm_quickpurgecam$") & filters.user(ADMIN_ID))
async def fm_quickpurge_cam(client: Client, callback: CallbackQuery):
    """One-tap CAM/PreDVD purge — replaces the old /purge_cams command."""
    pattern = r"cam|predvd|hdcam|tsrip|1xbet"
    await answer_callback_safely(callback)
    count = await db.count_by_pattern(pattern)
    if count == 0:
        await callback.message.edit_text(
            "✅ No CAM/PreDVD files found. Database is clean.", reply_markup=_BACK_BTN
        )
        return
    await callback.message.edit_text(
        f"⚡ **Quick Purge CAMs/PreDVD**\n\n"
        f"Found: **{count:,}** low-quality files\n"
        f"Pattern: `CAM | PreDVD | HDCAM | TSRip | 1xBet`\n\n"
        f"Confirm delete?",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"🗑 Delete {count:,} files", callback_data=f"fm_bulkconfirm#{pattern}"
                    ),
                    InlineKeyboardButton("❌ Cancel", callback_data="file_manager_menu"),
                ]
            ]
        ),
    )


@Client.on_callback_query(filters.regex(r"^fm_bulkdelete$") & filters.user(ADMIN_ID))
async def fm_bulkdelete_prompt(client: Client, callback: CallbackQuery):
    await begin_prompt(
        callback,
        "fm_bulkdelete",
        "🗑 **Bulk Delete by Pattern**\n\n"
        "Send me a keyword or pattern to match file names.\n\n"
        "**Examples:**\n"
        "`HDCAM` — deletes all files with HDCAM in name\n"
        "`480p Tamil` — deletes all 480p Tamil files\n\n"
        "⚠️ You will see a **preview count** before anything is deleted.",
    )


@Client.on_callback_query(filters.regex(r"^fm_bulkconfirm#") & filters.user(ADMIN_ID))
async def fm_bulk_confirm(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    pattern = callback.data.split("#")[1]
    status = await callback.message.edit_text(f"🗑 Deleting files matching `{pattern}`...")
    await answer_callback_safely(callback)

    deleted = await db.purge_by_pattern(pattern)
    await status.edit_text(
        f"✅ **Bulk Delete Complete!**\n\n🗑 Deleted: `{deleted}` files matching `{pattern}`",
        reply_markup=_BACK_BTN,
    )


# ── CLUSTER MIGRATION ─────────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^fm_migrate$") & filters.user(ADMIN_ID))
async def fm_migrate_prompt(client: Client, callback: CallbackQuery):
    await begin_prompt(
        callback,
        "fm_migrate",
        "📦 **Cluster Migration**\n\n"
        "Send me the source and destination cluster numbers on one line.\n\n"
        "**Format:** `from_cluster to_cluster`\n"
        "**Example:** `1 2` (moves all files from Cluster 1 → Cluster 2)\n\n"
        "⚠️ Destination cluster must have enough free space.",
    )


@Client.on_callback_query(filters.regex(r"^fm_migrate_confirm#") & filters.user(ADMIN_ID))
async def fm_migrate_confirm(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    parts = callback.data.split("#")
    from_idx = int(parts[1]) - 1
    to_idx = int(parts[2]) - 1
    status = await callback.message.edit_text(
        f"📦 **Migrating Cluster {from_idx + 1} → Cluster {to_idx + 1}...**\n\n"
        "This runs in the background. You will be notified when it finishes."
    )
    await answer_callback_safely(callback)
    try:
        supervisor.spawn(
            _run_migration(client, status, from_idx, to_idx),
            key=f"maintenance:migration:{from_idx}:{to_idx}",
            owner=f"admin:{callback.from_user.id}",
            resources=("movie-catalog", f"cluster:{from_idx}", f"cluster:{to_idx}"),
            drain_on_shutdown=True,
        )
    except TaskConflict as exc:
        await status.edit_text(f"⚠️ **Migration not started:** `{exc}`")


async def _run_migration(client, status_msg, from_idx, to_idx):
    try:
        migrated, skipped = await db.migrate_cluster(from_idx, to_idx)
        if skipped == -1:
            await status_msg.edit_text(
                f"❌ **Migration Failed!**\n\nCluster {to_idx + 1} is full (>450MB). "
                f"Choose a cluster with more free space.",
                reply_markup=_BACK_BTN,
            )
        else:
            await status_msg.edit_text(
                f"✅ **Migration Complete!**\n\n"
                f"📦 Moved: `{migrated}` files\n"
                f"⚠️ Skipped: `{skipped}` (duplicates or errors)\n\n"
                f"Cluster {from_idx + 1} → Cluster {to_idx + 1}",
                reply_markup=_BACK_BTN,
            )
    except Exception as error:
        reference = report_internal_error(logger, "file_migration", error)
        await status_msg.edit_text(f"❌ Migration error. Reference: `{reference}`", reply_markup=_BACK_BTN)


# ── FILES BY LANGUAGE ─────────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^fm_bylang$") & filters.user(ADMIN_ID))
async def fm_by_language(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    await callback.message.edit_text("📊 **Counting files by language...**")
    await answer_callback_safely(callback)

    try:
        lang_counts = await db.get_files_by_language()
        total = sum(lang_counts.values())

        lang_emojis = {
            "Malayalam": "🌴",
            "Tamil": "🎭",
            "Telugu": "⭐",
            "Hindi": "🇮🇳",
            "English": "🌍",
            "Kannada": "🏵",
            "Dual Audio": "🎧",
            "Multi Audio": "🎵",
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
    except Exception as error:
        reference = report_internal_error(logger, "language_counts", error)
        await callback.message.edit_text(
            f"❌ Could not count languages. Reference: `{reference}`",
            reply_markup=_BACK_BTN,
        )


# ── TOP MISSING FILES ─────────────────────────────────────────────────────────


@Client.on_callback_query(filters.regex(r"^fm_missing$") & filters.user(ADMIN_ID))
async def fm_missing_files(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    await callback.message.edit_text("📋 **Fetching top missing searches...**")
    await answer_callback_safely(callback)

    try:
        missed = await db.get_top_missed(limit=15)
        if not missed:
            await callback.message.edit_text(
                "📋 **Top Missing Files**\n\nNo missed searches recorded yet.", reply_markup=_BACK_BTN
            )
            return

        buttons = []
        text = "📋 <b>Top Missing Files</b> <i>(most searched, not in DB)</i>\n\n"
        for i, entry in enumerate(missed, 1):
            title = entry.get("original", entry["_id"])
            text += f"{i}. <code>{_html(title)}</code> — <b>{entry['count']}x</b>\n"
            buttons.append(
                [
                    InlineKeyboardButton(
                        f"✅ Mark Fulfilled — {title[:20]}", callback_data=f"fm_clear_missed#{entry['_id']}"
                    )
                ]
            )

        buttons.append(
            [InlineKeyboardButton("‹ File Manager", callback_data="file_manager_menu")]
        )
        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML,
        )
    except Exception as error:
        reference = report_internal_error(logger, "missing_files", error)
        await callback.message.edit_text(
            f"❌ Could not load missing files. Reference: `{reference}`",
            reply_markup=_BACK_BTN,
        )


@Client.on_callback_query(filters.regex(r"^fm_clear_missed#") & filters.user(ADMIN_ID))
async def fm_clear_missed(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    query_id = callback.data.split("#")[1]
    await db.clear_missed_search(query_id)
    await answer_callback_safely(callback, "✅ Removed from missing list.", show_alert=False)
    # Refresh the list
    await fm_missing_files(client, callback)


# ── INPUT HANDLER for file manager states ────────────────────────────────────
# This catches admin text input for fm_* states.
# It is a separate handler from admin.py's catch_admin_input — uses ContinuePropagation
# so if state doesn't match here it passes through to admin.py.

from pyrogram import ContinuePropagation, StopPropagation


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
    # matching catch_admin_input handler for the full explanation.
)
async def fm_input_handler(client: Client, message: Message):
    admin_id = message.from_user.id
    state = get_state(admin_id)

    if not state or not state.startswith("fm_"):
        raise ContinuePropagation

    if message.text.lower() in ("/cancel", "cancel"):
        await restore_prompt(client, admin_id, fallback_message=message)
        await delete_prompt_input(message)
        raise StopPropagation

    # fm_search
    if state == "fm_search":
        await restore_prompt(client, admin_id, fallback_message=message)
        await _do_file_search(client, message, message.text.strip())

    # fm_bulkdelete — show dry-run preview first
    elif state == "fm_bulkdelete":
        pattern = message.text.strip()
        count = await db.count_by_pattern(pattern)
        if count == 0:
            await finish_prompt(
                client,
                admin_id,
                f"ℹ️ No files match `{pattern}`. Nothing to delete.",
                back_callback="file_manager_menu",
                back_label="‹ File Manager",
                fallback_message=message,
            )
            raise StopPropagation
        await finish_prompt(
            client,
            admin_id,
            f"⚠️ **Dry Run Preview**\n\n"
            f"Pattern: `{pattern}`\n"
            f"Files that would be deleted: **{count:,}**\n\n"
            f"Confirm?",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            f"🗑 Delete {count:,} files", callback_data=f"fm_bulkconfirm#{pattern}"
                        ),
                        InlineKeyboardButton("❌ Cancel", callback_data="file_manager_menu"),
                    ]
                ]
            ),
            back_callback="file_manager_menu",
            back_label="‹ File Manager",
            fallback_message=message,
        )

    # fm_migrate — parse "from to" format
    elif state == "fm_migrate":
        parts = message.text.strip().split()
        if len(parts) != 2 or not all(p.isdigit() for p in parts):
            await finish_prompt(
                client,
                admin_id,
                "❌ Invalid format. Send two numbers like `1 2`.",
                back_callback="file_manager_menu",
                back_label="‹ File Manager",
                fallback_message=message,
            )
            raise StopPropagation
        from_c, to_c = int(parts[0]), int(parts[1])
        total_clusters = len(db.file_cols)
        if not (1 <= from_c <= total_clusters) or not (1 <= to_c <= total_clusters):
            await finish_prompt(
                client,
                admin_id,
                f"❌ Cluster numbers must be between 1 and {total_clusters}.",
                back_callback="file_manager_menu",
                back_label="‹ File Manager",
                fallback_message=message,
            )
            raise StopPropagation
        if from_c == to_c:
            await finish_prompt(
                client,
                admin_id,
                "❌ Source and destination cannot be the same.",
                back_callback="file_manager_menu",
                back_label="‹ File Manager",
                fallback_message=message,
            )
            raise StopPropagation
        # Check destination has space
        dest_size = await db.get_db_size(db.dbs[to_c - 1])
        src_count = await db.file_cols[from_c - 1].count_documents({})
        await finish_prompt(
            client,
            admin_id,
            f"📦 **Migration Preview**\n\n"
            f"From: Cluster {from_c} (`{src_count:,}` files)\n"
            f"To: Cluster {to_c} (`{dest_size:.1f} MB` used / 512 MB)\n\n"
            f"Confirm?",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "✅ Start Migration", callback_data=f"fm_migrate_confirm#{from_c}#{to_c}"
                        ),
                        InlineKeyboardButton("❌ Cancel", callback_data="file_manager_menu"),
                    ]
                ]
            ),
            back_callback="file_manager_menu",
            back_label="‹ File Manager",
            fallback_message=message,
        )

    # fm_editname_id — first step: admin sent the file object ID
    elif state == "fm_editname_id":
        obj_id = message.text.strip()
        # Validate it looks like a MongoDB ObjectId (24 hex chars)
        if len(obj_id) != 24 or not all(c in "0123456789abcdefABCDEF" for c in obj_id):
            await message.reply_text(
                "❌ That doesn't look like a valid File ID. "
                "Get the ID from a `/filesearch` result.\n\n"
                "Try again or use the Cancel button."
            )
            return  # keep state alive
        set_state(admin_id, f"fm_rename#{obj_id}")
        context = get_state_context(admin_id)
        if context and context.get("prompt_chat_id") and context.get("prompt_message_id"):
            await client.edit_message_text(
                context["prompt_chat_id"],
                context["prompt_message_id"],
                f"✏️ **Edit Search Name**\n\nFile ID: `{obj_id}`\n\n"
                "Send the new name used by search and result labels. The Telegram filename is unchanged.",
                reply_markup=cancel_button(),
            )

    # fm_rename#<obj_id> — second step: admin sent new name
    elif state.startswith("fm_rename#"):
        obj_id = state.split("#")[1]
        new_name = message.text.strip()
        success = await db.update_file_name(obj_id, new_name)
        if success:
            await finish_prompt(
                client,
                admin_id,
                f"✅ **Search Name Updated**\n\nNew searchable name: `{new_name}`",
                back_callback="file_manager_menu",
                back_label="‹ File Manager",
                fallback_message=message,
            )
        else:
            await finish_prompt(
                client,
                admin_id,
                "❌ File not found. It may have been deleted already.",
                back_callback="file_manager_menu",
                back_label="‹ File Manager",
                fallback_message=message,
            )

    else:
        raise ContinuePropagation
    raise StopPropagation
