"""
updater.py — GitHub auto-updater
Pulls latest code from GitHub and restarts the bot.
Only .env is never touched. Everything else gets updated.

Triggered by:
  • /update  command (admin only)
  • 🔄 Update Bot  button in /admin panel  (callback: upd_start)
"""

import os
import sys
import asyncio
import logging
import aiohttp
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode

load_dotenv()
logger = logging.getLogger(__name__)
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

GITHUB_REPO   = "c7xto/mccxmoviebot"
GITHUB_BRANCH = "main"


def _skip(path: str) -> bool:
    """Only .env is protected — everything else gets updated."""
    return path.replace("\\", "/").split("/")[-1] == ".env"


async def _get_tree(session: aiohttp.ClientSession) -> list:
    url = (
        f"https://api.github.com/repos/{GITHUB_REPO}/git/trees/{GITHUB_BRANCH}"
        "?recursive=1"
    )
    headers = {
        "Accept":     "application/vnd.github+json",
        "User-Agent": "MCCxMovieBot-Updater/1.0",
    }
    async with session.get(url, headers=headers,
                           timeout=aiohttp.ClientTimeout(total=30)) as r:
        if r.status == 403:
            raise RuntimeError("GitHub API rate-limited (403). Wait a minute and retry.")
        if r.status == 404:
            raise RuntimeError(
                f"Repo '{GITHUB_REPO}' / branch '{GITHUB_BRANCH}' not found."
            )
        if r.status != 200:
            raise RuntimeError(f"GitHub API {r.status}: {(await r.text())[:150]}")
        data = await r.json(content_type=None)

    return [item["path"] for item in data.get("tree", []) if item["type"] == "blob"]


async def _download(session: aiohttp.ClientSession, path: str) -> bytes:
    url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/{path}"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status}")
        return await r.read()


async def _do_update(client: Client, status: Message):
    # Step 1 — fetch file list
    try:
        await status.edit_text(
            "🔄 **Step 1/3** — Fetching file list from GitHub...",
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        pass

    try:
        async with aiohttp.ClientSession() as s:
            all_files = await _get_tree(s)
    except Exception as e:
        await status.edit_text(
            f"❌ **Failed — could not reach GitHub**\n\n`{e}`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    to_update = [f for f in all_files if not _skip(f)]
    protected = len(all_files) - len(to_update)

    try:
        await status.edit_text(
            f"🔄 **Step 2/3** — Downloading `{len(to_update)}` files...\n"
            f"_({protected} protected file(s) skipped: .env)_",
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        pass

    # Step 2 — download and write
    updated, failed = [], []
    async with aiohttp.ClientSession() as s:
        for path in to_update:
            try:
                content = await _download(s, path)
                parent  = os.path.dirname(path)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                with open(path, "wb") as f:
                    f.write(content)
                updated.append(path)
            except Exception as e:
                failed.append(f"{path} ({e})")
                logger.warning(f"Updater: failed {path}: {e}")

    # Step 3 — report and restart
    lines = [
        "✅ **Update complete!**\n",
        f"📦 Updated : `{len(updated)}` files",
        f"🔒 Skipped : `.env` _(protected)_",
    ]
    if failed:
        lines.append(f"❌ Failed  : `{len(failed)}`")
        for f in failed[:5]:
            lines.append(f"  • `{f}`")
    lines.append("\n🔄 **Restarting in 3 seconds…**")

    try:
        await status.edit_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

    await asyncio.sleep(3)
    logger.info("Restarting bot after update (os.execv).")
    os.execv(sys.executable, [sys.executable] + sys.argv)


# ── /update command ───────────────────────────────────────────────────────────

@Client.on_message(
    filters.command("update") & filters.private & filters.user(ADMIN_ID)
)
async def cmd_update(client: Client, message: Message):
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes, update now", callback_data="upd_confirm"),
        InlineKeyboardButton("❌ Cancel",          callback_data="upd_cancel"),
    ]])
    await message.reply_text(
        f"🔄 **Bot Updater**\n\n"
        f"Repo: `github.com/{GITHUB_REPO}` (`{GITHUB_BRANCH}`)\n\n"
        f"• All files will be replaced with latest from GitHub.\n"
        f"• `.env` will **never** be touched.\n\n"
        f"⚠️ Bot will auto-restart after update.\n\nProceed?",
        reply_markup=markup,
        parse_mode=ParseMode.MARKDOWN,
        quote=True,
    )


# ── Admin panel button → show confirm prompt ──────────────────────────────────

@Client.on_callback_query(
    filters.regex(r"^upd_start$") & filters.user(ADMIN_ID)
)
async def cb_upd_start(client: Client, callback: CallbackQuery):
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes, update now", callback_data="upd_confirm"),
        InlineKeyboardButton("❌ Cancel",          callback_data="upd_cancel"),
    ]])
    await callback.message.edit_text(
        f"🔄 **Bot Updater**\n\n"
        f"Repo: `github.com/{GITHUB_REPO}` (`{GITHUB_BRANCH}`)\n\n"
        f"• All files will be replaced with latest from GitHub.\n"
        f"• `.env` will **never** be touched.\n\n"
        f"⚠️ Bot will auto-restart after update.\n\nProceed?",
        reply_markup=markup,
        parse_mode=ParseMode.MARKDOWN,
    )
    await callback.answer()


# ── Confirm ───────────────────────────────────────────────────────────────────

@Client.on_callback_query(
    filters.regex(r"^upd_confirm$") & filters.user(ADMIN_ID)
)
async def cb_upd_confirm(client: Client, callback: CallbackQuery):
    await callback.answer("Starting update…")
    status = await callback.message.edit_text(
        "🔄 **Updater** — initialising…", parse_mode=ParseMode.MARKDOWN
    )
    asyncio.create_task(_do_update(client, status))


# ── Cancel ────────────────────────────────────────────────────────────────────

@Client.on_callback_query(
    filters.regex(r"^upd_cancel$") & filters.user(ADMIN_ID)
)
async def cb_upd_cancel(client: Client, callback: CallbackQuery):
    await callback.message.edit_text(
        "❌ **Update cancelled.**", parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()
