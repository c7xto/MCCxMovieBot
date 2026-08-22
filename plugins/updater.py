"""
updater.py — GitHub self-updater, pinned to an admin-approved commit SHA.
Pulls a single specific commit (never "whatever is on main right now") and
restarts the bot. Runtime secrets, Telegram sessions, and deployment state
are never overwritten.

Why pinned: blindly pulling the head of a public branch on every /update tap
means a compromised GitHub account, a bad merge, or a malicious PR becomes
full remote code execution on the bot host the instant an admin taps
"Update Bot" — with the Telegram bot token and every MongoDB credential on
the line. Requiring the admin to name a commit, see its message/author/diff
link, and explicitly confirm turns that into a deliberate, reviewable
action instead of an unpinned trust-the-branch button.

Triggered by:
  • /update [sha]  command (admin only)
  • 🔄 Update Bot  button in /admin panel  (callback: upd_start)
"""

import os
import re
import sys
import asyncio
import logging
import json
import shutil
import tempfile
import compileall
from pathlib import Path
import aiohttp
from dotenv import load_dotenv
from pyrogram import Client, filters, ContinuePropagation, StopPropagation
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode
from database.db import db
from utils import ADMIN_ID, _html
from plugins.state import get_state, set_state, clear_state
from plugins.health_monitor import _log_task_crash

load_dotenv()
logger = logging.getLogger(__name__)

GITHUB_REPO   = "c7xto/mccxmoviebot"
GITHUB_BRANCH = "main"
PROJECT_ROOT  = Path(__file__).resolve().parents[1]
_DEPLOYED_FILES = PROJECT_ROOT / ".deployed_files.json"

_SHA_RE = re.compile(r"^[0-9a-fA-F]{7,40}$")


def _skip(path: str) -> bool:
    """Protect runtime credentials and state from repository updates."""
    name = path.replace("\\", "/").split("/")[-1]
    return (
        name in {".env", ".deployed_sha", ".deployed_files.json"}
        or name.endswith((".session", ".session-journal"))
    )


def _safe_relative(path: str) -> Path:
    candidate = Path(path.replace("\\", "/"))
    if candidate.is_absolute() or ".." in candidate.parts:
        raise RuntimeError(f"Unsafe update path: {path}")
    return candidate


async def _get_commit(session: aiohttp.ClientSession, sha: str) -> dict:
    """Resolves a (possibly short) SHA to full commit metadata — message,
    author, date, and the html_url an admin can open to review the actual
    diff before approving it."""
    url = f"https://api.github.com/repos/{GITHUB_REPO}/commits/{sha}"
    headers = {
        "Accept":     "application/vnd.github+json",
        "User-Agent": "MCCxMovieBot-Updater/2.0",
    }
    async with session.get(url, headers=headers,
                           timeout=aiohttp.ClientTimeout(total=30)) as r:
        if r.status == 404 or r.status == 422:
            raise RuntimeError(f"No commit matching `{sha}` found in {GITHUB_REPO}.")
        if r.status == 403:
            raise RuntimeError("GitHub API rate-limited (403). Wait a minute and retry.")
        if r.status != 200:
            raise RuntimeError(f"GitHub API {r.status}: {(await r.text())[:150]}")
        return await r.json(content_type=None)


async def _get_tree(session: aiohttp.ClientSession, sha: str) -> list:
    url = f"https://api.github.com/repos/{GITHUB_REPO}/git/trees/{sha}?recursive=1"
    headers = {
        "Accept":     "application/vnd.github+json",
        "User-Agent": "MCCxMovieBot-Updater/2.0",
    }
    async with session.get(url, headers=headers,
                           timeout=aiohttp.ClientTimeout(total=30)) as r:
        if r.status == 403:
            raise RuntimeError("GitHub API rate-limited (403). Wait a minute and retry.")
        if r.status == 404:
            raise RuntimeError(f"Commit `{sha}` not found in {GITHUB_REPO}.")
        if r.status != 200:
            raise RuntimeError(f"GitHub API {r.status}: {(await r.text())[:150]}")
        data = await r.json(content_type=None)

    return [item["path"] for item in data.get("tree", []) if item["type"] == "blob"]


async def _download(session: aiohttp.ClientSession, sha: str, path: str) -> bytes:
    url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{sha}/{path}"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status}")
        return await r.read()


async def _install_requirements(requirements_path: Path) -> tuple[bool, str]:
    """Install a staged dependency lock with the bot's current interpreter."""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        "-r",
        str(requirements_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    try:
        output, _ = await asyncio.wait_for(process.communicate(), timeout=300)
    except asyncio.TimeoutError:
        process.kill()
        await process.communicate()
        return False, "Dependency installation timed out after 5 minutes."
    detail = output.decode("utf-8", errors="replace")[-3000:]
    return process.returncode == 0, detail


def _write_file(path: str, content: bytes):
    """Synchronous disk I/O — always call via asyncio.to_thread so it
    doesn't block the event loop for every user while an update runs."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        f.write(content)


async def _do_update(client: Client, status: Message, sha: str):
    # Step 1 — fetch file list for this exact commit
    try:
        await status.edit_text(
            f"🔄 **Step 1/4** — Fetching file list for `{sha[:12]}`...",
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        pass

    try:
        async with aiohttp.ClientSession() as s:
            all_files = await _get_tree(s, sha)
    except Exception as e:
        await status.edit_text(
            f"❌ **Failed — could not reach GitHub**\n\n`{e}`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    try:
        # Keep URL paths POSIX-style even when the bot runs on Windows.
        to_update = [_safe_relative(f).as_posix() for f in all_files if not _skip(f)]
    except RuntimeError as e:
        await status.edit_text(f"❌ **Update rejected**\n\n`{e}`", parse_mode=ParseMode.MARKDOWN)
        return
    protected = len(all_files) - len(to_update)

    try:
        await status.edit_text(
            f"🔄 **Step 2/4** — Downloading `{len(to_update)}` files...\n"
            f"_({protected} runtime secret/state file(s) protected)_",
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        pass

    # Step 2 — stage the complete release before touching the live tree.
    # A single failed download or compile check aborts with zero live changes.
    updated, failed = [], []
    with tempfile.TemporaryDirectory(prefix="mccx-update-") as stage_name, \
         tempfile.TemporaryDirectory(prefix="mccx-backup-") as backup_name:
        stage_root = Path(stage_name)
        backup_root = Path(backup_name)
        semaphore = asyncio.Semaphore(8)

        async def _stage_one(session, path):
            async with semaphore:
                try:
                    content = await _download(session, sha, path)
                    await asyncio.to_thread(_write_file, stage_root / path, content)
                    return None
                except Exception as e:
                    return f"{path} ({e})"

        async with aiohttp.ClientSession() as s:
            failures = await asyncio.gather(*[_stage_one(s, path) for path in to_update])
        failed = [failure for failure in failures if failure]
        if failed:
            await status.edit_text(
                f"❌ **Update aborted safely**\n\n"
                f"{len(failed)} file(s) failed to download. The live bot was not changed.\n"
                + "\n".join(f"• `{item}`" for item in failed[:5]),
                parse_mode=ParseMode.MARKDOWN,
            )
            return

        compile_ok = await asyncio.to_thread(
            compileall.compile_dir, str(stage_root), quiet=1, force=True
        )
        if not compile_ok:
            await status.edit_text(
                "❌ **Update aborted safely**\n\nThe staged release failed Python compilation. "
                "The live bot was not changed.",
                parse_mode=ParseMode.MARKDOWN,
            )
            return

        staged_requirements = stage_root / "requirements.txt"
        live_requirements = PROJECT_ROOT / "requirements.txt"
        requirements_changed = (
            staged_requirements.is_file()
            and (
                not live_requirements.is_file()
                or staged_requirements.read_bytes() != live_requirements.read_bytes()
            )
        )
        if requirements_changed:
            await status.edit_text(
                "🔄 **Step 3/4** — Installing verified dependencies...",
                parse_mode=ParseMode.MARKDOWN,
            )
            dependencies_ok, detail = await _install_requirements(staged_requirements)
            if not dependencies_ok:
                logger.error("Updater dependency installation failed:\n%s", detail)
                await status.edit_text(
                    "❌ **Update aborted safely**\n\nDependency installation failed. "
                    "The live source tree was not changed; check the server log for details.",
                    parse_mode=ParseMode.MARKDOWN,
                )
                return

        await status.edit_text(
            "🔄 **Step 4/4** — Applying the staged release...",
            parse_mode=ParseMode.MARKDOWN,
        )

        previous_files = set()
        if _DEPLOYED_FILES.exists():
            try:
                previous_files = set(json.loads(_DEPLOYED_FILES.read_text(encoding="utf-8")))
            except (OSError, ValueError, TypeError):
                logger.warning("Could not read prior deployed-file manifest; stale cleanup skipped.")
        stale_files = sorted(previous_files - set(to_update))
        affected = list(to_update) + stale_files
        existed = set()

        try:
            # Back up every path that will change, then apply the release.
            for rel in affected:
                live = PROJECT_ROOT / rel
                if live.is_file():
                    existed.add(rel)
                    backup = backup_root / rel
                    backup.parent.mkdir(parents=True, exist_ok=True)
                    await asyncio.to_thread(shutil.copy2, live, backup)

            for rel in to_update:
                live = PROJECT_ROOT / rel
                live.parent.mkdir(parents=True, exist_ok=True)
                await asyncio.to_thread(shutil.copy2, stage_root / rel, live)
                updated.append(rel)

            for rel in stale_files:
                live = PROJECT_ROOT / rel
                if live.is_file() and not _skip(rel):
                    live.unlink()

            _DEPLOYED_FILES.write_text(json.dumps(sorted(to_update), indent=2), encoding="utf-8")
        except Exception as e:
            logger.exception("Updater apply failed; rolling back")
            for rel in affected:
                live = PROJECT_ROOT / rel
                backup = backup_root / rel
                try:
                    if backup.is_file():
                        live.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(backup, live)
                    elif rel not in existed and live.is_file():
                        live.unlink()
                except OSError:
                    logger.exception("Updater rollback failed for %s", rel)
            await status.edit_text(
                f"❌ **Update failed and was rolled back**\n\n`{e}`",
                parse_mode=ParseMode.MARKDOWN,
            )
            return

    # Persist which commit is now deployed — both locally (survives even if
    # Mongo is unreachable) and in bot_config (so the admin panel can show
    # it without SSHing in).
    try:
        await asyncio.to_thread(_write_file, PROJECT_ROOT / ".deployed_sha", sha.encode())
    except Exception as e:
        logger.warning(f"Could not record deployed SHA locally: {e}")
    try:
        await db.update_config("last_update_sha", sha)
    except Exception as e:
        logger.warning(f"Could not record deployed SHA in bot_config: {e}")

    # Step 3 — report and restart
    lines = [
        "✅ **Update complete!**\n",
        f"📌 Commit  : `{sha[:12]}`",
        f"📦 Updated : `{len(updated)}` files",
        f"🔒 Protected: runtime secrets/state",
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
    logger.info(f"Restarting bot after update to {sha} (os.execv).")
    os.execv(sys.executable, [sys.executable] + sys.argv)


async def _show_commit_review(client: Client, target, sha: str, *, is_callback: bool):
    """Fetches commit metadata for `sha` and shows a review/confirm screen.
    `target` is a Message (reply) or CallbackQuery (edit) depending on
    `is_callback`."""
    try:
        async with aiohttp.ClientSession() as s:
            commit = await _get_commit(s, sha)
    except Exception as e:
        text = f"❌ **Couldn't look up that commit.**\n\n`{e}`\n\nSend a different SHA, or /cancel."
        if is_callback:
            await target.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
        else:
            await target.reply_text(text, parse_mode=ParseMode.MARKDOWN)
        return

    full_sha    = commit.get("sha", sha)
    html_url    = commit.get("html_url", f"https://github.com/{GITHUB_REPO}/commit/{full_sha}")
    commit_info = commit.get("commit", {})
    message_1st = (commit_info.get("message") or "").split("\n", 1)[0][:200]
    author      = (commit_info.get("author") or {}).get("name", "unknown")
    date        = (commit_info.get("author") or {}).get("date", "unknown")

    set_state(target.from_user.id, f"upd_wait_confirm#{full_sha}")

    # Commit message/author come from GitHub, not our own admin — avoid
    # Markdown parsing on that part so stray `*`/`_`/backticks can't break
    # message formatting.
    text = (
        "🔄 **Bot Updater — Review Commit**\n\n"
        f"Repo: {GITHUB_REPO}\n"
        f"SHA: {full_sha}\n"
        f"Author: {_html(author)}\n"
        f"Date: {date}\n"
        f"Message: {_html(message_1st)}\n\n"
        "Repository files will be replaced with this commit's contents. "
        "Runtime secrets, session files and deployment state are protected.\n\n"
        "Review the diff on GitHub before confirming."
    )
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 View diff on GitHub", url=html_url)],
        [InlineKeyboardButton("✅ Confirm update", callback_data=f"upd_confirm#{full_sha}"),
         InlineKeyboardButton("❌ Cancel",          callback_data="upd_cancel")],
    ])

    if is_callback:
        await target.message.edit_text(text, reply_markup=markup)
    else:
        await target.reply_text(text, reply_markup=markup)


_PROMPT_TEXT = (
    "🔄 **Bot Updater**\n\n"
    "For safety, updates are pinned to a specific commit — the bot will "
    "never blindly pull whatever is currently on `main`.\n\n"
    "Send the **commit SHA** you want to update to (copy it from the commits "
    "page below), or /cancel."
)
_BROWSE_MARKUP = InlineKeyboardMarkup([[
    InlineKeyboardButton("🔗 Browse commits on GitHub",
                          url=f"https://github.com/{GITHUB_REPO}/commits/{GITHUB_BRANCH}")
]])


# ── /update [sha] command ──────────────────────────────────────────────────────

@Client.on_message(
    filters.command("update") & filters.private & filters.user(ADMIN_ID)
)
async def cmd_update(client: Client, message: Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) == 2 and _SHA_RE.match(parts[1].strip()):
        await _show_commit_review(client, message, parts[1].strip().lower(), is_callback=False)
        return
    set_state(message.from_user.id, "upd_wait_sha")
    await message.reply_text(_PROMPT_TEXT, reply_markup=_BROWSE_MARKUP, parse_mode=ParseMode.MARKDOWN, quote=True)


# ── Admin panel button → prompt for a SHA ──────────────────────────────────────

@Client.on_callback_query(
    filters.regex(r"^upd_start$") & filters.user(ADMIN_ID)
)
async def cb_upd_start(client: Client, callback: CallbackQuery):
    set_state(callback.from_user.id, "upd_wait_sha")
    await callback.message.edit_text(_PROMPT_TEXT, reply_markup=_BROWSE_MARKUP, parse_mode=ParseMode.MARKDOWN)
    await callback.answer()


# ── Text input handler for upd_wait_sha ─────────────────────────────────────────

@Client.on_message(
    filters.private & filters.text & filters.user(ADMIN_ID) &
    ~filters.command(["start", "admin", "ban", "unban", "reset_db", "broadcast", "filesearch", "cancel"]),
    group=-1,  # must win the race against filter.py's auto_filter — see admin.py's
               # matching catch_admin_input handler for the full explanation.
)
async def upd_input_handler(client: Client, message: Message):
    admin_id = message.from_user.id
    state = get_state(admin_id)

    if not state or not state.startswith("upd_"):
        raise ContinuePropagation

    if message.text.strip().lower() in ("/cancel", "cancel"):
        clear_state(admin_id)
        await message.reply_text("🚫 **Update cancelled.**", parse_mode=ParseMode.MARKDOWN)
        raise StopPropagation

    if state != "upd_wait_sha":
        # upd_wait_confirm#<sha> — admin should be tapping a button, not typing.
        raise ContinuePropagation

    sha_input = message.text.strip()
    if not _SHA_RE.match(sha_input):
        await message.reply_text(
            "❌ That doesn't look like a commit SHA (7-40 hex characters). "
            "Try again, or /cancel.",
            parse_mode=ParseMode.MARKDOWN,
        )
        raise StopPropagation

    clear_state(admin_id)
    await _show_commit_review(client, message, sha_input.lower(), is_callback=False)
    raise StopPropagation


# ── Confirm ───────────────────────────────────────────────────────────────────

@Client.on_callback_query(
    filters.regex(r"^upd_confirm#") & filters.user(ADMIN_ID)
)
async def cb_upd_confirm(client: Client, callback: CallbackQuery):
    sha = callback.data.split("#", 1)[1]
    clear_state(callback.from_user.id)
    await callback.answer("Starting update…")
    status = await callback.message.edit_text(
        f"🔄 **Updater** — initialising for `{sha[:12]}`…", parse_mode=ParseMode.MARKDOWN
    )
    # _do_update ends with os.execv() — if that call itself fails (rare, but
    # possible depending on host), the failure would otherwise be completely
    # invisible: the bot appears to hang mid-"update" with no restart and no
    # log line anywhere.
    update_task = asyncio.create_task(_do_update(client, status, sha))
    update_task.add_done_callback(lambda t: _log_task_crash(t, client, f"self_update({sha[:12]})"))


# ── Cancel ────────────────────────────────────────────────────────────────────

@Client.on_callback_query(
    filters.regex(r"^upd_cancel$") & filters.user(ADMIN_ID)
)
async def cb_upd_cancel(client: Client, callback: CallbackQuery):
    clear_state(callback.from_user.id)
    await callback.message.edit_text(
        "❌ **Update cancelled.**", parse_mode=ParseMode.MARKDOWN
    )
    await callback.answer()
