"""Admin-approved, commit-pinned self updater.

The updater stages a complete reviewed commit, validates every path, compiles
the staged Python tree, installs only hash-locked dependencies, backs up every
affected live file, and rolls back if applying the release fails.
"""

import asyncio
import compileall
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import uuid
from pathlib import Path

import aiohttp
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from database.db import db
from plugins.callbacks import answer_callback_safely
from plugins.task_supervisor import TaskConflict, supervisor
from utils import ADMIN_ID, _html, report_internal_error

load_dotenv()
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEPLOYED_FILES = PROJECT_ROOT / ".deployed_files.json"
DEPLOYED_SHA = PROJECT_ROOT / ".deployed_sha"
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main").strip() or "main"
MAX_TREE_FILES = 5000
MAX_FILE_BYTES = 25 * 1024 * 1024
MAX_RELEASE_BYTES = 250 * 1024 * 1024
_SHA_RE = re.compile(r"^[0-9a-fA-F]{7,40}$")


def _github_slug(value: str | None) -> str:
    raw = (value or "c7xto/MCCxMovieBot").strip().rstrip("/")
    raw = re.sub(r"^https?://github\.com/", "", raw, flags=re.IGNORECASE)
    raw = raw.removesuffix(".git").strip("/")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", raw):
        raise RuntimeError("GITHUB_REPO must be a GitHub owner/repository URL or slug")
    return raw


GITHUB_REPO = _github_slug(os.getenv("GITHUB_REPO"))


def _skip(path: str) -> bool:
    normalized = path.replace("\\", "/").lstrip("/")
    name = normalized.rsplit("/", 1)[-1]
    return (
        normalized == ".git"
        or normalized.startswith((".git/", "runtime/"))
        or name in {".env", ".deployed_sha", ".deployed_files.json"}
        or name.endswith((".session", ".session-journal"))
    )


def _safe_relative(path: str) -> Path:
    normalized = str(path).replace("\\", "/")
    candidate = Path(normalized)
    if (
        not normalized
        or "\x00" in normalized
        or normalized.startswith("/")
        or re.match(r"^[A-Za-z]:/", normalized)
        or candidate.is_absolute()
        or candidate.drive
        or any(part in {"", ".", ".."} for part in candidate.parts)
    ):
        raise RuntimeError("Unsafe update path rejected")
    return candidate


def _safe_target(root: Path, relative: str | Path) -> Path:
    """Resolve a release path below root and reject every symlink component."""
    root = root.resolve(strict=True)
    rel = _safe_relative(str(relative))
    current = root
    for part in rel.parts:
        current = current / part
        if current.exists() and current.is_symlink():
            raise RuntimeError("Symlinked update path rejected")
    resolved_parent = current.parent.resolve(strict=False)
    if not resolved_parent.is_relative_to(root):
        raise RuntimeError("Update path escapes the project root")
    return current


def _atomic_copy(source: Path, target: Path):
    target.parent.mkdir(parents=True, exist_ok=True)
    checked = _safe_target(PROJECT_ROOT, target.relative_to(PROJECT_ROOT))
    temporary = checked.with_name(f".{checked.name}.{uuid.uuid4().hex}.tmp")
    try:
        shutil.copyfile(source, temporary)
        os.replace(temporary, checked)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_write(path: Path, content: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_bytes(content)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


async def _github_json(session: aiohttp.ClientSession, endpoint: str) -> dict:
    url = f"https://api.github.com/repos/{GITHUB_REPO}/{endpoint.lstrip('/')}"
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "MCCxMovieBot-Updater/3.0",
    }
    async with session.get(url, headers=headers) as response:
        if response.status in {404, 422}:
            raise RuntimeError("The requested GitHub commit was not found")
        if response.status == 403:
            raise RuntimeError("GitHub temporarily rate-limited the updater")
        if response.status != 200:
            raise RuntimeError(f"GitHub returned HTTP {response.status}")
        return await response.json(content_type=None)


async def _get_commit(session: aiohttp.ClientSession, sha: str) -> dict:
    commit = await _github_json(session, f"commits/{sha}")
    full_sha = str(commit.get("sha", ""))
    if not re.fullmatch(r"[0-9a-f]{40}", full_sha):
        raise RuntimeError("GitHub returned an invalid commit identity")
    comparison = await _github_json(session, f"compare/{GITHUB_BRANCH}...{full_sha}")
    if comparison.get("status") not in {"identical", "behind"}:
        raise RuntimeError(f"Commit is not contained in the protected {GITHUB_BRANCH} branch")
    return commit


async def _get_tree(session: aiohttp.ClientSession, sha: str) -> list[str]:
    data = await _github_json(session, f"git/trees/{sha}?recursive=1")
    if data.get("truncated"):
        raise RuntimeError("GitHub returned a truncated release tree")
    paths = [str(item.get("path", "")) for item in data.get("tree", []) if item.get("type") == "blob"]
    if not paths or len(paths) > MAX_TREE_FILES:
        raise RuntimeError("Release tree size is outside the allowed range")
    validated = []
    for path in paths:
        rel = _safe_relative(path).as_posix()
        if not _skip(rel):
            validated.append(rel)
    return validated


async def _download(session: aiohttp.ClientSession, sha: str, path: str) -> bytes:
    url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{sha}/{path}"
    async with session.get(url) as response:
        if response.status != 200:
            raise RuntimeError(f"Download failed with HTTP {response.status}")
        declared = int(response.headers.get("Content-Length", "0") or 0)
        if declared > MAX_FILE_BYTES:
            raise RuntimeError("Release file exceeds the updater size limit")
        content = await response.read()
        if len(content) > MAX_FILE_BYTES:
            raise RuntimeError("Release file exceeds the updater size limit")
        return content


async def _install_lock(lock_path: Path) -> tuple[bool, str]:
    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        "--require-hashes",
    ]
    local_prefix = PROJECT_ROOT / ".local"
    if local_prefix.is_dir():
        command.extend(("--prefix", str(local_prefix)))
    command.extend(("-r", str(lock_path)))
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    try:
        output, _ = await asyncio.wait_for(process.communicate(), timeout=600)
    except asyncio.TimeoutError:
        process.kill()
        await process.communicate()
        return False, "Dependency installation timed out"
    detail = output.decode("utf-8", errors="replace")[-3000:]
    return process.returncode == 0, detail


async def _stage_release(sha: str, stage_root: Path) -> list[str]:
    timeout = aiohttp.ClientTimeout(total=120, connect=15, sock_read=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        paths = await _get_tree(session, sha)
        semaphore = asyncio.Semaphore(8)
        total_bytes = 0
        total_lock = asyncio.Lock()

        async def stage_one(path: str):
            nonlocal total_bytes
            async with semaphore:
                content = await _download(session, sha, path)
                async with total_lock:
                    total_bytes += len(content)
                    if total_bytes > MAX_RELEASE_BYTES:
                        raise RuntimeError("Release exceeds the total size limit")
                target = _safe_target(stage_root, path)
                target.parent.mkdir(parents=True, exist_ok=True)
                await asyncio.to_thread(target.write_bytes, content)

        await asyncio.gather(*(stage_one(path) for path in paths))
    return paths


def _read_previous_manifest() -> set[str]:
    if not DEPLOYED_FILES.is_file():
        return set()
    try:
        raw = json.loads(DEPLOYED_FILES.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError
        return {_safe_relative(value).as_posix() for value in raw if not _skip(str(value))}
    except (OSError, TypeError, ValueError, RuntimeError):
        logger.warning("Prior update manifest is invalid; stale cleanup disabled")
        return set()


def _apply_release(stage_root: Path, backup_root: Path, paths: list[str]):
    previous = _read_previous_manifest()
    stale = sorted(previous - set(paths))
    affected = list(dict.fromkeys(paths + stale))
    existed = set()

    for rel in affected:
        live = _safe_target(PROJECT_ROOT, rel)
        if live.is_file():
            existed.add(rel)
            backup = _safe_target(backup_root, rel)
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(live, backup)

    try:
        for rel in paths:
            source = _safe_target(stage_root, rel)
            target = _safe_target(PROJECT_ROOT, rel)
            _atomic_copy(source, target)
        for rel in stale:
            target = _safe_target(PROJECT_ROOT, rel)
            if target.is_file() and not _skip(rel):
                target.unlink()
        _atomic_write(
            DEPLOYED_FILES,
            json.dumps(sorted(paths), indent=2).encode("utf-8"),
        )
    except Exception:
        logger.exception("Update apply failed; restoring the previous release")
        for rel in affected:
            live = _safe_target(PROJECT_ROOT, rel)
            backup = _safe_target(backup_root, rel)
            try:
                if backup.is_file():
                    _atomic_copy(backup, live)
                elif rel not in existed and live.is_file():
                    live.unlink()
            except OSError:
                logger.exception("Updater rollback failed for %s", rel)
        raise


async def _do_update(client: Client, status: Message, sha: str):
    owner = await db.acquire_action_lease("self_update", "global", 15 * 60)
    if owner is None:
        await status.edit_text("⚠️ Another update is already running.")
        return
    try:
        await status.edit_text("🔄 <b>1/4</b> Downloading the reviewed release…")
        with (
            tempfile.TemporaryDirectory(prefix="mccx-stage-") as stage_name,
            tempfile.TemporaryDirectory(prefix="mccx-backup-") as backup_name,
        ):
            stage_root = Path(stage_name).resolve()
            backup_root = Path(backup_name).resolve()
            paths = await _stage_release(sha, stage_root)

            await status.edit_text("🧪 <b>2/4</b> Checking Python files…")
            compiled = await asyncio.to_thread(compileall.compile_dir, str(stage_root), quiet=1, force=True)
            if not compiled:
                raise RuntimeError("The staged release failed Python compilation")

            staged_requirements = _safe_target(stage_root, "requirements.txt")
            live_requirements = PROJECT_ROOT / "requirements.txt"
            requirements_changed = staged_requirements.is_file() and (
                not live_requirements.is_file()
                or staged_requirements.read_bytes() != live_requirements.read_bytes()
            )
            if requirements_changed:
                staged_lock = _safe_target(stage_root, "requirements.lock")
                if not staged_lock.is_file():
                    raise RuntimeError("Dependencies changed but the release has no requirements.lock")
                await status.edit_text("📦 <b>3/4</b> Installing locked dependencies…")
                ok, detail = await _install_lock(staged_lock)
                if not ok:
                    logger.error("Updater dependency install failed: %s", detail)
                    raise RuntimeError("Locked dependency installation failed")
            else:
                await status.edit_text("📦 <b>3/4</b> Dependencies unchanged.")

            await status.edit_text("🚀 <b>4/4</b> Applying the release safely…")
            await asyncio.to_thread(_apply_release, stage_root, backup_root, paths)

        await asyncio.to_thread(_atomic_write, DEPLOYED_SHA, sha.encode("ascii"))
        try:
            await db.update_config("last_update_sha", sha)
        except Exception as exc:
            logger.warning("Could not record deployed SHA: %s", type(exc).__name__)

        await status.edit_text(
            "✅ <b>Update complete</b>\n\n"
            f"Commit: <code>{sha[:12]}</code>\n"
            f"Files: <code>{len(paths)}</code>\n\n"
            "Restarting the bot…",
            parse_mode=ParseMode.HTML,
        )
        await db.release_action_lease("self_update", "global", owner)
        owner = None
        await asyncio.sleep(2)
        os.execv(sys.executable, [sys.executable, *sys.argv])
    except Exception as error:
        reference = report_internal_error(logger, "self_update", error)
        await status.edit_text(
            "❌ <b>Update stopped safely</b>\n\n"
            "The running release was kept or restored.\n"
            f"Reference: <code>{reference}</code>",
            parse_mode=ParseMode.HTML,
        )
    finally:
        if owner is not None:
            await db.release_action_lease("self_update", "global", owner)


async def _show_commit_review(message: Message, sha: str):
    timeout = aiohttp.ClientTimeout(total=45, connect=15, sock_read=30)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            commit = await _get_commit(session, sha)
    except Exception as error:
        reference = report_internal_error(logger, "update_review", error)
        await message.reply_text(
            f"❌ <b>Commit could not be approved</b>\n\nReference: <code>{reference}</code>",
            parse_mode=ParseMode.HTML,
            reply_parameters=None,
        )
        return

    full_sha = commit["sha"]
    info = commit.get("commit", {})
    author = (info.get("author") or {}).get("name", "Unknown")
    title = (info.get("message") or "Untitled commit").splitlines()[0][:180]
    url = commit.get("html_url", f"https://github.com/{GITHUB_REPO}/commit/{full_sha}")
    markup = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("View changes", url=url)],
            [
                InlineKeyboardButton("Confirm update", callback_data=f"upd_confirm#{full_sha}"),
                InlineKeyboardButton("Cancel", callback_data="upd_cancel"),
            ],
        ]
    )
    await message.reply_text(
        "🔄 <b>Review update</b>\n\n"
        f"Commit: <code>{full_sha}</code>\n"
        f"Author: {_html(author)}\n"
        f"Change: {_html(title)}\n\n"
        "Open the changes before confirming.",
        reply_markup=markup,
        parse_mode=ParseMode.HTML,
        reply_parameters=None,
    )


@Client.on_message(filters.command("update") & filters.private & filters.user(ADMIN_ID))
async def cmd_update(_client: Client, message: Message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) != 2 or not _SHA_RE.fullmatch(parts[1].strip()):
        await message.reply_text(
            "🔄 <b>Safe Bot Updater</b>\n\n"
            "Send <code>/update COMMIT_SHA</code>. The commit must already be "
            f"part of <code>{_html(GITHUB_BRANCH)}</code>.",
            parse_mode=ParseMode.HTML,
            reply_parameters=None,
        )
        return
    await _show_commit_review(message, parts[1].strip().lower())


@Client.on_callback_query(filters.regex(r"^upd_start$") & filters.user(ADMIN_ID))
async def cb_upd_start(_client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    await callback.message.edit_text(
        "🔄 <b>Safe Bot Updater</b>\n\n"
        "Copy a commit SHA from GitHub, then send:\n"
        "<code>/update COMMIT_SHA</code>\n\n"
        "Only commits already included in the protected main branch are accepted.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "Browse commits",
                        url=f"https://github.com/{GITHUB_REPO}/commits/{GITHUB_BRANCH}",
                    )
                ]
            ]
        ),
    )


@Client.on_callback_query(filters.regex(r"^upd_confirm#[0-9a-f]{40}$") & filters.user(ADMIN_ID))
async def cb_upd_confirm(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback, "Starting safe update…")
    sha = callback.data.split("#", 1)[1]
    status = await callback.message.edit_text(
        f"🔄 Preparing <code>{sha[:12]}</code>…", parse_mode=ParseMode.HTML
    )
    try:
        supervisor.spawn(
            _do_update(client, status, sha),
            key="maintenance:self-update",
            owner=f"admin:{callback.from_user.id}",
            resources=("maintenance", "movie-catalog"),
            drain_on_shutdown=True,
        )
    except TaskConflict:
        await status.edit_text("⚠️ Another maintenance task is already running.")


@Client.on_callback_query(filters.regex(r"^upd_cancel$") & filters.user(ADMIN_ID))
async def cb_upd_cancel(_client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback, "Update cancelled")
    await callback.message.edit_text("❌ <b>Update cancelled.</b>", parse_mode=ParseMode.HTML)
