"""Durable, one-time filename branding for newly indexed Telegram files."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
import tempfile
import unicodedata
from pathlib import Path

from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait

from database.db import db
from plugins.filter import send_smart_log
from utils import _html, report_internal_error


logger = logging.getLogger(__name__)

_RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime" / "branding"
_URL_RE = re.compile(r"(?i)(?:https?://|www\.)\S+|(?:t|telegram)\.me/\S+")
_HANDLE_RE = re.compile(r"(?<![A-Za-z0-9])@[A-Za-z0-9_]{3,}")
_INVALID_RE = re.compile(r"[\\/:*?\"<>|\x00-\x1f]")
_SPACE_RE = re.compile(r"\s+")
_EXT_RE = re.compile(r"(?i)(\.[a-z0-9]{2,8})$")
_KNOWN_EXTENSIONS = {
    ".mkv",
    ".mp4",
    ".avi",
    ".mov",
    ".webm",
    ".m4v",
    ".ts",
    ".m2ts",
    ".mp3",
    ".m4a",
    ".aac",
    ".flac",
    ".ogg",
    ".wav",
    ".zip",
    ".rar",
}


class BrandingStorageError(RuntimeError):
    pass


class _BrandingProgress:
    """Throttle live queue progress while renewing the durable job lease."""

    def __init__(self, job: dict, stage: str):
        self.job = job
        self.stage = stage
        self.last_update = 0.0

    async def update(self, current, total):
        now = asyncio.get_running_loop().time()
        if current < total and now - self.last_update < 20:
            return
        self.last_update = now
        renewed = await db.update_file_branding_progress(
            self.job["_id"],
            self.job["lock_token"],
            stage=self.stage,
            current=current,
            total=total,
        )
        if not renewed:
            raise RuntimeError("The branding job lease is no longer active")


def normalize_brand_text(value: str, bot_username: str = "") -> str:
    brand = str(value or "").strip()
    if not brand and bot_username:
        brand = f"@{bot_username.lstrip('@')}"
    brand = _INVALID_RE.sub(" ", unicodedata.normalize("NFKC", brand))
    brand = _SPACE_RE.sub(" ", brand).strip()
    return brand[:48]


def _utf8_trim(value: str, max_bytes: int) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value
    return encoded[:max_bytes].decode("utf-8", errors="ignore").rstrip()


def build_branded_file_name(original_name: str, brand: str) -> str:
    """Create a clean filename while preserving title/release metadata."""
    original = unicodedata.normalize("NFKC", str(original_name or "File")).strip()
    match = _EXT_RE.search(original)
    extension = match.group(1).lower() if match else ""
    if extension not in _KNOWN_EXTENSIONS:
        extension = ""
    base = original[: match.start()] if match and extension else original
    base = _URL_RE.sub(" ", base)
    base = _HANDLE_RE.sub(" ", base)
    base = _INVALID_RE.sub(" ", base)
    base = re.sub(r"[_.+]+", " ", base)
    base = re.sub(r"\s+-\s+|(?<=\w)-(?=\w)", " ", base)
    if brand:
        base = re.sub(re.escape(brand), " ", base, flags=re.IGNORECASE)
    base = _SPACE_RE.sub(" ", base).strip(" .-_[]()") or "File"
    suffix = f" {brand}" if brand else ""
    reserved = len((suffix + extension).encode("utf-8"))
    base = _utf8_trim(base, max(24, 240 - reserved))
    return f"{base}{suffix}{extension}"


def _source_extension(name: str) -> str:
    match = _EXT_RE.search(str(name or ""))
    extension = match.group(1).lower() if match else ""
    return extension if extension in _KNOWN_EXTENSIONS else ".bin"


async def _notify_original_available(file_name: str):
    await db.enqueue_announcement(file_name, delay_seconds=1)
    await db.enqueue_request_fulfillment(file_name, delay_seconds=1)


async def _process_branding_job(client, job: dict, config: dict):
    payload = job.get("payload", {})
    source_chat_id = int(payload["source_chat_id"])
    source_message_id = int(payload["source_message_id"])
    source_file_id = str(payload["source_file_id"])
    original_name = str(payload.get("original_file_name") or "File")
    target_channel_id = int(config.get("file_branding_channel_id", 0) or 0)
    if not target_channel_id:
        raise RuntimeError("Branded cache channel is not configured")
    if target_channel_id == source_chat_id:
        raise RuntimeError("Branded cache channel must differ from the source channel")

    brand = normalize_brand_text(config.get("file_branding_text", ""), client.me.username)
    branded_name = build_branded_file_name(original_name, brand)
    if branded_name.casefold() == original_name.casefold():
        await db.set_file_branding_status(source_file_id, "complete")
        await _notify_original_available(original_name)
        return {
            "file_id": source_file_id,
            "file_name": original_name,
            "already_branded": True,
        }

    sent = None
    uploaded = job.get("uploaded") or {}
    if (
        int(uploaded.get("channel_id", 0) or 0) == target_channel_id
        and int(uploaded.get("message_id", 0) or 0)
    ):
        candidate = await client.get_messages(
            target_channel_id, int(uploaded["message_id"])
        )
        if candidate and not getattr(candidate, "empty", False):
            candidate_media = getattr(candidate, "document", None)
            if candidate_media and candidate_media.file_id:
                sent = candidate

    if sent is None:
        _RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
        file_size = max(0, int(payload.get("file_size", 0) or 0))
        free_bytes = shutil.disk_usage(_RUNTIME_DIR).free
        required = file_size + max(100 * 1024 * 1024, file_size // 10)
        if file_size and free_bytes < required:
            raise BrandingStorageError(
                f"Need {required / (1024**3):.2f} GB temporary space; "
                f"only {free_bytes / (1024**3):.2f} GB is free"
            )

        with tempfile.TemporaryDirectory(
            prefix="mccx-brand-", dir=_RUNTIME_DIR
        ) as temp_dir:
            temp_path = Path(temp_dir) / f"source{_source_extension(original_name)}"
            source_message = await client.get_messages(
                source_chat_id, source_message_id
            )
            download_source = (
                source_message
                if source_message and not getattr(source_message, "empty", False)
                else source_file_id
            )
            download_progress = _BrandingProgress(job, "Downloading")
            downloaded = await client.download_media(
                download_source,
                file_name=str(temp_path),
                progress=download_progress.update,
            )
            if not downloaded or not Path(downloaded).is_file():
                raise RuntimeError("Telegram did not provide the source file")

            upload_progress = _BrandingProgress(job, "Uploading")
            sent = await client.send_document(
                target_channel_id,
                document=str(downloaded),
                file_name=branded_name,
                caption=(
                    f"🏷 <b>{_html(branded_name)}</b>\n"
                    f"<blockquote>Ready for cached delivery by "
                    f"@{_html(client.me.username)}</blockquote>"
                ),
                parse_mode=ParseMode.HTML,
                progress=upload_progress.update,
            )
            media = sent.document
            if not media or not media.file_id:
                raise RuntimeError("The branded upload did not return a document")
            checkpointed = await db.checkpoint_file_branding_upload(
                job["_id"],
                job["lock_token"],
                {
                    "channel_id": target_channel_id,
                    "message_id": sent.id,
                    "file_id": media.file_id,
                    "file_unique_id": getattr(media, "file_unique_id", "") or "",
                    "file_name": branded_name,
                },
            )
            if not checkpointed:
                raise RuntimeError("The branding job lease changed during upload")

    media = sent.document
    result = await db.replace_with_branded_media(
        source_file_id=source_file_id,
        branded_media=media,
        branded_file_name=branded_name,
        branded_channel_id=target_channel_id,
        branded_message_id=sent.id,
    )

    await _notify_original_available(original_name)
    result["file_name"] = branded_name
    result["already_branded"] = False
    return result


async def run_file_branding_worker(client):
    """Process one durable branding job at a time to bound disk and bandwidth."""
    while True:
        job = None
        try:
            config = await db.get_config()
            if not int(config.get("file_branding_channel_id", 0) or 0):
                await asyncio.sleep(5)
                continue

            job = await db.claim_due_file_branding()
            if not job:
                await asyncio.sleep(3)
                continue

            result = await _process_branding_job(client, job, config)
            await db.complete_file_branding(job["_id"], job["lock_token"], result)
            logger.info(
                "file_branding_complete job=%s file=%s",
                job["_id"],
                result.get("file_name"),
            )
        except asyncio.CancelledError:
            raise
        except FloodWait as exc:
            if job:
                await db.retry_file_branding(
                    job["_id"], job["lock_token"], exc.value + 5, "Telegram rate limit"
                )
        except Exception as exc:
            if job:
                attempts = int(job.get("attempts", 1))
                if attempts >= 8:
                    await db.fail_file_branding(job["_id"], job["lock_token"], str(exc))
                    payload = job.get("payload", {})
                    await db.set_file_branding_status(str(payload.get("source_file_id") or ""), "fallback")
                    await _notify_original_available(str(payload.get("original_file_name") or "File"))
                    reference = report_internal_error(
                        logger,
                        "file_branding",
                        exc,
                        source_message_id=payload.get("source_message_id"),
                    )
                    await send_smart_log(
                        client,
                        "⚠️ **#FileBrandingFallback**\n\n"
                        "The original file remains searchable and usable.\n"
                        f"Reference: `{reference}`",
                    )
                else:
                    delay = min(1800, 20 * (2 ** min(attempts - 1, 6)))
                    await db.retry_file_branding(job["_id"], job["lock_token"], delay, type(exc).__name__)
            else:
                logger.warning("File-branding worker error: %s", type(exc).__name__)
        await asyncio.sleep(1)
