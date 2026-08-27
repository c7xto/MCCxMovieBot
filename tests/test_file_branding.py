import asyncio
import inspect
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from plugins import file_branding
from plugins.file_branding import build_branded_file_name, normalize_brand_text


def test_movie_branding_removes_promotional_noise_and_keeps_release_details():
    branded = build_branded_file_name(
        "Aavesham_2024_Malayalam_1080p_WEB-DL_@oldchannel.mkv",
        "@lucasmoviebot",
    )

    assert branded == ("Aavesham 2024 Malayalam 1080p WEB DL @lucasmoviebot.mkv")
    assert "_" not in branded.replace("@lucasmoviebot", "")
    assert "@oldchannel" not in branded


def test_series_branding_preserves_season_episode_and_extension():
    branded = build_branded_file_name("Reacher.S01E03.English.1080p.HEVC.mkv", "MCCx Movie Bot")

    assert branded == "Reacher S01E03 English 1080p HEVC MCCx Movie Bot.mkv"


def test_branding_is_idempotent_and_telegram_filename_is_bounded():
    brand = normalize_brand_text("@lucasmoviebot")
    first = build_branded_file_name("Movie 2026 1080p.mkv", brand)
    second = build_branded_file_name(first, brand)
    long_name = build_branded_file_name("🎬" * 300 + ".mkv", brand)

    assert first == second
    assert len(long_name.encode("utf-8")) <= 240


def test_progress_callback_is_async_and_renews_job_lease(monkeypatch):
    fake_db = SimpleNamespace(
        update_file_branding_progress=AsyncMock(return_value=True)
    )
    monkeypatch.setattr(file_branding, "db", fake_db)
    tracker = file_branding._BrandingProgress(
        {"_id": "brand:test", "lock_token": "lease"}, "Uploading"
    )

    assert inspect.iscoroutinefunction(tracker.update)
    asyncio.run(tracker.update(50, 100))
    fake_db.update_file_branding_progress.assert_awaited_once_with(
        "brand:test",
        "lease",
        stage="Uploading",
        current=50,
        total=100,
    )


def test_worker_uploads_renamed_copy_before_switching_database(monkeypatch, tmp_path):
    events = []
    source_media = SimpleNamespace(file_id="source-id")
    source_message = SimpleNamespace(empty=False, document=source_media)
    branded_media = SimpleNamespace(file_id="branded-id", file_unique_id="stable-unique-id")
    sent_message = SimpleNamespace(id=77, document=branded_media)

    async def download_media(_source, *, file_name, **_kwargs):
        events.append("download")
        Path(file_name).write_bytes(b"movie")
        return file_name

    async def send_document(_chat_id, **kwargs):
        events.append(("upload", kwargs["file_name"]))
        return sent_message

    async def replace_with_branded_media(**kwargs):
        events.append(("switch", kwargs["branded_file_name"]))
        return {"movie_id": "abc", "file_id": "branded-id"}

    fake_db = SimpleNamespace(
        replace_with_branded_media=AsyncMock(side_effect=replace_with_branded_media),
        enqueue_announcement=AsyncMock(),
        enqueue_request_fulfillment=AsyncMock(),
        set_file_branding_status=AsyncMock(),
        checkpoint_file_branding_upload=AsyncMock(return_value=True),
    )
    fake_client = SimpleNamespace(
        me=SimpleNamespace(username="lucasmoviebot"),
        get_messages=AsyncMock(return_value=source_message),
        download_media=AsyncMock(side_effect=download_media),
        send_document=AsyncMock(side_effect=send_document),
    )
    job = {
        "_id": "brand:test",
        "lock_token": "lease",
        "payload": {
            "source_chat_id": -1001,
            "source_message_id": 42,
            "source_file_id": "source-id",
            "original_file_name": "Movie_2026_1080p.mkv",
            "file_size": 5,
        }
    }
    config = {
        "file_branding_channel_id": -1002,
        "file_branding_text": "@lucasmoviebot",
    }

    monkeypatch.setattr(file_branding, "db", fake_db)
    monkeypatch.setattr(file_branding, "_RUNTIME_DIR", tmp_path)
    result = asyncio.run(file_branding._process_branding_job(fake_client, job, config))

    expected_name = "Movie 2026 1080p @lucasmoviebot.mkv"
    assert events == [
        "download",
        ("upload", expected_name),
        ("switch", expected_name),
    ]
    assert result["file_id"] == "branded-id"
    assert result["file_name"] == expected_name
    fake_db.enqueue_announcement.assert_awaited_once_with("Movie_2026_1080p.mkv", delay_seconds=1)


def test_retry_reuses_checkpointed_upload_without_downloading(monkeypatch):
    branded_media = SimpleNamespace(
        file_id="branded-id", file_unique_id="stable-unique-id"
    )
    sent_message = SimpleNamespace(id=77, document=branded_media, empty=False)
    fake_db = SimpleNamespace(
        replace_with_branded_media=AsyncMock(
            return_value={"movie_id": "abc", "file_id": "branded-id"}
        ),
        enqueue_announcement=AsyncMock(),
        enqueue_request_fulfillment=AsyncMock(),
        set_file_branding_status=AsyncMock(),
        checkpoint_file_branding_upload=AsyncMock(return_value=True),
    )
    fake_client = SimpleNamespace(
        me=SimpleNamespace(username="lucasmoviebot"),
        get_messages=AsyncMock(return_value=sent_message),
        download_media=AsyncMock(),
        send_document=AsyncMock(),
    )
    job = {
        "_id": "brand:test",
        "lock_token": "lease-two",
        "uploaded": {"channel_id": -1002, "message_id": 77},
        "payload": {
            "source_chat_id": -1001,
            "source_message_id": 42,
            "source_file_id": "source-id",
            "original_file_name": "Movie_2026_1080p.mkv",
            "file_size": 5,
        },
    }
    config = {
        "file_branding_channel_id": -1002,
        "file_branding_text": "@lucasmoviebot",
    }

    monkeypatch.setattr(file_branding, "db", fake_db)
    result = asyncio.run(
        file_branding._process_branding_job(fake_client, job, config)
    )

    assert result["file_id"] == "branded-id"
    fake_client.download_media.assert_not_awaited()
    fake_client.send_document.assert_not_awaited()
    fake_db.checkpoint_file_branding_upload.assert_not_awaited()
