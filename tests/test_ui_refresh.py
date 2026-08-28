from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from plugins.state import clear_state, get_state, set_state
from plugins.ui_helpers import finish_prompt, restore_prompt


ROOT = Path(__file__).resolve().parents[1]


def test_callback_handlers_use_expiry_safe_answer_helper():
    offenders = []
    for path in (ROOT / "plugins").glob("*.py"):
        if path.name == "callbacks.py":
            continue
        source = path.read_text(encoding="utf-8")
        if "await callback.answer(" in source:
            offenders.append(path.name)

    assert offenders == []


def test_admin_prompts_do_not_show_raw_markdown_or_cancel_instructions():
    sources = "\n".join(
        (ROOT / "plugins" / name).read_text(encoding="utf-8")
        for name in (
            "admin.py",
            "file_manager.py",
            "group_manager.py",
        )
    )

    assert "Type /cancel" not in sources
    assert "Action Cancelled" not in sources
    assert "_Select an action._" not in sources
    assert "Select an action." not in sources
    assert "🔙" not in sources


def test_removed_filename_branding_is_not_exposed_or_started():
    assert not (ROOT / "plugins" / "branding_admin.py").exists()
    assert not (ROOT / "plugins" / "file_branding.py").exists()
    admin_source = (ROOT / "plugins" / "admin.py").read_text(encoding="utf-8")
    bot_source = (ROOT / "bot.py").read_text(encoding="utf-8")
    indexer_source = (ROOT / "plugins" / "realtime_indexer.py").read_text(encoding="utf-8")
    assert "File Branding" not in admin_source
    assert "file_branding_worker" not in bot_source
    assert "enqueue_file_branding" not in indexer_source


def test_overlapping_admin_features_have_one_clear_name_and_entry_point():
    source = (ROOT / "plugins" / "admin.py").read_text(encoding="utf-8")
    assert source.count('(\"📊 Analytics\", callback_data=\"admin_stats\")') == 1
    assert "Public Updates" not in source
    assert "Announcement Channel" not in source
    assert "File Captions" not in source
    assert '(\"📰 New Releases Channel\", \"releases_channel_menu\")' in source
    assert '(\"🎫 Request Inbox\", \"edit_requestchannel\")' in source
    assert '(\"💬 Request Group\", \"edit_maingroup\")' in source


def test_cached_media_delivery_has_one_shared_implementation():
    combined = "\n".join(
        (ROOT / "plugins" / name).read_text(encoding="utf-8")
        for name in ("filter.py", "start.py", "req_fsub.py")
    )
    assert combined.count("client.send_cached_media(") == 1
    assert "async def deliver_cached_file" in combined


@pytest.mark.asyncio
async def test_prompt_result_reuses_existing_panel():
    admin_id = 7001
    await clear_state(admin_id)
    await set_state(
        admin_id,
        "example",
        prompt_chat_id=123,
        prompt_message_id=456,
        return_text="Old menu",
    )
    client = SimpleNamespace(edit_message_text=AsyncMock())
    fallback = SimpleNamespace(reply_text=AsyncMock(), delete=AsyncMock())

    await finish_prompt(
        client,
        admin_id,
        "✅ Saved",
        back_callback="back_to_admin",
        back_label="‹ Control Center",
        fallback_message=fallback,
    )

    client.edit_message_text.assert_awaited_once()
    fallback.reply_text.assert_not_awaited()
    fallback.delete.assert_awaited_once()
    assert await get_state(admin_id) is None


@pytest.mark.asyncio
async def test_cancel_restores_previous_panel_without_cancelled_bubble():
    admin_id = 7002
    await clear_state(admin_id)
    await set_state(
        admin_id,
        "example",
        prompt_chat_id=123,
        prompt_message_id=456,
        return_text="Preferences & Backup",
        return_markup="buttons",
        return_entities="formatting",
    )
    client = SimpleNamespace(edit_message_text=AsyncMock())
    fallback = SimpleNamespace(reply_text=AsyncMock())

    assert await restore_prompt(client, admin_id, fallback_message=fallback)
    client.edit_message_text.assert_awaited_once_with(
        123,
        456,
        "Preferences & Backup",
        reply_markup="buttons",
        entities="formatting",
    )
    fallback.reply_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_fallback_keeps_menu_buttons_and_formatting():
    admin_id = 7003
    await clear_state(admin_id)
    await set_state(
        admin_id,
        "example",
        prompt_chat_id=123,
        prompt_message_id=456,
        return_text="File Manager",
        return_markup="buttons",
        return_entities="formatting",
    )
    client = SimpleNamespace(
        edit_message_text=AsyncMock(side_effect=RuntimeError("message gone"))
    )
    fallback = SimpleNamespace(reply_text=AsyncMock())

    assert await restore_prompt(client, admin_id, fallback_message=fallback)
    fallback.reply_text.assert_awaited_once_with(
        "File Manager",
        reply_parameters=None,
        reply_markup="buttons",
        entities="formatting",
    )
