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
            "branding_admin.py",
        )
    )

    assert "Type /cancel" not in sources
    assert "Action Cancelled" not in sources
    assert "_Select an action._" not in sources
    assert "🔙" not in sources


@pytest.mark.asyncio
async def test_prompt_result_reuses_existing_panel():
    admin_id = 7001
    clear_state(admin_id)
    set_state(
        admin_id,
        "example",
        prompt_chat_id=123,
        prompt_message_id=456,
        return_text="Old menu",
    )
    client = SimpleNamespace(edit_message_text=AsyncMock())
    fallback = SimpleNamespace(reply_text=AsyncMock())

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
    assert get_state(admin_id) is None


@pytest.mark.asyncio
async def test_cancel_restores_previous_panel_without_cancelled_bubble():
    admin_id = 7002
    clear_state(admin_id)
    set_state(
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
    clear_state(admin_id)
    set_state(
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
