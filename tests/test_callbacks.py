from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pyrogram.errors import QueryIdInvalid

from plugins.callbacks import answer_callback_safely


@pytest.mark.asyncio
async def test_expired_callback_is_harmless():
    callback = SimpleNamespace(
        data="old-button",
        answer=AsyncMock(side_effect=QueryIdInvalid()),
    )
    assert await answer_callback_safely(callback) is False


@pytest.mark.asyncio
async def test_fresh_callback_is_answered():
    callback = SimpleNamespace(data="button", answer=AsyncMock())
    assert await answer_callback_safely(callback, "Working…") is True
    callback.answer.assert_awaited_once_with("Working…", show_alert=False)


@pytest.mark.asyncio
async def test_idless_mock_callbacks_cannot_collide_via_reused_memory_ids():
    first = SimpleNamespace(data="button", answer=AsyncMock())
    second = SimpleNamespace(data="button", answer=AsyncMock())
    assert await answer_callback_safely(first) is True
    assert await answer_callback_safely(second) is True
    first.answer.assert_awaited_once()
    second.answer.assert_awaited_once()
