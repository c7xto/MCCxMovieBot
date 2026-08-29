from pyrogram.types import InlineKeyboardButton

from plugins.admin import _analytics_markup, _cluster_status_line
from plugins.broadcast import _broadcast_controls
from plugins.bulk_indexer import _index_controls
from plugins.mobile_ui import (
    MOBILE_BUTTON_WIDTH,
    MOBILE_ROW_WIDTH,
    MobileInlineKeyboardMarkup,
    display_width,
    responsive_rows,
)
from plugins.req_fsub import _gates_markup
from plugins.start import _build_start_ui


def _button(label):
    return InlineKeyboardButton(label, callback_data=label)


def _assert_mobile_rows(markup):
    for row in markup.inline_keyboard:
        assert 1 <= len(row) <= 2
        if len(row) == 2:
            widths = [display_width(button.text) for button in row]
            assert max(widths) <= MOBILE_BUTTON_WIDTH
            assert sum(widths) <= MOBILE_ROW_WIDTH


def test_short_action_pair_stays_compact():
    rows = responsive_rows([[_button("📊 Stats"), _button("🩺 Health")]])
    assert len(rows) == 1
    assert len(rows[0]) == 2


def test_crowded_action_pair_stacks_for_phone_width():
    rows = responsive_rows(
        [[_button("📰 New Releases Channel"), _button("🛠 System Log Channel")]]
    )
    assert [len(row) for row in rows] == [1, 1]


def test_more_than_two_actions_are_reflowed_without_reordering():
    labels = ["One", "Two", "Three", "Four", "Five"]
    markup = MobileInlineKeyboardMarkup([[_button(label) for label in labels]])
    assert [button.text for row in markup.inline_keyboard for button in row] == labels
    _assert_mobile_rows(markup)


def test_core_bot_keyboards_share_the_mobile_policy():
    _, home = _build_start_ui(
        {}, "Tester", 100, "examplebot", "https://t.me/releases", "https://t.me/requests"
    )
    markups = [
        home,
        _analytics_markup(),
        _index_controls(-1001),
        _broadcast_controls({"_id": "job", "status": "running"}),
        _gates_markup(
            [{"label": "A Very Long Verification Channel", "link": "https://t.me/example"}],
            "file-id",
        ),
    ]
    for markup in markups:
        _assert_mobile_rows(markup)


def test_cluster_status_uses_two_short_mobile_lines():
    text = _cluster_status_line({"cluster": 4, "state": "near_limit", "size_mb": 440.0})
    lines = text.splitlines()
    assert len(lines) == 2
    assert "Cluster 4" in lines[0]
    assert "440.0 MB" in lines[1]

