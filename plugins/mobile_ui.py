"""Shared responsive layout rules for every Telegram inline keyboard."""

from __future__ import annotations

import unicodedata

from pyrogram.types import InlineKeyboardMarkup as _InlineKeyboardMarkup


MOBILE_ROW_WIDTH = 34
MOBILE_BUTTON_WIDTH = 18


def display_width(value: str) -> int:
    """Estimate how much horizontal room a Telegram button label needs."""
    width = 0
    for character in str(value or ""):
        if character in {"\u200d", "\ufe0f"} or unicodedata.combining(character):
            continue
        width += 2 if unicodedata.east_asian_width(character) in {"W", "F"} else 1
    return width


def responsive_rows(rows) -> list[list]:
    """Return keyboard rows that remain readable on narrow phone screens.

    Telegram controls the final pixel dimensions. The bot can still prevent
    two long labels from being squeezed into unreadable side-by-side tiles.
    Short action pairs stay compact; crowded pairs are safely reflowed.
    """
    normalized: list[list] = []
    for source_row in rows or []:
        row = [button for button in source_row if button is not None]
        while len(row) > 2:
            normalized.extend(responsive_rows([row[:2]]))
            row = row[2:]
        if not row:
            continue
        if len(row) == 2:
            widths = [display_width(getattr(button, "text", "")) for button in row]
            if max(widths) > MOBILE_BUTTON_WIDTH or sum(widths) > MOBILE_ROW_WIDTH:
                normalized.extend([[button] for button in row])
                continue
        normalized.append(row)
    return normalized


class MobileInlineKeyboardMarkup(_InlineKeyboardMarkup):
    """Drop-in InlineKeyboardMarkup with shared phone-safe row reflow."""

    def __init__(self, inline_keyboard):
        super().__init__(responsive_rows(inline_keyboard))

