"""Reusable animated search indicator for DM, deep-link and group searches."""

import asyncio
import gzip
import io
import json
import logging

from pyrogram.enums import ParseMode

logger = logging.getLogger(__name__)

_sticker_file_id = None

# A tiny Telegram animated-sticker (Lottie/TGS) drawn locally.  The glass
# gently rotates and pulses, so searches never leave users staring at a stale
# text-only progress message.  Keeping the source as JSON avoids shipping an
# opaque binary asset; it is gzip-compressed in memory when first uploaded.
_MAGNIFYING_GLASS_LOTTIE = {
    "v": "5.7.4", "fr": 30, "ip": 0, "op": 60, "w": 512, "h": 512,
    "nm": "Searching", "ddd": 0, "assets": [],
    "layers": [{
        "ddd": 0, "ind": 1, "ty": 4, "nm": "Magnifying Glass", "sr": 1,
        "ks": {
            "o": {"a": 0, "k": 100},
            "r": {"a": 1, "k": [
                {"t": 0, "s": [-12], "e": [12]},
                {"t": 30, "s": [12], "e": [-12]},
                {"t": 60, "s": [-12]},
            ]},
            "p": {"a": 0, "k": [256, 256, 0]},
            "a": {"a": 0, "k": [0, 0, 0]},
            "s": {"a": 1, "k": [
                {"t": 0, "s": [92, 92, 100], "e": [104, 104, 100]},
                {"t": 30, "s": [104, 104, 100], "e": [92, 92, 100]},
                {"t": 60, "s": [92, 92, 100]},
            ]},
        },
        "ao": 0,
        "shapes": [
            {"ty": "gr", "nm": "Lens", "it": [
                {"ty": "el", "d": 1, "p": {"a": 0, "k": [-36, -36]},
                 "s": {"a": 0, "k": [230, 230]}, "nm": "Lens Circle"},
                {"ty": "st", "c": {"a": 0, "k": [0.08, 0.62, 0.96, 1]},
                 "o": {"a": 0, "k": 100}, "w": {"a": 0, "k": 30},
                 "lc": 2, "lj": 2, "ml": 4, "nm": "Blue Stroke"},
                {"ty": "tr", "p": {"a": 0, "k": [0, 0]},
                 "a": {"a": 0, "k": [0, 0]}, "s": {"a": 0, "k": [100, 100]},
                 "r": {"a": 0, "k": 0}, "o": {"a": 0, "k": 100}, "sk": {"a": 0, "k": 0},
                 "sa": {"a": 0, "k": 0}, "nm": "Lens Transform"},
            ]},
            {"ty": "gr", "nm": "Handle", "it": [
                {"ty": "rc", "d": 1, "p": {"a": 0, "k": [82, 82]},
                 "s": {"a": 0, "k": [34, 155]}, "r": {"a": 0, "k": 17},
                 "nm": "Handle Bar"},
                {"ty": "fl", "c": {"a": 0, "k": [0.08, 0.62, 0.96, 1]},
                 "o": {"a": 0, "k": 100}, "r": 1, "nm": "Blue Fill"},
                {"ty": "tr", "p": {"a": 0, "k": [0, 0]},
                 "a": {"a": 0, "k": [0, 0]}, "s": {"a": 0, "k": [100, 100]},
                 "r": {"a": 0, "k": -45}, "o": {"a": 0, "k": 100}, "sk": {"a": 0, "k": 0},
                 "sa": {"a": 0, "k": 0}, "nm": "Handle Transform"},
            ]},
        ],
        "ip": 0, "op": 60, "st": 0, "bm": 0,
    }],
}


def _sticker_stream() -> io.BytesIO:
    raw = json.dumps(_MAGNIFYING_GLASS_LOTTIE, separators=(",", ":")).encode()
    stream = io.BytesIO(gzip.compress(raw, compresslevel=9))
    stream.name = "searching.tgs"
    return stream


async def show_search_indicator(client, chat_id):
    """Show the animated sticker, falling back to an editable text message."""
    global _sticker_file_id
    try:
        source = _sticker_file_id or _sticker_stream()
        message = await asyncio.wait_for(
            client.send_sticker(chat_id=chat_id, sticker=source, emoji="🔎"),
            timeout=5,
        )
        if not _sticker_file_id and getattr(message, "sticker", None):
            _sticker_file_id = message.sticker.file_id
        return message
    except Exception as exc:
        logger.warning("Animated search indicator unavailable; using text: %s", exc)
        return await client.send_message(
            chat_id,
            "🔎 <b>Searching your library…</b>\n<i>Finding the best matches</i>",
            parse_mode=ParseMode.HTML,
        )


async def remove_search_indicator(message):
    if not message:
        return
    try:
        await message.delete()
    except Exception:
        pass
