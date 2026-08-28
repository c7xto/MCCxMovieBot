"""Redis-backed short-lived admin prompt sessions."""

import time

from pyrogram.enums import MessageEntityType
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, MessageEntity

from database.redis_client import redis_state


STATE_TIMEOUT = 300


def _markup_to_data(markup):
    if markup is None:
        return None
    if not hasattr(markup, "inline_keyboard"):
        return {"_raw": markup}
    rows = []
    for row in getattr(markup, "inline_keyboard", []) or []:
        buttons = []
        for button in row:
            item = {"text": str(getattr(button, "text", ""))}
            for field in (
                "callback_data",
                "url",
                "switch_inline_query",
                "switch_inline_query_current_chat",
            ):
                value = getattr(button, field, None)
                if value is not None:
                    item[field] = value
            buttons.append(item)
        rows.append(buttons)
    return rows


def _markup_from_data(rows):
    if not rows:
        return None
    if isinstance(rows, dict) and "_raw" in rows:
        return rows["_raw"]
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(**button) for button in row] for row in rows]
    )


def _entities_to_data(entities):
    if entities is None:
        return None
    if not isinstance(entities, (list, tuple)):
        return {"_raw": entities}
    serialized = []
    for entity in entities:
        entity_type = getattr(entity, "type", None)
        value = getattr(entity_type, "value", entity_type)
        serialized.append(
            {
                "type": str(value),
                "offset": int(getattr(entity, "offset", 0)),
                "length": int(getattr(entity, "length", 0)),
                "url": getattr(entity, "url", None),
                "language": getattr(entity, "language", None),
                "custom_emoji_id": getattr(entity, "custom_emoji_id", None),
            }
        )
    return {"items": serialized}


def _entities_from_data(data):
    if not data:
        return None
    if "_raw" in data:
        return data["_raw"]
    entities = []
    for item in data.get("items", []):
        try:
            entity_type = MessageEntityType(item["type"])
        except ValueError:
            continue
        entities.append(
            MessageEntity(
                type=entity_type,
                offset=item["offset"],
                length=item["length"],
                url=item.get("url"),
                language=item.get("language"),
                custom_emoji_id=item.get("custom_emoji_id"),
            )
        )
    return entities or None


async def get_state(admin_id: int):
    entry = await get_state_context(admin_id)
    return entry.get("state") if entry else None


async def get_state_context(admin_id: int):
    entry = await redis_state.get_json("admin-state", admin_id)
    if not entry:
        return None
    entry["return_markup"] = _markup_from_data(entry.pop("return_markup_data", None))
    entry["return_entities"] = _entities_from_data(
        entry.pop("return_entities_data", None)
    )
    return entry


async def set_state(
    admin_id: int,
    state: str,
    *,
    prompt_chat_id=None,
    prompt_message_id=None,
    return_text=None,
    return_markup=None,
    return_entities=None,
):
    current = await get_state_context(admin_id) or {}
    payload = {
        "state": state,
        "timestamp": time.time(),
        "prompt_chat_id": prompt_chat_id if prompt_chat_id is not None else current.get("prompt_chat_id"),
        "prompt_message_id": prompt_message_id
        if prompt_message_id is not None
        else current.get("prompt_message_id"),
        "return_text": return_text if return_text is not None else current.get("return_text"),
        "return_markup_data": _markup_to_data(
            return_markup if return_markup is not None else current.get("return_markup")
        ),
        "return_entities_data": _entities_to_data(
            return_entities
            if return_entities is not None
            else current.get("return_entities")
        ),
    }
    await redis_state.set_json("admin-state", admin_id, payload, STATE_TIMEOUT)


async def clear_state(admin_id: int):
    await redis_state.delete("admin-state", admin_id)
