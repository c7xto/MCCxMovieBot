"""Shared, short-lived admin prompt sessions.

Besides the action name, a session remembers the panel that opened the prompt.
That lets an inline Cancel button (or the ``/cancel`` fallback) restore the
previous panel instead of adding another untidy "Cancelled" message.
"""

import time

ADMIN_STATE = {}
STATE_TIMEOUT = 300  # 5 minutes — stale sessions auto-expire


def get_state(admin_id: int):
    """Returns current state string if session alive, else None."""
    entry = get_state_context(admin_id)
    if not entry:
        return None
    return entry["state"]


def get_state_context(admin_id: int):
    """Return the complete live prompt context, if one exists."""
    entry = ADMIN_STATE.get(admin_id)
    if not entry:
        return None

    # Compatibility with sessions created before an in-process update.
    if isinstance(entry, tuple):
        state, timestamp = entry
        entry = {"state": state, "timestamp": timestamp}
        ADMIN_STATE[admin_id] = entry

    if time.time() - float(entry.get("timestamp", 0)) > STATE_TIMEOUT:
        ADMIN_STATE.pop(admin_id, None)
        return None
    return entry


def set_state(
    admin_id: int,
    state: str,
    *,
    prompt_chat_id=None,
    prompt_message_id=None,
    return_text=None,
    return_markup=None,
    return_entities=None,
):
    current = get_state_context(admin_id) or {}
    ADMIN_STATE[admin_id] = {
        "state": state,
        "timestamp": time.time(),
        "prompt_chat_id": prompt_chat_id if prompt_chat_id is not None else current.get("prompt_chat_id"),
        "prompt_message_id": prompt_message_id
        if prompt_message_id is not None
        else current.get("prompt_message_id"),
        "return_text": return_text if return_text is not None else current.get("return_text"),
        "return_markup": return_markup if return_markup is not None else current.get("return_markup"),
        "return_entities": (
            return_entities
            if return_entities is not None
            else current.get("return_entities")
        ),
    }


def clear_state(admin_id: int):
    ADMIN_STATE.pop(admin_id, None)
