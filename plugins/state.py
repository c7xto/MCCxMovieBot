"""
Shared admin input state manager.
All plugin files import from here so ADMIN_STATE is a single dict in memory.
Previously each file had its own state — this caused state to be lost when
a different plugin handled the next message.
"""
import time

ADMIN_STATE = {}   # {user_id: (state_string, unix_timestamp)}
STATE_TIMEOUT = 300  # 5 minutes — stale sessions auto-expire


def get_state(admin_id: int):
    """Returns current state string if session alive, else None."""
    entry = ADMIN_STATE.get(admin_id)
    if not entry:
        return None
    state, ts = entry
    if time.time() - ts > STATE_TIMEOUT:
        ADMIN_STATE.pop(admin_id, None)
        return None
    return state


def set_state(admin_id: int, state: str):
    ADMIN_STATE[admin_id] = (state, time.time())


def clear_state(admin_id: int):
    ADMIN_STATE.pop(admin_id, None)
