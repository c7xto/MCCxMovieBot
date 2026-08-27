"""Central authorization policy for every public user action boundary."""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AccessDecision:
    allowed: bool
    action: str
    config: dict
    reason: str | None = None
    message: str | None = None


async def authorize_user_action(
    user_id: int | None,
    action: str,
    config: dict | None = None,
    *,
    repository: Any = None,
    admin_ids=None,
) -> AccessDecision:
    """Return the canonical ban/maintenance decision for a user action."""
    if repository is None:
        from database.db import db as repository
    if admin_ids is None:
        from utils import ADMIN_ID

        admin_ids = ADMIN_ID

    current_config = config if config is not None else await repository.get_config()
    if user_id is None:
        return AccessDecision(
            False,
            action,
            current_config,
            reason="missing_user",
            message="This action requires an identifiable Telegram user.",
        )

    if await repository.is_banned(user_id):
        return AccessDecision(
            False,
            action,
            current_config,
            reason="banned",
            message="You are banned from using this bot.",
        )

    if current_config.get("maintenance_mode") and user_id not in set(admin_ids):
        return AccessDecision(
            False,
            action,
            current_config,
            reason="maintenance",
            message=str(
                current_config.get(
                    "maintenance_message", "Bot is under maintenance. Back soon!"
                )
            ),
        )

    return AccessDecision(True, action, current_config)


async def enforce_user_action(event, action: str, config: dict | None = None) -> AccessDecision:
    """Authorize an event and render its denial through the matching UI API."""
    user = getattr(event, "from_user", None)
    decision = await authorize_user_action(getattr(user, "id", None), action, config)
    if decision.allowed:
        return decision

    if hasattr(event, "answer") and getattr(event, "message", None) is not None:
        await event.answer((decision.message or "Action denied.")[:180], show_alert=True)
    elif hasattr(event, "reply_text"):
        await event.reply_text(decision.message or "Action denied.", reply_parameters=None)
    return decision
