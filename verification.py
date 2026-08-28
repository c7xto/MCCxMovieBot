"""Shared tri-state verification primitives for Telegram membership gates."""

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum

from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import FloodWait, UserNotParticipant


logger = logging.getLogger(__name__)
_MAX_INLINE_FLOOD_WAIT = 5


class VerificationStatus(str, Enum):
    PASS = "pass"
    DENY = "deny"
    INDETERMINATE = "indeterminate"


@dataclass(frozen=True)
class VerificationResult:
    status: VerificationStatus
    reason: str = ""
    retry_after: int | None = None

    @property
    def passed(self) -> bool:
        return self.status is VerificationStatus.PASS

    @classmethod
    def allow(cls, reason: str = "verified"):
        return cls(VerificationStatus.PASS, reason)

    @classmethod
    def deny(cls, reason: str = "not_verified"):
        return cls(VerificationStatus.DENY, reason)

    @classmethod
    def indeterminate(cls, reason: str, retry_after: int | None = None):
        return cls(VerificationStatus.INDETERMINATE, reason, retry_after)


def verification_unavailable_message(result: VerificationResult | None = None) -> str:
    if result and result.retry_after:
        return f"Verification is temporarily unavailable. Try again in {result.retry_after} seconds."
    return "Verification is temporarily unavailable. Please try again shortly."


async def check_channel_membership(
    client,
    channel_id,
    user_id: int,
    *,
    allow_pending_request: bool = False,
) -> VerificationResult:
    """Check one channel without ever converting dependency errors to success."""
    channel_text = str(channel_id).strip() if channel_id is not None else ""
    if not channel_text:
        return VerificationResult.indeterminate("missing_channel_id")
    channel = (
        int(channel_text)
        if channel_text.lstrip("-").isdigit()
        else channel_text
    )

    for attempt in range(2):
        try:
            member = await client.get_chat_member(channel, user_id)
            status = member.status
            if status is ChatMemberStatus.RESTRICTED and not bool(
                getattr(member, "is_member", True)
            ):
                return VerificationResult.deny("restricted_not_a_member")
            if status in {
                ChatMemberStatus.MEMBER,
                ChatMemberStatus.ADMINISTRATOR,
                ChatMemberStatus.OWNER,
                ChatMemberStatus.RESTRICTED,
            }:
                return VerificationResult.allow("telegram_membership")
            status_text = str(status).upper()
            if allow_pending_request and (
                "PENDING" in status_text or "REQUEST" in status_text
            ):
                return VerificationResult.allow("pending_join_request")
            if status in {
                ChatMemberStatus.LEFT,
                ChatMemberStatus.BANNED,
            } or status_text.endswith(("KICKED", "LEFT")):
                return VerificationResult.deny("not_a_member")
            return VerificationResult.indeterminate(
                f"unexpected_membership_status:{status_text}"
            )
        except UserNotParticipant:
            return VerificationResult.deny("user_not_participant")
        except FloodWait as exc:
            wait_seconds = max(1, int(getattr(exc, "value", 1)))
            if attempt == 0 and wait_seconds <= _MAX_INLINE_FLOOD_WAIT:
                logger.warning(
                    "verification_flood_wait channel=%s wait=%s retry=1",
                    channel_id,
                    wait_seconds,
                )
                await asyncio.sleep(wait_seconds)
                continue
            logger.warning(
                "verification_indeterminate channel=%s reason=flood_wait wait=%s",
                channel_id,
                wait_seconds,
            )
            return VerificationResult.indeterminate("flood_wait", wait_seconds)
        except Exception as exc:
            logger.warning(
                "verification_indeterminate channel=%s error_type=%s",
                channel_id,
                type(exc).__name__,
            )
            return VerificationResult.indeterminate(
                f"telegram_error:{type(exc).__name__}"
            )

    return VerificationResult.indeterminate("retry_exhausted")
