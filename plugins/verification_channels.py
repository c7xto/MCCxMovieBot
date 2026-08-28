"""Validation and normalization for administrator-configured gate channels."""

from dataclasses import dataclass
from urllib.parse import urlparse

from pyrogram.enums import ChatMemberStatus, ChatType


class ChannelConfigurationError(ValueError):
    pass


@dataclass(frozen=True)
class VerifiedChannel:
    chat_id: int
    link: str
    title: str


def _telegram_link(value: str) -> str | None:
    candidate = value.strip().rstrip("/.,")
    if candidate.casefold().startswith(("t.me/", "telegram.me/")):
        candidate = f"https://{candidate}"
    parsed = urlparse(candidate)
    if parsed.scheme.casefold() not in {"http", "https"}:
        return None
    if parsed.netloc.casefold() not in {"t.me", "www.t.me", "telegram.me", "www.telegram.me"}:
        return None
    return f"https://t.me/{parsed.path.strip('/')}" if parsed.path.strip("/") else None


def _public_username_from_link(link: str) -> str | None:
    path = urlparse(link).path.strip("/")
    segments = [segment for segment in path.split("/") if segment]
    if not segments:
        return None
    if segments[0].casefold() == "s" and len(segments) > 1:
        segments.pop(0)
    username = segments[0]
    if username.startswith("+") or username.casefold() in {"joinchat", "c"}:
        return None
    if not username.replace("_", "").isalnum():
        return None
    return username


def _is_private_invite(link: str) -> bool:
    path = urlparse(link).path.strip("/")
    return path.startswith("+") or path.casefold().startswith("joinchat/")


def parse_request_fsub_input(raw: str) -> tuple[int | str, str | None]:
    """Parse a public identifier or ``numeric-id private-invite`` input."""
    parts = raw.strip().split()
    if not parts:
        raise ChannelConfigurationError("Send a channel link, @username, or numeric chat ID.")

    normalized_links = {part: _telegram_link(part) for part in parts}
    private_links = [
        link for link in normalized_links.values() if link and _is_private_invite(link)
    ]
    identifiers = [
        part
        for part in parts
        if not (normalized_links.get(part) and _is_private_invite(normalized_links[part]))
    ]
    if private_links and len(private_links) != 1:
        raise ChannelConfigurationError("Provide exactly one private invite link.")
    if private_links and not identifiers:
        raise ChannelConfigurationError(
            "Telegram cannot identify a private chat from its invite link alone. "
            "Send the numeric ID and link together, for example: "
            "-1001234567890 https://t.me/+xxxx"
        )
    if len(identifiers) != 1:
        raise ChannelConfigurationError("Send exactly one channel or group.")

    identifier_text = identifiers[0].rstrip("/")
    public_link = _telegram_link(identifier_text)
    if public_link:
        username = _public_username_from_link(public_link)
        if not username:
            raise ChannelConfigurationError(
                "That link does not contain a public username. For a private chat, "
                "send its numeric ID and invite link together."
            )
        identifier: int | str = f"@{username}"
    elif identifier_text.lstrip("-").isdigit():
        identifier = int(identifier_text)
    elif identifier_text.startswith("@") and len(identifier_text) > 1:
        identifier = identifier_text
    else:
        raise ChannelConfigurationError(
            "Use @username, t.me/username, a numeric ID, or numeric ID plus private invite link."
        )
    if private_links and not isinstance(identifier, int):
        raise ChannelConfigurationError(
            "Pair a private invite link with its numeric chat ID, not with a public username."
        )
    return identifier, private_links[0] if private_links else None


async def resolve_channel_id(client, raw: str) -> int:
    """Resolve admin removal input to the numeric identity stored in MongoDB."""
    identifier, _private_link = parse_request_fsub_input(raw)
    if isinstance(identifier, int):
        return identifier
    try:
        chat = await client.get_chat(identifier)
    except Exception as exc:
        raise ChannelConfigurationError(
            "I could not find that public channel or group. Check the username and try again."
        ) from exc
    return int(chat.id)


async def resolve_request_fsub_channel(client, raw: str) -> VerifiedChannel:
    """Resolve a Telegram chat and prove the bot can check its members."""
    identifier, supplied_private_link = parse_request_fsub_input(raw)
    try:
        chat = await client.get_chat(identifier)
    except Exception as exc:
        raise ChannelConfigurationError(
            "I could not find that channel or group. For private chats, send the "
            "numeric ID while the bot is already an administrator there."
        ) from exc

    chat_type = getattr(chat, "type", None)
    if chat_type and chat_type not in {ChatType.CHANNEL, ChatType.SUPERGROUP, ChatType.GROUP}:
        raise ChannelConfigurationError("That Telegram target is not a channel or group.")

    try:
        bot_member = await client.get_chat_member(chat.id, client.me.id)
    except Exception as exc:
        raise ChannelConfigurationError(
            "I found the chat but could not check its members. Add the bot as an "
            "administrator, then try again."
        ) from exc

    if bot_member.status not in {
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.OWNER,
    }:
        raise ChannelConfigurationError(
            "The bot must be an administrator in the verification channel."
        )

    chat_id = int(chat.id)
    username = getattr(chat, "username", None)
    if username:
        link = f"https://t.me/{username}"
    else:
        link = None
        try:
            generated = await client.create_chat_invite_link(
                chat_id,
                creates_join_request=False,
            )
            link = generated.invite_link
        except Exception:
            link = supplied_private_link
        if not link:
            raise ChannelConfigurationError(
                "The chat is private and I could not create its join link. Grant the bot "
                "Invite Users permission, or send: numeric ID + existing private invite link."
            )

    return VerifiedChannel(
        chat_id=chat_id,
        link=link,
        title=str(getattr(chat, "title", None) or username or chat_id),
    )
