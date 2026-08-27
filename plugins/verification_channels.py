"""Validation and normalization for administrator-configured gate channels."""

from dataclasses import dataclass

from pyrogram.enums import ChatMemberStatus


class ChannelConfigurationError(ValueError):
    pass


@dataclass(frozen=True)
class VerifiedChannel:
    chat_id: int
    link: str
    title: str


def parse_request_fsub_input(raw: str) -> tuple[int | str, str | None]:
    """Parse public identifiers or ``numeric-id private-invite`` input."""
    parts = raw.strip().split()
    if not parts:
        raise ChannelConfigurationError("Channel input is empty.")

    private_links = [part for part in parts if part.startswith("https://t.me/+")]
    identifiers = [part for part in parts if part not in private_links]
    if private_links and len(private_links) != 1:
        raise ChannelConfigurationError("Provide exactly one private invite link.")
    if private_links and not identifiers:
        raise ChannelConfigurationError(
            "A private invite link must be accompanied by its numeric -100... channel ID."
        )
    if len(identifiers) != 1:
        raise ChannelConfigurationError("Provide exactly one channel identifier.")

    identifier_text = identifiers[0].rstrip("/")
    if identifier_text.startswith("https://t.me/"):
        username = identifier_text.rsplit("/", 1)[-1]
        if not username or username.startswith("+"):
            raise ChannelConfigurationError("Invalid public channel URL.")
        identifier: int | str = f"@{username}"
    elif identifier_text.lstrip("-").isdigit():
        identifier = int(identifier_text)
    elif identifier_text.startswith("@") and len(identifier_text) > 1:
        identifier = identifier_text
    else:
        raise ChannelConfigurationError(
            "Use @username, https://t.me/username, or a numeric -100... ID."
        )
    return identifier, private_links[0] if private_links else None


async def resolve_request_fsub_channel(client, raw: str) -> VerifiedChannel:
    """Resolve a channel and prove the bot can perform membership checks."""
    identifier, supplied_private_link = parse_request_fsub_input(raw)
    try:
        chat = await client.get_chat(identifier)
        bot_member = await client.get_chat_member(chat.id, client.me.id)
    except Exception as exc:
        raise ChannelConfigurationError(
            f"Telegram could not verify this channel ({type(exc).__name__})."
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
        if not supplied_private_link:
            raise ChannelConfigurationError(
                "Private channels require: -100CHANNEL_ID https://t.me/+INVITE_LINK"
            )
        try:
            generated = await client.create_chat_invite_link(
                chat_id,
                creates_join_request=True,
            )
            link = generated.invite_link
        except Exception as exc:
            raise ChannelConfigurationError(
                f"Could not generate a verified invite link ({type(exc).__name__})."
            ) from exc

    return VerifiedChannel(
        chat_id=chat_id,
        link=link,
        title=str(getattr(chat, "title", None) or username or chat_id),
    )
