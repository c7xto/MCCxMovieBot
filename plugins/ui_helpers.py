"""Small UI primitives shared by the Telegram admin panels."""

import logging

from pyrogram import Client, filters
from pyrogram.errors import MessageNotModified
from pyrogram.types import InlineKeyboardButton
from plugins.mobile_ui import MobileInlineKeyboardMarkup as InlineKeyboardMarkup

from plugins.callbacks import answer_callback_safely
from plugins.state import clear_state, get_state_context, set_state
from utils import ADMIN_ID


logger = logging.getLogger(__name__)


def cancel_button(label="✕ Cancel"):
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data="ui_prompt_cancel")]])


async def begin_prompt(callback, state: str, text: str):
    """Turn the current menu into an input prompt and remember its old view."""
    message = callback.message
    await set_state(
        callback.from_user.id,
        state,
        prompt_chat_id=message.chat.id,
        prompt_message_id=message.id,
        return_text=message.text or message.caption or "",
        return_markup=message.reply_markup,
        return_entities=(
            getattr(message, "entities", None)
            or getattr(message, "caption_entities", None)
        ),
    )
    await answer_callback_safely(callback)
    await message.edit_text(text, reply_markup=cancel_button())


async def delete_prompt_input(message) -> bool:
    """Best-effort cleanup for an admin value already captured by a prompt."""
    delete = getattr(message, "delete", None) if message is not None else None
    if not callable(delete):
        return False
    try:
        await delete()
        return True
    except Exception as exc:
        logger.info("Could not remove consumed prompt input: %s", type(exc).__name__)
        return False


async def restore_prompt(client, admin_id: int, fallback_message=None) -> bool:
    """Restore the menu saved before an input prompt without a new bot bubble."""
    context = await get_state_context(admin_id)
    await clear_state(admin_id)
    if not context:
        return False

    chat_id = context.get("prompt_chat_id")
    message_id = context.get("prompt_message_id")
    return_text = context.get("return_text")
    if chat_id is None or message_id is None or return_text is None:
        return False
    try:
        kwargs = {"reply_markup": context.get("return_markup")}
        if context.get("return_entities"):
            kwargs["entities"] = context["return_entities"]
        await client.edit_message_text(chat_id, message_id, return_text, **kwargs)
        return True
    except MessageNotModified:
        return True
    except Exception as exc:
        logger.info("Could not restore prompt panel: %s", type(exc).__name__)
        if fallback_message is not None:
            try:
                await fallback_message.reply_text(
                    return_text,
                    reply_parameters=None,
                    **kwargs,
                )
                return True
            except Exception:
                pass
        return False


async def finish_prompt(
    client,
    admin_id: int,
    text: str,
    *,
    back_callback: str,
    back_label: str,
    fallback_message=None,
    reply_markup=None,
    parse_mode=None,
):
    """Replace the active prompt with its result and a concise back button."""
    context = await get_state_context(admin_id)
    await clear_state(admin_id)
    markup = reply_markup or InlineKeyboardMarkup(
        [[InlineKeyboardButton(back_label, callback_data=back_callback)]]
    )
    edit_kwargs = {"reply_markup": markup}
    if parse_mode is not None:
        edit_kwargs["parse_mode"] = parse_mode
    if context and context.get("prompt_chat_id") and context.get("prompt_message_id"):
        try:
            await client.edit_message_text(
                context["prompt_chat_id"],
                context["prompt_message_id"],
                text,
                **edit_kwargs,
            )
            await delete_prompt_input(fallback_message)
            return
        except Exception as exc:
            logger.info("Could not finish prompt in place: %s", type(exc).__name__)
    if fallback_message is not None:
        await fallback_message.reply_text(
            text,
            reply_parameters=None,
            **edit_kwargs,
        )
        await delete_prompt_input(fallback_message)


@Client.on_callback_query(filters.regex(r"^ui_prompt_cancel$") & filters.user(ADMIN_ID))
async def cancel_active_prompt(client, callback):
    await answer_callback_safely(callback)
    if not await restore_prompt(client, callback.from_user.id):
        await callback.message.edit_text(
            "No active action.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("‹ Control Center", callback_data="back_to_admin")]]
            ),
        )
