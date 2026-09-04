import os
import logging
import urllib.parse
import time
import secrets
from dotenv import load_dotenv
from plugins.access_policy import authorize_user_action, enforce_user_action
from plugins.callbacks import answer_callback_safely
from plugins.filter import route_menu
from plugins.search_indicator import show_search_indicator, remove_search_indicator
from plugins.workload import WorkloadRejected, enforce_search_rate_limits, search_slot, validate_search_query
from plugins.telegram_retry import send_message_resilient
from utils import (
    _no_preview,
    _html,
    callback_data,
    HELP_STEPS_EN,
    HELP_FOOTER_EN,
    html_user_mention,
)
from pyrogram import Client, filters
from pyrogram.types import CallbackQuery, InlineKeyboardButton, Message
from plugins.mobile_ui import MobileInlineKeyboardMarkup as InlineKeyboardMarkup
from pyrogram.enums import ParseMode, ChatAction
from database.db import db

load_dotenv()
logger = logging.getLogger(__name__)

# ── Shared UI icons ───────────────────────────────────────────────────────
# Shared icon set for this file, scoped here because it only belongs to these menus.
ICON_SEARCH = "🔍"
ICON_MOVIE = "🎬"
ICON_UPDATES = "📢"
ICON_SUCCESS = "✅"
ICON_FAIL = "❌"
ICON_REQUEST = "📝"

# All config (log channel, media, links) is read from MongoDB inside each
# handler via db.get_config() — no module-level env reads needed here.

# ── Language toggle ──────────────────────────────────────────────────────────
# Scoped translation: covers the home/welcome panel, Help menu, and the
# no-results message — the core chrome a first-time user sees before ever
# reaching a search result. Does NOT translate search results, file
# captions, or the admin panel (out of scope for this pass), and does not
# apply to an admin-customized welcome_text (that's a single free-text
# field with no per-language variants — the language toggle only affects
# the *default* welcome copy).
LANG_NAMES = {"en": "English", "ml": "മലയാളം"}

LANG_STRINGS = {
    "en": {
        "welcome_body": (
            "Search movies and series by title, year, language or quality.\n"
            "Choose the version you want and receive it here."
        ),
        "welcome_greeting": "Hello, {first_name} 👋",
        "files_counting": "{total_files:,} files available",
        "onboarding_title": "Find a file in three steps",
        "onboarding_steps": [
            "Type any movie or series name",
            "Tap the file you want",
            "It lands in this chat instantly",
        ],
        "help_steps": HELP_STEPS_EN,
        "help_footer": HELP_FOOTER_EN,
        "no_results": (
            "<b>No files found</b>\n\nWe couldn't find <code>{query}</code>.\n"
            "<blockquote>Try only the title, remove the year or language, "
            "or check the spelling.</blockquote>"
        ),
    },
    "ml": {
        "welcome_body": (
            "സിനിമകളും സീരീസുകളും വേഗത്തിൽ കണ്ടെത്താനുള്ള നിങ്ങളുടെ ലൈബ്രറി.\nതാഴെ പേര് അയച്ച് വേണ്ട പതിപ്പ് തിരഞ്ഞെടുക്കൂ."
        ),
        "welcome_greeting": "സ്വാഗതം, {first_name}",
        "files_counting": "{total_files:,} ഫയലുകൾ ഇപ്പോൾ ലഭ്യമാണ്.",
        "onboarding_title": "പുതിയതാണോ? ഇത്ര ലളിതം:",
        "onboarding_steps": [
            "ഏതെങ്കിലും സിനിമ/സീരീസ് പേര് ടൈപ്പ് ചെയ്യുക",
            "വേണ്ട ഫയൽ ടാപ്പ് ചെയ്യുക",
            "ഇത് ഉടൻ ഈ ചാറ്റിൽ എത്തും",
        ],
        "help_steps": [
            "സിനിമ/സീരീസ് പേര് ടൈപ്പ് ചെയ്യുക",
            "വേണ്ട ഫയൽ ടാപ്പ് ചെയ്യുക",
            "ഇത് ഉടൻ നിങ്ങളുടെ PM-ൽ ലഭിക്കും",
        ],
        "help_footer": "കിട്ടിയില്ലേ? Request ബട്ടൺ ഉപയോഗിക്കൂ — 24 മണിക്കൂറിനുള്ളിൽ അപ്‌ലോഡ് ചെയ്യാം.",
        "no_results": "<code>{query}</code> എന്നതിന് ഫലങ്ങളൊന്നും ലഭിച്ചില്ല\n\nഇത് ഇതുവരെ അപ്‌ലോഡ് ചെയ്തിട്ടില്ലായിരിക്കാം, അല്ലെങ്കിൽ അക്ഷരത്തെറ്റ് ഉണ്ടാകാം.",
    },
}


def _lang_button(lang: str) -> InlineKeyboardButton:
    """Shows the *other* language as the tap target, standard toggle UX."""
    next_lang = "ml" if lang == "en" else "en"
    return InlineKeyboardButton(f"🌐 {LANG_NAMES[next_lang]}", callback_data=f"toggle_lang#{next_lang}")


def _build_start_ui(
    config,
    mention,
    total_files,
    bot_username,
    update_link,
    group_link,
    is_new=False,
    first_name="",
    lang="en",
):
    """Shared welcome UI builder — used by /start and start_home callback."""
    strings = LANG_STRINGS.get(lang, LANG_STRINGS["en"])
    safe_name = _html(first_name or mention)
    default_welcome = (
        "<b>🎬 MCCx Movie Bot</b>\n"
        "<blockquote>" + strings["welcome_greeting"] + "\n" + strings["welcome_body"] + "</blockquote>\n"
        "<b>📚 " + strings["files_counting"] + "</b>"
    )
    # An admin-customized welcome_text is a single free-text field with no
    # per-language variant — the language toggle only swaps the *default*.
    configured_welcome = config.get("welcome_text") or ""
    legacy_markers = (
        "Your ultimate movie search bot is here",
        "900,000+ files available",
        "Just type the movie name and enjoy instant results",
    )
    # Automatically retire the old bundled welcome copy while preserving
    # genuinely customized admin text.
    raw = (
        default_welcome
        if not configured_welcome or any(m in configured_welcome for m in legacy_markers)
        else configured_welcome
    )
    try:
        # Both {mention} and {first_name} are accepted — admin-set welcome
        # text written before this update still uses {mention} and keeps
        # working exactly as before.
        text = raw.format(mention=safe_name, first_name=safe_name, total_files=total_files)
    except Exception:
        text = raw

    # Clean onboarding: first-time users get a short "how this works" block
    # appended after the (possibly admin-customized) welcome text; returning
    # users just see the normal welcome — no repeated hand-holding.
    if is_new:
        steps = "\n".join(f"{i}. {s}" for i, s in enumerate(strings["onboarding_steps"], 1))
        text += f"\n\n<blockquote><b>{strings['onboarding_title']}</b>\n{steps}</blockquote>"

    text += "\n\n<i>🔍 Send a title below to begin.</i>"

    buttons = []

    buttons.append(
        [
            InlineKeyboardButton("🔎 Search Guide", callback_data="help_menu"),
            _lang_button(lang),
        ]
    )
    discovery_row = []
    if group_link:
        discovery_row.append(InlineKeyboardButton("💬 Request Group", url=group_link))
    if update_link:
        discovery_row.append(InlineKeyboardButton("📢 New Releases", url=update_link))
    if discovery_row:
        buttons.append(discovery_row)
    buttons.append(
        [
            InlineKeyboardButton(
                "➕ Add Bot to Group",
                url=f"https://t.me/{bot_username}?startgroup=true",
            )
        ]
    )
    return text, InlineKeyboardMarkup(buttons)


async def _show_workload_rejection(client, status_msg, message):
    if getattr(status_msg, "sticker", None):
        chat_id = status_msg.chat.id
        await remove_search_indicator(status_msg)
        return await send_message_resilient(
            client, chat_id, message, route="start_workload_rejection"
        )
    return await status_msg.edit_text(message)


async def _execute_search(client, status_msg, query: str, config: dict, user_id=None, first_name="", release_key=None):
    """Runs a search and renders page 0 of results into status_msg.
    Shared by every /start deep-link search entry point (search_, req_
    fulfillment redirects, etc.) so they stay in sync with one implementation.
    """
    access = await authorize_user_action(user_id, "search", config)
    if not access.allowed:
        await status_msg.edit_text(access.message or "Action denied.")
        return
    config = access.config

    try:
        query = validate_search_query(query)
        await enforce_search_rate_limits(user_id)
    except WorkloadRejected as exc:
        return await _show_workload_rejection(client, status_msg, exc.public_message)

    try:
        await client.send_chat_action(status_msg.chat.id, ChatAction.TYPING)
    except Exception:
        pass

    try:
        async with search_slot("start_search"):
            release_after = None
            if release_key:
                from plugins.live_library import release_file_page

                results, release_after = await release_file_page(release_key)
            else:
                results = await db.get_search_results(query)
    except WorkloadRejected as exc:
        return await _show_workload_rejection(client, status_msg, exc.public_message)

    if not results:
        lang = await db.get_user_language(user_id) if user_id is not None else "en"
        strings = LANG_STRINGS.get(lang, LANG_STRINGS["en"])
        markup = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"{ICON_REQUEST} Request This Movie",
                        callback_data=callback_data("reqmovie#", query),
                    )
                ],
                [InlineKeyboardButton("⌂ Back to Home", callback_data="start_home")],
            ]
        )
        text = f"🔎 " + strings["no_results"].format(query=_html(query))
        if getattr(status_msg, "sticker", None):
            chat_id = status_msg.chat.id
            await remove_search_indicator(status_msg)
            return await send_message_resilient(
                client,
                chat_id,
                text,
                reply_markup=markup,
                parse_mode=ParseMode.HTML,
                route="start_no_results",
            )
        return await status_msg.edit_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)

    await db.clear_old_searches()
    session_id = secrets.token_urlsafe(9)
    session_data = {
        "results": results,
        "query": query,
        "speed": "0.001s",
        "time": time.time(),
        "auto_delete_time": int(config.get("auto_delete_time", 300)),
        "user_id": user_id,
        "first_name": first_name or "",
        "release_key": release_key,
        "release_after": release_after,
    }
    await db.save_search(session_id, session_data)
    return await route_menu(client, status_msg, session_id, 0)


async def _handle_file_link(client, message, file_obj_id: str, delete_seconds_override=None):
    """Deep-link payload: file_<obj_id> — direct file delivery (mainly the
    group-search "Open in PM" buttons), gated by the same unified
    Verification Gates check as the in-DM sendfile# button."""
    if not (await enforce_user_action(message, "file_delivery")).allowed:
        return
    file_data = await db.get_file(file_obj_id)
    if not file_data:
        return await message.reply_text(
            f"{ICON_FAIL} This file was deleted or is no longer available.", parse_mode=ParseMode.HTML
        )

    from plugins.req_fsub import check_verification_gates

    if not await check_verification_gates(client, message, file_obj_id):
        return

    access = await enforce_user_action(message, "file_delivery")
    if not access.allowed:
        return
    config = access.config
    if delete_seconds_override is not None:
        config = dict(config)
        config["auto_delete_time"] = max(60, min(3600, int(delete_seconds_override)))
    from plugins.filter import deliver_cached_file

    await deliver_cached_file(
        client,
        chat_id=message.chat.id,
        user_id=message.from_user.id,
        file_obj_id=file_obj_id,
        file_data=file_data,
        config=config,
        route="file_delivery_deep_link",
    )


async def _handle_request_link(message, raw_query: str):
    """Deep-link payload: req_<query> — pre-fills a request confirmation."""
    movie_name = urllib.parse.unquote(raw_query).replace("_", " ")
    markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"{ICON_SUCCESS} Confirm Request",
                    callback_data=callback_data("reqmovie#", movie_name),
                )
            ]
        ]
    )
    return await message.reply_text(
        f"{ICON_REQUEST} <b>Movie Request</b>\n\n"
        f"Requesting: <code>{_html(movie_name)}</code>\n\n"
        f"Tap below to send this to the admins.",
        reply_markup=markup,
        parse_mode=ParseMode.HTML,
        reply_parameters=None,
    )


async def _handle_search_payload(client, message, config, payload: str):
    """Deep-link payload: search_<query>, or a bare payload treated as a
    search term (legacy / QR-code links)."""
    if payload.startswith("search_"):
        raw_query = payload.split("search_", 1)[1]
    else:
        raw_query = payload
    query = urllib.parse.unquote(raw_query).replace("_", " ")
    status_msg = await show_search_indicator(client, message.chat.id)
    return await _execute_search(
        client,
        status_msg,
        query,
        config,
        user_id=message.from_user.id,
        first_name=message.from_user.first_name,
    )


@Client.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    # 1. Fetch live config from Database
    config = await db.get_config()
    if not (await enforce_user_action(message, "start", config)).allowed:
        return
    START_MEDIA = config.get("start_media", "https://files.catbox.moe/wvdeci.mp4")
    UPDATE_CHANNEL_LINK = config.get("update_channel", "")
    MAIN_GROUP_LINK = config.get("main_group", "")
    LOG_CHANNEL_ID = config.get("log_channel", 0)

    # Live file count across all clusters
    total_files = await db.get_total_files()

    # 2. Silently log the user to the database
    is_new = await db.save_user(message.from_user.id, message.from_user.first_name)

    if is_new and LOG_CHANNEL_ID:
        try:
            user_count = await db.get_user_count()
            await send_message_resilient(
                client,
                LOG_CHANNEL_ID,
                "🆕 <b>New User Alert</b>\n\n"
                f"👤 <b>User:</b> {html_user_mention(message.from_user)}\n"
                f"🆔 <b>ID:</b> <code>{message.from_user.id}</code>\n"
                f"📊 <b>Total Users:</b> <code>{user_count:,}</code>",
                parse_mode=ParseMode.HTML,
                route="new_user_log",
            )
        except Exception:
            pass

    # Catch Group Search Deep-Links
    if len(message.command) > 1 and message.command[1].startswith("search_"):
        query = message.command[1].replace("search_", "").replace("_", " ")
        from plugins.filter import auto_filter

        return await auto_filter(client, message, manual_query=query)

    # 3. Deep-link dispatch — one clearly separated handler per payload type.
    if len(message.command) > 1:
        payload = message.command[1]
        if payload.startswith("release_"):
            from plugins.live_library import store

            key = payload.removeprefix("release_")
            post = await store().posts.find_one({"_id": key}) if len(key) == 32 else None
            if not post:
                return await message.reply_text("This release is not available. Please search by title.")
            meta = post["metadata"]
            query = f"{meta['title']} {meta.get('year', '')}".strip()
            if post.get("season") is not None:
                query += f" S{post['season']:02d}"
            indicator = await show_search_indicator(client, message.chat.id)
            return await _execute_search(client, indicator, query, config,
                                         user_id=message.from_user.id,
                                         first_name=message.from_user.first_name,
                                         release_key=key)
        elif payload.startswith("file_"):
            file_payload = payload.split("file_", 1)[1]
            delete_override = None
            if "_d" in file_payload:
                candidate, marker, raw_seconds = file_payload.rpartition("_d")
                if marker and raw_seconds.isdigit():
                    file_payload = candidate
                    delete_override = int(raw_seconds)
            return await _handle_file_link(
                client,
                message,
                file_payload,
                delete_seconds_override=delete_override,
            )
        elif payload.startswith("req_"):
            return await _handle_request_link(message, payload.split("req_", 1)[1])
        else:
            return await _handle_search_payload(client, message, config, payload)

    # No payload — render the home panel.
    lang = await db.get_user_language(message.from_user.id)
    caption_text, reply_markup = _build_start_ui(
        config,
        message.from_user.mention,
        total_files,
        client.me.username,
        UPDATE_CHANNEL_LINK,
        MAIN_GROUP_LINK,
        is_new=is_new,
        first_name=message.from_user.first_name,
        lang=lang,
    )

    try:
        media_lower = START_MEDIA.lower()
        if media_lower.endswith((".mp4", ".mkv", ".mov")):
            await message.reply_video(
                video=START_MEDIA,
                caption=caption_text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML,
                reply_parameters=None,
            )
        elif media_lower.endswith((".gif")):
            await message.reply_animation(
                animation=START_MEDIA,
                caption=caption_text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML,
                reply_parameters=None,
            )
        else:
            await message.reply_photo(
                photo=START_MEDIA,
                caption=caption_text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML,
                reply_parameters=None,
            )
    except Exception:
        await message.reply_text(
            text=caption_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
            reply_parameters=None,
            **_no_preview(),
        )


@Client.on_callback_query(filters.regex(r"^help_menu$"))
async def help_menu_callback(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    lang = await db.get_user_language(callback.from_user.id)
    strings = LANG_STRINGS.get(lang, LANG_STRINGS["en"])
    steps = "\n".join(f"{i}. {s}" for i, s in enumerate(strings["help_steps"], 1))
    help_text = (
        "<b>🔎 How to find your movie</b>\n"
        f"<blockquote>{steps}</blockquote>\n"
        "<b>Power search</b>\n"
        "Try <code>Leo Malayalam 1080p</code> to narrow results instantly.\n\n"
        f"<i>{strings['help_footer']}</i>"
    )
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("⌂ Back to Home", callback_data="start_home")]])

    try:
        if (
            getattr(callback.message, "video", None)
            or getattr(callback.message, "photo", None)
            or getattr(callback.message, "animation", None)
        ):
            await callback.message.edit_caption(
                caption=help_text, reply_markup=markup, parse_mode=ParseMode.HTML
            )
        else:
            await callback.message.edit_text(text=help_text, reply_markup=markup, parse_mode=ParseMode.HTML)
    except Exception:
        pass


@Client.on_callback_query(filters.regex(r"^start_home$"))
async def start_home_callback(client: Client, callback: CallbackQuery):
    await answer_callback_safely(callback)
    config = await db.get_config()
    UPDATE_CHANNEL_LINK = config.get("update_channel", "")
    MAIN_GROUP_LINK = config.get("main_group", "")

    total_files = await db.get_total_files()
    lang = await db.get_user_language(callback.from_user.id)

    caption_text, reply_markup = _build_start_ui(
        config,
        callback.from_user.mention,
        total_files,
        client.me.username,
        UPDATE_CHANNEL_LINK,
        MAIN_GROUP_LINK,
        is_new=False,
        first_name=callback.from_user.first_name,
        lang=lang,
    )

    try:
        if (
            getattr(callback.message, "video", None)
            or getattr(callback.message, "photo", None)
            or getattr(callback.message, "animation", None)
        ):
            await callback.message.edit_caption(
                caption=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML
            )
        else:
            await callback.message.edit_text(
                text=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML
            )
    except Exception:
        pass
    await answer_callback_safely(callback)


@Client.on_callback_query(filters.regex(r"^toggle_lang#"))
async def toggle_lang_callback(client: Client, callback: CallbackQuery):
    new_lang = callback.data.split("#", 1)[1]
    if new_lang not in LANG_STRINGS:
        return await answer_callback_safely(callback)
    await db.set_user_language(callback.from_user.id, new_lang)
    await answer_callback_safely(callback, f"🌐 {LANG_NAMES[new_lang]}")
    # Re-render the home panel in the new language — same code path as
    # start_home_callback so the two never drift apart.
    await start_home_callback(client, callback)
