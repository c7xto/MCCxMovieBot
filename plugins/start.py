import os
import asyncio
import urllib.parse
import time
import random
import string
from dotenv import load_dotenv
from plugins.filter import route_menu
from utils import _no_preview, _html, HELP_STEPS_EN, HELP_FOOTER_EN
from pyrogram import Client, filters
from pyrogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.enums import ParseMode, ChatAction
from database.db import db

load_dotenv()

# ── Brand kit ─────────────────────────────────────────────────────────────
# Shared icon set for this file — keep in sync with plugins/filter.py's
# LANG_EMOJI and the rest of the bot's emoji usage. Centralizing this into a
# dedicated branding module is a reasonable follow-up once more files need
# it; scoped to start.py for now since this phase only touches this file.
ICON_SEARCH   = "🔍"
ICON_MOVIE    = "🎬"
ICON_UPDATES  = "📢"
ICON_SUCCESS  = "✅"
ICON_FAIL     = "❌"
ICON_REQUEST  = "📝"

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
            f"{ICON_MOVIE} I'm MCCxMovieBot — a movie &amp; series finder.\n"
            f"{ICON_SEARCH} Send a title to search."
        ),
        "welcome_greeting": "Welcome, {first_name}",
        "files_counting": "{total_files:,} files and counting.",
        "onboarding_title": "New here? It's simple:",
        "onboarding_steps": [
            "Type any movie or series name",
            "Tap the file you want",
            "It lands in this chat instantly",
        ],
        "help_steps": HELP_STEPS_EN,
        "help_footer": HELP_FOOTER_EN,
        "no_results": "No matches for <code>{query}</code>\n\nIt may not be uploaded yet, or there's a typo in the name.",
    },
    "ml": {
        "welcome_body": (
            f"{ICON_MOVIE} ഞാൻ MCCxMovieBot — സിനിമകളും സീരീസുകളും കണ്ടെത്താൻ സഹായിക്കുന്ന ബോട്ട്.\n"
            f"{ICON_SEARCH} സെർച്ച് ചെയ്യാൻ ഒരു പേര് അയക്കൂ."
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


def _build_start_ui(config, mention, total_files, bot_username, update_link, group_link,
                     is_new=False, first_name="", lang="en"):
    """Shared welcome UI builder — used by /start and start_home callback."""
    strings = LANG_STRINGS.get(lang, LANG_STRINGS["en"])
    default_welcome = (
        "<b>" + strings["welcome_greeting"] + "</b>\n\n"
        + strings["welcome_body"] + "\n\n"
        "<i>" + strings["files_counting"] + "</i>"
    )
    # An admin-customized welcome_text is a single free-text field with no
    # per-language variant — the language toggle only swaps the *default*.
    raw = config.get("welcome_text") or default_welcome
    try:
        # Both {mention} and {first_name} are accepted — admin-set welcome
        # text written before this update still uses {mention} and keeps
        # working exactly as before.
        text = raw.format(mention=mention, first_name=first_name or mention, total_files=total_files)
    except Exception:
        text = raw

    # Clean onboarding: first-time users get a short "how this works" block
    # appended after the (possibly admin-customized) welcome text; returning
    # users just see the normal welcome — no repeated hand-holding.
    if is_new:
        steps = "\n".join(f"{i}. {s}" for i, s in enumerate(strings["onboarding_steps"], 1))
        text += f"\n\n<blockquote><b>{strings['onboarding_title']}</b>\n{steps}</blockquote>"

    text += f"\n\n@{bot_username}"

    buttons = []

    row1 = [InlineKeyboardButton("👥 Add To Group", url=f"https://t.me/{bot_username}?startgroup=true")]
    if update_link:
        row1.append(InlineKeyboardButton(f"{ICON_UPDATES} Join Our Channel", url=update_link))
    buttons.append(row1)

    if group_link:
        buttons.append([InlineKeyboardButton("⚡ Movie Request Group", url=group_link)])

    buttons.append([InlineKeyboardButton("ℹ️ Help", callback_data="help_menu"), _lang_button(lang)])
    return text, InlineKeyboardMarkup(buttons)


async def _execute_search(client, status_msg, query: str, config: dict, user_id=None, first_name=""):
    """Runs a search and renders page 0 of results into status_msg.
    Shared by every /start deep-link search entry point (search_, req_
    fulfillment redirects, etc.) so they stay in sync with one implementation.
    """
    try:
        await client.send_chat_action(status_msg.chat.id, ChatAction.TYPING)
    except Exception:
        pass

    results = await db.get_search_results(query)

    if not results:
        lang = await db.get_user_language(user_id) if user_id is not None else "en"
        strings = LANG_STRINGS.get(lang, LANG_STRINGS["en"])
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{ICON_REQUEST} Request This Movie", callback_data=f"reqmovie#{query[:40]}")]
        ])
        return await status_msg.edit_text(
            f"{ICON_SEARCH} " + strings["no_results"].format(query=_html(query)),
            reply_markup=markup, parse_mode=ParseMode.HTML
        )

    await db.clear_old_searches()
    session_id = ''.join(random.choices(string.ascii_letters + string.digits, k=6))
    session_data = {
        "results":          results,
        "query":            query,
        "speed":            "0.001s",
        "time":             time.time(),
        "auto_delete_time": int(config.get("auto_delete_time", 300)),
        "user_id":          user_id,
        "first_name":       first_name or "",
    }
    await db.save_search(session_id, session_data)
    return await route_menu(client, status_msg, session_id, 0)


async def _handle_file_link(client, message, file_obj_id: str):
    """Deep-link payload: file_<obj_id> — direct file delivery (mainly the
    group-search "Open in PM" buttons), gated by the same unified
    Verification Gates check as the in-DM sendfile# button."""
    file_data = await db.get_file(file_obj_id)
    if not file_data:
        return await message.reply_text(
            f"{ICON_FAIL} This file was deleted or is no longer available.",
            parse_mode=ParseMode.HTML
        )

    from plugins.req_fsub import check_verification_gates
    if not await check_verification_gates(client, message, file_obj_id):
        return

    config = await db.get_config()
    delete_seconds = int(config.get("auto_delete_time", 300))
    delete_minutes = delete_seconds // 60

    from plugins.filter import _auto_delete_file, _build_caption
    sent = await client.send_cached_media(
        chat_id=message.chat.id,
        file_id=file_data["file_id"],
        caption=_build_caption(config, file_data, delete_minutes, client.me.username),
        parse_mode=ParseMode.HTML
    )
    asyncio.create_task(_auto_delete_file(sent, file_data['file_name'], client.me.username, delete_seconds))


async def _handle_request_link(message, raw_query: str):
    """Deep-link payload: req_<query> — pre-fills a request confirmation."""
    movie_name = urllib.parse.unquote(raw_query).replace("_", " ")
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{ICON_SUCCESS} Confirm Request", callback_data=f"reqmovie#{movie_name[:40]}")]
    ])
    return await message.reply_text(
        f"{ICON_REQUEST} <b>Movie Request</b>\n\n"
        f"Requesting: <code>{_html(movie_name)}</code>\n\n"
        f"Tap below to send this to the admins.",
        reply_markup=markup, parse_mode=ParseMode.HTML, quote=True
    )


async def _handle_search_payload(client, message, config, payload: str):
    """Deep-link payload: search_<query>, or a bare payload treated as a
    search term (legacy / QR-code links)."""
    if payload.startswith("search_"):
        raw_query = payload.split("search_", 1)[1]
    else:
        raw_query = payload
    query = urllib.parse.unquote(raw_query).replace("_", " ")
    status_msg = await message.reply_text(
        f"{ICON_SEARCH} <b>Searching databases...</b>", parse_mode=ParseMode.HTML, quote=True
    )
    return await _execute_search(
        client, status_msg, query, config,
        user_id=message.from_user.id, first_name=message.from_user.first_name
    )


@Client.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    # 1. Fetch live config from Database
    config = await db.get_config()
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
            users = await db.get_all_users()
            await client.send_message(
                LOG_CHANNEL_ID,
                f"🆕 **New User Alert**\n\n"
                f"👤 **User:** {message.from_user.mention}\n"
                f"🆔 **ID:** `{message.from_user.id}`\n"
                f"📊 **Total Users:** `{len(users)}`"
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
        if payload.startswith("file_"):
            return await _handle_file_link(client, message, payload.split("file_", 1)[1])
        elif payload.startswith("req_"):
            return await _handle_request_link(message, payload.split("req_", 1)[1])
        else:
            return await _handle_search_payload(client, message, config, payload)

    # No payload — render the home panel.
    lang = await db.get_user_language(message.from_user.id)
    caption_text, reply_markup = _build_start_ui(
        config, message.from_user.mention, total_files, client.me.username,
        UPDATE_CHANNEL_LINK, MAIN_GROUP_LINK, is_new=is_new,
        first_name=message.from_user.first_name, lang=lang
    )

    try:
        media_lower = START_MEDIA.lower()
        if media_lower.endswith((".mp4", ".mkv", ".mov")):
            await message.reply_video(video=START_MEDIA, caption=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, quote=True)
        elif media_lower.endswith((".gif")):
            await message.reply_animation(animation=START_MEDIA, caption=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, quote=True)
        else:
            await message.reply_photo(photo=START_MEDIA, caption=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, quote=True)
    except Exception:
        await message.reply_text(text=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, quote=True, **_no_preview())


@Client.on_callback_query(filters.regex(r"^help_menu$"))
async def help_menu_callback(client: Client, callback: CallbackQuery):
    lang = await db.get_user_language(callback.from_user.id)
    strings = LANG_STRINGS.get(lang, LANG_STRINGS["en"])
    steps = "\n".join(f"{i}. {s}" for i, s in enumerate(strings["help_steps"], 1))
    help_text = f"<blockquote>{steps}</blockquote>\n\n<i>{strings['help_footer']}</i>"
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("◀️ Back", callback_data="start_home")]
    ])

    try:
        if getattr(callback.message, "video", None) or getattr(callback.message, "photo", None) or getattr(callback.message, "animation", None):
            await callback.message.edit_caption(caption=help_text, reply_markup=markup, parse_mode=ParseMode.HTML)
        else:
            await callback.message.edit_text(text=help_text, reply_markup=markup, parse_mode=ParseMode.HTML)
    except Exception:
        pass
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^start_home$"))
async def start_home_callback(client: Client, callback: CallbackQuery):
    config = await db.get_config()
    UPDATE_CHANNEL_LINK = config.get("update_channel", "")
    MAIN_GROUP_LINK = config.get("main_group", "")

    total_files = await db.get_total_files()
    lang = await db.get_user_language(callback.from_user.id)

    caption_text, reply_markup = _build_start_ui(
        config, callback.from_user.mention, total_files, client.me.username,
        UPDATE_CHANNEL_LINK, MAIN_GROUP_LINK, is_new=False,
        first_name=callback.from_user.first_name, lang=lang
    )

    try:
        if getattr(callback.message, "video", None) or getattr(callback.message, "photo", None) or getattr(callback.message, "animation", None):
            await callback.message.edit_caption(caption=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        else:
            await callback.message.edit_text(text=caption_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception:
        pass
    await callback.answer()


@Client.on_callback_query(filters.regex(r"^toggle_lang#"))
async def toggle_lang_callback(client: Client, callback: CallbackQuery):
    new_lang = callback.data.split("#", 1)[1]
    if new_lang not in LANG_STRINGS:
        return await callback.answer()
    await db.set_user_language(callback.from_user.id, new_lang)
    await callback.answer(f"🌐 {LANG_NAMES[new_lang]}")
    # Re-render the home panel in the new language — same code path as
    # start_home_callback so the two never drift apart.
    await start_home_callback(client, callback)
