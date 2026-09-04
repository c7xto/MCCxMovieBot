"""Controls attached to Source Channels and New Releases, not a second system."""

import time
from html import escape

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardButton

from database.db import db
from plugins.callbacks import answer_callback_safely
from plugins.live_library import (
    store,
    source_ids,
    validate_source,
    validate_destination,
    release_markup,
)
from plugins.mobile_ui import MobileInlineKeyboardMarkup as Markup
from plugins.release_identity import render_release
from plugins.telegram_retry import INTERACTIVE_RETRY, telegram_call
from tmdb import tmdb_configured, release_metadata
from utils import ADMIN_ID


def button(text, data):
    return InlineKeyboardButton(text, callback_data=data)


async def show_live_status(client, callback):
    config = await db.get_config()
    status = await store().status(source_ids(config))
    lines = [
        "<b>Automatic Library</b>",
        "",
        f"Indexed receipts: {status['indexed']:,}",
        f"Pending: {status['pending']:,} · Retrying: {status['retrying']:,}",
        f"Release queue: {status['queued']:,} · Needs matching: {status['review']:,}",
        f"Posts needing attention: {status['uncertain']:,}",
        "",
        "Posts: " + ("Enabled" if config.get("release_posts_enabled", False) else "Paused — preview first"),
        "TMDB: " + ("Configured" if tmdb_configured() else "Key missing"),
        "New Releases: " + str(config.get("update_channel_id") or "Not configured"),
    ]
    for source in status["sources"][:10]:
        last = source.get("last_seen")
        age = f"{int((time.time() - last) / 60)} min ago" if last else "No upload received"
        checkpoint = source.get("checkpoint")
        frontier = source.get("frontier")
        progress = (
            f"Saved through {checkpoint:,} / observed {frontier:,}"
            if frontier is not None
            else "Awaiting latest channel update"
        )
        lines.extend(["", f"<b>Source {source['_id']}</b> · {age}", progress])
        if source.get("error"):
            lines.append("Needs attention: " + escape(source["error"]))
    if not status["sources"]:
        lines.append("\nAdd the Transfer Bot destination in Source Channels.")
    rows = [
        [button("Refresh", "live_status"), button("Check access", "live_check")],
        [button("Pause posts" if config.get("release_posts_enabled") else "Enable posts", "live_toggle")],
        [button("Previews & review", "live_review")],
        [button("Source Channels", "db_chan_menu"), button("New Releases", "releases_channel_menu")],
    ]
    await callback.message.edit_text("\n".join(lines), reply_markup=Markup(rows), parse_mode=ParseMode.HTML)


@Client.on_callback_query(filters.regex(r"^live_(status|check|toggle|review)$") & filters.user(ADMIN_ID))
async def live_controls(client, callback):
    await answer_callback_safely(callback)
    config = await db.get_config()
    action = callback.data
    if action in {"live_check", "live_toggle"}:
        enabling = action == "live_toggle" and not config.get("release_posts_enabled", False)
        if action == "live_toggle" and not enabling:
            await db.update_config_fields({"release_posts_enabled": False})
            return await show_live_status(client, callback)
        try:
            ids = source_ids(config)
            if not ids:
                raise ValueError("Register the Transfer Bot destination in Source Channels first")
            for source in ids:
                await validate_source(client, source)
                await store().watch(source)
            destination = int(config.get("update_channel_id") or 0)
            if not destination:
                raise ValueError("Configure New Releases first")
            await validate_destination(client, destination)
            if enabling:
                if not tmdb_configured():
                    raise ValueError("Configure TMDB_API_KEY or TMDB_BEARER_TOKEN before enabling posters")
                if config.get("release_preview_destination") != destination:
                    raise ValueError("Open a private release preview first, then enable posts")
                await db.update_config_fields({"release_posts_enabled": True})
        except Exception as error:
            message = str(error) if isinstance(error, ValueError) else type(error).__name__
            await callback.message.edit_text(
                "<b>Setup needs attention</b>\n\n" + escape(message),
                parse_mode=ParseMode.HTML,
                reply_markup=Markup([[button("Back", "live_status")]]),
            )
            return
    if action == "live_review":
        persistence = store()
        posts = [
            row
            async for row in persistence.posts.find({"state": {"$in": ["pending", "blocked", "uncertain"]}})
            .sort("created", 1)
            .limit(8)
        ]
        review = [row async for row in persistence.candidates.find({"state": "review"}).limit(8)]
        lines = ["<b>Release previews & matching</b>", "", "Previews are sent only to this admin chat."]
        rows = []
        for post in posts:
            label = post["metadata"]["title"][:28]
            if post.get("season") is not None:
                label += f" S{post['season']:02d}"
            rows.append([button(label, f"relpreview#{post['_id']}")])
            if post["state"] in {"uncertain", "blocked"}:
                lines.append(f"{escape(label)}: {post['state']} · <code>{post['_id']}</code>")
        for item in review:
            lines.extend(["", escape(item["parsed"]["title"]), f"<code>{item['_id']}</code>"])
        lines.extend(
            [
                "",
                "Confirm a TMDB match:",
                "<code>/release_match candidate_id tmdb_id</code>",
                "Add a season number at the end if missing.",
                "",
                "Preview any release by its reference ID:",
                "<code>/release_preview release_id</code>",
                "",
                "Link an uncertain post you can see in New Releases:",
                "<code>/release_link release_id message_id</code>",
                "Retry an explicitly rejected post:",
                "<code>/release_retry release_id</code>",
            ]
        )
        rows.append([button("Back", "live_status")])
        await callback.message.edit_text(
            "\n".join(lines), reply_markup=Markup(rows), parse_mode=ParseMode.HTML
        )
        return
    await show_live_status(client, callback)


@Client.on_callback_query(filters.regex(r"^relpreview#[a-f0-9]{32}$") & filters.user(ADMIN_ID))
async def preview_release(client, callback):
    await answer_callback_safely(callback)
    persistence = store()
    post = await persistence.posts.find_one({"_id": callback.data.split("#")[1]})
    if not post:
        return
    await send_release_preview(client, callback.from_user.id, post)


async def send_release_preview(client, admin_id, post):
    persistence = store()
    caption = render_release(post, await persistence.summary(post["_id"]))
    options = {
        "chat_id": admin_id,
        "parse_mode": ParseMode.HTML,
        "reply_markup": release_markup(client, post["_id"]),
    }
    if post["metadata"].get("poster"):
        operation = lambda: client.send_photo(photo=post["metadata"]["poster"], caption=caption, **options)
    else:
        operation = lambda: client.send_message(text=caption, **options)
    await telegram_call(operation, route="release_preview", policy=INTERACTIVE_RETRY, retry_safe=False)
    await db.update_config_fields({"release_preview_destination": post["destination"]})


@Client.on_message(
    filters.command(["release_match", "release_link", "release_retry", "release_preview"])
    & filters.private
    & filters.user(ADMIN_ID)
)
async def release_review_command(client, message):
    persistence = store()
    args = message.command
    try:
        if len(args) < 2 or len(args[1]) != 32:
            raise ValueError("Open Previews & review for the command format and reference ID")
        if args[0] == "release_preview":
            post = await persistence.posts.find_one({"_id": args[1]})
            if not post:
                raise ValueError("Release not found")
            await send_release_preview(client, message.from_user.id, post)
            return
        elif args[0] == "release_match":
            if len(args) not in {3, 4}:
                raise ValueError("Use /release_match candidate_id tmdb_id [season]")
            candidate = await persistence.candidates.find_one({"_id": args[1], "state": "review"})
            if not candidate:
                raise ValueError("That candidate is not awaiting review")
            parsed = dict(candidate["parsed"])
            if len(args) == 4:
                parsed["season"] = int(args[3])
                parsed["kind"] = "tv"
            metadata = await release_metadata(parsed, confirmed_id=int(args[2]))
            if not metadata:
                raise ValueError("TMDB could not validate that title/season/episode combination")
            # Apply the explicit choice to variants with the same parsed identity.
            await persistence.candidates.update_many(
                {"identity": candidate["identity"], "state": "review"},
                {
                    "$set": {
                        "confirmed_id": int(args[2]),
                        "state": "pending",
                        "due": time.time(),
                        "lease_until": 0,
                        "parsed.season": parsed["season"],
                        "parsed.kind": parsed["kind"],
                    },
                },
            )
            text = f"Match saved: {metadata['title']}. Open the private preview before publishing."
        else:
            post = await persistence.posts.find_one({"_id": args[1]})
            if not post:
                raise ValueError("Release not found")
            if args[0] == "release_link":
                if len(args) != 3 or post["state"] != "uncertain":
                    raise ValueError("Use /release_link release_id message_id for an uncertain post")
                sent = await client.get_messages(post["destination"], int(args[2]))
                expected = f"release_{post['_id']}"
                buttons = getattr(getattr(sent, "reply_markup", None), "inline_keyboard", [])
                if not any(expected in (getattr(item, "url", "") or "") for row in buttons for item in row):
                    raise ValueError("The selected post does not contain this release's download link")
                await persistence.posts.update_one(
                    {"_id": post["_id"], "state": "uncertain"},
                    {
                        "$set": {
                            "state": "posted",
                            "message_id": sent.id,
                            "photo": bool(sent.photo),
                            "lease_until": 0,
                            "due": time.time(),
                            "error": "",
                        },
                    },
                )
                text = "Existing post linked. Later uploads will edit it."
            else:
                if post["state"] != "blocked":
                    raise ValueError(
                        "Only explicitly rejected posts can be retried. Uncertain sends must be linked first"
                    )
                await validate_destination(client, post["destination"])
                await persistence.posts.update_one(
                    {"_id": post["_id"], "state": "blocked"},
                    {
                        "$set": {
                            "state": "posted" if post.get("message_id") else "pending",
                            "lease_until": 0,
                            "due": time.time(),
                            "error": "",
                        },
                    },
                )
                text = "Post queued for retry. It remains paused if public posts are disabled."
    except Exception as error:
        text = (
            str(error)
            if isinstance(error, ValueError)
            else f"Could not complete review: {type(error).__name__}"
        )
    await message.reply_text(text, parse_mode=ParseMode.DISABLED)
