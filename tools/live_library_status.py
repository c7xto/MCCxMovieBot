"""Read-only local configuration/source-access diagnostic (never starts the bot)."""

import argparse
import asyncio
import json
import os
from pathlib import Path

import aiohttp
import certifi
from dotenv import load_dotenv
from pymongo import AsyncMongoClient


async def inspect(check_telegram=False):
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    uri = os.getenv("OPERATIONS_DATABASE_URI")
    if not uri:
        print(json.dumps({"error": "OPERATIONS_DATABASE_URI missing"}))
        return
    client = AsyncMongoClient(
        uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000, tlsCAFile=certifi.where()
    )
    try:
        config = await client["MCCxBot_Operations"]["bot_config"].find_one({"_id": "bot_config"}) or {}
        values = list(config.get("db_channels", [])) + [os.getenv("DATABASE_CHANNEL_ID", "0")]
        sources = sorted(
            {int(value) for value in values if str(value).lstrip("-").isdigit() and int(value) < 0}
        )
        destination = int(config.get("update_channel_id") or 0)
        result = {
            "scope": "local .env operations database; hosted environment may differ",
            "sources": sources,
            "new_releases": destination,
            "posts_enabled": config.get("release_posts_enabled", False),
            "tmdb_configured": bool(os.getenv("TMDB_BEARER_TOKEN") or os.getenv("TMDB_API_READ_TOKEN") or os.getenv("TMDB_API_KEY")),
        }
        if check_telegram and os.getenv("BOT_TOKEN"):
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:

                async def read(method, **params):
                    url = f"https://api.telegram.org/bot{os.environ['BOT_TOKEN']}/{method}"
                    async with session.get(url, params=params) as response:
                        data = await response.json()
                        if not data.get("ok"):
                            return {"error": data.get("description", "Telegram request failed")}
                        return data["result"]

                me = await read("getMe")
                result["bot"] = me.get("username", me.get("error"))
                result["access"] = []
                if me.get("id"):
                    for channel in sorted(set(sources + ([destination] if destination else []))):
                        chat = await read("getChat", chat_id=channel)
                        member = await read("getChatMember", chat_id=channel, user_id=me["id"])
                        result["access"].append(
                            {
                                "id": channel,
                                "title": chat.get("title"),
                                "type": chat.get("type"),
                                "status": member.get("status"),
                                "can_post": member.get("can_post_messages"),
                                "error": chat.get("error") or member.get("error"),
                            }
                        )
        print(json.dumps(result, ensure_ascii=True, indent=2))
    except Exception as error:
        # Driver/network exceptions can contain a credential-bearing URI.
        print(json.dumps({"error_type": type(error).__name__}))
    finally:
        await client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-telegram", action="store_true")
    args = parser.parse_args()
    asyncio.run(inspect(args.check_telegram))
