"""Behaviour tests plus optional real, loopback-only MongoDB integration tests.

Set MCCX_TEST_MONGO_URI=mongodb://127.0.0.1:27029 to run the latter. Each
test owns a randomly named database; no application .env URI is ever used.
"""

import asyncio
import copy
import os
import time
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from urllib.parse import urlparse

import pytest
import pytest_asyncio
from bson import ObjectId
from pymongo import AsyncMongoClient
from pyrogram.enums import ChatType
from pyrogram.errors import FloodWait, MessageNotModified

from database.releases import ReleaseStore
from plugins.release_identity import parse_release, choose_match, render_release
from plugins import live_library as live
import tmdb


def metadata(kind="movie", movie_id=10):
    return {
        "id": movie_id,
        "kind": kind,
        "title": "Life",
        "year": "2026",
        "poster": None,
        "overview": "A small story.",
        "rating": 7.4,
        "genres": ["Drama"],
    }


def summary(episodes=None):
    return {
        "count": 2,
        "languages": ["Malayalam", "Tamil"],
        "qualities": ["720p", "1080p"],
        "episodes": episodes or [],
    }


def client_mock():
    return SimpleNamespace(
        me=SimpleNamespace(username="test_bot", id=1),
        send_message=AsyncMock(return_value=SimpleNamespace(id=55)),
        send_photo=AsyncMock(return_value=SimpleNamespace(id=56)),
        edit_message_text=AsyncMock(),
        edit_message_caption=AsyncMock(),
    )


@pytest.mark.parametrize(
    "name,title,year,season,episodes",
    [
        ("Life.2026.Malayalam.1080p.mkv", "Life", "2026", None, []),
        ("2012.2009.720p.mkv", "2012", "2009", None, []),
        ("Dark_S02E03_Episode_Name_1080p.mkv", "Dark", "", 2, [3]),
        ("Dark 2x04 Episode Name 720p.mkv", "Dark", "", 2, [4]),
        ("Dark S02E01-E03 720p.mkv", "Dark", "", 2, [1, 2, 3]),
        ("Dark Season 2 Episode 4 720p.mkv", "Dark", "", 2, [4]),
        ("Dark E04 720p.mkv", "Dark", "", None, [4]),
        ("മലയാളം 2026 720p.mkv", "മലയാളം", "2026", None, []),
    ],
)
def test_conservative_identity(name, title, year, season, episodes):
    parsed = parse_release(name)
    assert (parsed["title"], parsed["year"], parsed["season"], parsed["episodes"]) == (
        title,
        year,
        season,
        episodes,
    )


def test_remakes_and_missing_year_are_not_fuzzy_merged():
    rows = [
        {"id": 1, "title": "Life", "release_date": "2017-01-01"},
        {"id": 2, "title": "Life", "release_date": "2026-01-01"},
    ]
    assert choose_match(parse_release("Life 2026 1080p.mkv"), rows)["id"] == 2
    assert choose_match(parse_release("Life 1080p.mkv"), rows) is None
    assert choose_match(parse_release("Lief 2026 1080p.mkv"), rows) is None
    assert parse_release("Life 2017.mkv")["identity"] != parse_release("Life 2026.mkv")["identity"]


def test_caption_preserves_episode_gaps_and_escapes_html():
    meta = metadata("tv")
    meta["title"] = "Life & <Other>"
    caption = render_release({"metadata": meta, "season": 2}, summary([1, 2, 5, 6, 8]))
    assert "1–2, 5–6, 8" in caption
    assert "&amp; &lt;Other&gt;" in caption
    assert "TMDB: 7.4/10" in caption
    assert "complete" not in caption.lower()
    assert len(caption) < 1024


async def test_source_capture_precedes_readiness_and_dedupe(monkeypatch):
    from plugins import readiness

    capture = AsyncMock(return_value=True)
    monkeypatch.setattr(live, "capture_source_message", capture)
    message = SimpleNamespace(stop_propagation=lambda: None)
    with patch.object(readiness.redis_state, "claim_once", AsyncMock()) as claim:
        await readiness.readiness_message_gate(None, message)
    capture.assert_awaited_once()
    claim.assert_not_awaited()


async def test_full_shards_do_not_complete_or_announce(monkeypatch):
    monkeypatch.setattr(live.db, "save_file", AsyncMock(return_value=(False, "All clusters full")))
    persistence = SimpleNamespace(enqueue_candidate=AsyncMock(), finish=AsyncMock())
    receipt = {"media": {"file_id": "x", "file_name": "Life 2026.mkv"}, "announce": True}
    with pytest.raises(RuntimeError, match="All clusters full"):
        await live.ingest_receipt(receipt, persistence)
    persistence.finish.assert_not_awaited()
    persistence.enqueue_candidate.assert_not_awaited()


@pytest_asyncio.fixture
async def mongo_store():
    uri = os.getenv("MCCX_TEST_MONGO_URI")
    if not uri:
        pytest.skip("Set MCCX_TEST_MONGO_URI for isolated MongoDB integration tests")
    parsed = urlparse(uri)
    if parsed.scheme != "mongodb" or parsed.hostname not in {"127.0.0.1", "localhost"}:
        pytest.fail("Integration tests only permit a loopback MongoDB URI")
    connection = AsyncMongoClient(uri, serverSelectionTimeoutMS=3000)
    name = "mccx_release_test_" + uuid.uuid4().hex
    operations = connection[name]
    persistence = ReleaseStore(operations)
    await persistence.ensure_indexes()
    try:
        yield persistence
    finally:
        await connection.drop_database(name)
        await connection.close()


async def add_variant(persistence, number, *, name="Life 2026 Malayalam 720p.mkv", movie_id=10, kind="movie"):
    parsed = parse_release(name)
    receipt = {"received": time.time()}
    await persistence.enqueue_candidate(
        receipt,
        parsed,
        {
            "file_key": f"unique-{number}",
            "movie_id": str(ObjectId()),
            "shard": 0,
            "file_size": number * 1000,
        },
    )
    candidate = await persistence.claim(persistence.candidates, ["pending"])
    assert candidate
    return await persistence.attach(candidate, metadata(kind, movie_id), -100123)


async def due_post(persistence):
    await persistence.posts.update_many({}, {"$set": {"due": time.time() - 1}})
    return await persistence.claim(persistence.posts, ["pending", "posted"])


async def test_mongo_ten_variants_one_post_then_late_edit(mongo_store):
    persistence = mongo_store
    keys = [await add_variant(persistence, n) for n in range(10)]
    assert len(set(keys)) == 1
    assert (await persistence.summary(keys[0]))["count"] == 10
    client = client_mock()
    job = await due_post(persistence)
    await live.publish_release(client, job, persistence)
    client.send_message.assert_awaited_once()
    # Recreate the repository object to simulate process-local state loss.
    reopened = ReleaseStore(persistence.posts.database)
    await add_variant(reopened, 11, name="Life 2026 Tamil 1080p.mkv")
    job = await due_post(reopened)
    await live.publish_release(client, job, reopened)
    client.send_message.assert_awaited_once()
    client.edit_message_text.assert_awaited_once()
    assert client.edit_message_text.await_args.kwargs["message_id"] == 55
    assert "Tamil" in client.edit_message_text.await_args.kwargs["text"]


async def test_mongo_duplicate_handoff_and_season_boundaries(mongo_store):
    key1 = await add_variant(mongo_store, 1, name="Life S01E01 720p.mkv", kind="tv")
    candidate = await mongo_store.candidates.find_one({"release": key1})
    await mongo_store.attach(candidate, metadata("tv"), -100123)
    assert (await mongo_store.summary(key1))["count"] == 1
    key2 = await add_variant(mongo_store, 2, name="Life S01E05 1080p.mkv", kind="tv")
    key3 = await add_variant(mongo_store, 3, name="Life S02E01 1080p.mkv", kind="tv")
    key4 = await add_variant(mongo_store, 4, name="Life 2017.mkv", movie_id=11)
    assert key1 == key2
    assert len({key1, key3, key4}) == 3
    assert (await mongo_store.summary(key1))["episodes"] == [1, 5]


async def test_mongo_single_claim_under_concurrent_workers(mongo_store):
    await add_variant(mongo_store, 1)
    await mongo_store.posts.update_many({}, {"$set": {"due": 0}})
    claims = await asyncio.gather(
        *[mongo_store.claim(mongo_store.posts, ["pending", "posted"]) for _ in range(12)]
    )
    assert sum(job is not None for job in claims) == 1


async def test_mongo_ambiguous_send_is_not_replayed(mongo_store):
    await add_variant(mongo_store, 1)
    client = client_mock()
    client.send_message.side_effect = TimeoutError()
    job = await due_post(mongo_store)
    with pytest.raises(TimeoutError):
        await live.publish_release(client, job, mongo_store)
    await mongo_store.posts.update_many({}, {"$set": {"lease_until": 0}})
    await mongo_store.recover_uncertain()
    assert (await mongo_store.posts.find_one({"_id": job["_id"]}))["state"] == "uncertain"
    assert await mongo_store.claim(mongo_store.posts, ["pending", "posted"]) is None


async def test_mongo_floodwait_does_not_lose_post(mongo_store):
    await add_variant(mongo_store, 1)
    client = client_mock()
    client.send_message.side_effect = FloodWait(30)
    job = await due_post(mongo_store)
    await live.publish_release(client, job, mongo_store)
    record = await mongo_store.posts.find_one({"_id": job["_id"]})
    assert record["state"] == "pending"
    assert record["due"] > time.time() + 25


async def test_mongo_debounce_max_wait_and_concurrent_edit(mongo_store):
    key = await add_variant(mongo_store, 1)
    original = await mongo_store.posts.find_one({"_id": key})
    with patch("database.releases.time.time", return_value=original["created"] + 280):
        await add_variant(mongo_store, 2)
    updated = await mongo_store.posts.find_one({"_id": key})
    assert updated["due"] == original["created"] + 300
    job = await due_post(mongo_store)
    await add_variant(mongo_store, 3)
    await live.publish_release(client_mock(), job, mongo_store)
    posted = await mongo_store.posts.find_one({"_id": key})
    assert posted["revision"] > posted["published_revision"]
    assert posted["due"] <= time.time() + 301


async def test_mongo_receipt_idempotence_and_silent_catchup(mongo_store):
    await mongo_store.watch(-100123)
    media = {"file_id": "1", "file_name": "Life 2026.mkv"}
    receipt = await mongo_store.receive(-100123, 100, media, uploaded_at=time.time() + 1)
    await mongo_store.receive(-100123, 100, media, uploaded_at=time.time() + 2)
    assert await mongo_store.receipts.count_documents({}) == 1
    assert (await mongo_store.receipts.find_one({"_id": receipt}))["announce"] is True
    await mongo_store.receive(-100123, 101, media, uploaded_at=time.time(), live=False)
    assert (await mongo_store.receipts.find_one({"message": 101}))["announce"] is False
    source = await mongo_store.sources.find_one({"_id": -100123})
    assert (source["checkpoint"], source["frontier"]) == (99, 101)


async def test_mongo_recovery_does_not_skip_failed_receipt(mongo_store):
    await mongo_store.watch(-100123)
    await mongo_store.sources.update_one({"_id": -100123}, {"$set": {"checkpoint": 99, "frontier": 105}})
    client = client_mock()
    client.get_messages = AsyncMock(
        return_value=[
            SimpleNamespace(
                id=n,
                empty=False,
                date=datetime.now(timezone.utc),
                document=SimpleNamespace(
                    file_id=str(n),
                    file_unique_id=str(n),
                    file_name="Life 2026.mkv",
                    file_size=10,
                    mime_type="video/mp4",
                ),
                video=None,
                audio=None,
            )
            for n in range(100, 106)
        ]
    )
    await live.recover_source(client, mongo_store, [-100123])
    assert await mongo_store.receipts.count_documents({}) == 6
    assert (await mongo_store.sources.find_one({"_id": -100123}))["checkpoint"] == 99
    assert await mongo_store.receipts.count_documents({"announce": True}) == 0
    await mongo_store.receipts.update_many({}, {"$set": {"state": "done"}})
    await live.recover_source(client, mongo_store, [-100123])
    assert (await mongo_store.sources.find_one({"_id": -100123}))["checkpoint"] == 105


async def test_mongo_save_then_enqueue_crash_repaired(mongo_store, monkeypatch):
    media = {"file_id": "abc", "file_unique_id": "unique", "file_name": "Life 2026.mkv"}
    await mongo_store.receive(-100123, 1, media, uploaded_at=time.time() + 1)
    receipt = await mongo_store.claim(mongo_store.receipts, ["pending"])
    document = {"_id": ObjectId(), **media, "file_size": 100}
    monkeypatch.setattr(live.db, "save_file", AsyncMock(return_value=(False, "Duplicate")))
    monkeypatch.setattr(live, "find_indexed_file", AsyncMock(return_value=(document, 0)))
    monkeypatch.setattr(live.db, "enqueue_request_fulfillment", AsyncMock(return_value="job"))
    await live.ingest_receipt(receipt, mongo_store)
    assert await mongo_store.candidates.count_documents({}) == 1
    saved = await mongo_store.receipts.find_one({"_id": receipt["_id"]})
    assert saved["state"] == "done" and "media" not in saved


async def test_mongo_exact_release_links_page_without_other_titles(mongo_store, monkeypatch):
    database = mongo_store.posts.database
    await add_variant(mongo_store, 1)
    key = (await mongo_store.posts.find_one({}))["_id"]
    for n in range(2, 6):
        await add_variant(mongo_store, n)
    members = [row async for row in mongo_store.members.find({"release": key})]
    for row in members:
        await database["movies"].insert_one(
            {"_id": ObjectId(row["movie_id"]), "file_name": "Life", "file_size": row["file_size"]}
        )
    await database["movies"].insert_one({"file_name": "Other movie", "file_size": 100000})
    monkeypatch.setattr(live, "store", lambda: mongo_store)
    monkeypatch.setattr(live.db, "file_cols", [database["movies"]])
    first, cursor = await live.release_file_page(key, limit=3)
    second, cursor2 = await live.release_file_page(key, cursor, limit=3)
    assert [row["file_size"] for row in first + second] == [5000, 4000, 3000, 2000, 1000]
    assert cursor2 is None
    assert all(row["file_name"] == "Life" for row in first + second)


async def test_mongo_paused_worker_never_publishes(mongo_store, monkeypatch):
    await add_variant(mongo_store, 1)
    await mongo_store.posts.update_many({}, {"$set": {"due": 0}})
    monkeypatch.setattr(live, "store", lambda: mongo_store)
    monkeypatch.setattr(live.db, "get_config", AsyncMock(return_value={"update_channel_id": -100123}))
    client = client_mock()

    async def stop_after_tick(_seconds):
        raise asyncio.CancelledError()

    with patch.object(live.asyncio, "sleep", stop_after_tick), pytest.raises(asyncio.CancelledError):
        await live.run_release_worker(client)
    client.send_message.assert_not_awaited()
    client.send_photo.assert_not_awaited()
    assert (await mongo_store.posts.find_one({}))["state"] == "pending"


async def test_mongo_poster_rejection_falls_back_to_text(mongo_store):
    from pyrogram.errors import WebpageCurlFailed

    key = await add_variant(mongo_store, 1)
    await mongo_store.posts.update_one(
        {"_id": key}, {"$set": {"metadata.poster": "https://example.org/missing.jpg"}}
    )
    client = client_mock()
    client.send_photo.side_effect = WebpageCurlFailed()
    await live.publish_release(client, await due_post(mongo_store), mongo_store)
    record = await mongo_store.posts.find_one({"_id": key})
    assert record["state"] == "pending" and record["metadata"]["poster"] is None
    await live.publish_release(client, await due_post(mongo_store), mongo_store)
    client.send_message.assert_awaited_once()


async def test_mongo_unchanged_caption_does_not_edit(mongo_store):
    await add_variant(mongo_store, 1)
    client = client_mock()
    await live.publish_release(client, await due_post(mongo_store), mongo_store)
    await live.publish_release(client, await due_post(mongo_store), mongo_store)
    client.send_message.assert_awaited_once()
    client.edit_message_text.assert_not_awaited()


async def test_mongo_stale_claim_cannot_send(mongo_store):
    await add_variant(mongo_store, 1)
    old_job = await due_post(mongo_store)
    await mongo_store.posts.update_many({}, {"$set": {"lease_until": 0}})
    new_job = await due_post(mongo_store)
    client = client_mock()
    await live.publish_release(client, old_job, mongo_store)
    client.send_message.assert_not_awaited()
    await live.publish_release(client, new_job, mongo_store)
    client.send_message.assert_awaited_once()


async def test_mongo_metadata_outage_keeps_candidate_pending(mongo_store, monkeypatch):
    await mongo_store.enqueue_candidate(
        {"received": time.time()},
        parse_release("Life 2026.mkv"),
        {
            "file_key": "u",
            "movie_id": str(ObjectId()),
            "shard": 0,
        },
    )
    candidate = await mongo_store.claim(mongo_store.candidates, ["pending"])
    monkeypatch.setattr(live, "release_metadata", AsyncMock(side_effect=TimeoutError()))
    with pytest.raises(TimeoutError):
        await live.resolve_candidate(candidate, mongo_store, -100123)
    assert (await mongo_store.candidates.find_one({}))["state"] == "pending"
    assert await mongo_store.posts.count_documents({}) == 0


async def test_mongo_ambiguous_metadata_waits_for_admin(mongo_store, monkeypatch):
    await mongo_store.enqueue_candidate(
        {"received": time.time()},
        parse_release("Life 2026.mkv"),
        {
            "file_key": "u",
            "movie_id": str(ObjectId()),
            "shard": 0,
        },
    )
    candidate = await mongo_store.claim(mongo_store.candidates, ["pending"])
    monkeypatch.setattr(live, "release_metadata", AsyncMock(return_value=None))
    await live.resolve_candidate(candidate, mongo_store, -100123)
    assert (await mongo_store.candidates.find_one({}))["state"] == "review"
    assert await mongo_store.posts.count_documents({}) == 0


class MetadataResponse:
    status = 200

    def __init__(self, body):
        self.body = body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    def raise_for_status(self):
        return None

    async def json(self):
        return self.body


class MetadataSession:
    def __init__(self, bodies):
        self.bodies = iter(bodies)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return MetadataResponse(next(self.bodies))


async def test_tmdb_movie_type_year_and_pool_cache(monkeypatch):
    tmdb._cache.clear()
    session = MetadataSession(
        [
            {"results": [{"id": 10, "title": "Life", "release_date": "2026-01-01"}]},
            {"id": 10, "title": "Life", "release_date": "2026-01-01", "vote_average": 7.4},
        ]
    )
    monkeypatch.setattr(tmdb, "start_tmdb_client", AsyncMock(return_value=session))
    parsed = parse_release("Life 2026.mkv")
    first = await tmdb.release_metadata(parsed)
    second = await tmdb.release_metadata(parsed)
    assert first == second
    assert len(session.calls) == 2
    assert session.calls[0][0].endswith("/search/movie")
    assert session.calls[0][1]["params"]["year"] == "2026"


async def test_tmdb_season_poster_and_unknown_episode_rejected(monkeypatch):
    tmdb._cache.clear()
    session = MetadataSession(
        [
            {"results": [{"id": 10, "name": "Life", "first_air_date": "2020-01-01"}]},
            {"id": 10, "name": "Life", "first_air_date": "2020-01-01"},
            {"poster_path": "/season.jpg", "overview": "Season story", "episodes": [{"episode_number": 1}]},
        ]
    )
    monkeypatch.setattr(tmdb, "start_tmdb_client", AsyncMock(return_value=session))
    result = await tmdb.release_metadata(parse_release("Life 2026 S02E01.mkv"))
    assert result["poster"].endswith("/season.jpg")
    assert result["year"] == "2020"
    assert session.calls[0][0].endswith("/search/tv")
    assert "year" not in session.calls[0][1]["params"]
    assert session.calls[-1][0].endswith("/tv/10/season/2")
