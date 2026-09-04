import asyncio
import os
import unittest
from pathlib import Path
from unittest.mock import patch

import tmdb


ROOT = Path(__file__).resolve().parents[1]


class FakeResponse:
    status = 200

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def json(self):
        return {"results": [{
            "media_type": "movie",
            "title": "Alien",
            "poster_path": None,
            "overview": "Space",
            "vote_average": 8.2,
        }]}


class FakeSession:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.closed = False
        self.requests = []

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        return FakeResponse()

    async def close(self):
        self.closed = True


class TmdbClientTests(unittest.TestCase):
    def setUp(self):
        tmdb._session = None
        tmdb._cache.clear()

    def tearDown(self):
        if tmdb._session is not None:
            asyncio.run(tmdb.close_tmdb_client())

    def test_session_is_pooled_and_uses_bearer_header(self):
        created = []

        def make_session(**kwargs):
            session = FakeSession(**kwargs)
            created.append(session)
            return session

        with (
            patch.dict(os.environ, {"TMDB_BEARER_TOKEN": "read-token"}),
            patch.object(tmdb.aiohttp, "TCPConnector", return_value=object()),
            patch.object(tmdb.aiohttp, "ClientTimeout", return_value=object()),
            patch.object(tmdb.aiohttp, "ClientSession", side_effect=make_session),
        ):
            first = asyncio.run(tmdb.start_tmdb_client())
            second = asyncio.run(tmdb.start_tmdb_client())
        self.assertIs(first, second)
        self.assertEqual(len(created), 1)
        self.assertEqual(
            created[0].kwargs["headers"]["Authorization"], "Bearer read-token"
        )

    def test_query_uses_params_without_credentials_and_shutdown_closes(self):
        session = FakeSession()
        tmdb._session = session
        ok, result = asyncio.run(tmdb._fetch_movie_data("Alien & sequel"))
        self.assertTrue(ok)
        self.assertEqual(result["title"], "Alien")
        url, kwargs = session.requests[0]
        self.assertEqual(url, "https://api.themoviedb.org/3/search/multi")
        self.assertNotIn("api_key", url)
        self.assertEqual(kwargs["params"]["query"], "Alien & sequel")
        asyncio.run(tmdb.close_tmdb_client())
        self.assertTrue(session.closed)
        self.assertIsNone(tmdb._session)

    def test_bot_owns_tmdb_start_and_shutdown(self):
        source = (ROOT / "bot.py").read_text(encoding="utf-8")
        self.assertIn("await start_tmdb_client()", source)
        self.assertIn("await close_tmdb_client()", source)
        tmdb_source = (ROOT / "tmdb.py").read_text(encoding="utf-8")
        self.assertNotIn("?api_key=", tmdb_source)
        self.assertEqual(tmdb_source.count("aiohttp.ClientSession("), 1)

    def test_v3_api_key_uses_params_without_bearer_header(self):
        with (
            patch.dict(os.environ, {"TMDB_API_KEY": "test-key"}, clear=True),
            patch.object(tmdb.aiohttp, "TCPConnector", return_value=object()),
            patch.object(tmdb.aiohttp, "ClientTimeout", return_value=object()),
            patch.object(tmdb.aiohttp, "ClientSession", side_effect=FakeSession),
        ):
            self.assertTrue(tmdb.tmdb_configured())
            session = asyncio.run(tmdb.start_tmdb_client())
            self.assertNotIn("Authorization", session.kwargs["headers"])
            ok, _ = asyncio.run(tmdb._fetch_movie_data("Alien"))
            self.assertTrue(ok)
            self.assertEqual(session.requests[0][1]["params"]["api_key"], "test-key")
            original = {"year": "1979"}
            self.assertEqual(tmdb._request_params(original), {"year": "1979", "api_key": "test-key"})
            self.assertEqual(original, {"year": "1979"})

    def test_bearer_takes_precedence_over_v3_key(self):
        with patch.dict(os.environ, {
            "TMDB_BEARER_TOKEN": "read-token", "TMDB_API_KEY": "test-key",
        }, clear=True):
            self.assertEqual(tmdb._request_params({"query": "Alien"}), {"query": "Alien"})

    def test_no_credentials_means_not_configured(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(tmdb.tmdb_configured())
            self.assertIsNone(asyncio.run(tmdb.start_tmdb_client()))


if __name__ == "__main__":
    unittest.main()
