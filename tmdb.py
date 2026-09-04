import os
import time
import asyncio
import aiohttp
import logging
from collections import OrderedDict
from dotenv import load_dotenv

# 1. Force load the .env file immediately so the key isn't blank
load_dotenv()

logger = logging.getLogger(__name__)

# In-process TTL cache keyed by the cleaned query string. TMDB metadata
# barely changes day to day, and without this every auto-post lookup for
# the same handful of trending titles (across every upload of that title,
# by any admin, at any time) would hit the live API fresh — this collapses
# that down to one real request per title per day. Bounded size (LRU) so it
# can't grow unbounded on an indexer running for hours across many titles.
_CACHE_MAXSIZE = 1000
_CACHE_TTL     = 24 * 3600  # 24h
_cache = OrderedDict()  # normalized_query -> (cached_at, result_or_None)
_session = None
_session_lock = asyncio.Lock()


def _bearer_token():
    return os.getenv("TMDB_BEARER_TOKEN") or os.getenv("TMDB_API_READ_TOKEN")


def tmdb_configured():
    return bool(_bearer_token() or os.getenv("TMDB_API_KEY"))


def _request_params(params=None):
    result = dict(params or {})
    if not _bearer_token() and os.getenv("TMDB_API_KEY"):
        result["api_key"] = os.environ["TMDB_API_KEY"]
    return result


async def start_tmdb_client():
    """Create the application-owned pooled HTTP session once."""
    global _session
    if _session is not None and not _session.closed:
        return _session
    async with _session_lock:
        if _session is not None and not _session.closed:
            return _session
        token = _bearer_token()
        if not tmdb_configured():
            return None
        connector = aiohttp.TCPConnector(
            limit=10,
            limit_per_host=5,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(
            total=10,
            connect=3,
            sock_connect=3,
            sock_read=7,
        )
        _session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"Accept": "application/json", **(
                {"Authorization": f"Bearer {token}"} if token else {}
            )},
            raise_for_status=False,
        )
        return _session


async def close_tmdb_client():
    global _session
    session, _session = _session, None
    if session is not None and not session.closed:
        await session.close()


def _cache_get(key):
    entry = _cache.get(key)
    if not entry:
        return None
    cached_at, result = entry
    if time.time() - cached_at > _CACHE_TTL:
        del _cache[key]
        return None
    _cache.move_to_end(key)
    return (result,)  # wrap so a cached "no match" (None) is distinguishable from a cache miss


def _cache_set(key, result):
    _cache[key] = (time.time(), result)
    _cache.move_to_end(key)
    if len(_cache) > _CACHE_MAXSIZE:
        _cache.popitem(last=False)  # evict least-recently-used


async def get_movie_data(query):
    cache_key = query.strip().lower()
    hit = _cache_get(cache_key)
    if hit is not None:
        return hit[0]

    ok, result = await _fetch_movie_data(query)
    if ok:
        # Only cache a genuine API response (a match, or a confirmed "TMDB
        # has nothing for this title") — never a network/API failure, or a
        # transient blip would suppress lookups for that title for a full day.
        _cache_set(cache_key, result)
    return result


async def _fetch_movie_data(query):
    """Returns (ok, result). ok=False means the call failed (missing key,
    non-200, network error) and should not be cached; ok=True covers both
    a real match and a confirmed no-results response."""
    session = await start_tmdb_client()
    if session is None:
        logger.warning("⚠️ TMDB Error: API key or access token is missing from .env!")
        return False, None
    return await _fetch_legacy_multi(session, query)


async def release_metadata(parsed, *, confirmed_id=None):
    """Type-aware metadata; ambiguity is distinct from an API outage.

    Return None only for an actual no-match. Transport/configuration failures
    raise so the durable resolver retries instead of caching a false absence.
    """
    from plugins.release_identity import choose_match

    cache_key = ("release", parsed["identity"], confirmed_id,
                 parsed.get("season"), tuple(parsed.get("episodes", [])))
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached[0]

    session = await start_tmdb_client()
    if session is None:
        raise RuntimeError("TMDB API key or access token is not configured")

    async def fetch(path, params=None):
        try:
            async with session.get(
                f"https://api.themoviedb.org/3/{path}", params=_request_params(params)
            ) as response:
                if response.status == 404:
                    return None
                if response.status != 200:
                    raise RuntimeError(f"TMDB returned HTTP {response.status}")
                return await response.json()
        except (aiohttp.ClientError, TimeoutError) as error:
            # HTTP exceptions can include the credential-bearing request URL.
            raise RuntimeError(f"TMDB request failed ({type(error).__name__})") from None

    kind = parsed["kind"]
    if kind == "tv" and parsed.get("season") is None:
        return None
    if confirmed_id is None:
        params = {"query": parsed["title"], "include_adult": "false"}
        if kind == "movie" and parsed.get("year"):
            params["year"] = parsed["year"]
        data = await fetch(f"search/{kind}", params)
        # Refuse partial result sets: a later page could contain a remake.
        if not data or data.get("total_pages", 1) > 1:
            return None
        match = choose_match(parsed, data.get("results", []))
        if not match:
            return None
        confirmed_id = match["id"]
    detail = await fetch(f"{kind}/{int(confirmed_id)}")
    if not detail:
        return None
    season_data = {}
    if kind == "tv":
        season_data = await fetch(f"tv/{int(confirmed_id)}/season/{parsed['season']}")
        if not season_data:
            return None
        known = {item["episode_number"] for item in season_data.get("episodes", [])}
        if any(number not in known for number in parsed.get("episodes", [])):
            return None
    poster = season_data.get("poster_path") or detail.get("poster_path")
    result = {"id": int(confirmed_id), "kind": kind,
            "title": detail.get("title") or detail.get("name"),
            "year": (detail.get("release_date") or detail.get("first_air_date") or "")[:4],
            "poster": f"https://image.tmdb.org/t/p/w500{poster}" if poster else None,
            "overview": season_data.get("overview") or detail.get("overview", ""),
            "genres": [genre["name"] for genre in detail.get("genres", [])],
            "rating": float(detail.get("vote_average") or 0)}
    _cache_set(cache_key, result)
    return result

async def _fetch_legacy_multi(session, query):
    url = "https://api.themoviedb.org/3/search/multi"

    try:
        async with session.get(
            url,
            params=_request_params({"query": query, "include_adult": "false"}),
        ) as response:
            if response.status != 200:
                logger.warning("⚠️ TMDB Error: API returned status %s", response.status)
                return False, None

            data = await response.json()
            results = data.get("results", [])

            if not results:
                logger.debug("⚠️ TMDB Info: no results for query=%r", query)
                return True, None

            # Grab the first valid movie or TV show
            for item in results:
                if item.get("media_type") in ["movie", "tv"]:
                    title = item.get("title") or item.get("name")
                    poster_path = item.get("poster_path")
                    poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None

                    overview = item.get("overview", "")
                    if len(overview) > 150:
                        overview = overview[:147] + "..."

                    rating = item.get("vote_average", 0)

                    return True, {
                        "title": title,
                        "poster": poster_url,
                        "overview": overview,
                        "rating": round(rating, 1)
                    }
        return True, None
    except Exception as error:
        logger.warning("⚠️ TMDB Network Error: %s", type(error).__name__)
        return False, None
