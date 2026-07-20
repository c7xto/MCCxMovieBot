import os
import time
import aiohttp
import logging
from collections import OrderedDict
from urllib.parse import quote
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
    TMDB_API_KEY = os.getenv("TMDB_API_KEY")

    if not TMDB_API_KEY:
        logger.warning("⚠️ TMDB Error: API Key is missing from .env!")
        return False, None

    safe_query = quote(query)
    url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={safe_query}"

    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    logger.warning(f"⚠️ TMDB Error: API returned status {response.status}")
                    return False, None

                data = await response.json()
                results = data.get("results", [])

                if not results:
                    logger.debug(f"⚠️ TMDB Info: No results found for '{query}' on TMDB.")
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
    except Exception as e:
        logger.warning(f"⚠️ TMDB Network Error: {e}")
        return False, None
