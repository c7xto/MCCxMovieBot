import os
import aiohttp
import logging
from urllib.parse import quote
from dotenv import load_dotenv

# 1. Force load the .env file immediately so the key isn't blank
load_dotenv()

logger = logging.getLogger(__name__)

async def get_movie_data(query):
    # 2. Fetch the key directly inside the function
    TMDB_API_KEY = os.getenv("TMDB_API_KEY")
    
    if not TMDB_API_KEY:
        logger.warning("⚠️ TMDB Error: API Key is missing from .env!")
        return None

    safe_query = quote(query)
    url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={safe_query}"

    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    logger.warning(f"⚠️ TMDB Error: API returned status {response.status}")
                    return None
                    
                data = await response.json()
                results = data.get("results", [])
                
                if not results:
                    logger.debug(f"⚠️ TMDB Info: No results found for '{query}' on TMDB.")
                    return None
                
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
                        
                        return {
                            "title": title,
                            "poster": poster_url,
                            "overview": overview,
                            "rating": round(rating, 1)
                        }
        return None
    except Exception as e:
        logger.warning(f"⚠️ TMDB Network Error: {e}")
        return None