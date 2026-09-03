import json
import os
from pathlib import Path

import requests

from config import REGION, NETFLIX_PROVIDER_ID, MAX_OMDB_CALLS_PER_RUN


ROOT = Path(__file__).parent.resolve()
IMDB_CACHE_FILE = ROOT / "imdb_cache.json"



def load_local_env():
    env_file = ROOT / ".env"
    if not env_file.exists():
        return

    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def require_tmdb_key():
    key = os.getenv("TMDB_API_KEY", "").strip()
    if not key:
        raise SystemExit("TMDB_API_KEY ontbreekt.")
    return key



def require_omdb_key():
    key = os.getenv("OMDB_API_KEY", "").strip()
    if not key:
        raise SystemExit("OMDB_API_KEY ontbreekt.")
    return key


def load_imdb_cache():
    if not IMDB_CACHE_FILE.exists():
        return {}

    with IMDB_CACHE_FILE.open(encoding="utf-8") as f:
        data = json.load(f)

    return data if isinstance(data, dict) else {}


def cached_imdb_score(cache, imdb_id):
    if not imdb_id:
        return None

    for key in (f"imdb:{imdb_id}", imdb_id):
        value = cache.get(key)
        if not isinstance(value, dict):
            continue

        score = value.get("val")
        if score is None:
            score = value.get("imdb")

        try:
            return float(score)
        except (TypeError, ValueError):
            continue

    return None


def fetch_imdb_id_from_tmdb(media_type, tmdb_id):
    api_key = require_tmdb_key()

    response = requests.get(
        f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/external_ids",
        params={"api_key": api_key},
        timeout=30,
    )
    response.raise_for_status()

    return response.json().get("imdb_id")



def fetch_omdb_data(imdb_id, call_state):
    if not imdb_id:
        return None

    if call_state["calls"] >= MAX_OMDB_CALLS_PER_RUN:
        raise RuntimeError(
            f"OMDb veiligheidslimiet bereikt: {MAX_OMDB_CALLS_PER_RUN} calls."
        )

    api_key = require_omdb_key()

    response = requests.get(
        "https://www.omdbapi.com/",
        params={
            "apikey": api_key,
            "i": imdb_id,
        },
        timeout=30,
    )
    response.raise_for_status()
    call_state["calls"] += 1

    data = response.json()

    if data.get("Response") != "True":
        return None

    return data


def fetch_netflix_catalog(media_type):
    api_key = require_tmdb_key()

    if media_type == "movie":
        endpoint = "https://api.themoviedb.org/3/discover/movie"
    elif media_type == "tv":
        endpoint = "https://api.themoviedb.org/3/discover/tv"
    else:
        raise ValueError(f"Onbekend media_type: {media_type}")

    items = []
    page = 1

    while True:
        response = requests.get(
            endpoint,
            params={
                "api_key": api_key,
                "watch_region": REGION,
                "with_watch_providers": NETFLIX_PROVIDER_ID,
                "with_watch_monetization_types": "flatrate",
                "page": page,
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        results = data.get("results", [])
        items.extend(results)

        total_pages = int(data.get("total_pages", 1))
        print(f"{media_type}: pagina {page}/{total_pages} · totaal {len(items)}")

        if page >= total_pages:
            break

        page += 1

    return items


def main():
    load_local_env()

    movies = fetch_netflix_catalog("movie")
    series = fetch_netflix_catalog("tv")

    print()
    print("=== RESULTAAT ===")
    print("Netflix BE films:", len(movies))
    print("Netflix BE series:", len(series))
    print("Totaal:", len(movies) + len(series))


if __name__ == "__main__":
    main()
