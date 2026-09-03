import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests

from config import REGION, NETFLIX_PROVIDER_ID, MAX_OMDB_CALLS_PER_RUN, TMDB_IMDB_CACHE_FILE


ROOT = Path(__file__).parent.resolve()
IMDB_CACHE_FILE = ROOT / "imdb_cache.json"
TMDB_IMDB_CACHE_PATH = ROOT / TMDB_IMDB_CACHE_FILE



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



def save_imdb_cache(cache):
    with IMDB_CACHE_FILE.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2, sort_keys=True)


def store_omdb_in_cache(cache, imdb_id, data):
    if not imdb_id or not data:
        return None

    try:
        score = float(data.get("imdbRating"))
    except (TypeError, ValueError):
        return None

    votes_raw = str(data.get("imdbVotes", "")).replace(",", "")
    try:
        votes = int(votes_raw)
    except ValueError:
        votes = 0

    cache[f"imdb:{imdb_id}"] = {
        "val": score,
        "votes": votes,
        "ts": datetime.now(timezone.utc).isoformat(),
    }

    return score


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



def load_tmdb_imdb_cache():
    if not TMDB_IMDB_CACHE_PATH.exists():
        return {}

    with TMDB_IMDB_CACHE_PATH.open(encoding="utf-8") as f:
        data = json.load(f)

    return data if isinstance(data, dict) else {}


def save_tmdb_imdb_cache(cache):
    with TMDB_IMDB_CACHE_PATH.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2, sort_keys=True)


def fetch_imdb_id_from_tmdb(media_type, tmdb_id, cache=None):
    cache_key = f"{media_type}:{tmdb_id}"

    if cache is not None and cache_key in cache:
        return cache[cache_key] or None

    api_key = require_tmdb_key()

    response = requests.get(
        f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/external_ids",
        params={"api_key": api_key},
        timeout=30,
    )
    response.raise_for_status()

    imdb_id = response.json().get("imdb_id")

    if cache is not None:
        cache[cache_key] = imdb_id

    return imdb_id




def resolve_imdb_score(media_type, tmdb_id, tmdb_imdb_cache, imdb_cache, call_state):
    imdb_id = fetch_imdb_id_from_tmdb(media_type, tmdb_id, tmdb_imdb_cache)

    if not imdb_id:
        return None, None

    score = cached_imdb_score(imdb_cache, imdb_id)
    if score is not None:
        return imdb_id, score

    data = fetch_omdb_data(imdb_id, call_state)
    score = store_omdb_in_cache(imdb_cache, imdb_id, data)

    return imdb_id, score


def fetch_omdb_data(imdb_id, call_state):
    if not imdb_id:
        return None

    if call_state["calls"] >= MAX_OMDB_CALLS_PER_RUN:
        raise RuntimeError(
            f"OMDb veiligheidslimiet bereikt: {MAX_OMDB_CALLS_PER_RUN} calls."
        )

    api_key = require_omdb_key()
    call_state["calls"] += 1

    response = requests.get(
        "https://www.omdbapi.com/",
        params={
            "apikey": api_key,
            "i": imdb_id,
        },
        timeout=30,
    )
    response.raise_for_status()

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
