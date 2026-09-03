import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests

from config import REGION, NETFLIX_PROVIDER_ID, MAX_OMDB_CALLS_PER_RUN, TMDB_IMDB_CACHE_FILE, MIN_IMDB_RATING, MIN_IMDB_VOTES, NETFLIX_STATE_FILE, BUILD_DATA_FILE


ROOT = Path(__file__).parent.resolve()
IMDB_CACHE_FILE = ROOT / "imdb_cache.json"
TMDB_IMDB_CACHE_PATH = ROOT / TMDB_IMDB_CACHE_FILE
NETFLIX_STATE_PATH = ROOT / NETFLIX_STATE_FILE
BUILD_DATA_PATH = ROOT / BUILD_DATA_FILE



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



def cached_imdb_votes(cache, imdb_id):
    if not imdb_id:
        return 0

    for key in (f"imdb:{imdb_id}", imdb_id):
        value = cache.get(key)
        if not isinstance(value, dict):
            continue

        votes = value.get("votes", 0)

        try:
            return int(str(votes).replace(",", ""))
        except (TypeError, ValueError):
            continue

    return 0


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




def load_netflix_state():
    if not NETFLIX_STATE_PATH.exists():
        return {}

    with NETFLIX_STATE_PATH.open(encoding="utf-8") as f:
        data = json.load(f)

    return data if isinstance(data, dict) else {}


def save_netflix_state(state):
    with NETFLIX_STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)



def update_netflix_state(state, movies, series, today=None):
    if today is None:
        today = datetime.now(timezone.utc).date().isoformat()

    current_keys = {
        *(f"movie:{item['id']}" for item in movies if item.get("id")),
        *(f"tv:{item['id']}" for item in series if item.get("id")),
    }

    is_baseline = not state

    # Eerst alleen titels markeren die verdwenen zijn.
    for key, entry in state.items():
        if isinstance(entry, dict) and key not in current_keys:
            entry["active"] = False

    # Daarna huidige catalogus verwerken.
    for key in current_keys:
        if key not in state:
            state[key] = {
                "active": True,
                "firstSeen": None if is_baseline else today,
            }
            continue

        entry = state[key]
        if not isinstance(entry, dict):
            entry = {"active": False, "firstSeen": None}
            state[key] = entry

        was_active = bool(entry.get("active"))

        if not was_active:
            entry["firstSeen"] = today

        entry["active"] = True

    return state


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
        return None, None, 0

    score = cached_imdb_score(imdb_cache, imdb_id)
    votes = cached_imdb_votes(imdb_cache, imdb_id)

    # Oude cachewaarden zonder stemmentotaal zijn onvolledig:
    # controleer ze opnieuw via OMDb.
    if score is not None and votes > 0:
        return imdb_id, score, votes

    data = fetch_omdb_data(imdb_id, call_state)
    score = store_omdb_in_cache(imdb_cache, imdb_id, data)
    votes = cached_imdb_votes(imdb_cache, imdb_id)

    return imdb_id, score, votes


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



def process_tmdb_item(
    item,
    media_type,
    tmdb_imdb_cache,
    imdb_cache,
    call_state,
    netflix_state=None,
):
    tmdb_id = item.get("id")
    if not tmdb_id:
        return None

    imdb_id, imdb_score, imdb_votes = resolve_imdb_score(
        media_type,
        tmdb_id,
        tmdb_imdb_cache,
        imdb_cache,
        call_state,
    )

    if (
        imdb_score is None
        or imdb_score < MIN_IMDB_RATING
        or imdb_votes < MIN_IMDB_VOTES
    ):
        return None

    if media_type == "movie":
        title = item.get("title")
        release_date = item.get("release_date") or ""
        item_type = "Film"
    else:
        title = item.get("name")
        release_date = item.get("first_air_date") or ""
        item_type = "Serie"

    return {
        "tmdbId": tmdb_id,
        "imdbId": imdb_id,
        "title": title,
        "type": item_type,
        "imdbRating": imdb_score,
        "imdbVotes": imdb_votes,
        "releaseDate": release_date,
        "dateAdded": (
            (netflix_state or {})
            .get(f"{media_type}:{tmdb_id}", {})
            .get("firstSeen")
        ),
        "poster_path": item.get("poster_path"),
        "availableBE": True,
        "ratingSource": "imdb",
    }


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





def save_build_data(items):
    with BUILD_DATA_PATH.open("w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)


def save_caches(tmdb_imdb_cache, imdb_cache):
    save_tmdb_imdb_cache(tmdb_imdb_cache)
    save_imdb_cache(imdb_cache)


def process_catalog(
    items,
    media_type,
    tmdb_imdb_cache,
    imdb_cache,
    call_state,
    netflix_state=None,
):
    results = []
    stopped_at_limit = False

    for index, item in enumerate(items, start=1):
        try:
            result = process_tmdb_item(
                item,
                media_type,
                tmdb_imdb_cache,
                imdb_cache,
                call_state,
                netflix_state,
            )
        except RuntimeError as exc:
            if "OMDb veiligheidslimiet bereikt" not in str(exc):
                raise

            stopped_at_limit = True
            print(f"{media_type}: gestopt bij item {index}/{len(items)} wegens OMDb-limiet.")
            break

        if result is not None:
            results.append(result)

    return results, stopped_at_limit


def main():
    load_local_env()

    movies = fetch_netflix_catalog("movie")
    series = fetch_netflix_catalog("tv")

    tmdb_imdb_cache = load_tmdb_imdb_cache()
    imdb_cache = load_imdb_cache()
    netflix_state = load_netflix_state()

    netflix_state = update_netflix_state(
        netflix_state,
        movies,
        series,
    )

    call_state = {"calls": 0}

    processed_movies, stopped_movies = process_catalog(
        movies,
        "movie",
        tmdb_imdb_cache,
        imdb_cache,
        call_state,
        netflix_state,
    )

    processed_series = []
    stopped_series = False

    if not stopped_movies:
        processed_series, stopped_series = process_catalog(
            series,
            "tv",
            tmdb_imdb_cache,
            imdb_cache,
            call_state,
            netflix_state,
        )

    results = processed_movies + processed_series

    save_caches(tmdb_imdb_cache, imdb_cache)
    save_netflix_state(netflix_state)
    save_build_data(results)

    complete = not stopped_movies and not stopped_series

    print()
    print("=== RESULTAAT ===")
    print("Netflix BE films gevonden:", len(movies))
    print("Netflix BE series gevonden:", len(series))
    print("Titels die voldoen:", len(results))
    print("OMDb-calls gebruikt:", call_state["calls"])
    print("Volledige run:", complete)

    if not complete:
        print("Productiedata NIET overschreven.")
        print(f"Tussentijdse resultaten staan in {BUILD_DATA_FILE}.")


if __name__ == "__main__":
    main()
