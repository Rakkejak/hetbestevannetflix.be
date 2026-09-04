import gzip
import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

from config import (
    REGION,
    NETFLIX_PROVIDER_ID,
    MAX_OMDB_CALLS_PER_RUN,
    MAX_OMDB_CALLS_PER_DAY,
    OMDB_DAILY_USAGE_FILE,
    TMDB_IMDB_CACHE_FILE,
    MIN_IMDB_RATING,
    MIN_IMDB_VOTES,
    NETFLIX_STATE_FILE,
    BUILD_DATA_FILE,
)


ROOT = Path(__file__).parent.resolve()
IMDB_CACHE_FILE = ROOT / "imdb_cache.json"
TMDB_IMDB_CACHE_PATH = ROOT / TMDB_IMDB_CACHE_FILE
NETFLIX_STATE_PATH = ROOT / NETFLIX_STATE_FILE
BUILD_DATA_PATH = ROOT / BUILD_DATA_FILE
OMDB_DAILY_USAGE_PATH = ROOT / OMDB_DAILY_USAGE_FILE
NETFLIX_ID_CACHE_PATH = ROOT / "netflix_id_cache.json"
IMDB_DAILY_RATINGS_PATH = ROOT / "imdb_daily_ratings.tsv.gz"
MANUAL_AVAILABILITY_OVERRIDES_PATH = ROOT / "manual_availability_overrides.json"
PRODUCT_DATA_PATH = ROOT / "netflix_data.json"
RECENT_DATA_PATH = ROOT / "netflix_last_month.json"



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


def download_imdb_daily_ratings():
    if IMDB_DAILY_RATINGS_PATH.exists():
        age_seconds = time.time() - IMDB_DAILY_RATINGS_PATH.stat().st_mtime

        if age_seconds < 7 * 24 * 60 * 60:
            print("IMDb Daily: lokale kopie jonger dan 7 dagen hergebruikt.")
            return IMDB_DAILY_RATINGS_PATH

    url = "https://datasets.imdbws.com/title.ratings.tsv.gz"
    tmp_path = IMDB_DAILY_RATINGS_PATH.with_suffix(".tmp")

    print("IMDb Daily: nieuwe dataset downloaden...")
    urllib.request.urlretrieve(url, tmp_path)
    tmp_path.replace(IMDB_DAILY_RATINGS_PATH)

    return IMDB_DAILY_RATINGS_PATH


def load_imdb_daily_ratings(path=None):
    if path is None:
        path = IMDB_DAILY_RATINGS_PATH

    ratings = {}

    with gzip.open(path, "rt", encoding="utf-8") as f:
        header = next(f, None)

        for line in f:
            imdb_id, rating, votes = line.rstrip("\n").split("\t")

            ratings[imdb_id] = {
                "rating": float(rating),
                "votes": int(votes),
            }

    return ratings


def load_manual_availability_overrides():
    if not MANUAL_AVAILABILITY_OVERRIDES_PATH.exists():
        return {}

    try:
        data = json.loads(
            MANUAL_AVAILABILITY_OVERRIDES_PATH.read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError):
        return {}

    return data if isinstance(data, dict) else {}


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




def load_legacy_netflix_id_map():
    path = ROOT / "legacy_netflix_data.json"

    if not path.exists():
        return {}

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    result = {}

    for item in data if isinstance(data, list) else []:
        netflix_id = item.get("nfid") or item.get("netflix_id")
        if not netflix_id:
            continue

        title = str(item.get("title", "")).strip().casefold()
        item_type = item.get("type")
        year = str(item.get("releaseDate", ""))[:4]

        if title and item_type and year:
            result[(title, item_type, year)] = str(netflix_id)

    return result


def load_netflix_id_cache():
    if not NETFLIX_ID_CACHE_PATH.exists():
        return {}

    try:
        data = json.loads(NETFLIX_ID_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    return data if isinstance(data, dict) else {}


def save_netflix_id_cache(cache):
    with NETFLIX_ID_CACHE_PATH.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2, sort_keys=True)


def fetch_netflix_id_from_wikidata(imdb_id, cache=None):
    if not imdb_id:
        return None

    if cache is not None and cache.get(imdb_id):
        return cache[imdb_id]

    query = f"""
    SELECT ?netflix WHERE {{
      ?item wdt:P345 "{imdb_id}" ;
            wdt:P1874 ?netflix .
    }}
    """

    response = None

    for attempt in range(3):
        try:
            response = requests.get(
                "https://query.wikidata.org/sparql",
                params={"query": query, "format": "json"},
                headers={
                    "User-Agent": "HetBesteVanNetflix/1.0 https://hetbestevannetflix.be"
                },
                timeout=30,
            )

            if response.status_code == 429 or response.status_code >= 500:
                if attempt < 2:
                    time.sleep(2 ** attempt)
                    continue

            response.raise_for_status()
            break

        except requests.RequestException as exc:
            if attempt < 2:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError("Wikidata tijdelijk onbeschikbaar.") from exc

    rows = response.json().get("results", {}).get("bindings", [])
    netflix_id = (
        rows[0].get("netflix", {}).get("value")
        if rows
        else None
    )

    if cache is not None:
        if netflix_id:
            cache[imdb_id] = netflix_id
        else:
            cache.pop(imdb_id, None)

    return netflix_id


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




def resolve_imdb_score(media_type, tmdb_id, tmdb_imdb_cache, imdb_daily_ratings):
    imdb_id = fetch_imdb_id_from_tmdb(media_type, tmdb_id, tmdb_imdb_cache)

    if not imdb_id:
        return None, None, 0

    current = imdb_daily_ratings.get(imdb_id)

    if not isinstance(current, dict):
        return imdb_id, None, 0

    try:
        score = float(current.get("rating"))
        votes = int(current.get("votes", 0))
    except (TypeError, ValueError):
        return imdb_id, None, 0

    return imdb_id, score, votes


def load_omdb_daily_usage():
    today = datetime.now(timezone.utc).date().isoformat()

    if not OMDB_DAILY_USAGE_PATH.exists():
        return {"date": today, "calls": 0}

    try:
        data = json.loads(OMDB_DAILY_USAGE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"date": today, "calls": 0}

    if data.get("date") != today:
        return {"date": today, "calls": 0}

    try:
        calls = int(data.get("calls", 0))
    except (TypeError, ValueError):
        calls = 0

    return {"date": today, "calls": max(calls, 0)}


def reserve_omdb_daily_call():
    usage = load_omdb_daily_usage()

    if usage["calls"] >= MAX_OMDB_CALLS_PER_DAY:
        raise RuntimeError(
            f"OMDb veiligheidslimiet bereikt: daglimiet "
            f"{MAX_OMDB_CALLS_PER_DAY} calls."
        )

    usage["calls"] += 1

    tmp_path = OMDB_DAILY_USAGE_PATH.with_suffix(".tmp")
    tmp_path.write_text(
        json.dumps(usage, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(OMDB_DAILY_USAGE_PATH)

    return usage["calls"]


def fetch_omdb_data(imdb_id, call_state):
    if not imdb_id:
        return None

    call_limit = call_state.get("limit", MAX_OMDB_CALLS_PER_RUN)

    if call_state["calls"] >= call_limit:
        raise RuntimeError(
            f"OMDb veiligheidslimiet bereikt: {call_limit} calls."
        )

    api_key = require_omdb_key()

    # Reserveer de call vóór de request, zodat ook mislukte requests
    # tegen de dagelijkse veiligheidslimiet tellen.
    call_state["daily_calls"] = reserve_omdb_daily_call()
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
    imdb_daily_ratings,
    netflix_state=None,
    netflix_id_cache=None,
    legacy_netflix_ids=None,
    manual_availability_overrides=None,
):
    tmdb_id = item.get("id")
    if not tmdb_id:
        return None

    imdb_id, imdb_score, imdb_votes = resolve_imdb_score(
        media_type,
        tmdb_id,
        tmdb_imdb_cache,
        imdb_daily_ratings,
    )

    if (
        imdb_score is None
        or imdb_score < MIN_IMDB_RATING
        or imdb_votes < MIN_IMDB_VOTES
    ):
        return None

    if (manual_availability_overrides or {}).get(imdb_id) is False:
        return None

    if media_type == "movie":
        legacy_type = "Film"
        legacy_title = str(item.get("title", "")).strip().casefold()
        legacy_year = str(item.get("release_date", ""))[:4]
    else:
        legacy_type = "Series"
        legacy_title = str(item.get("name", "")).strip().casefold()
        legacy_year = str(item.get("first_air_date", ""))[:4]

    legacy_key = (legacy_title, legacy_type, legacy_year)
    netflix_id = (legacy_netflix_ids or {}).get(legacy_key)

    if netflix_id:
        if netflix_id_cache is not None:
            netflix_id_cache[imdb_id] = netflix_id
    else:
        netflix_id = fetch_netflix_id_from_wikidata(
            imdb_id,
            netflix_id_cache,
        )

    if media_type == "movie":
        title = item.get("title")
        original_title = item.get("original_title") or title
        release_date = item.get("release_date") or ""
        item_type = "Film"
    else:
        title = item.get("name")
        original_title = item.get("original_name") or title
        release_date = item.get("first_air_date") or ""
        item_type = "Serie"

    return {
        "tmdbId": tmdb_id,
        "imdbId": imdb_id,
        "netflix_id": netflix_id,
        "title": title,
        "originalTitle": original_title,
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



def print_progress(label, current, total, extra=""):
    total = max(total, 1)
    current = min(current, total)
    fraction = current / total
    width = 30
    filled = round(width * fraction)
    bar = "█" * filled + "░" * (width - filled)
    percent = fraction * 100

    suffix = f" · {extra}" if extra else ""
    print(
        f"\r\033[2K{label}: [{bar}] {percent:6.2f}% · {current}/{total}{suffix}",
        end="",
        flush=True,
    )

    if current >= total:
        print()


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
                "language": "en-US",
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


def write_json_atomic(path, data):
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(path)


def active_product_items(items, netflix_state):
    active = []

    for item in items:
        tmdb_id = item.get("tmdbId")
        if not tmdb_id:
            continue

        media_type = "movie" if item.get("type") == "Film" else "tv"
        state_entry = netflix_state.get(f"{media_type}:{tmdb_id}", {})

        if not isinstance(state_entry, dict) or not state_entry.get("active"):
            continue

        active.append(item)

    return active


def recent_product_items(items, today=None):
    if today is None:
        today = datetime.now(timezone.utc).date()

    cutoff = today - timedelta(days=90)
    recent = []

    for item in items:
        raw = item.get("dateAdded")
        if not raw:
            continue

        try:
            added = datetime.fromisoformat(str(raw)).date()
        except ValueError:
            continue

        if cutoff <= added <= today:
            recent.append(item)

    return recent


def dedupe_product_items(items):
    seen_tmdb_ids = set()
    seen_imdb_ids = set()
    seen_netflix_ids = set()
    deduped = []

    for item in items:
        media_type = str(item.get("type") or "")
        tmdb_id = item.get("tmdbId")
        imdb_id = str(item.get("imdbId") or "").strip()
        netflix_id = str(item.get("netflix_id") or "").strip()

        tmdb_key = (media_type, tmdb_id) if tmdb_id else None

        if tmdb_key and tmdb_key in seen_tmdb_ids:
            continue
        if imdb_id and imdb_id in seen_imdb_ids:
            continue
        if netflix_id and netflix_id in seen_netflix_ids:
            continue

        if tmdb_key:
            seen_tmdb_ids.add(tmdb_key)
        if imdb_id:
            seen_imdb_ids.add(imdb_id)
        if netflix_id:
            seen_netflix_ids.add(netflix_id)

        deduped.append(item)

    return deduped


def publish_product_data(items, netflix_state):
    active = dedupe_product_items(
        active_product_items(items, netflix_state)
    )
    recent = recent_product_items(active)

    write_json_atomic(PRODUCT_DATA_PATH, active)
    write_json_atomic(RECENT_DATA_PATH, recent)

    return len(active), len(recent)


def save_caches(tmdb_imdb_cache, imdb_cache):
    save_tmdb_imdb_cache(tmdb_imdb_cache)
    save_imdb_cache(imdb_cache)


def process_catalog(
    items,
    media_type,
    tmdb_imdb_cache,
    imdb_daily_ratings,
    netflix_state=None,
    netflix_id_cache=None,
    legacy_netflix_ids=None,
    manual_availability_overrides=None,
):
    results = []
    stopped_early = False

    label = "Ratings films" if media_type == "movie" else "Ratings series"

    print_progress(
        label,
        0,
        len(items),
        "geselecteerd 0",
    )

    for index, item in enumerate(items, start=1):
        try:
            result = process_tmdb_item(
                item,
                media_type,
                tmdb_imdb_cache,
                imdb_daily_ratings,
                netflix_state,
                netflix_id_cache,
                legacy_netflix_ids,
                manual_availability_overrides,
            )
        except RuntimeError as exc:
            message = str(exc)

            if "Wikidata tijdelijk onbeschikbaar" not in message:
                raise

            stopped_early = True
            save_tmdb_imdb_cache(tmdb_imdb_cache)
            print()
            print(
                f"{media_type}: gestopt bij item {index}/{len(items)} "
                "omdat Wikidata tijdelijk onbeschikbaar is."
            )
            break

        if result is not None:
            results.append(result)

        if index % 50 == 0:
            save_tmdb_imdb_cache(tmdb_imdb_cache)

        print_progress(
            label,
            index,
            len(items),
            f"geselecteerd {len(results)}",
        )

    return results, stopped_early

def main():
    load_local_env()

    movies = fetch_netflix_catalog("movie")
    series = fetch_netflix_catalog("tv")

    print("IMDb Daily ratings downloaden...")
    ratings_path = download_imdb_daily_ratings()
    imdb_daily_ratings = load_imdb_daily_ratings(ratings_path)
    print("IMDb Daily ratings geladen:", len(imdb_daily_ratings))

    tmdb_imdb_cache = load_tmdb_imdb_cache()
    netflix_state = load_netflix_state()
    netflix_id_cache = load_netflix_id_cache()
    legacy_netflix_ids = load_legacy_netflix_id_map()
    manual_availability_overrides = load_manual_availability_overrides()

    netflix_state = update_netflix_state(
        netflix_state,
        movies,
        series,
    )

    processed_movies, stopped_movies = process_catalog(
        movies,
        "movie",
        tmdb_imdb_cache,
        imdb_daily_ratings,
        netflix_state,
        netflix_id_cache,
        legacy_netflix_ids,
        manual_availability_overrides,
    )

    processed_series, stopped_series = process_catalog(
        series,
        "tv",
        tmdb_imdb_cache,
        imdb_daily_ratings,
        netflix_state,
        netflix_id_cache,
        legacy_netflix_ids,
        manual_availability_overrides,
    )

    results = dedupe_product_items(
        processed_movies + processed_series
    )

    save_tmdb_imdb_cache(tmdb_imdb_cache)
    save_netflix_state(netflix_state)
    save_netflix_id_cache(netflix_id_cache)
    save_build_data(results)

    complete = not stopped_movies and not stopped_series
    build_only = os.getenv("BUILD_ONLY") == "1"

    print()
    print("=== RESULTAAT ===")
    print("Netflix BE films gevonden:", len(movies))
    print("Netflix BE series gevonden:", len(series))
    print("Titels die voldoen:", len(results))
    print("Ratingbron: IMDb Daily")
    print("Volledige run:", complete)

    if complete and not build_only:
        active_count, recent_count = publish_product_data(
            results,
            netflix_state,
        )
        print("Productiedata veilig gepubliceerd.")
        print("Actieve titels gepubliceerd:", active_count)
        print("Recente toevoegingen gepubliceerd:", recent_count)
    else:
        print("Productiedata NIET overschreven.")
        print(f"Resultaten staan in {BUILD_DATA_FILE}.")
        if complete and build_only:
            print("Build-only modus actief.")


if __name__ == "__main__":
    main()
