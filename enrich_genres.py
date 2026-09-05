import json
import os
from pathlib import Path

import requests


ROOT = Path(__file__).parent.resolve()
DATA_PATH = ROOT / "netflix_data.json"
RECENT_PATH = ROOT / "netflix_last_month.json"
CACHE_PATH = ROOT / "tmdb_genre_cache.json"


def load_json(path, default):
    if not path.exists():
        return default
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def write_json_atomic(path, data):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    tmp.replace(path)


def media_type(item):
    return "movie" if item.get("type") == "Film" else "tv"


def fetch_metadata(item, api_key, cache):
    tmdb_id = item.get("tmdbId")
    if not tmdb_id:
        return [], None, None

    kind = media_type(item)
    key = f"{kind}:{tmdb_id}"
    cached = cache.get(key)

    # Cachevorm: genres + land + oorspronkelijke taal.
    if isinstance(cached, dict):
        genres = cached.get("genres", [])
        origin_country = cached.get("originCountry")
        original_language = cached.get("originalLanguage")

        if original_language and (kind == "movie" or "originCountry" in cached):
            return genres, origin_country, original_language

    response = requests.get(
        f"https://api.themoviedb.org/3/{kind}/{tmdb_id}",
        params={"api_key": api_key, "language": "en-US"},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()

    genres = [
        genre.get("name")
        for genre in payload.get("genres", [])
        if genre.get("name")
    ]

    origin_country = None
    if kind == "tv":
        countries = payload.get("origin_country", [])
        if countries:
            origin_country = countries[0]

    original_language = payload.get("original_language") or None

    cache[key] = {
        "genres": genres,
        "originCountry": origin_country,
        "originalLanguage": original_language,
    }

    return genres, origin_country, original_language


def main():
    api_key = os.getenv("TMDB_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("TMDB_API_KEY ontbreekt.")

    items = load_json(DATA_PATH, [])
    recent = load_json(RECENT_PATH, [])
    cache = load_json(CACHE_PATH, {})

    if not isinstance(items, list):
        raise SystemExit("netflix_data.json is geen lijst.")
    if not isinstance(recent, list):
        raise SystemExit("netflix_last_month.json is geen lijst.")
    if not isinstance(cache, dict):
        cache = {}

    total = len(items)

    for index, item in enumerate(items, start=1):
        genres, origin_country, original_language = fetch_metadata(item, api_key, cache)
        item["genres"] = genres

        if origin_country:
            item["originCountry"] = origin_country

        if original_language:
            item["originalLanguage"] = original_language

        if index == 1 or index % 25 == 0 or index == total:
            print(f"TMDb metadata: {index}/{total}")

    metadata_by_title = {
        (item.get("type"), item.get("tmdbId")): {
            "genres": item.get("genres", []),
            "originCountry": item.get("originCountry"),
            "originalLanguage": item.get("originalLanguage"),
        }
        for item in items
    }

    for item in recent:
        metadata = metadata_by_title.get(
            (item.get("type"), item.get("tmdbId")),
            {},
        )
        item["genres"] = metadata.get("genres", [])

        if metadata.get("originCountry"):
            item["originCountry"] = metadata["originCountry"]

        if metadata.get("originalLanguage"):
            item["originalLanguage"] = metadata["originalLanguage"]

    write_json_atomic(DATA_PATH, items)
    write_json_atomic(RECENT_PATH, recent)
    write_json_atomic(CACHE_PATH, cache)

    with_genres = sum(bool(item.get("genres")) for item in items)
    with_country = sum(bool(item.get("originCountry")) for item in items if item.get("type") == "Serie")
    with_language = sum(bool(item.get("originalLanguage")) for item in items)

    print(f"Genre-enrichment klaar: {with_genres}/{total} titels met genres.")
    print(f"Land-enrichment klaar: {with_country} reeksen met land van oorsprong.")
    print(f"Taal-enrichment klaar: {with_language}/{total} titels met oorspronkelijke taal.")


if __name__ == "__main__":
    main()
