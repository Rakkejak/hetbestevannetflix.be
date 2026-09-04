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


def fetch_genres(item, api_key, cache):
    tmdb_id = item.get("tmdbId")
    if not tmdb_id:
        return []

    kind = media_type(item)
    key = f"{kind}:{tmdb_id}"

    cached = cache.get(key)
    if isinstance(cached, list):
        return cached

    response = requests.get(
        f"https://api.themoviedb.org/3/{kind}/{tmdb_id}",
        params={"api_key": api_key, "language": "en-US"},
        timeout=30,
    )
    response.raise_for_status()

    genres = [
        genre.get("name")
        for genre in response.json().get("genres", [])
        if genre.get("name")
    ]
    cache[key] = genres
    return genres


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
        item["genres"] = fetch_genres(item, api_key, cache)
        if index == 1 or index % 25 == 0 or index == total:
            print(f"Genres: {index}/{total}")

    genres_by_title = {
        (item.get("type"), item.get("tmdbId")): item.get("genres", [])
        for item in items
    }

    for item in recent:
        item["genres"] = genres_by_title.get(
            (item.get("type"), item.get("tmdbId")),
            [],
        )

    write_json_atomic(DATA_PATH, items)
    write_json_atomic(RECENT_PATH, recent)
    write_json_atomic(CACHE_PATH, cache)

    with_genres = sum(bool(item.get("genres")) for item in items)
    print(f"Genre-enrichment klaar: {with_genres}/{total} titels met genres.")


if __name__ == "__main__":
    main()
