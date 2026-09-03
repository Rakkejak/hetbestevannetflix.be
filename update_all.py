import os
from pathlib import Path

import requests

from config import REGION, NETFLIX_PROVIDER_ID


ROOT = Path(__file__).parent.resolve()


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
