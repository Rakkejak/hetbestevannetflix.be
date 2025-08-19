# update_all.py — weekly data pipeline for hetbestevannetflix.be
# - Fetch from uNoGS (RapidAPI) for Belgium (added-to-Netflix + pagination)
# - Enrich Trakt rating (via Trakt API, using TMDb id)
# - Filter to Netflix BE (TMDb watch/providers)
# - Enforce benchmark: require Trakt rating; IMDb>=8 or (if IMDb missing) TMDb>=8
# - Enforce 30% difference rule (only if IMDb present)
# - Build "recent" (last 90 days by dateAdded)
# - Normalize fields for the frontend (title, type, releaseDate, imdbRating, traktRating, dateAdded)

import os, sys, json, hashlib, datetime, time, requests
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).parent.resolve()
OUT_FULL = ROOT / "netflix_data.json"            # classics page
OUT_RECENT = ROOT / "netflix_last_month.json"    # recent page
DAYS_RECENT = 90

TMDB_API_KEY    = os.environ.get("TMDB_API_KEY", "")
TRAKT_CLIENT_ID = os.environ.get("TRAKT_CLIENT_ID", "")
UNOGS_API_KEY   = os.environ.get("UNOGS_API_KEY", "")

def sha12(p: Path) -> str:
    if not p.exists(): return "-"
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()[:12]

# ---------- helpers ----------
def _num_or_none(x):
    try:
        v = float(x)
        return None if v <= 0 else round(v, 1)
    except Exception:
        return None

def _parse_date(s):
    if s is None or s == "": return None
    s = str(s)
    # uNoGS 'ndate' is vaak epoch in milliseconden
    if s.isdigit():
        try:
            ts = int(s)
            if ts > 1_000_000_000_000:  # ms
                ts //= 1000
            return datetime.datetime.utcfromtimestamp(ts).date()
        except Exception:
            return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None

# ---------- TMDb: Netflix BE availability ----------
def is_on_netflix_be(tmdb_id: int, media_type: str) -> bool:
    if not TMDB_API_KEY or not tmdb_id:
        return False
    url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/watch/providers"
    try:
        r = requests.get(url, params={"api_key": TMDB_API_KEY}, timeout=15)
        r.raise_for_status()
        data = r.json() or {}
        be = (data.get("results") or {}).get("BE") or {}
        flatrate = be.get("flatrate") or []
        return any(p.get("provider_id") == 8 for p in flatrate)  # 8 = Netflix
    except Exception as e:
        print(f"[WARN] providers {media_type}/{tmdb_id}: {e}")
        return False

# ---------- Trakt rating via TMDb id ----------
_trakt_cache: Dict[str, Optional[float]] = {}

def get_trakt_rating_via_tmdb(tmdb_id: Optional[int], media_type: str) -> Optional[float]:
    """
    1) /search/tmdb/{id}?type=movie|show  -> pak eerste hit -> trakt id/slug
    2) /movies/{id}/ratings of /shows/{id}/ratings -> rating
    returns float 0..10 or None
    """
    if not tmdb_id or not TRAKT_CLIENT_ID:
        return None
    cache_key = f"{media_type}:{tmdb_id}"
    if cache_key in _trakt_cache:
        return _trakt_cache[cache_key]

    headers = {"Content-Type": "application/json", "trakt-api-version": "2", "trakt-api-key": TRAKT_CLIENT_ID}
    try:
               # 1) zoek mapping via tmdb id
        t = "movie" if media_type == "movie" else "show"

