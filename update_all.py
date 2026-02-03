# update_all.py — weekly data pipeline for hetbestevannetflix.be

import os, sys, json, hashlib, datetime, time
from typing import Any, Dict, List, Optional
from pathlib import Path

import requests

ROOT = Path(__file__).parent.resolve()
OUT_FULL = ROOT / "netflix_data.json"            # classics page
OUT_RECENT = ROOT / "netflix_last_month.json"    # recent page
DAYS_RECENT = 90

TMDB_API_KEY    = os.environ.get("TMDB_API_KEY", "")
TRAKT_CLIENT_ID = os.environ.get("TRAKT_CLIENT_ID", "")
UNOGS_API_KEY   = os.environ.get("UNOGS_API_KEY", "")

# Optioneel: ook TMDb providers checken (kost extra API-calls). Standaard uit.
REQUIRE_TMDB_PROVIDER_CHECK = os.environ.get("REQUIRE_TMDB_PROVIDER_CHECK", "0") == "1"

def sha12(p: Path) -> str:
    if not p.exists(): return "-"
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()[:12]

def _num_or_none(x) -> Optional[float]:
    try:
        v = float(x)
        return None if v <= 0 else round(v, 1)
    except Exception:
        return None

def _parse_date(s) -> Optional[datetime.date]:
    if s is None or s == "": return None
    s = str(s)
    # uNoGS 'ndate' is vaak epoch milliseconden
    if s.isdigit():
        try:
            ts = int(s)
            if ts > 1_000_000_000_000:  # ms -> s
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

# ---------- TMDb details (fallback voor releaseDate) ----------
def tmdb_detail_date(media_type: str, tmdb_id: int) -> Optional[str]:
    if not TMDB_API_KEY or not tmdb_id:
        return None
    try:
        url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}"
        r = requests.get(url, params={"api_key": TMDB_API_KEY}, timeout=15)
        r.raise_for_status()
        j = r.json() or {}
        if media_type == "movie":
            return j.get("release_date") or None
        else:
            return j.get("first_air_date") or None
    except Exception as e:
        print(f"[WARN] tmdb detail date {media_type}/{tmdb_id}: {e}")
        return None

# ---------- Trakt rating via TMDb id ----------
def get_trakt_rating_via_tmdb(tmdb_id: Optional[int], media_type: str) -> Optional[float]:
    """
    1) /search/tmdb/{id}?type=movie|show -> 1e hit
    2) /movies/{id}/ratings of /shows/{id}/ratings -> rating (0..10)
    """
    if not tmdb_id or not TRAKT_CLIENT_ID:
        return None

    headers = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": TRAKT_CLIENT_ID,   # client_id hier!
    }

    try:
        t = "movie" if media_type == "movie" else "show"
        # 1) mapping via TMDb-id
        url = f"https://api.trakt.tv/search/tmdb/{tmdb_id}?type={t}&limit=1"
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        arr = r.json() or []
        if not arr:
            return None

        ent = arr[0].get(t) or {}
        trakt_id = ent.get("ids", {}).get("trakt")
        if not trakt_id:
            return None

        # 2) ratings endpoint
        url2 = f"https://api.trakt.tv/{'movies' if t=='movie' else 'shows'}/{trakt_id}/ratings"
        r2 = requests.get(url2, headers=headers, timeout=20)
        r2.raise_for_status()
        rating = (r2.json() or {}).get("rating")
        val = _num_or_none(rating)
        time.sleep(0.12)  # simpele rate-limit
        return val
    except Exception as e:
        print(f"[WARN] Trakt rating fetch failed for tmdb {tmdb_id} ({media_type}): {e}")
        return None

# ---------- uNoGS fetch (BE; pagination) ----------
def fetch_candidates() -> List[Dict[str, Any]]:
    if not UNOGS_API_KEY:
        print("[ERROR] UNOGS_API_KEY missing – cannot fetch added dates.")
        return []

    url = "https://unogsng.p.rapidapi.com/search"
    headers = {
        "X-RapidAPI-Key": UNOGS_API_KEY,
        "X-RapidAPI-Host": "unogsng.p.rapidapi.com",
    }
    items: List[Dict[str, Any]] = []
    try:
        for t in ("movie", "series"):
            offset = 0
            while True:
                params = {
                    "type": t,
                    "countrylist": "21",   # Belgium
                    "orderby": "date",
                    "limit": "100",
                    "offset": str(offset),
                }
                r = requests.get(url, headers=headers, params=params, timeout=25)
                if r.status_code != 200:
                    print(f"[uNoGS] HTTP {r.status_code} body: {r.text[:300]}")
                    break
                data = r.json() or {}
                batch = data.get("results") or []
                if not batch:
                    break
                for x in batch:
                    items.append({
                        "title": x.get("title") or x.get("t"),
                        "type": t,  # 'movie'/'series'
                        "tmdb_id": x.get("tmid") or x.get("tmdbid") or None,
                        "releaseDate": (
                            (x.get("release_year") and f"{x['release_year']}-01-01")
                            or x.get("released") or ""
                        ),
                        "dateAdded": str(x.get("ndate") or ""),  # epoch ms als string
                        "imdbRating": x.get("imdbrating"),
                        "tmdb_vote_average": x.get("tmdb_rating") or x.get("rating"),
                    })
                offset += len(batch)
        print(f"[uNoGS] fetched {len(items)} items (movie+series)")
    except Exception as e:
        print(f"[WARN] uNoGS fetch failed: {e}")

    return items

# ---------- normaliseren + business rules ----------
def normalize_and_filter(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    BENCHMARK_MIN = 7.8
    out: List[Dict[str, Any]] = []
    
    for it in items:
        # 1. Alleen IMDb score checken
        imdb = _num_or_none(it.get("imdbRating"))
        if not imdb or imdb < BENCHMARK_MIN:
            continue

        # 2. Datum fix: uNoGS datum omzetten
        parsed_date = _parse_date(it.get("dateAdded"))
        final_date_added = parsed_date.isoformat() if parsed_date else "2020-01-01"

        # 3. ID's ophalen (maar niet verplicht stellen!)
        tmdb_id = it.get("tmdb_id") or it.get("tmid") or it.get("tmdbid")
        try:
            tmdb_id_int = int(tmdb_id) if tmdb_id else None
        except:
            tmdb_id_int = None

        # 4. Trakt score ophalen (alleen als ID er is)
        trakt_score = 0.0
        if tmdb_id_int and TRAKT_CLIENT_ID:
            trakt = get_trakt_rating_via_tmdb(tmdb_id_int, it.get("type"))
            trakt_score = float(trakt) if trakt else 0.0

        # 5. Titel opschonen
        title = (it.get("title") or "").replace("&#39;", "'").replace("&amp;", "&")

        norm = {
            "title": title,
            "type": "Series" if it.get("type") == "series" else "Film",
            "imdbRating": imdb,
            "traktRating": trakt_score,
            "releaseDate": it.get("releaseDate") or str(it.get("year") or ""),
            "dateAdded": final_date_added,
            "tmdb_id": tmdb_id_int,
        }
        out.append(norm)

    print(f"[STATS] Kept {len(out)} titles.")
    return out

# ---------- recent ----------
def build_recent(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    today = datetime.date.today()
    cutoff = today - datetime.timedelta(days=DAYS_RECENT)
    recent = []
    for it in items:
        d = _parse_date(it.get("dateAdded"))
        if d and cutoff <= d <= today:
            recent.append(it)
    return recent

# ---------- write ----------
def write_json(path: Path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

# ---------- main ----------
def main():
    print("=== UPDATE ALL START ===")
    candidates = fetch_candidates()
    if not candidates:
        print("[ERROR] No candidates – aborting.")
        sys.exit(1)

    if not TMDB_API_KEY and REQUIRE_TMDB_PROVIDER_CHECK:
        print("[ERROR] TMDB_API_KEY missing but provider check is required.")
        sys.exit(1)

    cleaned = normalize_and_filter(candidates)

    # Volledige set (klassiekers)
    h0 = sha12(OUT_FULL)
    write_json(OUT_FULL, cleaned)
    h1 = sha12(OUT_FULL)
    print(f"Wrote {OUT_FULL.name}  {h0} -> {h1}  (n={len(cleaned)})")

    # Recent (90 dagen o.b.v. dateAdded)
    recent = build_recent(cleaned)
    r0 = sha12(OUT_RECENT)
    write_json(OUT_RECENT, recent)
    r1 = sha12(OUT_RECENT)
    print(f"Wrote {OUT_RECENT.name} {r0} -> {r1} (n={len(recent)})")
    print("=== UPDATE ALL DONE ===")

if __name__ == "__main__":
    main()
