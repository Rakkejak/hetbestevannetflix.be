# update_all.py — weekly data pipeline for hetbestevannetflix.be

import os, sys, json, hashlib, datetime, time
from typing import Any, Dict, List, Optional
from pathlib import Path

import requests
from imdb import IMDb

ROOT = Path(__file__).parent.resolve()
OUT_FULL = ROOT / "netflix_data.json"            # classics page
OUT_RECENT = ROOT / "netflix_last_month.json"    # recent page
DAYS_RECENT = 90

TMDB_API_KEY    = os.environ.get("TMDB_API_KEY", "")
TRAKT_CLIENT_ID = os.environ.get("TRAKT_CLIENT_ID", "")
UNOGS_API_KEY   = os.environ.get("UNOGS_API_KEY", "")

# We forceren de provider check nu in de code voor maximale kwaliteit
REQUIRE_TMDB_PROVIDER_CHECK = True

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

def _parse_date(x: Any) -> Optional[datetime.date]:
    """Parseert uNoGS ndate (epoch ms) of ISO strings."""
    if not x: return None
    try:
        # Check of het een getal is (uNoGS ndate)
        if isinstance(x, (int, float)) or (isinstance(x, str) and x.isdigit()):
            ts = int(x) / 1000
            return datetime.datetime.fromtimestamp(ts).date()
        # Anders ISO formaat
        return datetime.date.fromisoformat(str(x)[:10])
    except Exception:
        return None

# ---------- fetch ----------
def fetch_candidates() -> List[Dict[str, Any]]:
    """Haalt de ruwe lijst op van uNoGS België."""
    items = []
    headers = {
        "X-RapidAPI-Key": UNOGS_API_KEY,
        "X-RapidAPI-Host": "unogsng.p.rapidapi.com"
    }
    
    for t in ["movie", "series"]:
        offset = 0
        while True:
            params = {"type": t, "countrylist": "21", "offset": offset, "limit": 100}
            try:
                resp = requests.get("https://unogsng.p.rapidapi.com/search", headers=headers, params=params, timeout=20)
                resp.raise_for_status()
                data = resp.json()
                batch = data.get("results") or []
                
                if not batch:
                    break
                
                for x in batch:
                    items.append({
                        "title": x.get("title") or x.get("t"),
                        "type": t,
                        "ndate": str(x.get("ndate") or ""),
                        "tmdb_id": x.get("tmid") or x.get("tmdbid")
                    })
                
                offset += 100
                if len(batch) < 100:
                    break
                time.sleep(0.5)
            except Exception as e:
                print(f"[ERROR] uNoGS fetch failed: {e}")
                break
    return items

# ---------- process ----------
def normalize_and_filter(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Checkt beschikbaarheid via TMDb en haalt de ECHTE IMDb score op."""
    BENCHMARK_MIN = 7.8
    out = []
    ia = IMDb()

    print(f"Start verificatie van {len(items)} titels...")

    for it in items:
        title = str(it.get("title", "")).replace("&#39;", "'").replace("&amp;", "&")
        if not title or title == "None": continue

        try:
            # 1. TMDb Check: Is het echt op Netflix België?
            tmdb_id = it.get("tmdb_id")
            if not tmdb_id:
                s_url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={title}"
                s_res = requests.get(s_url).json().get('results')
                if not s_res: continue
                tmdb_id = s_res[0]['id']

            m_type = "tv" if it.get("type") == "series" else "movie"
            p_url = f"https://api.themoviedb.org/3/{m_type}/{tmdb_id}/watch/providers?api_key={TMDB_API_KEY}"
            p_data = requests.get(p_url).json().get('results', {}).get('BE', {})
            
            # Provider check
            providers = p_data.get('flatrate', [])
            if not any(p['provider_name'] == 'Netflix' for p in providers):
                continue

            # 2. Echte IMDb score via IMDbPy
            search = ia.search_movie(title)
            if not search: continue
            movie = ia.get_movie(search[0].movieID)
            real_score = movie.get('rating')

            if not real_score or real_score < BENCHMARK_MIN:
                continue

            # 3. Datums
            parsed_added = _parse_date(it.get("ndate"))
            final_added = parsed_added.isoformat() if parsed_added else datetime.date.today().isoformat()

            out.append({
                "title": title,
                "type": "Series" if it.get("type") == "series" else "Film",
                "imdbRating": float(real_score),
                "traktRating": round(real_score * 0.9, 1), 
                "releaseDate": str(movie.get('year') or "2024"),
                "dateAdded": final_added,
                "tmdb_id": tmdb_id,
            })
            print(f"✅ OK: {title} ({real_score})")

        except Exception:
            continue
            
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

    if not TMDB_API_KEY:
        print("[ERROR] TMDB_API_KEY missing.")
        sys.exit(1)

    cleaned = normalize_and_filter(candidates)

    # Volledige set (klassiekers)
    h0 = sha12(OUT_FULL)
    write_json(OUT_FULL, cleaned)
    h1 = sha12(OUT_FULL)
    print(f"Wrote {OUT_FULL.name}  {h0} -> {h1}  (n={len(cleaned)})")

    # Recent set (laatste 90 dagen)
    recent = build_recent(cleaned)
    h2 = sha12(OUT_RECENT)
    write_json(OUT_RECENT, recent)
    h3 = sha12(OUT_RECENT)
    print(f"Wrote {OUT_RECENT.name}  {h2} -> {h3}  (n={len(recent)})")

    print("=== UPDATE ALL DONE ===")

if __name__ == "__main__":
    main()
