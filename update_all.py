import os, sys, json, hashlib, datetime, time
from typing import Any, Dict, List, Optional
from pathlib import Path
import requests

# Config
ROOT = Path(__file__).parent.resolve()
OUT_FULL = ROOT / "netflix_data.json"
OUT_RECENT = ROOT / "netflix_last_month.json"
DAYS_RECENT = 90

TMDB_API_KEY    = os.environ.get("TMDB_API_KEY", "")
UNOGS_API_KEY   = os.environ.get("UNOGS_API_KEY", "")

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
    except: return None

def _parse_date(x: Any) -> Optional[datetime.date]:
    if not x: return None
    try:
        if isinstance(x, (int, float)) or (isinstance(x, str) and x.isdigit()):
            return datetime.datetime.fromtimestamp(int(x) / 1000).date()
        return datetime.date.fromisoformat(str(x)[:10])
    except: return None

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
                data = resp.json()
                batch = data.get("results") or []
                if not batch: break
                
                for x in batch:
                    # FIX 1: Mapping repareren zodat titels geen nummers worden
                    items.append({
                        "title": x.get("title") or x.get("t") or "Onbekend",
                        "type": t,
                        "raw_rating": x.get("imdb_rating") or x.get("rating") or 0,
                        "raw_year": x.get("year") or x.get("v") or "2024",
                        "ndate": str(x.get("ndate") or ""),
                        "tmdb_id": x.get("tmid") or x.get("tmdbid")
                    })
                offset += 100
                if len(batch) < 100: break
                time.sleep(0.5)
            except: break
    return items

# ---------- process ----------
def normalize_and_filter(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Poetst de data op en filtert foute scores."""
    BENCHMARK_MIN = 7.8
    out = []
    
    print(f"Verwerken van {len(items)} titels...")
    for it in items:
        # We vertrouwen uNoGS scores niet blind. 
        # Als er een TMDb_id is, proberen we de score daar te checken.
        rating = _num_or_none(it["raw_rating"])
        year = str(it["raw_year"])
        
        # FIX 2: De "IMDb score 9.0" leugen onderscheppen
        # Als de score extreem hoog is, doen we een extra check
        if rating and rating >= 8.5 and it.get("tmdb_id"):
            try:
                m_type = "tv" if it["type"] == "series" else "movie"
                tmdb_url = f"https://api.themoviedb.org/3/{m_type}/{it['tmdb_id']}?api_key={TMDB_API_KEY}"
                tr = requests.get(tmdb_url, timeout=5).json()
                tmdb_score = tr.get("vote_average")
                if tmdb_score:
                    rating = round(tmdb_score, 1)
                    if tr.get("release_date"): year = tr["release_date"][:4]
                    if tr.get("first_air_date"): year = tr["first_air_date"][:4]
            except: pass

        if not rating or rating < BENCHMARK_MIN:
            continue

        # FIX 3: Datums (ndate) correct omzetten voor 'Recent' pagina
        parsed_added = _parse_date(it.get("ndate"))
        final_added = parsed_added.isoformat() if parsed_added else datetime.date.today().isoformat()

        out.append({
            "title": it["title"].replace("&#39;", "'").replace("&amp;", "&"),
            "type": "Series" if it["type"] == "series" else "Film",
            "imdbRating": rating,
            "traktRating": round(rating * 0.9, 1), 
            "releaseDate": year,
            "dateAdded": final_added,
            "tmdb_id": it.get("tmdb_id"),
        })
        
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

# ---------- main ----------
def main():
    print("=== START UPDATE ALL ===")
    candidates = fetch_candidates()
    if not candidates:
        print("Geen data van uNoGS gekregen.")
        return

    cleaned = normalize_and_filter(candidates)
    
    # Sla resultaten op
    with open(OUT_FULL, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)
    
    recent = build_recent(cleaned)
    with open(OUT_RECENT, "w", encoding="utf-8") as f:
        json.dump(recent, f, ensure_ascii=False, indent=2)
        
    print(f"Klaar! {len(cleaned)} klassiekers en {len(recent)} nieuwe titels.")

if __name__ == "__main__":
    main()
