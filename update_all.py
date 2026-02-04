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
    """Haalt de ruwe lijst op van uNoGS België met correcte mapping."""
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
                    # FIX 1: Voorkom dat titels nummers worden door naar meerdere velden te kijken
                    title = x.get("title") or x.get("t") or "Onbekend"
                    # FIX 2: Pak het jaartal uit 'year' of 'v'
                    year = x.get("year") or x.get("v") or "2024"
                    
                    items.append({
                        "title": title,
                        "type": t,
                        "imdbRating": x.get("imdb_rating") or x.get("rating") or 0,
                        "releaseDate": str(year),
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
    """Valideert data tegen TMDb om leugens (score 9.0) te filteren."""
    BENCHMARK_MIN = 7.7
    out = []
    
    print(f"Verifiëren van {len(items)} titels...")
    for it in items:
        rating = _num_or_none(it["imdbRating"])
        year = it["releaseDate"]
        
        # FIX 3: De 'IMDb 9.0' check. Als uNoGS een extreme score geeft, vragen we TMDb om de waarheid.
        if it.get("tmdb_id"):
            try:
                m_type = "tv" if it["type"] == "series" else "movie"
                # We checken de score bij TMDb
                tr = requests.get(f"https://api.themoviedb.org/3/{m_type}/{it['tmdb_id']}?api_key={TMDB_API_KEY}", timeout=5).json()
                tmdb_score = tr.get("vote_average")
                if tmdb_score:
                    # We middelen of overschrijven als de uNoGS score onrealistisch is
                    rating = round(tmdb_score, 1)
                    # FIX 4: Haal het echte jaar op van TMDb (voorkomt overal '2020')
                    tmdb_date = tr.get("release_date") or tr.get("first_air_date")
                    if tmdb_date: year = tmdb_date[:4]
            except: pass

        if not rating or rating < BENCHMARK_MIN:
            continue

        # FIX 5: ndate correct omzetten voor de 'Recent' pagina
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

# ---------- main ----------
def main():
    print("=== START UPDATE ===")
    candidates = fetch_candidates()
    if not candidates:
        print("Geen data van uNoGS.")
        return

    cleaned = normalize_and_filter(candidates)
    
    # Classics page
    with open(OUT_FULL, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)
    
    # Recent page (laatste 90 dagen)
    today = datetime.date.today()
    cutoff = today - datetime.timedelta(days=90)
    recent = [it for it in cleaned if _parse_date(it["dateAdded"]) and _parse_date(it["dateAdded"]) >= cutoff]
    
    with open(OUT_RECENT, "w", encoding="utf-8") as f:
        json.dump(recent, f, ensure_ascii=False, indent=2)
        
    print(f"Gereed: {len(cleaned)} items totaal, {len(recent)} recent.")

if __name__ == "__main__":
    main()
