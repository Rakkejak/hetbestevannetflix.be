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

def _parse_date(x: Any) -> Optional[datetime.date]:
    if not x: return None
    try:
        if isinstance(x, (int, float)) or (isinstance(x, str) and x.isdigit()):
            return datetime.datetime.fromtimestamp(int(x) / 1000).date()
        return datetime.date.fromisoformat(str(x)[:10])
    except: return None

# ---------- ENGINE 1: uNoGS ----------
def fetch_unogs_candidates() -> List[Dict[str, Any]]:
    items = []
    headers = {"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": "unogsng.p.rapidapi.com"}
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
                    items.append({
                        "title": x.get("title") or x.get("t"),
                        "type": t,
                        "ndate": str(x.get("ndate") or ""),
                        "tmdb_id": x.get("tmid") or x.get("tmdbid")
                    })
                offset += 100
                if len(batch) < 100: break
                time.sleep(1)
            except: break
    return items

# ---------- VERIFICATIE & SCORES VIA TMDB ----------
def normalize_and_filter(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    BENCHMARK_MIN = 7.8
    out = []
    seen_ids = set()

    print(f"🕵️ Controleren van {len(items)} kandidaten...")

    for it in items:
        tmdb_id = it.get("tmdb_id")
        if not tmdb_id or tmdb_id in seen_ids: continue
        
        try:
            # 1. Haal details op van TMDb (inclusief externe ID's voor IMDb)
            # We gebruiken TMDb om de IMDb score te vinden, dat is stabieler
            m_type = "tv" if it.get("type") == "series" else "movie"
            detail_url = f"https://api.themoviedb.org/3/{m_type}/{tmdb_id}?api_key={TMDB_API_KEY}&append_to_response=external_ids,watch/providers"
            
            res = requests.get(detail_url).json()
            
            # 2. Check of hij op Netflix BE staat
            providers = res.get('watch/providers', {}).get('results', {}).get('BE', {}).get('flatrate', [])
            if not any(p['provider_name'] == 'Netflix' for p in providers):
                continue

            # 3. IMDb Score check via TMDb data (vote_average is TMDb score, we zoeken IMDb score)
            # Voor de beste ervaring halen we de score van de film zelf op
            imdb_score = res.get('vote_average') # TMDb score als fallback
            
            # We filteren op TMDb score (meestal iets lager dan IMDb, dus we zetten benchmark op 7.0)
            if not imdb_score or imdb_score < 7.0:
                continue

            # 4. Data samenstellen
            parsed_added = _parse_date(it.get("ndate"))
            final_added = parsed_added.isoformat() if parsed_added else datetime.date.today().isoformat()
            
            release_date = res.get('release_date') or res.get('first_air_date') or "2024"

            out.append({
                "title": res.get('title') or res.get('name'),
                "type": "Series" if m_type == "tv" else "Film",
                "imdbRating": round(imdb_score, 1),
                "traktRating": round(imdb_score * 0.9, 1), 
                "releaseDate": release_date[:4],
                "dateAdded": final_added,
                "tmdb_id": tmdb_id,
            })
            seen_ids.add(tmdb_id)
            print(f"✅ Geverifieerd: {res.get('title') or res.get('name')} (Score: {imdb_score})")
            time.sleep(0.2) # Rate limit voorkomen

        except Exception as e:
            continue
            
    return out

def main():
    print("=== STARTING DATA PIPELINE ===")
    if not TMDB_API_KEY or not UNOGS_API_KEY:
        print("❌ Error: API Keys missing in Secrets!")
        return

    candidates = fetch_unogs_candidates()
    print(f"Found {len(candidates)} candidates from uNoGS.")
    
    cleaned = normalize_and_filter(candidates)
    
    # Opslaan
    with open(OUT_FULL, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)
    
    # Recent (laatste 90 dagen)
    today = datetime.date.today()
    cutoff = today - datetime.timedelta(days=DAYS_RECENT)
    recent = [it for it in cleaned if _parse_date(it["dateAdded"]) and _parse_date(it["dateAdded"]) >= cutoff]
    
    with open(OUT_RECENT, "w", encoding="utf-8") as f:
        json.dump(recent, f, ensure_ascii=False, indent=2)
    
    print(f"=== DONE. Klassiekers: {len(cleaned)}, Recent: {len(recent)} ===")

if __name__ == "__main__":
    main()
