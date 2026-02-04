import os, json, datetime, time, re
from typing import Any, Dict, List, Optional
from pathlib import Path
import requests

# Config
ROOT = Path(__file__).parent.resolve()
OUT_FULL = ROOT / "netflix_data.json"
OUT_RECENT = ROOT / "netflix_last_month.json"
MANUAL_SCORES = ROOT / "manual_scores.json"
DAYS_RECENT = 90

TMDB_API_KEY  = os.environ.get("TMDB_API_KEY", "").strip()
UNOGS_API_KEY = os.environ.get("UNOGS_API_KEY", "").strip()
UNOGS_HOST = "unogsng.p.rapidapi.com"
UNOGS_URL  = f"https://{UNOGS_HOST}/search"

def _parse_rating(x: Any) -> Optional[float]:
    if x in (None, "", "no score"): return None
    s = str(x).strip().replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m: return None
    try:
        v = float(m.group(1))
        return None if v <= 0 else round(v, 1)
    except: return None

def _parse_date(x: Any) -> Optional[datetime.date]:
    if not x: return None
    try:
        if isinstance(x, (int, float)) or (isinstance(x, str) and str(x).isdigit()):
            n = int(x)
            if n > 10_000_000_000: n /= 1000
            return datetime.datetime.fromtimestamp(n, tz=datetime.timezone.utc).date()
        return datetime.date.fromisoformat(str(x)[:10])
    except: return None

def _norm_title(s: str) -> str:
    """Normaliseert titels voor betere matching: lower, strip, single spaces."""
    return " ".join((s or "").strip().lower().split()).replace("'", "").replace("’", "")

def _norm_type(t: str) -> str:
    t = (t or "").strip().lower()
    if t in ("serie", "series", "tv", "show"): return "series"
    if t in ("film", "movie"): return "movie"
    return t or "unknown"

def _pick(d: Dict[str, Any], *keys: str, default=None, skip_ids: bool = False):
    for k in keys:
        if k not in d: continue
        v = d[k]
        if v in (None, "", 0, "0", "None"): continue
        if isinstance(v, str):
            vv = v.strip()
            if not vv: continue
            if skip_ids and vv.isdigit() and len(vv) > 4: continue 
        return v
    return default

def load_manual_scores() -> Dict[tuple, Dict[str, Any]]:
    if not MANUAL_SCORES.exists(): return {}
    try:
        raw = json.loads(MANUAL_SCORES.read_text(encoding="utf-8"))
        out: Dict[tuple, Dict[str, Any]] = {}
        for row in (raw if isinstance(raw, list) else []):
            title = str(row.get("title", "")).strip()
            if not title: continue
            ntype = _norm_type(row.get("type", ""))
            rating = _parse_rating(row.get("imdbRating"))
            rd = row.get("releaseDate")
            year = str(rd)[:4] if rd else ""
            # Gebruik genormaliseerde titel als key
            out[(_norm_title(title), ntype)] = {
                "title": title,
                "type": ntype,
                "imdbRating": rating,
                "releaseYear": year
            }
        return out
    except Exception as e:
        print(f"⚠️ Fout bij laden manual_scores.json: {e}")
        return {}

# ---------- FETCH ----------
def fetch_candidates() -> List[Dict[str, Any]]:
    if not UNOGS_API_KEY: return []
    items: List[Dict[str, Any]] = []
    headers = {"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": UNOGS_HOST}

    for t in ("movie", "series"):
        offset = 0
        while True:
            params = {"type": t, "countrylist": "21", "offset": offset, "limit": 100}
            try:
                resp = requests.get(UNOGS_URL, headers=headers, params=params, timeout=30)
                if resp.status_code != 200:
                    print(f"⚠️ uNoGS HTTP {resp.status_code}: {resp.text[:200]}")
                    break
                
                data = resp.json()
                batch = data.get("results") or data.get("RESULTS") or []
                if not batch: break

                for x in batch:
                    items.append({
                        "title": _pick(x, "title", "t", "name", default="Onbekend", skip_ids=True),
                        "type": t,
                        "raw_rating": _pick(x, "imdb_rating", "imdbrating", "imdbRating", "rating", "imdb", "rating_imdb"),
                        "releaseYear": str(_pick(x, "year", "v", "releaseYear", default="")),
                        "ndate": x.get("ndate") or x.get("new_date") or "",
                        "tmdb_id": _pick(x, "tmdb_id", "tmid", "tmdbid"),
                    })
                offset += 100
                if len(batch) < 100: break
                time.sleep(0.5)
            except Exception as e:
                print(f"💥 uNoGS netwerkfout ({t}, offset {offset}): {e}")
                break
    return items

# ---------- PROCESS ----------
def normalize_and_filter(items: List[Dict[str, Any]], manual_map: Dict[tuple, Dict[str, Any]]) -> List[Dict[str, Any]]:
    IMDB_MIN, TMDB_MIN = 7.7, 7.0
    out: List[Dict[str, Any]] = []
    seen = set()

    for it in items:
        if it["title"] == "Onbekend": continue

        norm_type = _norm_type(it.get("type"))
        norm_t = _norm_title(it["title"])
        manual = manual_map.get((norm_t, norm_type))
        
        rating = _parse_rating(it.get("raw_rating"))
        year = it.get("releaseYear") or ""

        manual_force_include = (manual is not None)
        if manual_force_include:
            if manual.get("imdbRating") is not None: rating = manual["imdbRating"]
            if manual.get("releaseYear"): year = manual["releaseYear"]

        should_check_tmdb = bool(it.get("tmdb_id")) and (rating is None or rating >= 9.0)
        if should_check_tmdb and TMDB_API_KEY and not manual_force_include:
            try:
                m_api_type = "tv" if norm_type == "series" else "movie"
                r = requests.get(f"https://api.themoviedb.org/3/{m_api_type}/{it['tmdb_id']}", 
                                 params={"api_key": TMDB_API_KEY}, timeout=10)
                if r.status_code == 200:
                    tr = r.json()
                    tmdb_score = _parse_rating(tr.get("vote_average"))
                    if tmdb_score is not None: rating = tmdb_score
            except: pass

        # Filter: manual items altijd doorlaten
        if not manual_force_include:
            if rating is None: continue
            min_needed = TMDB_MIN if should_check_tmdb else IMDB_MIN
            if rating < min_needed: continue

        key = (norm_t, norm_type, year)
        if key in seen: continue
        seen.add(key)

        parsed_added = _parse_date(it.get("ndate"))
        out.append({
            "title": str(it["title"]).replace("&#39;", "'").replace("&amp;", "&"),
            "type": "Series" if norm_type == "series" else "Film",
            "imdbRating": rating,
            "traktRating": round(rating * 0.9, 1) if rating else None,
            "releaseDate": year,
            "dateAdded": parsed_added.isoformat() if parsed_added else None,
            "tmdb_id": it.get("tmdb_id"),
            "manual": manual_force_include
        })

    # Voeg manual-only items toe
    for (norm_t, ntype), m in manual_map.items():
        year = m.get("releaseYear", "")
        key = (norm_t, ntype, year)
        if key in seen: continue
        
        out.append({
            "title": m["title"],
            "type": "Series" if ntype == "series" else "Film",
            "imdbRating": m["imdbRating"],
            "traktRating": round(m["imdbRating"] * 0.9, 1) if m["imdbRating"] else None,
            "releaseDate": year,
            "dateAdded": None,
            "tmdb_id": None,
            "manual": True
        })
        seen.add(key)
    return out

# ---------- MAIN ----------
def main():
    print("=== START FINAL ROBUST UPDATE ===")
    manual_map = load_manual_scores()
    candidates = fetch_candidates()
    
    if not candidates and not manual_map:
        print("Geen data gevonden.")
        return

    cleaned = normalize_and_filter(candidates, manual_map)

    OUT_FULL.write_text(json.dumps(cleaned, ensure_ascii=False, indent=2), encoding="utf-8")
    
    today_utc = datetime.datetime.now(datetime.timezone.utc).date()
    cutoff = today_utc - datetime.timedelta(days=DAYS_RECENT)
    recent = [it for it in cleaned if it["dateAdded"] and datetime.date.fromisoformat(it["dateAdded"]) >= cutoff]
    
    OUT_RECENT.write_text(json.dumps(recent, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ GEREED: {len(cleaned)} totaal, {len(recent)} recent.")

if __name__ == "__main__":
    main()
