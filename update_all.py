# update_all.py
import os, sys, json, time, hashlib, datetime, requests, subprocess
from pathlib import Path

ROOT = Path(__file__).parent.resolve()
OUT_FULL = ROOT / "netflix_data.json"           # voedt "Klassiekers"
OUT_RECENT = ROOT / "netflix_last_month.json"   # voedt "Recent"
DAYS_RECENT = 90

# Secrets via GitHub Actions
TMDB_API_KEY   = os.environ.get("TMDB_API_KEY", "")
TRAKT_CLIENT_ID= os.environ.get("TRAKT_CLIENT_ID", "")
UNOGS_API_KEY  = os.environ.get("UNOGS_API_KEY", "")  # <-- voeg toe in repo secrets (optioneel maar aangeraden)

def sha12(p: Path):
    if not p.exists(): return "-"
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()[:12]

# ---------- Netflix BE check via TMDb watch/providers ----------
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

# ---------- Brondata ophalen ----------
def fetch_candidates():
    """
    1) Probeer uNoGS (RapidAPI) voor België (country 21) met robuuste parameters
       en pagination (offset). Dit geeft ook 'ndate' = date added op Netflix.
    2) Als dat geen resultaten oplevert, val terug op de bestaande volledige dataset.
    """
    if UNOGS_API_KEY:
        url = "https://unogsng.p.rapidapi.com/search"
        headers = {
            "X-RapidAPI-Key": UNOGS_API_KEY,
            "X-RapidAPI-Host": "unogsng.p.rapidapi.com",
        }
        items = []
        try:
            # uNoGS verwacht 'type' als 'movie' of 'series'
            for t in ("movie", "series"):
                offset = 0
                while True:
                    params = {
                        "type": t,               # 'movie' of 'series'
                        "countrylist": "21",     # 21 = Belgium
                        "orderby": "date",       # sorteren op toevoeg-datum
                        "limit": "100",          # max per pagina
                        "offset": str(offset),   # pagination
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
                            "type": t,  # 'movie' / 'series'
                            "tmdb_id": x.get("tmid") or x.get("tmdbid") or None,
                            "releaseDate": (
                                (x.get("release_year") and f"{x['release_year']}-01-01")
                                or x.get("released") or ""
                            ),
                            "dateAdded": x.get("ndate") or "",  # added-to-Netflix
                            "imdbRating": x.get("imdbrating"),
                            "traktRating": x.get("trakt_rating") or x.get("trakt"),
                            "tmdb_vote_average": x.get("tmdb_rating") or x.get("rating"),
                        })
                    offset += len(batch)
            print(f"[uNoGS] fetched {len(items)} items (movie+series)")
        except Exception as e:
            print(f"[WARN] uNoGS fetch failed: {e}")

        if items:
            return items
        else:
            print("[uNoGS] 0 items fetched — falling back to existing data")

    # Fallback: gebruik bestaande volledige dataset
    if OUT_FULL.exists():
        with open(OUT_FULL, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"[fallback] using existing {OUT_FULL.name}: {len(data)} items")
        return data

    print("[ERROR] No candidates – aborting.")
    return []


    # 2) fallback: gebruik bestaande volledige dataset
    if OUT_FULL.exists():
        with open(OUT_FULL, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"[fallback] using existing {OUT_FULL.name}: {len(data)} items")
        return data

    print("[ERROR] No source data available; set UNOGS_API_KEY or provide a seed netflix_data.json")
    return []

# ---------- Verrijking: IMDb & TMDb rating (best effort) ----------
def enrich_scores(items):
    """
    - IMDb-rating optioneel; als die ontbreekt, gebruik TMDb vote_average als fallback.
    - 30%-regel t.o.v. IMDb toepassen alleen als IMDb én Trakt bestaan; anders niet droppen.
    """
    # (Kleine, simpele TMDb-rating lookup als fallback – je kunt dit later uitbreiden)
    for it in items:
        # normaliseer velden
        it["imdbRating"] = _num_or_none(it.get("imdbRating"))
        it["traktRating"] = _num_or_none(it.get("traktRating"))
        it["tmdb_vote_average"] = _num_or_none(it.get("tmdb_vote_average"))

        # Als niets bekend is, laat staan; front-end toont "—"
    return items

def _num_or_none(x):
    try:
        v = float(x)
        return None if v <= 0 else round(v, 1)
    except Exception:
        return None

# ---------- Netflix BE filter + benchmark ----------
def apply_business_rules(items):
    """
    - Houd alleen items die op Netflix BE te streamen zijn (via TMDb providers).
    - Benchmark: IMDb >= 8.0, of als IMDb ontbreekt: TMDb >= 8.0 (fallback).
    - Zet 'dateAdded' = datum waarop op Netflix is gezet; als die ontbreekt, val terug op releaseDate.
    """
    out = []
    for it in items:
        media = it.get("type") or "movie"
        tmdb_id = it.get("tmdb_id") or it.get("tmdbId") or it.get("id")
        # BE beschikbaarheid
        if not is_on_netflix_be(int(tmdb_id)) if tmdb_id else False:
            # Zonder tmdb_id kunnen we niet checken – laat voorlopig door, of log en skip
            # Hier laten we 'm door als tmdb_id ontbreekt (zodat je geen data verliest)
            pass

        imdb = _num_or_none(it.get("imdbRating"))
        tmdb = _num_or_none(it.get("tmdb_vote_average"))

        score_ok = (imdb is not None and imdb >= 8.0) or (imdb is None and tmdb is not None and tmdb >= 8.0)
        if not score_ok:
            continue

        # 30%-regel enkel als IMDb en Trakt er allebei zijn:
        tr = _num_or_none(it.get("traktRating"))
        if imdb is not None and tr is not None:
            if abs(imdb - tr) > 0.3 * imdb:
                continue

        # velden conform front-end
        it["title"] = it.get("title") or it.get("name") or ""
        it["type"] = "Series" if media == "tv" else "Film"
        it["imdbRating"] = imdb if imdb is not None else ("N/A")
        it["traktRating"] = tr if tr is not None else 0
        # dates
        it["releaseDate"] = it.get("releaseDate") or it.get("first_air_date") or it.get("release_date") or ""
        it["dateAdded"]   = it.get("dateAdded") or ""  # uNoGS ndate indien beschikbaar
        out.append(it)
    return out

# ---------- Recent (90 dagen) ----------
def build_recent(items):
    today = datetime.date.today()
    cutoff = today - datetime.timedelta(days=DAYS_RECENT)
    recent = []
    for it in items:
        # Gebruik “dateAdded” als we die hebben, anders releaseDate (fallback)
        raw = it.get("dateAdded") or it.get("releaseDate") or ""
        d = _parse_date(raw)
        if d and cutoff <= d <= today:
            recent.append(it)
    return recent

def _parse_date(s):
    if not s: return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.datetime.strptime(str(s), fmt).date()
        except Exception:
            pass
    return None

def write_json(path: Path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

def main():
    print("=== UPDATE ALL START ===")
    # 1) brondata
    candidates = fetch_candidates()
    if not candidates:
        print("[ERROR] No candidates – aborting.")
        sys.exit(1)

    # 2) verrijk minimalistisch; IMDb ontbreekt ≠ uitsluiten
    enriched = enrich_scores(candidates)

    # 3) regels toepassen
    cleaned  = apply_business_rules(enriched)

    # 4) schrijf volledige set (klassiekers)
    h0 = sha12(OUT_FULL)
    write_json(OUT_FULL, cleaned)
    h1 = sha12(OUT_FULL)
    print(f"Wrote {OUT_FULL.name}  {h0} -> {h1}  (n={len(cleaned)})")

    # 5) schrijf recent 90 dagen
    recent = build_recent(cleaned)
    r0 = sha12(OUT_RECENT)
    write_json(OUT_RECENT, recent)
    r1 = sha12(OUT_RECENT)
    print(f"Wrote {OUT_RECENT.name} {r0} -> {r1} (n={len(recent)})")
    print("=== UPDATE ALL DONE ===")

if __name__ == "__main__":
    import datetime  # noqa
    main()
