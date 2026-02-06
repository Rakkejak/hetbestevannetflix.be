#!/usr/bin/env python3
import os, json, datetime, time, re, html, random
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import requests

# ---------------- CONFIG ----------------
ROOT = Path(__file__).parent.resolve()
OUT_FULL   = ROOT / "netflix_data.json"
OUT_RECENT = ROOT / "netflix_last_month.json"

MANUAL_SCORES = ROOT / "manual_scores.json"
AVAIL_CACHE   = ROOT / "availability_cache.json"
IMDB_CACHE    = ROOT / "imdb_cache.json"

DAYS_RECENT = 90
BE_ID = 21

# Rating threshold (IMDb)
IMDB_MIN = 7.7

# Availability cache TTL:
# - True: refresh fast (so "recent" stays correct)
# - False: can be cached longer
TTL_TRUE_DAYS  = 1
TTL_FALSE_DAYS = 7

# IMDb cache TTL (OMDb)
IMDB_TTL_DAYS = 30

# Throttles (avoid 429)
SLEEP_BETWEEN_UNOGS_PAGES = 0.35
SLEEP_BETWEEN_COUNTRY_CALLS = 0.08  # tweak if you hit 429

UNOGS_API_KEY = os.environ.get("UNOGS_API_KEY", "").strip()
TMDB_API_KEY  = os.environ.get("TMDB_API_KEY", "").strip()
OMDB_API_KEY  = os.environ.get("OMDB_API_KEY", "").strip()

UNOGS_HOST = "unogsng.p.rapidapi.com"
UNOGS_SEARCH_URL = f"https://{UNOGS_HOST}/search"
UNOGS_TITLECOUNTRIES_URL = f"https://{UNOGS_HOST}/titlecountries"

SESSION = requests.Session()

# ---------------- PARSERS ----------------
_RATING_RE = re.compile(r"(\d+(?:\.\d+)?)")
def parse_rating(x: Any) -> Optional[float]:
    if x in (None, "", "no score", "N/A", "nan", "None", "0", 0):
        return None
    s = str(x).strip().replace(",", ".")
    m = _RATING_RE.search(s)
    if not m:
        return None
    try:
        v = float(m.group(1))
        return round(v, 1) if (0.0 < v <= 10.0) else None
    except:
        return None

def parse_int(x: Any) -> Optional[int]:
    if x in (None, "", "N/A", "nan", "None"):
        return None
    try:
        return int(float(str(x).replace(",", "").strip()))
    except:
        return None

def parse_date(x: Any) -> Optional[datetime.date]:
    if not x:
        return None
    try:
        s = str(x).strip()

        # Epoch (ms/sec)
        if s.isdigit():
            n = int(s)
            if n > 10_000_000_000:  # ms
                n //= 1000
            return datetime.datetime.fromtimestamp(n, tz=datetime.timezone.utc).date()

        # YYYYMMDD
        if len(s) == 8 and s.isdigit():
            return datetime.date(int(s[:4]), int(s[4:6]), int(s[6:8]))

        # ISO-ish
        return datetime.date.fromisoformat(s[:10].replace("/", "-"))
    except:
        return None

def norm_type(t: Any) -> str:
    t = str(t or "").strip().lower()
    if t in ("serie", "series", "tv", "show", "tvseries", "tv series"):
        return "series"
    if t in ("film", "movie", "movies"):
        return "movie"
    # uNoGS sometimes returns already "movie"/"series"
    if t in ("movie", "series"):
        return t
    return "unknown"

def norm_title(s: Any) -> str:
    s = html.unescape(str(s or ""))
    return " ".join(s.strip().lower().split()).replace("'", "").replace("’", "").replace('"', "")

# ---------------- HTTP HELPERS ----------------
def get_json(url: str, headers: dict = None, params: dict = None,
             timeout: int = 15, retries: int = 2) -> Optional[dict]:
    """
    Robust GET JSON: retries on 429 and 5xx with backoff + jitter.
    """
    for i in range(retries + 1):
        try:
            r = SESSION.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code == 429 and i < retries:
                ra = r.headers.get("Retry-After")
                wait = float(ra) if ra and str(ra).replace(".", "", 1).isdigit() else (2.0 * (i + 1))
                time.sleep(wait + random.random() * 0.3)
                continue
            if r.status_code in (500, 502, 503, 504) and i < retries:
                time.sleep(2.0 * (i + 1) + random.random() * 0.3)
                continue
            if r.status_code != 200:
                return None
            return r.json()
        except:
            if i < retries:
                time.sleep(1.5 * (i + 1))
                continue
            return None
    return None

# ---------------- CACHE ----------------
def load_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except:
        return {}

def save_cache(path: Path, data: dict, max_days: int = 180) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)

    pruned: Dict[str, Any] = {}
    for k, v in (data or {}).items():
        try:
            ts = datetime.datetime.fromisoformat(v.get("ts", ""))
            if (now - ts).days < max_days:
                pruned[k] = v
        except:
            # skip junk
            pass

    path.write_text(json.dumps(pruned, ensure_ascii=False, indent=2), encoding="utf-8")

# ---------------- MANUAL OVERRIDES ----------------
def load_manual_overrides() -> Tuple[Dict[int, dict], Dict[Tuple[str, str], dict]]:
    """
    Supports:
      - By nfid: {"nfid": 123, ...}
      - By (title,type): {"title":"Champion","type":"series",...}
    Fields you can use:
      exclude: true
      imdbRating: 8.2
      ignoreThresholds: true
      forceInclude: true
      type: "movie"/"series"
      title: "Display Title"
      releaseYear / releaseDate / dateAdded (optional)
    """
    by_nfid: Dict[int, dict] = {}
    by_key: Dict[Tuple[str, str], dict] = {}

    if not MANUAL_SCORES.exists():
        return by_nfid, by_key

    try:
        raw_txt = re.sub(r",\s*(\]|\})", r"\1", MANUAL_SCORES.read_text(encoding="utf-8"))
        raw = json.loads(raw_txt)
        if not isinstance(raw, list):
            return by_nfid, by_key

        for row in raw:
            if not isinstance(row, dict):
                continue

            if "type" in row:
                row["type"] = norm_type(row.get("type"))

            nfid_val = row.get("nfid")
            if isinstance(nfid_val, int) or (isinstance(nfid_val, str) and nfid_val.isdigit()):
                by_nfid[int(nfid_val)] = row
                continue

            t = row.get("title")
            ty = row.get("type")
            if t and ty in ("movie", "series"):
                by_key[(norm_title(t), ty)] = row

    except Exception as e:
        print(f"⚠️ manual_scores.json parse error: {e}")

    return by_nfid, by_key

# ---------------- uNoGS: CATALOG ----------------
def fetch_unogs_be_catalog() -> List[dict]:
    """
    Pull full BE catalog from uNoGS search (movies + series).
    IMPORTANT: we keep raw fields so we can parse dates/ids flexibly.
    """
    headers = {"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": UNOGS_HOST}
    items: List[dict] = []

    for t in ("movie", "series"):
        offset = 0
        while True:
            params = {
                "countrylist": str(BE_ID),
                "type": t,
                "offset": offset,
                "limit": 100,
                # Often helps to actually include date-ish fields / stable ordering:
                "orderby": "date",
            }
            j = get_json(UNOGS_SEARCH_URL, headers=headers, params=params, timeout=30, retries=3)
            if not j:
                break

            batch = j.get("results") or j.get("RESULTS") or []
            if not isinstance(batch, list) or not batch:
                break

            for x in batch:
                if not isinstance(x, dict):
                    continue
                items.append({
                    "nfid": x.get("nfid"),
                    "title": html.unescape(str(x.get("title") or x.get("name") or x.get("t") or "Onbekend")),
                    "vtype": norm_type(x.get("vtype") or t),
                    "raw_imdb": x.get("imdb_rating") or x.get("rating_imdb") or x.get("imdbrating"),
                    "imdb_id": x.get("imdbid") or x.get("imdb_id"),
                    "imdb_votes": parse_int(x.get("imdbvotes") or x.get("imdb_votes")),
                    "releaseYear": str(x.get("year") or x.get("releaseYear") or "")[:4],
                    "ndate": x.get("ndate") or x.get("new_date") or x.get("date") or x.get("added"),
                    "tmdb_id": x.get("tmdb_id") or x.get("tmid") or x.get("tmdbid"),
                })

            if len(batch) < 100:
                break
            offset += 100
            time.sleep(SLEEP_BETWEEN_UNOGS_PAGES)

    return items

# ---------------- uNoGS: AVAILABILITY + BE DATE ----------------
def verify_be_strict(nfid: int, avail_cache: dict, stats: dict) -> Tuple[Optional[bool], Optional[datetime.date]]:
    """
    Returns:
      (be_available, be_date_added_for_BE)
    be_date is taken from the BE record if present (new_date/ndate/...).
    """
    key = str(nfid)
    now = datetime.datetime.now(datetime.timezone.utc)

    # cache
    if key in avail_cache:
        try:
            ts = datetime.datetime.fromisoformat(avail_cache[key].get("ts", ""))
            cached_be = avail_cache[key].get("be")
            ttl = TTL_TRUE_DAYS if cached_be is True else TTL_FALSE_DAYS
            if (now - ts).days < ttl:
                bd = parse_date(avail_cache[key].get("be_date"))
                stats["avail_cache_hit"] += 1
                return cached_be, bd
        except:
            pass

    stats["avail_cache_miss"] += 1
    headers = {"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": UNOGS_HOST}
    res = get_json(UNOGS_TITLECOUNTRIES_URL, headers=headers, params={"netflixid": nfid}, timeout=12, retries=3)

    if res is None:
        # keep as unknown; don't poison cache
        stats["avail_unknown"] += 1
        return None, None

    countries = res.get("results") or res.get("RESULTS") or res.get("Countries") or []
    be_ok = False
    be_date: Optional[datetime.date] = None

    if isinstance(countries, list):
        for c in countries:
            if not isinstance(c, dict):
                continue
            cid = c.get("id") or c.get("countryid") or c.get("country_id")
            cc  = c.get("cc") or c.get("countrycode") or c.get("country_code")
            if (cid and str(cid) == str(BE_ID)) or (cc and str(cc).upper() == "BE"):
                be_ok = True
                be_date = (
                    parse_date(c.get("new_date")) or
                    parse_date(c.get("ndate")) or
                    parse_date(c.get("date")) or
                    parse_date(c.get("added")) or
                    None
                )
                break

    avail_cache[key] = {
        "be": be_ok,
        "be_date": be_date.isoformat() if be_date else None,
        "ts": now.isoformat(),
    }
    time.sleep(SLEEP_BETWEEN_COUNTRY_CALLS)
    return be_ok, be_date

# ---------------- IMDb via OMDb (gold path) ----------------
def get_imdb_from_omdb(imdb_id: str, imdb_cache: dict, stats: dict) -> Optional[float]:
    if not (OMDB_API_KEY and imdb_id):
        return None

    imdb_id = str(imdb_id).strip()
    if not imdb_id.startswith("tt"):
        # OMDb expects tt123...
        return None

    key = f"imdb:{imdb_id}"
    now = datetime.datetime.now(datetime.timezone.utc)

    if key in imdb_cache:
        try:
            ts = datetime.datetime.fromisoformat(imdb_cache[key].get("ts", ""))
            if (now - ts).days < IMDB_TTL_DAYS:
                stats["imdb_cache_hit"] += 1
                return imdb_cache[key].get("rating")
        except:
            pass

    stats["imdb_cache_miss"] += 1
    j = get_json("https://www.omdbapi.com/", params={"i": imdb_id, "apikey": OMDB_API_KEY}, timeout=12, retries=2)
    if not j or j.get("Response") == "False":
        imdb_cache[key] = {"rating": None, "ts": now.isoformat()}
        return None

    rating = parse_rating(j.get("imdbRating"))
    imdb_cache[key] = {"rating": rating, "ts": now.isoformat()}
    return rating

def get_imdb_via_tmdb_external_ids(tmdb_id: str, vtype: str, imdb_cache: dict, stats: dict) -> Optional[float]:
    if not (TMDB_API_KEY and tmdb_id):
        return None

    cat = "tv" if vtype == "series" else "movie"
    ext = get_json(f"https://api.themoviedb.org/3/{cat}/{tmdb_id}/external_ids",
                   params={"api_key": TMDB_API_KEY}, timeout=12, retries=2)
    imdb_id = (ext or {}).get("imdb_id")
    if not imdb_id:
        return None

    return get_imdb_from_omdb(imdb_id, imdb_cache, stats)

# ---------------- MAIN ----------------
def main() -> None:
    if not UNOGS_API_KEY:
        print("❌ UNOGS_API_KEY ontbreekt (GitHub Secret).")
        return
    if not OMDB_API_KEY:
        print("❌ OMDB_API_KEY ontbreekt (GitHub Secret).")
        return

    manual_nfid, manual_key = load_manual_overrides()
    avail_cache = load_cache(AVAIL_CACHE)
    imdb_cache  = load_cache(IMDB_CACHE)

    stats = {
        "bad_nfid": 0,
        "duplicate_nfid": 0,
        "excluded_manual": 0,
        "unknown_type": 0,
        "no_rating": 0,
        "below_threshold": 0,
        "unogs_placeholder_10": 0,
        "availability_false": 0,
        "availability_unknown_strict": 0,
        "kept": 0,

        "imdb_cache_hit": 0,
        "imdb_cache_miss": 0,
        "avail_cache_hit": 0,
        "avail_cache_miss": 0,
        "avail_unknown": 0,
    }

    candidates = fetch_unogs_be_catalog()
    print(f"📡 uNoGS candidates: {len(candidates)}")

    cutoff = datetime.datetime.now(datetime.timezone.utc).date() - datetime.timedelta(days=DAYS_RECENT)

    final_list: List[dict] = []
    seen_nfids: set = set()

    for it in candidates:
        # nfid
        nfid_raw = it.get("nfid")
        try:
            nfid = int(nfid_raw)
        except:
            stats["bad_nfid"] += 1
            continue

        if nfid in seen_nfids:
            stats["duplicate_nfid"] += 1
            continue
        seen_nfids.add(nfid)

        # manual override lookup
        ov = manual_nfid.get(nfid) or manual_key.get((norm_title(it.get("title")), it.get("vtype")))
        if ov and ov.get("exclude"):
            stats["excluded_manual"] += 1
            continue

        vtype = norm_type(ov.get("type")) if (ov and ov.get("type")) else it.get("vtype")
        if vtype == "unknown":
            stats["unknown_type"] += 1
            continue

        ignore_thresholds = bool(ov and (ov.get("ignoreThresholds") or ov.get("forceInclude")))

        # Date added (first pass)
        added_date = parse_date(it.get("ndate"))

        # Rating pipeline
        rating: Optional[float] = None
        source: Optional[str] = None

        # 1) manual rating
        if ov and ov.get("imdbRating") is not None:
            rating = parse_rating(ov.get("imdbRating"))
            source = "manual"

        # 2) OMDb via imdb_id (uNoGS)
        if rating is None and it.get("imdb_id"):
            r = get_imdb_from_omdb(str(it["imdb_id"]), imdb_cache, stats)
            if r is not None:
                rating, source = r, "imdb"

        # 3) OMDb via TMDb external_ids
        if rating is None and it.get("tmdb_id") and TMDB_API_KEY:
            r = get_imdb_via_tmdb_external_ids(str(it["tmdb_id"]), vtype, imdb_cache, stats)
            if r is not None:
                rating, source = r, "imdb"

        # 4) fallback: uNoGS rating (with sanity check for placeholder 10.0)
        unogs_rating = parse_rating(it.get("raw_imdb"))
        if rating is None and unogs_rating is not None:
            votes = it.get("imdb_votes") or 0
            if unogs_rating >= 9.9 and votes < 5000 and not it.get("imdb_id"):
                stats["unogs_placeholder_10"] += 1
            else:
                rating, source = unogs_rating, "unogs_imdb"

        if rating is None:
            stats["no_rating"] += 1
            continue

        # Thresholds (post-enrichment)
        if not ignore_thresholds and rating < IMDB_MIN:
            stats["below_threshold"] += 1
            continue

        # Availability + be_date (this is where we fix your "0 recent")
        be_ok, be_date = verify_be_strict(nfid, avail_cache, stats)

        # If we still don't have added_date, use BE date from titlecountries
        if added_date is None and be_date is not None:
            added_date = be_date

        is_recent = bool(added_date and added_date >= cutoff)

        # Strictness:
        # - Recent list MUST be confirmed available (be_ok == True)
        # - Full list: drop if be_ok == False; allow None (API hiccup) to keep (but not "recent")
        if is_recent:
            if be_ok is not True:
                stats["availability_unknown_strict"] += 1
                continue
        else:
            if be_ok is False:
                stats["availability_false"] += 1
                continue

        title = ov.get("title") if (ov and ov.get("title")) else it.get("title", "Onbekend")
        release_year = ov.get("releaseYear") or ov.get("releaseDate") or it.get("releaseYear")

        final_list.append({
            "nfid": nfid,
            "title": title,
            "type": "Series" if vtype == "series" else "Film",
            "imdbRating": rating,
            "ratingSource": source,
            "releaseDate": str(release_year)[:4] if release_year else "",
            "dateAdded": added_date.isoformat() if added_date else None,
            "availableBE": True if be_ok is True else None,  # None = unknown (only possible in full list)
        })
        stats["kept"] += 1

    # Write outputs
    OUT_FULL.write_text(json.dumps(final_list, ensure_ascii=False, indent=2), encoding="utf-8")

    recent_list = [x for x in final_list if x.get("dateAdded") and datetime.date.fromisoformat(x["dateAdded"]) >= cutoff]
    OUT_RECENT.write_text(json.dumps(recent_list, ensure_ascii=False, indent=2), encoding="utf-8")

    # Save caches
    save_cache(AVAIL_CACHE, avail_cache, max_days=180)
    save_cache(IMDB_CACHE, imdb_cache, max_days=365)

    # Debug summary
    with_date = sum(1 for x in final_list if x.get("dateAdded"))
    print(f"✅ GEREED: {len(final_list)} totaal, {len(recent_list)} recent.")
    print(f"ℹ️ Debug: {with_date}/{len(final_list)} items hebben dateAdded; cutoff={cutoff.isoformat()}.")
    print(f"📉 Dropped stats: { {k:v for k,v in stats.items() if k not in ('imdb_cache_hit','imdb_cache_miss','avail_cache_hit','avail_cache_miss','avail_unknown')} }")
    print(f"🧠 Cache: imdb_hit={stats['imdb_cache_hit']} imdb_miss={stats['imdb_cache_miss']} | avail_hit={stats['avail_cache_hit']} avail_miss={stats['avail_cache_miss']} avail_unknown={stats['avail_unknown']}")

if __name__ == "__main__":
    main()
