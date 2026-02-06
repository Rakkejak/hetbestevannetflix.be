import os, json, datetime, time, re, html
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import requests

# --- CONFIG ---
ROOT = Path(__file__).parent.resolve()
OUT_FULL = ROOT / "netflix_data.json"
OUT_RECENT = ROOT / "netflix_last_month.json"

MANUAL_SCORES = ROOT / "manual_scores.json"
AVAIL_CACHE = ROOT / "availability_cache.json"
IMDB_CACHE = ROOT / "imdb_cache.json"

DAYS_RECENT = int(os.environ.get("DAYS_RECENT", "90"))
BE_ID = int(os.environ.get("NETFLIX_COUNTRY_ID", "21"))

IMDB_MIN = float(os.environ.get("IMDB_MIN", "7.7"))
TMDB_MIN = float(os.environ.get("TMDB_MIN", "7.0"))

# Availability cache TTL: True quickly revalidated; False longer
TTL_TRUE_DAYS = int(os.environ.get("TTL_TRUE_DAYS", "1"))
TTL_FALSE_DAYS = int(os.environ.get("TTL_FALSE_DAYS", "7"))

# IMDb cache TTL
IMDB_TTL_DAYS = int(os.environ.get("IMDB_TTL_DAYS", "30"))

# Rate limiting / politeness
SLEEP_FETCH_PAGE = float(os.environ.get("SLEEP_FETCH_PAGE", "0.3"))
SLEEP_TITLECOUNTRIES = float(os.environ.get("SLEEP_TITLECOUNTRIES", "0.05"))

TMDB_API_KEY  = os.environ.get("TMDB_API_KEY", "").strip()
UNOGS_API_KEY = os.environ.get("UNOGS_API_KEY", "").strip()
OMDB_API_KEY  = os.environ.get("OMDB_API_KEY", "").strip()

UNOGS_HOST = "unogsng.p.rapidapi.com"
UNOGS_URL  = "https://%s/search" % UNOGS_HOST
UNOGS_TITLECOUNTRIES_URL = "https://%s/titlecountries" % UNOGS_HOST

SESSION = requests.Session()

# ---------- HTTP ----------
def _get_json(url: str, headers: Optional[dict] = None, params: Optional[dict] = None,
              retries: int = 2, timeout: int = 15) -> Optional[dict]:
    """GET JSON with basic retry/backoff for 429 and 5xx."""
    for i in range(retries + 1):
        try:
            r = SESSION.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504) and i < retries:
                time.sleep(2 * (i + 1))
                continue
            if r.status_code != 200:
                return None
            return r.json()
        except Exception:
            if i < retries:
                time.sleep(1.5)
            continue
    return None

# ---------- PARSING ----------
_RATING_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(?:/|$)")

def _parse_rating(x: Any) -> Optional[float]:
    if x in (None, "", "no score", "N/A"):
        return None
    s = str(x).strip().replace(",", ".")
    m = _RATING_RE.search(s)
    if not m:
        return None
    try:
        v = float(m.group(1))
        return round(v, 1) if (0 < v <= 10.0) else None
    except Exception:
        return None

def _parse_int(x: Any) -> Optional[int]:
    if x in (None, "", "N/A"):
        return None
    try:
        return int(float(str(x).replace(",", "").strip()))
    except Exception:
        return None

def _parse_date(x: Any) -> Optional[datetime.date]:
    if not x:
        return None
    try:
        # unix seconds or millis
        if isinstance(x, (int, float)) or (isinstance(x, str) and str(x).isdigit()):
            n = int(x)
            if n > 10_000_000_000:
                n //= 1000
            return datetime.datetime.fromtimestamp(n, tz=datetime.timezone.utc).date()
        return datetime.date.fromisoformat(str(x)[:10])
    except Exception:
        return None

def _norm_type(t: Any) -> str:
    t = str(t or "").strip().lower()
    if t in ("serie", "series", "tv", "show", "tvseries", "tv series", "tvshow", "tv show"):
        return "series"
    if t in ("film", "movie", "movies"):
        return "movie"
    return "unknown"

def _norm_title(s: Any) -> str:
    s = html.unescape(str(s or ""))
    return " ".join(s.strip().lower().split()).replace("'", "").replace("’", "").replace('"', "")

def _pick_title(d: Dict[str, Any], *keys: str, default: str = "Onbekend") -> str:
    for k in keys:
        v = d.get(k)
        if v in (None, "", 0, "0", "None"):
            continue
        s = str(v).strip()
        # skip numeric IDs masquerading as title
        if s.isdigit() and len(s) > 4:
            continue
        return html.unescape(s)
    return default

# ---------- CACHE ----------
def _load_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_cache(path: Path, data: dict, max_days: int = 180) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    pruned = {}
    for k, v in (data or {}).items():
        try:
            ts_s = (v or {}).get("ts")
            if not ts_s:
                continue
            ts = datetime.datetime.fromisoformat(ts_s)
            if (now - ts).days <= max_days:
                pruned[k] = v
        except Exception:
            continue
    path.write_text(json.dumps(pruned, ensure_ascii=False, indent=2), encoding="utf-8")

# ---------- MANUAL OVERRIDES ----------
def load_manual_overrides(path: Path) -> Tuple[Dict[int, dict], Dict[Tuple[str, str], dict]]:
    by_nfid = {}  # type: Dict[int, dict]
    by_key = {}   # type: Dict[Tuple[str, str], dict]
    if not path.exists():
        return by_nfid, by_key
    try:
        raw = json.loads(re.sub(r",\s*(\]|\})", r"\1", path.read_text(encoding="utf-8")))
        if not isinstance(raw, list):
            return by_nfid, by_key
        for row in raw:
            if not isinstance(row, dict):
                continue
            if "type" in row:
                row["type"] = _norm_type(row.get("type"))
            if "nfid" in row and str(row["nfid"]).isdigit():
                by_nfid[int(row["nfid"])] = row
            elif row.get("title") and row.get("type") in ("movie", "series"):
                by_key[(_norm_title(row["title"]), row["type"])] = row
    except Exception as e:
        print("⚠️ manual_scores.json error:", e)
    return by_nfid, by_key

# ---------- uNoGS: availability ----------
def verify_belgium_availability(nfid: int, cache: dict) -> Optional[bool]:
    key = str(nfid)
    now = datetime.datetime.now(datetime.timezone.utc)

    if key in cache:
        try:
            ts = datetime.datetime.fromisoformat(cache[key]["ts"])
            cached_be = cache[key].get("be")
            ttl = TTL_TRUE_DAYS if cached_be is True else TTL_FALSE_DAYS
            if (now - ts).days < ttl:
                return cached_be
        except Exception:
            pass

    res = _get_json(
        UNOGS_TITLECOUNTRIES_URL,
        headers={"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": UNOGS_HOST},
        params={"netflixid": nfid},
        timeout=15
    )
    if res is None:
        return None

    results = res.get("results") or res.get("RESULTS") or res.get("Countries") or []
    in_be = False
    if isinstance(results, list):
        for c in results:
            if not isinstance(c, dict):
                continue
            cid = c.get("id") or c.get("countryid") or c.get("country_id")
            cc = c.get("cc") or c.get("countrycode") or c.get("country_code")
            if (cid is not None and str(cid) == str(BE_ID)) or (cc and str(cc).upper() == "BE"):
                in_be = True
                break

    cache[key] = {"be": in_be, "ts": now.isoformat()}
    if SLEEP_TITLECOUNTRIES:
        time.sleep(SLEEP_TITLECOUNTRIES)
    return in_be

# ---------- IMDb rating via OMDb ----------
def get_imdb_from_omdb(imdb_id: str, cache: dict) -> Tuple[Optional[float], Optional[int]]:
    """Return (rating, votes) from OMDb for an IMDb tt-id."""
    if not (OMDB_API_KEY and imdb_id):
        return None, None

    imdb_id = str(imdb_id).strip()
    if not imdb_id.startswith("tt"):
        return None, None

    key = "imdb:%s" % imdb_id
    now = datetime.datetime.now(datetime.timezone.utc)

    if key in cache:
        try:
            ts = datetime.datetime.fromisoformat(cache[key]["ts"])
            if (now - ts).days < IMDB_TTL_DAYS:
                return cache[key].get("imdb"), cache[key].get("votes")
        except Exception:
            pass

    om = _get_json("https://www.omdbapi.com/", params={"i": imdb_id, "apikey": OMDB_API_KEY}, timeout=15)
    if not om:
        cache[key] = {"imdb": None, "votes": None, "ts": now.isoformat()}
        return None, None

    rating = _parse_rating(om.get("imdbRating"))
    votes = _parse_int(om.get("imdbVotes"))
    cache[key] = {"imdb": rating, "votes": votes, "ts": now.isoformat()}
    return rating, votes

def get_imdb_via_tmdb(tmdb_id: str, vtype: str, cache: dict) -> Tuple[Optional[float], Optional[int], Optional[str]]:
    """Return (imdb_rating, votes, imdb_id) using TMDb external_ids -> OMDb."""
    if not (TMDB_API_KEY and OMDB_API_KEY and tmdb_id):
        return None, None, None

    cat = "tv" if vtype == "series" else "movie"
    ext = _get_json(
        "https://api.themoviedb.org/3/%s/%s/external_ids" % (cat, tmdb_id),
        params={"api_key": TMDB_API_KEY},
        timeout=15
    )
    imdb_id = (ext or {}).get("imdb_id")
    if not imdb_id:
        return None, None, None

    rating, votes = get_imdb_from_omdb(str(imdb_id), cache)
    return rating, votes, str(imdb_id)

# ---------- uNoGS: fetching ----------
def fetch_candidates() -> List[Dict[str, Any]]:
    """Fetch ALL titles in BE catalog from uNoGS search (paged)."""
    items = []  # type: List[Dict[str, Any]]
    headers = {"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": UNOGS_HOST}

    for t in ("movie", "series"):
        offset = 0
        while True:
            j = _get_json(
                UNOGS_URL,
                headers=headers,
                params={"type": t, "countrylist": str(BE_ID), "offset": offset, "limit": 100},
                timeout=30
            )
            if not j:
                break
            batch = j.get("results") or j.get("RESULTS") or []
            if not batch:
                break

            for x in batch:
                items.append({
                    "nfid": x.get("nfid"),
                    "title": _pick_title(x, "title", "name", "t"),
                    "vtype": _norm_type(x.get("vtype") or t),
                    "raw_imdb": x.get("imdb_rating") or x.get("rating_imdb") or x.get("imdbrating"),
                    "imdb_id": x.get("imdbid") or x.get("imdb_id"),
                    "imdb_votes": _parse_int(x.get("imdbvotes") or x.get("imdb_votes")),
                    "releaseYear": str(x.get("year") or x.get("releaseYear") or "")[:4],
                    "ndate": x.get("ndate") or x.get("new_date") or x.get("newDate") or x.get("date"),
                    "tmdb_id": x.get("tmdb_id") or x.get("tmid"),
                })

            if len(batch) < 100:
                break
            offset += 100
            if SLEEP_FETCH_PAGE:
                time.sleep(SLEEP_FETCH_PAGE)

    return items

def _fetch_recent_pages(params_base: dict, headers: dict, cutoff: datetime.date, stop_when_older: bool) -> Dict[int, datetime.date]:
    """Internal helper for recent-date map building."""
    out = {}  # type: Dict[int, datetime.date]
    offset = 0
    while True:
        params = dict(params_base)
        params.update({"offset": offset, "limit": 100})
        j = _get_json(UNOGS_URL, headers=headers, params=params, timeout=30)
        if not j:
            break
        batch = j.get("results") or j.get("RESULTS") or []
        if not batch:
            break

        oldest_in_batch = None  # type: Optional[datetime.date]

        for x in batch:
            nfid = _parse_int(x.get("nfid"))
            if not nfid:
                continue
            d = _parse_date(x.get("ndate") or x.get("new_date") or x.get("newDate") or x.get("date"))
            if not d:
                continue
            out[nfid] = d
            if oldest_in_batch is None or d < oldest_in_batch:
                oldest_in_batch = d

        # If sorted descending and we are past cutoff, we can stop
        if stop_when_older and oldest_in_batch and oldest_in_batch < cutoff:
            break

        if len(batch) < 100:
            break
        offset += 100
        if SLEEP_FETCH_PAGE:
            time.sleep(SLEEP_FETCH_PAGE)

    # keep only >= cutoff
    out = {k: v for k, v in out.items() if v >= cutoff}
    return out

def fetch_recent_added_dates(cutoff: datetime.date) -> Dict[int, datetime.date]:
    """
    Fetch recently added titles from uNoGS search.
    Primary: newdate=cutoff + orderby dateDesc.
    Fallback: orderby dateDesc without newdate (then stop when older than cutoff).
    """
    headers = {"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": UNOGS_HOST}
    out = {}  # type: Dict[int, datetime.date]

    # 1) primary path using newdate (this is the "old script" trick, but scaled)
    for t in ("movie", "series"):
        base = {"type": t, "countrylist": str(BE_ID), "newdate": cutoff.isoformat(), "orderby": "dateDesc"}
        out.update(_fetch_recent_pages(base, headers, cutoff, stop_when_older=False))

    if out:
        return out

    # 2) fallback path (some uNoGS setups ignore newdate)
    print("ℹ️ recent-date map empty via newdate; fallback to orderby=dateDesc without newdate")
    for t in ("movie", "series"):
        base = {"type": t, "countrylist": str(BE_ID), "orderby": "dateDesc"}
        out.update(_fetch_recent_pages(base, headers, cutoff, stop_when_older=True))

    return out

# ---------- MAIN ----------
def main() -> None:
    if not UNOGS_API_KEY:
        print("❌ UNOGS_API_KEY ontbreekt.")
        return

    manual_nfid, manual_key = load_manual_overrides(MANUAL_SCORES)
    avail_cache = _load_cache(AVAIL_CACHE)
    imdb_cache = _load_cache(IMDB_CACHE)

    cutoff = datetime.datetime.now(datetime.timezone.utc).date() - datetime.timedelta(days=DAYS_RECENT)

    # KEY FIX: get dates from /search orderby=dateDesc (+newdate)
    recent_date_map = fetch_recent_added_dates(cutoff)
    print("🕒 Recent date-map:", len(recent_date_map), "NFIDs sinds", cutoff.isoformat())

    candidates = fetch_candidates()
    print("📡 uNoGS candidates:", len(candidates))

    stats = {
        "bad_nfid": 0,
        "duplicate_nfid": 0,
        "excluded_manual": 0,
        "unknown_type": 0,
        "no_rating": 0,
        "below_threshold": 0,
        "unogs_placeholder_10": 0,
        "availability_false_recent": 0,
        "availability_unknown_recent": 0,
        "availability_false_full": 0,
        "availability_unknown_full": 0,
    }

    seen_nfids = set()  # type: set
    final_list = []     # type: List[Dict[str, Any]]

    for it in candidates:
        nfid = _parse_int(it.get("nfid"))
        if not nfid:
            stats["bad_nfid"] += 1
            continue
        if nfid in seen_nfids:
            stats["duplicate_nfid"] += 1
            continue
        seen_nfids.add(nfid)

        ov = manual_nfid.get(nfid) or manual_key.get((_norm_title(it.get("title")), it.get("vtype")))
        if ov and ov.get("exclude"):
            stats["excluded_manual"] += 1
            continue

        vtype = _norm_type(ov.get("type")) if (ov and ov.get("type")) else it.get("vtype")
        if vtype == "unknown":
            stats["unknown_type"] += 1
            continue

        ignore_thresholds = bool(ov and (ov.get("ignoreThresholds") or ov.get("forceInclude")))

        # dateAdded: from recent map first, then from candidate field (if present)
        added_date = recent_date_map.get(nfid) or _parse_date(it.get("ndate"))
        is_recent = bool(added_date and added_date >= cutoff)

        # rating priority
        rating = None   # type: Optional[float]
        votes = None    # type: Optional[int]
        source = None   # type: Optional[str]

        # 1) manual rating
        if ov and ov.get("imdbRating") is not None:
            rating = _parse_rating(ov.get("imdbRating"))
            source = "manual"

        # 2) OMDb by imdb_id
        if rating is None and it.get("imdb_id"):
            r, v = get_imdb_from_omdb(str(it["imdb_id"]), imdb_cache)
            if r is not None:
                rating, votes, source = r, v, "imdb"

        # 3) TMDb external ids -> OMDb
        if rating is None and it.get("tmdb_id") and TMDB_API_KEY and OMDB_API_KEY:
            r, v, _ = get_imdb_via_tmdb(str(it["tmdb_id"]), vtype, imdb_cache)
            if r is not None:
                rating, votes, source = r, v, "imdb"

        # 4) uNoGS rating fallback (with sanity for fake 10.0)
        unogs_rating = _parse_rating(it.get("raw_imdb"))
        if rating is None and unogs_rating is not None:
            u_votes = it.get("imdb_votes") or 0
            if unogs_rating >= 9.9 and u_votes < 5000:
                stats["unogs_placeholder_10"] += 1
            else:
                rating, votes, source = unogs_rating, (u_votes or None), "unogs_imdb"

        if rating is None:
            stats["no_rating"] += 1
            continue

        # thresholds (post-enrichment)
        if not ignore_thresholds:
            if source in ("manual", "imdb", "unogs_imdb") and rating < IMDB_MIN:
                stats["below_threshold"] += 1
                continue
            if source == "tmdb" and rating < TMDB_MIN:
                stats["below_threshold"] += 1
                continue

        # availability (strict for recent)
        be = verify_belgium_availability(nfid, avail_cache)
        if is_recent:
            if be is not True:
                stats["availability_false_recent" if be is False else "availability_unknown_recent"] += 1
                continue
        else:
            if be is False:
                stats["availability_false_full"] += 1
                continue
            if be is None:
                stats["availability_unknown_full"] += 1

        final_list.append({
            "nfid": nfid,
            "title": it.get("title"),
            "type": "Series" if vtype == "series" else "Film",
            "imdbRating": rating,
            "ratingSource": source,
            "votes": votes,
            "releaseDate": it.get("releaseYear"),
            "dateAdded": added_date.isoformat() if added_date else None,
        })

    OUT_FULL.write_text(json.dumps(final_list, ensure_ascii=False, indent=2), encoding="utf-8")

    recent = [x for x in final_list if x.get("dateAdded") and datetime.date.fromisoformat(x["dateAdded"]) >= cutoff]
    OUT_RECENT.write_text(json.dumps(recent, ensure_ascii=False, indent=2), encoding="utf-8")

    _save_cache(AVAIL_CACHE, avail_cache, max_days=180)
    _save_cache(IMDB_CACHE, imdb_cache, max_days=365)

    print("✅ GEREED:", len(final_list), "totaal,", len(recent), "recent.")
    print("📉 Dropped stats:", stats)
    print(
        "ℹ️ Debug:",
        sum(1 for x in final_list if x.get("dateAdded")), "/", len(final_list),
        "items hebben een dateAdded; cutoff=%s" % cutoff.isoformat()
    )

if __name__ == "__main__":
    main()
