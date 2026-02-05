import os, json, datetime, time, re, html
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

import requests

# -----------------------------
# CONFIG
# -----------------------------
ROOT = Path(__file__).parent.resolve()
OUT_FULL   = ROOT / "netflix_data.json"
OUT_RECENT = ROOT / "netflix_last_month.json"

MANUAL_SCORES = ROOT / "manual_scores.json"
AVAIL_CACHE   = ROOT / "availability_cache.json"
IMDB_CACHE    = ROOT / "imdb_cache.json"

DAYS_RECENT = 90
BE_ID = 21

# Score thresholds (apply unless manual says ignoreThresholds/forceInclude)
IMDB_MIN = 7.7
TMDB_MIN = 7.0

# Availability cache TTLs (days)
TTL_TRUE_DAYS  = 1     # refresh "still in BE" quickly
TTL_FALSE_DAYS = 7     # "not in BE" can be cached longer
TTL_NONE_HOURS = 6     # short cache for temporary API hiccups / rate limit

# IMDb cache TTL (days)
IMDB_TTL_DAYS = 30

TMDB_API_KEY  = os.environ.get("TMDB_API_KEY", "").strip()
UNOGS_API_KEY = os.environ.get("UNOGS_API_KEY", "").strip()
OMDB_API_KEY  = os.environ.get("OMDB_API_KEY", "").strip()

UNOGS_HOST = "unogsng.p.rapidapi.com"
UNOGS_URL  = f"https://{UNOGS_HOST}/search"
UNOGS_TITLECOUNTRIES_URL = f"https://{UNOGS_HOST}/titlecountries"

SESSION = requests.Session()

# Pace calls to /titlecountries (avoid 429). Recent items are checked first; archive uses cache-only.
_MIN_SECONDS_BETWEEN_TITLECOUNTRIES = 0.25
_last_titlecountries_call_ts = 0.0


# -----------------------------
# HTTP helper
# -----------------------------
def _get_json(
    url: str,
    *,
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
    retries: int = 3,
    timeout: int = 12,
    backoff_base: float = 1.8,
) -> Optional[dict]:
    """
    Resilient GET helper:
    - retries on 429 and common 5xx
    - exponential backoff
    - returns dict or None
    """
    for i in range(retries + 1):
        try:
            r = SESSION.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504) and i < retries:
                time.sleep(backoff_base ** i)
                continue
            if r.status_code != 200:
                return None
            return r.json()
        except Exception:
            if i < retries:
                time.sleep(backoff_base ** i)
            continue
    return None


# -----------------------------
# Parsing helpers
# -----------------------------
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
        s = str(x).replace(",", "").strip()
        return int(float(s))
    except Exception:
        return None

def _parse_date(x: Any) -> Optional[datetime.date]:
    if not x:
        return None
    try:
        # unix timestamp (seconds or ms)
        if isinstance(x, (int, float)) or (isinstance(x, str) and str(x).isdigit()):
            n = int(float(x))
            if n > 10_000_000_000:  # ms
                n //= 1000
            return datetime.datetime.fromtimestamp(n, tz=datetime.timezone.utc).date()
        return datetime.date.fromisoformat(str(x)[:10])
    except Exception:
        return None

def _norm_type(t: Any) -> str:
    t = str(t or "").strip().lower()
    if t in ("serie", "series", "tv", "show", "tvseries", "tv series"):
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
        # prevent picking NFIDs mistakenly stored as title
        if s.isdigit() and len(s) > 4:
            continue
        return html.unescape(s)
    return default


# -----------------------------
# Cache and manual overrides
# -----------------------------
def _load_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_cache(path: Path, data: dict, *, max_days: int = 180) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    pruned: dict = {}
    for k, v in (data or {}).items():
        try:
            ts = datetime.datetime.fromisoformat(v.get("ts", ""))
            if (now - ts).days < max_days:
                pruned[k] = v
        except Exception:
            # drop bad cache entries
            continue
    path.write_text(json.dumps(pruned, ensure_ascii=False, indent=2), encoding="utf-8")

def load_manual_overrides() -> Tuple[Dict[int, dict], Dict[Tuple[str, str], dict]]:
    """
    Supports two manual modes:
    - by nfid: {"nfid": 123, "exclude": true, "imdbRating": "8.4", "type": "series", ...}
    - by (title,type): {"title":"...", "type":"series", ...}
    """
    by_nfid: Dict[int, dict] = {}
    by_key: Dict[Tuple[str, str], dict] = {}

    if not MANUAL_SCORES.exists():
        return by_nfid, by_key

    try:
        txt = MANUAL_SCORES.read_text(encoding="utf-8")
        # tolerate trailing commas
        txt = re.sub(r",\s*(\]|\})", r"\1", txt)
        raw = json.loads(txt)
        for row in (raw if isinstance(raw, list) else []):
            if not isinstance(row, dict):
                continue
            if "type" in row:
                row["type"] = _norm_type(row.get("type"))
            if "nfid" in row and str(row["nfid"]).isdigit():
                by_nfid[int(row["nfid"])] = row
            elif row.get("title") and row.get("type") in ("movie", "series"):
                by_key[(_norm_title(row["title"]), row["type"])] = row
    except Exception as e:
        print(f"⚠️ manual_scores.json kon niet geladen worden: {e}")

    return by_nfid, by_key


# -----------------------------
# Availability (Belgium) check
# -----------------------------
def verify_belgium_availability(nfid: int, cache: dict, *, allow_network: bool, strict: bool) -> Optional[bool]:
    """
    Returns:
      True  -> confirmed in BE
      False -> confirmed not in BE
      None  -> unknown (API hiccup / rate limit / no cache)
    """
    global _last_titlecountries_call_ts

    key = str(nfid)
    now = datetime.datetime.now(datetime.timezone.utc)

    # Cache hit?
    if key in cache:
        try:
            ts = datetime.datetime.fromisoformat(cache[key].get("ts", ""))
            cached_be = cache[key].get("be", None)
            if cached_be is True:
                ttl_days = TTL_TRUE_DAYS
            elif cached_be is False:
                ttl_days = TTL_FALSE_DAYS
            else:
                ttl_days = TTL_NONE_HOURS / 24.0

            if (now - ts).total_seconds() < ttl_days * 86400:
                return cached_be
        except Exception:
            pass

    if not allow_network:
        return None

    # pace requests to avoid 429
    dt = time.time() - _last_titlecountries_call_ts
    if dt < _MIN_SECONDS_BETWEEN_TITLECOUNTRIES:
        time.sleep(_MIN_SECONDS_BETWEEN_TITLECOUNTRIES - dt)

    # stronger retries for strict mode (recent list)
    retries = 6 if strict else 2
    timeout = 18 if strict else 12

    res = _get_json(
        UNOGS_TITLECOUNTRIES_URL,
        headers={"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": UNOGS_HOST},
        params={"netflixid": nfid},
        retries=retries,
        timeout=timeout,
        backoff_base=2.0,
    )
    _last_titlecountries_call_ts = time.time()

    if res is None:
        cache[key] = {"be": None, "ts": now.isoformat()}
        return None

    results = res.get("results") or res.get("RESULTS") or res.get("Countries") or []
    in_be = False
    if isinstance(results, list):
        for c in results:
            if not isinstance(c, dict):
                continue
            cid = c.get("id") or c.get("countryid") or c.get("country_id")
            cc = c.get("cc") or c.get("countrycode") or c.get("country_code")
            if (cid is not None and str(cid) == str(BE_ID)) or (cc is not None and str(cc).upper() == "BE"):
                in_be = True
                break

    cache[key] = {"be": in_be, "ts": now.isoformat()}
    return in_be


# -----------------------------
# IMDb rating: OMDb (gold) + TMDb external ids (fallback)
# -----------------------------
def get_imdb_from_omdb(imdb_id: str, cache: dict) -> Optional[float]:
    if not (OMDB_API_KEY and imdb_id):
        return None
    key = f"imdb:{imdb_id}"
    now = datetime.datetime.now(datetime.timezone.utc)

    if key in cache:
        try:
            ts = datetime.datetime.fromisoformat(cache[key].get("ts", ""))
            if (now - ts).days < IMDB_TTL_DAYS:
                return cache[key].get("imdb")
        except Exception:
            pass

    om = _get_json("https://www.omdbapi.com/", params={"i": imdb_id, "apikey": OMDB_API_KEY}, retries=2, timeout=12)
    imdb = _parse_rating((om or {}).get("imdbRating"))
    cache[key] = {"imdb": imdb, "ts": now.isoformat()}
    return imdb

def get_imdb_via_tmdb(tmdb_id: str, vtype: str, cache: dict) -> Optional[float]:
    if not (TMDB_API_KEY and tmdb_id):
        return None
    cat = "tv" if vtype == "series" else "movie"
    ext = _get_json(f"https://api.themoviedb.org/3/{cat}/{tmdb_id}/external_ids", params={"api_key": TMDB_API_KEY}, retries=2, timeout=12)
    imdb_id = (ext or {}).get("imdb_id")
    return get_imdb_from_omdb(str(imdb_id), cache) if imdb_id else None


# -----------------------------
# uNoGS fetch
# -----------------------------
def fetch_candidates() -> List[Dict[str, Any]]:
    if not UNOGS_API_KEY:
        print("❌ UNOGS_API_KEY ontbreekt.")
        return []

    items: List[Dict[str, Any]] = []
    headers = {"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": UNOGS_HOST}

    for t in ("movie", "series"):
        offset = 0
        while True:
            j = _get_json(
                UNOGS_URL,
                headers=headers,
                params={"type": t, "countrylist": str(BE_ID), "offset": offset, "limit": 100},
                retries=2,
                timeout=30,
                backoff_base=1.8,
            )
            if not j:
                break

            batch = j.get("results") or j.get("RESULTS") or []
            if not batch:
                break

            for x in batch:
                items.append(
                    {
                        "nfid": x.get("nfid"),
                        "title": _pick_title(x, "title", "name", "t"),
                        "vtype": _norm_type(x.get("vtype") or t),
                        "raw_imdb": x.get("imdb_rating") or x.get("rating_imdb") or x.get("imdbrating"),
                        "imdb_id": x.get("imdbid") or x.get("imdb_id"),
                        "imdb_votes": _parse_int(x.get("imdbvotes") or x.get("imdb_votes")),
                        "releaseYear": str(x.get("year") or x.get("releaseYear") or "")[:4],
                        "ndate": x.get("ndate") or x.get("new_date"),
                        "tmdb_id": x.get("tmdb_id") or x.get("tmid"),
                    }
                )

            if len(batch) < 100:
                break
            offset += 100
            time.sleep(0.2)

    return items


# -----------------------------
# Normalization + rating selection
# -----------------------------
def build_scored_list(
    candidates: List[Dict[str, Any]],
    manual_nfid: Dict[int, dict],
    manual_key: Dict[Tuple[str, str], dict],
    imdb_cache: dict,
) -> Tuple[List[dict], dict]:
    """
    Returns (scored_items, dropped_stats).
    scored_items contains (nfid,title,type,vtype,releaseDate,dateAdded,rating,...)
    """
    dropped = {
        "bad_nfid": 0,
        "duplicate_nfid": 0,
        "excluded_manual": 0,
        "unknown_type": 0,
        "no_rating": 0,
        "below_threshold": 0,
        "unogs_placeholder_10": 0,
    }

    seen: set[int] = set()
    out: List[dict] = []

    for it in candidates:
        nfid_raw = it.get("nfid")
        try:
            nfid = int(nfid_raw)
        except Exception:
            dropped["bad_nfid"] += 1
            continue

        if nfid in seen:
            dropped["duplicate_nfid"] += 1
            continue
        seen.add(nfid)

        title = it.get("title") or "Onbekend"
        vtype = it.get("vtype") or "unknown"

        # Manual override lookup
        ov = manual_nfid.get(nfid) or manual_key.get((_norm_title(title), vtype))
        if ov and ov.get("exclude"):
            dropped["excluded_manual"] += 1
            continue

        if ov and ov.get("type"):
            vtype = _norm_type(ov.get("type"))

        if vtype not in ("movie", "series"):
            dropped["unknown_type"] += 1
            continue

        ignore_thresholds = bool(ov and (ov.get("ignoreThresholds") or ov.get("forceInclude")))

        release_year = it.get("releaseYear") or ""
        if ov and ov.get("releaseYear"):
            release_year = str(ov.get("releaseYear"))[:4]
        elif ov and ov.get("releaseDate"):
            release_year = str(ov.get("releaseDate"))[:4]

        added_date = _parse_date(it.get("ndate"))

        # Rating selection (priority)
        rating: Optional[float] = None
        source: Optional[str] = None

        # 1) manual imdbRating
        if ov and ov.get("imdbRating") not in (None, "", "no score", "N/A"):
            rating = _parse_rating(ov.get("imdbRating"))
            source = "manual"

        # 2) OMDb by imdb_id from uNoGS (if present)
        if rating is None and it.get("imdb_id"):
            rating = get_imdb_from_omdb(str(it.get("imdb_id")), imdb_cache)
            if rating is not None:
                source = "imdb"

        # 3) OMDb via TMDb external ids (if no imdb_id)
        if rating is None and it.get("tmdb_id"):
            rating = get_imdb_via_tmdb(str(it.get("tmdb_id")), vtype, imdb_cache)
            if rating is not None:
                source = "imdb"

        # 4) uNoGS "imdb" field as last resort, with sanity for 10.0 placeholders
        unogs_rating = _parse_rating(it.get("raw_imdb"))
        if rating is None and unogs_rating is not None:
            votes = it.get("imdb_votes") or 0
            if unogs_rating >= 9.9 and votes < 5000:
                dropped["unogs_placeholder_10"] += 1
            else:
                rating = unogs_rating
                source = "unogs_imdb"

        # Thresholds (apply after enrichment)
        if not ignore_thresholds:
            if rating is None:
                dropped["no_rating"] += 1
                continue
            if source in ("imdb", "unogs_imdb", "manual") and rating < IMDB_MIN:
                dropped["below_threshold"] += 1
                continue
            if source == "tmdb" and rating < TMDB_MIN:
                dropped["below_threshold"] += 1
                continue

        out.append(
            {
                "nfid": nfid,
                "title": title,
                "vtype": vtype,  # internal
                "type": "Series" if vtype == "series" else "Film",  # for frontend
                "releaseDate": release_year,
                "dateAdded": added_date.isoformat() if added_date else None,
                "rating": rating,         # for frontend (compat)
                "imdbRating": rating,     # explicit
                "ratingSource": source,
                "ignoreThresholds": ignore_thresholds,
                "tmdb_id": it.get("tmdb_id"),
                "imdb_id": it.get("imdb_id"),
            }
        )

    return out, dropped


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    if not UNOGS_API_KEY:
        print("❌ UNOGS_API_KEY ontbreekt. Abort.")
        return

    manual_nfid, manual_key = load_manual_overrides()
    avail_cache = _load_cache(AVAIL_CACHE)
    imdb_cache  = _load_cache(IMDB_CACHE)

    candidates = fetch_candidates()
    print(f"📡 uNoGS candidates: {len(candidates)}")

    scored, dropped = build_scored_list(candidates, manual_nfid, manual_key, imdb_cache)

    # Partition: recent first (prevents rate-limit issues making "recent" empty)
    today_utc = datetime.datetime.now(datetime.timezone.utc).date()
    cutoff = today_utc - datetime.timedelta(days=DAYS_RECENT)

    def _is_recent(item: dict) -> bool:
        try:
            return bool(item.get("dateAdded") and datetime.date.fromisoformat(item["dateAdded"]) >= cutoff)
        except Exception:
            return False

    recent_items = [x for x in scored if _is_recent(x)]
    other_items  = [x for x in scored if not _is_recent(x)]

    stats = {
        "availability_false_recent": 0,
        "availability_unknown_recent": 0,
        "availability_false_full": 0,
        "availability_unknown_full": 0,
    }

    final_list: List[dict] = []

    # 1) Validate recent items strictly (network allowed)
    for it in recent_items:
        be = verify_belgium_availability(it["nfid"], avail_cache, allow_network=True, strict=True)
        it["availableBE"] = be
        if be is False:
            stats["availability_false_recent"] += 1
            continue
        if be is None:
            stats["availability_unknown_recent"] += 1
        final_list.append(it)

    # 2) For older items: avoid hammering API.
    #    - If cached False: drop
    #    - Else: keep (cached True/None)
    for it in other_items:
        be = verify_belgium_availability(it["nfid"], avail_cache, allow_network=False, strict=False)
        it["availableBE"] = be
        if be is False:
            stats["availability_false_full"] += 1
            continue
        if be is None:
            stats["availability_unknown_full"] += 1
        final_list.append(it)

    # Save caches
    _save_cache(AVAIL_CACHE, avail_cache, max_days=180)
    _save_cache(IMDB_CACHE, imdb_cache, max_days=365)

    # Output FULL
    OUT_FULL.write_text(json.dumps(final_list, ensure_ascii=False, indent=2), encoding="utf-8")

    # Output RECENT:
    # Prefer verified True, but if that becomes empty due to API hiccup,
    # fall back to "not False" so the homepage doesn't go blank.
    recent_verified = [x for x in final_list if _is_recent(x) and x.get("availableBE") is True]
    if recent_verified:
        recent_out = recent_verified
    else:
        recent_out = [x for x in final_list if _is_recent(x) and x.get("availableBE") is not False]

    OUT_RECENT.write_text(json.dumps(recent_out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"✅ GEREED: {len(final_list)} totaal, {len(recent_out)} recent.")
    print(f"📉 Dropped stats: {dict(dropped, **stats)}")


if __name__ == "__main__":
    main()
