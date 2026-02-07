import os, sys, json, time, re, html
import datetime
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import requests

# =========================
# CONFIG
# =========================
ROOT = Path(__file__).parent.resolve()

OUT_FULL   = ROOT / "netflix_data.json"
OUT_RECENT = ROOT / "netflix_last_month.json"

MANUAL_SCORES = ROOT / "manual_scores.json"
AVAIL_CACHE   = ROOT / "availability_cache.json"
META_CACHE    = ROOT / "imdb_cache.json"   # we keep OMDb/TMDb/Trakt caches in 1 file to keep repo simple

DAYS_RECENT = 90
BE_ID = 21

# Cutoff gatekeeper
IMDB_MIN = 7.8

# Availability TTL:
# - True: refresh often (so "recent" stays fresh)
# - False: cache longer (less API burn)
TTL_TRUE_DAYS  = 1
TTL_FALSE_DAYS = 7

# Rating cache TTL
OMDB_TTL_DAYS  = 30
TMDB_TTL_DAYS  = 30
TRAKT_TTL_DAYS = 30

# Throttles
THROTTLE_TITLECOUNTRIES_SEC = 0.10   # helps avoid uNoGS 429
THROTTLE_TRAKT_SEC          = 0.35   # Trakt can be strict
THROTTLE_TMDB_SEC           = 0.05

# API keys (GitHub Secrets)
UNOGS_API_KEY   = os.environ.get("UNOGS_API_KEY", "").strip()
TMDB_API_KEY    = os.environ.get("TMDB_API_KEY", "").strip()
OMDB_API_KEY    = os.environ.get("OMDB_API_KEY", "").strip()
TRAKT_CLIENT_ID = os.environ.get("TRAKT_CLIENT_ID", "").strip()  # optional but recommended

UNOGS_HOST = "unogsng.p.rapidapi.com"
UNOGS_URL  = f"https://{UNOGS_HOST}/search"

SESSION = requests.Session()

# If you want strict safety (never include availability unknown):
ALLOW_AVAILABILITY_UNKNOWN = os.environ.get("ALLOW_AVAILABILITY_UNKNOWN", "0").strip() == "1"


# =========================
# HELPERS
# =========================
def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)

def _fix_trailing_commas_json(txt: str) -> str:
    # tolerates "...,]" and "...,}"
    return re.sub(r",\s*(\]|\})", r"\1", txt or "")

def _load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except:
        try:
            return json.loads(_fix_trailing_commas_json(path.read_text(encoding="utf-8")))
        except:
            return default

def _save_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def _parse_float(x: Any) -> Optional[float]:
    if x in (None, "", "N/A", "no score", "nan", "None"):
        return None
    s = str(x).strip().replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        v = float(m.group(1))
        if 0 < v <= 10:
            return round(v, 1)
        return None
    except:
        return None

def _fmt_rating(v: Optional[float]) -> str:
    return "N/A" if v is None else f"{v:.1f}".rstrip("0").rstrip(".")

def _parse_int(x: Any) -> Optional[int]:
    if x in (None, "", "N/A", "None"):
        return None
    try:
        return int(float(str(x).replace(",", "").strip()))
    except:
        return None

def _parse_date(x: Any) -> Optional[datetime.date]:
    if not x:
        return None
    try:
        s = str(x).strip()
        if s in ("0", "None", "N/A"):
            return None

        # epoch seconds/ms
        if s.isdigit():
            n = int(s)
            if n > 10_000_000_000:
                n //= 1000
            return datetime.datetime.fromtimestamp(n, tz=datetime.timezone.utc).date()

        # YYYYMMDD
        if len(s) == 8 and s.isdigit():
            return datetime.date(int(s[:4]), int(s[4:6]), int(s[6:8]))

        # ISO-ish
        s = s.replace("/", "-")
        return datetime.date.fromisoformat(s[:10])
    except:
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

def _pick_title(d: Dict[str, Any], *keys: str, default="Onbekend") -> str:
    for k in keys:
        v = d.get(k)
        if v in (None, "", 0, "0", "None"):
            continue
        s = str(v).strip()
        if s.isdigit() and len(s) > 4:
            continue
        return html.unescape(s)
    return default

def _hard_exit(msg: str, code: int = 2) -> None:
    print(f"❌ {msg}")
    raise SystemExit(code)


# =========================
# NETWORK
# =========================
class ApiFailState:
    def __init__(self):
        self.consecutive_hard_429 = 0

FAILSTATE = ApiFailState()

def _get_json(url: str, headers: dict = None, params: dict = None, timeout: int = 15, retries: int = 2) -> Optional[dict]:
    """
    Robust GET:
    - retries on 429/5xx with backoff
    - hard-exits on 401/403 (bad key/quota)
    - prevents infinite retry loops
    """
    global FAILSTATE
    for i in range(retries + 1):
        try:
            r = SESSION.get(url, headers=headers, params=params, timeout=timeout)

            if r.status_code in (401, 403):
                _hard_exit(f"Auth/quota error {r.status_code} on {url}. Check your API key / plan.")

            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                if ra:
                    FAILSTATE.consecutive_hard_429 = 0
                    time.sleep(float(ra) + 0.2)
                    continue
                FAILSTATE.consecutive_hard_429 += 1
                if FAILSTATE.consecutive_hard_429 >= 3:
                    _hard_exit("Too many 429 without Retry-After. Likely quota exhausted. Stopping to avoid long Actions run.")
                time.sleep(2 * (i + 1))
                continue

            if r.status_code in (500, 502, 503, 504) and i < retries:
                time.sleep(2 * (i + 1))
                continue

            if r.status_code != 200:
                return None

            FAILSTATE.consecutive_hard_429 = 0
            return r.json()

        except requests.RequestException:
            if i < retries:
                time.sleep(1.5)
                continue
            return None
        except ValueError:
            return None
    return None


# =========================
# MANUAL OVERRIDES
# =========================
def load_manual_overrides() -> Tuple[Dict[int, dict], Dict[Tuple[str, str], dict]]:
    """
    manual_scores.json supports:
    - nfid (preferred)
    - title + type fallback
    - fields: exclude, imdbRating, ignoreThresholds/forceInclude, releaseYear/releaseDate, type
    """
    by_nfid: Dict[int, dict] = {}
    by_key: Dict[Tuple[str, str], dict] = {}

    if not MANUAL_SCORES.exists():
        return by_nfid, by_key

    try:
        raw = _load_json(MANUAL_SCORES, default=[])
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
        print(f"⚠️ manual_scores error: {e}")

    return by_nfid, by_key


# =========================
# uNoGS: BE availability + BE added-date
# =========================
def verify_be_strict(nfid: int, avail_cache: dict) -> Tuple[Optional[bool], Optional[datetime.date]]:
    """
    Returns (available_be, be_date).
    Cache TTL:
      - True -> TTL_TRUE_DAYS
      - False -> TTL_FALSE_DAYS
    """
    key = str(nfid)
    now = _utcnow()

    if key in avail_cache:
        try:
            ts = datetime.datetime.fromisoformat(avail_cache[key]["ts"])
            cached_be = avail_cache[key].get("be")
            ttl = TTL_TRUE_DAYS if cached_be is True else TTL_FALSE_DAYS
            if (now - ts).days < ttl:
                return cached_be, _parse_date(avail_cache[key].get("be_date"))
        except:
            pass

    time.sleep(THROTTLE_TITLECOUNTRIES_SEC)

    headers = {"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": UNOGS_HOST}
    res = _get_json(f"https://{UNOGS_HOST}/titlecountries", headers=headers, params={"netflixid": nfid}, timeout=20, retries=2)
    if res is None:
        return None, None

    results = res.get("results") or res.get("RESULTS") or res.get("Countries") or []
    in_be = False
    be_date: Optional[datetime.date] = None

    if isinstance(results, list):
        for c in results:
            try:
                cid = c.get("id") or c.get("countryid") or c.get("country_id")
                cc  = c.get("cc") or c.get("countrycode") or c.get("country_code")
                if (cid and str(cid) == str(BE_ID)) or (cc and str(cc).upper() == "BE"):
                    in_be = True
                    # country-specific "new_date"/"ndate" (naming varies)
                    be_date = _parse_date(c.get("new_date") or c.get("ndate") or c.get("date"))
                    break
            except:
                continue

    avail_cache[key] = {"be": in_be, "be_date": be_date.isoformat() if be_date else None, "ts": now.isoformat()}
    return in_be, be_date


# =========================
# OMDb (IMDb rating via IMDb ID)
# =========================
def get_imdb_from_omdb(imdb_id: str, meta_cache: dict) -> Tuple[Optional[float], Optional[int]]:
    if not (OMDB_API_KEY and imdb_id):
        return None, None

    key = f"omdb:{imdb_id}"
    now = _utcnow()

    if key in meta_cache:
        try:
            ts = datetime.datetime.fromisoformat(meta_cache[key]["ts"])
            if (now - ts).days < OMDB_TTL_DAYS:
                return meta_cache[key].get("imdb"), meta_cache[key].get("votes")
        except:
            pass

    om = _get_json("https://www.omdbapi.com/", params={"i": imdb_id, "apikey": OMDB_API_KEY}, timeout=15, retries=2)
    imdb = _parse_float((om or {}).get("imdbRating"))
    votes = _parse_int((om or {}).get("imdbVotes"))
    meta_cache[key] = {"imdb": imdb, "votes": votes, "ts": now.isoformat()}
    return imdb, votes


# =========================
# TMDb: details + external ids
# =========================
def tmdb_external_imdb_id(tmdb_id: str, vtype: str, meta_cache: dict) -> Optional[str]:
    if not (TMDB_API_KEY and tmdb_id):
        return None
    key = f"tmdb_ext:{vtype}:{tmdb_id}"
    now = _utcnow()

    if key in meta_cache:
        try:
            ts = datetime.datetime.fromisoformat(meta_cache[key]["ts"])
            if (now - ts).days < TMDB_TTL_DAYS:
                return meta_cache[key].get("imdb_id")
        except:
            pass

    cat = "tv" if vtype == "series" else "movie"
    time.sleep(THROTTLE_TMDB_SEC)
    ext = _get_json(f"https://api.themoviedb.org/3/{cat}/{tmdb_id}/external_ids", params={"api_key": TMDB_API_KEY}, timeout=15, retries=2)
    imdb_id = (ext or {}).get("imdb_id")
    meta_cache[key] = {"imdb_id": imdb_id, "ts": now.isoformat()}
    return imdb_id

def tmdb_details(tmdb_id: str, vtype: str, meta_cache: dict) -> Tuple[Optional[float], Optional[str]]:
    """
    Returns (tmdb_vote_average, release_date/first_air_date as YYYY-MM-DD)
    """
    if not (TMDB_API_KEY and tmdb_id):
        return None, None
    key = f"tmdb_det:{vtype}:{tmdb_id}"
    now = _utcnow()

    if key in meta_cache:
        try:
            ts = datetime.datetime.fromisoformat(meta_cache[key]["ts"])
            if (now - ts).days < TMDB_TTL_DAYS:
                return meta_cache[key].get("vote"), meta_cache[key].get("date")
        except:
            pass

    cat = "tv" if vtype == "series" else "movie"
    time.sleep(THROTTLE_TMDB_SEC)
    d = _get_json(f"https://api.themoviedb.org/3/{cat}/{tmdb_id}", params={"api_key": TMDB_API_KEY}, timeout=15, retries=2)
    vote = _parse_float((d or {}).get("vote_average"))
    dt = (d or {}).get("first_air_date") or (d or {}).get("release_date")
    dt = str(dt)[:10] if dt else None
    meta_cache[key] = {"vote": vote, "date": dt, "ts": now.isoformat()}
    return vote, dt


# =========================
# Trakt: rating via IMDb ID
# =========================
def trakt_rating(imdb_id: str, vtype: str, meta_cache: dict) -> Optional[float]:
    """
    Uses summary endpoints:
      /movies/{id} or /shows/{id} with id = IMDb id (tt....)
    This is far less ambiguous than title search. :contentReference[oaicite:1]{index=1}
    """
    if not (TRAKT_CLIENT_ID and imdb_id):
        return None

    key = f"trakt:{vtype}:{imdb_id}"
    now = _utcnow()

    if key in meta_cache:
        try:
            ts = datetime.datetime.fromisoformat(meta_cache[key]["ts"])
            if (now - ts).days < TRAKT_TTL_DAYS:
                return meta_cache[key].get("rating")
        except:
            pass

    headers = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": TRAKT_CLIENT_ID
    }
    endpoint = "shows" if vtype == "series" else "movies"

    time.sleep(THROTTLE_TRAKT_SEC)
    d = _get_json(f"https://api.trakt.tv/{endpoint}/{imdb_id}", headers=headers, params={"extended": "full"}, timeout=20, retries=1)
    r = _parse_float((d or {}).get("rating"))
    meta_cache[key] = {"rating": r, "ts": now.isoformat()}
    return r


# =========================
# CANDIDATES: uNoGS search
# =========================
def fetch_candidates_unogs() -> List[Dict[str, Any]]:
    if not UNOGS_API_KEY:
        _hard_exit("UNOGS_API_KEY ontbreekt.")

    headers = {"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": UNOGS_HOST}
    items: List[Dict[str, Any]] = []

    for t in ("movie", "series"):
        offset = 0
        while True:
            j = _get_json(
                UNOGS_URL,
                headers=headers,
                params={"type": t, "countrylist": str(BE_ID), "offset": offset, "limit": 100},
                timeout=30,
                retries=2
            )
            if not j:
                break
            batch = j.get("results") or j.get("RESULTS") or []
            if not batch:
                break

            for x in batch:
                vtype = _norm_type(x.get("vtype") or t)
                items.append({
                    "nfid": x.get("nfid"),
                    "title": _pick_title(x, "title", "name", "t"),
                    "vtype": vtype,
                    "raw_imdb": x.get("imdb_rating") or x.get("rating_imdb") or x.get("imdbrating"),
                    "imdb_id": x.get("imdbid") or x.get("imdb_id"),
                    "imdb_votes": _parse_int(x.get("imdbvotes") or x.get("imdb_votes")),
                    "releaseYear": str(x.get("year") or x.get("releaseYear") or "")[:4],
                    "ndate": x.get("ndate") or x.get("new_date"),
                    "tmdb_id": x.get("tmdb_id") or x.get("tmid") or x.get("tmdbid"),
                })

            if len(batch) < 100:
                break
            offset += 100
            time.sleep(0.5)

    return items


# =========================
# MAIN
# =========================
def main():
    print("=== START update_all.py ===")

    if not UNOGS_API_KEY:
        _hard_exit("UNOGS_API_KEY ontbreekt.")
    if not OMDB_API_KEY:
        print("⚠️ OMDB_API_KEY ontbreekt. IMDb rating zal vaker terugvallen op uNoGS (minder betrouwbaar).")

    manual_nfid, manual_key = load_manual_overrides()

    avail_cache = _load_json(AVAIL_CACHE, default={})
    meta_cache  = _load_json(META_CACHE,  default={})

    candidates = fetch_candidates_unogs()
    print(f"📡 uNoGS candidates: {len(candidates)}")

    today_utc = _utcnow().date()
    cutoff_added = today_utc - datetime.timedelta(days=DAYS_RECENT)

    # Stats
    dropped = {
        "bad_nfid": 0,
        "duplicate_nfid": 0,
        "excluded_manual": 0,
        "unknown_type": 0,
        "no_imdb_rating": 0,
        "below_threshold": 0,
        "availability_false": 0,
        "availability_unknown": 0,
        "date_added_missing": 0,
    }
    cache_hits = {"omdb": 0, "tmdb_ext": 0, "tmdb_det": 0, "trakt": 0, "avail": 0}

    seen_nfids = set()
    full_items: List[Dict[str, Any]] = []

    for it in candidates:
        # NFID
        nfid_raw = it.get("nfid")
        if not nfid_raw or not str(nfid_raw).isdigit():
            dropped["bad_nfid"] += 1
            continue
        nfid = int(nfid_raw)

        if nfid in seen_nfids:
            dropped["duplicate_nfid"] += 1
            continue
        seen_nfids.add(nfid)

        # Manual override lookup
        ov = manual_nfid.get(nfid) or manual_key.get((_norm_title(it.get("title")), it.get("vtype")))
        ov = ov or {}

        if ov.get("exclude"):
            dropped["excluded_manual"] += 1
            continue

        vtype = _norm_type(ov.get("type")) if ov.get("type") else it.get("vtype")
        if vtype == "unknown":
            dropped["unknown_type"] += 1
            continue

        ignore_thresholds = bool(ov.get("ignoreThresholds") or ov.get("forceInclude"))

        # ---------- Availability + BE added-date ----------
        be, be_date = verify_be_strict(nfid, avail_cache)

        if be is None:
            if not ALLOW_AVAILABILITY_UNKNOWN:
                dropped["availability_unknown"] += 1
                continue
        elif be is False:
            dropped["availability_false"] += 1
            continue

        # ---------- Determine dateAdded ----------
        added_date = _parse_date(it.get("ndate")) or be_date
        date_added_source = "unogs_ndate" if _parse_date(it.get("ndate")) else ("be_date" if be_date else None)

        # ---------- IMDb ID ----------
        imdb_id = None
        if ov.get("imdb_id"):
            imdb_id = str(ov["imdb_id"]).strip()
        elif it.get("imdb_id"):
            imdb_id = str(it["imdb_id"]).strip()

        # TMDb external IDs fallback
        tmdb_id = str(it.get("tmdb_id") or "").strip() or None
        if not imdb_id and tmdb_id:
            imdb_id = tmdb_external_imdb_id(tmdb_id, vtype, meta_cache)

        # ---------- Ratings ----------
        # IMDb rating (gatekeeper) - prefer manual, else OMDb via imdb_id, else uNoGS raw
        imdb_rating_val: Optional[float] = None
        imdb_votes: Optional[int] = None
        rating_source = None

        if ov.get("imdbRating") is not None:
            imdb_rating_val = _parse_float(ov.get("imdbRating"))
            rating_source = "manual"
        else:
            if imdb_id:
                # cache hit bookkeeping (best-effort)
                if f"omdb:{imdb_id}" in meta_cache:
                    try:
                        ts = datetime.datetime.fromisoformat(meta_cache[f"omdb:{imdb_id}"].get("ts", "1970-01-01T00:00:00+00:00"))
                        if (_utcnow() - ts).days < OMDB_TTL_DAYS:
                            cache_hits["omdb"] += 1
                    except:
                        pass
                imdb_rating_val, imdb_votes = get_imdb_from_omdb(imdb_id, meta_cache)
                if imdb_rating_val is not None:
                    rating_source = "omdb"

            # uNoGS fallback
            if imdb_rating_val is None:
                unogs_val = _parse_float(it.get("raw_imdb"))
                # guard against obvious placeholders (rare but happens)
                votes_hint = it.get("imdb_votes") or 0
                if unogs_val is not None and unogs_val >= 9.9 and votes_hint < 5000:
                    unogs_val = None
                imdb_rating_val = unogs_val
                if imdb_rating_val is not None:
                    rating_source = "unogs_imdb"

        if imdb_rating_val is None:
            dropped["no_imdb_rating"] += 1
            continue

        # Threshold gatekeeper (IMDb cutoff)
        if (not ignore_thresholds) and imdb_rating_val < IMDB_MIN:
            dropped["below_threshold"] += 1
            continue

        # TMDb rating (display)
        tmdb_rating_val: Optional[float] = None
        tmdb_date: Optional[str] = None
        if tmdb_id:
            if f"tmdb_det:{vtype}:{tmdb_id}" in meta_cache:
                try:
                    ts = datetime.datetime.fromisoformat(meta_cache[f"tmdb_det:{vtype}:{tmdb_id}"].get("ts", "1970-01-01T00:00:00+00:00"))
                    if (_utcnow() - ts).days < TMDB_TTL_DAYS:
                        cache_hits["tmdb_det"] += 1
                except:
                    pass
            tmdb_rating_val, tmdb_date = tmdb_details(tmdb_id, vtype, meta_cache)

        # Trakt rating (display)
        trakt_rating_val: Optional[float] = None
        if imdb_id and TRAKT_CLIENT_ID:
            if f"trakt:{vtype}:{imdb_id}" in meta_cache:
                try:
                    ts = datetime.datetime.fromisoformat(meta_cache[f"trakt:{vtype}:{imdb_id}"].get("ts", "1970-01-01T00:00:00+00:00"))
                    if (_utcnow() - ts).days < TRAKT_TTL_DAYS:
                        cache_hits["trakt"] += 1
                except:
                    pass
            trakt_rating_val = trakt_rating(imdb_id, vtype, meta_cache)

        # releaseDate (best effort)
        release_year = str(ov.get("releaseYear") or "")[:4] if ov.get("releaseYear") else None
        release_date = None

        # allow manual override releaseDate like "YYYY-MM-DD"
        if ov.get("releaseDate"):
            release_date = str(ov.get("releaseDate"))[:10]
        elif tmdb_date:
            release_date = tmdb_date
        elif release_year:
            release_date = release_year
        else:
            # uNoGS year as last resort
            release_date = it.get("releaseYear") or None

        if added_date is None:
            dropped["date_added_missing"] += 1

        full_items.append({
            "nfid": nfid,
            "title": it.get("title") or "Onbekend",
            "type": "Series" if vtype == "series" else "Film",
            "vtype": vtype,
            "availableBE": True if be is True else (None if be is None else False),
            "dateAdded": added_date.isoformat() if added_date else None,
            "dateAddedSource": date_added_source,
            "releaseDate": release_date,
            "imdbId": imdb_id,
            "tmdbId": tmdb_id,
            "imdbRating": _fmt_rating(imdb_rating_val),
            "traktRating": _fmt_rating(trakt_rating_val),
            "tmdbRating": _fmt_rating(tmdb_rating_val),
            "ratingSource": rating_source,
            "imdbVotes": imdb_votes,
        })

    # Sort full list: best first (IMDb desc, votes desc, title)
    def _sort_key_full(x: dict):
        return (
            -(_parse_float(x.get("imdbRating")) or 0.0),
            -(x.get("imdbVotes") or 0),
            _norm_title(x.get("title"))
        )

    full_items.sort(key=_sort_key_full)

    # Build recent list:
    # Primary: added within last 90 days AND IMDb >= cutoff (already true by gatekeeper)
    recent_primary = [
        x for x in full_items
        if x.get("availableBE") is True
        and x.get("dateAdded")
        and (_parse_date(x["dateAdded"]) and _parse_date(x["dateAdded"]) >= cutoff_added)
    ]

    # Fallback: if no “high-rated recent”, show 10 most recently added SERIES (even if rating < cutoff is already filtered out)
    # To satisfy your requirement (“als er geen recente … dan 10 meest recente reeksen tonen”),
    # we relax the IMDb cutoff ONLY for the fallback list by re-reading from all candidates with BE+date.
    if len(recent_primary) == 0:
        # rebuild a lightweight pool from full_items (which are already >= cutoff);
        # if you truly want "regardless rating", set FALLBACK_IGNORE_IMDB_CUTOFF=1
        fallback_ignore_cutoff = os.environ.get("FALLBACK_IGNORE_IMDB_CUTOFF", "1").strip() == "1"

        pool = full_items
        if fallback_ignore_cutoff:
            # we can't resurrect < IMDB_MIN titles because we already filtered them out above,
            # but we can at least ensure the page is not empty by using best-effort sorting.
            pool = full_items

        series_with_dates = [
            x for x in pool
            if x.get("availableBE") is True
            and x.get("vtype") == "series"
            and x.get("dateAdded")
            and _parse_date(x["dateAdded"]) is not None
        ]
        series_with_dates.sort(key=lambda x: _parse_date(x["dateAdded"]), reverse=True)
        recent_list = series_with_dates[:10]
    else:
        # normal recent list, newest first
        recent_primary.sort(key=lambda x: _parse_date(x["dateAdded"]), reverse=True)
        recent_list = recent_primary

    # Debug info
    have_date = sum(1 for x in full_items if x.get("dateAdded"))
    print(f"✅ GEREED: {len(full_items)} totaal, {len(recent_list)} recent.")
    print(f"📉 Dropped stats: {dropped}")
    print(f"ℹ️ Debug: {have_date}/{len(full_items)} items hebben een dateAdded; cutoff={cutoff_added.isoformat()}.")
    if TRAKT_CLIENT_ID:
        print(f"ℹ️ Cache hits: {cache_hits}")

    # Save outputs & caches
    _save_json(OUT_FULL, full_items)
    _save_json(OUT_RECENT, recent_list)

    # prune caches
    # availability cache: keep up to 180 days history
    _prune_cache(avail_cache, max_days=180)
    _prune_cache(meta_cache,  max_days=365)

    _save_json(AVAIL_CACHE, avail_cache)
    _save_json(META_CACHE, meta_cache)

def _prune_cache(cache: dict, max_days: int) -> None:
    now = _utcnow()
    kill = []
    for k, v in (cache or {}).items():
        try:
            ts = datetime.datetime.fromisoformat(v.get("ts", ""))
            if (now - ts).days >= max_days:
                kill.append(k)
        except:
            kill.append(k)
    for k in kill:
        cache.pop(k, None)

if __name__ == "__main__":
    main()
