#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
update_all.py — Netflix België lijst bouwen met:
- uNoGS: volledige BE-catalogus (nfid, type, (soms) ndate, (soms) imdb_id, (soms) imdb_rating)
- uNoGS /titlecountries: harde BE-availability + be_date (nieuwe datum voor BE)  ✅ vult dateAdded
- OMDb: “gold” IMDb rating via imdb_id (tt....)  ✅ correcte rating
- TMDb: alleen fallback om imdb_id te vinden als uNoGS geen imdb_id heeft

Belangrijk:
- "recent" wordt bepaald op basis van dateAdded (ndate of be_date).
- Availability cache heeft asymmetrische TTL: True kort (1d), False lang (7d).
- Manual overrides via manual_scores.json (NFID of (title,type) key) met exclude/forceInclude/ignoreThresholds.
"""

import os, json, datetime, time, re, html, random
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

import requests

# ----------------------------
# CONFIG & PATHS
# ----------------------------
ROOT = Path(__file__).parent.resolve()
OUT_FULL = ROOT / "netflix_data.json"
OUT_RECENT = ROOT / "netflix_last_month.json"

MANUAL_SCORES = ROOT / "manual_scores.json"
AVAIL_CACHE = ROOT / "availability_cache.json"
IMDB_CACHE = ROOT / "imdb_cache.json"

DAYS_RECENT = 90
BE_ID = 21

# Rating thresholds
IMDB_MIN = 7.7
TMDB_MIN = 7.0  # enkel relevant als je ooit echt tmdb-score gebruikt (hier niet)

# Availability TTL (asymmetrisch)
TTL_TRUE_DAYS = 1
TTL_FALSE_DAYS = 7

# IMDb cache TTL
IMDB_TTL_DAYS = 30

# API keys (GitHub secrets / env)
UNOGS_API_KEY = os.environ.get("UNOGS_API_KEY", "").strip()
TMDB_API_KEY  = os.environ.get("TMDB_API_KEY", "").strip()
OMDB_API_KEY  = os.environ.get("OMDB_API_KEY", "").strip()

UNOGS_HOST = "unogsng.p.rapidapi.com"
UNOGS_SEARCH_URL = f"https://{UNOGS_HOST}/search"
UNOGS_TITLECOUNTRIES_URL = f"https://{UNOGS_HOST}/titlecountries"

SESSION = requests.Session()


# ----------------------------
# LOW-LEVEL HELPERS
# ----------------------------
def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)

def _today_utc() -> datetime.date:
    return _utcnow().date()

def _norm_title(s: Any) -> str:
    s = html.unescape(str(s or ""))
    return " ".join(s.strip().lower().split()).replace("'", "").replace("’", "").replace('"', "")

def _norm_type(t: Any) -> str:
    t = str(t or "").strip().lower()
    if t in ("serie", "series", "tv", "show", "tvseries", "tv series"):
        return "series"
    if t in ("film", "movie", "movies"):
        return "movie"
    return "unknown"

def _parse_rating(x: Any) -> Optional[float]:
    if x in (None, "", "no score", "N/A", "nan", "None", 0, "0"):
        return None
    try:
        s = str(x).strip().replace(",", ".")
        m = re.search(r"(\d+(?:\.\d+)?)", s)
        if not m:
            return None
        v = float(m.group(1))
        return round(v, 1) if (0 < v <= 10.0) else None
    except:
        return None

def _parse_int(x: Any) -> Optional[int]:
    if x in (None, "", "N/A", "None"):
        return None
    try:
        return int(float(str(x).replace(",", "").strip()))
    except:
        return None

def _parse_date(x: Any) -> Optional[datetime.date]:
    """Ondersteunt: epoch (ms/s), ISO, YYYYMMDD, 'YYYY-MM-DD ...'."""
    if not x:
        return None
    try:
        s = str(x).strip()
        # epoch?
        if s.isdigit():
            n = int(s)
            if n > 10_000_000_000:  # ms -> s
                n //= 1000
            return datetime.datetime.fromtimestamp(n, tz=datetime.timezone.utc).date()
        # YYYYMMDD?
        if len(s) == 8 and s.isdigit():
            return datetime.date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        # ISO-ish
        s10 = s[:10].replace("/", "-")
        return datetime.date.fromisoformat(s10)
    except:
        return None

def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except:
        return {}

def _save_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _get_json(
    url: str,
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
    retries: int = 2,
    timeout: int = 15,
    jitter: float = 0.15,
) -> Optional[dict]:
    """
    Requests helper met:
    - retry bij 429 + 5xx
    - respecteert Retry-After bij 429
    """
    for i in range(retries + 1):
        try:
            r = SESSION.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code == 429 and i < retries:
                ra = r.headers.get("Retry-After")
                wait = float(ra) if ra and str(ra).isdigit() else (2.0 * (i + 1))
                time.sleep(wait + random.random() * jitter)
                continue
            if r.status_code in (500, 502, 503, 504) and i < retries:
                time.sleep(1.5 * (i + 1) + random.random() * jitter)
                continue
            if r.status_code != 200:
                return None
            return r.json()
        except:
            if i < retries:
                time.sleep(1.2 * (i + 1) + random.random() * jitter)
                continue
    return None


# ----------------------------
# MANUAL OVERRIDES
# ----------------------------
def load_manual_overrides() -> Tuple[Dict[int, dict], Dict[Tuple[str, str], dict]]:
    """
    manual_scores.json ondersteunt:
      - nfid-based rows: {"nfid": 70123456, "exclude": true, "imdbRating": 8.4, "type": "series", ...}
      - title+type fallback: {"title": "...", "type": "movie", ...}
    """
    by_nfid: Dict[int, dict] = {}
    by_key: Dict[Tuple[str, str], dict] = {}

    if not MANUAL_SCORES.exists():
        return by_nfid, by_key

    try:
        # simple trailing comma fix
        raw_txt = re.sub(r",\s*(\]|\})", r"\1", MANUAL_SCORES.read_text(encoding="utf-8"))
        raw = json.loads(raw_txt)
        rows = raw if isinstance(raw, list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if "type" in row:
                row["type"] = _norm_type(row.get("type"))
            if "nfid" in row and str(row["nfid"]).isdigit():
                by_nfid[int(row["nfid"])] = row
            elif row.get("title") and row.get("type") in ("movie", "series"):
                by_key[(_norm_title(row["title"]), row["type"])] = row
    except Exception as e:
        print(f"⚠️ manual_scores.json error: {e}")

    return by_nfid, by_key


# ----------------------------
# CACHES
# ----------------------------
def save_cache_pruned(path: Path, data: dict, max_days: int = 180) -> None:
    now = _utcnow()
    out = {}
    for k, v in (data or {}).items():
        try:
            ts = datetime.datetime.fromisoformat((v or {}).get("ts", ""))
            if (now - ts).days < max_days:
                out[k] = v
        except:
            # drop broken records
            pass
    _save_json(path, out)


# ----------------------------
# uNoGS: FETCH CANDIDATES
# ----------------------------
def fetch_unogs_candidates() -> List[Dict[str, Any]]:
    """Volledige BE-catalogus via uNoGS /search (movie + series)."""
    headers = {"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": UNOGS_HOST}
    items: List[Dict[str, Any]] = []

    for vtype in ("movie", "series"):
        offset = 0
        while True:
            params = {"type": vtype, "countrylist": str(BE_ID), "offset": offset, "limit": 100}
            j = _get_json(UNOGS_SEARCH_URL, headers=headers, params=params, timeout=30)
            if not j:
                break
            batch = j.get("results") or j.get("RESULTS") or []
            if not batch:
                break

            for x in batch:
                title = x.get("title") or x.get("name") or x.get("t") or "Onbekend"
                items.append({
                    "nfid": x.get("nfid"),
                    "title": html.unescape(str(title)),
                    "vtype": _norm_type(x.get("vtype") or vtype),
                    "raw_imdb": x.get("imdb_rating") or x.get("rating_imdb") or x.get("imdbrating"),
                    "imdb_id": x.get("imdbid") or x.get("imdb_id"),
                    "imdb_votes": _parse_int(x.get("imdbvotes") or x.get("imdb_votes")),
                    "releaseYear": str(x.get("year") or x.get("releaseYear") or "")[:4],
                    "ndate": x.get("ndate") or x.get("new_date"),
                    "tmdb_id": x.get("tmdb_id") or x.get("tmid"),
                })

            if len(batch) < 100:
                break
            offset += 100
            time.sleep(0.3)

    return items


# ----------------------------
# uNoGS: AVAILABILITY + be_date
# ----------------------------
def verify_be_strict(nfid: int, cache: dict) -> Tuple[Optional[bool], Optional[datetime.date]]:
    """
    Returns: (in_be, be_date)
      - in_be True/False/None (None = network hiccup)
      - be_date = parse(new_date/ndate) for BE row if available
    TTL:
      - True cached for 1 day
      - False cached for 7 days
    """
    key = str(nfid)
    now = _utcnow()

    if key in cache:
        try:
            ts = datetime.datetime.fromisoformat(cache[key]["ts"])
            cached_be = cache[key].get("be")
            ttl = TTL_TRUE_DAYS if cached_be is True else TTL_FALSE_DAYS
            if (now - ts).days < ttl:
                bd = _parse_date(cache[key].get("be_date"))
                return cached_be, bd
        except:
            pass

    headers = {"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": UNOGS_HOST}
    res = _get_json(UNOGS_TITLECOUNTRIES_URL, headers=headers, params={"netflixid": nfid}, timeout=15)
    if res is None:
        return None, None

    results = res.get("results") or res.get("RESULTS") or res.get("Countries") or []
    in_be, be_date = False, None

    if isinstance(results, list):
        for c in results:
            cid = c.get("id") or c.get("countryid") or c.get("country_id")
            cc = c.get("cc") or c.get("countrycode") or c.get("country_code")
            if (cid and str(cid) == str(BE_ID)) or (cc and str(cc).upper() == "BE"):
                in_be = True
                be_date = _parse_date(c.get("new_date") or c.get("ndate") or c.get("date"))
                break

    cache[key] = {
        "be": in_be,
        "be_date": be_date.isoformat() if be_date else None,
        "ts": now.isoformat(),
    }
    return in_be, be_date


# ----------------------------
# IMDb rating via OMDb (gold)
# ----------------------------
def get_imdb_from_omdb(imdb_id: str, cache: dict) -> Optional[float]:
    """Direct via tt-id: OMDb is bron voor IMDb rating."""
    if not (OMDB_API_KEY and imdb_id):
        return None

    imdb_id = str(imdb_id).strip()
    if not imdb_id.startswith("tt"):
        # OMDb expects tt..., so reject
        return None

    key = f"imdb:{imdb_id}"
    now = _utcnow()

    if key in cache:
        try:
            ts = datetime.datetime.fromisoformat(cache[key]["ts"])
            if (now - ts).days < IMDB_TTL_DAYS:
                return cache[key].get("imdb")
        except:
            pass

    j = _get_json("https://www.omdbapi.com/", params={"i": imdb_id, "apikey": OMDB_API_KEY}, timeout=15)
    imdb = _parse_rating((j or {}).get("imdbRating"))

    cache[key] = {"imdb": imdb, "ts": now.isoformat()}
    return imdb


def get_imdb_id_via_tmdb(tmdb_id: str, vtype: str) -> Optional[str]:
    """TMDb external_ids -> imdb_id (tt...)."""
    if not (TMDB_API_KEY and tmdb_id):
        return None

    cat = "tv" if vtype == "series" else "movie"
    j = _get_json(
        f"https://api.themoviedb.org/3/{cat}/{tmdb_id}/external_ids",
        params={"api_key": TMDB_API_KEY},
        timeout=15,
    )
    imdb_id = (j or {}).get("imdb_id")
    return imdb_id if imdb_id else None


# ----------------------------
# MAIN
# ----------------------------
def main():
    if not UNOGS_API_KEY:
        print("❌ UNOGS_API_KEY ontbreekt (GitHub Secret). Abort.")
        return

    manual_nfid, manual_key = load_manual_overrides()
    avail_cache = _load_json(AVAIL_CACHE)
    imdb_cache = _load_json(IMDB_CACHE)

    cutoff = _today_utc() - datetime.timedelta(days=DAYS_RECENT)

    candidates = fetch_unogs_candidates()
    print(f"📡 uNoGS candidates: {len(candidates)}")

    dropped = {
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

    seen_nfids = set()
    final_list: List[dict] = []

    for it in candidates:
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
        ov = ov or {}  # ✅ prevents NoneType.get crashes everywhere

        if ov.get("exclude"):
            dropped["excluded_manual"] += 1
            continue

        vtype = _norm_type(ov.get("type")) if ov.get("type") else _norm_type(it.get("vtype"))
        if vtype == "unknown":
            dropped["unknown_type"] += 1
            continue

        ignore_thresholds = bool(ov.get("ignoreThresholds") or ov.get("forceInclude"))

        # ------------------
        # DateAdded logic (ndate or BE be_date)
        # ------------------
        added_date = _parse_date(it.get("ndate"))
        # We'll fetch availability anyway for accepted items, and use be_date as fallback.

        # ------------------
        # Rating logic (priority: manual -> OMDb via imdb_id -> OMDb via TMDb -> uNoGS)
        # ------------------
        rating: Optional[float] = None
        source: Optional[str] = None

        # manual rating override
        if ov.get("imdbRating") is not None:
            rating = _parse_rating(ov.get("imdbRating"))
            source = "manual"

        # OMDb via direct imdb_id from uNoGS (gold)
        if rating is None and it.get("imdb_id"):
            r = get_imdb_from_omdb(str(it.get("imdb_id")), imdb_cache)
            if r is not None:
                rating, source = r, "imdb"

        # OMDb via TMDb external ids -> imdb_id
        if rating is None and it.get("tmdb_id") and TMDB_API_KEY:
            imdb_id = get_imdb_id_via_tmdb(str(it["tmdb_id"]), vtype)
            if imdb_id:
                r = get_imdb_from_omdb(imdb_id, imdb_cache)
                if r is not None:
                    rating, source = r, "imdb"

        # uNoGS rating fallback (with sanity)
        unogs_rating = _parse_rating(it.get("raw_imdb"))
        if rating is None and unogs_rating is not None:
            votes = it.get("imdb_votes") or 0
            # Drop suspicious "10.0" placeholders unless votes are meaningful
            if unogs_rating >= 9.9 and votes < 5000:
                dropped["unogs_placeholder_10"] += 1
            else:
                rating, source = unogs_rating, "unogs_imdb"

        if rating is None:
            dropped["no_rating"] += 1
            continue

        # Release year (manual override if present)
        release_year = (ov.get("releaseYear") or ov.get("releaseDate") or it.get("releaseYear") or "")  # ✅ ov is dict
        release_year = str(release_year)[:4] if release_year else ""

        # Thresholds
        if not ignore_thresholds:
            if source in ("imdb", "unogs_imdb", "manual") and rating < IMDB_MIN:
                dropped["below_threshold"] += 1
                continue

        # ------------------
        # Availability + be_date (also fixes missing dateAdded)
        # ------------------
        in_be, be_date = verify_be_strict(nfid, avail_cache)

        # Fill added_date from be_date if missing
        if added_date is None and be_date is not None:
            added_date = be_date

        is_recent = bool(added_date and added_date >= cutoff)

        if is_recent:
            # strict: must be True (None = hiccup => drop)
            if in_be is not True:
                dropped["availability_unknown_recent" if in_be is None else "availability_false_recent"] += 1
                continue
        else:
            # full list: drop only if explicitly False
            if in_be is False:
                dropped["availability_false_full"] += 1
                continue
            if in_be is None:
                # keep in full list (lenient), but track
                dropped["availability_unknown_full"] += 1

        final_list.append({
            "nfid": nfid,
            "title": it.get("title") or "Onbekend",
            "type": "Series" if vtype == "series" else "Film",
            "imdbRating": rating,
            "ratingSource": source,
            "releaseDate": release_year,
            "dateAdded": added_date.isoformat() if added_date else None,
        })

        # small throttle to be gentle on RapidAPI (esp. titlecountries)
        time.sleep(0.03)

    # Save outputs
    _save_json(OUT_FULL, final_list)

    recent_list = [x for x in final_list if x.get("dateAdded") and datetime.date.fromisoformat(x["dateAdded"]) >= cutoff]
    _save_json(OUT_RECENT, recent_list)

    # Save caches
    save_cache_pruned(AVAIL_CACHE, avail_cache, max_days=180)
    save_cache_pruned(IMDB_CACHE, imdb_cache, max_days=365)

    # Debug summary
    with_date = sum(1 for x in final_list if x.get("dateAdded"))
    print(f"✅ GEREED: {len(final_list)} totaal, {len(recent_list)} recent.")
    print(f"📉 Dropped stats: {dropped}")
    print(f"ℹ️ Debug: {with_date}/{len(final_list)} items hebben een dateAdded; cutoff={cutoff.isoformat()}.")


if __name__ == "__main__":
    main()
