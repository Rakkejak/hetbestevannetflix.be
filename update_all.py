#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, datetime, time, re, html, random
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import requests

# --- PATHS ---
ROOT = Path(__file__).parent.resolve()
OUT_FULL   = ROOT / "netflix_data.json"
OUT_RECENT = ROOT / "netflix_last_month.json"

MANUAL_SCORES = ROOT / "manual_scores.json"

AVAIL_CACHE = ROOT / "availability_cache.json"
IMDB_CACHE  = ROOT / "imdb_cache.json"
TMDB_CACHE  = ROOT / "tmdb_rating_cache.json"
TRAKT_CACHE = ROOT / "trakt_rating_cache.json"

# --- LOGIC ---
DAYS_RECENT = 90
BE_ID = 21

IMDB_MIN = 7.8  # <-- jouw cutoff

# Availability TTL: True snel verversen (24u), False langer (7d)
TTL_TRUE_DAYS  = 1
TTL_FALSE_DAYS = 7

IMDB_TTL_DAYS  = 30
TMDB_TTL_DAYS  = 30
TRAKT_TTL_DAYS = 30

# --- API KEYS ---
UNOGS_API_KEY = os.environ.get("UNOGS_API_KEY", "").strip()
TMDB_API_KEY  = os.environ.get("TMDB_API_KEY", "").strip()
OMDB_API_KEY  = os.environ.get("OMDB_API_KEY", "").strip()
TRAKT_CLIENT_ID = os.environ.get("TRAKT_CLIENT_ID", "").strip()  # optioneel

UNOGS_HOST = "unogsng.p.rapidapi.com"
UNOGS_SEARCH_URL = f"https://{UNOGS_HOST}/search"
UNOGS_TITLECOUNTRIES_URL = f"https://{UNOGS_HOST}/titlecountries"

SESSION = requests.Session()

# ----------------------------
# Generic helpers
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
    if not x:
        return None
    try:
        s = str(x).strip()
        if s.isdigit():
            n = int(s)
            if n > 10_000_000_000:
                n //= 1000
            return datetime.datetime.fromtimestamp(n, tz=datetime.timezone.utc).date()
        if len(s) == 8 and s.isdigit():
            return datetime.date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        return datetime.date.fromisoformat(s[:10].replace("/", "-"))
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

def _get_json(url: str, headers: dict = None, params: dict = None, retries: int = 2, timeout: int = 15) -> Optional[dict]:
    for i in range(retries + 1):
        try:
            r = SESSION.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code == 429 and i < retries:
                ra = r.headers.get("Retry-After")
                wait = float(ra) if ra and str(ra).isdigit() else (2.0 * (i + 1))
                time.sleep(wait + random.random() * 0.2)
                continue
            if r.status_code in (500, 502, 503, 504) and i < retries:
                time.sleep(1.5 * (i + 1) + random.random() * 0.2)
                continue
            if r.status_code != 200:
                return None
            return r.json()
        except:
            if i < retries:
                time.sleep(1.2 * (i + 1) + random.random() * 0.2)
                continue
    return None

def _cache_get(cache: dict, key: str, ttl_days: int) -> Any:
    if key not in cache:
        return None
    try:
        ts = datetime.datetime.fromisoformat(cache[key]["ts"])
        if (_utcnow() - ts).days < ttl_days:
            return cache[key].get("val")
    except:
        return None
    return None

def _cache_set(cache: dict, key: str, val: Any) -> None:
    cache[key] = {"val": val, "ts": _utcnow().isoformat()}

def _save_cache_pruned(path: Path, cache: dict, max_days: int = 180) -> None:
    now = _utcnow()
    pruned = {}
    for k, v in (cache or {}).items():
        try:
            ts = datetime.datetime.fromisoformat((v or {}).get("ts", ""))
            if (now - ts).days < max_days:
                pruned[k] = v
        except:
            pass
    _save_json(path, pruned)

# ----------------------------
# Manual overrides
# ----------------------------
def load_manual_overrides() -> Tuple[Dict[int, dict], Dict[Tuple[str, str], dict]]:
    by_nfid, by_key = {}, {}
    if not MANUAL_SCORES.exists():
        return by_nfid, by_key
    try:
        raw_txt = re.sub(r",\s*(\]|\})", r"\1", MANUAL_SCORES.read_text(encoding="utf-8"))
        raw = json.loads(raw_txt)
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
        print(f"⚠️ manual_scores error: {e}")
    return by_nfid, by_key

# ----------------------------
# Previous output persistence (dateAdded fix)
# ----------------------------
def load_previous_dates() -> Dict[int, str]:
    prev: Dict[int, str] = {}
    if not OUT_FULL.exists():
        return prev
    try:
        data = json.loads(OUT_FULL.read_text(encoding="utf-8"))
        if isinstance(data, list):
            for row in data:
                try:
                    nfid = int(row.get("nfid"))
                    da = row.get("dateAdded")
                    if da:
                        prev[nfid] = str(da)[:10]
                except:
                    continue
    except:
        pass
    return prev

# ----------------------------
# uNoGS fetch
# ----------------------------
def fetch_unogs_candidates() -> List[Dict[str, Any]]:
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
# Availability + be_date
# ----------------------------
def verify_be_strict(nfid: int, cache: dict) -> Tuple[Optional[bool], Optional[datetime.date]]:
    key = str(nfid)
    now = _utcnow()

    if key in cache:
        try:
            ts = datetime.datetime.fromisoformat(cache[key]["ts"])
            c_be = cache[key].get("be")
            ttl = TTL_TRUE_DAYS if c_be is True else TTL_FALSE_DAYS
            if (now - ts).days < ttl:
                return c_be, _parse_date(cache[key].get("be_date"))
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
            cc  = c.get("cc") or c.get("countrycode") or c.get("country_code")
            if (cid and str(cid) == str(BE_ID)) or (cc and str(cc).upper() == "BE"):
                in_be = True
                be_date = _parse_date(
                    c.get("new_date") or c.get("newDate") or c.get("ndate") or c.get("date") or c.get("added_date")
                )
                break

    cache[key] = {"be": in_be, "be_date": be_date.isoformat() if be_date else None, "ts": now.isoformat()}
    return in_be, be_date

# ----------------------------
# Ratings
# ----------------------------
def get_imdb_from_omdb(imdb_id: str, cache: dict) -> Optional[float]:
    if not (OMDB_API_KEY and imdb_id):
        return None
    imdb_id = str(imdb_id).strip()
    if not imdb_id.startswith("tt"):
        return None

    key = f"imdb:{imdb_id}"
    cached = _cache_get(cache, key, IMDB_TTL_DAYS)
    if cached is not None:
        return cached

    j = _get_json("https://www.omdbapi.com/", params={"i": imdb_id, "apikey": OMDB_API_KEY}, timeout=15)
    imdb = _parse_rating((j or {}).get("imdbRating"))
    _cache_set(cache, key, imdb)
    return imdb

def get_imdb_id_via_tmdb(tmdb_id: str, vtype: str) -> Optional[str]:
    if not (TMDB_API_KEY and tmdb_id):
        return None
    cat = "tv" if vtype == "series" else "movie"
    j = _get_json(f"https://api.themoviedb.org/3/{cat}/{tmdb_id}/external_ids", params={"api_key": TMDB_API_KEY}, timeout=15)
    return (j or {}).get("imdb_id") or None

def get_tmdb_rating(tmdb_id: str, vtype: str, cache: dict) -> Optional[float]:
    """TMDb vote_average (0..10)."""
    if not (TMDB_API_KEY and tmdb_id):
        return None
    key = f"tmdb:{vtype}:{tmdb_id}"
    cached = _cache_get(cache, key, TMDB_TTL_DAYS)
    if cached is not None:
        return cached

    cat = "tv" if vtype == "series" else "movie"
    j = _get_json(f"https://api.themoviedb.org/3/{cat}/{tmdb_id}", params={"api_key": TMDB_API_KEY}, timeout=15)
    val = _parse_rating((j or {}).get("vote_average"))
    _cache_set(cache, key, val)
    return val

def get_trakt_rating(imdb_id: str, vtype: str, cache: dict) -> Optional[float]:
    """Trakt rating (0..10). Optioneel: vereist TRAKT_CLIENT_ID."""
    if not (TRAKT_CLIENT_ID and imdb_id and str(imdb_id).startswith("tt")):
        return None

    imdb_id = str(imdb_id).strip()
    key = f"trakt:{vtype}:{imdb_id}"
    cached = _cache_get(cache, key, TRAKT_TTL_DAYS)
    if cached is not None:
        return cached

    base = "https://api.trakt.tv"
    headers = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": TRAKT_CLIENT_ID,
    }

    # 1) zoek trakt id via imdb
    ttype = "show" if vtype == "series" else "movie"
    sr = _get_json(f"{base}/search/imdb/{imdb_id}", headers=headers, params={"type": ttype}, timeout=15)
    trakt_id = None

    if isinstance(sr, list) and sr:
        obj = sr[0].get(ttype) or {}
        ids = obj.get("ids") or {}
        trakt_id = ids.get("trakt")

    if not trakt_id:
        _cache_set(cache, key, None)
        return None

    # 2) haal details met rating
    details = _get_json(f"{base}/{ttype}s/{trakt_id}", headers=headers, params={"extended": "full"}, timeout=15)
    rating = _parse_rating((details or {}).get("rating"))
    _cache_set(cache, key, rating)
    return rating

# ----------------------------
# MAIN
# ----------------------------
def main():
    if not UNOGS_API_KEY:
        print("❌ UNOGS_API_KEY ontbreekt. Abort.")
        return

    manual_nfid, manual_key = load_manual_overrides()
    prev_dates = load_previous_dates()

    avail_cache = _load_json(AVAIL_CACHE)
    imdb_cache  = _load_json(IMDB_CACHE)
    tmdb_cache  = _load_json(TMDB_CACHE)
    trakt_cache = _load_json(TRAKT_CACHE)

    cutoff = _today_utc() - datetime.timedelta(days=DAYS_RECENT)
    today  = _today_utc()

    candidates = fetch_unogs_candidates()
    print(f"📡 uNoGS candidates: {len(candidates)}")

    dropped = {
        "bad_nfid": 0,
        "duplicate_nfid": 0,
        "excluded_manual": 0,
        "unknown_type": 0,
        "no_imdb": 0,
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

        ov = manual_nfid.get(nfid) or manual_key.get((_norm_title(it.get("title")), it.get("vtype")))
        ov = ov or {}

        if ov.get("exclude"):
            dropped["excluded_manual"] += 1
            continue

        vtype = _norm_type(ov.get("type")) if ov.get("type") else _norm_type(it.get("vtype"))
        if vtype == "unknown":
            dropped["unknown_type"] += 1
            continue

        ignore_thresholds = bool(ov.get("ignoreThresholds") or ov.get("forceInclude"))

        # --- dateAdded (uNoGS -> be_date -> prev -> first seen) ---
        added_date = _parse_date(it.get("ndate"))

        # --- availability + be_date ---
        in_be, be_date = verify_be_strict(nfid, avail_cache)
        if added_date is None and be_date is not None:
            added_date = be_date
        if added_date is None and nfid in prev_dates:
            added_date = _parse_date(prev_dates[nfid])
        if added_date is None and nfid not in prev_dates:
            added_date = today  # nieuw item vandaag

        is_recent = bool(added_date and added_date >= cutoff)

        if is_recent:
            if in_be is not True:
                dropped["availability_unknown_recent" if in_be is None else "availability_false_recent"] += 1
                continue
        else:
            if in_be is False:
                dropped["availability_false_full"] += 1
                continue
            if in_be is None:
                dropped["availability_unknown_full"] += 1

        # --- IDs ---
        imdb_id = str(it.get("imdb_id") or "").strip() or None
        tmdb_id = str(it.get("tmdb_id") or "").strip() or None

        # --- IMDb (gold) ---
        imdb_rating = None
        imdb_source = None

        if ov.get("imdbRating") is not None:
            imdb_rating = _parse_rating(ov.get("imdbRating"))
            imdb_source = "manual"

        if imdb_rating is None and imdb_id:
            r = get_imdb_from_omdb(imdb_id, imdb_cache)
            if r is not None:
                imdb_rating = r
                imdb_source = "imdb"

        if imdb_rating is None and tmdb_id and TMDB_API_KEY:
            imdb_id2 = get_imdb_id_via_tmdb(tmdb_id, vtype)
            if imdb_id2:
                imdb_id = imdb_id2
                r = get_imdb_from_omdb(imdb_id2, imdb_cache)
                if r is not None:
                    imdb_rating = r
                    imdb_source = "imdb"

        # fallback: uNoGS
        unogs_rating = _parse_rating(it.get("raw_imdb"))
        votes = it.get("imdb_votes") or 0
        if imdb_rating is None and unogs_rating is not None:
            if unogs_rating >= 9.9 and votes < 5000:
                dropped["unogs_placeholder_10"] += 1
            else:
                imdb_rating = unogs_rating
                imdb_source = "unogs_imdb"

        if imdb_rating is None:
            dropped["no_imdb"] += 1
            continue

        # cutoff
        if not ignore_thresholds and imdb_rating < IMDB_MIN:
            dropped["below_threshold"] += 1
            continue

        # --- Extra scores (mag NA/None zijn) ---
        tmdb_rating = get_tmdb_rating(tmdb_id, vtype, tmdb_cache) if tmdb_id else None
        trakt_rating = get_trakt_rating(imdb_id, vtype, trakt_cache) if imdb_id else None

        release_year = str(ov.get("releaseYear") or ov.get("releaseDate") or it.get("releaseYear") or "")[:4]

        final_list.append({
            "nfid": nfid,
            "title": it.get("title") or "Onbekend",
            "type": "Series" if vtype == "series" else "Film",

            "imdbRating": imdb_rating,
            "tmdbRating": tmdb_rating,     # kan None zijn
            "traktRating": trakt_rating,   # kan None zijn

            "ratingSource": imdb_source,   # waar IMDb vandaan kwam
            "releaseDate": release_year,
            "dateAdded": added_date.isoformat() if added_date else None,
            "availableBE": True if in_be is True else (False if in_be is False else None),

            # optioneel handig voor debugging
            "unogsImdbRating": unogs_rating,
            "imdbVotes": votes,
        })

        time.sleep(0.03)

    _save_json(OUT_FULL, final_list)

    recent_list = [x for x in final_list if x.get("dateAdded") and datetime.date.fromisoformat(x["dateAdded"]) >= cutoff]
    _save_json(OUT_RECENT, recent_list)

    _save_cache_pruned(AVAIL_CACHE, avail_cache, max_days=180)
    _save_cache_pruned(IMDB_CACHE, imdb_cache, max_days=365)
    _save_cache_pruned(TMDB_CACHE, tmdb_cache, max_days=365)
    _save_cache_pruned(TRAKT_CACHE, trakt_cache, max_days=365)

    with_date = sum(1 for x in final_list if x.get("dateAdded"))
    print(f"✅ GEREED: {len(final_list)} totaal, {len(recent_list)} recent.")
    print(f"📉 Dropped stats: {dropped}")
    print(f"ℹ️ Debug: {with_date}/{len(final_list)} items hebben een dateAdded; cutoff={cutoff.isoformat()}.")

if __name__ == "__main__":
    main()
