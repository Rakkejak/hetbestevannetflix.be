#!/usr/bin/env python3
import os, json, datetime, time, re, html
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
IMDB_CACHE    = ROOT / "imdb_cache.json"

DAYS_RECENT = 90
BE_ID       = 21

# score thresholds
IMDB_MIN = 7.7

# availability cache TTL:
# - TRUE: snel verversen (Netflix wisselt vaak)
# - FALSE: langer cachen
TTL_TRUE_DAYS  = 1
TTL_FALSE_DAYS = 7

# imdb rating cache TTL
IMDB_TTL_DAYS = 30

# small delay to reduce 429 on titlecountries (only on cache misses)
TITLECOUNTRIES_SLEEP = 0.08

TMDB_API_KEY  = os.environ.get("TMDB_API_KEY", "").strip()
UNOGS_API_KEY = os.environ.get("UNOGS_API_KEY", "").strip()
OMDB_API_KEY  = os.environ.get("OMDB_API_KEY", "").strip()

UNOGS_HOST = "unogsng.p.rapidapi.com"
UNOGS_URL  = f"https://{UNOGS_HOST}/search"

SESSION = requests.Session()

# =========================
# UTIL
# =========================
def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)

def _today_utc() -> datetime.date:
    return _utcnow().date()

def _get_json(
    url: str,
    headers: dict = None,
    params: dict = None,
    retries: int = 2,
    timeout: int = 10,
) -> Optional[dict]:
    """Robuuste GET: retry op 429 + 5xx, terug None bij blijvende errors."""
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

def _parse_rating(x: Any) -> Optional[float]:
    if x in (None, "", "no score", "N/A"):
        return None
    s = str(x).strip().replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:/|$)", s)
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
    """Accepteert epoch (ms/s) en ISO-achtige strings; slices op YYYY-MM-DD."""
    if not x:
        return None
    try:
        if isinstance(x, (int, float)) or (isinstance(x, str) and str(x).isdigit()):
            n = int(x)
            if n > 10_000_000_000:  # ms -> s
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

def _pick_title(d: Dict[str, Any], *keys: str, default="Onbekend") -> str:
    """Kiest een titelveld; slaat “title is eigenlijk nfid” over."""
    for k in keys:
        v = d.get(k)
        if v in (None, "", 0, "0", "None"):
            continue
        s = str(v).strip()
        if s.isdigit() and len(s) > 4:
            continue
        return html.unescape(s)
    return default

# =========================
# CACHE + MANUAL
# =========================
def _load_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_cache(path: Path, data: dict, max_days: int = 180) -> None:
    now = _utcnow()
    pruned: Dict[str, Any] = {}
    for k, v in (data or {}).items():
        try:
            ts = datetime.datetime.fromisoformat(v.get("ts", ""))
            if (now - ts).days < max_days:
                pruned[k] = v
        except Exception:
            # als ts niet parsebaar is, gooien we het item weg
            pass
    path.write_text(json.dumps(pruned, ensure_ascii=False, indent=2), encoding="utf-8")

def load_manual_overrides() -> Tuple[Dict[int, dict], Dict[Tuple[str, str], dict]]:
    """
    Manual overrides worden geladen op 2 manieren:
    - by_nfid: { 12345: {...} }
    - by_key:  { (norm_title, type): {...} }  als nfid ontbreekt
    """
    by_nfid: Dict[int, dict] = {}
    by_key: Dict[Tuple[str, str], dict] = {}

    if not MANUAL_SCORES.exists():
        return by_nfid, by_key

    try:
        txt = MANUAL_SCORES.read_text(encoding="utf-8")
        # tolerant voor trailing commas
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
        print(f"⚠️ manual_scores.json error: {e}")

    return by_nfid, by_key

# =========================
# uNoGS: Availability + BE date
# =========================
def _extract_country_id_and_cc(c: dict) -> Tuple[Optional[str], Optional[str]]:
    cid = c.get("id") or c.get("countryid") or c.get("country_id")
    cc  = c.get("cc") or c.get("countrycode") or c.get("country_code")
    return (str(cid) if cid is not None else None, str(cc).upper() if cc is not None else None)

def _extract_added_date_from_country_entry(c: dict) -> Optional[datetime.date]:
    # uNoGS varieert: probeer meerdere keys
    for k in ("new_date", "newdate", "newDate", "ndate", "date", "added_date", "addedDate", "added"):
        if k in c and c.get(k) not in (None, "", 0, "0"):
            d = _parse_date(c.get(k))
            if d:
                return d
    return None

def verify_belgium_availability_and_date(nfid: int, cache: dict) -> Tuple[Optional[bool], Optional[datetime.date]]:
    """
    Return:
      (be_available, be_added_date)

    Cache entry:
      cache[str(nfid)] = {"be": bool, "be_date": "YYYY-MM-DD"|None, "ts": ISO}
    TTL:
      be=True  => 1 dag
      be=False => 7 dagen
    """
    key = str(nfid)
    now = _utcnow()

    if key in cache:
        try:
            ts = datetime.datetime.fromisoformat(cache[key]["ts"])
            cached_be = cache[key].get("be")
            ttl = TTL_TRUE_DAYS if cached_be is True else TTL_FALSE_DAYS
            if (now - ts).days < ttl:
                be_date = _parse_date(cache[key].get("be_date"))
                return cached_be, be_date
        except Exception:
            pass

    res = _get_json(
        f"https://{UNOGS_HOST}/titlecountries",
        headers={"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": UNOGS_HOST},
        params={"netflixid": nfid},
        timeout=12,
        retries=2,
    )
    if res is None:
        return None, None  # geen cache update bij hiccup

    results = res.get("results") or res.get("RESULTS") or res.get("Countries") or []
    be_available = False
    be_date: Optional[datetime.date] = None

    if isinstance(results, list):
        for c in results:
            if not isinstance(c, dict):
                continue
            cid, cc = _extract_country_id_and_cc(c)
            if (cid and cid == str(BE_ID)) or (cc and cc == "BE"):
                be_available = True
                be_date = _extract_added_date_from_country_entry(c)
                break

    cache[key] = {
        "be": be_available,
        "be_date": be_date.isoformat() if be_date else None,
        "ts": now.isoformat(),
    }

    # kleine pauze enkel bij echte call (niet cache hit)
    time.sleep(TITLECOUNTRIES_SLEEP)
    return be_available, be_date

# =========================
# IMDb via OMDb (cache)
# =========================
def get_imdb_from_omdb(imdb_id: str, cache: dict) -> Optional[float]:
    """Directe IMDb rating (OMDb) met cache."""
    if not (OMDB_API_KEY and imdb_id):
        return None

    imdb_id = str(imdb_id).strip()
    if not imdb_id.startswith("tt"):
        # sommige feeds geven numeric ids; OMDb verwacht "tt..."
        # liever None dan foute lookup
        return None

    key = f"imdb:{imdb_id}"
    now = _utcnow()

    if key in cache:
        try:
            ts = datetime.datetime.fromisoformat(cache[key]["ts"])
            if (now - ts).days < IMDB_TTL_DAYS:
                return cache[key].get("imdb")
        except Exception:
            pass

    om = _get_json("https://www.omdbapi.com/", params={"i": imdb_id, "apikey": OMDB_API_KEY}, retries=2, timeout=12)
    imdb = _parse_rating((om or {}).get("imdbRating"))

    cache[key] = {"imdb": imdb, "ts": now.isoformat()}
    return imdb

def get_imdb_via_tmdb(tmdb_id: str, vtype: str, cache: dict) -> Optional[float]:
    """
    TMDb -> external_ids -> imdb_id -> OMDb rating.
    We cachen ook de tmdb->imdb mapping zodat TMDb calls dalen.
    """
    if not (TMDB_API_KEY and OMDB_API_KEY and tmdb_id):
        return None

    vtype = "tv" if vtype == "series" else "movie"
    tmdb_id = str(tmdb_id).strip()

    # 1) eerst: rating cache op tmdb id
    key_rating = f"tmdbimdb:{vtype}:{tmdb_id}"
    now = _utcnow()
    if key_rating in cache:
        try:
            ts = datetime.datetime.fromisoformat(cache[key_rating]["ts"])
            if (now - ts).days < IMDB_TTL_DAYS:
                return cache[key_rating].get("imdb")
        except Exception:
            pass

    # 2) mapping cache tmdb -> imdb_id
    key_map = f"tmdbext:{vtype}:{tmdb_id}"
    imdb_id = None
    if key_map in cache:
        try:
            ts = datetime.datetime.fromisoformat(cache[key_map]["ts"])
            if (now - ts).days < 180:
                imdb_id = cache[key_map].get("imdb_id")
        except Exception:
            pass

    if not imdb_id:
        ext = _get_json(
            f"https://api.themoviedb.org/3/{vtype}/{tmdb_id}/external_ids",
            params={"api_key": TMDB_API_KEY},
            retries=2,
            timeout=12,
        )
        imdb_id = (ext or {}).get("imdb_id")
        cache[key_map] = {"imdb_id": imdb_id, "ts": now.isoformat()}

    imdb = get_imdb_from_omdb(str(imdb_id), cache) if imdb_id else None
    cache[key_rating] = {"imdb": imdb, "ts": now.isoformat()}
    return imdb

# =========================
# FETCH
# =========================
def fetch_candidates() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    headers = {"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": UNOGS_HOST}

    for t in ("movie", "series"):
        offset = 0
        while True:
            j = _get_json(
                UNOGS_URL,
                headers=headers,
                params={"type": t, "countrylist": str(BE_ID), "offset": offset, "limit": 100},
                timeout=30,
                retries=2,
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
                    "ndate": x.get("ndate") or x.get("new_date"),
                    "tmdb_id": x.get("tmdb_id") or x.get("tmid"),
                })

            if len(batch) < 100:
                break
            offset += 100

    return items

# =========================
# MAIN
# =========================
def main() -> None:
    if not UNOGS_API_KEY:
        print("❌ UNOGS_API_KEY ontbreekt. Abort.")
        return

    manual_nfid, manual_key = load_manual_overrides()
    avail_cache = _load_cache(AVAIL_CACHE)
    imdb_cache  = _load_cache(IMDB_CACHE)

    candidates = fetch_candidates()
    print(f"📡 uNoGS candidates: {len(candidates)}")

    cutoff = _today_utc() - datetime.timedelta(days=DAYS_RECENT)

    seen_nfids = set()
    final_list: List[dict] = []

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

    for it in candidates:
        # --- NFID ---
        nfid_raw = it.get("nfid")
        if not nfid_raw or not str(nfid_raw).isdigit():
            dropped["bad_nfid"] += 1
            continue
        nfid = int(nfid_raw)

        if nfid in seen_nfids:
            dropped["duplicate_nfid"] += 1
            continue
        seen_nfids.add(nfid)

        # --- Manual override lookup ---
        ov = manual_nfid.get(nfid) or manual_key.get((_norm_title(it.get("title")), it.get("vtype")))
        if ov and ov.get("exclude"):
            dropped["excluded_manual"] += 1
            continue

        vtype = _norm_type(ov.get("type")) if (ov and ov.get("type")) else it.get("vtype")
        if vtype not in ("movie", "series"):
            dropped["unknown_type"] += 1
            continue

        ignore_thresholds = bool(ov and (ov.get("ignoreThresholds") or ov.get("forceInclude")))

        # --- Added date (global) ---
        added_global = _parse_date(it.get("ndate"))

        # --- Rating logic ---
        rating: Optional[float] = None
        source: Optional[str] = None
        path: Optional[str] = None

        # 1) manual rating
        if ov and ov.get("imdbRating"):
            rating = _parse_rating(ov.get("imdbRating"))
            source, path = ("manual", "manual")

        # 2) direct imdb_id -> OMDb
        if rating is None and it.get("imdb_id"):
            r2 = get_imdb_from_omdb(str(it["imdb_id"]), imdb_cache)
            if r2 is not None:
                rating, source, path = (r2, "imdb", "imdb_direct")

        # 3) via tmdb -> external_ids -> imdb -> OMDb
        if rating is None and it.get("tmdb_id"):
            r3 = get_imdb_via_tmdb(str(it["tmdb_id"]), vtype, imdb_cache)
            if r3 is not None:
                rating, source, path = (r3, "imdb", "tmdb_external_ids")

        # 4) uNoGS fallback (met sanity check op 10.0 placeholders)
        unogs_rating = _parse_rating(it.get("raw_imdb"))
        if rating is None and unogs_rating is not None:
            votes = it.get("imdb_votes") or 0
            if unogs_rating >= 9.9 and votes < 5000:
                # vaak placeholder/artefact
                dropped["unogs_placeholder_10"] += 1
            else:
                rating, source, path = (unogs_rating, "unogs_imdb", "unogs_search")

        if rating is None:
            dropped["no_rating"] += 1
            continue

        # --- thresholds ---
        if not ignore_thresholds:
            if rating < IMDB_MIN:
                dropped["below_threshold"] += 1
                continue

        # --- availability + BE add date (BELANGRIJK voor "recent") ---
        be, be_date = verify_belgium_availability_and_date(nfid, avail_cache)

        # BE-date heeft voorrang (want je site is BE)
        date_added = be_date or added_global
        is_recent = bool(date_added and date_added >= cutoff)

        # strict for recent, lenient for full:
        if is_recent:
            if be is not True:
                if be is False:
                    dropped["availability_false_recent"] += 1
                else:
                    dropped["availability_unknown_recent"] += 1
                continue
        else:
            if be is False:
                dropped["availability_false_full"] += 1
                continue
            if be is None:
                # lenient: keep in full list
                dropped["availability_unknown_full"] += 1

        # --- output row ---
        title = it.get("title") or "Onbekend"
        out = {
            "nfid": nfid,
            "title": title,
            "type": "Series" if vtype == "series" else "Film",
            "imdbRating": rating,
            "ratingSource": source,      # imdb | manual | unogs_imdb
            "ratingPath": path,          # imdb_direct | tmdb_external_ids | unogs_search | manual
            "releaseDate": it.get("releaseYear") or "",
            # dateAdded is BE-prefered
            "dateAdded": date_added.isoformat() if date_added else None,
            # handig voor debugging
            "dateAddedGlobal": added_global.isoformat() if added_global else None,
            "dateAddedBE": be_date.isoformat() if be_date else None,
            "availableBE": True if be is True else (False if be is False else None),
        }

        final_list.append(out)

    # write outputs
    OUT_FULL.write_text(json.dumps(final_list, ensure_ascii=False, indent=2), encoding="utf-8")

    recent = []
    for x in final_list:
        da = x.get("dateAdded")
        if da:
            try:
                if datetime.date.fromisoformat(da) >= cutoff:
                    recent.append(x)
            except Exception:
                pass

    OUT_RECENT.write_text(json.dumps(recent, ensure_ascii=False, indent=2), encoding="utf-8")

    # save caches
    _save_cache(AVAIL_CACHE, avail_cache, max_days=180)
    _save_cache(IMDB_CACHE, imdb_cache, max_days=365)

    print(f"✅ GEREED: {len(final_list)} totaal, {len(recent)} recent.")
    print(f"📉 Dropped stats: {dropped}")

    # extra hint als recent leeg blijft
    if len(recent) == 0:
        # tel hoeveel items wél een dateAdded hebben maar buiten cutoff vallen
        with_date = sum(1 for x in final_list if x.get("dateAdded"))
        print(f"ℹ️ Debug: {with_date}/{len(final_list)} items hebben een dateAdded; cutoff={cutoff.isoformat()}.")

if __name__ == "__main__":
    main()
