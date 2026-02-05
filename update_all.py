#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

MANUAL_SCORES = ROOT / "manual_scores.json"          # optional
AVAIL_CACHE   = ROOT / "availability_cache.json"     # auto
IMDB_CACHE    = ROOT / "imdb_cache.json"             # auto

DAYS_RECENT = 90
BE_ID = 21

IMDB_MIN = 7.7

# Availability cache policy:
# - "True" refresh fast (things can disappear quickly)
# - "False" refresh slower (stays gone usually)
TTL_TRUE_DAYS  = 1
TTL_FALSE_DAYS = 7
TTL_UNKNOWN_HOURS = 6  # only used if you decide to cache "unknown" (we don't by default)

# IMDb cache policy
IMDB_TTL_DAYS = 30
IMDB_TTL_NONE_DAYS = 2  # if OMDb returns None, cache shorter to avoid hammering

# Strictness:
# If you want ZERO titles not currently on Netflix Belgium, keep this True.
STRICT_AVAILABILITY_FULL = True
# Recent list should be strict (homepage list).
STRICT_AVAILABILITY_RECENT = True

# uNoGS sanity: many bogus/placeholder values show up as 10.0 with low votes.
DROP_UNOGS_99PLUS_UNDER_VOTES = 5000

TMDB_API_KEY  = os.environ.get("TMDB_API_KEY", "").strip()
UNOGS_API_KEY = os.environ.get("UNOGS_API_KEY", "").strip()
OMDB_API_KEY  = os.environ.get("OMDB_API_KEY", "").strip()

UNOGS_HOST = "unogsng.p.rapidapi.com"
UNOGS_URL  = f"https://{UNOGS_HOST}/search"

SESSION = requests.Session()


# =========================
# HTTP
# =========================
def _get_json(
    url: str,
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
    retries: int = 2,
    timeout: int = 10,
    backoff_base: float = 2.0,
) -> Optional[dict]:
    """
    Robust GET with basic retry on 429/5xx.
    Returns parsed JSON dict or None.
    """
    for i in range(retries + 1):
        try:
            r = SESSION.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504) and i < retries:
                # respect Retry-After if provided
                ra = r.headers.get("Retry-After")
                if ra and str(ra).isdigit():
                    time.sleep(int(ra))
                else:
                    time.sleep(backoff_base * (i + 1))
                continue

            if r.status_code != 200:
                return None

            try:
                return r.json()
            except Exception:
                return None

        except Exception:
            if i < retries:
                time.sleep(1.5)
                continue
    return None


# =========================
# PARSERS / NORMALIZERS
# =========================
def _parse_rating(x: Any) -> Optional[float]:
    if x in (None, "", "no score", "N/A"):
        return None
    s = str(x).strip().replace(",", ".")
    # accepts "8.3/10" or "8.3"
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
        # handles "12,345" or "12345"
        s = str(x).strip().replace(",", "")
        return int(float(s))
    except Exception:
        return None


def _parse_date(x: Any) -> Optional[datetime.date]:
    if not x:
        return None
    try:
        # unix seconds/ms
        if isinstance(x, (int, float)) or (isinstance(x, str) and str(x).isdigit()):
            n = int(x)
            if n > 10_000_000_000:
                n //= 1000
            return datetime.datetime.fromtimestamp(n, tz=datetime.timezone.utc).date()
        # iso date
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
        # skip numeric “titles” that are probably IDs
        if s.isdigit() and len(s) > 4:
            continue
        return html.unescape(s)
    return default


# =========================
# CACHE / MANUAL
# =========================
def _load_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_cache(path: Path, data: dict, max_days: int = 90) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    pruned: Dict[str, dict] = {}
    for k, v in (data or {}).items():
        ts_s = (v or {}).get("ts")
        if not ts_s:
            continue
        try:
            ts = datetime.datetime.fromisoformat(ts_s)
        except Exception:
            continue
        if (now - ts).days < max_days:
            pruned[k] = v
    path.write_text(json.dumps(pruned, ensure_ascii=False, indent=2), encoding="utf-8")


def load_manual_overrides() -> Tuple[Dict[int, dict], Dict[Tuple[str, str], dict]]:
    """
    manual_scores.json supports:
      - nfid-based rows: {"nfid":123, "exclude":true, "type":"series|movie", "imdbRating":8.2, "releaseYear": "2020", "ignoreThresholds":true, "forceInclude":true}
      - title+type rows (fallback): {"title":"Champion", "type":"series", ...}

    Returns: (by_nfid, by_key)
      by_key key = (norm_title, type)
    """
    by_nfid: Dict[int, dict] = {}
    by_key: Dict[Tuple[str, str], dict] = {}

    if not MANUAL_SCORES.exists():
        return by_nfid, by_key

    try:
        # tolerate trailing commas
        txt = re.sub(r",\s*(\]|\})", r"\1", MANUAL_SCORES.read_text(encoding="utf-8"))
        raw = json.loads(txt)
        rows = raw if isinstance(raw, list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if "type" in row:
                row["type"] = _norm_type(row.get("type"))

            nfid = row.get("nfid")
            if nfid is not None and str(nfid).isdigit():
                by_nfid[int(nfid)] = row
                continue

            title = row.get("title")
            rtype = row.get("type")
            if title and rtype in ("movie", "series"):
                by_key[(_norm_title(title), rtype)] = row

    except Exception as e:
        print(f"⚠️ manual_scores error: {e}")

    return by_nfid, by_key


# =========================
# uNoGS availability (BE)
# =========================
def verify_belgium_availability(nfid: int, cache: dict) -> Optional[bool]:
    """
    Returns:
      True  -> definitely in BE
      False -> definitely not in BE
      None  -> network/API hiccup (unknown)
    """
    key = str(nfid)
    now = datetime.datetime.now(datetime.timezone.utc)

    if key in cache:
        try:
            ts = datetime.datetime.fromisoformat(cache[key].get("ts", ""))
            c_be = cache[key].get("be", None)

            if c_be is True:
                ttl = TTL_TRUE_DAYS
            elif c_be is False:
                ttl = TTL_FALSE_DAYS
            else:
                ttl = 0

            if ttl and (now - ts).days < ttl:
                return c_be
        except Exception:
            pass

    res = _get_json(
        f"https://{UNOGS_HOST}/titlecountries",
        headers={"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": UNOGS_HOST},
        params={"netflixid": nfid},
        timeout=10,
        retries=2,
    )
    if res is None:
        return None

    results = res.get("results") or res.get("RESULTS") or res.get("Countries") or []
    if not isinstance(results, list):
        results = []

    in_be = False
    for c in results:
        if not isinstance(c, dict):
            continue
        cid = c.get("id") or c.get("countryid") or c.get("country_id")
        cc  = c.get("cc") or c.get("countrycode") or c.get("country_code")
        if (cid is not None and str(cid) == str(BE_ID)) or (cc and str(cc).upper() == "BE"):
            in_be = True
            break

    cache[key] = {"be": in_be, "ts": now.isoformat()}
    return in_be


# =========================
# IMDb (OMDb) – golden path
# =========================
def get_imdb_from_omdb(imdb_id: str, cache: dict) -> Optional[float]:
    if not (OMDB_API_KEY and imdb_id):
        return None

    key = f"imdb:{imdb_id}"
    now = datetime.datetime.now(datetime.timezone.utc)

    if key in cache:
        try:
            ts = datetime.datetime.fromisoformat(cache[key].get("ts", ""))
            cached = cache[key].get("imdb", None)
            ttl = IMDB_TTL_DAYS if cached is not None else IMDB_TTL_NONE_DAYS
            if (now - ts).days < ttl:
                return cached
        except Exception:
            pass

    om = _get_json("https://www.omdbapi.com/", params={"i": imdb_id, "apikey": OMDB_API_KEY}, timeout=10, retries=2)
    imdb = _parse_rating((om or {}).get("imdbRating"))

    cache[key] = {"imdb": imdb, "ts": now.isoformat()}
    return imdb


def tmdb_to_imdb_id(tmdb_id: str, vtype: str) -> Optional[str]:
    if not (TMDB_API_KEY and tmdb_id):
        return None
    cat = "tv" if vtype == "series" else "movie"
    ext = _get_json(
        f"https://api.themoviedb.org/3/{cat}/{tmdb_id}/external_ids",
        params={"api_key": TMDB_API_KEY},
        timeout=10,
        retries=2,
    )
    imdb_id = (ext or {}).get("imdb_id")
    return imdb_id if imdb_id else None


# =========================
# FETCH CANDIDATES (uNoGS)
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
            if not isinstance(batch, list) or not batch:
                break

            for x in batch:
                if not isinstance(x, dict):
                    continue
                vtype = _norm_type(x.get("vtype") or t)
                items.append(
                    {
                        "nfid": x.get("nfid"),
                        "title": _pick_title(x, "title", "name", "t"),
                        "vtype": vtype,  # movie|series|unknown
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
            time.sleep(0.35)  # throttle to reduce 429

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

    cutoff = datetime.datetime.now(datetime.timezone.utc).date() - datetime.timedelta(days=DAYS_RECENT)

    final_list: List[dict] = []
    seen_nfids = set()

    # Stats
    dropped = {
        "bad_nfid": 0,
        "duplicate_nfid": 0,
        "excluded_manual": 0,
        "unknown_type": 0,
        "no_rating": 0,
        "below_threshold": 0,
        "availability_not_true": 0,
        "availability_false_full": 0,
        "availability_unknown_strict": 0,
    }

    for it in candidates:
        # nfid
        nfid_raw = it.get("nfid")
        if not (nfid_raw and str(nfid_raw).isdigit()):
            dropped["bad_nfid"] += 1
            continue
        nfid = int(nfid_raw)

        if nfid in seen_nfids:
            dropped["duplicate_nfid"] += 1
            continue
        seen_nfids.add(nfid)

        # manual override lookup
        ov = manual_nfid.get(nfid) or manual_key.get((_norm_title(it.get("title")), it.get("vtype")))
        if ov and ov.get("exclude"):
            dropped["excluded_manual"] += 1
            continue

        # type (manual can override)
        vtype = _norm_type(ov.get("type")) if (ov and ov.get("type")) else _norm_type(it.get("vtype"))
        if vtype == "unknown":
            dropped["unknown_type"] += 1
            continue

        # dates
        added_date = _parse_date(it.get("ndate"))
        is_recent = bool(added_date and added_date >= cutoff)

        # ignore thresholds?
        ignore_thresholds = bool(ov and (ov.get("ignoreThresholds") or ov.get("forceInclude")))

        # release year override
        release_year = it.get("releaseYear") or ""
        if ov:
            if ov.get("releaseYear"):
                release_year = str(ov.get("releaseYear"))[:4]
            elif ov.get("releaseDate"):
                release_year = str(ov.get("releaseDate"))[:4]

        # -------------------------
        # RATING LOGIC (priority)
        # -------------------------
        rating: Optional[float] = None
        source: Optional[str] = None

        # 1) manual imdbRating
        if ov and ov.get("imdbRating") is not None:
            mr = _parse_rating(ov.get("imdbRating"))
            if mr is not None:
                rating, source = mr, "manual"

        # 2) direct imdb_id -> OMDb
        if rating is None and it.get("imdb_id"):
            tmp = get_imdb_from_omdb(str(it.get("imdb_id")), imdb_cache)
            if tmp is not None:
                rating, source = tmp, "imdb"

        # 3) TMDb -> imdb_id -> OMDb
        if rating is None and it.get("tmdb_id") and TMDB_API_KEY and OMDB_API_KEY:
            imdb_id = tmdb_to_imdb_id(str(it.get("tmdb_id")), vtype)
            if imdb_id:
                tmp = get_imdb_from_omdb(imdb_id, imdb_cache)
                if tmp is not None:
                    rating, source = tmp, "imdb"

        # 4) uNoGS imdb_rating (last resort) with sanity checks
        unogs_rating = _parse_rating(it.get("raw_imdb"))
        if rating is None and unogs_rating is not None:
            votes = it.get("imdb_votes") or 0
            # drop "10.0" placeholders unless votes are meaningful
            if unogs_rating >= 9.9 and votes < DROP_UNOGS_99PLUS_UNDER_VOTES:
                rating, source = None, None
            else:
                rating, source = unogs_rating, "unogs_imdb"

        # thresholds (post enrichment)
        if not ignore_thresholds:
            if rating is None:
                dropped["no_rating"] += 1
                continue
            if rating < IMDB_MIN:
                dropped["below_threshold"] += 1
                continue

        # -------------------------
        # AVAILABILITY CHECK (BE)
        # -------------------------
        be = verify_belgium_availability(nfid, avail_cache)

        if is_recent:
            # strict recent
            if STRICT_AVAILABILITY_RECENT:
                if be is not True:
                    dropped["availability_not_true"] += 1
                    continue
            else:
                if be is False:
                    dropped["availability_not_true"] += 1
                    continue
        else:
            # full list strictness
            if STRICT_AVAILABILITY_FULL:
                if be is not True:
                    # unknown or false => skip (prevents “not in BE anymore” from leaking)
                    dropped["availability_unknown_strict"] += 1
                    continue
            else:
                if be is False:
                    dropped["availability_false_full"] += 1
                    continue

        # output item
        final_list.append(
            {
                "nfid": nfid,
                "title": it.get("title"),
                "type": "Series" if vtype == "series" else "Film",
                "vtype": vtype,
                "imdbRating": rating,
                "ratingSource": source,
                "releaseDate": release_year,
                "dateAdded": added_date.isoformat() if added_date else None,
                # debug/support fields (optional but useful)
                "tmdb_id": it.get("tmdb_id"),
                "imdb_id": it.get("imdb_id"),
                "unogsImdbRating": unogs_rating,
                "imdbVotes": it.get("imdb_votes"),
                "availableBE": True,
                "manual": bool(ov),
            }
        )

    # Save outputs
    OUT_FULL.write_text(json.dumps(final_list, ensure_ascii=False, indent=2), encoding="utf-8")

    recent = [
        x for x in final_list
        if x.get("dateAdded") and datetime.date.fromisoformat(x["dateAdded"]) >= cutoff
    ]
    OUT_RECENT.write_text(json.dumps(recent, ensure_ascii=False, indent=2), encoding="utf-8")

    # Save caches
    _save_cache(AVAIL_CACHE, avail_cache, max_days=180)
    _save_cache(IMDB_CACHE, imdb_cache, max_days=365)

    print(f"✅ GEREED: {len(final_list)} totaal, {len(recent)} recent.")
    print("📉 Dropped stats:", dropped)


if __name__ == "__main__":
    main()
