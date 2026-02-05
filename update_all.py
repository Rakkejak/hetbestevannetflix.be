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

DAYS_RECENT = 90
BE_ID = 21
IMDB_MIN = 7.7
TMDB_MIN = 7.0

# TTL (availability): True snel verversen (24u), False langer cachen (7d)
TTL_TRUE_DAYS = 1
TTL_FALSE_DAYS = 7
IMDB_TTL_DAYS = 30

TMDB_API_KEY  = os.environ.get("TMDB_API_KEY", "").strip()
UNOGS_API_KEY = os.environ.get("UNOGS_API_KEY", "").strip()
OMDB_API_KEY  = os.environ.get("OMDB_API_KEY", "").strip()

UNOGS_HOST = "unogsng.p.rapidapi.com"
UNOGS_URL  = f"https://{UNOGS_HOST}/search"

SESSION = requests.Session()

# ------------------ HTTP ------------------
def _get_json(url: str, headers: dict = None, params: dict = None,
              retries: int = 2, timeout: int = 10, backoff: float = 2.0) -> Optional[dict]:
    """Robuuste GET met retry op 429/5xx."""
    for i in range(retries + 1):
        try:
            r = SESSION.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504) and i < retries:
                time.sleep(backoff * (i + 1))
                continue
            if r.status_code != 200:
                return None
            return r.json()
        except:
            if i < retries:
                time.sleep(1.5)
            continue
    return None

# ------------------ PARSERS ------------------
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
    except:
        return None

def _parse_int(x: Any) -> Optional[int]:
    if x in (None, "", "N/A"):
        return None
    try:
        return int(float(str(x).replace(",", "").strip()))
    except:
        return None

def _parse_date(x: Any) -> Optional[datetime.date]:
    if not x:
        return None
    try:
        # unix seconds / ms
        if isinstance(x, (int, float)) or (isinstance(x, str) and str(x).isdigit()):
            n = int(float(str(x)))
            if n > 10_000_000_000:
                n //= 1000
            return datetime.datetime.fromtimestamp(n, tz=datetime.timezone.utc).date()
        # ISO
        return datetime.date.fromisoformat(str(x)[:10])
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

def _pick_title(d: Dict[str, Any], *keys: str, default: str = "Onbekend") -> str:
    for k in keys:
        v = d.get(k)
        if v in (None, "", 0, "0", "None"):
            continue
        s = str(v).strip()
        # skip pure NFID strings
        if s.isdigit() and len(s) > 4:
            continue
        return html.unescape(s)
    return default

# ------------------ CACHE ------------------
def _load_cache(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except:
        return {}

def _save_cache(path: Path, data: dict, max_days: int = 180) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    pruned: Dict[str, Any] = {}
    for k, v in (data or {}).items():
        try:
            ts = datetime.datetime.fromisoformat(v.get("ts", ""))
            if (now - ts).days < max_days:
                pruned[k] = v
        except:
            # drop broken entries
            pass
    path.write_text(json.dumps(pruned, ensure_ascii=False, indent=2), encoding="utf-8")

# ------------------ MANUAL OVERRIDES ------------------
def load_manual_overrides() -> Tuple[Dict[int, dict], Dict[Tuple[str, str], dict]]:
    """
    Ondersteunt:
      - per nfid: {"nfid": 123, ...}
      - per title+type: {"title": "...", "type": "serie/film", ...}
    """
    by_nfid: Dict[int, dict] = {}
    by_key: Dict[Tuple[str, str], dict] = {}

    if not MANUAL_SCORES.exists():
        return by_nfid, by_key

    try:
        txt = re.sub(r",\s*(\]|\})", r"\1", MANUAL_SCORES.read_text(encoding="utf-8"))
        raw = json.loads(txt)
        for row in (raw if isinstance(raw, list) else []):
            if "type" in row:
                row["type"] = _norm_type(row.get("type"))
            if "nfid" in row and str(row["nfid"]).isdigit():
                by_nfid[int(row["nfid"])] = row
            elif row.get("title") and row.get("type") in ("movie", "series"):
                by_key[(_norm_title(row["title"]), row["type"])] = row
    except Exception as e:
        print(f"⚠️ manual_scores error: {e}")

    return by_nfid, by_key

# ------------------ uNoGS: availability + be_date ------------------
def _extract_country_date(c: dict) -> Optional[datetime.date]:
    """
    Probeert BE 'date added' te vinden in een country record.
    We kennen de exacte key(s) niet 100% zeker (uNoGS varieert), dus we proberen meerdere.
    """
    for k in (
        "new_date", "ndate", "date", "added_date", "addedDate", "netflix_date",
        "newDate", "newdate", "first_date", "firstDate"
    ):
        if k in c and c.get(k):
            d = _parse_date(c.get(k))
            if d:
                return d
    return None

def verify_belgium_availability(nfid: int, cache: dict, sleep_s: float = 0.05) -> Tuple[Optional[bool], Optional[datetime.date]]:
    """
    Returns: (in_be, be_date)
      - in_be: True/False/None (None = hiccup)
      - be_date: dateAdded voor België (als gevonden)
    Cache bewaart: {"be": bool, "be_date": "YYYY-MM-DD"|None, "ts": iso}
    """
    key = str(nfid)
    now = datetime.datetime.now(datetime.timezone.utc)

    if key in cache:
        try:
            ts = datetime.datetime.fromisoformat(cache[key]["ts"])
            c_be = cache[key].get("be")
            ttl = TTL_TRUE_DAYS if c_be is True else TTL_FALSE_DAYS
            if (now - ts).days < ttl:
                be_date = _parse_date(cache[key].get("be_date"))
                return c_be, be_date
        except:
            pass

    res = _get_json(
        f"https://{UNOGS_HOST}/titlecountries",
        headers={"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": UNOGS_HOST},
        params={"netflixid": nfid},
        timeout=10,
        retries=2,
    )
    if sleep_s:
        time.sleep(sleep_s)

    if res is None:
        return None, None

    results = res.get("results") or res.get("RESULTS") or res.get("Countries") or []
    in_be = False
    be_date: Optional[datetime.date] = None

    if isinstance(results, list):
        for c in results:
            cid = c.get("id") or c.get("countryid") or c.get("country_id")
            cc = c.get("cc") or c.get("countrycode") or c.get("country_code")
            if (cid and str(cid) == str(BE_ID)) or (cc and str(cc).upper() == "BE"):
                in_be = True
                be_date = _extract_country_date(c)
                break

    cache[key] = {
        "be": in_be,
        "be_date": be_date.isoformat() if be_date else None,
        "ts": now.isoformat()
    }
    return in_be, be_date

# ------------------ OMDb IMDb ------------------
def get_imdb_from_omdb(imdb_id: str, cache: dict) -> Optional[float]:
    if not (OMDB_API_KEY and imdb_id):
        return None

    imdb_id = str(imdb_id).strip()
    if not imdb_id.startswith("tt"):
        # OMDb verwacht tt1234567; als het iets anders is, laten we het vallen.
        return None

    key = f"imdb:{imdb_id}"
    now = datetime.datetime.now(datetime.timezone.utc)

    if key in cache:
        try:
            ts = datetime.datetime.fromisoformat(cache[key]["ts"])
            if (now - ts).days < IMDB_TTL_DAYS:
                return cache[key].get("imdb")
        except:
            pass

    om = _get_json("https://www.omdbapi.com/", params={"i": imdb_id, "apikey": OMDB_API_KEY}, timeout=10, retries=2)
    imdb = _parse_rating((om or {}).get("imdbRating"))
    cache[key] = {"imdb": imdb, "ts": now.isoformat()}
    return imdb

def get_imdb_id_via_tmdb(tmdb_id: str, vtype: str, cache: dict) -> Optional[str]:
    """
    Haalt imdb_id (tt...) via TMDb external_ids.
    Cache in imdb_cache onder key: tmdb:<vtype>:<tmdb_id>
    """
    if not (TMDB_API_KEY and tmdb_id):
        return None
    key = f"tmdb:{vtype}:{tmdb_id}"
    now = datetime.datetime.now(datetime.timezone.utc)

    if key in cache:
        try:
            ts = datetime.datetime.fromisoformat(cache[key]["ts"])
            # we kunnen deze lang cachen
            if (now - ts).days < 180:
                return cache[key].get("imdb_id")
        except:
            pass

    cat = "tv" if vtype == "series" else "movie"
    ext = _get_json(
        f"https://api.themoviedb.org/3/{cat}/{tmdb_id}/external_ids",
        params={"api_key": TMDB_API_KEY},
        timeout=10,
        retries=2
    )
    imdb_id = (ext or {}).get("imdb_id")
    cache[key] = {"imdb_id": imdb_id, "ts": now.isoformat()}
    return imdb_id

# ------------------ FETCH uNoGS candidates ------------------
def fetch_candidates() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    headers = {"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": UNOGS_HOST}

    for t in ("movie", "series"):
        offset = 0
        while True:
            j = _get_json(
                UNOGS_URL,
                headers=headers,
                params={
                    "type": t,
                    "countrylist": str(BE_ID),
                    "offset": offset,
                    "limit": 100,
                },
                timeout=30,
                retries=2
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
                    # date fields vary; capture a few
                    "ndate": x.get("ndate") or x.get("new_date") or x.get("date") or x.get("newdate"),
                    "tmdb_id": x.get("tmdb_id") or x.get("tmid"),
                })

            if len(batch) < 100:
                break

            offset += 100
            time.sleep(0.2)

    return items

# ------------------ MAIN ------------------
def main():
    if not UNOGS_API_KEY:
        print("❌ UNOGS_API_KEY ontbreekt.")
        return

    manual_nfid, manual_key = load_manual_overrides()
    avail_cache = _load_cache(AVAIL_CACHE)
    imdb_cache = _load_cache(IMDB_CACHE)

    candidates = fetch_candidates()
    print(f"📡 uNoGS candidates: {len(candidates)}")

    cutoff = datetime.datetime.now(datetime.timezone.utc).date() - datetime.timedelta(days=DAYS_RECENT)

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

    final_list: List[Dict[str, Any]] = []
    seen_nfids = set()

    for it in candidates:
        # ---- NFID ----
        try:
            nfid = int(it["nfid"])
        except:
            dropped["bad_nfid"] += 1
            continue

        if nfid in seen_nfids:
            dropped["duplicate_nfid"] += 1
            continue
        seen_nfids.add(nfid)

        # ---- Manual override lookup ----
        ov = manual_nfid.get(nfid) or manual_key.get((_norm_title(it["title"]), it["vtype"]))
        if ov and ov.get("exclude"):
            dropped["excluded_manual"] += 1
            continue

        vtype = _norm_type(ov.get("type")) if (ov and ov.get("type")) else it["vtype"]
        if vtype == "unknown":
            dropped["unknown_type"] += 1
            continue

        ignore_thresholds = bool(ov and (ov.get("ignoreThresholds") or ov.get("forceInclude")))

        # ---- dateAdded (first try from search) ----
        added_date = _parse_date(it.get("ndate"))

        # ---- Rating ----
        rating: Optional[float] = None
        source: Optional[str] = None

        # manual rating wins
        if ov and ov.get("imdbRating"):
            rating = _parse_rating(ov.get("imdbRating"))
            source = "manual"

        # IMDb via OMDb (gold path): imdb_id direct
        if rating is None and it.get("imdb_id"):
            r = get_imdb_from_omdb(str(it["imdb_id"]), imdb_cache)
            if r is not None:
                rating, source = r, "imdb"

        # IMDb via TMDb -> external_ids -> OMDb
        if rating is None and it.get("tmdb_id") and TMDB_API_KEY:
            imdb_id = get_imdb_id_via_tmdb(str(it["tmdb_id"]), vtype, imdb_cache)
            if imdb_id:
                r = get_imdb_from_omdb(imdb_id, imdb_cache)
                if r is not None:
                    rating, source = r, "imdb"

        # fallback: uNoGS rating, but detect 10.0 placeholders
        unogs_rating = _parse_rating(it.get("raw_imdb"))
        if rating is None and unogs_rating is not None:
            votes = it.get("imdb_votes") or 0
            if unogs_rating >= 9.9 and votes < 5000:
                dropped["unogs_placeholder_10"] += 1
            else:
                rating, source = unogs_rating, "unogs_imdb"

        if rating is None:
            dropped["no_rating"] += 1
            continue

        # ---- Thresholds AFTER enrichment ----
        if not ignore_thresholds:
            if source in ("imdb", "unogs_imdb", "manual") and rating < IMDB_MIN:
                dropped["below_threshold"] += 1
                continue
            if source == "tmdb" and rating < TMDB_MIN:
                dropped["below_threshold"] += 1
                continue

        # ---- Availability + BE dateAdded fallback ----
        be, be_date = verify_belgium_availability(nfid, avail_cache, sleep_s=0.05)

        # Cruciale fix: dateAdded aanvullen met BE-date als search geen ndate heeft
        if added_date is None and be is True and be_date is not None:
            added_date = be_date

        is_recent = bool(added_date and added_date >= cutoff)

        # Strict voor recent: moet True zijn; Full: False drop, None mag blijven (lenient)
        if is_recent:
            if be is False:
                dropped["availability_false_recent"] += 1
                continue
            if be is None:
                dropped["availability_unknown_recent"] += 1
                continue
            if be is not True:
                dropped["availability_unknown_recent"] += 1
                continue
        else:
            if be is False:
                dropped["availability_false_full"] += 1
                continue
            if be is None:
                dropped["availability_unknown_full"] += 1
                # lenient: keep, but mark not guaranteed
                # (je kan dit ook op continue zetten als je ultra-strikt wil)

        final_list.append({
            "nfid": nfid,
            "title": it["title"],
            "type": "Series" if vtype == "series" else "Film",
            "imdbRating": rating,
            "ratingSource": source,
            "releaseDate": it.get("releaseYear") or None,
            "dateAdded": added_date.isoformat() if added_date else None,
            "availableBE": (be is True),
        })

    # Save outputs + caches
    OUT_FULL.write_text(json.dumps(final_list, ensure_ascii=False, indent=2), encoding="utf-8")

    recent = [x for x in final_list if x.get("dateAdded") and datetime.date.fromisoformat(x["dateAdded"]) >= cutoff]
    OUT_RECENT.write_text(json.dumps(recent, ensure_ascii=False, indent=2), encoding="utf-8")

    _save_cache(AVAIL_CACHE, avail_cache, max_days=180)
    _save_cache(IMDB_CACHE, imdb_cache, max_days=365)

    # Debug
    n_dates = sum(1 for x in final_list if x.get("dateAdded"))
    print(f"✅ GEREED: {len(final_list)} totaal, {len(recent)} recent.")
    print(f"📉 Dropped stats: {dropped}")
    print(f"ℹ️ Debug: {n_dates}/{len(final_list)} items hebben een dateAdded; cutoff={cutoff.isoformat()}.")

if __name__ == "__main__":
    main()
