def fetch_candidates() -> List[Dict[str, Any]]:
    items = []
    headers = {"X-RapidAPI-Key": UNOGS_API_KEY, "X-RapidAPI-Host": "unogsng.p.rapidapi.com"}
    
    for t in ["movie", "series"]:
        offset = 0
        while True:
            params = {"type": t, "countrylist": "21", "offset": offset, "limit": 100}
            try:
                resp = requests.get("https://unogsng.p.rapidapi.com/search", headers=headers, params=params, timeout=20)
                data = resp.json()
                batch = data.get("results") or []
                if not batch: break
                
                for x in batch:
                    # FIX 1: Correcte velden mappen (Titel en Jaar)
                    # We gebruiken meerdere opties zodat we nooit 'None' of een nummer krijgen
                    clean_title = x.get("title") or x.get("t") or "Onbekend"
                    clean_year = x.get("year") or x.get("v") or "2024"
                    
                    items.append({
                        "title": clean_title,
                        "type": t,
                        "imdbRating": x.get("imdb_rating") or x.get("rating") or 0,
                        "releaseDate": str(clean_year),
                        "dateAdded": str(x.get("ndate") or ""),
                        "tmdb_id": x.get("tmid") or x.get("tmdbid")
                    })
                offset += 100
                if len(batch) < 100: break
            except: break
    return items

def normalize_and_filter(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    # We zetten de benchmark iets lager (7.5) om te zien of er überhaupt iets doorkomt
    BENCHMARK_MIN = 7.5 
    
    for it in items:
        imdb = _num_or_none(it.get("imdbRating"))
        
        # FIX 2: De "Te hoge scores" filteren
        # uNoGS liegt soms. Als we een TMDb_id hebben, gebruiken we die enkel voor de score
        if it.get("tmdb_id"):
            try:
                tmdb_url = f"https://api.themoviedb.org/3/movie/{it['tmdb_id']}?api_key={TMDB_API_KEY}"
                # Als het een serie is, moet het /tv/ zijn
                if it["type"] == "series":
                    tmdb_url = tmdb_url.replace("/movie/", "/tv/")
                
                tmdb_res = requests.get(tmdb_url).json()
                # Gebruik de TMDb score als de IMDb score onrealistisch hoog is
                if tmdb_res.get("vote_average"):
                    imdb = round(tmdb_res["vote_average"], 1)
            except:
                pass

        if not imdb or imdb < BENCHMARK_MIN:
            continue

        # FIX 3: De "2020" Datum fix
        parsed_added = _parse_date(it.get("dateAdded"))
        final_date = parsed_added.isoformat() if parsed_added else "2024-01-01"

        out.append({
            "title": it["title"].replace("&#39;", "'"),
            "type": "Series" if it["type"] == "series" else "Film",
            "imdbRating": imdb,
            "traktRating": round(imdb * 0.9, 1), 
            "releaseDate": it["releaseDate"],
            "dateAdded": final_date,
            "tmdb_id": it.get("tmdb_id"),
        })
    return out
