def get_trakt_rating_via_tmdb(tmdb_id, media_type="movie"):
    """
    Haal Trakt-rating op via TMDb id.
    Retourneert float of None.
    """
    headers = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": TRAKT_API_KEY,
    }
    try:
        # 1) zoek mapping via tmdb id
        t = "movie" if media_type == "movie" else "show"
        url = f"https://api.trakt.tv/search/tmdb/{tmdb_id}?type={t}&limit=1"
        r = requests.get(url, headers=headers, timeout=20)
        if r.status_code != 200:
            return None
        arr = r.json()
        if not arr:
            return None

        slug = arr[0].get(t, {}).get("ids", {}).get("slug")
        if not slug:
            return None

        # 2) haal summary met rating
        url2 = f"https://api.trakt.tv/{t}s/{slug}?extended=full"
        r2 = requests.get(url2, headers=headers, timeout=20)
        if r2.status_code != 200:
            return None
        js = r2.json()
        rating = js.get("rating")
        if rating:
            return float(rating)
    except Exception:
        return None
    return None
