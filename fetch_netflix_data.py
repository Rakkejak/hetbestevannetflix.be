import requests
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor
from imdb import IMDb
from imdb._exceptions import IMDbDataAccessError

# API-sleutels en regio
TMDB_API_KEY = "ca7bc87061528b91ac4b42e235851f9a"  # TMDb API-sleutel
TRAKT_CLIENT_ID = "1c0cbb71ec18b665a18642f01cac9c2a46f3a35435f505d46150eb403ccdcf1c"
REGION = "BE"  # Landcode voor België

# IMDbPy initialisatie
ia = IMDb(accessSystem='http', timeout=10)

# Helper functie om uitsluitingen te loggen
def log_exclusion(reason, title):
    """Logt uitsluitingen naar een bestand met de reden."""
    with open("exclusions.log", "a") as log_file:
        log_file.write(f"{title.get('name') or title.get('title')} ({title.get('release_date') or title.get('first_air_date')}) → {reason}\n")

def fetch_trakt_rating(title):
    """Haalt de Trakt.tv score en stemmen op."""
    url = f"https://api.trakt.tv/search/movie,show"
    headers = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": TRAKT_CLIENT_ID,
    }
    params = {
        "query": title,  # Zoek op titel
        "extended": "full"  # Haal volledige details op
    }
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        if response.status_code == 200:
            results = response.json()
            if results:
                for result in results:
                    # Zoek naar de juiste media (film of serie)
                    rating = result.get("movie", {}).get("rating") or result.get("show", {}).get("rating")
                    votes = result.get("movie", {}).get("votes") or result.get("show", {}).get("votes")
                    return rating, votes
        print(f"No Trakt.tv rating found for {title}.")
        return None, None
    except Exception as e:
        print(f"Error fetching Trakt.tv rating for {title}: {e}")
        return None, None


def fetch_imdb_rating(title, release_year=None, media_type=None):
    """Haalt de IMDb-rating en stemmen op."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            search_results = ia.search_movie(title)
            for result in search_results:
                if result.get('title').lower() == title.lower():
                    if media_type == "movie" and result.get('kind') != "movie":
                        continue
                    if media_type == "serie" and result.get('kind') != "tv series":
                        continue
                    if release_year and result.get('year') != release_year:
                        continue
                    movie = ia.get_movie(result.movieID, info=['main'])
                    rating = movie.get('rating', 'N/A')
                    votes = movie.get('votes', 0)
                    return rating, votes
        except IMDbDataAccessError as imdb_err:
            print(f"IMDb access error for {title}: {imdb_err}")
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {title}: {e}")
            time.sleep(2)
    print(f"Failed to fetch IMDb rating for {title}. Returning default 'N/A'.")
    return "N/A", 0


def process_title(title, media_type):
    """Verwerkt een titel met IMDb- en Trakt.tv-validatie."""
    try:
        release_year = None
        if title.get("release_date"):
            release_year = int(title["release_date"].split("-")[0])
        elif title.get("first_air_date"):
            release_year = int(title["first_air_date"].split("-")[0])

        imdb_rating, imdb_votes = fetch_imdb_rating(title.get("name") or title.get("title"), release_year, media_type)
        trakt_rating, trakt_votes = fetch_trakt_rating(title.get("name") or title.get("title"))

        # Allow titles with missing Trakt.tv scores if IMDb rating is above 8
        if imdb_rating == "N/A":
            log_exclusion("Missing IMDb score", title)
            return None

        if trakt_rating == "N/A":
            trakt_rating = "geen score"  # Use "geen score" when Trakt.tv score is missing
        elif abs(float(imdb_rating) - float(trakt_rating)) > 0.3 * float(imdb_rating):
            log_exclusion("Inconsistent scores", title)
            return None

        release_date = title.get("first_air_date") or title.get("release_date") or "Unknown"
        return {
            "title": title.get("name") or title.get("title"),
            "type": media_type,
            "imdbRating": imdb_rating,
            "traktRating": trakt_rating,
            "votes": max(imdb_votes, trakt_votes or 0),
            "releaseDate": release_date
        }
    except Exception as e:
        log_exclusion(f"Error processing title: {e}", title)
        return None


def fetch_netflix_movies():
    """Haalt ALLE films op die beschikbaar zijn op Netflix."""
    url = f"https://api.themoviedb.org/3/discover/movie"
    params = {
        "api_key": TMDB_API_KEY,
        "with_watch_providers": "8",
        "watch_region": REGION,
        "language": "en-US",
        "page": 1
    }
    all_movies = []
    while True:
        print(f"Fetching movies page {params['page']}...")
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            all_movies.extend(data.get("results", []))
            if params["page"] >= data.get("total_pages", 1):
                break
            params["page"] += 1
        else:
            print(f"Error fetching movies: {response.status_code}")
            break
    print(f"Total movies fetched: {len(all_movies)}")
    return all_movies


def fetch_netflix_series():
    """Haalt ALLE series op die beschikbaar zijn op Netflix."""
    url = f"https://api.themoviedb.org/3/discover/tv"
    params = {
        "api_key": TMDB_API_KEY,
        "with_watch_providers": "8",
        "watch_region": REGION,
        "language": "en-US",
        "page": 1
    }
    all_series = []
    while True:
        print(f"Fetching series page {params['page']}...")
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            all_series.extend(data.get("results", []))
            if params["page"] >= data.get("total_pages", 1):
                break
            params["page"] += 1
        else:
            print(f"Error fetching series: {response.status_code}")
            break
    print(f"Total series fetched: {len(all_series)}")
    return all_series


def save_to_file(data, filename):
    """Slaat data op in een JSON-bestand."""
    try:
        print(f"Saving {len(data)} items to {filename}...")
        with open(filename, "w") as f:
            json.dump(data, f, indent=4)
        print(f"Data successfully saved to {filename}")
    except Exception as e:
        print(f"Error saving data to {filename}: {e}")


def filter_last_month(titles):
    """Filters titles released in the last month."""
    one_month_ago = datetime.now() - timedelta(days=30)
    current_time = datetime.now()
    filtered_titles = []

    for title in titles:
        try:
            release_date = title.get("releaseDate")
            if not release_date:
                print(f"Skipping {title['title']} due to missing release date.")
                continue

            # Parse the release date using datetime
            try:
                release_datetime = datetime.strptime(release_date, "%Y-%m-%d")
            except ValueError:
                print(f"Skipping {title['title']} due to invalid release date format: {release_date}")
                continue

            # Include titles released in the last month or today
            if one_month_ago <= release_datetime <= current_time:
                filtered_titles.append(title)
            else:
                print(f"Excluding {title['title']} - Release date {release_date} is outside the last month.")
        except Exception as e:
            print(f"Error processing release date for {title['title']}: {e}")
            continue

    print(f"Filtered {len(filtered_titles)} titles from the last month.")
    return filtered_titles


def load_manual_scores(filename="manual_scores.json"):
    """Laadt handmatig toegevoegde scores uit een JSON-bestand."""
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Manual scores file '{filename}' not found. Skipping.")
        return []
    except Exception as e:
        print(f"Error loading manual scores: {e}")
        return []

def merge_manual_scores(processed_titles, manual_scores):
    """Voegt handmatige scores toe aan de verwerkte titels."""
    titles_dict = {title["title"]: title for title in processed_titles}
    for manual in manual_scores:
        if manual["title"] not in titles_dict:
            titles_dict[manual["title"]] = manual
        else:
            print(f"Skipping manual entry for '{manual['title']}' as it already exists in processed titles.")
    return list(titles_dict.values())

def push_to_github():
    """Automatically commits and pushes updates to GitHub."""
    try:
        os.system("git add netflix_data.json netflix_last_month.json")
        os.system('git commit -m "Auto-update Netflix data"')
        os.system("git push origin main")
        print("✅ Data pushed to GitHub!")
    except Exception as e:
        print(f"❌ Error pushing to GitHub: {e}")

def main():
    # Clear the exclusions log before each run
    open("exclusions.log", "w").close()

    movies = fetch_netflix_movies()
    series = fetch_netflix_series()

    with ThreadPoolExecutor(max_workers=10) as executor:
        processed_movies = list(executor.map(lambda x: process_title(x, "movie"), movies))
        processed_series = list(executor.map(lambda x: process_title(x, "serie"), series))

    all_titles = [x for x in processed_movies + processed_series if x]
    print(f"Total processed titles: {len(all_titles)}")

    manual_scores = load_manual_scores()
    all_titles = merge_manual_scores(all_titles, manual_scores)
    print(f"Total titles after merging manual scores: {len(all_titles)}")

    high_rated = [x for x in all_titles if x["imdbRating"] != "N/A" and float(x["imdbRating"]) > 8]
    print(f"High-rated titles (IMDb > 8.0):")
    for title in high_rated:
        print(f"- {title['title']} ({title['releaseDate']}) - IMDb: {title['imdbRating']}")

    save_to_file(high_rated, "netflix_data.json")
    save_to_file(filter_last_month(high_rated), "netflix_last_month.json")
    print("Data processing complete.")

    # Push the new JSON files to GitHub
    push_to_github()

if __name__ == "__main__":
    main()
