import requests
import json

# API-sleutels (vervang door je eigen sleutels als je registreert)
UNOGS_API_KEY = "YOUR_UNOGS_API_KEY"  # Vervang dit met jouw sleutel
OMDB_API_KEY = "YOUR_OMDB_API_KEY"    # Vervang dit met jouw sleutel

def fetch_netflix_titles():
    """Haalt titels op die beschikbaar zijn op Netflix in Vlaanderen."""
    url = "https://unogsng.p.rapidapi.com/search"
    headers = {
        "X-RapidAPI-Key": UNOGS_API_KEY,
        "X-RapidAPI-Host": "unogsng.p.rapidapi.com"
    }
    params = {
        "countrylist": "21",  # Code voor België
        "orderby": "date",
        "limit": 50,  # Max aantal titels
        "type": "movie,tvshow"
    }
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Netflix data: {e}")
        return []

def fetch_imdb_rating(title):
    """Haalt IMDb-rating op voor een titel."""
    url = f"http://www.omdbapi.com/?t={title}&apikey={OMDB_API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("imdbRating", "N/A")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching IMDb data for {title}: {e}")
        return "N/A"

def save_to_file(data, filename):
    """Slaat data op in een bestand."""
    try:
        with open(filename, "w") as file:
            json.dump(data, file, indent=4)
        print(f"Data saved to {filename}")
    except Exception as e:
        print(f"Error saving data to {filename}: {e}")

def main():
    print("Fetching Netflix titles...")
    netflix_titles = fetch_netflix_titles()
    if not netflix_titles:
        print(f"No Netflix titles fetched. Exiting.")
        return

    print(f"Fetched {len(netflix_titles)} titles from Netflix.")
    print("Fetching IMDb ratings...")
    for title in netflix_titles:
        print(f"Fetching IMDb rating for {title['title']}...")
        title["imdbRating"] = fetch_imdb_rating(title["title"])
        print(f"IMDb rating for {title['title']}: {title['imdbRating']}")

    save_to_file(netflix_titles, "netflix_data.json")

if __name__ == "__main__":
    main()

