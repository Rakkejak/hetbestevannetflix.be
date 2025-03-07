from imdb import IMDb

ia = IMDb()

def fetch_imdb_rating(title):
    """Zoekt een titel op IMDb en haalt de rating op."""
    search_results = ia.search_movie(title)
    if search_results:
        movie = ia.get_movie(search_results[0].movieID)
        return movie.get('rating', 'N/A')
    return "N/A"

# Test de functie met een voorbeeldtitel
if __name__ == "__main__":
    title = "Inception"
    rating = fetch_imdb_rating(title)
    print(f"IMDb Rating for '{title}': {rating}")

