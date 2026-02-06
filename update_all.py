Omdat je de git push uit de Python-code hebt gehaald (zeer verstandig voor de stabiliteit), moet je je .yml file als volgt instellen. Deze workflow geeft de Action de rechten om de JSON-bestanden en de caches terug naar je repository te schrijven.

YAML
name: Update Netflix Data
on:
  schedule:
    - cron: '0 4 * * *' # Elke nacht om 4u
  workflow_dispatch: # Knop om handmatig te starten

permissions:
  contents: write # Cruciaal voor het pushen van de data

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'
          
      - name: Install dependencies
        run: pip install requests
        
      - name: Run Scraper
        env:
          UNOGS_API_KEY: ${{ secrets.UNOGS_API_KEY }}
          TMDB_API_KEY: ${{ secrets.TMDB_API_KEY }}
          OMDB_API_KEY: ${{ secrets.OMDB_API_KEY }}
        run: python update_all.py
        
      - name: Commit and Push
        run: |
          git config --global user.name "Netflix Update Bot"
          git config --global user.email "bot@yourdomain.be"
          git add .
          # Alleen committen als er echt wijzigingen zijn
          git diff --quiet && git diff --staged --quiet || (git commit -m "Automated update: $(date +'%Y-%m-%d')" && git push)
Laatste tip:
Houd de stats in de console-output van je GitHub Actions in de gaten. Als avail_cache_miss elke dag heel hoog blijft, kun je de TTL_FALSE_DAYS wat verhogen om minder API-credits van uNoGS te verbruiken.
