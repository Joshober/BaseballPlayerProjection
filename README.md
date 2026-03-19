# MiLB Baseball-Reference Scraper

Simple Python scraper for MiLB player pages on Baseball-Reference. Handles BBRef's hidden tables (HTML comments), extracts batting and pitching tables and basic player metadata, and returns clean pandas DataFrames.

Installation
```
python -m pip install -r requirements.txt
```

Quick usage (programmatic)
```python
from milb_scraper import MiLBScraper
scraper = MiLBScraper(delay=2.5)
res = scraper.scrape_player('https://www.baseball-reference.com/register/player.fcgi?id=INSERT_ID')
print(res['metadata'])
print(res['batting'].head())
print(res['pitching'].head())
```

CLI usage
```
python milb_scraper.py "https://www.baseball-reference.com/register/player.fcgi?id=..."
```

Notes
- The scraper includes polite rate limiting and retry logic; adjust `delay` in `MiLBScraper` if scraping many pages.
- The HTML structure on Baseball-Reference can change; treat this as a robust but best-effort parser.
