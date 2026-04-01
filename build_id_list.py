"""
build_id_list.py
================
Discovers BBRef register IDs from multiple sources and writes
them to player_ids.txt for use with collect_players.py.

Sources scraped:
  1. BBRef draft pages  -- 2005-2018 draft classes (~200-300 IDs each)
  2. BBRef minor-league leader pages  -- catches international signees
     not found in the US draft

Expected output size:  3,000-5,000 unique IDs
Expected runtime:      ~3 minutes (14 draft years x 3.5 s/request)

USAGE:
  python build_id_list.py                      # all 2005-2018 classes
  python build_id_list.py --years 2008 2012    # specific years only
  python build_id_list.py --include-leaders    # also scrape leader pages
  python build_id_list.py --append             # add to existing file

Then:
  python collect_players.py --id-file player_ids.txt --no-db --limit 50
"""

import argparse
import random
import re
import sys
import time
import logging
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1))],
)
log = logging.getLogger("build_ids")

BBREF_BASE    = "https://www.baseball-reference.com"
BBREF_DELAY   = 3.5
OUTPUT_FILE   = "player_ids.txt"
DEFAULT_YEARS = list(range(2005, 2019))


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": BBREF_BASE,
    })
    retry = Retry(total=4, backoff_factor=1.5,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=frozenset(["GET"]))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


_S = _session()

_PW_BROWSER = None
_PW_CONTEXT = None
_PW_PAGE    = None


def _pw_start():
    global _PW_BROWSER, _PW_CONTEXT, _PW_PAGE
    if _PW_PAGE is not None:
        return
    _pw = sync_playwright().start()
    _PW_BROWSER = _pw.chromium.launch(headless=True)
    _PW_CONTEXT = _PW_BROWSER.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        locale="en-US",
    )
    _PW_PAGE = _PW_CONTEXT.new_page()
    log.info("Playwright Chromium started")


def _pw_stop():
    global _PW_BROWSER, _PW_CONTEXT, _PW_PAGE
    if _PW_BROWSER:
        _PW_BROWSER.close()
        _PW_BROWSER = _PW_CONTEXT = _PW_PAGE = None


def _pw_get(url: str) -> str | None:
    _pw_start()
    time.sleep(BBREF_DELAY + random.uniform(0, BBREF_DELAY * 0.15))
    try:
        resp = _PW_PAGE.goto(url, wait_until="domcontentloaded", timeout=30_000)
        if resp and resp.status == 429:
            wait = int(resp.headers.get("retry-after", 90))
            log.warning(f"  429 -- sleeping {wait}s")
            time.sleep(wait)
            resp = _PW_PAGE.goto(url, wait_until="domcontentloaded", timeout=30_000)
        if resp and resp.status >= 400:
            log.error(f"  PW fetch {resp.status} for {url}")
            return None
        return _PW_PAGE.content()
    except Exception as e:
        log.error(f"  PW fetch failed {url}: {e}")
        return None


def _extract_ids(soup: BeautifulSoup, limit: int = 9999) -> list[str]:
    """Pull every register player ID from all links on the page."""
    ids: list[str] = []
    for a in soup.find_all("a", href=re.compile(r"/register/player\.fcgi\?id=")):
        m = re.search(r"id=([^&\"'\s]+)", a["href"])
        if m:
            pid = m.group(1).strip()
            if pid and pid not in ids:
                ids.append(pid)
                if len(ids) >= limit:
                    break
    return ids


def draft_class(year: int, limit: int = 300) -> list[str]:
    """IDs from one BBRef draft page."""
    log.info(f"  Draft {year}...")
    url = (f"{BBREF_BASE}/draft/?year_ID={year}"
           f"&draft_type=junreg&query_type=year_round")
    html = _pw_get(url)
    if not html:
        return []
    ids = _extract_ids(BeautifulSoup(html, "lxml"), limit)
    log.info(f"    -> {len(ids)} IDs")
    return ids


def milb_leaders(year: int, stat_type: str = "b", limit: int = 150) -> list[str]:
    """
    IDs from BBRef minor-league leader pages.
    stat_type: 'b' = batting (sort by PA), 'p' = pitching (sort by IP)
    """
    sort_stat = "pa" if stat_type == "b" else "ip"
    log.info(f"  MiLB leaders {year} ({'bat' if stat_type=='b' else 'pit'})...")
    url = (f"{BBREF_BASE}/register/leader.fcgi"
           f"?type={stat_type}&id={sort_stat}&year={year}")
    html = _pw_get(url)
    if not html:
        return []
    ids = _extract_ids(BeautifulSoup(html, "lxml"), limit)
    log.info(f"    -> {len(ids)} IDs")
    return ids


def main():
    ap = argparse.ArgumentParser(description="Build BBRef ID list for collect_players.py")
    ap.add_argument("--years", type=int, nargs="+", default=None,
                    help="Draft years (default: 2005-2018)")
    ap.add_argument("--include-leaders", action="store_true",
                    help="Also scrape MiLB leader pages (adds ~1,000 extra IDs)")
    ap.add_argument("--append", action="store_true",
                    help="Append to existing player_ids.txt instead of overwriting")
    ap.add_argument("--output", default=OUTPUT_FILE,
                    help=f"Output file (default: {OUTPUT_FILE})")
    args = ap.parse_args()

    years  = args.years or DEFAULT_YEARS
    all_ids: list[str] = []

    log.info(f"Scraping draft classes: {years[0]}-{years[-1]}")
    for yr in years:
        all_ids.extend(draft_class(yr))

    if args.include_leaders:
        log.info("Scraping MiLB leader pages (every other year to save requests)...")
        for yr in range(years[0], years[-1] + 1, 2):
            all_ids.extend(milb_leaders(yr, "b"))
            all_ids.extend(milb_leaders(yr, "p"))

    existing: set[str] = set()
    if args.append and Path(args.output).exists():
        with open(args.output) as f:
            existing = {ln.strip() for ln in f if ln.strip()}
        log.info(f"Existing IDs in {args.output}: {len(existing)}")

    seen = set(existing)
    new_ids = []
    for pid in all_ids:
        if pid not in seen:
            seen.add(pid)
            new_ids.append(pid)

    mode = "a" if args.append else "w"
    with open(args.output, mode, encoding="utf-8") as f:
        for pid in new_ids:
            f.write(pid + "\n")

    total = len(existing) + len(new_ids)
    hrs   = total * 4 / 3600

    log.info(f"\n{'='*50}")
    log.info(f"  New IDs written:  {len(new_ids):,}")
    log.info(f"  Total in file:    {total:,}")
    log.info(f"  Est. collect time: {hrs:.1f} h  ({total} players x 4s)")
    log.info(f"\n  Next step:")
    log.info(f"    python collect_players.py --id-file {args.output} --no-db --limit 50")
    log.info(f"{'='*50}\n")

    _pw_stop()


if __name__ == "__main__":
    main()
