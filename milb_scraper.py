"""MiLB Baseball-Reference scraper

Features:
- Handles tables hidden inside HTML comments (BBRef hides many MiLB tables)
- Extracts batting and pitching tables
- Gathers basic player metadata
- Rate limiting + retry handling
- Returns clean pandas DataFrames

Usage:
    from milb_scraper import MiLBScraper
    s = MiLBScraper(delay=2.5)
    data = s.scrape_player('https://www.baseball-reference.com/register/player.fcgi?id=...')

Outputs a dict: {
    'metadata': { ... },
    'batting': DataFrame or None,
    'pitching': DataFrame or None,
}
"""
from typing import Optional, Dict, Any, List
from io import StringIO
import json
from pathlib import Path
import re
import time
import random
from urllib.parse import quote_plus, urljoin, unquote
import requests
import pandas as pd
from bs4 import BeautifulSoup, Comment
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# optional playwright import; used for headless-browser fallback
try:
    from playwright.sync_api import sync_playwright
    _HAS_PLAYWRIGHT = True
except Exception:
    _HAS_PLAYWRIGHT = False


def _clean_numeric_col(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.replace(r"[^0-9.\-]", "", regex=True)
    return pd.to_numeric(s.replace("", pd.NA), errors="coerce")


def _strip_notes(val: Any) -> Any:
    if isinstance(val, str):
        return re.sub(r"[\*†#\+%]", "", val).strip()
    return val


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # remove repeated header rows where a row contains column names like 'Rk'
    if df.shape[0] == 0:
        return df
    first_col_name = df.columns[0]
    # drop rows that are clearly header repeats
    mask = ~(df.iloc[:, 0].astype(str).str.lower() == str(first_col_name).lower())
    if mask.sum() < len(mask):
        df = df[mask]
    try:
        df = df.applymap(_strip_notes)
    except Exception:
        df = df.copy()
        for c in df.columns:
            df[c] = df[c].apply(lambda v: _strip_notes(v) if not pd.isna(v) else v)
    df = df.reset_index(drop=True)

    # attempt to convert obvious numeric columns
    for col in df.columns:
        if col.lower() in ("name", "team", "lg", "school", "pos", "tm", "park"):
            continue
        # heuristic: convert columns that contain mostly numeric-like entries
        sample = df[col].dropna().astype(str).head(10).str.replace(r"[^0-9.\-\.]", "", regex=True)
        if sample.size and sample.str.len().gt(0).sum() / sample.size > 0.6:
            try:
                df[col] = _clean_numeric_col(df[col])
            except Exception:
                pass
    return df


class MiLBScraper:
    def __init__(self, delay: float = 3.0, retries: int = 3, backoff_factor: float = 0.3, user_agent: Optional[str] = None):
        self.delay = float(delay)
        self.session = requests.Session()
        # use a current browser User-Agent by default to reduce risk of 403s
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
        )
        self.session.headers.update({
            "User-Agent": self.user_agent,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Referer": "https://www.baseball-reference.com/",
        })

        retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor,
                      status_forcelist=(429, 500, 502, 503, 504), allowed_methods=frozenset(["GET"]))
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _rate_limit_sleep(self) -> None:
        jitter = random.random() * (self.delay * 0.3)
        time.sleep(self.delay + jitter)

    def fetch(self, url: str, timeout: int = 15) -> str:
        # primary: try lightweight requests
        try:
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            html = resp.text
            return html
        except requests.RequestException:
            # if blocked and Playwright is available, fall back to headless browser
            if _HAS_PLAYWRIGHT:
                try:
                    html = self.fetch_via_playwright(url)
                    return html
                except Exception as e:
                    raise RuntimeError(f"Both requests and Playwright failed fetching {url}: {e}")
            else:
                raise RuntimeError(f"HTTP error fetching {url} and Playwright not available")
        finally:
            # always delay to be polite
            self._rate_limit_sleep()

    def fetch_via_playwright(self, url: str, timeout: int = 30) -> str:
        """Use Playwright headless Chromium to fetch page content. Requires `playwright` package and browsers installed."""
        if not _HAS_PLAYWRIGHT:
            raise RuntimeError("Playwright is not installed in this environment")

        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-blink-features=AutomationControlled"])
            except Exception as e:
                err = str(e).lower()
                if "executable doesn't exist" in err or "browserType.launch" in err:
                    raise RuntimeError(
                        "Playwright browser binaries are missing. Install with: python -m playwright install chromium"
                    ) from e
                raise
            context = browser.new_context(user_agent=self.user_agent, locale="en-US")
            page = context.new_page()
            # set extra headers similar to session
            page.set_extra_http_headers({
                "Referer": "https://www.baseball-reference.com/",
                "Accept-Language": "en-US,en;q=0.9",
            })

            # block images/fonts to speed up and reduce chance of network hangs
            def _route_intercept(route):
                if route.request.resource_type in ("image", "font"):
                    return route.abort()
                return route.continue_()

            try:
                page.route("**/*", lambda route: _route_intercept(route))
            except Exception:
                pass

            # prefer DOMContentLoaded to avoid waiting for third-party trackers
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=max(60000, timeout * 1000))
            except Exception:
                # fallback to a simple navigation without waiting for networkidle
                page.goto(url, wait_until="load", timeout=max(60000, timeout * 1000))

            content = page.content()
            try:
                page.close()
            except Exception:
                pass
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass
            return content

    def search_bbref_register_pages(self, name: str, max_results: int = 15) -> List[Dict[str, Any]]:
        """Search Baseball-Reference for MiLB register player pages matching ``name``.

        Returns rows with ``url``, ``bbref_id``, and ``label`` (anchor text when available).
        """
        q = quote_plus(name.strip())
        search_url = f"https://www.baseball-reference.com/search/search.fcgi?search={q}"
        html = self.fetch(search_url)
        soup = BeautifulSoup(html, "html.parser")
        base = "https://www.baseball-reference.com"
        seen: set[str] = set()
        out: List[Dict[str, Any]] = []

        def push(href: str, label: str) -> None:
            if not href or "register/player.fcgi" not in href:
                return
            full = urljoin(base, href)
            m = re.search(r"[?&]id=([^&]+)", full)
            if not m:
                return
            bid = unquote(m.group(1))
            if bid in seen:
                return
            seen.add(bid)
            out.append(
                {
                    "url": full.split("#")[0],
                    "bbref_id": bid,
                    "label": label.strip() or bid,
                }
            )

        for a in soup.select('a[href*="register/player.fcgi"]'):
            push(a.get("href") or "", a.get_text(" ", strip=True) or "")

        if not out:
            for m in re.finditer(r'href="([^"]*register/player\.fcgi[^"]*)"', html, re.I):
                push(m.group(1), "")

        return out[:max_results]

    def _extract_comment_tables(self, soup: BeautifulSoup) -> List[BeautifulSoup]:
        tables = []
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            txt = str(comment)
            if "<table" in txt:
                try:
                    csoup = BeautifulSoup(txt, "lxml")
                    tables.extend(csoup.find_all("table"))
                except Exception:
                    continue
        return tables

    def _gather_tables(self, html: str) -> Dict[str, List[pd.DataFrame]]:
        soup = BeautifulSoup(html, "lxml")
        collected = []
        # visible tables
        collected.extend(soup.find_all("table"))
        # hidden inside comments
        collected.extend(self._extract_comment_tables(soup))

        # deduplicate by id / string
        seen = set()
        unique_tables = []
        for t in collected:
            key = t.get("id") or (str(t)[:300])
            if key in seen:
                continue
            seen.add(key)
            unique_tables.append(t)

        batting_dfs: List[pd.DataFrame] = []
        pitching_dfs: List[pd.DataFrame] = []

        for t in unique_tables:
            caption = t.caption.string if t.caption and t.caption.string else ""
            th_text = " ".join([th.get_text(strip=True).lower() for th in t.find_all("th")])
            identifier = (t.get("id") or "") + " " + caption + " " + th_text
            try:
                df = pd.read_html(StringIO(str(t)), flavor="bs4")[0]
            except ValueError:
                try:
                    df = pd.read_html(StringIO(str(t)))[0]
                except Exception:
                    continue

            ident_low = identifier.lower()
            if any(k in ident_low for k in ("batting", "bat", "batters", "batter", "pa", "rbi", "hr", "avg", "obp", "slg")):
                batting_dfs.append(_clean_dataframe(df))
            elif any(k in ident_low for k in ("pitching", "pitch", "pitchers", "era", "ip", "so", "bb", "era+")):
                pitching_dfs.append(_clean_dataframe(df))
            else:
                # fallback heuristics based on headers
                headers = ",".join([c.lower() for c in df.columns.astype(str)])
                if any(h in headers for h in ("era", "ip", "so", "bb")):
                    pitching_dfs.append(_clean_dataframe(df))
                elif any(h in headers for h in ("rbi", "hr", "pa", "ab", "avg")):
                    batting_dfs.append(_clean_dataframe(df))

        return {"batting": batting_dfs, "pitching": pitching_dfs, "soup": soup}

    def _extract_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        meta = {}
        # name
        name_tag = soup.find("h1")
        meta["name"] = name_tag.get_text(" ", strip=True) if name_tag else None

        meta_block = soup.find(id="meta") or soup.find("div", class_=re.compile(r"player"))
        text = meta_block.get_text(" | ", strip=True) if meta_block else ""

        # regex pulls
        m = re.search(r"Born:?\s*([^|†\n]+)", text, re.I)
        if m:
            meta["born"] = m.group(1).strip()
        m = re.search(r"Bats:?\s*([RLS])", text, re.I)
        if m:
            meta["bats"] = m.group(1).upper()
        m = re.search(r"Throws:?\s*([RLS])", text, re.I)
        if m:
            meta["throws"] = m.group(1).upper()
        # position
        m = re.search(r"Position[s]?:?\s*([^|†\n]+)", text, re.I)
        if m:
            meta["position"] = m.group(1).strip()

        return meta

    def scrape_player(self, url: str) -> Dict[str, Any]:
        """Fetches a player page and returns metadata and cleaned batting/pitching DataFrames.

        Returns dict with keys: 'metadata', 'batting', 'pitching'. Each of batting/pitching is either a
        single DataFrame (concatenated if multiple tables found) or None.
        """
        html = self.fetch(url)
        result = self._gather_tables(html)
        soup = result.get("soup")

        metadata = self._extract_metadata(soup)

        batting_list = result.get("batting", [])
        pitching_list = result.get("pitching", [])

        batting_df = None
        pitching_df = None

        if batting_list:
            try:
                batting_df = pd.concat(batting_list, ignore_index=True, sort=False)
                batting_df = _clean_dataframe(batting_df)
            except Exception:
                batting_df = batting_list[0]
        if pitching_list:
            try:
                pitching_df = pd.concat(pitching_list, ignore_index=True, sort=False)
                pitching_df = _clean_dataframe(pitching_df)
            except Exception:
                pitching_df = pitching_list[0]

        return {"metadata": metadata, "batting": batting_df, "pitching": pitching_df}

    def save_results(self, data: Dict[str, Any], out_dir: str = "output", prefix: Optional[str] = None, csv: bool = True, json_out: bool = True) -> Dict[str, str]:
        """Save scraped results to `out_dir`.

        - `data` is the dict returned by `scrape_player`
        - `prefix` is optional filename prefix (e.g., player id)
        - returns a dict of written file paths
        """
        p = Path(out_dir)
        p.mkdir(parents=True, exist_ok=True)
        prefix = prefix or "player"
        written: Dict[str, str] = {}

        # metadata
        if json_out and data.get("metadata") is not None:
            meta_path = p / f"{prefix}_metadata.json"
            with open(meta_path, "w", encoding="utf-8") as fh:
                json.dump(data["metadata"], fh, ensure_ascii=False, indent=2)
            written["metadata"] = str(meta_path)

        # batting
        bat = data.get("batting")
        if bat is not None:
            if csv:
                bat_path = p / f"{prefix}_batting.csv"
                bat.to_csv(bat_path, index=False)
                written["batting_csv"] = str(bat_path)
            if json_out:
                bat_json = p / f"{prefix}_batting.json"
                try:
                    bat.to_json(bat_json, orient="records", force_ascii=False)
                    written["batting_json"] = str(bat_json)
                except Exception:
                    # fallback: convert to list of dicts
                    with open(bat_json, "w", encoding="utf-8") as fh:
                        json.dump(bat.to_dict(orient="records"), fh, ensure_ascii=False, indent=2)
                    written["batting_json"] = str(bat_json)

        # pitching
        pit = data.get("pitching")
        if pit is not None:
            if csv:
                pit_path = p / f"{prefix}_pitching.csv"
                pit.to_csv(pit_path, index=False)
                written["pitching_csv"] = str(pit_path)
            if json_out:
                pit_json = p / f"{prefix}_pitching.json"
                try:
                    pit.to_json(pit_json, orient="records", force_ascii=False)
                    written["pitching_json"] = str(pit_json)
                except Exception:
                    with open(pit_json, "w", encoding="utf-8") as fh:
                        json.dump(pit.to_dict(orient="records"), fh, ensure_ascii=False, indent=2)
                    written["pitching_json"] = str(pit_json)

        return written


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scrape MiLB player page from Baseball-Reference")
    parser.add_argument("url", help="Player page URL (e.g. https://www.baseball-reference.com/register/player.fcgi?id=...)")
    parser.add_argument("--out", help="Output directory prefix to save CSV/JSON (optional)")
    parser.add_argument("--delay", type=float, default=2.5, help="Seconds between requests (default 2.5)")
    args = parser.parse_args()

    s = MiLBScraper(delay=args.delay)
    out = s.scrape_player(args.url)
    print("Metadata:")
    print(out["metadata"])
    print("\nBatting: ")
    print(out["batting"].head() if out["batting"] is not None else "None")
    print("\nPitching: ")
    print(out["pitching"].head() if out["pitching"] is not None else "None")
    if getattr(args, "out", None):
        prefix = None
        # if URL has id=... try to extract id for file prefix
        m = re.search(r"[?&]id=([^&]+)", args.url)
        if m:
            prefix = m.group(1)
        written = s.save_results(out, out_dir=args.out, prefix=prefix)
        print("\nSaved files:")
        for k, v in written.items():
            print(f"- {k}: {v}")
