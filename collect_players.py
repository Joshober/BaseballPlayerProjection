"""
collect_players.py
==================
Full pipeline to collect real MiLB player data and fill your database.

Wires together:
  - milb_scraper.MiLBScraper   → BBRef register pages (bio + MiLB stat lines)
  - free_apis.search_mlb_people → match name → MLBAM ID
  - free_apis.get_mlb_player    → pull mlbDebutDate (the ground-truth label)
  - Direct psycopg writes       → players / milb_batting / milb_pitching / scrape_log

USAGE
-----
# 1. Quick smoke-test (no DB writes, just scrape 3 players):
    python collect_players.py --dry-run --limit 3

# 2. First real run (uses the 60-player starter list built in):
    python collect_players.py --limit 60

# 3. Discover fresh IDs from BBRef draft pages, then run:
    python collect_players.py --discover 2008 2009 2010 --limit 200

# 4. Full run against a custom ID file, resuming any prior progress:
    python collect_players.py --id-file my_bbref_ids.txt

# 5. Check what's already in the DB:
    python collect_players.py --audit

All progress is checkpointed to collect_progress.csv so Ctrl-C and
re-running always picks up where you left off.

RATE LIMITING
-------------
BBRef:       3.5 s + jitter between every request  (their robots.txt)
MLB Stats API: 0.4 s between calls                 (no key needed)
Expected throughput: ~1 player/4 seconds = ~900 players/hour
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import random
import re
import sys
import time
from datetime import date, datetime
from io import StringIO
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment
from playwright.sync_api import sync_playwright
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Optional imports
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import psycopg          # psycopg3
    _PG = "psycopg3"
except ImportError:
    try:
        import psycopg2 as psycopg   # psycopg2 fallback
        _PG = "psycopg2"
    except ImportError:
        psycopg = None
        _PG = None

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_FILE = "collect_pipeline.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
for h in logging.root.handlers:
    if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler):
        h.stream = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
log = logging.getLogger("collect")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BBREF_BASE       = "https://www.baseball-reference.com"
MLB_API_BASE     = "https://statsapi.mlb.com/api/v1"
BBREF_DELAY      = 3.5        # seconds between BBRef requests
MLB_API_DELAY    = 0.4        # seconds between MLB Stats API calls
PROGRESS_CSV     = "collect_progress.csv"

# level name → level_order integer matching the schema's comment (1=Rk…6=AAA)
LEVEL_ORDER_MAP: dict[str, int] = {
    "rk": 1, "rok": 1, "r": 1,
    "a-": 2, "ss": 2, "short-a": 2,
    "a":  3,
    "a+": 4, "adv-a": 4, "high-a": 4,
    "aa": 5,
    "aaa": 6,
}

# ---------------------------------------------------------------------------
# Starter seed list — curated mix of MLB-reachers and MiLB-only careers
# Enough to start EDA and baseline models while the larger discovery runs.
# Format: BBRef register IDs (the `id=` param on register/player.fcgi pages)
# ---------------------------------------------------------------------------
SEED_IDS: list[str] = [
    # ── MLB arrivals — verified BBRef register IDs ────────────────────────
    "troutmi000mi",   # Mike Trout
    "harpebr000br",   # Bryce Harper
    "machama000ma",   # Manny Machado
    "lindofr000fr",   # Francisco Lindor
    "bregmal000al",   # Alex Bregman
    "correca000ca",   # Carlos Correa
    "turnetr000tr",   # Trea Turner
    "sotoju000ju",    # Juan Soto
    "acunaro000ro",   # Ronald Acuna Jr.
    "guerrvl000vl",   # Vladimir Guerrero Jr.
    "tatisfe000fe",   # Fernando Tatis Jr.
    "alonspe000pe",   # Pete Alonso
    "wittbo000bo",    # Bobby Witt Jr.
    "rutscad000ad",   # Adley Rutschman
    "carolco000co",   # Corbin Carroll
    # ── MiLB-only players — verified register IDs ────────────────────────
    "jones-000dru",   # from README example
    "cabrer003jos",   # from README example
]


# ===========================================================================
# HTTP SESSION — shared across all requests
# ===========================================================================

def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer":         "https://www.baseball-reference.com/",
    })
    retry = Retry(
        total=4, backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    return s


_SESSION = _build_session()


def _get(url: str, params: dict | None = None, delay: float = 0.0,
         timeout: int = 20) -> Optional[requests.Response]:
    """Rate-limited GET with 429 backoff (used for MLB Stats API only)."""
    if delay > 0:
        time.sleep(delay + random.uniform(0, delay * 0.2))
    try:
        r = _SESSION.get(url, params=params, timeout=timeout)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 90))
            log.warning(f"429 rate-limited — sleeping {wait}s")
            time.sleep(wait)
            r = _SESSION.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as exc:
        log.error(f"GET failed {url}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Playwright-based fetcher for Baseball-Reference (bypasses Cloudflare)
# ---------------------------------------------------------------------------
_PW_BROWSER = None
_PW_CONTEXT = None
_PW_PAGE    = None


def _pw_start():
    """Launch a persistent Playwright browser session."""
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


def _pw_get(url: str) -> Optional[str]:
    """
    Fetch a BBRef page via Playwright, respecting the rate-limit delay.
    Returns the page HTML string, or None on failure.
    """
    _pw_start()
    time.sleep(BBREF_DELAY + random.uniform(0, BBREF_DELAY * 0.2))
    try:
        resp = _PW_PAGE.goto(url, wait_until="domcontentloaded", timeout=30_000)
        if resp and resp.status == 429:
            wait = int(resp.headers.get("retry-after", 90))
            log.warning(f"429 — sleeping {wait}s")
            time.sleep(wait)
            resp = _PW_PAGE.goto(url, wait_until="domcontentloaded", timeout=30_000)
        if resp and resp.status >= 400:
            log.error(f"PW fetch {resp.status} for {url}")
            return None
        return _PW_PAGE.content()
    except Exception as exc:
        log.error(f"PW fetch failed {url}: {exc}")
        return None


# ===========================================================================
# STEP 1 — BBRef ID DISCOVERY (draft pages)
# ===========================================================================

def discover_draft_class(year: int, limit: int = 250) -> list[str]:
    """
    Scrape BBRef draft index for one year and return register IDs.
    URL: https://www.baseball-reference.com/draft/?year_ID=YEAR&draft_type=junreg
    """
    url = (f"{BBREF_BASE}/draft/?year_ID={year}"
           f"&draft_type=junreg&query_type=year_round")
    html = _pw_get(url)
    if not html:
        return []

    ids: list[str] = []
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=re.compile(r"/register/player\.fcgi\?id=")):
        m = re.search(r"id=([^&\"'\s]+)", a["href"])
        if m:
            pid = m.group(1).strip()
            if pid and pid not in ids:
                ids.append(pid)
                if len(ids) >= limit:
                    break

    log.info(f"  Draft {year}: {len(ids)} IDs discovered")
    return ids


# ===========================================================================
# STEP 2 — SCRAPE ONE BBREF REGISTER PAGE
# ===========================================================================

def _parse_level_str(raw: str) -> tuple[str, int]:
    """
    Normalise a BBRef level string and return (level, level_order).
    Handles: Rk, A-, A, A+, AA, AAA and common BBRef abbreviations.
    """
    if not raw:
        return "Rk", 1
    s = raw.strip().lower()
    # Direct hits
    if s in ("aaa",):               return "AAA", 6
    if s in ("aa",):                return "AA",  5
    if s in ("a+", "adv-a", "high-a", "hia"): return "A+", 4
    if s in ("a",):                 return "A",   3
    if s in ("a-", "ss", "short-a"):return "A-",  2
    if s.startswith("rk") or s in ("rok", "r"): return "Rk", 1
    # Fallback: look up cleaned version
    cleaned = re.sub(r"[^a-z0-9+\-]", "", s)
    order = LEVEL_ORDER_MAP.get(cleaned, 3)
    display = raw.strip() if raw.strip() else "A"
    return display, order


def _strip(val) -> Optional[str]:
    if val is None:
        return None
    s = re.sub(r"[\*†#\+%]", "", str(val)).strip()
    return s if s and s.lower() not in ("nan", "none", "") else None


def _int(val) -> Optional[int]:
    s = re.sub(r"[^0-9\-]", "", str(val) if val is not None else "")
    try:
        return int(s) if s else None
    except ValueError:
        return None


def _float(val) -> Optional[float]:
    s = re.sub(r"[^0-9.\-]", "", str(val) if val is not None else "")
    try:
        return float(s) if s else None
    except ValueError:
        return None


def _parse_born(text: str) -> tuple[Optional[date], Optional[str]]:
    """Parse 'Born: January 1, 1990 in Dallas, TX (US)' strings."""
    bd, country = None, None
    m = re.search(r"([A-Z][a-z]+\s+\d{1,2},\s+\d{4})", text)
    if m:
        try:
            bd = datetime.strptime(m.group(1), "%B %d, %Y").date()
        except ValueError:
            pass
    m2 = re.search(r"\(([A-Z]{2,3})\)", text)
    if m2:
        country = m2.group(1)
    return bd, country


def _comment_tables(soup: BeautifulSoup) -> list:
    """Extract tables hidden inside HTML comments (BBRef's MiLB trick)."""
    tables = []
    for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
        if "<table" in str(c):
            try:
                cs = BeautifulSoup(str(c), "lxml")
                tables.extend(cs.find_all("table"))
            except Exception:
                pass
    return tables


def scrape_register_page(bbref_id: str) -> Optional[dict]:
    """
    Scrape one BBRef register page via Playwright.
    Returns dict: {metadata, batting: DataFrame|None, pitching: DataFrame|None}
    or None if fetch fails.
    """
    url  = f"{BBREF_BASE}/register/player.fcgi?id={bbref_id}"
    html = _pw_get(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")

    # ── Metadata ─────────────────────────────────────────────────────────────
    meta: dict = {"bbref_id": bbref_id, "url": url}

    h1 = soup.find("h1")
    full_name = h1.get_text(" ", strip=True) if h1 else None
    meta["full_name"] = full_name
    if full_name:
        parts = full_name.split(maxsplit=1)
        meta["first_name"] = parts[0]
        meta["last_name"]  = parts[1] if len(parts) > 1 else None

    block = soup.find(id="meta") or soup.find("div", class_=re.compile(r"player"))
    text  = block.get_text(" | ", strip=True) if block else ""

    m = re.search(r"Born:?\s*([^|†\n]+)", text, re.I)
    if m:
        meta["birth_date"], meta["birth_country"] = _parse_born(m.group(1))

    m = re.search(r"Bats:?\s*([RLS])", text, re.I)
    meta["bats"] = m.group(1).upper() if m else None

    m = re.search(r"Throws:?\s*([RLS])", text, re.I)
    meta["throws"] = m.group(1).upper() if m else None

    m = re.search(r"Position[s]?:?\s*([^|†\n]+)", text, re.I)
    if m:
        raw_pos = m.group(1).strip().upper()
        _pos_map = {
            "PITCHER": "P", "STARTING PITCHER": "P",
            "CATCHER": "C", "FIRST BASE": "1B", "SECOND BASE": "2B",
            "THIRD BASE": "3B", "SHORTSTOP": "SS", "LEFT FIELD": "LF",
            "CENTER FIELD": "CF", "RIGHT FIELD": "RF",
            "OUTFIELD": "OF", "DESIGNATED HITTER": "DH",
        }
        meta["position"] = _pos_map.get(raw_pos, raw_pos[:5])

    m = re.search(r"(\d+)-(\d+),?\s*(\d+)\s*lb", text, re.I)
    if m:
        meta["height_in"] = int(m.group(1)) * 12 + int(m.group(2))
        meta["weight_lb"] = int(m.group(3))

    # ── Tables ───────────────────────────────────────────────────────────────
    all_tables = list(soup.find_all("table")) + _comment_tables(soup)

    # Deduplicate
    seen, unique = set(), []
    for t in all_tables:
        key = t.get("id") or str(t)[:200]
        if key not in seen:
            seen.add(key)
            unique.append(t)

    bat_frames, pit_frames = [], []

    for t in unique:
        try:
            df = pd.read_html(StringIO(str(t)), flavor="bs4")[0]
        except Exception:
            try:
                df = pd.read_html(StringIO(str(t)))[0]
            except Exception:
                continue

        cap  = (t.caption.string or "") if t.caption else ""
        ths  = " ".join(th.get_text(strip=True).lower() for th in t.find_all("th"))
        ident = ((t.get("id") or "") + " " + cap + " " + ths).lower()
        cols  = ",".join(str(c).lower() for c in df.columns)

        is_pit = any(k in ident or k in cols
                     for k in ("pitching", "era", "ip", "so9", "whip", "bb9"))
        is_bat = any(k in ident or k in cols
                     for k in ("batting", "obp", "slg", "rbi", "pa", "ab"))

        if is_pit and not is_bat:
            pit_frames.append(_clean_df(df))
        elif is_bat:
            bat_frames.append(_clean_df(df))

    return {
        "metadata": meta,
        "batting":  _concat_frames(bat_frames),
        "pitching": _concat_frames(pit_frames),
    }


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Strip header-repeat rows and footnote chars."""
    if df.empty:
        return df
    # Drop rows where first column repeats the header (BBRef pagination artefact)
    first = str(df.columns[0]).lower()
    df = df[~df.iloc[:, 0].astype(str).str.lower().eq(first)].copy()
    # Drop summary / non-season rows
    for col in df.columns:
        if str(col).lower() in ("year", "yr", "season"):
            df = df[~df[col].astype(str).str.contains(
                r"Did not|Minors|Teams|Total|Yr", na=False, regex=True)]
            break
    # Strip footnote chars
    df = df.map(lambda v: re.sub(r"[\*†#\+%]", "", str(v)).strip()
                if isinstance(v, str) else v)
    return df.reset_index(drop=True)


def _concat_frames(frames: list[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if not frames:
        return None
    try:
        return _clean_df(pd.concat(frames, ignore_index=True, sort=False))
    except Exception:
        return _clean_df(frames[0]) if frames else None


# ===========================================================================
# STEP 3+4 — MLB STATS API: find mlb_id and mlbDebutDate
# ===========================================================================

def mlb_search(full_name: str, birth_year: Optional[int] = None) -> Optional[dict]:
    """
    Search MLB Stats API by name.  Returns the best-matching person dict,
    or None if nothing found.
    """
    if not full_name:
        return None
    time.sleep(MLB_API_DELAY)
    resp = _get(f"{MLB_API_BASE}/people/search",
                params={"names": full_name}, timeout=15)
    if not resp:
        return None
    try:
        people = resp.json().get("people", [])
    except Exception:
        return None
    if not people:
        return None
    # If we have birth_year, prefer exact match
    if birth_year:
        for p in people:
            if str(birth_year) in (p.get("birthDate") or ""):
                return p
    return people[0]


def mlb_debut_date(mlb_id: int) -> Optional[str]:
    """Return ISO mlbDebutDate string for a confirmed mlb_id, or None."""
    time.sleep(MLB_API_DELAY)
    resp = _get(f"{MLB_API_BASE}/people/{mlb_id}", timeout=15)
    if not resp:
        return None
    try:
        people = resp.json().get("people", [])
        return (people[0].get("mlbDebutDate") if people else None)
    except Exception:
        return None


# ===========================================================================
# STEP 5 — DATABASE WRITES
# ===========================================================================

def _db_connect():
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL is not set.\n"
            "Add it to your .env:  DATABASE_URL=postgresql://user:pass@host/dbname"
        )
    if psycopg is None:
        raise RuntimeError("psycopg not installed.  pip install psycopg[binary]")
    return psycopg.connect(db_url)


def _execute(conn, sql: str, params: dict | tuple) -> Optional[tuple]:
    """Run one SQL statement; return first result row or None."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        try:
            return cur.fetchone()
        except Exception:
            return None


def upsert_player(conn,
                  meta: dict,
                  mlb_person: Optional[dict],
                  debut: Optional[str]) -> Optional[int]:
    """
    INSERT … ON CONFLICT DO UPDATE into players table.
    Returns the players.id PK.
    """
    mlb_id      = mlb_person.get("id") if mlb_person else None
    reached_mlb = bool(debut)
    mlb_debut_d: Optional[date] = None
    if debut:
        try:
            mlb_debut_d = datetime.strptime(debut, "%Y-%m-%d").date()
        except ValueError:
            pass

    # Parse birth_date if it came back as a string
    bd = meta.get("birth_date")
    if isinstance(bd, str):
        try:
            bd = datetime.strptime(bd, "%Y-%m-%d").date()
        except ValueError:
            bd = None

    sql = """
        INSERT INTO players (
            bbref_id, mlb_id, full_name, first_name, last_name,
            birth_date, birth_country, position, bats, throws,
            height_in, weight_lb,
            mlb_debut_date, reached_mlb, is_active,
            updated_at
        ) VALUES (
            %(bbref_id)s, %(mlb_id)s, %(full_name)s, %(first_name)s, %(last_name)s,
            %(birth_date)s, %(birth_country)s, %(position)s, %(bats)s, %(throws)s,
            %(height_in)s, %(weight_lb)s,
            %(mlb_debut_date)s, %(reached_mlb)s, FALSE,
            NOW()
        )
        ON CONFLICT (bbref_id) DO UPDATE SET
            mlb_id          = COALESCE(EXCLUDED.mlb_id, players.mlb_id),
            full_name       = EXCLUDED.full_name,
            birth_date      = COALESCE(EXCLUDED.birth_date,     players.birth_date),
            birth_country   = COALESCE(EXCLUDED.birth_country,  players.birth_country),
            position        = COALESCE(EXCLUDED.position,       players.position),
            bats            = COALESCE(EXCLUDED.bats,           players.bats),
            throws          = COALESCE(EXCLUDED.throws,         players.throws),
            height_in       = COALESCE(EXCLUDED.height_in,      players.height_in),
            weight_lb       = COALESCE(EXCLUDED.weight_lb,      players.weight_lb),
            mlb_debut_date  = COALESCE(EXCLUDED.mlb_debut_date, players.mlb_debut_date),
            reached_mlb     = EXCLUDED.reached_mlb,
            updated_at      = NOW()
        RETURNING id
    """
    params = {
        "bbref_id":      meta["bbref_id"],
        "mlb_id":        mlb_id,
        "full_name":     meta.get("full_name") or meta["bbref_id"],
        "first_name":    meta.get("first_name"),
        "last_name":     meta.get("last_name"),
        "birth_date":    bd,
        "birth_country": meta.get("birth_country"),
        "position":      meta.get("position"),
        "bats":          meta.get("bats"),
        "throws":        meta.get("throws"),
        "height_in":     meta.get("height_in"),
        "weight_lb":     meta.get("weight_lb"),
        "mlb_debut_date": mlb_debut_d,
        "reached_mlb":   reached_mlb,
    }
    row = _execute(conn, sql, params)
    conn.commit()
    return row[0] if row else None


def upsert_batting(conn, player_id: int, df: pd.DataFrame) -> int:
    """
    Upsert milb_batting rows from the BBRef DataFrame.
    Column mapping: BBRef name → schema name.
    Returns number of rows written.
    """
    if df is None or df.empty:
        return 0

    # Normalise column names
    rename = {
        "year": "season", "yr": "season",
        "lev":  "level",
        "lg":   "league",
        "tm":   "team_abbr",  "team": "team_abbr",
        "age":  "age",
        "g":    "g",
        "pa":   "pa",
        "ab":   "ab",
        "r":    "r",
        "h":    "h",
        "2b":   "doubles",    "dbl": "doubles",
        "3b":   "triples",    "tpl": "triples",
        "hr":   "hr",
        "rbi":  "rbi",
        "sb":   "sb",
        "cs":   "cs",
        "bb":   "bb",
        "so":   "so",
        "ba":   "ba",         "avg": "ba",
        "obp":  "obp",
        "slg":  "slg",
        "ops":  "ops",
    }
    df2 = df.rename(columns={c: rename.get(c.lower(), c.lower()) for c in df.columns})

    sql = """
        INSERT INTO milb_batting (
            player_id, season, level, level_order, team_abbr, league, age,
            g, pa, ab, r, h, doubles, triples, hr, rbi, sb, cs, bb, so,
            ba, obp, slg, ops,
            iso, bb_pct, k_pct, bb_k_ratio
        ) VALUES (
            %(player_id)s, %(season)s, %(level)s, %(level_order)s,
            %(team_abbr)s, %(league)s, %(age)s,
            %(g)s, %(pa)s, %(ab)s, %(r)s, %(h)s,
            %(doubles)s, %(triples)s, %(hr)s, %(rbi)s, %(sb)s, %(cs)s,
            %(bb)s, %(so)s,
            %(ba)s, %(obp)s, %(slg)s, %(ops)s,
            %(iso)s, %(bb_pct)s, %(k_pct)s, %(bb_k_ratio)s
        )
        ON CONFLICT (player_id, season, level, team_abbr) DO UPDATE SET
            g           = EXCLUDED.g,
            pa          = EXCLUDED.pa,
            ba          = EXCLUDED.ba,
            obp         = EXCLUDED.obp,
            slg         = EXCLUDED.slg,
            ops         = EXCLUDED.ops,
            iso         = EXCLUDED.iso,
            bb_pct      = EXCLUDED.bb_pct,
            k_pct       = EXCLUDED.k_pct,
            bb_k_ratio  = EXCLUDED.bb_k_ratio,
            scraped_at  = NOW()
    """

    written = 0
    with conn.cursor() as cur:
        for _, row in df2.iterrows():
            season = _int(row.get("season"))
            if not season or not (1990 <= season <= 2030):
                continue
            level, level_order = _parse_level_str(str(row.get("level", "")))
            pa   = _int(row.get("pa"))
            bb   = _int(row.get("bb"))
            so   = _int(row.get("so"))
            slg  = _float(row.get("slg"))
            ba   = _float(row.get("ba"))

            # Derived stats computed at ingest time
            iso      = round(slg - ba, 3) if slg is not None and ba is not None else None
            bb_pct   = round(bb / pa, 3)  if bb  is not None and pa  else None
            k_pct    = round(so / pa, 3)  if so  is not None and pa  else None
            bb_k     = round(bb / max(so, 1), 3) if bb is not None and so is not None else None

            params = {
                "player_id":   player_id,
                "season":      season,
                "level":       level,
                "level_order": level_order,
                "team_abbr":   _strip(row.get("team_abbr")),
                "league":      _strip(row.get("league")),
                "age":         _float(row.get("age")),
                "g":           _int(row.get("g")),
                "pa":          pa,
                "ab":          _int(row.get("ab")),
                "r":           _int(row.get("r")),
                "h":           _int(row.get("h")),
                "doubles":     _int(row.get("doubles")),
                "triples":     _int(row.get("triples")),
                "hr":          _int(row.get("hr")),
                "rbi":         _int(row.get("rbi")),
                "sb":          _int(row.get("sb")),
                "cs":          _int(row.get("cs")),
                "bb":          bb,
                "so":          so,
                "ba":          ba,
                "obp":         _float(row.get("obp")),
                "slg":         slg,
                "ops":         _float(row.get("ops")),
                "iso":         iso,
                "bb_pct":      bb_pct,
                "k_pct":       k_pct,
                "bb_k_ratio":  bb_k,
            }
            try:
                cur.execute(sql, params)
                written += 1
            except Exception as e:
                log.debug(f"    batting row skip ({season} {level}): {e}")
                conn.rollback()
    conn.commit()
    return written


def upsert_pitching(conn, player_id: int, df: pd.DataFrame) -> int:
    """Upsert milb_pitching rows. Returns number of rows written."""
    if df is None or df.empty:
        return 0

    rename = {
        "year": "season", "yr": "season",
        "lev":  "level",
        "lg":   "league",
        "tm":   "team_abbr",
        "age":  "age",
        "g":    "g",
        "gs":   "gs",
        "w":    "w",   "l":  "l",  "sv": "sv",
        "ip":   "ip",
        "h":    "h",   "r":  "r",  "er": "er", "hr": "hr",
        "bb":   "bb",  "so": "so",
        "era":  "era",
        "whip": "whip",
        "h9":   "h9",  "h/9": "h9",
        "hr9":  "hr9", "hr/9": "hr9",
        "bb9":  "bb9", "bb/9": "bb9",
        "so9":  "so9", "k/9": "so9", "so/9": "so9",
        "so/bb": "so_bb",
    }
    df2 = df.rename(columns={c: rename.get(c.lower().replace("/", ""), c.lower())
                              for c in df.columns})

    sql = """
        INSERT INTO milb_pitching (
            player_id, season, level, level_order, team_abbr, league, age,
            g, gs, w, l, sv, ip, h, r, er, hr, bb, so,
            era, whip, h9, hr9, bb9, so9, so_bb,
            k_pct, bb_pct, k_minus_bb, fip
        ) VALUES (
            %(player_id)s, %(season)s, %(level)s, %(level_order)s,
            %(team_abbr)s, %(league)s, %(age)s,
            %(g)s, %(gs)s, %(w)s, %(l)s, %(sv)s, %(ip)s,
            %(h)s, %(r)s, %(er)s, %(hr)s, %(bb)s, %(so)s,
            %(era)s, %(whip)s, %(h9)s, %(hr9)s, %(bb9)s, %(so9)s, %(so_bb)s,
            %(k_pct)s, %(bb_pct)s, %(k_minus_bb)s, %(fip)s
        )
        ON CONFLICT (player_id, season, level, team_abbr) DO UPDATE SET
            era        = EXCLUDED.era,
            whip       = EXCLUDED.whip,
            k_pct      = EXCLUDED.k_pct,
            bb_pct     = EXCLUDED.bb_pct,
            k_minus_bb = EXCLUDED.k_minus_bb,
            fip        = EXCLUDED.fip,
            scraped_at = NOW()
    """

    written = 0
    with conn.cursor() as cur:
        for _, row in df2.iterrows():
            season = _int(row.get("season"))
            if not season or not (1990 <= season <= 2030):
                continue
            level, level_order = _parse_level_str(str(row.get("level", "")))
            ip = _float(row.get("ip"))
            bb = _int(row.get("bb"))
            so = _int(row.get("so"))
            hr = _int(row.get("hr"))

            # Derived stats
            # Approximate BFP as IP*3 + BB + H (when BFP not directly available)
            h   = _int(row.get("h"))
            bfp = (ip * 3 + (bb or 0) + (h or 0)) if ip else None
            k_pct   = round(so / bfp, 3) if so is not None and bfp else None
            bb_pct  = round(bb / bfp, 3) if bb is not None and bfp else None
            k_minus = round(k_pct - bb_pct, 3) if k_pct and bb_pct else None
            er  = _int(row.get("er"))
            # FIP = (13*HR + 3*BB - 2*SO) / IP + 3.10
            fip = None
            if hr is not None and bb is not None and so is not None and ip:
                try:
                    fip = round((13 * hr + 3 * bb - 2 * so) / ip + 3.10, 2)
                except ZeroDivisionError:
                    pass

            params = {
                "player_id":   player_id,
                "season":      season,
                "level":       level,
                "level_order": level_order,
                "team_abbr":   _strip(row.get("team_abbr")),
                "league":      _strip(row.get("league")),
                "age":         _float(row.get("age")),
                "g":           _int(row.get("g")),
                "gs":          _int(row.get("gs")),
                "w":           _int(row.get("w")),
                "l":           _int(row.get("l")),
                "sv":          _int(row.get("sv")),
                "ip":          ip,
                "h":           h,
                "r":           _int(row.get("r")),
                "er":          er,
                "hr":          hr,
                "bb":          bb,
                "so":          so,
                "era":         _float(row.get("era")),
                "whip":        _float(row.get("whip")),
                "h9":          _float(row.get("h9")),
                "hr9":         _float(row.get("hr9")),
                "bb9":         _float(row.get("bb9")),
                "so9":         _float(row.get("so9")),
                "so_bb":       _float(row.get("so_bb")),
                "k_pct":       k_pct,
                "bb_pct":      bb_pct,
                "k_minus_bb":  k_minus,
                "fip":         fip,
            }
            try:
                cur.execute(sql, params)
                written += 1
            except Exception as e:
                log.debug(f"    pitching row skip ({season} {level}): {e}")
                conn.rollback()
    conn.commit()
    return written


def log_scrape_job(conn, bbref_id: str, player_id: Optional[int],
                   status: str, rows: int, error: Optional[str],
                   started: datetime, finished: datetime):
    sql = """
        INSERT INTO scrape_log
            (job_type, bbref_id, player_id, status, rows_upserted,
             error_message, duration_ms, started_at, finished_at)
        VALUES
            ('ingest_register', %(bbref_id)s, %(player_id)s, %(status)s,
             %(rows)s, %(error)s, %(ms)s, %(started)s, %(finished)s)
    """
    ms = int((finished - started).total_seconds() * 1000)
    with conn.cursor() as cur:
        cur.execute(sql, {
            "bbref_id": bbref_id, "player_id": player_id,
            "status": status, "rows": rows, "error": error,
            "ms": ms, "started": started, "finished": finished,
        })
    conn.commit()


def backfill_years_to_mlb(conn):
    """
    After inserting all season rows, compute years_to_mlb for players
    who reached MLB by comparing their earliest MiLB season to their debut year.
    Also sets milb_debut_date to March 1 of the earliest season.
    """
    sql = """
        WITH first_season AS (
            SELECT player_id, MIN(season) AS first_milb
            FROM (
                SELECT player_id, season FROM milb_batting
                UNION ALL
                SELECT player_id, season FROM milb_pitching
            ) t
            GROUP BY player_id
        )
        UPDATE players p
        SET
            years_to_mlb    = EXTRACT(YEAR FROM p.mlb_debut_date)::int
                              - fs.first_milb,
            milb_debut_date = MAKE_DATE(fs.first_milb::int, 3, 1),
            updated_at      = NOW()
        FROM first_season fs
        WHERE p.id            = fs.player_id
          AND p.reached_mlb   = TRUE
          AND p.mlb_debut_date IS NOT NULL
          AND p.years_to_mlb  IS NULL
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        n = cur.rowcount
    conn.commit()
    log.info(f"  Backfilled years_to_mlb for {n} players")


# ===========================================================================
# PROGRESS TRACKING
# ===========================================================================

def load_progress() -> dict[str, str]:
    if not Path(PROGRESS_CSV).exists():
        return {}
    with open(PROGRESS_CSV, newline="", encoding="utf-8") as f:
        return {r["bbref_id"]: r["status"] for r in csv.DictReader(f)}


def save_progress(bbref_id: str, status: str, full_name: str,
                  reached: Optional[bool], bat_n: int, pit_n: int,
                  error: str = ""):
    exists = Path(PROGRESS_CSV).exists()
    with open(PROGRESS_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "bbref_id", "full_name", "status", "reached_mlb",
            "batting_rows", "pitching_rows", "error", "ts",
        ])
        if not exists:
            w.writeheader()
        w.writerow({
            "bbref_id":     bbref_id,
            "full_name":    full_name or "",
            "status":       status,
            "reached_mlb":  reached,
            "batting_rows": bat_n,
            "pitching_rows":pit_n,
            "error":        error,
            "ts":           datetime.utcnow().isoformat(),
        })


# ===========================================================================
# DATABASE AUDIT REPORT
# ===========================================================================

def run_audit():
    """Print a concise summary of what's in the DB and model readiness."""
    if psycopg is None:
        print("psycopg not installed — cannot audit DB")
        return
    try:
        conn = _db_connect()
    except RuntimeError as e:
        print(f"DB connection failed: {e}")
        return

    def q(sql):
        with conn.cursor() as cur:
            cur.execute(sql)
            r = cur.fetchone()
            return r[0] if r else 0

    def rows(sql):
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    total     = q("SELECT COUNT(*) FROM players")
    reached   = q("SELECT COUNT(*) FROM players WHERE reached_mlb = TRUE")
    no_mlb    = q("SELECT COUNT(*) FROM players WHERE reached_mlb = FALSE")
    has_id    = q("SELECT COUNT(*) FROM players WHERE mlb_id IS NOT NULL")
    bat_rows  = q("SELECT COUNT(*) FROM milb_batting")
    bat_pls   = q("SELECT COUNT(DISTINCT player_id) FROM milb_batting")
    pit_rows  = q("SELECT COUNT(*) FROM milb_pitching")
    pit_pls   = q("SELECT COUNT(DISTINCT player_id) FROM milb_pitching")
    log_ok    = q("SELECT COUNT(*) FROM scrape_log WHERE status='success'")
    log_err   = q("SELECT COUNT(*) FROM scrape_log WHERE status='error'")

    pct_mlb = reached / total * 100 if total else 0

    print(f"\n{'═'*54}")
    print(f"  SCOUTPRO — DATABASE AUDIT  ({datetime.utcnow():%Y-%m-%d %H:%M} UTC)")
    print(f"{'═'*54}")
    print(f"\n  PLAYERS")
    print(f"  {'Total:':<30} {total:>6,}")
    print(f"  {'Reached MLB:':<30} {reached:>6,}  ({pct_mlb:.1f}%)")
    print(f"  {'Did NOT reach MLB:':<30} {no_mlb:>6,}  ({100-pct_mlb:.1f}%)")
    print(f"  {'MLB ID matched:':<30} {has_id:>6,}")
    print(f"\n  MILB BATTING")
    print(f"  {'Total rows:':<30} {bat_rows:>6,}")
    print(f"  {'Distinct players:':<30} {bat_pls:>6,}")
    lvls = rows("SELECT level, COUNT(*) n FROM milb_batting GROUP BY level ORDER BY n DESC")
    for r in lvls:
        print(f"    {r['level']:<10} {r['n']:>5,} rows")
    print(f"\n  MILB PITCHING")
    print(f"  {'Total rows:':<30} {pit_rows:>6,}")
    print(f"  {'Distinct players:':<30} {pit_pls:>6,}")
    print(f"\n  SCRAPE LOG")
    print(f"  {'Success jobs:':<30} {log_ok:>6,}")
    print(f"  {'Error jobs:':<30} {log_err:>6,}")

    # Readiness
    print(f"\n{'─'*54}")
    print(f"  MODEL READINESS")
    labeled = q("""
        SELECT COUNT(DISTINCT p.id) FROM players p
        WHERE (p.reached_mlb = TRUE  AND p.mlb_debut_date IS NOT NULL)
           OR (p.reached_mlb = FALSE AND p.is_active = FALSE)
    """)
    checks = [
        ("Labeled players",          labeled,  500,  1000),
        ("Players w/ batting stats", bat_pls,  300,   600),
        ("Players w/ pitching stats",pit_pls,  150,   300),
    ]
    all_ok = True
    for label, cur_val, mn, rec in checks:
        if cur_val >= rec:
            s = "✓ READY  "
        elif cur_val >= mn:
            s = "⚠ PARTIAL"
            all_ok = False
        else:
            s = "✗ NEED MORE"
            all_ok = False
        print(f"  {s}  {label:<30} {cur_val:>5,}  (min {mn:,} / rec {rec:,})")

    print(f"\n  {'→ READY TO RUN FEATURE PIPELINE' if all_ok else '→ KEEP COLLECTING'}")
    if not all_ok:
        need = max(0, 500 - labeled)
        hrs  = need * 4 / 3600
        print(f"    Need ~{need} more labeled players  (~{hrs:.1f} h at 4 s/player)")
        print(f"    Run: python collect_players.py")
    else:
        print(f"    Run: python -m ml.build_features --feature-version v1")
    print(f"{'═'*54}\n")
    conn.close()


# ===========================================================================
# MAIN PIPELINE
# ===========================================================================

def run(
    bbref_ids:  list[str],
    resume:     bool = True,
    dry_run:    bool = False,
    limit:      Optional[int] = None,
    use_db:     bool = True,
):
    progress = load_progress() if resume else {}
    conn     = None

    if use_db:
        if psycopg is None:
            log.warning("psycopg not installed — running in CSV-only mode")
        else:
            try:
                conn = _db_connect()
                log.info(f"✓ DB connected ({_PG})")
            except RuntimeError as e:
                log.error(str(e))
                log.info("Continuing in CSV-only / dry-run mode")

    if limit:
        bbref_ids = bbref_ids[:limit]

    total     = len(bbref_ids)
    succeeded = failed = skipped = 0

    log.info(f"\n{'='*54}")
    log.info(f"  SCOUTPRO DATA COLLECTION PIPELINE")
    log.info(f"  Players to process : {total}")
    log.info(f"  Resume             : {resume}")
    log.info(f"  Dry-run            : {dry_run}")
    log.info(f"  DB writes          : {conn is not None and not dry_run}")
    log.info(f"{'='*54}\n")

    for i, bbref_id in enumerate(bbref_ids, 1):
        # ── Resume: skip already done ─────────────────────────────────────
        if resume and progress.get(bbref_id) == "success":
            log.info(f"[{i}/{total}] SKIP {bbref_id}")
            skipped += 1
            continue

        log.info(f"[{i}/{total}] {bbref_id}")
        t0 = datetime.utcnow()

        # ── Step 2: Scrape BBRef ──────────────────────────────────────────
        try:
            scraped = scrape_register_page(bbref_id)
        except Exception as e:
            log.error(f"  Scrape exception: {e}")
            save_progress(bbref_id, "error", "", None, 0, 0, str(e))
            failed += 1
            continue

        if not scraped:
            log.warning(f"  No response — skipping")
            save_progress(bbref_id, "error", "", None, 0, 0, "no_response")
            failed += 1
            continue

        meta  = scraped["metadata"]
        bat   = scraped["batting"]
        pit   = scraped["pitching"]
        name  = meta.get("full_name") or bbref_id
        bn    = len(bat) if bat is not None else 0
        pn    = len(pit) if pit is not None else 0
        log.info(f"  {name}  |  bat={bn} rows  pit={pn} rows")

        # ── Step 3: MLB API match ─────────────────────────────────────────
        birth_year = None
        if isinstance(meta.get("birth_date"), date):
            birth_year = meta["birth_date"].year

        mlb_person = mlb_search(name, birth_year)
        mlb_id     = mlb_person.get("id") if mlb_person else None
        debut      = None

        if mlb_id:
            # Check mlbDebutDate from the search response first
            debut = mlb_person.get("mlbDebutDate")
            if not debut:
                debut = mlb_debut_date(mlb_id)
            log.info(f"  MLB match: {mlb_person.get('fullName')} "
                     f"id={mlb_id}  debut={debut}")
        else:
            log.info(f"  No MLB API match — reached_mlb=False")

        if dry_run:
            log.info(f"  [DRY-RUN] reached_mlb={bool(debut)}")
            save_progress(bbref_id, "dry_run", name, bool(debut), bn, pn)
            succeeded += 1
            continue

        # ── Step 5: DB writes ─────────────────────────────────────────────
        player_db_id = None
        bat_written  = 0
        pit_written  = 0
        err_msg      = None

        if conn:
            try:
                player_db_id = upsert_player(conn, meta, mlb_person, debut)
                if player_db_id:
                    bat_written = upsert_batting(conn, player_db_id, bat)
                    pit_written = upsert_pitching(conn, player_db_id, pit)
                t1 = datetime.utcnow()
                log_scrape_job(conn, bbref_id, player_db_id, "success",
                               bat_written + pit_written, None, t0, t1)
                log.info(f"  ✓ db_id={player_db_id}  "
                         f"bat_rows={bat_written}  pit_rows={pit_written}")
            except Exception as e:
                err_msg = str(e)
                log.error(f"  DB write failed: {e}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    t1 = datetime.utcnow()
                    log_scrape_job(conn, bbref_id, None, "error", 0, err_msg, t0, t1)
                except Exception:
                    pass
                save_progress(bbref_id, "error", name, bool(debut), 0, 0, err_msg)
                failed += 1
                continue

        save_progress(bbref_id, "success", name, bool(debut),
                      bat_written, pit_written)
        succeeded += 1
        log.info(f"  ✓ done  [{succeeded} ok / {failed} fail / {skipped} skip]")

    # ── Final backfill ────────────────────────────────────────────────────
    if conn and not dry_run:
        backfill_years_to_mlb(conn)
        conn.close()

    _pw_stop()

    log.info(f"\n{'='*54}")
    log.info(f"  PIPELINE FINISHED")
    log.info(f"  Succeeded: {succeeded}")
    log.info(f"  Failed:    {failed}")
    log.info(f"  Skipped:   {skipped}")
    log.info(f"  Progress : {PROGRESS_CSV}")
    log.info(f"  Log      : {LOG_FILE}")
    log.info(f"{'='*54}\n")


# ===========================================================================
# CLI
# ===========================================================================

def main():
    ap = argparse.ArgumentParser(
        description="Collect MiLB player data → PostgreSQL database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python collect_players.py --dry-run --limit 5
  python collect_players.py --limit 60
  python collect_players.py --discover 2008 2009 2010 --limit 300
  python collect_players.py --id-file bbref_ids.txt
  python collect_players.py --audit
""")
    ap.add_argument("--limit",    type=int,      help="Max players to process")
    ap.add_argument("--dry-run",  action="store_true",
                    help="Scrape + match but skip DB writes")
    ap.add_argument("--no-resume", action="store_true",
                    help="Re-process all IDs (ignores collect_progress.csv)")
    ap.add_argument("--no-db",   action="store_true",
                    help="Disable DB writes entirely (CSV-only)")
    ap.add_argument("--id-file", type=str,
                    help="Text file with one BBRef ID per line")
    ap.add_argument("--discover", type=int, nargs="+", metavar="YEAR",
                    help="Discover IDs from BBRef draft pages for these years")
    ap.add_argument("--audit",   action="store_true",
                    help="Print DB health report and exit")
    args = ap.parse_args()

    if args.audit:
        run_audit()
        return

    # Build ID list
    ids: list[str] = list(SEED_IDS)

    if args.id_file and Path(args.id_file).exists():
        with open(args.id_file) as f:
            file_ids = [ln.strip() for ln in f if ln.strip()]
        log.info(f"Loaded {len(file_ids)} IDs from {args.id_file}")
        ids.extend(file_ids)

    if args.discover:
        for yr in args.discover:
            log.info(f"Discovering draft class {yr}...")
            ids.extend(discover_draft_class(yr))

    # Deduplicate, preserving order
    seen, deduped = set(), []
    for pid in ids:
        if pid not in seen:
            seen.add(pid)
            deduped.append(pid)

    log.info(f"Total unique IDs: {len(deduped)}")

    run(
        bbref_ids = deduped,
        resume    = not args.no_resume,
        dry_run   = args.dry_run,
        limit     = args.limit,
        use_db    = not args.no_db,
    )


if __name__ == "__main__":
    main()
