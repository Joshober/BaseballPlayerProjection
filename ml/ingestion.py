"""Phase 1.5: upsert scraped Baseball-Reference register data into Postgres."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd
import psycopg

from db.config import load_project_env

load_project_env()


def register_id_from_url(url: str) -> str | None:
    m = re.search(r"[?&]id=([^&]+)", url or "")
    return m.group(1).strip() if m else None


def level_order_from_lev(lev: str | None) -> int:
    if not lev or not str(lev).strip():
        return 3
    s = str(lev).upper()
    if "AAA" in s:
        return 6
    if "A+" in s:
        return 4
    if re.search(r"\bAA\b", s):
        return 5
    if "A-" in s or "SS-A" in s or "RK-A" in s:
        return 2
    if "Rk" in s or "DSL" in s or "FRk" in s:
        return 1
    if s.strip() == "A" or s.startswith("A "):
        return 3
    return 3


def _parse_born(born: str | None) -> tuple[date | None, str | None]:
    if not born:
        return None, None
    text = str(born).strip()
    m = re.search(r"([A-Za-z]+ \d{1,2}, \d{4})", text)
    birth_date = None
    if m:
        try:
            birth_date = datetime.strptime(m.group(1), "%B %d, %Y").date()
        except ValueError:
            birth_date = None
    country: str | None = None
    m2 = re.search(r"\bin\s+([^,]+),?\s*([A-Za-z]{2})?\s*(us)?", text, re.I)
    if m2:
        parts = [p.strip() for p in m2.groups() if p]
        country = " ".join(parts)[:60] if parts else None
    return birth_date, country


def _parse_position(pos: str | None) -> str | None:
    if not pos:
        return None
    p = pos.upper()
    if "PITCHER" in p or p.strip() == "P":
        return "P"
    if "CATCHER" in p or p.strip() == "C":
        return "C"
    if "FIRST" in p:
        return "1B"
    if "SECOND" in p:
        return "2B"
    if "THIRD" in p:
        return "3B"
    if "SHORTSTOP" in p or p.strip() == "SS":
        return "SS"
    if "DESIGNATED" in p:
        return "DH"
    if "LEFT" in p and "FIELD" in p:
        return "LF"
    if "CENTER" in p and "FIELD" in p:
        return "CF"
    if "RIGHT" in p and "FIELD" in p:
        return "RF"
    if "OUTFIELD" in p or "FIELDER" in p:
        return "OF"
    s = pos.strip()
    return s[:5] if s else None


def _split_name(full: str | None) -> tuple[str | None, str | None, str]:
    if not full:
        return None, None, "Unknown"
    parts = full.strip().split()
    if len(parts) >= 2:
        return parts[0], parts[-1], full.strip()
    return None, None, full.strip()


def _is_batting_stats_df(df: pd.DataFrame | None) -> bool:
    if df is None or df.empty:
        return False
    cols = {str(c).lower() for c in df.columns}
    return "pa" in cols or "ab" in cols


def _is_pitching_stats_df(df: pd.DataFrame | None) -> bool:
    if df is None or df.empty:
        return False
    cols = {str(c).lower() for c in df.columns}
    if "pa" in cols and "ab" in cols:
        return False
    if "fld%" in cols:
        return False
    if "ch" in cols and "po" in cols and "ip" not in cols and "era" not in cols and "inn" not in cols:
        return False
    return "ip" in cols or "era" in cols or "inn" in cols or ("so" in cols and "bb" in cols and "w" in cols)


def _season_line_ok(row: pd.Series) -> bool:
    y = row.get("Year")
    if y is None or (isinstance(y, float) and pd.isna(y)):
        return False
    try:
        yi = int(float(y))
        if yi < 1980 or yi > 2035:
            return False
    except (TypeError, ValueError):
        return False
    tm = row.get("Tm")
    if tm is not None and isinstance(tm, str):
        tl = tm.lower()
        if "minors" in tl and "season" in tl:
            return False
        if "all levels" in tl:
            return False
    return True


def _trunc(s: Any, n: int) -> str | None:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    t = str(s).strip()
    if not t:
        return None
    return t[:n]


def _to_int(v: Any) -> int | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return None


def _to_float(v: Any) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


@dataclass
class IngestResult:
    bbref_id: str
    player_id: int
    batting_rows: int
    pitching_rows: int


def upsert_player(
    cur: psycopg.Cursor,
    bbref_id: str,
    metadata: dict[str, Any],
    mlb_id: int | None = None,
) -> int:
    meta = metadata or {}
    birth_date, birth_country = _parse_born(meta.get("born"))
    first, last, full = _split_name(meta.get("name"))
    position = _parse_position(meta.get("position"))
    bats = _trunc(meta.get("bats"), 1)
    throws = _trunc(meta.get("throws"), 1)

    cur.execute(
        """
        INSERT INTO players (
            bbref_id, mlb_id, full_name, first_name, last_name,
            birth_date, birth_country, position, bats, throws, updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, NOW()
        )
        ON CONFLICT (bbref_id) DO UPDATE SET
            mlb_id = COALESCE(EXCLUDED.mlb_id, players.mlb_id),
            full_name = EXCLUDED.full_name,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            birth_date = COALESCE(EXCLUDED.birth_date, players.birth_date),
            birth_country = COALESCE(EXCLUDED.birth_country, players.birth_country),
            position = COALESCE(EXCLUDED.position, players.position),
            bats = COALESCE(EXCLUDED.bats, players.bats),
            throws = COALESCE(EXCLUDED.throws, players.throws),
            updated_at = NOW()
        RETURNING id
        """,
        (bbref_id, mlb_id, full, first, last, birth_date, birth_country, position, bats, throws),
    )
    row = cur.fetchone()
    if not row:
        cur.execute("SELECT id FROM players WHERE bbref_id = %s", (bbref_id,))
        row = cur.fetchone()
    return int(row[0])


def upsert_milb_batting_rows(cur: psycopg.Cursor, player_id: int, df: pd.DataFrame) -> int:
    """Insert/upsert batting stat rows (register batting table)."""
    if not _is_batting_stats_df(df):
        return 0
    n = 0
    for _, row in df.iterrows():
        if not _season_line_ok(row):
            continue
        lev_raw = row.get("Lev")
        level = _trunc(lev_raw, 10) or "UNK"
        lo = level_order_from_lev(str(lev_raw) if lev_raw is not None else None)
        team_abbr = _trunc(row.get("Tm"), 10) or ""
        season = _to_int(row.get("Year"))
        if season is None:
            continue
        pa = _to_int(row.get("PA"))
        ab = _to_int(row.get("AB"))
        if (pa or 0) == 0 and (ab or 0) == 0:
            continue

        ba = _to_float(row.get("BA"))
        slg = _to_float(row.get("SLG"))
        obp = _to_float(row.get("OBP"))
        ops = _to_float(row.get("OPS"))
        bb_v = _to_int(row.get("BB"))
        so_v = _to_int(row.get("SO"))
        dbl = _to_int(row.get("2B"))
        tpl = _to_int(row.get("3B"))
        iso = (slg - ba) if ba is not None and slg is not None else None
        bb_pct = (bb_v / pa) if pa and pa > 0 and bb_v is not None else None
        k_pct = (so_v / pa) if pa and pa > 0 and so_v is not None else None
        bb_k_ratio = (bb_v / max(so_v or 0, 1)) if bb_v is not None and so_v is not None else None

        cur.execute(
            """
            INSERT INTO milb_batting (
                player_id, season, level, level_order, team_abbr, league, age,
                g, pa, ab, r, h, doubles, triples, hr, rbi, sb, cs, bb, so,
                ba, obp, slg, ops, iso, bb_pct, k_pct, bb_k_ratio, scraped_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, NOW()
            )
            ON CONFLICT (player_id, season, level, team_abbr) DO UPDATE SET
                league = EXCLUDED.league,
                age = EXCLUDED.age,
                g = EXCLUDED.g,
                pa = EXCLUDED.pa,
                ab = EXCLUDED.ab,
                r = EXCLUDED.r,
                h = EXCLUDED.h,
                doubles = EXCLUDED.doubles,
                triples = EXCLUDED.triples,
                hr = EXCLUDED.hr,
                rbi = EXCLUDED.rbi,
                sb = EXCLUDED.sb,
                cs = EXCLUDED.cs,
                bb = EXCLUDED.bb,
                so = EXCLUDED.so,
                ba = EXCLUDED.ba,
                obp = EXCLUDED.obp,
                slg = EXCLUDED.slg,
                ops = EXCLUDED.ops,
                iso = EXCLUDED.iso,
                bb_pct = EXCLUDED.bb_pct,
                k_pct = EXCLUDED.k_pct,
                bb_k_ratio = EXCLUDED.bb_k_ratio,
                scraped_at = NOW()
            """,
            (
                player_id,
                season,
                level,
                lo,
                team_abbr,
                _trunc(row.get("Lg"), 30),
                _to_float(row.get("Age")),
                _to_int(row.get("G")),
                pa,
                ab,
                _to_int(row.get("R")),
                _to_int(row.get("H")),
                dbl,
                tpl,
                _to_int(row.get("HR")),
                _to_int(row.get("RBI")),
                _to_int(row.get("SB")),
                _to_int(row.get("CS")),
                bb_v,
                so_v,
                ba,
                obp,
                slg,
                ops,
                iso,
                bb_pct,
                k_pct,
                bb_k_ratio,
            ),
        )
        n += 1
    return n


def upsert_milb_pitching_rows(cur: psycopg.Cursor, player_id: int, df: pd.DataFrame) -> int:
    if not _is_pitching_stats_df(df):
        return 0
    n = 0
    ip_col = "IP" if "IP" in df.columns else ("Inn" if "Inn" in df.columns else None)
    for _, row in df.iterrows():
        if not _season_line_ok(row):
            continue
        lev_raw = row.get("Lev")
        level = _trunc(lev_raw, 10) or "UNK"
        lo = level_order_from_lev(str(lev_raw) if lev_raw is not None else None)
        team_abbr = _trunc(row.get("Tm"), 10) or ""
        season = _to_int(row.get("Year"))
        if season is None:
            continue
        ip_val = _to_float(row.get(ip_col)) if ip_col else None
        era = _to_float(row.get("ERA"))
        g = _to_int(row.get("G"))
        if (ip_val is None or ip_val <= 0) and era is None and (g is None or g == 0):
            continue

        ip = ip_val
        bf = _to_int(row.get("BF"))
        bb_v = _to_int(row.get("BB"))
        so_v = _to_int(row.get("SO"))
        k_pct = (so_v / bf) if bf and bf > 0 and so_v is not None else None
        bb_pct = (bb_v / bf) if bf and bf > 0 and bb_v is not None else None
        k_minus_bb = (k_pct - bb_pct) if k_pct is not None and bb_pct is not None else None

        cur.execute(
            """
            INSERT INTO milb_pitching (
                player_id, season, level, level_order, team_abbr, league, age,
                g, gs, w, l, sv, ip, h, r, er, hr, bb, so,
                era, whip, h9, hr9, bb9, so9, so_bb,
                k_pct, bb_pct, k_minus_bb, scraped_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, NOW()
            )
            ON CONFLICT (player_id, season, level, team_abbr) DO UPDATE SET
                league = EXCLUDED.league,
                age = EXCLUDED.age,
                g = EXCLUDED.g,
                gs = EXCLUDED.gs,
                w = EXCLUDED.w,
                l = EXCLUDED.l,
                sv = EXCLUDED.sv,
                ip = EXCLUDED.ip,
                h = EXCLUDED.h,
                r = EXCLUDED.r,
                er = EXCLUDED.er,
                hr = EXCLUDED.hr,
                bb = EXCLUDED.bb,
                so = EXCLUDED.so,
                era = EXCLUDED.era,
                whip = EXCLUDED.whip,
                h9 = EXCLUDED.h9,
                hr9 = EXCLUDED.hr9,
                bb9 = EXCLUDED.bb9,
                so9 = EXCLUDED.so9,
                so_bb = EXCLUDED.so_bb,
                k_pct = EXCLUDED.k_pct,
                bb_pct = EXCLUDED.bb_pct,
                k_minus_bb = EXCLUDED.k_minus_bb,
                scraped_at = NOW()
            """,
            (
                player_id,
                season,
                level,
                lo,
                team_abbr,
                _trunc(row.get("Lg"), 30),
                _to_float(row.get("Age")),
                g,
                _to_int(row.get("GS")),
                _to_int(row.get("W")),
                _to_int(row.get("L")),
                _to_int(row.get("SV")),
                ip,
                _to_int(row.get("H")),
                _to_int(row.get("R")),
                _to_int(row.get("ER")),
                _to_int(row.get("HR")),
                bb_v,
                so_v,
                era,
                _to_float(row.get("WHIP")),
                _to_float(row.get("H9")),
                _to_float(row.get("HR9")),
                _to_float(row.get("BB9")),
                _to_float(row.get("SO9")),
                _to_float(row.get("SO/BB") or row.get("SO_BB")),
                k_pct,
                bb_pct,
                k_minus_bb,
            ),
        )
        n += 1
    return n


def ingest_scrape(
    conn: psycopg.Connection,
    url: str,
    metadata: dict[str, Any],
    batting: pd.DataFrame | None,
    pitching: pd.DataFrame | None,
    mlb_id: int | None = None,
) -> IngestResult:
    bbref_id = register_id_from_url(url)
    if not bbref_id:
        raise ValueError("Could not parse register id from URL")

    t0 = time.perf_counter()
    with conn.cursor() as cur:
        player_id = upsert_player(cur, bbref_id, metadata, mlb_id=mlb_id)
        b_n = upsert_milb_batting_rows(cur, player_id, batting) if batting is not None else 0
        p_n = upsert_milb_pitching_rows(cur, player_id, pitching) if pitching is not None else 0
        dur_ms = int((time.perf_counter() - t0) * 1000)
        cur.execute(
            """
            INSERT INTO scrape_log (job_type, player_id, bbref_id, status, rows_upserted, duration_ms, finished_at)
            VALUES ('ingest_register', %s, %s, 'success', %s, %s, NOW())
            """,
            (player_id, bbref_id, b_n + p_n, dur_ms),
        )
    conn.commit()
    return IngestResult(bbref_id=bbref_id, player_id=player_id, batting_rows=b_n, pitching_rows=p_n)


def ingest_from_url(
    database_url: str,
    url: str,
    delay: float = 2.0,
    mlb_id: int | None = None,
) -> IngestResult:
    from milb_scraper import MiLBScraper

    scraper = MiLBScraper(delay=delay)
    data = scraper.scrape_player(url)
    with psycopg.connect(database_url) as conn:
        return ingest_scrape(
            conn,
            url,
            data.get("metadata") or {},
            data.get("batting"),
            data.get("pitching"),
            mlb_id=mlb_id,
        )
