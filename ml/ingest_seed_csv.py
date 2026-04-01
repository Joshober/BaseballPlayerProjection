"""Ingest MiLB history from a CSV of Baseball-Reference register URLs (Phase 1 seed list).

Columns:
  - register_url (required): full player.fcgi URL or bbref_id (id=xxx only)
  - mlb_id (optional): MLBAM person id — strongly recommended for correct labels
  - min_season / max_season (optional): if ingested rows fall outside [min,max], row is still stored but noted in report

Usage:
  python -m ml.ingest_seed_csv --csv data/seed_register_urls.csv --delay 2.0
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import psycopg

from db.config import load_project_env
from free_apis import search_mlb_people
from ml.scrape_pipeline import ingest_bbref_register

load_project_env()


def _register_url_from_row(bbref_id_or_url: str) -> str:
    s = (bbref_id_or_url or "").strip()
    if not s:
        raise ValueError("empty register_url")
    if s.startswith("http"):
        return s
    return f"https://www.baseball-reference.com/register/player.fcgi?id={quote(s, safe='')}"


def _parse_int(val: str | None) -> int | None:
    if val is None or str(val).strip() == "":
        return None
    try:
        return int(float(str(val).strip()))
    except ValueError:
        return None


def _resolve_mlb_id(
    name_hint: str | None,
    birth_year: int | None,
    *,
    delay_s: float,
) -> int | None:
    if not name_hint or len(name_hint.strip()) < 3:
        return None
    _ = birth_year  # reserved for future disambiguation (birth year not in search JSON)
    time.sleep(delay_s)
    try:
        hits = search_mlb_people(name_hint.strip())
    except Exception:
        return None
    if not hits:
        return None
    return int(hits[0]["id"]) if hits[0].get("id") else None


def _season_bounds_for_player(conn: psycopg.Connection, player_id: int) -> tuple[int | None, int | None]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MIN(season), MAX(season) FROM (
                SELECT season FROM milb_batting WHERE player_id = %s
                UNION ALL
                SELECT season FROM milb_pitching WHERE player_id = %s
            ) u
            """,
            (player_id, player_id),
        )
        row = cur.fetchone()
    if not row or row[0] is None:
        return None, None
    return int(row[0]), int(row[1])


def run_seed_csv(
    csv_path: Path,
    *,
    delay: float = 2.0,
    cohort_min_season: int | None = None,
    cohort_max_season: int | None = None,
    build_features: bool = False,
    feature_version: str = "v1",
    name_column: str = "full_name",
) -> dict[str, Any]:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    rows_ok = 0
    rows_err = 0
    report: list[dict[str, Any]] = []

    text = csv_path.read_text(encoding="utf-8", errors="replace")
    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    if not lines:
        return {"ingested": 0, "errors": 0, "details": []}

    reader = csv.DictReader(io.StringIO("\n".join(lines)))
    fieldnames = reader.fieldnames or []
    url_key = "register_url" if "register_url" in fieldnames else ("url" if "url" in fieldnames else None)
    if not url_key:
        raise ValueError("CSV must include register_url (or url) column")

    for raw in reader:
        url_cell = (raw.get(url_key) or "").strip()
        if not url_cell:
            continue
        mlb_id = _parse_int(raw.get("mlb_id"))
        name_hint = (raw.get(name_column) or raw.get("name") or "").strip() or None
        birth_year = _parse_int(raw.get("birth_year"))

        try:
            url = _register_url_from_row(url_cell)
        except ValueError as e:
            rows_err += 1
            report.append({"url": url_cell, "error": str(e)})
            continue

        if mlb_id is None and name_hint:
            mlb_id = _resolve_mlb_id(name_hint, birth_year, delay_s=delay)

        try:
            out = ingest_bbref_register(
                url=url,
                delay=delay,
                mlb_id=mlb_id,
                build_features=build_features,
                feature_version=feature_version,
            )
            pid = out.get("player_id")
            in_range = True
            if pid is not None and (cohort_min_season is not None or cohort_max_season is not None):
                with psycopg.connect(database_url) as conn:
                    lo, hi = _season_bounds_for_player(conn, int(pid))
                mn, mx = cohort_min_season or 1900, cohort_max_season or 2100
                if lo is not None and hi is not None:
                    in_range = not (hi < mn or lo > mx)
            rows_ok += 1
            report.append(
                {
                    "register_url": url,
                    "mlb_id": mlb_id,
                    "player_id": pid,
                    "bbref_id": out.get("bbref_id"),
                    "batting_rows": out.get("batting_rows_upserted"),
                    "pitching_rows": out.get("pitching_rows_upserted"),
                    "season_window_ok": in_range,
                }
            )
        except Exception as exc:
            rows_err += 1
            report.append({"register_url": url, "error": str(exc)})
        time.sleep(delay)

    return {"ingested": rows_ok, "errors": rows_err, "details": report}


def main() -> None:
    p = argparse.ArgumentParser(description="Ingest BBRef register URLs from CSV seed file")
    p.add_argument("--csv", type=Path, default=Path("data/seed_register_urls.csv"))
    p.add_argument("--delay", type=float, default=2.0)
    p.add_argument("--min-season", type=int, default=None, help="Report flag if no MiLB rows overlap this window")
    p.add_argument("--max-season", type=int, default=None)
    p.add_argument("--build-features", action="store_true")
    p.add_argument("--feature-version", default="v1")
    args = p.parse_args()

    if not args.csv.is_file():
        print(f"CSV not found: {args.csv} — copy data/seed_register_urls.example.csv")
        raise SystemExit(1)

    stats = run_seed_csv(
        args.csv,
        delay=args.delay,
        cohort_min_season=args.min_season,
        cohort_max_season=args.max_season,
        build_features=args.build_features,
        feature_version=args.feature_version,
    )
    print(stats)


if __name__ == "__main__":
    main()
