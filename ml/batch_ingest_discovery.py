"""Automate MLB search → BBRef register resolve → ingest until dataset targets are met."""
from __future__ import annotations

import argparse
import os
import random
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import psycopg

from db.config import load_project_env
from free_apis import search_mlb_people
from milb_scraper import MiLBScraper
from ml.data_status import report as data_status_report
from ml.scrape_pipeline import ingest_bbref_register

load_project_env()

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_QUERIES_FILE = ROOT / "data" / "batch_search_queries.txt"


def _load_queries(extra: list[str] | None, queries_file: Path | None) -> list[str]:
    path = queries_file or DEFAULT_QUERIES_FILE
    lines: list[str] = []
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if len(s) >= 2:
                lines.append(s)
    if extra:
        lines.extend(e.strip() for e in extra if len(e.strip()) >= 2)
    if not lines:
        lines = ["Trout", "Ohtani", "Judge", "Acuna", "Harper"]
    random.shuffle(lines)
    return lines


def _already_has_milb(
    conn: psycopg.Connection,
    mlb_id: int,
    *,
    min_season: int | None = None,
    max_season: int | None = None,
) -> bool:
    """If min/max season set, only True when existing MiLB rows overlap [min_season, max_season]."""
    with conn.cursor() as cur:
        if min_season is None and max_season is None:
            cur.execute(
                """
                SELECT 1 FROM players p
                WHERE p.mlb_id = %s
                AND (
                    EXISTS (SELECT 1 FROM milb_batting b WHERE b.player_id = p.id LIMIT 1)
                    OR EXISTS (SELECT 1 FROM milb_pitching m WHERE m.player_id = p.id LIMIT 1)
                )
                LIMIT 1
                """,
                (mlb_id,),
            )
            return cur.fetchone() is not None
        cur.execute(
            """
            SELECT MIN(s), MAX(s) FROM (
                SELECT season AS s FROM milb_batting b
                JOIN players p ON p.id = b.player_id AND p.mlb_id = %s
                UNION ALL
                SELECT season FROM milb_pitching m
                JOIN players p ON p.id = m.player_id AND p.mlb_id = %s
            ) x
            """,
            (mlb_id, mlb_id),
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            return False
        lo, hi = int(row[0]), int(row[1])
        mn = min_season if min_season is not None else 1900
        mx = max_season if max_season is not None else 2100
        return not (hi < mn or lo > mx)


def run_batch_ingest(
    *,
    target_new_ingests: int = 30,
    target_milb_rows: int = 0,
    max_per_query: int = 8,
    delay_seconds: float = 3.0,
    bbref_delay: float = 1.5,
    build_features: bool = False,
    feature_version: str = "v2",
    extra_queries: list[str] | None = None,
    queries_file: str | None = None,
    max_rounds: int = 500,
    min_season: int | None = None,
    max_season: int | None = None,
    log: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Loop: MLB search → BBRef register candidates → ingest. Skips players already in DB with MiLB rows."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    def _log(msg: str) -> None:
        if log:
            log(msg)

    qpath = Path(queries_file) if queries_file else None
    queries = _load_queries(extra_queries, qpath)
    scraper = MiLBScraper(delay=bbref_delay)
    seen_mlb: set[int] = set()
    ingested = 0
    skipped = 0
    errors = 0
    rounds = 0

    while rounds < max_rounds:
        rounds += 1
        if ingested >= target_new_ingests:
            _log(f"Stop: reached target_new_ingests={target_new_ingests}")
            break
        st = data_status_report()
        total_milb = st.get("milb_batting_rows", 0) + st.get("milb_pitching_rows", 0)
        if target_milb_rows > 0 and total_milb >= target_milb_rows:
            _log(f"Stop: milb rows {total_milb} >= target_milb_rows={target_milb_rows}")
            break

        if not queries:
            _log("No queries left; reshuffling default list.")
            queries = _load_queries(extra_queries, qpath)

        q = queries.pop(0)
        _log(f"Round {rounds}: MLB search “{q}”")
        try:
            results = search_mlb_people(q)
        except Exception as exc:
            _log(f"  MLB search failed: {exc}")
            errors += 1
            time.sleep(delay_seconds)
            continue

        for row in results[:max_per_query]:
            if ingested >= target_new_ingests:
                break
            mid = row.get("id")
            name = (row.get("full_name") or "").strip()
            if mid is None or not name:
                continue
            mlb_id = int(mid)
            if mlb_id in seen_mlb:
                continue
            seen_mlb.add(mlb_id)

            with psycopg.connect(database_url) as conn:
                if _already_has_milb(conn, mlb_id, min_season=min_season, max_season=max_season):
                    skipped += 1
                    _log(f"  skip mlb_id={mlb_id} ({name}) — already have MiLB rows in season window")
                    continue

            try:
                cands = scraper.search_bbref_register_pages(name)
            except Exception as exc:
                _log(f"  BBRef search failed for {name}: {exc}")
                errors += 1
                time.sleep(delay_seconds)
                continue

            if not cands:
                _log(f"  no BBRef register link for {name} (mlb_id={mlb_id})")
                time.sleep(delay_seconds)
                continue

            url = cands[0]["url"]
            _log(f"  ingest {name} mlb_id={mlb_id} -> {cands[0].get('bbref_id', '?')}")
            try:
                ingest_bbref_register(
                    url=url,
                    delay=bbref_delay,
                    mlb_id=mlb_id,
                    build_features=build_features,
                    feature_version=feature_version,
                )
                ingested += 1
            except Exception as exc:
                _log(f"  ingest error: {exc}")
                errors += 1
            time.sleep(delay_seconds)

        time.sleep(delay_seconds)

    final = data_status_report()
    out = {
        "ingested": ingested,
        "skipped_existing": skipped,
        "errors": errors,
        "rounds": rounds,
        "data_status": final,
    }
    _log(f"Batch finished: {out}")
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Batch MLB → BBRef → ingest until targets met")
    p.add_argument("--target-ingests", type=int, default=30, help="Successful new ingests before stop")
    p.add_argument("--target-milb-rows", type=int, default=0, help="Stop when total MiLB stat rows reach this (0=off)")
    p.add_argument("--max-per-query", type=int, default=8)
    p.add_argument("--delay", type=float, default=3.0, help="Pause between ingests (seconds)")
    p.add_argument("--bbref-delay", type=float, default=1.5, help="MiLBScraper politeness delay")
    p.add_argument("--build-features", action="store_true", help="Run feature build after each ingest (slow)")
    p.add_argument("--queries-file", default=None, help="Override data/batch_search_queries.txt")
    p.add_argument("--max-rounds", type=int, default=500, help="Safety cap on MLB search rounds")
    p.add_argument("--min-season", type=int, default=None, help="Treat existing MiLB as duplicate only if overlaps this window")
    p.add_argument("--max-season", type=int, default=None)
    args = p.parse_args()

    def _print(msg: str) -> None:
        print(msg, flush=True)

    stats = run_batch_ingest(
        target_new_ingests=args.target_ingests,
        target_milb_rows=args.target_milb_rows,
        max_per_query=args.max_per_query,
        delay_seconds=args.delay,
        bbref_delay=args.bbref_delay,
        build_features=args.build_features,
        queries_file=args.queries_file,
        max_rounds=args.max_rounds,
        min_season=args.min_season,
        max_season=args.max_season,
        log=_print,
    )
    print("Summary:", stats)


if __name__ == "__main__":
    main()
