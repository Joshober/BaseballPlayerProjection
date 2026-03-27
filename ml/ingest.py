"""CLI: scrape a register URL and upsert into Postgres."""

from __future__ import annotations

import argparse
import os

from db.config import load_project_env
from ml.ingestion import ingest_from_url


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 1.5: ingest scraped MiLB stats into the database")
    parser.add_argument("--url", required=True, help="Register player page URL")
    parser.add_argument("--delay", type=float, default=2.0, help="Seconds between HTTP requests")
    parser.add_argument("--mlb-id", type=int, default=None, help="Optional MLB StatsAPI person id")
    args = parser.parse_args()

    load_project_env()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    result = ingest_from_url(database_url=database_url, url=args.url, delay=args.delay, mlb_id=args.mlb_id)
    print(
        f"Ingest OK: player_id={result.player_id} bbref_id={result.bbref_id} "
        f"batting={result.batting_rows} pitching={result.pitching_rows}"
    )


if __name__ == "__main__":
    main()
