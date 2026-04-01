"""Print row counts for ML-related tables (players, MiLB stats, engineered_features)."""
from __future__ import annotations

import os

import psycopg

from db.config import load_project_env

load_project_env()


def report() -> dict[str, int]:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    queries = {
        "players": "SELECT COUNT(*) FROM players",
        "players_with_mlb_id": "SELECT COUNT(*) FROM players WHERE mlb_id IS NOT NULL",
        "players_labeled_reached": "SELECT COUNT(*) FROM players WHERE reached_mlb IS TRUE",
        "milb_batting_rows": "SELECT COUNT(*) FROM milb_batting",
        "milb_pitching_rows": "SELECT COUNT(*) FROM milb_pitching",
        "engineered_features_v1": "SELECT COUNT(*) FROM engineered_features WHERE feature_version = 'v1'",
        "engineered_features_v2": "SELECT COUNT(*) FROM engineered_features WHERE feature_version = 'v2'",
    }
    out: dict[str, int] = {}
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            for name, sql in queries.items():
                cur.execute(sql)
                row = cur.fetchone()
                out[name] = int(row[0]) if row else 0
    return out


def main() -> None:
    stats = report()
    print("ML data status (database)")
    print("-" * 40)
    for k, v in stats.items():
        print(f"  {k}: {v}")
    print("-" * 40)
    need = []
    if stats.get("players", 0) < 5:
        need.append("Ingest more players (POST /api/scrape/ingest or Tools page pipeline).")
    if stats.get("milb_batting_rows", 0) + stats.get("milb_pitching_rows", 0) < 10:
        need.append("MiLB stat rows are low — scrape register pages with MiLB history.")
    if stats.get("players_with_mlb_id", 0) > 0 and stats.get("players_labeled_reached", 0) == 0:
        need.append("Run: python -m ml.backfill_player_labels (needs players.mlb_id set).")
    if stats.get("engineered_features_v1", 0) < 1 and stats.get("engineered_features_v2", 0) < 1:
        need.append("Run: python -m ml.build_features --feature-version v2")
    if need:
        print("Suggested next steps:")
        for line in need:
            print(f"  - {line}")
    else:
        print("You have enough raw data to run feature build and training experiments.")


if __name__ == "__main__":
    main()
