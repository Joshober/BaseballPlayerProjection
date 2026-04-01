"""Row counts and dataset health for ML tables (players, MiLB stats, engineered_features)."""
from __future__ import annotations

import os
from typing import Any

import psycopg

from db.config import load_project_env

load_project_env()


def report_extended(feature_version: str | None = None) -> dict[str, Any]:
    """Dataset health: cohort sizes, label rate, level/position breakdowns, season span."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    out: dict[str, Any] = {}

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM players")
            out["total_players"] = int(cur.fetchone()[0])

            cur.execute(
                "SELECT COUNT(DISTINCT player_id) FROM milb_batting"
            )
            out["players_with_milb_batting"] = int(cur.fetchone()[0])

            cur.execute(
                "SELECT COUNT(DISTINCT player_id) FROM milb_pitching"
            )
            out["players_with_milb_pitching"] = int(cur.fetchone()[0])

            cur.execute("SELECT COUNT(*) FROM milb_batting")
            out["milb_batting_rows"] = int(cur.fetchone()[0])

            cur.execute("SELECT COUNT(*) FROM milb_pitching")
            out["milb_pitching_rows"] = int(cur.fetchone()[0])

            cur.execute(
                "SELECT COUNT(*) FROM players WHERE reached_mlb IS NOT NULL"
            )
            out["players_with_reached_label"] = int(cur.fetchone()[0])

            cur.execute("SELECT COUNT(*) FROM players WHERE reached_mlb IS TRUE")
            reached = int(cur.fetchone()[0])
            out["players_reached_mlb"] = reached

            cur.execute("SELECT COUNT(*) FROM players WHERE reached_mlb IS FALSE")
            not_reached = int(cur.fetchone()[0])
            out["players_not_reached_mlb"] = not_reached
            denom = reached + not_reached
            out["positive_rate_reached_mlb"] = float(reached / denom) if denom else None

            cur.execute(
                """
                SELECT COUNT(*) FROM players p
                WHERE p.mlb_id IS NOT NULL
                """
            )
            out["players_with_mlb_id"] = int(cur.fetchone()[0])

            if feature_version:
                cur.execute(
                    """
                    SELECT ef.position_group, COUNT(*)
                    FROM engineered_features ef
                    WHERE ef.feature_version = %s
                    GROUP BY ef.position_group ORDER BY ef.position_group
                    """,
                    (feature_version,),
                )
            else:
                cur.execute(
                    """
                    SELECT ef.position_group, COUNT(*)
                    FROM engineered_features ef
                    GROUP BY ef.position_group ORDER BY ef.position_group
                    """
                )
            out["engineered_by_position_group"] = {str(r[0]): int(r[1]) for r in cur.fetchall()}

            if feature_version:
                cur.execute(
                    """
                    SELECT ef.peak_level_order, COUNT(*)
                    FROM engineered_features ef
                    WHERE ef.feature_version = %s
                    GROUP BY ef.peak_level_order ORDER BY ef.peak_level_order NULLS LAST
                    """,
                    (feature_version,),
                )
            else:
                cur.execute(
                    """
                    SELECT ef.peak_level_order, COUNT(*)
                    FROM engineered_features ef
                    GROUP BY ef.peak_level_order ORDER BY ef.peak_level_order NULLS LAST
                    """
                )
            out["engineered_by_peak_level_order"] = {
                int(r[0]) if r[0] is not None else None: int(r[1]) for r in cur.fetchall()
            }

            cur.execute(
                """
                SELECT MIN(m.season), MAX(m.season)
                FROM (
                    SELECT season FROM milb_batting
                    UNION ALL
                    SELECT season FROM milb_pitching
                ) m
                """
            )
            row = cur.fetchone()
            out["milb_season_min"] = int(row[0]) if row and row[0] is not None else None
            out["milb_season_max"] = int(row[1]) if row and row[1] is not None else None

            if feature_version:
                cur.execute(
                    "SELECT COUNT(*) FROM engineered_features WHERE feature_version = %s",
                    (feature_version,),
                )
                out["engineered_features_rows"] = int(cur.fetchone()[0])
            else:
                cur.execute("SELECT feature_version, COUNT(*) FROM engineered_features GROUP BY feature_version")
                out["engineered_features_by_version"] = {str(r[0]): int(r[1]) for r in cur.fetchall()}

    return out


def report() -> dict[str, int]:
    """Legacy flat counts for ML-related tables."""
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
        "engineered_features_v3": "SELECT COUNT(*) FROM engineered_features WHERE feature_version = 'v3'",
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
    try:
        ext = report_extended("v3")
        print("Extended health (v3):")
        for k, v in ext.items():
            print(f"  {k}: {v}")
    except Exception as exc:
        print("(extended report skipped:", exc, ")")
    need = []
    if stats.get("players", 0) < 5:
        need.append("Ingest more players (POST /api/scrape/ingest or Tools page pipeline).")
    if stats.get("milb_batting_rows", 0) + stats.get("milb_pitching_rows", 0) < 10:
        need.append("MiLB stat rows are low — scrape register pages with MiLB history.")
    if stats.get("players_with_mlb_id", 0) > 0 and stats.get("players_labeled_reached", 0) == 0:
        need.append("Run: python -m ml.backfill_player_labels (needs players.mlb_id set).")
    if stats.get("engineered_features_v1", 0) < 1 and stats.get("engineered_features_v2", 0) < 1:
        need.append("Run: python -m ml.build_features --feature-version v3")
    if need:
        print("Suggested next steps:")
        for line in need:
            print(f"  - {line}")
    else:
        print("You have enough raw data to run feature build and training experiments.")


if __name__ == "__main__":
    main()
