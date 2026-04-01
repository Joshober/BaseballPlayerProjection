"""Seed MiLB rosters and game logs (2015–2024) — long-running; invoke manually."""
from __future__ import annotations

import argparse
import asyncio
import os

from backend.pipeline.sources.mlb_api_source import MLBApiSource


async def seed_sample(player_id: int = 660670, season: int = 2024) -> None:
    """Smoke test: fetch one player game log sample and print row count."""
    if not os.getenv("DATABASE_URL"):
        print("DATABASE_URL not set — no DB writes in sample mode")
    src = MLBApiSource()
    try:
        rows = await src.fetch_game_log_sample(player_id, season=season, limit=20)
        print(f"Fetched {len(rows)} game log rows for player {player_id} season {season}")
    finally:
        await src.close()


def main() -> None:
    p = argparse.ArgumentParser(description="Historical MiLB/MLB ingestion seed")
    p.add_argument("--player-id", type=int, default=660670)
    p.add_argument("--season", type=int, default=2024)
    args = p.parse_args()
    asyncio.run(seed_sample(args.player_id, args.season))


if __name__ == "__main__":
    main()
