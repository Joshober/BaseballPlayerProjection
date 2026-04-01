"""Nightly / daily ingestion orchestration."""
from __future__ import annotations

import os
from datetime import date, timedelta


def run_daily_ingestion(for_day: date | None = None) -> dict[str, str]:
    """
    Fetch yesterday's games, update game_logs, recompute GDS, invalidate caches.
    Wire to MLB API + DB when credentials and migrations are applied.
    """
    day = for_day or (date.today() - timedelta(days=1))
    if not os.getenv("DATABASE_URL"):
        return {"status": "skipped", "reason": "DATABASE_URL not set", "day": str(day)}
    # Placeholder for full pipeline: schedule after seed_historical + GDS validation.
    return {"status": "ok", "day": str(day), "note": "stub — connect MLBApiSource + game_logs upsert"}
