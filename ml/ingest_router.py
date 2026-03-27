"""Phase 1.5: HTTP routes for scrape-to-database ingestion."""

from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException, Query

from ml.ingestion import ingest_from_url

router = APIRouter(tags=["phase15-ingest"])


@router.post("/ingest/scrape")
def ingest_scrape_route(
    url: str = Query(..., description="Baseball-Reference register player URL"),
    delay: float = Query(2.0, ge=0.0, le=10.0),
    mlb_id: int | None = Query(None, description="Optional MLB StatsAPI player id"),
) -> dict:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    try:
        result = ingest_from_url(database_url=database_url, url=url, delay=delay, mlb_id=mlb_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Ingest failed: {exc}") from exc

    return {
        "status": "ok",
        "bbref_id": result.bbref_id,
        "player_id": result.player_id,
        "batting_rows_upserted": result.batting_rows,
        "pitching_rows_upserted": result.pitching_rows,
    }
