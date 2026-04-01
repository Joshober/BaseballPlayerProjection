"""Phase 1.5: HTTP routes for scrape-to-database ingestion."""

from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException, Query

from ml.scrape_pipeline import ingest_bbref_register

router = APIRouter(tags=["phase15-ingest"])


@router.post("/ingest/scrape")
def ingest_scrape_route(
    url: str = Query(..., description="Baseball-Reference register player URL"),
    delay: float = Query(2.0, ge=0.0, le=10.0),
    mlb_id: int | None = Query(None, description="Optional MLB StatsAPI player id"),
    build_features: bool = Query(False, description="If true, run feature engineering after ingest"),
    feature_version: str = Query("v2"),
) -> dict:
    """Scrape + DB upsert — shared implementation with POST /api/scrape/ingest."""
    if not os.getenv("DATABASE_URL"):
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    try:
        return ingest_bbref_register(
            url=url,
            delay=delay,
            mlb_id=mlb_id,
            build_features=build_features,
            feature_version=feature_version,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Ingest failed: {exc}") from exc
