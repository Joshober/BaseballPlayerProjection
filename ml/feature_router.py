from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException, Query

from ml.feature_engineering import build_and_upsert_features

router = APIRouter(tags=["phase1-features"])


@router.post("/features/build")
def build_features(feature_version: str = Query("v1")) -> dict:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    try:
        result = build_and_upsert_features(database_url=database_url, feature_version=feature_version)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Feature build failed: {exc}") from exc

    return {
        "status": "ok",
        "feature_version": result.feature_version,
        "built_rows": result.built_rows,
        "upserted_rows": result.upserted_rows,
    }
