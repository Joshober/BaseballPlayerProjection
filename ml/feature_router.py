from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException, Query

from ml.cutoff_policy import DEFAULT_FIRST_K_MILB_SEASONS
from ml.feature_engineering import build_and_upsert_features

router = APIRouter(tags=["phase1-features"])


@router.post("/features/build")
def build_features(
    feature_version: str = Query("v2"),
    cohort_train_ids: str | None = Query(
        None,
        description="Comma-separated player ids — limit cohort age means to these rows (reduces temporal leakage)",
    ),
    first_k_milb_seasons: int | None = Query(
        None,
        ge=1,
        description="If set, only the first K distinct MiLB seasons are used (v3 leakage-safe window). "
        "When omitted and feature_version starts with v3, defaults to the project policy (see cutoff_policy).",
    ),
) -> dict:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not set")

    cohort_ids = None
    if cohort_train_ids:
        cohort_ids = {int(x.strip()) for x in cohort_train_ids.split(",") if x.strip().isdigit()}

    fk = first_k_milb_seasons
    if fk is None and str(feature_version).startswith("v3"):
        fk = DEFAULT_FIRST_K_MILB_SEASONS

    try:
        result = build_and_upsert_features(
            database_url=database_url,
            feature_version=feature_version,
            cohort_player_ids=cohort_ids,
            first_k_milb_seasons=fk,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Feature build failed: {exc}") from exc

    return {
        "status": "ok",
        "feature_version": result.feature_version,
        "built_rows": result.built_rows,
        "upserted_rows": result.upserted_rows,
    }
