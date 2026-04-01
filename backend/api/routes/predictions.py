"""Prediction generation and retrieval."""
from __future__ import annotations

import os

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException

from backend.api.deps import require_auth
from backend.api.services import inference_service

router = APIRouter(tags=["predictions"], prefix="/predictions")


def _run_inference(mlbam_id: int) -> None:
    bundle = inference_service.generate_full_report(mlbam_id)
    inference_service.store_prediction_stub(mlbam_id, bundle)


@router.post("/generate/{mlbam_id}")
def generate_predictions(
    mlbam_id: int,
    background_tasks: BackgroundTasks,
    _user: dict = Depends(require_auth),
):
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise HTTPException(status_code=503, detail="DATABASE_URL not configured")
    background_tasks.add_task(_run_inference, mlbam_id)
    return {"status": "queued", "mlbam_id": mlbam_id}


@router.get("/{mlbam_id}/latest")
def latest_prediction(mlbam_id: int, _user: dict = Depends(require_auth)):
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise HTTPException(status_code=503, detail="DATABASE_URL not configured")
    import psycopg

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pred.* FROM predictions pred
                JOIN players pl ON pl.id = pred.player_id
                WHERE pl.mlb_id = %s
                ORDER BY pred.predicted_at DESC
                LIMIT 1
                """,
                (mlbam_id,),
            )
            row = cur.fetchone()
            cols = [d.name for d in cur.description] if cur.description else []
    if not row:
        return {"mlbam_id": mlbam_id, "prediction": None}
    return {"mlbam_id": mlbam_id, "prediction": dict(zip(cols, row))}
