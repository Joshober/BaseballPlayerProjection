"""Inference orchestration — loads models when present and writes predictions."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import psycopg

from db.config import load_project_env

load_project_env()


def models_loaded() -> bool:
    root = Path(__file__).resolve().parents[3]
    models_dir = root / "data" / "models"
    if not models_dir.is_dir():
        return False
    return any(models_dir.glob("*.json")) or any(models_dir.glob("*.pkl")) or any(models_dir.glob("*.joblib"))


def generate_full_report(mlbam_id: int) -> dict[str, Any]:
    """Build a prediction bundle for an MLBAM player id (stub when no trained models)."""
    return {
        "mlbam_id": mlbam_id,
        "mlb_probability": 0.42,
        "years_to_mlb_estimate": 2.5,
        "similar_players": [],
        "note": "Stub response until models are trained (see ml/train_all.py)",
    }


def store_prediction_stub(mlbam_id: int, bundle: dict[str, Any]) -> None:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return
    similar = json.dumps(bundle.get("similar_players") or [])
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO predictions (
                    player_id, model_version, mlb_probability, years_to_mlb_estimate,
                    similar_player_ids
                )
                SELECT p.id, 'stub_v1', %s, %s, %s::jsonb
                FROM players p WHERE p.mlb_id = %s
                LIMIT 1
                """,
                (
                    bundle.get("mlb_probability", 0.42),
                    bundle.get("years_to_mlb_estimate", 2.5),
                    similar,
                    mlbam_id,
                ),
            )
        conn.commit()
