"""Inference — load calibrated arrival models and score engineered_features rows."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import psycopg
from psycopg.rows import dict_row

from db.config import load_project_env
from ml.comparison_engine import (
    top_similar_with_drivers,
    V3_ARRIVAL_FEATURE_WEIGHTS,
    V3_ARRIVAL_FEATURES,
)

logger = logging.getLogger(__name__)

load_project_env()

ROOT = Path(__file__).resolve().parents[3]
MODELS = ROOT / "data" / "models"

_manifest: dict[str, Any] | None = None
_models: dict[str, Any] = {}


def _load_manifest() -> dict[str, Any] | None:
    global _manifest
    p = MODELS / "arrival_manifest.json"
    if not p.is_file():
        return None
    with p.open(encoding="utf-8") as f:
        _manifest = json.load(f)
    return _manifest


def _model_for_role(role: str) -> Any | None:
    """role: bat | pitch"""
    if role in _models:
        return _models[role]
    m = _load_manifest()
    if not m:
        return None
    roles = m.get("roles") or {}
    key = "bat" if role == "bat" else "pitch"
    art = (roles.get(key) or {}).get("artifact")
    if not art:
        return None
    path = MODELS / str(art)
    if not path.is_file():
        return None
    _models[role] = joblib.load(path)
    return _models[role]


def _feature_list_for_role(role: str) -> list[str]:
    m = _load_manifest()
    if not m:
        return []
    r = (m.get("roles") or {}).get(role) or {}
    return list(r.get("features_used") or [])


def models_loaded() -> bool:
    """True when arrival_manifest.json exists and every role artifact file is on disk."""
    m = _load_manifest()
    if not m:
        return False
    for role in ("bat", "pitch"):
        r = (m.get("roles") or {}).get(role) or {}
        art = r.get("artifact")
        if not art:
            return False
        if not (MODELS / str(art)).is_file():
            return False
    return True


def _position_to_role(position_group: str | None) -> str:
    if not position_group:
        return "bat"
    pg = str(position_group).lower()
    if pg in ("sp", "rp"):
        return "pitch"
    return "bat"


def _fetch_knn_corpus(
    database_url: str,
    feature_version: str,
    role: str,
    exclude_player_id: int,
    feature_cols: list[str],
) -> pd.DataFrame:
    """Rows from engineered_features for same feature_version and role cohort (bat vs pitch)."""
    allowed = set(V3_ARRIVAL_FEATURES)
    for c in feature_cols:
        if c not in allowed:
            logger.warning("KNN corpus: feature %s not in v3 arrival set, aborting corpus load", c)
            return pd.DataFrame()
    meta = "pl.id AS player_id, pl.mlb_id, pl.full_name, ef.position_group"
    feat_sql = ", ".join(f"ef.{c}" for c in feature_cols)
    sql = f"""
        SELECT {meta}, {feat_sql}
        FROM engineered_features ef
        JOIN players pl ON pl.id = ef.player_id
        WHERE ef.feature_version = %s
          AND pl.id != %s
          AND (
            (%s = 'bat' AND (ef.position_group IS NULL OR LOWER(ef.position_group) NOT IN ('sp', 'rp')))
            OR (%s = 'pitch' AND LOWER(ef.position_group) IN ('sp', 'rp'))
          )
    """
    with psycopg.connect(database_url) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (feature_version, exclude_player_id, role, role))
            rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def generate_full_report(mlbam_id: int) -> dict[str, Any]:
    """Return prediction bundle for an MLBAM id; honest insufficient_data when models/rows missing."""
    database_url = os.getenv("DATABASE_URL")
    out: dict[str, Any] = {
        "mlbam_id": mlbam_id,
        "mlb_probability": None,
        "years_to_mlb_estimate": None,
        "similar_players": [],
        "model_version": None,
        "feature_version": None,
        "scored_at": None,
        "top_features": [],
        "insufficient_data": True,
        "note": None,
    }
    if not database_url:
        out["note"] = "DATABASE_URL not configured"
        return out

    m = _load_manifest()
    if not m:
        out["note"] = "No trained models (run ml.train_all after building features)"
        return out

    fv = str(m.get("feature_version") or "v3")
    out["feature_version"] = fv

    row = None
    cols: list[str] = []
    tried: list[str] = []
    for version in (fv, "v3", "v2"):
        if version in tried:
            continue
        tried.append(version)
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ef.* FROM engineered_features ef
                    JOIN players pl ON pl.id = ef.player_id
                    WHERE pl.mlb_id = %s AND ef.feature_version = %s
                    LIMIT 1
                    """,
                    (mlbam_id, version),
                )
                row = cur.fetchone()
                cols = [d.name for d in cur.description] if cur.description else []
        if row:
            out["feature_version"] = version
            break
    if not row:
        out["note"] = (
            f"No engineered_features row for {mlbam_id} (tried {tried}); run build_features for this player."
        )
        return out

    ef = dict(zip(cols, row))
    role = _position_to_role(ef.get("position_group"))
    model = _model_for_role(role)
    feats = _feature_list_for_role(role)
    if model is None or not feats:
        out["note"] = f"No trained model artifact for role={role}"
        return out

    if ef.get("low_sample_season_flag") is True:
        out["note"] = "Low MiLB sample for this player — probability suppressed."
        return out

    x = np.zeros((1, len(feats)))
    for i, c in enumerate(feats):
        v = ef.get(c)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            x[0, i] = 0.0
        else:
            x[0, i] = float(v)

    try:
        proba = float(model.predict_proba(x)[0, 1])
    except Exception as exc:
        out["note"] = f"Scoring error: {exc}"
        return out

    out["mlb_probability"] = proba
    out["insufficient_data"] = False
    out["model_version"] = f"arrival_{role}_{fv}"
    out["scored_at"] = datetime.now(timezone.utc).isoformat()
    out["top_features"] = []
    q = pd.Series(ef)
    try:
        corpus_df = _fetch_knn_corpus(
            database_url,
            str(out["feature_version"]),
            role,
            int(ef["player_id"]),
            feats,
        )
        w = {c: V3_ARRIVAL_FEATURE_WEIGHTS.get(c, 1.0) for c in feats}
        out["similar_players"] = top_similar_with_drivers(
            q,
            corpus_df,
            feats,
            weights=w,
            k=5,
            exclude_player_id=int(ef["player_id"]),
            driver_top_n=5,
        )
    except Exception as exc:
        logger.warning("Similar players KNN failed for mlbam_id=%s: %s", mlbam_id, exc)
        out["similar_players"] = []
    return out


def store_prediction_stub(mlbam_id: int, bundle: dict[str, Any]) -> None:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return
    similar = json.dumps(bundle.get("similar_players") or [])
    prob = bundle.get("mlb_probability")
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO predictions (
                    player_id, model_version, mlb_probability, years_to_mlb_estimate,
                    similar_player_ids
                )
                SELECT p.id, %s, %s, %s, %s::jsonb
                FROM players p WHERE p.mlb_id = %s
                LIMIT 1
                """,
                (
                    bundle.get("model_version") or "scoutpro_arrival",
                    prob,
                    bundle.get("years_to_mlb_estimate"),
                    similar,
                    mlbam_id,
                ),
            )
        conn.commit()


def store_comparison_stub(mlbam_id: int, bundle: dict[str, Any]) -> None:
    """Persist latest similar-player KNN output for GET /comparisons/{mlbam_id}."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return
    comps = bundle.get("similar_players") or []
    if not comps:
        return
    payload = {
        "feature_version": bundle.get("feature_version"),
        "model": "knn_weighted_l2_v3",
        "comps": comps,
    }
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO comparisons (player_id, comp_json)
                SELECT p.id, %s::jsonb
                FROM players p WHERE p.mlb_id = %s
                LIMIT 1
                """,
                (json.dumps(payload), mlbam_id),
            )
        conn.commit()
