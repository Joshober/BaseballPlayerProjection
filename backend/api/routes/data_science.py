"""Dataset health, model card, feature glossary, and two-player comparison."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import psycopg
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.api.deps import require_auth

router = APIRouter(tags=["data-science"], prefix="")


ROOT = Path(__file__).resolve().parents[3]
MODELS = ROOT / "data" / "models"

FEATURE_GLOSSARY: dict[str, str] = {
    "peak_level_order": "Highest MiLB level reached (numeric: 1=Rk … 6=AAA).",
    "peak_level": "Highest MiLB level label (e.g. AA, AAA).",
    "seasons_in_minors": "Count of distinct seasons with MiLB playing time (within cutoff window for v3).",
    "promotion_speed_score": "Higher = faster climb by level tier per season in the minors.",
    "career_age_vs_level_avg": "PA/IP-weighted mean of (player age − league-average age at that level).",
    "ops_trajectory": "Linear slope of OPS across MiLB seasons in the feature window.",
    "ops_yoy_delta": "Change in season-weighted OPS from prior season to latest in window.",
    "ever_repeated_level": "True if the player spent multiple seasons at the same level.",
    "career_milb_bb_pct": "Walk rate BB/PA aggregated over the feature window.",
    "career_milb_k_pct": "Strikeout rate SO/PA aggregated over the feature window.",
    "career_milb_iso": "Mean isolated power (SLG − AVG) over batting lines in window.",
    "age_at_pro_debut": "Age at first MiLB line in the window.",
    "draft_round_feat": "MLB draft round when known (undrafted/international may be null).",
    "low_sample_season_flag": "True when PA or IP in a season is too small for stable rates.",
    "ops_pctile_milb_weighted": "PA-weighted mean of within-level-year OPS percentiles (v3).",
    "era_pctile_milb_weighted": "IP-weighted mean of within-level-year ERA percentiles (v3; higher=better).",
    "age_pctile_milb_weighted": "Weighted mean age percentile vs peers at level-year (v3).",
    "prediction_cutoff_season": "Last MiLB season included under the v3 first-K-seasons policy.",
    "cutoff_policy": "Human-readable cutoff rule, e.g. first_k_milb_seasons:2.",
    "label_reached_mlb": "Training label: player eventually reached MLB.",
    "label_years_to_mlb": "Years from first MiLB season to MLB debut (when known).",
}


@router.get("/data/summary")
def dataset_summary(
    feature_version: str | None = None,
    _user: dict = Depends(require_auth),
) -> dict[str, Any]:
    from ml.data_status import report_extended

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise HTTPException(status_code=503, detail="DATABASE_URL not configured")
    try:
        return report_extended(feature_version)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/models/card")
def model_card(_user: dict = Depends(require_auth)) -> dict[str, Any]:
    database_url = os.getenv("DATABASE_URL")
    out: dict[str, Any] = {"manifest": None, "registry": [], "artifact_dir": str(MODELS)}
    mp = MODELS / "arrival_manifest.json"
    if mp.is_file():
        with mp.open(encoding="utf-8") as f:
            out["manifest"] = json.load(f)
    if database_url:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT model_name, version, feature_version, algorithm, auc_roc, brier_score,
                           artifact_path, trained_at, notes
                    FROM model_registry
                    WHERE model_name IN ('arrival_bat', 'arrival_pitch')
                    ORDER BY trained_at DESC NULLS LAST
                    LIMIT 8
                    """
                )
                cols = [d.name for d in cur.description] if cur.description else []
                for row in cur.fetchall() or []:
                    out["registry"].append(dict(zip(cols, row)))
    return out


@router.get("/data/glossary")
def feature_glossary(_user: dict = Depends(require_auth)) -> dict[str, str]:
    return FEATURE_GLOSSARY


class CompareBody(BaseModel):
    mlbam_id_a: int = Field(..., description="MLB Advanced Media player id")
    mlbam_id_b: int = Field(..., description="MLB Advanced Media player id")
    feature_version: str = "v3"


COMPARE_KEYS: list[str] = [
    "peak_level_order",
    "promotion_speed_score",
    "career_age_vs_level_avg",
    "ops_trajectory",
    "career_milb_bb_pct",
    "career_milb_k_pct",
    "ops_pctile_milb_weighted",
    "era_pctile_milb_weighted",
    "seasons_in_minors",
]


def _explain_why(a: dict[str, Any], b: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    try:

        def gt(key: str, label: str) -> None:
            va, vb = a.get(key), b.get(key)
            if va is None or vb is None:
                return
            if float(va) > float(vb) + 1e-6:
                reasons.append(f"A higher {label} than B.")
            elif float(vb) > float(va) + 1e-6:
                reasons.append(f"B higher {label} than A.")

        gt("promotion_speed_score", "promotion speed")
        gt("ops_pctile_milb_weighted", "OPS vs peers at level")
        gt("era_pctile_milb_weighted", "ERA vs peers at level (pitchers)")
        po = a.get("peak_level_order")
        pb = b.get("peak_level_order")
        if po is not None and pb is not None:
            if float(po) > float(pb):
                reasons.append("A reached a higher MiLB level in the feature window.")
            elif float(pb) > float(po):
                reasons.append("B reached a higher MiLB level in the feature window.")
    except (TypeError, ValueError):
        pass
    return reasons[:6]


@router.post("/compare/players")
def compare_players(body: CompareBody, _user: dict = Depends(require_auth)) -> dict[str, Any]:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise HTTPException(status_code=503, detail="DATABASE_URL not configured")

    q = """
    SELECT pl.mlb_id, pl.full_name, pl.position, ef.*
    FROM engineered_features ef
    JOIN players pl ON pl.id = ef.player_id
    WHERE pl.mlb_id = %s AND ef.feature_version = %s
    LIMIT 1
    """
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (body.mlbam_id_a, body.feature_version))
            ca = cur.description
            ra = cur.fetchone()
            cur.execute(q, (body.mlbam_id_b, body.feature_version))
            cb = cur.description
            rb = cur.fetchone()

    if not ra or not ca:
        raise HTTPException(
            status_code=404,
            detail=f"No engineered_features for player A (mlbam={body.mlbam_id_a}, version={body.feature_version})",
        )
    if not rb or not cb:
        raise HTTPException(
            status_code=404,
            detail=f"No engineered_features for player B (mlbam={body.mlbam_id_b}, version={body.feature_version})",
        )

    cols_a = [d.name for d in ca]
    da = dict(zip(cols_a, ra))
    cols_b = [d.name for d in cb]
    db = dict(zip(cols_b, rb))

    side_a = {k: da.get(k) for k in COMPARE_KEYS if k in da}
    side_b = {k: db.get(k) for k in COMPARE_KEYS if k in db}
    deltas = {}
    for k in COMPARE_KEYS:
        if k in da and k in db and da[k] is not None and db[k] is not None:
            try:
                deltas[k] = float(da[k]) - float(db[k])
            except (TypeError, ValueError):
                continue

    return {
        "feature_version": body.feature_version,
        "player_a": {"mlbam_id": body.mlbam_id_a, "full_name": da.get("full_name"), "position": da.get("position"), "features": side_a},
        "player_b": {"mlbam_id": body.mlbam_id_b, "full_name": db.get("full_name"), "position": db.get("position"), "features": side_b},
        "deltas": deltas,
        "why_a_vs_b": _explain_why(da, db),
    }
