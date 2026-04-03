"""KNN-style similarity in arrival-model feature space (v3 aligned with arrival_manifest)."""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

# Same feature names as data/models/arrival_manifest.json roles.bat.features_used (bat == pitch list).
V3_ARRIVAL_FEATURES: tuple[str, ...] = (
    "career_age_vs_level_avg",
    "career_milb_bb_pct",
    "career_milb_k_pct",
    "career_milb_iso",
    "promotion_speed_score",
    "ever_repeated_level",
    "ops_yoy_delta",
    "ops_trajectory",
    "seasons_in_minors",
    "peak_level_order",
    "age_at_pro_debut",
    "draft_round_feat",
    "low_sample_season_flag",
    "ops_pctile_milb_weighted",
    "era_pctile_milb_weighted",
    "age_pctile_milb_weighted",
)

# Uniform weights by default; tune from model coefficients later.
V3_ARRIVAL_FEATURE_WEIGHTS: dict[str, float] = {f: 1.0 for f in V3_ARRIVAL_FEATURES}


def weighted_l2(a: np.ndarray, b: np.ndarray, w: np.ndarray) -> float:
    d = a - b
    return float(np.sqrt(np.sum(w * (d**2))))


def _series_to_float_vec(series: pd.Series, feature_cols: list[str]) -> np.ndarray | None:
    vals: list[float] = []
    for c in feature_cols:
        v = series.get(c)
        if v is None:
            return None
        if isinstance(v, (bool, np.bool_)):
            vals.append(1.0 if v else 0.0)
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            return None
        if np.isnan(fv):
            return None
        vals.append(fv)
    return np.array(vals)


def top_feature_drivers(
    query: pd.Series,
    neighbor: pd.Series,
    feature_cols: list[str],
    weights: dict[str, float] | None = None,
    top_n: int = 5,
) -> list[dict[str, Any]]:
    """Shared drivers: largest weighted squared deltas between query and neighbor."""
    wdict = weights or V3_ARRIVAL_FEATURE_WEIGHTS
    contrib: list[dict[str, Any]] = []
    for c in feature_cols:
        qv = query.get(c)
        nv = neighbor.get(c)
        if qv is None or nv is None:
            continue
        qf = float(bool(qv)) if isinstance(qv, (bool, np.bool_)) else float(qv)
        nf = float(bool(nv)) if isinstance(nv, (bool, np.bool_)) else float(nv)
        if np.isnan(qf) or np.isnan(nf):
            continue
        w = float(wdict.get(c, 1.0))
        delta = qf - nf
        weighted_sq = w * (delta**2)
        contrib.append(
            {
                "feature": c,
                "delta": float(delta),
                "weighted_sq": float(weighted_sq),
            }
        )
    contrib.sort(key=lambda x: -x["weighted_sq"])
    return contrib[:top_n]


def top_comps(
    query: pd.Series,
    corpus: pd.DataFrame,
    feature_cols: list[str],
    weights: dict[str, float] | None = None,
    k: int = 3,
) -> list[dict[str, Any]]:
    """Return top-k nearest neighbors by weighted Euclidean distance (legacy shape)."""
    wdict = weights or V3_ARRIVAL_FEATURE_WEIGHTS
    W = np.array([wdict.get(c, 1.0) for c in feature_cols])
    q = _series_to_float_vec(query, feature_cols)
    if q is None:
        return []
    rows: list[tuple[float, int]] = []
    for idx, row in corpus.iterrows():
        v = _series_to_float_vec(row, feature_cols)
        if v is None:
            continue
        dist = weighted_l2(q, v, W)
        rows.append((dist, int(idx)))
    rows.sort(key=lambda x: x[0])
    out: list[dict[str, Any]] = []
    for dist, i in rows[:k]:
        out.append({"index": i, "distance": dist})
    return out


def top_similar_with_drivers(
    query: pd.Series,
    corpus: pd.DataFrame,
    feature_cols: list[str],
    *,
    weights: dict[str, float] | None = None,
    k: int = 5,
    exclude_player_id: int | None = None,
    driver_top_n: int = 5,
) -> list[dict[str, Any]]:
    """Top-k similar players with distance, similarity score, and top feature deltas."""
    if corpus is None or corpus.empty:
        return []
    wdict = weights or V3_ARRIVAL_FEATURE_WEIGHTS
    W = np.array([wdict.get(c, 1.0) for c in feature_cols])
    q = _series_to_float_vec(query, feature_cols)
    if q is None:
        return []
    scored: list[tuple[float, pd.Series]] = []
    for _, nrow in corpus.iterrows():
        pid = nrow.get("player_id")
        if exclude_player_id is not None and pid is not None and int(pid) == int(exclude_player_id):
            continue
        v = _series_to_float_vec(nrow, feature_cols)
        if v is None:
            continue
        dist = weighted_l2(q, v, W)
        scored.append((dist, nrow))
    scored.sort(key=lambda x: x[0])
    out: list[dict[str, Any]] = []
    for dist, nrow in scored[:k]:
        drivers = top_feature_drivers(query, nrow, feature_cols, wdict, driver_top_n)
        mlb = nrow.get("mlb_id")
        sim = float(1.0 / (1.0 + dist)) if dist >= 0 else 0.0
        entry: dict[str, Any] = {
            "mlb_id": int(mlb) if mlb is not None and not (isinstance(mlb, float) and np.isnan(mlb)) else None,
            "name": nrow.get("full_name"),
            "similarity": round(sim, 6),
            "distance": float(dist),
            "top_features": drivers,
        }
        out.append(entry)
    return out
