"""KNN-style similarity in feature space (stub weights until training data is wired)."""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


DEFAULT_WEIGHTS: dict[str, float] = {
    "career_milb_bb_pct": 1.2,
    "career_milb_k_pct": 1.0,
    "best_season_ops_level_adj": 1.5,
    "ops_trajectory": 0.8,
    "age_at_peak_level": 0.6,
}


def weighted_l2(a: np.ndarray, b: np.ndarray, w: np.ndarray) -> float:
    d = a - b
    return float(np.sqrt(np.sum(w * (d**2))))


def top_comps(
    query: pd.Series,
    corpus: pd.DataFrame,
    feature_cols: list[str],
    weights: dict[str, float] | None = None,
    k: int = 3,
) -> list[dict[str, Any]]:
    """Return top-k nearest neighbors by weighted Euclidean distance."""
    wdict = weights or DEFAULT_WEIGHTS
    W = np.array([wdict.get(c, 1.0) for c in feature_cols])
    q = query[feature_cols].astype(float).to_numpy()
    rows: list[tuple[float, int]] = []
    for idx, row in corpus.iterrows():
        v = row[feature_cols].astype(float).to_numpy()
        if np.any(np.isnan(q)) or np.any(np.isnan(v)):
            continue
        dist = weighted_l2(q, v, W)
        rows.append((dist, int(idx)))
    rows.sort(key=lambda x: x[0])
    out: list[dict[str, Any]] = []
    for dist, i in rows[:k]:
        out.append({"index": i, "distance": dist})
    return out
