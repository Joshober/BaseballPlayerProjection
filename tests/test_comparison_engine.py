"""Unit tests for ml.comparison_engine (v3 KNN + drivers)."""
import numpy as np
import pandas as pd
import pytest

from ml.comparison_engine import (
    top_feature_drivers,
    top_similar_with_drivers,
    weighted_l2,
    V3_ARRIVAL_FEATURES,
)


def test_weighted_l2():
    a = np.array([0.0, 1.0])
    b = np.array([0.0, 2.0])
    w = np.array([1.0, 4.0])
    assert weighted_l2(a, b, w) == pytest.approx(2.0)


def test_top_similar_excludes_self_and_orders_by_distance():
    cols = list(V3_ARRIVAL_FEATURES)
    q = pd.Series({c: 1.0 for c in cols} | {"player_id": 1, "mlb_id": 100, "full_name": "Q", "position_group": None})
    rows = []
    for pid, mlb, offset in [(1, 100, 0.0), (2, 200, 0.5), (3, 300, 2.0)]:
        r = {c: 1.0 + offset for c in cols}
        r["player_id"] = pid
        r["mlb_id"] = mlb
        r["full_name"] = f"P{pid}"
        r["position_group"] = None
        rows.append(r)
    corpus = pd.DataFrame(rows)
    out = top_similar_with_drivers(
        q,
        corpus,
        cols,
        k=2,
        exclude_player_id=1,
        driver_top_n=3,
    )
    assert len(out) == 2
    assert out[0]["mlb_id"] == 200
    assert out[0]["distance"] < out[1]["distance"]
    assert "top_features" in out[0]
    assert isinstance(out[0]["top_features"], list)


def test_top_feature_drivers():
    cols = ["career_milb_iso", "ops_trajectory"]
    q = pd.Series({"career_milb_iso": 1.0, "ops_trajectory": 0.0})
    n = pd.Series({"career_milb_iso": 2.0, "ops_trajectory": 0.0})
    d = top_feature_drivers(q, n, cols, {"career_milb_iso": 1.0, "ops_trajectory": 1.0}, top_n=2)
    assert d[0]["feature"] == "career_milb_iso"
    assert d[0]["delta"] == pytest.approx(-1.0)
