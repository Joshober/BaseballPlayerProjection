"""Train arrival classifier: LogisticRegression baseline + XGBoost; stratified + temporal validation."""
from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import psycopg
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

from db.config import load_project_env
from ml.validation_splits import (
    fetch_first_milb_season_by_player,
    temporal_test_mask,
    temporal_train_mask,
)

load_project_env()

ROOT = Path(__file__).resolve().parents[1]
MODELS = ROOT / "data" / "models"
MODELS.mkdir(parents=True, exist_ok=True)

# Top feature set for arrival (v2 + key v1 aggregates); align with ml/explore_promotion.py
DEFAULT_ARRIVAL_FEATURES: list[str] = [
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
]


def _fetch_features(conn: psycopg.Connection, feature_version: str) -> pd.DataFrame:
    q = """
    SELECT ef.*, pl.mlb_id
    FROM engineered_features ef
    JOIN players pl ON pl.id = ef.player_id
    WHERE ef.feature_version = %s
    """
    with conn.cursor() as cur:
        cur.execute(q, (feature_version,))
        rows = cur.fetchall()
        cols = [d.name for d in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=cols)


def _prepare_matrix(
    df: pd.DataFrame,
    feat_cols: list[str],
) -> tuple[pd.DataFrame, np.ndarray, list[str]]:
    use = [c for c in feat_cols if c in df.columns]
    if len(use) < 3:
        return pd.DataFrame(), np.array([]), []
    X = df[use].copy()
    for c in use:
        if X[c].dtype == object or X[c].dtype == bool:
            X[c] = X[c].astype(float)
    X = X.fillna(0.0)
    y = df["label_reached_mlb"].fillna(False).astype(int).to_numpy()
    return X, y, use


def _auc_safe(y_true: np.ndarray, proba: np.ndarray) -> float:
    if len(np.unique(y_true)) < 2:
        return float("nan")
    return float(roc_auc_score(y_true, proba))


def train_arrival(
    df: pd.DataFrame,
    feat_cols: list[str],
    *,
    random_state: int = 42,
) -> dict[str, Any]:
    if "label_eligible_for_training" in df.columns:
        sub = df[df["label_eligible_for_training"].fillna(True).eq(True)]
    else:
        sub = df
    if len(sub) < 30:
        return {"auc_lr": float("nan"), "auc_xgb": float("nan"), "note": "insufficient rows"}

    X, y, use_cols = _prepare_matrix(sub, feat_cols)
    if len(y) < 30 or y.sum() < 5 or (1 - y).sum() < 5:
        return {"auc_lr": float("nan"), "auc_xgb": float("nan"), "note": "insufficient class balance"}

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=random_state, stratify=y
    )

    pipe_lr = Pipeline(
        [
            ("scaler", StandardScaler()),
            (
                "clf",
                LogisticRegression(max_iter=2000, class_weight="balanced", solver="lbfgs"),
            ),
        ]
    )
    pipe_lr.fit(X_train, y_train)
    proba_lr = pipe_lr.predict_proba(X_test)[:, 1]
    auc_lr = _auc_safe(y_test, proba_lr)

    clf_xgb = XGBClassifier(
        max_depth=6,
        n_estimators=400,
        learning_rate=0.05,
        subsample=0.9,
        eval_metric="logloss",
        random_state=random_state,
    )
    clf_xgb.fit(X_train, y_train)
    proba_x = clf_xgb.predict_proba(X_test)[:, 1]
    auc_xgb = _auc_safe(y_test, proba_x)

    joblib.dump(pipe_lr, MODELS / "arrival_lr.joblib")
    joblib.dump(clf_xgb, MODELS / "arrival_xgb.joblib")
    coefs = pipe_lr.named_steps["clf"].coef_.ravel()
    top_lr = sorted(zip(use_cols, coefs), key=lambda x: abs(x[1]), reverse=True)[:12]

    imp = list(zip(use_cols, clf_xgb.feature_importances_.tolist()))
    imp.sort(key=lambda x: x[1], reverse=True)

    with open(MODELS / "arrival_features.json", "w", encoding="utf-8") as f:
        json.dump(
            {
                "features": use_cols,
                "auc_lr": auc_lr,
                "auc_xgb": auc_xgb,
                "top_lr": top_lr,
                "top_xgb": imp[:12],
            },
            f,
            indent=2,
        )
    _log_drift("arrival_lr", auc_lr)
    _log_drift("arrival_xgb", auc_xgb)

    return {"auc_lr": auc_lr, "auc_xgb": auc_xgb, "features_used": use_cols}


def evaluate_temporal(
    df: pd.DataFrame,
    feat_cols: list[str],
    conn: psycopg.Connection,
    *,
    train_before: int = 2015,
    test_start: int = 2015,
    test_end: int = 2018,
) -> dict[str, float]:
    fs = fetch_first_milb_season_by_player(conn)
    d = df.copy()
    tr = temporal_train_mask(d, fs, train_before_year=train_before)
    te = temporal_test_mask(d, fs, test_start_year=test_start, test_end_year=test_end)
    if "label_eligible_for_training" in d.columns:
        ok = d["label_eligible_for_training"].fillna(True).eq(True)
        tr = tr & ok
        te = te & ok
    sub_tr = d[tr]
    sub_te = d[te]
    if len(sub_te) < 10 or sub_tr["label_reached_mlb"].sum() < 3:
        return {"auc_temporal_xgb": float("nan")}
    X_tr, y_tr, use_cols = _prepare_matrix(sub_tr, feat_cols)
    if not use_cols:
        return {"auc_temporal_xgb": float("nan")}
    X_te, y_te, _ = _prepare_matrix(sub_te, feat_cols)
    for c in use_cols:
        if c not in X_te.columns:
            X_te[c] = 0.0
    X_te = X_te[use_cols].fillna(0.0)
    X_tr = X_tr[use_cols].fillna(0.0)
    y_tr = sub_tr["label_reached_mlb"].fillna(False).astype(int).to_numpy()
    y_te = sub_te["label_reached_mlb"].fillna(False).astype(int).to_numpy()
    clf = XGBClassifier(
        max_depth=6,
        n_estimators=400,
        learning_rate=0.05,
        subsample=0.9,
        eval_metric="logloss",
        random_state=42,
    )
    clf.fit(X_tr, y_tr)
    proba = clf.predict_proba(X_te)[:, 1]
    return {"auc_temporal_xgb": _auc_safe(y_te, proba)}


def evaluate_by_group(df: pd.DataFrame, feat_cols: list[str]) -> dict[str, float]:
    out: dict[str, float] = {}
    if "label_eligible_for_training" in df.columns:
        base = df[df["label_eligible_for_training"].fillna(True).eq(True)]
    else:
        base = df
    for g in ("bat", "sp", "rp"):
        sub = base[base["position_group"].eq(g)]
        X, y, use_cols = _prepare_matrix(sub, feat_cols)
        if len(y) < 20 or y.sum() < 3 or (1 - y).sum() < 3:
            out[f"auc_xgb_{g}"] = float("nan")
            continue
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        clf = XGBClassifier(
            max_depth=5,
            n_estimators=300,
            learning_rate=0.06,
            eval_metric="logloss",
            random_state=42,
        )
        clf.fit(X_train, y_train)
        proba = clf.predict_proba(X_test)[:, 1]
        out[f"auc_xgb_{g}"] = _auc_safe(y_test, proba)
    # Level strata: peak_level_order >= 5 = AA+
    pk = base["peak_level_order"].fillna(0).astype(int)
    for name, mask in (("aa_plus", pk >= 5), ("below_aa", pk < 5)):
        sub = base[mask]
        X, y, _ = _prepare_matrix(sub, feat_cols)
        if len(y) < 20 or y.sum() < 3 or (1 - y).sum() < 3:
            out[f"auc_xgb_{name}"] = float("nan")
            continue
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        clf = XGBClassifier(
            max_depth=5,
            n_estimators=300,
            learning_rate=0.06,
            eval_metric="logloss",
            random_state=42,
        )
        clf.fit(X_train, y_train)
        proba = clf.predict_proba(X_test)[:, 1]
        out[f"auc_xgb_{name}"] = _auc_safe(y_test, proba)
    return out


def _log_drift(model: str, metric: float) -> None:
    p = MODELS / "drift_log.csv"
    row = [pd.Timestamp.utcnow().isoformat(), model, metric]
    write_header = not p.is_file()
    with p.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(["ts_utc", "model", "metric"])
        w.writerow(row)


def train_timeline(df: pd.DataFrame) -> dict[str, float]:
    from sklearn.metrics import mean_absolute_error
    from xgboost import XGBRegressor

    feat_cols = [c for c in df.columns if c.startswith("career_milb_")][:12]
    sub = df[df["label_reached_mlb"].eq(True)].dropna(subset=["label_years_to_mlb"])
    if len(sub) < 20 or len(feat_cols) < 3:
        return {"mae": float("nan"), "note": "insufficient timeline labels"}
    X = sub[feat_cols].fillna(0).to_numpy()
    y = sub["label_years_to_mlb"].astype(float).to_numpy()
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)
    reg = XGBRegressor(
        max_depth=6,
        n_estimators=500,
        learning_rate=0.05,
        objective="reg:squarederror",
    )
    reg.fit(X_train, y_train)
    pred = reg.predict(X_test)
    mae = float(mean_absolute_error(y_test, pred))
    joblib.dump(reg, MODELS / "timeline_xgb.joblib")
    with open(MODELS / "timeline_features.json", "w", encoding="utf-8") as f:
        json.dump({"features": feat_cols, "mae": mae}, f, indent=2)
    _log_drift("timeline", mae)
    return {"mae": mae}


def main() -> None:
    parser = argparse.ArgumentParser(description="Train ScoutPro arrival models (LR + XGB)")
    parser.add_argument("--feature-version", default="v2")
    parser.add_argument(
        "--features",
        default=None,
        help="Comma-separated feature columns (default: built-in list)",
    )
    args = parser.parse_args()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL not set; training skipped")
        return

    feat_cols = [x.strip() for x in args.features.split(",")] if args.features else list(DEFAULT_ARRIVAL_FEATURES)

    with psycopg.connect(database_url) as conn:
        df = _fetch_features(conn, args.feature_version)
        temporal = evaluate_temporal(df, feat_cols, conn)
        groups = evaluate_by_group(df, feat_cols)

    print("rows:", len(df))
    arr = train_arrival(df, feat_cols)
    tl = train_timeline(df)
    print("arrival:", arr)
    print("timeline:", tl)
    print("temporal_validation:", temporal)
    print("subgroup_auc:", groups)


if __name__ == "__main__":
    main()
