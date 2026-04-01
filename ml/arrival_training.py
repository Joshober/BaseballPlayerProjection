"""Train separate MiLB→MLB arrival models for hitters and pitchers (LR, RF, XGB + calibration)."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import psycopg
from sklearn.base import clone
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

ROOT = Path(__file__).resolve().parents[1]
MODELS = ROOT / "data" / "models"

DEFAULT_FEATURES_V2: list[str] = [
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

# v3 adds cohort-relative percentiles (nullable → filled with 0 at matrix prep)
DEFAULT_FEATURES_V3: list[str] = DEFAULT_FEATURES_V2 + [
    "ops_pctile_milb_weighted",
    "era_pctile_milb_weighted",
    "age_pctile_milb_weighted",
]


def default_feature_list(feature_version: str) -> list[str]:
    if str(feature_version).startswith("v3"):
        return list(DEFAULT_FEATURES_V3)
    return list(DEFAULT_FEATURES_V2)


def _eligible(df: pd.DataFrame) -> pd.DataFrame:
    if "label_eligible_for_training" in df.columns:
        return df[df["label_eligible_for_training"].fillna(True).eq(True)]
    return df


def _prepare_matrix(
    df: pd.DataFrame, feat_cols: list[str]
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


def _safe_auc(y, proba) -> float:
    if len(np.unique(y)) < 2:
        return float("nan")
    return float(roc_auc_score(y, proba))


def _safe_pr_auc(y, proba) -> float:
    if len(np.unique(y)) < 2:
        return float("nan")
    return float(average_precision_score(y, proba))


def _score_model(y_test: np.ndarray, proba: np.ndarray) -> dict[str, float]:
    out: dict[str, float] = {}
    out["roc_auc"] = _safe_auc(y_test, proba)
    out["pr_auc"] = _safe_pr_auc(y_test, proba)
    try:
        out["log_loss"] = float(log_loss(y_test, np.clip(proba, 1e-6, 1 - 1e-6)))
    except ValueError:
        out["log_loss"] = float("nan")
    out["brier"] = float(brier_score_loss(y_test, proba))
    return out


def _composite(roc: float, brier: float) -> float:
    if np.isnan(roc):
        return -1.0
    br = brier if not np.isnan(brier) else 0.5
    return float(roc - 0.5 * br)


def train_one_role(
    df: pd.DataFrame,
    feat_cols: list[str],
    role: str,
    *,
    random_state: int = 42,
) -> dict[str, Any]:
    """role: 'bat' or 'pitch' (sp+rp)."""
    sub = _eligible(df)
    if role == "bat":
        sub = sub[sub["position_group"].eq("bat")]
    else:
        sub = sub[sub["position_group"].isin(["sp", "rp"])]

    out: dict[str, Any] = {"role": role, "note": None}
    if len(sub) < 30:
        out["note"] = "insufficient rows"
        return out
    X, y, use_cols = _prepare_matrix(sub, feat_cols)
    if len(y) < 30 or y.sum() < 5 or (1 - y).sum() < 5:
        out["note"] = "insufficient class balance"
        return out

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=random_state, stratify=y
    )

    candidates: list[tuple[str, Any, dict[str, float]]] = []

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
    m_lr = _score_model(y_test, proba_lr)
    candidates.append(("logistic_regression", pipe_lr, m_lr))

    rf = RandomForestClassifier(
        n_estimators=400,
        max_depth=12,
        min_samples_leaf=3,
        class_weight="balanced_subsample",
        random_state=random_state,
        n_jobs=-1,
    )
    rf.fit(X_train, y_train)
    proba_rf = rf.predict_proba(X_test)[:, 1]
    m_rf = _score_model(y_test, proba_rf)
    candidates.append(("random_forest", rf, m_rf))

    xgb = XGBClassifier(
        max_depth=6,
        n_estimators=400,
        learning_rate=0.05,
        subsample=0.9,
        eval_metric="logloss",
        random_state=random_state,
    )
    xgb.fit(X_train, y_train)
    proba_x = xgb.predict_proba(X_test)[:, 1]
    m_x = _score_model(y_test, proba_x)
    candidates.append(("xgboost", xgb, m_x))

    best_name, best_est, best_metrics = max(
        candidates,
        key=lambda t: _composite(t[2]["roc_auc"], t[2]["brier"]),
    )

    n_splits = min(5, max(2, len(y_train) // 8))
    cal = CalibratedClassifierCV(
        clone(best_est),
        method="isotonic",
        cv=n_splits,
    )
    cal.fit(X_train, y_train)
    proba_cal = cal.predict_proba(X_test)[:, 1]
    metrics_cal = _score_model(y_test, proba_cal)

    artifact_name = "bat_arrival.joblib" if role == "bat" else "pitch_arrival.joblib"
    path = MODELS / artifact_name
    joblib.dump(cal, path)

    out.update(
        {
            "algorithm_raw": best_name,
            "artifact": artifact_name,
            "features_used": use_cols,
            "metrics_raw": best_metrics,
            "metrics_calibrated": metrics_cal,
            "top_importance": _top_xgb_importance(xgb, use_cols) if best_name == "xgboost" else [],
        }
    )
    return out


def _top_xgb_importance(xgb: XGBClassifier, cols: list[str], n: int = 12) -> list[dict[str, float]]:
    imp = list(zip(cols, xgb.feature_importances_.tolist()))
    imp.sort(key=lambda x: x[1], reverse=True)
    return [{"feature": a, "importance": float(b)} for a, b in imp[:n]]


def train_arrival_by_role(
    df: pd.DataFrame,
    feat_cols: list[str],
    feature_version: str,
    conn: psycopg.Connection | None = None,
) -> dict[str, Any]:
    MODELS.mkdir(parents=True, exist_ok=True)
    bat = train_one_role(df, feat_cols, "bat")
    pitch = train_one_role(df, feat_cols, "pitch")

    manifest: dict[str, Any] = {
        "feature_version": feature_version,
        "roles": {
            "bat": {k: v for k, v in bat.items() if k != "role"},
            "pitch": {k: v for k, v in pitch.items() if k != "role"},
        },
    }
    man_path = MODELS / "arrival_manifest.json"
    with man_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    # Legacy single-file compatibility for older inference paths
    if bat.get("artifact"):
        try:
            import shutil

            shutil.copy(MODELS / bat["artifact"], MODELS / "arrival_xgb.joblib")
        except OSError:
            pass

    if conn is not None:
        _upsert_registry(conn, "arrival_bat", feature_version, bat)
        _upsert_registry(conn, "arrival_pitch", feature_version, pitch)

    return {"manifest_path": str(man_path), "bat": bat, "pitch": pitch}


def _upsert_registry(
    conn: psycopg.Connection,
    model_name: str,
    feature_version: str,
    role_result: dict[str, Any],
) -> None:
    metrics = role_result.get("metrics_calibrated") or role_result.get("metrics_raw")
    if not metrics:
        return
    algo = role_result.get("algorithm_raw", "")
    art = role_result.get("artifact", "")
    notes = json.dumps({"metrics": metrics, "algorithm": algo})
    auc = metrics.get("roc_auc")
    brier = metrics.get("brier")
    ver = feature_version.replace(".", "_")[:20]
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO model_registry (
                model_name, version, feature_version, algorithm,
                auc_roc, brier_score, artifact_path, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (model_name, version) DO UPDATE SET
                feature_version = EXCLUDED.feature_version,
                algorithm = EXCLUDED.algorithm,
                auc_roc = EXCLUDED.auc_roc,
                brier_score = EXCLUDED.brier_score,
                artifact_path = EXCLUDED.artifact_path,
                notes = EXCLUDED.notes,
                trained_at = NOW()
            """,
            (
                model_name,
                ver,
                feature_version,
                algo,
                auc,
                brier,
                art,
                notes,
            ),
        )
    conn.commit()
