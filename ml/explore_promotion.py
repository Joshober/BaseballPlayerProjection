"""Exploratory analysis: group means, histograms, correlation pruning (Phase 4)."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pandas as pd
import psycopg

from db.config import load_project_env

load_project_env()

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "reports"
OUT.mkdir(parents=True, exist_ok=True)

DEFAULT_NUMERIC_FEATURES = [
    "career_age_vs_level_avg",
    "career_milb_bb_pct",
    "career_milb_k_pct",
    "promotion_speed_score",
    "career_milb_iso",
    "ops_trajectory",
    "seasons_in_minors",
]


def _fetch(conn: psycopg.Connection, feature_version: str) -> pd.DataFrame:
    q = """
    SELECT ef.* FROM engineered_features ef
    WHERE ef.feature_version = %s
    """
    with conn.cursor() as cur:
        cur.execute(q, (feature_version,))
        rows = cur.fetchall()
        cols = [d.name for d in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=cols)


def _means_by_label(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    sub = df[df["label_eligible_for_training"].fillna(True).eq(True)] if "label_eligible_for_training" in df.columns else df
    out = []
    for c in cols:
        if c not in sub.columns or sub[c].dtype == object:
            continue
        for reached, name in ((True, "reached_mlb"), (False, "not_mlb")):
            slice_ = sub[sub["label_reached_mlb"].eq(reached)][c].dropna()
            out.append({"feature": c, "group": name, "mean": float(slice_.mean()) if len(slice_) else None, "n": len(slice_)})
    return pd.DataFrame(out)


def _correlation_prune(df: pd.DataFrame, cols: list[str], threshold: float = 0.85) -> list[tuple[str, str, float]]:
    use = [c for c in cols if c in df.columns and pd.api.types.is_numeric_dtype(df[c])]
    if len(use) < 2:
        return []
    m = df[use].fillna(0).corr()
    pairs: list[tuple[str, str, float]] = []
    for i, a in enumerate(use):
        for b in use[i + 1 :]:
            v = float(m.loc[a, b])
            if abs(v) >= threshold:
                pairs.append((a, b, v))
    return pairs


def main() -> None:
    parser = argparse.ArgumentParser(description="EDA for MLB promotion features")
    parser.add_argument("--feature-version", default="v2")
    parser.add_argument("--corr-threshold", type=float, default=0.85)
    args = parser.parse_args()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    with psycopg.connect(database_url) as conn:
        df = _fetch(conn, args.feature_version)

    if df.empty:
        print("No engineered_features rows for this version.")
        return

    num_cols = [c for c in df.columns if c.startswith(("career_", "promotion_", "ops_", "age_", "peak_", "seasons_", "draft_", "ever_", "low_", "is_", "k_minus_")) and df[c].dtype != object]
    means = _means_by_label(df, num_cols or DEFAULT_NUMERIC_FEATURES)
    means_path = OUT / f"eda_means_{args.feature_version}.csv"
    means.to_csv(means_path, index=False)
    print(f"Wrote {means_path}")

    corr_pairs = _correlation_prune(df, num_cols or DEFAULT_NUMERIC_FEATURES, args.corr_threshold)
    with open(OUT / f"eda_correlations_{args.feature_version}.json", "w", encoding="utf-8") as f:
        json.dump([{"a": a, "b": b, "r": r} for a, b, r in corr_pairs], f, indent=2)
    print("Correlation pairs |r|>=", args.corr_threshold, ":", len(corr_pairs))

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        plot_cols = [c for c in DEFAULT_NUMERIC_FEATURES if c in df.columns][:4]
        sub = df[df["label_eligible_for_training"].fillna(True).eq(True)] if "label_eligible_for_training" in df.columns else df
        fig, axes = plt.subplots(2, 2, figsize=(10, 8))
        axes = axes.ravel()
        for ax, col in zip(axes, plot_cols):
            for reached, label in ((True, "reached"), (False, "not")):
                s = sub[sub["label_reached_mlb"].eq(reached)][col].dropna()
                if len(s) > 1:
                    ax.hist(s, bins=20, alpha=0.5, label=label, density=True)
            ax.set_title(col)
            ax.legend()
        fig.tight_layout()
        ppath = OUT / f"eda_hist_{args.feature_version}.png"
        fig.savefig(ppath, dpi=120)
        plt.close()
        print(f"Wrote {ppath}")
    except ImportError:
        print("matplotlib not installed; skipping histograms.")


if __name__ == "__main__":
    main()
