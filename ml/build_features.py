from __future__ import annotations

import argparse
import os

from db.config import load_project_env
from ml.cutoff_policy import DEFAULT_FIRST_K_MILB_SEASONS
from ml.feature_engineering import build_and_upsert_features


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 1: Build engineered features and labels")
    parser.add_argument("--feature-version", default="v2", help="Feature version tag (use v3 for cutoff-safe features)")
    parser.add_argument(
        "--first-k-milb-seasons",
        type=int,
        default=None,
        help="If set, only MiLB stats from the first K distinct seasons are used (leakage-safe). Default for v3: 2.",
    )
    parser.add_argument(
        "--cohort-train-ids",
        default=None,
        help="Optional comma-separated player ids to limit cohort age means (leak-safe temporal fits)",
    )
    args = parser.parse_args()

    load_project_env()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    cohort_ids = None
    if args.cohort_train_ids:
        cohort_ids = {int(x.strip()) for x in args.cohort_train_ids.split(",") if x.strip().isdigit()}

    first_k = args.first_k_milb_seasons
    if first_k is None and str(args.feature_version).startswith("v3"):
        first_k = DEFAULT_FIRST_K_MILB_SEASONS

    result = build_and_upsert_features(
        database_url=database_url,
        feature_version=args.feature_version,
        cohort_player_ids=cohort_ids,
        first_k_milb_seasons=first_k,
    )
    print(
        f"Feature build complete. version={result.feature_version} "
        f"built_rows={result.built_rows} upserted_rows={result.upserted_rows}"
    )


if __name__ == "__main__":
    main()
