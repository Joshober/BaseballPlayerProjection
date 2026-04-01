from __future__ import annotations

import argparse
import os

from db.config import load_project_env
from ml.feature_engineering import build_and_upsert_features


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 1: Build engineered features and labels")
    parser.add_argument("--feature-version", default="v2", help="Feature version tag (default: v2)")
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

    result = build_and_upsert_features(
        database_url=database_url,
        feature_version=args.feature_version,
        cohort_player_ids=cohort_ids,
    )
    print(
        f"Feature build complete. version={result.feature_version} "
        f"built_rows={result.built_rows} upserted_rows={result.upserted_rows}"
    )


if __name__ == "__main__":
    main()
