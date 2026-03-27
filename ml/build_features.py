from __future__ import annotations

import argparse
import os

from db.config import load_project_env
from ml.feature_engineering import build_and_upsert_features


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 1: Build engineered features and labels")
    parser.add_argument("--feature-version", default="v1", help="Feature version tag (default: v1)")
    args = parser.parse_args()

    load_project_env()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    result = build_and_upsert_features(database_url=database_url, feature_version=args.feature_version)
    print(
        f"Feature build complete. version={result.feature_version} "
        f"built_rows={result.built_rows} upserted_rows={result.upserted_rows}"
    )


if __name__ == "__main__":
    main()
