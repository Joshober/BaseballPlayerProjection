"""Orchestrate ML data prep: status report → label backfill → feature build → optional training."""
from __future__ import annotations

import argparse
import os
import subprocess
import sys

from db.config import load_project_env

load_project_env()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare database rows for ML (labels, engineered_features, optional train_all)"
    )
    parser.add_argument("--skip-status", action="store_true", help="Skip data_status report")
    parser.add_argument("--skip-labels", action="store_true", help="Skip MLB API label backfill")
    parser.add_argument("--label-limit", type=int, default=None, help="Pass --limit to backfill_player_labels")
    parser.add_argument("--skip-features", action="store_true", help="Skip engineered_features build")
    parser.add_argument("--feature-version", default="v3")
    parser.add_argument("--train", action="store_true", help="Run ml.train_all after feature build")
    args = parser.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    py = sys.executable

    if not args.skip_status:
        print("=== Data status ===\n")
        subprocess.run([py, "-m", "ml.data_status"], cwd=root, check=False)
        print()

    if not args.skip_labels:
        print("=== Backfill labels (MLB Stats API) ===\n")
        cmd = [py, "-m", "ml.backfill_player_labels"]
        if args.label_limit is not None:
            cmd.extend(["--limit", str(args.label_limit)])
        subprocess.run(cmd, cwd=root, check=False)
        print()

    if not args.skip_features:
        print("=== Build engineered features ===\n")
        subprocess.run(
            [py, "-m", "ml.build_features", "--feature-version", args.feature_version],
            cwd=root,
            check=False,
        )
        print()

    if args.train:
        print("=== Train models (train_all) ===\n")
        subprocess.run([py, "-m", "ml.train_all", "--feature-version", args.feature_version], cwd=root, check=False)

    print("Done. See ml/train_all.py for minimum row counts; ingest more BBRef register pages to grow the dataset.")


if __name__ == "__main__":
    main()
