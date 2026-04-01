"""Build training labels from Lahman + Chadwick when CSVs are present (offline)."""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"


def main() -> None:
    p = argparse.ArgumentParser(description="Build reached_mlb / years_to_debut CSV")
    p.add_argument("--out", default=str(ROOT / "data" / "processed" / "labels.csv"))
    args = p.parse_args()
    lahman_people = RAW / "lahman" / "People.csv"
    if not lahman_people.is_file():
        print(f"Missing {lahman_people}; download Lahman CSVs into data/raw/lahman/")
        return
    people = pd.read_csv(lahman_people, low_memory=False)
    # Placeholder: use debut column if present
    cols = {c.lower(): c for c in people.columns}
    debut_col = cols.get("debut")
    if debut_col:
        people["_debut"] = pd.to_datetime(people[debut_col], errors="coerce")
        people["reached_mlb"] = people["_debut"].notna()
    else:
        people["reached_mlb"] = False
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    keep = ["playerID"] if "playerID" in people.columns else people.columns[:3]
    people[list(keep) + [c for c in people.columns if c in ("reached_mlb",)]].to_csv(out, index=False)
    print("Wrote", out)


if __name__ == "__main__":
    main()
