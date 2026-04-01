"""Optional Lahman Appearances cross-check for reached_mlb (requires manual ID mapping).

Set LAHMAN_APPEARANCES_CSV to a CSV with columns: mlb_id, has_mlb_games (1/0 or true/false).
The Lahman public Appearances table uses lahman_id, not MLBAM ids — use Chadwick or a custom
mapping export to build this file. If unset or file missing, helpers return empty dict.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def load_mlb_id_flags_from_csv() -> dict[int, bool]:
    path = os.getenv("LAHMAN_MLB_FLAGS_CSV")
    if not path:
        return {}
    p = Path(path)
    if not p.is_file():
        return {}
    try:
        import pandas as pd

        df = pd.read_csv(p)
    except Exception:
        return {}
    if "mlb_id" not in df.columns:
        return {}
    col = "has_mlb_games" if "has_mlb_games" in df.columns else df.columns[-1]
    out: dict[int, bool] = {}
    for _, row in df.iterrows():
        try:
            mid = int(row["mlb_id"])
            v = row[col]
            out[mid] = bool(int(v)) if str(v).isdigit() else str(v).lower() in ("1", "true", "yes", "y")
        except (TypeError, ValueError, KeyError):
            continue
    return out
