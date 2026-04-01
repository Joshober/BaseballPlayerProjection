"""Temporal and subgroup splits for ScoutPro arrival model evaluation."""
from __future__ import annotations

import os

import pandas as pd
import psycopg

from db.config import load_project_env

load_project_env()


def fetch_first_milb_season_by_player(conn: psycopg.Connection) -> pd.Series:
    q = """
    SELECT player_id, MIN(season) AS first_season FROM (
        SELECT player_id, season FROM milb_batting
        UNION ALL
        SELECT player_id, season FROM milb_pitching
    ) u GROUP BY player_id
    """
    with conn.cursor() as cur:
        cur.execute(q)
        rows = cur.fetchall()
    if not rows:
        return pd.Series(dtype=int)
    return pd.Series({int(r[0]): int(r[1]) for r in rows})


def temporal_train_mask(
    df: pd.DataFrame,
    first_season: pd.Series,
    *,
    train_before_year: int = 2015,
) -> pd.Series:
    """Train on players whose first MiLB season in DB is strictly before train_before_year."""
    fs = df["player_id"].map(first_season)
    return fs.notna() & (fs < train_before_year)


def temporal_test_mask(
    df: pd.DataFrame,
    first_season: pd.Series,
    *,
    test_start_year: int = 2015,
    test_end_year: int = 2018,
) -> pd.Series:
    fs = df["player_id"].map(first_season)
    return fs.notna() & (fs >= test_start_year) & (fs <= test_end_year)


def peak_level_bucket(mask_high: pd.Series) -> str:
    return "high_minors" if bool(mask_high.iloc[0]) else "low_minors"


def add_first_milb_season_column(df: pd.DataFrame, database_url: str | None = None) -> pd.DataFrame:
    url = database_url or os.getenv("DATABASE_URL")
    if not url:
        return df
    with psycopg.connect(url) as conn:
        fs = fetch_first_milb_season_by_player(conn)
    out = df.copy()
    out["first_milb_season"] = out["player_id"].map(fs)
    return out
