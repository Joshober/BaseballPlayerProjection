"""Baseball-Reference register scraping integrated with DB ingestion and feature builds."""
from __future__ import annotations

import os
from typing import Any, Optional

from milb_scraper import MiLBScraper

from ml.feature_engineering import build_and_upsert_features
from ml.ingestion import ingest_from_url


def df_to_records_json_safe(df, limit: Optional[int] = None) -> list[dict[str, Any]]:
    out_df = df.head(limit) if limit is not None else df
    out_df = out_df.astype(object).where(out_df.notna(), None)
    return out_df.to_dict(orient="records")


def preview_bbref_register(
    url: str,
    delay: float = 2.0,
    include_tables: bool = True,
    table_limit: Optional[int] = 2000,
) -> dict[str, Any]:
    """Scrape a Baseball-Reference register player URL; return metadata and optional stat tables."""
    scraper = MiLBScraper(delay=delay)
    data = scraper.scrape_player(url)

    payload: dict[str, Any] = {
        "metadata": data.get("metadata") or {},
        "batting_rows": 0,
        "pitching_rows": 0,
    }

    batting = data.get("batting")
    pitching = data.get("pitching")

    if batting is not None:
        payload["batting_rows"] = int(len(batting))
    if pitching is not None:
        payload["pitching_rows"] = int(len(pitching))

    if include_tables:
        if batting is not None:
            if table_limit is not None and len(batting) > table_limit:
                payload["batting"] = df_to_records_json_safe(batting, table_limit)
                payload["batting_truncated"] = True
            else:
                payload["batting"] = df_to_records_json_safe(batting)
        else:
            payload["batting"] = []

        if pitching is not None:
            if table_limit is not None and len(pitching) > table_limit:
                payload["pitching"] = df_to_records_json_safe(pitching, table_limit)
                payload["pitching_truncated"] = True
            else:
                payload["pitching"] = df_to_records_json_safe(pitching)
        else:
            payload["pitching"] = []

    return payload


def ingest_bbref_register(
    url: str,
    delay: float = 2.0,
    mlb_id: int | None = None,
    build_features: bool = False,
    feature_version: str = "v2",
) -> dict[str, Any]:
    """Scrape + upsert into Postgres; optionally run feature engineering for all players."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    result = ingest_from_url(database_url=database_url, url=url, delay=delay, mlb_id=mlb_id)
    out: dict[str, Any] = {
        "status": "ok",
        "bbref_id": result.bbref_id,
        "player_id": result.player_id,
        "batting_rows_upserted": result.batting_rows,
        "pitching_rows_upserted": result.pitching_rows,
    }

    if build_features:
        fb = build_and_upsert_features(database_url=database_url, feature_version=feature_version)
        out["feature_build"] = {
            "feature_version": fb.feature_version,
            "built_rows": fb.built_rows,
            "upserted_rows": fb.upserted_rows,
        }

    return out
