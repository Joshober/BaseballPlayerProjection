"""HTTP helpers for BBRef scraping (delegates to ml.scrape_pipeline)."""
from __future__ import annotations

from typing import Optional

from fastapi.responses import JSONResponse

from ml.scrape_pipeline import preview_bbref_register


def preview_response(
    url: str,
    delay: float = 2.0,
    include_tables: bool = True,
    table_limit: Optional[int] = 2000,
) -> JSONResponse:
    payload = preview_bbref_register(url, delay, include_tables, table_limit)
    return JSONResponse(content=payload)
