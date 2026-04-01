"""Baseball-Reference web scraping integrated under /api/scrape."""
from __future__ import annotations

import threading
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field

from backend.api.services.scrape_integration import preview_response
from milb_scraper import MiLBScraper
from ml.scrape_pipeline import ingest_bbref_register

router = APIRouter(tags=["scraping"], prefix="/scrape")

_batch_lock = threading.Lock()
_batch_state: dict[str, Any] = {
    "running": False,
    "log": [],
    "stats": None,
    "error": None,
}


class BatchStartBody(BaseModel):
    target_new_ingests: int = Field(30, ge=1, le=2000, description="Stop after this many successful new ingests")
    target_milb_rows: int = Field(
        0,
        ge=0,
        description="Optional stop when milb_batting + milb_pitching rows reach this (0 = ignore)",
    )
    max_per_query: int = Field(8, ge=1, le=50)
    delay_seconds: float = Field(3.0, ge=1.0, le=120.0)
    bbref_delay: float = Field(1.5, ge=0.5, le=30.0)
    build_features: bool = False
    feature_version: str = "v2"
    extra_queries: list[str] = Field(default_factory=list)
    queries_file: str | None = None


def _append_batch_log(line: str) -> None:
    with _batch_lock:
        _batch_state["log"].append(line)
        if len(_batch_state["log"]) > 600:
            _batch_state["log"] = _batch_state["log"][-500:]


def _run_batch_job(body: BatchStartBody) -> None:
    try:
        from ml.batch_ingest_discovery import run_batch_ingest

        stats = run_batch_ingest(
            target_new_ingests=body.target_new_ingests,
            target_milb_rows=body.target_milb_rows,
            max_per_query=body.max_per_query,
            delay_seconds=body.delay_seconds,
            bbref_delay=body.bbref_delay,
            build_features=body.build_features,
            feature_version=body.feature_version,
            extra_queries=body.extra_queries or None,
            queries_file=body.queries_file,
            log=_append_batch_log,
        )
        with _batch_lock:
            _batch_state["stats"] = stats
    except Exception as exc:
        with _batch_lock:
            _batch_state["error"] = str(exc)
    finally:
        with _batch_lock:
            _batch_state["running"] = False

_DB_HELP = (
    "Check DATABASE_URL in your .env: user, password, host, port, and database name must match your "
    "Postgres server. Create the role/database or switch to an existing user (e.g. postgres)."
)


def _http_for_db_error(exc: BaseException) -> HTTPException | None:
    """Turn psycopg / connection failures into a clear 503 instead of a generic 500."""
    msg = str(exc).lower()
    if "password authentication failed" in msg or "authentication failed" in msg:
        return HTTPException(
            status_code=503,
            detail=f"PostgreSQL rejected the username or password in DATABASE_URL. {_DB_HELP}",
        )
    if "connection refused" in msg or "could not connect" in msg:
        return HTTPException(
            status_code=503,
            detail=f"Could not reach PostgreSQL (is it running?). {_DB_HELP}",
        )
    if "does not exist" in msg and "database" in msg:
        return HTTPException(
            status_code=503,
            detail=f"Database in DATABASE_URL does not exist. Create it or fix the name in .env. {_DB_HELP}",
        )
    return None


@router.post("/batch-start")
def batch_start(body: BatchStartBody, background_tasks: BackgroundTasks) -> dict[str, str]:
    """Start MLB search → BBRef → ingest loop until targets are met (runs in background)."""
    with _batch_lock:
        if _batch_state["running"]:
            raise HTTPException(status_code=409, detail="A batch ingest job is already running.")
        _batch_state["running"] = True
        _batch_state["log"] = []
        _batch_state["stats"] = None
        _batch_state["error"] = None
    background_tasks.add_task(_run_batch_job, body)
    return {"status": "started", "message": "Polling GET /api/scrape/batch-status for progress."}


@router.get("/batch-status")
def batch_status() -> dict[str, Any]:
    """Log lines and completion stats for the last or current batch ingest."""
    with _batch_lock:
        return {
            "running": _batch_state["running"],
            "log": list(_batch_state["log"]),
            "stats": _batch_state["stats"],
            "error": _batch_state["error"],
        }


@router.get("/register-search")
def register_search(
    name: str = Query(..., min_length=2, description="Player name to find on BBRef MiLB register"),
    delay: float = Query(1.5, ge=0.5, le=10.0),
):
    """Search baseball-reference.com for MiLB register/player.fcgi links (no manual URL paste required)."""
    try:
        scraper = MiLBScraper(delay=delay)
        candidates = scraper.search_bbref_register_pages(name)
        return {"count": len(candidates), "candidates": candidates}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"BBRef register search failed: {exc}") from exc


@router.get("/preview")
def scrape_preview(
    url: str = Query(
        ...,
        description="Baseball-Reference register URL, e.g. .../register/player.fcgi?id=...",
    ),
    delay: float = Query(2.0, ge=0.0, le=10.0),
    include_tables: bool = Query(True),
    table_limit: Optional[int] = Query(2000, ge=1, le=20000),
):
    """Return scraped metadata and stat tables (same behavior as legacy GET /scrape)."""
    try:
        return preview_response(url, delay, include_tables, table_limit)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to scrape URL: {exc}") from exc


@router.post("/ingest")
def scrape_ingest(
    url: str = Query(..., description="Baseball-Reference register player URL"),
    delay: float = Query(2.0, ge=0.0, le=10.0),
    mlb_id: int | None = Query(None, description="Optional MLB Stats API player id"),
    build_features: bool = Query(False, description="Run POST /api/features/build after ingest"),
    feature_version: str = Query("v2"),
):
    """Scrape and upsert into Postgres; optionally rebuild engineered_features."""
    try:
        return ingest_bbref_register(
            url=url,
            delay=delay,
            mlb_id=mlb_id,
            build_features=build_features,
            feature_version=feature_version,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        mapped = _http_for_db_error(exc)
        if mapped:
            raise mapped from exc
        raise HTTPException(status_code=500, detail=f"Ingest failed: {exc}") from exc
