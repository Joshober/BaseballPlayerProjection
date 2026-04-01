"""FastAPI application entry — project root must be on PYTHONPATH (repo root)."""
from __future__ import annotations

from db.config import load_project_env

load_project_env()

import os
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse

from db.health import check_database
from free_apis import get_mlb_player, search_mlb_people
from ml.feature_router import router as feature_router
from ml.ingest_router import router as ingest_router

from backend.api.routes import comparisons, health as health_routes, players, predictions, reports, scrape, subscriptions, watchlist
from backend.api.services.scrape_integration import preview_response

_sentry_dsn = os.getenv("SENTRY_DSN", "").strip()
if _sentry_dsn:
    import sentry_sdk
    from sentry_sdk.integrations.fastapi import FastApiIntegration

    sentry_sdk.init(dsn=_sentry_dsn, integrations=[FastApiIntegration()], traces_sample_rate=0.1)

app = FastAPI(
    title="ScoutPro — Baseball Player Projection API",
    description="MiLB intelligence: scraping, features, predictions, and comparisons",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(feature_router, prefix="/api")
app.include_router(ingest_router, prefix="/api")
app.include_router(health_routes.router, prefix="/api")
app.include_router(predictions.router, prefix="/api")
app.include_router(comparisons.router, prefix="/api")
app.include_router(reports.router, prefix="/api")
app.include_router(subscriptions.router, prefix="/api")
app.include_router(watchlist.router, prefix="/api")
app.include_router(players.router, prefix="/api")
app.include_router(scrape.router, prefix="/api")


@app.get("/")
def home() -> PlainTextResponse:
    """SPA lives on the frontend service (e.g. port 5173); API root is for discovery."""
    return PlainTextResponse(
        "ScoutPro API — Open /docs. Web UI: http://localhost:5173 (or your deployed frontend URL).",
        status_code=200,
    )


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/db/health")
def db_health() -> dict:
    status = check_database()
    if status["ok"]:
        return {"status": "ok", "database": status["message"]}
    return JSONResponse(status_code=503, content={"status": "error", "database": status["message"]})


@app.get("/mlb/search")
def mlb_search(name: str = Query(..., min_length=2, description="Player name, e.g. Mike Trout")):
    try:
        results = search_mlb_people(name)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"MLB search failed: {exc}") from exc
    return {"query": name, "count": len(results), "results": results}


@app.get("/mlb/player/{player_id}")
def mlb_player(player_id: int):
    try:
        data = get_mlb_player(player_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"MLB player fetch failed: {exc}") from exc
    return JSONResponse(content=data)


@app.get("/scrape")
def scrape_player_legacy(
    url: str = Query(
        ...,
        description="Baseball-Reference player URL, e.g. https://www.baseball-reference.com/register/player.fcgi?id=...",
    ),
    delay: float = Query(2.0, ge=0.0, le=10.0),
    include_tables: bool = Query(True),
    table_limit: Optional[int] = Query(2000, ge=1, le=20000),
):
    """Legacy alias — same as GET /api/scrape/preview."""
    try:
        return preview_response(url, delay, include_tables, table_limit)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to scrape URL: {exc}") from exc
