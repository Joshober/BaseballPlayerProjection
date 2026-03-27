from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from db.config import load_project_env
from db.health import check_database
from free_apis import get_mlb_player, search_mlb_people
from milb_scraper import MiLBScraper
from ml.feature_router import router as feature_router
from ml.ingest_router import router as ingest_router

load_project_env()

app = FastAPI(
    title="Baseball Player Projection API",
    description="MiLB Baseball-Reference scraping API",
    version="1.0.0",
)
app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(feature_router, prefix="/api")
app.include_router(ingest_router, prefix="/api")


@app.get("/")
def home() -> FileResponse:
    return FileResponse("static/index.html")


def _df_to_records_json_safe(df, limit: Optional[int] = None):
    out_df = df.head(limit) if limit is not None else df
    # Convert DataFrame to object dtype first, then replace NaN/NaT with None.
    # This prevents JSON serialization failures in strict JSON mode.
    out_df = out_df.astype(object).where(out_df.notna(), None)
    return out_df.to_dict(orient="records")


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
def scrape_player(
    url: str = Query(
        ...,
        description="Baseball-Reference player URL, e.g. https://www.baseball-reference.com/register/player.fcgi?id=...",
    ),
    delay: float = Query(2.0, ge=0.0, le=10.0),
    include_tables: bool = Query(True),
    table_limit: Optional[int] = Query(2000, ge=1, le=20000),
):
    try:
        scraper = MiLBScraper(delay=delay)
        data = scraper.scrape_player(url)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to scrape URL: {exc}") from exc

    payload = {
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
                payload["batting"] = _df_to_records_json_safe(batting, table_limit)
                payload["batting_truncated"] = True
            else:
                payload["batting"] = _df_to_records_json_safe(batting)
        else:
            payload["batting"] = []

        if pitching is not None:
            if table_limit is not None and len(pitching) > table_limit:
                payload["pitching"] = _df_to_records_json_safe(pitching, table_limit)
                payload["pitching_truncated"] = True
            else:
                payload["pitching"] = _df_to_records_json_safe(pitching)
        else:
            payload["pitching"] = []

    return JSONResponse(content=payload)
