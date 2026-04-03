import os

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport

os.environ.setdefault("SCOUTPRO_DEV_AUTH", "1")

from backend.main import app


@pytest_asyncio.fixture()
async def client():
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_health(client: httpx.AsyncClient):
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json().get("status") == "ok"


@pytest.mark.asyncio
async def test_openapi(client: httpx.AsyncClient):
    r = await client.get("/openapi.json")
    assert r.status_code == 200
    assert "openapi" in r.json()


@pytest.mark.asyncio
async def test_health_detail(client: httpx.AsyncClient):
    r = await client.get("/api/health/detail")
    assert r.status_code == 200
    body = r.json()
    assert "db" in body and "cache" in body and "models" in body


@pytest.mark.asyncio
async def test_milb_stats_requires_db(client: httpx.AsyncClient):
    """MiLB stats endpoint shape; skips when DATABASE_URL unset (CI without Postgres)."""
    if not os.getenv("DATABASE_URL"):
        pytest.skip("DATABASE_URL not set")
    r = await client.get("/api/players/660670/milb-stats")
    if r.status_code == 503:
        pytest.skip("Database not reachable (start Postgres, e.g. docker compose up -d postgres)")
    assert r.status_code == 200
    body = r.json()
    assert body.get("mlbam_id") == 660670
    assert "ingested" in body
    assert isinstance(body.get("batting"), list)
    assert isinstance(body.get("pitching"), list)
