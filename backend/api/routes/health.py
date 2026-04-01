"""Aggregated health: DB, Redis, model artifacts."""
from __future__ import annotations

import os

from fastapi import APIRouter

from db.health import check_database

router = APIRouter(tags=["health"])


def _redis_ping() -> bool:
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    try:
        import redis

        r = redis.from_url(url, socket_connect_timeout=2)
        return bool(r.ping())
    except Exception:
        return False


def _models_ok() -> bool:
    from backend.api.services.inference_service import models_loaded

    return models_loaded()


@router.get("/health/detail")
def health_detail() -> dict:
    db_ok = check_database()["ok"]
    cache_ok = _redis_ping()
    models_ok = _models_ok()
    all_ok = db_ok and cache_ok
    return {
        "status": "ok" if all_ok else "degraded",
        "db": db_ok,
        "cache": cache_ok,
        "models": models_ok,
    }
