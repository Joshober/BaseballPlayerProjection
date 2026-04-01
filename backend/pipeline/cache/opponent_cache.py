"""Trailing-window opponent profiles in Redis."""
from __future__ import annotations

import json
import os
from typing import Any

from backend.pipeline.gds.opponent_quality import OpponentProfile


def _client():
    import redis

    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return redis.from_url(url, decode_responses=True)


def cache_opponent_profile(mlbam_id: int, profile: OpponentProfile, ttl_seconds: int = 86400) -> None:
    key = f"opp:{mlbam_id}"
    payload = {
        "mlbam_id": profile.mlbam_id,
        "trailing_woba": profile.trailing_woba,
        "trailing_era": profile.trailing_era,
        "level_score": profile.level_score,
    }
    r = _client()
    r.setex(key, ttl_seconds, json.dumps(payload))


def get_cached_profile(mlbam_id: int) -> dict[str, Any] | None:
    r = _client()
    raw = r.get(f"opp:{mlbam_id}")
    if not raw:
        return None
    return json.loads(raw)
