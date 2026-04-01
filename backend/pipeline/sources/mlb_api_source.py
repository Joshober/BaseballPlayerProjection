"""MLB Stats API client with asyncio rate limiting and 429 backoff."""
from __future__ import annotations

import asyncio
import os
import random
from typing import Any

import httpx

BASE = "https://statsapi.mlb.com/api/v1"


class MLBApiSource:
    """Fetches player stats and game logs from the free MLB Stats API."""

    def __init__(self, max_concurrent: int = 5) -> None:
        self._sem = asyncio.Semaphore(max_concurrent)
        self._client: httpx.AsyncClient | None = None

    async def _client_get(self) -> httpx.AsyncClient:
        if self._client is None:
            timeout = httpx.Timeout(60.0)
            self._client = httpx.AsyncClient(timeout=timeout, headers={"User-Agent": "ScoutPro/2.0"})
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        async with self._sem:
            client = await self._client_get()
            url = path if path.startswith("http") else f"{BASE}{path}"
            delay = 0.5
            for attempt in range(6):
                try:
                    r = await client.get(url, params=params or {})
                    if r.status_code == 429:
                        await asyncio.sleep(delay + random.random() * 0.2)
                        delay = min(delay * 2, 8.0)
                        continue
                    r.raise_for_status()
                    return r.json()
                except httpx.HTTPError:
                    if attempt == 5:
                        raise
                    await asyncio.sleep(delay + random.random() * 0.2)
                    delay = min(delay * 2, 8.0)
            raise RuntimeError("unreachable")

    async def fetch_player(self, player_id: int) -> dict[str, Any]:
        return await self.get_json(f"/people/{player_id}", params={"hydrate": "stats(group=[hitting,pitching])"})

    async def fetch_game_log_sample(self, player_id: int, season: int = 2024, limit: int = 5) -> list[dict[str, Any]]:
        """Return a small list of hitting game log rows if available."""
        data = await self.get_json(
            f"/people/{player_id}/stats",
            params={"stats": "gameLog", "group": "hitting", "season": str(season), "limit": str(limit)},
        )
        stats = data.get("stats") or []
        if not stats:
            return []
        splits = stats[0].get("splits") or []
        return splits[:limit]


async def demo_player(player_id: int = 660670) -> None:
    src = MLBApiSource()
    try:
        p = await src.fetch_player(player_id)
        print("people:", p.get("people", [{}])[0].get("fullName"))
        gl = await src.fetch_game_log_sample(player_id)
        print("game_log_rows:", len(gl))
        if gl:
            print("sample keys:", list(gl[0].keys())[:12])
    finally:
        await src.close()


if __name__ == "__main__":
    asyncio.run(demo_player(int(os.getenv("MLB_PLAYER_ID", "660670"))))
