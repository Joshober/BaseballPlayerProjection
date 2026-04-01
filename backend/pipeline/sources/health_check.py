"""Print status of configured data tiers (MLB API, Lahman, Chadwick, Retrosheet)."""
from __future__ import annotations

import os
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[3]
RAW = ROOT / "data" / "raw"


def _tier_mlb() -> str:
    try:
        r = httpx.get(
            "https://statsapi.mlb.com/api/v1/people/660670",
            timeout=10.0,
            headers={"User-Agent": "ScoutPro-health/1.0"},
        )
        return "PASS" if r.status_code == 200 else f"FAIL ({r.status_code})"
    except Exception as exc:
        return f"FAIL ({exc})"


def _file_exists(rel: str) -> str:
    p = RAW / rel
    return "PASS" if p.is_file() else "FAIL (missing file)"


def _tier_lahman() -> str:
    return _file_exists("lahman/Batting.csv")


def _tier_chadwick() -> str:
    return _file_exists("chadwick/people.csv")


def _tier_retrosheet() -> str:
    d = RAW / "retrosheet"
    if not d.is_dir():
        return "FAIL (no directory)"
    gl = list(d.glob("GL*.TXT")) + list(d.glob("GL*.txt"))
    return "PASS" if gl else "FAIL (no GL*.txt)"


def main() -> None:
    mlb = _tier_mlb()
    lahman = _tier_lahman()
    chadwick = _tier_chadwick()
    retro = _tier_retrosheet()
    print("Tier 1 — MLB Stats API:", mlb)
    print("Tier 2 — Retrosheet (optional):", retro)
    print("Tier 3 — Chadwick register:", chadwick)
    print("Tier 4 — Lahman CSV:", lahman)
    redis_url = os.getenv("REDIS_URL", "")
    print("Redis URL set:", "yes" if redis_url else "no")


if __name__ == "__main__":
    main()
