"""Opponent quality (GDS) — scale ~50 average, higher = tougher competition."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class OpponentProfile:
    mlbam_id: int
    trailing_woba: float | None
    trailing_era: float | None
    level_score: float


def opponent_strength_score(profile: OpponentProfile) -> float:
    """Map opponent profile to 0–100-ish difficulty; blended into GDS."""
    base = 50.0
    if profile.trailing_woba is not None:
        base += (profile.trailing_woba - 0.320) * 120.0
    if profile.trailing_era is not None:
        base += (4.20 - profile.trailing_era) * 8.0
    base += profile.level_score
    return max(30.0, min(95.0, base))


def compute_gds_for_game(
    opponent_profiles: list[OpponentProfile],
    performance_index: float,
) -> float:
    """Combine opponent difficulty with raw performance to produce GDS (50 = avg)."""
    if not opponent_profiles:
        return 50.0 + (performance_index - 0.5) * 20.0
    strengths = [opponent_strength_score(p) for p in opponent_profiles]
    avg_opp = sum(strengths) / len(strengths)
    # Performance vs expectation given opponent strength
    adj = performance_index * 30.0 + (avg_opp - 50.0) * 0.4
    return max(35.0, min(90.0, 50.0 + adj))


def game_row_to_performance_index(row: dict[str, Any]) -> float:
    """Derive a 0–1 index from a game log split row."""
    stat = row.get("stat") or {}
    ab = int(stat.get("atBats") or 0)
    h = int(stat.get("hits") or 0)
    if ab <= 0:
        return 0.45
    return min(1.0, max(0.0, h / max(ab, 1)))
