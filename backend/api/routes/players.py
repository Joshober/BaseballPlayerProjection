"""Player-facing read APIs (MiLB rows from Postgres)."""
from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from backend.api.deps import require_auth

router = APIRouter(tags=["players"], prefix="/players")


def _rows_to_dicts(cur) -> list[dict[str, Any]]:
    cols = [d.name for d in cur.description] if cur.description else []
    out = []
    for row in cur.fetchall() or []:
        out.append(dict(zip(cols, row)))
    return out


@router.get("/{mlbam_id}/milb-stats")
def get_milb_stats(mlbam_id: int, _user: dict = Depends(require_auth)):
    """Latest MiLB season rows from the warehouse for this MLBAM id (empty if not ingested)."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise HTTPException(status_code=503, detail="DATABASE_URL not configured")
    import psycopg

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.id AS player_pk FROM players p WHERE p.mlb_id = %s LIMIT 1
                """,
                (mlbam_id,),
            )
            prow = cur.fetchone()
            if not prow:
                return {
                    "mlbam_id": mlbam_id,
                    "ingested": False,
                    "batting": [],
                    "pitching": [],
                }
            player_pk = prow[0]

            cur.execute(
                """
                SELECT season, level, level_order, team_abbr, league, age, g, pa, ab, r, h,
                       doubles, triples, hr, rbi, sb, cs, bb, so, ba, obp, slg, ops,
                       iso, bb_pct, k_pct
                FROM milb_batting
                WHERE player_id = %s
                ORDER BY season DESC, level_order DESC, level DESC
                LIMIT 80
                """,
                (player_pk,),
            )
            batting = _rows_to_dicts(cur)

            cur.execute(
                """
                SELECT season, level, level_order, team_abbr, league, age, g, gs, w, l, sv,
                       ip, h, r, er, hr, bb, so, era, whip, h9, hr9, bb9, so9, so_bb, fip
                FROM milb_pitching
                WHERE player_id = %s
                ORDER BY season DESC, level_order DESC, level DESC
                LIMIT 80
                """,
                (player_pk,),
            )
            pitching = _rows_to_dicts(cur)

    return {
        "mlbam_id": mlbam_id,
        "ingested": True,
        "batting": batting,
        "pitching": pitching,
    }
