"""Player comparison endpoints."""
from __future__ import annotations

import json
import os

from fastapi import APIRouter, Depends, HTTPException

from backend.api.deps import require_auth

router = APIRouter(tags=["comparisons"], prefix="/comparisons")


@router.get("/{mlbam_id}")
def get_comparisons(mlbam_id: int, _user: dict = Depends(require_auth)):
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise HTTPException(status_code=503, detail="DATABASE_URL not configured")
    import psycopg

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.* FROM comparisons c
                JOIN players pl ON pl.id = c.player_id
                WHERE pl.mlb_id = %s
                ORDER BY c.computed_at DESC NULLS LAST
                LIMIT 1
                """,
                (mlbam_id,),
            )
            row = cur.fetchone()
            cols = [d.name for d in cur.description] if cur.description else []
    if not row:
        return {"mlbam_id": mlbam_id, "comps": [], "note": "No comparison row; check predictions.similar_player_ids or run comparison engine"}
    rec = dict(zip(cols, row))
    cj = rec.get("comp_json")
    if isinstance(cj, str):
        try:
            cj = json.loads(cj)
        except json.JSONDecodeError:
            cj = {}
    if not isinstance(cj, dict):
        cj = {}
    comps_list = cj.get("comps") or []
    return {"mlbam_id": mlbam_id, "row": rec, "comps": comps_list}
