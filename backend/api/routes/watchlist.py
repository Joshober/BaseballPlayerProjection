"""User watchlist (MLBAM ids) backed by Postgres."""
from __future__ import annotations

import os

from fastapi import APIRouter, Depends, HTTPException

from backend.api.deps import require_auth

router = APIRouter(tags=["watchlist"], prefix="/watchlist")


@router.get("")
def list_watchlist(user: dict = Depends(require_auth)):
    database_url = os.getenv("DATABASE_URL")
    uid = user.get("user_id")
    if not database_url or not uid:
        return {"items": []}
    import psycopg

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT mlbam_id, created_at FROM watchlist
                WHERE clerk_user_id = %s
                ORDER BY created_at DESC
                """,
                (uid,),
            )
            rows = cur.fetchall()
    return {"items": [{"mlbam_id": r[0], "created_at": str(r[1])} for r in rows]}


@router.post("/{mlbam_id}")
def add_watchlist(mlbam_id: int, user: dict = Depends(require_auth)):
    database_url = os.getenv("DATABASE_URL")
    uid = user.get("user_id")
    if not database_url or not uid:
        raise HTTPException(status_code=503, detail="DATABASE_URL or user missing")
    import psycopg

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO watchlist (clerk_user_id, mlbam_id)
                VALUES (%s, %s)
                ON CONFLICT (clerk_user_id, mlbam_id) DO NOTHING
                """,
                (uid, mlbam_id),
            )
        conn.commit()
    return {"status": "ok", "mlbam_id": mlbam_id}


@router.delete("/{mlbam_id}")
def remove_watchlist(mlbam_id: int, user: dict = Depends(require_auth)):
    database_url = os.getenv("DATABASE_URL")
    uid = user.get("user_id")
    if not database_url or not uid:
        raise HTTPException(status_code=503, detail="DATABASE_URL or user missing")
    import psycopg

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM watchlist WHERE clerk_user_id = %s AND mlbam_id = %s",
                (uid, mlbam_id),
            )
        conn.commit()
    return {"status": "ok", "mlbam_id": mlbam_id}
