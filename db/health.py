from __future__ import annotations

import os

import psycopg

from db.config import load_project_env


def check_database() -> dict:
    load_project_env()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        return {"ok": False, "message": "DATABASE_URL is not set"}

    try:
        with psycopg.connect(db_url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return {"ok": True, "message": "Database reachable"}
    except Exception as exc:
        return {"ok": False, "message": f"Database unreachable: {exc}"}
