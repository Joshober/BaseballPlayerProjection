from __future__ import annotations

import argparse
import os
from pathlib import Path

import psycopg

from db.config import load_project_env

load_project_env()


def init_db(schema_path: str = "schema.sql", database_url: str | None = None) -> None:
    db_url = database_url or os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set. Pass --database-url or set env var.")

    p = Path(schema_path)
    if not p.exists():
        raise FileNotFoundError(f"Schema file not found: {p}")

    sql = p.read_text(encoding="utf-8")
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize PostgreSQL schema for Baseball Project")
    parser.add_argument("--schema", default="schema.sql", help="Path to schema.sql (default: schema.sql)")
    parser.add_argument("--database-url", default=None, help="Override DATABASE_URL env var")
    args = parser.parse_args()

    init_db(schema_path=args.schema, database_url=args.database_url)
    print("Database schema initialized successfully.")


if __name__ == "__main__":
    main()
