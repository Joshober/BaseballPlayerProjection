"""Apply existing schema.sql (baseline).

Revision ID: 001_baseline
Revises:
Create Date: 2026-03-27

"""
from __future__ import annotations

from pathlib import Path
from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

revision: str = "001_baseline"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    root = Path(__file__).resolve().parents[3]
    sql_path = root / "schema.sql"
    if not sql_path.is_file():
        raise FileNotFoundError(f"schema.sql not found at {sql_path}")
    sql = sql_path.read_text(encoding="utf-8")
    connection = op.get_bind()
    connection.execute(text(sql))


def downgrade() -> None:
    pass
