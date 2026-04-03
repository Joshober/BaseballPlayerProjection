"""Align players table with schema.sql (older DBs may lack columns).

Revision ID: 004_players_align
Revises: 003_v3_cols
Create Date: 2026-04-02

"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "004_players_align"
down_revision: Union[str, None] = "003_v3_cols"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            ALTER TABLE players ADD COLUMN IF NOT EXISTS draft_round SMALLINT;
            ALTER TABLE players ADD COLUMN IF NOT EXISTS draft_year SMALLINT;
            ALTER TABLE players ADD COLUMN IF NOT EXISTS is_international BOOLEAN;
            ALTER TABLE players ADD COLUMN IF NOT EXISTS signing_bonus_usd BIGINT;
            ALTER TABLE players ADD COLUMN IF NOT EXISTS eligible_for_training BOOLEAN;
            ALTER TABLE players ADD COLUMN IF NOT EXISTS years_to_mlb_fractional NUMERIC(4,2);
            """
        )
    )


def downgrade() -> None:
    pass
