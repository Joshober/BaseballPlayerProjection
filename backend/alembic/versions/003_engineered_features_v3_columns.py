"""engineered_features: v3 cutoff + cohort percentile columns.

Revision ID: 003_v3_cols
Revises: 002_scoutpro
Create Date: 2026-04-01

"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "003_v3_cols"
down_revision: Union[str, None] = "002_scoutpro"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS prediction_cutoff_season SMALLINT;
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS cutoff_policy VARCHAR(64);
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS ops_pctile_milb_weighted NUMERIC(8,5);
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS era_pctile_milb_weighted NUMERIC(8,5);
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS age_pctile_milb_weighted NUMERIC(8,5);
            """
        )
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            """
            ALTER TABLE engineered_features DROP COLUMN IF EXISTS age_pctile_milb_weighted;
            ALTER TABLE engineered_features DROP COLUMN IF EXISTS era_pctile_milb_weighted;
            ALTER TABLE engineered_features DROP COLUMN IF EXISTS ops_pctile_milb_weighted;
            ALTER TABLE engineered_features DROP COLUMN IF EXISTS cutoff_policy;
            ALTER TABLE engineered_features DROP COLUMN IF EXISTS prediction_cutoff_season;
            """
        )
    )
