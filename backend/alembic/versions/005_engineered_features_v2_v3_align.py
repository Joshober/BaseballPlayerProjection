"""Add engineered_features v2/v3 columns if missing (older DBs).

Revision ID: 005_ef_align
Revises: 004_players_align
Create Date: 2026-04-02

"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "005_ef_align"
down_revision: Union[str, None] = "004_players_align"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS first_milb_season SMALLINT;
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS career_age_vs_level_avg NUMERIC(6,3);
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS ever_repeated_level BOOLEAN;
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS promotion_speed_score NUMERIC(8,4);
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS ops_yoy_delta NUMERIC(7,4);
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS k_minus_bb_yoy_delta NUMERIC(7,4);
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS is_improving BOOLEAN;
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS low_sample_season_flag BOOLEAN;
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS draft_round_feat SMALLINT;
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS is_international_feat BOOLEAN;
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS signing_bonus_usd_feat BIGINT;
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS label_eligible_for_training BOOLEAN;
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS prediction_cutoff_season SMALLINT;
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS cutoff_policy VARCHAR(64);
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS ops_pctile_milb_weighted NUMERIC(8,5);
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS era_pctile_milb_weighted NUMERIC(8,5);
            ALTER TABLE engineered_features ADD COLUMN IF NOT EXISTS age_pctile_milb_weighted NUMERIC(8,5);
            """
        )
    )


def downgrade() -> None:
    pass
