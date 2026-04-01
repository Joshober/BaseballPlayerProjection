"""ScoutPro: game logs, GDS, subscriptions, watchlist, comparisons.

Revision ID: 002_scoutpro
Revises: 001_baseline
Create Date: 2026-03-27

"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "002_scoutpro"
down_revision: Union[str, None] = "001_baseline"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS game_logs (
                id SERIAL PRIMARY KEY,
                player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
                mlbam_id INTEGER,
                game_date DATE,
                game_pk BIGINT,
                level VARCHAR(10),
                stat_line JSONB,
                gds_score NUMERIC(6,2),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_game_logs_player ON game_logs(player_id);
            CREATE INDEX IF NOT EXISTS idx_game_logs_mlbam ON game_logs(mlbam_id);
            CREATE INDEX IF NOT EXISTS idx_game_logs_date ON game_logs(game_date);

            CREATE TABLE IF NOT EXISTS opponent_scores (
                id SERIAL PRIMARY KEY,
                game_log_id INTEGER NOT NULL REFERENCES game_logs(id) ON DELETE CASCADE,
                opponent_mlbam_id INTEGER,
                opponent_role VARCHAR(10),
                gds_component NUMERIC(6,2),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS subscriptions (
                id SERIAL PRIMARY KEY,
                clerk_user_id VARCHAR(128) UNIQUE NOT NULL,
                tier VARCHAR(32) NOT NULL DEFAULT 'starter',
                reports_used_this_month INTEGER NOT NULL DEFAULT 0,
                reports_limit INTEGER NOT NULL DEFAULT 10,
                stripe_customer_id VARCHAR(128),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS watchlist (
                id SERIAL PRIMARY KEY,
                clerk_user_id VARCHAR(128) NOT NULL,
                mlbam_id INTEGER NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (clerk_user_id, mlbam_id)
            );

            CREATE TABLE IF NOT EXISTS comparisons (
                id SERIAL PRIMARY KEY,
                player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
                comp_json JSONB NOT NULL,
                computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_comparisons_player ON comparisons(player_id);
            """
        )
    )


def downgrade() -> None:
    op.execute(sa.text("DROP TABLE IF EXISTS opponent_scores CASCADE;"))
    op.execute(sa.text("DROP TABLE IF EXISTS game_logs CASCADE;"))
    op.execute(sa.text("DROP TABLE IF EXISTS watchlist CASCADE;"))
    op.execute(sa.text("DROP TABLE IF EXISTS subscriptions CASCADE;"))
    op.execute(sa.text("DROP TABLE IF EXISTS comparisons CASCADE;"))
