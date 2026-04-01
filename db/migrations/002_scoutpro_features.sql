-- ScoutPro plan: players draft/eligibility + engineered_features v2 columns
-- Apply to existing DBs: psql $DATABASE_URL -f db/migrations/002_scoutpro_features.sql

ALTER TABLE players ADD COLUMN IF NOT EXISTS draft_round SMALLINT;
ALTER TABLE players ADD COLUMN IF NOT EXISTS draft_year SMALLINT;
ALTER TABLE players ADD COLUMN IF NOT EXISTS is_international BOOLEAN;
ALTER TABLE players ADD COLUMN IF NOT EXISTS signing_bonus_usd BIGINT;
ALTER TABLE players ADD COLUMN IF NOT EXISTS eligible_for_training BOOLEAN;
ALTER TABLE players ADD COLUMN IF NOT EXISTS years_to_mlb_fractional NUMERIC(4,2);

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
