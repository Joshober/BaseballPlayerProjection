-- =============================================================================
-- MiLB Analytics Platform — Database Schema
-- Compatible with PostgreSQL 14+
-- =============================================================================

-- -----------------------------------------------------------------------------
-- PLAYERS
-- Core identity table. One row per player, regardless of level.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS players (
    id                  SERIAL PRIMARY KEY,
    bbref_id            VARCHAR(20)  UNIQUE NOT NULL,   -- Baseball-Reference slug e.g. "troutmi01"
    mlb_id              INTEGER      UNIQUE,             -- MLB Stats API person ID (NULL if never reached MLB)
    full_name           VARCHAR(100) NOT NULL,
    first_name          VARCHAR(50),
    last_name           VARCHAR(50),
    birth_date          DATE,
    birth_country       VARCHAR(60),
    position            VARCHAR(5),                     -- P, C, 1B, 2B, 3B, SS, LF, CF, RF, DH, OF
    bats                CHAR(1),                        -- L, R, S
    throws              CHAR(1),                        -- L, R
    height_in           SMALLINT,                       -- inches
    weight_lb           SMALLINT,
    pro_debut_date      DATE,                           -- first professional game (any level)
    milb_debut_date     DATE,
    mlb_debut_date      DATE,                           -- NULL if never reached MLB
    reached_mlb         BOOLEAN      NOT NULL DEFAULT FALSE,
    years_to_mlb        NUMERIC(4,2),                   -- debut year minus first MiLB season year (see backfill); NULL if never reached
    is_active           BOOLEAN      NOT NULL DEFAULT TRUE,
    draft_round         SMALLINT,                        -- MLB draft round; NULL = undrafted / int'l
    draft_year          SMALLINT,
    is_international    BOOLEAN,                         -- true if signed as international amateur
    signing_bonus_usd   BIGINT,                          -- from MLB bio when available
    eligible_for_training BOOLEAN,                       -- false = active under 28 with no MLB debut (unresolved)
    years_to_mlb_fractional NUMERIC(4,2),              -- fractional years MiLB→MLB (parallel to integer years_to_mlb)
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_players_bbref      ON players(bbref_id);
CREATE INDEX IF NOT EXISTS idx_players_mlb_id     ON players(mlb_id);
CREATE INDEX IF NOT EXISTS idx_players_position   ON players(position);
CREATE INDEX IF NOT EXISTS idx_players_reached_mlb ON players(reached_mlb);


-- -----------------------------------------------------------------------------
-- MILB_SEASONS — BATTING
-- One row per player × season × level × team.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS milb_batting (
    id              SERIAL PRIMARY KEY,
    player_id       INTEGER      NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    season          SMALLINT     NOT NULL,               -- e.g. 2019
    level           VARCHAR(10)  NOT NULL,               -- Rk, A-, A, A+, AA, AAA
    level_order     SMALLINT     NOT NULL,               -- 1=Rk … 6=AAA (for sorting)
    team_abbr       VARCHAR(10),
    league          VARCHAR(30),
    age             NUMERIC(4,1),                        -- age at start of season
    g               SMALLINT,
    pa              SMALLINT,
    ab              SMALLINT,
    r               SMALLINT,
    h               SMALLINT,
    doubles         SMALLINT,
    triples         SMALLINT,
    hr              SMALLINT,
    rbi             SMALLINT,
    sb              SMALLINT,
    cs              SMALLINT,
    bb              SMALLINT,
    so              SMALLINT,
    ba              NUMERIC(5,3),
    obp             NUMERIC(5,3),
    slg             NUMERIC(5,3),
    ops             NUMERIC(5,3),
    -- Derived / engineered fields (populated by feature pipeline)
    iso             NUMERIC(5,3),                        -- SLG - BA
    bb_pct          NUMERIC(5,3),                        -- BB / PA
    k_pct           NUMERIC(5,3),                        -- SO / PA
    bb_k_ratio      NUMERIC(6,3),                        -- BB / max(SO,1)
    woba            NUMERIC(5,3),                        -- if weights available
    level_adj_ops   NUMERIC(5,3),                        -- OPS adjusted for level run environment
    age_adj_ops     NUMERIC(5,3),                        -- OPS adjusted for player age vs league-average age at level
    scraped_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (player_id, season, level, team_abbr)
);

CREATE INDEX IF NOT EXISTS idx_milb_batting_player  ON milb_batting(player_id);
CREATE INDEX IF NOT EXISTS idx_milb_batting_season  ON milb_batting(season);
CREATE INDEX IF NOT EXISTS idx_milb_batting_level   ON milb_batting(level_order);


-- -----------------------------------------------------------------------------
-- MILB_SEASONS — PITCHING
-- One row per player × season × level × team.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS milb_pitching (
    id              SERIAL PRIMARY KEY,
    player_id       INTEGER      NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    season          SMALLINT     NOT NULL,
    level           VARCHAR(10)  NOT NULL,
    level_order     SMALLINT     NOT NULL,
    team_abbr       VARCHAR(10),
    league          VARCHAR(30),
    age             NUMERIC(4,1),
    g               SMALLINT,
    gs              SMALLINT,
    w               SMALLINT,
    l               SMALLINT,
    sv              SMALLINT,
    ip              NUMERIC(6,1),
    h               SMALLINT,
    r               SMALLINT,
    er              SMALLINT,
    hr              SMALLINT,
    bb              SMALLINT,
    so              SMALLINT,
    era             NUMERIC(5,2),
    whip            NUMERIC(5,3),
    h9              NUMERIC(5,2),
    hr9             NUMERIC(5,2),
    bb9             NUMERIC(5,2),
    so9             NUMERIC(5,2),
    so_bb           NUMERIC(5,2),
    -- Derived / engineered fields
    fip             NUMERIC(5,2),                        -- Fielding Independent Pitching (if HR data available)
    k_pct           NUMERIC(5,3),
    bb_pct          NUMERIC(5,3),
    k_minus_bb      NUMERIC(5,3),                        -- K% - BB% (strong single predictor)
    gb_pct          NUMERIC(5,3),                        -- ground ball % (if available)
    level_adj_era   NUMERIC(5,2),
    age_adj_era     NUMERIC(5,2),
    scraped_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (player_id, season, level, team_abbr)
);

CREATE INDEX IF NOT EXISTS idx_milb_pitching_player ON milb_pitching(player_id);
CREATE INDEX IF NOT EXISTS idx_milb_pitching_season ON milb_pitching(season);
CREATE INDEX IF NOT EXISTS idx_milb_pitching_level  ON milb_pitching(level_order);


-- -----------------------------------------------------------------------------
-- MLB_SEASONS — BATTING
-- Career MLB stats for players who reached the majors.
-- Used for label construction and salary modeling.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mlb_batting (
    id              SERIAL PRIMARY KEY,
    player_id       INTEGER      NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    season          SMALLINT     NOT NULL,
    team_abbr       VARCHAR(10),
    league          CHAR(2),                             -- AL, NL
    age             NUMERIC(4,1),
    g               SMALLINT,
    pa              SMALLINT,
    ab              SMALLINT,
    r               SMALLINT,
    h               SMALLINT,
    doubles         SMALLINT,
    triples         SMALLINT,
    hr              SMALLINT,
    rbi             SMALLINT,
    sb              SMALLINT,
    bb              SMALLINT,
    so              SMALLINT,
    ba              NUMERIC(5,3),
    obp             NUMERIC(5,3),
    slg             NUMERIC(5,3),
    ops             NUMERIC(5,3),
    ops_plus        SMALLINT,                            -- park/league adjusted OPS
    war             NUMERIC(5,2),                        -- fWAR or rWAR; note source in model card
    scraped_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (player_id, season, team_abbr)
);

CREATE INDEX IF NOT EXISTS idx_mlb_batting_player ON mlb_batting(player_id);
CREATE INDEX IF NOT EXISTS idx_mlb_batting_season ON mlb_batting(season);


-- -----------------------------------------------------------------------------
-- MLB_SEASONS — PITCHING
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mlb_pitching (
    id              SERIAL PRIMARY KEY,
    player_id       INTEGER      NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    season          SMALLINT     NOT NULL,
    team_abbr       VARCHAR(10),
    league          CHAR(2),
    age             NUMERIC(4,1),
    g               SMALLINT,
    gs              SMALLINT,
    w               SMALLINT,
    l               SMALLINT,
    sv              SMALLINT,
    ip              NUMERIC(6,1),
    era             NUMERIC(5,2),
    era_plus        SMALLINT,
    fip             NUMERIC(5,2),
    whip            NUMERIC(5,3),
    so9             NUMERIC(5,2),
    bb9             NUMERIC(5,2),
    war             NUMERIC(5,2),
    scraped_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (player_id, season, team_abbr)
);

CREATE INDEX IF NOT EXISTS idx_mlb_pitching_player ON mlb_pitching(player_id);


-- -----------------------------------------------------------------------------
-- SALARY_HISTORY
-- Historical salary data per player per season.
-- Source: MLB Trade Rumors, Spotrac, or MLB API.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS salary_history (
    id              SERIAL PRIMARY KEY,
    player_id       INTEGER      NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    season          SMALLINT     NOT NULL,
    salary_usd      BIGINT,                              -- annual salary in dollars
    service_years   NUMERIC(4,2),                        -- cumulative service time at start of season
    contract_status VARCHAR(20),                         -- pre_arb, arb1, arb2, arb3, free_agent
    team_abbr       VARCHAR(10),
    source          VARCHAR(50),                         -- data provenance
    scraped_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (player_id, season)
);

CREATE INDEX IF NOT EXISTS idx_salary_player ON salary_history(player_id);


-- -----------------------------------------------------------------------------
-- ENGINEERED_FEATURES
-- Flattened, model-ready feature vectors per player.
-- Populated by the feature engineering pipeline; versioned by pipeline run.
-- One row per player × feature_version.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS engineered_features (
    id                          SERIAL PRIMARY KEY,
    player_id                   INTEGER      NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    feature_version             VARCHAR(20)  NOT NULL DEFAULT 'v1',  -- bump when feature set changes
    computed_at                 TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    -- Identity / context
    position_group              VARCHAR(10),             -- bat, sp, rp
    peak_level                  VARCHAR(10),             -- highest MiLB level reached
    peak_level_order            SMALLINT,
    seasons_in_minors           SMALLINT,
    age_at_pro_debut            NUMERIC(4,1),
    age_at_peak_level           NUMERIC(4,1),

    -- Batting aggregate features (NULL for pitchers)
    career_milb_pa              INTEGER,
    best_season_ops             NUMERIC(5,3),
    best_season_ops_level_adj   NUMERIC(5,3),
    career_milb_iso             NUMERIC(5,3),
    career_milb_bb_pct          NUMERIC(5,3),
    career_milb_k_pct           NUMERIC(5,3),
    ops_trajectory              NUMERIC(6,4),            -- slope of OPS over seasons (linear fit)
    ops_aaa                     NUMERIC(5,3),            -- OPS specifically at AAA (NULL if never reached)
    pa_at_aa_plus               INTEGER,                 -- total PA at AA or above
    age_adj_ops_peak            NUMERIC(5,3),

    -- Pitching aggregate features (NULL for batters)
    career_milb_ip              NUMERIC(7,1),
    best_season_era             NUMERIC(5,2),
    best_season_era_level_adj   NUMERIC(5,2),
    career_milb_k9              NUMERIC(5,2),
    career_milb_bb9             NUMERIC(5,2),
    career_milb_k_minus_bb      NUMERIC(5,3),
    career_milb_whip            NUMERIC(5,3),
    era_trajectory              NUMERIC(6,4),            -- slope of ERA over seasons
    era_aaa                     NUMERIC(5,2),
    ip_at_aa_plus               NUMERIC(7,1),

    -- Labels (for supervised training — NULL for active/current players)
    label_reached_mlb           BOOLEAN,
    label_years_to_mlb          NUMERIC(4,2),            -- NULL = right-censored (still in minors or retired without reaching)
    label_censored              BOOLEAN,                 -- TRUE = still in minors at observation cutoff
    label_career_war            NUMERIC(6,2),
    label_peak_salary_usd       BIGINT,
    label_career_earnings_usd   BIGINT,

    -- v2: cohort age, progression, durability, draft snapshot
    first_milb_season           SMALLINT,
    career_age_vs_level_avg     NUMERIC(6,3),            -- PA/IP-weighted mean (age - league avg age at level)
    ever_repeated_level         BOOLEAN,
    promotion_speed_score       NUMERIC(8,4),            -- tier score / seasons in system
    ops_yoy_delta               NUMERIC(7,4),
    k_minus_bb_yoy_delta        NUMERIC(7,4),
    is_improving                BOOLEAN,
    low_sample_season_flag      BOOLEAN,                 -- noisy career (e.g. max season PA < 200 as batter)
    draft_round_feat            SMALLINT,
    is_international_feat       BOOLEAN,
    signing_bonus_usd_feat      BIGINT,
    label_eligible_for_training BOOLEAN,

    -- v3: leakage-safe cutoff + cohort percentiles (see ml/cutoff_policy.py)
    prediction_cutoff_season       SMALLINT,
    cutoff_policy                  VARCHAR(64),
    ops_pctile_milb_weighted       NUMERIC(8,5),
    era_pctile_milb_weighted       NUMERIC(8,5),
    age_pctile_milb_weighted       NUMERIC(8,5),

    UNIQUE (player_id, feature_version)
);

CREATE INDEX IF NOT EXISTS idx_features_player  ON engineered_features(player_id);
CREATE INDEX IF NOT EXISTS idx_features_version ON engineered_features(feature_version);


-- -----------------------------------------------------------------------------
-- MODEL_REGISTRY
-- One row per trained model version. Tracks hyperparameters and eval metrics
-- for reproducibility (required for academic submission).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS model_registry (
    id                  SERIAL PRIMARY KEY,
    model_name          VARCHAR(50)  NOT NULL,           -- mlb_probability, timeline, similarity, salary
    version             VARCHAR(20)  NOT NULL,
    feature_version     VARCHAR(20)  NOT NULL,
    algorithm           VARCHAR(50),                     -- XGBClassifier, CoxPH, KNeighborsRegressor, etc.
    hyperparameters     JSONB,                           -- full param dict for reproducibility
    train_cutoff_year   SMALLINT,                        -- years BEFORE this used for training
    test_start_year     SMALLINT,                        -- holdout test set start year
    -- Classification metrics (mlb_probability model)
    auc_roc             NUMERIC(6,4),
    auc_pr              NUMERIC(6,4),
    brier_score         NUMERIC(6,4),
    calibration_slope   NUMERIC(6,4),
    -- Regression / survival metrics (timeline, salary models)
    concordance_index   NUMERIC(6,4),                   -- for Cox model
    mae                 NUMERIC(8,3),
    rmse                NUMERIC(8,3),
    r2                  NUMERIC(6,4),
    -- Metadata
    artifact_path       VARCHAR(255),                    -- path to serialized model file (.joblib / .pkl)
    trained_by          VARCHAR(50)  DEFAULT 'pipeline',
    trained_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    notes               TEXT,
    UNIQUE (model_name, version)
);


-- -----------------------------------------------------------------------------
-- PREDICTIONS
-- One row per player × model_version × run_date.
-- Stores the full prediction bundle returned to the API.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS predictions (
    id                      SERIAL PRIMARY KEY,
    player_id               INTEGER      NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    model_version           VARCHAR(20)  NOT NULL,
    predicted_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    -- Model 1: MLB probability
    mlb_probability         NUMERIC(5,4),               -- 0.0000–1.0000
    mlb_probability_ci_low  NUMERIC(5,4),
    mlb_probability_ci_high NUMERIC(5,4),

    -- Model 2: Timeline
    years_to_mlb_estimate   NUMERIC(4,2),
    years_to_mlb_ci_low     NUMERIC(4,2),
    years_to_mlb_ci_high    NUMERIC(4,2),
    survival_curve          JSONB,                       -- [{year: 1, prob_still_waiting: 0.82}, …]

    -- Model 3: Similarity
    similar_player_ids      JSONB,                       -- [{"mlb_id": 592450, "name": "…", "similarity": 0.91, "top_features": […]}, …]

    -- Model 4: Salary
    projected_salary_low    BIGINT,
    projected_salary_mid    BIGINT,
    projected_salary_high   BIGINT,
    projected_career_earn   BIGINT,
    salary_scenario         JSONB,                       -- [{year_in_mlb: 1, salary: 720000, status: "pre_arb"}, …]

    -- Explainability
    shap_values             JSONB                        -- [{feature, value, shap_impact}, …] sorted by |impact|
);

CREATE INDEX IF NOT EXISTS idx_predictions_player  ON predictions(player_id);
CREATE INDEX IF NOT EXISTS idx_predictions_version ON predictions(model_version);
-- Use UTC date so the index expression is immutable (plain ::date on timestamptz is not).
CREATE UNIQUE INDEX IF NOT EXISTS uq_predictions_player_model_day
    ON predictions (player_id, model_version, ((predicted_at AT TIME ZONE 'UTC')::date));


-- -----------------------------------------------------------------------------
-- SCRAPE_LOG
-- Audit trail for every scraping job. Useful for scheduling and debugging.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS scrape_log (
    id              SERIAL PRIMARY KEY,
    job_type        VARCHAR(30)  NOT NULL,               -- milb_batting, milb_pitching, mlb_salary, etc.
    player_id       INTEGER      REFERENCES players(id),
    bbref_id        VARCHAR(20),
    status          VARCHAR(10)  NOT NULL,               -- success, error, skipped
    rows_upserted   INTEGER      DEFAULT 0,
    error_message   TEXT,
    duration_ms     INTEGER,
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_scrape_log_player ON scrape_log(player_id);
CREATE INDEX IF NOT EXISTS idx_scrape_log_status ON scrape_log(status);


-- =============================================================================
-- VIEWS — convenience queries for the API layer
-- =============================================================================

-- Latest prediction per player
CREATE OR REPLACE VIEW v_latest_predictions AS
SELECT DISTINCT ON (player_id)
    p.*,
    pl.full_name,
    pl.position,
    pl.bbref_id,
    pl.mlb_id
FROM predictions p
JOIN players pl ON pl.id = p.player_id
ORDER BY player_id, predicted_at DESC;


-- Full player profile for the dashboard
CREATE OR REPLACE VIEW v_player_profile AS
SELECT
    pl.id,
    pl.bbref_id,
    pl.mlb_id,
    pl.full_name,
    pl.position,
    pl.bats,
    pl.throws,
    pl.birth_date,
    pl.birth_country,
    pl.pro_debut_date,
    pl.milb_debut_date,
    pl.mlb_debut_date,
    pl.reached_mlb,
    pl.years_to_mlb,
    ef.peak_level,
    ef.seasons_in_minors,
    ef.age_at_pro_debut,
    ef.best_season_ops,
    ef.best_season_era,
    pred.mlb_probability,
    pred.years_to_mlb_estimate,
    pred.projected_salary_mid,
    pred.projected_career_earn,
    pred.predicted_at
FROM players pl
LEFT JOIN engineered_features ef
    ON ef.player_id = pl.id AND ef.feature_version = 'v1'
LEFT JOIN v_latest_predictions pred
    ON pred.player_id = pl.id;
