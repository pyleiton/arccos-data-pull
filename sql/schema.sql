-- schema.sql
--
-- Complete schema for a fresh Arccos analytics database.
-- Idempotent (uses IF NOT EXISTS) so it's safe to re-apply.
--
-- Apply with:
--   docker exec -i arccos-postgres psql -U postgres -d arccos < sql/schema.sql
-- or:
--   psql -h localhost -U postgres -d arccos -f sql/schema.sql
--
-- For an existing DB created before multi-tenancy, run migrations/001_multitenant.sql
-- instead. This file represents the post-migration target state.

BEGIN;

-- ============================================================
-- Users (multi-tenancy root)
-- ============================================================
CREATE TABLE IF NOT EXISTS users (
    user_id        BIGSERIAL PRIMARY KEY,
    handle         TEXT NOT NULL UNIQUE,
    display_name   TEXT NOT NULL,
    email          TEXT UNIQUE,
    arccos_user_id TEXT UNIQUE,
    ghin_number    TEXT UNIQUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active      BOOLEAN NOT NULL DEFAULT TRUE
);

-- ============================================================
-- Reference data (shared across users)
-- ============================================================
CREATE TABLE IF NOT EXISTS sga_categories (
    category_id   INTEGER PRIMARY KEY,
    category_name TEXT NOT NULL,
    description   TEXT
);

INSERT INTO sga_categories (category_id, category_name, description) VALUES
    (1, 'driving',  'Tee shots on par 4s and par 5s'),
    (2, 'approach', 'Approach shots to the green'),
    (3, 'chip',     'Chipping around the green'),
    (4, 'sand',     'Sand/bunker shots'),
    (5, 'putting',  'Putting on the green')
ON CONFLICT (category_id) DO NOTHING;

-- ============================================================
-- Course catalog (shared — Pebble Beach is Pebble Beach for everyone)
-- ============================================================
CREATE TABLE IF NOT EXISTS courses (
    course_id      INTEGER PRIMARY KEY,
    course_version INTEGER,
    course_name    TEXT,
    latitude       DOUBLE PRECISION,
    longitude      DOUBLE PRECISION,
    city           TEXT,
    state          TEXT,
    country        TEXT,
    num_holes      INTEGER,
    mens_par       INTEGER,
    womens_par     INTEGER,
    raw_json       JSONB
);

CREATE TABLE IF NOT EXISTS course_tees (
    course_id INTEGER,
    tee_id    INTEGER,
    tee_name  TEXT,
    distance  INTEGER,
    slope     INTEGER,
    rating    DOUBLE PRECISION,
    PRIMARY KEY (course_id, tee_id)
);

CREATE TABLE IF NOT EXISTS course_holes (
    course_id       INTEGER,
    course_version  INTEGER,
    hole_id         INTEGER,
    mens_par        INTEGER,
    womens_par      INTEGER,
    mens_handicap   INTEGER,
    womens_handicap INTEGER,
    is_dual_green   TEXT,
    PRIMARY KEY (course_id, hole_id)
);

CREATE TABLE IF NOT EXISTS course_hole_tees (
    course_id INTEGER,
    hole_id   INTEGER,
    tee_id    INTEGER,
    tee_name  TEXT,
    distance  DOUBLE PRECISION,
    PRIMARY KEY (course_id, hole_id, tee_id)
);

-- ============================================================
-- Weather cache (keyed by round; shared cache across users not useful
-- because each round has its own GPS+time window)
-- ============================================================
CREATE TABLE IF NOT EXISTS weather_cache (
    round_id     INTEGER PRIMARY KEY,
    lat          DOUBLE PRECISION,
    lon          DOUBLE PRECISION,
    date         TEXT,
    start_hour   INTEGER,
    end_hour     INTEGER,
    raw_response JSONB
);

-- ============================================================
-- Per-user: clubs, player_profile
-- arccos_user_id retains the Arccos-issued identifier; user_id is the
-- internal FK used everywhere else.
-- ============================================================
CREATE TABLE IF NOT EXISTS clubs (
    user_id            BIGINT  NOT NULL REFERENCES users(user_id),
    arccos_user_id     TEXT,
    bag_id             INTEGER,
    club_id            INTEGER,
    club_type          INTEGER,
    sensor_uuid        TEXT,
    sensor_type_id     INTEGER,
    name               TEXT,
    perceived_distance DOUBLE PRECISION,
    start_date         TIMESTAMP,
    end_date           TIMESTAMP,
    is_deleted         TEXT,
    make               TEXT,
    model              TEXT,
    loft               TEXT,
    flex               TEXT,
    raw_json           JSONB,
    PRIMARY KEY (user_id, bag_id, club_id)
);
CREATE INDEX IF NOT EXISTS clubs_user_id_idx ON clubs(user_id);

CREATE TABLE IF NOT EXISTS player_profile (
    user_id             BIGINT  PRIMARY KEY REFERENCES users(user_id),
    arccos_user_id      TEXT,
    handicap            DOUBLE PRECISION,
    total_rounds        INTEGER,
    holes_played        INTEGER,
    shots_played        INTEGER,
    goal_hcp            DOUBLE PRECISION,
    num_rounds_setting  INTEGER
);

-- ============================================================
-- Rounds (per-user root for round-level data)
-- ============================================================
CREATE TABLE IF NOT EXISTS rounds (
    round_id            INTEGER PRIMARY KEY,
    round_uuid          TEXT,
    user_id             BIGINT  NOT NULL REFERENCES users(user_id),
    arccos_user_id      TEXT,
    course_id           INTEGER,
    course_version      INTEGER,
    course_name         TEXT,
    course_and_tee      TEXT,                    -- computed: "{course_name} - {tee_name}"
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    last_modified       TIMESTAMP,
    tee_id              INTEGER,
    num_holes           INTEGER,
    num_shots           INTEGER,
    par                 INTEGER,
    over_under          INTEGER,
    is_ended            TEXT,
    ball_make_id        INTEGER,
    ball_model_id       INTEGER,
    drive_hcp           DOUBLE PRECISION,
    approach_hcp        DOUBLE PRECISION,
    chip_hcp            DOUBLE PRECISION,
    sand_hcp            DOUBLE PRECISION,
    putt_hcp            DOUBLE PRECISION,
    notes               TEXT,
    -- Weather (Visual Crossing)
    avg_temp_f             DOUBLE PRECISION,
    avg_feels_like_f       DOUBLE PRECISION,
    temp_max_f             DOUBLE PRECISION,
    temp_min_f             DOUBLE PRECISION,
    avg_humidity_pct       DOUBLE PRECISION,
    avg_dew_point_f        DOUBLE PRECISION,
    avg_wind_mph           DOUBLE PRECISION,
    avg_wind_dir           DOUBLE PRECISION,
    max_wind_gust_mph      DOUBLE PRECISION,
    total_precip_in        DOUBLE PRECISION,
    precip_prob_pct        DOUBLE PRECISION,
    precip_type            TEXT,
    snow_in                DOUBLE PRECISION,
    avg_cloud_cover_pct    DOUBLE PRECISION,
    avg_pressure_hpa       DOUBLE PRECISION,
    avg_visibility_mi      DOUBLE PRECISION,
    avg_uv_index           DOUBLE PRECISION,
    avg_solar_radiation    DOUBLE PRECISION,
    solar_energy           DOUBLE PRECISION,
    severe_risk            DOUBLE PRECISION,
    moon_phase             DOUBLE PRECISION,
    feels_like_max_f       DOUBLE PRECISION,
    feels_like_min_f       DOUBLE PRECISION,
    precip_cover_pct       DOUBLE PRECISION,
    snow_depth_in          DOUBLE PRECISION,
    weather_conditions     TEXT,
    weather_description    TEXT,
    weather_icon           TEXT,
    -- Sunrise/sunset
    sunrise            TEXT,
    sunset             TEXT,
    dawn               TEXT,
    dusk               TEXT,
    day_length_hrs     DOUBLE PRECISION,
    solar_noon         TEXT,
    golden_hour        TEXT,
    first_light        TEXT,
    last_light         TEXT,
    sun_altitude       DOUBLE PRECISION,
    sunrise_azimuth    DOUBLE PRECISION,
    sunset_azimuth     DOUBLE PRECISION,
    moon_illumination  DOUBLE PRECISION,
    moon_phase_name    TEXT,
    moonrise           TEXT,
    moonset            TEXT
);
CREATE INDEX IF NOT EXISTS rounds_user_id_idx ON rounds(user_id);
CREATE UNIQUE INDEX IF NOT EXISTS rounds_round_uuid_idx ON rounds(round_uuid)
    WHERE round_uuid IS NOT NULL;

-- ============================================================
-- Per-round children: holes, shots
-- ============================================================
CREATE TABLE IF NOT EXISTS holes (
    round_id              INTEGER,
    hole_id               INTEGER,
    user_id               BIGINT NOT NULL REFERENCES users(user_id),
    num_shots             INTEGER,
    is_gir                TEXT,
    putts                 INTEGER,
    is_fairway            TEXT,
    is_fairway_right      TEXT,
    is_fairway_left       TEXT,
    is_sand_save_chance   TEXT,
    is_sand_save          TEXT,
    is_up_down_chance     TEXT,
    is_up_down            TEXT,
    approach_shot_id      INTEGER,
    pin_lat               DOUBLE PRECISION,
    pin_long              DOUBLE PRECISION,
    start_time            TIMESTAMP,
    end_time              TIMESTAMP,
    score_override        INTEGER,
    PRIMARY KEY (round_id, hole_id)
);
CREATE INDEX IF NOT EXISTS holes_user_id_idx ON holes(user_id);

CREATE TABLE IF NOT EXISTS shots (
    round_id                       INTEGER,
    hole_id                        INTEGER,
    shot_id                        INTEGER,
    user_id                        BIGINT NOT NULL REFERENCES users(user_id),
    shot_uuid                      TEXT,
    club_type                      INTEGER,
    club_id                        INTEGER,
    start_lat                      DOUBLE PRECISION,
    start_long                     DOUBLE PRECISION,
    end_lat                        DOUBLE PRECISION,
    end_long                       DOUBLE PRECISION,
    distance                       DOUBLE PRECISION,
    is_half_swing                  TEXT,
    start_altitude                 DOUBLE PRECISION,
    end_altitude                   DOUBLE PRECISION,
    shot_time                      TIMESTAMP,
    should_ignore                  TEXT,
    num_penalties                  INTEGER,
    user_start_terrain_override    INTEGER,
    should_consider_putt_as_chip   TEXT,
    tour_quality                   JSONB,
    PRIMARY KEY (round_id, hole_id, shot_id)
);
CREATE INDEX IF NOT EXISTS shots_user_id_idx ON shots(user_id);
-- shot_uuid has known duplicates from Arccos; index for lookups but don't enforce UNIQUE.
CREATE INDEX IF NOT EXISTS shots_shot_uuid_idx ON shots(shot_uuid)
    WHERE shot_uuid IS NOT NULL;

-- ============================================================
-- SGA (Strokes Gained Analysis) tables — all per-round, denormalize user_id
-- ============================================================
CREATE TABLE IF NOT EXISTS sga_analysis (
    round_id  INTEGER,
    goal_hcp  INTEGER,
    user_id   BIGINT NOT NULL REFERENCES users(user_id),
    overall_sga             DOUBLE PRECISION,
    drive_sga               DOUBLE PRECISION,
    approach_sga            DOUBLE PRECISION,
    short_game_sga          DOUBLE PRECISION,
    putting_sga             DOUBLE PRECISION,
    historic_drive_sga      DOUBLE PRECISION,
    historic_approach_sga   DOUBLE PRECISION,
    historic_short_sga      DOUBLE PRECISION,
    historic_putting_sga    DOUBLE PRECISION,
    pace_of_play            TEXT,
    avg_drive_distance      DOUBLE PRECISION,
    longest_drive           DOUBLE PRECISION,
    avg_approach_distance   DOUBLE PRECISION,
    total_putts             INTEGER,
    zero_putts              INTEGER,
    one_putts               INTEGER,
    two_putts               INTEGER,
    three_putts             INTEGER,
    gir_hit                 INTEGER,
    gir_total               INTEGER,
    fairways_hit            INTEGER,
    fairways_total          INTEGER,
    up_and_down_success     INTEGER,
    up_and_down_total       INTEGER,
    total_distance          DOUBLE PRECISION,
    par3_avg_score          DOUBLE PRECISION,
    par3_sga                DOUBLE PRECISION,
    par4_avg_score          DOUBLE PRECISION,
    par4_sga                DOUBLE PRECISION,
    par5_avg_score          DOUBLE PRECISION,
    par5_sga                DOUBLE PRECISION,
    birdie_pct              DOUBLE PRECISION,
    par_pct                 DOUBLE PRECISION,
    bogey_pct               DOUBLE PRECISION,
    double_plus_pct         DOUBLE PRECISION,
    sg_distance             DOUBLE PRECISION,
    sg_accuracy             DOUBLE PRECISION,
    sg_penalties            DOUBLE PRECISION,
    gir_pct                 DOUBLE PRECISION,
    gir_miss_left_pct       DOUBLE PRECISION,
    gir_miss_right_pct      DOUBLE PRECISION,
    gir_miss_short_pct      DOUBLE PRECISION,
    gir_miss_long_pct       DOUBLE PRECISION,
    avg_proximity_gir_ft    DOUBLE PRECISION,
    avg_proximity_all_ft    DOUBLE PRECISION,
    one_putt_pct            DOUBLE PRECISION,
    two_putt_pct            DOUBLE PRECISION,
    three_putt_pct          DOUBLE PRECISION,
    putts_per_hole          DOUBLE PRECISION,
    putts_per_gir           DOUBLE PRECISION,
    raw_json                JSONB,
    PRIMARY KEY (round_id, goal_hcp)
);
CREATE INDEX IF NOT EXISTS sga_analysis_user_id_idx ON sga_analysis(user_id);

CREATE TABLE IF NOT EXISTS sga_by_distance (
    round_id    INTEGER,
    goal_hcp    INTEGER,
    category    TEXT,
    slab_id     INTEGER,
    user_id     BIGINT NOT NULL REFERENCES users(user_id),
    slab_label  TEXT,
    sga         DOUBLE PRECISION,
    shots_count INTEGER,
    PRIMARY KEY (round_id, goal_hcp, category, slab_id)
);
CREATE INDEX IF NOT EXISTS sga_by_distance_user_id_idx ON sga_by_distance(user_id);

CREATE TABLE IF NOT EXISTS sga_by_terrain (
    round_id    INTEGER,
    goal_hcp    INTEGER,
    category    TEXT,
    terrain     TEXT,
    user_id     BIGINT NOT NULL REFERENCES users(user_id),
    sga         DOUBLE PRECISION,
    shots_count INTEGER,
    PRIMARY KEY (round_id, goal_hcp, category, terrain)
);
CREATE INDEX IF NOT EXISTS sga_by_terrain_user_id_idx ON sga_by_terrain(user_id);

CREATE TABLE IF NOT EXISTS sga_by_hole_shape (
    round_id    INTEGER,
    goal_hcp    INTEGER,
    hole_shape  TEXT,
    user_id     BIGINT NOT NULL REFERENCES users(user_id),
    sga         DOUBLE PRECISION,
    shots_count INTEGER,
    PRIMARY KEY (round_id, goal_hcp, hole_shape)
);
CREATE INDEX IF NOT EXISTS sga_by_hole_shape_user_id_idx ON sga_by_hole_shape(user_id);

CREATE TABLE IF NOT EXISTS sga_putting_by_hole (
    round_id  INTEGER,
    goal_hcp  INTEGER,
    hole_id   INTEGER,
    user_id   BIGINT NOT NULL REFERENCES users(user_id),
    sga       DOUBLE PRECISION,
    PRIMARY KEY (round_id, goal_hcp, hole_id)
);
CREATE INDEX IF NOT EXISTS sga_putting_by_hole_user_id_idx ON sga_putting_by_hole(user_id);

-- ============================================================
-- GHIN data (per-user)
-- ============================================================
CREATE TABLE IF NOT EXISTS ghin_scores (
    id                          INTEGER PRIMARY KEY,
    user_id                     BIGINT NOT NULL REFERENCES users(user_id),
    golfer_id                   INTEGER,
    played_at                   DATE,
    course_name                 TEXT,
    course_id                   TEXT,
    tee_name                    TEXT,
    tee_set_id                  TEXT,
    tee_set_side                TEXT,
    adjusted_gross_score        INTEGER,
    front9_adjusted             INTEGER,
    back9_adjusted              INTEGER,
    net_score                   INTEGER,
    net_score_differential      DOUBLE PRECISION,
    course_rating               DOUBLE PRECISION,
    slope_rating                INTEGER,
    differential                DOUBLE PRECISION,
    unadjusted_differential     DOUBLE PRECISION,
    pcc                         INTEGER,
    course_handicap             TEXT,
    score_type                  TEXT,
    number_of_holes             INTEGER,
    number_of_played_holes      INTEGER,
    posted_at                   TIMESTAMP,
    posted_on_home_course       BOOLEAN,
    is_manual                   BOOLEAN,
    edited                      BOOLEAN,
    exceptional                 BOOLEAN,
    used                        BOOLEAN,
    revision                    BOOLEAN,
    penalty                     TEXT,
    penalty_type                TEXT,
    raw_json                    JSONB
);
CREATE INDEX IF NOT EXISTS ghin_scores_user_id_idx ON ghin_scores(user_id);

CREATE TABLE IF NOT EXISTS ghin_handicap_history (
    id              TEXT PRIMARY KEY,
    user_id         BIGINT NOT NULL REFERENCES users(user_id),
    rev_date        DATE,
    handicap_index  DOUBLE PRECISION,
    low_hi          DOUBLE PRECISION,
    hard_cap        TEXT,
    soft_cap        TEXT
);
CREATE INDEX IF NOT EXISTS ghin_handicap_history_user_id_idx ON ghin_handicap_history(user_id);

CREATE TABLE IF NOT EXISTS ghin_hole_scores (
    ghin_score_id        INTEGER,
    hole_number          INTEGER,
    user_id              BIGINT NOT NULL REFERENCES users(user_id),
    par                  INTEGER,
    raw_score            INTEGER,
    adjusted_gross_score INTEGER,
    stroke_allocation    INTEGER,
    x_hole               BOOLEAN,
    PRIMARY KEY (ghin_score_id, hole_number)
);
CREATE INDEX IF NOT EXISTS ghin_hole_scores_user_id_idx ON ghin_hole_scores(user_id);

-- ============================================================
-- Metabase views — every view exposes user_id (+ handle, display_name)
-- so dashboards can filter via Metabase user attributes.
-- DROP first so column reorderings between versions don't trip
-- CREATE OR REPLACE's "compatible columns" rule.
-- ============================================================
DROP VIEW IF EXISTS v_round_summary    CASCADE;
DROP VIEW IF EXISTS v_hole_detail      CASCADE;
DROP VIEW IF EXISTS v_shot_detail      CASCADE;
DROP VIEW IF EXISTS v_handicap_history CASCADE;

CREATE OR REPLACE VIEW v_round_summary AS
SELECT
    r.user_id,
    u.handle,
    u.display_name,
    r.round_id,
    r.course_name,
    r.course_and_tee,
    r.start_time,
    r.start_time::DATE                          AS round_date,
    EXTRACT(YEAR  FROM r.start_time)::INT       AS year,
    EXTRACT(MONTH FROM r.start_time)::INT       AS month,
    TO_CHAR(r.start_time, 'Mon')                AS month_name,
    r.par,
    r.par + r.over_under                        AS score,
    r.over_under,
    r.num_holes,
    r.avg_temp_f,
    r.avg_feels_like_f,
    r.avg_wind_mph,
    r.max_wind_gust_mph,
    r.avg_humidity_pct,
    r.total_precip_in,
    r.weather_conditions,
    r.weather_description,
    s.overall_sga,
    s.drive_sga,
    s.approach_sga,
    s.short_game_sga,
    s.putting_sga,
    s.avg_drive_distance,
    s.longest_drive,
    s.total_putts,
    s.one_putts,
    s.two_putts,
    s.three_putts,
    s.gir_hit,
    s.gir_total,
    CASE WHEN s.gir_total > 0
         THEN ROUND(s.gir_hit * 100.0 / s.gir_total, 1) END AS gir_pct,
    s.fairways_hit,
    s.fairways_total,
    CASE WHEN s.fairways_total > 0
         THEN ROUND(s.fairways_hit * 100.0 / s.fairways_total, 1) END AS fairway_pct,
    s.up_and_down_success,
    s.up_and_down_total,
    s.birdie_pct,
    s.par_pct,
    s.bogey_pct,
    s.double_plus_pct,
    s.par3_avg_score,
    s.par4_avg_score,
    s.par5_avg_score,
    s.putts_per_hole,
    s.putts_per_gir
FROM rounds r
LEFT JOIN sga_analysis s ON r.round_id = s.round_id AND s.goal_hcp = 0
LEFT JOIN users        u ON r.user_id  = u.user_id
WHERE r.num_holes = 18;

CREATE OR REPLACE VIEW v_hole_detail AS
SELECT
    h.user_id,
    u.handle,
    u.display_name,
    h.round_id,
    r.course_name,
    r.start_time::DATE             AS round_date,
    h.hole_id,
    ch.mens_par                    AS par,
    h.num_shots                    AS score,
    h.num_shots - COALESCE(ch.mens_par, 0) AS over_under,
    h.putts,
    h.is_gir,
    h.is_fairway,
    h.is_fairway_left,
    h.is_fairway_right,
    h.is_sand_save_chance,
    h.is_sand_save,
    h.is_up_down_chance,
    h.is_up_down,
    cht.distance                   AS tee_distance,
    ch.mens_handicap               AS handicap_rating
FROM holes h
JOIN rounds r        ON h.round_id  = r.round_id
LEFT JOIN users u    ON h.user_id   = u.user_id
LEFT JOIN course_holes ch
    ON r.course_id = ch.course_id AND h.hole_id = ch.hole_id
LEFT JOIN course_hole_tees cht
    ON r.course_id = cht.course_id
   AND h.hole_id   = cht.hole_id
   AND r.tee_id    = cht.tee_id
WHERE r.num_holes = 18;

CREATE OR REPLACE VIEW v_shot_detail AS
SELECT
    s.user_id,
    u.handle,
    u.display_name,
    s.round_id,
    r.course_name,
    r.start_time::DATE         AS round_date,
    s.hole_id,
    s.shot_id,
    c.club_type                AS real_club_type,
    CASE c.club_type
        WHEN  1 THEN 'Driver'  WHEN  2 THEN '3-Wood' WHEN  3 THEN '5-Wood'
        WHEN  4 THEN '7-Wood'  WHEN  5 THEN 'Hybrid'
        WHEN  6 THEN '2-Iron'  WHEN  7 THEN '3-Iron' WHEN  8 THEN '4-Iron'
        WHEN  9 THEN '5-Iron'  WHEN 10 THEN '6-Iron' WHEN 11 THEN '7-Iron'
        WHEN 12 THEN '8-Iron'  WHEN 13 THEN '9-Iron' WHEN 14 THEN 'PW'
        WHEN 15 THEN 'GW'      WHEN 16 THEN 'SW'     WHEN 17 THEN 'LW'
        WHEN 42 THEN 'Putter'
        ELSE 'Club ' || c.club_type
    END AS club_name,
    s.distance,
    s.start_lat,
    s.start_long,
    s.end_lat,
    s.end_long,
    s.start_altitude,
    s.end_altitude,
    s.num_penalties,
    s.shot_time
FROM shots s
JOIN rounds r     ON s.round_id  = r.round_id
LEFT JOIN users u ON s.user_id   = u.user_id
LEFT JOIN clubs c ON s.user_id   = c.user_id
                  AND s.club_type = c.club_id
WHERE s.should_ignore != 'T'
  AND r.num_holes = 18;

CREATE OR REPLACE VIEW v_handicap_history AS
SELECT
    h.user_id,
    u.handle,
    u.display_name,
    h.rev_date,
    h.handicap_index,
    h.low_hi
FROM ghin_handicap_history h
LEFT JOIN users u ON h.user_id = u.user_id
WHERE h.handicap_index < 900
ORDER BY h.user_id, h.rev_date;

COMMIT;
