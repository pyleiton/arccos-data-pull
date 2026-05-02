-- 001_multitenant.sql
--
-- Adds the users table and user_id FK to every per-user table.
-- Renames Arccos's user_id (TEXT) to arccos_user_id to avoid collision
-- with the new internal user_id (BIGINT FK to users).
--
-- Run with one of:
--   docker exec -i arccos-postgres psql -U postgres -d arccos < migrations/001_multitenant.sql
--   psql -h localhost -U postgres -d arccos -f migrations/001_multitenant.sql
--
-- Idempotent: safe to re-run. Wrapped in a single transaction.

BEGIN;

-- ============================================================
-- 1. users
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
-- 2. Seed initial user (Jason)
--    Auto-pulls arccos_user_id from existing player_profile/clubs
--    and ghin_number from existing ghin_scores, if present.
-- ============================================================
DO $$
DECLARE
    v_arccos_id TEXT;
    v_ghin_id   TEXT;
BEGIN
    SELECT user_id INTO v_arccos_id FROM player_profile LIMIT 1;
    IF v_arccos_id IS NULL THEN
        SELECT user_id INTO v_arccos_id FROM clubs WHERE user_id IS NOT NULL LIMIT 1;
    END IF;

    SELECT golfer_id::TEXT INTO v_ghin_id FROM ghin_scores LIMIT 1;

    INSERT INTO users (handle, display_name, email, arccos_user_id, ghin_number)
    VALUES ('jason', 'Jason Pyle', 'jason.pyle@doctums.com', v_arccos_id, v_ghin_id)
    ON CONFLICT (handle) DO UPDATE
        SET arccos_user_id = COALESCE(users.arccos_user_id, EXCLUDED.arccos_user_id),
            ghin_number    = COALESCE(users.ghin_number,    EXCLUDED.ghin_number);
END $$;

-- ============================================================
-- 3. clubs: rename user_id (TEXT, Arccos) -> arccos_user_id, add new user_id FK
-- ============================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'clubs' AND column_name = 'user_id' AND data_type = 'text'
    ) THEN
        ALTER TABLE clubs DROP CONSTRAINT IF EXISTS clubs_pkey;
        ALTER TABLE clubs RENAME COLUMN user_id TO arccos_user_id;
    END IF;
END $$;

ALTER TABLE clubs ADD COLUMN IF NOT EXISTS user_id BIGINT;
UPDATE clubs c SET user_id = u.user_id
FROM users u
WHERE c.user_id IS NULL AND c.arccos_user_id = u.arccos_user_id;
ALTER TABLE clubs ALTER COLUMN user_id SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'clubs_user_fk') THEN
        ALTER TABLE clubs ADD CONSTRAINT clubs_user_fk
            FOREIGN KEY (user_id) REFERENCES users(user_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'clubs_pkey') THEN
        ALTER TABLE clubs ADD PRIMARY KEY (user_id, bag_id, club_id);
    END IF;
END $$;

-- ============================================================
-- 4. player_profile: rename user_id -> arccos_user_id, new user_id PK
-- ============================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'player_profile' AND column_name = 'user_id' AND data_type = 'text'
    ) THEN
        ALTER TABLE player_profile DROP CONSTRAINT IF EXISTS player_profile_pkey;
        ALTER TABLE player_profile RENAME COLUMN user_id TO arccos_user_id;
    END IF;
END $$;

ALTER TABLE player_profile ADD COLUMN IF NOT EXISTS user_id BIGINT;
UPDATE player_profile p SET user_id = u.user_id
FROM users u
WHERE p.user_id IS NULL AND p.arccos_user_id = u.arccos_user_id;
ALTER TABLE player_profile ALTER COLUMN user_id SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'player_profile_user_fk') THEN
        ALTER TABLE player_profile ADD CONSTRAINT player_profile_user_fk
            FOREIGN KEY (user_id) REFERENCES users(user_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'player_profile_pkey') THEN
        ALTER TABLE player_profile ADD PRIMARY KEY (user_id);
    END IF;
END $$;

-- ============================================================
-- 5. rounds: rename user_id (TEXT, Arccos) -> arccos_user_id, add new user_id FK
-- ============================================================
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'rounds' AND column_name = 'user_id' AND data_type = 'text'
    ) THEN
        ALTER TABLE rounds RENAME COLUMN user_id TO arccos_user_id;
    END IF;
END $$;

ALTER TABLE rounds ADD COLUMN IF NOT EXISTS user_id BIGINT;

-- Prefer matching via arccos_user_id; fall back to default user for any orphans.
UPDATE rounds r SET user_id = u.user_id
FROM users u
WHERE r.user_id IS NULL AND r.arccos_user_id = u.arccos_user_id;

UPDATE rounds SET user_id = (SELECT user_id FROM users WHERE handle = 'jason')
WHERE user_id IS NULL;

ALTER TABLE rounds ALTER COLUMN user_id SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'rounds_user_fk') THEN
        ALTER TABLE rounds ADD CONSTRAINT rounds_user_fk
            FOREIGN KEY (user_id) REFERENCES users(user_id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS rounds_user_id_idx ON rounds(user_id);

-- ============================================================
-- 6. Per-round children: holes, shots, sga_*
--    Backfill via JOIN to rounds (user_id inherits from the parent round).
-- ============================================================

-- holes
ALTER TABLE holes ADD COLUMN IF NOT EXISTS user_id BIGINT;
UPDATE holes h SET user_id = r.user_id FROM rounds r
WHERE h.user_id IS NULL AND h.round_id = r.round_id;
ALTER TABLE holes ALTER COLUMN user_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'holes_user_fk') THEN
        ALTER TABLE holes ADD CONSTRAINT holes_user_fk FOREIGN KEY (user_id) REFERENCES users(user_id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS holes_user_id_idx ON holes(user_id);

-- shots
ALTER TABLE shots ADD COLUMN IF NOT EXISTS user_id BIGINT;
UPDATE shots s SET user_id = r.user_id FROM rounds r
WHERE s.user_id IS NULL AND s.round_id = r.round_id;
ALTER TABLE shots ALTER COLUMN user_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'shots_user_fk') THEN
        ALTER TABLE shots ADD CONSTRAINT shots_user_fk FOREIGN KEY (user_id) REFERENCES users(user_id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS shots_user_id_idx ON shots(user_id);

-- sga_analysis
ALTER TABLE sga_analysis ADD COLUMN IF NOT EXISTS user_id BIGINT;
UPDATE sga_analysis s SET user_id = r.user_id FROM rounds r
WHERE s.user_id IS NULL AND s.round_id = r.round_id;
ALTER TABLE sga_analysis ALTER COLUMN user_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sga_analysis_user_fk') THEN
        ALTER TABLE sga_analysis ADD CONSTRAINT sga_analysis_user_fk FOREIGN KEY (user_id) REFERENCES users(user_id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS sga_analysis_user_id_idx ON sga_analysis(user_id);

-- sga_by_distance
ALTER TABLE sga_by_distance ADD COLUMN IF NOT EXISTS user_id BIGINT;
UPDATE sga_by_distance s SET user_id = r.user_id FROM rounds r
WHERE s.user_id IS NULL AND s.round_id = r.round_id;
ALTER TABLE sga_by_distance ALTER COLUMN user_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sga_by_distance_user_fk') THEN
        ALTER TABLE sga_by_distance ADD CONSTRAINT sga_by_distance_user_fk FOREIGN KEY (user_id) REFERENCES users(user_id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS sga_by_distance_user_id_idx ON sga_by_distance(user_id);

-- sga_by_terrain
ALTER TABLE sga_by_terrain ADD COLUMN IF NOT EXISTS user_id BIGINT;
UPDATE sga_by_terrain s SET user_id = r.user_id FROM rounds r
WHERE s.user_id IS NULL AND s.round_id = r.round_id;
ALTER TABLE sga_by_terrain ALTER COLUMN user_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sga_by_terrain_user_fk') THEN
        ALTER TABLE sga_by_terrain ADD CONSTRAINT sga_by_terrain_user_fk FOREIGN KEY (user_id) REFERENCES users(user_id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS sga_by_terrain_user_id_idx ON sga_by_terrain(user_id);

-- sga_by_hole_shape
ALTER TABLE sga_by_hole_shape ADD COLUMN IF NOT EXISTS user_id BIGINT;
UPDATE sga_by_hole_shape s SET user_id = r.user_id FROM rounds r
WHERE s.user_id IS NULL AND s.round_id = r.round_id;
ALTER TABLE sga_by_hole_shape ALTER COLUMN user_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sga_by_hole_shape_user_fk') THEN
        ALTER TABLE sga_by_hole_shape ADD CONSTRAINT sga_by_hole_shape_user_fk FOREIGN KEY (user_id) REFERENCES users(user_id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS sga_by_hole_shape_user_id_idx ON sga_by_hole_shape(user_id);

-- sga_putting_by_hole
ALTER TABLE sga_putting_by_hole ADD COLUMN IF NOT EXISTS user_id BIGINT;
UPDATE sga_putting_by_hole s SET user_id = r.user_id FROM rounds r
WHERE s.user_id IS NULL AND s.round_id = r.round_id;
ALTER TABLE sga_putting_by_hole ALTER COLUMN user_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sga_putting_by_hole_user_fk') THEN
        ALTER TABLE sga_putting_by_hole ADD CONSTRAINT sga_putting_by_hole_user_fk FOREIGN KEY (user_id) REFERENCES users(user_id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS sga_putting_by_hole_user_id_idx ON sga_putting_by_hole(user_id);

-- ============================================================
-- 7. GHIN tables: add user_id FK
-- ============================================================
ALTER TABLE ghin_scores ADD COLUMN IF NOT EXISTS user_id BIGINT;
UPDATE ghin_scores SET user_id = (SELECT user_id FROM users WHERE handle = 'jason')
WHERE user_id IS NULL;
ALTER TABLE ghin_scores ALTER COLUMN user_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ghin_scores_user_fk') THEN
        ALTER TABLE ghin_scores ADD CONSTRAINT ghin_scores_user_fk FOREIGN KEY (user_id) REFERENCES users(user_id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS ghin_scores_user_id_idx ON ghin_scores(user_id);

ALTER TABLE ghin_handicap_history ADD COLUMN IF NOT EXISTS user_id BIGINT;
UPDATE ghin_handicap_history SET user_id = (SELECT user_id FROM users WHERE handle = 'jason')
WHERE user_id IS NULL;
ALTER TABLE ghin_handicap_history ALTER COLUMN user_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ghin_handicap_history_user_fk') THEN
        ALTER TABLE ghin_handicap_history ADD CONSTRAINT ghin_handicap_history_user_fk FOREIGN KEY (user_id) REFERENCES users(user_id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS ghin_handicap_history_user_id_idx ON ghin_handicap_history(user_id);

ALTER TABLE ghin_hole_scores ADD COLUMN IF NOT EXISTS user_id BIGINT;
UPDATE ghin_hole_scores h SET user_id = s.user_id FROM ghin_scores s
WHERE h.user_id IS NULL AND h.ghin_score_id = s.id;
ALTER TABLE ghin_hole_scores ALTER COLUMN user_id SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ghin_hole_scores_user_fk') THEN
        ALTER TABLE ghin_hole_scores ADD CONSTRAINT ghin_hole_scores_user_fk FOREIGN KEY (user_id) REFERENCES users(user_id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS ghin_hole_scores_user_id_idx ON ghin_hole_scores(user_id);

-- ============================================================
-- 8. Indexes on Arccos UUIDs
-- ============================================================
-- round_uuid is unique in observed data; enforce it.
CREATE UNIQUE INDEX IF NOT EXISTS rounds_round_uuid_idx ON rounds(round_uuid)
    WHERE round_uuid IS NOT NULL;

-- shot_uuid has at least one observed duplicate from Arccos (data quality bug
-- on their end — composite PK (round_id, hole_id, shot_id) is the real identity).
-- Index for lookups, but do NOT enforce uniqueness.
CREATE INDEX IF NOT EXISTS shots_shot_uuid_idx ON shots(shot_uuid)
    WHERE shot_uuid IS NOT NULL;

COMMIT;

-- ============================================================
-- Verify (psql meta-commands; will no-op outside psql)
-- ============================================================
\echo ''
\echo '=== Users ==='
SELECT user_id, handle, display_name, email, arccos_user_id, ghin_number FROM users;

\echo ''
\echo '=== Row counts per user ==='
SELECT 'rounds'         AS tbl, user_id, COUNT(*) FROM rounds                GROUP BY user_id
UNION ALL SELECT 'holes',          user_id, COUNT(*) FROM holes              GROUP BY user_id
UNION ALL SELECT 'shots',          user_id, COUNT(*) FROM shots              GROUP BY user_id
UNION ALL SELECT 'clubs',          user_id, COUNT(*) FROM clubs              GROUP BY user_id
UNION ALL SELECT 'player_profile', user_id, COUNT(*) FROM player_profile     GROUP BY user_id
UNION ALL SELECT 'ghin_scores',    user_id, COUNT(*) FROM ghin_scores        GROUP BY user_id
ORDER BY tbl, user_id;
