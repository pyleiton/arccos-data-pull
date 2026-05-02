-- 002_tour_quality_jsonb.sql
--
-- Convert shots.tour_quality from TEXT to JSONB.
--
-- Background: Arccos changed tour_quality from a scalar string to a structured
-- object like {"percentile": 99, "method": "DISTANCE"}. Some legacy values were
-- stored as Python repr() strings (single quotes, unquoted enum values) which
-- are not valid JSON and cannot be cast. Those rows are NULL'd out — the data
-- is recoverable later by re-pulling those rounds with --fresh.
--
-- Run with:
--   docker exec -i arccos-postgres psql -U postgres -d arccos < migrations/002_tour_quality_jsonb.sql
--
-- Idempotent: safe to re-run.

BEGIN;

-- Skip everything if the column is already JSONB
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'shots' AND column_name = 'tour_quality'
          AND data_type = 'text'
    ) THEN
        -- 1. Helper: validate text as JSON, returning NULL on parse failure
        CREATE OR REPLACE FUNCTION pg_temp.is_valid_json(t TEXT) RETURNS BOOLEAN AS $f$
        BEGIN
            PERFORM t::JSONB;
            RETURN TRUE;
        EXCEPTION WHEN OTHERS THEN
            RETURN FALSE;
        END;
        $f$ LANGUAGE plpgsql IMMUTABLE;

        -- 2. NULL out empty strings and any value that won't parse as JSON
        UPDATE shots
        SET tour_quality = NULL
        WHERE tour_quality IS NOT NULL
          AND (tour_quality = '' OR NOT pg_temp.is_valid_json(tour_quality));

        -- 3. Convert column type
        ALTER TABLE shots
            ALTER COLUMN tour_quality TYPE JSONB USING tour_quality::JSONB;
    END IF;
END $$;

COMMIT;

-- Sanity check
\echo ''
\echo '=== shots.tour_quality column type ==='
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'shots' AND column_name = 'tour_quality';

\echo ''
\echo '=== tour_quality value distribution ==='
SELECT
    CASE WHEN tour_quality IS NULL THEN 'NULL' ELSE 'set' END AS state,
    COUNT(*)
FROM shots
GROUP BY 1;
