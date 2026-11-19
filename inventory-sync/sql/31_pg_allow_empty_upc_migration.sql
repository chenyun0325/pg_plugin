\set ON_ERROR_STOP on
\encoding UTF8

-- MySQL CHAR NOT NULL allows ''. Preserve that source key through component,
-- stage, summary and outbox; NOT NULL, varchar limits and unique keys still apply.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

DO $$
BEGIN
    IF strpos(lower(pg_get_functiondef(
        'operations.refresh_inventory_summary(character varying,character varying,integer)'::regprocedure
    )), 'btrim(') > 0 THEN
        RAISE EXCEPTION 'Apply migration 28 first; this migration preserves raw source keys';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION operations.normalize_inventory_stage_row()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- Preserve source business keys byte for byte; whitespace can distinguish rows.
    -- MySQL permits an empty UPC; keep it as the same non-null business key.
    IF NEW.wh_id = '' THEN
        RAISE EXCEPTION 'blank wh_id is not allowed';
    END IF;
    RETURN NEW;
END;
$$;

ALTER TABLE operations.inventory_summary
    DROP CONSTRAINT IF EXISTS inventory_summary_upc_check;

COMMIT;
