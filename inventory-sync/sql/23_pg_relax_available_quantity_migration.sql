\set ON_ERROR_STOP on
\encoding UTF8

-- Temporary compatibility mode: retain the three source quantities even when
-- on_hand < reserved + hold. Per-quantity nonnegative constraints stay active.
-- Restore with 24_pg_restore_available_quantity_check.sql after reconciliation.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $migration$
DECLARE
    v_definition text;
    v_guard text := $guard$    IF v_on_hand < v_reserved + v_hold THEN
        RAISE EXCEPTION
            'on_hand(%) < reserved(%) + hold(%) for wh_id=%, upc=%, uid=%',
            v_on_hand, v_reserved, v_hold, v_wh_id, v_upc, p_uid;
    END IF;$guard$;
    v_marker text := '    -- TEMPORARY_CDC_ALLOW_NEGATIVE_AVAILABLE: retain source quantities.';
BEGIN
    SELECT replace(pg_get_functiondef(
        'operations.refresh_inventory_summary(character varying,character varying,integer)'::regprocedure
    ), E'\r\n', E'\n') INTO v_definition;

    IF strpos(v_definition, v_guard) > 0 THEN
        EXECUTE replace(v_definition, v_guard, v_marker);
    ELSIF strpos(v_definition, v_marker) = 0 THEN
        RAISE EXCEPTION 'Unexpected projection function: availability guard not recognized; migration cancelled';
    END IF;
END;
$migration$;

ALTER TABLE operations.inventory_summary
    DROP CONSTRAINT IF EXISTS ck_summary_available_nonnegative;

COMMIT;
