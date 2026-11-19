\set ON_ERROR_STOP on
\encoding UTF8

-- Legacy import compatibility: preserve signed source quantities. Apply after
-- SQL 23 when negative available stock must also be retained. Owner checks,
-- keys, TBA rules and outbox integer/range checks remain active.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $migration$
DECLARE
    v_definition text;
    v_guard text := $guard$    IF v_on_hand < 0 OR v_reserved < 0 OR v_hold < 0 THEN
        RAISE EXCEPTION 'negative quantity for wh_id=%, upc=%, uid=%',
            v_wh_id, v_upc, p_uid;
    END IF;$guard$;
    v_marker text := '    -- TEMPORARY_CDC_ALLOW_NEGATIVE_QUANTITIES: retain signed source quantities.';
BEGIN
    SELECT replace(pg_get_functiondef(
        'operations.refresh_inventory_summary(character varying,character varying,integer)'::regprocedure
    ), E'\r\n', E'\n') INTO v_definition;
    IF strpos(v_definition, v_guard) > 0 THEN
        EXECUTE replace(v_definition, v_guard, v_marker);
    ELSIF strpos(v_definition, v_marker) = 0 THEN
        RAISE EXCEPTION 'Unexpected projection function: negative quantity guard not recognized; migration cancelled';
    END IF;
END;
$migration$;

ALTER TABLE operations.inventory_summary
    DROP CONSTRAINT IF EXISTS inventory_summary_on_hand_qty_check,
    DROP CONSTRAINT IF EXISTS inventory_summary_reserved_qty_check,
    DROP CONSTRAINT IF EXISTS inventory_summary_hold_qty_check;

COMMIT;
