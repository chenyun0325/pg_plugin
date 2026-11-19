\set ON_ERROR_STOP on
\encoding UTF8

-- Explicit restore after source-data reconciliation. This script fails and
-- rolls back if any existing summary has negative available stock.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $migration$
BEGIN
    IF EXISTS (
        SELECT 1 FROM operations.inventory_summary
        WHERE on_hand_qty < reserved_qty + hold_qty
    ) THEN
        RAISE EXCEPTION 'Reconcile rows with on_hand_qty < reserved_qty + hold_qty before restoring the availability check';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'operations.inventory_summary'::regclass
          AND conname = 'ck_summary_available_nonnegative'
    ) THEN
        ALTER TABLE operations.inventory_summary
            ADD CONSTRAINT ck_summary_available_nonnegative
            CHECK (on_hand_qty >= reserved_qty + hold_qty) NOT VALID;
    END IF;
END;
$migration$;

ALTER TABLE operations.inventory_summary
    VALIDATE CONSTRAINT ck_summary_available_nonnegative;

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
    IF strpos(v_definition, v_marker) > 0 THEN
        EXECUTE replace(v_definition, v_marker, v_guard);
    ELSIF strpos(v_definition, v_guard) = 0 THEN
        RAISE EXCEPTION 'Unexpected projection function: temporary marker not recognized; restore cancelled';
    END IF;
END;
$migration$;

COMMIT;
