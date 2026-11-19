\set ON_ERROR_STOP on
\encoding UTF8

-- Explicit restore after signed legacy quantities have been reconciled. This
-- does not restore the separate available-stock invariant (use SQL 24 for it).
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $migration$
DECLARE
    v_column text;
    v_constraint text;
BEGIN
    IF EXISTS (
        SELECT 1 FROM operations.inventory_summary
        WHERE on_hand_qty < 0 OR reserved_qty < 0 OR hold_qty < 0
    ) THEN
        RAISE EXCEPTION 'Reconcile negative on_hand_qty/reserved_qty/hold_qty before restoring nonnegative checks';
    END IF;
    FOREACH v_column IN ARRAY ARRAY['on_hand_qty','reserved_qty','hold_qty'] LOOP
        v_constraint := 'inventory_summary_' || v_column || '_check';
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = 'operations.inventory_summary'::regclass
              AND conname = v_constraint
        ) THEN
            EXECUTE format('ALTER TABLE operations.inventory_summary ADD CONSTRAINT %I CHECK (%I >= 0) NOT VALID',
                           v_constraint, v_column);
        END IF;
        EXECUTE format('ALTER TABLE operations.inventory_summary VALIDATE CONSTRAINT %I', v_constraint);
    END LOOP;
END;
$migration$;

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
    IF strpos(v_definition, v_marker) > 0 THEN
        EXECUTE replace(v_definition, v_marker, v_guard);
    ELSIF strpos(v_definition, v_guard) = 0 THEN
        RAISE EXCEPTION 'Unexpected projection function: negative quantity marker not recognized; restore cancelled';
    END IF;
END;
$migration$;

COMMIT;
