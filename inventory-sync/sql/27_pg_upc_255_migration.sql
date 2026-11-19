\set ON_ERROR_STOP on
\encoding UTF8

-- Run only after approval on an existing merged database. Preserve the current
-- projection's owner/signed-quantity/fallback behavior and widen UPC end to end.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

DO $migration$
DECLARE
    v_definition text;
    v_table text;
    v_type text;
    v_length integer;
BEGIN
    SELECT replace(pg_get_functiondef(
        'operations.refresh_inventory_summary(character varying,character varying,integer)'::regprocedure
    ), E'\r\n', E'\n') INTO v_definition;

    IF strpos(v_definition, 'IF length(v_upc) > 64 THEN') > 0 THEN
        v_definition := replace(v_definition, 'IF length(v_upc) > 64 THEN',
                                'IF length(v_upc) > 255 THEN');
    ELSIF strpos(v_definition, 'IF length(v_upc) > 255 THEN') = 0 THEN
        RAISE EXCEPTION 'Unrecognized UPC length guard; migration cancelled';
    END IF;
    IF strpos(v_definition, 'v_upc::varchar(64),') > 0 THEN
        v_definition := replace(v_definition, 'v_upc::varchar(64),', 'v_upc,');
    ELSIF strpos(v_definition, E'\n        v_upc,') = 0 THEN
        RAISE EXCEPTION 'Unrecognized UPC insert expression; migration cancelled';
    END IF;

    FOREACH v_table IN ARRAY ARRAY['inventory_goods_map', 'inventory_summary', 'inventory_summary_outbox'] LOOP
        SELECT data_type, character_maximum_length INTO v_type, v_length
        FROM information_schema.columns
        WHERE table_schema = 'operations' AND table_name = v_table AND column_name = 'upc';
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Missing operations.%.upc; migration cancelled', v_table;
        END IF;
        IF v_type <> 'character varying' OR v_length NOT IN (64, 255) THEN
            RAISE EXCEPTION 'Unexpected operations.%.upc type %(%); migration cancelled', v_table, v_type, v_length;
        END IF;
        IF v_length = 64 THEN
            EXECUTE format('ALTER TABLE operations.%I ALTER COLUMN upc TYPE varchar(255)', v_table);
        END IF;
    END LOOP;

    EXECUTE v_definition;
END;
$migration$;

COMMIT;
