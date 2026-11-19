\set ON_ERROR_STOP on
\encoding UTF8

-- Preserve legacy UPC/warehouse identifiers, including surrounding ASCII spaces.
-- Existing normalized data must subsequently be restored from the MySQL source.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

DO $migration$
DECLARE
    definition text;
BEGIN
    SELECT replace(pg_get_functiondef(
        'operations.refresh_inventory_summary(character varying,character varying,integer)'::regprocedure
    ), E'\r\n', E'\n') INTO definition;
    IF strpos(definition, 'v_wh_id varchar(16) := btrim(p_wh_id);') > 0 THEN
        definition := replace(definition, 'v_wh_id varchar(16) := btrim(p_wh_id);',
                             'v_wh_id varchar(16) := p_wh_id;');
    ELSIF strpos(definition, 'v_wh_id varchar(16) := p_wh_id;') = 0 THEN
        RAISE EXCEPTION 'Unrecognized warehouse key initialization; migration cancelled';
    END IF;
    IF strpos(definition, 'v_upc varchar(255) := btrim(p_upc);') > 0 THEN
        definition := replace(definition, 'v_upc varchar(255) := btrim(p_upc);',
                             'v_upc varchar(255) := p_upc;');
    ELSIF strpos(definition, 'v_upc varchar(255) := p_upc;') = 0 THEN
        RAISE EXCEPTION 'Unrecognized UPC initialization; migration cancelled';
    END IF;
    IF strpos(lower(definition), 'btrim(') > 0 THEN
        RAISE EXCEPTION 'Unexpected remaining normalization in projection; migration cancelled';
    END IF;
    -- Patch only key handling, retaining the installed signed-quantity, zero-owner,
    -- length-limit and missing-product behavior.
    EXECUTE definition;
END;
$migration$;

CREATE OR REPLACE FUNCTION operations.normalize_inventory_stage_row()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- Preserve source business keys byte for byte; whitespace can distinguish rows.
    IF NEW.upc = '' OR NEW.wh_id = '' THEN
        RAISE EXCEPTION 'blank UPC/wh_id is not allowed';
    END IF;
    RETURN NEW;
END;
$$;

ALTER TABLE operations.inventory_summary
    DROP CONSTRAINT IF EXISTS inventory_summary_upc_check,
    ADD CONSTRAINT inventory_summary_upc_check CHECK (length(upc) > 0);
ALTER TABLE operations.inventory_goods_map
    DROP CONSTRAINT IF EXISTS inventory_goods_map_upc_check,
    ADD CONSTRAINT inventory_goods_map_upc_check CHECK (length(upc) > 0);
ALTER TABLE operations.inventory_warehouse_map
    DROP CONSTRAINT IF EXISTS inventory_warehouse_map_legacy_wh_id_check,
    ADD CONSTRAINT inventory_warehouse_map_legacy_wh_id_check CHECK (length(legacy_wh_id) > 0);

COMMIT;
