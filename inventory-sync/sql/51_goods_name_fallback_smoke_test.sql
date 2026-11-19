\set ON_ERROR_STOP on
\encoding UTF8

-- Safe on an already-populated merged database: choose unused keys and roll
-- back every fixture, including all trigger-generated summary/outbox changes.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SET CONSTRAINTS ALL IMMEDIATE;

DO $$
DECLARE
    v_token text := substr(md5(clock_timestamp()::text || random()::text), 1, 12);
    v_wh_id varchar(16);
    v_upc varchar(64);
    v_id integer;
    v_warehouse_id bigint;
    v_attempt integer;
    v_source_key text;
BEGIN
    v_wh_id := 'fb-' || v_token;
    v_upc := 'cdc-goods-fallback-' || v_token;
    FOR v_attempt IN 1..100 LOOP
        v_id := 2000000000 + floor(random() * 100000000)::integer;
        v_warehouse_id := 10000000000::bigint + v_id;
        EXIT WHEN NOT EXISTS (SELECT 1 FROM inventory_stage.mem_user_inventory WHERE id = v_id)
          AND NOT EXISTS (SELECT 1 FROM inventory_stage.mem_user_inventory_lock WHERE id = v_id)
          AND NOT EXISTS (SELECT 1 FROM inventory_stage.mem_user_inventory_tracking WHERE id = v_id)
          AND NOT EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id = v_id)
          AND NOT EXISTS (SELECT 1 FROM operations.inventory_summary_outbox WHERE summary_id = v_id)
          AND NOT EXISTS (SELECT 1 FROM inventory_flink_stage.inventory_component WHERE source_id = v_id)
          AND NOT EXISTS (SELECT 1 FROM operations.inventory_warehouse_map WHERE warehouse_id = v_warehouse_id);
        IF v_attempt = 100 THEN
            RAISE EXCEPTION 'could not allocate unused smoke test keys';
        END IF;
    END LOOP;
    v_source_key := 'fallback_smoke_' || v_token;

    INSERT INTO operations.inventory_warehouse_map(legacy_wh_id, warehouse_id)
    VALUES (v_wh_id, v_warehouse_id);

    IF EXISTS (SELECT 1 FROM operations.inventory_goods_map WHERE upc = v_upc) THEN
        RAISE EXCEPTION 'smoke UPC is already mapped';
    END IF;

    INSERT INTO inventory_flink_stage.inventory_component
        (source_row_id, source_table, source_id, quantity_kind,
         upc, uid, wh_id, quantity, last_update_time)
    VALUES (v_source_key || '_on_hand', 'mem_user_inventory', v_id, 'ON_HAND',
            v_upc, v_id, v_wh_id, 10, 1700000000);

    IF NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary
        WHERE id = v_id AND warehouse_id = v_warehouse_id AND upc = v_upc
          AND owner_id = v_id AND goods_name = v_upc AND goods_version = 0
          AND on_hand_qty = 10 AND reserved_qty = 0 AND hold_qty = 0
    ) THEN
        RAISE EXCEPTION 'unmapped UPC fallback failed through component adapter';
    END IF;
    IF EXISTS (SELECT 1 FROM operations.inventory_goods_map WHERE upc = v_upc) THEN
        RAISE EXCEPTION 'fallback unexpectedly created a goods mapping';
    END IF;

    UPDATE inventory_flink_stage.inventory_component
       SET quantity = 12, last_update_time = 1700000001
     WHERE source_row_id = v_source_key || '_on_hand';

    INSERT INTO inventory_flink_stage.inventory_component
        (source_row_id, source_table, source_id, quantity_kind,
         upc, uid, wh_id, quantity, last_update_time)
    VALUES
        (v_source_key || '_reserved', 'mem_user_inventory_lock', v_id, 'RESERVED',
         v_upc, v_id, v_wh_id, 3, 1700000002),
        (v_source_key || '_hold', 'mem_user_inventory_tracking', v_id, 'HOLD',
         v_upc, v_id, v_wh_id, 2, 1700000003);

    IF NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary
        WHERE id = v_id AND goods_name = v_upc AND goods_version = 0
          AND on_hand_qty = 12 AND reserved_qty = 3 AND hold_qty = 2
    ) THEN
        RAISE EXCEPTION 'unmapped UPC quantity update/merge failed';
    END IF;

    INSERT INTO operations.inventory_goods_map(upc, goods_name, goods_version)
    VALUES (v_upc, 'Fallback smoke mapped goods', 7);
    UPDATE inventory_flink_stage.inventory_component
       SET last_update_time = 1700000004
     WHERE source_row_id = v_source_key || '_on_hand';

    IF NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary
        WHERE id = v_id AND goods_name = 'Fallback smoke mapped goods'
          AND goods_version = 7 AND on_hand_qty = 12
          AND reserved_qty = 3 AND hold_qty = 2
    ) THEN
        RAISE EXCEPTION 'real goods mapping did not replace fallback on next inventory event';
    END IF;

    DELETE FROM inventory_flink_stage.inventory_component
     WHERE source_row_id = v_source_key || '_reserved';
    DELETE FROM inventory_flink_stage.inventory_component
     WHERE source_row_id = v_source_key || '_hold';
    DELETE FROM inventory_flink_stage.inventory_component
     WHERE source_row_id = v_source_key || '_on_hand';
    IF EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id = v_id)
       OR EXISTS (SELECT 1 FROM operations.inventory_summary_outbox WHERE summary_id = v_id) THEN
        RAISE EXCEPTION 'component delete did not remove summary/outbox';
    END IF;

    RAISE NOTICE 'PASS: fallback insert, update, quantity merge, mapped name refresh and delete';
END;
$$;

ROLLBACK;
