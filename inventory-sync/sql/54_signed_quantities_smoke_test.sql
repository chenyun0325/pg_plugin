\set ON_ERROR_STOP on
\encoding UTF8

-- Use after SQL 25. Dynamic fixture keys avoid collisions with real data.
-- Every fixture and trigger side effect is rolled back.
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
    v_kind text;
    v_failed_constraint text;
BEGIN
    v_wh_id := 'sg-' || v_token;
    v_upc := 'cdc-signed-smoke-' || v_token;
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
    v_source_key := 'signed_smoke_' || v_token;
    INSERT INTO operations.inventory_warehouse_map(legacy_wh_id, warehouse_id)
    VALUES (v_wh_id, v_warehouse_id);

    -- Reproduce tracking -50 arriving before its zero-quantity anchor.
    INSERT INTO inventory_flink_stage.inventory_component
        (source_row_id, source_table, source_id, quantity_kind,
         upc, uid, wh_id, quantity, last_update_time)
    VALUES
        (v_source_key || '_HOLD', 'mem_user_inventory_tracking', v_id, 'HOLD',
         v_upc, 1324, v_wh_id, -50, 1700000000),
        (v_source_key || '_RESERVED', 'mem_user_inventory_lock', v_id, 'RESERVED',
         v_upc, 1324, v_wh_id, 0, 1700000001),
        (v_source_key || '_ON_HAND', 'mem_user_inventory', v_id, 'ON_HAND',
         v_upc, 1324, v_wh_id, 0, 1700000002);

    IF NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary
        WHERE id = v_id AND owner_id = 1324 AND ownership_status = 'CONFIRMED'
          AND on_hand_qty = 0 AND reserved_qty = 0 AND hold_qty = -50
          AND goods_name = v_upc AND goods_version = 0
    ) OR NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary_outbox
        WHERE summary_id = v_id AND owner_id = 1324
          AND on_hand_qty = 0 AND reserved_qty = 0 AND hold_qty = -50
    ) THEN
        RAISE EXCEPTION 'tracking -50 was not preserved in summary/outbox';
    END IF;

    FOREACH v_kind IN ARRAY ARRAY['ON_HAND','RESERVED','HOLD'] LOOP
        UPDATE inventory_flink_stage.inventory_component
           SET quantity = -2147483648, last_update_time = 1700000003
         WHERE source_row_id = v_source_key || '_' || v_kind;
    END LOOP;
    IF NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary
        WHERE id = v_id AND on_hand_qty = -2147483648
          AND reserved_qty = -2147483648 AND hold_qty = -2147483648
    ) OR NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary_outbox
        WHERE summary_id = v_id AND on_hand_qty = -2147483648
          AND reserved_qty = -2147483648 AND hold_qty = -2147483648
    ) THEN
        RAISE EXCEPTION 'signed int4 minimum was changed or rejected';
    END IF;

    BEGIN
        UPDATE operations.inventory_summary_outbox SET hold_qty = -1.5
        WHERE summary_id = v_id;
        RAISE EXCEPTION 'fractional outbox quantity was unexpectedly accepted';
    EXCEPTION WHEN check_violation THEN
        GET STACKED DIAGNOSTICS v_failed_constraint = CONSTRAINT_NAME;
        IF v_failed_constraint <> 'inventory_summary_outbox_hold_qty_check' THEN
            RAISE EXCEPTION 'unexpected outbox integer constraint: %', v_failed_constraint;
        END IF;
    END;

    UPDATE inventory_flink_stage.inventory_component
       SET quantity = CASE quantity_kind
               WHEN 'ON_HAND' THEN 10 WHEN 'RESERVED' THEN 3 ELSE 2 END,
           last_update_time = 1700000004
     WHERE source_row_id IN (v_source_key || '_ON_HAND',
                             v_source_key || '_RESERVED', v_source_key || '_HOLD');
    IF NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary
        WHERE id = v_id AND on_hand_qty = 10 AND reserved_qty = 3 AND hold_qty = 2
    ) OR NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary_outbox
        WHERE summary_id = v_id AND on_hand_qty = 10 AND reserved_qty = 3 AND hold_qty = 2
    ) THEN
        RAISE EXCEPTION 'positive quantities did not replace signed quantities on next event';
    END IF;

    DELETE FROM inventory_flink_stage.inventory_component
     WHERE source_row_id = v_source_key || '_HOLD';
    DELETE FROM inventory_flink_stage.inventory_component
     WHERE source_row_id = v_source_key || '_RESERVED';
    DELETE FROM inventory_flink_stage.inventory_component
     WHERE source_row_id = v_source_key || '_ON_HAND';
    IF EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id = v_id)
       OR EXISTS (SELECT 1 FROM operations.inventory_summary_outbox WHERE summary_id = v_id) THEN
        RAISE EXCEPTION 'anchor delete did not clean up summary/outbox';
    END IF;

    RAISE NOTICE 'PASS: hold=-50, all signed int4 minima, outbox integer restriction, later positive updates and delete';
END;
$$;

ROLLBACK;
