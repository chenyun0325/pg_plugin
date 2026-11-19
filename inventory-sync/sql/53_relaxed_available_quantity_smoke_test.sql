\set ON_ERROR_STOP on
\encoding UTF8

-- For SQL 23 mode, which retains nonnegative quantity checks. After applying
-- SQL 25, use 54_signed_quantities_smoke_test.sql instead.
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
    v_wh_id := 'av-' || v_token;
    v_upc := 'cdc-available-smoke-' || v_token;
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
    v_source_key := 'available_smoke_' || v_token;
    INSERT INTO operations.inventory_warehouse_map(legacy_wh_id, warehouse_id)
    VALUES (v_wh_id, v_warehouse_id);

    -- Reproduce the real arrival order: hold/reserved before inventory anchor.
    INSERT INTO inventory_flink_stage.inventory_component
        (source_row_id, source_table, source_id, quantity_kind,
         upc, uid, wh_id, quantity, last_update_time)
    VALUES
        (v_source_key || '_HOLD', 'mem_user_inventory_tracking', v_id, 'HOLD',
         v_upc, 215, v_wh_id, 24, 1700000000),
        (v_source_key || '_RESERVED', 'mem_user_inventory_lock', v_id, 'RESERVED',
         v_upc, 215, v_wh_id, 0, 1700000001);
    IF EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id = v_id) THEN
        RAISE EXCEPTION 'orphan components unexpectedly created a summary';
    END IF;
    INSERT INTO inventory_flink_stage.inventory_component
        (source_row_id, source_table, source_id, quantity_kind,
         upc, uid, wh_id, quantity, last_update_time)
    VALUES (v_source_key || '_ON_HAND', 'mem_user_inventory', v_id, 'ON_HAND',
            v_upc, 215, v_wh_id, 0, 1700000002);

    IF NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary
        WHERE id = v_id AND on_hand_qty = 0 AND reserved_qty = 0 AND hold_qty = 24
          AND on_hand_qty - reserved_qty - hold_qty = -24
          AND goods_name = v_upc AND goods_version = 0
    ) OR NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary_outbox
        WHERE summary_id = v_id AND on_hand_qty = 0 AND reserved_qty = 0 AND hold_qty = 24
    ) THEN
        RAISE EXCEPTION 'source quantities 0/0/24 were not retained in summary/outbox';
    END IF;

    FOREACH v_kind IN ARRAY ARRAY['ON_HAND','RESERVED','HOLD'] LOOP
        BEGIN
            UPDATE inventory_flink_stage.inventory_component
               SET quantity = -1
             WHERE source_row_id = v_source_key || '_' || v_kind;
            RAISE EXCEPTION 'negative % quantity was unexpectedly accepted', v_kind;
        EXCEPTION WHEN raise_exception THEN
            IF SQLERRM NOT LIKE 'negative quantity for wh_id=%' THEN
                RAISE;
            END IF;
        END;
    END LOOP;

    BEGIN
        UPDATE operations.inventory_summary SET hold_qty = -1 WHERE id = v_id;
        RAISE EXCEPTION 'direct negative summary quantity was unexpectedly accepted';
    EXCEPTION WHEN check_violation THEN
        GET STACKED DIAGNOSTICS v_failed_constraint = CONSTRAINT_NAME;
        IF v_failed_constraint <> 'inventory_summary_hold_qty_check' THEN
            RAISE EXCEPTION 'unexpected negative quantity constraint: %', v_failed_constraint;
        END IF;
    END;

    UPDATE inventory_flink_stage.inventory_component
       SET quantity = 30, last_update_time = 1700000003
     WHERE source_row_id = v_source_key || '_ON_HAND';
    IF NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary
        WHERE id = v_id AND on_hand_qty = 30 AND hold_qty = 24
          AND on_hand_qty - reserved_qty - hold_qty = 6
    ) OR NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary_outbox
        WHERE summary_id = v_id AND on_hand_qty = 30 AND hold_qty = 24
    ) THEN
        RAISE EXCEPTION 'later source update did not reconcile summary/outbox';
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

    RAISE NOTICE 'PASS: 0/0/24 source quantities and outbox, negative quantity rejection, later reconciliation and delete';
END;
$$;

ROLLBACK;
