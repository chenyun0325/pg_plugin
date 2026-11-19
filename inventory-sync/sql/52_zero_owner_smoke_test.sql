\set ON_ERROR_STOP on
\encoding UTF8

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
    v_failed_constraint text;
BEGIN
    v_wh_id := 'z0-' || v_token;
    v_upc := 'cdc-zero-owner-' || v_token;
    FOR v_attempt IN 1..100 LOOP
        v_id := 2000000000 + floor(random() * 100000000)::integer;
        v_warehouse_id := 10000000000::bigint + v_id;
        EXIT WHEN NOT EXISTS (SELECT 1 FROM inventory_stage.mem_user_inventory WHERE id BETWEEN v_id AND v_id + 3)
          AND NOT EXISTS (SELECT 1 FROM inventory_stage.mem_user_inventory_lock WHERE id BETWEEN v_id AND v_id + 3)
          AND NOT EXISTS (SELECT 1 FROM inventory_stage.mem_user_inventory_tracking WHERE id BETWEEN v_id AND v_id + 3)
          AND NOT EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id BETWEEN v_id AND v_id + 3)
          AND NOT EXISTS (SELECT 1 FROM operations.inventory_summary_outbox WHERE summary_id BETWEEN v_id AND v_id + 3)
          AND NOT EXISTS (SELECT 1 FROM inventory_flink_stage.inventory_component WHERE source_id BETWEEN v_id AND v_id + 3)
          AND NOT EXISTS (SELECT 1 FROM operations.inventory_warehouse_map WHERE warehouse_id = v_warehouse_id);
        IF v_attempt = 100 THEN
            RAISE EXCEPTION 'could not allocate unused smoke test keys';
        END IF;
    END LOOP;
    v_source_key := 'zero_owner_smoke_' || v_token;

    INSERT INTO operations.inventory_warehouse_map(legacy_wh_id, warehouse_id)
    VALUES (v_wh_id, v_warehouse_id);

    INSERT INTO inventory_flink_stage.inventory_component
        (source_row_id, source_table, source_id, quantity_kind,
         upc, uid, wh_id, quantity, last_update_time)
    VALUES (v_source_key || '_on_hand', 'mem_user_inventory', v_id, 'ON_HAND',
            v_upc, 0, v_wh_id, 0, 1700000000);

    IF NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary
        WHERE id = v_id AND owner_id = 0 AND ownership_status = 'CONFIRMED'
          AND on_hand_qty = 0 AND goods_name = v_upc AND goods_version = 0
    ) OR NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary_outbox
        WHERE summary_id = v_id AND owner_id = 0 AND on_hand_qty = 0
    ) THEN
        RAISE EXCEPTION 'uid=0 / zero quantity did not reach summary and outbox';
    END IF;

    UPDATE inventory_flink_stage.inventory_component
       SET quantity = 10, last_update_time = 1700000001
     WHERE source_row_id = v_source_key || '_on_hand';
    INSERT INTO inventory_flink_stage.inventory_component
        (source_row_id, source_table, source_id, quantity_kind,
         upc, uid, wh_id, quantity, last_update_time)
    VALUES
        (v_source_key || '_reserved', 'mem_user_inventory_lock', v_id, 'RESERVED',
         v_upc, 0, v_wh_id, 3, 1700000002),
        (v_source_key || '_hold', 'mem_user_inventory_tracking', v_id, 'HOLD',
         v_upc, 0, v_wh_id, 2, 1700000003);

    IF NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary
        WHERE id = v_id AND owner_id = 0 AND ownership_status = 'CONFIRMED'
          AND on_hand_qty = 10 AND reserved_qty = 3 AND hold_qty = 2
    ) OR NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary_outbox
        WHERE summary_id = v_id AND owner_id = 0
          AND on_hand_qty = 10 AND reserved_qty = 3 AND hold_qty = 2
    ) THEN
        RAISE EXCEPTION 'uid=0 quantity merge/update did not reach summary and outbox';
    END IF;

    BEGIN
        UPDATE inventory_flink_stage.inventory_component
           SET uid = -1
         WHERE source_row_id = v_source_key || '_on_hand';
        RAISE EXCEPTION 'negative uid was unexpectedly accepted';
    EXCEPTION WHEN check_violation THEN
        GET STACKED DIAGNOSTICS v_failed_constraint = CONSTRAINT_NAME;
        IF v_failed_constraint <> 'inventory_summary_owner_id_check' THEN
            RAISE EXCEPTION 'unexpected negative uid constraint: %', v_failed_constraint;
        END IF;
    END;

    BEGIN
        UPDATE operations.inventory_summary_outbox SET owner_id = -1
        WHERE summary_id = v_id;
        RAISE EXCEPTION 'negative outbox owner was unexpectedly accepted';
    EXCEPTION WHEN check_violation THEN
        GET STACKED DIAGNOSTICS v_failed_constraint = CONSTRAINT_NAME;
        IF v_failed_constraint <> 'inventory_summary_outbox_owner_id_check' THEN
            RAISE EXCEPTION 'unexpected outbox constraint: %', v_failed_constraint;
        END IF;
    END;

    BEGIN
        UPDATE operations.inventory_summary_outbox SET owner_id = 2147483648
        WHERE summary_id = v_id;
        RAISE EXCEPTION 'outbox owner beyond int4 was unexpectedly accepted';
    EXCEPTION WHEN check_violation THEN
        GET STACKED DIAGNOSTICS v_failed_constraint = CONSTRAINT_NAME;
        IF v_failed_constraint <> 'inventory_summary_outbox_owner_id_check' THEN
            RAISE EXCEPTION 'unexpected outbox upper-bound constraint: %', v_failed_constraint;
        END IF;
    END;

    INSERT INTO operations.inventory_summary
        (id, warehouse_id, upc, goods_name, goods_version, owner_id, on_hand_qty)
    VALUES
        (v_id + 1, v_warehouse_id, v_upc || '-tba', 'TBA smoke', 0, NULL, 1),
        (v_id + 2, v_warehouse_id, v_upc || '-positive', 'Positive owner smoke', 0, 1, 1);
    IF NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary
        WHERE id = v_id + 1 AND owner_id IS NULL AND ownership_status = 'TBA'
    ) OR EXISTS (
        SELECT 1 FROM operations.inventory_summary_outbox WHERE summary_id = v_id + 1
    ) OR NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary_outbox WHERE summary_id = v_id + 2 AND owner_id = 1
    ) THEN
        RAISE EXCEPTION 'NULL TBA or positive owner compatibility failed';
    END IF;

    DELETE FROM inventory_flink_stage.inventory_component
     WHERE source_row_id = v_source_key || '_reserved';
    DELETE FROM inventory_flink_stage.inventory_component
     WHERE source_row_id = v_source_key || '_hold';
    DELETE FROM inventory_flink_stage.inventory_component
     WHERE source_row_id = v_source_key || '_on_hand';
    IF EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id = v_id)
       OR EXISTS (SELECT 1 FROM operations.inventory_summary_outbox WHERE summary_id = v_id) THEN
        RAISE EXCEPTION 'uid=0 anchor delete did not remove summary/outbox';
    END IF;

    RAISE NOTICE 'PASS: uid=0 insert/update/merge/delete and outbox, negative/upper-bound rejection, NULL TBA and positive owner';
END;
$$;

ROLLBACK;
