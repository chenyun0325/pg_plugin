\set ON_ERROR_STOP on
\encoding UTF8

-- Safe integration smoke test for the merged node: all changes are rolled back.
BEGIN;

INSERT INTO operations.inventory_warehouse_map(legacy_wh_id, warehouse_id)
VALUES ('smoke-wh', 990001);

INSERT INTO operations.inventory_goods_map(upc, goods_name, goods_version)
VALUES ('smoke-upc', 'Smoke test goods', 1);

INSERT INTO inventory_stage.mem_user_inventory
    (id, upc, uid, wh_id, quantity, last_update_time)
VALUES
    (990001, 'smoke-upc', 990001, 'smoke-wh', 10, 1700000000);

INSERT INTO inventory_stage.mem_user_inventory_lock
    (id, upc, uid, wh_id, quantity, last_update_time)
VALUES
    (990002, 'smoke-upc', 990001, 'smoke-wh', 3, 1700000001);

INSERT INTO inventory_stage.mem_user_inventory_tracking
    (id, upc, uid, wh_id, quantity, last_update_time)
VALUES
    (990003, 'smoke-upc', 990001, 'smoke-wh', 2, 1700000002);

SET CONSTRAINTS ALL IMMEDIATE;

DO $$
DECLARE
    v_summary operations.inventory_summary%ROWTYPE;
    v_outbox operations.inventory_summary_outbox%ROWTYPE;
BEGIN
    SELECT * INTO STRICT v_summary
      FROM operations.inventory_summary
     WHERE warehouse_id = 990001
       AND upc = 'smoke-upc'
       AND owner_id = 990001;
    IF (v_summary.on_hand_qty, v_summary.reserved_qty, v_summary.hold_qty)
       IS DISTINCT FROM (10::numeric, 3::numeric, 2::numeric) THEN
        RAISE EXCEPTION 'unexpected summary quantities: %, %, %',
            v_summary.on_hand_qty, v_summary.reserved_qty, v_summary.hold_qty;
    END IF;

    SELECT * INTO STRICT v_outbox
      FROM operations.inventory_summary_outbox
     WHERE warehouse_id = 990001
       AND upc = 'smoke-upc'
       AND owner_id = 990001;
    IF (v_outbox.on_hand_qty, v_outbox.reserved_qty, v_outbox.hold_qty)
       IS DISTINCT FROM (10::numeric, 3::numeric, 2::numeric) THEN
        RAISE EXCEPTION 'unexpected outbox quantities';
    END IF;
END;
$$;

UPDATE inventory_stage.mem_user_inventory_lock
   SET quantity = 4,
       last_update_time = 1700000003
 WHERE id = 990002;

DO $$
BEGIN
    IF (SELECT reserved_qty
          FROM operations.inventory_summary
         WHERE warehouse_id = 990001
           AND upc = 'smoke-upc'
           AND owner_id = 990001) <> 4 THEN
        RAISE EXCEPTION 'lock update was not projected';
    END IF;
END;
$$;

DELETE FROM inventory_stage.mem_user_inventory_lock WHERE id = 990002;
DELETE FROM inventory_stage.mem_user_inventory_tracking WHERE id = 990003;
DELETE FROM inventory_stage.mem_user_inventory WHERE id = 990001;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM operations.inventory_summary
         WHERE warehouse_id = 990001
           AND upc = 'smoke-upc'
           AND owner_id = 990001
    ) THEN
        RAISE EXCEPTION 'anchor delete did not remove summary';
    END IF;
END;
$$;

-- Missing product master data must not block inventory CDC. The summary keeps
-- a deterministic UPC/zero-version fallback without inserting a fake map row.
INSERT INTO inventory_stage.mem_user_inventory
    (id, upc, uid, wh_id, quantity, last_update_time)
VALUES
    (990004, 'smoke-unmapped-upc', 990002, 'smoke-wh', 1, 1700000004);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM operations.inventory_summary
         WHERE id = 990004
           AND warehouse_id = 990001
           AND upc = 'smoke-unmapped-upc'
           AND owner_id = 990002
           AND goods_name = 'smoke-unmapped-upc'
           AND goods_version = 0
           AND on_hand_qty = 1
    ) THEN
        RAISE EXCEPTION 'missing goods mapping fallback was not projected';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM operations.inventory_goods_map
         WHERE upc = 'smoke-unmapped-upc'
    ) THEN
        RAISE EXCEPTION 'fallback must not create a fake goods mapping';
    END IF;
END;
$$;

DELETE FROM inventory_stage.mem_user_inventory WHERE id = 990004;

-- Exercise the optional direct Flink N:1 component adapter as well.
INSERT INTO inventory_flink_stage.inventory_component
    (source_row_id, source_table, source_id, quantity_kind,
     upc, uid, wh_id, quantity, last_update_time)
VALUES
    ('mem_user_inventory_990001', 'mem_user_inventory', 990001, 'ON_HAND',
     'smoke-upc', 990001, 'smoke-wh', 10, 1700000000),
    ('mem_user_inventory_lock_990002', 'mem_user_inventory_lock', 990002, 'RESERVED',
     'smoke-upc', 990001, 'smoke-wh', 3, 1700000001),
    ('mem_user_inventory_tracking_990003', 'mem_user_inventory_tracking', 990003, 'HOLD',
     'smoke-upc', 990001, 'smoke-wh', 2, 1700000002);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM operations.inventory_summary
         WHERE warehouse_id = 990001
           AND upc = 'smoke-upc'
           AND owner_id = 990001
           AND on_hand_qty = 10
           AND reserved_qty = 3
           AND hold_qty = 2
    ) THEN
        RAISE EXCEPTION 'direct Flink N:1 adapter did not project quantities';
    END IF;
END;
$$;

DELETE FROM inventory_flink_stage.inventory_component
 WHERE source_row_id = 'mem_user_inventory_lock_990002';
DELETE FROM inventory_flink_stage.inventory_component
 WHERE source_row_id = 'mem_user_inventory_tracking_990003';
DELETE FROM inventory_flink_stage.inventory_component
 WHERE source_row_id = 'mem_user_inventory_990001';

ROLLBACK;
