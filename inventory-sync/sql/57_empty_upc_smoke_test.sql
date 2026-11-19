\set ON_ERROR_STOP on
\encoding UTF8

-- Test fixtures and their projection/outbox effects are always rolled back.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SET CONSTRAINTS ALL IMMEDIATE;

DO $$
DECLARE
    token text := substr(md5(clock_timestamp()::text || random()::text),1,10);
    wh varchar(16);
    next_id integer;
    warehouse bigint;
    prefix text;
    rejected boolean;
BEGIN
    wh := 'emp-' || token;
    prefix := 'empty-upc-smoke-' || token;
    LOOP
        next_id := 2050000000 + floor(random()*50000000)::integer;
        EXIT WHEN NOT EXISTS (SELECT 1 FROM inventory_flink_stage.inventory_component WHERE source_id=next_id)
          AND NOT EXISTS (SELECT 1 FROM inventory_stage.mem_user_inventory WHERE id=next_id)
          AND NOT EXISTS (SELECT 1 FROM inventory_stage.mem_user_inventory_lock WHERE id=next_id)
          AND NOT EXISTS (SELECT 1 FROM inventory_stage.mem_user_inventory_tracking WHERE id=next_id)
          AND NOT EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id=next_id)
          AND NOT EXISTS (SELECT 1 FROM operations.inventory_summary_outbox WHERE summary_id=next_id);
    END LOOP;
    warehouse := 30000000000::bigint + next_id;
    INSERT INTO operations.inventory_warehouse_map VALUES (wh,warehouse);

    -- Lock/hold can arrive before the inventory anchor, including an empty UPC.
    INSERT INTO inventory_flink_stage.inventory_component
        (source_row_id,source_table,source_id,quantity_kind,upc,uid,wh_id,quantity,last_update_time)
    VALUES
        (prefix||'-l','mem_user_inventory_lock',next_id,'RESERVED','',0,wh,0,1700000000),
        (prefix||'-t','mem_user_inventory_tracking',next_id,'HOLD','',0,wh,0,1700000000),
        (prefix||'-i','mem_user_inventory',next_id,'ON_HAND','',0,wh,0,1700000000)
    ON CONFLICT(source_row_id) DO UPDATE SET upc=EXCLUDED.upc,quantity=EXCLUDED.quantity;

    IF NOT EXISTS (SELECT 1 FROM operations.inventory_summary
        WHERE id=next_id AND warehouse_id=warehouse AND upc='' AND owner_id=0
          AND goods_name='' AND goods_version=0 AND on_hand_qty=0
          AND reserved_qty=0 AND hold_qty=0)
       OR NOT EXISTS (SELECT 1 FROM operations.inventory_summary_outbox
        WHERE summary_id=next_id AND legacy_wh_id=wh AND upc='' AND owner_id=0) THEN
        RAISE EXCEPTION 'Empty UPC did not survive stage/summary/outbox projection';
    END IF;

    INSERT INTO inventory_flink_stage.inventory_component
        (source_row_id,source_table,source_id,quantity_kind,upc,uid,wh_id,quantity,last_update_time)
    VALUES (prefix||'-i','mem_user_inventory',next_id,'ON_HAND','',0,wh,0,1700000000)
    ON CONFLICT(source_row_id) DO UPDATE SET upc=EXCLUDED.upc,quantity=EXCLUDED.quantity;
    UPDATE inventory_flink_stage.inventory_component SET quantity=-3
    WHERE source_row_id=prefix||'-t';
    IF NOT EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id=next_id AND upc='' AND hold_qty=-3)
       OR NOT EXISTS (SELECT 1 FROM operations.inventory_summary_outbox WHERE summary_id=next_id AND upc='' AND hold_qty=-3) THEN
        RAISE EXCEPTION 'Empty UPC replay or signed quantity update failed';
    END IF;

    -- NOT NULL and warehouse validation continue to reject unsupported values.
    rejected := false;
    BEGIN
        UPDATE inventory_flink_stage.inventory_component SET upc=NULL WHERE source_row_id=prefix||'-i';
    EXCEPTION WHEN not_null_violation THEN rejected := true;
    END;
    IF NOT rejected THEN RAISE EXCEPTION 'NULL UPC was unexpectedly accepted'; END IF;
    rejected := false;
    BEGIN
        UPDATE inventory_flink_stage.inventory_component SET wh_id='' WHERE source_row_id=prefix||'-i';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM <> 'blank wh_id is not allowed' THEN RAISE; END IF;
        rejected := true;
    END;
    IF NOT rejected THEN RAISE EXCEPTION 'Empty warehouse ID was unexpectedly accepted'; END IF;

    DELETE FROM inventory_flink_stage.inventory_component WHERE source_row_id=prefix||'-i';
    IF EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id=next_id)
       OR EXISTS (SELECT 1 FROM operations.inventory_summary_outbox WHERE summary_id=next_id) THEN
        RAISE EXCEPTION 'Empty UPC anchor delete failed';
    END IF;
    RAISE NOTICE 'PASS: empty UPC across all three components, zero owner, replay, signed update, outbox and delete; NULL UPC/empty warehouse remain rejected';
END;
$$;
ROLLBACK;
