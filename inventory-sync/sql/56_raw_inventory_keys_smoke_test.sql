\set ON_ERROR_STOP on
\encoding UTF8
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SET CONSTRAINTS ALL IMMEDIATE;

DO $$
DECLARE
    token text := substr(md5(clock_timestamp()::text || random()::text),1,10);
    raw_wh varchar(16);
    base_upc varchar(255);
    padded_upc varchar(255);
    source_key text;
    first_id integer;
    warehouse bigint;
BEGIN
    raw_wh := ' wh-' || token || ' ';
    base_upc := 'upc-' || token;
    padded_upc := ' ' || base_upc || ' ';
    source_key := 'raw-key-smoke-' || token;
    LOOP
        first_id := 2000000000 + floor(random()*50000000)::integer;
        EXIT WHEN NOT EXISTS (SELECT 1 FROM inventory_flink_stage.inventory_component WHERE source_id IN (first_id,first_id+1))
          AND NOT EXISTS (SELECT 1 FROM inventory_stage.mem_user_inventory WHERE id IN (first_id,first_id+1))
          AND NOT EXISTS (SELECT 1 FROM inventory_stage.mem_user_inventory_lock WHERE id IN (first_id,first_id+1))
          AND NOT EXISTS (SELECT 1 FROM inventory_stage.mem_user_inventory_tracking WHERE id IN (first_id,first_id+1))
          AND NOT EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id IN (first_id,first_id+1))
          AND NOT EXISTS (SELECT 1 FROM operations.inventory_summary_outbox WHERE summary_id IN (first_id,first_id+1));
    END LOOP;
    warehouse := 20000000000::bigint + first_id;
    INSERT INTO operations.inventory_warehouse_map VALUES (raw_wh,warehouse);
    INSERT INTO operations.inventory_goods_map VALUES (padded_upc,'Raw UPC goods',0);

    INSERT INTO inventory_flink_stage.inventory_component
        (source_row_id,source_table,source_id,quantity_kind,upc,uid,wh_id,quantity,last_update_time)
    VALUES
        (source_key||'-i1','mem_user_inventory',first_id,'ON_HAND',padded_upc,0,raw_wh,0,1700000000),
        (source_key||'-i2','mem_user_inventory',first_id+1,'ON_HAND',base_upc,0,raw_wh,5,1700000000),
        (source_key||'-l1','mem_user_inventory_lock',first_id,'RESERVED',padded_upc,0,raw_wh,0,1700000000),
        (source_key||'-l2','mem_user_inventory_lock',first_id+1,'RESERVED',base_upc,0,raw_wh,2,1700000000),
        (source_key||'-t1','mem_user_inventory_tracking',first_id,'HOLD',padded_upc,0,raw_wh,-3,1700000000),
        (source_key||'-t2','mem_user_inventory_tracking',first_id+1,'HOLD',base_upc,0,raw_wh,7,1700000000)
    ON CONFLICT (source_row_id) DO UPDATE SET upc=EXCLUDED.upc,quantity=EXCLUDED.quantity;

    IF NOT EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id=first_id
        AND upc=padded_upc AND owner_id=0 AND on_hand_qty=0 AND reserved_qty=0
        AND hold_qty=-3 AND goods_name='Raw UPC goods')
       OR NOT EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id=first_id+1
        AND upc=base_upc AND owner_id=0 AND on_hand_qty=5 AND reserved_qty=2
        AND hold_qty=7 AND goods_name=base_upc) THEN
        RAISE EXCEPTION 'Raw and unpadded source keys were collapsed or quantities changed';
    END IF;
    IF (SELECT count(*) FROM operations.inventory_summary_outbox
        WHERE summary_id IN (first_id,first_id+1) AND legacy_wh_id=raw_wh) <> 2 THEN
        RAISE EXCEPTION 'Outbox failed to preserve raw keys';
    END IF;

    INSERT INTO inventory_flink_stage.inventory_component
        (source_row_id,source_table,source_id,quantity_kind,upc,uid,wh_id,quantity,last_update_time)
    VALUES (source_key||'-i1','mem_user_inventory',first_id,'ON_HAND',padded_upc,0,raw_wh,0,1700000000)
    ON CONFLICT (source_row_id) DO UPDATE SET upc=EXCLUDED.upc,quantity=EXCLUDED.quantity;
    UPDATE inventory_flink_stage.inventory_component SET quantity=-4
    WHERE source_row_id=source_key||'-t1';
    DELETE FROM inventory_flink_stage.inventory_component WHERE source_row_id=source_key||'-t2';
    IF NOT EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id=first_id AND hold_qty=-4)
       OR NOT EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id=first_id+1 AND hold_qty=0) THEN
        RAISE EXCEPTION 'Raw-key replay/update/delete affected the wrong summary';
    END IF;
    DELETE FROM inventory_flink_stage.inventory_component WHERE source_row_id=source_key||'-i2';
    IF EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id=first_id+1)
       OR EXISTS (SELECT 1 FROM operations.inventory_summary_outbox WHERE summary_id=first_id+1)
       OR NOT EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id=first_id AND upc=padded_upc) THEN
        RAISE EXCEPTION 'Deleting one variant removed the wrong identity';
    END IF;
    RAISE NOTICE 'PASS: raw UPC/warehouse spaces, distinct variants, replay, signed quantities, zero owner, goods lookup, outbox, updates and deletes';
END;
$$;
ROLLBACK;
