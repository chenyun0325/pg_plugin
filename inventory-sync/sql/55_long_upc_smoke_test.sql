\set ON_ERROR_STOP on
\encoding UTF8

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SET CONSTRAINTS ALL IMMEDIATE;

DO $$
DECLARE
    v_token text := substr(md5(clock_timestamp()::text || random()::text), 1, 12);
    v_wh_id varchar(16) := 'up-' || v_token;
    v_a text := repeat('K',64) || '-A-' || v_token;
    v_b text := repeat('K',64) || '-B-' || v_token;
    v_reported text := 'JF446J118821:3920RW-SW:RC1066:3026000188:NETFLIX:DISNEY+:APPLE TV+:PRIME VIDEO:1';
    v_max text := repeat('X',242) || '-' || v_token;
    v_unicode text := repeat(chr(20013),243) || v_token;
    v_id integer;
    v_warehouse_id bigint;
    v_attempt integer;
    v_item record;
BEGIN
    FOR v_attempt IN 1..100 LOOP
        v_id := 2000000000 + floor(random() * 100000000)::integer;
        v_warehouse_id := 10000000000::bigint + v_id;
        EXIT WHEN NOT EXISTS (SELECT 1 FROM inventory_stage.mem_user_inventory WHERE id BETWEEN v_id AND v_id + 4)
          AND NOT EXISTS (SELECT 1 FROM inventory_stage.mem_user_inventory_lock WHERE id = v_id)
          AND NOT EXISTS (SELECT 1 FROM inventory_stage.mem_user_inventory_tracking WHERE id = v_id)
          AND NOT EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id BETWEEN v_id AND v_id + 4)
          AND NOT EXISTS (SELECT 1 FROM operations.inventory_summary_outbox WHERE summary_id BETWEEN v_id AND v_id + 4)
          AND NOT EXISTS (SELECT 1 FROM inventory_flink_stage.inventory_component WHERE source_id BETWEEN v_id AND v_id + 4)
          AND NOT EXISTS (SELECT 1 FROM operations.inventory_warehouse_map WHERE warehouse_id = v_warehouse_id);
        IF v_attempt = 100 THEN RAISE EXCEPTION 'could not allocate unused test keys'; END IF;
    END LOOP;
    INSERT INTO operations.inventory_warehouse_map(legacy_wh_id,warehouse_id)
    VALUES (v_wh_id,v_warehouse_id);
    INSERT INTO operations.inventory_goods_map(upc,goods_name,goods_version)
    VALUES (v_b,'Long UPC mapped goods',7);

    FOR v_item IN SELECT * FROM (VALUES
        (0,v_a),(1,v_b),(2,v_reported),(3,v_max),(4,v_unicode)
    ) AS cases(offset_id,upc) LOOP
        INSERT INTO inventory_flink_stage.inventory_component
            (source_row_id,source_table,source_id,quantity_kind,upc,uid,wh_id,quantity,last_update_time)
        VALUES ('long_upc_' || v_token || '_' || v_item.offset_id,'mem_user_inventory',
                v_id + v_item.offset_id,'ON_HAND',v_item.upc,0,v_wh_id,10,1700000000);
        IF NOT EXISTS (
            SELECT 1 FROM operations.inventory_summary
            WHERE id = v_id + v_item.offset_id AND owner_id = 0
              AND upc = v_item.upc AND length(upc) = length(v_item.upc)
        ) OR NOT EXISTS (
            SELECT 1 FROM operations.inventory_summary_outbox
            WHERE summary_id = v_id + v_item.offset_id AND upc = v_item.upc
        ) THEN
            RAISE EXCEPTION 'UPC was truncated or failed to reach summary/outbox: case %',v_item.offset_id;
        END IF;
    END LOOP;
    IF NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary WHERE id = v_id
        AND goods_name = v_a AND goods_version = 0
    ) OR NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary WHERE id = v_id + 1
        AND goods_name = 'Long UPC mapped goods' AND goods_version = 7
    ) THEN RAISE EXCEPTION 'long UPC mapped/fallback goods names failed'; END IF;

    INSERT INTO inventory_flink_stage.inventory_component
        (source_row_id,source_table,source_id,quantity_kind,upc,uid,wh_id,quantity,last_update_time)
    VALUES
        ('long_upc_' || v_token || '_lock','mem_user_inventory_lock',v_id,'RESERVED',v_a,0,v_wh_id,3,1700000001),
        ('long_upc_' || v_token || '_hold','mem_user_inventory_tracking',v_id,'HOLD',v_a,0,v_wh_id,2,1700000002);
    UPDATE inventory_flink_stage.inventory_component SET quantity = 15
    WHERE source_row_id = 'long_upc_' || v_token || '_0';
    IF NOT EXISTS (
        SELECT 1 FROM operations.inventory_summary WHERE id = v_id AND upc = v_a
        AND on_hand_qty = 15 AND reserved_qty = 3 AND hold_qty = 2
    ) THEN RAISE EXCEPTION 'long UPC quantity merge/update failed'; END IF;

    BEGIN
        INSERT INTO operations.inventory_goods_map(upc,goods_name,goods_version)
        VALUES (v_max || 'Z','too long',0);
        RAISE EXCEPTION '256-character UPC was unexpectedly accepted';
    EXCEPTION WHEN string_data_right_truncation THEN NULL;
    END;
    DELETE FROM inventory_flink_stage.inventory_component
    WHERE source_row_id IN ('long_upc_' || v_token || '_lock','long_upc_' || v_token || '_hold');
    DELETE FROM inventory_flink_stage.inventory_component
    WHERE source_row_id IN ('long_upc_' || v_token || '_0','long_upc_' || v_token || '_1',
        'long_upc_' || v_token || '_2','long_upc_' || v_token || '_3','long_upc_' || v_token || '_4');
    IF EXISTS (SELECT 1 FROM operations.inventory_summary WHERE id BETWEEN v_id AND v_id + 4)
       OR EXISTS (SELECT 1 FROM operations.inventory_summary_outbox WHERE summary_id BETWEEN v_id AND v_id + 4) THEN
        RAISE EXCEPTION 'long UPC delete left summary/outbox rows';
    END IF;
    RAISE NOTICE 'PASS: reported 80-character UPC, same-64-prefix distinct keys, 255 ASCII/Unicode characters, mapping/fallback, update/delete and 256 rejection';
END;
$$;

ROLLBACK;
