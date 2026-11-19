\set ON_ERROR_STOP on
\encoding UTF8

BEGIN;

-- Existing merged databases already have the projection triggers installed.
-- Replace only the projection function so missing product master rows no longer
-- stop inventory CDC. Fresh installs receive the same definition from
-- 20_pg_merged_projection.sql.
CREATE OR REPLACE FUNCTION operations.refresh_inventory_summary(
    p_wh_id varchar,
    p_upc varchar,
    p_uid integer
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_wh_id varchar(16) := p_wh_id;
    v_upc varchar(255) := p_upc;
    v_warehouse_id bigint;
    v_inventory_id integer;
    v_on_hand integer;
    v_reserved integer := 0;
    v_hold integer := 0;
    v_last_update integer;
    v_component_update integer;
    v_goods_name varchar(256);
    v_goods_version integer;
BEGIN
    SELECT warehouse_id
      INTO v_warehouse_id
      FROM operations.inventory_warehouse_map
     WHERE legacy_wh_id = v_wh_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'missing warehouse mapping for legacy wh_id=%', v_wh_id;
    END IF;

    SELECT id, quantity, last_update_time
      INTO v_inventory_id, v_on_hand, v_last_update
      FROM inventory_stage.mem_user_inventory
     WHERE uid = p_uid AND wh_id = v_wh_id AND upc = v_upc;

    IF NOT FOUND THEN
        DELETE FROM operations.inventory_summary
         WHERE warehouse_id = v_warehouse_id
           AND upc = v_upc
           AND owner_id = p_uid;
        RETURN;
    END IF;

    IF length(v_upc) > 64 THEN
        RAISE EXCEPTION 'UPC exceeds operations.inventory_summary limit: %', v_upc;
    END IF;

    SELECT goods_name, goods_version
      INTO v_goods_name, v_goods_version
      FROM operations.inventory_goods_map
     WHERE upc = v_upc;
    IF NOT FOUND THEN
        v_goods_name := v_upc::varchar(256);
        v_goods_version := 0;
    END IF;

    SELECT quantity, last_update_time
      INTO v_reserved, v_component_update
      FROM inventory_stage.mem_user_inventory_lock
     WHERE uid = p_uid AND wh_id = v_wh_id AND upc = v_upc;
    IF NOT FOUND THEN
        v_reserved := 0;
    ELSE
        v_last_update := GREATEST(v_last_update, v_component_update);
    END IF;

    SELECT quantity, last_update_time
      INTO v_hold, v_component_update
      FROM inventory_stage.mem_user_inventory_tracking
     WHERE uid = p_uid AND wh_id = v_wh_id AND upc = v_upc;
    IF NOT FOUND THEN
        v_hold := 0;
    ELSE
        v_last_update := GREATEST(v_last_update, v_component_update);
    END IF;

    IF v_on_hand < 0 OR v_reserved < 0 OR v_hold < 0 THEN
        RAISE EXCEPTION 'negative quantity for wh_id=%, upc=%, uid=%',
            v_wh_id, v_upc, p_uid;
    END IF;
    IF v_on_hand < v_reserved + v_hold THEN
        RAISE EXCEPTION
            'on_hand(%) < reserved(%) + hold(%) for wh_id=%, upc=%, uid=%',
            v_on_hand, v_reserved, v_hold, v_wh_id, v_upc, p_uid;
    END IF;

    INSERT INTO operations.inventory_summary (
        id, warehouse_id, upc, goods_name, goods_version, owner_id,
        on_hand_qty, reserved_qty, hold_qty, updated_at
    ) VALUES (
        v_inventory_id::bigint,
        v_warehouse_id,
        v_upc::varchar(64),
        v_goods_name,
        v_goods_version,
        p_uid::bigint,
        v_on_hand,
        v_reserved,
        v_hold,
        CASE
            WHEN v_last_update > 0 THEN to_timestamp(v_last_update::double precision)
            ELSE clock_timestamp()
        END
    )
    ON CONFLICT (warehouse_id, upc, owner_id) DO UPDATE SET
        goods_name = EXCLUDED.goods_name,
        goods_version = EXCLUDED.goods_version,
        on_hand_qty = EXCLUDED.on_hand_qty,
        reserved_qty = EXCLUDED.reserved_qty,
        hold_qty = EXCLUDED.hold_qty,
        updated_at = EXCLUDED.updated_at,
        version_id = operations.inventory_summary.version_id + 1;
END;
$$;

COMMIT;
