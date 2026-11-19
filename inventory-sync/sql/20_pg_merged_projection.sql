\set ON_ERROR_STOP on
\encoding UTF8

BEGIN;

CREATE OR REPLACE FUNCTION operations.normalize_inventory_stage_row()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- Preserve source business keys byte for byte; whitespace can distinguish rows.
    -- MySQL permits an empty UPC; keep it as the same non-null business key.
    IF NEW.wh_id = '' THEN
        RAISE EXCEPTION 'blank wh_id is not allowed';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER normalize_inventory_stage
BEFORE INSERT OR UPDATE ON inventory_stage.mem_user_inventory
FOR EACH ROW EXECUTE FUNCTION operations.normalize_inventory_stage_row();

CREATE TRIGGER normalize_inventory_lock_stage
BEFORE INSERT OR UPDATE ON inventory_stage.mem_user_inventory_lock
FOR EACH ROW EXECUTE FUNCTION operations.normalize_inventory_stage_row();

CREATE TRIGGER normalize_inventory_tracking_stage
BEFORE INSERT OR UPDATE ON inventory_stage.mem_user_inventory_tracking
FOR EACH ROW EXECUTE FUNCTION operations.normalize_inventory_stage_row();

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

    -- mem_user_inventory is the aggregate anchor and supplies the stable target ID.
    IF NOT FOUND THEN
        -- A lock/hold snapshot chunk may arrive before its inventory anchor.
        -- Leave no summary row; arrival of the anchor will recompute all fields.
        -- Persistent orphans are reported by 40_preflight.sql/operational checks.
        DELETE FROM operations.inventory_summary
         WHERE warehouse_id = v_warehouse_id
           AND upc = v_upc
           AND owner_id = p_uid;
        RETURN;
    END IF;

    IF length(v_upc) > 255 THEN
        RAISE EXCEPTION 'UPC exceeds operations.inventory_summary limit: %', v_upc;
    END IF;

    SELECT goods_name, goods_version
      INTO v_goods_name, v_goods_version
      FROM operations.inventory_goods_map
     WHERE upc = v_upc;
    IF NOT FOUND THEN
        -- Inventory CDC must not stop when product master data lags behind.
        -- Keep the reference table authoritative and use a deterministic value
        -- only in the summary row until a later inventory event recomputes it.
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
        v_upc,
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

CREATE OR REPLACE FUNCTION operations.project_inventory_stage_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        PERFORM operations.refresh_inventory_summary(OLD.wh_id, OLD.upc, OLD.uid);
    END IF;
    IF TG_OP IN ('INSERT', 'UPDATE') AND
       (TG_OP = 'INSERT' OR (OLD.wh_id, OLD.upc, OLD.uid) IS DISTINCT FROM
                            (NEW.wh_id, NEW.upc, NEW.uid)) THEN
        PERFORM operations.refresh_inventory_summary(NEW.wh_id, NEW.upc, NEW.uid);
    END IF;
    RETURN NULL;
END;
$$;

-- Deferred execution is essential: snapshot rows and multi-table changes are
-- evaluated from their final state at target transaction commit.
CREATE CONSTRAINT TRIGGER project_inventory_stage
AFTER INSERT OR UPDATE OR DELETE ON inventory_stage.mem_user_inventory
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION operations.project_inventory_stage_change();

CREATE CONSTRAINT TRIGGER project_inventory_lock_stage
AFTER INSERT OR UPDATE OR DELETE ON inventory_stage.mem_user_inventory_lock
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION operations.project_inventory_stage_change();

CREATE CONSTRAINT TRIGGER project_inventory_tracking_stage
AFTER INSERT OR UPDATE OR DELETE ON inventory_stage.mem_user_inventory_tracking
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION operations.project_inventory_stage_change();

CREATE OR REPLACE FUNCTION operations.sync_inventory_summary_outbox()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_legacy_wh_id varchar(16);
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') AND OLD.owner_id IS NOT NULL THEN
        DELETE FROM operations.inventory_summary_outbox
         WHERE warehouse_id = OLD.warehouse_id
           AND upc = OLD.upc
           AND owner_id = OLD.owner_id;
    END IF;

    -- TBA rows have no legacy uid and therefore cannot be represented by the
    -- three legacy MySQL-shaped tables.
    IF TG_OP = 'DELETE' OR NEW.owner_id IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT legacy_wh_id
      INTO v_legacy_wh_id
      FROM operations.inventory_warehouse_map
     WHERE warehouse_id = NEW.warehouse_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'missing reverse warehouse mapping for warehouse_id=%',
            NEW.warehouse_id;
    END IF;

    INSERT INTO operations.inventory_summary_outbox (
        summary_id, warehouse_id, legacy_wh_id, upc, owner_id,
        on_hand_qty, reserved_qty, hold_qty, last_update_time
    ) VALUES (
        NEW.id, NEW.warehouse_id, v_legacy_wh_id, NEW.upc, NEW.owner_id,
        NEW.on_hand_qty, NEW.reserved_qty, NEW.hold_qty,
        floor(extract(epoch FROM NEW.updated_at))::integer
    )
    ON CONFLICT (warehouse_id, upc, owner_id) DO UPDATE SET
        summary_id = EXCLUDED.summary_id,
        legacy_wh_id = EXCLUDED.legacy_wh_id,
        on_hand_qty = EXCLUDED.on_hand_qty,
        reserved_qty = EXCLUDED.reserved_qty,
        hold_qty = EXCLUDED.hold_qty,
        last_update_time = EXCLUDED.last_update_time;
    RETURN NULL;
END;
$$;

CREATE TRIGGER sync_inventory_summary_outbox
AFTER INSERT OR UPDATE OR DELETE ON operations.inventory_summary
FOR EACH ROW EXECUTE FUNCTION operations.sync_inventory_summary_outbox();

CREATE PUBLICATION inventory_summary_pub
    FOR TABLE operations.inventory_summary_outbox;

COMMIT;
