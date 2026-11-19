\set ON_ERROR_STOP on
\encoding UTF8

BEGIN;

-- Optional direct Flink N:1 landing adapter. Install on the merged node after
-- 10_pg_merged.sql and 20_pg_merged_projection.sql. Do not run this Flink job
-- together with the pgcat mirror-to-summary forward job.

CREATE SCHEMA inventory_flink_stage;

CREATE TABLE inventory_flink_stage.inventory_component (
    source_row_id varchar(320) PRIMARY KEY,
    source_table varchar(64) NOT NULL,
    source_id integer NOT NULL,
    quantity_kind varchar(16) NOT NULL,
    upc varchar(255) NOT NULL,
    uid integer NOT NULL,
    wh_id varchar(16) NOT NULL,
    quantity integer NOT NULL,
    last_update_time integer NOT NULL,
    CONSTRAINT inventory_component_kind_check
        CHECK (quantity_kind IN ('ON_HAND', 'RESERVED', 'HOLD')),
    CONSTRAINT inventory_component_business_key
        UNIQUE (quantity_kind, uid, wh_id, upc)
);

CREATE TRIGGER normalize_inventory_component
BEFORE INSERT OR UPDATE ON inventory_flink_stage.inventory_component
FOR EACH ROW EXECUTE FUNCTION operations.normalize_inventory_stage_row();

CREATE OR REPLACE FUNCTION operations.route_inventory_component()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        CASE OLD.quantity_kind
            WHEN 'ON_HAND' THEN
                DELETE FROM inventory_stage.mem_user_inventory
                 WHERE id = OLD.source_id;
            WHEN 'RESERVED' THEN
                DELETE FROM inventory_stage.mem_user_inventory_lock
                 WHERE id = OLD.source_id;
            WHEN 'HOLD' THEN
                DELETE FROM inventory_stage.mem_user_inventory_tracking
                 WHERE id = OLD.source_id;
            ELSE
                RAISE EXCEPTION 'unsupported old quantity kind %', OLD.quantity_kind;
        END CASE;
    END IF;

    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        CASE NEW.quantity_kind
            WHEN 'ON_HAND' THEN
                INSERT INTO inventory_stage.mem_user_inventory
                    (id, upc, uid, wh_id, quantity, last_update_time)
                VALUES
                    (NEW.source_id, NEW.upc, NEW.uid, NEW.wh_id,
                     NEW.quantity, NEW.last_update_time)
                ON CONFLICT (id) DO UPDATE SET
                    upc = EXCLUDED.upc,
                    uid = EXCLUDED.uid,
                    wh_id = EXCLUDED.wh_id,
                    quantity = EXCLUDED.quantity,
                    last_update_time = EXCLUDED.last_update_time;
            WHEN 'RESERVED' THEN
                INSERT INTO inventory_stage.mem_user_inventory_lock
                    (id, upc, uid, wh_id, quantity, last_update_time)
                VALUES
                    (NEW.source_id, NEW.upc, NEW.uid, NEW.wh_id,
                     NEW.quantity, NEW.last_update_time)
                ON CONFLICT (id) DO UPDATE SET
                    upc = EXCLUDED.upc,
                    uid = EXCLUDED.uid,
                    wh_id = EXCLUDED.wh_id,
                    quantity = EXCLUDED.quantity,
                    last_update_time = EXCLUDED.last_update_time;
            WHEN 'HOLD' THEN
                INSERT INTO inventory_stage.mem_user_inventory_tracking
                    (id, upc, uid, wh_id, quantity, last_update_time)
                VALUES
                    (NEW.source_id, NEW.upc, NEW.uid, NEW.wh_id,
                     NEW.quantity, NEW.last_update_time)
                ON CONFLICT (id) DO UPDATE SET
                    upc = EXCLUDED.upc,
                    uid = EXCLUDED.uid,
                    wh_id = EXCLUDED.wh_id,
                    quantity = EXCLUDED.quantity,
                    last_update_time = EXCLUDED.last_update_time;
            ELSE
                RAISE EXCEPTION 'unsupported new quantity kind %', NEW.quantity_kind;
        END CASE;
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER route_inventory_component
AFTER INSERT OR UPDATE OR DELETE
ON inventory_flink_stage.inventory_component
FOR EACH ROW EXECUTE FUNCTION operations.route_inventory_component();

COMMIT;
