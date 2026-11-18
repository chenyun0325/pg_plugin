CREATE SCHEMA inventory_app;
CREATE SCHEMA inventory_projection;

CREATE TABLE inventory_app.summary (uid bigint PRIMARY KEY, stock_qty int NOT NULL, reserved_qty int NOT NULL, hold_qty int NOT NULL, available_qty int NOT NULL);
ALTER TABLE inventory_app.summary REPLICA IDENTITY FULL;
CREATE TABLE inventory_projection.parts (
  id int PRIMARY KEY, uid bigint NOT NULL, kind text NOT NULL, qty int NOT NULL,
  _sync_source text, _sync_table text, _sync_pk text,
  UNIQUE(uid,kind)
);
CREATE FUNCTION inventory_projection.refresh_summary() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected_uid bigint := CASE WHEN TG_OP='DELETE' THEN OLD.uid ELSE NEW.uid END;
BEGIN
  IF pg_trigger_depth()>1 THEN RETURN NULL; END IF;
  IF TG_OP='UPDATE' AND NEW.uid<>OLD.uid THEN
    RAISE EXCEPTION 'Lab forbids reassigning a component between UIDs';
  END IF;
  INSERT INTO inventory_app.summary
    SELECT uid, max(qty) FILTER (WHERE kind='stock'), max(qty) FILTER (WHERE kind='reserved'), max(qty) FILTER (WHERE kind='hold'), max(qty) FILTER (WHERE kind='available')
    FROM inventory_projection.parts WHERE uid=affected_uid
    GROUP BY uid HAVING count(*)=4
  ON CONFLICT(uid) DO UPDATE SET stock_qty=excluded.stock_qty, reserved_qty=excluded.reserved_qty, hold_qty=excluded.hold_qty, available_qty=excluded.available_qty;
  IF NOT FOUND THEN DELETE FROM inventory_app.summary WHERE uid=affected_uid; END IF;
  RETURN NULL;
END;
$$;
CREATE TRIGGER project_parts AFTER INSERT OR UPDATE OR DELETE ON inventory_projection.parts
  FOR EACH ROW EXECUTE FUNCTION inventory_projection.refresh_summary();

-- A native summary UPDATE also refreshes its four projection rows so that the next forward UPDATE can safely recompute it.
CREATE FUNCTION inventory_projection.align_parts() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF pg_trigger_depth()>1 THEN RETURN NULL; END IF;
  IF TG_OP<>'UPDATE' OR NEW.uid<>OLD.uid THEN
    RAISE EXCEPTION 'Lab supports native summary UPDATE for an existing UID only';
  END IF;
  UPDATE inventory_projection.parts SET qty=NEW.stock_qty WHERE uid=NEW.uid AND kind='stock';
  IF NOT FOUND THEN RAISE EXCEPTION 'Missing stock projection for UID %', NEW.uid; END IF;
  UPDATE inventory_projection.parts SET qty=NEW.reserved_qty WHERE uid=NEW.uid AND kind='reserved';
  IF NOT FOUND THEN RAISE EXCEPTION 'Missing reserved projection for UID %', NEW.uid; END IF;
  UPDATE inventory_projection.parts SET qty=NEW.hold_qty WHERE uid=NEW.uid AND kind='hold';
  IF NOT FOUND THEN RAISE EXCEPTION 'Missing hold projection for UID %', NEW.uid; END IF;
  UPDATE inventory_projection.parts SET qty=NEW.available_qty WHERE uid=NEW.uid AND kind='available';
  IF NOT FOUND THEN RAISE EXCEPTION 'Missing available projection for UID %', NEW.uid; END IF;
  RETURN NULL;
END;
$$;
INSERT INTO inventory_projection.parts(id,uid,kind,qty) VALUES
  (1,1,'stock',10),
  (11,2,'stock',20),
  (2,1,'reserved',11),
  (12,2,'reserved',21),
  (3,1,'hold',12),
  (13,2,'hold',22),
  (4,1,'available',13),
  (14,2,'available',23);
UPDATE inventory_projection.parts
SET _sync_source='inventory_mirror', _sync_table='inventory_legacy.'||kind, _sync_pk='['||id||']';
CREATE TRIGGER align_parts AFTER INSERT OR UPDATE OR DELETE ON inventory_app.summary
  FOR EACH ROW EXECUTE FUNCTION inventory_projection.align_parts();
INSERT INTO cdc_meta.write_ownership VALUES ('inventory',1,'MYSQL',1,'ACTIVE',NULL),('inventory',2,'MYSQL',1,'ACTIVE',NULL);
CREATE PUBLICATION inv_reverse FOR TABLE inventory_app.summary,cdc_meta.tx_context;
