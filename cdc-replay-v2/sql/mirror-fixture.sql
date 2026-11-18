CREATE SCHEMA inventory_legacy;
CREATE TABLE inventory_legacy.stock (id int PRIMARY KEY, uid bigint NOT NULL UNIQUE, qty int NOT NULL);
ALTER TABLE inventory_legacy.stock REPLICA IDENTITY FULL;
INSERT INTO inventory_legacy.stock VALUES (1,1,10),(11,2,20);
CREATE TABLE inventory_legacy.reserved (id int PRIMARY KEY, uid bigint NOT NULL UNIQUE, qty int NOT NULL);
ALTER TABLE inventory_legacy.reserved REPLICA IDENTITY FULL;
INSERT INTO inventory_legacy.reserved VALUES (2,1,11),(12,2,21);
CREATE TABLE inventory_legacy.hold (id int PRIMARY KEY, uid bigint NOT NULL UNIQUE, qty int NOT NULL);
ALTER TABLE inventory_legacy.hold REPLICA IDENTITY FULL;
INSERT INTO inventory_legacy.hold VALUES (3,1,12),(13,2,22);
CREATE TABLE inventory_legacy.available (id int PRIMARY KEY, uid bigint NOT NULL UNIQUE, qty int NOT NULL);
ALTER TABLE inventory_legacy.available REPLICA IDENTITY FULL;
INSERT INTO inventory_legacy.available VALUES (4,1,13),(14,2,23);
CREATE SCHEMA inventory_reverse;
CREATE TABLE inventory_reverse.summary_delta (
  uid bigint PRIMARY KEY,
  stock_qty int NOT NULL,
  reserved_qty int NOT NULL,
  hold_qty int NOT NULL,
  available_qty int NOT NULL,
  _sync_source text, _sync_table text, _sync_pk text
);
CREATE FUNCTION inventory_reverse.apply_delta() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN
    RAISE EXCEPTION 'Lab has no reverse delete mapping. Supply a business-approved policy before enabling DELETE.';
  END IF;
  UPDATE inventory_legacy.stock SET qty=NEW.stock_qty WHERE uid=NEW.uid;
  IF NOT FOUND THEN RAISE EXCEPTION 'Missing original stock identity for stock UID %', NEW.uid; END IF;
  UPDATE inventory_legacy.reserved SET qty=NEW.reserved_qty WHERE uid=NEW.uid;
  IF NOT FOUND THEN RAISE EXCEPTION 'Missing original stock identity for reserved UID %', NEW.uid; END IF;
  UPDATE inventory_legacy.hold SET qty=NEW.hold_qty WHERE uid=NEW.uid;
  IF NOT FOUND THEN RAISE EXCEPTION 'Missing original stock identity for hold UID %', NEW.uid; END IF;
  UPDATE inventory_legacy.available SET qty=NEW.available_qty WHERE uid=NEW.uid;
  IF NOT FOUND THEN RAISE EXCEPTION 'Missing original stock identity for available UID %', NEW.uid; END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER apply_delta AFTER INSERT OR UPDATE OR DELETE ON inventory_reverse.summary_delta
  FOR EACH ROW EXECUTE FUNCTION inventory_reverse.apply_delta();

INSERT INTO cdc_meta.write_ownership VALUES ('inventory',1,'MYSQL',1,'ACTIVE',NULL),('inventory',2,'MYSQL',1,'ACTIVE',NULL);
INSERT INTO cdc_meta.apply_progress(channel,source_stream_id) VALUES ('mysql_to_mirror','mysql-lab-v1');
CREATE PUBLICATION inv_forward FOR TABLE inventory_legacy.stock, inventory_legacy.reserved, inventory_legacy.hold, inventory_legacy.available,cdc_meta.tx_context;
CREATE PUBLICATION inv_mysql_sink FOR TABLE inventory_legacy.stock, inventory_legacy.reserved, inventory_legacy.hold, inventory_legacy.available,cdc_meta.tx_context;
