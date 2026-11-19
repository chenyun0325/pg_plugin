\set ON_ERROR_STOP on
\encoding UTF8

BEGIN;

-- Run on the PostgreSQL node that receives the three MySQL tables from Flink CDC.
-- The _sync_* columns are reserved for pgcat-yaml. The Flink PostgreSQL sink must
-- write explicit source columns and must not rebuild these pre-created tables.

CREATE SCHEMA inventory_mirror;

CREATE TABLE inventory_mirror.mem_user_inventory (
    id               integer      NOT NULL,
    upc              varchar(255) NOT NULL,
    uid              integer      NOT NULL,
    wh_id            varchar(16)  NOT NULL,
    quantity         integer      NOT NULL DEFAULT 0,
    last_update_time integer      NOT NULL,
    _sync_source     text,
    _sync_table      text,
    _sync_pk         text,
    CONSTRAINT mem_user_inventory_pkey PRIMARY KEY (id),
    CONSTRAINT mem_user_inventory_ukey UNIQUE (uid, wh_id, upc)
);

CREATE INDEX mem_user_inventory_uid_last_update_idx
    ON inventory_mirror.mem_user_inventory (uid, last_update_time);
CREATE INDEX mem_user_inventory_wh_id_id_idx
    ON inventory_mirror.mem_user_inventory (wh_id, id);

CREATE TABLE inventory_mirror.mem_user_inventory_lock (
    id               integer      NOT NULL,
    upc              varchar(255) NOT NULL,
    uid              integer      NOT NULL,
    wh_id            varchar(16)  NOT NULL,
    quantity         integer      NOT NULL DEFAULT 0,
    last_update_time integer      NOT NULL,
    _sync_source     text,
    _sync_table      text,
    _sync_pk         text,
    CONSTRAINT mem_user_inventory_lock_pkey PRIMARY KEY (id),
    CONSTRAINT mem_user_inventory_lock_ukey UNIQUE (upc, uid, wh_id)
);

CREATE INDEX mem_user_inventory_lock_uid_last_update_idx
    ON inventory_mirror.mem_user_inventory_lock (uid, last_update_time);
CREATE INDEX mem_user_inventory_lock_wh_id_id_idx
    ON inventory_mirror.mem_user_inventory_lock (wh_id, id);

CREATE TABLE inventory_mirror.mem_user_inventory_tracking (
    id               integer      NOT NULL,
    upc              varchar(255) NOT NULL,
    uid              integer      NOT NULL,
    wh_id            varchar(16)  NOT NULL,
    quantity         integer      NOT NULL DEFAULT 0,
    last_update_time integer      NOT NULL,
    _sync_source     text,
    _sync_table      text,
    _sync_pk         text,
    CONSTRAINT mem_user_inventory_tracking_pkey PRIMARY KEY (id),
    CONSTRAINT mem_user_inventory_tracking_ukey UNIQUE (upc, uid, wh_id)
);

CREATE INDEX mem_user_inventory_tracking_uid_last_update_idx
    ON inventory_mirror.mem_user_inventory_tracking (uid, last_update_time);
CREATE INDEX mem_user_inventory_tracking_wh_id_id_idx
    ON inventory_mirror.mem_user_inventory_tracking (wh_id, id);

ALTER TABLE inventory_mirror.mem_user_inventory REPLICA IDENTITY FULL;
ALTER TABLE inventory_mirror.mem_user_inventory_lock REPLICA IDENTITY FULL;
ALTER TABLE inventory_mirror.mem_user_inventory_tracking REPLICA IDENTITY FULL;

CREATE PUBLICATION inventory_mirror_pub FOR TABLE
    inventory_mirror.mem_user_inventory,
    inventory_mirror.mem_user_inventory_lock,
    inventory_mirror.mem_user_inventory_tracking;

COMMIT;
