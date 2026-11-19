\set ON_ERROR_STOP on
\encoding UTF8

BEGIN;

-- Run on the rollback PostgreSQL node. Keep this separate from the active Flink
-- mirror schema unless write ownership has been switched and identity reconciled.

CREATE SCHEMA inventory_rollback;

CREATE TABLE inventory_rollback.mem_user_inventory (
    id bigint NOT NULL,
    upc varchar(255) NOT NULL,
    uid integer NOT NULL,
    wh_id varchar(16) NOT NULL,
    quantity integer NOT NULL DEFAULT 0,
    last_update_time integer NOT NULL,
    _sync_source text,
    _sync_table text,
    _sync_pk text,
    PRIMARY KEY (id),
    UNIQUE (uid, wh_id, upc)
);

CREATE TABLE inventory_rollback.mem_user_inventory_lock (
    id bigint NOT NULL,
    upc varchar(255) NOT NULL,
    uid integer NOT NULL,
    wh_id varchar(16) NOT NULL,
    quantity integer NOT NULL DEFAULT 0,
    last_update_time integer NOT NULL,
    _sync_source text,
    _sync_table text,
    _sync_pk text,
    PRIMARY KEY (id),
    UNIQUE (upc, uid, wh_id)
);

CREATE TABLE inventory_rollback.mem_user_inventory_tracking (
    id bigint NOT NULL,
    upc varchar(255) NOT NULL,
    uid integer NOT NULL,
    wh_id varchar(16) NOT NULL,
    quantity integer NOT NULL DEFAULT 0,
    last_update_time integer NOT NULL,
    _sync_source text,
    _sync_table text,
    _sync_pk text,
    PRIMARY KEY (id),
    UNIQUE (upc, uid, wh_id)
);

COMMIT;
