\set ON_ERROR_STOP on
\encoding UTF8

BEGIN;

-- Run on the merged PostgreSQL node.

CREATE SCHEMA IF NOT EXISTS operations;
CREATE SCHEMA inventory_stage;

CREATE TABLE operations.inventory_summary (
    id bigint NOT NULL,
    warehouse_id bigint NOT NULL,
    upc varchar(255) NOT NULL,
    goods_name varchar(256) NOT NULL,
    goods_version integer NOT NULL,
    owner_id bigint,
    on_hand_qty numeric(24, 6) DEFAULT 0 NOT NULL,
    reserved_qty numeric(24, 6) DEFAULT 0 NOT NULL,
    hold_qty numeric(24, 6) DEFAULT 0 NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    updated_at timestamptz DEFAULT now() NOT NULL,
    created_by bigint,
    updated_by bigint,
    deleted smallint DEFAULT 0 NOT NULL,
    version_id integer DEFAULT 0 NOT NULL,
    ownership_status varchar(16) GENERATED ALWAYS AS (
        CASE
            WHEN owner_id IS NULL THEN 'TBA'::text
            ELSE 'CONFIRMED'::text
        END
    ) STORED NULL,
    CONSTRAINT ck_summary_available_nonnegative
        CHECK (on_hand_qty >= reserved_qty + hold_qty),
    CONSTRAINT ck_summary_tba_quantities
        CHECK (owner_id IS NOT NULL OR (reserved_qty = 0 AND hold_qty = 0)),
    CONSTRAINT inventory_inventory_summary_pkey_afa75ec4 PRIMARY KEY (id),
    CONSTRAINT inventory_inventory_summary_warehouse_id_upc_owner_id__2db664d2
        UNIQUE (warehouse_id, upc, owner_id),
    CONSTRAINT inventory_summary_deleted_check CHECK (deleted = 0),
    CONSTRAINT inventory_summary_goods_version_check CHECK (goods_version >= 0),
    CONSTRAINT inventory_summary_hold_qty_check CHECK (hold_qty >= 0),
    CONSTRAINT inventory_summary_on_hand_qty_check CHECK (on_hand_qty >= 0),
    CONSTRAINT inventory_summary_owner_id_check CHECK (owner_id >= 0),
    CONSTRAINT inventory_summary_reserved_qty_check CHECK (reserved_qty >= 0),
    CONSTRAINT inventory_summary_warehouse_id_check CHECK (warehouse_id > 0)
);

CREATE INDEX inventory_idx_summary_goods_ba1cf310
    ON operations.inventory_summary USING btree (upc, warehouse_id, owner_id);

CREATE INDEX inventory_idx_summary_owner_383a3489
    ON operations.inventory_summary USING btree (owner_id, warehouse_id, upc);

CREATE INDEX ix_summary_page_keys
    ON operations.inventory_summary USING btree (warehouse_id, upc, owner_id)
    INCLUDE (ownership_status)
    WHERE deleted = 0 AND (owner_id IS NOT NULL OR on_hand_qty > 0);

COMMENT ON INDEX operations.ix_summary_page_keys IS
    '库存明细精确计数与分页键覆盖索引；包含归属状态，数量按本页读取后在应用汇总';

CREATE INDEX ix_summary_upc_read
    ON operations.inventory_summary USING btree (upc, warehouse_id, owner_id)
    INCLUDE (on_hand_qty, reserved_qty, hold_qty, version_id)
    WHERE deleted = 0 AND (owner_id IS NOT NULL OR on_hand_qty > 0);

-- PostgreSQL UNIQUE treats NULL values as distinct. This partial index guarantees
-- one TBA row for each warehouse/UPC pair.
CREATE UNIQUE INDEX uk_summary_tba
    ON operations.inventory_summary USING btree (warehouse_id, upc)
    WHERE owner_id IS NULL;

-- Explicit mappings are required because legacy wh_id is text and the three
-- MySQL tables do not contain goods_name/goods_version.
CREATE TABLE operations.inventory_warehouse_map (
    legacy_wh_id varchar(16) PRIMARY KEY,
    warehouse_id bigint NOT NULL UNIQUE CHECK (warehouse_id > 0),
    CHECK (length(legacy_wh_id) > 0)
);

CREATE TABLE operations.inventory_goods_map (
    upc varchar(255) PRIMARY KEY,
    goods_name varchar(256) NOT NULL,
    goods_version integer NOT NULL CHECK (goods_version >= 0),
    CHECK (length(upc) > 0)
);

-- The physical N:1 landing tables. pgcat owns the provenance columns; a deferred
-- trigger below projects their final transaction state into inventory_summary.
CREATE TABLE inventory_stage.mem_user_inventory (
    id integer PRIMARY KEY,
    upc varchar(255) NOT NULL,
    uid integer NOT NULL,
    wh_id varchar(16) NOT NULL,
    quantity integer NOT NULL DEFAULT 0,
    last_update_time integer NOT NULL,
    _sync_source text,
    _sync_table text,
    _sync_pk text,
    UNIQUE (uid, wh_id, upc)
);

CREATE TABLE inventory_stage.mem_user_inventory_lock (
    id integer PRIMARY KEY,
    upc varchar(255) NOT NULL,
    uid integer NOT NULL,
    wh_id varchar(16) NOT NULL,
    quantity integer NOT NULL DEFAULT 0,
    last_update_time integer NOT NULL,
    _sync_source text,
    _sync_table text,
    _sync_pk text,
    UNIQUE (upc, uid, wh_id)
);

CREATE TABLE inventory_stage.mem_user_inventory_tracking (
    id integer PRIMARY KEY,
    upc varchar(255) NOT NULL,
    uid integer NOT NULL,
    wh_id varchar(16) NOT NULL,
    quantity integer NOT NULL DEFAULT 0,
    last_update_time integer NOT NULL,
    _sync_source text,
    _sync_table text,
    _sync_pk text,
    UNIQUE (upc, uid, wh_id)
);

-- A plain, non-generated publication table is used for the reverse 1:N job.
-- Forward-origin transactions remain marked by pgcat and are ignored by the
-- reverse pgcat job, preventing a replication loop.
CREATE TABLE operations.inventory_summary_outbox (
    summary_id bigint NOT NULL UNIQUE,
    warehouse_id bigint NOT NULL,
    legacy_wh_id varchar(16) NOT NULL,
    upc varchar(255) NOT NULL,
    owner_id bigint NOT NULL,
    on_hand_qty numeric(24, 6) NOT NULL,
    reserved_qty numeric(24, 6) NOT NULL,
    hold_qty numeric(24, 6) NOT NULL,
    last_update_time integer NOT NULL,
    PRIMARY KEY (warehouse_id, upc, owner_id),
    CHECK (summary_id BETWEEN 1 AND 2147483647),
    CHECK (owner_id BETWEEN 0 AND 2147483647),
    CHECK (on_hand_qty = trunc(on_hand_qty) AND on_hand_qty <= 2147483647),
    CHECK (reserved_qty = trunc(reserved_qty) AND reserved_qty <= 2147483647),
    CHECK (hold_qty = trunc(hold_qty) AND hold_qty <= 2147483647)
);

ALTER TABLE operations.inventory_summary_outbox REPLICA IDENTITY FULL;

COMMIT;
