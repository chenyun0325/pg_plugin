CREATE SCHEMA inventory_rollback;
CREATE SCHEMA store;
CREATE TABLE inventory_rollback.mem_user_inventory (
  id bigint NOT NULL, uid bigint NOT NULL, wh_id text NOT NULL, upc text NOT NULL,
  quantity integer, PRIMARY KEY(uid,wh_id,upc)
);
CREATE TABLE inventory_rollback.mem_user_inventory_lock
  (LIKE inventory_rollback.mem_user_inventory INCLUDING ALL);
CREATE TABLE inventory_rollback.mem_user_inventory_tracking
  (LIKE inventory_rollback.mem_user_inventory INCLUDING ALL);
CREATE TABLE store.sys_warehouse_outbound_m (
  _pgflo_source text NOT NULL, _pgflo_source_table text NOT NULL, _pgflo_source_pk text NOT NULL,
  _pk_id text NOT NULL, outbound_id bigint NOT NULL, wh_type integer NOT NULL,
  PRIMARY KEY(_pgflo_source,_pgflo_source_table,_pgflo_source_pk)
);
CREATE TABLE store.sys_warehouse_outbound_product_bo_sn_m (
  _pgflo_source text NOT NULL, _pgflo_source_table text NOT NULL, _pgflo_source_pk text NOT NULL,
  _pk_id text NOT NULL, upc text NOT NULL, bo_sn text NOT NULL, quantity integer,
  PRIMARY KEY(_pgflo_source,_pgflo_source_table,_pgflo_source_pk)
);
CREATE TABLE store.sys_warehouse_outbound_tracking_m (
  _pgflo_source text NOT NULL, _pgflo_source_table text NOT NULL, _pgflo_source_pk text NOT NULL,
  _pk_id text NOT NULL, tracking_number text NOT NULL, "delete" integer NOT NULL,
  PRIMARY KEY(_pgflo_source,_pgflo_source_table,_pgflo_source_pk)
);
CREATE TABLE store.mem_customer_tracking (
  _pgflo_source text NOT NULL, _pgflo_source_table text NOT NULL, _pgflo_source_pk text NOT NULL,
  id bigint NOT NULL, outbound_id bigint,
  PRIMARY KEY(_pgflo_source,_pgflo_source_table,_pgflo_source_pk)
);
SELECT pg_replication_origin_create('pgflo_inventory_wms_lab');
