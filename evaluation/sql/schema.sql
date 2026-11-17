-- Reference schema for PostgreSQL 16+. Install in an isolated test database.
CREATE SCHEMA sync_meta;
CREATE SCHEMA app;
CREATE SCHEMA legacy;
CREATE TABLE sync_meta.progress (
 source text NOT NULL, epoch text NOT NULL, seq bigint NOT NULL,
 end_lsn pg_lsn, PRIMARY KEY(source,epoch)
);
CREATE TABLE sync_meta.applied (
 source text NOT NULL, epoch text NOT NULL, txid text NOT NULL,
 version text NOT NULL, PRIMARY KEY(source,epoch,txid)
);
CREATE TABLE app.orders (
 _sync_source text NOT NULL, _sync_table text NOT NULL, _sync_pk text NOT NULL,
 order_id bigint NOT NULL, tenant_id bigint NOT NULL, customer_name text,
 PRIMARY KEY(_sync_source,_sync_table,_sync_pk)
);
CREATE TABLE legacy.orders_0 (LIKE app.orders INCLUDING ALL);
CREATE TABLE legacy.orders_1 (LIKE app.orders INCLUDING ALL);
CREATE TABLE app.order_detail (
 _sync_source text NOT NULL, _sync_table text NOT NULL, _sync_pk text NOT NULL,
 order_id bigint NOT NULL, note text,
 PRIMARY KEY(_sync_source,_sync_table,_sync_pk)
);
-- Admin provisions origins; workers receive only required privileges.
SELECT pg_replication_origin_create('mapping_from_a');
SELECT pg_replication_origin_create('mapping_from_b');

-- Reverse conversion updates one JSON path, preserving unrelated business keys.
-- Deleting a detail record must have an explicit business rule; never infer a
-- deletion of the complete aggregate merely from a vertical table DELETE.
CREATE FUNCTION sync_meta.patch_customer(doc jsonb, customer_name text)
RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$
 SELECT jsonb_set(
   COALESCE(doc,'{}'::jsonb), '{customer}',
   COALESCE(doc->'customer','{}'::jsonb) ||
     jsonb_build_object('name', customer_name), true)
$$;
