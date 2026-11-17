\set ON_ERROR_STOP on
-- Dedicated sample schemas only. Refuse an existing schema; no business DDL.
CREATE SCHEMA pgcat_demo_a;
CREATE SCHEMA pgcat_demo_b;
CREATE TABLE pgcat_demo_a.orders_0(
 id bigint PRIMARY KEY, tenant_id bigint NOT NULL, param jsonb NOT NULL,
 _sync_source text, _sync_table text, _sync_pk text
);
CREATE TABLE pgcat_demo_a.orders_1(LIKE pgcat_demo_a.orders_0 INCLUDING ALL);
CREATE TABLE pgcat_demo_a.order_detail(
 record_key text PRIMARY KEY, order_id bigint NOT NULL, note text,
 _sync_source text, _sync_table text, _sync_pk text
);
CREATE TABLE pgcat_demo_b.orders(
 record_key text PRIMARY KEY, order_id bigint NOT NULL, tenant_id bigint NOT NULL,
 customer_name text, outbound_id bigint, note text, payload jsonb NOT NULL,
 _sync_source text, _sync_table text, _sync_pk text
);
ALTER TABLE pgcat_demo_a.orders_0 REPLICA IDENTITY FULL;
ALTER TABLE pgcat_demo_a.orders_1 REPLICA IDENTITY FULL;
ALTER TABLE pgcat_demo_a.order_detail REPLICA IDENTITY FULL;
ALTER TABLE pgcat_demo_b.orders REPLICA IDENTITY FULL;
CREATE PUBLICATION pgcat_demo_a_pub FOR TABLE pgcat_demo_a.orders_0, pgcat_demo_a.orders_1,pgcat_demo_a.order_detail;
CREATE PUBLICATION pgcat_demo_b_pub FOR TABLE pgcat_demo_b.orders;
