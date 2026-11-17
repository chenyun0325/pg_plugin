\set ON_ERROR_STOP on
-- Dedicated sample schemas only. Refuse an existing schema; no business DDL.
CREATE SCHEMA pgcat_sidecar_a;
CREATE SCHEMA pgcat_sidecar_b;
CREATE TABLE pgcat_sidecar_a.orders_0(
 id bigint PRIMARY KEY, tenant_id bigint NOT NULL, param jsonb NOT NULL
);
CREATE TABLE pgcat_sidecar_a.orders_1(LIKE pgcat_sidecar_a.orders_0 INCLUDING ALL);
CREATE TABLE pgcat_sidecar_a.order_detail(
 record_key text PRIMARY KEY, order_id bigint NOT NULL, note text
);
CREATE TABLE pgcat_sidecar_b.orders(
 record_key text PRIMARY KEY, order_id bigint NOT NULL, tenant_id bigint NOT NULL,
 customer_name text, outbound_id bigint, note text, payload jsonb NOT NULL
);
ALTER TABLE pgcat_sidecar_a.orders_0 REPLICA IDENTITY FULL;
ALTER TABLE pgcat_sidecar_a.orders_1 REPLICA IDENTITY FULL;
ALTER TABLE pgcat_sidecar_a.order_detail REPLICA IDENTITY FULL;
ALTER TABLE pgcat_sidecar_b.orders REPLICA IDENTITY FULL;
CREATE PUBLICATION pgcat_sidecar_a_pub FOR TABLE pgcat_sidecar_a.orders_0, pgcat_sidecar_a.orders_1,pgcat_sidecar_a.order_detail;
CREATE PUBLICATION pgcat_sidecar_b_pub FOR TABLE pgcat_sidecar_b.orders;
