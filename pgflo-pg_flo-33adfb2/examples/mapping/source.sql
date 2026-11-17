-- PostgreSQL 18 source for the complete mapping example.
CREATE TABLE public.orders (
    id bigint PRIMARY KEY,
    tenant_id bigint NOT NULL,
    payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE public.legacy_orders_0 (
    id bigint PRIMARY KEY,
    tenant_id bigint NOT NULL,
    customer_name text,
    amount numeric(18,2)
);

CREATE TABLE public.legacy_orders_1 (LIKE public.legacy_orders_0 INCLUDING ALL);

-- A mapped UPDATE needs the old shard key and old target key. FULL also makes
-- the example fail-safe for future routing/transform columns.
ALTER TABLE public.orders REPLICA IDENTITY FULL;
ALTER TABLE public.legacy_orders_0 REPLICA IDENTITY FULL;
ALTER TABLE public.legacy_orders_1 REPLICA IDENTITY FULL;

-- Origin used when the reverse job applies a target-side local edit here.
SELECT pg_replication_origin_create('pgflo_from_target_demo');

-- Initial-copy sample data.
INSERT INTO public.orders(id, tenant_id, payload) VALUES
  (101, 2, '{"customer":{"name":"Alice"},"note":"seed-even","outbound_id":9001}');
INSERT INTO public.legacy_orders_0 VALUES (1, 8, 'Legacy-A', 12.50);
INSERT INTO public.legacy_orders_1 VALUES (1, 9, 'Legacy-B', 99.00);
