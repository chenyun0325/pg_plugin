CREATE SCHEMA legacy;
CREATE SCHEMA app;

-- Source-isolated identity is the N:1 delete/ownership contract.
CREATE TABLE legacy.orders_0 (
    _pgflo_source text NOT NULL,
    _pgflo_source_table text NOT NULL,
    _pgflo_source_pk text NOT NULL,
    order_id bigint NOT NULL,
    tenant_id bigint NOT NULL,
    customer_name text,
    payload jsonb,
    PRIMARY KEY (_pgflo_source, _pgflo_source_table, _pgflo_source_pk)
);
CREATE TABLE legacy.orders_1 (LIKE legacy.orders_0 INCLUDING ALL);

CREATE TABLE app.order_detail (
    _pgflo_source text NOT NULL,
    _pgflo_source_table text NOT NULL,
    _pgflo_source_pk text NOT NULL,
    order_id bigint NOT NULL,
    note text,
    outbound_id bigint,
    PRIMARY KEY (_pgflo_source, _pgflo_source_table, _pgflo_source_pk)
);

CREATE TABLE app.all_legacy_orders (
    _pgflo_source text NOT NULL,
    _pgflo_source_table text NOT NULL,
    _pgflo_source_pk text NOT NULL,
    order_id bigint NOT NULL,
    tenant_id bigint NOT NULL,
    customer_name text,
    amount numeric(18,2),
    PRIMARY KEY (_pgflo_source, _pgflo_source_table, _pgflo_source_pk)
);

-- Reverse capture observes only local target edits. Forward-applied writes are
-- tagged with pgflo_from_source_demo and filtered by pgoutput origin=none.
ALTER TABLE legacy.orders_0 REPLICA IDENTITY FULL;
ALTER TABLE legacy.orders_1 REPLICA IDENTITY FULL;
SELECT pg_replication_origin_create('pgflo_from_source_demo');
