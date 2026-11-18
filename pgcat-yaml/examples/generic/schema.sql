\set ON_ERROR_STOP on
-- Disposable example database only; refuses existing schemas.
CREATE SCHEMA pgcat_generic_a;
CREATE SCHEMA pgcat_generic_b;
CREATE TABLE pgcat_generic_a.orders_0(
 id bigint PRIMARY KEY, uid bigint NOT NULL, write_epoch bigint NOT NULL, param jsonb NOT NULL
);
CREATE TABLE pgcat_generic_a.orders_1(LIKE pgcat_generic_a.orders_0 INCLUDING ALL);
CREATE TABLE pgcat_generic_a.customer(id bigint PRIMARY KEY,name text NOT NULL);
CREATE TABLE pgcat_generic_b.orders(
 _pk_id text PRIMARY KEY,id bigint NOT NULL,uid bigint NOT NULL,
 write_epoch bigint NOT NULL,param jsonb NOT NULL,outbound_id bigint NOT NULL
);
CREATE TABLE pgcat_generic_b.customer(LIKE pgcat_generic_a.customer INCLUDING ALL);
ALTER TABLE pgcat_generic_a.orders_0 REPLICA IDENTITY FULL;
ALTER TABLE pgcat_generic_a.orders_1 REPLICA IDENTITY FULL;
ALTER TABLE pgcat_generic_a.customer REPLICA IDENTITY FULL;
ALTER TABLE pgcat_generic_b.orders REPLICA IDENTITY FULL;
ALTER TABLE pgcat_generic_b.customer REPLICA IDENTITY FULL;
CREATE PUBLICATION pgcat_generic_a_pub FOR TABLE
 pgcat_generic_a.orders_0,pgcat_generic_a.orders_1,pgcat_generic_a.customer;
CREATE PUBLICATION pgcat_generic_b_pub FOR TABLE pgcat_generic_b.orders,pgcat_generic_b.customer;
INSERT INTO pgcat_generic_a.orders_0 VALUES(101,1,1,'{"outbound_id":10,"keep":"original"}');
INSERT INTO pgcat_generic_a.orders_1 VALUES(101,2,1,'{"outbound_id":20,"keep":"other"}');
INSERT INTO pgcat_generic_a.customer VALUES(1,'before');
