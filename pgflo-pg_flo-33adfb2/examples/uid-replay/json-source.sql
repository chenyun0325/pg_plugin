CREATE SCHEMA app;
CREATE TABLE app.orders(
 id bigint PRIMARY KEY,uid bigint NOT NULL,payload jsonb NOT NULL,reserved_qty bigint,
 _pgflo_source text NOT NULL,_pgflo_source_table text NOT NULL,_pgflo_source_pk text NOT NULL);
ALTER TABLE app.orders REPLICA IDENTITY FULL;
ALTER TABLE app.orders ALTER COLUMN payload SET STORAGE EXTERNAL;
INSERT INTO app.orders
 SELECT 42,0,jsonb_build_object('customer',jsonb_build_object('name',' Alice '),
 'large',string_agg(md5(i::text),'')),4,'mysql-wms','legacy.orders_1','[9007199254740993]'
 FROM generate_series(1,2000) i;
