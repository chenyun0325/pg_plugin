\set ON_ERROR_STOP on
SELECT record_key,order_id,tenant_id,customer_name,outbound_id,note,_sync_source,_sync_table,_sync_pk
FROM pgcat_demo_b.orders ORDER BY record_key;
SELECT id,tenant_id,param FROM pgcat_demo_a.orders_0 ORDER BY id;
SELECT id,tenant_id,param FROM pgcat_demo_a.orders_1 ORDER BY id;
SELECT * FROM pgcat_demo_a.order_detail ORDER BY record_key;
SELECT job,end_lsn FROM _pgcat_yaml.checkpoint ORDER BY job;
SELECT slot_name,active,confirmed_flush_lsn,pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)) AS retained_wal
FROM pg_replication_slots WHERE slot_name IN ('pgcat_demo_a_v1','pgcat_demo_b_v1');
