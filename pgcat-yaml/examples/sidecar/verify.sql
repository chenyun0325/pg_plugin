\set ON_ERROR_STOP on
SELECT record_key,order_id,tenant_id,customer_name,outbound_id,note
FROM pgcat_sidecar_b.orders ORDER BY record_key;
SELECT id,tenant_id,param FROM pgcat_sidecar_a.orders_0 ORDER BY id;
SELECT id,tenant_id,param FROM pgcat_sidecar_a.orders_1 ORDER BY id;
SELECT * FROM pgcat_sidecar_a.order_detail ORDER BY record_key;
SELECT job,end_lsn FROM _pgcat_yaml.checkpoint ORDER BY job;
SELECT node_id,table_name,pk,alive,identity->>'entity_id' AS entity_id,identity->'owner' AS original_owner
FROM _pgcat_yaml.row_binding WHERE namespace='orders_sidecar_v1' ORDER BY node_id,table_name,pk;
SELECT job,count(*) AS pending_outbox FROM _pgcat_yaml.identity_outbox
WHERE namespace='orders_sidecar_v1' GROUP BY job;
SELECT slot_name,active,confirmed_flush_lsn,pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)) AS retained_wal
FROM pg_replication_slots WHERE slot_name IN ('pgcat_sidecar_a_v1','pgcat_sidecar_b_v1');
