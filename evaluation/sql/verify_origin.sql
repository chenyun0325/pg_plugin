-- Run on isolated PostgreSQL 16+, wal_level=logical, admin role, psql autocommit.
-- pgcat_mode=false: native pgoutput path proposed for pg-flo.
-- pgcat_mode=true: installed pgcat output plugin from pgcat-pgxs.
\set ON_ERROR_STOP on
\if :{?pgcat_mode}
\else
\set pgcat_mode false
\endif
CREATE SCHEMA origin_poc;
CREATE TABLE origin_poc.events(id int PRIMARY KEY);
CREATE PUBLICATION origin_poc_pub FOR TABLE origin_poc.events;
\if :pgcat_mode
SELECT * FROM pg_create_logical_replication_slot('origin_poc_slot','pgcat');
\else
SELECT * FROM pg_create_logical_replication_slot('origin_poc_slot','pgoutput');
\endif
INSERT INTO origin_poc.events VALUES(1);
\if :pgcat_mode
SELECT count(*) > 0 AS ok FROM pg_logical_slot_get_binary_changes('origin_poc_slot',NULL,NULL,'proto_version','1','publication_names','origin_poc_pub') \gset
\else
SELECT count(*) > 0 AS ok FROM pg_logical_slot_get_binary_changes('origin_poc_slot',NULL,NULL,'proto_version','1','publication_names','origin_poc_pub','origin','none') \gset
\endif
\if :ok
\else
\quit 1
\endif
SELECT pg_replication_origin_create('origin_poc_remote');
SELECT pg_replication_origin_session_setup('origin_poc_remote');
BEGIN;
INSERT INTO origin_poc.events VALUES(2);
SELECT pg_replication_origin_xact_setup('0/1',clock_timestamp());
COMMIT;
SELECT pg_replication_origin_session_reset();
\if :pgcat_mode
SELECT count(*) = 0 AS ok FROM pg_logical_slot_get_binary_changes('origin_poc_slot',NULL,NULL,'proto_version','1','publication_names','origin_poc_pub') \gset
\else
SELECT count(*) = 0 AS ok FROM pg_logical_slot_get_binary_changes('origin_poc_slot',NULL,NULL,'proto_version','1','publication_names','origin_poc_pub','origin','none') \gset
\endif
\if :ok
\else
\quit 1
\endif
INSERT INTO origin_poc.events VALUES(3);
\if :pgcat_mode
SELECT count(*) > 0 AS ok FROM pg_logical_slot_get_binary_changes('origin_poc_slot',NULL,NULL,'proto_version','1','publication_names','origin_poc_pub') \gset
\else
SELECT count(*) > 0 AS ok FROM pg_logical_slot_get_binary_changes('origin_poc_slot',NULL,NULL,'proto_version','1','publication_names','origin_poc_pub','origin','none') \gset
\endif
\if :ok
\else
\quit 1
\endif
SELECT pg_drop_replication_slot('origin_poc_slot');
SELECT pg_replication_origin_drop('origin_poc_remote');
DROP PUBLICATION origin_poc_pub;
DROP TABLE origin_poc.events;
DROP SCHEMA origin_poc;
\echo 'PASS: local writes emitted, replayed writes filtered, new local writes emitted'
