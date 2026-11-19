\set ON_ERROR_STOP on
\encoding UTF8

\ir 10_pg_merged.sql
\ir 20_pg_merged_projection.sql
\ir 15_pg_flink_n_to1_adapter.sql
\ir 50_projection_smoke_test.sql

\echo 'inventory merged schema and smoke test completed successfully'
