\set ON_ERROR_STOP on
\encoding UTF8

-- Keep the metadata-lock transaction short. NOT VALID constraints immediately
-- check new writes; validate existing rows afterwards with a weaker lock.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE operations.inventory_summary
    DROP CONSTRAINT inventory_summary_owner_id_check,
    ADD CONSTRAINT inventory_summary_owner_id_check
        CHECK (owner_id >= 0) NOT VALID;

ALTER TABLE operations.inventory_summary_outbox
    DROP CONSTRAINT inventory_summary_outbox_owner_id_check,
    ADD CONSTRAINT inventory_summary_outbox_owner_id_check
        CHECK (owner_id BETWEEN 0 AND 2147483647) NOT VALID;

COMMIT;

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE operations.inventory_summary
    VALIDATE CONSTRAINT inventory_summary_owner_id_check;
ALTER TABLE operations.inventory_summary_outbox
    VALIDATE CONSTRAINT inventory_summary_outbox_owner_id_check;

COMMIT;
