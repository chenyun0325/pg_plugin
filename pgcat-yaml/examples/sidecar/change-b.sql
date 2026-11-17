\set ON_ERROR_STOP on
-- After A init(copy), B init(seed), and both run processes have started.
-- Switch this UID to the B writer before executing this update.
UPDATE pgcat_sidecar_b.orders
SET tenant_id=3, customer_name='Alice revised', note='edited by new application'
WHERE order_id=101;
