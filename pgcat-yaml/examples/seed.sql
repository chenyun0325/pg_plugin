\set ON_ERROR_STOP on
INSERT INTO pgcat_demo_a.orders_0(id,tenant_id,param) VALUES
 (101,2,'{"customer":{"name":"Alice","tier":"gold"},"outbound_id":9007199254740993,"note":"first","keep":"preserve me"}');
INSERT INTO pgcat_demo_a.orders_1(id,tenant_id,param) VALUES
 (202,3,'{"customer":{"name":"Bob"},"outbound_id":202,"note":"second"}');
