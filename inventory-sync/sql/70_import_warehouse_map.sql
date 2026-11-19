\set ON_ERROR_STOP on
\encoding UTF8

BEGIN;

CREATE TEMP TABLE warehouse_map_import (
    warehouse_id bigint PRIMARY KEY,
    legacy_wh_id varchar(16) NOT NULL UNIQUE
) ON COMMIT DROP;

INSERT INTO warehouse_map_import(warehouse_id, legacy_wh_id) VALUES
    (1, 'OR11085'),
    (2, 'DE106'),
    (4, 'CA5001'),
    (5, 'TN2801'),
    (6, 'NH4D'),
    (7, 'FL8465'),
    (8, 'PA707'),
    (9, 'TX7810');

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM operations.inventory_warehouse_map target
          JOIN warehouse_map_import source
            ON source.warehouse_id = target.warehouse_id
         WHERE source.legacy_wh_id <> target.legacy_wh_id
    ) THEN
        RAISE EXCEPTION
            'warehouse_id is already assigned to a different legacy_wh_id';
    END IF;
END;
$$;

INSERT INTO operations.inventory_warehouse_map(legacy_wh_id, warehouse_id)
SELECT legacy_wh_id, warehouse_id
  FROM warehouse_map_import
ON CONFLICT (legacy_wh_id) DO UPDATE
SET warehouse_id = EXCLUDED.warehouse_id;

DO $$
DECLARE
    v_expected integer;
    v_actual integer;
BEGIN
    SELECT count(*) INTO v_expected FROM warehouse_map_import;
    SELECT count(*)
      INTO v_actual
      FROM warehouse_map_import source
      JOIN operations.inventory_warehouse_map target
        ON target.legacy_wh_id = source.legacy_wh_id
       AND target.warehouse_id = source.warehouse_id;
    IF v_actual <> v_expected THEN
        RAISE EXCEPTION 'warehouse mapping verification failed: expected %, got %',
            v_expected, v_actual;
    END IF;
END;
$$;

COMMIT;

SELECT legacy_wh_id, warehouse_id
  FROM operations.inventory_warehouse_map
 WHERE warehouse_id IN (1, 2, 4, 5, 6, 7, 8, 9)
 ORDER BY warehouse_id;
