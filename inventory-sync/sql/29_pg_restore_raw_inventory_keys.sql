\set ON_ERROR_STOP on
\encoding UTF8

-- Execute in the same connection/transaction as the snapshot produced by
-- InventoryRawKeyExport, after applying migration 28. No missing source row is
-- inserted and no quantity/timestamp is overwritten from the export snapshot.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';
LOCK TABLE inventory_flink_stage.inventory_component,
    inventory_stage.mem_user_inventory, inventory_stage.mem_user_inventory_lock,
    inventory_stage.mem_user_inventory_tracking,
    operations.inventory_summary, operations.inventory_summary_outbox
    IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF strpos(lower(pg_get_functiondef('operations.normalize_inventory_stage_row()'::regprocedure)), 'btrim(') > 0
       OR strpos(lower(pg_get_functiondef('operations.refresh_inventory_summary(character varying,character varying,integer)'::regprocedure)), 'btrim(') > 0 THEN
        RAISE EXCEPTION 'Apply migration 28 before restoring raw source keys';
    END IF;
END;
$$;

CREATE TEMP TABLE inventory_raw_key_targets ON COMMIT DROP AS
SELECT source_table, source_id, uid,
       convert_from(decode(upc_hex,'hex'),'UTF8') AS upc,
       convert_from(decode(wh_id_hex,'hex'),'UTF8') AS wh_id,
       last_update_time
FROM inventory_source_raw_keys;
ALTER TABLE inventory_raw_key_targets ADD PRIMARY KEY (source_table,source_id);

CREATE TEMP VIEW inventory_raw_key_landed AS
SELECT 'mem_user_inventory'::text AS source_table,id AS source_id,uid,upc,wh_id,last_update_time
FROM inventory_stage.mem_user_inventory
UNION ALL
SELECT 'mem_user_inventory_lock',id,uid,upc,wh_id,last_update_time
FROM inventory_stage.mem_user_inventory_lock
UNION ALL
SELECT 'mem_user_inventory_tracking',id,uid,upc,wh_id,last_update_time
FROM inventory_stage.mem_user_inventory_tracking;

-- Abort on drift rather than assigning an old snapshot's keys to a newer event.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM inventory_raw_key_landed landed
        JOIN inventory_raw_key_targets source USING (source_table,source_id)
        WHERE (landed.upc,landed.wh_id) IS DISTINCT FROM (source.upc,source.wh_id)
          AND (landed.uid <> source.uid OR landed.upc <> btrim(source.upc)
               OR landed.wh_id <> btrim(source.wh_id)
               OR landed.last_update_time > source.last_update_time)
    ) OR EXISTS (
        SELECT 1 FROM inventory_flink_stage.inventory_component landed
        JOIN inventory_raw_key_targets source USING (source_table,source_id)
        WHERE (landed.upc,landed.wh_id) IS DISTINCT FROM (source.upc,source.wh_id)
          AND (landed.uid <> source.uid OR landed.upc <> btrim(source.upc)
               OR landed.wh_id <> btrim(source.wh_id)
               OR landed.last_update_time > source.last_update_time)
    ) THEN
        RAISE EXCEPTION 'Source keys changed after normalization; re-export and investigate before restoration';
    END IF;
END;
$$;

CREATE TEMP TABLE inventory_raw_key_changes ON COMMIT DROP AS
SELECT landed.source_table,landed.source_id,landed.uid,
       landed.upc AS old_upc,landed.wh_id AS old_wh_id,
       source.upc AS new_upc,source.wh_id AS new_wh_id
FROM inventory_raw_key_landed landed
JOIN inventory_raw_key_targets source USING (source_table,source_id)
WHERE (landed.upc,landed.wh_id) IS DISTINCT FROM (source.upc,source.wh_id)
UNION
SELECT landed.source_table,landed.source_id,landed.uid,
       landed.upc,landed.wh_id,source.upc,source.wh_id
FROM inventory_flink_stage.inventory_component landed
JOIN inventory_raw_key_targets source USING (source_table,source_id)
WHERE (landed.upc,landed.wh_id) IS DISTINCT FROM (source.upc,source.wh_id);

CREATE TEMP TABLE inventory_raw_key_affected ON COMMIT DROP AS
SELECT uid,old_upc AS upc,old_wh_id AS wh_id FROM inventory_raw_key_changes
UNION
SELECT uid,new_upc,new_wh_id FROM inventory_raw_key_changes;
ALTER TABLE inventory_raw_key_affected ADD PRIMARY KEY (uid,upc,wh_id);

-- Before-images go to the deployment log; source keys also remain in the export.
SELECT 'BEFORE_COMPONENT' AS audit,row_to_json(landed) AS row_data
FROM inventory_flink_stage.inventory_component landed
JOIN inventory_raw_key_changes changed USING (source_table,source_id);
SELECT 'BEFORE_STAGE' AS audit,row_to_json(landed) AS row_data
FROM inventory_raw_key_landed landed
JOIN inventory_raw_key_changes changed USING (source_table,source_id);
CREATE TEMP TABLE inventory_raw_key_summary_before ON COMMIT DROP AS
SELECT summary.* FROM operations.inventory_summary summary
JOIN operations.inventory_warehouse_map warehouse USING (warehouse_id)
JOIN inventory_raw_key_affected affected
  ON affected.wh_id=warehouse.legacy_wh_id AND affected.upc=summary.upc
 AND affected.uid=summary.owner_id;
SELECT 'BEFORE_SUMMARY' AS audit,row_to_json(summary) AS row_data
FROM inventory_raw_key_summary_before summary;
SELECT 'BEFORE_OUTBOX' AS audit,row_to_json(outbox) AS row_data
FROM operations.inventory_summary_outbox outbox
JOIN inventory_raw_key_summary_before summary ON summary.id=outbox.summary_id;

-- Remove old derived identities before recreating them. Otherwise a business-key
-- UPSERT could retain a source ID now belonging to a different raw key.
DELETE FROM operations.inventory_summary summary
USING operations.inventory_warehouse_map warehouse,inventory_raw_key_affected affected
WHERE summary.warehouse_id=warehouse.warehouse_id
  AND affected.wh_id=warehouse.legacy_wh_id AND affected.upc=summary.upc
  AND affected.uid=summary.owner_id;

WITH changed AS (
    UPDATE inventory_flink_stage.inventory_component landed
    SET upc=source.upc,wh_id=source.wh_id
    FROM inventory_raw_key_targets source
    WHERE landed.source_table=source.source_table AND landed.source_id=source.source_id
      AND (landed.upc,landed.wh_id) IS DISTINCT FROM (source.upc,source.wh_id)
    RETURNING landed.source_row_id
)
SELECT count(*) AS restored_component_rows FROM changed;

-- The component route normally restored these already; also cover stage rows
-- written by the PostgreSQL mirror path instead of the Flink adapter.
UPDATE inventory_stage.mem_user_inventory landed SET upc=source.upc,wh_id=source.wh_id
FROM inventory_raw_key_targets source
WHERE source.source_table='mem_user_inventory' AND landed.id=source.source_id
  AND (landed.upc,landed.wh_id) IS DISTINCT FROM (source.upc,source.wh_id);
UPDATE inventory_stage.mem_user_inventory_lock landed SET upc=source.upc,wh_id=source.wh_id
FROM inventory_raw_key_targets source
WHERE source.source_table='mem_user_inventory_lock' AND landed.id=source.source_id
  AND (landed.upc,landed.wh_id) IS DISTINCT FROM (source.upc,source.wh_id);
UPDATE inventory_stage.mem_user_inventory_tracking landed SET upc=source.upc,wh_id=source.wh_id
FROM inventory_raw_key_targets source
WHERE source.source_table='mem_user_inventory_tracking' AND landed.id=source.source_id
  AND (landed.upc,landed.wh_id) IS DISTINCT FROM (source.upc,source.wh_id);

SET CONSTRAINTS ALL IMMEDIATE;
SELECT operations.refresh_inventory_summary(wh_id::varchar,upc::varchar,uid)
FROM inventory_raw_key_affected ORDER BY wh_id,upc,uid;

-- A key repair must not make an existing inventory identity appear newly created.
UPDATE operations.inventory_summary summary
SET created_at=previous.created_at,created_by=previous.created_by,
    updated_by=previous.updated_by,
    version_id=GREATEST(summary.version_id,previous.version_id+1)
FROM inventory_raw_key_summary_before previous
WHERE summary.id=previous.id;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM inventory_raw_key_landed landed
        JOIN inventory_raw_key_targets source USING (source_table,source_id)
        WHERE (landed.uid,landed.upc,landed.wh_id) IS DISTINCT FROM (source.uid,source.upc,source.wh_id)
    ) OR EXISTS (
        SELECT 1 FROM inventory_flink_stage.inventory_component landed
        JOIN inventory_raw_key_targets source USING (source_table,source_id)
        WHERE (landed.uid,landed.upc,landed.wh_id) IS DISTINCT FROM (source.uid,source.upc,source.wh_id)
    ) OR EXISTS (
        SELECT 1 FROM inventory_raw_key_affected affected
        JOIN operations.inventory_warehouse_map warehouse ON warehouse.legacy_wh_id=affected.wh_id
        JOIN inventory_stage.mem_user_inventory inventory
          ON (inventory.uid,inventory.upc,inventory.wh_id)=(affected.uid,affected.upc,affected.wh_id)
        LEFT JOIN operations.inventory_summary summary
          ON (summary.warehouse_id,summary.upc,summary.owner_id)=(warehouse.warehouse_id,affected.upc,affected.uid)
        LEFT JOIN operations.inventory_summary_outbox outbox
          ON (outbox.warehouse_id,outbox.upc,outbox.owner_id)=(warehouse.warehouse_id,affected.upc,affected.uid)
        WHERE summary.id IS DISTINCT FROM inventory.id::bigint
           OR outbox.summary_id IS DISTINCT FROM inventory.id::bigint
    ) THEN
        RAISE EXCEPTION 'Raw-key restoration verification failed; all data changes rolled back';
    END IF;
END;
$$;

SELECT source_table,count(*) AS restored_stage_identities
FROM inventory_raw_key_changes GROUP BY source_table ORDER BY source_table;
DROP VIEW inventory_raw_key_landed;
COMMIT;
