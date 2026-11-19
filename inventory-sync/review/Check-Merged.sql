\set ON_ERROR_STOP on
\encoding UTF8

-- Read-only checks. Every issue query should return zero rows.
-- Stage/summary equality applies ONLY while stage is the authoritative writer.
BEGIN READ ONLY;

SELECT 'invalid_summary' AS issue, id, warehouse_id, upc, owner_id
FROM operations.inventory_summary
WHERE on_hand_qty < 0 OR reserved_qty < 0 OR hold_qty < 0
   OR on_hand_qty < reserved_qty + hold_qty;

SELECT 'orphan_lock' AS issue, l.id, l.wh_id, l.upc, l.uid
FROM inventory_stage.mem_user_inventory_lock l
WHERE NOT EXISTS (SELECT FROM inventory_stage.mem_user_inventory i
                  WHERE (i.wh_id,i.upc,i.uid)=(l.wh_id,l.upc,l.uid))
UNION ALL
SELECT 'orphan_tracking', t.id, t.wh_id, t.upc, t.uid
FROM inventory_stage.mem_user_inventory_tracking t
WHERE NOT EXISTS (SELECT FROM inventory_stage.mem_user_inventory i
                  WHERE (i.wh_id,i.upc,i.uid)=(t.wh_id,t.upc,t.uid));

SELECT 'missing_mapping' AS issue, i.id, i.wh_id, i.upc
FROM inventory_stage.mem_user_inventory i
LEFT JOIN operations.inventory_warehouse_map w ON w.legacy_wh_id=i.wh_id
LEFT JOIN operations.inventory_goods_map g ON g.upc=i.upc
WHERE w.warehouse_id IS NULL OR g.upc IS NULL;

WITH expected AS (
    SELECT w.warehouse_id, i.upc, i.uid::bigint AS owner_id,
           i.quantity::numeric AS on_hand_qty,
           coalesce(l.quantity,0)::numeric AS reserved_qty,
           coalesce(t.quantity,0)::numeric AS hold_qty
    FROM inventory_stage.mem_user_inventory i
    JOIN operations.inventory_warehouse_map w ON w.legacy_wh_id=i.wh_id
    LEFT JOIN inventory_stage.mem_user_inventory_lock l
      ON (l.wh_id,l.upc,l.uid)=(i.wh_id,i.upc,i.uid)
    LEFT JOIN inventory_stage.mem_user_inventory_tracking t
      ON (t.wh_id,t.upc,t.uid)=(i.wh_id,i.upc,i.uid)
), actual AS (
    SELECT * FROM operations.inventory_summary WHERE owner_id IS NOT NULL
)
SELECT 'stage_summary_difference' AS issue,
       coalesce(e.warehouse_id,s.warehouse_id) AS warehouse_id,
       coalesce(e.upc,s.upc) AS upc, coalesce(e.owner_id,s.owner_id) AS owner_id,
       e.on_hand_qty AS expected_on_hand, s.on_hand_qty AS actual_on_hand,
       e.reserved_qty AS expected_reserved, s.reserved_qty AS actual_reserved,
       e.hold_qty AS expected_hold, s.hold_qty AS actual_hold
FROM expected e FULL JOIN actual s USING(warehouse_id,upc,owner_id)
WHERE e.owner_id IS NULL OR s.owner_id IS NULL
   OR (e.on_hand_qty,e.reserved_qty,e.hold_qty)
      IS DISTINCT FROM (s.on_hand_qty,s.reserved_qty,s.hold_qty);

WITH confirmed AS (
    SELECT s.*, w.legacy_wh_id,
           floor(extract(epoch FROM s.updated_at))::integer AS last_update_time
    FROM operations.inventory_summary s
    LEFT JOIN operations.inventory_warehouse_map w USING(warehouse_id)
    WHERE s.owner_id IS NOT NULL
)
SELECT 'summary_outbox_difference' AS issue,
       coalesce(s.warehouse_id,o.warehouse_id) AS warehouse_id,
       coalesce(s.upc,o.upc) AS upc, coalesce(s.owner_id,o.owner_id) AS owner_id
FROM confirmed s FULL JOIN operations.inventory_summary_outbox o
  USING(warehouse_id,upc,owner_id)
WHERE s.id IS NULL OR o.summary_id IS NULL
   OR (s.id,s.legacy_wh_id,s.on_hand_qty,s.reserved_qty,s.hold_qty,s.last_update_time)
      IS DISTINCT FROM (o.summary_id,o.legacy_wh_id,o.on_hand_qty,o.reserved_qty,o.hold_qty,o.last_update_time);

ROLLBACK;
