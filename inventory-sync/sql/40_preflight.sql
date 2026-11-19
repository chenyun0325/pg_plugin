\set ON_ERROR_STOP on
\encoding UTF8

-- Run on the mirror node before pgcat init. A successful preflight returns zero
-- rows from every query below.

SELECT 'invalid_inventory' AS issue, id, wh_id, upc, uid, quantity
FROM inventory_mirror.mem_user_inventory
WHERE uid < 0
   OR wh_id = ''
   OR length(upc) > 255
   OR quantity < 0;

SELECT 'invalid_lock' AS issue, id, wh_id, upc, uid, quantity
FROM inventory_mirror.mem_user_inventory_lock
WHERE uid < 0
   OR wh_id = ''
   OR length(upc) > 255
   OR quantity < 0;

SELECT 'invalid_tracking' AS issue, id, wh_id, upc, uid, quantity
FROM inventory_mirror.mem_user_inventory_tracking
WHERE uid < 0
   OR wh_id = ''
   OR length(upc) > 255
   OR quantity < 0;

WITH quantities AS (
    SELECT
        i.id,
        i.wh_id,
        i.upc,
        i.uid,
        i.quantity AS on_hand,
        COALESCE(l.quantity, 0) AS reserved,
        COALESCE(t.quantity, 0) AS hold
    FROM inventory_mirror.mem_user_inventory i
    LEFT JOIN inventory_mirror.mem_user_inventory_lock l
      ON l.uid = i.uid
     AND l.wh_id = i.wh_id
     AND l.upc = i.upc
    LEFT JOIN inventory_mirror.mem_user_inventory_tracking t
      ON t.uid = i.uid
     AND t.wh_id = i.wh_id
     AND t.upc = i.upc
)
SELECT 'quantity_constraint' AS issue, *
FROM quantities
WHERE on_hand < reserved + hold;

SELECT 'orphan_lock' AS issue, l.id, l.wh_id, l.upc, l.uid
FROM inventory_mirror.mem_user_inventory_lock l
LEFT JOIN inventory_mirror.mem_user_inventory i
  ON i.uid = l.uid
 AND i.wh_id = l.wh_id
 AND i.upc = l.upc
WHERE i.id IS NULL;

SELECT 'orphan_tracking' AS issue, t.id, t.wh_id, t.upc, t.uid
FROM inventory_mirror.mem_user_inventory_tracking t
LEFT JOIN inventory_mirror.mem_user_inventory i
  ON i.uid = t.uid
 AND i.wh_id = t.wh_id
 AND i.upc = t.upc
WHERE i.id IS NULL;
