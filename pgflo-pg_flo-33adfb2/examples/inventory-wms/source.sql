CREATE TABLE public.inventory_summary_outbox (
  summary_id bigint PRIMARY KEY, owner_id bigint NOT NULL, legacy_wh_id text NOT NULL,
  upc text NOT NULL, on_hand_qty numeric(24,6), reserved_qty numeric(24,6), hold_qty numeric(24,6)
);
CREATE TABLE public.sys_warehouse_outbound_1 (outbound_id bigint PRIMARY KEY);
CREATE TABLE public.sys_warehouse_outbound_1_history (LIKE public.sys_warehouse_outbound_1 INCLUDING ALL);
CREATE TABLE public.thd_warehouse_outbound (LIKE public.sys_warehouse_outbound_1 INCLUDING ALL);
CREATE TABLE public.mem_supplier_warehouse_outbound (LIKE public.sys_warehouse_outbound_1 INCLUDING ALL);
CREATE TABLE public.sys_warehouse_outbound_product_bo_sn_1 (
  outbound_id bigint NOT NULL, upc text NOT NULL, bo_sn text NOT NULL, quantity integer,
  PRIMARY KEY(outbound_id,upc,bo_sn)
);
CREATE TABLE public.sys_warehouse_outbound_tracking_1 (
  outbound_id bigint NOT NULL, tracking_number text NOT NULL, "delete" integer NOT NULL,
  PRIMARY KEY(outbound_id,tracking_number,"delete")
);
CREATE TABLE public.mem_customer_tracking (id bigint PRIMARY KEY, param jsonb);
ALTER TABLE public.inventory_summary_outbox REPLICA IDENTITY FULL;
ALTER TABLE public.sys_warehouse_outbound_1 REPLICA IDENTITY FULL;
ALTER TABLE public.sys_warehouse_outbound_1_history REPLICA IDENTITY FULL;
ALTER TABLE public.thd_warehouse_outbound REPLICA IDENTITY FULL;
ALTER TABLE public.mem_supplier_warehouse_outbound REPLICA IDENTITY FULL;
ALTER TABLE public.sys_warehouse_outbound_product_bo_sn_1 REPLICA IDENTITY FULL;
ALTER TABLE public.sys_warehouse_outbound_tracking_1 REPLICA IDENTITY FULL;
ALTER TABLE public.mem_customer_tracking REPLICA IDENTITY FULL;
