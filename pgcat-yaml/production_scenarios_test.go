package main

import (
	"encoding/json"
	"fmt"
	"testing"
)

// Keep the pgcat implementation and the new pg_flo scenarios on the same
// inventory/outbound data contracts. This fixture omits connection secrets.
func TestProductionInventorySplitScenarios(t *testing.T) {
	c, err := loadConfig("../inventory-sync/pgcat/summary-to-rollback-1ton.yaml")
	if err != nil {
		t.Fatal(err)
	}
	row := func(warehouse string, reserved any) Row {
		return Row{"warehouse_id": 1, "upc": "UPC-1", "owner_id": 0, "summary_id": 101,
			"legacy_wh_id": warehouse, "on_hand_qty": json.Number("20.000000"),
			"reserved_qty": reserved, "hold_qty": json.Number("-2.000000"), "last_update_time": 1700000000}
	}
	t.Run("zero_uid_signed_quantities_and_three_projections", func(t *testing.T) {
		plan, err := c.plan("operations.inventory_summary_outbox", nil, row("WH-A", json.Number("4.000000")))
		if err != nil || len(plan) != 3 {
			t.Fatalf("plan=%v err=%v", plan, err)
		}
		quantities := map[string]bool{}
		for _, m := range plan {
			if m.Key["uid"] != 0 || m.Key["wh_id"] != "WH-A" {
				t.Fatal(m)
			}
			quantities[jsonString(m.Values["quantity"])] = true
		}
		for _, expected := range []string{"20", "4", "-2"} {
			if !quantities[expected] {
				t.Fatal(quantities)
			}
		}
	})
	t.Run("business_key_move_and_fraction_fail", func(t *testing.T) {
		plan, err := c.plan("operations.inventory_summary_outbox", row("WH-A", "4.000000"), row("WH-B", "4.000000"))
		if err != nil || len(plan) != 6 {
			t.Fatalf("plan=%v err=%v", plan, err)
		}
		deletes := 0
		for _, m := range plan {
			if m.Delete {
				deletes++
				if m.Key["wh_id"] != "WH-A" {
					t.Fatal(m)
				}
			} else if m.Key["wh_id"] != "WH-B" {
				t.Fatal(m)
			}
		}
		if deletes != 3 {
			t.Fatal("old keys not removed", plan)
		}
		plan, err = c.plan("operations.inventory_summary_outbox", nil, row("WH-A", "4.000001"))
		if err == nil || plan != nil {
			t.Fatal("fraction accepted or partial plan returned", plan, err)
		}
	})
}

func TestProductionOutboundCompositeAndDeleteScenarios(t *testing.T) {
	c := convertedGeneric(t)
	t.Run("warehouse_type_and_history_identity", func(t *testing.T) {
		keys := map[string]bool{}
		for _, tc := range []struct {
			table  string
			whType int64
		}{
			{"sys_warehouse_outbound_1", 1}, {"sys_warehouse_outbound_1_history", 1},
			{"mem_supplier_warehouse_outbound", 2}, {"thd_warehouse_outbound", 3},
		} {
			plan, err := c.plan("StoreServer."+tc.table, nil, Row{"outbound_id": 7})
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, m := range plan {
				if m.Route.Target != "StoreServerHaV2.sys_warehouse_outbound_m" {
					continue
				}
				found = true
				key := jsonString(m.Key)
				if keys[key] || fmt.Sprint(m.Values["wh_type"]) != fmt.Sprint(tc.whType) {
					t.Fatal(m)
				}
				keys[key] = true
			}
			if !found {
				t.Fatal("missing merged target", tc.table)
			}
		}
	})
	t.Run("serial_siblings_and_delete_flag_change", func(t *testing.T) {
		before := Row{"outbound_id": 7, "upc": "UPC-1", "bo_sn": "SN-A"}
		after := Row{"outbound_id": 7, "upc": "UPC-1", "bo_sn": "SN-B"}
		plan, err := c.plan("StoreServer.sys_warehouse_outbound_product_bo_sn_1", before, after)
		if err != nil || len(plan) != 2 || !plan[0].Delete || plan[1].Delete {
			t.Fatalf("%v %v", plan, err)
		}
		if plan[0].Values["bo_sn"] != "SN-A" || plan[1].Values["bo_sn"] != "SN-B" || plan[0].Owner.PK == plan[1].Owner.PK {
			t.Fatal(plan)
		}
		before = Row{"outbound_id": 7, "tracking_number": "TRACK-1", "delete": 0}
		after = Row{"outbound_id": 7, "tracking_number": "TRACK-1", "delete": 1}
		plan, err = c.plan("StoreServer.sys_warehouse_outbound_tracking_1", before, after)
		if err != nil || len(plan) != 2 || !plan[0].Delete || plan[1].Delete {
			t.Fatalf("%v %v", plan, err)
		}
	})
}

func jsonString(value any) string { data, _ := json.Marshal(value); return string(data) }
