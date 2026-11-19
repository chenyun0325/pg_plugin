package scenarios

import (
	"fmt"
	"testing"

	"github.com/pgflo/pg_flo/pkg/jobconfig"
	"github.com/pgflo/pg_flo/pkg/mapping"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/stretchr/testify/require"
)

const fixtureDir = "../../examples/inventory-wms/"

func scenarioEngine(t *testing.T) *mapping.Engine {
	t.Helper()
	job, err := jobconfig.Load(fixtureDir + "job.yaml")
	require.NoError(t, err)
	engine, err := mapping.NewEngine(job.Mapping)
	require.NoError(t, err)
	return engine
}

func inventoryRow(warehouse string, onHand, reserved, hold interface{}) map[string]interface{} {
	return map[string]interface{}{"summary_id": int64(101), "owner_id": int64(0), "legacy_wh_id": warehouse,
		"upc": "UPC-1", "on_hand_qty": onHand, "reserved_qty": reserved, "hold_qty": hold}
}

func event(table, tx string, before, after map[string]interface{}) *utils.CDCMessage {
	operation := utils.OperationUpdate
	if before == nil {
		operation = utils.OperationInsert
	} else if after == nil {
		operation = utils.OperationDelete
	}
	return &utils.CDCMessage{Type: operation, Schema: "public", Table: table,
		TransactionID: tx, MappingVersion: "inventory-wms-v1", LSN: "0/10", Values: after, OldValues: before}
}

func TestInventoryWMSScenarios(t *testing.T) {
	e := scenarioEngine(t)
	t.Run("inventory_three_projections_zero_owner_and_signed_counts", func(t *testing.T) {
		out, err := e.Map(event("inventory_summary_outbox", "inv-1", nil, inventoryRow("WH-A", "20.000000", "4.000000", "-2.000000")))
		require.NoError(t, err)
		require.Len(t, out, 3)
		for i, quantity := range []int64{20, 4, -2} {
			require.Equal(t, quantity, out[i].Values["quantity"])
			require.Equal(t, int64(0), out[i].Values["uid"])
			require.Equal(t, []string{"uid", "wh_id", "upc"}, out[i].ReplicationKey.Columns)
		}
	})
	t.Run("warehouse_key_change_cleans_all_old_rows", func(t *testing.T) {
		out, err := e.Map(event("inventory_summary_outbox", "inv-move", inventoryRow("WH-A", "20", "4", "2"), inventoryRow("WH-B", "20", "4", "2")))
		require.NoError(t, err)
		require.Len(t, out, 6)
		for i := 0; i < len(out); i += 2 {
			require.Equal(t, utils.OperationDelete, out[i].Type)
			require.Equal(t, "WH-A", out[i].OldValues["wh_id"])
			require.Equal(t, utils.OperationInsert, out[i+1].Type)
			require.Equal(t, "WH-B", out[i+1].Values["wh_id"])
		}
	})
	t.Run("fraction_and_int4_overflow_fail_whole_plan", func(t *testing.T) {
		for _, quantity := range []string{"4.000001", "2147483648.000000", "NaN"} {
			out, err := e.Map(event("inventory_summary_outbox", "inv-bad", nil, inventoryRow("WH-A", "20", quantity, "2")))
			require.Error(t, err, quantity)
			require.Nil(t, out, "a failed second projection must not expose the first one")
		}
		out, err := e.Map(event("inventory_summary_outbox", "inv-boundary", nil, inventoryRow("WH-A", "2147483647.000000", "-2147483648.000000", nil)))
		require.NoError(t, err)
		require.Equal(t, int64(2147483647), out[0].Values["quantity"])
		require.Equal(t, int64(-2147483648), out[1].Values["quantity"])
		require.Nil(t, out[2].Values["quantity"])
	})
	t.Run("current_history_supplier_thd_have_distinct_identity", func(t *testing.T) {
		seen := map[string]bool{}
		for i, table := range []string{"sys_warehouse_outbound_1", "sys_warehouse_outbound_1_history", "mem_supplier_warehouse_outbound", "thd_warehouse_outbound"} {
			out, err := e.Map(event(table, fmt.Sprint("merge-", i), nil, map[string]interface{}{"outbound_id": int64(7)}))
			require.NoError(t, err)
			require.Len(t, out, 1)
			require.Equal(t, table+"_7", out[0].Values["_pk_id"])
			require.False(t, seen[out[0].SourceKey])
			seen[out[0].SourceKey] = true
			types := []int64{1, 1, 2, 3}
			require.Equal(t, types[i], out[0].Values["wh_type"])
			deleted, err := e.Map(event(table, "delete-"+table, map[string]interface{}{"outbound_id": int64(7)}, nil))
			require.NoError(t, err)
			require.Equal(t, out[0].SourceKey, deleted[0].SourceKey)
		}
	})
	t.Run("serial_number_is_part_of_identity", func(t *testing.T) {
		var keys []string
		for _, serial := range []string{"SN-A", "SN-B"} {
			out, err := e.Map(event("sys_warehouse_outbound_product_bo_sn_1", "sn-"+serial, nil, map[string]interface{}{"outbound_id": 7, "upc": "UPC-1", "bo_sn": serial, "quantity": 1}))
			require.NoError(t, err)
			keys = append(keys, out[0].SourceKey)
		}
		require.NotEqual(t, keys[0], keys[1])
	})
	t.Run("soft_delete_flag_change_moves_composite_identity", func(t *testing.T) {
		before := map[string]interface{}{"outbound_id": 7, "tracking_number": "TRACK-1", "delete": 0}
		after := map[string]interface{}{"outbound_id": 7, "tracking_number": "TRACK-1", "delete": 1}
		out, err := e.Map(event("sys_warehouse_outbound_tracking_1", "track", before, after))
		require.NoError(t, err)
		require.Len(t, out, 2)
		require.Equal(t, utils.OperationDelete, out[0].Type)
		require.Equal(t, utils.OperationInsert, out[1].Type)
		require.NotEqual(t, out[0].SourceKey, out[1].SourceKey)
	})
	t.Run("json_big_integer_null_and_bad_input", func(t *testing.T) {
		out, err := e.Map(event("mem_customer_tracking", "json-big", nil, map[string]interface{}{"id": 1, "param": `{"outbound_id":9007199254740993}`}))
		require.NoError(t, err)
		require.Equal(t, int64(9007199254740993), out[0].Values["outbound_id"])
		for _, param := range []interface{}{nil, `{"outbound_id":null}`} {
			out, err := e.Map(event("mem_customer_tracking", "json-null", nil, map[string]interface{}{"id": 1, "param": param}))
			require.NoError(t, err)
			require.Nil(t, out[0].Values["outbound_id"])
		}
		for _, param := range []string{`{broken`, `{}`, `{"outbound_id":1.5}`, `{"outbound_id":9223372036854775808}`} {
			_, err := e.Map(event("mem_customer_tracking", "json-bad", nil, map[string]interface{}{"id": 1, "param": param}))
			require.Error(t, err)
		}
	})
	t.Run("delete_requires_only_keys_not_quantity_or_json", func(t *testing.T) {
		out, err := e.Map(event("inventory_summary_outbox", "inv-delete", map[string]interface{}{"owner_id": 0, "legacy_wh_id": "WH-A", "upc": "UPC-1"}, nil))
		require.NoError(t, err)
		require.Len(t, out, 3)
		for _, mutation := range out {
			require.Equal(t, utils.OperationDelete, mutation.Type)
		}
	})
	t.Run("origin_and_version_guard", func(t *testing.T) {
		message := event("mem_customer_tracking", "loop", nil, map[string]interface{}{"id": 1, "param": `{"outbound_id":1}`})
		message.Origin = "remote-replay"
		out, err := e.Map(message)
		require.NoError(t, err)
		require.Empty(t, out)
		message.Origin, message.MappingVersion = "", "wrong-version"
		_, err = e.Map(message)
		require.ErrorContains(t, err, "version")
	})
}
