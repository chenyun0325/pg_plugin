package mapping

import (
	"fmt"
	"testing"

	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/stretchr/testify/require"
)

func exampleConfig() Config {
	identity := IdentityConfig{Strategy: "source", SourceKey: []string{"id"}}
	base := map[string]ColumnConfig{
		"order_id":  {Source: "id"},
		"tenant_id": {Source: "tenant_id"},
		"customer_name": {
			Source:     "payload",
			Transforms: []TransformConfig{{Function: "json_get", Args: []string{"customer", "name"}}},
		},
	}
	return Config{
		Version: "v1", SourceID: "source-a", Unmatched: "error", LoopProtection: true,
		Routes: []RouteConfig{
			{ID: "orders-even", Mode: "horizontal", Sources: []string{"public.orders"}, Target: "legacy.orders_0", Identity: identity, Conflict: "upsert", Columns: base, When: &PredicateConfig{Modulo: &ModuloPredicate{Column: "tenant_id", Divisor: 2, Remainder: 0}}},
			{ID: "orders-odd", Mode: "horizontal", Sources: []string{"public.orders"}, Target: "legacy.orders_1", Identity: identity, Conflict: "upsert", Columns: base, When: &PredicateConfig{Modulo: &ModuloPredicate{Column: "tenant_id", Divisor: 2, Remainder: 1}}},
			{ID: "order-detail", Mode: "vertical", Sources: []string{"public.orders"}, Target: "app.order_detail", Identity: identity, Conflict: "upsert", Columns: map[string]ColumnConfig{
				"order_id": {Source: "id"},
				"note":     {Source: "payload", Transforms: []TransformConfig{{Function: "json_get", Args: []string{"note"}}}},
			}},
		},
	}
}

func row(tenant int, customer string) map[string]interface{} {
	return map[string]interface{}{
		"id": int64(9007199254740993), "tenant_id": int64(tenant),
		"payload": fmt.Sprintf(`{"customer":{"name":%q},"note":"hello","keep":true}`, customer),
	}
}

func message(operation utils.OperationType, before, after map[string]interface{}) *utils.CDCMessage {
	return &utils.CDCMessage{
		Type: operation, Schema: "public", Table: "orders", OldValues: before, Values: after,
		ReplicationKey: utils.ReplicationKey{Type: utils.ReplicationKeyPK, Columns: []string{"id"}},
		TransactionID:  "tx-1", LSN: "0/10",
	}
}

func TestHorizontalVerticalJSONLifecycle(t *testing.T) {
	engine, err := NewEngine(exampleConfig())
	require.NoError(t, err)

	insert, err := engine.Map(message(utils.OperationInsert, nil, row(2, "Alice")))
	require.NoError(t, err)
	require.Len(t, insert, 2)
	require.Equal(t, "legacy.orders_0", insert[0].Schema+"."+insert[0].Table)
	require.Equal(t, "Alice", insert[0].Values["customer_name"])
	require.Equal(t, int64(9007199254740993), insert[0].Values["order_id"])
	require.Equal(t, "app.order_detail", insert[1].Schema+"."+insert[1].Table)

	update, err := engine.Map(message(utils.OperationUpdate, row(2, "Alice"), row(3, "Bob")))
	require.NoError(t, err)
	require.Len(t, update, 3)
	require.Equal(t, utils.OperationDelete, update[0].Type)
	require.Equal(t, "legacy.orders_0", update[0].Schema+"."+update[0].Table)
	require.Equal(t, utils.OperationInsert, update[1].Type)
	require.Equal(t, "legacy.orders_1", update[1].Schema+"."+update[1].Table)
	require.Equal(t, "Bob", update[1].Values["customer_name"])
	require.Equal(t, utils.OperationUpdate, update[2].Type)
	require.Equal(t, "app.order_detail", update[2].Schema+"."+update[2].Table)

	deleted, err := engine.Map(message(utils.OperationDelete, row(3, "Bob"), nil))
	require.NoError(t, err)
	require.Len(t, deleted, 2)
	for _, output := range deleted {
		require.Equal(t, utils.OperationDelete, output.Type)
	}
}

func TestMergeIdentityTracksSourceTableAndOwnsDelete(t *testing.T) {
	config := Config{
		SourceID: "legacy-cluster", Unmatched: "error",
		Routes: []RouteConfig{{
			ID: "merge", Mode: "merge", SourcePatterns: []string{`^public\.orders_[0-9]+$`}, Target: "app.orders_all",
			Identity: IdentityConfig{Strategy: "source", SourceKey: []string{"id"}}, Conflict: "upsert",
			Columns: map[string]ColumnConfig{"order_id": {Source: "id"}, "tenant_id": {Source: "tenant_id"}},
		}},
	}
	engine, err := NewEngine(config)
	require.NoError(t, err)
	first := &utils.CDCMessage{Type: utils.OperationInsert, Schema: "public", Table: "orders_0", Values: map[string]interface{}{"id": 1, "tenant_id": 10}, ReplicationKey: utils.ReplicationKey{Type: utils.ReplicationKeyPK, Columns: []string{"id"}}}
	second := *first
	second.Table = "orders_1"
	a, err := engine.Map(first)
	require.NoError(t, err)
	b, err := engine.Map(&second)
	require.NoError(t, err)
	require.NotEqual(t, a[0].SourceKey, b[0].SourceKey)
	require.Equal(t, "public.orders_0", a[0].Values["_pgflo_source_table"])
	require.Equal(t, "public.orders_1", b[0].Values["_pgflo_source_table"])

	deleteMessage := *first
	deleteMessage.Type = utils.OperationDelete
	deleteMessage.OldValues, deleteMessage.Values = first.Values, nil
	deleted, err := engine.Map(&deleteMessage)
	require.NoError(t, err)
	require.Equal(t, a[0].SourceKey, deleted[0].SourceKey)
	require.Equal(t, utils.OperationDelete, deleted[0].Type)
}

func TestLoopProtectionAndCustomFunction(t *testing.T) {
	config := exampleConfig()
	engine, err := NewEngine(config)
	require.NoError(t, err)
	looped := message(utils.OperationInsert, nil, row(2, "Alice"))
	looped.Origin = "pgflo_from_remote"
	outputs, err := engine.Map(looped)
	require.NoError(t, err)
	require.Empty(t, outputs)

	customConfig := Config{SourceID: "a", Routes: []RouteConfig{{
		ID: "custom", Sources: []string{"public.input"}, Target: "public.output", Conflict: "error",
		Identity: IdentityConfig{Strategy: "mapped", TargetKey: []string{"id"}, DeletePolicy: "delete"},
		Columns: map[string]ColumnConfig{
			"id":    {Source: "id"},
			"label": {Source: "name", Transforms: []TransformConfig{{Function: "prefix", Args: []string{"P:"}}}},
		},
	}}}
	custom, err := NewEngine(customConfig)
	require.NoError(t, err)
	require.NoError(t, custom.Register("prefix", func(_ TransformContext, value interface{}, args []string) (interface{}, error) {
		return args[0] + fmt.Sprint(value), nil
	}))
	result, err := custom.Map(&utils.CDCMessage{Type: utils.OperationInsert, Schema: "public", Table: "input", Values: map[string]interface{}{"id": 1, "name": "x"}})
	require.NoError(t, err)
	require.Equal(t, "P:x", result[0].Values["label"])
}

func TestConfigurationRejectsAmbiguousIdentity(t *testing.T) {
	_, err := NewEngine(Config{SourceID: "a", Routes: []RouteConfig{{
		ID: "bad", Sources: []string{"public.a"}, Target: "public.b",
		Identity: IdentityConfig{Strategy: "mapped", TargetKey: []string{"missing"}},
		Columns:  map[string]ColumnConfig{"id": {Source: "id"}},
	}}})
	require.ErrorContains(t, err, "not projected")
}
