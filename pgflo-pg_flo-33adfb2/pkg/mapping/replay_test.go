package mapping

import (
	"testing"

	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestLineageReverseAndJSONPatch(t *testing.T) {
	c := Config{SourceID: "capture-b", Routes: []RouteConfig{{ID: "reverse", Sources: []string{"app.orders"}, TargetFrom: "lineage", AllowedTargets: []string{"legacy.orders_1"}, Identity: IdentityConfig{Strategy: "mapped", TargetKey: []string{"id"}, DeletePolicy: "delete"}, Columns: map[string]ColumnConfig{"id": {Source: "__owner_pk_0", Transforms: []TransformConfig{{Function: "to_int64"}}}}, JSONPatches: map[string][]JSONPatch{"payload": {{Path: []string{"customer", "name"}, Source: "customer_name"}}}}}}
	e, err := NewEngine(c)
	require.NoError(t, err)
	row := map[string]interface{}{"_pgflo_source": "a", "_pgflo_source_table": "legacy.orders_1", "_pgflo_source_pk": "[9007199254740993]", "customer_name": "Bob"}
	out, err := e.Map(&utils.CDCMessage{Type: utils.OperationUpdate, Schema: "app", Table: "orders", Values: row, OldValues: row})
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, "orders_1", out[0].Table)
	require.Equal(t, int64(9007199254740993), out[0].Values["id"])
	require.True(t, out[0].RequireExisting)
	require.Equal(t, "Bob", out[0].JSONPatches["payload"][0].Value)
	row["_pgflo_source_table"] = "evil.anything"
	_, err = e.Map(&utils.CDCMessage{Type: utils.OperationInsert, Schema: "app", Table: "orders", Values: row})
	require.ErrorContains(t, err, "not allowed")
}

func TestOriginalLineageSurvivesNewCaptureNode(t *testing.T) {
	e, err := NewEngine(Config{SourceID: "capture-b", Routes: []RouteConfig{{ID: "keep", Sources: []string{"app.orders"}, Target: "archive.orders", Columns: map[string]ColumnConfig{"id": {Source: "id"}}, Identity: IdentityConfig{Strategy: "source", SourceKey: []string{"id"}}}}})
	require.NoError(t, err)
	row := map[string]interface{}{"id": 11, "_pgflo_source": "original-a", "_pgflo_source_table": "legacy.original", "_pgflo_source_pk": "[7]"}
	out, err := e.Map(&utils.CDCMessage{Type: utils.OperationInsert, Schema: "app", Table: "orders", Values: row})
	require.NoError(t, err)
	require.Equal(t, "original-a", out[0].Values["_pgflo_source"])
	require.Equal(t, "[7]", out[0].Values["_pgflo_source_pk"])
}

func TestProtectedLineageAndNullKeyRejected(t *testing.T) {
	_, err := NewEngine(Config{SourceID: "a", Routes: []RouteConfig{{ID: "bad", Sources: []string{"a.t"}, Target: "b.t", Columns: map[string]ColumnConfig{"_pgflo_source": {Literal: "forged"}}}}})
	require.ErrorContains(t, err, "protected lineage")
	e, err := NewEngine(Config{SourceID: "a", Routes: []RouteConfig{{ID: "reverse", Sources: []string{"a.t"}, TargetFrom: "lineage", AllowedTargets: []string{"b.t", "b.other"}, Identity: IdentityConfig{Strategy: "mapped", TargetKey: []string{"id"}, DeletePolicy: "ignore"}, Columns: map[string]ColumnConfig{"id": {Source: "__owner_pk_0"}}}}})
	require.NoError(t, err)
	before := map[string]interface{}{"_pgflo_source": "original", "_pgflo_source_table": "b.t", "_pgflo_source_pk": "[7]"}
	after := cloneMap(before)
	after["_pgflo_source_table"] = "b.other"
	_, err = e.Map(&utils.CDCMessage{Type: utils.OperationUpdate, Schema: "a", Table: "t", OldValues: before, Values: after})
	require.ErrorContains(t, err, "protected lineage changed")
	after = cloneMap(before)
	after["_pgflo_source_pk"] = "[null]"
	_, err = e.Map(&utils.CDCMessage{Type: utils.OperationInsert, Schema: "a", Table: "t", Values: after})
	require.ErrorContains(t, err, "cannot be derived")
}
