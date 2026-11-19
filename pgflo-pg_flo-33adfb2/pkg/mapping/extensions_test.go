package mapping

import (
	"fmt"
	"testing"

	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestCompiledExtensionAvailableInNewEngines(t *testing.T) {
	name := "test_versioned_prefix"
	require.NoError(t, RegisterExtension(name, func(ctx TransformContext, value interface{}, args []string) (interface{}, error) {
		return args[0] + ctx.RouteID + ":" + fmt.Sprint(value), nil
	}))
	defer func() {
		extensionRegistry.Lock()
		delete(extensionRegistry.functions, name)
		extensionRegistry.Unlock()
	}()
	config := Config{SourceID: "source", Routes: []RouteConfig{{ID: "r", Sources: []string{"a.t"}, Target: "b.t", Identity: IdentityConfig{Strategy: "mapped", TargetKey: []string{"id"}, DeletePolicy: "delete"}, Columns: map[string]ColumnConfig{"id": {Source: "id"}, "label": {Source: "label", Transforms: []TransformConfig{{Function: name, Args: []string{"v1:"}}}}}}}}
	for i := 0; i < 2; i++ {
		e, err := NewEngine(config)
		require.NoError(t, err)
		out, err := e.Map(&utils.CDCMessage{Type: utils.OperationInsert, Schema: "a", Table: "t", Values: map[string]interface{}{"id": 1, "label": "x"}})
		require.NoError(t, err)
		require.Equal(t, "v1:r:x", out[0].Values["label"])
	}
	require.Error(t, RegisterExtension("json_get", func(TransformContext, interface{}, []string) (interface{}, error) { return nil, nil }))
}
