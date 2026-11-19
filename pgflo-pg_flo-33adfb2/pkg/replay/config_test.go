package replay

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContextValidation(t *testing.T) {
	row := map[string]interface{}{"marker_id": "x", "scope_namespace": "orders", "root_tx_id": "root", "writer_side": "PG", "apply_stage": "native", "uid": int64(0), "epoch": int64(2), "barrier_id": nil}
	m, err := ParseContext(row)
	require.NoError(t, err)
	require.Zero(t, m.UID)
	require.Equal(t, int64(2), m.Epoch)
	row["epoch"] = 0
	_, err = ParseContext(row)
	require.Error(t, err)
	c := Config{Namespace: "orders", Direction: "reverse", UIDField: "uid", PeerOrigin: "a", StreamID: "v1"}
	require.NoError(t, c.Validate("b"))
	require.Error(t, c.Validate("a"))
}
