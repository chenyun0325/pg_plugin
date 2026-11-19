package jobconfig

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReplayConfigBindsCaptureScopeAndRejectsUnsafeChanges(t *testing.T) {
	j, err := Load("../../examples/uid-replay/forward.yaml")
	require.NoError(t, err)
	initial := j.Replay.CaptureSignature
	j.Source.Password = "rotated-secret"
	require.NoError(t, j.Validate())
	require.Equal(t, initial, j.Replay.CaptureSignature)
	j.Source.Database = "other_database"
	require.NoError(t, j.Validate())
	require.NotEqual(t, initial, j.Replay.CaptureSignature)
	j.NATS.MaxAge = "24h"
	require.ErrorContains(t, j.Validate(), "max-age=0s")
	j.NATS.MaxAge = "0s"
	j.Source.Tables = append(j.Source.Tables, "inventory_legacy.stock")
	require.ErrorContains(t, j.Validate(), "duplicate replay table")
	j.Source.Tables = j.Source.Tables[:len(j.Source.Tables)-1]
	j.Source.Group = "invalid'group"
	require.ErrorContains(t, j.Validate(), "source.group")
}
