package worker

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestDecodeEnvelopeAndLegacyMessage(t *testing.T) {
	message := utils.CDCMessage{Type: utils.OperationInsert, Schema: "public", Table: "events", LSN: "0/1"}
	for _, input := range []interface{}{
		utils.CDCEnvelope{Version: 1, TransactionID: "tx", CommitLSN: "0/1", Messages: []utils.CDCMessage{message}},
		message,
	} {
		var buffer bytes.Buffer
		require.NoError(t, gob.NewEncoder(&buffer).Encode(input))
		envelope, err := decodeEnvelope(buffer.Bytes())
		require.NoError(t, err)
		require.Len(t, envelope.Messages, 1)
		require.Equal(t, "events", envelope.Messages[0].Table)
	}
}
