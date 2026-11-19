package replicator

import (
	"context"

	"github.com/jackc/pglogrepl"
)

// StreamReplicator handles PostgreSQL logical replication streaming
type StreamReplicator struct {
	*BaseReplicator
}

// NewStreamReplicator creates a new stream replicator instance
func NewStreamReplicator(base *BaseReplicator) *StreamReplicator {
	return &StreamReplicator{
		BaseReplicator: base,
	}
}

// Start begins the stream replication process
func (r *StreamReplicator) Start(ctx context.Context) error {
	if err := r.BaseReplicator.Start(ctx); err != nil {
		return err
	}

	startLSN, err := r.GetLastState()
	if err != nil {
		if r.Config.CaptureContext {
			return err
		}
		r.Logger.Warn().Err(err).Msg("Failed to get last LSN, starting from 0")
		startLSN = pglogrepl.LSN(0)
	}
	r.LastLSN = startLSN

	r.Logger.Info().Str("startLSN", startLSN.String()).Msg("Starting replication")

	// Return only after the WAL reader has stopped. The caller may then close
	// replication connections and persist the final position without racing it.
	return r.StartReplicationFromLSN(ctx, startLSN, r.stopChan)
}

// Stop stops the stream replicator
func (r *StreamReplicator) Stop(ctx context.Context) error {
	return r.BaseReplicator.Stop(ctx)
}
