// Package replay implements the shared UID-fenced CDC transaction protocol.
package replay

import (
	"fmt"
	"strconv"

	"github.com/pgflo/pg_flo/pkg/utils"
)

const ContextTable = "cdc_meta.tx_context"

type Config struct {
	Namespace     string `yaml:"namespace"`
	Direction     string `yaml:"direction"`
	UIDField      string `yaml:"uid-field"`
	PeerOrigin    string `yaml:"peer-origin"`
	StreamID      string `yaml:"source-stream-id"`
	IncomingStage string `yaml:"incoming-stage,omitempty"`
	// CaptureSignature is derived from the source address/table set by jobconfig;
	// operators cannot supply it in YAML. It binds a baseline to its capture scope.
	CaptureSignature string `yaml:"-"`
}

func (c *Config) Validate(origin string) error {
	if c.Namespace == "" || c.UIDField == "" || c.StreamID == "" || c.PeerOrigin == "" || c.PeerOrigin == origin || origin == "" {
		return fmt.Errorf("replay-v2 requires namespace, uid-field, source-stream-id and distinct peer/sink origins")
	}
	if c.Direction != "forward" && c.Direction != "reverse" {
		return fmt.Errorf("replay-v2 direction must be forward or reverse")
	}
	if c.IncomingStage != "" && c.IncomingStage != "native" && c.IncomingStage != "pg-sink" {
		return fmt.Errorf("invalid incoming-stage")
	}
	return nil
}

// Stage names are the existing four-hop wire protocol, shared with Java
// MySQL/Flink adapters. They describe a hop, not a pgcat binary dependency.
func (c Config) Stages() (writer, incoming, outgoing, peerWriter, peerStage string) {
	writer, incoming, outgoing, peerWriter, peerStage = "MYSQL", "pg-sink", "pgcat-forward", "PG", "pgcat-reverse"
	if c.Direction == "reverse" {
		writer, incoming, outgoing, peerWriter, peerStage = "PG", "native", "pgcat-reverse", "MYSQL", "pgcat-forward"
	}
	if c.IncomingStage != "" {
		incoming = c.IncomingStage
	}
	return
}

func ParseContext(row map[string]interface{}) (*utils.ReplayContext, error) {
	get := func(key string) string { v, _ := row[key].(string); return v }
	m := &utils.ReplayContext{MarkerID: get("marker_id"), Namespace: get("scope_namespace"), RootID: get("root_tx_id"), Writer: get("writer_side"), Stage: get("apply_stage")}
	var err error
	m.UID, err = strconv.ParseInt(fmt.Sprint(row["uid"]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid context UID")
	}
	m.Epoch, err = strconv.ParseInt(fmt.Sprint(row["epoch"]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid context epoch")
	}
	if v := row["barrier_id"]; v != nil {
		s, ok := v.(string)
		if !ok || s == "" {
			return nil, fmt.Errorf("invalid barrier")
		}
		m.Barrier = &s
	}
	if m.MarkerID == "" || m.Namespace == "" || m.RootID == "" || m.Stage == "" || m.UID < 0 || m.Epoch <= 0 || (m.Writer != "MYSQL" && m.Writer != "PG") {
		return nil, fmt.Errorf("incomplete replay context")
	}
	return m, nil
}
