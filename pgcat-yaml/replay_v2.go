package main

import (
	"crypto/sha256"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
)

const replayContextTable = "cdc_meta.tx_context"

type ReplayConfig struct {
	Namespace  string `yaml:"namespace"`
	Direction  string `yaml:"direction"`
	UIDField   string `yaml:"uid-field"`
	PeerOrigin string `yaml:"peer-origin"`
}

type replayMarker struct {
	id, namespace, root, writer, stage string
	uid, epoch                         int64
	barrier                            *string
}

func (c *ReplayConfig) validate(cfg *Config) error {
	if c.Namespace == "" || c.UIDField == "" || c.PeerOrigin == "" || c.PeerOrigin == cfg.Sink.Origin ||
		(c.Direction != "forward" && c.Direction != "reverse") ||
		cfg.Source.Plugin != "pgoutput" || cfg.Source.Snapshot != "seed" || cfg.Ownership != nil {
		return fmt.Errorf("replay-v2 needs namespace, uid-field, direction, distinct peer-origin, pgoutput and seed; legacy ownership is mutually exclusive")
	}
	return nil
}

func parseReplayMarker(row Row) (*replayMarker, error) {
	fields := make([]string, 0, 5)
	for _, name := range []string{"marker_id", "scope_namespace", "root_tx_id", "writer_side", "apply_stage"} {
		value, ok := row[name].(string)
		if !ok || value == "" {
			return nil, fmt.Errorf("missing marker field %s", name)
		}
		fields = append(fields, value)
	}
	uid, uerr := strconv.ParseInt(fmt.Sprint(row["uid"]), 10, 64)
	epoch, eerr := strconv.ParseInt(fmt.Sprint(row["epoch"]), 10, 64)
	if uerr != nil || eerr != nil || uid < 0 || epoch <= 0 {
		return nil, fmt.Errorf("invalid marker UID or epoch")
	}
	marker := &replayMarker{id: fields[0], namespace: fields[1], root: fields[2], writer: fields[3],
		stage: fields[4], uid: uid, epoch: epoch}
	if value := row["barrier_id"]; value != nil {
		barrier, ok := value.(string)
		if !ok || barrier == "" {
			return nil, fmt.Errorf("invalid barrier marker")
		}
		marker.barrier = &barrier
	}
	return marker, nil
}

func (c *ReplayConfig) checkScope(marker *replayMarker, rows ...Row) error {
	for _, row := range rows {
		if row == nil {
			continue
		}
		uid, err := strconv.ParseInt(fmt.Sprint(row[c.UIDField]), 10, 64)
		if err != nil || uid != marker.uid {
			return fmt.Errorf("row UID does not match replay marker")
		}
	}
	return nil
}

func (r *runtime) prepareReplay(tx pgx.Tx, m *replayMarker, origin string, count int) (bool, error) {
	c := r.c.Replay
	if count == 0 && m == nil {
		return false, nil
	}
	if m != nil && m.namespace != c.Namespace && count == 0 {
		return false, nil // Other namespaces may share the marker publication.
	}
	if m == nil || m.namespace != c.Namespace || (m.barrier == nil) != (count > 0) {
		return false, fmt.Errorf("missing/invalid replay-v2 transaction context")
	}
	writer, incoming, echoWriter, echoStage := "MYSQL", "pg-sink", "PG", "pgcat-reverse"
	if c.Direction == "reverse" {
		writer, incoming, echoWriter, echoStage = "PG", "native", "MYSQL", "pgcat-forward"
	}
	if origin == c.PeerOrigin && m.writer == echoWriter && m.stage == echoStage {
		return false, nil
	}
	if origin != "" || m.writer != writer || m.stage != incoming {
		return false, fmt.Errorf("unexpected replay-v2 origin or writer/stage")
	}
	var currentWriter, state string
	var epoch int64
	var barrier *string
	e := tx.QueryRow(r.ctx, `SELECT writer_side,epoch,state,barrier_id
FROM cdc_meta.write_ownership WHERE scope_namespace=$1 AND uid=$2 FOR SHARE`, c.Namespace, m.uid).
		Scan(&currentWriter, &epoch, &state, &barrier)
	if e != nil {
		return false, e
	}
	if currentWriter != m.writer || epoch != m.epoch {
		return false, fmt.Errorf("replay-v2 writer/epoch fenced uid=%d", m.uid)
	}
	if state == "ACTIVE" && m.barrier == nil {
		return true, nil
	}
	if state != "DRAINING" || barrier == nil || (m.barrier != nil && *m.barrier != *barrier) {
		return false, fmt.Errorf("replay-v2 barrier not authorized uid=%d", m.uid)
	}
	var passed bool
	e = tx.QueryRow(r.ctx, `SELECT EXISTS(SELECT FROM cdc_meta.cutover_step WHERE barrier_id=$1 AND channel=$2)`,
		*barrier, r.c.Job).Scan(&passed)
	if passed {
		return false, fmt.Errorf("replay-v2 event after barrier uid=%d", m.uid)
	}
	return e == nil, e
}

func (r *runtime) recordReplay(tx pgx.Tx, m *replayMarker, position string) error {
	id := fmt.Sprintf("%x", sha256.Sum256([]byte(r.c.Job+"\x00"+m.id+"\x00"+position)))
	_, e := tx.Exec(r.ctx, `INSERT INTO cdc_meta.tx_context
(marker_id,scope_namespace,root_tx_id,writer_side,apply_stage,uid,epoch,barrier_id)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, id, m.namespace, m.root, m.writer,
		"pgcat-"+r.c.Replay.Direction, m.uid, m.epoch, m.barrier)
	if e == nil && m.barrier != nil {
		_, e = tx.Exec(r.ctx, `INSERT INTO cdc_meta.cutover_step
(barrier_id,channel,scope_namespace,uid,epoch,source_position)
 VALUES($1,$2,$3,$4,$5,$6)`, *m.barrier, r.c.Job, m.namespace, m.uid, m.epoch, position)
	}
	return e
}
