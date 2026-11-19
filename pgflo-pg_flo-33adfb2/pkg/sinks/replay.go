package sinks

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pgflo/pg_flo/pkg/mapping"
	"github.com/pgflo/pg_flo/pkg/replay"
	"github.com/pgflo/pg_flo/pkg/utils"
)

type EnvelopeSink interface{ WriteEnvelope(utils.CDCEnvelope) error }
type ManagedSink interface {
	Sink
	Close() error
}

type ReplaySink struct {
	base    *PostgresSink
	config  replay.Config
	channel string
	mapper  *mapping.Engine
	digest  string
}

func NewReplaySink(connection PostgresSinkConfig, config replay.Config, channel string, projection mapping.Config) (*ReplaySink, error) {
	if err := config.Validate(connection.ReplicationOrigin); err != nil {
		return nil, err
	}
	mapper, err := mapping.NewEngine(projection)
	if err != nil {
		return nil, err
	}
	if err = mapper.ValidateFunctions(); err != nil {
		return nil, err
	}
	base, err := NewPostgresSinkWithConfig(connection)
	if err != nil {
		return nil, err
	}
	data, _ := json.Marshal(struct {
		Config  replay.Config
		Mapping mapping.Config
		Origin  string
	}{config, projection, connection.ReplicationOrigin})
	s := &ReplaySink{base: base, config: config, channel: channel, mapper: mapper, digest: fmt.Sprintf("%x", sha256.Sum256(data))}
	if _, err = base.conn.Exec(context.Background(), replay.Schema); err != nil {
		base.Close()
		return nil, err
	}
	_, err = base.conn.Exec(context.Background(), `INSERT INTO pgflo_meta.replay_binding VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, channel, config.StreamID, s.digest)
	if err == nil {
		var stream, digest string
		err = base.conn.QueryRow(context.Background(), `SELECT stream_id,config_hash FROM pgflo_meta.replay_binding WHERE channel=$1`, channel).Scan(&stream, &digest)
		if err == nil && (stream != config.StreamID || digest != s.digest) {
			err = fmt.Errorf("replay channel config/stream changed; use a versioned channel and reviewed baseline")
		}
	}
	if err != nil {
		base.Close()
		return nil, err
	}
	return s, nil
}
func (s *ReplaySink) Close() error { return s.base.Close() }
func (s *ReplaySink) WriteBatch(_ []*utils.CDCMessage) error {
	return fmt.Errorf("replay sink requires complete source envelopes")
}

func (s *ReplaySink) WriteEnvelope(envelope utils.CDCEnvelope) error {
	ctx := context.Background()
	if envelope.Version != 2 || envelope.TransactionID == "" || envelope.CommitLSN == "" {
		return fmt.Errorf("replay requires v2 transaction/commit identity")
	}
	// Timestamps are observation metadata, not part of a source transaction.
	canonical := envelope
	canonical.Messages = append([]utils.CDCMessage(nil), envelope.Messages...)
	for i := range canonical.Messages {
		canonical.Messages[i].EmittedAt = time.Time{}
	}
	data, err := json.Marshal(canonical)
	if err != nil {
		return err
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(data))
	key := fmt.Sprintf("%x", sha256.Sum256([]byte(envelope.TransactionID)))
	conn := s.base.conn
	if _, err = conn.Exec(ctx, `SELECT pg_replication_origin_session_setup($1)`, s.base.replicationOrigin); err != nil {
		return err
	}
	defer conn.Exec(ctx, `SELECT pg_replication_origin_session_reset()`)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// A transaction-scoped channel lock prevents a second consumer racing a
	// freeze/checkpoint. Source order is enforced by one consumer per channel.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, s.channel); err != nil {
		return err
	}
	var previous string
	err = tx.QueryRow(ctx, `SELECT payload_hash FROM cdc_meta.applied_tx WHERE channel=$1 AND source_stream_id=$2 AND source_tx_key=$3`, s.channel, s.config.StreamID, key).Scan(&previous)
	if err == nil {
		if previous != hash {
			return fmt.Errorf("same transaction identity has different payload")
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	m := envelope.Context
	apply := false
	result := "IGNORED"
	if len(envelope.Messages) > 0 && m == nil {
		return fmt.Errorf("business transaction has no replay context")
	}
	if m != nil && m.Namespace != s.config.Namespace {
		if len(envelope.Messages) > 0 {
			return fmt.Errorf("business transaction belongs to another namespace")
		}
	} else if m != nil {
		apply, err = s.authorize(ctx, tx, m, envelope.Origin, len(envelope.Messages))
		if err != nil {
			return err
		}
		result = "PG_ECHO"
		if apply {
			result = "APPLIED"
		}
	}
	for i := range envelope.Messages {
		message := envelope.Messages[i]
		if message.Type == utils.OperationDDL {
			return fmt.Errorf("DDL is not permitted in replay channels")
		}
		before, after, err := s.hydrate(ctx, tx, &message)
		if err != nil {
			return err
		}
		for _, row := range []map[string]interface{}{before, after} {
			if row != nil {
				uid, err := strconv.ParseInt(fmt.Sprint(row[s.config.UIDField]), 10, 64)
				if err != nil || m == nil || uid != m.UID {
					return fmt.Errorf("mixed UID or missing UID in source transaction")
				}
			}
		}
		if apply {
			message.Values, message.OldValues = after, before
			message.Origin = "" // origin/stage already checked at envelope scope
			message.TransactionID = envelope.TransactionID
			message.MappingVersion = envelope.MappingVersion
			outputs, err := s.mapper.Map(&message)
			if err != nil {
				return err
			}
			for _, output := range outputs {
				if err = s.base.applyProjection(ctx, tx, output); err != nil {
					return err
				}
			}
		}
	}
	if apply {
		_, _, stage, _, _ := s.config.Stages()
		marker := fmt.Sprintf("%x", sha256.Sum256([]byte(s.channel+"/"+m.MarkerID+"/"+envelope.CommitLSN)))
		_, err = tx.Exec(ctx, `INSERT INTO cdc_meta.tx_context(marker_id,scope_namespace,root_tx_id,writer_side,apply_stage,uid,epoch,barrier_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, marker, m.Namespace, m.RootID, m.Writer, stage, m.UID, m.Epoch, m.Barrier)
		if err != nil {
			return err
		}
		if m.Barrier != nil {
			_, err = tx.Exec(ctx, `INSERT INTO cdc_meta.cutover_step VALUES($1,$2,$3,$4,$5,$6)`, *m.Barrier, s.channel, m.Namespace, m.UID, m.Epoch, envelope.CommitLSN)
			if err != nil {
				return err
			}
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO cdc_meta.applied_tx VALUES($1,$2,$3,$4,$5)`, s.channel, s.config.StreamID, key, hash, result)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO cdc_meta.apply_progress(channel,source_stream_id,started,committed_position) VALUES($1,$2,true,$3) ON CONFLICT(channel) DO UPDATE SET started=true,committed_position=EXCLUDED.committed_position`, s.channel, s.config.StreamID, envelope.CommitLSN)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_replication_origin_xact_setup($1::pg_lsn,clock_timestamp())`, envelope.CommitLSN); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *ReplaySink) authorize(ctx context.Context, tx pgx.Tx, m *utils.ReplayContext, origin string, count int) (bool, error) {
	writer, incoming, _, peerWriter, peerStage := s.config.Stages()
	if origin == s.config.PeerOrigin && m.Writer == peerWriter && m.Stage == peerStage {
		return false, nil
	}
	if origin != "" || m.Writer != writer || m.Stage != incoming {
		return false, fmt.Errorf("unexpected replay writer/stage/origin")
	}
	if (m.Barrier != nil && count != 0) || (m.Barrier == nil && count == 0) {
		return false, fmt.Errorf("barriers must be context-only transactions")
	}
	var actual, state string
	var epoch int64
	var barrier *string
	err := tx.QueryRow(ctx, `SELECT writer_side,epoch,state,barrier_id FROM cdc_meta.write_ownership WHERE scope_namespace=$1 AND uid=$2 FOR SHARE`, m.Namespace, m.UID).Scan(&actual, &epoch, &state, &barrier)
	if err != nil {
		return false, err
	}
	if actual != m.Writer || epoch != m.Epoch {
		return false, fmt.Errorf("writer/epoch fenced uid=%d event=%s/%d", m.UID, m.Writer, m.Epoch)
	}
	if state == "ACTIVE" && m.Barrier == nil {
		return true, nil
	}
	if state != "DRAINING" || barrier == nil || (m.Barrier != nil && *m.Barrier != *barrier) {
		return false, fmt.Errorf("cutover barrier not authorized")
	}
	var passed bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT FROM cdc_meta.cutover_step WHERE barrier_id=$1 AND channel=$2)`, *barrier, s.channel).Scan(&passed)
	if passed {
		return false, fmt.Errorf("event after cutover barrier")
	}
	return err == nil, err
}

func tupleRow(message *utils.CDCMessage, old bool) (map[string]interface{}, error) {
	if old && message.OldValues != nil {
		return message.OldValues, nil
	}
	if !old && message.Values != nil {
		return message.Values, nil
	}
	tuple := message.NewTuple
	if old {
		tuple = message.OldTuple
	}
	if tuple == nil {
		return nil, nil
	}
	row := map[string]interface{}{}
	for i, column := range message.Columns {
		if i >= len(tuple.Columns) {
			return nil, fmt.Errorf("short row image")
		}
		cell := tuple.Columns[i]
		if cell.DataType == 'u' {
			continue
		}
		var value interface{}
		var err error
		if cell.DataType != 'n' {
			value, err = utils.GlobalPostgreSQLTypeConverter.DecodePostgreSQLValue(cell.Data, column.DataType, 0)
			if err != nil {
				return nil, err
			}
		}
		if n, ok := value.(pgtype.Numeric); ok {
			value, err = n.Value()
			if err != nil {
				return nil, err
			}
		}
		row[column.Name] = value
	}
	return row, nil
}

func (s *ReplaySink) hydrate(ctx context.Context, tx pgx.Tx, message *utils.CDCMessage) (map[string]interface{}, map[string]interface{}, error) {
	old, err := tupleRow(message, true)
	if err != nil {
		return nil, nil, err
	}
	fresh, err := tupleRow(message, false)
	if err != nil {
		return nil, nil, err
	}
	if len(message.ReplicationKey.Columns) == 0 {
		return nil, nil, fmt.Errorf("source primary key required")
	}
	keyOf := func(row map[string]interface{}) (string, error) {
		parts := []interface{}{}
		for _, column := range message.ReplicationKey.Columns {
			v, ok := row[column]
			if !ok || v == nil {
				return "", fmt.Errorf("source key missing %s", column)
			}
			parts = append(parts, v)
		}
		b, err := json.Marshal(parts)
		return string(b), err
	}
	keyRow := old
	if message.Type == utils.OperationInsert {
		keyRow = fresh
	}
	pk, err := keyOf(keyRow)
	if err != nil {
		return nil, nil, err
	}
	table := message.Schema + "." + message.Table
	var stored string
	cached := map[string]interface{}{}
	err = tx.QueryRow(ctx, `SELECT row_data::text FROM pgflo_meta.source_rows WHERE channel=$1 AND source_table=$2 AND source_pk=$3 FOR UPDATE`, s.channel, table, pk).Scan(&stored)
	if err == nil {
		d := json.NewDecoder(bytes.NewBufferString(stored))
		d.UseNumber()
		if err = d.Decode(&cached); err != nil {
			return nil, nil, err
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, err
	}
	before := map[string]interface{}{}
	for k, v := range cached {
		before[k] = v
	}
	for k, v := range old {
		before[k] = v
	}
	after := map[string]interface{}{}
	for k, v := range before {
		after[k] = v
	}
	for k, v := range fresh {
		after[k] = v
	}
	if message.Type == utils.OperationInsert {
		before = nil
	}
	if message.Type == utils.OperationDelete {
		after = nil
	}
	for _, row := range []map[string]interface{}{before, after} {
		if row != nil {
			for _, column := range message.Columns {
				if _, ok := row[column.Name]; !ok {
					return nil, nil, fmt.Errorf("missing/TOAST field %s requires seeded source state", column.Name)
				}
			}
		}
	}
	if before != nil {
		if _, err = tx.Exec(ctx, `DELETE FROM pgflo_meta.source_rows WHERE channel=$1 AND source_table=$2 AND source_pk=$3`, s.channel, table, pk); err != nil {
			return nil, nil, err
		}
	}
	if after != nil {
		next, err := keyOf(after)
		if err != nil {
			return nil, nil, err
		}
		data, err := json.Marshal(after)
		if err != nil {
			return nil, nil, err
		}
		_, err = tx.Exec(ctx, `INSERT INTO pgflo_meta.source_rows VALUES($1,$2,$3,$4::jsonb) ON CONFLICT(channel,source_table,source_pk) DO UPDATE SET row_data=EXCLUDED.row_data`, s.channel, table, next, string(data))
		if err != nil {
			return nil, nil, err
		}
	}
	return before, after, nil
}

func (s *PostgresSink) applyProjection(ctx context.Context, tx pgx.Tx, m *utils.CDCMessage) error {
	if m.Type == utils.OperationDelete {
		return s.handleDelete(tx, m)
	}
	if !m.RequireExisting && len(m.JSONPatches) == 0 {
		if m.Type == utils.OperationUpdate {
			return s.handleUpdate(tx, m)
		}
		return s.handleInsert(tx, m)
	}
	where, args, err := getWhereConditions(m, false, 1)
	if err != nil {
		return err
	}
	query := "SELECT row_to_json(t)::text FROM " + quoteTable(m.Schema, m.Table) + " t WHERE " + strings.Join(where, " AND ") + " FOR UPDATE"
	var raw string
	if err = tx.QueryRow(ctx, query, args...).Scan(&raw); err != nil {
		return fmt.Errorf("required target row %s.%s: %w", m.Schema, m.Table, err)
	}
	var row map[string]interface{}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if err = decoder.Decode(&row); err != nil {
		return err
	}
	for column, patches := range m.JSONPatches {
		doc, _ := row[column].(map[string]interface{})
		if doc == nil {
			if row[column] != nil {
				return fmt.Errorf("JSON patch target must be object")
			}
			doc = map[string]interface{}{}
		}
		for _, patch := range patches {
			if len(patch.Path) == 0 {
				return fmt.Errorf("JSON patch path is empty")
			}
			cur := doc
			for _, part := range patch.Path[:len(patch.Path)-1] {
				next, ok := cur[part].(map[string]interface{})
				if !ok {
					if cur[part] != nil {
						return fmt.Errorf("JSON patch crosses scalar")
					}
					next = map[string]interface{}{}
					cur[part] = next
				}
				cur = next
			}
			cur[patch.Path[len(patch.Path)-1]] = patch.Value
		}
		data, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		m.Values[column] = string(data)
		m.Columns = append(m.Columns, &pglogrepl.RelationMessageColumn{Name: column})
	}
	// Update only projected/owned columns, under the row lock acquired above.
	var sets []string
	for _, column := range m.Columns {
		args = append(args, m.Values[column.Name])
		sets = append(sets, fmt.Sprintf("%s=$%d", quoteIdentifier(column.Name), len(args)))
	}
	_, err = tx.Exec(ctx, "UPDATE "+quoteTable(m.Schema, m.Table)+" SET "+strings.Join(sets, ",")+" WHERE "+strings.Join(where, " AND "), args...)
	return err
}
