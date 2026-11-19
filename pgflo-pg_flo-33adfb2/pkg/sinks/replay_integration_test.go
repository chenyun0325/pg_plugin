package sinks

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/pgflo/pg_flo/pkg/mapping"
	"github.com/pgflo/pg_flo/pkg/replay"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/stretchr/testify/require"
)

// Opt in against a loopback lab server with CREATE DATABASE permission. Every
// run creates and removes its own database and replication origins.
func TestReplaySinkPostgres(t *testing.T) {
	dsn := os.Getenv("PGFLO_REPLAY_TEST_DSN")
	if dsn == "" {
		t.Skip("set PGFLO_REPLAY_TEST_DSN for PostgreSQL replay integration tests")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer admin.Close(ctx)
	cfg := admin.Config().Copy()
	require.Contains(t, []string{"127.0.0.1", "localhost", "::1"}, cfg.Host)
	id := fmt.Sprintf("pgflo_it_%x", time.Now().UnixNano())
	_, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{id}.Sanitize())
	require.NoError(t, err)
	defer func() {
		_, e := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{id}.Sanitize()+" WITH (FORCE)")
		require.NoError(t, e)
	}()
	cfg.Database = id
	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	defer conn.Close(ctx)
	execSQL := func(sql string, args ...interface{}) {
		t.Helper()
		_, err := conn.Exec(ctx, sql, args...)
		require.NoError(t, err)
	}
	boolSQL := func(sql string) {
		t.Helper()
		var ok bool
		require.NoError(t, conn.QueryRow(ctx, sql).Scan(&ok))
		require.True(t, ok, sql)
	}
	execSQL(replay.Schema)
	execSQL(`CREATE SCHEMA it;
	 CREATE TABLE it.merged(_pgflo_source text, _pgflo_source_table text, _pgflo_source_pk text,
	 uid bigint, quantity bigint CHECK(quantity<1000), payload jsonb, PRIMARY KEY(_pgflo_source,_pgflo_source_table,_pgflo_source_pk));
	 CREATE TABLE it.raw_a(id bigint PRIMARY KEY,uid bigint,payload jsonb,unowned text DEFAULT 'keep');
	 CREATE TABLE it.raw_b(LIKE it.raw_a INCLUDING ALL);
	 CREATE TABLE it.even(id bigint PRIMARY KEY,uid bigint,quantity bigint);
	 CREATE TABLE it.odd(LIKE it.even INCLUDING ALL);
	 CREATE TABLE it.core(id bigint PRIMARY KEY,uid bigint);
	 CREATE TABLE it.detail(id bigint PRIMARY KEY,quantity bigint);
	 INSERT INTO cdc_meta.write_ownership VALUES('it',0,'MYSQL',1,'ACTIVE',NULL),('it',1,'MYSQL',1,'ACTIVE',NULL);`)
	origin := id + "_fwd"
	execSQL(`SELECT pg_replication_origin_create($1)`, origin)
	defer func() { _, _ = admin.Exec(ctx, `SELECT pg_replication_origin_drop($1)`, origin) }()
	connection := PostgresSinkConfig{TargetHost: cfg.Host, TargetPort: int(cfg.Port), TargetDBName: cfg.Database, TargetUser: cfg.User, TargetPassword: cfg.Password, ReplicationOrigin: origin}
	projection := mapping.Config{Version: "it-v1", SourceID: "original_mysql", Unmatched: "error", Routes: []mapping.RouteConfig{
		{ID: "merge", Sources: []string{"it.raw_a", "it.raw_b"}, Target: "it.merged", Identity: mapping.IdentityConfig{Strategy: "source"}, Columns: map[string]mapping.ColumnConfig{
			"uid": {Source: "uid"}, "payload": {Source: "payload"}, "quantity": {Source: "payload", Transforms: []mapping.TransformConfig{{Function: "json_get", Args: []string{"qty"}}, {Function: "to_int64"}}}}},
		{ID: "even", Sources: []string{"it.raw_a"}, Target: "it.even", When: &mapping.PredicateConfig{Modulo: &mapping.ModuloPredicate{Column: "uid", Divisor: 2, Remainder: 0}}, Identity: mapping.IdentityConfig{Strategy: "mapped", TargetKey: []string{"id"}, DeletePolicy: "delete"}, Columns: map[string]mapping.ColumnConfig{"id": {Source: "id"}, "uid": {Source: "uid"}, "quantity": {Source: "payload", Transforms: []mapping.TransformConfig{{Function: "json_get", Args: []string{"qty"}}, {Function: "to_int64"}}}}},
		{ID: "odd", Sources: []string{"it.raw_a"}, Target: "it.odd", When: &mapping.PredicateConfig{Modulo: &mapping.ModuloPredicate{Column: "uid", Divisor: 2, Remainder: 1}}, Identity: mapping.IdentityConfig{Strategy: "mapped", TargetKey: []string{"id"}, DeletePolicy: "delete"}, Columns: map[string]mapping.ColumnConfig{"id": {Source: "id"}, "uid": {Source: "uid"}, "quantity": {Source: "payload", Transforms: []mapping.TransformConfig{{Function: "json_get", Args: []string{"qty"}}, {Function: "to_int64"}}}}},
		{ID: "core", Sources: []string{"it.raw_a"}, Target: "it.core", Identity: mapping.IdentityConfig{Strategy: "mapped", TargetKey: []string{"id"}, DeletePolicy: "delete"}, Columns: map[string]mapping.ColumnConfig{"id": {Source: "id"}, "uid": {Source: "uid"}}},
		{ID: "detail", Sources: []string{"it.raw_a"}, Target: "it.detail", Identity: mapping.IdentityConfig{Strategy: "mapped", TargetKey: []string{"id"}, DeletePolicy: "delete"}, Columns: map[string]mapping.ColumnConfig{"id": {Source: "id"}, "quantity": {Source: "payload", Transforms: []mapping.TransformConfig{{Function: "json_get", Args: []string{"qty"}}, {Function: "to_int64"}}}}},
	}}
	sink, err := NewReplaySink(connection, replay.Config{Namespace: "it", Direction: "forward", UIDField: "uid", PeerOrigin: "peer", StreamID: "it-source", IncomingStage: "native"}, "it_forward", projection)
	require.NoError(t, err)
	defer sink.Close()
	message := func(table string, typ utils.OperationType, before, after map[string]interface{}) utils.CDCMessage {
		return utils.CDCMessage{Schema: "it", Table: table, Type: typ, Values: after, OldValues: before, ReplicationKey: utils.ReplicationKey{Type: utils.ReplicationKeyPK, Columns: []string{"id"}}}
	}
	row := func(id, uid, qty int64) map[string]interface{} {
		return map[string]interface{}{"id": id, "uid": uid, "payload": map[string]interface{}{"qty": json.Number(fmt.Sprint(qty)), "unknown": "preserve"}, "unowned": "keep"}
	}
	envelope := func(name string, uid int64, messages ...utils.CDCMessage) utils.CDCEnvelope {
		return utils.CDCEnvelope{Version: 2, TransactionID: name, CommitLSN: "0/10", MappingVersion: "it-v1", Context: &utils.ReplayContext{MarkerID: name, Namespace: "it", RootID: name, Writer: "MYSQL", Stage: "native", UID: uid, Epoch: 1}, Messages: messages}
	}
	const bigID int64 = 9007199254740993
	a, b := row(bigID, 0, 5), row(bigID, 0, 6)
	first := envelope("first", 0, message("raw_a", utils.OperationInsert, nil, a), message("raw_b", utils.OperationInsert, nil, b))
	require.NoError(t, sink.WriteEnvelope(first))
	boolSQL(`SELECT count(*)=2 AND min(_pgflo_source)='original_mysql' AND min(_pgflo_source_pk)='[9007199254740993]' FROM it.merged`)
	boolSQL(`SELECT (SELECT quantity FROM it.even)=5 AND (SELECT count(*) FROM it.odd)=0 AND (SELECT uid FROM it.core)=0 AND (SELECT quantity FROM it.detail)=5`)
	t.Log("PASS merge collision isolation, JSON extraction, exact bigint, horizontal and vertical projections")
	// Receipt lookup precedes epoch checks, so old committed transactions remain
	// safely replayable after ownership has advanced.
	execSQL(`UPDATE it.merged SET quantity=99`)
	require.NoError(t, sink.WriteEnvelope(first))
	boolSQL(`SELECT min(quantity)=99 FROM it.merged`)
	changed := first
	changed.Messages = append([]utils.CDCMessage(nil), first.Messages...)
	changed.Messages[0].Values = row(bigID, 0, 7)
	require.ErrorContains(t, sink.WriteEnvelope(changed), "different payload")
	bad := envelope("rollback", 0, message("raw_a", utils.OperationUpdate, a, row(bigID, 0, 10)), message("raw_b", utils.OperationUpdate, b, row(bigID, 0, 1000)))
	require.Error(t, sink.WriteEnvelope(bad))
	boolSQL(`SELECT min(quantity)=99 AND (SELECT count(*) FROM cdc_meta.applied_tx WHERE source_tx_key=encode(sha256('rollback'::bytea),'hex'))=0 FROM it.merged`)
	t.Log("PASS same-identity hash check and complete target transaction rollback")
	// An unchanged TOAST marker is hydrated from the seeded/committed row state.
	toast := message("raw_a", utils.OperationUpdate, nil, nil)
	toast.Columns = []*pglogrepl.RelationMessageColumn{{Name: "id", DataType: 20}, {Name: "uid", DataType: 20}, {Name: "payload", DataType: 3802}, {Name: "unowned", DataType: 25}}
	toast.OldTuple = &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{DataType: 't', Data: []byte("9007199254740993")}, {DataType: 't', Data: []byte("0")}, {DataType: 'u'}, {DataType: 't', Data: []byte("keep")}}}
	toast.NewTuple = &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{DataType: 't', Data: []byte("9007199254740993")}, {DataType: 't', Data: []byte("0")}, {DataType: 'u'}, {DataType: 't', Data: []byte("changed")}}}
	require.NoError(t, sink.WriteEnvelope(envelope("toast", 0, toast)))
	boolSQL(`SELECT quantity=5 AND payload->>'unknown'='preserve' FROM it.merged WHERE _pgflo_source_table='it.raw_a'`)
	wrongUID := envelope("wrong-uid", 0, message("raw_b", utils.OperationInsert, nil, row(9, 1, 4)))
	require.ErrorContains(t, sink.WriteEnvelope(wrongUID), "mixed UID")
	missing := envelope("no-marker", 0, message("raw_b", utils.OperationInsert, nil, row(10, 0, 4)))
	missing.Context = nil
	require.ErrorContains(t, sink.WriteEnvelope(missing), "no replay context")
	deleted := envelope("delete", 0, message("raw_b", utils.OperationDelete, b, nil))
	require.NoError(t, sink.WriteEnvelope(deleted))
	boolSQL(`SELECT count(*)=1 AND min(_pgflo_source_table)='it.raw_a' FROM it.merged`)
	t.Log("PASS TOAST hydration, missing/mixed UID rejection and source-owned delete")
	// Fence, barrier receipt, activation and idempotent retry.
	execSQL(`SELECT cdc_meta.freeze_writer('it',0,'MYSQL',1,'barrier-1')`)
	_, err = conn.Exec(ctx, `SELECT cdc_meta.activate_writer('it',0,'MYSQL',1,'PG',2,'barrier-1',ARRAY['it_forward'])`)
	require.ErrorContains(t, err, "barrier not reached")
	barrier := envelope("barrier-1", 0)
	barrier.Context.Barrier = &barrier.Context.MarkerID
	require.NoError(t, sink.WriteEnvelope(barrier))
	require.ErrorContains(t, sink.WriteEnvelope(envelope("after-barrier", 0, message("raw_a", utils.OperationUpdate, a, a))), "after cutover barrier")
	execSQL(`SELECT cdc_meta.activate_writer('it',0,'MYSQL',1,'PG',2,'barrier-1',ARRAY['it_forward'])`)
	execSQL(`SELECT cdc_meta.activate_writer('it',0,'MYSQL',1,'PG',2,'barrier-1',ARRAY['it_forward'])`)
	require.NoError(t, sink.WriteEnvelope(first))
	require.ErrorContains(t, sink.WriteEnvelope(envelope("stale", 0, message("raw_a", utils.OperationUpdate, a, a))), "fenced")
	t.Log("PASS drain barrier, early activation rejection, stale epoch fence and idempotent activation")

	// Reverse only owned JSON paths into the exact original shard row.
	execSQL(`INSERT INTO it.raw_a VALUES(9007199254740993,0,'{"qty":5,"unknown":"original"}','keep')`)
	reverseOrigin := id + "_rev"
	execSQL(`SELECT pg_replication_origin_create($1)`, reverseOrigin)
	defer func() { _, _ = admin.Exec(ctx, `SELECT pg_replication_origin_drop($1)`, reverseOrigin) }()
	connection.ReplicationOrigin = reverseOrigin
	reverse, err := NewReplaySink(connection, replay.Config{Namespace: "it", Direction: "reverse", UIDField: "uid", PeerOrigin: origin, StreamID: "it-reverse"}, "it_reverse", mapping.Config{Version: "it-v1", SourceID: "new_capture_node", Unmatched: "error", Routes: []mapping.RouteConfig{{ID: "reverse", Sources: []string{"it.merged"}, TargetFrom: "lineage", AllowedTargets: []string{"it.raw_a", "it.raw_b"}, RequireExisting: true, Identity: mapping.IdentityConfig{Strategy: "mapped", TargetKey: []string{"id"}, DeletePolicy: "ignore"}, Columns: map[string]mapping.ColumnConfig{"id": {Source: "__owner_pk_0"}, "uid": {Source: "uid"}}, JSONPatches: map[string][]mapping.JSONPatch{"payload": {{Path: []string{"qty"}, Source: "quantity"}}}}}})
	require.NoError(t, err)
	defer reverse.Close()
	projectionRow := map[string]interface{}{"_pgflo_source": "original_mysql", "_pgflo_source_table": "it.raw_a", "_pgflo_source_pk": "[9007199254740993]", "id": int64(1), "uid": int64(0), "quantity": json.Number("9007199254740993")}
	rev := envelope("reverse-1", 0, message("merged", utils.OperationInsert, nil, projectionRow))
	rev.Context.Writer = "PG"
	rev.Context.Epoch = 2
	require.NoError(t, reverse.WriteEnvelope(rev))
	boolSQL(`SELECT payload->>'qty'='9007199254740993' AND payload->>'unknown'='original' AND unowned='keep' FROM it.raw_a`)
	projectionRow["_pgflo_source_table"] = "it.forbidden"
	rev.TransactionID = "reverse-untrusted"
	rev.Context.MarkerID = "reverse-untrusted"
	require.ErrorContains(t, reverse.WriteEnvelope(rev), "not allowed")
	t.Log("PASS whitelist reverse routing, original bigint PK and JSON patch preserving unrelated fields")
}
