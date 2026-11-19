package scenarios

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/pgflo/pg_flo/pkg/jobconfig"
	"github.com/pgflo/pg_flo/pkg/mapping"
	"github.com/pgflo/pg_flo/pkg/replay"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestReplayWALSnapshotToastRestart(t *testing.T) {
	binary, dsn := os.Getenv("PGFLO_REPLAY_BINARY"), os.Getenv("PGFLO_REPLAY_TEST_DSN")
	if binary == "" || dsn == "" {
		t.Skip("set PGFLO_REPLAY_BINARY and PGFLO_REPLAY_TEST_DSN; NATS must listen on loopback :4242")
	}
	binary, err := filepath.Abs(binary)
	require.NoError(t, err)
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer admin.Close(ctx)
	cfg := admin.Config().Copy()
	require.Contains(t, []string{"127.0.0.1", "localhost", "::1"}, cfg.Host)
	id := fmt.Sprintf("rw_%x", time.Now().UnixNano())
	sourceDB, targetDB := id+"_src", id+"_dst"
	for _, name := range []string{sourceDB, targetDB} {
		_, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
		require.NoError(t, err)
		defer func(name string) {
			_, e := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
			require.NoError(t, e)
		}(name)
	}
	cfg.Database = sourceDB
	source, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	defer source.Close(ctx)
	cfg.Database = targetDB
	target, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	defer target.Close(ctx)
	execSQL := func(c *pgx.Conn, sql string) { t.Helper(); _, e := c.Exec(ctx, sql); require.NoError(t, e) }
	for _, c := range []*pgx.Conn{source, target} {
		execSQL(c, replay.Schema)
	}
	execSQL(source, `CREATE SCHEMA app;
	 CREATE TABLE app.orders(id bigint PRIMARY KEY,uid bigint,payload jsonb,label text,
	 _pgflo_source text,_pgflo_source_table text,_pgflo_source_pk text);
	 ALTER TABLE app.orders REPLICA IDENTITY FULL;
	 ALTER TABLE app.orders ALTER COLUMN payload SET STORAGE EXTERNAL;
	 INSERT INTO app.orders SELECT 1,0,jsonb_build_object('customer',jsonb_build_object('name','Alice'),'large',string_agg(md5(i::text),'')),
	 'initial','original','raw.orders','[9007199254740993]' FROM generate_series(1,2000) i;
	 INSERT INTO cdc_meta.write_ownership VALUES('toast_lab',0,'PG',1,'ACTIVE',NULL);`)
	execSQL(target, `CREATE SCHEMA raw;
	 CREATE TABLE raw.orders(id bigint PRIMARY KEY,uid bigint,customer_name text,payload jsonb,unowned text);
	 INSERT INTO raw.orders VALUES(9007199254740993,0,'Alice','{"customer":{"label":"initial"},"unknown":9007199254740993}','keep');
	 INSERT INTO cdc_meta.write_ownership VALUES('toast_lab',0,'PG',1,'ACTIVE',NULL);`)
	origin := id + "_origin"
	defer func() {
		_, _ = admin.Exec(ctx, `SELECT pg_replication_origin_drop($1) WHERE pg_replication_origin_oid($1) IS NOT NULL`, origin)
	}()
	job := jobconfig.Job{APIVersion: "pgflo/v1", Kind: "ReplicationJob", Metadata: jobconfig.Metadata{Name: id}, Source: jobconfig.Source{Type: "postgres", ID: id, Host: cfg.Host, Port: int(cfg.Port), Database: sourceDB, User: cfg.User, Password: cfg.Password, Schema: "app", Tables: []string{"orders", "cdc_meta.tx_context"}, Group: id, Mode: "stream"}, Sink: jobconfig.Sink{Type: "postgres", Host: cfg.Host, Port: int(cfg.Port), Database: targetDB, User: cfg.User, Password: cfg.Password, Origin: origin}, NATS: jobconfig.NATS{URL: "nats://127.0.0.1:4242", MaxAge: "0s", Replicas: 1}, Worker: jobconfig.Worker{BatchSize: 100, FlushInterval: "100ms", MaxDeliver: 20}, Replay: &replay.Config{Namespace: "toast_lab", Direction: "reverse", UIDField: "uid", PeerOrigin: id + "_peer", StreamID: id}, Mapping: mapping.Config{Version: "toast-v1", SourceID: id, Unmatched: "error", Routes: []mapping.RouteConfig{{ID: "restore", Sources: []string{"app.orders"}, TargetFrom: "lineage", AllowedTargets: []string{"raw.orders"}, RequireExisting: true, Identity: mapping.IdentityConfig{Strategy: "mapped", TargetKey: []string{"id"}, DeletePolicy: "ignore"}, Columns: map[string]mapping.ColumnConfig{"id": {Source: "__owner_pk_0", Transforms: []mapping.TransformConfig{{Function: "to_int64"}}}, "uid": {Source: "uid"}, "customer_name": {Source: "payload", Transforms: []mapping.TransformConfig{{Function: "json_get", Args: []string{"customer", "name"}}}}}, JSONPatches: map[string][]mapping.JSONPatch{"payload": {{Path: []string{"customer", "label"}, Source: "label"}}}}}}}
	data, err := yaml.Marshal(job)
	require.NoError(t, err)
	jobPath := filepath.Join(t.TempDir(), "replay.yaml")
	require.NoError(t, os.WriteFile(jobPath, data, 0600))
	setup := exec.Command(binary, "job", "setup", "--job", jobPath)
	out, err := setup.CombinedOutput()
	require.NoError(t, err, string(out))
	nc, err := nats.Connect(job.NATS.URL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := nc.JetStream()
	require.NoError(t, err)
	stream := "pgflo_" + id + "_stream"
	defer func() { _ = js.DeleteStream(stream); _ = js.DeleteKeyValue("pg_flo_state_" + id) }()
	var command *exec.Cmd
	var done chan error
	var logFile *os.File
	stop := func() {
		if command == nil {
			return
		}
		_ = command.Process.Kill()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("job stop timed out")
		}
		_ = logFile.Close()
		if t.Failed() {
			data, _ := os.ReadFile(logFile.Name())
			t.Logf("worker log:\n%s", data)
		}
		command = nil
	}
	defer stop()
	start := func() {
		logFile, err = os.CreateTemp(t.TempDir(), "replay-*.log")
		require.NoError(t, err)
		command = exec.Command(binary, "job", "run", "--job", jobPath)
		command.Stdout, command.Stderr = logFile, logFile
		require.NoError(t, command.Start())
		done = make(chan error, 1)
		go func(c *exec.Cmd) { done <- c.Wait() }(command)
		waitSQL(t, source, `SELECT EXISTS(SELECT FROM pg_replication_slots WHERE database=current_database() AND active)`, "replay slot active")
	}
	write := func(tx, label string) {
		execSQL(source, fmt.Sprintf("BEGIN; SELECT cdc_meta.begin_write('toast_lab',0,'PG',1,'%s'); UPDATE app.orders SET label='%s'; COMMIT", tx, label))
	}
	start()
	write("wal-toast-1", "updated")
	waitSQL(t, target, `SELECT customer_name='Alice' AND payload#>>'{customer,label}'='updated' AND payload->>'unknown'='9007199254740993' AND unowned='keep' FROM raw.orders`, "snapshot hydration and JSON patch")
	info, err := js.StreamInfo(stream)
	require.NoError(t, err)
	stored, err := js.GetMsg(stream, info.State.LastSeq)
	require.NoError(t, err)
	var captured utils.CDCEnvelope
	require.NoError(t, gob.NewDecoder(bytes.NewReader(stored.Data)).Decode(&captured))
	require.NotNil(t, captured.Context)
	require.Equal(t, "wal-toast-1", captured.Context.RootID)
	toastObserved := false
	for _, m := range captured.Messages {
		if m.NewTuple != nil {
			for _, v := range m.NewTuple.Columns {
				toastObserved = toastObserved || v.DataType == 'u'
			}
		}
	}
	require.True(t, toastObserved, "fixture must exercise a real unchanged-TOAST marker")
	t.Log("PASS slot-exported snapshot restores unchanged TOAST from real WAL and preserves unrelated JSON keys")
	stop()
	write("wal-toast-offline", "offline")
	start()
	waitSQL(t, target, `SELECT payload#>>'{customer,label}'='offline' FROM raw.orders`, "offline WAL after restart")
	execSQL(target, `UPDATE raw.orders SET customer_name='sentinel'`)
	_, err = js.Publish(stored.Subject, stored.Data)
	require.NoError(t, err)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		c, e := js.ConsumerInfo(stream, "pgflo_"+id+"_consumer")
		if e == nil && c.NumPending == 0 && c.NumAckPending == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	c, err := js.ConsumerInfo(stream, "pgflo_"+id+"_consumer")
	require.NoError(t, err)
	require.Zero(t, c.NumPending)
	require.Zero(t, c.NumAckPending)
	var sentinel string
	require.NoError(t, target.QueryRow(ctx, `SELECT customer_name FROM raw.orders`).Scan(&sentinel))
	require.Equal(t, "sentinel", sentinel)
	t.Log("PASS kill/restart, offline WAL replay and duplicate envelope with no repeated target DML")
	stop()
	// Loss of an acknowledged source offset/history must not silently create a
	// new live stream at the slot's advanced position.
	_, err = source.Exec(ctx, `SELECT pg_replication_slot_advance(slot_name,pg_current_wal_lsn()) FROM pg_replication_slots WHERE database=current_database()`)
	require.NoError(t, err)
	kv, err := js.KeyValue("pg_flo_state_" + id)
	require.NoError(t, err)
	saved, err := kv.Get("state")
	require.NoError(t, err)
	require.NoError(t, kv.Delete("state"))
	refusedOffset := exec.Command(binary, "job", "run", "--job", jobPath)
	out, err = refusedOffset.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(out), "capture offset missing")
	_, err = kv.Put("state", saved.Value())
	require.NoError(t, err)
	require.NoError(t, js.DeleteMsg(stream, 1))
	refusedHistory := exec.Command(binary, "job", "run", "--job", jobPath)
	out, err = refusedHistory.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(out), "transport history missing")
	t.Log("PASS missing NATS capture offset and deleted transport history fail closed")
	_, err = source.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database=current_database()`)
	require.NoError(t, err)
	refused := exec.Command(binary, "job", "run", "--job", jobPath)
	out, err = refused.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(out), "slot missing")
	t.Log("PASS lost source slot fails closed instead of silently skipping history")
}
