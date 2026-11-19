package scenarios

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/pgflo/pg_flo/pkg/jobconfig"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

// Each run creates two private databases and a private NATS stream. The opt-in
// endpoints must be loopback services; deployment connection strings are unused.
func TestInventoryWMSWAL(t *testing.T) {
	binary := os.Getenv("PGFLO_LAB_BINARY")
	if binary == "" {
		t.Skip("set PGFLO_LAB_BINARY and start examples/inventory-wms/compose.yaml for true WAL/NATS E2E")
	}
	binary, err := filepath.Abs(binary)
	require.NoError(t, err)
	job, err := jobconfig.Load(fixtureDir + "job.yaml")
	require.NoError(t, err)
	ctx := context.Background()
	id := fmt.Sprintf("%x", time.Now().UnixNano())
	job.Metadata.Name = "lab-" + id
	job.Source.Group = "lab_" + id
	job.Source.ID, job.Mapping.SourceID = "lab_"+id, "lab_"+id
	job.Source.Database, job.Sink.Database = "pgflo_src_"+id, "pgflo_dst_"+id
	job.Sink.Origin = "pgflo_lab_" + id
	// Override ports only; credentials and loopback hosts are fixed to the lab.
	adminSource, err := pgx.Connect(ctx, "host=127.0.0.1 port=55442 dbname=source_db user=postgres password=pgflo_lab sslmode=disable")
	require.NoError(t, err)
	defer adminSource.Close(ctx)
	adminTarget, err := pgx.Connect(ctx, "host=127.0.0.1 port=55443 dbname=target_db user=postgres password=pgflo_lab sslmode=disable")
	require.NoError(t, err)
	defer adminTarget.Close(ctx)
	defer func() {
		_, _ = adminTarget.Exec(ctx, "SELECT pg_replication_origin_drop($1) WHERE pg_replication_origin_oid($1) IS NOT NULL", job.Sink.Origin)
	}()
	_, err = adminSource.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{job.Source.Database}.Sanitize())
	require.NoError(t, err)
	defer func() {
		_, err := adminSource.Exec(ctx, "DROP DATABASE "+pgx.Identifier{job.Source.Database}.Sanitize()+" WITH (FORCE)")
		if err != nil {
			t.Errorf("cleanup source lab database: %v", err)
		}
	}()
	_, err = adminTarget.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{job.Sink.Database}.Sanitize())
	require.NoError(t, err)
	defer func() {
		_, err := adminTarget.Exec(ctx, "DROP DATABASE "+pgx.Identifier{job.Sink.Database}.Sanitize()+" WITH (FORCE)")
		if err != nil {
			t.Errorf("cleanup target lab database: %v", err)
		}
	}()
	sourceConfig := adminSource.Config().Copy()
	sourceConfig.Database = job.Source.Database
	source, err := pgx.ConnectConfig(ctx, sourceConfig)
	require.NoError(t, err)
	defer source.Close(ctx)
	targetConfig := adminTarget.Config().Copy()
	targetConfig.Database = job.Sink.Database
	target, err := pgx.ConnectConfig(ctx, targetConfig)
	require.NoError(t, err)
	defer target.Close(ctx)
	for _, fixture := range []struct {
		conn *pgx.Conn
		file string
	}{{source, "source.sql"}, {target, "target.sql"}} {
		data, err := os.ReadFile(fixtureDir + fixture.file)
		require.NoError(t, err)
		_, err = fixture.conn.Exec(ctx, strings.ReplaceAll(string(data), "pgflo_inventory_wms_lab", job.Sink.Origin))
		require.NoError(t, err)
	}
	jobData, err := yaml.Marshal(job)
	require.NoError(t, err)
	jobPath := filepath.Join(t.TempDir(), "job.yaml")
	require.NoError(t, os.WriteFile(jobPath, jobData, 0600))

	nc, err := nats.Connect(job.NATS.URL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := nc.JetStream()
	require.NoError(t, err)
	defer func() {
		_ = js.DeleteStream("pgflo_" + job.Source.Group + "_stream")
		_ = js.DeleteKeyValue("pg_flo_state_" + job.Source.Group)
	}()

	var command *exec.Cmd
	var done chan error
	var logFile *os.File
	stop := func() {
		if command != nil {
			_ = command.Process.Kill()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("job did not terminate")
			}
			_ = logFile.Close()
			if t.Failed() {
				data, _ := os.ReadFile(logFile.Name())
				if len(data) > 18000 {
					data = data[len(data)-18000:]
				}
				t.Logf("job failure log:\n%s", data)
			}
			command = nil
		}
	}
	defer stop()
	start := func() {
		logFile, err = os.CreateTemp(t.TempDir(), "pgflo-*.log")
		require.NoError(t, err)
		t.Logf("job log: %s", logFile.Name())
		command = exec.Command(binary, "job", "run", "--job", jobPath)
		command.Stdout, command.Stderr = logFile, logFile
		require.NoError(t, command.Start())
		done = make(chan error, 1)
		go func(c *exec.Cmd) { done <- c.Wait() }(command)
		waitSQL(t, source, "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE database=current_database() AND active)", "source logical slot active")
	}
	execSQL := func(conn *pgx.Conn, query string) {
		_, err := conn.Exec(ctx, query)
		require.NoError(t, err)
	}
	start()
	run := func(name string, test func(*testing.T)) {
		if !t.Run(name, test) {
			t.FailNow()
		}
	}

	run("inventory_atomic_three_table_split_and_key_move", func(t *testing.T) {
		execSQL(source, `INSERT INTO inventory_summary_outbox VALUES(101,0,'WH-A','UPC-1',20,4,2)`)
		waitSQL(t, target, `SELECT (SELECT quantity FROM inventory_rollback.mem_user_inventory WHERE uid=0)=20 AND
		 (SELECT quantity FROM inventory_rollback.mem_user_inventory_lock WHERE uid=0)=4 AND
		 (SELECT quantity FROM inventory_rollback.mem_user_inventory_tracking WHERE uid=0)=2`, "three inventory projections")
		execSQL(source, `UPDATE inventory_summary_outbox SET legacy_wh_id='WH-B',on_hand_qty=-2,reserved_qty=-1,hold_qty=0 WHERE summary_id=101`)
		waitSQL(t, target, `SELECT (SELECT quantity FROM inventory_rollback.mem_user_inventory WHERE wh_id='WH-B')=-2 AND
		 (SELECT count(*) FROM inventory_rollback.mem_user_inventory WHERE wh_id='WH-A')=0 AND
		 (SELECT count(*) FROM inventory_rollback.mem_user_inventory_lock WHERE wh_id='WH-A')=0 AND
		 (SELECT count(*) FROM inventory_rollback.mem_user_inventory_tracking WHERE wh_id='WH-A')=0`, "old warehouse keys removed")
	})
	run("outbound_merge_source_owned_delete", func(t *testing.T) {
		execSQL(source, `BEGIN; INSERT INTO sys_warehouse_outbound_1 VALUES(7); INSERT INTO sys_warehouse_outbound_1_history VALUES(7);
		 INSERT INTO thd_warehouse_outbound VALUES(7); INSERT INTO mem_supplier_warehouse_outbound VALUES(7); COMMIT`)
		waitSQL(t, target, `SELECT count(*)=4 AND min(wh_type)=1 AND max(wh_type)=3 FROM store.sys_warehouse_outbound_m`, "four origins with same id coexist")
		execSQL(source, `DELETE FROM sys_warehouse_outbound_1 WHERE outbound_id=7`)
		waitSQL(t, target, `SELECT count(*)=3 AND count(*) FILTER(WHERE _pk_id='sys_warehouse_outbound_1_7')=0 FROM store.sys_warehouse_outbound_m`, "delete only current source")
	})
	run("composite_serial_and_reserved_word_delete", func(t *testing.T) {
		execSQL(source, `BEGIN; INSERT INTO sys_warehouse_outbound_product_bo_sn_1 VALUES(7,'UPC-1','SN-A',1),(7,'UPC-1','SN-B',2);
		 INSERT INTO sys_warehouse_outbound_tracking_1 VALUES(7,'TRACK-1',0); COMMIT`)
		waitSQL(t, target, `SELECT (SELECT count(*) FROM store.sys_warehouse_outbound_product_bo_sn_m)=2 AND
		 (SELECT count(*) FROM store.sys_warehouse_outbound_tracking_m)=1`, "composite key siblings")
		execSQL(source, `BEGIN; DELETE FROM sys_warehouse_outbound_product_bo_sn_1 WHERE bo_sn='SN-A';
		 UPDATE sys_warehouse_outbound_tracking_1 SET "delete"=1; COMMIT`)
		waitSQL(t, target, `SELECT (SELECT count(*) FROM store.sys_warehouse_outbound_product_bo_sn_m)=1 AND
		 (SELECT bo_sn FROM store.sys_warehouse_outbound_product_bo_sn_m)='SN-B' AND
		 (SELECT count(*) FROM store.sys_warehouse_outbound_tracking_m)=1 AND
		 (SELECT "delete" FROM store.sys_warehouse_outbound_tracking_m)=1`, "composite deletion and flag key migration")
	})
	run("json_exact_bigint_then_explicit_null", func(t *testing.T) {
		execSQL(source, `INSERT INTO mem_customer_tracking VALUES(1,'{"outbound_id":9007199254740993}')`)
		waitSQL(t, target, `SELECT outbound_id=9007199254740993 FROM store.mem_customer_tracking WHERE id=1`, "JSON bigint exact")
		execSQL(source, `UPDATE mem_customer_tracking SET param='{"outbound_id":null}' WHERE id=1`)
		waitSQL(t, target, `SELECT outbound_id IS NULL FROM store.mem_customer_tracking WHERE id=1`, "JSON null persists as SQL NULL")
	})
	run("origin_replay_filtered_but_next_local_write_captured", func(t *testing.T) {
		origin := "remote_" + id
		_, err := source.Exec(ctx, `SELECT pg_replication_origin_create($1)`, origin)
		require.NoError(t, err)
		defer func() {
			_, _ = source.Exec(ctx, `SELECT pg_replication_origin_session_reset()`)
			_, _ = source.Exec(ctx, `SELECT pg_replication_origin_drop($1)`, origin)
		}()
		_, err = source.Exec(ctx, `SELECT pg_replication_origin_session_setup($1)`, origin)
		require.NoError(t, err)
		execSQL(source, `BEGIN; INSERT INTO mem_customer_tracking VALUES(2,'{"outbound_id":2}');
		 SELECT pg_replication_origin_xact_setup('0/1',clock_timestamp()); COMMIT;
		 SELECT pg_replication_origin_session_reset()`)
		execSQL(source, `INSERT INTO mem_customer_tracking VALUES(3,'{"outbound_id":3}')`)
		waitSQL(t, target, `SELECT count(*)=1 FROM store.mem_customer_tracking WHERE id=3`, "local write after origin transaction")
		var echoed int
		require.NoError(t, target.QueryRow(ctx, `SELECT count(*) FROM store.mem_customer_tracking WHERE id=2`).Scan(&echoed))
		require.Zero(t, echoed, "replayed origin transaction must not escape capture filter")
	})
	run("injected_target_error_rolls_back_then_retries", func(t *testing.T) {
		execSQL(target, `ALTER TABLE inventory_rollback.mem_user_inventory_lock ADD CONSTRAINT lab_fail CHECK(quantity<>5)`)
		var beforeLedger int
		require.NoError(t, target.QueryRow(ctx, `SELECT count(*) FROM pgflo_meta.applied_transactions`).Scan(&beforeLedger))
		execSQL(source, `UPDATE inventory_summary_outbox SET on_hand_qty=21,reserved_qty=5,hold_qty=3 WHERE summary_id=101`)
		select {
		case processErr := <-done:
			require.Error(t, processErr, "poison transaction should stop the job")
			_ = logFile.Close()
			command = nil
		case <-time.After(30 * time.Second):
			t.Fatal("worker failed to stop on sink constraint error")
		}
		var valid bool
		require.NoError(t, target.QueryRow(ctx, `SELECT (SELECT quantity FROM inventory_rollback.mem_user_inventory)=-2 AND
		 (SELECT quantity FROM inventory_rollback.mem_user_inventory_lock)=-1 AND
		 (SELECT quantity FROM inventory_rollback.mem_user_inventory_tracking)=0`).Scan(&valid))
		require.True(t, valid, "all three writes must have rolled back")
		var afterLedger int
		require.NoError(t, target.QueryRow(ctx, `SELECT count(*) FROM pgflo_meta.applied_transactions`).Scan(&afterLedger))
		require.Equal(t, beforeLedger, afterLedger, "failed transaction must not be recorded as applied")
		execSQL(target, `ALTER TABLE inventory_rollback.mem_user_inventory_lock DROP CONSTRAINT lab_fail`)
		start()
		waitSQL(t, target, `SELECT (SELECT quantity FROM inventory_rollback.mem_user_inventory)=21 AND
		 (SELECT quantity FROM inventory_rollback.mem_user_inventory_lock)=5 AND
		 (SELECT quantity FROM inventory_rollback.mem_user_inventory_tracking)=3`, "same WAL/JetStream transaction recovered")
	})
	run("replayed_envelope_does_not_reapply_committed_dml", func(t *testing.T) {
		info, err := js.StreamInfo("pgflo_" + job.Source.Group + "_stream")
		require.NoError(t, err)
		stored, err := js.GetMsg(info.Config.Name, info.State.LastSeq)
		require.NoError(t, err)
		// A distinctive target value would be overwritten if the replay escaped
		// the transaction ledger, even though ON CONFLICT itself is an upsert.
		execSQL(target, `UPDATE inventory_rollback.mem_user_inventory SET quantity=99`)
		_, err = js.Publish(stored.Subject, stored.Data)
		require.NoError(t, err)
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			consumer, err := js.ConsumerInfo(info.Config.Name, "pgflo_"+job.Source.Group+"_consumer")
			if err == nil && consumer.NumPending == 0 && consumer.NumAckPending == 0 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		consumer, err := js.ConsumerInfo(info.Config.Name, "pgflo_"+job.Source.Group+"_consumer")
		require.NoError(t, err)
		require.Zero(t, consumer.NumPending)
		require.Zero(t, consumer.NumAckPending)
		var quantity int
		require.NoError(t, target.QueryRow(ctx, `SELECT quantity FROM inventory_rollback.mem_user_inventory`).Scan(&quantity))
		require.Equal(t, 99, quantity)
	})
	run("delete_removes_three_projections", func(t *testing.T) {
		execSQL(source, `DELETE FROM inventory_summary_outbox WHERE summary_id=101`)
		waitSQL(t, target, `SELECT (SELECT count(*) FROM inventory_rollback.mem_user_inventory)=0 AND
		 (SELECT count(*) FROM inventory_rollback.mem_user_inventory_lock)=0 AND
		 (SELECT count(*) FROM inventory_rollback.mem_user_inventory_tracking)=0`, "three-table delete")
	})
}

func waitSQL(t *testing.T, conn *pgx.Conn, query, description string) {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		var ok bool
		lastErr = conn.QueryRow(context.Background(), query).Scan(&ok)
		if lastErr == nil && ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: %v (%s)", description, lastErr, strings.TrimSpace(query))
}
