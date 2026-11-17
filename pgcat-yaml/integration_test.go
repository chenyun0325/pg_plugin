package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPostgresWALBidirectional(t *testing.T) {
	dsn := os.Getenv("PGCAT_TEST_DSN")
	if dsn == "" {
		t.Skip("set PGCAT_TEST_DSN for isolated real-WAL test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()
	db, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close(context.Background())
	prefix := "pgy_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	a, b := prefix+"a", prefix+"b"
	jobA, jobB := prefix+"_ja", prefix+"_jb"
	originA, originB := prefix+"_oa", prefix+"_ob"
	var ra, rb *runtime
	defer func() {
		if ra != nil {
			ra.close()
		}
		if rb != nil {
			rb.close()
		}
		cleanup := context.Background()
		for _, slot := range []string{a + "_v1", b + "_v1"} {
			var exists bool
			_ = db.QueryRow(cleanup, `SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)`, slot).Scan(&exists)
			if exists {
				_, _ = db.Exec(cleanup, `SELECT pg_drop_replication_slot($1)`, slot)
			}
		}
		for _, origin := range []string{originA, originB} {
			var exists bool
			_ = db.QueryRow(cleanup, `SELECT pg_replication_origin_oid($1) IS NOT NULL`, origin).Scan(&exists)
			if exists {
				_, _ = db.Exec(cleanup, `SELECT pg_replication_origin_drop($1)`, origin)
			}
		}
		_, _ = db.Exec(cleanup, "DROP PUBLICATION IF EXISTS "+q(a+"_pub")+","+q(b+"_pub"))
		_, _ = db.Exec(cleanup, "DROP SCHEMA IF EXISTS "+q(a)+","+q(b)+" CASCADE")
		_, _ = db.Exec(cleanup, `DELETE FROM _pgcat_yaml.row_state WHERE job=ANY($1)`, []string{jobA, jobB})
		_, _ = db.Exec(cleanup, `DELETE FROM _pgcat_yaml.checkpoint WHERE job=ANY($1)`, []string{jobA, jobB})
	}()
	rewrite := func(s string) string {
		s = strings.ReplaceAll(s, "pgcat_demo_a", a)
		s = strings.ReplaceAll(s, "pgcat_demo_b", b)
		s = strings.ReplaceAll(s, "demo_a_to_b_v1", jobA)
		s = strings.ReplaceAll(s, "demo_b_to_a_v1", jobB)
		s = strings.ReplaceAll(s, "pgcat_demo_from_a", originA)
		s = strings.ReplaceAll(s, "pgcat_demo_from_b", originB)
		return s
	}
	execFile := func(path string) {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var lines []string
		for _, line := range strings.Split(string(data), "\n") {
			if !strings.HasPrefix(line, "\\") {
				lines = append(lines, line)
			}
		}
		if _, err = db.Exec(ctx, rewrite(strings.Join(lines, "\n"))); err != nil {
			t.Fatal(err)
		}
	}
	execFile("examples/schema.sql")
	execFile("examples/seed.sql")
	t.Setenv("PGCAT_A_DSN", dsn)
	t.Setenv("PGCAT_B_DSN", dsn)
	load := func(path string) *Config {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file := filepath.Join(t.TempDir(), "job.yaml")
		if err = os.WriteFile(file, []byte(rewrite(string(data))), 0600); err != nil {
			t.Fatal(err)
		}
		c, err := loadConfig(file)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	ca, cb := load("examples/a-to-b.yaml"), load("examples/b-to-a.yaml")
	ra, e = openRuntime(ctx, ca)
	if e != nil {
		t.Fatal(e)
	}
	if e = ra.prepare(); e != nil {
		t.Fatal(e)
	}
	if e = ra.init(false); e != nil {
		t.Fatal(e)
	}
	rb, e = openRuntime(ctx, cb)
	if e != nil {
		t.Fatal(e)
	}
	if e = rb.prepare(); e != nil {
		t.Fatal(e)
	}
	if e = rb.init(false); e != nil {
		t.Fatal(e)
	}
	pump := func() {
		for i := 0; i < 3; i++ {
			if _, err := ra.poll(); err != nil {
				t.Fatal("A poll:", err)
			}
			if _, err := rb.poll(); err != nil {
				t.Fatal("B poll:", err)
			}
		}
	}
	scalar := func(query string) string {
		var v string
		if err := db.QueryRow(ctx, query).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	assert := func(query, want string) {
		t.Helper()
		if got := scalar(query); got != want {
			t.Fatalf("%s got %s want %s", query, got, want)
		}
	}
	assert("SELECT count(*)::text FROM "+q(b)+".orders", "2")
	assert("SELECT outbound_id::text FROM "+q(b)+".orders WHERE order_id=101", "9007199254740993")
	// Source-local CDC INSERT, including a large TOAST document with an unchanged update.
	_, e = db.Exec(ctx, "INSERT INTO "+q(a)+`.orders_0(id,tenant_id,param) VALUES(303,4,jsonb_build_object('customer',jsonb_build_object('name','Toast'),'outbound_id',303,'note','large','blob',repeat(md5(random()::text),10000)))`)
	if e != nil {
		t.Fatal(e)
	}
	pump()
	_, e = db.Exec(ctx, "UPDATE "+q(a)+".orders_0 SET tenant_id=6 WHERE id=303")
	if e != nil {
		t.Fatal(e)
	}
	pump()
	assert("SELECT tenant_id::text FROM "+q(b)+".orders WHERE order_id=303", "6")
	// Switch writing this entity to B (single-writer ownership). Move shard 0 -> 1.
	_, e = db.Exec(ctx, "UPDATE "+q(b)+".orders SET tenant_id=3,customer_name='Alice revised',note='changed' WHERE order_id=101")
	if e != nil {
		t.Fatal(e)
	}
	pump()
	assert("SELECT count(*)::text FROM "+q(a)+".orders_0 WHERE id=101", "0")
	assert("SELECT param->'customer'->>'name' FROM "+q(a)+".orders_1 WHERE id=101", "Alice revised")
	assert("SELECT param->>'keep' FROM "+q(a)+".orders_1 WHERE id=101", "preserve me")
	assert("SELECT param->'customer'->>'tier' FROM "+q(a)+".orders_1 WHERE id=101", "gold")
	assert("SELECT note FROM "+q(a)+".order_detail WHERE order_id=101", "changed")
	// A vertical projection changes one owned field and preserves the aggregate.
	_, e = db.Exec(ctx, "UPDATE "+q(a)+".order_detail SET note='detail edit' WHERE order_id=101")
	if e != nil {
		t.Fatal(e)
	}
	pump()
	assert("SELECT payload->>'note' FROM "+q(b)+".orders WHERE order_id=101", "detail edit")
	_, e = db.Exec(ctx, "DELETE FROM "+q(a)+".order_detail WHERE order_id=101")
	if e != nil {
		t.Fatal(e)
	}
	pump()
	assert("SELECT count(*)::text FROM "+q(b)+".orders WHERE order_id=101", "1")
	assert("SELECT (note IS NULL)::text FROM "+q(b)+".orders WHERE order_id=101", "true")
	// Delete the entity at B: both physical projections are removed at A.
	_, e = db.Exec(ctx, "DELETE FROM "+q(b)+".orders WHERE order_id=101")
	if e != nil {
		t.Fatal(e)
	}
	pump()
	assert("SELECT count(*)::text FROM "+q(a)+".orders_1 WHERE id=101", "0")
	assert("SELECT count(*)::text FROM "+q(a)+".order_detail WHERE order_id=101", "0")
	// Same raw ID in another source table remains a distinct aggregate.
	_, e = db.Exec(ctx, "INSERT INTO "+q(a)+`.orders_1(id,tenant_id,param) VALUES(303,7,'{"customer":{"name":"other"},"outbound_id":304,"note":"other-source"}')`)
	if e != nil {
		t.Fatal(e)
	}
	pump()
	assert("SELECT count(*)::text FROM "+q(b)+".orders WHERE order_id=303", "2")
	// With no business writes the two origin-aware consumers become idle, not a ring.
	for i := 0; i < 4; i++ {
		x, err := ra.poll()
		if err != nil {
			t.Fatal(err)
		}
		y, err := rb.poll()
		if err != nil {
			t.Fatal(err)
		}
		if i == 3 && (x || y) {
			t.Fatal("replication ring did not quiesce")
		}
	}
	// Restart: reconnect, persisted row state and LSN, no duplicate effects.
	ra.close()
	ra = nil
	ra, e = openRuntime(ctx, ca)
	if e != nil {
		t.Fatal(e)
	}
	if e = ra.prepare(); e != nil {
		t.Fatal(e)
	}
	if _, e = ra.poll(); e != nil {
		t.Fatal(e)
	}
	assert("SELECT count(*)::text FROM "+q(b)+".orders WHERE order_id=303", "2")
	// Target-side failure must not commit a checkpoint or source mirror row.
	beforePos := scalar("SELECT end_lsn::text FROM _pgcat_yaml.checkpoint WHERE job='" + jobA + "'")
	_, e = db.Exec(ctx, "ALTER TABLE "+q(b)+".orders ADD CONSTRAINT blocked_note CHECK(note IS DISTINCT FROM 'blocked')")
	if e != nil {
		t.Fatal(e)
	}
	_, e = db.Exec(ctx, "INSERT INTO "+q(a)+`.orders_0(id,tenant_id,param) VALUES(404,4,'{"customer":{"name":"retry"},"outbound_id":404,"note":"blocked"}')`)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = ra.poll(); e == nil {
		t.Fatal("expected target constraint failure")
	}
	assert("SELECT end_lsn::text FROM _pgcat_yaml.checkpoint WHERE job='"+jobA+"'", beforePos)
	assert("SELECT count(*)::text FROM "+q(b)+".orders WHERE order_id=404", "0")
	_, e = db.Exec(ctx, "ALTER TABLE "+q(b)+".orders DROP CONSTRAINT blocked_note")
	if e != nil {
		t.Fatal(e)
	}
	// Commit then interrupt before slot advancement. Retrying must not reapply.
	ra.beforeAdvance = func() error { return fmt.Errorf("injected post-commit interruption") }
	if _, e = ra.poll(); e == nil {
		t.Fatal("expected injected post-commit interruption")
	}
	assert("SELECT count(*)::text FROM "+q(b)+".orders WHERE order_id=404", "1")
	ra.beforeAdvance = nil
	if _, e = ra.poll(); e != nil {
		t.Fatal(e)
	}
	assert("SELECT count(*)::text FROM "+q(b)+".orders WHERE order_id=404", "1")
	t.Log(fmt.Sprintf("PASS actual PG WAL: snapshot, JSON int64, TOAST, CDC, shard relocation, vertical patch/delete, provenance, loop quiescence, restart, rollback and post-commit replay (%s)", prefix))
}
