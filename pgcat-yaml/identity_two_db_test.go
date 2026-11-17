package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Opt-in: the admin DSN must point to a disposable test server with CREATEDB.
func TestSidecarTwoDatabaseWAL(t *testing.T) {
	adminDSN := os.Getenv("PGCAT_TEST_ADMIN_DSN")
	if adminDSN == "" {
		t.Skip("set PGCAT_TEST_ADMIN_DSN on a disposable server for two-database validation")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	admin, e := pgx.Connect(ctx, adminDSN)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close(context.Background())
	prefix := "pgy_two_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	var conns []*pgx.Conn
	var runtimes []*runtime
	var created []string
	defer func() {
		for _, r := range runtimes {
			r.close()
		}
		for _, c := range conns {
			c.Close(context.Background())
		}
		// Replication origins are cluster-wide and survive DROP DATABASE.
		for _, origin := range []string{prefix + "_from_a", prefix + "_from_b", prefix + "_external"} {
			var exists bool
			if err := admin.QueryRow(context.Background(), `SELECT pg_replication_origin_oid($1) IS NOT NULL`, origin).Scan(&exists); err != nil {
				t.Errorf("inspect origin: %v", err)
			} else if exists {
				if _, err = admin.Exec(context.Background(), `SELECT pg_replication_origin_drop($1)`, origin); err != nil {
					t.Errorf("cleanup origin: %v", err)
				}
			}
		}
		for _, name := range created {
			if _, err := admin.Exec(context.Background(), "DROP DATABASE "+q(name)+" WITH (FORCE)"); err != nil {
				t.Errorf("cleanup database %s: %v", name, err)
			}
		}
	}()
	connect := func(suffix string) *pgx.Conn {
		name := prefix + suffix
		if _, err := admin.Exec(ctx, "CREATE DATABASE "+q(name)+" TEMPLATE template0"); err != nil {
			t.Fatal(err)
		}
		created = append(created, name)
		dsn := adminDSN + " dbname=" + name
		if strings.HasPrefix(adminDSN, "postgres://") || strings.HasPrefix(adminDSN, "postgresql://") {
			u, err := url.Parse(adminDSN)
			if err != nil {
				t.Fatal(err)
			}
			u.Path = "/" + name
			params := u.Query()
			params.Del("dbname")
			u.RawQuery = params.Encode()
			dsn = u.String()
		}
		cfg, err := pgx.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		c, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
		return c
	}
	a, b := connect("a"), connect("b")
	rewrite := func(s string) string {
		return strings.ReplaceAll(strings.ReplaceAll(s, "orders_sidecar_v1", prefix), "pgcat_sidecar", prefix)
	}
	exec := func(c *pgx.Conn, sql string, args ...any) {
		t.Helper()
		if _, err := c.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	assert := func(c *pgx.Conn, sql, want string) {
		t.Helper()
		var got string
		if err := c.QueryRow(ctx, sql).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s got=%s want=%s", sql, got, want)
		}
	}
	fileSQL := func(c *pgx.Conn, name string) {
		data, err := os.ReadFile("examples/sidecar/" + name)
		if err != nil {
			t.Fatal(err)
		}
		var lines []string
		for _, l := range strings.Split(string(data), "\n") {
			if !strings.HasPrefix(l, "\\") {
				lines = append(lines, l)
			}
		}
		exec(c, rewrite(strings.Join(lines, "\n")))
	}
	fileSQL(a, "schema.sql")
	fileSQL(b, "schema.sql")
	fileSQL(a, "seed.sql")
	t.Setenv("PGCAT_A_DSN", a.Config().ConnString())
	t.Setenv("PGCAT_B_DSN", b.Config().ConnString())
	load := func(name string) *Config {
		data, err := os.ReadFile("examples/sidecar/" + name)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), name)
		if err = os.WriteFile(path, []byte(rewrite(string(data))), 0600); err != nil {
			t.Fatal(err)
		}
		c, err := loadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	ca, cb := load("a-to-b.yaml"), load("b-to-a.yaml")
	open := func(c *Config) *runtime {
		r, err := openRuntime(ctx, c)
		if err != nil {
			t.Fatal(err)
		}
		runtimes = append(runtimes, r)
		if err = r.prepare(); err != nil {
			t.Fatal(err)
		}
		return r
	}
	ra := open(ca)
	if e = ra.init(false); e != nil {
		t.Fatal(e)
	}
	rb := open(cb)
	// B seed must not silently relabel an unbound row as native B data.
	exec(b, "INSERT INTO "+q(prefix+"_b")+`.orders VALUES('rogue',999,2,'rogue',999,'n','{}')`)
	if e = rb.init(false); e == nil || !strings.Contains(e.Error(), "no live binding") {
		t.Fatalf("seed accepted unbound replica: %v", e)
	}
	exec(b, "DELETE FROM "+q(prefix+"_b")+".orders WHERE record_key='rogue'")
	if e = rb.init(true); e != nil {
		t.Fatal(e)
	}
	pump := func() {
		for i := 0; i < 3; i++ {
			if _, err := ra.poll(); err != nil {
				t.Fatal(err)
			}
			if _, err := rb.poll(); err != nil {
				t.Fatal(err)
			}
		}
	}
	pump()
	assert(b, "SELECT count(*)::text FROM "+q(prefix+"_b")+".orders", "2")
	exec(b, "UPDATE "+q(prefix+"_b")+".orders SET tenant_id=3,customer_name='two-database' WHERE order_id=101")
	pump()
	assert(a, "SELECT count(*)::text FROM "+q(prefix+"_a")+".orders_0 WHERE id=101", "0")
	assert(a, "SELECT param->'customer'->>'name' FROM "+q(prefix+"_a")+".orders_1 WHERE id=101", "two-database")
	exec(a, "UPDATE "+q(prefix+"_a")+".order_detail SET note='outbox-retry' WHERE order_id=101")
	ra.afterOutboxSourceCommit = func() error { return fmt.Errorf("source-commit-before-ack") }
	if _, e = ra.poll(); e == nil {
		t.Fatal("expected outbox ack failure")
	}
	assert(b, "SELECT note FROM "+q(prefix+"_b")+".orders WHERE order_id=101", "outbox-retry")
	ra.close()
	ra = open(ca)
	pump()
	assert(b, "SELECT count(*)::text FROM _pgcat_yaml.identity_outbox WHERE job='"+ca.Job+"'", "0")
	assert(a, "SELECT param->>'keep' FROM "+q(prefix+"_a")+".orders_1 WHERE id=101", "preserve me")
	// Source histories live in A; checkpoint/identity_state for its job live in B.
	assert(a, "SELECT (count(*)>0)::text FROM _pgcat_yaml.row_binding WHERE namespace='"+prefix+"' AND node_id='legacy'", "true")
	assert(b, "SELECT (count(*)>0)::text FROM _pgcat_yaml.identity_state WHERE job='"+ca.Job+"'", "true")
	for i := 0; i < 3; i++ {
		x, err := ra.poll()
		if err != nil {
			t.Fatal(err)
		}
		y, err := rb.poll()
		if err != nil {
			t.Fatal(err)
		}
		if i == 2 && (x || y) {
			t.Fatal("two-database replication ring")
		}
	}
	// A remote-origin DML from a tool outside our identity protocol must fail safe.
	pos, _, err := rb.position()
	if err != nil {
		t.Fatal(err)
	}
	var origin string
	if err = b.QueryRow(ctx, "SELECT pg_replication_origin_create($1)::text", prefix+"_external").Scan(&origin); err != nil {
		t.Fatal(err)
	}
	exec(b, "SELECT pg_replication_origin_session_setup($1)", prefix+"_external")
	exec(b, "BEGIN; SELECT pg_replication_origin_xact_setup('0/123'::pg_lsn,clock_timestamp()); UPDATE "+q(prefix+"_b")+".orders SET note='untracked' WHERE order_id=101; COMMIT")
	exec(b, "SELECT pg_replication_origin_session_reset()")
	if _, e = rb.poll(); e == nil || !strings.Contains(e.Error(), "lacks matching") {
		t.Fatalf("untracked remote DML accepted: %v", e)
	}
	after, _, err := rb.position()
	if err != nil || after != pos {
		t.Fatal("invalid remote DML advanced checkpoint", err)
	}
	t.Log("PASS two separate databases: copy, strict seed, shard relocation, source-outbox crash/restart, quiescence, missing lineage rejection")
}
