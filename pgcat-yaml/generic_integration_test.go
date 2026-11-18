package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

type mappingLab struct {
	t                    *testing.T
	ctx                  context.Context
	db                   *pgx.Conn
	prefix, a, b         string
	runtimes             []*runtime
	slots, origins, jobs []string
}

func newMappingLab(t *testing.T) *mappingLab {
	t.Helper()
	dsn := os.Getenv("PGCAT_TEST_DSN")
	if dsn == "" {
		t.Skip("set PGCAT_TEST_DSN for isolated generic WAL tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	p := "pgy_gen_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	l := &mappingLab{t: t, ctx: ctx, db: db, prefix: p, a: p + "a", b: p + "b"}
	l.exec("CREATE SCHEMA " + q(l.a) + "; CREATE SCHEMA " + q(l.b))
	t.Cleanup(func() {
		for _, r := range l.runtimes {
			r.close()
		}
		ctx := context.Background()
		for _, s := range l.slots {
			_, _ = db.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1`, s)
		}
		for _, o := range l.origins {
			_, _ = db.Exec(ctx, `SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin WHERE roname=$1`, o)
		}
		_, _ = db.Exec(ctx, "DROP PUBLICATION IF EXISTS "+q(l.a+"_pub")+","+q(l.b+"_pub"))
		_, _ = db.Exec(ctx, "DROP SCHEMA "+q(l.a)+","+q(l.b)+" CASCADE")
		for _, table := range []string{"row_state", "checkpoint", "identity_state", "identity_outbox"} {
			_, _ = db.Exec(ctx, "DELETE FROM _pgcat_yaml."+q(table)+" WHERE job=ANY($1)", l.jobs)
		}
		for _, table := range []string{"key_spec", "row_binding", "identity_history", "write_ownership"} {
			_, _ = db.Exec(ctx, "DELETE FROM _pgcat_yaml."+q(table)+" WHERE namespace=$1", p)
		}
		db.Close(ctx)
	})
	return l
}
func (l *mappingLab) exec(sql string, args ...any) {
	l.t.Helper()
	if _, err := l.db.Exec(l.ctx, sql, args...); err != nil {
		l.t.Fatal(err)
	}
}
func (l *mappingLab) assert(sql string, want string, args ...any) {
	l.t.Helper()
	var got string
	if err := l.db.QueryRow(l.ctx, sql, args...).Scan(&got); err != nil || got != want {
		l.t.Fatalf("%s: got %q (%v), want %q", sql, got, err, want)
	}
}
func (l *mappingLab) base(reverse, sidecar bool) *Config {
	src, dst := l.a, l.b
	if reverse {
		src, dst = dst, src
	}
	c := &Config{Version: 1, Job: src + "_job"}
	c.Source.ID = src
	c.Source.DSNEnv = "PGCAT_TEST_DSN"
	c.Source.Publication = src + "_pub"
	c.Source.Slot = src + "_slot"
	c.Source.Plugin = "pgoutput"
	c.Source.Snapshot = "copy"
	c.Sink.ID = dst
	c.Sink.DSNEnv = "PGCAT_TEST_DSN"
	c.Sink.Origin = src + "_origin"
	if sidecar {
		c.Identity.Mode = "sidecar"
		c.Identity.Namespace = l.prefix
		c.Identity.Bootstrap = "local"
	}
	return c
}
func (l *mappingLab) open(c *Config) *runtime {
	l.t.Helper()
	raw, e := yaml.Marshal(c)
	if e != nil {
		l.t.Fatal(e)
	}
	file := filepath.Join(l.t.TempDir(), "job.yaml")
	if e = os.WriteFile(file, raw, 0600); e != nil {
		l.t.Fatal(e)
	}
	parsed, e := loadConfig(file)
	if e != nil {
		l.t.Fatal(e)
	}
	r, e := openRuntime(l.ctx, parsed)
	if e != nil {
		l.t.Fatal(e)
	}
	l.runtimes = append(l.runtimes, r)
	l.slots = append(l.slots, c.Source.Slot)
	l.origins = append(l.origins, c.Sink.Origin)
	l.jobs = append(l.jobs, c.Job)
	if e = r.prepare(); e != nil {
		l.t.Fatal(e)
	}
	return r
}
func (l *mappingLab) poll(r *runtime) {
	l.t.Helper()
	if _, e := r.poll(); e != nil {
		l.t.Fatal(e)
	}
}

func TestGenericMappingWAL(t *testing.T) {
	l := newMappingLab(t)
	a, b := l.a, l.b
	l.exec(fmt.Sprintf(`CREATE TABLE %s.orders_0(id bigint PRIMARY KEY,uid bigint NOT NULL,write_epoch bigint NOT NULL,param jsonb NOT NULL);
 CREATE TABLE %s.orders_1(LIKE %s.orders_0 INCLUDING ALL);
 CREATE TABLE %s.customer(id bigint PRIMARY KEY,name text NOT NULL);
 CREATE TABLE %s.orders(_pk_id text PRIMARY KEY,id bigint NOT NULL,uid bigint NOT NULL,write_epoch bigint NOT NULL,param jsonb NOT NULL,outbound_id bigint NOT NULL);
 CREATE TABLE %s.customer(LIKE %s.customer INCLUDING ALL);
 ALTER TABLE %s.orders_0 REPLICA IDENTITY FULL;ALTER TABLE %s.orders_1 REPLICA IDENTITY FULL;ALTER TABLE %s.customer REPLICA IDENTITY FULL;
 ALTER TABLE %s.orders REPLICA IDENTITY FULL;ALTER TABLE %s.customer REPLICA IDENTITY FULL;
 CREATE PUBLICATION %s FOR TABLE %s.orders_0,%s.orders_1,%s.customer;
 CREATE PUBLICATION %s FOR TABLE %s.orders,%s.customer;
 INSERT INTO %s.orders_0 VALUES(101,1,1,'{"outbound_id":10,"keep":"original"}');
 INSERT INTO %s.orders_1 VALUES(101,2,1,'{"outbound_id":20,"keep":"other"}');
 INSERT INTO %s.customer VALUES(1,'before');`, a, a, a, a, b, b, a, a, a, a, b, b, q(a+"_pub"), a, a, a, q(b+"_pub"), b, b, a, a, a))
	ca := l.base(false, true)
	ca.Routes = []Route{
		{Name: "merge", Source: a + `\.orders_[01]`, Target: b + ".orders", SourceKey: []string{"id"}, Keys: []string{"_pk_id"}, UseTransform: true},
		{Name: "copy", Source: a + `\.customer`, Target: b + ".<>", ReplaceSymbol: "<>", SourceKey: []string{"id"}, Keys: []string{"id"}, IncludeAll: true},
	}
	all, cols, err := parseProjection("*, __table_name__ || '_' || id _pk_id, json(param,'outbound_id') outbound_id")
	if err != nil {
		t.Fatal(err)
	}
	ca.Transforms = []TransformRule{{Source: a + `\.orders_[01]`, SourceKey: []string{"id"}, IncludeAll: all, Columns: cols}}
	ra := l.open(ca)
	if err = ra.init(false); err != nil {
		t.Fatal(err)
	}
	l.assert("SELECT count(*)::text FROM "+q(b)+".orders", "2")
	cb := l.base(true, true)
	cb.Source.Snapshot = "seed"
	cb.Identity.Bootstrap = "require-binding"
	cb.Routes = []Route{
		{Name: "restore-original", Source: b + `\.orders`, TargetFrom: "owner-table", AllowedTargets: []string{a + ".orders_0", a + ".orders_1"}, SourceKey: []string{"_pk_id"}, Keys: []string{"id"}, Columns: map[string]Expr{
			"id": {Function: "json_index", Args: []Expr{{From: "__owner_pk__"}, {Literal: 0}}}, "uid": {From: "uid"}, "write_epoch": {From: "write_epoch"}, "param": {From: "param"}}, JSONPatches: map[string][]JSONPatch{"param": {{Path: []string{"outbound_id"}, Value: Expr{From: "outbound_id"}}}}},
		{Name: "copy-back", Source: b + `\.customer`, Target: a + ".<>", ReplaceSymbol: "<>", SourceKey: []string{"id"}, Keys: []string{"id"}, IncludeAll: true},
	}
	rb := l.open(cb)
	if err = rb.init(false); err != nil {
		t.Fatal(err)
	}
	l.exec("UPDATE " + q(b) + ".orders SET outbound_id=9007199254740993 WHERE _pk_id='orders_0_101'; UPDATE " + q(b) + ".customer SET name='after' WHERE id=1")
	l.poll(rb)
	l.poll(ra)
	l.assert("SELECT (param->>'outbound_id')||':'||(param->>'keep') FROM "+q(a)+".orders_0 WHERE id=101", "9007199254740993:original")
	l.assert("SELECT param->>'outbound_id' FROM "+q(a)+".orders_1 WHERE id=101", "20")
	l.assert("SELECT name FROM "+q(a)+".customer WHERE id=1", "after")
	l.exec("DELETE FROM " + q(b) + ".orders WHERE _pk_id='orders_0_101'")
	l.poll(rb)
	l.poll(ra)
	l.assert("SELECT count(*)::text FROM "+q(a)+".orders_0", "0")
	l.assert("SELECT count(*)::text FROM "+q(a)+".orders_1", "1")
	l.poll(rb)
	l.poll(ra)
	l.assert("SELECT count(*)::text FROM "+q(b)+".orders", "1")
	t.Log("PASS direct 1:1, horizontal N:1, template targets, sidecar owner-table reverse, JSON patch, bigint, delete and origin quiescence without business outbox")
}

func TestOwnershipWAL(t *testing.T) {
	l := newMappingLab(t)
	a, b := l.a, l.b
	l.exec(fmt.Sprintf(`CREATE TABLE %s.events(id bigint PRIMARY KEY,uid bigint NOT NULL,write_epoch bigint NOT NULL,note text);
 CREATE TABLE %s.events(LIKE %s.events INCLUDING ALL);
 ALTER TABLE %s.events ADD COLUMN _sync_source text;ALTER TABLE %s.events ADD COLUMN _sync_table text;ALTER TABLE %s.events ADD COLUMN _sync_pk text;
 ALTER TABLE %s.events REPLICA IDENTITY FULL;CREATE PUBLICATION %s FOR TABLE %s.events;`, a, b, a, b, b, b, a, q(a+"_pub"), a))
	c := l.base(false, false)
	c.Ownership = &OwnershipConfig{Namespace: l.prefix, UIDField: "uid", EpochField: "write_epoch", Writer: "mysql"}
	c.Routes = []Route{{Name: "events", Source: a + `\.events`, Target: b + ".events", SourceKey: []string{"id"}, Keys: []string{"id"}, IncludeAll: true}}
	r := l.open(c)
	if e := r.init(false); e != nil {
		t.Fatal(e)
	}
	l.exec(`INSERT INTO _pgcat_yaml.write_ownership VALUES($1,'1','mysql',1,'active'),($1,'2','mysql',1,'active')`, l.prefix)
	l.exec(`SELECT _pgcat_yaml.assert_writer($1,'1','mysql',1)`, l.prefix)
	if _, e := l.db.Exec(l.ctx, `SELECT _pgcat_yaml.assert_writer($1,'1','pg',1)`, l.prefix); e == nil {
		t.Fatal("SQL adapter accepted wrong writer")
	}
	l.exec("INSERT INTO " + q(a) + ".events VALUES(1,1,1,'old'),(2,2,1,'other')")
	l.poll(r)
	l.exec(`SELECT _pgcat_yaml.freeze_writer($1,'1',1)`, l.prefix)
	tx, err := l.db.Begin(l.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.guardOwnership(tx, nil, Row{"uid": 1, "write_epoch": 1}); err == nil {
		t.Fatal("draining owner accepted")
	}
	_ = tx.Rollback(l.ctx)
	if _, e := l.db.Exec(l.ctx, `SELECT _pgcat_yaml.activate_writer($1,'1',1,'mysql',2,jsonb_build_object($2::text,'FFFFFFFF/FFFFFFFF'))`, l.prefix, c.Job); e == nil {
		t.Fatal("unreached barrier activated")
	}
	var cp string
	if e := l.db.QueryRow(l.ctx, `SELECT end_lsn::text FROM _pgcat_yaml.checkpoint WHERE job=$1`, c.Job).Scan(&cp); e != nil {
		t.Fatal(e)
	}
	l.exec(`SELECT _pgcat_yaml.activate_writer($1,'1',1,'mysql',2,jsonb_build_object($2::text,$3::text))`, l.prefix, c.Job, cp)
	tx, err = l.db.Begin(l.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.guardOwnership(tx, nil, Row{"uid": 1, "write_epoch": 1}); err == nil {
		t.Fatal("old epoch accepted")
	}
	_ = tx.Rollback(l.ctx)
	l.exec("UPDATE " + q(a) + ".events SET write_epoch=2,note='new generation' WHERE id=1")
	l.poll(r)
	l.assert("SELECT note FROM "+q(b)+".events WHERE id=1", "new generation")
	// uid=2 has a different master, while uid=1 remains legitimately MySQL-owned.
	l.exec(`SELECT _pgcat_yaml.freeze_writer($1,'2',1)`, l.prefix)
	l.exec(`SELECT _pgcat_yaml.activate_writer($1,'2',1,'pg',2,jsonb_build_object($2::text,$3::text))`, l.prefix, c.Job, cp)
	if e := l.db.QueryRow(l.ctx, `SELECT end_lsn::text FROM _pgcat_yaml.checkpoint WHERE job=$1`, c.Job).Scan(&cp); e != nil {
		t.Fatal(e)
	}
	l.exec("BEGIN; UPDATE " + q(a) + ".events SET note='must rollback' WHERE id=1; UPDATE " + q(a) + ".events SET note='late old writer' WHERE id=2; COMMIT;")
	if _, e := r.poll(); e == nil || !strings.Contains(e.Error(), "ownership fenced") {
		t.Fatalf("late writer not fenced: %v", e)
	}
	l.assert("SELECT note FROM "+q(b)+".events WHERE id=1", "new generation")
	l.assert(`SELECT end_lsn::text FROM _pgcat_yaml.checkpoint WHERE job=$1`, cp, c.Job)
	t.Log("PASS uid ownership epoch, draining CAS, local checkpoint barrier and whole-source-transaction rollback on stale master")
}
