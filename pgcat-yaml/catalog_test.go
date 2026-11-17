package main

import (
	"context"
	"github.com/jackc/pgx/v5"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestPostgresCatalogGuard(t *testing.T) {
	dsn := os.Getenv("PGCAT_TEST_DSN")
	if dsn == "" {
		t.Skip("set PGCAT_TEST_DSN")
	}
	ctx := context.Background()
	db, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close(ctx)
	schema := "pgy_guard_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	pub := schema + "_pub"
	table := schema + ".orders"
	defer func() {
		_, _ = db.Exec(ctx, "DROP PUBLICATION IF EXISTS "+q(pub))
		_, _ = db.Exec(ctx, "DROP SCHEMA IF EXISTS "+q(schema)+" CASCADE")
	}()
	_, e = db.Exec(ctx, "CREATE SCHEMA "+q(schema)+"; CREATE TABLE "+qTable(table)+"(id bigint PRIMARY KEY,tenant_id bigint NOT NULL,note text);ALTER TABLE "+qTable(table)+" REPLICA IDENTITY FULL; CREATE PUBLICATION "+q(pub)+" FOR TABLE "+qTable(table))
	if e != nil {
		t.Fatal(e)
	}
	if e = validateSourceTable(ctx, db, pub, table, []string{"id"}); e != nil {
		t.Fatal(e)
	}
	if e = validateSourceTable(ctx, db, pub, table, []string{"tenant_id"}); e == nil {
		t.Fatal("nonunique source key accepted")
	}
	if _, e = db.Exec(ctx, "ALTER PUBLICATION "+q(pub)+" SET TABLE "+qTable(table)+" WHERE (tenant_id>0)"); e != nil {
		t.Fatal(e)
	}
	if e = validateSourceTable(ctx, db, pub, table, []string{"id"}); e == nil {
		t.Fatal("filtered publication accepted")
	}
	if _, e = db.Exec(ctx, "ALTER PUBLICATION "+q(pub)+" SET TABLE "+qTable(table)+"(id,tenant_id)"); e != nil {
		t.Fatal(e)
	}
	if e = validateSourceTable(ctx, db, pub, table, []string{"id"}); e == nil {
		t.Fatal("partial column publication accepted")
	}
}
