// Run schema.sql first, then PG_DSN=... go run .
// This tests new adapter/planner/sink contracts; it is not an upstream WAL/NATS E2E.
package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib"
	"os"
	m "pgmapping"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		return fmt.Errorf("PG_DSN required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	s := m.SQLStore{Conn: conn, Origin: "mapping_from_b"}
	e := m.ExampleEngine()
	acks := 0
	old := m.Row{"id": int64(1), "tenant_id": int64(2), "payload": map[string]any{"customer": map[string]any{"name": "Alice"}, "note": "hello"}}
	new := m.Row{"id": int64(1), "tenant_id": int64(3), "payload": map[string]any{"customer": map[string]any{"name": "Bob"}, "note": "changed"}}
	for i, c := range []m.Change{{Table: "app.orders", After: old, KeyColumns: []string{"id"}}, {Table: "app.orders", Before: old, After: new, KeyColumns: []string{"id"}}, {Table: "app.orders", Before: new, KeyColumns: []string{"id"}}} {
		t := m.Transaction{Source: "live_B", Epoch: "e1", ID: fmt.Sprint(i + 1), Sequence: uint64(i + 1), EndLSN: fmt.Sprintf("0/%X", i+1), Version: "v1", Changes: []m.Change{c}}
		if i%2 == 0 {
			err = (m.PgcatAdapter{Engine: e, Store: s}).Commit(t)
		} else {
			err = (m.PgFloAdapter{Engine: e, Store: s}).Consume(t, func() error { acks++; return nil })
		}
		if err != nil {
			return err
		}
		if err = (m.PgcatAdapter{Engine: e, Store: s}).Commit(t); err != nil {
			return err
		} // duplicate
		for _, table := range []string{"legacy.orders_0", "legacy.orders_1", "app.order_detail"} {
			var count int
			if err = db.QueryRow("SELECT count(*) FROM " + table + " WHERE _sync_source='live_B'").Scan(&count); err != nil {
				return err
			}
			want := 1
			if i == 2 || (i == 0 && table == "legacy.orders_1") || (i == 1 && table == "legacy.orders_0") {
				want = 0
			}
			if count != want {
				return fmt.Errorf("stage %d table %s count %d want %d", i, table, count, want)
			}
		}
		if i == 1 {
			var name string
			if err = db.QueryRow("SELECT customer_name FROM legacy.orders_1 WHERE _sync_source='live_B'").Scan(&name); err != nil {
				return err
			}
			if name != "Bob" {
				return fmt.Errorf("JSON transform failed: %s", name)
			}
		}
	}
	if acks != 1 {
		return fmt.Errorf("ACK mismatch")
	}
	fmt.Println("PASS: pgcat + pg-flo reference adapters, JSON extract, horizontal relocation, vertical projection, delete, duplicate replay")
	return nil
}
