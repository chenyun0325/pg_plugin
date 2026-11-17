package pgmapping

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

// Atomic memory sink is a model, not a PostgreSQL/origin test.
type memory struct {
	rows map[string]Row
	seen map[string]bool
	seq  map[string]uint64
	fail bool
}

func newMemory() *memory {
	return &memory{rows: map[string]Row{}, seen: map[string]bool{}, seq: map[string]uint64{}}
}
func (s *memory) Apply(p Plan) error {
	t := p.Transaction
	stream := t.Source + "/" + t.Epoch
	id := stream + "/" + t.ID
	if s.seen[id] {
		return nil
	}
	if t.Sequence != s.seq[stream]+1 {
		return errors.New("sequence gap")
	}
	next := map[string]Row{}
	for k, v := range s.rows {
		next[k] = v
	}
	for _, m := range p.Mutations {
		key := mutationID(m)
		if m.Kind == "delete" {
			delete(next, key)
		} else {
			next[key] = m.Values
		}
	}
	if s.fail {
		return errors.New("injected rollback")
	}
	s.rows = next
	s.seen[id] = true
	s.seq[stream] = t.Sequence
	return nil
}
func event(source, table string, seq uint64, before, after Row) Transaction {
	return Transaction{Source: source, Epoch: "e1", ID: fmt.Sprint(seq), Sequence: seq, EndLSN: fmt.Sprintf("0/%X", seq), Version: "v1", Changes: []Change{{Table: table, Before: before, After: after, KeyColumns: []string{"id"}}}}
}
func order(tenant int) Row {
	return Row{"id": json.Number("9007199254740993"), "tenant_id": tenant, "payload": map[string]any{"customer": map[string]any{"name": "Alice"}, "note": "hello", "unrelated": true}}
}
func TestHorizontalVerticalJSONAndMigration(t *testing.T) {
	e := ExampleEngine()
	s := newMemory()
	a := PgcatAdapter{e, s}
	old := order(2)
	if err := a.Commit(event("B", "app.orders", 1, nil, old)); err != nil {
		t.Fatal(err)
	}
	if len(s.rows) != 2 {
		t.Fatal(s.rows)
	}
	for _, r := range s.rows {
		if r["customer_name"] != nil && r["customer_name"] != "Alice" {
			t.Fatal(r)
		}
		if r["order_id"].(json.Number).String() != "9007199254740993" {
			t.Fatal("precision lost")
		}
	}
	new := order(3)
	p, err := e.Plan(event("B", "app.orders", 2, old, new))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Mutations) != 3 || p.Mutations[0].Kind != "delete" || p.Mutations[0].Table != "legacy.orders_0" {
		t.Fatal(p)
	}
	if err = a.Commit(p.Transaction); err != nil {
		t.Fatal(err)
	}
	if len(s.rows) != 2 {
		t.Fatal(s.rows)
	}
	for _, r := range p.Mutations {
		if r.Kind == "upsert" && r.Table == "legacy.orders_0" {
			t.Fatal("wrong shard")
		}
	}
	if err = a.Commit(event("B", "app.orders", 3, new, nil)); err != nil {
		t.Fatal(err)
	}
	if len(s.rows) != 0 {
		t.Fatal(s.rows)
	}
}
func TestMergeSourceIsolationAndDeleteOwnership(t *testing.T) {
	e := ExampleEngine()
	s := newMemory()
	a := PgcatAdapter{e, s}
	r := Row{"id": 1, "tenant_id": 2, "customer": "X"}
	for _, src := range []string{"A", "C"} {
		if err := a.Commit(event(src, "legacy.orders_0", 1, nil, r)); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.rows) != 2 {
		t.Fatal("source collision")
	}
	if err := a.Commit(event("A", "legacy.orders_0", 2, r, nil)); err != nil {
		t.Fatal(err)
	}
	if len(s.rows) != 1 {
		t.Fatal("deleted another source")
	}
}
func TestAckRollbackRetryDuplicate(t *testing.T) {
	e := ExampleEngine()
	s := newMemory()
	a := PgFloAdapter{e, s}
	tx := event("B", "app.orders", 1, nil, order(2))
	acks := 0
	ack := func() error { acks++; return nil }
	s.fail = true
	if a.Consume(tx, ack) == nil || acks != 0 || len(s.rows) != 0 || len(s.seen) != 0 {
		t.Fatal("failed transaction acked or partially applied")
	}
	s.fail = false
	// Crash after commit but before ACK: retry must be an idempotent no-op.
	if err := a.Consume(tx, func() error { return errors.New("ACK lost") }); err == nil {
		t.Fatal("expected ACK failure")
	}
	if err := a.Consume(tx, ack); err != nil {
		t.Fatal(err)
	}
	if len(s.rows) != 2 || acks != 1 {
		t.Fatal(s.rows, acks)
	}
}
func TestMissingOldImageAndFunctionError(t *testing.T) {
	e := ExampleEngine()
	tx := event("B", "app.orders", 1, Row{"id": 1}, order(3))
	if _, err := e.Plan(tx); err == nil {
		t.Fatal("missing old shard key accepted")
	}
	r := order(2)
	r["payload"] = map[string]any{"note": "x"}
	if _, err := e.Plan(event("B", "app.orders", 1, nil, r)); err == nil {
		t.Fatal("missing JSON key accepted")
	}
}
func TestNullAndCustomFunction(t *testing.T) {
	e := ExampleEngine()
	e.Register("custom", func(v any, args []string) (any, error) { return "CUSTOM:" + fmt.Sprint(v), nil })
	e.Routes[0].Columns["customer_name"] = Column{Source: "customer", Calls: []Call{{Name: "custom"}}}
	p, err := e.Plan(event("A", "legacy.orders_0", 1, nil, Row{"id": 1, "tenant_id": 2, "customer": "X"}))
	if err != nil || p.Mutations[0].Values["customer_name"] != "CUSTOM:X" {
		t.Fatal(p, err)
	}
	r := order(2)
	r["payload"] = nil
	if _, err = e.Plan(event("B", "app.orders", 1, nil, r)); err != nil {
		t.Fatal("SQL null should propagate", err)
	}
}
func TestOriginFilterModelAndNewLocalEdit(t *testing.T) {
	e := ExampleEngine()
	s := newMemory()
	a := PgcatAdapter{e, s}
	tx := event("B", "app.orders", 1, nil, order(2))
	tx.Origin = "from_A"
	if err := a.Commit(tx); err != nil {
		t.Fatal(err)
	}
	if len(s.rows) != 0 {
		t.Fatal("replayed change escaped")
	}
	tx = event("B", "app.orders", 2, nil, order(2))
	if err := a.Commit(tx); err != nil {
		t.Fatal(err)
	}
	if len(s.rows) != 2 {
		t.Fatal("local edit suppressed")
	}
}
func TestProvenanceSurvivesReverseCapture(t *testing.T) {
	e := ExampleEngine()
	tx := event("B", "app.orders", 1, nil, order(2))
	tx.Changes[0].AfterOwner = &Identity{Source: "A", Table: "legacy.orders_0", PK: "[1]"}
	p, err := e.Plan(tx)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range p.Mutations {
		if m.Key["_sync_source"] != "A" {
			t.Fatal("capture node replaced record owner")
		}
	}
}
func TestSameSourceDifferentTables(t *testing.T) {
	e := ExampleEngine()
	s := newMemory()
	a := PgcatAdapter{e, s}
	r := Row{"id": 1, "tenant_id": 2, "customer": "X"}
	if err := a.Commit(event("A", "legacy.orders_0", 1, nil, r)); err != nil {
		t.Fatal(err)
	}
	if err := a.Commit(event("A", "legacy.orders_1", 2, nil, r)); err != nil {
		t.Fatal(err)
	}
	if len(s.rows) != 2 {
		t.Fatal("source table identity lost")
	}
}
func TestGapVersionAndOverlappingRoutes(t *testing.T) {
	e := ExampleEngine()
	s := newMemory()
	a := PgcatAdapter{e, s}
	if a.Commit(event("B", "app.orders", 2, nil, order(2))) == nil {
		t.Fatal("gap accepted")
	}
	tx := event("B", "app.orders", 1, nil, order(2))
	tx.Version = "v2"
	if a.Commit(tx) == nil {
		t.Fatal("version drift accepted")
	}
	e.Routes = append(e.Routes, e.Routes[2])
	if _, err := e.Plan(event("B", "app.orders", 1, nil, order(2))); err == nil {
		t.Fatal("ambiguous overlap accepted")
	}
}

func TestReverseJSONPatchPreservesOtherKeys(t *testing.T) {
	e, err := NewEngine("v1", []Route{{ID: "reverse", SourceTable: "legacy.orders_0", TargetTable: "app.orders", Columns: map[string]Column{"payload": {Source: "*", Calls: []Call{{Name: "json_put", Args: []string{"base_json", "customer_name", "customer", "name"}}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	r := Row{"id": 1, "customer_name": "Bob", "base_json": map[string]any{"customer": map[string]any{"name": "Alice", "level": "gold"}, "other": "preserve"}}
	p, err := e.Plan(event("A", "legacy.orders_0", 1, nil, r))
	if err != nil {
		t.Fatal(err)
	}
	doc := p.Mutations[0].Values["payload"].(map[string]any)
	customer := doc["customer"].(map[string]any)
	if doc["other"] != "preserve" || customer["level"] != "gold" || customer["name"] != "Bob" {
		t.Fatal(doc)
	}
	if r["base_json"].(map[string]any)["customer"].(map[string]any)["name"] != "Alice" {
		t.Fatal("input was mutated")
	}
}
