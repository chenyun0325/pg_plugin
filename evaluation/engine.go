// Package pgmapping is a reference planner shared by pgcat and pg-flo adapters.
// Inputs must be committed transactions with complete, typed before/after images.
package pgmapping

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type Row map[string]any

// Identity is populated by the adapter from reserved provenance columns on a
// locally edited replica. It is never replaced by the current capture node.
type Identity struct{ Source, Table, PK string }
type Change struct {
	Table                   string
	Before, After           Row // nil means row absent; an absent field is not SQL NULL.
	KeyColumns              []string
	BeforeOwner, AfterOwner *Identity
}
type Transaction struct {
	Source, Epoch, ID, Origin string
	Sequence                  uint64 // contiguous per source/epoch; assigned durably by the adapter
	EndLSN                    string
	Version                   string
	Changes                   []Change
}
type Call struct {
	Name string
	Args []string
}
type Column struct {
	Source string
	Calls  []Call
}
type Shard struct {
	Column             string
	Modulus, Remainder int64
}
type Route struct {
	ID, SourceTable, TargetTable string
	Columns                      map[string]Column // explicit projection supports vertical splitting
	Shard                        *Shard
}
type Function func(any, []string) (any, error)
type Engine struct {
	Version   string
	Routes    []Route
	Functions map[string]Function
}
type Mutation struct {
	Kind, Table string
	Key         Row
	Values      Row
}
type Plan struct {
	Transaction Transaction
	Mutations   []Mutation
	Suppressed  bool
}

var identityColumns = []string{"_sync_source", "_sync_table", "_sync_pk"}

func NewEngine(version string, routes []Route) (*Engine, error) {
	e := &Engine{Version: version, Routes: routes, Functions: map[string]Function{}}
	e.Register("json_get", func(v any, path []string) (any, error) {
		if v == nil {
			return nil, nil
		}
		if raw, ok := v.(string); ok {
			d := json.NewDecoder(strings.NewReader(raw))
			d.UseNumber()
			if err := d.Decode(&v); err != nil {
				return nil, err
			}
		}
		for _, part := range path {
			obj, ok := v.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("JSON path %q is not an object", part)
			}
			var found bool
			v, found = obj[part]
			if !found {
				return nil, fmt.Errorf("missing JSON key %q", part)
			}
		}
		return v, nil
	})
	e.Register("text", func(v any, _ []string) (any, error) {
		if v == nil {
			return nil, nil
		}
		switch x := v.(type) {
		case string:
			return x, nil
		case json.Number:
			return x.String(), nil
		default:
			return nil, fmt.Errorf("text requires scalar string/number")
		}
	})
	// Source="*" passes a private copy of the complete input row. This permits
	// multi-column business functions and JSON patches without external reads.
	e.Register("json_put", func(v any, args []string) (any, error) {
		if len(args) < 3 {
			return nil, fmt.Errorf("json_put needs document column, value column, path")
		}
		row, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("json_put requires Source=*")
		}
		doc, found := row[args[0]]
		if !found {
			return nil, fmt.Errorf("missing base document")
		}
		value, found := row[args[1]]
		if !found {
			return nil, fmt.Errorf("missing patch value")
		}
		if raw, ok := doc.(string); ok {
			d := json.NewDecoder(strings.NewReader(raw))
			d.UseNumber()
			if err := d.Decode(&doc); err != nil {
				return nil, err
			}
		}
		if doc == nil {
			doc = map[string]any{}
		}
		obj, ok := doc.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("base document is not an object")
		}
		current := obj
		for _, part := range args[2 : len(args)-1] {
			child, exists := current[part]
			if !exists || child == nil {
				child = map[string]any{}
				current[part] = child
			}
			next, ok := child.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("patch path crosses non-object")
			}
			current = next
		}
		current[args[len(args)-1]] = value
		return obj, nil
	})
	seen := map[string]bool{}
	for _, r := range routes {
		if r.ID == "" || seen[r.ID] {
			return nil, fmt.Errorf("duplicate/empty route ID")
		}
		seen[r.ID] = true
		if r.SourceTable == "" || !qualified(r.TargetTable) || len(r.Columns) == 0 {
			return nil, fmt.Errorf("invalid route %s", r.ID)
		}
		for target := range r.Columns {
			if strings.HasPrefix(target, "_sync_") {
				return nil, fmt.Errorf("reserved column %s", target)
			}
		}
		if s := r.Shard; s != nil && (s.Modulus <= 0 || s.Remainder < 0 || s.Remainder >= s.Modulus) {
			return nil, fmt.Errorf("invalid shard")
		}
	}
	return e, nil
}
func (e *Engine) Register(name string, f Function) { e.Functions[name] = f }
func qualified(s string) bool {
	p := strings.Split(s, ".")
	return len(p) == 2 && p[0] != "" && p[1] != ""
}
func clone(v any) (any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out any
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.UseNumber()
	err = d.Decode(&out)
	return out, err
}
func (e *Engine) project(t Transaction, c Change, r Route, row Row, owner *Identity) (*Mutation, error) {
	if row == nil {
		return nil, nil
	}
	if s := r.Shard; s != nil {
		v, ok := row[s.Column]
		if !ok || v == nil {
			return nil, fmt.Errorf("missing shard key %s", s.Column)
		}
		n, err := strconv.ParseInt(fmt.Sprint(v), 10, 64)
		if err != nil {
			return nil, err
		}
		if ((n%s.Modulus)+s.Modulus)%s.Modulus != s.Remainder {
			return nil, nil
		}
	}
	if len(c.KeyColumns) == 0 {
		return nil, fmt.Errorf("missing source identity")
	}
	parts := make([]any, 0, len(c.KeyColumns))
	for _, k := range c.KeyColumns {
		v, ok := row[k]
		if !ok || v == nil {
			return nil, fmt.Errorf("missing identity %s", k)
		}
		parts = append(parts, v)
	}
	pk, err := json.Marshal(parts)
	if err != nil {
		return nil, err
	}
	key := Row{"_sync_source": t.Source, "_sync_table": c.Table, "_sync_pk": string(pk)}
	if owner != nil {
		if owner.Source == "" || owner.Table == "" || owner.PK == "" {
			return nil, fmt.Errorf("incomplete provenance")
		}
		key = Row{"_sync_source": owner.Source, "_sync_table": owner.Table, "_sync_pk": owner.PK}
	}
	values := Row{}
	for k, v := range key {
		values[k] = v
	}
	for target, col := range r.Columns {
		v, ok := row[col.Source]
		if col.Source == "*" {
			v = row
			ok = true
		}
		if !ok {
			return nil, fmt.Errorf("missing field %s (NULL and unchanged TOAST must be decoded separately)", col.Source)
		}
		v, err = clone(v)
		if err != nil {
			return nil, err
		}
		for _, call := range col.Calls {
			f, ok := e.Functions[call.Name]
			if !ok {
				return nil, fmt.Errorf("unknown function %s", call.Name)
			}
			v, err = f(v, call.Args)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", r.ID, target, err)
			}
		}
		values[target] = v
	}
	return &Mutation{Kind: "upsert", Table: r.TargetTable, Key: key, Values: values}, nil
}
func mutationID(m Mutation) string { b, _ := json.Marshal(m.Key); return m.Table + "/" + string(b) }
func (e *Engine) Plan(t Transaction) (Plan, error) {
	p := Plan{Transaction: t}
	if t.Source == "" || t.Epoch == "" || t.ID == "" || t.Sequence == 0 || t.Version != e.Version {
		return p, fmt.Errorf("invalid transaction identity/version")
	}
	// Defense in depth. The decoder must already filter replayed transactions.
	if t.Origin != "" {
		p.Suppressed = true
		return p, nil
	}
	for _, c := range t.Changes {
		matched := false
		old, new := map[string]Mutation{}, map[string]Mutation{}
		for _, r := range e.Routes {
			if r.SourceTable != c.Table {
				continue
			}
			matched = true
			for i, row := range []Row{c.Before, c.After} {
				owner := c.AfterOwner
				if i == 0 {
					owner = c.BeforeOwner
				}
				m, err := e.project(t, c, r, row, owner)
				if err != nil {
					return p, err
				}
				if m == nil {
					continue
				}
				dest := new
				if i == 0 {
					dest = old
				}
				id := mutationID(*m)
				if _, ok := dest[id]; ok {
					return p, fmt.Errorf("overlapping routes address same target row: %s", id)
				}
				dest[id] = *m
			}
		}
		if !matched {
			return p, fmt.Errorf("no route for %s", c.Table)
		}
		var ids []string
		for id := range old {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			if _, ok := new[id]; !ok {
				m := old[id]
				m.Kind = "delete"
				m.Values = nil
				p.Mutations = append(p.Mutations, m)
			}
		}
		ids = nil
		for id := range new {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			p.Mutations = append(p.Mutations, new[id])
		}
	}
	return p, nil
}

// Same-db targets commit in one transaction. Cross-db relocation is deliberately rejected.
type Store interface{ Apply(Plan) error }
type PgcatAdapter struct {
	Engine *Engine
	Store  Store
}

func (a PgcatAdapter) Commit(t Transaction) error {
	p, err := a.Engine.Plan(t)
	if err != nil {
		return err
	}
	return a.Store.Apply(p)
}

type PgFloAdapter struct {
	Engine *Engine
	Store  Store
}

// ACK is called only after a complete source transaction has committed in the sink.
func (a PgFloAdapter) Consume(t Transaction, ack func() error) error {
	p, err := a.Engine.Plan(t)
	if err != nil {
		return err
	}
	if err = a.Store.Apply(p); err != nil {
		return err
	}
	return ack()
}
