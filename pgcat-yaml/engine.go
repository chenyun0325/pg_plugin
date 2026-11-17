package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type Owner struct{ Source, Table, PK string }
type Mutation struct {
	Route    *Route
	Values   Row
	Key      Row
	Owner    Owner
	Identity *RecordIdentity
	Delete   bool
	Patches  map[string][]patchValue
}
type patchValue struct {
	Path  []string
	Value any
}

func (c *Config) project(r *Route, table string, row Row) (*Mutation, error) {
	return c.projectIdentity(r, table, row, nil)
}
func (c *Config) projectIdentity(r *Route, table string, row Row, identity *RecordIdentity) (*Mutation, error) {
	if row == nil {
		return nil, nil
	}
	if p := r.When; p != nil {
		v, ok := row[p.Field]
		if !ok || v == nil {
			return nil, fmt.Errorf("missing predicate field %s", p.Field)
		}
		if p.Equals != nil {
			if fmt.Sprint(v) != *p.Equals {
				return nil, nil
			}
		} else {
			n, e := strconv.ParseInt(fmt.Sprint(v), 10, 64)
			if e != nil {
				return nil, e
			}
			if (n%p.Modulus+p.Modulus)%p.Modulus != p.Remainder {
				return nil, nil
			}
		}
	}
	pk, e := rowKey(row, r.SourceKey)
	if e != nil {
		return nil, e
	}
	owner := Owner{c.Source.ID, table, pk}
	if c.sidecar() {
		if identity == nil {
			return nil, fmt.Errorf("sidecar planning needs a resolved record identity")
		}
		owner = identity.Owner
	} else if row["_sync_source"] != nil && fmt.Sprint(row["_sync_source"]) != "" {
		for _, k := range []string{"_sync_table", "_sync_pk"} {
			if row[k] == nil || fmt.Sprint(row[k]) == "" {
				return nil, fmt.Errorf("incomplete provenance")
			}
		}
		owner = Owner{fmt.Sprint(row["_sync_source"]), fmt.Sprint(row["_sync_table"]), fmt.Sprint(row["_sync_pk"])}
	}
	parts := strings.SplitN(table, ".", 2)
	meta := Row{"__table_name__": parts[1], "__schema_name__": parts[0], "__source_id__": c.Source.ID, "__owner_source__": owner.Source, "__owner_table__": owner.Table, "__owner_pk__": owner.PK}
	if identity != nil {
		meta["__entity_id__"] = identity.EntityID
	}
	out := Row{}
	if r.IncludeAll {
		for k, v := range row {
			if c.sidecar() && strings.HasPrefix(k, "_sync_") {
				continue
			}
			out[k] = v
		}
	}
	for _, k := range r.Exclude {
		delete(out, k)
	}
	for k, expr := range r.Columns {
		v, err := eval(expr, row, meta)
		if err != nil {
			return nil, fmt.Errorf("route %s column %s: %w", r.Name, k, err)
		}
		out[k] = v
	}
	if !c.sidecar() {
		out["_sync_source"] = owner.Source
		out["_sync_table"] = owner.Table
		out["_sync_pk"] = owner.PK
	}
	key := Row{}
	for _, k := range r.Keys {
		v, ok := out[k]
		if !ok || v == nil {
			return nil, fmt.Errorf("route %s: missing target key %s", r.Name, k)
		}
		key[k] = v
	}
	m := &Mutation{Route: r, Values: out, Key: key, Owner: owner, Identity: identity, Patches: map[string][]patchValue{}}
	for col, patches := range r.JSONPatches {
		for _, p := range patches {
			v, err := eval(p.Value, row, meta)
			if err != nil {
				return nil, err
			}
			m.Patches[col] = append(m.Patches[col], patchValue{p.Path, v})
		}
	}
	return m, nil
}
func mutationID(m *Mutation) string { b, _ := json.Marshal(m.Key); return m.Route.Target + string(b) }
func (c *Config) plan(table string, before, after Row) ([]*Mutation, error) {
	return c.planIdentity(table, before, after, nil)
}
func (c *Config) planIdentity(table string, before, after Row, identity *RecordIdentity) ([]*Mutation, error) {
	old, new := map[string]*Mutation{}, map[string]*Mutation{}
	rs := c.routes(table)
	if len(rs) == 0 {
		return nil, fmt.Errorf("published table has no route: %s", table)
	}
	for _, r := range rs {
		for i, row := range []Row{before, after} {
			m, e := c.projectIdentity(r, table, row, identity)
			if e != nil {
				return nil, e
			}
			if m == nil {
				continue
			}
			set := new
			if i == 0 {
				set = old
			}
			id := mutationID(m)
			if _, ok := set[id]; ok {
				return nil, fmt.Errorf("overlapping routes write same target row: %s", id)
			}
			set[id] = m
		}
	}
	var out []*Mutation
	for _, id := range sortedKeys(old) {
		if _, ok := new[id]; !ok {
			m := old[id]
			switch m.Route.OnDelete {
			case "ignore":
				continue
			case "patch-null":
				for k := range m.Values {
					if _, key := m.Key[k]; !key && !strings.HasPrefix(k, "_sync_") {
						m.Values[k] = nil
					}
				}
				for col, ps := range m.Patches {
					for i := range ps {
						ps[i].Value = nil
					}
					m.Patches[col] = ps
				}
			default:
				m.Delete = true
			}
			out = append(out, m)
		}
	}
	for _, id := range sortedKeys(new) {
		out = append(out, new[id])
	}
	return out, nil
}
func patchJSON(doc any, path []string, value any) (any, error) {
	if len(path) == 0 {
		return value, nil
	}
	if doc == nil {
		doc = map[string]any{}
	}
	m, ok := doc.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("JSON patch crosses non-object at %s", path[0])
	}
	v, e := patchJSON(m[path[0]], path[1:], value)
	if e != nil {
		return nil, e
	}
	out := map[string]any{}
	for k, value := range m {
		out[k] = value
	}
	out[path[0]] = v
	return out, nil
}
