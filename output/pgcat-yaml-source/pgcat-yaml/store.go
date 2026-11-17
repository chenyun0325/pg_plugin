package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"sort"
	"strings"
)

func sortedKeys[T any](m map[string]T) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
func qTable(t string) string { return pgx.Identifier(strings.Split(t, ".")).Sanitize() }
func q(s string) string      { return pgx.Identifier{s}.Sanitize() }

const metadataDDL = `CREATE SCHEMA IF NOT EXISTS _pgcat_yaml;
CREATE TABLE IF NOT EXISTS _pgcat_yaml.checkpoint(job text PRIMARY KEY,source_id text NOT NULL,slot text NOT NULL,config_hash text NOT NULL,end_lsn pg_lsn NOT NULL);
CREATE TABLE IF NOT EXISTS _pgcat_yaml.row_state(job text NOT NULL,source_table text NOT NULL,pk text NOT NULL,data jsonb NOT NULL,PRIMARY KEY(job,source_table,pk));`

type columnTypes map[string]string
type runtime struct {
	ctx           context.Context
	c             *Config
	src, dst      *pgx.Conn
	types         map[string]columnTypes
	tables        []string
	beforeAdvance func() error // tests inject a crash after target commit
}

func (r *runtime) targetTypes(table string) (columnTypes, error) {
	if t := r.types[table]; t != nil {
		return t, nil
	}
	rows, e := r.dst.Query(r.ctx, `SELECT attname,pg_catalog.format_type(atttypid,atttypmod) FROM pg_attribute WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped`, qTable(table))
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	t := columnTypes{}
	for rows.Next() {
		var col, typ string
		if e = rows.Scan(&col, &typ); e != nil {
			return nil, e
		}
		t[col] = typ
	}
	if e = rows.Err(); e != nil {
		return nil, e
	}
	if len(t) == 0 {
		return nil, fmt.Errorf("missing target table %s", table)
	}
	for _, col := range []string{"_sync_source", "_sync_table", "_sync_pk"} {
		if t[col] == "" {
			return nil, fmt.Errorf("target %s needs provenance column %s", table, col)
		}
	}
	r.types[table] = t
	return t, nil
}
func whereKey(key Row, types columnTypes, start int) (string, []any, error) {
	var terms []string
	var args []any
	for _, k := range sortedKeys(key) {
		typ, ok := types[k]
		if !ok {
			return "", nil, fmt.Errorf("unknown key column %s", k)
		}
		v, e := plain(key[k])
		if e != nil {
			return "", nil, e
		}
		args = append(args, v)
		terms = append(terms, fmt.Sprintf("%s=$%d::text::%s", q(k), start+len(args)-1, typ))
	}
	if len(terms) == 0 {
		return "", nil, fmt.Errorf("empty target key")
	}
	return strings.Join(terms, " AND "), args, nil
}
func (r *runtime) apply(tx pgx.Tx, m *Mutation) error {
	types, e := r.targetTypes(m.Route.Target)
	if e != nil {
		return e
	}
	where, args, e := whereKey(m.Key, types, 1)
	if e != nil {
		return e
	}
	var raw string
	existing := Row{}
	err := tx.QueryRow(r.ctx, "SELECT to_jsonb(t)::text FROM "+qTable(m.Route.Target)+" t WHERE "+where+" FOR UPDATE", args...).Scan(&raw)
	if err != nil && err != pgx.ErrNoRows {
		return err
	}
	if err == nil {
		existing, e = decodeJSON([]byte(raw))
		if e != nil {
			return e
		}
		if existing["_sync_source"] == nil {
			if existing["_sync_table"] != nil || existing["_sync_pk"] != nil {
				return fmt.Errorf("incomplete target provenance")
			}
			if m.Route.AdoptSource != m.Owner.Source || m.Owner.Table != m.Route.Target {
				return fmt.Errorf("unmarked existing row: route %s must explicitly adopt its own source table", m.Route.Name)
			}
		} else if fmt.Sprint(existing["_sync_source"]) != m.Owner.Source || fmt.Sprint(existing["_sync_table"]) != m.Owner.Table || fmt.Sprint(existing["_sync_pk"]) != m.Owner.PK {
			return fmt.Errorf("ownership conflict at %s key %v", m.Route.Target, m.Key)
		}
	}
	if m.Delete {
		if err == pgx.ErrNoRows {
			return nil
		}
		_, e = tx.Exec(r.ctx, "DELETE FROM "+qTable(m.Route.Target)+" WHERE "+where, args...)
		return e
	}
	if err == pgx.ErrNoRows && m.Route.RequireExisting {
		return fmt.Errorf("route %s needs existing aggregate row", m.Route.Name)
	}
	for col, patches := range m.Patches {
		var doc any = existing[col]
		if err == pgx.ErrNoRows {
			doc = m.Values[col]
		}
		for _, p := range patches {
			doc, e = patchJSON(doc, p.Path, p.Value)
			if e != nil {
				return e
			}
		}
		m.Values[col] = doc
	}
	var names, params, updates []string
	args = nil
	for _, col := range sortedKeys(m.Values) {
		typ, ok := types[col]
		if !ok {
			return fmt.Errorf("target %s has no column %s", m.Route.Target, col)
		}
		v, err := plain(m.Values[col])
		if err != nil {
			return err
		}
		args = append(args, v)
		names = append(names, q(col))
		params = append(params, fmt.Sprintf("$%d::text::%s", len(args), typ))
		updates = append(updates, q(col)+"=EXCLUDED."+q(col))
	}
	if err == nil {
		var sets []string
		for i, col := range sortedKeys(m.Values) {
			sets = append(sets, q(col)+"="+params[i])
		}
		w, keyargs, er := whereKey(m.Key, types, len(args)+1)
		if er != nil {
			return er
		}
		args = append(args, keyargs...)
		_, e = tx.Exec(r.ctx, "UPDATE "+qTable(m.Route.Target)+" SET "+strings.Join(sets, ",")+" WHERE "+w, args...)
		return e
	}
	var keys []string
	for _, k := range m.Route.Keys {
		keys = append(keys, q(k))
	}
	sql := "INSERT INTO " + qTable(m.Route.Target) + " (" + strings.Join(names, ",") + ") VALUES (" + strings.Join(params, ",") + ") ON CONFLICT (" + strings.Join(keys, ",") + ") DO UPDATE SET " + strings.Join(updates, ",")
	// A racing business INSERT cannot steal ownership after the locked read.
	sql += " WHERE (" + qTable(m.Route.Target) + "._sync_source IS NOT DISTINCT FROM EXCLUDED._sync_source AND " + qTable(m.Route.Target) + "._sync_table IS NOT DISTINCT FROM EXCLUDED._sync_table AND " + qTable(m.Route.Target) + "._sync_pk IS NOT DISTINCT FROM EXCLUDED._sync_pk)"
	if m.Route.AdoptSource == m.Owner.Source && m.Owner.Table == m.Route.Target {
		sql += " OR (" + qTable(m.Route.Target) + "._sync_source IS NULL AND " + qTable(m.Route.Target) + "._sync_table IS NULL AND " + qTable(m.Route.Target) + "._sync_pk IS NULL)"
	}
	tag, e := tx.Exec(r.ctx, sql, args...)
	if e == nil && tag.RowsAffected() != 1 {
		return fmt.Errorf("concurrent ownership conflict")
	}
	return e
}
func (r *runtime) getState(tx pgx.Tx, table, pk string) (Row, error) {
	var raw string
	e := tx.QueryRow(r.ctx, `SELECT data::text FROM _pgcat_yaml.row_state WHERE job=$1 AND source_table=$2 AND pk=$3`, r.c.Job, table, pk).Scan(&raw)
	if e != nil {
		return nil, e
	}
	return decodeJSON([]byte(raw))
}
func (r *runtime) putState(tx pgx.Tx, table, pk string, row Row) error {
	b, e := json.Marshal(row)
	if e != nil {
		return e
	}
	_, e = tx.Exec(r.ctx, `INSERT INTO _pgcat_yaml.row_state VALUES($1,$2,$3,$4::text::jsonb) ON CONFLICT(job,source_table,pk) DO UPDATE SET data=EXCLUDED.data`, r.c.Job, table, pk, string(b))
	return e
}
func (r *runtime) checkpoint(tx pgx.Tx, lsn string) error {
	_, e := tx.Exec(r.ctx, `INSERT INTO _pgcat_yaml.checkpoint VALUES($1,$2,$3,$4,$5::pg_lsn) ON CONFLICT(job) DO UPDATE SET end_lsn=EXCLUDED.end_lsn`, r.c.Job, r.c.Source.ID, r.c.Source.Slot, r.c.digest, lsn)
	return e
}
