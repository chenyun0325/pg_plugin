package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"reflect"
	"strings"
)

// Separate tables preserve the old column-mode checkpoint and row_state layout.
const identityDDL = `
CREATE TABLE IF NOT EXISTS _pgcat_yaml.sidecar_version(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),version int NOT NULL);
INSERT INTO _pgcat_yaml.sidecar_version VALUES(true,1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS _pgcat_yaml.key_spec(namespace text NOT NULL,node_id text NOT NULL,table_name text NOT NULL,keys jsonb NOT NULL,PRIMARY KEY(namespace,node_id,table_name));
CREATE TABLE IF NOT EXISTS _pgcat_yaml.row_binding(namespace text NOT NULL,node_id text NOT NULL,table_name text NOT NULL,pk text NOT NULL,identity jsonb NOT NULL,alive boolean NOT NULL,local_lsn pg_lsn NOT NULL,PRIMARY KEY(namespace,node_id,table_name,pk));
CREATE TABLE IF NOT EXISTS _pgcat_yaml.identity_state(job text NOT NULL,source_table text NOT NULL,pk text NOT NULL,identity jsonb NOT NULL,PRIMARY KEY(job,source_table,pk));
CREATE TABLE IF NOT EXISTS _pgcat_yaml.identity_history(namespace text NOT NULL,node_id text NOT NULL,table_name text NOT NULL,pk text NOT NULL,local_lsn pg_lsn NOT NULL,alive boolean NOT NULL,identity jsonb NOT NULL,entity_id text NOT NULL,PRIMARY KEY(namespace,node_id,table_name,pk,local_lsn,alive,entity_id));
CREATE TABLE IF NOT EXISTS _pgcat_yaml.identity_outbox(job text NOT NULL,event_lsn pg_lsn NOT NULL,event_index int NOT NULL,namespace text NOT NULL,node_id text NOT NULL,table_name text NOT NULL,pk text NOT NULL,identity jsonb NOT NULL,alive boolean NOT NULL,PRIMARY KEY(job,event_lsn,event_index));
`

func ensureIdentitySchema(ctx context.Context, conn *pgx.Conn) error {
	tx, e := conn.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('pgcat-yaml-sidecar-schema',0))`); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, metadataDDL+identityDDL); e != nil {
		return e
	}
	var version int
	if e = tx.QueryRow(ctx, `SELECT version FROM _pgcat_yaml.sidecar_version WHERE singleton`).Scan(&version); e != nil {
		return e
	}
	if version != 1 {
		return fmt.Errorf("unsupported sidecar schema version %d", version)
	}
	return tx.Commit(ctx)
}
func (r *runtime) registerKeys(conn *pgx.Conn, node, table string, keys []string) error {
	raw, _ := json.Marshal(keys)
	_, e := conn.Exec(r.ctx, `INSERT INTO _pgcat_yaml.key_spec VALUES($1,$2,$3,$4::text::jsonb) ON CONFLICT DO NOTHING`, r.c.Identity.Namespace, node, table, string(raw))
	if e != nil {
		return e
	}
	var saved []byte
	if e = conn.QueryRow(r.ctx, `SELECT keys FROM _pgcat_yaml.key_spec WHERE namespace=$1 AND node_id=$2 AND table_name=$3`, r.c.Identity.Namespace, node, table).Scan(&saved); e != nil {
		return e
	}
	var actual []string
	if e = json.Unmarshal(saved, &actual); e != nil {
		return e
	}
	if !reflect.DeepEqual(actual, keys) {
		return fmt.Errorf("incompatible key definitions for node=%s table=%s: saved=%v proposed=%v", node, table, actual, keys)
	}
	return nil
}
func (r *runtime) validateTargetKeys(route *Route) error {
	var ok bool
	e := r.dst.QueryRow(r.ctx, `SELECT EXISTS(SELECT 1 FROM pg_index i WHERE i.indrelid=to_regclass($1) AND i.indisunique AND i.indisvalid AND i.indpred IS NULL AND i.indexprs IS NULL AND (SELECT array_agg(a.attname::text ORDER BY a.attname::text) FROM unnest(i.indkey) WITH ORDINALITY k(attnum,ord) JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum WHERE k.ord<=i.indnkeyatts)=(SELECT array_agg(k ORDER BY k) FROM unnest($2::text[]) k))`, qTable(route.Target), route.Keys).Scan(&ok)
	if e != nil {
		return e
	}
	if !ok {
		return fmt.Errorf("sidecar target %s requires a real unique constraint on %v", route.Target, route.Keys)
	}
	return nil
}
func (r *runtime) loadBindings(snapshot pgx.Tx) (map[string]binding, error) {
	rows, e := snapshot.Query(r.ctx, `SELECT table_name,pk,identity,alive FROM _pgcat_yaml.row_binding WHERE namespace=$1 AND node_id=$2 AND table_name=ANY($3)`, r.c.Identity.Namespace, r.c.Source.ID, r.tables)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := map[string]binding{}
	for rows.Next() {
		var table, pk string
		var raw []byte
		var b binding
		if e = rows.Scan(&table, &pk, &raw, &b.Alive); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(raw, &b.Identity); e != nil {
			return nil, e
		}
		if e = b.Identity.valid(); e != nil {
			return nil, e
		}
		out[table+"\x00"+pk] = b
	}
	return out, rows.Err()
}
func (r *runtime) getIdentityState(tx pgx.Tx, table, pk string) (*RecordIdentity, error) {
	var raw []byte
	e := tx.QueryRow(r.ctx, `SELECT identity FROM _pgcat_yaml.identity_state WHERE job=$1 AND source_table=$2 AND pk=$3`, r.c.Job, table, pk).Scan(&raw)
	if e != nil {
		return nil, e
	}
	var id RecordIdentity
	if e = json.Unmarshal(raw, &id); e != nil {
		return nil, e
	}
	return &id, id.valid()
}
func (r *runtime) putIdentityState(tx pgx.Tx, table, pk string, id *RecordIdentity) error {
	if e := id.valid(); e != nil {
		return e
	}
	raw, _ := json.Marshal(id)
	_, e := tx.Exec(r.ctx, `INSERT INTO _pgcat_yaml.identity_state VALUES($1,$2,$3,$4::text::jsonb) ON CONFLICT(job,source_table,pk) DO UPDATE SET identity=EXCLUDED.identity`, r.c.Job, table, pk, string(raw))
	return e
}
func (r *runtime) queueSourceBinding(tx pgx.Tx, lsn string, index int, table, pk string, id *RecordIdentity, alive bool) error {
	raw, e := json.Marshal(id)
	if e != nil {
		return e
	}
	_, e = tx.Exec(r.ctx, `INSERT INTO _pgcat_yaml.identity_outbox VALUES($1,$2::pg_lsn,$3,$4,$5,$6,$7,$8::text::jsonb,$9)`, r.c.Job, lsn, index, r.c.Identity.Namespace, r.c.Source.ID, table, pk, string(raw), alive)
	return e
}

func writeBinding(ctx context.Context, tx pgx.Tx, namespace, node, table, pk string, id *RecordIdentity, alive bool, lsn string) error {
	raw, e := json.Marshal(id)
	if e != nil {
		return e
	}
	_, e = tx.Exec(ctx, `INSERT INTO _pgcat_yaml.identity_history VALUES($1,$2,$3,$4,$5::pg_lsn,$6,$7::text::jsonb,$8) ON CONFLICT DO NOTHING`, namespace, node, table, pk, lsn, alive, string(raw), id.EntityID)
	if e != nil {
		return e
	}
	_, e = tx.Exec(ctx, `INSERT INTO _pgcat_yaml.row_binding VALUES($1,$2,$3,$4,$5::text::jsonb,$6,$7::pg_lsn) ON CONFLICT(namespace,node_id,table_name,pk) DO UPDATE SET identity=EXCLUDED.identity,alive=EXCLUDED.alive,local_lsn=EXCLUDED.local_lsn WHERE _pgcat_yaml.row_binding.local_lsn<=EXCLUDED.local_lsn`, namespace, node, table, pk, string(raw), alive, lsn)
	return e
}
func (r *runtime) flushIdentityOutbox() error {
	type pending struct {
		lsn, namespace, node, table, pk string
		index                           int
		id                              RecordIdentity
		alive                           bool
	}
	for {
		rows, e := r.dst.Query(r.ctx, `SELECT event_lsn::text,event_index,namespace,node_id,table_name,pk,identity,alive FROM _pgcat_yaml.identity_outbox WHERE job=$1 ORDER BY event_lsn,event_index LIMIT 500`, r.c.Job)
		if e != nil {
			return e
		}
		var batch []pending
		for rows.Next() {
			var p pending
			var raw []byte
			if e = rows.Scan(&p.lsn, &p.index, &p.namespace, &p.node, &p.table, &p.pk, &raw, &p.alive); e != nil {
				rows.Close()
				return e
			}
			if e = json.Unmarshal(raw, &p.id); e != nil {
				rows.Close()
				return e
			}
			batch = append(batch, p)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		if len(batch) == 0 {
			return nil
		}
		tx, e := r.src.Begin(r.ctx)
		if e != nil {
			return e
		}
		for _, p := range batch {
			if e = writeBinding(r.ctx, tx, p.namespace, p.node, p.table, p.pk, &p.id, p.alive, p.lsn); e != nil {
				tx.Rollback(context.Background())
				return e
			}
		}
		if e = tx.Commit(r.ctx); e != nil {
			return e
		}
		if r.afterOutboxSourceCommit != nil {
			if e = r.afterOutboxSourceCommit(); e != nil {
				return e
			}
		}
		// Removing only this batch makes source-commit / target-ack crashes idempotent.
		ack, e := r.dst.Begin(r.ctx)
		if e != nil {
			return e
		}
		for _, p := range batch {
			if _, e = ack.Exec(r.ctx, `DELETE FROM _pgcat_yaml.identity_outbox WHERE job=$1 AND event_lsn=$2::pg_lsn AND event_index=$3`, r.c.Job, p.lsn, p.index); e != nil {
				ack.Rollback(context.Background())
				return e
			}
		}
		if e = ack.Commit(r.ctx); e != nil {
			return e
		}
	}
}

func (r *runtime) getBinding(tx pgx.Tx, node, table, pk string) (*binding, error) {
	var raw []byte
	var b binding
	e := tx.QueryRow(r.ctx, `SELECT identity,alive FROM _pgcat_yaml.row_binding WHERE namespace=$1 AND node_id=$2 AND table_name=$3 AND pk=$4 FOR UPDATE`, r.c.Identity.Namespace, node, table, pk).Scan(&raw, &b.Alive)
	if e != nil {
		return nil, e
	}
	if e = json.Unmarshal(raw, &b.Identity); e != nil {
		return nil, e
	}
	return &b, b.Identity.valid()
}

func (r *runtime) applySidecar(tx pgx.Tx, m *Mutation) error {
	if e := m.Identity.valid(); e != nil {
		return e
	}
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
	found := true
	e = tx.QueryRow(r.ctx, "SELECT to_jsonb(t)::text FROM "+qTable(m.Route.Target)+" t WHERE "+where+" FOR UPDATE", args...).Scan(&raw)
	if e == pgx.ErrNoRows {
		found = false
	} else if e != nil {
		return e
	} else {
		existing, e = decodeJSON([]byte(raw))
		if e != nil {
			return e
		}
	}
	var pk string
	if found {
		pk, e = rowKey(existing, m.Route.Keys)
		if e != nil {
			return e
		}
		bound, err := r.getBinding(tx, r.c.Sink.ID, m.Route.Target, pk)
		if err != nil && err != pgx.ErrNoRows {
			return err
		}
		if err == nil && bound.Alive {
			if !sameIdentity(&bound.Identity, m.Identity) {
				return fmt.Errorf("sidecar ownership conflict at %s key=%s", m.Route.Target, pk)
			}
		} else {
			if m.Route.AdoptSource != m.Owner.Source || m.Owner.Source != r.c.Sink.ID || m.Owner.Table != m.Route.Target || m.Owner.PK != pk {
				return fmt.Errorf("unbound existing business row at %s key=%s: explicit original-source adoption required", m.Route.Target, pk)
			}
			if err == nil && !sameIdentity(&bound.Identity, m.Identity) {
				return fmt.Errorf("tombstoned identity reused by an untracked row: reconcile before adoption")
			}
		}
	}
	if m.Delete && !found {
		return nil
	}
	if !m.Delete && !found && m.Route.RequireExisting {
		return fmt.Errorf("route %s needs existing aggregate", m.Route.Name)
	}
	kind := "U"
	if m.Delete {
		var tag pgconn.CommandTag
		if tag, e = tx.Exec(r.ctx, "DELETE FROM "+qTable(m.Route.Target)+" WHERE "+where, args...); e != nil {
			return e
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("target delete was suppressed or affected unexpected rows")
		}
		kind = "D"
	} else {
		for col, patches := range m.Patches {
			doc := existing[col]
			if !found {
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
		var cols, params, sets []string
		args = nil
		for _, col := range sortedKeys(m.Values) {
			typ, ok := types[col]
			if !ok {
				return fmt.Errorf("unknown business column %s.%s", m.Route.Target, col)
			}
			if strings.HasPrefix(col, "_sync_") {
				return fmt.Errorf("sidecar cannot write reserved provenance columns")
			}
			v, err := plain(m.Values[col])
			if err != nil {
				return err
			}
			args = append(args, v)
			cols = append(cols, q(col))
			p := fmt.Sprintf("$%d::text::%s", len(args), typ)
			params = append(params, p)
			sets = append(sets, q(col)+"="+p)
		}
		var sql string
		if found {
			w, ka, err := whereKey(m.Key, types, len(args)+1)
			if err != nil {
				return err
			}
			args = append(args, ka...)
			sql = "UPDATE " + qTable(m.Route.Target) + " AS t SET " + strings.Join(sets, ",") + " WHERE " + w + " RETURNING to_jsonb(t)::text"
		} else {
			kind = "I"
			sql = "INSERT INTO " + qTable(m.Route.Target) + " AS t (" + strings.Join(cols, ",") + ") VALUES (" + strings.Join(params, ",") + ") RETURNING to_jsonb(t)::text"
		}
		// Plain INSERT deliberately rejects concurrent key claims; it never silently
		// updates an unbound business row using ON CONFLICT.
		check, checkArgs, err := whereKey(m.Key, types, len(args)+1)
		if err != nil {
			return err
		}
		args = append(args, checkArgs...)
		sql += ",(" + check + ")"
		var keyMatches bool
		if e = tx.QueryRow(r.ctx, sql, args...).Scan(&raw, &keyMatches); e != nil {
			return e
		}
		if !keyMatches {
			return fmt.Errorf("target trigger changed the configured business key; transaction rolled back")
		}
		actual, err := decodeJSON([]byte(raw))
		if err != nil {
			return err
		}
		pk, e = rowKey(actual, m.Route.Keys)
		if e != nil {
			return e
		}
	}
	event := IdentityEvent{Version: 1, Namespace: r.c.Identity.Namespace, Node: r.c.Sink.ID, Table: m.Route.Target, Keys: m.Route.Keys, PK: pk, Kind: kind, Identity: *m.Identity}
	body, e := json.Marshal(event)
	if e != nil {
		return e
	}
	if len(body) > identityMessageLimit {
		return fmt.Errorf("identity envelope exceeds %d bytes; transaction rolled back", identityMessageLimit)
	}
	if _, e = tx.Exec(r.ctx, `SELECT pg_logical_emit_message(true,$1,$2::text)`, identityPrefix, string(body)); e != nil {
		return e
	}
	var localLSN string
	if e = tx.QueryRow(r.ctx, `SELECT pg_current_wal_insert_lsn()::text`).Scan(&localLSN); e != nil {
		return e
	}
	return writeBinding(r.ctx, tx, r.c.Identity.Namespace, r.c.Sink.ID, m.Route.Target, pk, m.Identity, !m.Delete, localLSN)
}
