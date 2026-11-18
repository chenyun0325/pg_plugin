package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"log"
	"os"
	"strings"
	"time"
)

func openRuntime(ctx context.Context, c *Config) (*runtime, error) {
	sd, td := os.Getenv(c.Source.DSNEnv), os.Getenv(c.Sink.DSNEnv)
	if sd == "" || td == "" {
		return nil, fmt.Errorf("set %s and %s", c.Source.DSNEnv, c.Sink.DSNEnv)
	}
	s, e := pgx.Connect(ctx, sd)
	if e != nil {
		return nil, e
	}
	d, e := pgx.Connect(ctx, td)
	if e != nil {
		s.Close(ctx)
		return nil, e
	}
	r := &runtime{ctx: ctx, c: c, src: s, dst: d, types: map[string]columnTypes{}}
	var locked bool
	if e = s.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1,0))`, "pgcat-yaml-slot:"+c.Source.Slot).Scan(&locked); e != nil || !locked {
		r.close()
		return nil, fmt.Errorf("source slot consumer already running: %v", e)
	}
	if e = d.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1,0))`, "pgcat-yaml-job:"+c.Job).Scan(&locked); e != nil || !locked {
		r.close()
		return nil, fmt.Errorf("sink job consumer already running: %v", e)
	}
	var version int
	if e = s.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version); e != nil || version < 160000 {
		r.close()
		return nil, fmt.Errorf("requires PostgreSQL 16+ source: %v", e)
	}
	rows, e := s.Query(ctx, `SELECT schemaname,tablename FROM pg_publication_tables WHERE pubname=$1 ORDER BY 1,2`, c.Source.Publication)
	if e != nil {
		r.close()
		return nil, e
	}
	for rows.Next() {
		var a, b string
		if e = rows.Scan(&a, &b); e != nil {
			rows.Close()
			r.close()
			return nil, e
		}
		t := a + "." + b
		if len(c.routes(t)) == 0 {
			rows.Close()
			r.close()
			return nil, fmt.Errorf("publication contains unmatched table %s", t)
		}
		r.tables = append(r.tables, t)
	}
	e = rows.Err()
	rows.Close()
	if e != nil || len(r.tables) == 0 {
		r.close()
		return nil, fmt.Errorf("empty or invalid publication: %v", e)
	}
	for _, t := range r.tables {
		var full bool
		if e = s.QueryRow(ctx, `SELECT relreplident='f' FROM pg_class WHERE oid=to_regclass($1)`, qTable(t)).Scan(&full); e != nil || !full {
			r.close()
			return nil, fmt.Errorf("%s requires REPLICA IDENTITY FULL", t)
		}
		if _, e = c.sourceKey(t); e != nil {
			r.close()
			return nil, e
		}
		keys, _ := c.sourceKey(t)
		if e = validateSourceTable(ctx, s, c.Source.Publication, t, keys); e != nil {
			r.close()
			return nil, e
		}
	}
	targets, err := c.targetRoutes(r.tables)
	if err != nil {
		r.close()
		return nil, err
	}
	for _, route := range targets {
		if _, e = r.targetTypes(route.Target); e != nil {
			r.close()
			return nil, e
		}
		if c.sidecar() {
			if e = r.validateTargetKeys(route); e != nil {
				r.close()
				return nil, e
			}
		}
	}
	return r, nil
}

func validateSourceTable(ctx context.Context, conn *pgx.Conn, pub, table string, keys []string) error {
	var ok bool
	err := conn.QueryRow(ctx, `SELECT c.relkind='r' AND NOT EXISTS(
 SELECT 1 FROM pg_publication_tables p WHERE p.pubname=$1 AND p.schemaname=n.nspname AND p.tablename=c.relname
 AND (p.rowfilter IS NOT NULL OR cardinality(p.attnames)<>(SELECT count(*) FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped)))
 AND NOT EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped AND a.attgenerated<>'')
 AND NOT EXISTS(SELECT 1 FROM unnest($3::text[]) k WHERE NOT EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attname=k AND a.attnotnull AND a.atttypid IN (16,20,21,23,25,26,1042,1043,1700,2950)))
 AND EXISTS(SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND i.indisunique AND i.indisvalid AND i.indpred IS NULL AND i.indexprs IS NULL
 AND (SELECT array_agg(a.attname::text ORDER BY a.attname::text) FROM unnest(i.indkey) WITH ORDINALITY x(attnum,ord) JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=x.attnum WHERE x.ord<=i.indnkeyatts)
 = (SELECT array_agg(k ORDER BY k) FROM unnest($3::text[]) k))
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass($2)`, pub, qTable(table), keys).Scan(&ok)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%s: requires plain unfiltered table, all publication columns, no generated columns, and non-NULL scalar source-key backed by UNIQUE/PRIMARY KEY", table)
	}
	return nil
}
func (r *runtime) close() {
	if r.src != nil {
		r.src.Close(context.Background())
	}
	if r.dst != nil {
		r.dst.Close(context.Background())
	}
}
func (r *runtime) prepare() error {
	if r.c.sidecar() {
		if e := ensureIdentitySchema(r.ctx, r.src); e != nil {
			return e
		}
		if e := ensureIdentitySchema(r.ctx, r.dst); e != nil {
			return e
		}
		for _, table := range r.tables {
			keys, _ := r.c.sourceKey(table)
			if e := r.registerKeys(r.src, r.c.Source.ID, table, keys); e != nil {
				return e
			}
		}
		targets, err := r.c.targetRoutes(r.tables)
		if err != nil {
			return err
		}
		for _, route := range targets {
			if e := r.registerKeys(r.dst, r.c.Sink.ID, route.Target, route.Keys); e != nil {
				return e
			}
		}
	} else if _, e := r.dst.Exec(r.ctx, metadataDDL); e != nil {
		return e
	}
	if r.c.Ownership != nil {
		if _, e := r.dst.Exec(r.ctx, ownershipDDL); e != nil {
			return e
		}
	}
	var exists bool
	if e := r.dst.QueryRow(r.ctx, `SELECT pg_replication_origin_oid($1) IS NOT NULL`, r.c.Sink.Origin).Scan(&exists); e != nil {
		return e
	}
	if !exists {
		if _, e := r.dst.Exec(r.ctx, `SELECT pg_replication_origin_create($1)`, r.c.Sink.Origin); e != nil {
			return e
		}
	}
	_, e := r.dst.Exec(r.ctx, `SELECT pg_replication_origin_session_setup($1)`, r.c.Sink.Origin)
	return e
}
func (r *runtime) position() (pglogrepl.LSN, bool, error) {
	var source, slot, hash, lsn string
	e := r.dst.QueryRow(r.ctx, `SELECT source_id,slot,config_hash,end_lsn::text FROM _pgcat_yaml.checkpoint WHERE job=$1`, r.c.Job).Scan(&source, &slot, &hash, &lsn)
	if errors.Is(e, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if e != nil {
		return 0, false, e
	}
	if source != r.c.Source.ID || slot != r.c.Source.Slot || hash != r.c.digest {
		return 0, false, fmt.Errorf("configuration changed: create a versioned new job/slot and controlled resnapshot; refusing to mix plans")
	}
	p, e := pglogrepl.ParseLSN(lsn)
	return p, true, e
}
func (r *runtime) init(recreate bool) error {
	if _, ok, e := r.position(); e != nil {
		return e
	} else if ok {
		return fmt.Errorf("job already initialized; use run")
	}
	var exists bool
	if e := r.src.QueryRow(r.ctx, `SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)`, r.c.Source.Slot).Scan(&exists); e != nil {
		return e
	}
	if exists {
		if !recreate {
			return fmt.Errorf("slot already exists without checkpoint; inspect it, then explicitly init --recreate-slot if safe")
		}
		if _, e := r.src.Exec(r.ctx, `SELECT pg_drop_replication_slot($1)`, r.c.Source.Slot); e != nil {
			return e
		}
	}
	cfg, e := pgconn.ParseConfig(os.Getenv(r.c.Source.DSNEnv))
	if e != nil {
		return e
	}
	cfg.RuntimeParams["replication"] = "database"
	repl, e := pgconn.ConnectConfig(r.ctx, cfg)
	if e != nil {
		return e
	}
	defer repl.Close(context.Background())
	slot, e := pglogrepl.CreateReplicationSlot(r.ctx, repl, r.c.Source.Slot, r.c.Source.Plugin, pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication, SnapshotAction: "EXPORT_SNAPSHOT"})
	if e != nil {
		return e
	}
	snap, e := r.src.BeginTx(r.ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if e != nil {
		return e
	}
	defer snap.Rollback(context.Background())
	if _, e = snap.Exec(r.ctx, "SET TRANSACTION SNAPSHOT '"+strings.ReplaceAll(slot.SnapshotName, "'", "''")+"'"); e != nil {
		return e
	}
	var bindings map[string]binding
	if r.c.sidecar() {
		bindings, e = r.loadBindings(snap)
		if e != nil {
			return e
		}
	}
	tx, e := r.dst.Begin(r.ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(context.Background())
	identityIndex := 0
	for _, table := range r.tables {
		keys, _ := r.c.sourceKey(table)
		rows, err := snap.Query(r.ctx, "SELECT to_jsonb(t)::text FROM "+qTable(table)+" t")
		if err != nil {
			return err
		}
		count := 0
		for rows.Next() {
			var raw string
			if e = rows.Scan(&raw); e != nil {
				rows.Close()
				return e
			}
			row, err := decodeJSON([]byte(raw))
			if err != nil {
				rows.Close()
				return err
			}
			pk, err := rowKey(row, keys)
			if err != nil {
				rows.Close()
				return err
			}
			// INSERT without upsert detects duplicate configured source identities.
			if _, err = tx.Exec(r.ctx, `INSERT INTO _pgcat_yaml.row_state VALUES($1,$2,$3,$4::text::jsonb)`, r.c.Job, table, pk, raw); err != nil {
				rows.Close()
				return err
			}
			var identity *RecordIdentity
			if r.c.sidecar() {
				bound, ok := bindings[table+"\x00"+pk]
				if ok && bound.Alive {
					copy := bound.Identity
					identity = &copy
				} else {
					if r.c.Identity.Bootstrap == "require-binding" || ok {
						rows.Close()
						return fmt.Errorf("snapshot row %s key=%s has no live binding; reconcile or explicitly bootstrap local original data", table, pk)
					}
					identity = newIdentity(r.c.Identity.Namespace, r.c.Source.ID, table, pk, "snapshot:"+slot.ConsistentPoint)
				}
				if err = r.putIdentityState(tx, table, pk, identity); err != nil {
					rows.Close()
					return err
				}
				if err = r.queueSourceBinding(tx, slot.ConsistentPoint, identityIndex, table, pk, identity, true); err != nil {
					rows.Close()
					return err
				}
				identityIndex++
			}
			owned := row["_sync_source"] == nil || fmt.Sprint(row["_sync_source"]) == r.c.Source.ID
			if r.c.sidecar() {
				owned = identity.Owner.Source == r.c.Source.ID
			}
			if r.c.Source.Snapshot == "copy" && owned {
				if err = r.guardOwnership(tx, nil, row); err != nil {
					rows.Close()
					return err
				}
				plan, err := r.c.planIdentity(table, nil, row, identity)
				if err != nil {
					rows.Close()
					return err
				}
				for _, m := range plan {
					if err = r.apply(tx, m); err != nil {
						rows.Close()
						return err
					}
				}
			}
			count++
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		log.Printf("snapshot table=%s rows=%d mode=%s", table, count, r.c.Source.Snapshot)
	}
	if e = r.checkpoint(tx, slot.ConsistentPoint); e != nil {
		return e
	}
	if _, e = tx.Exec(r.ctx, `SELECT pg_replication_origin_xact_setup($1::pg_lsn,clock_timestamp())`, slot.ConsistentPoint); e != nil {
		return e
	}
	if e = tx.Commit(r.ctx); e != nil {
		return e
	}
	if e = snap.Commit(r.ctx); e != nil {
		return e
	}
	log.Printf("initialized job=%s consistent_lsn=%s", r.c.Job, slot.ConsistentPoint)
	if r.c.sidecar() {
		return r.flushIdentityOutbox()
	}
	return nil
}

type wireChange struct {
	rel      *pglogrepl.RelationMessage
	old, new *pglogrepl.TupleData
	kind     byte
	identity *RecordIdentity
}

func tupleRow(rel *pglogrepl.RelationMessage, t *pglogrepl.TupleData) (Row, error) {
	if t == nil {
		return nil, nil
	}
	if len(t.Columns) != len(rel.Columns) {
		return nil, fmt.Errorf("requires FULL tuple width for %s.%s", rel.Namespace, rel.RelationName)
	}
	out := Row{}
	for i, c := range t.Columns {
		switch c.DataType {
		case 'u':
			continue
		case 'n':
			out[rel.Columns[i].Name] = nil
		case 't':
			v, e := decodeValue(c.Data, rel.Columns[i].DataType)
			if e != nil {
				return nil, e
			}
			out[rel.Columns[i].Name] = v
		default:
			return nil, fmt.Errorf("unsupported tuple format %q", c.DataType)
		}
	}
	return out, nil
}
func (r *runtime) commit(changes []wireChange, end pglogrepl.LSN, origin string) error {
	tx, e := r.dst.Begin(r.ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(context.Background())
	identityIndex := 0
	for ordinal, ch := range changes {
		table := ch.rel.Namespace + "." + ch.rel.RelationName
		keys, e := r.c.sourceKey(table)
		if e != nil {
			return e
		}
		old, e := tupleRow(ch.rel, ch.old)
		if e != nil {
			return e
		}
		new, e := tupleRow(ch.rel, ch.new)
		if e != nil {
			return e
		}
		var before, after Row
		var identity *RecordIdentity
		var oldPK string
		if ch.kind != 'I' {
			if old == nil {
				return fmt.Errorf("missing old FULL image for %s", table)
			}
			oldPK, e = rowKey(old, keys)
			if e != nil {
				return e
			}
			saved, err := r.getState(tx, table, oldPK)
			if err != nil && err != pgx.ErrNoRows {
				return err
			}
			before = merge(saved, old)
			if r.c.sidecar() {
				identity, e = r.getIdentityState(tx, table, oldPK)
				if e != nil {
					return fmt.Errorf("missing historical identity for %s key=%s: %w", table, oldPK, e)
				}
			}
		}
		if ch.kind != 'D' {
			after = merge(before, new)
			for _, col := range ch.rel.Columns {
				if _, ok := after[col.Name]; !ok {
					return fmt.Errorf("unavailable TOAST/full image %s.%s; stop and resnapshot", table, col.Name)
				}
			}
		}
		if r.c.sidecar() {
			if origin != "" {
				if ch.identity == nil {
					return fmt.Errorf("remote-origin business DML lacks matching transactional identity message: %s", table)
				}
				if identity != nil && !sameIdentity(identity, ch.identity) {
					return fmt.Errorf("remote identity does not match captured old record at %s", table)
				}
				identity = ch.identity
			} else {
				if ch.identity != nil {
					return fmt.Errorf("identity message on an unmarked local transaction")
				}
				if ch.kind == 'I' {
					pk, err := rowKey(after, keys)
					if err != nil {
						return err
					}
					identity = newIdentity(r.c.Identity.Namespace, r.c.Source.ID, table, pk, fmt.Sprintf("wal:%s:%d", end.String(), ordinal))
				}
			}
		}
		if origin == "" {
			if e = r.guardOwnership(tx, before, after); e != nil {
				return e
			}
			plan, err := r.c.planIdentity(table, before, after, identity)
			if err != nil {
				return err
			}
			for _, m := range plan {
				if err = r.apply(tx, m); err != nil {
					return err
				}
			}
		}
		if before != nil {
			if _, e = tx.Exec(r.ctx, `DELETE FROM _pgcat_yaml.row_state WHERE job=$1 AND source_table=$2 AND pk=$3`, r.c.Job, table, oldPK); e != nil {
				return e
			}
			if r.c.sidecar() {
				if _, e = tx.Exec(r.ctx, `DELETE FROM _pgcat_yaml.identity_state WHERE job=$1 AND source_table=$2 AND pk=$3`, r.c.Job, table, oldPK); e != nil {
					return e
				}
				if e = r.queueSourceBinding(tx, end.String(), identityIndex, table, oldPK, identity, false); e != nil {
					return e
				}
				identityIndex++
			}
		}
		if after != nil {
			pk, err := rowKey(after, keys)
			if err != nil {
				return err
			}
			if err = r.putState(tx, table, pk, after); err != nil {
				return err
			}
			if r.c.sidecar() {
				if err = r.putIdentityState(tx, table, pk, identity); err != nil {
					return err
				}
				if err = r.queueSourceBinding(tx, end.String(), identityIndex, table, pk, identity, true); err != nil {
					return err
				}
				identityIndex++
			}
		}
	}
	if e = r.checkpoint(tx, end.String()); e != nil {
		return e
	}
	if _, e = tx.Exec(r.ctx, `SELECT pg_replication_origin_xact_setup($1::pg_lsn,clock_timestamp())`, end.String()); e != nil {
		return e
	}
	if e = tx.Commit(r.ctx); e != nil {
		return e
	}
	log.Printf("commit job=%s lsn=%s rows=%d replay=%t", r.c.Job, end, len(changes), origin != "")
	return nil
}
func (r *runtime) poll() (bool, error) {
	pos, ok, e := r.position()
	if e != nil {
		return false, e
	}
	if !ok {
		return false, fmt.Errorf("run init first")
	}
	var plugin, database, confirmed string
	if e = r.src.QueryRow(r.ctx, `SELECT plugin,database,confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1`, r.c.Source.Slot).Scan(&plugin, &database, &confirmed); e != nil {
		return false, e
	}
	if plugin != r.c.Source.Plugin {
		return false, fmt.Errorf("slot plugin mismatch")
	}
	if database != r.src.Config().Database {
		return false, fmt.Errorf("slot belongs to another database")
	}
	cp, e := pglogrepl.ParseLSN(confirmed)
	if e != nil {
		return false, e
	}
	if cp > pos {
		return false, fmt.Errorf("source slot ahead of target checkpoint; possible target restore: resnapshot required")
	}
	// Validate the saved configuration and slot before any source metadata write.
	if r.c.sidecar() {
		if e = r.flushIdentityOutbox(); e != nil {
			return false, e
		}
	}
	args := []any{r.c.Source.Slot, "proto_version", "1", "publication_names", r.c.Source.Publication}
	sql := `SELECT data FROM pg_logical_slot_peek_binary_changes($1,NULL,1000,$2,$3,$4,$5)`
	// Read remote-origin images to maintain the row-state ledger, but never forward
	// them. This is needed for later local edits of replicas containing TOAST.
	if plugin == "pgoutput" {
		sql = `SELECT data FROM pg_logical_slot_peek_binary_changes($1,NULL,1000,$2,$3,$4,$5,$6,$7)`
		args = append(args, "origin", "any")
	}
	if r.c.sidecar() {
		sql = `SELECT data FROM pg_logical_slot_peek_binary_changes($1,NULL,1000,$2,$3,$4,$5,$6,$7,$8,$9)`
		args = append(args, "messages", "true")
	}
	rows, e := r.src.Query(r.ctx, sql, args...)
	if e != nil {
		return false, e
	}
	var frames [][]byte
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			rows.Close()
			return false, e
		}
		frames = append(frames, b)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return false, e
	}
	relations := map[uint32]*pglogrepl.RelationMessage{}
	var changes []wireChange
	origin := ""
	inTx := false
	last := pos
	for _, b := range frames {
		msg, err := pglogrepl.Parse(b)
		if err != nil {
			return false, err
		}
		switch m := msg.(type) {
		case *pglogrepl.RelationMessage:
			relations[m.RelationID] = m
		case *pglogrepl.BeginMessage:
			inTx = true
			origin = ""
			changes = nil
		case *pglogrepl.OriginMessage:
			origin = m.Name
		case *pglogrepl.LogicalDecodingMessage:
			if !r.c.sidecar() || m.Prefix != identityPrefix {
				continue
			}
			if !m.Transactional || !inTx || len(m.Content) > identityMessageLimit {
				return false, fmt.Errorf("invalid transactional identity message")
			}
			var event IdentityEvent
			if err = json.Unmarshal(m.Content, &event); err != nil {
				return false, err
			}
			if event.Namespace != r.c.Identity.Namespace || event.Node != r.c.Source.ID || len(r.c.routes(event.Table)) == 0 {
				continue
			}
			if err = matchIdentityEvent(r.c, changes, event); err != nil {
				return false, err
			}
		case *pglogrepl.InsertMessage:
			rel := relations[m.RelationID]
			if rel == nil {
				return false, fmt.Errorf("missing relation")
			}
			changes = append(changes, wireChange{rel: rel, new: m.Tuple, kind: 'I'})
		case *pglogrepl.UpdateMessage:
			rel := relations[m.RelationID]
			if rel == nil {
				return false, fmt.Errorf("missing relation")
			}
			changes = append(changes, wireChange{rel: rel, old: m.OldTuple, new: m.NewTuple, kind: 'U'})
		case *pglogrepl.DeleteMessage:
			rel := relations[m.RelationID]
			if rel == nil {
				return false, fmt.Errorf("missing relation")
			}
			changes = append(changes, wireChange{rel: rel, old: m.OldTuple, kind: 'D'})
		case *pglogrepl.CommitMessage:
			if m.TransactionEndLSN > pos {
				if err = r.commit(changes, m.TransactionEndLSN, origin); err != nil {
					return false, err
				}
				last = m.TransactionEndLSN
			}
			inTx = false
			changes = nil
		case *pglogrepl.TruncateMessage:
			return false, fmt.Errorf("TRUNCATE is forbidden for mapped tables")
		case *pglogrepl.TypeMessage: // Type announcements are safe; unknown data values fail in decodeValue.
		default:
			return false, fmt.Errorf("unsupported pgoutput message %T", m)
		}
	}
	if inTx {
		return false, fmt.Errorf("decoder returned incomplete transaction; no slot advancement")
	}
	if r.c.sidecar() {
		if e = r.flushIdentityOutbox(); e != nil {
			return false, e
		}
	}
	if last > cp {
		if r.beforeAdvance != nil {
			if err := r.beforeAdvance(); err != nil {
				return false, err
			}
		}
		if _, e = r.src.Exec(r.ctx, `SELECT pg_replication_slot_advance($1,$2::pg_lsn)`, r.c.Source.Slot, last.String()); e != nil {
			return false, e
		}
	}
	return last > pos, nil
}
func (r *runtime) run(once bool) error {
	for {
		_, e := r.poll()
		if e != nil {
			return e
		}
		if once {
			return nil
		}
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}
