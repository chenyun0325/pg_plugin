package pgmapping

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// SQLStore requires a dedicated DB connection per source/epoch, a PostgreSQL driver
// registered by the host, pre-created origin, and sql/schema.sql installed.
// Do not put arbitrary business writes on this connection.
type SQLStore struct {
	Conn   *sql.Conn
	Origin string
}

func quote(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func table(s string) string { p := strings.Split(s, "."); return quote(p[0]) + "." + quote(p[1]) }
func (s SQLStore) Apply(p Plan) error {
	ctx := context.Background()
	t := p.Transaction
	if s.Origin == "" {
		return fmt.Errorf("origin required")
	}
	// Session setup survives transactions; a reconnect must repeat this operation.
	if _, err := s.Conn.ExecContext(ctx, "SELECT pg_replication_origin_session_setup($1)", s.Origin); err != nil {
		return err
	}
	defer s.Conn.ExecContext(ctx, "SELECT pg_replication_origin_session_reset()")
	tx, err := s.Conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO sync_meta.progress(source,epoch,seq) VALUES($1,$2,0) ON CONFLICT DO NOTHING`, t.Source, t.Epoch); err != nil {
		return err
	}
	var seq uint64
	if err = tx.QueryRowContext(ctx, `SELECT seq FROM sync_meta.progress WHERE source=$1 AND epoch=$2 FOR UPDATE`, t.Source, t.Epoch).Scan(&seq); err != nil {
		return err
	}
	var exists bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sync_meta.applied WHERE source=$1 AND epoch=$2 AND txid=$3)`, t.Source, t.Epoch, t.ID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return tx.Commit()
	}
	if t.Sequence != seq+1 {
		return fmt.Errorf("sequence gap or stale unknown transaction: got %d want %d", t.Sequence, seq+1)
	}
	for _, m := range p.Mutations {
		if !qualified(m.Table) {
			return fmt.Errorf("invalid target table")
		}
		args := []any{}
		var query string
		if m.Kind == "delete" {
			var terms []string
			for i, k := range identityColumns {
				args = append(args, m.Key[k])
				terms = append(terms, fmt.Sprintf("%s=$%d", quote(k), i+1))
			}
			query = "DELETE FROM " + table(m.Table) + " WHERE " + strings.Join(terms, " AND ")
		} else if m.Kind == "upsert" {
			var names []string
			for k := range m.Values {
				names = append(names, k)
			}
			sort.Strings(names)
			var cols, params, updates []string
			for i, k := range names {
				v := m.Values[k]
				switch x := v.(type) {
				case json.Number:
					v = x.String()
				case map[string]any, []any:
					b, e := json.Marshal(x)
					if e != nil {
						return e
					}
					v = string(b)
				}
				cols = append(cols, quote(k))
				params = append(params, fmt.Sprintf("$%d", i+1))
				args = append(args, v)
				if !strings.HasPrefix(k, "_sync_") {
					updates = append(updates, quote(k)+"=EXCLUDED."+quote(k))
				}
			}
			query = "INSERT INTO " + table(m.Table) + " (" + strings.Join(cols, ",") + ") VALUES (" + strings.Join(params, ",") + ") ON CONFLICT (\"_sync_source\",\"_sync_table\",\"_sync_pk\") DO UPDATE SET " + strings.Join(updates, ",")
		} else {
			return fmt.Errorf("unknown mutation %s", m.Kind)
		}
		if _, err = tx.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO sync_meta.applied(source,epoch,txid,version) VALUES($1,$2,$3,$4)`, t.Source, t.Epoch, t.ID, t.Version); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE sync_meta.progress SET seq=$3,end_lsn=$4::pg_lsn WHERE source=$1 AND epoch=$2`, t.Source, t.Epoch, t.Sequence, t.EndLSN); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `SELECT pg_replication_origin_xact_setup($1::pg_lsn, clock_timestamp())`, t.EndLSN); err != nil {
		return err
	}
	return tx.Commit()
}
