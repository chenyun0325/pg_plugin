package replicator

import (
	"context"
	"fmt"
	"strings"

	"github.com/pgflo/pg_flo/pkg/utils"
)

// AddPrimaryKeyInfo adds replication key information to the CDCMessage
func (r *BaseReplicator) AddPrimaryKeyInfo(message *utils.CDCMessage, table string) {
	if key, ok := r.TableReplicationKeys[message.Schema+"."+message.Table]; ok {
		message.ReplicationKey = key
		return
	}
	if key, ok := r.TableReplicationKeys[table]; ok {
		message.ReplicationKey = key
	} else {
		r.Logger.Error().
			Str("table", table).
			Msg("No replication key information found for table. This should not happen as validation is done during initialization")
	}
}

// InitializePrimaryKeyInfo initializes primary key information for all tables
func (r *BaseReplicator) InitializePrimaryKeyInfo() error {
	if r.Config.CaptureContext {
		return r.initializeReplayKeys()
	}
	query := `
		WITH table_info AS (
			SELECT
				t.tablename,
				c.relreplident,
				(
					SELECT array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum))
					FROM pg_index i
					JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
					WHERE i.indrelid = c.oid AND i.indisprimary
				) as pk_columns,
				(
					SELECT array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum))
					FROM pg_index i
					JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
					WHERE i.indrelid = c.oid AND i.indisunique AND NOT i.indisprimary
					LIMIT 1
				) as unique_columns
			FROM pg_tables t
			JOIN pg_class c ON t.tablename = c.relname
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE t.schemaname = $1
		)
		SELECT
			tablename,
			relreplident::text,
			COALESCE(pk_columns, ARRAY[]::text[]) as pk_columns,
			COALESCE(unique_columns, ARRAY[]::text[]) as unique_columns
		FROM table_info;
	`

	rows, err := r.StandardConn.Query(context.Background(), query, r.Config.Schema)
	if err != nil {
		return fmt.Errorf("failed to query replication key info: %v", err)
	}
	defer rows.Close()

	r.TableReplicationKeys = make(map[string]utils.ReplicationKey)

	for rows.Next() {
		var (
			tableName       string
			replicaIdentity string
			pkColumns       []string
			uniqueColumns   []string
		)

		if err := rows.Scan(&tableName, &replicaIdentity, &pkColumns, &uniqueColumns); err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		key := utils.ReplicationKey{}

		switch {
		case len(pkColumns) > 0:
			key = utils.ReplicationKey{
				Type:    utils.ReplicationKeyPK,
				Columns: pkColumns,
			}
		case len(uniqueColumns) > 0:
			key = utils.ReplicationKey{
				Type:    utils.ReplicationKeyUnique,
				Columns: uniqueColumns,
			}
		case replicaIdentity == "f":
			key = utils.ReplicationKey{
				Type:    utils.ReplicationKeyFull,
				Columns: nil,
			}
		}

		if err := r.validateTableReplicationKey(tableName, key); err != nil {
			r.Logger.Warn().
				Str("table", tableName).
				Str("replica_identity", replicaIdentity).
				Str("key_type", string(key.Type)).
				Strs("columns", key.Columns).
				Err(err).
				Msg("Invalid replication key configuration")
			continue
		}

		r.TableReplicationKeys[tableName] = key

		r.Logger.Debug().
			Str("table", tableName).
			Str("key_type", string(key.Type)).
			Strs("columns", key.Columns).
			Str("replica_identity", replicaIdentity).
			Msg("Initialized replication key configuration")
	}

	return rows.Err()
}

// GetConfiguredTables returns all tables based on configuration
// If no specific tables are configured, returns all tables from the configured schema
func (r *BaseReplicator) GetConfiguredTables(ctx context.Context) ([]string, error) {
	if len(r.Config.Tables) > 0 {
		fullyQualifiedTables := make([]string, len(r.Config.Tables))
		for i, table := range r.Config.Tables {
			if strings.Contains(table, ".") {
				fullyQualifiedTables[i] = table
			} else {
				fullyQualifiedTables[i] = fmt.Sprintf("%s.%s", r.Config.Schema, table)
			}
		}
		return fullyQualifiedTables, nil
	}

	rows, err := r.StandardConn.Query(ctx, `
		SELECT schemaname || '.' || tablename
		FROM pg_tables
		WHERE schemaname = $1
		AND schemaname NOT IN ('pg_catalog', 'information_schema', 'internal_pg_flo')
	`, r.Config.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %v", err)
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

func (r *BaseReplicator) initializeReplayKeys() error {
	r.TableReplicationKeys = make(map[string]utils.ReplicationKey)
	tables, err := r.GetConfiguredTables(context.Background())
	if err != nil {
		return err
	}
	for _, table := range tables {
		parts := strings.Split(table, ".")
		if len(parts) != 2 {
			return fmt.Errorf("invalid source table %s", table)
		}
		var keys []string
		var identity string
		err := r.StandardConn.QueryRow(context.Background(), `SELECT c.relreplident::text,
 ARRAY(SELECT a.attname FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,n)
 JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
 WHERE i.indrelid=c.oid AND i.indisprimary ORDER BY k.n)
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2`, parts[0], parts[1]).Scan(&identity, &keys)
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return fmt.Errorf("replay source %s needs a primary key", table)
		}
		if table != "cdc_meta.tx_context" && identity != "f" {
			return fmt.Errorf("replay source %s needs REPLICA IDENTITY FULL", table)
		}
		r.TableReplicationKeys[table] = utils.ReplicationKey{Type: utils.ReplicationKeyPK, Columns: keys}
	}
	return nil
}

func (r *BaseReplicator) validateTableReplicationKey(tableName string, key utils.ReplicationKey) error {
	if !key.IsValid() {
		return fmt.Errorf(
			"table %q requires one of the following:\n"+
				"\t1. A PRIMARY KEY constraint\n"+
				"\t2. A UNIQUE constraint\n"+
				"\t3. REPLICA IDENTITY FULL (ALTER TABLE %s REPLICA IDENTITY FULL)",
			tableName, tableName)
	}
	return nil
}
