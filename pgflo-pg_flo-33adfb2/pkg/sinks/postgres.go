package sinks

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/rs/zerolog/log"
)

// PostgresSink represents a sink for PostgreSQL database
type PostgresSink struct {
	conn                    *pgx.Conn
	disableForeignKeyChecks bool
	connConfig              *pgx.ConnConfig
	retryConfig             utils.RetryConfig
	logger                  utils.Logger
	replicationOrigin       string
}

type PostgresSinkConfig struct {
	TargetHost              string
	TargetPort              int
	TargetDBName            string
	TargetUser              string
	TargetPassword          string
	SyncSchema              bool
	SourceHost              string
	SourcePort              int
	SourceDBName            string
	SourceUser              string
	SourcePassword          string
	DisableForeignKeyChecks bool
	ReplicationOrigin       string
}

// NewPostgresSink creates a new PostgresSink instance
func NewPostgresSink(targetHost string, targetPort int, targetDBName, targetUser, targetPassword string, syncSchema bool, sourceHost string, sourcePort int, sourceDBName, sourceUser, sourcePassword string, disableForeignKeyChecks bool) (*PostgresSink, error) {
	return NewPostgresSinkWithConfig(PostgresSinkConfig{
		TargetHost: targetHost, TargetPort: targetPort, TargetDBName: targetDBName,
		TargetUser: targetUser, TargetPassword: targetPassword, SyncSchema: syncSchema,
		SourceHost: sourceHost, SourcePort: sourcePort, SourceDBName: sourceDBName,
		SourceUser: sourceUser, SourcePassword: sourcePassword,
		DisableForeignKeyChecks: disableForeignKeyChecks,
	})
}

func NewPostgresSinkWithConfig(config PostgresSinkConfig) (*PostgresSink, error) {
	connConfig, err := pgx.ParseConfig(utils.PostgresURL(config.TargetHost, config.TargetPort, config.TargetDBName, config.TargetUser, config.TargetPassword))
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection config: %v", err)
	}

	sink := &PostgresSink{
		connConfig:              connConfig,
		disableForeignKeyChecks: config.DisableForeignKeyChecks,
		replicationOrigin:       config.ReplicationOrigin,
		logger:                  utils.NewZerologLogger(log.With().Str("component", "postgres_sink").Logger()),
		retryConfig: utils.RetryConfig{
			MaxAttempts: 5,
			InitialWait: 1 * time.Second,
			MaxWait:     30 * time.Second,
		},
	}

	if err := sink.connect(context.Background()); err != nil {
		return nil, err
	}
	if err := sink.ensureMetadata(context.Background()); err != nil {
		return nil, err
	}

	if config.SyncSchema {
		if err := sink.syncSchema(config.SourceHost, config.SourcePort, config.SourceDBName, config.SourceUser, config.SourcePassword); err != nil {
			return nil, err
		}
	}

	return sink, nil
}

func (s *PostgresSink) ensureMetadata(ctx context.Context) error {
	if _, err := s.conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS pgflo_meta"); err != nil {
		return fmt.Errorf("create pgflo metadata schema: %w", err)
	}
	_, err := s.conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS pgflo_meta.applied_transactions (
			source_id text NOT NULL,
			transaction_id text NOT NULL,
			mapping_version text NOT NULL,
			commit_lsn pg_lsn,
			applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			PRIMARY KEY (source_id, transaction_id)
		)`)
	if err != nil {
		return fmt.Errorf("create pgflo transaction ledger: %w", err)
	}
	return nil
}

// New method to handle connection
func (s *PostgresSink) connect(ctx context.Context) error {
	var connMutex sync.Mutex

	return utils.WithRetry(ctx, s.retryConfig, func() error {
		conn, err := pgx.ConnectConfig(ctx, s.connConfig)
		if err != nil {
			s.logger.Error().Err(err).Msg("Failed to connect to database, will retry")
			return err
		}
		connMutex.Lock()
		s.conn = conn
		connMutex.Unlock()
		return nil
	})
}

// syncSchema synchronizes the schema between source and target databases
func (s *PostgresSink) syncSchema(sourceHost string, sourcePort int, sourceDBName, sourceUser, sourcePassword string) error {
	dumpCmd := exec.Command("pg_dump", "--schema-only")
	dumpCmd.Env = append(os.Environ(),
		fmt.Sprintf("PGHOST=%s", sourceHost),
		fmt.Sprintf("PGPORT=%d", sourcePort),
		fmt.Sprintf("PGDATABASE=%s", sourceDBName),
		fmt.Sprintf("PGUSER=%s", sourceUser),
		fmt.Sprintf("PGPASSWORD=%s", sourcePassword),
	)
	schemaDump, err := dumpCmd.Output()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to dump schema from source database")
		return fmt.Errorf("failed to dump schema from source database: %v", err)
	}

	applyCmd := exec.Command("psql")
	applyCmd.Env = append(os.Environ(),
		fmt.Sprintf("PGHOST=%s", s.conn.Config().Host),
		fmt.Sprintf("PGPORT=%d", s.conn.Config().Port),
		fmt.Sprintf("PGDATABASE=%s", s.conn.Config().Database),
		fmt.Sprintf("PGUSER=%s", s.conn.Config().User),
		fmt.Sprintf("PGPASSWORD=%s", s.conn.Config().Password),
	)
	applyCmd.Stdin = strings.NewReader(string(schemaDump))

	output, err := applyCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to apply schema to target database: %v\nOutput: %s", err, string(output))
	}

	return nil
}

// handleInsert processes an insert operation
func (s *PostgresSink) handleInsert(tx pgx.Tx, message *utils.CDCMessage) error {
	columns := make([]string, 0, len(message.Columns))
	placeholders := make([]string, 0, len(message.Columns))
	values := make([]interface{}, 0, len(message.Columns))

	paramIndex := 1
	for _, col := range message.Columns {
		value, err := message.GetColumnValue(col.Name, false)
		if err != nil {
			return fmt.Errorf("failed to get column value: %w", err)
		}

		columns = append(columns, quoteIdentifier(col.Name))
		placeholders = append(placeholders, fmt.Sprintf("$%d", paramIndex))
		values = append(values, value)
		paramIndex++
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		quoteTable(message.Schema, message.Table),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	// Add an explicit conflict policy. Legacy messages keep DO NOTHING for
	// compatibility; mapped jobs must choose upsert, ignore, or error in YAML.
	if message.ReplicationKey.Type != utils.ReplicationKeyFull && len(message.ReplicationKey.Columns) > 0 {
		keys := quotedIdentifiers(message.ReplicationKey.Columns)
		switch message.ConflictAction {
		case utils.ConflictActionError:
		case utils.ConflictActionUpsert:
			keySet := make(map[string]bool, len(message.ReplicationKey.Columns))
			for _, key := range message.ReplicationKey.Columns {
				keySet[key] = true
			}
			var updates []string
			for _, column := range message.Columns {
				if !keySet[column.Name] {
					quoted := quoteIdentifier(column.Name)
					updates = append(updates, quoted+" = EXCLUDED."+quoted)
				}
			}
			if len(updates) == 0 {
				query += fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", strings.Join(keys, ", "))
			} else {
				query += fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s", strings.Join(keys, ", "), strings.Join(updates, ", "))
			}
		case utils.ConflictActionIgnore, "":
			query += fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", strings.Join(keys, ", "))
		default:
			return fmt.Errorf("unknown conflict action %q", message.ConflictAction)
		}
	}

	_, err := tx.Exec(context.Background(), query, values...)
	if err != nil {
		return fmt.Errorf("insert failed: %w", err)
	}
	return nil
}

// getWhereConditions builds WHERE clause conditions based on the replication key type
func getWhereConditions(message *utils.CDCMessage, useOldValues bool, startingIndex int) ([]string, []interface{}, error) {
	var conditions []string
	var values []interface{}
	valueIndex := startingIndex

	switch message.ReplicationKey.Type {
	case utils.ReplicationKeyFull:
		for _, col := range message.Columns {
			value, err := message.GetColumnValue(col.Name, useOldValues)
			if err != nil {
				continue
			}
			if value == nil {
				conditions = append(conditions, fmt.Sprintf("%s IS NULL", quoteIdentifier(col.Name)))
			} else {
				conditions = append(conditions, fmt.Sprintf("%s = $%d", quoteIdentifier(col.Name), valueIndex))
				values = append(values, value)
				valueIndex++
			}
		}
	case utils.ReplicationKeyPK, utils.ReplicationKeyUnique:
		for _, colName := range message.ReplicationKey.Columns {
			value, err := message.GetColumnValue(colName, useOldValues)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to get value for key column %s: %w", colName, err)
			}
			if value == nil {
				conditions = append(conditions, fmt.Sprintf("%s IS NULL", quoteIdentifier(colName)))
			} else {
				conditions = append(conditions, fmt.Sprintf("%s = $%d", quoteIdentifier(colName), valueIndex))
				values = append(values, value)
				valueIndex++
			}
		}
	}

	if len(conditions) == 0 {
		return nil, nil, fmt.Errorf("no valid conditions generated for WHERE clause")
	}

	return conditions, values, nil
}

// handleDelete processes a delete operation
func (s *PostgresSink) handleDelete(tx pgx.Tx, message *utils.CDCMessage) error {
	if !message.ReplicationKey.IsValid() {
		return fmt.Errorf("invalid replication key configuration")
	}

	startingIndex := 1
	whereConditions, whereValues, err := getWhereConditions(message, true, startingIndex)
	if err != nil {
		return fmt.Errorf("failed to build WHERE conditions: %w", err)
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s",
		quoteTable(message.Schema, message.Table),
		strings.Join(whereConditions, " AND "),
	)

	result, err := tx.Exec(context.Background(), query, whereValues...)
	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}

	if result.RowsAffected() == 0 {
		s.logger.Warn().
			Str("table", message.Table).
			Str("query", query).
			Interface("values", whereValues).
			Msg("Delete affected 0 rows")
	}

	return nil
}

// handleUpdate processes an update operation
func (s *PostgresSink) handleUpdate(tx pgx.Tx, message *utils.CDCMessage) error {
	if message.ConflictAction == utils.ConflictActionUpsert || message.ConflictAction == utils.ConflictActionIgnore {
		return s.handleInsert(tx, message)
	}
	if !message.ReplicationKey.IsValid() {
		return fmt.Errorf("invalid replication key configuration")
	}

	setClauses := make([]string, 0, len(message.Columns))
	setValues := make([]interface{}, 0, len(message.Columns))
	valueIndex := 1

	for _, column := range message.Columns {
		if message.IsColumnToasted(column.Name) {
			continue
		}

		value, err := message.GetColumnValue(column.Name, false)
		if err != nil {
			return fmt.Errorf("failed to get column value: %w", err)
		}

		if value == nil {
			setClauses = append(setClauses, fmt.Sprintf("%s = NULL", quoteIdentifier(column.Name)))
		} else {
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", quoteIdentifier(column.Name), valueIndex))
			setValues = append(setValues, value)
			valueIndex++
		}
	}

	if len(setClauses) == 0 {
		s.logger.Debug().Msg("No columns to update, skipping")
		return nil
	}

	whereConditions, whereValues, err := getWhereConditions(message, true, valueIndex)
	if err != nil {
		return fmt.Errorf("failed to build WHERE conditions: %w", err)
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		quoteTable(message.Schema, message.Table),
		strings.Join(setClauses, ", "),
		strings.Join(whereConditions, " AND "),
	)

	values := append(setValues, whereValues...)
	result, err := tx.Exec(context.Background(), query, values...)
	if err != nil {
		return fmt.Errorf("update failed for table %s.%s: %w (query: %s, values: %v)",
			message.Schema, message.Table, err, query, values)
	}

	if result.RowsAffected() == 0 {
		s.logger.Warn().
			Str("table", message.Table).
			Str("query", query).
			Interface("values", values).
			Msg("Update affected 0 rows")
	}

	return nil
}

// handleDDL processes a DDL operation
func (s *PostgresSink) handleDDL(tx pgx.Tx, message *utils.CDCMessage) (pgx.Tx, error) {
	if s.conn == nil || s.conn.IsClosed() {
		if err := s.connect(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to reconnect to database: %v", err)
		}
		newTx, err := s.conn.Begin(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to begin new transaction: %v", err)
		}
		tx = newTx
	}

	ddlCommand, err := message.GetColumnValue("ddl_command", false)
	if err != nil {
		return tx, fmt.Errorf("failed to get DDL command: %v", err)
	}

	ddlString, ok := ddlCommand.(string)
	if !ok {
		return tx, fmt.Errorf("DDL command is not a string")
	}

	s.logger.Debug().Msgf("Executing DDL: %s", ddlString)

	if strings.Contains(strings.ToUpper(ddlString), "CONCURRENTLY") {
		if err := tx.Commit(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to commit transaction before concurrent DDL: %v", err)
		}

		if _, err := s.conn.Exec(context.Background(), ddlString); err != nil {
			if strings.Contains(err.Error(), "does not exist") {
				s.logger.Warn().Msgf("Ignoring DDL for non-existent object: %s", ddlString)
				return s.conn.Begin(context.Background())
			}
			return nil, fmt.Errorf("failed to execute concurrent DDL: %v", err)
		}

		return s.conn.Begin(context.Background())
	}

	_, err = tx.Exec(context.Background(), ddlString)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			s.logger.Warn().Msgf("Ignoring DDL for non-existent object: %s", ddlString)
			return tx, nil
		}
		return tx, fmt.Errorf("failed to execute DDL: %v", err)
	}

	return tx, nil
}

// disableForeignKeys disables foreign key checks
func (s *PostgresSink) disableForeignKeys(ctx context.Context) error {
	_, err := s.conn.Exec(ctx, "SET session_replication_role = 'replica';")
	return err
}

// enableForeignKeys enables foreign key checks
func (s *PostgresSink) enableForeignKeys(ctx context.Context) error {
	_, err := s.conn.Exec(ctx, "SET session_replication_role = 'origin';")
	return err
}

// WriteBatch writes a batch of CDC messages to the target database
func (s *PostgresSink) WriteBatch(messages []*utils.CDCMessage) error {
	ctx := context.Background()

	if s.conn == nil || s.conn.IsClosed() {
		if err := s.connect(ctx); err != nil {
			return fmt.Errorf("failed to connect to database: %v", err)
		}
	}

	return s.writeBatchInternal(ctx, messages)
}

// New helper method to handle batch writing
func (s *PostgresSink) writeBatchInternal(ctx context.Context, messages []*utils.CDCMessage) error {
	if s.replicationOrigin != "" {
		if _, err := s.conn.Exec(ctx, "SELECT pg_replication_origin_session_setup($1)", s.replicationOrigin); err != nil {
			return fmt.Errorf("setup replication origin %s: %w", s.replicationOrigin, err)
		}
		defer func() {
			if _, err := s.conn.Exec(ctx, "SELECT pg_replication_origin_session_reset()"); err != nil {
				s.logger.Error().Err(err).Msg("failed to reset replication origin session")
			}
		}()
	}
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	defer func() {
		if tx != nil {
			if err := tx.Rollback(ctx); err != nil {
				s.logger.Error().Err(err).Msg("failed to rollback transaction")
			}
		}
	}()

	if s.disableForeignKeyChecks {
		if err := s.disableForeignKeys(ctx); err != nil {
			return fmt.Errorf("failed to disable foreign key checks: %v", err)
		}
		defer func() {
			if err := s.enableForeignKeys(ctx); err != nil {
				s.logger.Error().Err(err).Msg("failed to re-enable foreign key checks")
			}
		}()
	}

	transactionState := make(map[string]bool)
	appliedAny := false
	lastAppliedLSN := ""
	for _, message := range messages {
		if message.SourceID != "" && message.TransactionID != "" {
			transactionKey := message.SourceID + "\x00" + message.TransactionID
			apply, checked := transactionState[transactionKey]
			if !checked {
				var inserted int
				err := tx.QueryRow(ctx, `
					INSERT INTO pgflo_meta.applied_transactions(source_id,transaction_id,mapping_version,commit_lsn)
					VALUES($1,$2,$3,NULLIF($4::text,'')::pg_lsn)
					ON CONFLICT DO NOTHING
					RETURNING 1`, message.SourceID, message.TransactionID, message.MappingVersion, message.LSN).Scan(&inserted)
				if err != nil && !errors.Is(err, pgx.ErrNoRows) {
					return fmt.Errorf("record transaction ledger: %w", err)
				}
				apply = err == nil && inserted == 1
				transactionState[transactionKey] = apply
			}
			if !apply {
				continue
			}
		}
		appliedAny = true
		if message.LSN != "" {
			lastAppliedLSN = message.LSN
		}

		var operationErr error
		err := utils.WithRetry(ctx, s.retryConfig, func() error {
			if s.conn == nil || s.conn.IsClosed() {
				if err := s.connect(ctx); err != nil {
					return fmt.Errorf("failed to reconnect to database: %v", err)
				}
				if tx == nil {
					newTx, err := s.conn.Begin(ctx)
					if err != nil {
						return fmt.Errorf("failed to begin new transaction: %v", err)
					}
					tx = newTx
				}
			}

			switch message.Type {
			case utils.OperationInsert:
				if message.RequireExisting || len(message.JSONPatches) > 0 {
					operationErr = s.applyProjection(ctx, tx, message)
				} else {
					operationErr = s.handleInsert(tx, message)
				}
			case utils.OperationUpdate:
				if message.RequireExisting || len(message.JSONPatches) > 0 {
					operationErr = s.applyProjection(ctx, tx, message)
				} else {
					operationErr = s.handleUpdate(tx, message)
				}
			case utils.OperationDelete:
				operationErr = s.handleDelete(tx, message)
			case utils.OperationDDL:
				var newTx pgx.Tx
				newTx, operationErr = s.handleDDL(tx, message)
				tx = newTx
			default:
				operationErr = fmt.Errorf("unknown operation type: %s", message.Type)
			}

			if operationErr != nil && isConnectionError(operationErr) {
				return operationErr // Retry on connection errors
			}
			return nil
		})

		if err != nil || operationErr != nil {
			if tx != nil {
				if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
					s.logger.Error().Err(rollbackErr).Msg("failed to rollback transaction")
				}
			}
			tx = nil
			return fmt.Errorf("failed to handle %s for table %s.%s: %v-%v",
				message.Type,
				message.Schema,
				message.Table,
				err,
				operationErr,
			)
		}
	}

	if s.replicationOrigin != "" && appliedAny && lastAppliedLSN != "" {
		if _, err := tx.Exec(ctx, "SELECT pg_replication_origin_xact_setup($1::pg_lsn, clock_timestamp())", lastAppliedLSN); err != nil {
			return fmt.Errorf("set replication origin transaction LSN: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	tx = nil // Prevent deferred rollback
	return nil
}

func quoteIdentifier(value string) string {
	return pgx.Identifier{value}.Sanitize()
}

func quoteTable(schema, table string) string {
	return pgx.Identifier{schema, table}.Sanitize()
}

func quotedIdentifiers(values []string) []string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = quoteIdentifier(value)
	}
	return quoted
}

// New helper function to determine if an error is a connection issue
func isConnectionError(err error) bool {
	if errors.Is(err, pgx.ErrNoRows) {
		return false
	}
	if err == pgx.ErrTxClosed {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// Check for specific PostgreSQL error codes related to connection issues
		switch pgErr.Code {
		case "08006", // connection_failure
			"08003", // connection_does_not_exist
			"57P01", // admin_shutdown
			"57P02", // crash_shutdown
			"57P03": // cannot_connect_now
			return true
		}
	}
	return false
}

// Close closes the database connection
func (s *PostgresSink) Close() error {
	return s.conn.Close(context.Background())
}
