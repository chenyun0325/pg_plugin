// Package utils provides common utilities and data structures for pg_flo.
package utils //nolint:revive // utils is a standard package name

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
)

// init registers types with the gob package for encoding/decoding
func init() {
	gob.Register(json.RawMessage{})
	gob.Register(time.Time{})
	gob.Register(map[string]interface{}{})
	gob.Register(pglogrepl.RelationMessageColumn{})
	gob.Register(pglogrepl.LSN(0))

	gob.Register(CDCMessage{})
	gob.Register(pglogrepl.TupleData{})
	gob.Register(pglogrepl.TupleDataColumn{})
	gob.Register([][]byte{})
}

// ColumnNotFoundError is returned when a requested column is not found in the CDC message
type ColumnNotFoundError struct {
	ColumnName string
}

// CDCMessage represents a full message for Change Data Capture
type CDCMessage struct {
	Type           OperationType
	Schema         string
	Table          string
	Columns        []*pglogrepl.RelationMessageColumn
	NewTuple       *pglogrepl.TupleData // For WAL messages
	OldTuple       *pglogrepl.TupleData // For WAL messages
	CopyData       [][]byte             // For COPY messages
	ReplicationKey ReplicationKey
	LSN            string
	EmittedAt      time.Time
	ToastedColumns map[string]bool
	// Values and OldValues are typed row images produced by the mapping engine.
	// They avoid a lossy encode/decode round trip when a transform creates a new
	// target column. Legacy WAL and COPY messages continue to use the tuple fields.
	Values    map[string]interface{}
	OldValues map[string]interface{}

	// The following fields make target ownership and replay behaviour explicit.
	SourceID        string
	SourceTable     string
	SourceKey       string
	Origin          string
	TransactionID   string
	MappingVersion  string
	ConflictAction  ConflictAction
	JSONPatches     map[string][]JSONPatchValue
	RequireExisting bool
}

type JSONPatchValue struct {
	Path  []string
	Value interface{}
}

// CDCEnvelope is the durable wire unit. A source transaction is published as
// one envelope so all of its mapped rows are committed by the sink atomically.
// Snapshot rows and DDL events use a one-message envelope.
type CDCEnvelope struct {
	Version        uint8
	TransactionID  string
	CommitLSN      string
	MappingVersion string
	Messages       []CDCMessage
	Origin         string
	Context        *ReplayContext
}

// ReplayContext is captured in the SAME source transaction as business DML.
// It is never inferred from the consumer's current ownership state.
type ReplayContext struct {
	MarkerID  string  `json:"marker_id"`
	Namespace string  `json:"scope_namespace"`
	RootID    string  `json:"root_tx_id"`
	Writer    string  `json:"writer_side"`
	Stage     string  `json:"apply_stage"`
	UID       int64   `json:"uid"`
	Epoch     int64   `json:"epoch"`
	Barrier   *string `json:"barrier_id"`
}

// GetColumnIndex returns the index of a column by name, or -1 if not found
func (m *CDCMessage) GetColumnIndex(columnName string) int {
	for i, col := range m.Columns {
		if col.Name == columnName {
			return i
		}
	}
	return -1
}

// GetColumnValue gets a column value, optionally using old values for DELETE/UPDATE
func (m *CDCMessage) GetColumnValue(columnName string, useOldValues bool) (interface{}, error) {
	if useOldValues && m.OldValues != nil {
		value, ok := m.OldValues[columnName]
		if !ok {
			return nil, fmt.Errorf("column %s not found in old row image", columnName)
		}
		return value, nil
	}
	if !useOldValues && m.Values != nil {
		value, ok := m.Values[columnName]
		if !ok {
			return nil, fmt.Errorf("column %s not found in new row image", columnName)
		}
		return value, nil
	}

	colIndex := m.GetColumnIndex(columnName)
	if colIndex == -1 {
		return nil, fmt.Errorf("column %s not found", columnName)
	}

	if !useOldValues && m.CopyData != nil {
		if colIndex >= len(m.CopyData) {
			return nil, fmt.Errorf("column index %d out of range for copy data", colIndex)
		}

		rawBytes := m.CopyData[colIndex]
		if rawBytes == nil {
			return nil, nil
		}

		return GlobalPostgreSQLTypeConverter.DecodePostgreSQLValue(rawBytes, m.Columns[colIndex].DataType, 1)
	}

	var data []byte
	if useOldValues && m.OldTuple != nil {
		if colIndex >= len(m.OldTuple.Columns) {
			return nil, fmt.Errorf("column %s is absent from old row image; use REPLICA IDENTITY FULL for mapped tables", columnName)
		}
		column := m.OldTuple.Columns[colIndex]
		if column.DataType == 'u' {
			return nil, fmt.Errorf("column %s is unchanged TOAST data and requires state hydration", columnName)
		}
		data = column.Data
	} else if m.NewTuple != nil {
		if colIndex >= len(m.NewTuple.Columns) {
			return nil, fmt.Errorf("column %s is absent from new row image", columnName)
		}
		column := m.NewTuple.Columns[colIndex]
		if column.DataType == 'u' {
			return nil, fmt.Errorf("column %s is unchanged TOAST data and requires state hydration", columnName)
		}
		data = column.Data
	} else {
		return nil, fmt.Errorf("no data available for column %s", columnName)
	}

	return GlobalPostgreSQLTypeConverter.DecodePostgreSQLValue(data, m.Columns[colIndex].DataType, 0)
}

// RowValues returns a complete, typed row image. Missing fields are errors;
// callers must not confuse an absent/TOAST value with SQL NULL.
func (m *CDCMessage) RowValues(useOldValues bool) (map[string]interface{}, error) {
	if useOldValues && m.OldValues != nil {
		return cloneValueMap(m.OldValues), nil
	}
	if !useOldValues && m.Values != nil {
		return cloneValueMap(m.Values), nil
	}
	values := make(map[string]interface{}, len(m.Columns))
	for _, col := range m.Columns {
		value, err := m.GetColumnValue(col.Name, useOldValues)
		if err != nil {
			return nil, err
		}
		values[col.Name] = value
	}
	return values, nil
}

func cloneValueMap(input map[string]interface{}) map[string]interface{} {
	output := make(map[string]interface{}, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

// SetColumnValue sets the value of a column (only used by transform rules)
func (m *CDCMessage) SetColumnValue(columnName string, value interface{}) error {
	if m.Type == OperationDelete && m.OldValues != nil {
		if _, ok := m.OldValues[columnName]; !ok {
			return fmt.Errorf("column %s not found", columnName)
		}
		m.OldValues[columnName] = value
		return nil
	}
	if m.Type != OperationDelete && m.Values != nil {
		if _, ok := m.Values[columnName]; !ok {
			return fmt.Errorf("column %s not found", columnName)
		}
		m.Values[columnName] = value
		return nil
	}

	colIndex := m.GetColumnIndex(columnName)
	if colIndex == -1 {
		return fmt.Errorf("column %s not found", columnName)
	}

	data, err := GlobalPostgreSQLTypeConverter.EncodePostgreSQLValue(value, m.Columns[colIndex].DataType)
	if err != nil {
		return fmt.Errorf("failed to encode value for column %s: %w", columnName, err)
	}

	if m.Type == OperationDelete {
		m.OldTuple.Columns[colIndex] = &pglogrepl.TupleDataColumn{Data: data}
	} else {
		m.NewTuple.Columns[colIndex] = &pglogrepl.TupleDataColumn{Data: data}
	}

	return nil
}

func (e ColumnNotFoundError) Error() string {
	return fmt.Sprintf("column %s not found", e.ColumnName)
}

// RemoveColumn removes a column from the message
func (m *CDCMessage) RemoveColumn(columnName string) error {
	colIndex := m.GetColumnIndex(columnName)
	if colIndex == -1 {
		return ColumnNotFoundError{columnName}
	}

	newColumns := make([]*pglogrepl.RelationMessageColumn, len(m.Columns))
	copy(newColumns, m.Columns)
	m.Columns = append(newColumns[:colIndex], newColumns[colIndex+1:]...)

	if m.NewTuple != nil {
		m.NewTuple.ColumnNum--
		m.NewTuple.Columns = append(m.NewTuple.Columns[:colIndex], m.NewTuple.Columns[colIndex+1:]...)
	}
	if m.OldTuple != nil {
		m.OldTuple.ColumnNum--
		m.OldTuple.Columns = append(m.OldTuple.Columns[:colIndex], m.OldTuple.Columns[colIndex+1:]...)
	}
	delete(m.Values, columnName)
	delete(m.OldValues, columnName)
	return nil
}

// IsColumnToasted checks if a column was TOASTed
func (m *CDCMessage) IsColumnToasted(columnName string) bool {
	return m.ToastedColumns[columnName]
}
