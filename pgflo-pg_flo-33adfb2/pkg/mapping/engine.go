package mapping

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/pgflo/pg_flo/pkg/utils"
)

// TransformContext gives custom functions deterministic access to the current
// row and route. Functions must not perform I/O, read time, or mutate Row.
type TransformContext struct {
	SourceID, SourceTable, RouteID, TargetColumn string
	Row                                          map[string]interface{}
}

type Function func(TransformContext, interface{}, []string) (interface{}, error)

type Engine struct {
	config    Config
	functions map[string]Function
}

type projection struct {
	route        *RouteConfig
	values       map[string]interface{}
	keyColumns   []string
	keySignature string
}

func NewEngine(config Config) (*Engine, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	e := &Engine{config: config, functions: make(map[string]Function)}
	e.registerBuiltins()
	return e, nil
}

// Register installs a controlled Go transform. Register functions before the
// worker starts; hot replacement would make replay non-deterministic.
func (e *Engine) Register(name string, function Function) error {
	if name == "" || function == nil {
		return fmt.Errorf("function name and implementation are required")
	}
	if _, exists := e.functions[name]; exists {
		return fmt.Errorf("function %s is already registered", name)
	}
	e.functions[name] = function
	return nil
}

func (e *Engine) registerBuiltins() {
	e.functions["json_get"] = jsonGet
	e.functions["to_string"] = func(_ TransformContext, value interface{}, _ []string) (interface{}, error) {
		if value == nil {
			return nil, nil
		}
		switch typed := value.(type) {
		case string:
			return typed, nil
		case json.Number:
			return typed.String(), nil
		default:
			return fmt.Sprint(value), nil
		}
	}
	e.functions["to_int64"] = func(_ TransformContext, value interface{}, _ []string) (interface{}, error) {
		if value == nil {
			return nil, nil
		}
		number, err := strconv.ParseInt(fmt.Sprint(value), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("to_int64: %w", err)
		}
		return number, nil
	}
	e.functions["lower"] = func(_ TransformContext, value interface{}, _ []string) (interface{}, error) {
		if value == nil {
			return nil, nil
		}
		return strings.ToLower(fmt.Sprint(value)), nil
	}
	e.functions["upper"] = func(_ TransformContext, value interface{}, _ []string) (interface{}, error) {
		if value == nil {
			return nil, nil
		}
		return strings.ToUpper(fmt.Sprint(value)), nil
	}
	e.functions["concat_fields"] = func(ctx TransformContext, _ interface{}, args []string) (interface{}, error) {
		var result strings.Builder
		for _, name := range args {
			value, ok := ctx.Row[name]
			if !ok {
				return nil, fmt.Errorf("concat_fields: missing field %s", name)
			}
			result.WriteString(fmt.Sprint(value))
		}
		return result.String(), nil
	}
}

func jsonGet(_ TransformContext, value interface{}, path []string) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if raw, ok := value.(string); ok {
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("json_get: %w", err)
		}
	}
	for _, part := range path {
		switch node := value.(type) {
		case map[string]interface{}:
			var ok bool
			value, ok = node[part]
			if !ok {
				return nil, fmt.Errorf("json_get: missing key %s", part)
			}
		case []interface{}:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(node) {
				return nil, fmt.Errorf("json_get: invalid array index %s", part)
			}
			value = node[index]
		default:
			return nil, fmt.Errorf("json_get: path %s crosses a scalar", part)
		}
	}
	return value, nil
}

func (e *Engine) Map(message *utils.CDCMessage) ([]*utils.CDCMessage, error) {
	if message.MappingVersion != "" && message.MappingVersion != e.config.Version {
		return nil, fmt.Errorf("message mapping version %s does not match worker version %s", message.MappingVersion, e.config.Version)
	}
	if message.Type == utils.OperationDDL {
		return []*utils.CDCMessage{message}, nil
	}
	if message.Type != utils.OperationInsert && message.Type != utils.OperationUpdate && message.Type != utils.OperationDelete {
		return nil, fmt.Errorf("unsupported operation %q", message.Type)
	}
	if e.config.LoopProtection && message.Origin != "" {
		return nil, nil
	}
	sourceTable := message.Schema + "." + message.Table
	var before, after map[string]interface{}
	var err error
	if message.Type != utils.OperationInsert {
		before, err = message.RowValues(true)
		if err != nil {
			return nil, fmt.Errorf("%s %s old image: %w", message.Type, sourceTable, err)
		}
	}
	if message.Type != utils.OperationDelete {
		after, err = message.RowValues(false)
		if err != nil {
			return nil, fmt.Errorf("%s %s new image: %w", message.Type, sourceTable, err)
		}
	}

	matchedSource := false
	outputs := make([]*utils.CDCMessage, 0)
	seen := make(map[string]string)
	for i := range e.config.Routes {
		route := &e.config.Routes[i]
		if !route.matchesSource(sourceTable) {
			continue
		}
		matchedSource = true
		oldProjection, err := e.project(route, sourceTable, before, message, true)
		if err != nil {
			return nil, err
		}
		newProjection, err := e.project(route, sourceTable, after, message, false)
		if err != nil {
			return nil, err
		}
		mapped := e.diff(message, sourceTable, oldProjection, newProjection)
		for _, output := range mapped {
			key := output.Schema + "." + output.Table + "/" + output.SourceKey + "/" + string(output.Type)
			if previous, exists := seen[key]; exists {
				return nil, fmt.Errorf("routes %s and %s overlap on target row %s", previous, route.ID, key)
			}
			seen[key] = route.ID
			outputs = append(outputs, output)
		}
	}

	if !matchedSource {
		switch e.config.Unmatched {
		case UnmatchedPass:
			return []*utils.CDCMessage{message}, nil
		case UnmatchedDrop:
			return nil, nil
		default:
			return nil, fmt.Errorf("no mapping route for %s", sourceTable)
		}
	}
	return outputs, nil
}

func (r *RouteConfig) matchesSource(source string) bool {
	for _, candidate := range r.Sources {
		if candidate == source {
			return true
		}
	}
	for _, pattern := range r.patterns {
		if pattern.MatchString(source) {
			return true
		}
	}
	return false
}

func (e *Engine) project(route *RouteConfig, sourceTable string, row map[string]interface{}, message *utils.CDCMessage, keysOnly bool) (*projection, error) {
	if row == nil {
		return nil, nil
	}
	match, err := matchesPredicate(route.When, row)
	if err != nil {
		return nil, fmt.Errorf("route %s predicate: %w", route.ID, err)
	}
	if !match {
		return nil, nil
	}

	values := make(map[string]interface{})
	keyColumns := append([]string(nil), route.Identity.TargetKey...)
	if route.Identity.Strategy == "source" {
		sourceKey := route.Identity.SourceKey
		if len(sourceKey) == 0 {
			sourceKey = message.ReplicationKey.Columns
		}
		if len(sourceKey) == 0 {
			return nil, fmt.Errorf("route %s has no source identity; configure identity.source-key", route.ID)
		}
		parts := make([]interface{}, 0, len(sourceKey))
		for _, column := range sourceKey {
			value, ok := row[column]
			if !ok || value == nil {
				return nil, fmt.Errorf("route %s missing source key %s", route.ID, column)
			}
			parts = append(parts, value)
		}
		encoded, err := json.Marshal(parts)
		if err != nil {
			return nil, err
		}
		lineage := route.Identity.LineageColumns
		values[lineage.Source] = e.config.SourceID
		values[lineage.Table] = sourceTable
		values[lineage.Key] = string(encoded)
		keyColumns = []string{lineage.Source, lineage.Table, lineage.Key}
	}

	for target, column := range route.Columns {
		if keysOnly && !contains(keyColumns, target) {
			continue
		}
		value, ok := row[column.Source]
		if column.Source == "*" {
			value, ok = cloneMap(row), true
		}
		if !ok {
			if column.Default != nil {
				value = cloneValue(column.Default)
			} else {
				return nil, fmt.Errorf("route %s target %s: source field %s is absent", route.ID, target, column.Source)
			}
		} else {
			value = cloneValue(value)
		}
		ctx := TransformContext{SourceID: e.config.SourceID, SourceTable: sourceTable, RouteID: route.ID, TargetColumn: target, Row: cloneMap(row)}
		for _, transform := range column.Transforms {
			function, found := e.functions[transform.Function]
			if !found {
				return nil, fmt.Errorf("route %s target %s: unknown function %s", route.ID, target, transform.Function)
			}
			value, err = function(ctx, value, transform.Args)
			if err != nil {
				return nil, fmt.Errorf("route %s target %s function %s: %w", route.ID, target, transform.Function, err)
			}
		}
		values[target] = value
	}
	for _, key := range keyColumns {
		if _, ok := values[key]; !ok {
			return nil, fmt.Errorf("route %s target key %s cannot be derived from row image", route.ID, key)
		}
	}
	keyValues := make([]interface{}, 0, len(keyColumns))
	for _, key := range keyColumns {
		keyValues = append(keyValues, values[key])
	}
	encodedKey, err := json.Marshal(keyValues)
	if err != nil {
		return nil, err
	}
	return &projection{route: route, values: values, keyColumns: keyColumns, keySignature: string(encodedKey)}, nil
}

func (e *Engine) diff(input *utils.CDCMessage, sourceTable string, oldProjection, newProjection *projection) []*utils.CDCMessage {
	var output []*utils.CDCMessage
	changedIdentity := oldProjection != nil && newProjection != nil &&
		(oldProjection.route.Target != newProjection.route.Target || oldProjection.keySignature != newProjection.keySignature)
	if oldProjection != nil && oldProjection.route.Identity.DeletePolicy == "delete" && (newProjection == nil || changedIdentity) {
		output = append(output, mappedMessage(input, e.config.SourceID, sourceTable, oldProjection, utils.OperationDelete, nil, oldProjection.values))
	}
	if newProjection != nil {
		operation := utils.OperationUpdate
		var oldValues map[string]interface{}
		if oldProjection == nil || changedIdentity {
			operation = utils.OperationInsert
		} else {
			oldValues = oldProjection.values
		}
		output = append(output, mappedMessage(input, e.config.SourceID, sourceTable, newProjection, operation, newProjection.values, oldValues))
	}
	return output
}

func mappedMessage(input *utils.CDCMessage, sourceID, sourceTable string, p *projection, operation utils.OperationType, values, oldValues map[string]interface{}) *utils.CDCMessage {
	parts := strings.SplitN(p.route.Target, ".", 2)
	columnsMap := values
	if operation == utils.OperationDelete {
		columnsMap = oldValues
	}
	names := make([]string, 0, len(columnsMap))
	for name := range columnsMap {
		names = append(names, name)
	}
	sort.Strings(names)
	columns := make([]*pglogrepl.RelationMessageColumn, 0, len(names))
	for _, name := range names {
		columns = append(columns, &pglogrepl.RelationMessageColumn{Name: name})
	}
	transactionID := input.TransactionID
	if transactionID == "" {
		transactionID = "snapshot/" + sourceTable + "/" + p.keySignature
	}
	return &utils.CDCMessage{
		Type:           operation,
		Schema:         parts[0],
		Table:          parts[1],
		Columns:        columns,
		Values:         cloneMap(values),
		OldValues:      cloneMap(oldValues),
		ReplicationKey: utils.ReplicationKey{Type: utils.ReplicationKeyPK, Columns: append([]string(nil), p.keyColumns...)},
		LSN:            input.LSN,
		EmittedAt:      input.EmittedAt,
		SourceID:       sourceID,
		SourceTable:    sourceTable,
		SourceKey:      p.keySignature,
		TransactionID:  transactionID,
		MappingVersion: input.MappingVersion,
		ConflictAction: utils.ConflictAction(p.route.Conflict),
		ToastedColumns: map[string]bool{},
	}
}

func matchesPredicate(predicate *PredicateConfig, row map[string]interface{}) (bool, error) {
	if predicate == nil {
		return true, nil
	}
	if predicate.Modulo != nil {
		value, ok := row[predicate.Modulo.Column]
		if !ok || value == nil {
			return false, fmt.Errorf("missing modulo column %s", predicate.Modulo.Column)
		}
		number, err := strconv.ParseInt(fmt.Sprint(value), 10, 64)
		if err != nil {
			return false, fmt.Errorf("modulo column %s is not an integer: %w", predicate.Modulo.Column, err)
		}
		actual := ((number % predicate.Modulo.Divisor) + predicate.Modulo.Divisor) % predicate.Modulo.Divisor
		if actual != predicate.Modulo.Remainder {
			return false, nil
		}
	}
	for column, expected := range predicate.Equals {
		actual, ok := row[column]
		if !ok || fmt.Sprint(actual) != fmt.Sprint(expected) {
			return false, nil
		}
	}
	return true, nil
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func cloneMap(input map[string]interface{}) map[string]interface{} {
	if input == nil {
		return nil
	}
	output := make(map[string]interface{}, len(input))
	for key, value := range input {
		output[key] = cloneValue(value)
	}
	return output
}

func cloneValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		return cloneMap(typed)
	case []interface{}:
		output := make([]interface{}, len(typed))
		for i := range typed {
			output[i] = cloneValue(typed[i])
		}
		return output
	case []byte:
		return append([]byte(nil), typed...)
	default:
		return value
	}
}
