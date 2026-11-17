package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type Row map[string]any
type Function func([]any) (any, error)

// Register application functions before parsing configuration, in init().
var functions = map[string]Function{}

func Register(name string, f Function) {
	if _, ok := functions[name]; ok {
		panic("duplicate function " + name)
	}
	functions[name] = f
}
func init() {
	Register("concat", func(a []any) (any, error) {
		var b strings.Builder
		for _, v := range a {
			if v == nil {
				return nil, fmt.Errorf("concat NULL is ambiguous")
			}
			b.WriteString(fmt.Sprint(v))
		}
		return b.String(), nil
	})
	Register("json_get", jsonGet)
	Register("if_eq", func(a []any) (any, error) {
		if len(a) != 4 {
			return nil, fmt.Errorf("if_eq needs 4 args")
		}
		if fmt.Sprint(a[0]) == fmt.Sprint(a[1]) {
			return a[2], nil
		}
		return a[3], nil
	})
	Register("json_index", func(a []any) (any, error) {
		if len(a) != 2 {
			return nil, fmt.Errorf("json_index needs JSON array and index")
		}
		v := a[0]
		if s, ok := v.(string); ok {
			d := json.NewDecoder(strings.NewReader(s))
			d.UseNumber()
			if e := d.Decode(&v); e != nil {
				return nil, e
			}
		}
		arr, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("json_index requires array")
		}
		i, e := strconv.Atoi(fmt.Sprint(a[1]))
		if e != nil || i < 0 || i >= len(arr) {
			return nil, fmt.Errorf("invalid array index")
		}
		return arr[i], nil
	})
	Register("json_int", func(a []any) (any, error) {
		v, e := jsonGet(a)
		if e != nil || v == nil {
			return v, e
		}
		n, e := strconv.ParseInt(fmt.Sprint(v), 10, 64)
		if e != nil {
			return nil, fmt.Errorf("json_int: %w", e)
		}
		return n, nil
	})
	Register("int64", func(a []any) (any, error) {
		if len(a) != 1 {
			return nil, fmt.Errorf("int64 needs one arg")
		}
		if a[0] == nil {
			return nil, nil
		}
		return strconv.ParseInt(fmt.Sprint(a[0]), 10, 64)
	})
	Register("text", func(a []any) (any, error) {
		if len(a) != 1 {
			return nil, fmt.Errorf("text needs one arg")
		}
		if a[0] == nil {
			return nil, nil
		}
		return fmt.Sprint(a[0]), nil
	})
	Register("lower", func(a []any) (any, error) {
		if len(a) != 1 {
			return nil, fmt.Errorf("lower needs one arg")
		}
		if a[0] == nil {
			return nil, nil
		}
		return strings.ToLower(fmt.Sprint(a[0])), nil
	})
	Register("json_object", func(a []any) (any, error) {
		if len(a)%2 != 0 {
			return nil, fmt.Errorf("json_object needs key/value pairs")
		}
		o := map[string]any{}
		for i := 0; i < len(a); i += 2 {
			o[fmt.Sprint(a[i])] = a[i+1]
		}
		return o, nil
	})
}
func decodeJSON(b []byte) (Row, error) {
	var r Row
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.UseNumber()
	err := d.Decode(&r)
	return r, err
}
func jsonGet(a []any) (any, error) {
	if len(a) < 2 {
		return nil, fmt.Errorf("json_get needs document and path components")
	}
	v := a[0]
	if v == nil {
		return nil, nil
	}
	if s, ok := v.(string); ok {
		var x any
		d := json.NewDecoder(strings.NewReader(s))
		d.UseNumber()
		if e := d.Decode(&x); e != nil {
			return nil, e
		}
		v = x
	}
	for _, part := range a[1:] {
		if v == nil {
			return nil, nil
		}
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("JSON path crosses non-object")
		}
		var exists bool
		v, exists = obj[fmt.Sprint(part)]
		if !exists {
			return nil, fmt.Errorf("missing JSON key %s", part)
		}
	}
	return v, nil
}
func validateExpr(e Expr) error {
	if e.Function != "" {
		if _, ok := functions[e.Function]; !ok {
			return fmt.Errorf("unknown function %s", e.Function)
		}
		for _, a := range e.Args {
			if err := validateExpr(a); err != nil {
				return err
			}
		}
	}
	return nil
}
func eval(e Expr, row Row, meta Row) (any, error) {
	if e.Function != "" {
		var args []any
		for _, a := range e.Args {
			v, err := eval(a, row, meta)
			if err != nil {
				return nil, err
			}
			args = append(args, v)
		}
		return functions[e.Function](args)
	}
	if e.From != "" {
		if v, ok := meta[e.From]; ok {
			return v, nil
		}
		v, ok := row[e.From]
		if !ok {
			return nil, fmt.Errorf("missing field %s", e.From)
		}
		return v, nil
	}
	return e.Literal, nil
}
func rowKey(row Row, keys []string) (string, error) {
	var parts []any
	for _, k := range keys {
		v, ok := row[k]
		if !ok || v == nil {
			return "", fmt.Errorf("missing or NULL key %s", k)
		}
		parts = append(parts, v)
	}
	b, err := json.Marshal(parts)
	return string(b), err
}
func merge(a, b Row) Row {
	o := Row{}
	for k, v := range a {
		o[k] = v
	}
	for k, v := range b {
		o[k] = v
	}
	return o
}
func plain(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch x := v.(type) {
	case map[string]any, Row, []any:
		b, e := json.Marshal(v)
		return string(b), e
	case json.Number:
		return x.String(), nil
	default:
		return fmt.Sprint(v), nil
	}
}
