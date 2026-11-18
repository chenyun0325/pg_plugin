package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

var fieldName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var decimalLiteral = regexp.MustCompile(`^[+-]?[0-9]+(?:\.[0-9]+)?$`)

// Small deterministic expression grammar, never arbitrary SQL or Java UDF code.
func splitExpression(s, separator string) ([]string, error) {
	var parts []string
	start, depth := 0, 0
	quoted := false
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			if quoted && i+1 < len(s) && s[i+1] == '\'' {
				i++
				continue
			}
			quoted = !quoted
			continue
		}
		if quoted {
			continue
		}
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("unbalanced expression")
			}
		}
		if depth == 0 && strings.HasPrefix(s[i:], separator) {
			parts = append(parts, strings.TrimSpace(s[start:i]))
			i += len(separator) - 1
			start = i + 1
		}
	}
	if quoted || depth != 0 {
		return nil, fmt.Errorf("unbalanced expression")
	}
	return append(parts, strings.TrimSpace(s[start:])), nil
}

func parseExpression(s string) (Expr, error) {
	s = strings.TrimSpace(s)
	pieces, err := splitExpression(s, "||")
	if err != nil {
		return Expr{}, err
	}
	if len(pieces) > 1 {
		e := Expr{Function: "concat"}
		for _, p := range pieces {
			a, err := parseExpression(p)
			if err != nil {
				return Expr{}, err
			}
			e.Args = append(e.Args, a)
		}
		return e, nil
	}
	if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") && len(s) >= 2 {
		inner := s[1 : len(s)-1]
		for i := 0; i < len(inner); i++ {
			if inner[i] == '\'' {
				if i+1 >= len(inner) || inner[i+1] != '\'' {
					return Expr{}, fmt.Errorf("unsupported tokens after string literal")
				}
				i++
			}
		}
		return Expr{Literal: strings.ReplaceAll(s[1:len(s)-1], "''", "'")}, nil
	}
	if decimalLiteral.MatchString(s) {
		return Expr{Literal: json.Number(s)}, nil
	}
	if strings.EqualFold(s, "null") {
		return Expr{Literal: nil}, nil
	}
	if i := strings.IndexByte(s, '('); i > 0 && strings.HasSuffix(s, ")") {
		name := strings.ToLower(strings.TrimSpace(s[:i]))
		if name == "json" {
			name = "json_get"
		}
		e := Expr{Function: name}
		args, err := splitExpression(s[i+1:len(s)-1], ",")
		if err != nil {
			return Expr{}, err
		}
		if len(args) == 1 && args[0] == "" {
			args = nil
		}
		for _, a := range args {
			p, err := parseExpression(a)
			if err != nil {
				return Expr{}, err
			}
			e.Args = append(e.Args, p)
		}
		return e, validateExpr(e)
	}
	if fieldName.MatchString(s) {
		return Expr{From: s}, nil
	}
	return Expr{}, fmt.Errorf("unsupported expression %q", s)
}

func parseProjection(projection string) (bool, map[string]Expr, error) {
	items, err := splitExpression(projection, ",")
	if err != nil {
		return false, nil, err
	}
	all := false
	cols := map[string]Expr{}
	for _, item := range items {
		if item == "*" {
			all = true
			continue
		}
		tokens, err := splitExpression(item, " ")
		if err != nil {
			return false, nil, err
		}
		compact := []string{}
		for _, t := range tokens {
			if t != "" {
				compact = append(compact, t)
			}
		}
		alias, body := item, item
		if len(compact) > 1 {
			alias = compact[len(compact)-1]
			body = strings.TrimSpace(item[:strings.LastIndex(item, alias)])
			if strings.HasSuffix(strings.ToLower(body), " as") {
				body = strings.TrimSpace(body[:len(body)-3])
			}
		}
		if !fieldName.MatchString(alias) {
			return false, nil, fmt.Errorf("projection requires an identifier alias: %s", item)
		}
		if _, exists := cols[alias]; exists {
			return false, nil, fmt.Errorf("duplicate projection alias %s", alias)
		}
		e, err := parseExpression(body)
		if err != nil {
			return false, nil, err
		}
		cols[alias] = e
	}
	return all, cols, nil
}

func projectionSourceKeys(keys string, cols map[string]Expr) ([]string, error) {
	if strings.TrimSpace(keys) == "" {
		return []string{"id"}, nil
	}
	var out []string
	seen := map[string]bool{}
	var refs func(Expr)
	refs = func(e Expr) {
		if e.From != "" && !strings.HasPrefix(e.From, "__") && !seen[e.From] {
			seen[e.From] = true
			out = append(out, e.From)
		}
		for _, a := range e.Args {
			refs(a)
		}
	}
	for _, key := range strings.Split(keys, ",") {
		key = strings.TrimSpace(key)
		if e, ok := cols[key]; ok {
			refs(e)
		} else if fieldName.MatchString(key) {
			refs(Expr{From: key})
		} else {
			return nil, fmt.Errorf("invalid key %q", key)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("primary key contains no physical source columns")
	}
	return out, nil
}

// Translate the supplied Flink negative shard-suffix selector into RE2 includes
// and explicit exclusions. Other lookarounds fail rather than change semantics.
func flinkSelector(s string) (string, []string, error) {
	s = strings.TrimSpace(s)
	i := strings.IndexByte(s, '.')
	if i <= 0 {
		return "", nil, fmt.Errorf("unqualified Flink selector %s", s)
	}
	schema := strings.TrimSuffix(s[:i], `\`)
	if !fieldName.MatchString(schema) {
		return "", nil, fmt.Errorf("unsupported database selector %s", schema)
	}
	prefix := regexp.QuoteMeta(schema) + `\.`
	tail := s[i+1:]
	if strings.Contains(tail, "(?!") {
		if tail != `^(?!\.*(_[0-9]+|_[0-9]+_history)$)\.*$` {
			return "", nil, fmt.Errorf("unsupported lookaround %s; use explicit exclusions", s)
		}
		return prefix + `.*`, []string{prefix + `.*(?:_[0-9]+|_[0-9]+_history)`}, nil
	}
	tail = strings.ReplaceAll(tail, `\.*`, `.*`)
	result := prefix + tail
	if _, err := regexp.Compile("^(?:" + result + ")$"); err != nil {
		return "", nil, err
	}
	return result, nil, nil
}
