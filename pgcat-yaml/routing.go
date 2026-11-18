package main

import (
	"fmt"
	"regexp"
	"strings"
)

// Transform rules are matched against concrete source tables, not regex strings.
type TransformRule struct {
	Source     string          `yaml:"source-table"`
	SourceKey  []string        `yaml:"source-key"`
	IncludeAll bool            `yaml:"include-all"`
	Columns    map[string]Expr `yaml:"columns"`
	patterns   []*regexp.Regexp
}

func (c *Config) prepareTransforms() error {
	for i := range c.Transforms {
		t := &c.Transforms[i]
		if t.Source == "" || len(t.SourceKey) == 0 {
			return fmt.Errorf("transform requires source-table and physical source-key")
		}
		for _, p := range strings.Split(t.Source, ",") {
			re, e := regexp.Compile("^(?:" + strings.TrimSpace(p) + ")$")
			if e != nil {
				return e
			}
			t.patterns = append(t.patterns, re)
		}
		for col, e := range t.Columns {
			if strings.HasPrefix(col, "_sync_") {
				return fmt.Errorf("reserved transform column %s", col)
			}
			if err := validateExpr(e); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Config) effectiveRoute(r *Route, table string) (*Route, error) {
	if !r.UseTransform {
		return r, nil
	}
	result := *r
	var selected *TransformRule
	for i := range c.Transforms {
		t := &c.Transforms[i]
		matched := false
		for _, p := range t.patterns {
			if p.MatchString(table) {
				matched = true
				break
			}
		}
		if matched {
			if selected != nil {
				return nil, fmt.Errorf("ambiguous transforms for %s", table)
			}
			selected = t
		}
	}
	if selected != nil {
		result.SourceKey = selected.SourceKey
		result.IncludeAll = selected.IncludeAll
		result.Columns = map[string]Expr{}
		for k, v := range selected.Columns {
			result.Columns[k] = v
		}
		for k, v := range r.Columns {
			result.Columns[k] = v
		}
	}
	return &result, nil
}

func (r *Route) resolveTarget(table string, owner Owner) (string, error) {
	if r.TargetFrom == "owner-table" {
		for _, allowed := range r.AllowedTargets {
			if allowed == owner.Table {
				return allowed, nil
			}
		}
		return "", fmt.Errorf("owner table %s is not an allowed target for %s; new entities need explicit routing", owner.Table, r.Name)
	}
	target := r.Target
	if r.ReplaceSymbol != "" {
		parts := strings.SplitN(table, ".", 2)
		if len(parts) != 2 {
			return "", fmt.Errorf("unqualified source")
		}
		target = strings.ReplaceAll(target, r.ReplaceSymbol, parts[1])
	}
	if !qualified(target) {
		return "", fmt.Errorf("invalid resolved target %s", target)
	}
	return target, nil
}

// Enumerate finite targets before any write. Remote lineage cannot name a new table.
func (c *Config) targetRoutes(tables []string) ([]*Route, error) {
	seen := map[string]bool{}
	var out []*Route
	for _, table := range tables {
		for _, r := range c.routes(table) {
			effective, e := c.effectiveRoute(r, table)
			if e != nil {
				return nil, e
			}
			targets := effective.AllowedTargets
			if effective.TargetFrom == "" {
				target, e := effective.resolveTarget(table, Owner{})
				if e != nil {
					return nil, e
				}
				targets = []string{target}
			}
			for _, target := range targets {
				key := target + "\x00" + strings.Join(effective.Keys, "\x00")
				if !seen[key] {
					copy := *effective
					copy.Target = target
					out = append(out, &copy)
					seen[key] = true
				}
			}
		}
	}
	return out, nil
}
