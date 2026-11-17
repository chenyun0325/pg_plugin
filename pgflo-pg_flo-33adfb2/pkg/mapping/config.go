// Package mapping implements YAML-driven one-to-many row mapping for pg_flo.
package mapping

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	UnmatchedError = "error"
	UnmatchedPass  = "pass"
	UnmatchedDrop  = "drop"
)

// Config is the mapping section of a pgflo/v1 ReplicationJob.
type Config struct {
	Version        string           `yaml:"version" json:"version"`
	SourceID       string           `yaml:"source-id" json:"source-id"`
	Unmatched      string           `yaml:"unmatched" json:"unmatched"`
	LoopProtection bool             `yaml:"loop-protection" json:"loop-protection"`
	Routes         []RouteConfig    `yaml:"routes" json:"routes"`
	Functions      []FunctionConfig `yaml:"functions,omitempty" json:"functions,omitempty"`
}

// FunctionConfig declares a function name expected to be registered by the
// embedding binary. Built-ins do not need to be listed.
type FunctionConfig struct {
	Name string `yaml:"name" json:"name"`
}

type RouteConfig struct {
	ID             string                  `yaml:"id" json:"id"`
	Mode           string                  `yaml:"mode" json:"mode"`
	Sources        []string                `yaml:"sources" json:"sources"`
	SourcePatterns []string                `yaml:"source-patterns,omitempty" json:"source-patterns,omitempty"`
	Target         string                  `yaml:"target" json:"target"`
	When           *PredicateConfig        `yaml:"when,omitempty" json:"when,omitempty"`
	Identity       IdentityConfig          `yaml:"identity" json:"identity"`
	Conflict       string                  `yaml:"conflict" json:"conflict"`
	Columns        map[string]ColumnConfig `yaml:"columns" json:"columns"`

	patterns []*regexp.Regexp
}

type PredicateConfig struct {
	Modulo *ModuloPredicate       `yaml:"modulo,omitempty" json:"modulo,omitempty"`
	Equals map[string]interface{} `yaml:"equals,omitempty" json:"equals,omitempty"`
}

type ModuloPredicate struct {
	Column    string `yaml:"column" json:"column"`
	Divisor   int64  `yaml:"divisor" json:"divisor"`
	Remainder int64  `yaml:"remainder" json:"remainder"`
}

type IdentityConfig struct {
	Strategy       string         `yaml:"strategy" json:"strategy"`
	SourceKey      []string       `yaml:"source-key,omitempty" json:"source-key,omitempty"`
	TargetKey      []string       `yaml:"target-key,omitempty" json:"target-key,omitempty"`
	DeletePolicy   string         `yaml:"delete-policy,omitempty" json:"delete-policy,omitempty"`
	LineageColumns LineageColumns `yaml:"lineage-columns,omitempty" json:"lineage-columns,omitempty"`
}

type LineageColumns struct {
	Source string `yaml:"source" json:"source"`
	Table  string `yaml:"table" json:"table"`
	Key    string `yaml:"key" json:"key"`
}

type ColumnConfig struct {
	Source     string            `yaml:"source" json:"source"`
	Default    interface{}       `yaml:"default,omitempty" json:"default,omitempty"`
	Transforms []TransformConfig `yaml:"transforms,omitempty" json:"transforms,omitempty"`
}

type TransformConfig struct {
	Function string   `yaml:"function" json:"function"`
	Args     []string `yaml:"args,omitempty" json:"args,omitempty"`
}

func (c *Config) setDefaults() {
	if c.Version == "" {
		c.Version = "v1"
	}
	if c.Unmatched == "" {
		c.Unmatched = UnmatchedError
	}
	for i := range c.Routes {
		r := &c.Routes[i]
		if r.Mode == "" {
			r.Mode = "direct"
		}
		if r.Identity.Strategy == "" {
			r.Identity.Strategy = "source"
		}
		if r.Identity.Strategy == "source" && r.Identity.DeletePolicy == "" {
			r.Identity.DeletePolicy = "delete"
		}
		if r.Identity.LineageColumns.Source == "" {
			r.Identity.LineageColumns.Source = "_pgflo_source"
		}
		if r.Identity.LineageColumns.Table == "" {
			r.Identity.LineageColumns.Table = "_pgflo_source_table"
		}
		if r.Identity.LineageColumns.Key == "" {
			r.Identity.LineageColumns.Key = "_pgflo_source_pk"
		}
		if r.Conflict == "" {
			r.Conflict = "upsert"
		}
	}
}

func (c *Config) validate() error {
	c.setDefaults()
	if c.SourceID == "" {
		return fmt.Errorf("mapping.source-id is required and must remain stable across restarts")
	}
	if c.Unmatched != UnmatchedError && c.Unmatched != UnmatchedPass && c.Unmatched != UnmatchedDrop {
		return fmt.Errorf("mapping.unmatched must be error, pass, or drop")
	}
	if len(c.Routes) == 0 {
		return fmt.Errorf("mapping.routes must not be empty")
	}
	seen := make(map[string]bool, len(c.Routes))
	for i := range c.Routes {
		r := &c.Routes[i]
		if r.ID == "" || seen[r.ID] {
			return fmt.Errorf("route %d has an empty or duplicate id %q", i, r.ID)
		}
		seen[r.ID] = true
		if len(r.Sources) == 0 && len(r.SourcePatterns) == 0 {
			return fmt.Errorf("route %s needs sources or source-patterns", r.ID)
		}
		for _, source := range r.Sources {
			if !qualified(source) {
				return fmt.Errorf("route %s source %q must be schema.table", r.ID, source)
			}
		}
		if !qualified(r.Target) {
			return fmt.Errorf("route %s target %q must be schema.table", r.ID, r.Target)
		}
		if len(r.Columns) == 0 {
			return fmt.Errorf("route %s needs an explicit columns projection", r.ID)
		}
		for target, column := range r.Columns {
			if target == "" || column.Source == "" {
				return fmt.Errorf("route %s has an empty target/source column", r.ID)
			}
		}
		switch r.Identity.Strategy {
		case "source":
			lineage := r.Identity.LineageColumns
			if lineage.Source == lineage.Table || lineage.Source == lineage.Key || lineage.Table == lineage.Key {
				return fmt.Errorf("route %s lineage column names must be distinct", r.ID)
			}
		case "mapped":
			if len(r.Identity.TargetKey) == 0 {
				return fmt.Errorf("route %s mapped identity needs target-key", r.ID)
			}
			for _, key := range r.Identity.TargetKey {
				if _, ok := r.Columns[key]; !ok {
					return fmt.Errorf("route %s target key %s is not projected", r.ID, key)
				}
			}
			if r.Identity.DeletePolicy == "" {
				return fmt.Errorf("route %s mapped identity must declare delete-policy delete or ignore", r.ID)
			}
		default:
			return fmt.Errorf("route %s identity.strategy must be source or mapped", r.ID)
		}
		if r.Identity.DeletePolicy != "delete" && r.Identity.DeletePolicy != "ignore" {
			return fmt.Errorf("route %s delete-policy must be delete or ignore", r.ID)
		}
		switch r.Conflict {
		case "upsert", "ignore", "error":
		default:
			return fmt.Errorf("route %s conflict must be upsert, ignore, or error", r.ID)
		}
		if r.When != nil && r.When.Modulo != nil {
			m := r.When.Modulo
			if m.Column == "" || m.Divisor <= 0 || m.Remainder < 0 || m.Remainder >= m.Divisor {
				return fmt.Errorf("route %s has invalid modulo predicate", r.ID)
			}
		}
		r.patterns = nil
		for _, pattern := range r.SourcePatterns {
			re, err := regexp.Compile(pattern)
			if err != nil {
				return fmt.Errorf("route %s source pattern: %w", r.ID, err)
			}
			r.patterns = append(r.patterns, re)
		}
	}
	return nil
}

func qualified(value string) bool {
	parts := strings.Split(value, ".")
	return len(parts) == 2 && parts[0] != "" && parts[1] != ""
}
