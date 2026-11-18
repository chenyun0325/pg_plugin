package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"regexp"
	"strings"
)

type Config struct {
	Version int    `yaml:"version"`
	Job     string `yaml:"job"`
	Source  struct {
		ID          string `yaml:"id"`
		DSNEnv      string `yaml:"dsn-env"`
		Publication string `yaml:"publication"`
		Slot        string `yaml:"slot"`
		Plugin      string `yaml:"plugin"`
		Snapshot    string `yaml:"snapshot"` // copy or seed (state only)
	} `yaml:"source"`
	Sink struct {
		ID     string `yaml:"id,omitempty"`
		DSNEnv string `yaml:"dsn-env"`
		Origin string `yaml:"origin"`
	} `yaml:"sink"`
	Routes     []Route          `yaml:"route"`
	RouteMode  string           `yaml:"route-mode,omitempty"`
	Transforms []TransformRule  `yaml:"transform,omitempty"`
	Ownership  *OwnershipConfig `yaml:"ownership,omitempty"`
	Identity   struct {
		Mode      string `yaml:"mode,omitempty"`
		Namespace string `yaml:"namespace,omitempty"`
		Bootstrap string `yaml:"bootstrap,omitempty"`
	} `yaml:"identity,omitempty"`
	digest string
}
type Route struct {
	Name            string          `yaml:"name"`
	Source          string          `yaml:"source-table"`
	Target          string          `yaml:"sink-table"`
	ReplaceSymbol   string          `yaml:"replace-symbol,omitempty"`
	TargetFrom      string          `yaml:"target-from,omitempty"`
	AllowedTargets  []string        `yaml:"allowed-targets,omitempty"`
	ExcludeSources  []string        `yaml:"exclude-source-tables,omitempty"`
	UseTransform    bool            `yaml:"use-transform,omitempty"`
	SourceKey       []string        `yaml:"source-key"`
	Keys            []string        `yaml:"primary-keys"`
	IncludeAll      bool            `yaml:"include-all"`
	AdoptSource     string          `yaml:"adopt-unmarked-source"`
	RequireExisting bool            `yaml:"require-existing"`
	OnDelete        string          `yaml:"on-delete"`
	Exclude         []string        `yaml:"exclude"`
	Columns         map[string]Expr `yaml:"columns"`
	When            *Predicate      `yaml:"when"`
	// Patch only owned JSON paths, preserving other paths at the target.
	JSONPatches map[string][]JSONPatch `yaml:"json-patches"`
	patterns    []*regexp.Regexp
	excluded    []*regexp.Regexp
}
type JSONPatch struct {
	Path  []string `yaml:"path"`
	Value Expr     `yaml:"value"`
}
type Expr struct {
	From     string `yaml:"from,omitempty"`
	Literal  any    `yaml:"literal,omitempty"`
	Function string `yaml:"function,omitempty"`
	Args     []Expr `yaml:"args,omitempty"`
}
type Predicate struct {
	Field     string  `yaml:"field"`
	Equals    *string `yaml:"equals"`
	Modulus   int64   `yaml:"modulus"`
	Remainder int64   `yaml:"remainder"`
}

func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c := new(Config)
	d := yaml.NewDecoder(bytes.NewReader(b))
	d.KnownFields(true)
	if err = d.Decode(c); err != nil {
		return nil, err
	}
	if c.Version != 1 || c.Job == "" || c.Source.ID == "" || c.Source.Publication == "" || c.Source.Slot == "" || c.Source.DSNEnv == "" || c.Sink.DSNEnv == "" || c.Sink.Origin == "" || len(c.Routes) == 0 {
		return nil, fmt.Errorf("missing job/source/sink/routes or unsupported version")
	}
	if c.Source.Plugin == "" {
		c.Source.Plugin = "pgoutput"
	}
	if c.Source.Plugin != "pgoutput" && c.Source.Plugin != "pgcat" {
		return nil, fmt.Errorf("plugin must be pgoutput or pgcat")
	}
	if c.Source.Snapshot != "copy" && c.Source.Snapshot != "seed" {
		return nil, fmt.Errorf("snapshot must be copy or seed")
	}
	if c.Identity.Mode == "" {
		c.Identity.Mode = "columns"
	}
	if c.RouteMode != "" && c.RouteMode != "all-match" && c.RouteMode != "first-match" {
		return nil, fmt.Errorf("route-mode must be all-match or first-match")
	}
	if c.Identity.Mode != "columns" && c.Identity.Mode != "sidecar" {
		return nil, fmt.Errorf("identity.mode must be columns or sidecar")
	}
	if c.sidecar() {
		if c.Identity.Namespace == "" || c.Sink.ID == "" || c.Source.Plugin != "pgoutput" {
			return nil, fmt.Errorf("sidecar requires identity.namespace, sink.id and pgoutput")
		}
		if c.Identity.Bootstrap == "" {
			if c.Source.Snapshot == "seed" {
				c.Identity.Bootstrap = "require-binding"
			} else {
				c.Identity.Bootstrap = "local"
			}
		}
		if c.Identity.Bootstrap != "local" && c.Identity.Bootstrap != "require-binding" {
			return nil, fmt.Errorf("identity.bootstrap must be local or require-binding")
		}
	}
	if !regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`).MatchString(c.Source.Slot) {
		return nil, fmt.Errorf("invalid slot")
	}
	if err = c.prepareTransforms(); err != nil {
		return nil, err
	}
	if c.Ownership != nil {
		o := c.Ownership
		if o.Namespace == "" || o.UIDField == "" || o.Writer == "" || o.EpochField == "" {
			return nil, fmt.Errorf("ownership requires namespace, uid-field, writer and epoch-field; untagged events cannot be fenced")
		}
	}
	seen := map[string]bool{}
	for i := range c.Routes {
		r := &c.Routes[i]
		if r.Name == "" || seen[r.Name] || len(r.SourceKey) == 0 || len(r.Keys) == 0 || (r.TargetFrom == "" && !qualified(r.Target)) {
			return nil, fmt.Errorf("route %d invalid name/keys/target", i)
		}
		seen[r.Name] = true
		if r.TargetFrom != "" {
			if r.TargetFrom != "owner-table" || len(r.AllowedTargets) == 0 || r.Target != "" || r.ReplaceSymbol != "" {
				return nil, fmt.Errorf("route %s: owner-table requires only allowed-targets, no sink-table", r.Name)
			}
			for _, t := range r.AllowedTargets {
				if !qualified(t) {
					return nil, fmt.Errorf("invalid allowed target %s", t)
				}
			}
		}
		if r.ReplaceSymbol != "" && (r.ReplaceSymbol != "<>" || !strings.Contains(r.Target, r.ReplaceSymbol)) {
			return nil, fmt.Errorf("route %s: replace-symbol must be <> present in sink-table", r.Name)
		}
		for _, p := range r.ExcludeSources {
			re, e := regexp.Compile("^(?:" + p + ")$")
			if e != nil {
				return nil, e
			}
			r.excluded = append(r.excluded, re)
		}
		if c.sidecar() {
			for _, key := range r.Keys {
				if strings.HasPrefix(key, "_sync_") {
					return nil, fmt.Errorf("sidecar route %s must use real business keys, not %s", r.Name, key)
				}
			}
		}
		for _, p := range strings.Split(r.Source, ",") {
			re, e := regexp.Compile("^(?:" + strings.TrimSpace(p) + ")$")
			if e != nil {
				return nil, e
			}
			r.patterns = append(r.patterns, re)
		}
		if len(r.patterns) == 0 {
			return nil, fmt.Errorf("source-table required")
		}
		if p := r.When; p != nil {
			if p.Field == "" || (p.Equals == nil && (p.Modulus <= 0 || p.Remainder < 0 || p.Remainder >= p.Modulus)) {
				return nil, fmt.Errorf("invalid route predicate")
			}
		}
		if r.OnDelete != "" && r.OnDelete != "delete" && r.OnDelete != "ignore" && r.OnDelete != "patch-null" {
			return nil, fmt.Errorf("invalid on-delete")
		}
		for name, e := range r.Columns {
			if strings.HasPrefix(name, "_sync_") {
				return nil, fmt.Errorf("reserved target column %s", name)
			}
			if err = validateExpr(e); err != nil {
				return nil, err
			}
		}
		for _, ps := range r.JSONPatches {
			for _, p := range ps {
				if len(p.Path) == 0 {
					return nil, fmt.Errorf("empty JSON path")
				}
				if err = validateExpr(p.Value); err != nil {
					return nil, err
				}
			}
		}
	}
	sum := sha256.Sum256(b)
	c.digest = hex.EncodeToString(sum[:])
	return c, nil
}
func (c *Config) sidecar() bool { return c.Identity.Mode == "sidecar" }
func qualified(s string) bool {
	p := strings.Split(s, ".")
	return len(p) == 2 && p[0] != "" && p[1] != ""
}
func (r Route) matches(table string) bool {
	for _, p := range r.excluded {
		if p.MatchString(table) {
			return false
		}
	}
	for _, p := range r.patterns {
		if p.MatchString(table) {
			return true
		}
	}
	return false
}
func (c *Config) routes(table string) []*Route {
	var out []*Route
	for i := range c.Routes {
		if c.Routes[i].matches(table) {
			out = append(out, &c.Routes[i])
			if c.RouteMode == "first-match" {
				break
			}
		}
	}
	return out
}
func (c *Config) sourceKey(table string) ([]string, error) {
	rs := c.routes(table)
	if len(rs) == 0 {
		return nil, fmt.Errorf("no route for %s", table)
	}
	keys := strings.Join(rs[0].SourceKey, "\x00")
	for _, r := range rs {
		effective, err := c.effectiveRoute(r, table)
		if err != nil {
			return nil, err
		}
		if r == rs[0] {
			keys = strings.Join(effective.SourceKey, "\x00")
		}
		if strings.Join(effective.SourceKey, "\x00") != keys {
			return nil, fmt.Errorf("inconsistent source-key for %s", table)
		}
	}
	return strings.Split(keys, "\x00"), nil
}
