package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"strings"
)

type conversionOptions struct{ Mode, Namespace, SinkID, TargetKeys string }

func convertWMS(input, output string) error {
	return convertWMSOptions(input, output, conversionOptions{})
}

func convertWMSOptions(input, output string, opts conversionOptions) error {
	if opts.Mode != "" && opts.Mode != "columns" && opts.Mode != "sidecar" {
		return fmt.Errorf("identity-mode must be columns or sidecar")
	}
	var targetKeys map[string][]string
	if opts.Mode == "sidecar" {
		if opts.Namespace == "" || opts.SinkID == "" || opts.TargetKeys == "" {
			return fmt.Errorf("sidecar conversion requires --namespace, --sink-id and --target-keys; business keys cannot be inferred")
		}
		b, e := os.ReadFile(opts.TargetKeys)
		if e != nil {
			return e
		}
		if e = yaml.Unmarshal(b, &targetKeys); e != nil {
			return e
		}
	}
	var job struct {
		Route []struct {
			Source  string `yaml:"source-table"`
			Target  string `yaml:"sink-table"`
			Replace string `yaml:"replace-symbol"`
		} `yaml:"route"`
		Transform []struct {
			Source     string `yaml:"source-table"`
			Projection string `yaml:"projection"`
			Keys       string `yaml:"primary-keys"`
			Filter     string `yaml:"filter"`
		} `yaml:"transform"`
		Pipeline struct {
			Mode string `yaml:"route-mode"`
		} `yaml:"pipeline"`
	}
	raw, e := os.ReadFile(input)
	if e != nil {
		return e
	}
	if e = yaml.Unmarshal(raw, &job); e != nil {
		return e
	}
	if len(job.Route) == 0 {
		return fmt.Errorf("input has no route")
	}
	c := Config{Version: 1, Job: "wms_merge_v1"}
	c.Source.ID = "wms_legacy_pg"
	c.Source.DSNEnv = "PGCAT_SOURCE_DSN"
	c.Source.Publication = "pgcat_wms_pub"
	c.Source.Slot = "pgcat_wms_v1"
	c.Source.Plugin = "pgoutput"
	c.Source.Snapshot = "copy"
	c.Sink.DSNEnv = "PGCAT_TARGET_DSN"
	c.Sink.Origin = "pgcat_wms_from_legacy"
	c.RouteMode = strings.ToLower(strings.ReplaceAll(job.Pipeline.Mode, "_", "-"))
	if opts.Mode == "sidecar" {
		c.Job = "wms_merge_sidecar_v1"
		c.Source.Slot = "pgcat_wms_sidecar_v1"
		c.Sink.Origin = "pgcat_wms_sidecar_from_legacy"
		c.Sink.ID = opts.SinkID
		c.Identity.Mode = "sidecar"
		c.Identity.Namespace = opts.Namespace
		c.Identity.Bootstrap = "local"
	}
	for _, t := range job.Transform {
		if t.Filter != "" {
			return fmt.Errorf("transform filter requires explicit native when rules; refusing to discard it")
		}
		all, cols, e := parseProjection(t.Projection)
		if e != nil {
			return fmt.Errorf("projection for %s: %w", t.Source, e)
		}
		keys, e := projectionSourceKeys(t.Keys, cols)
		if e != nil {
			return e
		}
		var selectors []string
		for _, source := range strings.Split(t.Source, ",") {
			s, exclude, e := flinkSelector(source)
			if e != nil {
				return e
			}
			if len(exclude) > 0 {
				return fmt.Errorf("lookaround transform requires explicit selectors")
			}
			selectors = append(selectors, s)
		}
		c.Transforms = append(c.Transforms, TransformRule{Source: strings.Join(selectors, ","), SourceKey: keys, IncludeAll: all, Columns: cols})
	}
	for _, r := range job.Route {
		for _, source := range strings.Split(r.Source, ",") {
			selector, excludes, e := flinkSelector(source)
			if e != nil {
				return e
			}
			route := Route{Name: fmt.Sprintf("wms_%03d", len(c.Routes)+1), Source: selector, Target: r.Target, ReplaceSymbol: r.Replace, ExcludeSources: excludes, SourceKey: []string{"id"}, Keys: []string{"_sync_source", "_sync_table", "_sync_pk"}, IncludeAll: true, UseTransform: true, Columns: map[string]Expr{}}
			if opts.Mode == "sidecar" {
				route.Keys = targetKeys[r.Target]
				if len(route.Keys) == 0 {
					return fmt.Errorf("target-keys missing real business keys for %s", r.Target)
				}
				seen := map[string]bool{}
				for _, key := range route.Keys {
					if !fieldName.MatchString(key) || strings.HasPrefix(key, "_sync_") || seen[key] {
						return fmt.Errorf("invalid business keys for %s", r.Target)
					}
					seen[key] = true
				}
			}
			c.Routes = append(c.Routes, route)
		}
	}
	body, e := yaml.Marshal(&c)
	if e != nil {
		return e
	}
	header := "# Converted route/transform rules only; no credentials or connector settings copied.\n# Connect to PG mirror tables. Validate physical UNIQUE/NOT NULL source keys and target DDL.\n# json() maps to deterministic json_get; arbitrary Java UDF classes are not executed.\n"
	return os.WriteFile(output, append([]byte(header), body...), 0600)
}
