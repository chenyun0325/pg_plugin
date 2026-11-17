package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"regexp"
	"strings"
)

// convertWMS deliberately handles only the concrete expression forms present in
// the supplied Flink job. It does not run arbitrary Flink SQL or copy credentials.
func convertWMS(input, output string) error {
	return convertWMSOptions(input, output, conversionOptions{})
}

type conversionOptions struct {
	Mode, Namespace, SinkID, TargetKeys string
}

func convertWMSOptions(input, output string, opts conversionOptions) error {
	if opts.Mode != "" && opts.Mode != "columns" && opts.Mode != "sidecar" {
		return fmt.Errorf("identity-mode must be columns or sidecar")
	}
	var targetKeys map[string][]string
	if opts.Mode == "sidecar" {
		if opts.Namespace == "" || opts.SinkID == "" || opts.TargetKeys == "" {
			return fmt.Errorf("sidecar conversion requires --namespace, --sink-id and --target-keys; business keys cannot be inferred from provenance columns")
		}
		data, err := os.ReadFile(opts.TargetKeys)
		if err != nil {
			return err
		}
		if err = yaml.Unmarshal(data, &targetKeys); err != nil {
			return err
		}
	}
	var job struct {
		Route []struct {
			Source string `yaml:"source-table"`
			Target string `yaml:"sink-table"`
		} `yaml:"route"`
		Transform []struct {
			Source     string `yaml:"source-table"`
			Projection string `yaml:"projection"`
			Keys       string `yaml:"primary-keys"`
		} `yaml:"transform"`
	}
	b, e := os.ReadFile(input)
	if e != nil {
		return e
	}
	if e = yaml.Unmarshal(b, &job); e != nil {
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
	if opts.Mode == "sidecar" {
		c.Job = "wms_merge_sidecar_v1"
		c.Source.Slot = "pgcat_wms_sidecar_v1"
		c.Sink.Origin = "pgcat_wms_sidecar_from_legacy"
		c.Sink.ID = opts.SinkID
		c.Identity.Mode, c.Identity.Namespace, c.Identity.Bootstrap = "sidecar", opts.Namespace, "local"
	}
	pkre := regexp.MustCompile(`__table_name__\s*\|\|\s*'_'\s*\|\|\s*(\w+)\s+AS\s+_pk_id`)
	whre := regexp.MustCompile(`(\d+)\s+AS\s+wh_type`)
	jre := regexp.MustCompile(`json_int\((\w+),\s*'([^']+)'\)\s+AS\s+(\w+)`)
	for _, r := range job.Route {
		for _, source := range strings.Split(r.Source, ",") {
			source = strings.TrimSpace(source)
			route := Route{Name: fmt.Sprintf("wms_%03d", len(c.Routes)+1), Source: source, Target: r.Target, SourceKey: []string{"id"}, Keys: []string{"_sync_source", "_sync_table", "_sync_pk"}, IncludeAll: true, Columns: map[string]Expr{}}
			if opts.Mode == "sidecar" {
				route.Keys = targetKeys[r.Target]
				if len(route.Keys) == 0 {
					return fmt.Errorf("target-keys is missing real business keys for %s", r.Target)
				}
				seen := map[string]bool{}
				for _, key := range route.Keys {
					if key == "" || strings.HasPrefix(key, "_sync_") || seen[key] {
						return fmt.Errorf("invalid business keys for %s", r.Target)
					}
					seen[key] = true
				}
			}
			matched := false
			for _, t := range job.Transform {
				for _, ts := range strings.Split(t.Source, ",") {
					if strings.TrimSpace(ts) != source {
						continue
					}
					if matched {
						return fmt.Errorf("multiple transforms for %s", source)
					}
					matched = true
					projection := t.Projection
					if m := pkre.FindStringSubmatch(projection); m != nil {
						route.SourceKey = []string{m[1]}
						route.Columns["_pk_id"] = Expr{Function: "concat", Args: []Expr{{From: "__table_name__"}, {Literal: "_"}, {From: m[1]}}}
						projection = pkre.ReplaceAllString(projection, "")
					}
					if m := whre.FindStringSubmatch(projection); m != nil {
						route.Columns["wh_type"] = Expr{Function: "int64", Args: []Expr{{Literal: m[1]}}}
						projection = whre.ReplaceAllString(projection, "")
					}
					if m := jre.FindStringSubmatch(projection); m != nil {
						route.Columns[m[3]] = Expr{Function: "json_int", Args: []Expr{{From: m[1]}, {Literal: m[2]}}}
						projection = jre.ReplaceAllString(projection, "")
					}
					if strings.Trim(projection, " *,\t\r\n") != "" {
						return fmt.Errorf("unsupported projection for %s", source)
					}
					if t.Keys != "" {
						route.SourceKey = nil
						for _, key := range strings.Split(t.Keys, ",") {
							key = strings.TrimSpace(key)
							if key == "_pk_id" {
								m := pkre.FindStringSubmatch(t.Projection)
								if m == nil {
									return fmt.Errorf("missing _pk_id expression")
								}
								key = m[1]
							}
							route.SourceKey = append(route.SourceKey, key)
						}
					}
				}
			}
			c.Routes = append(c.Routes, route)
		}
	}
	out, e := yaml.Marshal(&c)
	if e != nil {
		return e
	}
	header := "# Generated from Flink routes/transforms only. No credentials copied.\n# Connect to PostgreSQL mirror tables, NOT to MySQL. Review source-key and target DDL against real schemas before init.\n# Source schema StoreServer is case-sensitive: create/quote it correctly in PostgreSQL.\n"
	return os.WriteFile(output, append([]byte(header), out...), 0600)
}
