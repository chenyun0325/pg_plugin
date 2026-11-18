package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func convertedGeneric(t *testing.T) *Config {
	t.Helper()
	out := filepath.Join(t.TempDir(), "generic.yaml")
	if err := convertWMS("testdata/wms-generic-routes.yaml", out); err != nil {
		t.Fatal(err)
	}
	c, err := loadConfig(out)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestGenericProductionConversion(t *testing.T) {
	c := convertedGeneric(t)
	tests := []struct {
		table  string
		row    Row
		key    string
		target string
		count  int
	}{
		{"StoreServer.sys_warehouse_outbound_1", Row{"outbound_id": 101}, "sys_warehouse_outbound_1_101", "StoreServerHaV2.sys_warehouse_outbound_m", 1},
		{"StoreServer.sys_warehouse_outbound_1_history", Row{"outbound_id": 101}, "sys_warehouse_outbound_1_history_101", "StoreServerHaV2.sys_warehouse_outbound_m", 1},
		{"StoreServer.sys_warehouse_outbound_product_1", Row{"outbound_id": 101, "upc": "u"}, "sys_warehouse_outbound_product_1_101", "StoreServerHaV2.sys_warehouse_outbound_product_m", 1},
		{"StoreServer.mem_customer_tracking", Row{"id": 1, "param": `{"outbound_id":9007199254740993}`}, "", "StoreServerHaV2.mem_customer_tracking", 1},
		// ALL_MATCH preserves fanout of nonsharded sources also matching the fallback.
		{"StoreServer.thd_warehouse_outbound", Row{"outbound_id": 101}, "thd_warehouse_outbound_101", "StoreServerHaV2.sys_warehouse_outbound_m", 2},
	}
	for _, tc := range tests {
		t.Run(tc.table, func(t *testing.T) {
			plan, err := c.plan(tc.table, nil, tc.row)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan) != tc.count {
				t.Fatalf("got %d mutations, want %d", len(plan), tc.count)
			}
			found := false
			for _, m := range plan {
				if m.Route.Target == tc.target {
					found = true
					if tc.key != "" && m.Values["_pk_id"] != tc.key {
						t.Fatal(m.Values)
					}
					if tc.key == "" && m.Values["outbound_id"] != json.Number("9007199254740993") {
						t.Fatal("JSON number lost", m.Values)
					}
				}
			}
			if !found {
				t.Fatal("missing target", plan)
			}
		})
	}
	if _, err := c.sourceKey("StoreServer.sys_warehouse_outbound_product_1"); err != nil {
		t.Fatal(err)
	}
	c.RouteMode = "first-match"
	if len(c.routes("StoreServer.thd_warehouse_outbound")) != 1 {
		t.Fatal("FIRST_MATCH ignored")
	}
}

func TestProjectionStrictness(t *testing.T) {
	all, cols, err := parseProjection("*, concat('a,b', lower(name)) AS label, __table_name__ || '_' || id _pk_id")
	if err != nil || !all {
		t.Fatal(err)
	}
	v, err := eval(cols["label"], Row{"name": "BOB"}, Row{})
	if err != nil || v != "a,bbob" {
		t.Fatal(v, err)
	}
	keys, err := projectionSourceKeys("_pk_id,upc,bo_sn", cols)
	if err != nil || strings.Join(keys, ",") != "id,upc,bo_sn" {
		t.Fatal(keys, err)
	}
	for _, bad := range []string{"*, arbitrary_java(id) x", "*, concat(id,'x' x", "*, id a,id a", "*, id + 1 x", "*, 'a' + 'b' x"} {
		if _, _, err := parseProjection(bad); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}

func TestOwnerTargetRouting(t *testing.T) {
	c, err := loadConfig("examples/sidecar/b-to-a.yaml")
	if err != nil {
		t.Fatal(err)
	}
	c.Routes = c.Routes[:1]
	r := &c.Routes[0]
	r.Target = ""
	r.TargetFrom = "owner-table"
	r.AllowedTargets = []string{"legacy.orders_4"}
	r.When = nil
	r.JSONPatches = nil
	r.SourceKey = []string{"record_key"}
	r.Keys = []string{"id"}
	r.Columns = map[string]Expr{"id": {Function: "json_index", Args: []Expr{{From: "__owner_pk__"}, {Literal: 0}}}}
	id := newIdentity(c.Identity.Namespace, "legacy", "legacy.orders_4", "[9007199254740993]", "g1")
	p, err := c.planIdentity("pgcat_sidecar_b.orders", nil, Row{"record_key": "x"}, id)
	if err != nil || len(p) != 1 || p[0].Route.Target != "legacy.orders_4" || p[0].Values["id"] != json.Number("9007199254740993") {
		t.Fatal(p, err)
	}
	id = newIdentity(c.Identity.Namespace, "new_app", "new.orders", "[1]", "g2")
	if _, err = c.planIdentity("pgcat_sidecar_b.orders", nil, Row{"record_key": "x"}, id); err == nil {
		t.Fatal("unknown lineage target accepted")
	}
}

func TestNoCredentialConversion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.yaml")
	output := filepath.Join(t.TempDir(), "output.yaml")
	raw, err := os.ReadFile("testdata/wms-generic-routes.yaml")
	if err != nil {
		t.Fatal(err)
	}
	raw = append([]byte("source:\n  password: do-not-export-this-secret\n"), raw...)
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err = convertWMS(path, output); err != nil {
		t.Fatal(err)
	}
	result, _ := os.ReadFile(output)
	if strings.Contains(string(result), "do-not-export-this-secret") {
		t.Fatal("secret copied")
	}
}

func TestGenericExampleConfigs(t *testing.T) {
	for _, file := range []string{"examples/generic/a-to-b.yaml", "examples/generic/b-to-a.yaml"} {
		c, err := loadConfig(file)
		if err != nil {
			t.Fatal(err)
		}
		tables := []string{"pgcat_generic_a.orders_0", "pgcat_generic_a.orders_1", "pgcat_generic_a.customer"}
		wantTargets := 2 // two shards share one orders target, plus customer
		if strings.Contains(file, "b-to-a") {
			tables = []string{"pgcat_generic_b.orders", "pgcat_generic_b.customer"}
			wantTargets = 3 // two distinct original shards, plus customer
		}
		targets, err := c.targetRoutes(tables)
		if err != nil || len(targets) != wantTargets {
			t.Fatalf("%s: targets=%v err=%v", file, targets, err)
		}
	}
}
