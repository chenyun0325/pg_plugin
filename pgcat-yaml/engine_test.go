package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestYAMLPlanAndJSON(t *testing.T) {
	c, e := loadConfig("examples/b-to-a.yaml")
	if e != nil {
		t.Fatal(e)
	}
	r := Row{"record_key": "k", "order_id": json.Number("9007199254740993"), "tenant_id": json.Number("2"), "customer_name": "A", "outbound_id": json.Number("9007199254740993"), "note": "n", "payload": map[string]any{"keep": "yes"}}
	after := merge(r, Row{"tenant_id": json.Number("3")})
	p, e := c.plan("pgcat_demo_b.orders", r, after)
	if e != nil {
		t.Fatal(e)
	}
	if len(p) != 3 || !p[0].Delete || p[0].Route.Target != "pgcat_demo_a.orders_0" {
		t.Fatal(p)
	}
	for _, m := range p {
		if m.Values["id"] != nil && m.Values["id"] != json.Number("9007199254740993") {
			t.Fatal("integer precision lost")
		}
	}
}
func TestMissingKeyStrict(t *testing.T) {
	if _, e := jsonGet([]any{map[string]any{}, "absent"}); e == nil {
		t.Fatal("missing key accepted")
	}
	if v, e := jsonGet([]any{nil, "x"}); e != nil || v != nil {
		t.Fatal(v, e)
	}
	if _, e := rowKey(Row{"id": nil}, []string{"id"}); e == nil {
		t.Fatal("null identity accepted")
	}
}
func TestProvenanceAndStableKey(t *testing.T) {
	c, e := loadConfig("examples/a-to-b.yaml")
	if e != nil {
		t.Fatal(e)
	}
	row := Row{"id": 1, "tenant_id": 3, "param": map[string]any{"customer": map[string]any{"name": "A"}, "outbound_id": 1, "note": "n"}, "_sync_source": "legacy", "_sync_table": "pgcat_demo_a.orders_0", "_sync_pk": "[1]"}
	p, e := c.plan("pgcat_demo_a.orders_1", nil, row)
	if e != nil {
		t.Fatal(e)
	}
	if p[0].Values["record_key"] != "pgcat_demo_a.orders_0:[1]" {
		t.Fatal(p[0].Values)
	}
}
func TestDetailDeletePatchesOnly(t *testing.T) {
	c, e := loadConfig("examples/a-to-b.yaml")
	if e != nil {
		t.Fatal(e)
	}
	row := Row{"record_key": "k", "note": "n", "_sync_source": "legacy", "_sync_table": "pgcat_demo_a.orders_0", "_sync_pk": "[1]"}
	p, e := c.plan("pgcat_demo_a.order_detail", row, nil)
	if e != nil {
		t.Fatal(e)
	}
	if len(p) != 1 || p[0].Delete || p[0].Values["note"] != nil {
		t.Fatal(p)
	}
}
func TestPatchPreservesJSON(t *testing.T) {
	doc := map[string]any{"keep": "x", "customer": map[string]any{"tier": "gold"}}
	v, e := patchJSON(doc, []string{"customer", "name"}, "A")
	if e != nil {
		t.Fatal(e)
	}
	if v.(map[string]any)["customer"].(map[string]any)["tier"] != "gold" {
		t.Fatal(v)
	}
	if doc["customer"].(map[string]any)["name"] != nil {
		t.Fatal("patch mutated source image")
	}
}
func TestConvertUserJob(t *testing.T) {
	out := filepath.Join(t.TempDir(), "wms.yaml")
	if e := convertWMS("testdata/wms-routes.yaml", out); e != nil {
		t.Fatal(e)
	}
	c, e := loadConfig(out)
	if e != nil {
		t.Fatal(e)
	}
	if len(c.Routes) < 30 {
		t.Fatal("missing routes")
	}
	b, _ := os.ReadFile(out)
	if len(b) == 0 {
		t.Fatal("empty conversion")
	}
}
