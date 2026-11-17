package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"gopkg.in/yaml.v3"
)

func TestSidecarIdentityAndShardPlan(t *testing.T) {
	c, e := loadConfig("examples/sidecar/b-to-a.yaml")
	if e != nil {
		t.Fatal(e)
	}
	id := newIdentity(c.Identity.Namespace, "legacy", "pgcat_sidecar_a.orders_0", "[101]", "snapshot:0/100")
	if !sameIdentity(id, newIdentity(c.Identity.Namespace, "legacy", "pgcat_sidecar_a.orders_0", "[101]", "snapshot:0/100")) {
		t.Fatal("identity is not deterministic")
	}
	if sameIdentity(id, newIdentity(c.Identity.Namespace, "legacy", "pgcat_sidecar_a.orders_0", "[101]", "wal:0/200:0")) {
		t.Fatal("reinsert reused identity")
	}
	before := Row{"record_key": "original:[101]", "order_id": json.Number("101"), "tenant_id": 2, "customer_name": "Alice", "outbound_id": json.Number("9007199254740993"), "note": "n", "payload": map[string]any{"keep": "yes"}}
	after := merge(before, Row{"tenant_id": 3, "order_id": json.Number("111")})
	plan, e := c.planIdentity("pgcat_sidecar_b.orders", before, after, id)
	if e != nil {
		t.Fatal(e)
	}
	if len(plan) != 3 || !plan[0].Delete || plan[0].Route.Target != "pgcat_sidecar_a.orders_0" {
		t.Fatal("wrong relocation", plan)
	}
	for _, m := range plan {
		if !sameIdentity(m.Identity, id) {
			t.Fatal("relocation changed identity")
		}
		for col := range m.Values {
			if strings.HasPrefix(col, "_sync_") {
				t.Fatal("business provenance column leaked")
			}
		}
	}
	if _, e = c.plan("pgcat_sidecar_b.orders", before, after); e == nil {
		t.Fatal("unresolved sidecar identity accepted")
	}
	for _, raw := range []string{"null", "[]", "[null]", "[{}]", "[[1]]", "[1] true"} {
		if _, e = canonicalKey(raw); e == nil {
			t.Fatalf("invalid key accepted: %s", raw)
		}
	}
	if k, e := canonicalKey("[9007199254740993, \"s\", true]"); e != nil || k != "[9007199254740993,\"s\",true]" {
		t.Fatal(k, e)
	}
}

func TestSidecarMessageMatchesEachDML(t *testing.T) {
	c, e := loadConfig("examples/sidecar/b-to-a.yaml")
	if e != nil {
		t.Fatal(e)
	}
	rel := &pglogrepl.RelationMessage{Namespace: "pgcat_sidecar_b", RelationName: "orders", Columns: []*pglogrepl.RelationMessageColumn{{Name: "record_key", DataType: 25}}}
	tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{DataType: 't', Data: []byte("k")}}}
	changes := []wireChange{{rel: rel, old: tuple, new: tuple, kind: 'U'}, {rel: rel, old: tuple, new: tuple, kind: 'U'}}
	id := newIdentity(c.Identity.Namespace, "legacy", "pgcat_sidecar_a.orders_0", "[101]", "wal:0/100:0")
	event := IdentityEvent{Version: 1, Namespace: c.Identity.Namespace, Node: c.Source.ID, Table: "pgcat_sidecar_b.orders", Keys: []string{"record_key"}, PK: `["k"]`, Kind: "U", Identity: *id}
	if e = matchIdentityEvent(c, changes, event); e != nil {
		t.Fatal(e)
	}
	if changes[0].identity != nil || changes[1].identity == nil {
		t.Fatal("did not bind latest unmatched DML")
	}
	if e = matchIdentityEvent(c, changes, event); e != nil {
		t.Fatal(e)
	}
	if e = matchIdentityEvent(c, changes, event); e == nil {
		t.Fatal("extra lineage message accepted")
	}
	event.Node = "wrong"
	if e = matchIdentityEvent(c, nil, event); e == nil {
		t.Fatal("foreign node accepted")
	}
	event.Node = c.Source.ID
	event.Keys = []string{"order_id"}
	if e = matchIdentityEvent(c, nil, event); e == nil {
		t.Fatal("wrong business key definition accepted")
	}
}

func TestSidecarConfigAndConversionGuards(t *testing.T) {
	data, e := os.ReadFile("examples/sidecar/a-to-b.yaml")
	if e != nil {
		t.Fatal(e)
	}
	file := filepath.Join(t.TempDir(), "job.yaml")
	for _, replacement := range [][2]string{{"  namespace: orders_sidecar_v1\n", ""}, {"  id: new_app\n", ""}, {"plugin: pgoutput", "plugin: pgcat"}, {"primary-keys: [record_key]", "primary-keys: [_sync_pk]"}} {
		s := strings.ReplaceAll(strings.ReplaceAll(string(data), "\r\n", "\n"), replacement[0], replacement[1])
		if e = os.WriteFile(file, []byte(s), 0600); e != nil {
			t.Fatal(e)
		}
		if _, e = loadConfig(file); e == nil {
			t.Fatal("unsafe sidecar configuration accepted", replacement)
		}
	}
	out := filepath.Join(t.TempDir(), "generated.yaml")
	opts := conversionOptions{Mode: "sidecar", Namespace: "wms-test", SinkID: "wms_new"}
	if e = convertWMSOptions("testdata/wms-routes.yaml", out, opts); e == nil {
		t.Fatal("conversion inferred keys")
	}
	if e = convertWMS("testdata/wms-routes.yaml", out); e != nil {
		t.Fatal(e)
	}
	c, e := loadConfig(out)
	if e != nil {
		t.Fatal(e)
	}
	keys := map[string][]string{}
	for _, r := range c.Routes {
		keys[r.Target] = []string{"_pk_id"}
	}
	raw, _ := yaml.Marshal(keys)
	opts.TargetKeys = filepath.Join(t.TempDir(), "keys.yaml")
	if e = os.WriteFile(opts.TargetKeys, raw, 0600); e != nil {
		t.Fatal(e)
	}
	if e = convertWMSOptions("testdata/wms-routes.yaml", out, opts); e != nil {
		t.Fatal(e)
	}
	converted, e := loadConfig(out)
	if e != nil {
		t.Fatal(e)
	}
	if !converted.sidecar() || len(converted.Routes) != len(c.Routes) {
		t.Fatal("routes lost in sidecar conversion")
	}
	// These test-only keys exercise conversion, not a claim about production DDL.
	delete(keys, c.Routes[0].Target)
	raw, _ = yaml.Marshal(keys)
	os.WriteFile(opts.TargetKeys, raw, 0600)
	if e = convertWMSOptions("testdata/wms-routes.yaml", out, opts); e == nil {
		t.Fatal("missing target key accepted")
	}
}
