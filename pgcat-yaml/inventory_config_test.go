package main

import (
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestInventoryConfigurations(t *testing.T) {
	for _, path := range []string{
		"../inventory-sync/pgcat/mirror-to-summary-n-to1.yaml",
		"../inventory-sync/pgcat/summary-to-rollback-1ton.yaml",
	} {
		config, err := loadConfig(path)
		if err != nil {
			t.Fatalf("load %s: %v", path, err)
		}
		if len(config.Routes) != 3 {
			t.Fatalf("%s: got %d routes, want 3", path, len(config.Routes))
		}
	}
}

func TestInventoryFlinkYAML(t *testing.T) {
	for _, path := range []string{
		"../inventory-sync/flink/inventory-mysql-to-pg-mirror-1to1.yaml",
		"../inventory-sync/flink/inventory-mysql-to-pg-component-n-to1.yaml",
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		var document map[string]any
		if err = yaml.Unmarshal(data, &document); err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, section := range []string{"source", "sink", "pipeline"} {
			if document[section] == nil {
				t.Fatalf("%s: missing %s", path, section)
			}
		}
	}
}
