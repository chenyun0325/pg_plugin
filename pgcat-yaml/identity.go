package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
)

const identityPrefix = "pgcat-yaml.identity.v1"
const identityMessageLimit = 64 << 10

// RecordIdentity describes the logical record, independent of physical shard/key.
type RecordIdentity struct {
	EntityID   string `json:"entity_id"`
	Owner      Owner  `json:"owner"`
	Generation string `json:"generation"`
}
type IdentityEvent struct {
	Version   int            `json:"version"`
	Namespace string         `json:"namespace"`
	Node      string         `json:"node"`
	Table     string         `json:"table"`
	Keys      []string       `json:"keys"`
	PK        string         `json:"pk"`
	Kind      string         `json:"kind"` // I/U/D, one message after each actual business DML
	Identity  RecordIdentity `json:"identity"`
}
type binding struct {
	Identity RecordIdentity
	Alive    bool
}

func newIdentity(namespace, source, table, pk, generation string) *RecordIdentity {
	raw, _ := json.Marshal([]string{"v1", namespace, source, table, pk, generation})
	sum := sha256.Sum256(raw)
	return &RecordIdentity{EntityID: hex.EncodeToString(sum[:]), Owner: Owner{source, table, pk}, Generation: generation}
}
func (i *RecordIdentity) valid() error {
	if i == nil || i.EntityID == "" || i.Owner.Source == "" || !qualified(i.Owner.Table) || i.Generation == "" {
		return fmt.Errorf("invalid record identity")
	}
	if _, e := canonicalKey(i.Owner.PK); e != nil {
		return e
	}
	return nil
}
func sameIdentity(a, b *RecordIdentity) bool { return a != nil && b != nil && *a == *b }
func canonicalKey(raw string) (string, error) {
	var a []any
	d := json.NewDecoder(strings.NewReader(raw))
	d.UseNumber()
	if e := d.Decode(&a); e != nil || len(a) == 0 {
		return "", fmt.Errorf("invalid key JSON array")
	}
	for _, v := range a {
		switch v.(type) {
		case string, json.Number, bool:
		default:
			return "", fmt.Errorf("identity keys must be non-NULL scalar values")
		}
	}
	if d.Decode(new(any)) != io.EOF {
		return "", fmt.Errorf("trailing key JSON")
	}
	b, e := json.Marshal(a)
	return string(b), e
}
func matchIdentityEvent(c *Config, changes []wireChange, event IdentityEvent) error {
	if event.Version != 1 || event.Namespace != c.Identity.Namespace || event.Node != c.Source.ID {
		return fmt.Errorf("identity message scope/version mismatch")
	}
	if event.Kind != "I" && event.Kind != "U" && event.Kind != "D" {
		return fmt.Errorf("invalid identity message kind")
	}
	if e := event.Identity.valid(); e != nil {
		return e
	}
	keys, e := c.sourceKey(event.Table)
	if e != nil {
		return e
	}
	if !reflect.DeepEqual(keys, event.Keys) {
		return fmt.Errorf("source-key and reverse target-key differ for %s", event.Table)
	}
	pk, e := canonicalKey(event.PK)
	if e != nil {
		return e
	}
	for j := len(changes) - 1; j >= 0; j-- {
		ch := &changes[j]
		if ch.identity != nil || string(ch.kind) != event.Kind || ch.rel.Namespace+"."+ch.rel.RelationName != event.Table {
			continue
		}
		tuple := ch.new
		if ch.kind == 'D' {
			tuple = ch.old
		}
		row, e := tupleRow(ch.rel, tuple)
		if e != nil {
			return e
		}
		k, e := rowKey(row, keys)
		if e != nil {
			return e
		}
		if k != pk {
			continue
		}
		ch.identity = &event.Identity
		return nil
	}
	return fmt.Errorf("identity message has no preceding matching DML for %s key=%s", event.Table, event.PK)
}
