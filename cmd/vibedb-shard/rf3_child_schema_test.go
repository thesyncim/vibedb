package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

func TestRF3ChildSchemaCanonicalMetadataPreflight(t *testing.T) {
	profile := rf3testfixture.DurableGatewayMemberProfiles()[rf3testfixture.DurableGatewayDataAGroup]
	raw, err := vibejson.Marshal(&profile.GlobalIndexes)
	if err != nil {
		t.Fatal(err)
	}
	parse := func(raw []byte) ([]sqldriver.ReplicatedGlobalIndexRelation, error) {
		document, err := vibejson.ParseOptions(raw, vibejson.Options{})
		if err != nil {
			return nil, err
		}
		return parseRF3ChildGlobalIndexes(document.Node())
	}
	got, err := parse(raw)
	if err != nil || !slices.Equal(got, profile.GlobalIndexes) {
		t.Fatalf("explicit descriptors: %v, %v", got, err)
	}
	for name, invalid := range map[string][]byte{
		"empty":            []byte(`[]`),
		"sparse":           bytes.Replace(raw, []byte(`"relation":2`), []byte(`"relation":3`), 1),
		"unknown field":    bytes.Replace(raw, []byte(`"table":`), []byte(`"unknown":`), 1),
		"extra field":      bytes.Replace(raw, []byte(`}]`), []byte(`,"extra":1}]`), 1),
		"numeric overflow": bytes.Replace(raw, []byte(`"key_arity":1`), []byte(`"key_arity":256`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parse(invalid); err == nil {
				t.Fatal("noncanonical descriptor accepted")
			}
		})
	}
	for _, source := range []string{`[]`, `[""]`, `["` + strings.Repeat("x", sqldriver.ReplicatedChildSchemaMaxBytes) + `"]`} {
		document, err := vibejson.ParseOptions([]byte(source), vibejson.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parseRF3ChildSchemaStatements(document.Node(), 40); err == nil {
			t.Fatal("unbounded/empty DDL accepted")
		}
	}
}

func TestRF3ChildSchemaMismatchRejectedBeforeFilesystemEffects(t *testing.T) {
	path := filepath.Join(t.TempDir(), "never-created", "child.vdb")
	if err := prepareRF3ChildSQL(context.Background(), rf3ManifestSplitChildRegistry{
		Table: "docs", CreateTable: "CREATE TABLE docs (PRIMARY KEY (id))",
	}, path, splitcontroller.ChildReplicaTarget{}); err == nil {
		t.Fatal("unauthenticated child schema accepted")
	}
	if _, err := os.Lstat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Fatalf("schema mismatch created root: %v", err)
	}
}
