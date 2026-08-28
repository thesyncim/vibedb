package driver

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

func schemaCatalogBundleSeedDocument(t testing.TB) []byte {
	t.Helper()
	// Schema image import uses canonical JSON. Seed that same representation
	// so exact byte comparisons continue to detect changed fields after rollover.
	document, err := vibejson.AppendCanonicalize(nil, []byte(`{"id":"schema-doc","email":"a"}`))
	if err != nil {
		t.Fatal(err)
	}
	return document
}

func TestSchemaCatalogBundleSeedPreservesExactCanonicalDataAndKeyBound(t *testing.T) {
	document := schemaCatalogBundleSeedDocument(t)
	if !bytes.Equal(document, []byte(`{"email":"a","id":"schema-doc"}`)) {
		t.Fatalf("seed canonical bytes changed: %q", document)
	}
	var fields struct {
		Email string `json:"email"`
		ID    string `json:"id"`
	}
	if err := vibejson.Unmarshal(document, &fields); err != nil || fields.Email != "a" || fields.ID != "schema-doc" {
		t.Fatalf("seed field values changed: %+v err=%v", fields, err)
	}
	again, err := vibejson.AppendCanonicalize(nil, document)
	if err != nil || !bytes.Equal(again, document) {
		t.Fatalf("seed canonicalization is not byte stable: %q err=%v", again, err)
	}
	primary, err := vibejson.CompilePointer("/id")
	if err != nil {
		t.Fatal(err)
	}
	key, err := documentKey(document, "/id", primary, replicatedMaxKeyBytes)
	if err != nil || key == "" {
		t.Fatalf("seed exact primary key: %q err=%v", key, err)
	}
	if _, err = documentKey(document, "/id", primary, len(key)-1); !errors.Is(err, durable.ErrKeyTooLarge) {
		t.Fatalf("seed key must retain bounded extraction: %v", err)
	}
	if got, err := documentKey(document, "/id", primary, len(key)); err != nil || got != key {
		t.Fatalf("exact key bound changed key: %q err=%v", got, err)
	}
}
