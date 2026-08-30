package store

import (
	"errors"
	"testing"
)

func TestStoreCollectionRejectsUniqueIndex(t *testing.T) {
	collection, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("one", []byte(`{"email":"same@example.com"}`)); err != nil {
		t.Fatal(err)
	}
	before := collection.Stats()

	info, err := collection.CreateIndex(IndexDefinition{
		Name: "email_unique", Paths: []string{"/email"}, Unique: true,
	})
	if !errors.Is(err, ErrIndexDefinition) {
		t.Fatalf("CreateIndex Unique error = %v, want %v", err, ErrIndexDefinition)
	}
	if info != (IndexInfo{}) {
		t.Fatalf("CreateIndex Unique info = %+v, want zero", info)
	}
	after := collection.Stats()
	if after.Generation != before.Generation || after.Indexes != before.Indexes ||
		after.PhysicalIndexes != before.PhysicalIndexes {
		t.Fatalf("rejected Unique changed collection: before=%+v after=%+v", before, after)
	}
}
