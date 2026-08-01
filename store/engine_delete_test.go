package store

import (
	"slices"
	"testing"
)

func TestCollectionDeletePreservesSnapshotsAndExactIndexes(t *testing.T) {
	collection, err := New(Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("a", []byte(`{"group":"x"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("b", []byte(`{"group":"x"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.CreateIndex(IndexDefinition{Name: "by_group", Paths: []string{"/group"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.BackfillIndex("by_group", 0); err != nil {
		t.Fatal(err)
	}
	before, _ := collection.Snapshot()
	beforeGeneration := collection.Generation()

	deleted, err := collection.Delete("a")
	if err != nil || !deleted {
		t.Fatalf("Delete = %v,%v", deleted, err)
	}
	if collection.Generation() != beforeGeneration+1 || collection.Len() != 1 {
		t.Fatalf("state = generation %d len %d", collection.Generation(), collection.Len())
	}
	if _, ok := collection.GetRaw("a"); ok {
		t.Fatal("deleted key remains in current collection")
	}
	if raw, ok := before.GetRaw("a"); !ok || string(raw.Bytes()) != `{"group":"x"}` {
		t.Fatalf("old snapshot = %q,%v", raw.Bytes(), ok)
	}
	current, _ := collection.Snapshot()
	keys, err := current.IndexRawKeys("by_group", []byte(`"x"`))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(keys, []string{"b"}) {
		t.Fatalf("indexed keys = %v", keys)
	}
	if deleted, err := collection.Delete("missing"); err != nil || deleted {
		t.Fatalf("missing Delete = %v,%v", deleted, err)
	}
	if collection.Generation() != beforeGeneration+1 {
		t.Fatal("missing delete advanced generation")
	}
}

func TestCollectionDeleteMappedBaseAndReuse(t *testing.T) {
	builder, err := NewBuilder(Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append("base", []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if deleted, err := collection.Delete("base"); err != nil || !deleted {
		t.Fatalf("Delete mapped key = %v,%v", deleted, err)
	}
	if _, ok := collection.GetRaw("base"); ok {
		t.Fatal("mapped key remains visible")
	}
	if created, err := collection.Put("base", []byte(`{"v":2}`)); err != nil || !created {
		t.Fatalf("reinsert = created %v, err %v", created, err)
	}
	raw, ok := collection.GetRaw("base")
	if !ok || string(raw.Bytes()) != `{"v":2}` {
		t.Fatalf("reinserted value = %q,%v", raw.Bytes(), ok)
	}
}
