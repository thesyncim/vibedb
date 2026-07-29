package durable

import (
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// TestFileStoreBufferedPutStoreAllocations pins the writer-side cost of a
// same-size buffered COW replacement with no indexes. A root-only schema keeps
// the canonical in-place optimization out of the measurement while validating
// allocation-free, so the test covers key scratch, transaction admission, and
// state publication.
func TestFileStoreBufferedPutStoreAllocations(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "buffered-put-alloc-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Durability = DurabilityBufferedVisible
	options.QueueSlots = 512
	options.GroupLimit = 512
	options.Collection.Schema, err = store.CompileSchema(store.SchemaDefinition{
		Root: store.SchemaObject,
	})
	if err != nil {
		t.Fatal(err)
	}
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	const key = "allocation-key"
	value := []byte(`{"value":"same-size-buffered-replacement"}`)
	if _, err := collection.Put(key, value); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	// Warm every collection-owned high-water scratch before measuring.
	if _, err := collection.Put(key, value); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		if created, putErr := collection.Put(key, value); putErr != nil || created {
			panic("same-size buffered replacement failed")
		}
	})
	if allocs != 1 {
		t.Fatalf(
			"buffered Put store allocations = %.2f, want 1 published state",
			allocs,
		)
	}
}
