package durable

import (
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// TestFileStoreWarmedPointMutationAllocations pins the writer-side cost of the
// mature same-size replacement and delete paths. A root-only schema keeps the
// parallel schemaless lane out of the measurement: buffered-visible exercises
// exclusive overlay publication and async-visible exercises the rooted
// mutation path. Once the collection-owned workspaces are warm, the only
// permitted allocation is the immutable fileStoreState each mutation publishes.
func TestFileStoreWarmedPointMutationAllocations(t *testing.T) {
	for _, tc := range []struct {
		name       string
		durability DurabilityMode
	}{
		{name: "buffered", durability: DurabilityBufferedVisible},
		{name: "async", durability: DurabilityAsyncVisible},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testFileStoreWarmedPointMutationAllocations(
				t, tc.durability,
			)
		})
	}
}

func testFileStoreWarmedPointMutationAllocations(
	t *testing.T,
	durability DurabilityMode,
) {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "buffered-put-alloc-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Durability = durability
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

	// key is converted once, outside the measured closure: the store speaks
	// []byte, so the caller owns the conversion and a per-call []byte(key) would
	// be counted as an allocation the store never makes.
	key := []byte("allocation-key")
	otherKey := []byte("allocation-key-other")
	value := []byte(`{"value":"same-size-buffered-replacement"}`)
	if _, err := collection.Put(key, value); err != nil {
		t.Fatal(err)
	}
	// Keep the routed leaf non-empty when key is deleted. Otherwise the
	// measurement includes the deliberately exceptional empty-leaf structural
	// reclamation transaction instead of steady delete churn.
	if _, err := collection.Put(otherKey, value); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	// Warm every collection-owned high-water scratch before measuring.
	if _, err := collection.Put(key, value); err != nil {
		t.Fatal(err)
	}
	replaceAllocs := testing.AllocsPerRun(100, func() {
		if created, putErr := collection.Put(key, value); putErr != nil || created {
			panic("same-size buffered replacement failed")
		}
	})
	if replaceAllocs != 1 {
		t.Fatalf(
			"same-size Put store allocations = %.2f, want 1 published state",
			replaceAllocs,
		)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	deleteRestoreAllocs := testing.AllocsPerRun(100, func() {
		if deleted, deleteErr := collection.Delete(key); deleteErr != nil || !deleted {
			panic("delete failed")
		}
		if created, putErr := collection.Put(key, value); putErr != nil || !created {
			panic("restore failed")
		}
	})
	if deleteRestoreAllocs != 2 {
		t.Fatalf(
			"delete+restore store allocations = %.2f, want 2 published states",
			deleteRestoreAllocs,
		)
	}
}
