package durable

import (
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson/document"
)

func TestFileStorePutHonorsCollectionMaxDepth(t *testing.T) {
	for _, test := range []struct {
		name       string
		durability DurabilityMode
		withSchema bool
		wantFast   bool
	}{
		{name: "buffered-concurrent-fast-path", durability: DurabilityBufferedVisible, wantFast: true},
		{name: "sync-structural-fallback", durability: DurabilitySync},
		{name: "buffered-schema-fallback", durability: DurabilityBufferedVisible, withSchema: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := testFileStoreOptions()
			options.Durability = test.durability
			options.Collection.IndexOptions = document.IndexOptions{MaxDepth: 1}
			if test.withSchema {
				schema, err := store.CompileSchema(store.SchemaDefinition{
					Root: store.SchemaAny,
				})
				if err != nil {
					t.Fatal(err)
				}
				options.Collection.Schema = schema
			}
			file, err := os.CreateTemp(t.TempDir(), "max-depth-*")
			if err != nil {
				t.Fatal(err)
			}
			collection, err := Create(file, options)
			if err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			defer func() {
				_ = collection.Close()
				_ = file.Close()
			}()

			if _, err := collection.Put([]byte("valid"), []byte(`[]`)); err != nil {
				t.Fatalf("valid Put: %v", err)
			}
			before := collection.Stats()
			if _, err := collection.Put([]byte("valid"), []byte(`[0]`)); err != nil {
				t.Fatalf("valid replacement: %v", err)
			}
			if got := collection.Stats().ConcurrentPrimaryReplaces -
				before.ConcurrentPrimaryReplaces; test.wantFast && got != 1 {
				t.Fatalf("concurrent fast-path replacements = %d, want 1", got)
			} else if !test.wantFast && got != 0 {
				t.Fatalf("fallback unexpectedly used concurrent fast path %d times", got)
			}
			generation := collection.Generation()
			if _, err := collection.Put([]byte("invalid"), []byte(`[[]]`)); err == nil {
				t.Fatal("Put accepted a document beyond Collection.IndexOptions.MaxDepth")
			}
			if got := collection.Generation(); got != generation {
				t.Fatalf("rejected Put changed generation from %d to %d", generation, got)
			}
			if _, found, err := collection.AppendRaw(nil, []byte("invalid")); err != nil || found {
				t.Fatalf("rejected key = found %v, err %v", found, err)
			}
		})
	}
}

func TestCreateFromPrimaryHonorsDestinationMaxDepthBeforeWriting(t *testing.T) {
	source, err := store.New(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Put("too-deep", []byte(`[[]]`)); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	file, err := os.CreateTemp(t.TempDir(), "bulk-max-depth-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	options := testFileStoreOptions()
	options.Collection.IndexOptions = document.IndexOptions{MaxDepth: 1}
	if _, err := CreateFromPrimary(source, file, options); err == nil {
		t.Fatal("CreateFromPrimary accepted a document beyond destination MaxDepth")
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Size(); got != 0 {
		t.Fatalf("rejected bulk build changed file size to %d", got)
	}
}

func TestFileStoreRejectsKeyBoundBeyondPhysicalFormat(t *testing.T) {
	options := testFileStoreOptions()
	options.MaxKeyBytes = 257
	if err := ValidateOptions(options); err == nil {
		t.Fatal("ValidateOptions accepted MaxKeyBytes beyond the primary format")
	}
}
