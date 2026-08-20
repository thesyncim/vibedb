package durable

import (
	"os"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/store"
)

func TestIndexProbeMemoryBoundCatalogOwnership(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "index-memory-bound-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := Options{
		Collection: store.Options{ChunkDocuments: 1},
		Indexes: []store.IndexDefinition{{
			Name: "long_logical_index_name",
			Paths: []string{
				"/a/long/catalog/path/whose/backing/stays/collection-owned",
			},
		}},
		PageSize: 4096, MaxPageSize: 64 << 10,
		MaxDocumentBytes: 1024,
	}
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	bound, err := snapshot.IndexProbeMemoryBound()
	if err != nil {
		t.Fatal(err)
	}
	wantCatalog := int64(2*unsafe.Sizeof(store.IndexInfo{}) + 64)
	if bound.CatalogBytes != wantCatalog ||
		bound.MaskCount != 0 ||
		bound.CandidateWorkspaceBytes != 0 ||
		bound.RangeWorkspaceBytes <= 0 ||
		bound.ExactSingleWorkspaceBytes != 0 ||
		bound.ExactCompoundWorkspaceBytes != 0 {
		t.Fatalf("empty indexed bound = %+v, catalog want %d", bound, wantCatalog)
	}

	// AppendIndexes only assigns collection-owned string headers into caller
	// storage. A warmed destination therefore performs no hidden name/path
	// payload copies, which is why CatalogBytes charges the struct array only.
	indexes := make([]store.IndexInfo, 0, 1)
	indexes = snapshot.AppendIndexes(indexes)
	allocs := testing.AllocsPerRun(100, func() {
		indexes = snapshot.AppendIndexes(indexes[:0])
		if len(indexes) != 1 ||
			indexes[0].Name != options.Indexes[0].Name ||
			indexes[0].Columns[0] != options.Indexes[0].Paths[0] {
			panic("catalog mismatch")
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed AppendIndexes allocated %.2f times", allocs)
	}
	boundAllocs := testing.AllocsPerRun(100, func() {
		if _, boundErr := snapshot.IndexProbeMemoryBound(); boundErr != nil {
			panic(boundErr)
		}
	})
	if boundAllocs != 0 {
		t.Fatalf("IndexProbeMemoryBound allocated %.2f times", boundAllocs)
	}
}

func TestIndexProbeMemoryBoundNoIndexLeavesProbeWorkspaceZero(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "index-memory-zone-only-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, Options{
		Collection:       store.Options{ChunkDocuments: 1},
		PageSize:         4096,
		MaxPageSize:      64 << 10,
		MaxDocumentBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if _, err := collection.Put([]byte("k"), []byte(`{"value":1}`)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	bound, err := snapshot.IndexProbeMemoryBound()
	if err != nil {
		t.Fatal(err)
	}
	if bound.CatalogBytes != 0 ||
		bound.MaskCount != 1 ||
		bound.CandidateWorkspaceBytes != 0 ||
		bound.RangeWorkspaceBytes != 0 ||
		bound.ExactSingleWorkspaceBytes != 0 ||
		bound.ExactCompoundWorkspaceBytes != 0 {
		t.Fatalf("zone-only bound = %+v", bound)
	}
}
