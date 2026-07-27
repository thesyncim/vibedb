package durable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func documentFormatSource(t *testing.T, count int) *store.Collection {
	t.Helper()
	// Repetitive shapes and a small categorical value set, because the compact
	// format only groups chunks when grouping actually saves bytes. A corpus it
	// would decline to group could not tell the two settings apart.
	source, err := store.New(store.Options{ChunkDocuments: 8})
	if err != nil {
		t.Fatal(err)
	}
	for i := range count {
		document := fmt.Appendf(nil,
			`{"kind":"same","tier":"gold","country":"PT","n":%d,"note":"a repeated note"}`, i)
		if _, err := source.Put(fmt.Sprintf("key-%05d", i), document); err != nil {
			t.Fatal(err)
		}
	}
	return source
}

func createFormatted(t *testing.T, dir string, format DocumentFormat, source *store.Collection) *Collection {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("format-%d.vibe", format))
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Collection: store.Options{ChunkDocuments: 8}, ResidentBytes: 8 << 20,
		Backend: BackendPortable, DocumentFormat: format,
	}
	if _, err := CreateFrom(source, file, options); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately reopened with the default DocumentFormat: the field selects
	// what CreateFrom writes, and a reader must never need to know which was
	// chosen. If it leaked into open-time compatibility this would fail.
	collection, err := Open(reopened, Options{
		Collection: store.Options{ChunkDocuments: 8}, ResidentBytes: 8 << 20,
		Backend: BackendPortable,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = collection.Close()
		_ = reopened.Close()
	})
	return collection
}

func documentFormatChunkKinds(t *testing.T, collection *Collection) map[storeio.PageKind]int {
	t.Helper()
	state := collection.state.Load()
	kinds := map[storeio.PageKind]int{}
	err := storeio.WalkChunkTree(collection.cache, state.chunkRoot, storeio.ChunkTreeBounds{
		FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
	}, func(_ uint32, ref storeio.PageRef) error {
		kinds[ref.Kind]++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return kinds
}

// TestCreateFromDocumentFormatSelectsRepresentation pins both halves of
// Options.DocumentFormat: that it actually changes the physical representation
// CreateFrom emits, and that changing it changes nothing a reader can observe.
//
// The default matters here. CreateFrom used to emit the compact form
// unconditionally, which silently traded an order of magnitude of resident scan
// speed for space; the default is now verbatim, and a test that only checked
// "compact produces groups" would keep passing if the default flipped back.
func TestCreateFromDocumentFormatSelectsRepresentation(t *testing.T) {
	const count = 200
	dir := t.TempDir()
	source := documentFormatSource(t, count)

	verbatim := createFormatted(t, dir, DocumentFormatVerbatim, source)
	compact := createFormatted(t, dir, DocumentFormatCompact, source)

	verbatimKinds := documentFormatChunkKinds(t, verbatim)
	if verbatimKinds[storeio.PageDocumentGroup] != 0 {
		t.Fatalf("default CreateFrom emitted %d grouped extents, want none: %v",
			verbatimKinds[storeio.PageDocumentGroup], verbatimKinds)
	}
	if verbatimKinds[storeio.PageDocument] == 0 {
		t.Fatalf("default CreateFrom emitted no verbatim document extents: %v", verbatimKinds)
	}
	compactKinds := documentFormatChunkKinds(t, compact)
	if compactKinds[storeio.PageDocumentGroup] == 0 {
		t.Fatalf("DocumentFormatCompact emitted no grouped extents: %v", compactKinds)
	}

	// Every read surface must agree, row for row and byte for byte.
	verbatimSnapshot, err := verbatim.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer verbatimSnapshot.Close()
	compactSnapshot, err := compact.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer compactSnapshot.Close()

	type row struct{ key, value string }
	collect := func(snapshot *Snapshot) []row {
		var rows []row
		if err := snapshot.RangeRaw(func(key, value []byte) error {
			rows = append(rows, row{string(key), string(value)})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return rows
	}
	verbatimRows, compactRows := collect(verbatimSnapshot), collect(compactSnapshot)
	if len(verbatimRows) != count || len(compactRows) != count {
		t.Fatalf("scanned %d and %d rows, want %d each", len(verbatimRows), len(compactRows), count)
	}
	for i := range verbatimRows {
		if verbatimRows[i] != compactRows[i] {
			t.Fatalf("row %d: verbatim %+v, compact %+v", i, verbatimRows[i], compactRows[i])
		}
	}

	for i := range count {
		key := fmt.Sprintf("key-%05d", i)
		want, okWant, err := verbatimSnapshot.AppendRaw(nil, key)
		if err != nil || !okWant {
			t.Fatalf("verbatim AppendRaw(%q) = (%v, %v)", key, okWant, err)
		}
		got, okGot, err := compactSnapshot.AppendRaw(nil, key)
		if err != nil || !okGot {
			t.Fatalf("compact AppendRaw(%q) = (%v, %v)", key, okGot, err)
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("point read %q: verbatim %q, compact %q", key, want, got)
		}
	}
}

// TestCreateFromDocumentFormatRejectsUnknownValue keeps the option from being
// silently ignored, which for a field that selects an on-disk representation
// would mean writing a format the caller did not ask for.
func TestCreateFromDocumentFormatRejectsUnknownValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	source := documentFormatSource(t, 8)
	if _, err := CreateFrom(source, file, Options{
		Collection: store.Options{ChunkDocuments: 8}, ResidentBytes: 8 << 20,
		Backend: BackendPortable, DocumentFormat: DocumentFormatCompact + 1,
	}); err == nil {
		t.Fatal("CreateFrom accepted an out-of-range DocumentFormat")
	}
}
