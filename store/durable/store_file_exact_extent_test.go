package durable

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func exactExtentJSON(length int) []byte {
	const framing = `{"v":""}`
	if length < len(framing) {
		panic("exact-extent JSON fixture is too short")
	}
	return []byte(`{"v":"` + strings.Repeat("x", length-len(framing)) + `"}`)
}

func exactExtentRefs(
	t *testing.T,
	collection *Collection,
	key string,
) (storeio.PageRef, storeio.PageRef) {
	t.Helper()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	state := snapshot.state
	match, found, err := collection.resolveFileFingerprint(state, []byte(key))
	if err != nil || !found {
		t.Fatalf("resolveFileFingerprint(%q) = (%+v,%v,%v)", key, match, found, err)
	}
	defer match.Release()
	document := match.documentRef
	if document.Kind != storeio.PageDocument {
		t.Fatalf("%q document kind = %v, want %v",
			key, document.Kind, storeio.PageDocument)
	}
	return document, match.value.value.Overflow
}

func assertExactExtentValue(
	t *testing.T,
	collection *Collection,
	key string,
	want []byte,
) {
	t.Helper()
	got, found, err := collection.AppendRaw(nil, key)
	if err != nil || !found || !bytes.Equal(got, want) {
		t.Fatalf("AppendRaw(%q) = (%d bytes,%v,%v), want %d bytes",
			key, len(got), found, err, len(want))
	}
}

func TestFileStorePrimaryDocumentsUseExactQuantumExtents(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-exact-document-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Collection.ChunkDocuments = 1
	options.InlineValueBytes = 32 << 10
	options.MaxDocumentBytes = 40 << 10
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		key        string
		valueBytes int
		extent     uint32
	}{
		{key: "doc-12", valueBytes: 9000, extent: 12 << 10},
		{key: "doc-20", valueBytes: 17000, extent: 20 << 10},
		{key: "doc-28", valueBytes: 25000, extent: 28 << 10},
	}
	values := make([][]byte, len(tests))
	for i, test := range tests {
		values[i] = exactExtentJSON(test.valueBytes)
		if created, putErr := collection.Put(test.key, values[i]); putErr != nil || !created {
			t.Fatalf("Put(%q) = (%v,%v)", test.key, created, putErr)
		}
		document, overflow := exactExtentRefs(t, collection, test.key)
		if document.Length != test.extent || overflow != (storeio.PageRef{}) {
			t.Fatalf("%q refs = document %+v overflow %+v, want %d-byte inline document",
				test.key, document, overflow, test.extent)
		}
		assertExactExtentValue(t, collection, test.key, values[i])
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for i, test := range tests {
		document, overflow := exactExtentRefs(t, reopened, test.key)
		if document.Length != test.extent || overflow != (storeio.PageRef{}) {
			t.Fatalf("reopened %q refs = document %+v overflow %+v",
				test.key, document, overflow)
		}
		assertExactExtentValue(t, reopened, test.key, values[i])
	}
}

func TestFileStoreExactOverflowExtentsRetireAndReuse(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-fs-exact-overflow-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	options.Collection.ChunkDocuments = 1
	options.MaxRetiredExtents = 1024
	values := [][]byte{
		exactExtentJSON(9000),
		exactExtentJSON(17000),
		exactExtentJSON(25000),
	}
	wantLengths := []uint32{12 << 10, 20 << 10, 28 << 10}

	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if created, putErr := collection.Put("hot", values[0]); putErr != nil || !created {
		t.Fatalf("initial Put = (%v,%v)", created, putErr)
	}
	document, firstOverflow := exactExtentRefs(t, collection, "hot")
	if document.Length != uint32(options.PageSize) ||
		firstOverflow.Length != wantLengths[0] {
		t.Fatalf("initial refs = document %+v overflow %+v", document, firstOverflow)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	collection, err = Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if created, putErr := collection.Put("hot", values[1]); putErr != nil || created {
		t.Fatalf("20KiB update = (%v,%v)", created, putErr)
	}
	_, secondOverflow := exactExtentRefs(t, collection, "hot")
	if secondOverflow.Length != wantLengths[1] ||
		secondOverflow.Offset == firstOverflow.Offset {
		t.Fatalf("second overflow = %+v, first %+v", secondOverflow, firstOverflow)
	}
	assertExactExtentValue(t, collection, "hot", values[1])
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	collection, err = Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if created, putErr := collection.Put("hot", values[2]); putErr != nil || created {
		t.Fatalf("28KiB update = (%v,%v)", created, putErr)
	}
	_, thirdOverflow := exactExtentRefs(t, collection, "hot")
	if thirdOverflow.Length != wantLengths[2] ||
		thirdOverflow.Offset == firstOverflow.Offset {
		t.Fatalf("third overflow = %+v, first %+v", thirdOverflow, firstOverflow)
	}
	assertExactExtentValue(t, collection, "hot", values[2])

	beforeReuse := collection.state.Load().super.FileEnd
	if created, putErr := collection.Put("reuse", values[0]); putErr != nil || !created {
		t.Fatalf("reuse Put = (%v,%v)", created, putErr)
	}
	_, reusedOverflow := exactExtentRefs(t, collection, "reuse")
	if reusedOverflow.Length != wantLengths[0] ||
		reusedOverflow.Offset+uint64(reusedOverflow.Length) > beforeReuse {
		t.Fatalf("reused overflow = %+v, pre-mutation file end %d; "+
			"retired exact extent was not reused", reusedOverflow, beforeReuse)
	}
	assertExactExtentValue(t, collection, "hot", values[2])
	assertExactExtentValue(t, collection, "reuse", values[0])
	t.Logf("retired overflow at %d reused below prior file end %d as %+v",
		firstOverflow.Offset, beforeReuse, reusedOverflow)
}

func TestFileStoreBulkVerbatimUsesExactPrimaryExtents(t *testing.T) {
	options := testFileStoreOptions()
	options.Collection = store.Options{ChunkDocuments: 1}
	options.MaxDocumentBytes = 96 << 10
	builder, err := store.NewBuilder(options.Collection)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		key        string
		valueBytes int
		extent     uint32
	}{
		{key: "bulk-12", valueBytes: 9000, extent: 12 << 10},
		{key: "bulk-20", valueBytes: 17000, extent: 20 << 10},
		{key: "bulk-28", valueBytes: 25000, extent: 28 << 10},
	}
	values := make([][]byte, len(tests))
	for i, test := range tests {
		values[i] = exactExtentJSON(test.valueBytes)
		if err := builder.Append(test.key, values[i]); err != nil {
			t.Fatal(err)
		}
	}
	overflowValue := exactExtentJSON(80 << 10)
	if err := builder.Append("bulk-overflow", overflowValue); err != nil {
		t.Fatal(err)
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "file-fs-bulk-exact-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := CreateFrom(source, file, options); err != nil {
		t.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	for i, test := range tests {
		document, overflow := exactExtentRefs(t, collection, test.key)
		if document.Length != test.extent || overflow != (storeio.PageRef{}) {
			t.Fatalf("%q bulk refs = document %+v overflow %+v, want %d-byte document",
				test.key, document, overflow, test.extent)
		}
		assertExactExtentValue(t, collection, test.key, values[i])
	}
	document, overflow := exactExtentRefs(t, collection, "bulk-overflow")
	if document.Length != uint32(options.PageSize) ||
		overflow.Length != uint32(options.MaxPageSize) {
		t.Fatalf("bulk overflow head = document %+v overflow %+v",
			document, overflow)
	}
	state := collection.state.Load()
	lease, err := collection.cache.Acquire(overflow)
	if err != nil {
		t.Fatal(err)
	}
	view, err := storeio.OpenOverflowPage(
		lease.Page(), state.super.FileEnd, state.root.NextLogicalID,
		state.root.PageSize, state.root.ChunkHighWater,
		uint8(state.root.ChunkDocuments),
	)
	next := view.Header().Next
	lease.Release()
	if err != nil {
		t.Fatal(err)
	}
	if next.Length != 20<<10 {
		t.Fatalf("bulk overflow exact tail = %+v, want 20KiB", next)
	}
	assertExactExtentValue(t, collection, "bulk-overflow", overflowValue)

	for _, test := range []struct {
		required int
		want     uint32
	}{
		{required: 9000, want: 12 << 10},
		{required: 17000, want: 20 << 10},
		{required: 25000, want: 28 << 10},
	} {
		got, ok := fileStoreBulkPrimaryExtent(
			test.required, options.PageSize, options.MaxPageSize,
		)
		if !ok || got != test.want {
			t.Fatalf("fileStoreBulkPrimaryExtent(%d) = (%d,%v), want (%d,true)",
				test.required, got, ok, test.want)
		}
	}
}
