package store

import (
	"runtime"
	"testing"
)

var storePackedIndexSink uint64

func testStorePackedIndexPending() map[uint64][]storeIndexChunkMask {
	pending := map[uint64][]storeIndexChunkMask{
		3: {
			{chunk: 2, mask: 1},
			{chunk: 9, mask: 3},
		},
		99: {{chunk: 7, mask: 1 << 63}},
	}
	long := make([]storeIndexChunkMask, 2500)
	for i := range long {
		long[i] = storeIndexChunkMask{chunk: uint32(10 + i*2), mask: uint64(1) << uint((i*17)&63)}
	}
	pending[41] = long
	return pending
}

func testStorePackedIndex(t *testing.T) (*storePackedIndex, map[uint64][]storeIndexChunkMask) {
	t.Helper()
	pending := testStorePackedIndexPending()
	packed, err := newStorePackedIndex(pending)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		runtime.SetFinalizer(packed, nil)
		packed.release()
	})
	return packed, pending
}

func TestStorePackedIndexRoundTripContinuationAndAllocation(t *testing.T) {
	packed, pending := testStorePackedIndex(t)
	if len(packed.views) < 2 || len(packed.refs) != len(pending) {
		t.Fatalf("packed pages/refs = %d/%d, want multiple/%d", len(packed.views), len(packed.refs), len(pending))
	}
	for i := 1; i < len(packed.refs); i++ {
		if packed.refs[i-1].hash >= packed.refs[i].hash {
			t.Fatalf("refs not strictly sorted at %d", i)
		}
	}
	for hash, want := range pending {
		got := make([]storeIndexChunkMask, 0, len(want))
		if complete := packed.each(hash, func(chunk uint32, mask uint64) bool {
			got = append(got, storeIndexChunkMask{chunk: chunk, mask: mask})
			return true
		}); !complete {
			t.Fatalf("packed.each(%d) stopped early", hash)
		}
		if len(got) != len(want) {
			t.Fatalf("packed.each(%d) length = %d, want %d", hash, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("packed.each(%d)[%d] = %+v, want %+v", hash, i, got[i], want[i])
			}
		}
	}
	if _, ok := packed.lookup(1000); ok {
		t.Fatal("absent packed hash hit")
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if complete := packed.each(41, func(chunk uint32, mask uint64) bool {
			storePackedIndexSink ^= uint64(chunk) ^ mask
			return true
		}); !complete {
			panic("packed iteration stopped")
		}
	}); allocs != 0 {
		t.Fatalf("packed lookup/continuation allocations = %g, want 0", allocs)
	}
}

func TestStorePackedIndexDeltaShadowsWholeChunk(t *testing.T) {
	builder, err := NewBuilder(Options{ChunkDocuments: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.CreateIndex(IndexDefinition{Name: "v", Paths: []string{"/v"}}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if err := builder.Append(string(rune('a'+i)), []byte(`{"v":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	collection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	before, _ := collection.Snapshot()
	baseIndex, ok := before.exactIndex("v")
	if !ok || baseIndex.base == nil || baseIndex.root != nil || baseIndex.dirty.root != nil {
		t.Fatalf("builder did not publish packed-only base: %+v", baseIndex)
	}
	if _, err := collection.Put("b", []byte(`{"v":2}`)); err != nil {
		t.Fatal(err)
	}
	after, _ := collection.Snapshot()
	changed, ok := after.exactIndex("v")
	if !ok || changed.base != baseIndex.base || changed.root == nil || changed.dirty.get(0) == 0 {
		t.Fatalf("mutation did not publish bounded base delta: %+v", changed)
	}
	oldKeys, err := before.AppendIndexRawKeys(nil, "v", []byte(`1`))
	if err != nil || len(oldKeys) != 8 {
		t.Fatalf("old packed snapshot = (%v,%v)", oldKeys, err)
	}
	one, err := after.AppendIndexRawKeys(nil, "v", []byte(`1`))
	if err != nil || len(one) != 7 {
		t.Fatalf("shadowed base value = (%v,%v)", one, err)
	}
	two, err := after.AppendIndexRawKeys(nil, "v", []byte(`2`))
	if err != nil || len(two) != 1 || two[0] != "b" {
		t.Fatalf("delta value = (%v,%v)", two, err)
	}
	for i := 1; i < len(one); i++ {
		if one[i-1] >= one[i] {
			t.Fatalf("merged result order = %v", one)
		}
	}
	stats := collection.Stats()
	if changed.base.externalBytes() != 0 && stats.ExternalIndexBytes == 0 {
		t.Fatal("external packed index base not reported")
	}
}

func TestStoreIndexMasksNextMatchesOrderedTraversal(t *testing.T) {
	entries := []storeIndexChunkMask{
		{chunk: 0, mask: 1},
		{chunk: 31, mask: 2},
		{chunk: 32, mask: 4},
		{chunk: 1023, mask: 8},
		{chunk: 1024, mask: 16},
		{chunk: 1 << 20, mask: 32},
		{chunk: ^uint32(0), mask: 64},
	}
	masks := storeIndexMasksFromSorted(entries)
	for i, want := range entries {
		gotChunk, gotMask, ok := masks.next(uint64(want.chunk))
		if !ok || gotChunk != want.chunk || gotMask != want.mask {
			t.Fatalf("next(%d) = (%d,%d,%v), want (%d,%d,true)", want.chunk, gotChunk, gotMask, ok, want.chunk, want.mask)
		}
		if i > 0 {
			gotChunk, gotMask, ok = masks.next(uint64(entries[i-1].chunk) + 1)
			if !ok || gotChunk != want.chunk || gotMask != want.mask {
				t.Fatalf("next(after %d) = (%d,%d,%v), want (%d,%d,true)", entries[i-1].chunk, gotChunk, gotMask, ok, want.chunk, want.mask)
			}
		}
	}
	if _, _, ok := masks.next(uint64(^uint32(0)) + 1); ok {
		t.Fatal("next past uint32 address space unexpectedly hit")
	}
}

func TestStoreOnlineBackfillFoldsPackedBase(t *testing.T) {
	collection := &Collection{Options: Options{ChunkDocuments: 2}}
	for i := 0; i < 10; i++ {
		if _, err := collection.Put(string(rune('a'+i)), []byte(`{"nested":{"v":1}}`)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := collection.CreateIndex(IndexDefinition{Name: "v", Paths: []string{"/nested/v"}}); err != nil {
		t.Fatal(err)
	}
	info, err := collection.BackfillIndex("v", 0)
	if err != nil || info.State != IndexReady {
		t.Fatalf("BackfillIndex = (%+v,%v)", info, err)
	}
	snap34, _ := collection.Snapshot()
	index, ok := snap34.exactIndex("v")
	if !ok || index.base == nil || index.root != nil || index.dirty.root != nil {
		t.Fatalf("ready online index was not folded: %+v", index)
	}
	snap33, _ := collection.Snapshot()
	if keys, err := snap33.AppendIndexRawKeys(nil, "v", []byte(`1`)); err != nil || len(keys) != 10 {
		t.Fatalf("packed online result = (%v,%v)", keys, err)
	}
	if _, err := collection.Put("a", []byte(`{"nested":{"v":2}}`)); err != nil {
		t.Fatal(err)
	}
	snap32, _ := collection.Snapshot()
	index, _ = snap32.exactIndex("v")
	if index.base == nil || index.root == nil || index.dirty.get(0) == 0 {
		t.Fatalf("online packed mutation did not create bounded delta: %+v", index)
	}
}
