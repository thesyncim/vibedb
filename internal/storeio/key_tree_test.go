package storeio

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

type keyTreeHarness struct {
	t          testing.TB
	file       *os.File
	committer  *Committer
	cache      *PageCache
	root       PageRef
	fileEnd    uint64
	nextID     uint64
	generation uint64
	bounds     KeyTreeBounds
}

func newKeyTreeHarness(t testing.TB) *keyTreeHarness {
	return newKeyTreeHarnessPages(t, 20, 32)
}

// newKeyTreeHarnessPages sizes the committer for one transaction. Batch
// descents publish many more pages per generation than a single-key upsert, so
// their tests must reserve for the whole batch rather than one root-to-leaf
// path.
func newKeyTreeHarnessPages(t testing.TB, maxPages, buffers int) *keyTreeHarness {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "key-tree-*")
	if err != nil {
		t.Fatal(err)
	}
	committer, err := NewCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: buffers, BufferSize: os.Getpagesize(),
	}, CommitterOptions{QueueSlots: 4, MaxPagesPerBatch: maxPages, GroupLimit: 2})
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: int(testSuperblockPageSize), ResidentBytes: 64 * int64(testSuperblockPageSize),
		StoreID: testStoreID, ReadConcurrency: 2,
	})
	if err != nil {
		committer.Close()
		file.Close()
		t.Fatal(err)
	}
	h := &keyTreeHarness{
		t: t, file: file, committer: committer, cache: cache,
		fileEnd: testMutableStoreDataStart(testSuperblockPageSize), nextID: 2,
		bounds: KeyTreeBounds{ChunkHighWater: 1024, ChunkDocuments: 64},
	}
	t.Cleanup(func() {
		if err := h.committer.Close(); err != nil {
			t.Errorf("committer Close: %v", err)
		}
		h.cache.MarkDurable(h.committer.DurableGeneration())
		if err := h.cache.Close(); err != nil {
			t.Errorf("cache Close: %v", err)
		}
		if err := h.file.Close(); err != nil {
			t.Errorf("file Close: %v", err)
		}
	})
	return h
}

func (h *keyTreeHarness) mutate(key string, location KeyLocation, deleting bool) KeyTreeMutation {
	h.t.Helper()
	h.generation++
	tx, err := BeginWriteTransaction(h.committer, h.cache, 20, WriteTransactionOptions{
		StoreID: testStoreID, Generation: h.generation, PageSize: testSuperblockPageSize,
		FileEnd: h.fileEnd, NextLogicalID: h.nextID,
	})
	if err != nil {
		h.t.Fatal(err)
	}
	h.bounds.FileEnd = h.fileEnd
	h.bounds.NextLogicalID = h.nextID
	var mutation KeyTreeMutation
	if deleting {
		mutation, err = DeleteKeyTree(h.cache, tx, h.root, []byte(key), h.bounds)
	} else {
		mutation, err = UpsertKeyTree(h.cache, tx, h.root, []byte(key), location, h.bounds)
	}
	if err != nil {
		_ = tx.Abort()
		h.t.Fatal(err)
	}
	statePage, err := tx.Allocate(PageStateRoot, testSuperblockPageSize, StateRootLogicalID)
	if err != nil {
		_ = tx.Abort()
		h.t.Fatal(err)
	}
	state := StateRoot{
		StoreID: testStoreID, Generation: h.generation, PageSize: testSuperblockPageSize,
		MaxPageSize: 64 << 10, NextLogicalID: tx.NextLogicalID(), ChunkDocuments: 64,
	}
	if _, err := EncodeStateRootPage(statePage.Bytes(), state, tx.FileEnd()); err != nil {
		_ = tx.Abort()
		h.t.Fatal(err)
	}
	if err := statePage.Stage(); err != nil {
		_ = tx.Abort()
		h.t.Fatal(err)
	}
	if err := tx.Publish(statePage.Ref(), PageChecksum(statePage.Bytes()), 0, 0, 0); err != nil {
		_ = tx.Abort()
		h.t.Fatal(err)
	}
	if err := h.committer.Wait(h.generation); err != nil {
		h.t.Fatal(err)
	}
	h.cache.MarkDurable(h.generation)
	h.root = mutation.Root
	h.fileEnd = tx.FileEnd()
	h.nextID = tx.NextLogicalID()
	return mutation
}

func (h *keyTreeHarness) lookup(key string) (KeyLocation, bool) {
	h.t.Helper()
	h.bounds.FileEnd = h.fileEnd
	h.bounds.NextLogicalID = h.nextID
	location, ok, err := LookupKeyTree(h.cache, h.root, []byte(key), h.bounds)
	if err != nil {
		h.t.Fatal(err)
	}
	return location, ok
}

func TestKeyTreeCopyOnWriteSplitUpdateDeleteAndCollapse(t *testing.T) {
	h := newKeyTreeHarness(t)
	keys := make([]string, 12)
	locations := make([]KeyLocation, len(keys))
	for i := range keys {
		keys[i] = fmt.Sprintf("%02d-%s", i, strings.Repeat(string(rune('a'+i)), 900))
		locations[i] = KeyLocation{Chunk: uint32(i), Slot: uint8(i)}
		mutation := h.mutate(keys[i], locations[i], false)
		if mutation.Found || !mutation.Changed || mutation.Root == (PageRef{}) {
			t.Fatalf("insert %d mutation = %+v", i, mutation)
		}
		for j := 0; j <= i; j++ {
			got, ok := h.lookup(keys[j])
			if !ok || got != locations[j] {
				t.Fatalf("after insert %d Lookup(%d) = (%+v,%v), want (%+v,true)", i, j, got, ok, locations[j])
			}
		}
	}
	rootLease, err := h.cache.Acquire(h.root)
	if err != nil {
		t.Fatal(err)
	}
	rootView, err := OpenKeyDirectoryPage(rootLease.Page(), h.fileEnd, h.nextID, h.bounds.ChunkHighWater, h.bounds.ChunkDocuments)
	if err != nil {
		t.Fatal(err)
	}
	if rootView.Header().Level == 0 || rootView.Len() < 2 {
		t.Fatalf("tree did not split: header=%+v len=%d", rootView.Header(), rootView.Len())
	}
	rootLease.Release()

	updated := KeyLocation{Chunk: 900, Slot: 63}
	mutation := h.mutate(keys[5], updated, false)
	if !mutation.Found || !mutation.Changed || mutation.RetiredCount < 2 {
		t.Fatalf("update mutation = %+v", mutation)
	}
	if got, ok := h.lookup(keys[5]); !ok || got != updated {
		t.Fatalf("updated Lookup = (%+v,%v), want (%+v,true)", got, ok, updated)
	}

	missing := h.mutate("missing", KeyLocation{}, true)
	if missing.Found || missing.Changed || missing.RetiredCount != 0 || missing.Root != h.root {
		t.Fatalf("missing delete mutation = %+v", missing)
	}

	for i := 0; i < len(keys)-1; i++ {
		mutation = h.mutate(keys[i], KeyLocation{}, true)
		if !mutation.Found || !mutation.Changed {
			t.Fatalf("delete %d mutation = %+v", i, mutation)
		}
		if _, ok := h.lookup(keys[i]); ok {
			t.Fatalf("deleted key %d still present", i)
		}
	}
	rootLease, err = h.cache.Acquire(h.root)
	if err != nil {
		t.Fatal(err)
	}
	rootView, err = OpenKeyDirectoryPage(rootLease.Page(), h.fileEnd, h.nextID, h.bounds.ChunkHighWater, h.bounds.ChunkDocuments)
	if err != nil {
		t.Fatal(err)
	}
	if rootView.Header().Level != 0 || rootView.Len() != 1 {
		t.Fatalf("root did not collapse to leaf: header=%+v len=%d", rootView.Header(), rootView.Len())
	}
	rootLease.Release()

	last := h.mutate(keys[len(keys)-1], KeyLocation{}, true)
	if !last.Found || !last.Changed || last.Root != (PageRef{}) || h.root != (PageRef{}) {
		t.Fatalf("final delete = %+v root=%+v", last, h.root)
	}
}

func TestKeyTreeRejectsOversizeKeyWithoutPublishing(t *testing.T) {
	h := newKeyTreeHarness(t)
	h.generation++
	tx, err := BeginWriteTransaction(h.committer, h.cache, 4, WriteTransactionOptions{
		StoreID: testStoreID, Generation: h.generation, PageSize: testSuperblockPageSize,
		FileEnd: h.fileEnd, NextLogicalID: h.nextID,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.bounds.FileEnd, h.bounds.NextLogicalID = h.fileEnd, h.nextID
	_, err = UpsertKeyTree(h.cache, tx, PageRef{}, []byte(strings.Repeat("x", 4096)), KeyLocation{}, h.bounds)
	if err == nil {
		t.Fatal("oversize key succeeded")
	}
	if abortErr := tx.Abort(); abortErr != nil {
		t.Fatal(abortErr)
	}
	if stats := h.cache.Stats(); stats.DirtyBytes != 0 {
		t.Fatalf("aborted mutation retained dirty pages: %+v", stats)
	}
}

func TestKeyTreeWarmLookupSteadyAllocation(t *testing.T) {
	h := newKeyTreeHarness(t)
	key := "allocation-key"
	keyBytes := []byte(key)
	want := KeyLocation{Chunk: 7, Slot: 3}
	h.mutate(key, want, false)
	h.bounds.FileEnd, h.bounds.NextLogicalID = h.fileEnd, h.nextID
	if allocs := testing.AllocsPerRun(1000, func() {
		got, ok, err := LookupKeyTree(h.cache, h.root, keyBytes, h.bounds)
		if err != nil || !ok || got != want {
			panic("key lookup")
		}
	}); allocs != 0 {
		t.Fatalf("warm key-tree lookup allocations = %g, want 0", allocs)
	}
}
