package storeio

import (
	"os"
	"testing"
)

type chunkTreeHarness struct {
	t          *testing.T
	file       *os.File
	committer  *Committer
	cache      *PageCache
	root       PageRef
	fileEnd    uint64
	nextID     uint64
	generation uint64
	bounds     ChunkTreeBounds
	documents  map[uint32]PageRef
}

func newChunkTreeHarness(t *testing.T) *chunkTreeHarness {
	return newChunkTreeHarnessPages(t, 12, 20)
}

func newChunkTreeHarnessPages(t *testing.T, maxPages, buffers int) *chunkTreeHarness {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "chunk-tree-*")
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
		PageSize:      int(testSuperblockPageSize),
		ResidentBytes: int64(max(64, buffers)) * int64(testSuperblockPageSize),
		StoreID:       testStoreID, ReadConcurrency: 2,
	})
	if err != nil {
		committer.Close()
		file.Close()
		t.Fatal(err)
	}
	h := &chunkTreeHarness{
		t: t, file: file, committer: committer, cache: cache,
		fileEnd: testMutableStoreDataStart(testSuperblockPageSize), nextID: 2,
		documents: make(map[uint32]PageRef),
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

func (h *chunkTreeHarness) mutate(chunkID uint32, deleting bool) ChunkTreeMutation {
	h.t.Helper()
	h.generation++
	tx, err := BeginWriteTransaction(h.committer, h.cache, 12, WriteTransactionOptions{
		StoreID: testStoreID, Generation: h.generation, PageSize: testSuperblockPageSize,
		FileEnd: h.fileEnd, NextLogicalID: h.nextID,
	})
	if err != nil {
		h.t.Fatal(err)
	}
	h.bounds = ChunkTreeBounds{FileEnd: h.fileEnd, NextLogicalID: h.nextID}
	var mutation ChunkTreeMutation
	if deleting {
		mutation, err = DeleteChunkTree(h.cache, tx, h.root, chunkID, h.bounds)
	} else {
		logicalID := uint64(0)
		if old := h.documents[chunkID]; old != (PageRef{}) {
			logicalID = old.LogicalID
		}
		document, allocateErr := tx.Allocate(PageDocument, testSuperblockPageSize, logicalID)
		if allocateErr != nil {
			_ = tx.Abort()
			h.t.Fatal(allocateErr)
		}
		header := DocumentPageHeader{
			StoreID: testStoreID, Generation: h.generation, LogicalID: document.Ref().LogicalID,
			PageSize: testSuperblockPageSize, ChunkID: chunkID, Live: 1,
		}
		row := [1]DocumentRecord{{Slot: 0, Key: []byte("k"), JSON: []byte("1")}}
		if _, encodeErr := EncodeDocumentPage(document.Bytes(), header, row[:], tx.NextLogicalID()); encodeErr != nil {
			_ = tx.Abort()
			h.t.Fatal(encodeErr)
		}
		if stageErr := document.Stage(); stageErr != nil {
			_ = tx.Abort()
			h.t.Fatal(stageErr)
		}
		mutation, err = UpsertChunkTree(h.cache, tx, h.root, chunkID, document.Ref(), h.bounds)
		if err == nil {
			h.documents[chunkID] = document.Ref()
		}
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
	h.root, h.fileEnd, h.nextID = mutation.Root, tx.FileEnd(), tx.NextLogicalID()
	if deleting && mutation.Found {
		delete(h.documents, chunkID)
	}
	return mutation
}

func (h *chunkTreeHarness) lookup(chunkID uint32) (PageRef, bool) {
	h.t.Helper()
	h.bounds = ChunkTreeBounds{FileEnd: h.fileEnd, NextLogicalID: h.nextID}
	ref, ok, err := LookupChunkTree(h.cache, h.root, chunkID, h.bounds)
	if err != nil {
		h.t.Fatal(err)
	}
	return ref, ok
}

func TestChunkTreeCopyOnWriteAcrossRadixLevels(t *testing.T) {
	h := newChunkTreeHarness(t)
	ids := []uint32{0, 1, 63, 64, 4096, 1<<24 + 3, ^uint32(0) - 1}
	for i, id := range ids {
		mutation := h.mutate(id, false)
		if mutation.Found || !mutation.Changed || mutation.Root == (PageRef{}) {
			t.Fatalf("insert %d mutation = %+v", id, mutation)
		}
		for _, present := range ids[:i+1] {
			got, ok := h.lookup(present)
			if !ok || got != h.documents[present] {
				t.Fatalf("after insert %d Lookup(%d) = (%+v,%v), want (%+v,true)", id, present, got, ok, h.documents[present])
			}
		}
	}
	old := h.documents[64]
	update := h.mutate(64, false)
	if !update.Found || !update.Changed || update.RetiredCount != 6 || h.documents[64] == old {
		t.Fatalf("update mutation = %+v old=%+v new=%+v", update, old, h.documents[64])
	}
	if got, ok := h.lookup(64); !ok || got != h.documents[64] {
		t.Fatalf("updated Lookup = (%+v,%v)", got, ok)
	}
	missing := h.mutate(1234567, true)
	if missing.Found || missing.Changed || missing.Root != h.root {
		t.Fatalf("missing delete = %+v", missing)
	}
	for _, id := range ids {
		mutation := h.mutate(id, true)
		if !mutation.Found || !mutation.Changed {
			t.Fatalf("delete %d = %+v", id, mutation)
		}
		if _, ok := h.lookup(id); ok {
			t.Fatalf("deleted chunk %d still present", id)
		}
	}
	if h.root != (PageRef{}) {
		t.Fatalf("final root = %+v, want zero", h.root)
	}
}

func TestChunkTreeWarmLookupSteadyAllocation(t *testing.T) {
	h := newChunkTreeHarness(t)
	h.mutate(4096, false)
	want := h.documents[4096]
	h.bounds = ChunkTreeBounds{FileEnd: h.fileEnd, NextLogicalID: h.nextID}
	if allocs := testing.AllocsPerRun(1000, func() {
		got, ok, err := LookupChunkTree(h.cache, h.root, 4096, h.bounds)
		if err != nil || !ok || got != want {
			panic("chunk lookup")
		}
	}); allocs != 0 {
		t.Fatalf("warm chunk-tree lookup allocations = %g, want 0", allocs)
	}
}
