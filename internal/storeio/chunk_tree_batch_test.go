package storeio

import (
	"math/rand"
	"slices"
	"sort"
	"testing"
)

type chunkTreeBatchAction struct {
	chunk   uint32
	delete  bool
	hasZone bool
	zone    ChunkZone
}

func (h *chunkTreeHarness) batchMutate(
	actions []chunkTreeBatchAction,
	maxPages int,
) ChunkTreeBatchMutation {
	h.t.Helper()
	h.generation++
	tx, err := BeginWriteTransaction(h.committer, h.cache, maxPages, WriteTransactionOptions{
		StoreID: testStoreID, Generation: h.generation, PageSize: testSuperblockPageSize,
		FileEnd: h.fileEnd, NextLogicalID: h.nextID,
	})
	if err != nil {
		h.t.Fatal(err)
	}
	h.bounds = ChunkTreeBounds{FileEnd: h.fileEnd, NextLogicalID: h.nextID}
	edits := make([]ChunkTreeEdit, len(actions))
	for i, action := range actions {
		edits[i] = ChunkTreeEdit{
			Chunk: action.chunk, Delete: action.delete,
			HasZone: action.hasZone, Zone: action.zone,
		}
		if action.delete {
			continue
		}
		logicalID := uint64(0)
		if old := h.documents[action.chunk]; old != (PageRef{}) {
			logicalID = old.LogicalID
		}
		document, allocateErr := tx.Allocate(
			PageDocument, testSuperblockPageSize, logicalID,
		)
		if allocateErr != nil {
			_ = tx.Abort()
			h.t.Fatal(allocateErr)
		}
		header := DocumentPageHeader{
			StoreID: testStoreID, Generation: h.generation,
			LogicalID: document.Ref().LogicalID, PageSize: testSuperblockPageSize,
			ChunkID: action.chunk, Live: 1,
		}
		row := [1]DocumentRecord{{
			Slot: 0, Key: []byte("k"), JSON: []byte("1"),
		}}
		if _, encodeErr := EncodeDocumentPage(
			document.Bytes(), header, row[:], tx.NextLogicalID(),
		); encodeErr != nil {
			_ = tx.Abort()
			h.t.Fatal(encodeErr)
		}
		if stageErr := document.Stage(); stageErr != nil {
			_ = tx.Abort()
			h.t.Fatal(stageErr)
		}
		edits[i].Document = document.Ref()
	}
	mutation, err := MutateChunkTreeBatch(
		h.cache, tx, h.root, edits, h.bounds, nil,
	)
	if err != nil {
		_ = tx.Abort()
		h.t.Fatal(err)
	}
	statePage, err := tx.Allocate(
		PageStateRoot, testSuperblockPageSize, StateRootLogicalID,
	)
	if err != nil {
		_ = tx.Abort()
		h.t.Fatal(err)
	}
	state := StateRoot{
		StoreID: testStoreID, Generation: h.generation,
		PageSize: testSuperblockPageSize, NextLogicalID: tx.NextLogicalID(),
		MaxPageSize: 64 << 10, ChunkDocuments: 64,
	}
	if _, err := EncodeStateRootPage(
		statePage.Bytes(), state, tx.FileEnd(),
	); err != nil {
		_ = tx.Abort()
		h.t.Fatal(err)
	}
	if err := statePage.Stage(); err != nil {
		_ = tx.Abort()
		h.t.Fatal(err)
	}
	if err := tx.Publish(
		statePage.Ref(), PageChecksum(statePage.Bytes()), 0, 0, 0,
	); err != nil {
		_ = tx.Abort()
		h.t.Fatal(err)
	}
	if err := h.committer.Wait(h.generation); err != nil {
		h.t.Fatal(err)
	}
	h.cache.MarkDurable(h.generation)
	h.root, h.fileEnd, h.nextID = mutation.Root, tx.FileEnd(), tx.NextLogicalID()
	for i, action := range actions {
		if action.delete {
			delete(h.documents, action.chunk)
		} else {
			h.documents[action.chunk] = edits[i].Document
		}
	}
	return mutation
}

func TestChunkTreeBatchMatchesSequentialMutations(t *testing.T) {
	random := rand.New(rand.NewSource(20260725))
	sequential := newChunkTreeHarnessPages(t, 256, 512)
	batched := newChunkTreeHarnessPages(t, 256, 512)
	live := make(map[uint32]struct{})
	edge := []uint32{
		0, 1, 63, 64, 65, 4095, 4096, 4097,
		1<<24 - 1, 1 << 24, ^uint32(0) - 1,
	}
	for round := range 20 {
		count := 1 + random.Intn(16)
		actions := make([]chunkTreeBatchAction, 0, count)
		seen := make(map[uint32]struct{}, count)
		existing := make([]uint32, 0, len(live))
		for chunk := range live {
			existing = append(existing, chunk)
		}
		for len(actions) < count {
			chunk := random.Uint32()
			if chunk == ^uint32(0) {
				chunk--
			}
			switch random.Intn(3) {
			case 0:
				chunk = edge[random.Intn(len(edge))]
			case 1:
				if len(existing) != 0 {
					chunk = existing[random.Intn(len(existing))]
				}
			}
			if _, duplicate := seen[chunk]; duplicate {
				continue
			}
			seen[chunk] = struct{}{}
			_, present := live[chunk]
			actions = append(actions, chunkTreeBatchAction{
				chunk: chunk, delete: present && random.Intn(2) == 0,
			})
		}
		sort.Slice(actions, func(i, j int) bool {
			return actions[i].chunk < actions[j].chunk
		})
		for _, action := range actions {
			sequential.mutate(action.chunk, action.delete)
			if action.delete {
				delete(live, action.chunk)
			} else {
				live[action.chunk] = struct{}{}
			}
		}
		batched.batchMutate(actions, 256)
		for chunk := range live {
			sequentialRef, sequentialOK := sequential.lookup(chunk)
			batchedRef, batchedOK := batched.lookup(chunk)
			if !sequentialOK || sequentialRef != sequential.documents[chunk] {
				t.Fatalf(
					"round %d sequential lookup %d = (%+v,%v)",
					round, chunk, sequentialRef, sequentialOK,
				)
			}
			if !batchedOK || batchedRef != batched.documents[chunk] {
				t.Fatalf(
					"round %d batched lookup %d = (%+v,%v)",
					round, chunk, batchedRef, batchedOK,
				)
			}
		}
		for chunk := range seen {
			if _, present := live[chunk]; present {
				continue
			}
			if _, ok := batched.lookup(chunk); ok {
				t.Fatalf("round %d deleted chunk %d still present", round, chunk)
			}
		}
	}
}

func TestChunkTreeBatchRewritesSharedPathsOnce(t *testing.T) {
	const count = 128
	sequential := newChunkTreeHarnessPages(t, 256, 512)
	batched := newChunkTreeHarnessPages(t, 256, 512)
	for chunk := range uint32(count) {
		sequential.mutate(chunk, false)
		batched.mutate(chunk, false)
	}

	sequentialRetired := 0
	actions := make([]chunkTreeBatchAction, count)
	for chunk := range uint32(count) {
		sequentialRetired += int(
			sequential.mutate(chunk, false).RetiredCount,
		)
		actions[chunk] = chunkTreeBatchAction{chunk: chunk}
	}
	mutation := batched.batchMutate(actions, 256)
	if sequentialRetired != 2*count || len(mutation.Retired) != 3 {
		t.Fatalf(
			"directory retirements = batch %d sequential %d, want 3 and %d",
			len(mutation.Retired), sequentialRetired, 2*count,
		)
	}
	if len(mutation.Retired) >= sequentialRetired/8 {
		t.Fatalf(
			"batch retired %d directory pages, sequential retired %d",
			len(mutation.Retired), sequentialRetired,
		)
	}
	if len(mutation.Retired) != len(
		slices.Compact(slices.Clone(mutation.Retired)),
	) {
		t.Fatalf("batch retired a directory page twice: %+v", mutation.Retired)
	}
	for chunk := range uint32(count) {
		got, ok := batched.lookup(chunk)
		if !ok || got != batched.documents[chunk] {
			t.Fatalf("lookup %d = (%+v,%v)", chunk, got, ok)
		}
	}
}

func TestChunkTreeBatchPreservesAndReplacesZones(t *testing.T) {
	h := newChunkTreeHarnessPages(t, 128, 256)
	zone := func(value byte) ChunkZone {
		var result ChunkZone
		result[0] = value
		return result
	}
	h.batchMutate([]chunkTreeBatchAction{
		{chunk: 0, hasZone: true, zone: zone(1)},
		{chunk: 1, hasZone: true, zone: zone(2)},
		{chunk: 64, hasZone: true, zone: zone(3)},
	}, 128)
	h.batchMutate([]chunkTreeBatchAction{
		{chunk: 0, delete: true},
		{chunk: 1, hasZone: true, zone: zone(4)},
		{chunk: 2},
		{chunk: 64, hasZone: true, zone: zone(5)},
	}, 128)

	want := map[uint32]ChunkZone{
		1:  zone(4),
		2:  {},
		64: zone(5),
	}
	for chunk, expected := range want {
		ref, got, ok, err := LookupChunkTreeDocumentZone(
			h.cache, h.root, chunk, ChunkTreeBounds{
				FileEnd: h.fileEnd, NextLogicalID: h.nextID,
			},
		)
		if err != nil || !ok || ref != h.documents[chunk] || got != expected {
			t.Fatalf(
				"chunk %d entry = (%+v,%+v,%v,%v), want (%+v,%+v,true,nil)",
				chunk, ref, got, ok, err, h.documents[chunk], expected,
			)
		}
	}
	if _, got, ok, err := LookupChunkTreeDocumentZone(
		h.cache, h.root, 0, ChunkTreeBounds{
			FileEnd: h.fileEnd, NextLogicalID: h.nextID,
		},
	); err != nil || ok || got != (ChunkZone{}) {
		t.Fatalf("deleted chunk zone = (%+v,%v,%v)", got, ok, err)
	}

	// A zone-disabled replacement clears only its lane. The neighbouring
	// summary stays in the same leaf and remains usable.
	h.batchMutate([]chunkTreeBatchAction{{chunk: 1}}, 128)
	for chunk, expected := range map[uint32]ChunkZone{
		1:  {},
		2:  {},
		64: zone(5),
	} {
		got, err := LookupChunkTreeZone(
			h.cache, h.root, chunk, ChunkTreeBounds{
				FileEnd: h.fileEnd, NextLogicalID: h.nextID,
			},
		)
		if err != nil || got != expected {
			t.Fatalf(
				"chunk %d zone after reset = (%+v,%v), want %+v",
				chunk, got, err, expected,
			)
		}
	}
}

func TestChunkTreeBatchRejectsUnsortedAndInvalidEdits(t *testing.T) {
	h := newChunkTreeHarnessPages(t, 64, 128)
	h.mutate(1, false)
	cases := map[string][]ChunkTreeEdit{
		"unsorted": {
			{Chunk: 2, Delete: true},
			{Chunk: 1, Delete: true},
		},
		"duplicate": {
			{Chunk: 2, Delete: true},
			{Chunk: 2, Delete: true},
		},
		"invalid document": {
			{Chunk: 2},
		},
	}
	for name, edits := range cases {
		t.Run(name, func(t *testing.T) {
			h.generation++
			tx, err := BeginWriteTransaction(
				h.committer, h.cache, 16, WriteTransactionOptions{
					StoreID: testStoreID, Generation: h.generation,
					PageSize: testSuperblockPageSize, FileEnd: h.fileEnd,
					NextLogicalID: h.nextID,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			h.bounds = ChunkTreeBounds{
				FileEnd: h.fileEnd, NextLogicalID: h.nextID,
			}
			_, err = MutateChunkTreeBatch(
				h.cache, tx, h.root, edits, h.bounds, nil,
			)
			_ = tx.Abort()
			h.generation--
			if err == nil {
				t.Fatalf("%s batch accepted", name)
			}
		})
	}
}
