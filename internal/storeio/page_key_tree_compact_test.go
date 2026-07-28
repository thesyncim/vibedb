package storeio

import (
	"errors"
	"testing"
)

func assertPageKeyTreeOccupancy(
	t *testing.T, h *pageKeyTreeHarness, root PageRef,
) (pages int, depth int) {
	t.Helper()
	h.bounds.FileEnd, h.bounds.NextLogicalID = h.fileEnd, h.nextID
	seenRefs := make(map[PageRef]struct{})
	seenLogicalIDs := make(map[uint64]PageRef)
	var walk func(PageRef, bool, uint8, uint64) int
	walk = func(ref PageRef, isRoot bool, expectedLevel uint8, expectedMax uint64) int {
		if _, exists := seenRefs[ref]; exists {
			t.Fatalf("duplicate or cyclic fingerprint reference %+v", ref)
		}
		seenRefs[ref] = struct{}{}
		if previous, exists := seenLogicalIDs[ref.LogicalID]; exists {
			t.Fatalf("duplicate fingerprint logical id %d in %+v and %+v",
				ref.LogicalID, previous, ref)
		}
		seenLogicalIDs[ref.LogicalID] = ref
		lease, view, err := acquirePageFingerprintDirectory(h.cache, ref, h.bounds)
		if err != nil {
			t.Fatal(err)
		}
		defer lease.Release()
		header := view.Header()
		if header.Level != expectedLevel || header.MaxHash != expectedMax {
			t.Fatalf("page range/level = (level=%d,max=%d), want (%d,%d)",
				header.Level, header.MaxHash, expectedLevel, expectedMax)
		}
		if !isRoot {
			if header.Level == 0 {
				entries := make([]PageKeyLocation, view.Len())
				for rank := range entries {
					entry, ok := view.LocationAt(rank)
					if !ok {
						t.Fatal("missing fingerprint leaf entry")
					}
					entries[rank] = entry
				}
				if body, minimum := pageKeyTreeLeafBodyBytes(entries),
					pageKeyTreeLeafMinimumBodyBytes(header.PageSize); body < minimum {
					t.Fatalf("underfull leaf body = %d, want >= %d", body, minimum)
				}
			} else if view.Len() < pageKeyTreeBranchMinimumChildren(header.PageSize) {
				t.Fatalf("underfull branch children = %d, want >= %d",
					view.Len(), pageKeyTreeBranchMinimumChildren(header.PageSize))
			}
		} else if header.Level != 0 && view.Len() == 1 {
			t.Fatal("root branch with one child was not collapsed")
		}
		pages++
		maxDepth := 1
		if header.Level != 0 {
			var previousChildMax uint64
			for rank := 0; rank < view.Len(); rank++ {
				child, ok := view.BranchAt(rank)
				if !ok {
					t.Fatal("missing fingerprint branch child")
				}
				childLease, childView, childErr := acquirePageFingerprintDirectory(
					h.cache, child.Child, h.bounds,
				)
				if childErr != nil {
					t.Fatal(childErr)
				}
				childHeader := childView.Header()
				childLease.Release()
				if childHeader.MaxHash != child.MaxHash {
					t.Fatalf("child maximum binding = %d, want %d",
						childHeader.MaxHash, child.MaxHash)
				}
				if rank == 0 {
					if childHeader.MinHash != header.MinHash {
						t.Fatalf("first child minimum = %d, want parent minimum %d",
							childHeader.MinHash, header.MinHash)
					}
				} else if previousChildMax > childHeader.MinHash {
					t.Fatalf("overlapping child ranges: previous max %d > next min %d",
						previousChildMax, childHeader.MinHash)
				}
				previousChildMax = childHeader.MaxHash
				childDepth := 1 + walk(child.Child, false, header.Level-1, child.MaxHash)
				if childDepth > maxDepth {
					maxDepth = childDepth
				}
			}
		}
		return maxDepth
	}
	lease, view, err := acquirePageFingerprintDirectory(h.cache, root, h.bounds)
	if err != nil {
		t.Fatal(err)
	}
	level := view.Header().Level
	maxHash := view.Header().MaxHash
	lease.Release()
	depth = walk(root, true, level, maxHash)
	return pages, depth
}

func collisionEntries(count int, hash uint64) []PageKeyLocation {
	entries := make([]PageKeyLocation, count)
	for index := range entries {
		entries[index] = PageKeyLocation{
			Hash: hash, Chunk: uint32(index / 64), Slot: uint8(index % 64),
		}
	}
	return entries
}

func TestPageKeyTreePointGrowthUsesFixedWidthBalancedSplit(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	entries := make([]PageKeyLocation, 247)
	for index := range entries {
		entries[index] = PageKeyLocation{
			Hash: uint64(index + 1), Chunk: uint32(index / 64),
			Slot: uint8(index % 64),
		}
	}
	if got := pageKeyTreeLeafBodyBytes(entries); got != 3952 {
		t.Fatalf("seed body = %d, want 3952", got)
	}
	inserted := PageKeyLocation{Hash: 248, Chunk: 4, Slot: 55}
	grown := append(append([]PageKeyLocation(nil), entries...), inserted)
	if got := pageKeyTreeLeafBodyBytes(grown); got != 3968 {
		t.Fatalf("grown body = %d, want 3968", got)
	}
	if split := pageKeyTreeLeafSplit(testSuperblockPageSize, grown); split != 124 {
		t.Fatalf("balanced split = %d, want 124", split)
	}

	tx := h.begin(4)
	leaf, err := encodePageKeyTreeLeaf(tx, 0, entries, PageRef{}, h.bounds)
	if err != nil {
		t.Fatal(err)
	}
	h.publish(tx, leaf.Ref())
	mutation, pages := h.mutate(pageKeyMutationInsert, inserted, 0)
	if !mutation.Changed || mutation.RetiredCount != 1 || pages != 3 {
		t.Fatalf("growth = (%+v,pages=%d), want two leaves and root", mutation, pages)
	}
	assertPageKeyTreeOccupancy(t, h, h.root)
	if got, ok := h.lookup(inserted); !ok || got != inserted {
		t.Fatalf("inserted entry = (%+v,%v)", got, ok)
	}

	lease, root, err := acquirePageFingerprintDirectory(h.cache, h.root, h.bounds)
	if err != nil {
		t.Fatal(err)
	}
	left, _ := root.BranchAt(0)
	right, _ := root.BranchAt(1)
	lease.Release()
	leftLease, leftView, err := acquirePageFingerprintDirectory(h.cache, left.Child, h.bounds)
	if err != nil {
		t.Fatal(err)
	}
	rightLease, rightView, err := acquirePageFingerprintDirectory(h.cache, right.Child, h.bounds)
	if err != nil {
		leftLease.Release()
		t.Fatal(err)
	}
	if leftView.Len() != 124 || rightView.Len() != 124 {
		leftLease.Release()
		rightLease.Release()
		t.Fatalf("child counts = (%d,%d), want (124,124)", leftView.Len(), rightView.Len())
	}
	leftLease.Release()
	rightLease.Release()
}

func TestPageKeyTreePointDeleteRedistributesEqualHashFixedWidthLeaves(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	const hash = uint64(0x51515151)
	entries := collisionEntries(249, hash)
	const split = 124
	h.seedCollisionLeaves(entries, split)
	oldRoot := h.root

	mutation, pages := h.mutate(pageKeyMutationDelete, entries[0], 0)
	if !mutation.Changed || mutation.RetiredCount != 3 || pages != 3 {
		t.Fatalf("redistribution = (%+v,pages=%d), want two leaves and root", mutation, pages)
	}
	assertPageKeyTreeOccupancy(t, h, h.root)

	lease, rootView, err := acquirePageFingerprintDirectory(h.cache, h.root, h.bounds)
	if err != nil {
		t.Fatal(err)
	}
	if rootView.Header().Level != 1 || rootView.Len() != 2 {
		lease.Release()
		t.Fatalf("redistributed root = (level=%d,len=%d), want (1,2)",
			rootView.Header().Level, rootView.Len())
	}
	leftRef, _, _ := rootView.ChildIndex(hash)
	leftRank := 0
	if leftRef == (PageRef{}) {
		lease.Release()
		t.Fatal("missing redistributed left child")
	}
	leftBranch, _ := rootView.BranchAt(leftRank)
	rightBranch, _ := rootView.BranchAt(1)
	lease.Release()
	leftLease, leftView, err := acquirePageFingerprintDirectory(h.cache, leftBranch.Child, h.bounds)
	if err != nil {
		t.Fatal(err)
	}
	rightLease, rightView, err := acquirePageFingerprintDirectory(h.cache, rightBranch.Child, h.bounds)
	if err != nil {
		leftLease.Release()
		t.Fatal(err)
	}
	leftBody := make([]PageKeyLocation, leftView.Len())
	rightBody := make([]PageKeyLocation, rightView.Len())
	for rank := range leftBody {
		leftBody[rank], _ = leftView.LocationAt(rank)
	}
	for rank := range rightBody {
		rightBody[rank], _ = rightView.LocationAt(rank)
	}
	leftLease.Release()
	rightLease.Release()
	difference := pageKeyTreeLeafBodyBytes(leftBody) - pageKeyTreeLeafBodyBytes(rightBody)
	if difference < 0 {
		difference = -difference
	}
	if difference > 32 {
		t.Fatalf("redistributed leaf body difference = %d, want byte-balanced", difference)
	}

	h.bounds.FileEnd, h.bounds.NextLogicalID = h.fileEnd, h.nextID
	if got, ok, err := LookupPageKeyTree(h.cache, oldRoot, entries[0], h.bounds); err != nil || !ok || got != entries[0] {
		t.Fatalf("old snapshot lookup = (%+v,%v,%v), want deleted preimage", got, ok, err)
	}
}

func TestPageKeyTreePointDeleteMergesAndCollapsesRoot(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	const hash = uint64(0x71717171)
	entries := collisionEntries(248, hash)
	const split = 124
	h.seedCollisionLeaves(entries, split)
	oldRoot := h.root

	mutation, pages := h.mutate(pageKeyMutationDelete, entries[0], 0)
	if !mutation.Changed || mutation.RetiredCount != 3 || pages != 1 {
		t.Fatalf("delete merge = (%+v,pages=%d), want one collapsed leaf", mutation, pages)
	}
	_, depth := assertPageKeyTreeOccupancy(t, h, h.root)
	if depth != 1 {
		t.Fatalf("collapsed tree depth = %d, want 1", depth)
	}
	if _, ok := h.lookup(entries[0]); ok {
		t.Fatal("deleted fingerprint remains after collapse")
	}
	h.bounds.FileEnd, h.bounds.NextLogicalID = h.fileEnd, h.nextID
	if got, ok, err := LookupPageKeyTree(h.cache, oldRoot, entries[0], h.bounds); err != nil || !ok || got != entries[0] {
		t.Fatalf("old snapshot lookup = (%+v,%v,%v), want %+v", got, ok, err, entries[0])
	}
}

func TestPageKeyTreePointDeletesNinetyPercentMaintainOccupancyAndReuse(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	entries := make([]PageKeyLocation, 260)
	edits := make([]PageKeyTreeEdit, len(entries))
	for index := range entries {
		entries[index] = PageKeyLocation{
			Hash: uint64(index + 1), Chunk: uint32(index / 64),
			Slot: uint8(index % 64),
		}
		edits[index] = PageKeyTreeEdit{
			Location: entries[index], Operation: PageKeyTreeInsert,
		}
	}
	h.mutateBatch(edits)
	oldRoot := h.root
	low, high := 0, len(entries)-1
	var last PageKeyTreeMutation
	for deleted := range 234 {
		index := low
		if deleted&1 != 0 {
			index = high
			high--
		} else {
			low++
		}
		mutation, pages := h.mutate(pageKeyMutationDelete, entries[index], 0)
		if !mutation.Changed || pages > int(mutation.RetiredCount) {
			t.Fatalf("edge delete %d = (%+v,pages=%d)", deleted, mutation, pages)
		}
		seen := make(map[PageRef]struct{}, mutation.RetiredCount)
		for retired := uint8(0); retired < mutation.RetiredCount; retired++ {
			ref := mutation.Retired[retired]
			if _, exists := seen[ref]; exists {
				t.Fatalf("edge delete %d retired %+v twice", deleted, ref)
			}
			seen[ref] = struct{}{}
		}
		assertPageKeyTreeOccupancy(t, h, h.root)
		last = mutation
	}
	for index := low; index <= high; index++ {
		if got, ok := h.lookup(entries[index]); !ok || got != entries[index] {
			t.Fatalf("90%% survivor %d = (%+v,%v)", index, got, ok)
		}
	}
	h.bounds.FileEnd, h.bounds.NextLogicalID = h.fileEnd, h.nextID
	if got, ok, err := LookupPageKeyTree(h.cache, oldRoot, entries[0], h.bounds); err != nil || !ok || got != entries[0] {
		t.Fatalf("old 90%% snapshot = (%+v,%v,%v)", got, ok, err)
	}

	reopened, err := NewPageCache(h.file, PageCacheOptions{
		PageSize:      int(testSuperblockPageSize),
		ResidentBytes: 64 * int64(testSuperblockPageSize),
		StoreID:       testStoreID, ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := LookupPageKeyTree(reopened, h.root, entries[low], h.bounds); err != nil || !ok {
		_ = reopened.Close()
		t.Fatalf("reopened survivor = (%v,%v)", ok, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	reusable := make([]FreeExtent, 0, last.RetiredCount)
	for index := uint8(0); index < last.RetiredCount; index++ {
		ref := last.Retired[index]
		reusable = append(reusable, FreeExtent{
			Offset: ref.Offset, Length: uint64(ref.Length),
			RetiredGeneration: h.generation,
		})
	}
	h.generation++
	tx, err := BeginWriteTransaction(h.committer, h.cache, 32, WriteTransactionOptions{
		StoreID: testStoreID, Generation: h.generation,
		PageSize: testSuperblockPageSize, FileEnd: h.fileEnd,
		NextLogicalID: h.nextID, Reusable: reusable,
		ReuseJournal: make([]ReuseEdit, 0, 32),
	})
	if err != nil {
		t.Fatal(err)
	}
	reusedMutation, err := DeletePageKeyTree(
		h.cache, tx, h.root, entries[low], h.bounds,
	)
	if err != nil {
		_ = tx.Abort()
		t.Fatal(err)
	}
	if len(tx.ReuseEdits()) == 0 {
		_ = tx.Abort()
		t.Fatal("point compaction did not reuse a reclaimed tree extent")
	}
	h.publish(tx, reusedMutation.Root)
	assertPageKeyTreeOccupancy(t, h, h.root)
}

func TestPageKeyTreePointDeleteCascadesBranchUnderflowAndCollapsesRoot(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	const leavesPerBranch = 50
	const branchCount = 2
	entriesPerLeaf := (pageKeyTreeLeafMinimumBodyBytes(testSuperblockPageSize) +
		PageKeyLeafEntrySize - 1) / PageKeyLeafEntrySize
	if entriesPerLeaf != 124 ||
		leavesPerBranch != pageKeyTreeBranchMinimumChildren(testSuperblockPageSize) {
		t.Fatalf("4 KiB minima = (leaf=%d,branch=%d), want (124,50)",
			entriesPerLeaf, pageKeyTreeBranchMinimumChildren(testSuperblockPageSize))
	}
	tx := h.begin(branchCount*leavesPerBranch + branchCount + 2)
	leafPages := make([]TransactionPage, branchCount*leavesPerBranch)
	leafEntries := make([][]PageKeyLocation, len(leafPages))
	ordinal := 0
	for index := range leafPages {
		page, err := tx.Allocate(PageFingerprintDirectory, testSuperblockPageSize, 0)
		if err != nil {
			t.Fatal(err)
		}
		leafPages[index] = page
		leafEntries[index] = make([]PageKeyLocation, entriesPerLeaf)
		for rank := range leafEntries[index] {
			leafEntries[index][rank] = PageKeyLocation{
				Hash: uint64(ordinal + 1), Chunk: uint32(ordinal / 64),
				Slot: uint8(ordinal % 64),
			}
			ordinal++
		}
	}
	branches := make([]TransactionPage, branchCount)
	for index := range branches {
		page, err := tx.Allocate(PageFingerprintDirectory, testSuperblockPageSize, 0)
		if err != nil {
			t.Fatal(err)
		}
		branches[index] = page
	}
	rootPage, err := tx.Allocate(PageFingerprintDirectory, testSuperblockPageSize, 0)
	if err != nil {
		t.Fatal(err)
	}
	for index, page := range leafPages {
		next := PageRef{}
		if index+1 < len(leafPages) {
			next = leafPages[index+1].Ref()
		}
		if err := encodePageKeyTreeLeafInto(
			tx, page, leafEntries[index], next, h.bounds,
		); err != nil {
			t.Fatal(err)
		}
	}
	rootChildren := make([]PageKeyBranch, branchCount)
	for branchIndex, page := range branches {
		first := branchIndex * leavesPerBranch
		children := make([]PageKeyBranch, leavesPerBranch)
		for rank := range children {
			leafIndex := first + rank
			entries := leafEntries[leafIndex]
			children[rank] = PageKeyBranch{
				MaxHash: entries[len(entries)-1].Hash,
				Child:   leafPages[leafIndex].Ref(),
			}
		}
		minHash := leafEntries[first][0].Hash
		header := pageKeyTreeHeader(tx, page.Ref(), 1)
		header.MinHash = minHash
		header.MaxHash = children[len(children)-1].MaxHash
		if _, err := EncodePageFingerprintBranch(
			page.Bytes(), header, children, tx.FileEnd(), tx.NextLogicalID(),
		); err != nil {
			t.Fatal(err)
		}
		if err := page.Stage(); err != nil {
			t.Fatal(err)
		}
		rootChildren[branchIndex] = PageKeyBranch{
			MaxHash: header.MaxHash, Child: page.Ref(),
		}
	}
	rootHeader := pageKeyTreeHeader(tx, rootPage.Ref(), 2)
	rootHeader.MinHash = leafEntries[0][0].Hash
	rootHeader.MaxHash = rootChildren[len(rootChildren)-1].MaxHash
	if _, err := EncodePageFingerprintBranch(
		rootPage.Bytes(), rootHeader, rootChildren, tx.FileEnd(), tx.NextLogicalID(),
	); err != nil {
		t.Fatal(err)
	}
	if err := rootPage.Stage(); err != nil {
		t.Fatal(err)
	}
	h.publish(tx, rootPage.Ref())
	oldRoot := h.root
	_, oldDepth := assertPageKeyTreeOccupancy(t, h, h.root)
	if oldDepth != 3 {
		t.Fatalf("seed tree depth = %d, want 3", oldDepth)
	}

	target := leafEntries[0][0]
	mutation, pages := h.mutate(pageKeyMutationDelete, target, 0)
	if !mutation.Changed || mutation.RetiredCount != 5 || pages != 2 {
		t.Fatalf("cascading delete = (%+v,pages=%d), want leaf+branch rewrite", mutation, pages)
	}
	_, depth := assertPageKeyTreeOccupancy(t, h, h.root)
	if depth != 2 {
		t.Fatalf("collapsed cascading depth = %d, want 2", depth)
	}
	if _, ok := h.lookup(target); ok {
		t.Fatal("cascading delete target remains")
	}
	h.bounds.FileEnd, h.bounds.NextLogicalID = h.fileEnd, h.nextID
	if got, ok, err := LookupPageKeyTree(h.cache, oldRoot, target, h.bounds); err != nil || !ok || got != target {
		t.Fatalf("old cascading snapshot = (%+v,%v,%v), want target", got, ok, err)
	}
}

func seedPointCompactionCorruptBranchSibling(
	t *testing.T, h *pageKeyTreeHarness, overlap, childMaxMismatch bool,
) PageKeyLocation {
	t.Helper()
	const leaves = 4
	const entriesPerLeaf = 124
	tx := h.begin(leaves + 4)
	leafPages := make([]TransactionPage, leaves)
	leafEntries := make([][]PageKeyLocation, leaves)
	ordinal := 0
	for index := range leafPages {
		page, err := tx.Allocate(PageFingerprintDirectory, testSuperblockPageSize, 0)
		if err != nil {
			t.Fatal(err)
		}
		leafPages[index] = page
		leafEntries[index] = make([]PageKeyLocation, entriesPerLeaf)
		for rank := range leafEntries[index] {
			leafEntries[index][rank] = PageKeyLocation{
				Hash: uint64(ordinal + 1), Chunk: uint32(ordinal / 64),
				Slot: uint8(ordinal % 64),
			}
			ordinal++
		}
	}
	for index, page := range leafPages {
		next := PageRef{}
		if index+1 < len(leafPages) {
			next = leafPages[index+1].Ref()
		}
		if err := encodePageKeyTreeLeafInto(
			tx, page, leafEntries[index], next, h.bounds,
		); err != nil {
			t.Fatal(err)
		}
	}
	branchPages := make([]TransactionPage, 2)
	for index := range branchPages {
		page, err := tx.Allocate(PageFingerprintDirectory, testSuperblockPageSize, 0)
		if err != nil {
			t.Fatal(err)
		}
		branchPages[index] = page
	}
	rootChildren := make([]PageKeyBranch, 2)
	leftMax := leafEntries[1][len(leafEntries[1])-1].Hash
	for branchIndex, page := range branchPages {
		first := branchIndex * 2
		children := []PageKeyBranch{
			{
				MaxHash: leafEntries[first][len(leafEntries[first])-1].Hash,
				Child:   leafPages[first].Ref(),
			},
			{
				MaxHash: leafEntries[first+1][len(leafEntries[first+1])-1].Hash,
				Child:   leafPages[first+1].Ref(),
			},
		}
		if branchIndex == 1 && childMaxMismatch {
			children[0].MaxHash--
		}
		minHash := leafEntries[first][0].Hash
		if branchIndex == 1 && overlap {
			minHash = leftMax - 1
		}
		header := pageKeyTreeHeader(tx, page.Ref(), 1)
		header.MinHash = minHash
		header.MaxHash = children[len(children)-1].MaxHash
		if _, err := EncodePageFingerprintBranch(
			page.Bytes(), header, children, tx.FileEnd(), tx.NextLogicalID(),
		); err != nil {
			t.Fatal(err)
		}
		if err := page.Stage(); err != nil {
			t.Fatal(err)
		}
		rootChildren[branchIndex] = PageKeyBranch{
			MaxHash: header.MaxHash, Child: page.Ref(),
		}
	}
	rootPage, err := tx.Allocate(PageFingerprintDirectory, testSuperblockPageSize, 0)
	if err != nil {
		t.Fatal(err)
	}
	rootHeader := pageKeyTreeHeader(tx, rootPage.Ref(), 2)
	rootHeader.MinHash = leafEntries[0][0].Hash
	rootHeader.MaxHash = rootChildren[1].MaxHash
	if _, err := EncodePageFingerprintBranch(
		rootPage.Bytes(), rootHeader, rootChildren, tx.FileEnd(), tx.NextLogicalID(),
	); err != nil {
		t.Fatal(err)
	}
	if err := rootPage.Stage(); err != nil {
		t.Fatal(err)
	}
	h.publish(tx, rootPage.Ref())
	return leafEntries[0][0]
}

func TestPageKeyTreePointCompactionRejectsCorruptBranchSibling(t *testing.T) {
	for _, test := range []struct {
		name             string
		overlap          bool
		childMaxMismatch bool
	}{
		{name: "overlapping node ranges", overlap: true},
		{name: "child maximum binding", childMaxMismatch: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newPageKeyTreeHarness(t)
			target := seedPointCompactionCorruptBranchSibling(
				t, h, test.overlap, test.childMaxMismatch,
			)
			oldRoot := h.root
			tx := h.begin(8)
			_, err := DeletePageKeyTree(h.cache, tx, h.root, target, h.bounds)
			if !errors.Is(err, ErrKeyDirectoryCorrupt) {
				_ = tx.Abort()
				t.Fatalf("corrupt sibling error = %v", err)
			}
			if err := tx.Abort(); err != nil {
				t.Fatal(err)
			}
			if h.root != oldRoot {
				t.Fatal("failed compaction changed the published root")
			}
			if got, ok := h.lookup(target); !ok || got != target {
				t.Fatalf("target after rejected compaction = (%+v,%v)", got, ok)
			}
		})
	}
}

func TestPageKeyTreeCompactRetirementCapacityIsExact(t *testing.T) {
	var mutation PageKeyTreeMutation
	for index := range mutation.Retired {
		ref := PageRef{
			Offset:    uint64(index+2) * uint64(testSuperblockPageSize),
			LogicalID: uint64(index + 2), Generation: 1,
			Length: testSuperblockPageSize, Kind: PageFingerprintDirectory,
		}
		if err := mutation.retire(ref); err != nil {
			t.Fatalf("retire %d: %v", index, err)
		}
	}
	if got, want := int(mutation.RetiredCount), 2*int(pageKeyDirectoryMaxLevel)+1; got != want {
		t.Fatalf("retirement capacity = %d, want %d", got, want)
	}
	if err := mutation.retire(mutation.Retired[0]); !errors.Is(err, ErrKeyDirectoryCorrupt) {
		t.Fatalf("duplicate retirement error = %v", err)
	}
}
