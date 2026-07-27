package storeio

import (
	"sort"
	"testing"
)

type pageKeyTreeBatchObservedLeaf struct {
	ref     PageRef
	parent  PageRef
	entries []PageKeyLocation
}

func TestPageKeyTreeBatchDeleteCompactsCrossParentRuns(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	leafCapacity := (int(testSuperblockPageSize) - PageHeaderSize - PageTrailerSize -
		PageKeyDirectoryPayloadHeaderSize) / PageKeyLeafEntrySize
	count := (pageKeyTreeBranchCapacity(testSuperblockPageSize) + 3) * leafCapacity
	build := make([]PageKeyTreeEdit, count)
	for index := range build {
		location := PageKeyLocation{
			Hash: uint64(index + 1), Chunk: uint32(index / 64), Slot: uint8(index % 64),
		}
		build[index] = PageKeyTreeEdit{Location: location, Operation: PageKeyTreeInsert}
	}
	built, _ := h.mutateBatch(build)
	if !built.Changed {
		t.Fatal("large batch build did not change the tree")
	}
	leaves, rootLevel := observePageKeyTreeBatchLeaves(t, h, h.root)
	if rootLevel < 2 {
		t.Fatalf("root level = %d, want cross-parent tree", rootLevel)
	}

	boundary := -1
	for index := 1; index < len(leaves); index++ {
		if leaves[index-1].parent != leaves[index].parent {
			boundary = index
			break
		}
	}
	if boundary < 1 {
		t.Fatal("no adjacent leaves with different old parents")
	}
	touched := []pageKeyTreeBatchObservedLeaf{leaves[boundary-1], leaves[boundary]}
	var edits []PageKeyTreeEdit
	var survivors []PageKeyLocation
	for _, leaf := range touched {
		keep := max(1, len(leaf.entries)/10)
		for _, location := range leaf.entries[:keep] {
			survivors = append(survivors, location)
		}
		for _, location := range leaf.entries[keep:] {
			edits = append(edits, PageKeyTreeEdit{
				Location: location, Operation: PageKeyTreeDelete,
			})
		}
	}
	sort.Slice(edits, func(left, right int) bool {
		return comparePageKeyLocation(edits[left].Location, edits[right].Location) < 0
	})

	oldRoot := h.root
	oldBounds := h.bounds
	oldBounds.FileEnd, oldBounds.NextLogicalID = h.fileEnd, h.nextID
	deletedProbe := PageKeyLocation{}
	for _, edit := range edits {
		if edit.Operation == PageKeyTreeDelete {
			deletedProbe = edit.Location
			break
		}
	}
	tx := h.begin(min(PageKeyTreeBatchPages(len(edits)), 1022))
	mutation, err := MutatePageKeyTreeBatch(
		h.cache, tx, h.root, edits, h.bounds, []PageRef{oldRoot},
	)
	if err != nil {
		_ = tx.Abort()
		t.Fatal(err)
	}
	metadataPages := tx.allocated
	if !mutation.Changed || mutation.Root == oldRoot {
		_ = tx.Abort()
		t.Fatalf("cross-parent delete mutation = %+v", mutation)
	}
	if len(mutation.Retired) < 2 || mutation.Retired[0] != oldRoot {
		_ = tx.Abort()
		t.Fatalf("retirement prefix = %+v, want old root first", mutation.Retired)
	}
	seen := make(map[PageRef]struct{}, len(mutation.Retired))
	for _, ref := range mutation.Retired {
		if _, duplicate := seen[ref]; duplicate {
			_ = tx.Abort()
			t.Fatalf("duplicate retirement %+v", ref)
		}
		seen[ref] = struct{}{}
	}
	if metadataPages >= len(mutation.Retired) {
		_ = tx.Abort()
		t.Fatalf("90%% delete allocated %d pages for %d retirements", metadataPages, len(mutation.Retired))
	}
	if metadataPages > PageKeyTreeBatchPages(len(edits)) {
		_ = tx.Abort()
		t.Fatalf("boundary-compaction pages = %d, reservation bound = %d",
			metadataPages, PageKeyTreeBatchPages(len(edits)))
	}
	allocatedRefs := pageKeyTreeBatchAllocatedRefs(t, tx)
	h.publish(tx, mutation.Root)

	reachableRefs := assertPageKeyTreeBatchOccupancy(t, h, h.root, h.generation)
	if len(reachableRefs) != metadataPages {
		t.Fatalf("reachable current-generation pages = %d, allocated = %d", len(reachableRefs), metadataPages)
	}
	for ref := range allocatedRefs {
		if _, reachable := reachableRefs[ref]; !reachable {
			t.Fatalf("allocated page is unreachable: %+v", ref)
		}
	}
	for ref := range reachableRefs {
		if _, allocated := allocatedRefs[ref]; !allocated {
			t.Fatalf("reachable current-generation page was not allocated: %+v", ref)
		}
	}
	for _, location := range edits {
		if location.Operation != PageKeyTreeDelete {
			continue
		}
		if _, ok := h.lookup(location.Location); ok {
			t.Fatalf("deleted location remains: %+v", location.Location)
		}
	}
	for _, location := range survivors {
		if got, ok := h.lookup(location); !ok || got != location {
			t.Fatalf("survivor = (%+v,%v), want %+v", got, ok, location)
		}
	}

	// Publication is COW: the old root retains every deleted identity under
	// the old immutable bounds.
	if got, ok, err := LookupPageKeyTree(
		h.cache, oldRoot, deletedProbe, oldBounds,
	); err != nil || !ok || got != deletedProbe {
		t.Fatalf("old snapshot lookup = (%+v,%v,%v), want %+v", got, ok, err, deletedProbe)
	}
}

func TestPageKeyTreeBatchEqualHashDeleteKeepsGlobalIdentityOrder(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	const hash = uint64(1) << 60
	const count = 4096
	entries := make([]PageKeyLocation, count)
	build := make([]PageKeyTreeEdit, count)
	for index := range entries {
		entries[index] = PageKeyLocation{
			Hash: hash, Chunk: uint32(index / 64), Slot: uint8(index % 64),
		}
		build[index] = PageKeyTreeEdit{Location: entries[index], Operation: PageKeyTreeInsert}
	}
	h.mutateBatch(build)
	collisionLeaves, collisionRootLevel := observePageKeyTreeBatchLeaves(t, h, h.root)

	edits := make([]PageKeyTreeEdit, 0, count*9/10)
	want := make([]PageKeyLocation, 0, count/10+1)
	for index, location := range entries {
		if index%10 == 0 {
			want = append(want, location)
		} else {
			edits = append(edits, PageKeyTreeEdit{
				Location: location, Operation: PageKeyTreeDelete,
			})
		}
	}
	var stats pageKeyTreeBatchPlanningStats
	plans, applied, err := planPageKeyTreeBatch(
		h.cache, h.root, edits, PageKeyTreeBounds{
			FileEnd: h.fileEnd, NextLogicalID: h.nextID,
			ChunkHighWater: h.bounds.ChunkHighWater,
			ChunkDocuments: h.bounds.ChunkDocuments,
		}, &stats,
	)
	if err != nil {
		t.Fatal(err)
	}
	if stats.HashGroups != 1 || stats.CandidateVisits != count ||
		stats.PageVisits != len(collisionLeaves)+int(collisionRootLevel) ||
		len(plans) != len(edits) || applied != len(edits) {
		t.Fatalf("linear collision plan = (groups=%d,candidates=%d,pages=%d,plans=%d,applied=%d), want (1,%d,%d,%d,%d)",
			stats.HashGroups, stats.CandidateVisits, stats.PageVisits, len(plans), applied,
			count, len(collisionLeaves)+int(collisionRootLevel), len(edits), len(edits))
	}
	mutation, pages := h.mutateBatch(edits)
	if !mutation.Changed || mutation.Applied != len(edits) ||
		pages == 0 || pages >= len(mutation.Retired) {
		t.Fatalf("equal-hash compaction = (%+v,pages=%d)", mutation, pages)
	}
	assertPageKeyTreeBatchOccupancy(t, h, h.root, h.generation)

	h.bounds.FileEnd, h.bounds.NextLogicalID = h.fileEnd, h.nextID
	cursor, err := OpenPageKeyTreeCursor(h.cache, h.root, hash, h.bounds)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	for index, expected := range want {
		got, ok, err := cursor.Next()
		if err != nil || !ok || got != expected {
			t.Fatalf("collision rank %d = (%+v,%v,%v), want %+v", index, got, ok, err, expected)
		}
	}
	if got, ok, err := cursor.Next(); err != nil || ok {
		t.Fatalf("collision tail = (%+v,%v,%v), want exhaustion", got, ok, err)
	}
}

func TestPageKeyTreeBatchAppliedCountsOnlyEffectiveEdits(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	existing := []PageKeyLocation{
		{Hash: 10, Chunk: 1, Slot: 1},
		{Hash: 20, Chunk: 2, Slot: 2},
	}
	h.mutateBatch([]PageKeyTreeEdit{
		{Location: existing[0], Operation: PageKeyTreeInsert},
		{Location: existing[1], Operation: PageKeyTreeInsert},
	})

	inserted := PageKeyLocation{Hash: 40, Chunk: 4, Slot: 4}
	edits := []PageKeyTreeEdit{
		{Location: existing[0], Operation: PageKeyTreeInsert},
		{Location: existing[1], Operation: PageKeyTreeDelete},
		{Location: PageKeyLocation{Hash: 30, Chunk: 3, Slot: 3}, Operation: PageKeyTreeDelete},
		{Location: inserted, Operation: PageKeyTreeInsert},
	}
	mutation, _ := h.mutateBatch(edits)
	if !mutation.Changed || mutation.Applied != 2 {
		t.Fatalf("mixed mutation = %+v, want Applied=2", mutation)
	}
	for _, want := range []PageKeyLocation{existing[0], inserted} {
		if got, ok := h.lookup(want); !ok || got != want {
			t.Fatalf("effective edit lookup = (%+v,%v), want %+v", got, ok, want)
		}
	}

	noops, pages := h.mutateBatch([]PageKeyTreeEdit{
		{Location: existing[0], Operation: PageKeyTreeInsert},
		{Location: PageKeyLocation{Hash: 35, Chunk: 3, Slot: 5}, Operation: PageKeyTreeDelete},
	})
	if noops.Changed || noops.Applied != 0 || pages != 0 {
		t.Fatalf("no-op mutation = (%+v,pages=%d)", noops, pages)
	}
}

func TestPageKeyTreeBatchReservationCoversDisjointSplitsAndRootPromotion(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	leafCapacity := (int(testSuperblockPageSize) - PageHeaderSize - PageTrailerSize -
		PageKeyDirectoryPayloadHeaderSize) / PageKeyLeafEntrySize
	branchCapacity := pageKeyTreeBranchCapacity(testSuperblockPageSize)
	count := leafCapacity * branchCapacity
	build := make([]PageKeyTreeEdit, count)
	for index := range build {
		build[index] = PageKeyTreeEdit{
			Location: PageKeyLocation{
				Hash:  uint64(2 * (index + 1)),
				Chunk: uint32(index / 64),
				Slot:  uint8(index % 64),
			},
			Operation: PageKeyTreeInsert,
		}
	}
	h.mutateBatch(build)
	leaves, rootLevel := observePageKeyTreeBatchLeaves(t, h, h.root)
	if rootLevel != 1 || len(leaves) != branchCapacity {
		t.Fatalf("adversarial seed = (level=%d,leaves=%d), want (1,%d)",
			rootLevel, len(leaves), branchCapacity)
	}

	edits := make([]PageKeyTreeEdit, len(leaves))
	for index, leaf := range leaves {
		if len(leaf.entries) != leafCapacity {
			t.Fatalf("seed leaf %d entries = %d, want full capacity %d",
				index, len(leaf.entries), leafCapacity)
		}
		middle := leaf.entries[len(leaf.entries)/2]
		edits[index] = PageKeyTreeEdit{
			Location: PageKeyLocation{
				Hash:  middle.Hash + 1,
				Chunk: uint32(900 + index/64),
				Slot:  uint8(index % 64),
			},
			Operation: PageKeyTreeInsert,
		}
	}
	sort.Slice(edits, func(left, right int) bool {
		return comparePageKeyLocation(edits[left].Location, edits[right].Location) < 0
	})
	mutation, pages := h.mutateBatch(edits)
	bound := PageKeyTreeBatchPages(len(edits))
	if !mutation.Changed || mutation.Applied != len(edits) || pages > bound {
		t.Fatalf("split/promotion mutation = (%+v,pages=%d,bound=%d)",
			mutation, pages, bound)
	}
	_, promotedLevel := observePageKeyTreeBatchLeaves(t, h, h.root)
	if promotedLevel != 2 {
		t.Fatalf("promoted root level = %d, want 2", promotedLevel)
	}
	assertPageKeyTreeBatchOccupancy(t, h, h.root, h.generation)
}

func observePageKeyTreeBatchLeaves(
	t testing.TB, h *pageKeyTreeHarness, root PageRef,
) ([]pageKeyTreeBatchObservedLeaf, uint8) {
	t.Helper()
	h.bounds.FileEnd, h.bounds.NextLogicalID = h.fileEnd, h.nextID
	var leaves []pageKeyTreeBatchObservedLeaf
	var walk func(PageRef, PageRef) uint8
	walk = func(ref, parent PageRef) uint8 {
		lease, view, err := acquirePageFingerprintDirectory(h.cache, ref, h.bounds)
		if err != nil {
			t.Fatal(err)
		}
		level := view.Header().Level
		if level == 0 {
			entries := make([]PageKeyLocation, view.Len())
			for index := range entries {
				var ok bool
				entries[index], ok = view.LocationAt(index)
				if !ok {
					lease.Release()
					t.Fatal(ErrKeyDirectoryCorrupt)
				}
			}
			leaves = append(leaves, pageKeyTreeBatchObservedLeaf{
				ref: ref, parent: parent, entries: entries,
			})
			lease.Release()
			return level
		}
		children := make([]PageRef, view.Len())
		for index := range children {
			entry, ok := view.BranchAt(index)
			if !ok {
				lease.Release()
				t.Fatal(ErrKeyDirectoryCorrupt)
			}
			children[index] = entry.Child
		}
		lease.Release()
		for _, child := range children {
			if childLevel := walk(child, ref); childLevel+1 != level {
				t.Fatalf("child level = %d below parent level %d", childLevel, level)
			}
		}
		return level
	}
	return leaves, walk(root, PageRef{})
}

func assertPageKeyTreeBatchOccupancy(
	t testing.TB, h *pageKeyTreeHarness, root PageRef, generation uint64,
) map[PageRef]struct{} {
	t.Helper()
	h.bounds.FileEnd, h.bounds.NextLogicalID = h.fileEnd, h.nextID
	currentPages := make(map[PageRef]struct{})
	visited := make(map[PageRef]struct{})
	logicalIDs := make(map[uint64]PageRef)
	var walk func(PageRef, bool)
	walk = func(ref PageRef, rootPage bool) {
		if _, duplicate := visited[ref]; duplicate {
			t.Fatalf("duplicate/cyclic reachable page: %+v", ref)
		}
		visited[ref] = struct{}{}
		if previous, duplicate := logicalIDs[ref.LogicalID]; duplicate {
			t.Fatalf("duplicate logical ID %d: %+v and %+v", ref.LogicalID, previous, ref)
		}
		logicalIDs[ref.LogicalID] = ref
		if ref.Generation == generation {
			currentPages[ref] = struct{}{}
		}
		lease, view, err := acquirePageFingerprintDirectory(h.cache, ref, h.bounds)
		if err != nil {
			t.Fatal(err)
		}
		level := view.Header().Level
		if level == 0 {
			entries := make([]PageKeyLocation, view.Len())
			for index := range entries {
				var ok bool
				entries[index], ok = view.LocationAt(index)
				if !ok {
					lease.Release()
					t.Fatal(ErrKeyDirectoryCorrupt)
				}
			}
			lease.Release()
			if !rootPage &&
				pageKeyTreeLeafBodyBytes(entries) < pageKeyTreeLeafMinimumBodyBytes(testSuperblockPageSize) {
				t.Fatalf("non-root leaf body = %d, minimum = %d",
					pageKeyTreeLeafBodyBytes(entries),
					pageKeyTreeLeafMinimumBodyBytes(testSuperblockPageSize))
			}
			return
		}
		if !rootPage && view.Len() < pageKeyTreeBranchMinimumChildren(testSuperblockPageSize) {
			count := view.Len()
			lease.Release()
			t.Fatalf("non-root branch children = %d, minimum = %d",
				count, pageKeyTreeBranchMinimumChildren(testSuperblockPageSize))
		}
		children := make([]PageRef, view.Len())
		for index := range children {
			entry, ok := view.BranchAt(index)
			if !ok {
				lease.Release()
				t.Fatal(ErrKeyDirectoryCorrupt)
			}
			children[index] = entry.Child
		}
		lease.Release()
		for _, child := range children {
			walk(child, false)
		}
	}
	walk(root, true)
	return currentPages
}

func pageKeyTreeBatchAllocatedRefs(
	t testing.TB, tx *WriteTransaction,
) map[PageRef]struct{} {
	t.Helper()
	refs := make(map[PageRef]struct{}, tx.allocated)
	logicalIDs := make(map[uint64]PageRef, tx.allocated)
	for index := 0; index < tx.allocated; index++ {
		write := tx.batch.pages[index]
		buffer, err := tx.batch.PageBuffer(index)
		if err != nil {
			t.Fatal(err)
		}
		header, _, err := OpenPage(buffer[:write.Length])
		if err != nil {
			t.Fatal(err)
		}
		ref := PageRef{
			Offset: uint64(write.Offset), LogicalID: header.LogicalID,
			Generation: header.Generation, Length: write.Length,
			Kind: header.Kind, Flags: header.Flags,
		}
		if _, duplicate := refs[ref]; duplicate {
			t.Fatalf("duplicate allocated ref: %+v", ref)
		}
		refs[ref] = struct{}{}
		if previous, duplicate := logicalIDs[ref.LogicalID]; duplicate {
			t.Fatalf("duplicate allocated logical ID %d: %+v and %+v",
				ref.LogicalID, previous, ref)
		}
		logicalIDs[ref.LogicalID] = ref
	}
	return refs
}
