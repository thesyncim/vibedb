package storeio

import (
	"errors"
	"os"
	"testing"
)

type pageKeyTreeHarness struct {
	t          testing.TB
	file       *os.File
	committer  *Committer
	cache      *PageCache
	root       PageRef
	fileEnd    uint64
	nextID     uint64
	generation uint64
	bounds     PageKeyTreeBounds
}

func newPageKeyTreeHarness(t testing.TB) *pageKeyTreeHarness {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "page-key-tree-*")
	if err != nil {
		t.Fatal(err)
	}
	committer, err := NewCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 2048, BufferSize: os.Getpagesize(),
	}, CommitterOptions{QueueSlots: 4, MaxPagesPerBatch: 1024, GroupLimit: 2})
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: int(testSuperblockPageSize), ResidentBytes: 256 * int64(testSuperblockPageSize),
		StoreID: testStoreID, ReadConcurrency: 2,
	})
	if err != nil {
		committer.Close()
		file.Close()
		t.Fatal(err)
	}
	h := &pageKeyTreeHarness{
		t: t, file: file, committer: committer, cache: cache,
		fileEnd: testMutableStoreDataStart(testSuperblockPageSize), nextID: 2,
		bounds: PageKeyTreeBounds{ChunkHighWater: 1024, ChunkDocuments: 64},
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

func (h *pageKeyTreeHarness) begin(maxPages int) *WriteTransaction {
	h.t.Helper()
	h.generation++
	tx, err := BeginWriteTransaction(h.committer, h.cache, maxPages, WriteTransactionOptions{
		StoreID: testStoreID, Generation: h.generation, PageSize: testSuperblockPageSize,
		FileEnd: h.fileEnd, NextLogicalID: h.nextID,
	})
	if err != nil {
		h.t.Fatal(err)
	}
	h.bounds.FileEnd = h.fileEnd
	h.bounds.NextLogicalID = h.nextID
	return tx
}

func (h *pageKeyTreeHarness) publish(tx *WriteTransaction, root PageRef) {
	h.t.Helper()
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
	h.root = root
	h.fileEnd = tx.FileEnd()
	h.nextID = tx.NextLogicalID()
}

func (h *pageKeyTreeHarness) mutate(
	operation pageKeyMutationOperation, expected PageKeyLocation, _ int64,
) (PageKeyTreeMutation, int) {
	h.t.Helper()
	tx := h.begin(32)
	var mutation PageKeyTreeMutation
	var err error
	switch operation {
	case pageKeyMutationInsert:
		mutation, err = InsertPageKeyTree(h.cache, tx, h.root, expected, h.bounds)
	case pageKeyMutationDelete:
		mutation, err = DeletePageKeyTree(h.cache, tx, h.root, expected, h.bounds)
	default:
		err = ErrInvalidWrite
	}
	if err != nil {
		_ = tx.Abort()
		h.t.Fatal(err)
	}
	pages := tx.allocated
	h.publish(tx, mutation.Root)
	return mutation, pages
}

func (h *pageKeyTreeHarness) mutateBatch(
	edits []PageKeyTreeEdit,
) (PageKeyTreeBatchMutation, int) {
	h.t.Helper()
	// The harness committer deliberately caps a generation at 1,024 pages.
	// Large batches below use far fewer in practice; the separate reservation
	// test pins the conservative public upper bound without requiring that much
	// test-only buffer memory.
	reservation := min(PageKeyTreeBatchPages(len(edits)), 1022)
	tx := h.begin(reservation + 1)
	mutation, err := MutatePageKeyTreeBatch(
		h.cache, tx, h.root, edits, h.bounds, nil,
	)
	if err != nil {
		_ = tx.Abort()
		h.t.Fatal(err)
	}
	pages := tx.allocated
	h.publish(tx, mutation.Root)
	return mutation, pages
}

func (h *pageKeyTreeHarness) lookup(target PageKeyLocation) (PageKeyLocation, bool) {
	h.t.Helper()
	h.bounds.FileEnd, h.bounds.NextLogicalID = h.fileEnd, h.nextID
	location, ok, err := LookupPageKeyTree(h.cache, h.root, target, h.bounds)
	if err != nil {
		h.t.Fatal(err)
	}
	return location, ok
}

func (h *pageKeyTreeHarness) seedCollisionLeaves(entries []PageKeyLocation, split int) {
	h.t.Helper()
	if split <= 0 || split >= len(entries) {
		h.t.Fatal("invalid collision split")
	}
	tx := h.begin(8)
	left, err := tx.Allocate(PageFingerprintDirectory, testSuperblockPageSize, 0)
	if err != nil {
		h.t.Fatal(err)
	}
	right, err := tx.Allocate(PageFingerprintDirectory, testSuperblockPageSize, 0)
	if err != nil {
		h.t.Fatal(err)
	}
	root, err := tx.Allocate(PageFingerprintDirectory, testSuperblockPageSize, 0)
	if err != nil {
		h.t.Fatal(err)
	}
	rightHeader := pageKeyTreeHeader(tx, right.Ref(), 0)
	rightHeader.MinHash = entries[split].Hash
	rightHeader.MaxHash = entries[len(entries)-1].Hash
	if _, err := EncodePageFingerprintLeaf(
		right.Bytes(), rightHeader, entries[split:], tx.FileEnd(), tx.NextLogicalID(),
		h.bounds.ChunkHighWater, h.bounds.ChunkDocuments,
	); err != nil {
		h.t.Fatal(err)
	}
	if err := right.Stage(); err != nil {
		h.t.Fatal(err)
	}
	leftHeader := pageKeyTreeHeader(tx, left.Ref(), 0)
	leftHeader.MinHash = entries[0].Hash
	leftHeader.MaxHash = entries[split-1].Hash
	leftHeader.Next = right.Ref()
	if _, err := EncodePageFingerprintLeaf(
		left.Bytes(), leftHeader, entries[:split], tx.FileEnd(), tx.NextLogicalID(),
		h.bounds.ChunkHighWater, h.bounds.ChunkDocuments,
	); err != nil {
		h.t.Fatal(err)
	}
	if err := left.Stage(); err != nil {
		h.t.Fatal(err)
	}
	rootHeader := pageKeyTreeHeader(tx, root.Ref(), 1)
	rootHeader.MinHash = entries[0].Hash
	rootHeader.MaxHash = entries[len(entries)-1].Hash
	if _, err := EncodePageFingerprintBranch(
		root.Bytes(), rootHeader, []PageKeyBranch{
			{MaxHash: entries[split-1].Hash, Child: left.Ref()},
			{MaxHash: entries[len(entries)-1].Hash, Child: right.Ref()},
		}, tx.FileEnd(), tx.NextLogicalID(),
	); err != nil {
		h.t.Fatal(err)
	}
	if err := root.Stage(); err != nil {
		h.t.Fatal(err)
	}
	h.publish(tx, root.Ref())
}

func TestPageKeyTreePointInsertDuplicateAndDelete(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	entries := []PageKeyLocation{
		{Hash: 10, Chunk: 1, Slot: 1},
		{Hash: 20, Chunk: 2, Slot: 2},
		{Hash: 30, Chunk: 3, Slot: 3},
		{Hash: 5, Chunk: 4, Slot: 4},
	}
	for index, entry := range entries {
		mutation, pages := h.mutate(pageKeyMutationInsert, entry, 0)
		if mutation.Found || !mutation.Changed || pages == 0 {
			t.Fatalf("insert %d = (%+v,pages=%d)", index, mutation, pages)
		}
		for _, want := range entries[:index+1] {
			if got, ok := h.lookup(want); !ok || got != want {
				t.Fatalf("lookup after insert = (%+v,%v), want %+v", got, ok, want)
			}
		}
	}

	duplicate, pages := h.mutate(pageKeyMutationInsert, entries[1], 0)
	if !duplicate.Found || duplicate.Changed || pages != 0 {
		t.Fatalf("duplicate insert = (%+v,pages=%d)", duplicate, pages)
	}

	missing := PageKeyLocation{Hash: 999, Chunk: 9, Slot: 9}
	deleted, pages := h.mutate(pageKeyMutationDelete, missing, 0)
	if deleted.Found || deleted.Changed || pages != 0 || deleted.Root != h.root {
		t.Fatalf("missing delete = (%+v,pages=%d)", deleted, pages)
	}
	deleted, pages = h.mutate(pageKeyMutationDelete, entries[1], 0)
	if !deleted.Found || !deleted.Changed || pages != 1 {
		t.Fatalf("delete = (%+v,pages=%d)", deleted, pages)
	}
	if _, ok := h.lookup(entries[1]); ok {
		t.Fatal("deleted fingerprint remains")
	}
}

func TestPageKeyTreeCollisionCursorSurvivesCrossLeafCOW(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	const hash = uint64(0xfeedface)
	leafCapacity := (int(testSuperblockPageSize) - PageHeaderSize - PageTrailerSize -
		PageKeyDirectoryPayloadHeaderSize) / PageKeyLeafEntrySize
	entries := make([]PageKeyLocation, leafCapacity+53)
	for index := range entries {
		entries[index] = PageKeyLocation{
			Hash: hash, Chunk: uint32(index * 2), Slot: 0,
		}
	}
	h.seedCollisionLeaves(entries, leafCapacity)
	h.bounds.FileEnd, h.bounds.NextLogicalID = h.fileEnd, h.nextID
	first, ok, err := FirstPageKeyTreeCandidate(h.cache, h.root, hash, h.bounds)
	if err != nil || !ok || first != entries[0] {
		t.Fatalf("first collision candidate = (%+v,%v,%v), want %+v", first, ok, err, entries[0])
	}
	if _, ok, err := FirstPageKeyTreeCandidate(h.cache, h.root, hash+1, h.bounds); err != nil || ok {
		t.Fatalf("missing first candidate = (%v,%v), want clean miss", ok, err)
	}

	// Identity order selects the last collision leaf for a new maximum. That
	// leaf has spare capacity, so only it and the root are copied.
	target := PageKeyLocation{Hash: hash, Chunk: 700, Slot: 0}
	mutation, pages := h.mutate(pageKeyMutationInsert, target, 0)
	if mutation.Found || !mutation.Changed || pages != 2 {
		t.Fatalf("cross-leaf insert = (%+v,pages=%d)", mutation, pages)
	}
	if got, ok := h.lookup(target); !ok || got != target {
		t.Fatalf("cross-leaf lookup = (%+v,%v), want %+v", got, ok, target)
	}

	// The first immutable leaf still links to the superseded second leaf. A
	// cursor following Header.Next would miss the new identity. The current-root
	// parent cursor must return it exactly once.
	cursor, err := OpenPageKeyTreeCursor(h.cache, h.root, hash, PageKeyTreeBounds{
		FileEnd: h.fileEnd, NextLogicalID: h.nextID,
		ChunkHighWater: h.bounds.ChunkHighWater, ChunkDocuments: h.bounds.ChunkDocuments,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	count := 0
	targetCount := 0
	for {
		location, ok, err := cursor.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		count++
		if samePageKeyIdentity(location, target) {
			targetCount++
		}
	}
	if count != len(entries)+1 || targetCount != 1 {
		t.Fatalf("collision enumeration = (count=%d,target=%d), want (%d,1)",
			count, targetCount, len(entries)+1)
	}

	// A new identity between the first two entries selects the full first leaf
	// and therefore exercises the actual split path.
	inserted := PageKeyLocation{Hash: hash, Chunk: 1, Slot: 0}
	mutation, pages = h.mutate(pageKeyMutationInsert, inserted, 0)
	if mutation.Found || !mutation.Changed || pages != 3 {
		t.Fatalf("full collision-leaf split = (%+v,pages=%d)", mutation, pages)
	}
	if got, ok := h.lookup(inserted); !ok || got != inserted {
		t.Fatalf("inserted collision lookup = (%+v,%v), want %+v", got, ok, inserted)
	}
	ordered, err := OpenPageKeyTreeCursor(h.cache, h.root, hash, PageKeyTreeBounds{
		FileEnd: h.fileEnd, NextLogicalID: h.nextID,
		ChunkHighWater: h.bounds.ChunkHighWater, ChunkDocuments: h.bounds.ChunkDocuments,
	})
	if err != nil {
		t.Fatal(err)
	}
	var previous PageKeyLocation
	havePrevious := false
	orderedCount := 0
	for {
		location, ok, nextErr := ordered.Next()
		if nextErr != nil {
			ordered.Close()
			t.Fatal(nextErr)
		}
		if !ok {
			break
		}
		if havePrevious && comparePageKeyLocation(previous, location) >= 0 {
			ordered.Close()
			t.Fatalf("collision identities out of global order: %+v then %+v", previous, location)
		}
		previous = location
		havePrevious = true
		orderedCount++
	}
	ordered.Close()
	if orderedCount != len(entries)+2 {
		t.Fatalf("ordered collision count = %d, want %d", orderedCount, len(entries)+2)
	}

	scanner, err := OpenPageKeyTreeScanner(h.cache, h.root, h.bounds)
	if err != nil {
		t.Fatal(err)
	}
	scannedCount := 0
	scannedTarget := 0
	for {
		location, ok, nextErr := scanner.Next()
		if nextErr != nil {
			scanner.Close()
			t.Fatal(nextErr)
		}
		if !ok {
			break
		}
		scannedCount++
		if samePageKeyIdentity(location, target) {
			scannedTarget++
		}
	}
	scanner.Close()
	if scannedCount != len(entries)+2 || scannedTarget != 1 {
		t.Fatalf("full scan = (count=%d,target=%d), want (%d,1)",
			scannedCount, scannedTarget, len(entries)+2)
	}

	deleted, pages := h.mutate(pageKeyMutationDelete, target, 0)
	if !deleted.Found || !deleted.Changed || pages != 2 {
		t.Fatalf("cross-leaf delete = (%+v,pages=%d)", deleted, pages)
	}
	if _, ok := h.lookup(target); ok {
		t.Fatal("cross-leaf target remains after delete")
	}
}

func TestPageKeyTreeWarmCursorSteadyAllocation(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	entry := PageKeyLocation{Hash: 77, Chunk: 7, Slot: 7}
	h.mutate(pageKeyMutationInsert, entry, 0)
	h.bounds.FileEnd, h.bounds.NextLogicalID = h.fileEnd, h.nextID
	if allocs := testing.AllocsPerRun(1000, func() {
		cursor, err := OpenPageKeyTreeCursor(h.cache, h.root, entry.Hash, h.bounds)
		if err != nil {
			panic(err)
		}
		got, ok, err := cursor.Next()
		cursor.Close()
		if err != nil || !ok || got != entry {
			panic("fingerprint cursor")
		}
	}); allocs != 0 {
		t.Fatalf("warm fingerprint cursor allocations = %g, want 0", allocs)
	}
}

func TestPageKeyTreeScannerExactOrderAndAllocations(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	empty, err := OpenPageKeyTreeScanner(h.cache, PageRef{}, PageKeyTreeBounds{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := empty.Next(); err != nil || ok {
		t.Fatalf("empty scan = (ok=%v, err=%v), want exhausted", ok, err)
	}

	leafCapacity := (int(testSuperblockPageSize) - PageHeaderSize - PageTrailerSize -
		PageKeyDirectoryPayloadHeaderSize) / PageKeyLeafEntrySize
	entries := make([]PageKeyLocation, leafCapacity*2+37)
	edits := make([]PageKeyTreeEdit, len(entries))
	for index := range entries {
		entries[index] = PageKeyLocation{
			Hash:  uint64(index/3 + 1),
			Chunk: uint32(index / 64),
			Slot:  uint8(index % 64),
		}
		edits[index] = PageKeyTreeEdit{
			Location: entries[index], Operation: PageKeyTreeInsert,
		}
	}
	h.mutateBatch(edits)
	h.bounds.FileEnd, h.bounds.NextLogicalID = h.fileEnd, h.nextID

	scanner, err := OpenPageKeyTreeScanner(h.cache, h.root, h.bounds)
	if err != nil {
		t.Fatal(err)
	}
	for index, want := range entries {
		got, ok, nextErr := scanner.Next()
		if nextErr != nil || !ok || got != want {
			scanner.Close()
			t.Fatalf("scan[%d] = (%+v,%v,%v), want %+v",
				index, got, ok, nextErr, want)
		}
	}
	if _, ok, err := scanner.Next(); err != nil || ok {
		t.Fatalf("scan tail = (ok=%v, err=%v), want exhausted", ok, err)
	}
	scanner.Close()

	failed := false
	allocs := testing.AllocsPerRun(100, func() {
		scanner, openErr := OpenPageKeyTreeScanner(h.cache, h.root, h.bounds)
		if openErr != nil {
			failed = true
			return
		}
		count := 0
		for {
			_, ok, nextErr := scanner.Next()
			if nextErr != nil {
				failed = true
				break
			}
			if !ok {
				break
			}
			count++
		}
		scanner.Close()
		if count != len(entries) {
			failed = true
		}
	})
	if failed {
		t.Fatal("allocation probe did not preserve scan parity")
	}
	if allocs != 0 {
		t.Fatalf("warm scanner allocations = %f, want 0", allocs)
	}
}

func TestPageKeyTreeCursorRejectsCrossLeafIdentityDisorder(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	const hash = uint64(88)
	// Each physical leaf is locally sorted and therefore encodes cleanly, but
	// the second leaf moves backward across the boundary.
	entries := []PageKeyLocation{
		{Hash: hash, Chunk: 10, Slot: 0},
		{Hash: hash, Chunk: 20, Slot: 0},
		{Hash: hash, Chunk: 5, Slot: 0},
		{Hash: hash, Chunk: 30, Slot: 0},
	}
	h.seedCollisionLeaves(entries, 2)
	h.bounds.FileEnd, h.bounds.NextLogicalID = h.fileEnd, h.nextID
	cursor, err := OpenPageKeyTreeCursor(h.cache, h.root, hash, h.bounds)
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	for {
		_, ok, err := cursor.Next()
		if errors.Is(err, ErrKeyDirectoryCorrupt) {
			return
		}
		if err != nil {
			t.Fatalf("cross-leaf disorder error = %v", err)
		}
		if !ok {
			t.Fatal("cross-leaf disorder was accepted")
		}
	}
}

func TestPageKeyTreeRejectsParentChildMaximumMismatch(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	tx := h.begin(8)
	leaf, err := tx.Allocate(PageFingerprintDirectory, testSuperblockPageSize, 0)
	if err != nil {
		t.Fatal(err)
	}
	root, err := tx.Allocate(PageFingerprintDirectory, testSuperblockPageSize, 0)
	if err != nil {
		t.Fatal(err)
	}
	entry := PageKeyLocation{Hash: 41, Chunk: 4, Slot: 1}
	leafHeader := pageKeyTreeHeader(tx, leaf.Ref(), 0)
	leafHeader.MinHash = entry.Hash
	leafHeader.MaxHash = entry.Hash
	if _, err := EncodePageFingerprintLeaf(
		leaf.Bytes(), leafHeader, []PageKeyLocation{entry},
		tx.FileEnd(), tx.NextLogicalID(), h.bounds.ChunkHighWater, h.bounds.ChunkDocuments,
	); err != nil {
		t.Fatal(err)
	}
	if err := leaf.Stage(); err != nil {
		t.Fatal(err)
	}
	rootHeader := pageKeyTreeHeader(tx, root.Ref(), 1)
	rootHeader.MinHash = entry.Hash
	rootHeader.MaxHash = entry.Hash + 1
	if _, err := EncodePageFingerprintBranch(
		root.Bytes(), rootHeader,
		[]PageKeyBranch{{MaxHash: entry.Hash + 1, Child: leaf.Ref()}},
		tx.FileEnd(), tx.NextLogicalID(),
	); err != nil {
		t.Fatal(err)
	}
	if err := root.Stage(); err != nil {
		t.Fatal(err)
	}
	h.publish(tx, root.Ref())
	h.bounds.FileEnd, h.bounds.NextLogicalID = h.fileEnd, h.nextID

	if _, err := OpenPageKeyTreeCursor(h.cache, h.root, entry.Hash, h.bounds); !errors.Is(err, ErrKeyDirectoryCorrupt) {
		t.Fatalf("parent/child maximum mismatch error = %v", err)
	}
}

func TestPageKeyTreeBatchPagesReservationBound(t *testing.T) {
	const depth = int(pageKeyDirectoryMaxLevel) + 1
	const perEdit = 3 * depth
	const fixed = depth
	if got := PageKeyTreeBatchPages(0); got != 0 {
		t.Fatalf("zero-edit reservation = %d, want 0", got)
	}
	if got, want := PageKeyTreeBatchPages(1), perEdit+fixed; got != want {
		t.Fatalf("one-edit reservation = %d, want %d", got, want)
	}
	previous := 0
	for edits := 1; edits <= 1_000; edits++ {
		got := PageKeyTreeBatchPages(edits)
		if got <= previous {
			t.Fatalf("reservation did not grow at %d: %d <= %d", edits, got, previous)
		}
		previous = got
	}
	maxInt := int(^uint(0) >> 1)
	lastExact := (maxInt - fixed) / perEdit
	if got, want := PageKeyTreeBatchPages(lastExact), lastExact*perEdit+fixed; got != want {
		t.Fatalf("last exact reservation = %d, want %d", got, want)
	}
	if got := PageKeyTreeBatchPages(lastExact + 1); got != maxInt {
		t.Fatalf("overflow reservation = %d, want saturation %d", got, maxInt)
	}
	if got := PageKeyTreeBatchPages(maxInt); got != maxInt {
		t.Fatalf("maximum edit reservation = %d, want saturation %d", got, maxInt)
	}
}

func TestPageKeyTreeBatchBuildAndCollisionAwareRewrite(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	const hash = uint64(0xc0111de)
	leafCapacity := (int(testSuperblockPageSize) - PageHeaderSize - PageTrailerSize -
		PageKeyDirectoryPayloadHeaderSize) / PageKeyLeafEntrySize
	entries := make([]PageKeyLocation, leafCapacity+40)
	build := make([]PageKeyTreeEdit, len(entries))
	for index := range entries {
		entries[index] = PageKeyLocation{
			Hash: uint64(index + 1), Chunk: uint32(index / 64), Slot: uint8(index % 64),
		}
		build[index] = PageKeyTreeEdit{
			Location: entries[index], Operation: PageKeyTreeInsert,
		}
	}
	mutation, pages := h.mutateBatch(build)
	if !mutation.Changed || mutation.Root == (PageRef{}) ||
		pages > PageKeyTreeBatchPages(len(build)) {
		t.Fatalf("batch build = (%+v,pages=%d,bound=%d)",
			mutation, pages, PageKeyTreeBatchPages(len(build)))
	}
	for _, want := range entries {
		if got, ok := h.lookup(want); !ok || got != want {
			t.Fatalf("batch build lookup = (%+v,%v), want %+v", got, ok, want)
		}
	}

	collisions := make([]PageKeyLocation, leafCapacity+53)
	for index := range collisions {
		collisions[index] = PageKeyLocation{
			Hash: hash, Chunk: uint32(index / 64), Slot: uint8(index % 64),
		}
	}
	h.seedCollisionLeaves(collisions, leafCapacity)
	leftDelete := collisions[20]
	insert := PageKeyLocation{Hash: hash, Chunk: 10, Slot: 0}
	edits := []PageKeyTreeEdit{
		{Location: leftDelete, Operation: PageKeyTreeDelete},
		{Location: insert, Operation: PageKeyTreeInsert},
	}
	// Every identity shares a hash, so chunk/slot order is the complete batch
	// order. leftDelete sorts before insert.
	mutation, pages = h.mutateBatch(edits)
	if !mutation.Changed || pages != 3 || len(mutation.Retired) != 3 {
		t.Fatalf("collision batch = (%+v,pages=%d), want three rewritten pages", mutation, pages)
	}
	if _, ok := h.lookup(leftDelete); ok {
		t.Fatal("batch-deleted collision remains")
	}
	if got, ok := h.lookup(insert); !ok || got != insert {
		t.Fatalf("batch-inserted collision = (%+v,%v), want %+v", got, ok, insert)
	}
}

func TestPageKeyTreeBatchRejectsUnsortedAndDoesNotAllocateForNoOps(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	entry := PageKeyLocation{Hash: 10, Chunk: 1, Slot: 1}
	h.mutate(pageKeyMutationInsert, entry, 0)

	tx := h.begin(8)
	_, err := MutatePageKeyTreeBatch(h.cache, tx, h.root, []PageKeyTreeEdit{
		{Location: PageKeyLocation{Hash: 20, Chunk: 2, Slot: 2}, Operation: PageKeyTreeInsert},
		{Location: PageKeyLocation{Hash: 10, Chunk: 3, Slot: 3}, Operation: PageKeyTreeInsert},
	}, h.bounds, nil)
	if !errors.Is(err, ErrInvalidWrite) || tx.allocated != 0 {
		_ = tx.Abort()
		t.Fatalf("unsorted batch = (err=%v,pages=%d)", err, tx.allocated)
	}
	if err := tx.Abort(); err != nil {
		t.Fatal(err)
	}

	noops := []PageKeyTreeEdit{
		{Location: entry, Operation: PageKeyTreeInsert},
		{Location: PageKeyLocation{Hash: 30, Chunk: 3, Slot: 3}, Operation: PageKeyTreeDelete},
	}
	mutation, pages := h.mutateBatch(noops)
	if mutation.Changed || mutation.Root != h.root || pages != 0 || len(mutation.Retired) != 0 {
		t.Fatalf("no-op batch = (%+v,pages=%d)", mutation, pages)
	}
}

func TestPageKeyTreeBatchCollapsesRootWithoutCopyingUntouchedLeaf(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	const hash = uint64(55)
	entries := []PageKeyLocation{
		{Hash: hash, Chunk: 0, Slot: 0},
		{Hash: hash, Chunk: 0, Slot: 1},
		{Hash: hash, Chunk: 0, Slot: 2},
		{Hash: hash, Chunk: 0, Slot: 3},
	}
	h.seedCollisionLeaves(entries, 2)
	mutation, pages := h.mutateBatch([]PageKeyTreeEdit{
		{Location: entries[2], Operation: PageKeyTreeDelete},
		{Location: entries[3], Operation: PageKeyTreeDelete},
	})
	if !mutation.Changed || pages != 0 || len(mutation.Retired) != 2 {
		t.Fatalf("root collapse = (%+v,pages=%d)", mutation, pages)
	}
	lease, view, err := acquirePageFingerprintDirectory(h.cache, h.root, PageKeyTreeBounds{
		FileEnd: h.fileEnd, NextLogicalID: h.nextID,
		ChunkHighWater: h.bounds.ChunkHighWater, ChunkDocuments: h.bounds.ChunkDocuments,
	})
	if err != nil {
		t.Fatal(err)
	}
	if view.Header().Level != 0 || view.Len() != 2 {
		lease.Release()
		t.Fatalf("collapsed root = (level=%d,len=%d), want leaf with two entries",
			view.Header().Level, view.Len())
	}
	lease.Release()
	for _, want := range entries[:2] {
		if got, ok := h.lookup(want); !ok || got != want {
			t.Fatalf("survivor after collapse = (%+v,%v), want %+v", got, ok, want)
		}
	}
	for _, deleted := range entries[2:] {
		if _, ok := h.lookup(deleted); ok {
			t.Fatalf("deleted entry %+v survived collapse", deleted)
		}
	}
}

func TestPageKeyTreeCollisionCursorClimbsMultipleParentLevelsAfterCOW(t *testing.T) {
	h := newPageKeyTreeHarness(t)
	const hash = uint64(0xdecafbad)
	leafCapacity := (int(testSuperblockPageSize) - PageHeaderSize - PageTrailerSize -
		PageKeyDirectoryPayloadHeaderSize) / PageKeyLeafEntrySize
	branchCapacity := (int(testSuperblockPageSize) - PageHeaderSize - PageTrailerSize -
		PageKeyDirectoryPayloadHeaderSize) / PageKeyBranchEntrySize
	count := leafCapacity*branchCapacity + 1
	edits := make([]PageKeyTreeEdit, count)
	for index := range edits {
		edits[index] = PageKeyTreeEdit{
			Location: PageKeyLocation{
				Hash: hash, Chunk: uint32(index / 64), Slot: uint8(index % 64),
			},
			Operation: PageKeyTreeInsert,
		}
	}
	tx := h.begin(256)
	mutation, err := MutatePageKeyTreeBatch(h.cache, tx, h.root, edits, h.bounds, nil)
	if err != nil {
		_ = tx.Abort()
		t.Fatal(err)
	}
	if tx.allocated > 255 {
		_ = tx.Abort()
		t.Fatalf("multi-level build allocated %d pages", tx.allocated)
	}
	h.publish(tx, mutation.Root)

	rootLease, rootView, err := acquirePageFingerprintDirectory(h.cache, h.root, PageKeyTreeBounds{
		FileEnd: h.fileEnd, NextLogicalID: h.nextID,
		ChunkHighWater: h.bounds.ChunkHighWater, ChunkDocuments: h.bounds.ChunkDocuments,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rootView.Header().Level < 2 {
		rootLease.Release()
		t.Fatalf("collision tree root level = %d, want at least 2", rootView.Header().Level)
	}
	rootLease.Release()

	target := PageKeyLocation{Hash: hash, Chunk: 1000, Slot: 0}
	inserted, pages := h.mutate(pageKeyMutationInsert, target, 0)
	if inserted.Found || !inserted.Changed || pages != 3 {
		t.Fatalf("deep collision insert = (%+v,pages=%d), want three-page path", inserted, pages)
	}
	if got, ok := h.lookup(target); !ok || got != target {
		t.Fatalf("deep collision lookup = (%+v,%v), want %+v", got, ok, target)
	}
}
