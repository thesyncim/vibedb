package storeio

import (
	"errors"
	"os"
	"slices"
	"testing"
)

func TestWriteTransactionPublishesRecoverableStateAndDirtyPage(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "write-transaction-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	committer, err := NewCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 8, BufferSize: os.Getpagesize(),
	}, CommitterOptions{QueueSlots: 4, MaxPagesPerBatch: 4, GroupLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: int(testSuperblockPageSize), ResidentBytes: int64(4 * testSuperblockPageSize),
		StoreID: testStoreID, ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	tx, err := BeginWriteTransaction(committer, cache, 4, WriteTransactionOptions{
		StoreID: testStoreID, Generation: 1, PageSize: testSuperblockPageSize,
		FileEnd: testMutableStoreDataStart(testSuperblockPageSize), NextLogicalID: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	dirty, err := tx.Allocate(PageIndexPosting, testSuperblockPageSize, 0)
	if err != nil {
		t.Fatal(err)
	}
	dirtyPayload, err := InitPage(dirty.Bytes(), PageHeader{
		StoreID: testStoreID, Generation: 1, LogicalID: dirty.Ref().LogicalID,
		PageSize: testSuperblockPageSize, PayloadLength: 1, Kind: PageIndexPosting,
	})
	if err != nil {
		t.Fatal(err)
	}
	dirtyPayload[0] = 0x5a
	if _, err := SealPage(dirty.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := dirty.Stage(); err != nil {
		t.Fatal(err)
	}
	write := tx.batch.pages[0]
	staged, err := tx.batch.PageBuffer(0)
	if err != nil || !write.frameNative() ||
		len(staged) < int(dirty.Ref().Length) ||
		&staged[0] != &dirty.Bytes()[0] {
		t.Fatal("dirty page was not staged in its page-cache frame")
	}
	lease, err := cache.Acquire(dirty.Ref())
	if err != nil {
		t.Fatal(err)
	}
	_, payload, err := OpenPage(lease.Page())
	if err != nil || len(payload) != 1 || payload[0] != 0x5a {
		t.Fatalf("dirty page readback = (%x,%v)", payload, err)
	}
	lease.Release()

	wantState := StateRoot{
		StoreID: testStoreID, Generation: 1, PageSize: testSuperblockPageSize,
		MaxPageSize: 64 << 10, NextLogicalID: tx.NextLogicalID(),
	}
	if err := tx.PublishInline(wantState, InlineFreeDelta{}); err != nil {
		t.Fatal(err)
	}
	if stats := cache.Stats(); stats.DirtyBytes != uint64(testSuperblockPageSize) {
		t.Fatalf("dirty cache before wait = %+v", stats)
	}
	if err := committer.Wait(1); err != nil {
		t.Fatal(err)
	}
	cache.MarkDurable(committer.DurableGeneration())
	if stats := cache.Stats(); stats.DirtyBytes != 0 {
		t.Fatalf("dirty cache after wait = %+v", stats)
	}
	scratch := make([]byte, testSuperblockPageSize)
	root, gotState, slot, err := RecoverInlineStateRoot(file, testSuperblockPageSize, scratch)
	if err != nil || slot != 0 || gotState != wantState || root.Generation != 1 || root.FileEnd != tx.FileEnd() {
		t.Fatalf("RecoverStateRoot = (%+v,%+v,%d,%v)", root, gotState, slot, err)
	}
}

func TestWriteTransactionValidationAndAbort(t *testing.T) {
	committer, _, _ := newPortableCommitter(t, 4, 2)
	defer committer.Close()
	if _, err := BeginWriteTransaction(committer, nil, 1, WriteTransactionOptions{}); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("invalid Begin = %v, want %v", err, ErrInvalidWrite)
	}
	tx, err := BeginWriteTransaction(committer, nil, 2, WriteTransactionOptions{
		StoreID: testStoreID, Generation: 1, PageSize: testSuperblockPageSize,
		FileEnd: testMutableStoreDataStart(testSuperblockPageSize), NextLogicalID: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Allocate(PageCatalogSegment, 2*testSuperblockPageSize, 0); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("variable metadata = %v, want %v", err, ErrInvalidWrite)
	}
	if page, err := tx.Allocate(PageIndexPosting, testSuperblockPageSize, 0); err != nil ||
		page.Ref().Kind != PageIndexPosting {
		t.Fatalf("posting metadata allocation = (%+v,%v)", page.Ref(), err)
	}
	if err := tx.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Allocate(PageOverflow, testSuperblockPageSize, 0); !errors.Is(err, ErrTooManyPages) {
		t.Fatalf("Allocate after abort = %v, want %v", err, ErrTooManyPages)
	}
}

func TestWriteTransactionResetRequiresInactiveReceiver(t *testing.T) {
	committer, _, _ := newPortableCommitter(t, 4, 2)
	defer committer.Close()
	options := WriteTransactionOptions{
		StoreID: testStoreID, Generation: 1, PageSize: testSuperblockPageSize,
		FileEnd: testMutableStoreDataStart(testSuperblockPageSize), NextLogicalID: 2,
	}
	var tx WriteTransaction
	if err := tx.Reset(committer, nil, 0, options); err != nil {
		t.Fatal(err)
	}
	if err := tx.Reset(committer, nil, 0, options); !errors.Is(err, ErrInvalidWrite) {
		t.Fatalf("Reset active transaction = %v, want %v", err, ErrInvalidWrite)
	}
	if err := tx.Abort(); err != nil {
		t.Fatal(err)
	}
	options.Generation++
	if err := tx.Reset(committer, nil, 0, options); err != nil {
		t.Fatalf("Reset aborted transaction: %v", err)
	}
	if tx.Generation() != options.Generation {
		t.Fatalf("reset generation = %d, want %d", tx.Generation(), options.Generation)
	}
	if err := tx.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteTransactionForeignStagingRunsFullVerification(t *testing.T) {
	committer, _, pageSize := newPortableCommitter(t, 4, 2)
	defer committer.Close()
	tx, err := BeginWriteTransaction(committer, nil, 2, WriteTransactionOptions{
		StoreID: testStoreID, Generation: 1, PageSize: uint32(pageSize),
		FileEnd: testMutableStoreDataStart(uint32(pageSize)), NextLogicalID: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	page, err := tx.Allocate(PageIndexPosting, uint32(pageSize), 0)
	if err != nil {
		t.Fatal(err)
	}
	foreign := make([]byte, pageSize)
	payload, err := InitPage(foreign, PageHeader{
		StoreID: testStoreID, Generation: 1, LogicalID: page.Ref().LogicalID,
		PageSize: uint32(pageSize), PayloadLength: 1, Kind: PageIndexPosting,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 1
	if _, err := SealPage(foreign); err != nil {
		t.Fatal(err)
	}
	// Keep every identity field and the seal complement intact while changing a
	// checksum-covered payload byte. Only full CRC verification rejects this.
	foreign[PageHeaderSize] ^= 1
	forged := page
	forged.bytes = foreign
	if err := forged.Stage(); !errors.Is(err, ErrPageCorrupt) {
		t.Fatalf("foreign Stage = %v, want %v", err, ErrPageCorrupt)
	}

	payload, err = InitPage(page.Bytes(), PageHeader{
		StoreID: testStoreID, Generation: 1, LogicalID: page.Ref().LogicalID,
		PageSize: uint32(pageSize), PayloadLength: 1, Kind: PageIndexPosting,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 2
	if _, err := SealPage(page.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := page.Stage(); err != nil {
		t.Fatalf("owned sealed Stage after foreign rejection: %v", err)
	}
}

func TestWriteTransactionAllowsPackedPrimaryExtents(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "write-transaction-packed-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	// BufferSize must cover the largest allocated extent below. Hosts with a
	// 4 KiB OS page (GitHub's linux runners) used to set BufferSize to only
	// 2*PageSize via max(Getpagesize(), 2*PageSize), so the compact 3*PageSize
	// leaf allocation failed with ErrTooManyPages even though MaxPagesPerBatch
	// still had room.
	committer, err := NewCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 12,
		BufferSize: max(os.Getpagesize(), 3*int(testSuperblockPageSize)),
	}, CommitterOptions{
		QueueSlots: 4, MaxPagesPerBatch: 8, GroupLimit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()
	tx, err := BeginWriteTransaction(
		committer, nil, 8, WriteTransactionOptions{
			StoreID: testStoreID, Generation: 1,
			PageSize:      testSuperblockPageSize,
			FileEnd:       testMutableStoreDataStart(testSuperblockPageSize),
			NextLogicalID: 2,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []PageKind{
		PageOverflow, PagePrimaryCatalog, PagePrimaryLocator,
		PageTabletRoute, PagePrimaryAnchor, PagePrimaryLeaf, PagePrimaryExactLeaf,
	} {
		if _, err := tx.Allocate(
			kind, 2*testSuperblockPageSize, 0,
		); err != nil {
			t.Fatalf("Allocate(%v) = %v", kind, err)
		}
	}
	if _, err := tx.Allocate(
		PageCatalogSegment, 2*testSuperblockPageSize, 0,
	); err == nil {
		t.Fatal("variable-size single-page metadata node accepted")
	}
	if _, err := tx.Allocate(
		PagePrimaryLeaf, 3*testSuperblockPageSize, 0,
	); err != nil {
		t.Fatalf("exact compact primary extent rejected: %v", err)
	}
	if err := tx.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteTransactionAllowsExactPrimaryValueExtents(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "write-transaction-exact-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	const maxPages = 8
	committer, err := NewCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 12,
		BufferSize: max(os.Getpagesize(), 8*int(testSuperblockPageSize)),
	}, CommitterOptions{
		QueueSlots: 4, MaxPagesPerBatch: maxPages, GroupLimit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()
	tx, err := BeginWriteTransaction(
		committer, nil, maxPages, WriteTransactionOptions{
			StoreID: testStoreID, Generation: 1,
			PageSize:      testSuperblockPageSize,
			FileEnd:       testMutableStoreDataStart(testSuperblockPageSize),
			NextLogicalID: 2,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	wantOffset := tx.FileEnd()
	for _, kind := range []PageKind{PageOverflow} {
		for _, pages := range []uint32{3, 5, 7} {
			length := pages * testSuperblockPageSize
			page, allocateErr := tx.Allocate(kind, length, 0)
			if allocateErr != nil {
				t.Fatalf("Allocate(%v,%d pages): %v", kind, pages, allocateErr)
			}
			if ref := page.Ref(); ref.Offset != wantOffset ||
				ref.Length != length || len(page.Bytes()) != int(length) {
				t.Fatalf("Allocate(%v,%d pages) = ref %+v bytes %d, offset %d",
					kind, pages, ref, len(page.Bytes()), wantOffset)
			}
			wantOffset += uint64(length)
		}
	}
	if _, err := tx.Allocate(
		PagePrimaryLeaf, 3*testSuperblockPageSize, 0,
	); err != nil {
		t.Fatalf("exact compact primary allocation = %v", err)
	}
	for _, kind := range []PageKind{
		PageIndexPosting, PageCatalogSegment,
		PageFreeImage, PageFreeDelta, PageFreeIndex,
	} {
		if _, err := tx.Allocate(
			kind, 3*testSuperblockPageSize, 0,
		); !errors.Is(err, ErrTooManyPages) {
			t.Fatalf("non-power %v allocation = %v, want %v",
				kind, err, ErrTooManyPages)
		}
	}
	if err := tx.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteTransactionReusesAndRollsBackSafeExtents(t *testing.T) {
	committer, _, _ := newPortableCommitter(t, 4, 2)
	defer committer.Close()
	pageSize := uint64(testSuperblockPageSize)
	reusable := []FreeExtent{{Offset: 4 * pageSize, Length: 2 * pageSize, RetiredGeneration: 1}}
	journal := make([]ReuseEdit, 0, 2)
	begin := func() *WriteTransaction {
		tx, err := BeginWriteTransaction(committer, nil, 2, WriteTransactionOptions{
			StoreID: testStoreID, Generation: 3, PageSize: testSuperblockPageSize,
			FileEnd: 6 * pageSize, NextLogicalID: 2,
			Reusable: reusable, ReuseJournal: journal,
		})
		if err != nil {
			t.Fatal(err)
		}
		return tx
	}

	tx := begin()
	page, err := tx.Allocate(PageOverflow, testSuperblockPageSize, 0)
	if err != nil {
		t.Fatal(err)
	}
	if page.Ref().Offset != 5*pageSize || tx.FileEnd() != 6*pageSize || reusable[0].Offset != 4*pageSize || reusable[0].Length != pageSize {
		t.Fatalf("reused allocation = ref %+v fileEnd %d pool %+v", page.Ref(), tx.FileEnd(), reusable)
	}
	if err := tx.Abort(); err != nil {
		t.Fatal(err)
	}
	if reusable[0].Offset != 4*pageSize || reusable[0].Length != 2*pageSize {
		t.Fatalf("Abort did not restore pool: %+v", reusable)
	}

	tx = begin()
	state, err := tx.Allocate(PageIndexPosting, testSuperblockPageSize, 0)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := InitPage(state.Bytes(), PageHeader{
		StoreID: testStoreID, Generation: 3,
		LogicalID: state.Ref().LogicalID, PageSize: testSuperblockPageSize,
		PayloadLength: 1, Kind: PageIndexPosting,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 1
	if _, err := SealPage(state.Bytes()); err != nil {
		t.Fatal(err)
	}
	root := StateRoot{
		StoreID: testStoreID, Generation: 3, PageSize: testSuperblockPageSize,
		MaxPageSize: 64 << 10, NextLogicalID: tx.NextLogicalID(),
	}
	if err := state.Stage(); err != nil {
		t.Fatal(err)
	}
	if err := tx.PublishInline(root, InlineFreeDelta{}); err != nil {
		t.Fatal(err)
	}
	if reusable[0].Offset != 4*pageSize || reusable[0].Length != pageSize {
		t.Fatalf("Publish rolled back pool: %+v", reusable)
	}
}

// Given free space that is large enough in total but spread over several
// extents, when a transaction allocates more pages than any one extent holds,
// then it draws from every extent and never advances FileEnd.
//
// This is the property the removed SingleReuseExtent option destroyed: bounding
// a transaction to one extent so that a commit could persist one free-tree edit
// made a store with megabytes free grow the file anyway.
func TestWriteTransactionAllocatesAcrossSeveralFreeExtents(t *testing.T) {
	committer, _, _ := newPortableCommitter(t, 16, 8)
	defer committer.Close()
	pageSize := uint64(testSuperblockPageSize)
	fileEnd := 16 * pageSize
	// Three separate two-page extents, none of which can serve four pages alone.
	reusable := []FreeExtent{
		{Offset: 4 * pageSize, Length: 2 * pageSize, RetiredGeneration: 1},
		{Offset: 8 * pageSize, Length: 2 * pageSize, RetiredGeneration: 1},
		{Offset: 12 * pageSize, Length: 2 * pageSize, RetiredGeneration: 1},
	}
	tx, err := BeginWriteTransaction(committer, nil, 8, WriteTransactionOptions{
		StoreID: testStoreID, Generation: 5, PageSize: testSuperblockPageSize,
		FileEnd: fileEnd, NextLogicalID: 2,
		Reusable: reusable, ReuseJournal: make([]ReuseEdit, 0, 8),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	seen := make(map[uint64]bool, 6)
	for i := range 6 {
		page, allocErr := tx.Allocate(PageOverflow, testSuperblockPageSize, 0)
		if allocErr != nil {
			t.Fatalf("allocate %d: %v", i, allocErr)
		}
		if seen[page.Ref().Offset] {
			t.Fatalf("allocate %d reused offset %d", i, page.Ref().Offset)
		}
		seen[page.Ref().Offset] = true
		if got := tx.FileEnd(); got != fileEnd {
			t.Fatalf("allocate %d advanced FileEnd to %d, want %d", i, got, fileEnd)
		}
	}
	for _, extent := range reusable {
		if extent.Length != 0 {
			t.Fatalf("free extent %+v survived a transaction that consumed every page", extent)
		}
	}
	// The journal has to name every extent the transaction touched, because the
	// free log turns exactly that journal into the commit's delta records.
	touched := make(map[uint32]bool, 3)
	for _, edit := range tx.ReuseEdits() {
		touched[edit.Index] = true
	}
	if len(touched) != 3 {
		t.Fatalf("reuse journal named %d extents, want 3", len(touched))
	}
}

type writeTransactionPromotionProbe struct {
	reusable []FreeExtent
	index    *FreeExtentIndex
	storage  []uint64
	pageSize uint64
	promoted bool
}

func (p *writeTransactionPromotionProbe) PromoteReusable(
	want, beforeOffset uint64, _ []ReuseEdit,
) ([]FreeExtent, bool, error) {
	if p.promoted || want < 2*p.pageSize {
		return p.reusable, false, nil
	}
	p.promoted = true
	p.reusable = p.reusable[:len(p.reusable)+1]
	copy(p.reusable[1:], p.reusable[:len(p.reusable)-1])
	p.reusable[0] = FreeExtent{
		Offset: 4 * p.pageSize, Length: 2 * p.pageSize, RetiredGeneration: 1,
	}
	if !p.index.Rebuild(p.reusable, p.storage) {
		return p.reusable, true, ErrInvalidWrite
	}
	if beforeOffset != 20*p.pageSize {
		return p.reusable, true, ErrInvalidWrite
	}
	return p.reusable, true, nil
}

func TestWriteTransactionIndexedPromotionRemapsAbortJournal(t *testing.T) {
	committer, _, _ := newPortableCommitter(t, 8, 4)
	defer committer.Close()
	pageSize := uint64(testSuperblockPageSize)
	reusable := make([]FreeExtent, 2, 4)
	reusable[0] = FreeExtent{
		Offset: 8 * pageSize, Length: pageSize, RetiredGeneration: 1,
	}
	reusable[1] = FreeExtent{
		Offset: 20 * pageSize, Length: 2 * pageSize, RetiredGeneration: 1,
	}
	storage := make([]uint64, FreeExtentIndexCapacity(cap(reusable)))
	var index FreeExtentIndex
	if !index.Rebuild(reusable, storage) {
		t.Fatal("index rebuild failed")
	}
	promoter := &writeTransactionPromotionProbe{
		reusable: reusable, index: &index, storage: storage, pageSize: pageSize,
	}
	tx, err := BeginWriteTransaction(committer, nil, 4, WriteTransactionOptions{
		StoreID: testStoreID, Generation: 3, PageSize: testSuperblockPageSize,
		FileEnd: 32 * pageSize, NextLogicalID: 2,
		Reusable: reusable, ReuseJournal: make([]ReuseEdit, 0, 4),
		ReusableIndex: &index, ReusablePromoter: promoter,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := tx.Allocate(PageIndexPosting, testSuperblockPageSize, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := first.Ref().Offset, 8*pageSize; got != want {
		t.Fatalf("first indexed allocation offset = %d, want %d", got, want)
	}
	second, err := tx.Allocate(PagePrimaryLeaf, 2*testSuperblockPageSize, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := second.Ref().Offset, 4*pageSize; got != want {
		t.Fatalf("promoted allocation offset = %d, want %d", got, want)
	}
	if edits := tx.ReuseEdits(); len(edits) != 2 ||
		edits[0].Index != 1 || edits[1].Index != 0 {
		t.Fatalf("remapped reuse journal = %+v, want indexes [1 0]", edits)
	}
	if err := tx.Abort(); err != nil {
		t.Fatal(err)
	}
	want := []FreeExtent{
		{Offset: 4 * pageSize, Length: 2 * pageSize, RetiredGeneration: 1},
		{Offset: 8 * pageSize, Length: pageSize, RetiredGeneration: 1},
		{Offset: 20 * pageSize, Length: 2 * pageSize, RetiredGeneration: 1},
	}
	if !slices.Equal(promoter.reusable, want) {
		t.Fatalf("abort restored %+v, want %+v", promoter.reusable, want)
	}
	if rank, ok := index.FirstFit(promoter.reusable, 2*pageSize); !ok || rank != 0 {
		t.Fatalf("index after abort = %d, %v, want 0, true", rank, ok)
	}
}
