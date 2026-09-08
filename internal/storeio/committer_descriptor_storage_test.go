package storeio

import (
	"errors"
	"os"
	"testing"
)

func TestCommitterDescriptorStorageIsProportionalToClaimedBatch(t *testing.T) {
	committer, _, _ := newPortableCommitter(t, 8, 4)
	defer committer.Close()

	batches := make([]*Batch, len(committer.batches))
	for index := range batches {
		batch, err := committer.Begin(1)
		if err != nil {
			t.Fatalf("Begin slot %d: %v", index, err)
		}
		batches[index] = batch
		if got := cap(batch.pages); got != 1 {
			t.Fatalf("slot %d page capacity = %d, want 1", batch.index, got)
		}
		if got := cap(batch.bufferIndexes); got != 3 {
			t.Fatalf("slot %d index capacity = %d, want 3", batch.index, got)
		}
	}
	for index := range batches {
		if index > 0 {
			batches[index-1].pages[0].Offset = int64(index)
			batches[index-1].bufferIndexes[0] = uint32(index)
			if batches[index].pages[0].Offset == int64(index) ||
				batches[index].bufferIndexes[0] == uint32(index) {
				t.Fatalf("descriptor storage for slots %d and %d aliases", index-1, index)
			}
		}
	}
	for _, batch := range batches {
		if err := batch.Abort(); err != nil {
			t.Fatal(err)
		}
	}
	for index, batch := range batches {
		if cap(batch.pages) != 1 || cap(batch.bufferIndexes) != 3 {
			t.Fatalf("slot %d high-water capacities changed after Abort: pages=%d indexes=%d", index, cap(batch.pages), cap(batch.bufferIndexes))
		}
	}
}

func TestCommitterDescriptorStorageGrowsOnlyClaimedSlot(t *testing.T) {
	committer, _, _ := newPortableCommitter(t, 10, 4)
	defer committer.Close()

	small, err := committer.Begin(1)
	if err != nil {
		t.Fatal(err)
	}
	large, err := committer.Begin(3)
	if err != nil {
		t.Fatal(err)
	}
	large.pages[0].Offset = 123
	large.bufferIndexes[0] = 456
	if err := small.Abort(); err != nil {
		t.Fatal(err)
	}

	grown, err := committer.Begin(4)
	if err != nil {
		t.Fatalf("Begin exact maximum after releasing small slot: %v", err)
	}
	if grown.index != small.index {
		t.Fatalf("grown slot = %d, want released slot %d", grown.index, small.index)
	}
	if cap(grown.pages) != 4 || cap(grown.bufferIndexes) != 6 {
		t.Fatalf("grown slot capacities = pages %d indexes %d, want 4 and 6", cap(grown.pages), cap(grown.bufferIndexes))
	}
	if cap(large.pages) != 3 || cap(large.bufferIndexes) != 5 ||
		large.pages[0].Offset != 123 || large.bufferIndexes[0] != 456 {
		t.Fatalf("growth changed the still-owned slot: pages cap=%d indexes cap=%d descriptor=%+v index=%d", cap(large.pages), cap(large.bufferIndexes), large.pages[0], large.bufferIndexes[0])
	}
	if err := grown.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := large.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitterDescriptorStorageReturnsGenericFrameAndMaterializationBuffers(t *testing.T) {
	pageSize := max(uint32(os.Getpagesize()), testSuperblockPageSize)
	device := newMaterializationRecordingDevice(8, int(pageSize))
	committer := newMaterializationTestCommitter(t, device, CommitterOptions{
		QueueSlots: 4, MaxPagesPerBatch: 4, GroupLimit: 4,
	})
	defer committer.Close()
	initial := committer.freeBuffers.availableCount()

	generic, err := committer.Begin(2)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := committer.freeBuffers.availableCount(), initial-3; got != want {
		t.Fatalf("generic Begin buffers = %d, want %d", got, want)
	}
	if err := generic.ResizePages(1); err != nil {
		t.Fatal(err)
	}
	if got, want := committer.freeBuffers.availableCount(), initial-2; got != want {
		t.Fatalf("generic ResizePages buffers = %d, want %d", got, want)
	}
	if err := generic.Abort(); err != nil {
		t.Fatal(err)
	}
	if got := committer.freeBuffers.availableCount(); got != initial {
		t.Fatalf("generic Abort buffers = %d, want %d", got, initial)
	}

	frameNative, err := committer.beginFrameNative(2)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := committer.freeBuffers.availableCount(), initial-1; got != want {
		t.Fatalf("frame-native Begin buffers = %d, want %d", got, want)
	}
	if cap(frameNative.pages) < 2 || cap(frameNative.bufferIndexes) < 2 {
		t.Fatalf("frame-native capacities = pages %d indexes %d, want at least 2 and 2", cap(frameNative.pages), cap(frameNative.bufferIndexes))
	}
	if err := frameNative.Abort(); err != nil {
		t.Fatal(err)
	}
	if got := committer.freeBuffers.availableCount(); got != initial {
		t.Fatalf("frame-native Abort buffers = %d, want %d", got, initial)
	}

	patchOnly, err := committer.BeginMaterialized(1)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := committer.freeBuffers.availableCount(), initial-3; got != want {
		t.Fatalf("patch-only Begin buffers = %d, want %d", got, want)
	}
	if cap(patchOnly.pages) < 1 || cap(patchOnly.bufferIndexes) < 3 {
		t.Fatalf("patch-only capacities = pages %d indexes %d, want at least 1 and 3", cap(patchOnly.pages), cap(patchOnly.bufferIndexes))
	}
	if got := uint32(patchOnly.journal.Buffer); got != patchOnly.bufferIndexes[int(patchOnly.dataBufferCount)+1] {
		t.Fatalf("journal index = %d, scratch index = %d", got, patchOnly.bufferIndexes[int(patchOnly.dataBufferCount)+1])
	}
	if err := patchOnly.Abort(); err != nil {
		t.Fatal(err)
	}
	if got := committer.freeBuffers.availableCount(); got != initial {
		t.Fatalf("patch-only Abort buffers = %d, want %d", got, initial)
	}

	hybrid, err := committer.beginHybridMaterialized(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := committer.freeBuffers.availableCount(), initial-4; got != want {
		t.Fatalf("hybrid Begin buffers = %d, want %d", got, want)
	}
	if cap(hybrid.pages) < 2 || cap(hybrid.bufferIndexes) < 4 {
		t.Fatalf("hybrid capacities = pages %d indexes %d, want at least 2 and 4", cap(hybrid.pages), cap(hybrid.bufferIndexes))
	}
	if err := hybrid.resizeMaterializationPages(0); err != nil {
		t.Fatal(err)
	}
	if got, want := committer.freeBuffers.availableCount(), initial-3; got != want {
		t.Fatalf("hybrid resize buffers = %d, want %d", got, want)
	}
	if err := hybrid.Abort(); err != nil {
		t.Fatal(err)
	}
	if got := committer.freeBuffers.availableCount(); got != initial {
		t.Fatalf("hybrid Abort buffers = %d, want %d", got, initial)
	}
}

func TestCommitterDescriptorStorageRootOnlyBatchAndWarmedReuse(t *testing.T) {
	committer, _, _ := newPortableCommitter(t, 8, 4)
	defer committer.Close()

	rootOnly, err := committer.Begin(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rootOnly.pages) != 0 || cap(rootOnly.pages) != 0 ||
		cap(rootOnly.bufferIndexes) != 2 {
		t.Fatalf("root-only descriptor storage = pages len/cap %d/%d indexes cap %d", len(rootOnly.pages), cap(rootOnly.pages), cap(rootOnly.bufferIndexes))
	}
	if _, err := rootOnly.RootBuffer(); err != nil {
		t.Fatal(err)
	}
	if err := rootOnly.Abort(); err != nil {
		t.Fatal(err)
	}

	for range committer.batches {
		batch, err := committer.Begin(4)
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Abort(); err != nil {
			t.Fatal(err)
		}
	}
	if allocs := testing.AllocsPerRun(20, func() {
		batch, err := committer.Begin(4)
		if err != nil {
			panic(err)
		}
		if err := batch.Abort(); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warmed Begin/Abort allocations = %g, want 0", allocs)
	}
}

func TestCommitterDescriptorStorageKeepsBoundChecks(t *testing.T) {
	committer, _, _ := newPortableCommitter(t, 4, 2)
	defer committer.Close()
	if _, err := committer.Begin(3); !errors.Is(err, ErrTooManyPages) {
		t.Fatalf("oversized Begin = %v, want %v", err, ErrTooManyPages)
	}
	if _, err := committer.begin(2, 3); !errors.Is(err, ErrTooManyPages) {
		t.Fatalf("oversized buffered Begin = %v, want %v", err, ErrTooManyPages)
	}
}

func TestCommitterDescriptorStorageWriteTransactionDurabilityAndReuse(t *testing.T) {
	const (
		queueSlots    = 64
		deviceBuffers = 1024
		maxPages      = deviceBuffers - 1
		generations   = 3
	)
	pageSize := testSuperblockPageSize
	file, err := os.CreateTemp(t.TempDir(), "descriptor-storage-durable-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	committer, err := NewCommitter(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: deviceBuffers,
		BufferSize: max(os.Getpagesize(), 2*int(pageSize)),
	}, CommitterOptions{
		FrameNativeStaging: true,
		QueueSlots:         queueSlots,
		MaxPagesPerBatch:   maxPages,
		GroupLimit:         1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()
	cache, err := NewPageCache(file, PageCacheOptions{
		Backend: BackendPortable, PageSize: int(pageSize),
		ResidentBytes: int64(4 * pageSize), StoreID: testStoreID,
		ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	fileEnd := testMutableStoreDataStart(pageSize)
	nextLogicalID := uint64(2)
	var tx WriteTransaction
	var firstSlot uint32
	touched := make(map[uint32]struct{})
	for generation := uint64(1); generation <= generations; generation++ {
		options := WriteTransactionOptions{
			StoreID: testStoreID, Generation: generation, PageSize: pageSize,
			FileEnd: fileEnd, NextLogicalID: nextLogicalID,
		}
		if err := tx.Reset(committer, cache, maxPages, options); err != nil {
			t.Fatalf("Reset generation %d: %v", generation, err)
		}
		batch := tx.batch
		touched[batch.index] = struct{}{}
		if generation == 1 {
			firstSlot = batch.index
		} else if batch.index != firstSlot {
			t.Fatalf("generation %d used slot %d, want warmed slot %d", generation, batch.index, firstSlot)
		}
		if len(batch.pages) != maxPages || cap(batch.pages) != maxPages ||
			len(batch.bufferIndexes) != 2 || cap(batch.bufferIndexes) != 2 {
			t.Fatalf("generation %d descriptor storage = pages len/cap %d/%d indexes len/cap %d/%d", generation,
				len(batch.pages), cap(batch.pages), len(batch.bufferIndexes), cap(batch.bufferIndexes))
		}

		page, err := tx.Allocate(PageIndexPosting, pageSize, 0)
		if err != nil {
			t.Fatalf("Allocate generation %d: %v", generation, err)
		}
		payload, err := InitPage(page.Bytes(), PageHeader{
			StoreID: testStoreID, Generation: generation,
			LogicalID: page.Ref().LogicalID, PageSize: pageSize,
			PayloadLength: 1, Kind: PageIndexPosting,
		})
		if err != nil {
			t.Fatalf("InitPage generation %d: %v", generation, err)
		}
		payload[0] = byte(generation)
		if _, err := SealPage(page.Bytes()); err != nil {
			t.Fatalf("SealPage generation %d: %v", generation, err)
		}
		if err := page.Stage(); err != nil {
			t.Fatalf("Stage generation %d: %v", generation, err)
		}
		wantState := StateRoot{
			StoreID: testStoreID, Generation: generation, PageSize: pageSize,
			MaxPageSize: 64 << 10, NextLogicalID: tx.NextLogicalID(),
		}
		wantFileEnd := tx.FileEnd()
		if err := tx.PublishInline(wantState, InlineFreeDelta{}); err != nil {
			t.Fatalf("PublishInline generation %d: %v", generation, err)
		}
		if err := committer.Wait(generation); err != nil {
			t.Fatalf("Wait generation %d: %v", generation, err)
		}
		if err := committer.Flush(); err != nil {
			t.Fatalf("Flush generation %d: %v", generation, err)
		}
		cache.MarkDurable(committer.DurableGeneration())
		if stats := cache.Stats(); stats.DirtyBytes != 0 {
			t.Fatalf("dirty cache after generation %d = %+v", generation, stats)
		}

		encoded := make([]byte, int(pageSize))
		n, err := file.ReadAt(encoded, int64(page.Ref().Offset))
		if err != nil || n != len(encoded) {
			t.Fatalf("file readback generation %d = bytes %d/%d, err %v", generation, n, len(encoded), err)
		}
		header, gotPayload, err := OpenPage(encoded)
		if err != nil || header.StoreID != testStoreID || header.Generation != generation ||
			header.LogicalID != page.Ref().LogicalID || len(gotPayload) != 1 || gotPayload[0] != byte(generation) {
			t.Fatalf("page readback generation %d = header=%+v payload=%x err=%v", generation, header, gotPayload, err)
		}
		scratch := make([]byte, int(pageSize))
		root, gotState, _, err := RecoverInlineStateRoot(file, pageSize, scratch)
		if err != nil || root.Generation != generation || root.FileEnd != wantFileEnd || gotState != wantState {
			t.Fatalf("root readback generation %d = root=%+v state=%+v err=%v", generation, root, gotState, err)
		}
		fileEnd, nextLogicalID = wantFileEnd, wantState.NextLogicalID
	}

	if len(touched) != 1 {
		t.Fatalf("sequential transactions touched %d slots, want 1", len(touched))
	}
	var pageCapacity, indexCapacity int
	for index := range committer.batches {
		batch := &committer.batches[index]
		pageCapacity += cap(batch.pages)
		indexCapacity += cap(batch.bufferIndexes)
		if index == int(firstSlot) {
			if cap(batch.pages) != maxPages || cap(batch.bufferIndexes) != 2 {
				t.Fatalf("warmed slot %d capacities = pages %d indexes %d, want %d and 2", index, cap(batch.pages), cap(batch.bufferIndexes), maxPages)
			}
		} else if cap(batch.pages) != 0 || cap(batch.bufferIndexes) != 0 {
			t.Fatalf("untouched slot %d capacities = pages %d indexes %d, want 0 and 0", index, cap(batch.pages), cap(batch.bufferIndexes))
		}
	}
	if pageCapacity != maxPages || indexCapacity != 2 {
		t.Fatalf("aggregate retained capacity = pages %d indexes %d, want %d and 2", pageCapacity, indexCapacity, maxPages)
	}

	options := WriteTransactionOptions{
		StoreID: testStoreID, Generation: generations + 1, PageSize: pageSize,
		FileEnd: fileEnd, NextLogicalID: nextLogicalID,
	}
	if allocs := testing.AllocsPerRun(20, func() {
		if err := tx.Reset(committer, cache, maxPages, options); err != nil {
			panic(err)
		}
		if err := tx.Abort(); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("warmed WriteTransaction Reset/Abort allocations = %g, want 0", allocs)
	}
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	root, gotState, _, err := RecoverInlineStateRoot(file, pageSize, make([]byte, int(pageSize)))
	if err != nil || root.Generation != generations || gotState.Generation != generations {
		t.Fatalf("final root after Close = root=%+v state=%+v err=%v", root, gotState, err)
	}
}
