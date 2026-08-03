package durable

import (
	"math"
	"os"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/internal/storemem"
	"github.com/thesyncim/vibedb/store"
)

// storeCommitterFactory constructs the persistence committer for a collection.
// It is a package variable so durability crash tests can interpose a
// fault-injecting Device between the committer and the platform backend;
// production keeps the platform committer.
var storeCommitterFactory = storeio.NewCommitter

// newCollectionResources builds the committer and cache for a collection.
// journalBacked is the recovered root contract on Open and the journal contract
// Create is about to mint. Buffered-visible and journal-backed synchronous
// stores defer root publication to explicit checkpoints; async-visible and a
// journal-less synchronous reopen must leave the committer automatic because
// their mutations publish through (and, for sync, wait on) its root fence.
func newCollectionResources(
	file *os.File, options normalizedFileStoreOptions, storeID [16]byte,
	journalBacked bool,
) (*Collection, error) {
	writeFile, directWrite, err := storeio.OpenPageCommitFile(file, storeio.DirectMode(options.WriteMode))
	if err != nil {
		return nil, err
	}
	committer, err := storeCommitterFactory(writeFile, storeio.DeviceOptions{
		Backend: storeio.Backend(options.Backend), BufferCount: options.BufferCount,
		BufferSize: max(options.MaxPageSize, os.Getpagesize()), QueueDepth: options.BufferCount,
		CheckpointSync: storeio.CheckpointSync(options.CheckpointStrength),
	}, storeio.CommitterOptions{
		FrameNativeStaging: true,
		QueueSlots:         options.QueueSlots, MaxPagesPerBatch: options.maxTransactionPages,
		GroupLimit: options.GroupLimit, CoalesceDelay: options.CommitCoalesce,
		MaterializationDamageGranule: uint32(options.MaterializationDamageGranule),
		ManualCheckpoint: options.Durability == DurabilityBufferedVisible ||
			options.Durability == DurabilitySync && journalBacked,
	})
	if err != nil {
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, err
	}
	readFile, directRead, err := storeio.OpenPageCacheFile(file, storeio.DirectMode(options.ReadMode))
	if err != nil {
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, err
	}
	pageValidator := newFileStorePageValidator(
		uint32(options.PageSize), uint32(len(options.indexes)),
	)
	cache, err := storeio.NewPageCache(readFile, storeio.PageCacheOptions{
		PageSize: options.PageSize, MaxPageSize: options.MaxPageSize,
		ResidentBytes: options.ResidentBytes -
			int64(options.primaryUnifiedOverlayBytes),
		StoreID:       storeID,
		PrefetchQueue: options.PrefetchQueue, ReadConcurrency: options.ReadConcurrency,
		ReadQueueDepth: options.ReadQueueDepth,
		Backend:        storeio.Backend(options.Backend),
		Validate:       pageValidator.validate,
	})
	if err != nil {
		if readFile != file {
			_ = readFile.Close()
		}
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, err
	}
	leases, err := storeio.NewGenerationLeases(storeio.GenerationLeaseOptions{MaxLeases: options.MaxSnapshotLeases})
	if err != nil {
		_ = cache.Close()
		if readFile != file {
			_ = readFile.Close()
		}
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, err
	}
	extentSize := int(unsafe.Sizeof(storeio.FreeExtent{}))
	// Keep one bounded handoff batch beyond the retirement-table capacity. When
	// a long-held snapshot is released, refreshReusable can drain the full table
	// into this reserve before the next transaction consumes those extents. The
	// old equal-sized arenas deadlocked at exactly that boundary: neither side
	// had a slot in which to move the first extent.
	reusableCapacity := options.MaxRetiredExtents +
		min(options.MaxRetiredExtents, freeReclaimBatch)
	freeExtentMaximaCapacity :=
		storeio.FreeExtentIndexCapacity(reusableCapacity)
	retiredIntervalIndexBytes :=
		storeio.RetiredIntervalIndexStorageBytes(options.MaxRetiredExtents)
	retiredExtentArenaBytes :=
		storeio.RetiredExtentStorageBytes(options.MaxRetiredExtents)
	if reusableCapacity > math.MaxInt/extentSize ||
		freeExtentMaximaCapacity > (math.MaxInt-reusableCapacity*extentSize)/8 ||
		retiredIntervalIndexBytes == 0 || retiredExtentArenaBytes == 0 {
		_ = leases.Close()
		_ = cache.Close()
		if readFile != file {
			_ = readFile.Close()
		}
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, store.ErrCheckpointTooLarge
	}
	reusableExtentBytes := reusableCapacity * extentSize
	reusableIndexBytes := freeExtentMaximaCapacity * 8
	reusableMetadataBytes := reusableExtentBytes + reusableIndexBytes
	if retiredExtentArenaBytes > math.MaxInt-reusableMetadataBytes ||
		retiredIntervalIndexBytes >
			math.MaxInt-reusableMetadataBytes-retiredExtentArenaBytes {
		_ = leases.Close()
		_ = cache.Close()
		if readFile != file {
			_ = readFile.Close()
		}
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, store.ErrCheckpointTooLarge
	}
	reusableBlock, err := storemem.Allocate(
		reusableMetadataBytes + retiredExtentArenaBytes +
			retiredIntervalIndexBytes,
	)
	if err != nil {
		_ = leases.Close()
		_ = cache.Close()
		if readFile != file {
			_ = readFile.Close()
		}
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, err
	}
	reusableArena := unsafe.Slice(
		(*storeio.FreeExtent)(unsafe.Pointer(unsafe.SliceData(reusableBlock.Bytes()))),
		reusableCapacity,
	)
	freeExtentMaxima := unsafe.Slice(
		(*uint64)(unsafe.Pointer(
			unsafe.SliceData(
				reusableBlock.Bytes()[reusableExtentBytes:reusableMetadataBytes],
			),
		)),
		freeExtentMaximaCapacity,
	)
	retiredExtentStorage := reusableBlock.Bytes()[reusableMetadataBytes : reusableMetadataBytes+retiredExtentArenaBytes]
	retiredIntervalIndexStorage := reusableBlock.Bytes()[reusableMetadataBytes+retiredExtentArenaBytes:]
	readEpochs := storeio.NewReadEpochs()
	reclaimer, err := storeio.NewExtentReclaimer(
		leases,
		storeio.ExtentReclaimerOptions{
			MaxRetiredExtents:    options.MaxRetiredExtents,
			Epochs:               readEpochs,
			IntervalIndexStorage: retiredIntervalIndexStorage,
			RetiredExtentStorage: retiredExtentStorage,
		},
	)
	if err != nil {
		_ = reusableBlock.Close()
		_ = leases.Close()
		_ = cache.Close()
		if readFile != file {
			_ = readFile.Close()
		}
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, err
	}
	var ownedRead *os.File
	if readFile != file {
		ownedRead = readFile
	}
	var ownedWrite *os.File
	if writeFile != file {
		ownedWrite = writeFile
	}
	pageSize := uint32(options.PageSize)
	deltaPerPage := storeio.FreeDeltaRecordCapacity(pageSize)
	imagePerPage := storeio.FreeImageRecordCapacity(pageSize)
	indexPerPage := storeio.FreeIndexRecordCapacity(pageSize)
	// The free set is capped at what the segment index can name, not at what one
	// image rewrite can hold. That is the whole change: the old cap was
	// FreeLogMaxImagePages*imagePerPage — sixteen pages, about 2,700 extents,
	// roughly 11 MiB of trackable free space at the 4 KiB page size — because a
	// fold rewrote the entire linked image and had to fit in one transaction.
	// At 4 KiB the index costs one page per 70 segments of 165 extents, so eight
	// index pages describe exactly 92,400 extents. The worst-case fold reserve
	// is 28 pages (8 index + 16 segment + 4 delta), and what must fit inside a
	// commit is a directory of the free set rather than the free set.
	//
	// A collection that fragments past this ceiling still stalls reclamation and
	// eventually fails writes with ErrRetiredExtentCapacity, exactly as before;
	// the ceiling is simply much further away, and raising it again
	// is a policy edit against FreeLogMaxIndexPages rather than a redesign.
	// Half the index's capacity, because the durable set now carries the fenced
	// extents as well as the reusable ones: a retirement is written down by the
	// commit that makes it, so both halves have to fit the same image.
	freeSetLimit := min(reusableCapacity,
		storeio.FreeLogMaxIndexPages*indexPerPage*imagePerPage/2)
	// How many segments an open reads before it stops. Everything past this stays
	// on disk until something needs it, which is what keeps open time a function
	// of the working set rather than of the free set: at 165 extents per segment a
	// store with ninety thousand free extents has five hundred and forty-six of
	// them, and reading all of them costs half a megabyte before the first write.
	//
	// The arena is the natural bound. A resident segment's extents live in
	// c.reusable, so residency can never usefully exceed what that holds, and
	// capping it there means a store configured for a small free set does not
	// read a large one. The floor of four is so that a fresh store, whose whole
	// free set is a handful of segments, behaves exactly as it did before.
	freeResidentBudget := max(4, freeSetLimit/imagePerPage)
	maxFreeSegments := storeio.FreeLogMaxIndexPages * indexPerPage
	freeFencedCapacity, freeImageScratchCapacity, ok :=
		checkedFileFreeScratchCounts(
			options.freeFoldLimit,
			imagePerPage,
			freeSetLimit,
			options.MaxRetiredExtents,
			options.maxTransactionPages,
		)
	if !ok {
		_ = reusableBlock.Close()
		_ = leases.Close()
		_ = cache.Close()
		if readFile != file {
			_ = readFile.Close()
		}
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, store.ErrCheckpointTooLarge
	}
	freeScratchBlock, freeScratch, err := newFileFreeScratch(
		freeFencedCapacity,
		freeImageScratchCapacity,
		options.freeFoldLimit,
		maxFreeSegments,
	)
	if err != nil {
		_ = reusableBlock.Close()
		_ = leases.Close()
		_ = cache.Close()
		if readFile != file {
			_ = readFile.Close()
		}
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, err
	}
	concurrentContexts := newPrimaryConcurrentContextPool(options)
	var concurrentStripes *[primaryConcurrentStripeCount]primaryConcurrentStripe
	if concurrentContexts != nil {
		concurrentStripes = new(
			[primaryConcurrentStripeCount]primaryConcurrentStripe,
		)
	}
	retireScratchCapacity := options.maxTransactionPages +
		fileStorePointPrimaryRetirePages + 1 +
		storeio.FreeLogMaxChainPages + storeio.FreeLogMaxIndexPages +
		options.freeFoldLimit
	collection := &Collection{
		file: file, options: options, storeID: storeID, committer: committer, cache: cache,
		primaryUnifiedOverlay: newLazyPrimaryUnifiedOverlay(
			options.primaryUnifiedOverlayBytes,
			options.primaryUnifiedOverlayBuckets,
			options.primaryUnifiedOverlayDirtyBytes,
			uint32(options.MaxPageSize),
			options.primaryUnifiedOverlayParentBytes,
		),
		readFile: ownedRead, writeFile: ownedWrite,
		directRead: directRead, directWrite: directWrite,
		leases: leases, readEpochs: readEpochs, reclaimer: reclaimer,
		// A fold retires the whole superseded chain on top of the commit's own
		// retirements, so the scratch reserves both.
		retireScratch:    make([]storeio.FreeExtent, 0, retireScratchCapacity),
		retireRefScratch: make([]storeio.PageRef, 0, retireScratchCapacity),
		reusable:         reusableArena[:0],
		reuseJournal:     make([]storeio.ReuseEdit, 0, options.maxTransactionPages),
		reusableBlock:    reusableBlock,
		freeExtentMaxima: freeExtentMaxima,
		freeScratchBlock: freeScratchBlock,
		pageValidator:    pageValidator,

		freeSegments:    make([]storeio.FreeSegment, 0, maxFreeSegments),
		freeNewSegments: make([]storeio.FreeSegment, 0, maxFreeSegments),
		freeIndexPages:  make([]storeio.PageRef, 0, storeio.FreeLogMaxIndexPages),
		freeNewIndex:    make([]storeio.PageRef, 0, storeio.FreeLogMaxIndexPages),
		freeDeltaPages:  make([]storeio.PageRef, 0, storeio.FreeLogMaxChainPages),
		freeNewDelta:    make([]storeio.PageRef, 0, storeio.FreeLogMaxDeltaPages),
		freeDirty:       make([]bool, 0, maxFreeSegments),
		freeResident:    make([]bool, 0, maxFreeSegments),
		freeReadBack:    make([]bool, 0, maxFreeSegments),
		freeNewResident: make([]bool, 0, maxFreeSegments),
		freeRetired:     make([]bool, 0, maxFreeSegments),
		freeFoldRanges:  freeScratch.ranges[:0],
		freeFoldOrder:   freeScratch.order[:0],
		freeFoldPages:   make([]storeio.TransactionPage, 0, options.freeFoldLimit),
		freeDirtyAll:    true,
		// A normal reclaim pulse retains every possible coalescing edit until the
		// commit selects a delta append or segmented fold. The separate delta
		// scratch also holds the transaction's reuse journal and complete
		// retirement set, so this decision allocates nothing on the Go heap.
		freePending: make([]storeio.FreeDelta, 0, freePendingCapacity),
		freeDeltas: make([]storeio.FreeDelta, 0,
			freePendingCapacity+options.maxTransactionPages+retireScratchCapacity),
		freeSpill: make([]storeio.FreeDelta, 0,
			storeio.InlineFreeDeltaCapacity),
		freeReclaimed:      make([]storeio.FreeExtent, 0, freeReclaimBatch),
		retirementAbsorbed: make([]storeio.FreeExtent, 0, freeReclaimBatch),
		// The fold image is the reusable set plus everything still fenced plus
		// what this commit just retired, so its scratch has to hold all three.
		freeFenced:         freeScratch.fenced[:0],
		freeImageScratch:   freeScratch.image[:0],
		freeAllocMark:      make([]uint32, freeSetLimit),
		freeSetLimit:       freeSetLimit,
		freeResidentBudget: freeResidentBudget,
		freeFoldLimit:      options.freeFoldLimit,
		freeDeltaPerPage:   deltaPerPage,
		freeImagePerPage:   imagePerPage,
		freeIndexPerPage:   indexPerPage,
		pendingVisible:     make([]filePendingState, fileVisibilitySlots(options.QueueSlots)),
		mutationCombiner: newPrimaryMutationCombiner(
			options.QueueSlots, options.MaxBatchDocuments, options.MaxBatchBytes,
		),
		primaryConcurrentContexts: concurrentContexts,
		primaryConcurrentStripes:  concurrentStripes,
	}
	if options.MaterializationDamageGranule != 0 {
		imageArenaBytes := options.MaxPageSize + options.PageSize
		block, allocateErr := storemem.Allocate(2 * imageArenaBytes)
		if allocateErr != nil {
			_ = collection.closeResources()
			return nil, allocateErr
		}
		collection.materializationBlock = block
		bytes := block.Bytes()
		collection.materializationBefore = bytes[:imageArenaBytes]
		collection.materializationAfter =
			bytes[imageArenaBytes : 2*imageArenaBytes]
	}
	if err := committer.SetCallbacks(
		collection.promoteDurableState,
		collection.poisonPersistence,
	); err != nil {
		_ = collection.closeResources()
		return nil, err
	}
	return collection, nil
}

func (c *Collection) beginWriteTransaction(
	maxPages int,
	options storeio.WriteTransactionOptions,
) (*storeio.WriteTransaction, error) {
	if err := c.writeTransaction.Reset(
		c.committer, c.cache, maxPages, options,
	); err != nil {
		return nil, err
	}
	return &c.writeTransaction, nil
}
