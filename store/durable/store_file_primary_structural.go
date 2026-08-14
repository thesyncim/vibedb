package durable

import (
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// Bounded structural transactions split full leaves and remove empty leaves.
// Each is one write transaction that rebuilds exactly one tablet's
// anchor pages, local-ID locator, and tablet root from the tablet's current
// leaf set, then rewrites the catalog path and state root and publishes one
// generation, retiring the predecessors. The resident router is rebuilt from
// the freshly published graph; a concurrent reader that observes the new state
// before the swap completes falls back to the rooted page-walk oracle through
// the existing generation guard, so the swap is race-free.
//
// The tablet is rebuilt wholesale, including all its anchor pages. At the
// current leaf-level scale a tablet holds one anchor page, so the rebuild
// rewrites the same pages a surgical edit would. Because slot movement is
// confined to this one bounded transaction, exact-index posting tiles for
// moved rows are rebuilt in the same publication.
const (
	// primaryStructuralRetryLimit bounds split re-attempts after a full leaf.
	// A lexical-median split of a full leaf leaves both halves with ample
	// slack, so one split makes room; the small bound only guards a pathological
	// placement cycle rather than looping forever.
	primaryStructuralRetryLimit = 16
)

// ErrPrimaryMacroSplitRequired reports that a structural transaction cannot
// proceed because the tablet's 4096 local IDs or its 16 anchor pages are
// exhausted, so a macro-tablet split (the next phase) is required. It is
// counted in Stats.PrimaryMacroSplitRequired.
var ErrPrimaryMacroSplitRequired = errors.New(
	"vibedb: primary macro-tablet split required",
)

type primaryStructuralKind uint8

const (
	structuralSplit primaryStructuralKind = iota
	structuralEmptyReclaim
)

// structuralLeaf is one enumerated current leaf of a tablet: its stable
// identity plus the physical handle, anchor page, and lexical fence needed to
// re-encode the tablet. fence is an owned copy because the source anchor page is
// retired in the same transaction.
type structuralLeaf struct {
	bucket  storeio.BucketID
	localID uint16
	pageID  uint8
	fence   []byte
	ref     storeio.PageRef
	zone    storeio.BucketZone
}

// structuralRepairPostingsHook repairs the exact-index postings for rows a
// structural transaction removes from the tablet. A split reassigns slots within
// re-encoded leaves; every such leaf's new posting contribution is captured in
// encodeStructuralLeaf as it is fitted. What that capture cannot see is an
// emptied bucket that vanishes entirely, so this hook records it.
// prepareStructuralExactLocked then drops the removed and re-encoded buckets'
// tiles and merges the captured contributions,
// and stagePrimaryExactPagesLocked writes the result inside the same bounded
// transaction that rebuilds the tablet, so the postings and the graph publish in
// one atomic generation.
func (c *Collection) structuralRepairPostingsHook(removed []storeio.BucketID) {
	if !c.primaryExactActive() {
		return
	}
	for _, bucket := range removed {
		c.recordStructuralRemovedBucketLocked(bucket)
	}
}

func updateMaxU64(dst *atomic.Uint64, value uint64) {
	for {
		current := dst.Load()
		if value <= current || dst.CompareAndSwap(current, value) {
			return
		}
	}
}

func (c *Collection) recordStructuralLatency(
	kind primaryStructuralKind, start time.Time,
) {
	ns := uint64(time.Since(start).Nanoseconds())
	switch kind {
	case structuralSplit:
		c.primaryLeafSplits.Add(1)
		updateMaxU64(&c.primarySplitMaxNS, ns)
	case structuralEmptyReclaim:
		c.primaryEmptyReclaims.Add(1)
		updateMaxU64(&c.primaryEmptyReclaimMaxNS, ns)
	}
}

// flushPendingForStructural materializes any buffered pending parents so the
// sealed graph a structural transaction rebuilds reflects every acknowledged
// mutation for the tablet. This is the "flush the tablet's pending parents"
// hook the split path requires; flushing all pending parents is a safe superset.
func (c *Collection) flushPendingForStructural() error {
	if c.deferredCanonicalLane() {
		// Full checkpoint (materialize + device flush), for two reasons. First, the
		// structural transaction rebuilds the tablet from the published graph and
		// then rebuilds the resident router from it, so every acknowledged canonical
		// edit must already be a real page or it is lost. Second, and load-bearing
		// for the retirement accounting: this advances durableState to a clean,
		// fully-durable baseline *before* the structural transaction retires any of
		// that baseline's pages. If the structural transaction then aborts, or a
		// structural attempt commits nothing, durableState still names live
		// pages -- never ones a half-done structural attempt already retired. The
		// commit itself finishes the job by flushing again afterward so durableState
		// advances past the structural generation too (see commitPrimaryStructural).
		//
		// Both deferred-canonical lanes need this: buffered-visible and the
		// journal-backed synchronous lane run the committer in manual-checkpoint
		// mode, so both leave durableState behind a plain inline publish. The sync
		// lane's per-mutation durability comes from its journal, but the durable
		// *root* it recovers from still lags until a checkpoint folds it forward.
		return c.checkpointBufferedLocked()
	}
	return nil
}

// enumerateTabletLeaves walks a tablet's anchor pages in lexical order and
// copies each current leaf's identity, handle, and fence. The first leaf's
// fence is the empty tablet floor.
func (c *Collection) enumerateTabletLeaves(
	tablet *storeio.GlobalTabletCatalogTabletRootView,
) ([]structuralLeaf, error) {
	leaves := make([]structuralLeaf, 0, 256)
	for anchorRank := 0; anchorRank < tablet.AnchorCount(); anchorRank++ {
		route, ok := tablet.AnchorAt(anchorRank)
		if !ok {
			return nil, storeio.ErrGlobalTabletCatalogCorrupt
		}
		lease, err := c.cache.Acquire(route.Ref)
		if err != nil {
			return nil, err
		}
		anchor := storeio.AdmittedGlobalTabletCatalogAnchor(
			lease.Page(), tablet, route.PageID,
		)
		count := anchor.Count()
		for rank := 0; rank < count; rank++ {
			r, rok := anchor.RouteAt(rank, 0)
			fence, fok := anchor.FenceAt(rank)
			_, localID, lok := storeio.SplitTabletLocalIdentityBucket(
				uint32(r.Bucket),
			)
			if !rok || !fok || !lok {
				lease.Release()
				return nil, storeio.ErrSegmentedTabletRouterCorrupt
			}
			leaves = append(leaves, structuralLeaf{
				bucket: r.Bucket, localID: uint16(localID), pageID: r.PageID,
				fence: fence, ref: r.Ref, zone: r.Zone,
			})
		}
		lease.Release()
	}
	if len(leaves) == 0 || len(leaves[0].fence) != 0 {
		return nil, storeio.ErrSegmentedTabletRouterCorrupt
	}
	return leaves, nil
}

// structuralFreeLocalID returns the smallest local ID not currently bound in
// the tablet, or false when all 4096 are live (a macro-tablet split trigger).
func structuralFreeLocalID(leaves []structuralLeaf) (uint16, bool) {
	var used [storeio.TabletLocalIdentityLocalCount / 64]uint64
	for i := range leaves {
		id := leaves[i].localID
		used[id>>6] |= uint64(1) << (id & 63)
	}
	for id := 0; id < storeio.TabletLocalIdentityLocalCount; id++ {
		if used[id>>6]&(uint64(1)<<uint(id&63)) == 0 {
			return uint16(id), true
		}
	}
	return 0, false
}

// structuralIndexOfBucket returns the enumeration index of bucket, or -1.
func structuralIndexOfBucket(
	leaves []structuralLeaf, bucket storeio.BucketID,
) int {
	for i := range leaves {
		if leaves[i].bucket == bucket {
			return i
		}
	}
	return -1
}

// appendLeafRecords appends a leaf's rows in lexical order to dst as
// build-and-mutate records. Keys and inline bytes borrow the leaf page, so the
// caller must keep the page leased through the re-encode.
func appendLeafRecords(
	dst []storeio.CommonPrimaryLeafRecord, leaf *storeio.CommonPrimaryLeafView,
) []storeio.CommonPrimaryLeafRecord {
	it := leaf.AllRows()
	for {
		row, ok := it.Next()
		if !ok {
			break
		}
		dst = append(dst, storeio.CommonPrimaryLeafRecord{
			Key: row.Key, Value: row.Value,
		})
	}
	return dst
}

func (c *Collection) extractLeafRecords(
	leaf *storeio.CommonPrimaryLeafView,
) []storeio.CommonPrimaryLeafRecord {
	c.structuralRows = appendLeafRecords(c.structuralRows[:0], leaf)
	return c.structuralRows
}

// fitStructuralLeaf places and encodes rows into the sole class-5 grammar and
// returns the planner-selected extent. Splits may assign fresh
// stable slots because their exact-index contribution is rebuilt in the same
// structural publication.
func (c *Collection) fitStructuralLeaf(
	tx *storeio.WriteTransaction,
	generation uint64,
	bucket storeio.BucketID,
	rows []storeio.CommonPrimaryLeafRecord,
) (int, error) {
	if len(rows) <= storeio.CommonPrimaryLeafWideSlots {
		if err := storeio.PlaceCommonPrimaryLeafRecords(
			storeio.CommonPrimaryLeafWide, c.storeID, rows,
		); err != nil {
			if errors.Is(err, storeio.ErrCommonPrimaryLeafNeedsWide) ||
				errors.Is(err, storeio.ErrCommonPrimaryLeafFull) {
				return 0, errors.Join(
					ErrPrimaryLeafSplitRequired,
					storeio.ErrCommonPrimaryLeafFull,
				)
			}
			return 0, err
		}
	}
	image, err := storeio.EncodeBestCompactPrimaryStripe(
		c.primaryLeafScratch,
		storeio.CommonPrimaryLeafHeader{
			StoreID: c.storeID, Generation: generation, Bucket: bucket,
		},
		c.storeID, rows, c.primaryUnifiedBuilder,
	)
	if errors.Is(err, storeio.ErrCommonPrimaryLeafFull) {
		return 0, errors.Join(
			ErrPrimaryLeafSplitRequired, storeio.ErrCommonPrimaryLeafFull,
		)
	}
	if err != nil {
		return 0, err
	}
	return len(image), nil
}

// encodeStructuralLeaf fits rows for bucket, allocates one leaf page of the
// chosen extent, copies the encoded image, and stages it.
func (c *Collection) encodeStructuralLeaf(
	tx *storeio.WriteTransaction,
	generation uint64,
	bucket storeio.BucketID,
	rows []storeio.CommonPrimaryLeafRecord,
) (storeio.PageRef, error) {
	extent, err := c.fitStructuralLeaf(tx, generation, bucket, rows)
	if err != nil {
		return storeio.PageRef{}, err
	}
	// Capture this re-encoded leaf's exact-index contribution from the fitted
	// image before the scratch is reused, so the structural transaction can
	// rebuild the affected postings atomically with the tablet.
	if err := c.accumulateStructuralLeafLocked(
		bucket, c.primaryLeafScratch[:extent],
		storeio.CommonPrimaryLeafBounds{
			FileEnd:           tx.FileEnd(),
			NextLogicalID:     tx.NextLogicalID(),
			AllocationQuantum: uint32(c.options.PageSize),
		},
	); err != nil {
		return storeio.PageRef{}, err
	}
	logicalID, ok := storeio.CommonPrimaryLeafLogicalID(bucket)
	if !ok {
		return storeio.PageRef{}, storeio.ErrCommonPrimaryLeafCorrupt
	}
	page, err := tx.Allocate(
		storeio.PagePrimaryLeaf, uint32(extent), logicalID,
	)
	if err != nil {
		return storeio.PageRef{}, err
	}
	copy(page.Bytes(), c.primaryLeafScratch[:extent])
	if err := page.Stage(); err != nil {
		return storeio.PageRef{}, err
	}
	return page.Ref(), nil
}

// structuralLeafStager stages the new leaf page(s) of a structural transaction
// inside tx and returns the tablet's final leaf set in lexical order (leaf 0
// with the empty floor) plus every leaf page superseded or removed.
type structuralLeafStager func(tx *storeio.WriteTransaction) (
	finalLeaves []storeio.SegmentedTabletRouterLeaf,
	retiredLeaves []storeio.PageRef,
	err error,
)

// commitPrimaryStructural runs one bounded structural transaction: it stages the
// caller's new leaves, rebuilds the tablet's anchor pages/locator/root from
// finalLeaves, rewrites the catalog path and state root, retires predecessors,
// publishes one generation, and rebuilds the resident router.
func (c *Collection) commitPrimaryStructural(
	state *fileStoreState,
	path *filePrimaryMutationPath,
	kind primaryStructuralKind,
	stage structuralLeafStager,
) (err error) {
	start := time.Now()
	generation := state.root.Generation + 1
	if generation == 0 || generation >= uint64(1)<<48 {
		return storeio.ErrGenerationOrder
	}
	tabletID := path.tablet.TabletID()

	if err := c.refreshReusableFor(
		state, c.options.maxTransactionPages, c.options.freeFoldLimit,
	); err != nil {
		return err
	}
	tx, err := c.beginWriteTransaction(
		c.options.maxTransactionPages,
		storeio.WriteTransactionOptions{
			StoreID: c.storeID, Generation: generation,
			PageSize:         uint32(c.options.PageSize),
			FileEnd:          state.fileEnd,
			NextLogicalID:    state.root.NextLogicalID,
			Reusable:         c.reusable,
			ReuseJournal:     c.reuseJournal,
			ReusableIndex:    &c.freeExtentIndex,
			ReusablePromoter: c.reusableExtentPromoter(),
		},
	)
	if err != nil {
		return err
	}
	abort := true
	retirementReserved := false
	defer func() {
		if abort {
			if retirementReserved {
				_ = c.reclaimer.CancelRetiredGeneration(
					state.root.Generation,
				)
			}
			err = errors.Join(err, tx.Abort())
		}
	}()
	c.retireScratch = c.retireScratch[:0]
	c.retireRefScratch = c.retireRefScratch[:0]
	c.resetStructuralExactLocked()

	finalLeaves, retiredLeaves, err := stage(tx)
	if err != nil {
		return err
	}
	if len(finalLeaves) == 0 || len(finalLeaves[0].Fence) != 0 {
		return storeio.ErrSegmentedTabletRouterCorrupt
	}
	pageCount := (len(finalLeaves) + storeio.SegmentedTabletRouterRowsPerPage - 1) /
		storeio.SegmentedTabletRouterRowsPerPage
	if pageCount == 0 || pageCount > storeio.SegmentedTabletRouterMaxPages {
		c.primaryMacroSplitRequired.Add(1)
		return ErrPrimaryMacroSplitRequired
	}

	// Allocate every anchor page (COW copies of the live stable IDs, fresh for
	// any trailing IDs the leaf set now needs), then the locator and tablet
	// root, before encoding so bounds cover every embedded ref.
	oldAnchorRefs := make([]storeio.PageRef, path.tablet.AnchorCount())
	for rank := 0; rank < path.tablet.AnchorCount(); rank++ {
		route, ok := path.tablet.AnchorAt(rank)
		if !ok || int(route.PageID) >= len(oldAnchorRefs) {
			return storeio.ErrGlobalTabletCatalogCorrupt
		}
		oldAnchorRefs[route.PageID] = route.Ref
	}
	anchorPages := make([]storeio.TransactionPage, pageCount)
	anchorRefs := make([]storeio.PageRef, pageCount)
	for pageID := 0; pageID < pageCount; pageID++ {
		logicalID, ok := storeio.GlobalTabletCatalogAnchorLogicalID(
			tabletID, uint8(pageID),
		)
		if !ok {
			return storeio.ErrGlobalTabletCatalogCorrupt
		}
		var page storeio.TransactionPage
		var allocErr error
		if pageID < len(oldAnchorRefs) &&
			oldAnchorRefs[pageID] != (storeio.PageRef{}) {
			page, allocErr = tx.AllocateNear(
				storeio.PagePrimaryAnchor,
				storeio.SegmentedTabletRouterAnchorPageBytes,
				logicalID, oldAnchorRefs[pageID].Offset,
			)
		} else {
			page, allocErr = tx.Allocate(
				storeio.PagePrimaryAnchor,
				storeio.SegmentedTabletRouterAnchorPageBytes,
				logicalID,
			)
		}
		if allocErr != nil {
			return allocErr
		}
		anchorPages[pageID] = page
		anchorRefs[pageID] = page.Ref()
	}

	oldLocator, ok := path.tablet.LocatorRef()
	if !ok {
		return storeio.ErrGlobalTabletCatalogCorrupt
	}
	locatorLogical, ok := storeio.GlobalTabletCatalogLocatorLogicalID(tabletID)
	if !ok {
		return storeio.ErrGlobalTabletCatalogCorrupt
	}
	locatorPage, err := tx.AllocateNear(
		storeio.PagePrimaryLocator, storeio.GlobalTabletCatalogLocatorBytes,
		locatorLogical, oldLocator.Offset,
	)
	if err != nil {
		return err
	}
	tabletPage, err := tx.AllocateNear(
		storeio.PageTabletRoute, storeio.GlobalTabletCatalogTabletBytes,
		path.tabletRoute.Ref.LogicalID, path.tabletRoute.Ref.Offset,
	)
	if err != nil {
		return err
	}

	header := storeio.SegmentedTabletRouterHeader{
		StoreID: c.storeID, TabletID: tabletID, Generation: generation,
		AnchorKind: storeio.PagePrimaryAnchor, LeafKind: storeio.PagePrimaryLeaf,
	}
	rawRoot := make([]byte, storeio.SegmentedTabletRouterRootBytes)
	rawLocator := make([]byte, storeio.SegmentedTabletRouterLocatorBytes)
	rawAnchors := make(
		[]byte, pageCount*storeio.SegmentedTabletRouterAnchorPageBytes,
	)
	if _, _, _, _, err := storeio.EncodeSegmentedTabletRouter(
		rawRoot, rawLocator, rawAnchors, header, anchorRefs, finalLeaves,
	); err != nil {
		return err
	}
	for pageID := 0; pageID < pageCount; pageID++ {
		offset := pageID * storeio.SegmentedTabletRouterAnchorPageBytes
		copy(
			anchorPages[pageID].Bytes(),
			rawAnchors[offset:offset+storeio.SegmentedTabletRouterAnchorPageBytes],
		)
		if err := anchorPages[pageID].Stage(); err != nil {
			return err
		}
	}

	locatorEntries := make(
		[]storeio.GlobalTabletCatalogLocatorEntry, len(finalLeaves),
	)
	for rank := range finalLeaves {
		locatorEntries[rank] = storeio.GlobalTabletCatalogLocatorEntry{
			LocalID: finalLeaves[rank].LocalID,
			PageID:  uint8(rank / storeio.SegmentedTabletRouterRowsPerPage),
			RowSlot: uint8(rank % storeio.SegmentedTabletRouterRowsPerPage),
			State:   storeio.GlobalTabletCatalogLocatorLive,
		}
	}
	sort.Slice(locatorEntries, func(i, j int) bool {
		return locatorEntries[i].LocalID < locatorEntries[j].LocalID
	})
	bounds := c.primaryMutationBounds(tx)
	if _, err := storeio.EncodeGlobalTabletCatalogLocatorPage(
		locatorPage.Bytes(), c.storeID, generation, tabletID,
		generation, bounds, locatorEntries,
	); err != nil {
		return err
	}
	if err := locatorPage.Stage(); err != nil {
		return err
	}
	if _, err := storeio.EncodeGlobalTabletCatalogTabletRoot(
		tabletPage.Bytes(),
		storeio.PageHeader{
			StoreID: c.storeID, Generation: generation,
			LogicalID: tabletPage.Ref().LogicalID,
			PageSize:  storeio.GlobalTabletCatalogTabletBytes,
			PayloadLength: storeio.GlobalTabletCatalogRootHeader +
				storeio.SegmentedTabletRouterRootBytes,
			Kind: storeio.PageTabletRoute,
		},
		bounds, locatorPage.Ref(), rawRoot,
	); err != nil {
		return err
	}
	if err := tabletPage.Stage(); err != nil {
		return err
	}

	catalogPage, err := tx.AllocateNear(
		storeio.PagePrimaryCatalog, storeio.GlobalTabletCatalogNodeBytes,
		path.catalogLease.Header().LogicalID, path.catalogRef.Offset,
	)
	if err != nil {
		return err
	}
	bounds = c.primaryMutationBounds(tx)
	if _, err := path.catalog.RewriteHandle(
		catalogPage.Bytes(), generation, bounds,
		path.tabletRoute.ID, tabletPage.Ref(),
	); err != nil {
		return err
	}
	if err := catalogPage.Stage(); err != nil {
		return err
	}

	childID := path.catalogRoute.ID
	childRef := catalogPage.Ref()
	if path.hasBranch {
		branchPage, allocateErr := tx.AllocateNear(
			storeio.PagePrimaryCatalog, storeio.GlobalTabletCatalogNodeBytes,
			path.branchLease.Header().LogicalID, path.branchRef.Offset,
		)
		if allocateErr != nil {
			return allocateErr
		}
		bounds = c.primaryMutationBounds(tx)
		if _, rewriteErr := path.branch.RewriteHandle(
			branchPage.Bytes(), generation, bounds,
			path.catalogRoute.ID, catalogPage.Ref(),
		); rewriteErr != nil {
			return rewriteErr
		}
		if stageErr := branchPage.Stage(); stageErr != nil {
			return stageErr
		}
		childID = path.rootRoute.ID
		childRef = branchPage.Ref()
	}

	rootPage, err := tx.AllocateNear(
		storeio.PagePrimaryCatalog, storeio.GlobalTabletCatalogRootBytes,
		state.root.PrimaryRoot.LogicalID, state.root.PrimaryRoot.Offset,
	)
	if err != nil {
		return err
	}
	bounds = c.primaryMutationBounds(tx)
	if _, err := path.root.RewriteHandle(
		rootPage.Bytes(), generation, bounds, childID, childRef,
	); err != nil {
		return err
	}
	if err := rootPage.Stage(); err != nil {
		return err
	}

	for _, ref := range retiredLeaves {
		if err := c.appendPrimaryRetirement(state, ref); err != nil {
			return err
		}
	}
	for _, ref := range oldAnchorRefs {
		if err := c.appendPrimaryRetirement(state, ref); err != nil {
			return err
		}
	}
	for _, ref := range [...]storeio.PageRef{
		oldLocator, path.tabletRoute.Ref, path.catalogRef,
	} {
		if err := c.appendPrimaryRetirement(state, ref); err != nil {
			return err
		}
	}
	if path.hasBranch {
		if err := c.appendPrimaryRetirement(state, path.branchRef); err != nil {
			return err
		}
	}
	if err := c.appendPrimaryRetirement(
		state, state.root.PrimaryRoot,
	); err != nil {
		return err
	}

	// Rebuild the affected exact-index postings from the leaves this transaction
	// re-encoded (captured in encodeStructuralLeaf) and the buckets it removed,
	// and stage them as durable pages in this same transaction so the postings
	// and the rearranged tablet publish in one atomic generation.
	preparedExact, prepareExactErr := c.prepareStructuralExactLocked(generation)
	if prepareExactErr != nil {
		return prepareExactErr
	}
	var exactRoot storeio.PageRef
	if preparedExact.active {
		exactRoot, err = c.stagePrimaryExactPagesLocked(
			tx, state, generation, preparedExact.epoch.exact,
		)
		if err != nil {
			return err
		}
	}

	freeLog, err := c.syncFreeLogFor(tx, state, c.options.freeFoldLimit)
	if err != nil {
		return fmt.Errorf(
			"vibedb: persist structural reusable extents: %w", err,
		)
	}
	nextState, nextInline, err := c.stagePrimaryState(
		tx, state, generation, rootPage.Ref(),
		freeLog.head, freeLog.inline,
		state.root.DocumentCount,
	)
	if err != nil {
		return err
	}
	if preparedExact.active {
		nextState.root.ExactIndexRoot = exactRoot
	}
	if err := c.reserveFileRetirements(); err != nil {
		return fmt.Errorf(
			"vibedb: reserve structural retirements: %w", err,
		)
	}
	retirementReserved = true

	c.snapshotGate.Lock()
	c.beginReaderFence()
	retiring := !c.anyActiveReaders()
	absorbedStart := len(c.retirementAbsorbed)
	if retiring {
		absorbed := c.retirementAbsorbed
		var extracted []storeio.FreeExtent
		extracted, err = tx.PublishInlineRetiring(
			nextState.root, nextInline,
			c.retireRefScratch, c.retireScratch,
			c.neverDurableRetirementOutput(),
		)
		c.retirementAbsorbed = absorbed[:len(extracted)]
	} else {
		err = tx.PublishInline(nextState.root, nextInline)
	}
	if err != nil {
		clear(c.retirementAbsorbed[absorbedStart:])
		c.retirementAbsorbed =
			c.retirementAbsorbed[:absorbedStart]
		c.endReaderFence()
		c.snapshotGate.Unlock()
		return err
	}
	abort = false
	c.installPrimaryExactResidentLocked(preparedExact)
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	if retiring {
		c.cache.MarkUnreachable(c.retireRefScratch)
		c.extractNeverDurableRetirements(absorbedStart)
	}
	c.endReaderFence()
	c.snapshotGate.Unlock()
	c.finalizeReusable()
	c.commitFreeLog(freeLog)
	c.inlineFree = nextInline

	// Rebuild the resident router from the freshly published graph. Until the
	// swap lands, a concurrent reader that already sees the new state observes a
	// generation mismatch and routes through the rooted page-walk oracle.
	newRouter, buildErr := storeio.BuildResidentPrimaryRouter(
		c.cache, nextState.root.PrimaryRoot,
		storeio.GlobalTabletCatalogBounds{
			StoreID: c.storeID, SelectedRootGeneration: generation,
			FileEnd:       nextState.fileEnd,
			NextLogicalID: nextState.root.NextLogicalID,
		},
	)
	if buildErr != nil {
		return buildErr
	}
	c.primaryRouter.Store(newRouter)
	c.recordStructuralLatency(kind, start)

	// A deferred-canonical structural transaction is a checkpoint, not a volatile
	// edit. Ordinary deferred-canonical mutations copy-on-write into memory-only
	// virtual extents and defer every real retirement to the next materialize; a
	// structural transaction instead allocates and retires REAL durable-graph pages
	// (anchors, locator, tablet root, catalog path, and the primary root). If it
	// only PublishInline'd like an ordinary put, durableState — the crash-recovery
	// baseline — would stay behind still referencing the pages this transaction
	// just retired, and the next materialize would load that stale base and retire
	// its root a second time (the "overlapping retired extent" defect); the
	// following structural transaction, re-planning against that stale durable
	// root, would also stage a tablet root the fresh bounds reject ("cacheable
	// tablet-root identity"). Flushing here advances durableState past the
	// structural generation so its retirements are durably superseded and never
	// re-collected. This holds for both deferred-canonical lanes: buffered-visible
	// and the journal-backed synchronous lane both run the committer in
	// manual-checkpoint mode and both publish structural pages inline, so both must
	// force the durable root forward here — the sync lane's journal makes each
	// mutation durable but does not by itself advance the recovered root. Structural
	// transactions are amortized rare (one per full/empty leaf), so this device
	// flush is not on the steady-state mutation path.
	if c.deferredCanonicalLane() {
		return c.flushPublishedPhysicalLocked()
	}
	return nil
}

// structuralSplitPrimaryLeaf splits the leaf routed by keyBytes at its lexical
// median into two leaves: the left keeps the source BucketID, the right takes a
// freshly allocated local ID. It flushes buffered pending parents first, then
// commits one bounded structural transaction. The caller retries the original
// mutation, which then routes into whichever half has room.
func (c *Collection) structuralSplitPrimaryLeaf(keyBytes []byte) error {
	if err := c.flushPendingForStructural(); err != nil {
		return err
	}
	state := c.state.Load()
	if state == nil {
		return ErrClosed
	}
	route, err := c.currentPrimaryResidentRoute(state, keyBytes)
	if err != nil {
		return err
	}
	var path filePrimaryMutationPath
	if err := c.acquirePrimaryRoutingPath(
		&path, state, keyBytes, route,
	); err != nil {
		return err
	}
	defer path.Release()
	current, err := c.enumerateTabletLeaves(&path.tablet)
	if err != nil {
		return err
	}
	sourceIndex := structuralIndexOfBucket(current, route.Bucket)
	if sourceIndex < 0 {
		return storeio.ErrSegmentedTabletRouterCorrupt
	}
	tabletID := path.tablet.TabletID()
	generation := state.root.Generation + 1
	return c.commitPrimaryStructural(
		state, &path, structuralSplit,
		func(tx *storeio.WriteTransaction) (
			[]storeio.SegmentedTabletRouterLeaf, []storeio.PageRef, error,
		) {
			stripe, ok := storeio.AdmittedCompactPrimaryStripe(
				path.leafLease.Page(), c.storeID, route.Bucket,
			)
			if !ok {
				return nil, nil, storeio.ErrCommonPrimaryLeafCorrupt
			}
			rows, renderErr := stripe.RenderRecordsWithScratch(
				c.primaryLeafMutationScratch,
			)
			if renderErr != nil {
				return nil, nil, renderErr
			}
			if len(rows) < 2 {
				return nil, nil, fmt.Errorf(
					"%w: split needs at least two rows", storeio.ErrInvalidWrite,
				)
			}
			median := len(rows) / 2
			rightLocalID, ok := structuralFreeLocalID(current)
			if !ok {
				c.primaryMacroSplitRequired.Add(1)
				return nil, nil, ErrPrimaryMacroSplitRequired
			}
			rightBucketU, ok := storeio.MakeTabletLocalIdentityBucket(
				tabletID, uint32(rightLocalID),
			)
			if !ok {
				return nil, nil, storeio.ErrSegmentedTabletRouterCorrupt
			}
			rightBucket := storeio.BucketID(rightBucketU)
			fenceScratch := make([]byte, len(rows[median].Key))
			rightFence, fenceErr := storeio.ShortestPrimaryFence(
				fenceScratch, rows[median-1].Key, rows[median].Key,
			)
			if fenceErr != nil {
				return nil, nil, fenceErr
			}
			leftRef, encErr := c.encodeStructuralLeaf(
				tx, generation, route.Bucket, rows[:median],
			)
			if encErr != nil {
				return nil, nil, encErr
			}
			rightRef, encErr := c.encodeStructuralLeaf(
				tx, generation, rightBucket, rows[median:],
			)
			if encErr != nil {
				return nil, nil, encErr
			}
			// A split re-encodes both halves; their new posting contributions were
			// captured in encodeStructuralLeaf, and no bucket is removed.
			final := make(
				[]storeio.SegmentedTabletRouterLeaf, 0, len(current)+1,
			)
			for i := range current {
				ref := current[i].ref
				if i == sourceIndex {
					ref = leftRef
				}
				final = append(final, storeio.SegmentedTabletRouterLeaf{
					LocalID: current[i].localID, Fence: current[i].fence,
					Ref: ref, Zone: current[i].zone,
				})
				if i == sourceIndex {
					final = append(final, storeio.SegmentedTabletRouterLeaf{
						LocalID: rightLocalID,
						Fence:   append([]byte(nil), rightFence...),
						Ref:     rightRef,
					})
				}
			}
			return final, []storeio.PageRef{current[sourceIndex].ref}, nil
		},
	)
}

// structuralRemoveEmptyPrimaryLeaf removes the routed leaf after its last row
// was deleted. Non-empty leaf compaction is handled by the unified fold/repack
// planner, which sees the canonical encoded size for the whole dirty leaf.
func (c *Collection) structuralRemoveEmptyPrimaryLeaf(keyBytes []byte) error {
	empty, err := c.primaryLeafEmpty(keyBytes)
	if err != nil {
		return fmt.Errorf("inspect primary leaf: %w", err)
	}
	if !empty {
		return nil
	}
	if err := c.flushPendingForStructural(); err != nil {
		return fmt.Errorf("flush before empty-leaf reclaim: %w", err)
	}
	state := c.state.Load()
	if state == nil {
		return ErrClosed
	}
	route, err := c.currentPrimaryResidentRoute(state, keyBytes)
	if err != nil {
		return err
	}
	var path filePrimaryMutationPath
	if err := c.acquirePrimaryMutationPath(
		&path, state, keyBytes, route,
	); err != nil {
		return fmt.Errorf("acquire empty-leaf path: %w", err)
	}
	defer path.Release()
	current, err := c.enumerateTabletLeaves(&path.tablet)
	if err != nil {
		return fmt.Errorf("enumerate empty-leaf tablet: %w", err)
	}
	sourceIndex := structuralIndexOfBucket(current, route.Bucket)
	if sourceIndex < 0 {
		return storeio.ErrSegmentedTabletRouterCorrupt
	}
	if path.leaf.Len() != 0 || len(current) <= 1 {
		return nil
	}
	if err := c.commitRemoveEmptyLeaf(
		state, &path, current, sourceIndex,
	); err != nil {
		return fmt.Errorf("remove empty primary leaf: %w", err)
	}
	c.removePrimaryEmptyLeaf()
	return nil
}

// primaryLeafEmpty checks the resident compact image plus its pending overlay
// without flushing. Most deletes leave a non-empty leaf and stop here.
func (c *Collection) primaryLeafEmpty(keyBytes []byte) (bool, error) {
	state := c.state.Load()
	if state == nil {
		return false, ErrClosed
	}
	route, err := c.currentPrimaryResidentRoute(state, keyBytes)
	if err != nil {
		return false, nil
	}
	lease, err := c.primaryRouter.Load().AcquireLeaf(c.cache, route)
	if err != nil {
		return false, err
	}
	view, ok := storeio.AdmittedCompactPrimaryStripe(
		lease.Page(), c.storeID, route.Bucket,
	)
	if !ok {
		lease.Release()
		return false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	sealedRows := view.Len()
	lease.Release()
	_, rowDelta := c.primaryUnifiedOverlay.pendingBucketDeltas(route.Bucket)
	live := sealedRows + rowDelta
	if live < 0 {
		return false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	return live == 0, nil
}

// commitRemoveEmptyLeaf drops an empty leaf from the tablet, moving no rows and
// freeing its local ID. The new leftmost leaf inherits the empty tablet floor.
func (c *Collection) commitRemoveEmptyLeaf(
	state *fileStoreState,
	path *filePrimaryMutationPath,
	current []structuralLeaf,
	sourceIndex int,
) error {
	return c.commitPrimaryStructural(
		state, path, structuralEmptyReclaim,
		func(tx *storeio.WriteTransaction) (
			[]storeio.SegmentedTabletRouterLeaf, []storeio.PageRef, error,
		) {
			// The emptied bucket vanishes; drop its (already empty) postings.
			c.structuralRepairPostingsHook(
				[]storeio.BucketID{current[sourceIndex].bucket},
			)
			final := make(
				[]storeio.SegmentedTabletRouterLeaf, 0, len(current)-1,
			)
			for i := range current {
				if i == sourceIndex {
					continue
				}
				fence := current[i].fence
				if len(final) == 0 {
					fence = nil
				}
				final = append(final, storeio.SegmentedTabletRouterLeaf{
					LocalID: current[i].localID, Fence: fence,
					Ref: current[i].ref, Zone: current[i].zone,
				})
			}
			return final, []storeio.PageRef{current[sourceIndex].ref}, nil
		},
	)
}

// putPrimaryWithSplit is the primary Put entry point. It attempts the ordinary
// mutation and, when the routed leaf is full, performs the bounded split
// structural transaction and retries. A lexical-median split makes room, so the
// retry succeeds; the small bound guards against a pathological placement loop.
func (c *Collection) putPrimaryWithSplit(
	key []byte, src []byte,
) (created bool, err error) {
	for attempt := 0; ; attempt++ {
		created, err = c.putPrimary(key, src)
		if !errors.Is(err, ErrPrimaryLeafSplitRequired) {
			if errors.Is(err, storeio.ErrCommonPrimaryLeafFull) {
				return created, fmt.Errorf(
					"put primary attempt %d without split signal: %w", attempt, err,
				)
			}
			return created, err
		}
		if attempt >= primaryStructuralRetryLimit {
			return false, err
		}
		if splitErr := c.splitPrimaryLeafForKey(key); splitErr != nil {
			return false, errors.Join(
				err, fmt.Errorf("split primary after attempt %d: %w", attempt, splitErr),
			)
		}
	}
}

// splitPrimaryLeafForKey runs one split structural transaction for the leaf
// routed by key under the single-writer lock, waiting for durability when
// synchronous.
func (c *Collection) splitPrimaryLeafForKey(key []byte) (err error) {
	c.writer.Lock()
	var generation uint64
	defer func() {
		// The journal-backed sync lane makes no chain-fence acknowledgement: a
		// structural rewrite changes no logical key/value, so recovery
		// reconstructs it by replaying the triggering Put/Delete through the
		// ordinary mutation path. A synchronous store without an open journal
		// still waits on the root fence.
		wait := generation != 0 && c.chainFenceSync()
		if wait {
			c.durabilityWait.Add(1)
		}
		c.writer.Unlock()
		if wait {
			err = errors.Join(err, c.waitPublished(generation))
			c.durabilityWait.Done()
		}
	}()
	if c.closed {
		return ErrClosed
	}
	if failure := c.PersistenceError(); failure != nil {
		return failure
	}
	state := c.state.Load()
	if state == nil || state.root.PrimaryRoot == (storeio.PageRef{}) {
		return ErrClosed
	}
	before := state.root.Generation
	// key is borrowed for the call; structuralSplitPrimaryLeaf copies it where
	// it persists, so pass it directly with no scratch round-trip.
	if err := c.structuralSplitPrimaryLeaf(key); err != nil {
		return err
	}
	if after := c.state.Load(); after != nil && after.root.Generation > before {
		generation = after.root.Generation
	}
	return nil
}

// deletePrimaryWithEmptyReclaim is the primary Delete entry point. After a
// successful delete empties a leaf, it best-effort removes that leaf without
// affecting the already-published delete if structural cleanup fails.
func (c *Collection) deletePrimaryWithEmptyReclaim(
	key []byte,
) (deleted bool, err error) {
	deleted, err = c.deletePrimary(key)
	if err != nil || !deleted {
		return deleted, err
	}
	// Class-5 Delete marks the resident route before returning when its pending
	// row count reaches zero. Healthy fixed-live-set churn therefore avoids a
	// second writer-lock acquisition, route lookup, and leaf admission entirely;
	// only eager empty-leaf removal pays post-delete structural hygiene.
	if c.primaryEmptyLeaves.Load() == 0 {
		return true, nil
	}
	_ = c.removeEmptyPrimaryLeafForKey(key)
	return deleted, nil
}

// removeEmptyPrimaryLeafForKey removes the leaf routed by key under the
// single-writer lock, waiting for durability when synchronous.
func (c *Collection) removeEmptyPrimaryLeafForKey(key []byte) (err error) {
	c.writer.Lock()
	var generation uint64
	defer func() {
		// The journal-backed sync lane makes no chain-fence acknowledgement: a
		// structural rewrite changes no logical key/value, so recovery
		// reconstructs it by replaying the triggering Put/Delete through the
		// ordinary mutation path. A synchronous store without an open journal
		// still waits on the root fence.
		wait := generation != 0 && c.chainFenceSync()
		if wait {
			c.durabilityWait.Add(1)
		}
		c.writer.Unlock()
		if wait {
			err = errors.Join(err, c.waitPublished(generation))
			c.durabilityWait.Done()
		}
	}()
	if c.closed {
		return ErrClosed
	}
	if failure := c.PersistenceError(); failure != nil {
		return failure
	}
	state := c.state.Load()
	if state == nil || state.root.PrimaryRoot == (storeio.PageRef{}) {
		return ErrClosed
	}
	before := state.root.Generation
	// key is borrowed for the call; structuralRemoveEmptyPrimaryLeaf copies it
	// where it persists, so pass it directly.
	if err := c.structuralRemoveEmptyPrimaryLeaf(key); err != nil {
		return err
	}
	if after := c.state.Load(); after != nil && after.root.Generation > before {
		generation = after.root.Generation
	}
	return nil
}
