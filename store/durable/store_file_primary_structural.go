package durable

import (
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// Phase-8 bounded structural transactions: leaf split, merge, and slot-class
// reclaim. Each is one write transaction that rebuilds exactly one tablet's
// anchor pages, local-ID locator, and tablet root from the tablet's current
// leaf set, then rewrites the catalog path and state root and publishes one
// generation, retiring the predecessors. The resident router is rebuilt from
// the freshly published graph; a concurrent reader that observes the new state
// before the swap completes falls back to the rooted page-walk oracle through
// the existing generation guard, so the swap is race-free.
//
// The tablet is rebuilt wholesale (all its anchor pages) rather than by a
// surgical single-page insert. At the leaf-level scale this phase targets a
// tablet holds one anchor page, so the rebuild rewrites the same pages a
// surgical edit would; a surgical anchor-page insert is a later optimization
// measured before it is adopted. Because slot movement is confined to this one
// bounded transaction, invariant 6 holds: any exact-index posting tile for a
// moved row is rebuilt here. Exact indexes have not landed, so
// structuralRepairPostingsHook is a named no-op marking where that attaches.
const (
	// primaryStructuralRetryLimit bounds split re-attempts after a full leaf.
	// A lexical-median split of a full leaf leaves both halves with ample
	// slack, so one split makes room; the small bound only guards a pathological
	// placement cycle rather than looping forever.
	primaryStructuralRetryLimit = 4

	// primaryMergeFloor is ~25% of the narrow live *slot* capacity: the slot
	// conjunct of the capacity-relative merge floor (primaryLeafBelowMergeFloor).
	// It preserves the historical 48-of-195 threshold for tiny values -- the churn
	// geometry, where a narrow leaf is slot-limited -- while the paired byte
	// conjunct makes the floor relative to what the leaf can actually hold. The
	// band up to primaryMergeAbsorbCeiling is the hysteresis gap that stops a
	// just-merged leaf from immediately re-splitting.
	primaryMergeFloor = storeio.CommonPrimaryLeafNarrowLive / 4

	// primaryMergeAbsorbCeiling is ~75% of narrow live capacity. A neighbor may
	// absorb a merge only if the combined live count stays at or below it, so
	// the survivor always fits the narrow class comfortably.
	primaryMergeAbsorbCeiling = storeio.CommonPrimaryLeafNarrowLive * 3 / 4

	// primaryNarrowComfortable is ~70% of narrow live capacity. A wide leaf at
	// or below it is rewritten to the narrow class at split/merge evaluation
	// time to reclaim slot-class drift (the churn B/key regression).
	primaryNarrowComfortable = storeio.CommonPrimaryLeafNarrowLive * 7 / 10
)

// ErrPrimaryMacroSplitRequired reports that a structural transaction cannot
// proceed because the tablet's 4096 local IDs or its 16 anchor pages are
// exhausted, so a macro-tablet split (the next phase) is required. It is
// counted in Stats.PrimaryMacroSplitRequired.
var ErrPrimaryMacroSplitRequired = errors.New(
	"vibejson: primary macro-tablet split required",
)

type primaryStructuralKind uint8

const (
	structuralSplit primaryStructuralKind = iota
	structuralMerge
	structuralReclass
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

// structuralRepairPostingsHook is the named attachment point for exact-index
// posting-tile repair when rows change BucketID during a split or merge.
//
// Exact posting tiles have landed on the ordered graph (see
// store_file_primary_exact.go), but they are build-and-read: putPrimaryWithSplit
// and deletePrimaryWithMerge reject a mutation of an indexed ordered-primary
// collection with ErrPrimaryExactIndexReadOnly before any structural transaction
// can fire. A structural transaction is therefore never reached with active
// exact indexes, and this hook is a no-op guarded upstream rather than a live
// repair. When incremental (or bounded-rebuild-at-checkpoint) posting
// maintenance replaces that gate, the atomic per-tablet posting rebuild for the
// moved buckets attaches here, inside the one bounded structural transaction.
func (c *Collection) structuralRepairPostingsHook(moved []storeio.BucketID) {
	_ = moved
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
	case structuralMerge:
		c.primaryLeafMerges.Add(1)
		updateMaxU64(&c.primaryMergeMaxNS, ns)
	case structuralReclass:
		c.primaryLeafReclass.Add(1)
		updateMaxU64(&c.primaryReclassMaxNS, ns)
	}
}

// flushPendingForStructural materializes any buffered pending parents so the
// sealed graph a structural transaction rebuilds reflects every acknowledged
// mutation for the tablet. This is the "flush the tablet's pending parents"
// hook the split path requires; flushing all pending parents is a safe superset.
func (c *Collection) flushPendingForStructural() error {
	if c.buffered() {
		// Full checkpoint (materialize + device flush), for two reasons. First, the
		// structural transaction rebuilds the tablet from the published graph and
		// then rebuilds the resident router from it, so every acknowledged buffered
		// edit must already be a real page or it is lost. Second, and load-bearing
		// for the retirement accounting: this advances durableState to a clean,
		// fully-durable baseline *before* the structural transaction retires any of
		// that baseline's pages. If the structural transaction then aborts, or a
		// merge/reclass evaluation commits nothing, durableState still names live
		// pages -- never ones a half-done structural attempt already retired. The
		// commit itself finishes the job by flushing again afterward so durableState
		// advances past the structural generation too (see commitPrimaryStructural).
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

// fitStructuralLeaf places and encodes rows into c.primaryLeafScratch, choosing
// the narrow class when the rows fit and falling back to wide otherwise, and
// returns the chosen extent. Preferring narrow is the slot-class reclaim: a
// split half or merged survivor that fits narrow is rewritten narrow in the
// same structural transaction.
func (c *Collection) fitStructuralLeaf(
	tx *storeio.WriteTransaction,
	generation uint64,
	bucket storeio.BucketID,
	rows []storeio.CommonPrimaryLeafRecord,
) (int, error) {
	bounds := storeio.CommonPrimaryLeafBounds{
		FileEnd:           tx.FileEnd(),
		NextLogicalID:     tx.NextLogicalID(),
		AllocationQuantum: uint32(c.options.PageSize),
	}
	try := func(class storeio.CommonPrimaryLeafClass, extent int) (bool, error) {
		if err := storeio.PlaceCommonPrimaryLeafRecords(
			class, c.storeID, rows,
		); err != nil {
			if errors.Is(err, storeio.ErrCommonPrimaryLeafNeedsWide) ||
				errors.Is(err, storeio.ErrCommonPrimaryLeafFull) {
				return false, nil
			}
			return false, err
		}
		if _, err := storeio.EncodeCommonPrimaryLeaf(
			c.primaryLeafScratch[:extent], class,
			storeio.CommonPrimaryLeafHeader{
				StoreID: c.storeID, Generation: generation,
				Bucket: bucket, PageSize: uint32(extent),
			},
			c.storeID, rows, bounds,
		); err != nil {
			if errors.Is(err, storeio.ErrCommonPrimaryLeafNeedsWide) ||
				errors.Is(err, storeio.ErrCommonPrimaryLeafFull) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	if ok, err := try(
		storeio.CommonPrimaryLeafNarrow, storeio.CommonPrimaryLeafNarrowBytes,
	); err != nil {
		return 0, err
	} else if ok {
		return storeio.CommonPrimaryLeafNarrowBytes, nil
	}
	if ok, err := try(
		storeio.CommonPrimaryLeafWide, storeio.CommonPrimaryLeafWideBytes,
	); err != nil {
		return 0, err
	} else if ok {
		return storeio.CommonPrimaryLeafWideBytes, nil
	}
	return 0, errors.Join(
		ErrPrimaryLeafSplitRequired, storeio.ErrCommonPrimaryLeafFull,
	)
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
			FileEnd:          state.super.FileEnd,
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

	freeLog, err := c.syncFreeLogFor(tx, state, c.options.freeFoldLimit)
	if err != nil {
		return fmt.Errorf(
			"vibejson: persist structural reusable extents: %w", err,
		)
	}
	nextState, nextInline, err := c.stagePrimaryState(
		tx, state, generation, rootPage.Ref(),
		freeLog.head, freeLog.checksum, freeLog.inline,
		state.root.DocumentCount,
	)
	if err != nil {
		return err
	}
	if err := c.reserveFileRetirements(); err != nil {
		return fmt.Errorf(
			"vibejson: reserve structural retirements: %w", err,
		)
	}
	retirementReserved = true

	c.snapshotGate.Lock()
	retiring := !c.leases.AnyActive()
	if retiring {
		err = tx.PublishInlineRetiring(
			nextState.root, nextInline, c.retireRefScratch,
		)
	} else {
		err = tx.PublishInline(nextState.root, nextInline)
	}
	if err != nil {
		c.snapshotGate.Unlock()
		return err
	}
	abort = false
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	if retiring {
		c.cache.MarkUnreachable(c.retireRefScratch)
	}
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
			FileEnd:       nextState.super.FileEnd,
			NextLogicalID: nextState.root.NextLogicalID,
		},
	)
	if buildErr != nil {
		return buildErr
	}
	c.primaryRouter.Store(newRouter)
	c.recordStructuralLatency(kind, start)

	// A buffered structural transaction is a checkpoint, not a volatile edit.
	// Ordinary buffered mutations copy-on-write into memory-only virtual extents
	// and defer every real retirement to the next materialize; a structural
	// transaction instead allocates and retires REAL durable-graph pages
	// (anchors, locator, tablet root, catalog path, and the primary root). If it
	// only PublishInline'd like a buffered put, durableState — the crash-recovery
	// baseline — would stay behind still referencing the pages this transaction
	// just retired, and the next materialize would load that stale base and retire
	// its root a second time (the "overlapping retired extent" defect). Flushing
	// here advances durableState past the structural generation so its retirements
	// are durably superseded and never re-collected. Structural transactions are
	// amortized rare (one per full/empty leaf), so this device flush is not on the
	// steady-state mutation path.
	if c.buffered() {
		if flushErr := c.committer.Flush(); flushErr != nil {
			return flushErr
		}
		c.cache.MarkDurable(c.committer.DurableGeneration())
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
	if err := c.acquirePrimaryMutationPath(
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
			rows := c.extractLeafRecords(&path.leaf)
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
			c.structuralRepairPostingsHook([]storeio.BucketID{rightBucket})
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

// primaryLeafBelowMergeFloor is the capacity-relative merge floor. A leaf is a
// merge candidate only when BOTH conjuncts hold: its live count is below ~25% of
// the narrow class's live slot budget (primaryMergeFloor), AND its live
// key/value heap fills below 25% of the leaf's own byte capacity. The slot
// conjunct preserves the historical 48-of-195 threshold for tiny values, where a
// narrow leaf is slot-limited (the churn geometry). The byte conjunct is what
// makes the floor capacity-relative rather than absolute: a leaf that is
// byte-full for its own row size -- a handful of large documents that already
// pack the heap, such as the low-cardinality competitive corpus's ~28-doc wide
// leaves -- is never sparse and never a merge candidate, even though its slot
// count sits below the absolute floor. Before this, every delete on such a leaf
// evaluated "warranted" and paid a full checkpoint before discovering nothing to
// merge. The two conjuncts are exactly min(slotCapacity, byteCapacity)/4 < n
// expanded so no division or average-row arithmetic is needed.
func primaryLeafBelowMergeFloor(liveCount, heapUsed, heapCapacity int) bool {
	if liveCount <= 0 || heapCapacity <= 0 {
		return false
	}
	return liveCount < primaryMergeFloor && heapUsed*4 < heapCapacity
}

// primaryWideLeafReclaimable reports whether a wide leaf's live rows would fit
// the narrow class comfortably, and so a wide->narrow reclass genuinely reclaims
// the 8 KiB->4 KiB slack. It is capacity-relative on BOTH axes: at most
// primaryNarrowComfortable live rows (~70% of narrow slots, the historical slot
// gate) AND a live heap at most ~70% of the narrow heap byte budget. The byte
// gate is what stops the low-cardinality corpus's byte-full wide leaves -- a
// handful of large documents that cannot fit the 4 KiB narrow class at all --
// from being reclassed on every delete into a no-op re-encode that falls back to
// wide yet still commits a full checkpoint.
func primaryWideLeafReclaimable(liveCount, heapUsed int) bool {
	if liveCount <= 0 || liveCount > primaryNarrowComfortable {
		return false
	}
	narrowHeap := storeio.CommonPrimaryLeafNarrowHeapCapacity()
	return narrowHeap > 0 && heapUsed*10 <= narrowHeap*7
}

// primaryMergeKind is the read-only classification of a routed leaf's own
// occupancy after a delete: which structural transaction, if any, it warrants.
type primaryMergeKind uint8

const (
	primaryMergeNone       primaryMergeKind = iota
	primaryMergeEmpty                       // emptied leaf: remove it
	primaryMergeBelowFloor                  // below the capacity floor: try a merge
	primaryMergeReclaim                     // wide leaf now fits narrow: reclass
)

// primaryMergeEval is one read-only merge/reclass evaluation of a routed leaf.
type primaryMergeEval struct {
	route       storeio.ResidentPrimaryRoute
	kind        primaryMergeKind
	liveCount   int
	wideReclaim bool // wide and <= narrowComfortable: a reclass fallback for merge
}

// evaluateMergeReclass peeks the routed leaf (the volatile buffered frame in
// buffered mode) and classifies its own occupancy WITHOUT flushing. An ordinary
// delete that leaves an ample or byte-full leaf returns primaryMergeNone here in
// nanoseconds, never touching the checkpoint path.
func (c *Collection) evaluateMergeReclass(
	keyBytes []byte,
) (primaryMergeEval, error) {
	state := c.state.Load()
	if state == nil {
		return primaryMergeEval{}, ErrClosed
	}
	route, err := c.currentPrimaryResidentRoute(state, keyBytes)
	if err != nil {
		// The routed leaf is gone (e.g. a concurrent structural rebuild); nothing
		// to evaluate. Treat as no-op rather than an error: hygiene is best-effort.
		return primaryMergeEval{}, nil
	}
	lease, err := c.primaryRouter.Load().AcquireLeaf(c.cache, route)
	if err != nil {
		return primaryMergeEval{}, err
	}
	leaf, _, admitErr := storeio.AdmittedPrimaryLeafForMutation(
		lease.Page(), c.storeID, route.Bucket, c.primaryLeafBounds(state),
	)
	if admitErr != nil {
		lease.Release()
		return primaryMergeEval{}, admitErr
	}
	n := leaf.Len()
	wide := leaf.Class() == storeio.CommonPrimaryLeafWide
	used, capacity := leaf.HeapOccupancy()
	lease.Release()

	eval := primaryMergeEval{
		route: route, liveCount: n,
		wideReclaim: wide && primaryWideLeafReclaimable(n, used),
	}
	switch {
	case n == 0:
		eval.kind = primaryMergeEmpty
	case primaryLeafBelowMergeFloor(n, used, capacity):
		eval.kind = primaryMergeBelowFloor
	case eval.wideReclaim:
		eval.kind = primaryMergeReclaim
	}
	return eval, nil
}

// mergeNeighborViable peeks the routed leaf's two lexical neighbours through the
// resident router -- read-only, no flush -- and reports whether either one's
// combined live count would fit the absorb ceiling. It is intentionally
// count-only and anchor-page-agnostic: a positive answer means the flush is
// worth paying so the committing path can confirm and repair the real tablet; a
// negative answer proves no merge can commit, so the abort stays checkpoint-free.
func (c *Collection) mergeNeighborViable(eval primaryMergeEval) bool {
	router := c.primaryRouter.Load()
	state := c.state.Load()
	if router == nil || state == nil {
		return false
	}
	bounds := c.primaryLeafBounds(state)
	for _, delta := range [2]int{-1, +1} {
		neighbor, ok := router.NeighborRoute(eval.route, delta)
		if !ok {
			continue
		}
		lease, err := router.AcquireLeaf(c.cache, neighbor)
		if err != nil {
			continue
		}
		leaf, _, admitErr := storeio.AdmittedPrimaryLeafForMutation(
			lease.Page(), c.storeID, neighbor.Bucket, bounds,
		)
		if admitErr != nil {
			lease.Release()
			continue
		}
		fits := eval.liveCount+leaf.Len() <= primaryMergeAbsorbCeiling
		lease.Release()
		if fits {
			return true
		}
	}
	return false
}

// structuralMergeReclassPrimaryLeaf is the delete-side structural evaluation. It
// first classifies the routed leaf read-only (evaluateMergeReclass) and, for a
// merge candidate, confirms a viable neighbour read-only (mergeNeighborViable)
// and consults the per-leaf hysteresis stamp. Only when a concrete, viable
// transaction is selected does it flush the tablet's buffered parents and commit
// one bounded structural transaction: a leaf emptied by the delete is removed; a
// below-floor leaf is merged right-into-left with a same-anchor-page neighbour
// that has room; otherwise a wide leaf that now fits the narrow class comfortably
// is reclassed. Every other outcome -- ample leaf, byte-full leaf, no viable
// neighbour, or a hysteresis skip -- returns without a checkpoint. It is
// best-effort hygiene: any failure aborts its own transaction and leaves the
// published post-delete state intact.
func (c *Collection) structuralMergeReclassPrimaryLeaf(keyBytes []byte) error {
	c.mergeReclassEvaluations.Add(1)
	eval, err := c.evaluateMergeReclass(keyBytes)
	if err != nil || eval.kind == primaryMergeNone {
		return err
	}

	router := c.primaryRouter.Load()
	// The read-only skip/abort applies only when merge is the leaf's sole option.
	// A wide leaf that also qualifies for reclass always has a viable transaction,
	// so it bypasses the hysteresis and neighbour-viability gates and proceeds to
	// commit (the committing path tries the merge first, then falls back to the
	// reclass).
	if eval.kind == primaryMergeBelowFloor && !eval.wideReclaim {
		// Hysteresis: a below-floor leaf that already found no viable neighbour at
		// this exact live count cannot become viable until its own count changes (a
		// delete/insert re-arms it by changing the stamp comparison) or a sibling's
		// own delete-side evaluation merges into it. Skip it read-only.
		if router.MergeAbortedAt(eval.route) == uint32(eval.liveCount) {
			c.mergeReclassSkips.Add(1)
			return nil
		}
		// Read-only viability: prove a neighbour can absorb this leaf before paying
		// the flush. If none can, abort now, stamping the count so the next
		// equal-count delete is skipped outright.
		if !c.mergeNeighborViable(eval) {
			router.RecordMergeAbort(eval.route, uint32(eval.liveCount))
			c.mergeReclassAborts.Add(1)
			return nil
		}
	}

	c.mergeReclassWarrantedHits.Add(1)
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
	if err := c.acquirePrimaryMutationPath(
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
	ourLen := path.leaf.Len()

	if ourLen == 0 {
		if len(current) <= 1 {
			c.mergeReclassAborts.Add(1)
			return nil
		}
		if err := c.commitRemoveEmptyLeaf(
			state, &path, current, sourceIndex,
		); err != nil {
			return err
		}
		c.removePrimaryEmptyLeaf()
		c.mergeReclassCommits.Add(1)
		return nil
	}

	used, capacity := path.leaf.HeapOccupancy()
	if primaryLeafBelowMergeFloor(ourLen, used, capacity) {
		merged, err := c.tryStructuralMerge(
			state, &path, current, sourceIndex,
		)
		if err != nil {
			return err
		}
		if merged {
			c.mergeReclassCommits.Add(1)
			return nil
		}
	}

	if path.leaf.Class() == storeio.CommonPrimaryLeafWide &&
		primaryWideLeafReclaimable(ourLen, used) {
		if err := c.reclassPrimaryLeaf(
			state, &path, current, sourceIndex,
		); err != nil {
			return err
		}
		c.mergeReclassCommits.Add(1)
		return nil
	}

	// Warranted and flushed, but the real tablet offered nothing to commit (the
	// only count-viable neighbour is on a different anchor page). Stamp the abort
	// so this leaf is not re-evaluated at this count.
	router.RecordMergeAbort(route, uint32(ourLen))
	c.mergeReclassAborts.Add(1)
	return nil
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
		state, path, structuralMerge,
		func(tx *storeio.WriteTransaction) (
			[]storeio.SegmentedTabletRouterLeaf, []storeio.PageRef, error,
		) {
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

// tryStructuralMerge merges the below-floor leaf at sourceIndex with an adjacent
// same-anchor-page neighbor when the combined live count stays within the
// hysteresis ceiling. The left leaf of the pair survives (keeping its BucketID
// and fence); the right leaf's rows move into it and it is retired.
func (c *Collection) tryStructuralMerge(
	state *fileStoreState,
	path *filePrimaryMutationPath,
	current []structuralLeaf,
	sourceIndex int,
) (bool, error) {
	ourLen := path.leaf.Len()
	for _, leftIdx := range [2]int{sourceIndex, sourceIndex - 1} {
		rightIdx := leftIdx + 1
		if leftIdx < 0 || rightIdx >= len(current) {
			continue
		}
		if current[leftIdx].pageID != current[rightIdx].pageID {
			continue
		}
		neighborIdx := rightIdx
		if leftIdx != sourceIndex {
			neighborIdx = leftIdx
		}
		lease, err := c.cache.Acquire(current[neighborIdx].ref)
		if err != nil {
			return false, err
		}
		neighbor, _, admitErr := storeio.AdmittedPrimaryLeafForMutation(
			lease.Page(), c.storeID, current[neighborIdx].bucket,
			storeio.CommonPrimaryLeafBounds{
				FileEnd:           state.super.FileEnd,
				NextLogicalID:     state.root.NextLogicalID,
				AllocationQuantum: state.root.PageSize,
			},
		)
		if admitErr != nil {
			lease.Release()
			return false, admitErr
		}
		if ourLen+neighbor.Len() > primaryMergeAbsorbCeiling {
			lease.Release()
			continue
		}
		leftLeaf, rightLeaf := &path.leaf, &neighbor
		if leftIdx != sourceIndex {
			leftLeaf, rightLeaf = &neighbor, &path.leaf
		}
		err = c.commitMerge(
			state, path, current, leftIdx, rightIdx, leftLeaf, rightLeaf,
		)
		lease.Release()
		return true, err
	}
	return false, nil
}

func (c *Collection) commitMerge(
	state *fileStoreState,
	path *filePrimaryMutationPath,
	current []structuralLeaf,
	leftIdx, rightIdx int,
	leftLeaf, rightLeaf *storeio.CommonPrimaryLeafView,
) error {
	survivorBucket := current[leftIdx].bucket
	generation := state.root.Generation + 1
	return c.commitPrimaryStructural(
		state, path, structuralMerge,
		func(tx *storeio.WriteTransaction) (
			[]storeio.SegmentedTabletRouterLeaf, []storeio.PageRef, error,
		) {
			rows := appendLeafRecords(c.structuralRows[:0], leftLeaf)
			rows = appendLeafRecords(rows, rightLeaf)
			c.structuralRows = rows
			survivorRef, err := c.encodeStructuralLeaf(
				tx, generation, survivorBucket, rows,
			)
			if err != nil {
				return nil, nil, err
			}
			// The right leaf's rows now carry the survivor's BucketID.
			c.structuralRepairPostingsHook(
				[]storeio.BucketID{survivorBucket},
			)
			final := make(
				[]storeio.SegmentedTabletRouterLeaf, 0, len(current)-1,
			)
			for i := range current {
				if i == rightIdx {
					continue
				}
				ref := current[i].ref
				if i == leftIdx {
					ref = survivorRef
				}
				fence := current[i].fence
				if len(final) == 0 {
					fence = nil
				}
				final = append(final, storeio.SegmentedTabletRouterLeaf{
					LocalID: current[i].localID, Fence: fence,
					Ref: ref, Zone: current[i].zone,
				})
			}
			return final, []storeio.PageRef{
				current[leftIdx].ref, current[rightIdx].ref,
			}, nil
		},
	)
}

// reclassPrimaryLeaf rewrites one wide leaf to the narrow class in place,
// reclaiming slot-class drift. Its BucketID, rows, fence, and local ID are
// unchanged; only its physical extent shrinks.
func (c *Collection) reclassPrimaryLeaf(
	state *fileStoreState,
	path *filePrimaryMutationPath,
	current []structuralLeaf,
	sourceIndex int,
) error {
	bucket := current[sourceIndex].bucket
	generation := state.root.Generation + 1
	return c.commitPrimaryStructural(
		state, path, structuralReclass,
		func(tx *storeio.WriteTransaction) (
			[]storeio.SegmentedTabletRouterLeaf, []storeio.PageRef, error,
		) {
			rows := c.extractLeafRecords(&path.leaf)
			newRef, err := c.encodeStructuralLeaf(
				tx, generation, bucket, rows,
			)
			if err != nil {
				return nil, nil, err
			}
			final := make([]storeio.SegmentedTabletRouterLeaf, len(current))
			for i := range current {
				ref := current[i].ref
				if i == sourceIndex {
					ref = newRef
				}
				final[i] = storeio.SegmentedTabletRouterLeaf{
					LocalID: current[i].localID, Fence: current[i].fence,
					Ref: ref, Zone: current[i].zone,
				}
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
	key string, src []byte,
) (created bool, err error) {
	if c.primaryExactActive() {
		return false, ErrPrimaryExactIndexReadOnly
	}
	for attempt := 0; ; attempt++ {
		created, err = c.putPrimary(key, src)
		if !errors.Is(err, ErrPrimaryLeafSplitRequired) {
			return created, err
		}
		if attempt >= primaryStructuralRetryLimit {
			return false, err
		}
		if splitErr := c.splitPrimaryLeafForKey(key); splitErr != nil {
			return false, errors.Join(err, splitErr)
		}
	}
}

// splitPrimaryLeafForKey runs one split structural transaction for the leaf
// routed by key under the single-writer lock, waiting for durability when
// synchronous.
func (c *Collection) splitPrimaryLeafForKey(key string) (err error) {
	c.writer.Lock()
	var generation uint64
	defer func() {
		// The journal-backed sync lane makes no chain-fence acknowledgement: a
		// structural split or merge changes no logical key/value, so recovery
		// reconstructs it by replaying the triggering Put/Delete through the
		// ordinary mutation path. Only the chunk sync lane (and a defensive
		// primary-sync store without a journal) still waits on the root fence.
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
	c.pointKeyScratch = append(c.pointKeyScratch[:0], key...)
	if err := c.structuralSplitPrimaryLeaf(c.pointKeyScratch); err != nil {
		return err
	}
	if after := c.state.Load(); after != nil && after.root.Generation > before {
		generation = after.root.Generation
	}
	return nil
}

// deletePrimaryWithMerge is the primary Delete entry point. After a successful
// delete it evaluates the routed leaf for merge, empty-leaf removal, or
// slot-class reclaim. That hygiene is best-effort: any failure aborts its own
// transaction and leaves the published post-delete state intact.
func (c *Collection) deletePrimaryWithMerge(
	key string,
) (deleted bool, err error) {
	if c.primaryExactActive() {
		return false, ErrPrimaryExactIndexReadOnly
	}
	deleted, err = c.deletePrimary(key)
	if err != nil || !deleted {
		return deleted, err
	}
	_ = c.mergeReclassPrimaryLeafForKey(key)
	return deleted, nil
}

// mergeReclassPrimaryLeafForKey runs one merge/reclass structural evaluation for
// the leaf routed by key under the single-writer lock, waiting for durability
// when synchronous.
func (c *Collection) mergeReclassPrimaryLeafForKey(key string) (err error) {
	c.writer.Lock()
	var generation uint64
	defer func() {
		// The journal-backed sync lane makes no chain-fence acknowledgement: a
		// structural split or merge changes no logical key/value, so recovery
		// reconstructs it by replaying the triggering Put/Delete through the
		// ordinary mutation path. Only the chunk sync lane (and a defensive
		// primary-sync store without a journal) still waits on the root fence.
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
	c.pointKeyScratch = append(c.pointKeyScratch[:0], key...)
	if err := c.structuralMergeReclassPrimaryLeaf(c.pointKeyScratch); err != nil {
		return err
	}
	if after := c.state.Load(); after != nil && after.root.Generation > before {
		generation = after.root.Generation
	}
	return nil
}
