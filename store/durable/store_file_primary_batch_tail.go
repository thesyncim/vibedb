package durable

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"slices"
	"sort"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// primaryTailSplitStage is the private, unpublished next state for an eligible
// append-only checkpoint-group batch. admitted aliases the newly admitted tail
// frames in batchPrimaryAdmitted; that shared owner is responsible for abort
// cleanup until marker decision.
type primaryTailSplitStage struct {
	nextRouter             *storeio.ResidentPrimaryRouter
	nextLineage            *primaryTailSplitLineage
	retireAtPublish        []storeio.PageRef
	nextFileEnd            uint64
	nextLogicalID          uint64
	nextDocumentCount      uint64
	documentCountDelta     int
	routerWorstCaseBytes   uint64
	routerRetireBytes      uint64
	routerRetiredNextBytes uint64
	reservationBytes       uint64
	updatedRoute           storeio.ResidentPrimaryRoute
	updatedRef             storeio.PageRef
}

func (c *Collection) ensurePrimaryBatchConditionalJournalRoomNoCheckpoint(
	entries []storeio.RecoveryBatchEntry,
) error {
	if c.journalReplaying || !c.journalEnabled() {
		return nil
	}
	plan, err := c.journal.PrepareConditionalBatch(entries)
	if err != nil {
		return err
	}
	if c.journal.PreparedBatchFits(plan) {
		return nil
	}
	if err := c.growJournalForRecordLocked(plan.PaddedSize()); err != nil {
		return err
	}
	if c.journal.PreparedBatchFits(plan) {
		return nil
	}
	return ErrCheckpointGroupPressure
}

func primaryTailAddCharge(total, amount uint64) (uint64, bool) {
	if amount > math.MaxUint64-total {
		return 0, false
	}
	return total + amount, true
}

func primaryTailSplitPlanningScratchCharge(
	currentRows, prospectiveRows, priorLeaves, maxFinalLeaves int,
) (uint64, bool) {
	maxInt := int(^uint(0) >> 1)
	if currentRows < 0 || prospectiveRows < 0 || priorLeaves < 0 ||
		maxFinalLeaves < 0 || currentRows > maxInt-prospectiveRows {
		return 0, false
	}
	keyCount := currentRows + prospectiveRows
	if keyCount == 0 || keyCount > maxInt/2 {
		return 0, false
	}
	prefixLeaves := max(0, priorLeaves-1)
	if prefixLeaves > maxInt-2 || maxFinalLeaves < prefixLeaves+2 {
		return 0, false
	}
	newLeafLimit := maxFinalLeaves - prefixLeaves
	startsLimit := max(currentRows, prospectiveRows)
	var charge uint64
	addProduct := func(count int, size uintptr) bool {
		if count < 0 || (size != 0 && uint64(count) >
			math.MaxUint64/uint64(size)) {
			return false
		}
		var ok bool
		charge, ok = primaryTailAddCharge(
			charge, uint64(count)*uint64(size),
		)
		return ok
	}
	// Planner payload: unionKeys (N), bounded cuts growth (old+new, 4N),
	// and bounded additions growth (old+new, 2N). Keys themselves are borrowed.
	if !addProduct(keyCount, 7*unsafe.Sizeof([]byte(nil))) ||
		!addProduct(startsLimit, unsafe.Sizeof(int(0))) {
		return 0, false
	}
	// newFloors and replacementFences each own up to newLeafLimit slice
	// headers. rightFences owns one header for every final route after the
	// source, including any existing prefix descendants.
	if !addProduct(prefixLeaves, unsafe.Sizeof(primaryTailSplitLeaf{})) ||
		!addProduct(newLeafLimit, unsafe.Sizeof([]byte(nil))) ||
		!addProduct(newLeafLimit, unsafe.Sizeof([]byte(nil))) ||
		!addProduct(maxFinalLeaves-1, unsafe.Sizeof([]byte(nil))) ||
		!addProduct(newLeafLimit, unsafe.Sizeof(uint16(0))) ||
		!addProduct(
			newLeafLimit,
			unsafe.Sizeof(storeio.SegmentedTabletRouterLeaf{}),
		) ||
		!addProduct(1, unsafe.Sizeof(primaryTailSplitStage{})) ||
		!addProduct(1, unsafe.Sizeof(storeio.PageRef{})) ||
		!addProduct(
			1, uintptr(storeio.TabletLocalIdentityLocalCount/8),
		) ||
		!addProduct(1, unsafe.Sizeof(primaryTailSplitLineage{})) ||
		!addProduct(maxFinalLeaves, unsafe.Sizeof(primaryTailSplitLeaf{})) ||
		!addProduct(
			maxFinalLeaves,
			uintptr(storeio.CommonPrimaryLeafMaxKeyBytes),
		) {
		return 0, false
	}
	return charge, true
}

func primaryTailLineageMetadataCharge(lineage *primaryTailSplitLineage) (uint64, bool) {
	if lineage == nil {
		return 0, true
	}
	charge := uint64(unsafe.Sizeof(*lineage))
	leaves := uint64(cap(lineage.leaves))
	leafSize := uint64(unsafe.Sizeof(primaryTailSplitLeaf{}))
	if leafSize != 0 && leaves > math.MaxUint64/leafSize {
		return 0, false
	}
	var ok bool
	charge, ok = primaryTailAddCharge(charge, leaves*leafSize)
	if !ok {
		return 0, false
	}
	for index := range lineage.leaves {
		charge, ok = primaryTailAddCharge(
			charge, uint64(cap(lineage.leaves[index].localFloor)),
		)
		if !ok {
			return 0, false
		}
	}
	return charge, true
}

func (c *Collection) refreshPrimaryTailSplitChargeLocked() {
	current := c.primaryTailRetiredRouterBytes.Load()
	if c.primaryTailSplit != nil {
		var ok bool
		current, ok = primaryTailAddCharge(
			current, c.primaryTailSplit.chargedBytes,
		)
		if ok {
			current, ok = primaryTailAddCharge(
				current, c.primaryTailSplit.routerRetainedBytes,
			)
		}
		if !ok {
			current = math.MaxUint64
		}
	}
	c.primaryTailCurrentChargeBytes.Store(current)
	peak := current
	if c.cache != nil {
		var ok bool
		peak, ok = primaryTailAddCharge(peak, c.cache.DirtyReservedBytes())
		if !ok {
			peak = math.MaxUint64
		}
	}
	c.recordPrimaryTailSplitPeak(peak)
}

func (c *Collection) recordPrimaryTailSplitPeak(value uint64) {
	for current := c.primaryTailPeakChargeBytes.Load(); value > current; {
		if c.primaryTailPeakChargeBytes.CompareAndSwap(current, value) {
			return
		}
		current = c.primaryTailPeakChargeBytes.Load()
	}
}

// recordPrimaryTailAdmissionPeak records the pre-admission dirty arena plus
// the complete transient reservation and retained router-history charge. The
// cache accessor is O(1), so this remains independent of cache size.
func (c *Collection) recordPrimaryTailAdmissionPeak(reservation uint64) bool {
	if c == nil || c.cache == nil {
		return false
	}
	peak, ok := primaryTailAddCharge(
		c.cache.DirtyReservedBytes(), reservation,
	)
	if !ok {
		return false
	}
	peak, ok = primaryTailAddCharge(
		peak, c.primaryTailRetiredRouterBytes.Load(),
	)
	if !ok {
		return false
	}
	c.recordPrimaryTailSplitPeak(peak)
	return true
}

func (c *Collection) primaryTailSplitAdmissionCharge(
	lineage *primaryTailSplitLineage, routerUpperBound uint64,
) (uint64, bool) {
	var charge uint64
	metadata, ok := primaryTailLineageMetadataCharge(lineage)
	if !ok {
		return 0, false
	}
	charge, ok = primaryTailAddCharge(charge, metadata)
	if !ok {
		return 0, false
	}
	// The staged descriptor coexists with the published one until the marker
	// decides. Charge both slices during that interval, even where immutable
	// local-floor payloads are shared between them.
	if c.primaryTailSplit != nil {
		currentMetadata, metadataOK := primaryTailLineageMetadataCharge(
			c.primaryTailSplit,
		)
		if !metadataOK {
			return 0, false
		}
		charge, ok = primaryTailAddCharge(charge, currentMetadata)
		if !ok {
			return 0, false
		}
	}
	charge, ok = primaryTailAddCharge(charge, routerUpperBound)
	if !ok {
		return 0, false
	}
	if routerUpperBound == 0 {
		charge, ok = primaryTailAddCharge(charge, lineage.routerRetainedBytes)
		if !ok {
			return 0, false
		}
	} else {
		// SplitLeafPartition's bound covers its source image and path-copy
		// allocations. Also reserve the complete current root explicitly so the
		// old-root + new-root publication ledger is covered before the marker.
		router := c.primaryRouter.Load()
		if router == nil {
			return 0, false
		}
		charge, ok = primaryTailAddCharge(
			charge, uint64(router.ResidentBytes()),
		)
		if !ok {
			return 0, false
		}
	}
	foldScratch, ok := primaryTailSplitFoldScratchCharge(len(lineage.leaves))
	if !ok {
		return 0, false
	}
	charge, ok = primaryTailAddCharge(charge, foldScratch)
	if !ok {
		return 0, false
	}
	if _, ok := c.primaryTailFoldResourceCharge(len(lineage.leaves)); !ok {
		return 0, false
	}
	foldReserve, ok := c.primaryTailFoldResidentResourceCharge(
		len(lineage.leaves),
	)
	if !ok {
		return 0, false
	}
	return primaryTailAddCharge(charge, foldReserve)
}

// primaryTailFoldResourceCharge bounds the physical pages for the complete
// final lineage, not only the most recent replacement. Each descendant can
// require a maximum-sized leaf. The six metadata extents use their actual page
// kinds; free-log pages use PageSize.
func (c *Collection) primaryTailFoldResourceCharge(leaves int) (uint64, bool) {
	if c == nil || leaves < 2 || leaves > storeio.TabletLocalIdentityLocalCount {
		return 0, false
	}
	const primaryMetadataPages = 6 // locator, anchor, tablet, catalog leaf/branch/root
	const freeMetadataPages = storeio.FreeLogMaxIndexPages +
		storeio.FreeLogMaxDeltaPages
	if c.options.freeFoldLimit > math.MaxInt-primaryMetadataPages-freeMetadataPages {
		return 0, false
	}
	metadataPages := primaryMetadataPages + freeMetadataPages +
		c.options.freeFoldLimit
	if leaves > math.MaxInt-metadataPages ||
		leaves+metadataPages > c.options.maxTransactionPages {
		return 0, false
	}
	leafCount := uint64(leaves)
	leafExtent := uint64(storeio.CommonPrimaryLeafMaxExtentBytes)
	if leafCount > math.MaxUint64/leafExtent {
		return 0, false
	}
	leafBytes := leafCount * leafExtent
	primaryMetadataBytes := uint64(
		storeio.GlobalTabletCatalogLocatorBytes +
			storeio.SegmentedTabletRouterAnchorPageBytes +
			storeio.GlobalTabletCatalogTabletBytes +
			2*storeio.GlobalTabletCatalogNodeBytes +
			storeio.GlobalTabletCatalogRootBytes,
	)
	freePageBytes := uint64(c.options.PageSize)
	freePageCount := uint64(c.options.freeFoldLimit + freeMetadataPages)
	if freePageBytes != 0 && freePageCount > math.MaxUint64/freePageBytes {
		return 0, false
	}
	physicalMetadataBytes, ok := primaryTailAddCharge(
		primaryMetadataBytes, freePageCount*freePageBytes,
	)
	if !ok {
		return 0, false
	}
	physicalBound, ok := primaryTailAddCharge(leafBytes, physicalMetadataBytes)
	if !ok || physicalBound > c.options.maxTransactionPhysicalBytes {
		return 0, false
	}
	return physicalBound, true
}

// primaryTailFoldResidentResourceCharge maps the same bounded transaction
// page mix into the page cache's resident classes. Persisted routing extents
// have fixed widths which may exceed MaxPageSize; cache admission follows its
// own configured class geometry instead.
func (c *Collection) primaryTailFoldResidentResourceCharge(leaves int) (uint64, bool) {
	if c == nil || leaves < 2 {
		return 0, false
	}
	var charge uint64
	addPageClass := func(count int, length uint32) bool {
		if count < 0 || c.cache == nil {
			return false
		}
		perPage := c.cache.ReservationBytes(length)
		if perPage == 0 || uint64(count) > math.MaxUint64/perPage {
			return false
		}
		var ok bool
		charge, ok = primaryTailAddCharge(charge, uint64(count)*perPage)
		return ok
	}
	if !addPageClass(leaves, uint32(storeio.CommonPrimaryLeafMaxExtentBytes)) ||
		!addPageClass(1, storeio.GlobalTabletCatalogLocatorBytes) ||
		!addPageClass(1, storeio.SegmentedTabletRouterAnchorPageBytes) ||
		!addPageClass(1, storeio.GlobalTabletCatalogTabletBytes) ||
		!addPageClass(2, storeio.GlobalTabletCatalogNodeBytes) ||
		!addPageClass(1, storeio.GlobalTabletCatalogRootBytes) ||
		!addPageClass(
			c.options.freeFoldLimit+storeio.FreeLogMaxIndexPages+
				storeio.FreeLogMaxDeltaPages,
			uint32(c.options.PageSize),
		) {
		return 0, false
	}
	return charge, true
}

func (c *Collection) primaryTailBatchReservationBytes(extra uint64) (uint64, bool) {
	total, ok := primaryTailAddCharge(extra, c.batchPrimaryOverflowDirty)
	if !ok {
		return 0, false
	}
	for index := range c.batchPrimaryLeaves {
		leaf := &c.batchPrimaryLeaves[index]
		if leaf.skip {
			continue
		}
		total, ok = primaryTailAddCharge(
			total, c.cache.ReservationBytes(uint32(leaf.imageLength)),
		)
		if !ok {
			return 0, false
		}
	}
	return total, true
}

func (c *Collection) primaryTailBatchBaseRows(
	state *fileStoreState, leaf *primaryBatchLeaf,
) ([]storeio.CommonPrimaryLeafRecord, storeio.PageLease, error) {
	if state == nil || leaf == nil {
		return nil, storeio.PageLease{}, storeio.ErrInvalidWrite
	}
	ref := leaf.resident.Ref
	if c.primaryTailSplit != nil {
		if leaf.tailLineageIndex < 0 ||
			leaf.tailLineageIndex >= len(c.primaryTailSplit.leaves) {
			return nil, storeio.PageLease{}, storeio.ErrSegmentedTabletRouterCorrupt
		}
		ref = c.primaryTailSplit.leaves[leaf.tailLineageIndex].volatile
	} else if leaf.pendingIndex >= 0 &&
		leaf.pendingIndex < len(c.primaryPendingParents) {
		if volatile := c.primaryPendingParents[leaf.pendingIndex].volatileRef; volatile != (storeio.PageRef{}) {
			ref = volatile
		}
	}
	lease, err := c.cache.Acquire(ref)
	if err != nil {
		return nil, storeio.PageLease{}, err
	}
	if storeio.PrimaryLeafClass(lease.Page()) != storeio.CommonPrimaryLeafCompact {
		lease.Release()
		return nil, storeio.PageLease{}, storeio.ErrCommonPrimaryLeafCorrupt
	}
	stripe, ok := storeio.AdmittedCompactPrimaryStripe(
		lease.Page(), c.storeID, leaf.resident.Bucket,
	)
	if !ok {
		lease.Release()
		return nil, storeio.PageLease{}, storeio.ErrCommonPrimaryLeafCorrupt
	}
	rows, err := stripe.RenderRecordsWithScratch(c.primaryLeafMutationScratch)
	if err != nil {
		lease.Release()
		return nil, storeio.PageLease{}, err
	}
	for index := range rows {
		if rows[index].Value.IsOverflow() {
			lease.Release()
			return nil, storeio.PageLease{}, ErrCheckpointGroupPressure
		}
	}
	return rows, lease, nil
}

func (c *Collection) validatePrimaryTailAppendOnly(
	leaf *primaryBatchLeaf, baseRows []storeio.CommonPrimaryLeafRecord,
) error {
	if leaf == nil || len(baseRows) == 0 {
		return ErrCheckpointGroupPressure
	}
	maximum := baseRows[len(baseRows)-1].Key
	for mutationAt := leaf.mutationAt; mutationAt < leaf.mutationEnd; mutationAt++ {
		mutation := &c.batchPrimaryMutations[mutationAt]
		if mutation.remove || mutation.found ||
			bytes.Compare(mutation.key, maximum) <= 0 {
			return ErrCheckpointGroupPressure
		}
	}
	return nil
}

func (c *Collection) preparePrimaryTailBatchUpdateLocked(
	state *fileStoreState, generation uint64,
) (stagedPrimaryBatch, error) {
	if !c.primaryTailBatchCandidate(state, true) ||
		c.primaryTailSplit == nil || len(c.batchPrimaryLeaves) != 1 {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	leaf := &c.batchPrimaryLeaves[0]
	baseRows, lease, err := c.primaryTailBatchBaseRows(state, leaf)
	if err != nil {
		return stagedPrimaryBatch{}, err
	}
	defer lease.Release()
	if err := c.validatePrimaryTailAppendOnly(leaf, baseRows); err != nil {
		return stagedPrimaryBatch{}, err
	}
	lineage := *c.primaryTailSplit
	lineage.leaves = slices.Clone(c.primaryTailSplit.leaves)
	last := len(lineage.leaves) - 1
	previous := lineage.leaves[last].volatile
	lineage.leaves[last].volatile = leaf.nextLeaf
	lineage.source.volatileRef = storeio.PageRef{}
	metadata, ok := primaryTailLineageMetadataCharge(&lineage)
	if !ok {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	lineage.chargedBytes = metadata
	lineage.peakChargedBytes = max(
		lineage.peakChargedBytes, metadata,
	)
	nextDocumentCount, ok := fileLogicalDocumentCount(
		state.root.DocumentCount, leaf.docDelta,
	)
	if !ok {
		return stagedPrimaryBatch{}, storeio.ErrInvalidWrite
	}
	stage := &primaryTailSplitStage{
		nextLineage:        lineagePointer(lineage),
		nextFileEnd:        c.batchPrimaryFileEnd,
		nextLogicalID:      c.batchPrimaryNextLogicalID,
		nextDocumentCount:  nextDocumentCount,
		documentCountDelta: leaf.docDelta,
		updatedRoute:       leaf.resident,
		updatedRef:         leaf.nextLeaf,
	}
	if previous != (storeio.PageRef{}) {
		stage.retireAtPublish = []storeio.PageRef{previous}
	}
	leaf.pending = lineage.source
	leaf.pending.volatileRef = previous
	leaf.pendingIndex = 0 // the original source parent represents the lineage
	extra, ok := c.primaryTailSplitAdmissionCharge(stage.nextLineage, 0)
	if !ok {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	stage.reservationBytes, ok = c.primaryTailBatchReservationBytes(extra)
	if !ok {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	if _, err := c.ensurePrimaryBatchCapacityWithPolicy(
		true, extra, false,
	); err != nil {
		return stagedPrimaryBatch{}, err
	}
	if !c.recordPrimaryTailAdmissionPeak(stage.reservationBytes) {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	c.batchPrimaryAdmitted = c.batchPrimaryAdmitted[:0]
	if err := c.admitPrimaryBatchOverflow(); err != nil {
		c.unadmitPrimaryBatchLeaves()
		return stagedPrimaryBatch{}, err
	}
	if err := c.admitPrimaryBatchLeaves(); err != nil {
		c.unadmitPrimaryBatchLeaves()
		return stagedPrimaryBatch{}, err
	}
	return stagedPrimaryBatch{
		state: state, generation: generation,
		tailSplit: stage, live: true,
	}, nil
}

func lineagePointer(lineage primaryTailSplitLineage) *primaryTailSplitLineage {
	return &lineage
}

func (c *Collection) preparePrimaryTailBatchSplitLocked(
	state *fileStoreState, generation uint64,
) (stagedPrimaryBatch, error) {
	if !c.primaryTailBatchCandidate(state, true) ||
		len(c.batchPrimaryLeaves) != 1 || generation != state.root.Generation+1 {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	leaf := &c.batchPrimaryLeaves[0]
	baseRows, rowLease, err := c.primaryTailBatchBaseRows(state, leaf)
	if err != nil {
		return stagedPrimaryBatch{}, err
	}
	defer rowLease.Release()
	if err := c.validatePrimaryTailAppendOnly(leaf, baseRows); err != nil {
		return stagedPrimaryBatch{}, err
	}
	if len(c.structuralRows) == 0 {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	prospective := c.structuralRows

	var source filePrimaryPendingParent
	var prefix []primaryTailSplitLeaf
	prefixCount := 0
	tabletID, sourceLocalID, ok := storeio.SplitTabletLocalIdentityBucket(
		uint32(leaf.pending.leafRoute.Bucket),
	)
	if !ok {
		return stagedPrimaryBatch{}, storeio.ErrSegmentedTabletRouterCorrupt
	}
	tailSourceLocal := uint16(sourceLocalID)
	if c.primaryTailSplit != nil {
		old := c.primaryTailSplit
		if old.tabletID != tabletID || len(old.leaves) < 2 ||
			leaf.tailLineageIndex != len(old.leaves)-1 {
			return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
		}
		source = old.source
		prefixCount = len(old.leaves) - 1
		tailSourceLocal = old.leaves[len(old.leaves)-1].localID
	} else {
		source = leaf.pending
	}
	source.volatileRef = storeio.PageRef{}
	source.resident = storeio.ResidentPrimaryRoute{
		Ref: source.resident.Ref, Bucket: source.resident.Bucket,
		Hash: source.resident.Hash,
	}
	lineageTablet, originalLocal, valid := storeio.SplitTabletLocalIdentityBucket(
		uint32(source.leafRoute.Bucket),
	)
	if !valid || lineageTablet != tabletID || uint16(originalLocal) !=
		uint16(sourceLocalID) {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}

	// Root the physical metadata path at the original sealed source while rows
	// are read from the current trailing volatile descendant. Its whole unsplit
	// range still belongs to this physical leaf until the final fold.
	var path filePrimaryMutationPath
	if err := c.acquirePrimaryRoutingPath(
		&path, state, leaf.firstKey, source.resident,
	); err != nil {
		return stagedPrimaryBatch{}, err
	}
	defer path.Release()
	if path.leafRoute.Ref != source.leafRoute.Ref ||
		path.leafRoute.Bucket != source.leafRoute.Bucket ||
		path.tablet.TabletID() != tabletID {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	anchorCount := path.anchor.Count()
	anchorPages := path.tablet.AnchorCount()
	if anchorCount <= 0 || anchorPages <= 0 {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	lastAnchor, lastAnchorOK := path.tablet.AnchorAt(anchorPages - 1)
	if !lastAnchorOK || lastAnchor != source.anchorRoute ||
		path.anchorRoute != lastAnchor {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	lastPhysicalLeaf, lastPhysicalLeafOK := path.anchor.RouteAt(anchorCount-1, 0)
	physicalTablet, physicalLocal, physicalIdentityOK :=
		storeio.SplitTabletLocalIdentityBucket(uint32(lastPhysicalLeaf.Bucket))
	if !lastPhysicalLeafOK || !physicalIdentityOK ||
		physicalTablet != tabletID || uint16(physicalLocal) != uint16(sourceLocalID) ||
		lastPhysicalLeaf.Ref != source.leafRoute.Ref ||
		lastPhysicalLeaf.Bucket != source.leafRoute.Bucket ||
		lastPhysicalLeaf.PageID != source.leafRoute.PageID ||
		lastPhysicalLeaf.Zone != source.leafRoute.Zone {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	if c.primaryTailSplit == nil {
		if leaf.pending.resident.Bucket != source.leafRoute.Bucket {
			return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
		}
	} else {
		old := c.primaryTailSplit
		if len(old.leaves) == 0 || old.leaves[0].localID != uint16(sourceLocalID) ||
			old.source.leafRoute.Ref != source.leafRoute.Ref {
			return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
		}
		for index := range old.leaves {
			bucket, bucketOK := storeio.MakeTabletLocalIdentityBucket(
				old.tabletID, uint32(old.leaves[index].localID),
			)
			route, routeOK := c.primaryRouter.Load().ResolveBucketID(
				storeio.BucketID(bucket),
			)
			if !bucketOK || !routeOK || route.Ref != old.leaves[index].volatile {
				return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
			}
		}
	}

	router := c.primaryRouter.Load()
	if router == nil {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	selected, residentFloor, selectedOK := router.ResolveBucketFloor(
		leaf.resident.Bucket,
	)
	lastRoute, lastOK := router.RouteAtRank(router.Len() - 1)
	if !selectedOK || selected.Ref != leaf.resident.Ref ||
		selected.Bucket != leaf.resident.Bucket ||
		!lastOK || lastRoute.Bucket != selected.Bucket ||
		lastRoute.Ref != selected.Ref {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	var allUsed [storeio.TabletLocalIdentityLocalCount / 64]uint64
	if !router.CopyTabletLocalIDs(tabletID, &allUsed) {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	usedCount := 0
	for _, word := range allUsed {
		usedCount += bits.OnesCount64(word)
	}
	if usedCount == 0 ||
		allUsed[tailSourceLocal>>6]&(uint64(1)<<uint(tailSourceLocal&63)) == 0 {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}

	maxFinalLeaves := 1 + storeio.SegmentedTabletRouterRowsPerPage - anchorCount
	if maxFinalLeaves < prefixCount+2 {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	planningCharge, chargeOK := primaryTailSplitPlanningScratchCharge(
		len(baseRows), len(prospective),
		func() int {
			if c.primaryTailSplit == nil {
				return 0
			}
			return len(c.primaryTailSplit.leaves)
		}(), maxFinalLeaves,
	)
	if !chargeOK {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	preflight := c.primaryTailCurrentChargeBytes.Load()
	if c.primaryTailSplit == nil {
		preflight, chargeOK = primaryTailAddCharge(
			preflight, uint64(router.ResidentBytes()),
		)
		if !chargeOK {
			return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
		}
	}
	preflight, chargeOK = primaryTailAddCharge(preflight, planningCharge)
	if !chargeOK || c.cache == nil ||
		c.cache.DirtyCapacityAvailable() < preflight {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	preplannerPeak, peakOK := primaryTailAddCharge(
		c.cache.DirtyReservedBytes(), preflight,
	)
	if !peakOK {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	c.recordPrimaryTailSplitPeak(preplannerPeak)

	cuts, unionKeys, err := c.planPrimaryBatchTopologyCuts(
		baseRows, prospective, false,
	)
	if err != nil {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	newFloorCount := len(cuts) + 1
	if len(cuts) == 0 || prefixCount+newFloorCount > maxFinalLeaves ||
		usedCount-1+newFloorCount > storeio.TabletLocalIdentityLocalCount {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}

	if c.primaryTailSplit != nil {
		prefix = slices.Clone(
			c.primaryTailSplit.leaves[:len(c.primaryTailSplit.leaves)-1],
		)
	}
	newFloors := make([][]byte, len(cuts)+1)
	if c.primaryTailSplit == nil {
		floor, floorOK := path.anchor.AppendFenceAt(
			make([]byte, 0, storeio.CommonPrimaryLeafMaxKeyBytes), anchorCount-1,
		)
		if !floorOK || len(floor) > storeio.CommonPrimaryLeafMaxKeyBytes {
			return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
		}
		newFloors[0] = floor
	} else {
		newFloors[0] = slices.Clone(
			c.primaryTailSplit.leaves[len(c.primaryTailSplit.leaves)-1].localFloor,
		)
	}
	for at, cut := range cuts {
		position := sort.Search(len(unionKeys), func(i int) bool {
			return bytes.Compare(unionKeys[i], cut) >= 0
		})
		if position <= 0 || position >= len(unionKeys) ||
			!bytes.Equal(unionKeys[position], cut) {
			return stagedPrimaryBatch{}, storeio.ErrInvalidWrite
		}
		floor, floorErr := storeio.ShortestPrimaryFence(
			make([]byte, len(cut)), unionKeys[position-1], cut,
		)
		if floorErr != nil {
			return stagedPrimaryBatch{}, floorErr
		}
		newFloors[at+1] = floor
	}

	allUsed[tailSourceLocal>>6] &^= uint64(1) << uint(tailSourceLocal&63)
	outputIDs := make([]uint16, len(newFloors))
	outputIDs[0] = tailSourceLocal
	allUsed[tailSourceLocal>>6] |= uint64(1) << uint(tailSourceLocal&63)
	nextOutputID := 1
	for local := 0; local < storeio.TabletLocalIdentityLocalCount &&
		nextOutputID < len(outputIDs); local++ {
		if allUsed[local>>6]&(uint64(1)<<uint(local&63)) != 0 {
			continue
		}
		outputIDs[nextOutputID] = uint16(local)
		allUsed[local>>6] |= uint64(1) << uint(local&63)
		nextOutputID++
	}
	if nextOutputID != len(outputIDs) {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}

	newLeaves := make([]primaryTailSplitLeaf, 0, len(prefix)+len(newFloors))
	newLeaves = append(newLeaves, prefix...)
	zone := source.leafRoute.Zone
	for rank := range newFloors {
		leafZone := storeio.BucketZone{}
		if rank == 0 {
			leafZone = zone
		}
		newLeaves = append(newLeaves, primaryTailSplitLeaf{
			localID: outputIDs[rank], localFloor: newFloors[rank], zone: leafZone,
		})
	}
	for index := 1; index < len(newLeaves); index++ {
		if bytes.Compare(newLeaves[index-1].localFloor,
			newLeaves[index].localFloor) >= 0 {
			return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
		}
	}
	rightFences := make([][]byte, len(newLeaves)-1)
	for index := 1; index < len(newLeaves); index++ {
		rightFences[index-1] = newLeaves[index].localFloor
	}
	partition, err := path.tablet.PlanLeafPartition(
		&path.anchor, source.leafRoute, rightFences,
	)
	if err != nil {
		return stagedPrimaryBatch{}, err
	}
	if partition.RequiresTabletRebuild() ||
		int(partition.ReplacementCount()) != len(newLeaves) {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}

	if _, ok := c.primaryTailFoldResourceCharge(len(newLeaves)); !ok {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	replacementFences := make([][]byte, len(newFloors))
	replacementFences[0] = residentFloor
	var addedFenceBytes uint64
	for rank := 1; rank < len(newFloors); rank++ {
		replacementFences[rank] = newFloors[rank]
		addedFenceBytes, ok = primaryTailAddCharge(
			addedFenceBytes, uint64(cap(newFloors[rank])),
		)
		if !ok || addedFenceBytes > math.MaxInt {
			return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
		}
	}
	routerWorstCase, ok := router.SplitLeafPartitionAllocationUpperBound(
		len(newFloors), int(addedFenceBytes),
	)
	if !ok {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}

	// Encode the prospective batch image into one frame per final range. The
	// planner used both current and prospective rows above, so the same topology
	// is safe for content-equivalent recovery and for this atomic publication.
	originalDocDelta := leaf.docDelta
	originalMutationAt, originalMutationEnd := leaf.mutationAt, leaf.mutationEnd
	c.batchPrimaryLeafArena = c.batchPrimaryLeafArena[:0]
	var oldTailRef storeio.PageRef
	if c.primaryTailSplit != nil {
		oldTailRef = c.primaryTailSplit.leaves[len(c.primaryTailSplit.leaves)-1].volatile
	} else if leaf.pending.volatileRef != (storeio.PageRef{}) {
		oldTailRef = leaf.pending.volatileRef
	}
	c.batchPrimaryLeaves = c.batchPrimaryLeaves[:0]
	if cap(c.batchPrimaryLeaves) < len(newFloors) {
		c.batchPrimaryLeaves = slices.Grow(
			c.batchPrimaryLeaves, len(newFloors)-len(c.batchPrimaryLeaves),
		)
	}
	baseAt, finalAt := 0, 0
	for rank := range newFloors {
		baseEnd, finalEnd := len(baseRows), len(prospective)
		if rank+1 < len(newFloors) {
			baseEnd = primaryBatchTopologyLowerBound(
				baseRows, baseAt, newFloors[rank+1],
			)
			finalEnd = primaryBatchTopologyLowerBound(
				prospective, finalAt, newFloors[rank+1],
			)
		}
		if baseEnd < baseAt || finalEnd < finalAt || finalEnd == finalAt {
			return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
		}
		bucketValue, bucketOK := storeio.MakeTabletLocalIdentityBucket(
			tabletID, uint32(outputIDs[rank]),
		)
		logicalID, logicalOK := storeio.CommonPrimaryLeafLogicalID(
			storeio.BucketID(bucketValue),
		)
		if !bucketOK || !logicalOK {
			return stagedPrimaryBatch{}, storeio.ErrSegmentedTabletRouterCorrupt
		}
		// prospective is the private c.structuralRows workspace; slot placement
		// mutates only these copied record headers, while key/value bodies remain
		// borrowed from the private merge and source rows.
		rows := prospective[finalAt:finalEnd]
		if len(rows) <= storeio.CommonPrimaryLeafWideSlots {
			if err := storeio.PlaceCommonPrimaryLeafRecords(
				storeio.CommonPrimaryLeafWide, c.storeID, rows,
			); err != nil {
				return stagedPrimaryBatch{}, err
			}
		}
		image, encodeErr := storeio.EncodeBestCompactPrimaryStripe(
			c.primaryLeafScratch,
			storeio.CommonPrimaryLeafHeader{
				StoreID: c.storeID, Generation: generation,
				Bucket: storeio.BucketID(bucketValue),
			},
			c.storeID, rows, c.primaryUnifiedBuilder,
		)
		if encodeErr != nil {
			if errors.Is(encodeErr, storeio.ErrCommonPrimaryLeafFull) {
				return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
			}
			return stagedPrimaryBatch{}, encodeErr
		}
		imageOffset := len(c.batchPrimaryLeafArena)
		c.batchPrimaryLeafArena = append(c.batchPrimaryLeafArena, image...)
		imageLength := len(c.batchPrimaryLeafArena) - imageOffset
		if uint64(imageLength) > uint64(storeio.CommonPrimaryLeafMaxExtentBytes) {
			return stagedPrimaryBatch{}, storeio.ErrCommonPrimaryLeafFull
		}
		ref := storeio.PageRef{
			Offset: c.batchPrimaryOverflowFileEnd, LogicalID: logicalID,
			Generation: generation, Length: uint32(imageLength),
			Kind: storeio.PagePrimaryLeaf,
		}
		if ref.Offset > math.MaxUint64-uint64(imageLength) {
			return stagedPrimaryBatch{}, storeio.ErrInvalidWrite
		}
		c.batchPrimaryOverflowFileEnd = ref.Offset + uint64(imageLength)
		newLeaves[len(prefix)+rank].volatile = ref
		pendingIndex := 0
		if rank == 0 && len(c.primaryPendingParents) == 0 {
			pendingIndex = -1
		}
		pending := source
		if rank == 0 {
			pending.volatileRef = oldTailRef
		} else {
			pending.volatileRef = storeio.PageRef{}
		}
		c.batchPrimaryLeaves = append(c.batchPrimaryLeaves, primaryBatchLeaf{
			resident: storeio.ResidentPrimaryRoute{
				Ref: ref, Bucket: storeio.BucketID(bucketValue),
			},
			firstKey: rows[0].Key, pending: pending,
			pendingIndex:     pendingIndex,
			tailLineageIndex: len(prefix) + rank,
			nextLeaf:         ref, imageOffset: imageOffset, imageLength: imageLength,
			initialLen: baseEnd - baseAt, finalLen: finalEnd - finalAt,
			docDelta:   (finalEnd - finalAt) - (baseEnd - baseAt),
			applied:    finalEnd - finalAt,
			mutationAt: originalMutationAt, mutationEnd: originalMutationEnd,
		})
		baseAt, finalAt = baseEnd, finalEnd
	}
	if baseAt != len(baseRows) || finalAt != len(prospective) {
		return stagedPrimaryBatch{}, storeio.ErrInvalidWrite
	}
	c.batchPrimaryFileEnd = c.batchPrimaryOverflowFileEnd
	for rank := range newFloors {
		newLeaves[len(prefix)+rank].volatile = c.batchPrimaryLeaves[rank].nextLeaf
	}

	lineage := &primaryTailSplitLineage{
		source: source, tabletID: tabletID, leaves: newLeaves,
		routerWorstCaseBytes: routerWorstCase,
		peakRouterBytes:      routerWorstCase,
	}
	metadata, ok := primaryTailLineageMetadataCharge(lineage)
	if !ok {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	lineage.chargedBytes = metadata
	lineage.peakChargedBytes = metadata
	nextDocumentCount, ok := fileLogicalDocumentCount(
		state.root.DocumentCount, originalDocDelta,
	)
	if !ok {
		return stagedPrimaryBatch{}, storeio.ErrInvalidWrite
	}
	addedRouterFences := make([]storeio.SegmentedTabletRouterLeaf, len(newFloors))
	for rank := range newFloors {
		addedRouterFences[rank] = storeio.SegmentedTabletRouterLeaf{
			LocalID: outputIDs[rank], Fence: replacementFences[rank],
			Ref:  c.batchPrimaryLeaves[rank].nextLeaf,
			Zone: newLeaves[len(prefix)+rank].zone,
		}
	}
	extra, ok := c.primaryTailSplitAdmissionCharge(lineage, routerWorstCase)
	if !ok {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	extra, ok = primaryTailAddCharge(extra, planningCharge)
	if !ok {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	stage := &primaryTailSplitStage{
		nextLineage:          lineage,
		nextFileEnd:          c.batchPrimaryFileEnd,
		nextLogicalID:        c.batchPrimaryNextLogicalID,
		nextDocumentCount:    nextDocumentCount,
		documentCountDelta:   originalDocDelta,
		routerWorstCaseBytes: routerWorstCase,
		routerRetireBytes:    uint64(router.ResidentBytes()),
	}
	if oldTailRef != (storeio.PageRef{}) {
		stage.retireAtPublish = []storeio.PageRef{oldTailRef}
	}
	stage.reservationBytes, ok = c.primaryTailBatchReservationBytes(extra)
	if !ok {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	if _, err := c.ensurePrimaryBatchCapacityWithPolicy(
		true, extra, false,
	); err != nil {
		return stagedPrimaryBatch{}, err
	}
	stage.routerRetiredNextBytes, ok = primaryTailAddCharge(
		c.primaryTailRetiredRouterBytes.Load(), stage.routerRetireBytes,
	)
	if !ok {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	if !c.recordPrimaryTailAdmissionPeak(stage.reservationBytes) {
		return stagedPrimaryBatch{}, ErrCheckpointGroupPressure
	}
	c.batchPrimaryAdmitted = c.batchPrimaryAdmitted[:0]
	if err := c.admitPrimaryBatchOverflow(); err != nil {
		c.unadmitPrimaryBatchLeaves()
		return stagedPrimaryBatch{}, err
	}
	stage.nextRouter, err = router.SplitLeafPartition(
		selected, addedRouterFences, generation,
	)
	if err != nil {
		c.unadmitPrimaryBatchLeaves()
		return stagedPrimaryBatch{}, err
	}
	actualRouterBytes := uint64(stage.nextRouter.ResidentBytes())
	if actualRouterBytes > routerWorstCase {
		c.unadmitPrimaryBatchLeaves()
		return stagedPrimaryBatch{}, fmt.Errorf(
			"%w: resident router allocation bound actual=%d reserved=%d",
			ErrCheckpointGroupPressure, actualRouterBytes, routerWorstCase,
		)
	}
	lineage.routerRetainedBytes = actualRouterBytes
	lineage.peakRouterBytes = max(lineage.peakRouterBytes, actualRouterBytes)
	for rank := range c.batchPrimaryLeaves {
		current, routeOK := stage.nextRouter.ResolveBucketID(
			c.batchPrimaryLeaves[rank].resident.Bucket,
		)
		if !routeOK || current.Ref != c.batchPrimaryLeaves[rank].nextLeaf {
			c.unadmitPrimaryBatchLeaves()
			return stagedPrimaryBatch{}, storeio.ErrSegmentedTabletRouterCorrupt
		}
		c.batchPrimaryLeaves[rank].resident = current
	}
	if err := c.admitPrimaryBatchLeaves(); err != nil {
		c.unadmitPrimaryBatchLeaves()
		return stagedPrimaryBatch{}, err
	}
	return stagedPrimaryBatch{
		state: state, generation: generation,
		tailSplit: stage, live: true,
	}, nil
}

// publishPrimaryTailBatchGateHeld publishes the complete staged tail-lineage
// state after its batch marker has committed. The new router, representative
// physical parent, and file root move together under the reader fence; no
// failure is possible after entering this method.
func (c *Collection) publishPrimaryTailBatchGateHeld(staged stagedPrimaryBatch) {
	stage := staged.tailSplit
	state := staged.state
	if stage == nil || stage.nextLineage == nil || state == nil {
		panic("vibedb: invalid staged primary tail publication")
	}
	nextRoot := state.root
	nextRoot.Generation = staged.generation
	nextRoot.NextLogicalID = stage.nextLogicalID
	nextRoot.DocumentCount = stage.nextDocumentCount
	nextState := &fileStoreState{
		root: nextRoot, fileEnd: stage.nextFileEnd,
		freeHead: state.freeHead,
	}
	lineage := stage.nextLineage
	lineage.source.volatileRef = storeio.PageRef{}

	c.beginReaderFence()
	hadReaders := c.anyActiveReaders()
	currentRouter := c.primaryRouter.Load()
	if currentRouter == nil {
		panic("vibedb: missing resident router during primary tail publication")
	}
	if stage.nextRouter != nil {
		if hadReaders {
			c.primaryTailRetiredRouterBytes.Store(
				stage.routerRetiredNextBytes,
			)
		} else {
			c.primaryTailRetiredRouterBytes.Store(0)
		}
		c.primaryRouter.Store(stage.nextRouter)
	} else {
		currentRouter.UpdateLeaf(
			stage.updatedRoute, stage.updatedRef, staged.generation,
		)
		if !hadReaders {
			c.primaryTailRetiredRouterBytes.Store(0)
		}
	}
	if len(c.primaryPendingParents) == 0 {
		c.primaryPendingParents = append(
			c.primaryPendingParents, lineage.source,
		)
	} else {
		c.primaryPendingParents[0] = lineage.source
		clear(c.primaryPendingParents[1:])
		c.primaryPendingParents = c.primaryPendingParents[:1]
	}
	c.primaryTailSplit = lineage
	c.installPrimaryExactResidentLocked(staged.preparedExact)
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	for _, ref := range stage.retireAtPublish {
		c.retirePrimaryVolatileRefLocked(ref)
	}
	c.refreshPrimaryTailSplitChargeLocked()
	c.primaryTailFallbackOnce = false
	c.endReaderFence()
}

func (c *Collection) primaryTailSplitLeafIndex(bucket storeio.BucketID) int {
	if c == nil || c.primaryTailSplit == nil {
		return -1
	}
	for index := range c.primaryTailSplit.leaves {
		local := uint32(c.primaryTailSplit.leaves[index].localID)
		identity, ok := storeio.MakeTabletLocalIdentityBucket(
			c.primaryTailSplit.tabletID, local,
		)
		if ok && storeio.BucketID(identity) == bucket {
			return index
		}
	}
	return -1
}

// primaryTailBatchCandidate performs the cheap, read-only part of the narrow
// append lane qualification after planPrimaryBatch has grouped mutations.
// Whether each key is strictly beyond the source image's maximum is checked
// while its source lease is held in buildPrimaryBatchLeaf/prepare helpers.
func (c *Collection) primaryTailBatchCandidate(
	state *fileStoreState, conditional bool,
) bool {
	if c == nil || state == nil || !conditional || c.journalReplaying ||
		c.checkpointGroup.Load() == nil || state.root.IndexCount != 0 ||
		state.root.Options&storeio.StateOptionTinIndexes != 0 ||
		c.options.OpaqueValues || c.primaryUnifiedOverlay == nil ||
		c.primaryUnifiedOverlay.hasPending() ||
		len(c.primaryPendingOverflowRetire) != 0 ||
		len(c.primaryMutationAdmitted) != 0 ||
		len(c.batchPrimaryAdmitted) != 0 ||
		len(c.batchPrimaryLeaves) != 1 ||
		len(c.batchPrimaryMutations) == 0 {
		return false
	}
	leaf := &c.batchPrimaryLeaves[0]
	if len(c.batchPrimaryMutations) != leaf.mutationEnd-leaf.mutationAt {
		return false
	}
	for mutationAt := leaf.mutationAt; mutationAt < leaf.mutationEnd; mutationAt++ {
		mutation := &c.batchPrimaryMutations[mutationAt]
		if mutation.remove || !c.primaryOverflowValueIsInline(len(mutation.value)) ||
			mutation.resident.Bucket != leaf.resident.Bucket {
			return false
		}
	}
	router := c.primaryRouter.Load()
	if router == nil || router.Generation() != state.root.Generation ||
		router.Len() == 0 {
		return false
	}
	last, ok := router.RouteAtRank(router.Len() - 1)
	if !ok || last.Bucket != leaf.resident.Bucket ||
		last.Ref != leaf.resident.Ref {
		return false
	}
	if c.primaryTailSplit != nil {
		lineage := c.primaryTailSplit
		if len(lineage.leaves) == 0 ||
			leaf.tailLineageIndex != len(lineage.leaves)-1 ||
			len(c.primaryPendingParents) != 1 ||
			c.primaryPendingParents[0].leafRoute.Bucket !=
				lineage.source.leafRoute.Bucket {
			return false
		}
		lastLeaf := lineage.leaves[len(lineage.leaves)-1]
		bucket, bucketOK := storeio.MakeTabletLocalIdentityBucket(
			lineage.tabletID, uint32(lastLeaf.localID),
		)
		return bucketOK && storeio.BucketID(bucket) == leaf.resident.Bucket &&
			lastLeaf.volatile == leaf.resident.Ref
	}
	if len(c.primaryPendingParents) > 1 ||
		len(c.primaryPendingParents) == 1 &&
			c.primaryPendingParents[0].leafRoute.Bucket != leaf.resident.Bucket {
		return false
	}
	return true
}
