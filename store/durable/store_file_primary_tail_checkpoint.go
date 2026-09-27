package durable

import (
	"fmt"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
)

const primaryTailSplitFoldFixedScratchBytes = uint64(
	storeio.SegmentedTabletRouterRootBytes + 2048,
)

// primaryTailSplitFoldScratchCharge conservatively accounts for the fold's
// right-fence slice, replacement descriptors, resolved route/result entries,
// InsertLeafPartition's second fence slice, its fixed identity bitmap, and the
// encoded root scratch. Full physical leaf transaction pages are charged by
// the foreground admission's bounded transaction-page reservation.
func primaryTailSplitFoldScratchCharge(leaves int) (uint64, bool) {
	if leaves < 2 {
		return 0, false
	}
	count := uint64(leaves)
	perLeaf := uint64(3*unsafe.Sizeof([]byte(nil)) +
		unsafe.Sizeof(storeio.SegmentedTabletRouterLeaf{}) +
		unsafe.Sizeof(primaryTailSplitFoldLeaf{}) + 64)
	maximum := ^uint64(0)
	if count > (maximum-primaryTailSplitFoldFixedScratchBytes)/
		perLeaf {
		return 0, false
	}
	return primaryTailSplitFoldFixedScratchBytes +
		count*perLeaf, true
}

// primaryTailSplitFoldLeaf pairs a reader-visible descendant with the route
// cell that must receive its physical handle when the checkpoint publishes.
// The route is resolved before the transaction's point of no return because
// no lookup may fail after PublishInline.
type primaryTailSplitFoldLeaf struct {
	route      storeio.ResidentPrimaryRoute
	volatile   storeio.PageRef
	checkpoint storeio.PageRef
}

// primaryTailSplitFold is private materialization scratch. The published
// lineage remains intact until the transaction and its route updates publish.
type primaryTailSplitFold struct {
	leaves []primaryTailSplitFoldLeaf
}

// stagePrimaryTailSplitCheckpointLocked folds the complete current partition
// of one original physical leaf into the transaction already used by the
// ordinary checkpoint path. The caller holds c.writer exclusively.
func (c *Collection) stagePrimaryTailSplitCheckpointLocked(
	tx *storeio.WriteTransaction,
	base, visible *fileStoreState,
	generation uint64,
) (*primaryTailSplitFold, error) {
	if c == nil || tx == nil || base == nil || visible == nil {
		return nil, fmt.Errorf("%w: primary tail split checkpoint state", storeio.ErrInvalidWrite)
	}
	lineage := c.primaryTailSplit
	if lineage == nil || c.journalReplaying || c.primaryExactActive() ||
		len(c.primaryPendingParents) != 1 || len(lineage.leaves) < 2 {
		return nil, fmt.Errorf("%w: primary tail split checkpoint state", storeio.ErrInvalidWrite)
	}
	parent := &c.primaryPendingParents[0]
	source := lineage.source
	if parent.leafRoute.Ref != source.leafRoute.Ref ||
		parent.leafRoute.Bucket != source.leafRoute.Bucket ||
		parent.anchorRoute.Ref != source.anchorRoute.Ref ||
		parent.tabletRoute.Ref != source.tabletRoute.Ref ||
		parent.catalogRef != source.catalogRef ||
		parent.branchRef != source.branchRef ||
		parent.hasBranch != source.hasBranch {
		return nil, fmt.Errorf("%w: primary tail split source parent", storeio.ErrInvalidWrite)
	}
	tabletID, sourceLocalID, ok := storeio.SplitTabletLocalIdentityBucket(
		uint32(source.leafRoute.Bucket),
	)
	if !ok || tabletID != lineage.tabletID ||
		lineage.leaves[0].localID != sourceLocalID ||
		generation != visible.root.Generation ||
		generation <= base.root.Generation {
		return nil, fmt.Errorf("%w: primary tail split identity", storeio.ErrInvalidWrite)
	}

	baseBounds := storeio.GlobalTabletCatalogBounds{
		StoreID: c.storeID, SelectedRootGeneration: base.root.Generation,
		FileEnd: base.fileEnd, NextLogicalID: base.root.NextLogicalID,
	}
	tabletLease, err := c.cache.Acquire(source.tabletRoute.Ref)
	if err != nil {
		return nil, err
	}
	tablet := storeio.AdmittedGlobalTabletCatalogTabletRoot(
		tabletLease.Page(), baseBounds,
	)
	defer tabletLease.Release()
	if tablet.TabletID() != lineage.tabletID {
		return nil, storeio.ErrGlobalTabletCatalogCorrupt
	}
	locatorRef, ok := tablet.LocatorRef()
	if !ok {
		return nil, storeio.ErrGlobalTabletCatalogCorrupt
	}
	locatorLease, err := c.cache.Acquire(locatorRef)
	if err != nil {
		return nil, err
	}
	locator, err := storeio.OpenGlobalTabletCatalogLocator(
		locatorLease.Page(), locatorRef, baseBounds,
	)
	if err != nil {
		locatorLease.Release()
		return nil, err
	}
	defer locatorLease.Release()
	anchorLease, err := c.cache.Acquire(source.anchorRoute.Ref)
	if err != nil {
		return nil, err
	}
	anchor := storeio.AdmittedGlobalTabletCatalogAnchor(
		anchorLease.Page(), &tablet, source.anchorRoute.PageID,
	)
	defer anchorLease.Release()

	// Requalify the complete partition against the original sealed anchor before
	// allocating any checkpoint pages. This preserves the physical anchor's
	// local fence coordinate even though the resident router carries a global
	// tablet floor for its first route.
	rightFences := make([][]byte, len(lineage.leaves)-1)
	for index := 1; index < len(lineage.leaves); index++ {
		rightFences[index-1] = lineage.leaves[index].localFloor
	}
	plan, err := tablet.PlanLeafPartition(
		&anchor, source.leafRoute, rightFences,
	)
	if err != nil {
		return nil, err
	}
	if plan.RequiresTabletRebuild() ||
		int(plan.ReplacementCount()) != len(lineage.leaves) {
		return nil, ErrCheckpointGroupPressure
	}

	replacements := make([]storeio.SegmentedTabletRouterLeaf, len(lineage.leaves))
	fold := &primaryTailSplitFold{
		leaves: make([]primaryTailSplitFoldLeaf, len(lineage.leaves)),
	}
	layout, err := storeio.MutableStoreLayout(uint32(c.options.PageSize))
	if err != nil {
		return nil, err
	}
	leafBounds := c.primaryLeafBounds(visible)
	for index := range lineage.leaves {
		leaf := &lineage.leaves[index]
		bucketValue, bucketOK := storeio.MakeTabletLocalIdentityBucket(
			lineage.tabletID, uint32(leaf.localID),
		)
		logicalID, logicalOK := storeio.CommonPrimaryLeafLogicalID(
			storeio.BucketID(bucketValue),
		)
		if !bucketOK || !logicalOK || leaf.volatile.Kind != storeio.PagePrimaryLeaf ||
			leaf.volatile.LogicalID != logicalID || leaf.volatile.Offset < base.fileEnd {
			return nil, fmt.Errorf("%w: primary tail split volatile leaf identity", storeio.ErrCommonPrimaryLeafCorrupt)
		}
		bucket := storeio.BucketID(bucketValue)
		lease, acquireErr := c.cache.Acquire(leaf.volatile)
		if acquireErr != nil {
			return nil, acquireErr
		}
		header := lease.Header()
		var image []byte
		var page storeio.TransactionPage
		var allocErr error
		if storeio.PrimaryLeafClass(lease.Page()) == storeio.CommonPrimaryLeafCompact {
			stripe, stripeOK := storeio.AdmittedCompactPrimaryStripe(
				lease.Page(), c.storeID, bucket,
			)
			if !stripeOK {
				lease.Release()
				return nil, storeio.ErrCommonPrimaryLeafCorrupt
			}
			records, renderErr := stripe.RenderRecordsWithScratch(
				c.primaryLeafMutationScratch,
			)
			if renderErr == nil {
				for _, record := range records {
					if record.Value.IsOverflow() {
						renderErr = ErrCheckpointGroupPressure
						break
					}
				}
			}
			leafHeader := stripe.Header()
			leafHeader.Generation = generation
			if renderErr == nil {
				image, renderErr = storeio.EncodeBestCompactPrimaryStripe(
					c.primaryLeafScratch, leafHeader, c.storeID,
					records, c.primaryUnifiedBuilder,
				)
			}
			lease.Release()
			if renderErr != nil {
				return nil, renderErr
			}
			page, allocErr = tx.AllocateNear(
				storeio.PagePrimaryLeaf, uint32(len(image)), leaf.volatile.LogicalID,
				primaryLeafPlacementHint(bucket, layout.DataStart),
			)
			if allocErr != nil {
				return nil, allocErr
			}
			copy(page.Bytes(), image)
		} else {
			view := storeio.AdmittedCommonPrimaryLeaf(
				lease.Page(), c.storeID, bucket, leafBounds,
			)
			if view.HasOverflowRows() {
				lease.Release()
				return nil, ErrCheckpointGroupPressure
			}
			header.Generation = generation
			page, acquireErr = tx.AllocateNear(
				storeio.PagePrimaryLeaf, header.PageSize, leaf.volatile.LogicalID,
				primaryLeafPlacementHint(bucket, layout.DataStart),
			)
			if acquireErr != nil {
				lease.Release()
				return nil, acquireErr
			}
			payload, initErr := storeio.InitPage(page.Bytes(), header)
			if initErr == nil {
				copy(payload, lease.Payload())
				_, initErr = storeio.SealPage(page.Bytes())
			}
			lease.Release()
			if initErr != nil {
				return nil, initErr
			}
		}
		if stageErr := page.Stage(); stageErr != nil {
			return nil, stageErr
		}
		ref := page.Ref()
		replacements[index] = storeio.SegmentedTabletRouterLeaf{
			LocalID: leaf.localID, Fence: leaf.localFloor,
			Ref: ref, Zone: leaf.zone,
		}
		fold.leaves[index] = primaryTailSplitFoldLeaf{
			volatile: leaf.volatile, checkpoint: ref,
		}
	}

	locatorLogical, ok := storeio.GlobalTabletCatalogLocatorLogicalID(
		lineage.tabletID,
	)
	if !ok {
		return nil, storeio.ErrGlobalTabletCatalogCorrupt
	}
	locatorPage, err := tx.AllocateNear(
		storeio.PagePrimaryLocator, storeio.GlobalTabletCatalogLocatorBytes,
		locatorLogical, locatorRef.Offset,
	)
	if err != nil {
		return nil, err
	}
	anchorPage, err := tx.AllocateNear(
		storeio.PagePrimaryAnchor,
		storeio.SegmentedTabletRouterAnchorPageBytes,
		source.anchorRoute.Ref.LogicalID, source.anchorRoute.Ref.Offset,
	)
	if err != nil {
		return nil, err
	}
	tabletPage, err := tx.AllocateNear(
		storeio.PageTabletRoute, storeio.GlobalTabletCatalogTabletBytes,
		source.tabletRoute.Ref.LogicalID, source.tabletRoute.Ref.Offset,
	)
	if err != nil {
		return nil, err
	}
	rawRoot := make([]byte, storeio.SegmentedTabletRouterRootBytes)
	partition, err := tablet.InsertLeafPartition(
		rawRoot, locatorPage.Bytes(), anchorPage.Bytes(), generation,
		source.leafRoute, replacements, anchorPage.Ref(), &locator, &anchor,
	)
	if err != nil {
		return nil, err
	}
	if err := anchorPage.Stage(); err != nil {
		return nil, err
	}
	if err := locatorPage.Stage(); err != nil {
		return nil, err
	}
	mutationBounds := c.primaryMutationBounds(tx)
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
		mutationBounds, locatorPage.Ref(), partition.Root,
	); err != nil {
		return nil, err
	}
	if err := tabletPage.Stage(); err != nil {
		return nil, err
	}
	for _, old := range []storeio.PageRef{
		source.leafRoute.Ref, locatorRef,
		source.anchorRoute.Ref, source.tabletRoute.Ref,
	} {
		if err := c.appendPrimaryRetirement(base, old); err != nil {
			return nil, err
		}
	}

	router := c.primaryRouter.Load()
	for index := range lineage.leaves {
		leaf := &lineage.leaves[index]
		bucketValue, _ := storeio.MakeTabletLocalIdentityBucket(
			lineage.tabletID, uint32(leaf.localID),
		)
		bucket := storeio.BucketID(bucketValue)
		route, routeOK := router.ResolveBucketID(bucket)
		if !routeOK || route.Ref != leaf.volatile || route.Bucket != bucket {
			return nil, fmt.Errorf("%w: primary tail split resident route", storeio.ErrSegmentedTabletRouterCorrupt)
		}
		fold.leaves[index].route = route
	}
	parent.checkpointLeaf = replacements[0].Ref
	parent.checkpointAnchor = anchorPage.Ref()
	parent.checkpointTablet = tabletPage.Ref()
	return fold, nil
}
