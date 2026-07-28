package durable

import (
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
	vibejson "github.com/thesyncim/vibejson"
)

const filePrimaryPendingParentLimit = 64

// primaryLeafPlacementStride is the per-rank spacing of the placement target
// below: a wide leaf extent plus half a narrow one of slack. It must be at least
// the widest leaf extent, since a churn insert promotes a leaf to the wide class
// and distinct ranks' targets must not overlap; the extra half page budgets for
// the anchor/tablet/catalog framing interleaved with the leaves so a target does
// not fall on a neighbour's live page. Undersizing it packs targets so tightly
// that a rank's slot is usually occupied and nearest-fit snaps the leaf onto a
// far free extent, scattering the order the hint is restoring; oversizing it
// spreads leaves wider than they need and grows the file with reclaimable gaps.
// Measured on the churn qualification (100k mutations, 513 leaves): this value
// holds the adjacency plateau near 25x for a ~7% file-size cost, an 8 KiB stride
// only reaches ~50x, and a 12 KiB stride reaches ~5x for a ~19% file-size cost.
// Nearest-fit only needs the stride monotonic in rank; the value is the knob.
const primaryLeafPlacementStride = uint64(storeio.CommonPrimaryLeafWideBytes +
	storeio.CommonPrimaryLeafNarrowBytes/2)

// primaryLeafPlacementHint maps a leaf's stable lexical rank to a target file
// offset, the placement hint a churned leaf's copy-on-write is drawn toward.
//
// A BucketID is tabletID<<localBits | localID, and both tablet and local ids are
// assigned in key order at build time and preserved across every rewrite, so the
// bucket increases monotonically with lexical order — it is the leaf's sorted
// rank. Hinting toward rank*stride rather than the leaf's last physical offset is
// what makes placement a restoring force instead of a random walk: a leaf that
// churned away from its sorted position is pulled back toward it, so the physical
// order converges on the lexical order the adjacency metric rewards. Nearest-fit
// snaps the target to the closest reusable extent, so the stride only needs to be
// monotonic in rank; the DataStart base keeps the hint non-zero, since a zero
// hint disables placement.
func primaryLeafPlacementHint(bucket storeio.BucketID, dataStart uint64) uint64 {
	return dataStart + uint64(bucket)*primaryLeafPlacementStride
}

type filePrimaryMutationPath struct {
	rootLease    storeio.PageLease
	branchLease  storeio.PageLease
	catalogLease storeio.PageLease
	tabletLease  storeio.PageLease
	anchorLease  storeio.PageLease
	leafLease    storeio.PageLease

	root    storeio.GlobalTabletCatalogNodeView
	branch  storeio.GlobalTabletCatalogNodeView
	catalog storeio.GlobalTabletCatalogNodeView
	tablet  storeio.GlobalTabletCatalogTabletRootView
	anchor  storeio.GlobalTabletCatalogAnchorView
	leaf    storeio.CommonPrimaryLeafView
	// detemplated is true when leaf is a raw view de-templated from a template
	// leaf. Its bytes do not alias the resident page, so an in-place patch is
	// invalid and the mutation must take the copy-on-write rewrite path.
	detemplated bool

	rootRoute    storeio.GlobalTabletCatalogNodeRoute
	catalogRoute storeio.GlobalTabletCatalogNodeRoute
	tabletRoute  storeio.GlobalTabletCatalogNodeRoute
	anchorRoute  storeio.GlobalTabletCatalogAnchorRoute
	leafRoute    storeio.SegmentedTabletRouterRoute
	branchRef    storeio.PageRef
	catalogRef   storeio.PageRef
	hasBranch    bool
}

// filePrimaryPendingParent is the bounded bridge between the mutable resident
// router and the last sealed primary graph. leafRoute and every parent route
// remain the sealed identities until checkpoint; volatileRef is the newest
// reader-visible leaf frame installed in the router.
type filePrimaryPendingParent struct {
	resident storeio.ResidentPrimaryRoute

	rootRoute    storeio.GlobalTabletCatalogNodeRoute
	catalogRoute storeio.GlobalTabletCatalogNodeRoute
	tabletRoute  storeio.GlobalTabletCatalogNodeRoute
	anchorRoute  storeio.GlobalTabletCatalogAnchorRoute
	leafRoute    storeio.SegmentedTabletRouterRoute
	branchRef    storeio.PageRef
	catalogRef   storeio.PageRef
	hasBranch    bool

	volatileRef storeio.PageRef

	checkpointLeaf    storeio.PageRef
	checkpointAnchor  storeio.PageRef
	checkpointTablet  storeio.PageRef
	checkpointCatalog storeio.PageRef
	checkpointBranch  storeio.PageRef
}

func (p *filePrimaryMutationPath) Release() {
	if p == nil {
		return
	}
	p.leafLease.Release()
	p.anchorLease.Release()
	p.tabletLease.Release()
	p.catalogLease.Release()
	p.branchLease.Release()
	p.rootLease.Release()
}

func (c *Collection) acquirePrimaryMutationPath(
	path *filePrimaryMutationPath,
	state *fileStoreState,
	key []byte,
	resident storeio.ResidentPrimaryRoute,
) (err error) {
	if c == nil || path == nil || state == nil ||
		state.root.PrimaryRoot == (storeio.PageRef{}) {
		return fmt.Errorf(
			"%w: ordered primary mutation path",
			storeio.ErrGlobalTabletCatalogCorrupt,
		)
	}
	*path = filePrimaryMutationPath{}
	defer func() {
		if err != nil {
			path.Release()
		}
	}()
	bounds := storeio.GlobalTabletCatalogBounds{
		StoreID: c.storeID, SelectedRootGeneration: state.root.Generation,
		FileEnd:       state.super.FileEnd,
		NextLogicalID: state.root.NextLogicalID,
	}
	path.rootLease, err = c.cache.Acquire(state.root.PrimaryRoot)
	if err != nil {
		return err
	}
	path.root = storeio.AdmittedGlobalTabletCatalogNode(
		path.rootLease.Page(), bounds,
	)
	if path.root.Level() != storeio.GlobalTabletCatalogRoot {
		return storeio.ErrGlobalTabletCatalogCorrupt
	}
	path.rootRoute = path.root.Route(key)
	if path.rootRoute.Ref == (storeio.PageRef{}) {
		return storeio.ErrGlobalTabletCatalogCorrupt
	}

	childRoute := path.rootRoute
	if path.root.ChildLevel() == storeio.GlobalTabletCatalogBranch {
		path.hasBranch = true
		path.branchRef = path.rootRoute.Ref
		path.branchLease, err = c.cache.Acquire(path.rootRoute.Ref)
		if err != nil {
			return err
		}
		path.branch = storeio.AdmittedGlobalTabletCatalogNode(
			path.branchLease.Page(), bounds,
		)
		if path.branch.Level() != storeio.GlobalTabletCatalogBranch {
			return storeio.ErrGlobalTabletCatalogCorrupt
		}
		path.catalogRoute = path.branch.Route(key)
		childRoute = path.catalogRoute
	} else {
		path.catalogRoute = path.rootRoute
	}
	if childRoute.Ref == (storeio.PageRef{}) {
		return storeio.ErrGlobalTabletCatalogCorrupt
	}
	path.catalogRef = childRoute.Ref
	path.catalogLease, err = c.cache.Acquire(childRoute.Ref)
	if err != nil {
		return err
	}
	path.catalog = storeio.AdmittedGlobalTabletCatalogNode(
		path.catalogLease.Page(), bounds,
	)
	if path.catalog.Level() != storeio.GlobalTabletCatalogLeaf {
		return storeio.ErrGlobalTabletCatalogCorrupt
	}
	path.tabletRoute = path.catalog.Route(key)
	if path.tabletRoute.Ref == (storeio.PageRef{}) {
		return storeio.ErrGlobalTabletCatalogCorrupt
	}
	path.tabletLease, err = c.cache.Acquire(path.tabletRoute.Ref)
	if err != nil {
		return err
	}
	path.tablet = storeio.AdmittedGlobalTabletCatalogTabletRoot(
		path.tabletLease.Page(), bounds,
	)
	path.anchorRoute, _ = path.tablet.RouteAnchor(key)
	if path.anchorRoute.Ref == (storeio.PageRef{}) {
		return storeio.ErrGlobalTabletCatalogCorrupt
	}
	path.anchorLease, err = c.cache.Acquire(path.anchorRoute.Ref)
	if err != nil {
		return err
	}
	path.anchor = storeio.AdmittedGlobalTabletCatalogAnchor(
		path.anchorLease.Page(), &path.tablet, path.anchorRoute.PageID,
	)
	hash := storeio.KeyHashBytes(c.storeID, key)
	path.leafRoute, _ = path.anchor.RouteHashed(hash, key)
	if path.leafRoute.Ref == (storeio.PageRef{}) ||
		path.leafRoute.Ref != resident.Ref ||
		path.leafRoute.Bucket != resident.Bucket {
		return storeio.ErrSegmentedTabletRouterCorrupt
	}
	path.leafLease, err = c.cache.Acquire(path.leafRoute.Ref)
	if err != nil {
		return err
	}
	path.leaf, path.detemplated, err = storeio.AdmittedPrimaryLeafForMutation(
		path.leafLease.Page(), c.storeID, path.leafRoute.Bucket,
		storeio.CommonPrimaryLeafBounds{
			FileEnd:           state.super.FileEnd,
			NextLogicalID:     state.root.NextLogicalID,
			AllocationQuantum: state.root.PageSize,
		},
	)
	return err
}

func (c *Collection) currentPrimaryResidentRoute(
	state *fileStoreState,
	key []byte,
) (storeio.ResidentPrimaryRoute, error) {
	router := c.primaryRouter.Load()
	if router == nil ||
		router.Generation() != state.root.Generation {
		return storeio.ResidentPrimaryRoute{},
			storeio.ErrSegmentedTabletRouterCorrupt
	}
	route, ok := router.Route(key)
	if !ok || router.Generation() != state.root.Generation {
		return storeio.ResidentPrimaryRoute{},
			storeio.ErrSegmentedTabletRouterCorrupt
	}
	return route, nil
}

func (c *Collection) primaryPendingParentIndex(
	bucket storeio.BucketID,
) int {
	for index := range c.primaryPendingParents {
		if c.primaryPendingParents[index].leafRoute.Bucket == bucket {
			return index
		}
	}
	return -1
}

func (c *Collection) primaryPendingParentRefIndex(
	ref storeio.PageRef,
) int {
	for index := range c.primaryPendingParents {
		if c.primaryPendingParents[index].volatileRef == ref {
			return index
		}
	}
	return -1
}

func (c *Collection) primaryPendingTablet(
	bucket storeio.BucketID,
) bool {
	tabletID, _, ok := storeio.SplitTabletLocalIdentityBucket(
		uint32(bucket),
	)
	if !ok {
		return false
	}
	for index := range c.primaryPendingParents {
		pendingTablet, _, pendingOK :=
			storeio.SplitTabletLocalIdentityBucket(
				uint32(
					c.primaryPendingParents[index].
						leafRoute.Bucket,
				),
			)
		if pendingOK && pendingTablet == tabletID {
			return true
		}
	}
	return false
}

func filePrimaryPendingParentFromPath(
	resident storeio.ResidentPrimaryRoute,
	path *filePrimaryMutationPath,
) filePrimaryPendingParent {
	return filePrimaryPendingParent{
		resident: resident,

		rootRoute:    path.rootRoute,
		catalogRoute: path.catalogRoute,
		tabletRoute:  path.tabletRoute,
		anchorRoute:  path.anchorRoute,
		leafRoute:    path.leafRoute,
		branchRef:    path.branchRef,
		catalogRef:   path.catalogRef,
		hasBranch:    path.hasBranch,
	}
}

func (c *Collection) clearPrimaryVolatileRetiredLocked() {
	if len(c.primaryVolatileRetired) == 0 {
		return
	}
	c.snapshotGate.Lock()
	if !c.leases.AnyActive() {
		c.cache.MarkUnreachable(c.primaryVolatileRetired)
		clear(c.primaryVolatileRetired)
		c.primaryVolatileRetired =
			c.primaryVolatileRetired[:0]
	}
	c.snapshotGate.Unlock()
}

// retirePrimaryVolatileRefLocked runs while snapshotGate is held. A selected
// route can race from the router to PageCache without holding that gate, so an
// active generation lease defers removal instead of turning a cache miss into
// an impossible read from the memory-only virtual extent.
func (c *Collection) retirePrimaryVolatileRefLocked(
	ref storeio.PageRef,
) {
	if ref == (storeio.PageRef{}) {
		return
	}
	if !c.leases.AnyActive() {
		c.cache.MarkUnreachable([]storeio.PageRef{ref})
		return
	}
	c.primaryVolatileRetired = append(
		c.primaryVolatileRetired, ref,
	)
}

func (c *Collection) ensureBufferedPrimaryMutationCapacity(
	resident storeio.ResidentPrimaryRoute,
) error {
	if cap(c.primaryPendingParents) == 0 {
		c.primaryPendingParents = make(
			[]filePrimaryPendingParent, 0,
			min(
				c.options.MaxRetiredExtents,
				filePrimaryPendingParentLimit,
			),
		)
	}
	if cap(c.primaryVolatileRetired) == 0 {
		c.primaryVolatileRetired = make(
			[]storeio.PageRef, 0,
			min(
				c.options.MaxRetiredExtents,
				filePrimaryPendingParentLimit,
			),
		)
	}
	c.clearPrimaryVolatileRetiredLocked()
	if len(c.primaryVolatileRetired) == cap(c.primaryVolatileRetired) {
		return fmt.Errorf(
			"%w: buffered primary volatile-reference capacity %d",
			storeio.ErrRetiredExtentCapacity,
			cap(c.primaryVolatileRetired),
		)
	}
	if c.primaryPendingParentIndex(resident.Bucket) < 0 &&
		len(c.primaryPendingParents) == cap(c.primaryPendingParents) {
		if err := c.checkpointBufferedLocked(); err != nil {
			return err
		}
		c.automaticCheckpoints.Add(1)
	}
	return c.ensureDirtyCapacityFor(
		0,
		c.cache.ReservationBytes(
			storeio.CommonPrimaryLeafWideBytes,
		),
	)
}

func (c *Collection) putPrimary(
	key string,
	src []byte,
) (created bool, err error) {
	c.writer.Lock()
	var generation uint64
	defer func() {
		wait := generation != 0 && c.synchronous()
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
		return false, ErrClosed
	}
	if failure := c.PersistenceError(); failure != nil {
		return false, failure
	}
	if len(key) == 0 ||
		len(key) > c.options.MaxKeyBytes ||
		len(key) > storeio.CommonPrimaryLeafMaxKeyBytes {
		return false, ErrKeyTooLarge
	}
	if len(src) == 0 ||
		len(src) > c.options.MaxDocumentBytes ||
		len(src) > c.options.InlineValueBytes {
		return false, ErrDocumentTooLarge
	}
	if err := vibejson.Validate(src); err != nil {
		return false, err
	}
	state := c.state.Load()
	if state == nil || state.root.PrimaryRoot == (storeio.PageRef{}) {
		return false, ErrClosed
	}
	c.pointKeyScratch = append(c.pointKeyScratch[:0], key...)
	keyBytes := c.pointKeyScratch
	resident, err := c.currentPrimaryResidentRoute(state, keyBytes)
	if err != nil {
		return false, err
	}
	if c.buffered() && !c.leases.AnyActive() {
		if err := c.ensureBufferedPrimaryMutationCapacity(
			resident,
		); err != nil {
			return false, err
		}
		state = c.state.Load()
		if state == nil {
			return false, ErrClosed
		}
		resident, err = c.currentPrimaryResidentRoute(
			state, keyBytes,
		)
		if err != nil {
			return false, err
		}
	}
	leafLease, err := c.primaryRouter.Load().AcquireLeaf(c.cache, resident)
	if err != nil {
		return false, err
	}
	leaf, detemplated, err := storeio.AdmittedPrimaryLeafForMutation(
		leafLease.Page(), c.storeID, resident.Bucket,
		storeio.CommonPrimaryLeafBounds{
			FileEnd:           state.super.FileEnd,
			NextLogicalID:     state.root.NextLogicalID,
			AllocationQuantum: state.root.PageSize,
		},
	)
	if err != nil {
		leafLease.Release()
		return false, err
	}
	slot, raw, overflow, found := leaf.LookupRawHashed(
		resident.Hash, keyBytes,
	)
	if overflow {
		leafLease.Release()
		return false, fmt.Errorf(
			"%w: ordered primary overflow mutation",
			ErrPrimaryCutoverUnsupported,
		)
	}
	created = !found
	trackFirstTouch := false
	// A de-templated leaf's bytes do not alias the resident page, so the
	// buffered in-place patch cannot apply: the rewrite below replaces the
	// template page with a raw leaf.
	if found && !detemplated {
		handled, track, inplaceErr := c.tryBufferedPrimaryInplace(
			state, src, raw, resident.Ref, &leafLease,
		)
		if inplaceErr != nil {
			leafLease.Release()
			return false, inplaceErr
		}
		trackFirstTouch = track
		if handled {
			leafLease.Release()
			generation = state.root.Generation + 1
			if err := c.journalAckLocked(
				storeio.RecoveryRecordKindPut, generation, keyBytes, src,
			); err != nil {
				return false, err
			}
			return false, nil
		}
	}
	if c.buffered() && !c.leases.AnyActive() {
		nextLeaf, becameEmpty, filledEmpty, mutationErr :=
			c.cowBufferedPrimaryMutation(
				state, keyBytes, src, false, found, slot,
				resident, &leaf,
			)
		leafLease.Release()
		if mutationErr != nil {
			// A split-required signal must leave the tablet's parents durable
			// before the caller acts on it: the split transaction (when routed)
			// or the caller's own retry both need real pages, not pending edits.
			if errors.Is(
				mutationErr, ErrPrimaryLeafSplitRequired,
			) && c.primaryPendingTablet(resident.Bucket) {
				mutationErr = errors.Join(
					mutationErr,
					c.checkpointBufferedLocked(),
				)
			}
			return false, mutationErr
		}
		if trackFirstTouch {
			c.rememberBufferedFirstTouch(nextLeaf)
		}
		if becameEmpty && c.primaryRouter.Load().MarkEmpty(resident) {
			c.primaryEmptyLeaves.Add(1)
		}
		if filledEmpty && c.primaryRouter.Load().ClearEmpty(resident) {
			c.removePrimaryEmptyLeaf()
		}
		generation = state.root.Generation + 1
		if err := c.journalAckLocked(
			storeio.RecoveryRecordKindPut, generation, keyBytes, src,
		); err != nil {
			return created, err
		}
		return created, nil
	}
	leafLease.Release()
	if c.buffered() && len(c.primaryPendingParents) != 0 {
		if err := c.materializePrimaryParentsLocked(); err != nil {
			return false, err
		}
		state = c.state.Load()
		if state == nil {
			return false, ErrClosed
		}
		resident, err = c.currentPrimaryResidentRoute(
			state, keyBytes,
		)
		if err != nil {
			return false, err
		}
	}
	if err := c.ensureDirtyCapacityFor(
		c.options.singleDocumentTransactionPages,
		c.options.singleDocumentTransactionBytes,
	); err != nil {
		return false, err
	}
	var path filePrimaryMutationPath
	if err := c.acquirePrimaryMutationPath(
		&path, state, keyBytes, resident,
	); err != nil {
		return false, err
	}
	defer path.Release()
	slot, _, overflow, resolvedFound := path.leaf.LookupRawHashed(
		path.leafRoute.Hash, keyBytes,
	)
	if overflow {
		return false, fmt.Errorf(
			"%w: ordered primary overflow mutation",
			ErrPrimaryCutoverUnsupported,
		)
	}
	if resolvedFound != found {
		return false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	nextLeaf, becameEmpty, filledEmpty, err :=
		c.cowPrimaryMutation(
			state, keyBytes, src, false, found, slot,
			resident, &path,
		)
	if err != nil {
		return false, err
	}
	if trackFirstTouch {
		c.rememberBufferedFirstTouch(nextLeaf)
	}
	if becameEmpty && c.primaryRouter.Load().MarkEmpty(resident) {
		c.primaryEmptyLeaves.Add(1)
	}
	if filledEmpty && c.primaryRouter.Load().ClearEmpty(resident) {
		c.removePrimaryEmptyLeaf()
	}
	generation = state.root.Generation + 1
	return created, nil
}

func (c *Collection) deletePrimary(
	key string,
) (deleted bool, err error) {
	c.writer.Lock()
	var generation uint64
	defer func() {
		wait := generation != 0 && c.synchronous()
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
		return false, ErrClosed
	}
	if failure := c.PersistenceError(); failure != nil {
		return false, failure
	}
	if len(key) == 0 ||
		len(key) > c.options.MaxKeyBytes ||
		len(key) > storeio.CommonPrimaryLeafMaxKeyBytes {
		return false, ErrKeyTooLarge
	}
	state := c.state.Load()
	if state == nil || state.root.PrimaryRoot == (storeio.PageRef{}) {
		return false, nil
	}
	c.pointKeyScratch = append(c.pointKeyScratch[:0], key...)
	keyBytes := c.pointKeyScratch
	resident, err := c.currentPrimaryResidentRoute(state, keyBytes)
	if err != nil {
		return false, err
	}
	if c.buffered() && !c.leases.AnyActive() {
		if err := c.ensureBufferedPrimaryMutationCapacity(
			resident,
		); err != nil {
			return false, err
		}
		state = c.state.Load()
		if state == nil {
			return false, ErrClosed
		}
		resident, err = c.currentPrimaryResidentRoute(
			state, keyBytes,
		)
		if err != nil {
			return false, err
		}
		leafLease, acquireErr :=
			c.primaryRouter.Load().AcquireLeaf(c.cache, resident)
		if acquireErr != nil {
			return false, acquireErr
		}
		leaf, _, detemplateErr := storeio.AdmittedPrimaryLeafForMutation(
			leafLease.Page(), c.storeID, resident.Bucket,
			storeio.CommonPrimaryLeafBounds{
				FileEnd:           state.super.FileEnd,
				NextLogicalID:     state.root.NextLogicalID,
				AllocationQuantum: state.root.PageSize,
			},
		)
		if detemplateErr != nil {
			leafLease.Release()
			return false, detemplateErr
		}
		slot, _, overflow, found := leaf.LookupRawHashed(
			resident.Hash, keyBytes,
		)
		if !found {
			leafLease.Release()
			return false, nil
		}
		if overflow {
			leafLease.Release()
			return false, fmt.Errorf(
				"%w: ordered primary overflow mutation",
				ErrPrimaryCutoverUnsupported,
			)
		}
		_, becameEmpty, _, mutationErr :=
			c.cowBufferedPrimaryMutation(
				state, keyBytes, nil, true, true, slot,
				resident, &leaf,
			)
		leafLease.Release()
		if mutationErr != nil {
			if errors.Is(
				mutationErr, ErrPrimaryLeafSplitRequired,
			) && c.primaryPendingTablet(resident.Bucket) {
				mutationErr = errors.Join(
					mutationErr,
					c.checkpointBufferedLocked(),
				)
			}
			return false, mutationErr
		}
		if becameEmpty && c.primaryRouter.Load().MarkEmpty(resident) {
			c.primaryEmptyLeaves.Add(1)
		}
		generation = state.root.Generation + 1
		if err := c.journalAckLocked(
			storeio.RecoveryRecordKindDelete, generation, keyBytes, nil,
		); err != nil {
			return true, err
		}
		return true, nil
	}
	if c.buffered() && len(c.primaryPendingParents) != 0 {
		if err := c.materializePrimaryParentsLocked(); err != nil {
			return false, err
		}
		state = c.state.Load()
		if state == nil {
			return false, ErrClosed
		}
		resident, err = c.currentPrimaryResidentRoute(
			state, keyBytes,
		)
		if err != nil {
			return false, err
		}
	}
	var path filePrimaryMutationPath
	if err := c.acquirePrimaryMutationPath(
		&path, state, keyBytes, resident,
	); err != nil {
		return false, err
	}
	defer path.Release()
	slot, _, overflow, found := path.leaf.LookupRawHashed(
		path.leafRoute.Hash, keyBytes,
	)
	if !found {
		return false, nil
	}
	if overflow {
		return false, fmt.Errorf(
			"%w: ordered primary overflow mutation",
			ErrPrimaryCutoverUnsupported,
		)
	}
	if err := c.ensureDirtyCapacityFor(
		c.options.singleDocumentTransactionPages,
		c.options.singleDocumentTransactionBytes,
	); err != nil {
		return false, err
	}
	_, becameEmpty, _, err := c.cowPrimaryMutation(
		state, keyBytes, nil, true, true, slot,
		resident, &path,
	)
	if err != nil {
		return false, err
	}
	if becameEmpty && c.primaryRouter.Load().MarkEmpty(resident) {
		c.primaryEmptyLeaves.Add(1)
	}
	generation = state.root.Generation + 1
	return true, nil
}

func (c *Collection) tryBufferedPrimaryInplace(
	state *fileStoreState,
	src, before []byte,
	ref storeio.PageRef,
	leafLease *storeio.PageLease,
) (handled bool, trackFirstTouch bool, err error) {
	if c == nil || state == nil || leafLease == nil || !c.buffered() {
		return false, false, nil
	}
	c.bufferedInplaceAttempts.Add(1)
	fallback := func() (bool, bool, error) {
		c.bufferedInplaceFallbacks.Add(1)
		return false, false, nil
	}
	if len(c.options.indexes) != 0 ||
		len(c.options.float64Columns) != 0 ||
		c.options.Collection.Schema != nil ||
		len(before) == 0 || len(before) != len(src) {
		return fallback()
	}
	valueOffset, ok := leafLease.OffsetOf(before)
	if !ok {
		return false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	if !c.bufferedFirstTouchContains(ref) {
		if !c.bufferedFirstTouchCapacityAvailable() {
			c.bufferedFirstTouchOverflows.Add(1)
			return fallback()
		}
		return false, true, nil
	}
	handled, err = c.replaceBufferedPrimaryInplace(
		state, src, before, valueOffset, ref, leafLease,
	)
	if handled || err != nil {
		return handled, false, err
	}
	c.takeBufferedFirstTouch(ref)
	return false, true, nil
}

func (c *Collection) replaceBufferedPrimaryInplace(
	state *fileStoreState,
	src, old []byte,
	valueOffset uint32,
	ref storeio.PageRef,
	leafLease *storeio.PageLease,
) (handled bool, err error) {
	generation := state.root.Generation + 1
	if generation == 0 {
		return false, storeio.ErrGenerationOrder
	}
	if c.primaryPendingParentRefIndex(ref) < 0 {
		// Only a leaf already represented in the pending-parent set can be
		// advanced without queuing a root token.
		return false, nil
	}
	nextRoot := state.root
	nextRoot.Generation = generation
	nextSuper := state.super
	nextSuper.Generation = generation
	nextState := &fileStoreState{
		root: nextRoot, super: nextSuper,
		keyRoot: state.keyRoot, chunkRoot: state.chunkRoot,
		indexRoot: state.indexRoot, freeHead: state.freeHead,
	}
	c.bufferedValueBefore = append(c.bufferedValueBefore[:0], old...)
	before := c.bufferedValueBefore

	c.snapshotGate.Lock()
	if c.leases.AnyActive() {
		c.snapshotGate.Unlock()
		c.bufferedInplaceFallbacks.Add(1)
		return false, nil
	}
	_, replaceErr := c.cache.ReplaceLeasedCanonicalDirty(
		leafLease,
		ref, ref,
		valueOffset, before, src, math.MaxUint64,
	)
	if replaceErr != nil {
		c.snapshotGate.Unlock()
		if errors.Is(replaceErr, storeio.ErrCanonicalPageBusy) ||
			errors.Is(replaceErr, storeio.ErrCanonicalPageChanged) {
			c.bufferedInplaceFallbacks.Add(1)
			return false, nil
		}
		return false, replaceErr
	}
	c.primaryRouter.Load().AdvanceGeneration(generation)
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	c.snapshotGate.Unlock()
	c.bufferedInplaceUpdates.Add(1)
	return true, nil
}

func (c *Collection) cowBufferedPrimaryMutation(
	state *fileStoreState,
	key, src []byte,
	deleting, found bool,
	slot uint8,
	resident storeio.ResidentPrimaryRoute,
	leaf *storeio.CommonPrimaryLeafView,
) (
	nextLeaf storeio.PageRef,
	becameEmpty bool,
	filledEmpty bool,
	err error,
) {
	if state == nil || leaf == nil || !c.buffered() {
		return storeio.PageRef{}, false, false,
			storeio.ErrInvalidWrite
	}
	generation := state.root.Generation + 1
	if generation == 0 || generation >= uint64(1)<<48 {
		return storeio.PageRef{}, false, false,
			storeio.ErrGenerationOrder
	}

	pendingIndex := c.primaryPendingParentIndex(resident.Bucket)
	var pending filePrimaryPendingParent
	if pendingIndex < 0 {
		if len(c.primaryPendingParents) ==
			cap(c.primaryPendingParents) {
			return storeio.PageRef{}, false, false,
				storeio.ErrCheckpointRequired
		}
		var path filePrimaryMutationPath
		if err := c.acquirePrimaryMutationPath(
			&path, state, key, resident,
		); err != nil {
			return storeio.PageRef{}, false, false, err
		}
		pending = filePrimaryPendingParentFromPath(
			resident, &path,
		)
		path.Release()
	} else {
		pending = c.primaryPendingParents[pendingIndex]
	}

	preparePath := filePrimaryMutationPath{leaf: *leaf}
	leafImage, leafBytes, prepareErr := c.preparePrimaryLeafMutation(
		&preparePath, generation, key, src,
		deleting, found, slot,
	)
	if prepareErr != nil {
		return storeio.PageRef{}, false, false, prepareErr
	}
	becameEmpty = deleting && leaf.Len() == 1
	filledEmpty = !deleting && !found && leaf.Len() == 0

	offset := state.super.FileEnd
	if len(c.primaryPendingParents) == 0 {
		gap := uint64(c.options.maxTransactionPages) *
			uint64(c.options.MaxPageSize)
		if offset > math.MaxUint64-gap {
			return storeio.PageRef{}, false, false,
				storeio.ErrInvalidWrite
		}
		offset += gap
	}
	if offset > math.MaxUint64-uint64(leafBytes) {
		return storeio.PageRef{}, false, false,
			storeio.ErrInvalidWrite
	}
	nextLeaf = storeio.PageRef{
		Offset: offset, LogicalID: resident.Ref.LogicalID,
		Generation: generation, Length: uint32(leafBytes),
		Kind: storeio.PagePrimaryLeaf,
	}
	if !c.primaryRouter.Load().CanUpdateLeaf(
		resident, nextLeaf, generation,
	) {
		return storeio.PageRef{}, false, false,
			storeio.ErrSegmentedTabletRouterCorrupt
	}
	if err := c.cache.AdmitBufferedDirty(
		nextLeaf, leafImage, math.MaxUint64,
	); err != nil {
		return storeio.PageRef{}, false, false, err
	}

	nextRoot := state.root
	nextRoot.Generation = generation
	if deleting {
		nextRoot.DocumentCount--
	} else if !found {
		nextRoot.DocumentCount++
	}
	nextSuper := state.super
	nextSuper.Generation = generation
	nextSuper.FileEnd = offset + uint64(leafBytes)
	nextState := &fileStoreState{
		root: nextRoot, super: nextSuper,
		keyRoot: state.keyRoot, chunkRoot: state.chunkRoot,
		indexRoot: state.indexRoot, freeHead: state.freeHead,
	}

	previousVolatile := pending.volatileRef
	pending.volatileRef = nextLeaf
	if pendingIndex < 0 {
		c.primaryPendingParents = append(
			c.primaryPendingParents, pending,
		)
	} else {
		c.primaryPendingParents[pendingIndex] = pending
	}

	c.snapshotGate.Lock()
	c.primaryRouter.Load().UpdateLeaf(
		resident, nextLeaf, generation,
	)
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	c.retirePrimaryVolatileRefLocked(previousVolatile)
	c.snapshotGate.Unlock()
	return nextLeaf, becameEmpty, filledEmpty, nil
}

func (c *Collection) cowPrimaryMutation(
	state *fileStoreState,
	key, src []byte,
	deleting, found bool,
	slot uint8,
	resident storeio.ResidentPrimaryRoute,
	path *filePrimaryMutationPath,
) (
	nextLeaf storeio.PageRef,
	becameEmpty bool,
	filledEmpty bool,
	err error,
) {
	generation := state.root.Generation + 1
	if generation == 0 {
		return storeio.PageRef{}, false, false,
			storeio.ErrGenerationOrder
	}
	leafImage, leafBytes, prepareErr := c.preparePrimaryLeafMutation(
		path, generation, key, src, deleting, found, slot,
	)
	if prepareErr != nil {
		return storeio.PageRef{}, false, false, prepareErr
	}
	becameEmpty = deleting && path.leaf.Len() == 1
	filledEmpty = !deleting && !found && path.leaf.Len() == 0
	if err := c.refreshReusableFor(
		state,
		c.options.singleDocumentTransactionPages,
		c.options.singleDocumentFreeFoldLimit,
	); err != nil {
		return storeio.PageRef{}, false, false, err
	}
	tx, err := c.beginWriteTransaction(
		c.options.singleDocumentTransactionPages,
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
		return storeio.PageRef{}, false, false, err
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

	// Draw the rewritten leaf toward its sorted-rank target so churn pulls leaves
	// back into lexical order instead of scattering them to the file tail and
	// destroying scan locality (see primaryLeafPlacementHint). The parent COW
	// pages below stay near their own retired offsets, which is enough for the
	// handful of them a point mutation touches.
	layout, layoutErr := storeio.MutableStoreLayout(uint32(c.options.PageSize))
	if layoutErr != nil {
		return storeio.PageRef{}, false, false, layoutErr
	}
	leafPage, err := tx.AllocateNear(
		storeio.PagePrimaryLeaf, uint32(leafBytes),
		path.leafRoute.Ref.LogicalID,
		primaryLeafPlacementHint(path.leafRoute.Bucket, layout.DataStart),
	)
	if err != nil {
		return storeio.PageRef{}, false, false, err
	}
	copy(leafPage.Bytes(), leafImage)
	if err := leafPage.Stage(); err != nil {
		return storeio.PageRef{}, false, false, err
	}
	nextLeaf = leafPage.Ref()

	anchorPage, err := tx.AllocateNear(
		storeio.PagePrimaryAnchor,
		storeio.SegmentedTabletRouterAnchorPageBytes,
		path.anchorRoute.Ref.LogicalID, path.anchorRoute.Ref.Offset,
	)
	if err != nil {
		return storeio.PageRef{}, false, false, err
	}
	cow, err := path.tablet.RewriteHandle(
		c.primaryRootScratch,
		anchorPage.Bytes(),
		generation, path.leafRoute, nextLeaf,
		path.leafRoute.Zone, anchorPage.Ref(), &path.anchor,
	)
	if err != nil {
		return storeio.PageRef{}, false, false, err
	}
	if err := anchorPage.Stage(); err != nil {
		return storeio.PageRef{}, false, false, err
	}

	tabletPage, err := tx.AllocateNear(
		storeio.PageTabletRoute,
		storeio.GlobalTabletCatalogTabletBytes,
		path.tabletRoute.Ref.LogicalID, path.tabletRoute.Ref.Offset,
	)
	if err != nil {
		return storeio.PageRef{}, false, false, err
	}
	locator, ok := path.tablet.LocatorRef()
	if !ok {
		return storeio.PageRef{}, false, false,
			storeio.ErrGlobalTabletCatalogCorrupt
	}
	bounds := c.primaryMutationBounds(tx)
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
		bounds, locator, cow.Root,
	); err != nil {
		return storeio.PageRef{}, false, false, err
	}
	if err := tabletPage.Stage(); err != nil {
		return storeio.PageRef{}, false, false, err
	}

	catalogPage, err := tx.AllocateNear(
		storeio.PagePrimaryCatalog,
		storeio.GlobalTabletCatalogNodeBytes,
		path.catalogLease.Header().LogicalID, path.catalogRef.Offset,
	)
	if err != nil {
		return storeio.PageRef{}, false, false, err
	}
	bounds = c.primaryMutationBounds(tx)
	if _, err := path.catalog.RewriteHandle(
		catalogPage.Bytes(), generation, bounds,
		path.tabletRoute.ID, tabletPage.Ref(),
	); err != nil {
		return storeio.PageRef{}, false, false, err
	}
	if err := catalogPage.Stage(); err != nil {
		return storeio.PageRef{}, false, false, err
	}

	childID := path.catalogRoute.ID
	childRef := catalogPage.Ref()
	if path.hasBranch {
		branchPage, allocateErr := tx.AllocateNear(
			storeio.PagePrimaryCatalog,
			storeio.GlobalTabletCatalogNodeBytes,
			path.branchLease.Header().LogicalID, path.branchRef.Offset,
		)
		if allocateErr != nil {
			return storeio.PageRef{}, false, false, allocateErr
		}
		bounds = c.primaryMutationBounds(tx)
		if _, rewriteErr := path.branch.RewriteHandle(
			branchPage.Bytes(), generation, bounds,
			path.catalogRoute.ID, catalogPage.Ref(),
		); rewriteErr != nil {
			return storeio.PageRef{}, false, false, rewriteErr
		}
		if stageErr := branchPage.Stage(); stageErr != nil {
			return storeio.PageRef{}, false, false, stageErr
		}
		childID = path.rootRoute.ID
		childRef = branchPage.Ref()
	}

	rootPage, err := tx.AllocateNear(
		storeio.PagePrimaryCatalog,
		storeio.GlobalTabletCatalogRootBytes,
		state.root.PrimaryRoot.LogicalID, state.root.PrimaryRoot.Offset,
	)
	if err != nil {
		return storeio.PageRef{}, false, false, err
	}
	bounds = c.primaryMutationBounds(tx)
	if _, err := path.root.RewriteHandle(
		rootPage.Bytes(), generation, bounds, childID, childRef,
	); err != nil {
		return storeio.PageRef{}, false, false, err
	}
	if err := rootPage.Stage(); err != nil {
		return storeio.PageRef{}, false, false, err
	}
	if !c.primaryRouter.Load().CanUpdateLeaf(
		resident, nextLeaf, generation,
	) {
		return storeio.PageRef{}, false, false,
			storeio.ErrSegmentedTabletRouterCorrupt
	}
	for _, ref := range [...]storeio.PageRef{
		path.leafRoute.Ref,
		path.anchorRoute.Ref,
		path.tabletRoute.Ref,
		path.catalogRef,
	} {
		if err := c.appendPrimaryRetirement(state, ref); err != nil {
			return storeio.PageRef{}, false, false, err
		}
	}
	if path.hasBranch {
		if err := c.appendPrimaryRetirement(
			state, path.branchRef,
		); err != nil {
			return storeio.PageRef{}, false, false, err
		}
	}
	if err := c.appendPrimaryRetirement(
		state, state.root.PrimaryRoot,
	); err != nil {
		return storeio.PageRef{}, false, false, err
	}
	freeLog, err := c.syncFreeLogFor(
		tx, state, c.options.singleDocumentFreeFoldLimit,
	)
	if err != nil {
		return storeio.PageRef{}, false, false,
			fmt.Errorf("vibejson: persist reusable extents: %w", err)
	}
	nextState, nextInline, err := c.stagePrimaryState(
		tx, state, generation, rootPage.Ref(),
		freeLog.head, freeLog.checksum, freeLog.inline,
		func() uint64 {
			if deleting {
				return state.root.DocumentCount - 1
			}
			if !found {
				return state.root.DocumentCount + 1
			}
			return state.root.DocumentCount
		}(),
	)
	if err != nil {
		return storeio.PageRef{}, false, false, err
	}
	if err := c.reserveFileRetirements(); err != nil {
		return storeio.PageRef{}, false, false,
			fmt.Errorf("vibejson: reserve retired extents: %w", err)
	}
	retirementReserved = true
	if err := c.publishStagedPrimaryMutation(
		tx, nextState, nextInline, freeLog,
		resident, nextLeaf,
	); err != nil {
		return storeio.PageRef{}, false, false, err
	}
	abort = false
	return nextLeaf, becameEmpty, filledEmpty, nil
}

func (c *Collection) materializePrimaryParentsLocked() (err error) {
	if len(c.primaryPendingParents) == 0 {
		return nil
	}
	c.clearPrimaryVolatileRetiredLocked()
	if c.leases.AnyActive() &&
		len(c.primaryVolatileRetired)+
			len(c.primaryPendingParents) >
			cap(c.primaryVolatileRetired) {
		return fmt.Errorf(
			"%w: buffered primary volatile-reference capacity %d",
			storeio.ErrRetiredExtentCapacity,
			cap(c.primaryVolatileRetired),
		)
	}
	// The base is the previous primary checkpoint, not necessarily the durable
	// one. A flush-less materialize (Snapshot, snapshot-contended mutation) leaves
	// durableState behind but records its published cut in primaryCheckpointBase;
	// deriving the next checkpoint from that in-memory cut is what keeps repeated
	// buffered materializes between flushes from re-retiring the durable root and
	// rebuilding from a stale graph. Once a flush advances durableState past the
	// recorded cut, the stale pointer is dropped and the durable state resumes as
	// the base. See primaryCheckpointBase.
	base := c.durableState.Load()
	if cp := c.primaryCheckpointBase; cp != nil {
		if base == nil || cp.root.Generation > base.root.Generation {
			base = cp
		} else {
			c.primaryCheckpointBase = nil
		}
	}
	visible := c.state.Load()
	if base == nil || visible == nil ||
		visible.root.Generation <= base.root.Generation ||
		visible.root.Generation >= uint64(1)<<48 {
		return storeio.ErrGenerationOrder
	}
	generation := visible.root.Generation
	for index := range c.primaryPendingParents {
		pending := &c.primaryPendingParents[index]
		pending.checkpointLeaf = storeio.PageRef{}
		pending.checkpointAnchor = storeio.PageRef{}
		pending.checkpointTablet = storeio.PageRef{}
		pending.checkpointCatalog = storeio.PageRef{}
		pending.checkpointBranch = storeio.PageRef{}
	}

	if err := c.refreshReusableFor(
		base,
		c.options.maxTransactionPages,
		c.options.freeFoldLimit,
	); err != nil {
		return err
	}
	tx, err := c.beginWriteTransaction(
		c.options.maxTransactionPages,
		storeio.WriteTransactionOptions{
			StoreID: c.storeID, Generation: generation,
			PageSize:         uint32(c.options.PageSize),
			FileEnd:          base.super.FileEnd,
			NextLogicalID:    base.root.NextLogicalID,
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
					base.root.Generation,
				)
			}
			err = errors.Join(err, tx.Abort())
		}
	}()
	c.retireScratch = c.retireScratch[:0]
	c.retireRefScratch = c.retireRefScratch[:0]

	layout, layoutErr := storeio.MutableStoreLayout(uint32(c.options.PageSize))
	if layoutErr != nil {
		return layoutErr
	}

	// Allocate the checkpointed leaves in ascending lexical rank so consecutive
	// ranks claim consecutive reusable extents. The nearest-fit placement below
	// only preserves order if the requests arrive in order: an out-of-order high
	// rank would otherwise take a low reusable slot a lower rank wanted. The
	// subsequent parent loops group by page reference and are order independent,
	// so this reordering is confined to placement quality.
	slices.SortFunc(c.primaryPendingParents, func(a, b filePrimaryPendingParent) int {
		switch {
		case a.leafRoute.Bucket < b.leafRoute.Bucket:
			return -1
		case a.leafRoute.Bucket > b.leafRoute.Bucket:
			return 1
		default:
			return 0
		}
	})

	for index := range c.primaryPendingParents {
		pending := &c.primaryPendingParents[index]
		lease, acquireErr := c.cache.Acquire(
			pending.volatileRef,
		)
		if acquireErr != nil {
			return acquireErr
		}
		header := lease.Header()
		// Draw the checkpointed leaf toward its sorted-rank target rather than its
		// last physical offset, so churn pulls each leaf back into lexical order
		// instead of scattering it. See primaryLeafPlacementHint.
		page, allocateErr := tx.AllocateNear(
			storeio.PagePrimaryLeaf, header.PageSize,
			pending.volatileRef.LogicalID,
			primaryLeafPlacementHint(pending.leafRoute.Bucket, layout.DataStart),
		)
		if allocateErr != nil {
			lease.Release()
			return allocateErr
		}
		header.Generation = generation
		payload, initErr := storeio.InitPage(
			page.Bytes(), header,
		)
		if initErr == nil {
			copy(payload, lease.Payload())
			_, initErr = storeio.SealPage(page.Bytes())
		}
		lease.Release()
		if initErr != nil {
			return initErr
		}
		if stageErr := page.Stage(); stageErr != nil {
			return stageErr
		}
		pending.checkpointLeaf = page.Ref()
		if appendErr := c.appendPrimaryRetirement(
			base, pending.leafRoute.Ref,
		); appendErr != nil {
			return appendErr
		}
	}

	bounds := func() storeio.GlobalTabletCatalogBounds {
		return storeio.GlobalTabletCatalogBounds{
			StoreID:                c.storeID,
			SelectedRootGeneration: generation,
			FileEnd:                tx.FileEnd(),
			NextLogicalID:          tx.NextLogicalID(),
		}
	}
	var anchorRewrites [filePrimaryPendingParentLimit]storeio.GlobalTabletCatalogAnchorHandleRewrite
	for index := range c.primaryPendingParents {
		pending := &c.primaryPendingParents[index]
		if pending.checkpointAnchor != (storeio.PageRef{}) {
			continue
		}
		tabletLease, acquireErr := c.cache.Acquire(
			pending.tabletRoute.Ref,
		)
		if acquireErr != nil {
			return acquireErr
		}
		tablet := storeio.AdmittedGlobalTabletCatalogTabletRoot(
			tabletLease.Page(),
			storeio.GlobalTabletCatalogBounds{
				StoreID:                c.storeID,
				SelectedRootGeneration: base.root.Generation,
				FileEnd:                base.super.FileEnd,
				NextLogicalID:          base.root.NextLogicalID,
			},
		)
		anchorLease, anchorErr := c.cache.Acquire(
			pending.anchorRoute.Ref,
		)
		if anchorErr != nil {
			tabletLease.Release()
			return anchorErr
		}
		anchor := storeio.AdmittedGlobalTabletCatalogAnchor(
			anchorLease.Page(), &tablet,
			pending.anchorRoute.PageID,
		)
		rewriteCount := 0
		for candidate := range c.primaryPendingParents {
			other := &c.primaryPendingParents[candidate]
			if other.anchorRoute.Ref !=
				pending.anchorRoute.Ref {
				continue
			}
			anchorRewrites[rewriteCount] =
				storeio.GlobalTabletCatalogAnchorHandleRewrite{
					Route: other.leafRoute,
					Ref:   other.checkpointLeaf,
				}
			rewriteCount++
		}
		anchorPage, allocateErr := tx.AllocateNear(
			storeio.PagePrimaryAnchor,
			storeio.SegmentedTabletRouterAnchorPageBytes,
			pending.anchorRoute.Ref.LogicalID, pending.anchorRoute.Ref.Offset,
		)
		if allocateErr == nil {
			_, allocateErr = tablet.RewriteAnchorHandles(
				anchorPage.Bytes(), generation,
				anchorRewrites[:rewriteCount],
				anchorPage.Ref(), &anchor,
			)
		}
		anchorLease.Release()
		tabletLease.Release()
		if allocateErr != nil {
			return allocateErr
		}
		if stageErr := anchorPage.Stage(); stageErr != nil {
			return stageErr
		}
		for candidate := range c.primaryPendingParents {
			other := &c.primaryPendingParents[candidate]
			if other.anchorRoute.Ref ==
				pending.anchorRoute.Ref {
				other.checkpointAnchor = anchorPage.Ref()
			}
		}
		if appendErr := c.appendPrimaryRetirement(
			base, pending.anchorRoute.Ref,
		); appendErr != nil {
			return appendErr
		}
	}

	var rootRewrites [storeio.SegmentedTabletRouterMaxPages]storeio.GlobalTabletCatalogAnchorRefRewrite
	for index := range c.primaryPendingParents {
		pending := &c.primaryPendingParents[index]
		if pending.checkpointTablet != (storeio.PageRef{}) {
			continue
		}
		tabletLease, acquireErr := c.cache.Acquire(
			pending.tabletRoute.Ref,
		)
		if acquireErr != nil {
			return acquireErr
		}
		tablet := storeio.AdmittedGlobalTabletCatalogTabletRoot(
			tabletLease.Page(),
			storeio.GlobalTabletCatalogBounds{
				StoreID:                c.storeID,
				SelectedRootGeneration: base.root.Generation,
				FileEnd:                base.super.FileEnd,
				NextLogicalID:          base.root.NextLogicalID,
			},
		)
		rewriteCount := 0
		for candidate := range c.primaryPendingParents {
			other := &c.primaryPendingParents[candidate]
			if other.tabletRoute.Ref !=
				pending.tabletRoute.Ref {
				continue
			}
			duplicate := false
			for prior := 0; prior < rewriteCount; prior++ {
				if rootRewrites[prior].PageID ==
					other.anchorRoute.PageID {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			rootRewrites[rewriteCount] =
				storeio.GlobalTabletCatalogAnchorRefRewrite{
					PageID: other.anchorRoute.PageID,
					Ref:    other.checkpointAnchor,
				}
			rewriteCount++
		}
		tabletPage, allocateErr := tx.AllocateNear(
			storeio.PageTabletRoute,
			storeio.GlobalTabletCatalogTabletBytes,
			pending.tabletRoute.Ref.LogicalID, pending.tabletRoute.Ref.Offset,
		)
		var rawRoot []byte
		if allocateErr == nil {
			rawRoot, allocateErr = tablet.RewriteAnchorRefs(
				c.primaryRootScratch, generation,
				rootRewrites[:rewriteCount],
			)
		}
		locator, locatorOK := tablet.LocatorRef()
		if allocateErr == nil && !locatorOK {
			allocateErr = storeio.ErrGlobalTabletCatalogCorrupt
		}
		if allocateErr == nil {
			_, allocateErr =
				storeio.EncodeGlobalTabletCatalogTabletRoot(
					tabletPage.Bytes(),
					storeio.PageHeader{
						StoreID:    c.storeID,
						Generation: generation,
						LogicalID:  tabletPage.Ref().LogicalID,
						PageSize: storeio.
							GlobalTabletCatalogTabletBytes,
						PayloadLength: storeio.
							GlobalTabletCatalogRootHeader +
							storeio.
								SegmentedTabletRouterRootBytes,
						Kind: storeio.PageTabletRoute,
					},
					bounds(), locator, rawRoot,
				)
		}
		tabletLease.Release()
		if allocateErr != nil {
			return allocateErr
		}
		if stageErr := tabletPage.Stage(); stageErr != nil {
			return stageErr
		}
		for candidate := range c.primaryPendingParents {
			other := &c.primaryPendingParents[candidate]
			if other.tabletRoute.Ref ==
				pending.tabletRoute.Ref {
				other.checkpointTablet = tabletPage.Ref()
			}
		}
		if appendErr := c.appendPrimaryRetirement(
			base, pending.tabletRoute.Ref,
		); appendErr != nil {
			return appendErr
		}
	}

	var nodeRewrites [filePrimaryPendingParentLimit]storeio.GlobalTabletCatalogNodeHandleRewrite
	for index := range c.primaryPendingParents {
		pending := &c.primaryPendingParents[index]
		if pending.checkpointCatalog != (storeio.PageRef{}) {
			continue
		}
		catalogLease, acquireErr := c.cache.Acquire(
			pending.catalogRef,
		)
		if acquireErr != nil {
			return acquireErr
		}
		catalog := storeio.AdmittedGlobalTabletCatalogNode(
			catalogLease.Page(),
			storeio.GlobalTabletCatalogBounds{
				StoreID:                c.storeID,
				SelectedRootGeneration: base.root.Generation,
				FileEnd:                base.super.FileEnd,
				NextLogicalID:          base.root.NextLogicalID,
			},
		)
		rewriteCount := 0
		for candidate := range c.primaryPendingParents {
			other := &c.primaryPendingParents[candidate]
			if other.catalogRef != pending.catalogRef {
				continue
			}
			duplicate := false
			for prior := 0; prior < rewriteCount; prior++ {
				if nodeRewrites[prior].ID ==
					other.tabletRoute.ID {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			nodeRewrites[rewriteCount] =
				storeio.GlobalTabletCatalogNodeHandleRewrite{
					ID:  other.tabletRoute.ID,
					Ref: other.checkpointTablet,
				}
			rewriteCount++
		}
		catalogPage, allocateErr := tx.AllocateNear(
			storeio.PagePrimaryCatalog,
			storeio.GlobalTabletCatalogNodeBytes,
			pending.catalogRef.LogicalID, pending.catalogRef.Offset,
		)
		if allocateErr == nil {
			_, allocateErr = catalog.RewriteHandles(
				catalogPage.Bytes(), generation, bounds(),
				nodeRewrites[:rewriteCount],
			)
		}
		catalogLease.Release()
		if allocateErr != nil {
			return allocateErr
		}
		if stageErr := catalogPage.Stage(); stageErr != nil {
			return stageErr
		}
		for candidate := range c.primaryPendingParents {
			other := &c.primaryPendingParents[candidate]
			if other.catalogRef == pending.catalogRef {
				other.checkpointCatalog = catalogPage.Ref()
			}
		}
		if appendErr := c.appendPrimaryRetirement(
			base, pending.catalogRef,
		); appendErr != nil {
			return appendErr
		}
	}

	for index := range c.primaryPendingParents {
		pending := &c.primaryPendingParents[index]
		if !pending.hasBranch ||
			pending.checkpointBranch != (storeio.PageRef{}) {
			continue
		}
		branchLease, acquireErr := c.cache.Acquire(
			pending.branchRef,
		)
		if acquireErr != nil {
			return acquireErr
		}
		branch := storeio.AdmittedGlobalTabletCatalogNode(
			branchLease.Page(),
			storeio.GlobalTabletCatalogBounds{
				StoreID:                c.storeID,
				SelectedRootGeneration: base.root.Generation,
				FileEnd:                base.super.FileEnd,
				NextLogicalID:          base.root.NextLogicalID,
			},
		)
		rewriteCount := 0
		for candidate := range c.primaryPendingParents {
			other := &c.primaryPendingParents[candidate]
			if other.branchRef != pending.branchRef {
				continue
			}
			duplicate := false
			for prior := 0; prior < rewriteCount; prior++ {
				if nodeRewrites[prior].ID ==
					other.catalogRoute.ID {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			nodeRewrites[rewriteCount] =
				storeio.GlobalTabletCatalogNodeHandleRewrite{
					ID:  other.catalogRoute.ID,
					Ref: other.checkpointCatalog,
				}
			rewriteCount++
		}
		branchPage, allocateErr := tx.AllocateNear(
			storeio.PagePrimaryCatalog,
			storeio.GlobalTabletCatalogNodeBytes,
			pending.branchRef.LogicalID, pending.branchRef.Offset,
		)
		if allocateErr == nil {
			_, allocateErr = branch.RewriteHandles(
				branchPage.Bytes(), generation, bounds(),
				nodeRewrites[:rewriteCount],
			)
		}
		branchLease.Release()
		if allocateErr != nil {
			return allocateErr
		}
		if stageErr := branchPage.Stage(); stageErr != nil {
			return stageErr
		}
		for candidate := range c.primaryPendingParents {
			other := &c.primaryPendingParents[candidate]
			if other.branchRef == pending.branchRef {
				other.checkpointBranch = branchPage.Ref()
			}
		}
		if appendErr := c.appendPrimaryRetirement(
			base, pending.branchRef,
		); appendErr != nil {
			return appendErr
		}
	}

	rootLease, err := c.cache.Acquire(base.root.PrimaryRoot)
	if err != nil {
		return err
	}
	root := storeio.AdmittedGlobalTabletCatalogNode(
		rootLease.Page(),
		storeio.GlobalTabletCatalogBounds{
			StoreID:                c.storeID,
			SelectedRootGeneration: base.root.Generation,
			FileEnd:                base.super.FileEnd,
			NextLogicalID:          base.root.NextLogicalID,
		},
	)
	rewriteCount := 0
	for index := range c.primaryPendingParents {
		pending := &c.primaryPendingParents[index]
		id := pending.catalogRoute.ID
		ref := pending.checkpointCatalog
		if pending.hasBranch {
			id = pending.rootRoute.ID
			ref = pending.checkpointBranch
		}
		duplicate := false
		for prior := 0; prior < rewriteCount; prior++ {
			if nodeRewrites[prior].ID == id {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		nodeRewrites[rewriteCount] =
			storeio.GlobalTabletCatalogNodeHandleRewrite{
				ID: id, Ref: ref,
			}
		rewriteCount++
	}
	rootPage, err := tx.AllocateNear(
		storeio.PagePrimaryCatalog,
		storeio.GlobalTabletCatalogRootBytes,
		base.root.PrimaryRoot.LogicalID, base.root.PrimaryRoot.Offset,
	)
	if err == nil {
		_, err = root.RewriteHandles(
			rootPage.Bytes(), generation, bounds(),
			nodeRewrites[:rewriteCount],
		)
	}
	rootLease.Release()
	if err != nil {
		return err
	}
	if err := rootPage.Stage(); err != nil {
		return err
	}
	if err := c.appendPrimaryRetirement(
		base, base.root.PrimaryRoot,
	); err != nil {
		return err
	}

	freeLog, err := c.syncFreeLogFor(
		tx, base, c.options.freeFoldLimit,
	)
	if err != nil {
		return fmt.Errorf(
			"vibejson: persist primary checkpoint reusable extents: %w",
			err,
		)
	}
	nextState, nextInline, err := c.stagePrimaryState(
		tx, base, generation, rootPage.Ref(),
		freeLog.head, freeLog.checksum, freeLog.inline,
		visible.root.DocumentCount,
	)
	if err != nil {
		return err
	}
	if err := c.reserveFileRetirements(); err != nil {
		return fmt.Errorf(
			"vibejson: reserve primary checkpoint retirements: %w",
			err,
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
	for index := range c.primaryPendingParents {
		pending := &c.primaryPendingParents[index]
		c.primaryRouter.Load().UpdateLeaf(
			pending.resident, pending.checkpointLeaf,
			generation,
		)
	}
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	if retiring {
		c.cache.MarkUnreachable(c.retireRefScratch)
		c.cache.MarkUnreachable(c.primaryVolatileRetired)
		clear(c.primaryVolatileRetired)
		c.primaryVolatileRetired =
			c.primaryVolatileRetired[:0]
	}
	for index := range c.primaryPendingParents {
		c.retirePrimaryVolatileRefLocked(
			c.primaryPendingParents[index].volatileRef,
		)
	}
	c.snapshotGate.Unlock()
	c.finalizeReusable()
	c.commitFreeLog(freeLog)
	c.inlineFree = nextInline
	clear(c.primaryPendingParents)
	c.primaryPendingParents = c.primaryPendingParents[:0]
	// Record the published cut so the next materialize derives its base from it
	// rather than a durableState a flush has not yet advanced. A following
	// checkpointBufferedLocked flush advances durableState past this generation,
	// and the base selection above then drops the pointer.
	c.primaryCheckpointBase = nextState
	return nil
}

func (c *Collection) preparePrimaryLeafMutation(
	path *filePrimaryMutationPath,
	generation uint64,
	key, src []byte,
	deleting, found bool,
	slot uint8,
) ([]byte, int, error) {
	size := len(path.leaf.PersistentBytes())
	dst := c.primaryLeafScratch[:size]
	var (
		page []byte
		err  error
	)
	switch {
	case deleting:
		page, err = path.leaf.DeleteTo(dst, generation, slot, key)
	case found:
		page, err = path.leaf.UpdateTo(
			dst, generation, slot, key,
			storeio.CommonPrimaryLeafValue{Inline: src},
		)
		if errors.Is(err, storeio.ErrCommonPrimaryLeafNeedsWide) {
			page, err = storeio.PromoteCommonPrimaryLeafUpdateTo(
				c.primaryLeafScratch, generation, &path.leaf,
				slot, key,
				storeio.CommonPrimaryLeafValue{Inline: src},
			)
		}
	default:
		page, _, err = path.leaf.InsertTo(
			dst, generation, key,
			storeio.CommonPrimaryLeafValue{Inline: src},
		)
		if errors.Is(err, storeio.ErrCommonPrimaryLeafNeedsWide) {
			page, _, err = storeio.PromoteCommonPrimaryLeafInsertTo(
				c.primaryLeafScratch, generation, &path.leaf,
				key, storeio.CommonPrimaryLeafValue{Inline: src},
			)
		}
	}
	if errors.Is(err, storeio.ErrCommonPrimaryLeafFull) {
		c.primaryLeafSplitRequired.Add(1)
		return nil, 0, errors.Join(
			ErrPrimaryLeafSplitRequired,
			storeio.ErrCommonPrimaryLeafFull,
		)
	}
	if err != nil {
		return nil, 0, err
	}
	return page, len(page), nil
}

func (c *Collection) primaryMutationBounds(
	tx *storeio.WriteTransaction,
) storeio.GlobalTabletCatalogBounds {
	return storeio.GlobalTabletCatalogBounds{
		StoreID: c.storeID, SelectedRootGeneration: tx.Generation(),
		FileEnd: tx.FileEnd(), NextLogicalID: tx.NextLogicalID(),
	}
}

// primaryLeafBounds are the leaf-admission bounds for the published state, used
// by read-only leaf peeks (merge/reclass evaluation) that admit a leaf image
// without a write transaction.
func (c *Collection) primaryLeafBounds(
	state *fileStoreState,
) storeio.CommonPrimaryLeafBounds {
	return storeio.CommonPrimaryLeafBounds{
		FileEnd:           state.super.FileEnd,
		NextLogicalID:     state.root.NextLogicalID,
		AllocationQuantum: state.root.PageSize,
	}
}

func (c *Collection) appendPrimaryRetirement(
	state *fileStoreState,
	ref storeio.PageRef,
) error {
	if ref == (storeio.PageRef{}) {
		return nil
	}
	if len(c.retireScratch) == cap(c.retireScratch) {
		return storeio.ErrRetiredExtentCapacity
	}
	c.retireScratch = append(c.retireScratch, storeio.FreeExtent{
		Offset: ref.Offset, Length: uint64(ref.Length),
		RetiredGeneration: state.root.Generation,
	})
	c.rememberRetiredRef(ref)
	return nil
}

func (c *Collection) removePrimaryEmptyLeaf() {
	for {
		current := c.primaryEmptyLeaves.Load()
		if current == 0 ||
			c.primaryEmptyLeaves.CompareAndSwap(current, current-1) {
			return
		}
	}
}

func (c *Collection) stagePrimaryState(
	tx *storeio.WriteTransaction,
	old *fileStoreState,
	generation uint64,
	primaryRoot, freeHead storeio.PageRef,
	freeChecksum uint32,
	inlineFree *storeio.InlineFreeDelta,
	documentCount uint64,
) (*fileStoreState, storeio.InlineFreeDelta, error) {
	if inlineFree == nil || inlineFree.ExternalPrev() != freeHead {
		return nil, storeio.InlineFreeDelta{},
			storeio.ErrFreeLogCorrupt
	}
	root := old.root
	root.Generation = generation
	root.DocumentCount = documentCount
	root.NextLogicalID = tx.NextLogicalID()
	root.PrimaryRoot = primaryRoot
	super := old.super
	super.Generation = generation
	super.FileEnd = tx.FileEnd()
	super.FreeOffset = 0
	super.FreeLength = 0
	super.FreeChecksum = 0
	if freeHead != (storeio.PageRef{}) {
		super.FreeOffset = freeHead.Offset
		super.FreeLength = freeHead.Length
		super.FreeChecksum = freeChecksum
	}
	return &fileStoreState{
		root: root, super: super,
		keyRoot: old.keyRoot, chunkRoot: old.chunkRoot,
		indexRoot: old.indexRoot, freeHead: freeHead,
	}, *inlineFree, nil
}

func (c *Collection) publishStagedPrimaryMutation(
	tx *storeio.WriteTransaction,
	nextState *fileStoreState,
	nextInline storeio.InlineFreeDelta,
	freeLog freeLogCommit,
	route storeio.ResidentPrimaryRoute,
	nextLeaf storeio.PageRef,
) error {
	publish := func(retiring bool) error {
		if retiring {
			return tx.PublishInlineRetiring(
				nextState.root, nextInline, c.retireRefScratch,
			)
		}
		return tx.PublishInline(nextState.root, nextInline)
	}
	if !c.buffered() {
		if err := publish(false); err != nil {
			return err
		}
		c.finalizeReusable()
		c.commitFreeLog(freeLog)
		c.inlineFree = nextInline
		c.snapshotGate.Lock()
		c.primaryRouter.Load().UpdateLeaf(
			route, nextLeaf, nextState.root.Generation,
		)
		c.pageValidator.update(nextState)
		c.publishFileState(nextState)
		c.snapshotGate.Unlock()
		return nil
	}

	c.snapshotGate.Lock()
	retiring := !c.leases.AnyActive()
	if err := publish(retiring); err != nil {
		c.snapshotGate.Unlock()
		return err
	}
	c.primaryRouter.Load().UpdateLeaf(
		route, nextLeaf, nextState.root.Generation,
	)
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	if retiring {
		c.cache.MarkUnreachable(c.retireRefScratch)
	}
	c.snapshotGate.Unlock()
	c.finalizeReusable()
	c.commitFreeLog(freeLog)
	c.inlineFree = nextInline
	// A snapshot-contended buffered mutation publishes a fresh primary root with no
	// flush, exactly like a materialize. Record it as the checkpoint base so the
	// next materialize allocates past this cut's FileEnd and retires its root, not
	// a stale durableState's. See primaryCheckpointBase.
	c.primaryCheckpointBase = nextState
	return nil
}
