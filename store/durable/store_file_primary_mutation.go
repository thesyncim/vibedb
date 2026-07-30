package durable

import (
	"bytes"
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
	return c.acquirePrimaryPath(path, state, key, resident, true)
}

// acquirePrimaryRoutingPath captures the same rooted parent routes and leaf
// lease as acquirePrimaryMutationPath without expanding a compressed leaf.
// The unified row overlay needs these routes only at checkpoint fold time.
func (c *Collection) acquirePrimaryRoutingPath(
	path *filePrimaryMutationPath,
	state *fileStoreState,
	key []byte,
	resident storeio.ResidentPrimaryRoute,
) (err error) {
	return c.acquirePrimaryPath(path, state, key, resident, false)
}

func (c *Collection) acquirePrimaryPath(
	path *filePrimaryMutationPath,
	state *fileStoreState,
	key []byte,
	resident storeio.ResidentPrimaryRoute,
	admitMutation bool,
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
	if !admitMutation {
		return nil
	}
	path.leaf, err = storeio.AdmittedPrimaryLeafForMutation(
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
	c.beginReaderFence()
	if !c.anyActiveReaders() {
		c.cache.MarkUnreachable(c.primaryVolatileRetired)
		clear(c.primaryVolatileRetired)
		c.primaryVolatileRetired =
			c.primaryVolatileRetired[:0]
	}
	c.endReaderFence()
	c.snapshotGate.Unlock()
}

// retirePrimaryVolatileRefLocked runs while snapshotGate is held and a reader
// fence is raised. A selected route can race from the router to PageCache
// without holding that gate, so an active generation lease or epoch reader
// defers removal instead of turning a cache miss into an impossible read from
// the memory-only virtual extent.
func (c *Collection) retirePrimaryVolatileRefLocked(
	ref storeio.PageRef,
) {
	if ref == (storeio.PageRef{}) {
		return
	}
	if !c.anyActiveReaders() {
		c.cache.MarkUnreachable([]storeio.PageRef{ref})
		return
	}
	c.primaryVolatileRetired = append(
		c.primaryVolatileRetired, ref,
	)
}

// unadmitPrimaryMutationFrames discards every buffered-dirty frame the current
// single-document mutation admitted before its point of no return. Nothing has
// published a reference to them yet — the router still routes the old leaf and
// the pending-parent entry was not installed — so no current or future snapshot
// can acquire the refs and the fence-free MarkUnreachable contract holds,
// exactly as it does for unadmitPrimaryBatchLeaves. Without this, an error
// between the first admission and the publish (a retryable
// ErrRetiredExtentCapacity under snapshot pressure, a journal device failure)
// would strand up to a whole overflow chain of frames in the cache's dirty
// accounting with no owner, and each retry would strand another.
func (c *Collection) unadmitPrimaryMutationFrames() {
	if len(c.primaryMutationAdmitted) == 0 {
		return
	}
	c.cache.MarkUnreachable(c.primaryMutationAdmitted)
	c.primaryMutationAdmitted = c.primaryMutationAdmitted[:0]
}

func (c *Collection) ensureBufferedPrimaryMutationCapacity(
	resident storeio.ResidentPrimaryRoute,
	valueLen int,
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
		// A full volatile-reference table is the retirement-pressure situation the
		// retirement table has: a held snapshot pins the frames a checkpoint would
		// otherwise drop. Force one checkpoint and count it as a retirement-pressure
		// checkpoint — the same response reserveFileRetirements' retry makes — then
		// re-clear. A checkpoint that runs while no reader has arrived drains the
		// table and lets the mutation proceed; a genuinely reader-pinned table stays
		// full, and the error is routed through the shared diagnostic so it names the
		// pinning snapshot and generation rather than surfacing a bare capacity
		// number an operator cannot act on. materializePrimaryParentsLocked fails
		// closed if it cannot fit the fold, so a failed checkpoint publishes nothing.
		c.retirementPressureCheckpoints.Add(1)
		if err := c.checkpointBufferedLocked(); err != nil &&
			!errors.Is(err, storeio.ErrRetiredExtentCapacity) {
			return err
		}
		c.clearPrimaryVolatileRetiredLocked()
		if len(c.primaryVolatileRetired) == cap(c.primaryVolatileRetired) {
			return c.absorbRetirementPressure(fmt.Errorf(
				"%w: buffered primary volatile-reference capacity %d",
				storeio.ErrRetiredExtentCapacity,
				cap(c.primaryVolatileRetired),
			))
		}
	}
	if c.primaryPendingParentIndex(resident.Bucket) < 0 &&
		len(c.primaryPendingParents) == cap(c.primaryPendingParents) {
		if err := c.checkpointBufferedLocked(); err != nil {
			return err
		}
		c.automaticCheckpoints.Add(1)
	}
	// Exact-index overlay pressure, same discipline as pending-parent
	// pressure above: when the window's record/entry arenas cannot absorb one
	// more ordinary mutation (≤ 2 term records per index plus one tile
	// record), fold now — the checkpoint empties the overlay — rather than
	// resizing structures concurrent probes are reading. These dimensions
	// are document-independent, so the check is exact; the document-dependent
	// ones (interned term bytes, a rebase group's fan-out) are handled by the
	// prepare-time fold escalation instead.
	if epoch := c.primaryEpoch; epoch != nil {
		need := 2*len(c.options.indexes) + 2
		if len(epoch.termRecords)-epoch.termRecordN < need ||
			len(epoch.tileRecords)-epoch.tileRecordN < 8 ||
			min(len(epoch.termEntries), len(epoch.termTable)/4)-
				epoch.termEntryN < need ||
			min(len(epoch.tileEntries), len(epoch.tileTable)/4)-
				epoch.tileEntryN < 8 {
			if err := c.checkpointBufferedLocked(); err != nil {
				return err
			}
			c.automaticCheckpoints.Add(1)
		}
	}
	// The journal-backed sync lane appends its record at the point of no return,
	// after the leaf frame is admitted dirty, so it cannot force a checkpoint
	// there. Fold and recycle a full journal now — before the frame is prepared —
	// sized for the larger of this value and the worst-case inline record, so both
	// an out-of-line value's full-length record and any later inline mutation are
	// guaranteed to fit. This is the sync lane's checkpoint cadence, the journal
	// analogue of pending-parent pressure above.
	if c.syncJournalLane() &&
		!c.journal.Fits(
			c.options.MaxKeyBytes, max(c.options.InlineValueBytes, valueLen),
		) {
		if err := c.checkpointBufferedLocked(); err != nil {
			return err
		}
		c.automaticCheckpoints.Add(1)
	}
	// A buffered overflow value holds its whole out-of-line chain as volatile dirty
	// frames until checkpoint, so a run of large values would fill the cache with
	// unflushable frames. Keep at least half the cache free for one of them: the
	// checkpoint that folds the pending volatile chains into durable pages must
	// stage the re-materialization, and that staging needs as much room as the
	// resident volatile frames it supersedes. Both deferred-canonical lanes fold
	// through checkpointBufferedLocked, so this covers buffered and sync-journal.
	if valueLen > c.options.InlineValueBytes && len(c.primaryPendingParents) != 0 {
		cacheCapacity := uint64(c.options.BufferCount) *
			uint64(c.options.PageSize)
		if c.cache.DirtyCapacityAvailable() < cacheCapacity*3/4 {
			if err := c.checkpointBufferedLocked(); err != nil {
				return err
			}
			c.automaticCheckpoints.Add(1)
		}
	}
	return c.ensureDirtyCapacityFor(
		0,
		c.cache.ReservationBytes(
			storeio.CommonPrimaryLeafWideBytes,
		),
	)
}

// validatePrimarySchema enforces the collection's declared schema before a Put
// is admitted, so a schema-violating document never changes a generation or
// grows the file. It reuses the exact primitive the heap builder validates with
// — Schema.ValidateIndex over a per-document vibejson.Index built with the
// collection's index options — so a document admitted here is one a rebuild
// would also accept. A schemaless collection returns immediately; the caller has
// already proven src is well-formed JSON with vibejson.Validate, so the index
// build cannot fail on structural grounds. It runs under the writer lock, so the
// reused IndexEntry arena is writer-private.
func (c *Collection) validatePrimarySchema(src []byte) error {
	schema := c.options.Collection.Schema
	if schema == nil {
		return nil
	}
	// The index needs at most one entry per source byte plus the two envelope
	// entries; sizing the arena up front keeps a steady-state schema Put from
	// growing it (mirrors the heap builder's segment scratch).
	if cap(c.schemaIndexScratch) < len(src)+2 {
		c.schemaIndexScratch = make([]vibejson.IndexEntry, 0, len(src)+2)
	}
	index, err := vibejson.BuildIndexOptions(
		src, c.schemaIndexScratch[:0], c.options.Collection.IndexOptions,
	)
	if err != nil {
		return err
	}
	c.schemaIndexScratch = index.Entries
	return schema.ValidateIndex(index)
}

func (c *Collection) putPrimary(
	key []byte,
	src []byte,
) (created bool, err error) {
	c.writer.Lock()
	var generation uint64
	var journalTarget uint64
	var canonicalCapacityEnsured bool
	defer func() {
		syncWait := generation != 0 && c.synchronous()
		// The buffered-journal lane deposited its redo record under the writer and
		// now shares one journal sync across concurrent callers: release the writer
		// first, then block on the group fence (phase 1 group commit).
		groupWait := journalTarget != 0
		if syncWait || groupWait {
			c.durabilityWait.Add(1)
		}
		c.writer.Unlock()
		if syncWait {
			err = errors.Join(err, c.waitPublished(generation))
		} else if groupWait {
			err = errors.Join(err, c.journalGroupAwait(journalTarget))
		}
		if syncWait || groupWait {
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
		len(src) > c.options.MaxDocumentBytes {
		return false, ErrDocumentTooLarge
	}
	if err := vibejson.Validate(src); err != nil {
		return false, err
	}
	if err := c.validatePrimarySchema(src); err != nil {
		return false, err
	}
retryAfterUnifiedFold:
	state := c.state.Load()
	if state == nil || state.root.PrimaryRoot == (storeio.PageRef{}) {
		return false, ErrClosed
	}
	// key is borrowed for this call; everything that persists it (leaf frame,
	// recovery-journal record) copies it, so route/stage directly with no scratch.
	keyBytes := key
	resident, err := c.currentPrimaryResidentRoute(state, keyBytes)
	if err != nil {
		return false, err
	}
	// One sample steers both the capacity ensure and the lane selection below.
	// Sampling reader presence twice let a reader arrive or leave in between,
	// choosing the canonical lane without its capacity ensure and surfacing
	// ErrCheckpointRequired to the caller. canonicalFramePathEligible folds the
	// epoch table into its veto (anyActiveReaders), so an active point read
	// defers the canonical lane exactly as an active lease does; the sync
	// journal lane stays eligible regardless of readers.
	// A value past the inline budget stays on the canonical frame lane: it mints
	// its out-of-line overflow chain as volatile buffered-dirty frames, so unlike
	// the transactional path it writes no device bytes at Put and the checkpoint
	// folds the chain into the durable graph.
	canonicalPath := c.canonicalFramePathEligible()
	if canonicalPath && !c.primaryUnifiedSeen &&
		!canonicalCapacityEnsured {
		if err := c.ensureBufferedPrimaryMutationCapacity(
			resident, len(src),
		); err != nil {
			return false, err
		}
		canonicalCapacityEnsured = true
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
	if canonicalPath {
		handled, overlayCreated, pressure, overlayErr :=
			c.tryPrimaryUnifiedOverlayPut(
				state, resident, leafLease.Page(), keyBytes, src,
			)
		if overlayErr != nil {
			leafLease.Release()
			return false, overlayErr
		}
		if handled {
			leafLease.Release()
			created = overlayCreated
			ackGeneration := state.root.Generation + 1
			if c.buffered() {
				target, ackErr := c.journalDepositLocked(
					storeio.RecoveryRecordKindPut,
					ackGeneration, keyBytes, src,
				)
				if ackErr != nil {
					return false, ackErr
				}
				journalTarget = target
			}
			return created, nil
		}
		if pressure || c.primaryUnifiedOverlay.hasPending() {
			leafLease.Release()
			if err := c.materializePrimaryOverlayPressureLocked(); err != nil {
				return false, err
			}
			goto retryAfterUnifiedFold
		}
	} else if c.primaryUnifiedOverlay.hasPending() {
		leafLease.Release()
		if err := c.materializePrimaryOverlayPressureLocked(); err != nil {
			return false, err
		}
		goto retryAfterUnifiedFold
	}
	// Overlay publication owns no page-cache dirty frame, pending-parent slot,
	// or volatile retirement, so it does not need the structural lane's
	// capacity reservation. Pay that reservation only after the class-5 fast
	// path declines; the retry rebinds the route if the ensure checkpointed.
	if canonicalPath && !canonicalCapacityEnsured {
		leafLease.Release()
		if err := c.ensureBufferedPrimaryMutationCapacity(
			resident, len(src),
		); err != nil {
			return false, err
		}
		canonicalCapacityEnsured = true
		goto retryAfterUnifiedFold
	}
	// Every class-5 mutation has one byte contract. The inline overlay already
	// canonicalized its accepted value; values that reach this exceptional COW
	// path (overflow, structural pressure, async mode, or a reader-forced
	// fallback) must do the same before exact-index deltas, journal records, and
	// leaf/overflow images observe them.
	src, err = c.canonicalPrimaryMutationValue(src)
	if err != nil {
		leafLease.Release()
		return false, err
	}
	leaf, err := storeio.AdmittedPrimaryLeafForMutationWithScratch(
		leafLease.Page(), c.storeID, resident.Bucket,
		storeio.CommonPrimaryLeafBounds{
			FileEnd:           state.super.FileEnd,
			NextLogicalID:     state.root.NextLogicalID,
			AllocationQuantum: state.root.PageSize,
		},
		c.primaryLeafMutationScratch,
	)
	if err != nil {
		leafLease.Release()
		return false, err
	}
	slot, _, _, found := leaf.LookupRawHashed(
		resident.Hash, keyBytes,
	)
	created = !found
	if canonicalPath {
		_, becameEmpty, filledEmpty, mutationErr :=
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
		if becameEmpty && c.primaryRouter.Load().MarkEmpty(resident) {
			c.primaryEmptyLeaves.Add(1)
		}
		if filledEmpty && c.primaryRouter.Load().ClearEmpty(resident) {
			c.removePrimaryEmptyLeaf()
		}
		ackGeneration := state.root.Generation + 1
		// The journal-backed sync lane already appended and synced this record
		// inside cowBufferedPrimaryMutation, before the frame was published, so
		// the outer generation stays zero and this ack never takes the retired
		// committer chain fence. Buffered-visible deposits its redo record here for
		// the shared group sync after the writer is released.
		if c.buffered() {
			target, ackErr := c.journalDepositLocked(
				storeio.RecoveryRecordKindPut, ackGeneration, keyBytes, src,
			)
			if ackErr != nil {
				return created, ackErr
			}
			journalTarget = target
		}
		return created, nil
	}
	leafLease.Release()
	// The journal-backed sync lane appends this Put's full-value redo record inside
	// cowPrimaryMutation, at a point of no return where it can no longer force a
	// checkpoint. Fold and recycle a journal that cannot hold the record now, using
	// the real value length rather than the inline bound, so an out-of-line value's
	// large record is guaranteed to fit.
	if c.syncJournalLane() && !c.journal.Fits(len(keyBytes), len(src)) {
		if err := c.checkpointBufferedLocked(); err != nil {
			return false, err
		}
		c.automaticCheckpoints.Add(1)
		state = c.state.Load()
		if state == nil {
			return false, ErrClosed
		}
		resident, err = c.currentPrimaryResidentRoute(state, keyBytes)
		if err != nil {
			return false, err
		}
	}
	if c.deferredCanonicalLane() && len(c.primaryPendingParents) != 0 {
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
	slot, _, _, resolvedFound := path.leaf.LookupRawHashed(
		path.leafRoute.Hash, keyBytes,
	)
	if resolvedFound != found {
		return false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	_, becameEmpty, filledEmpty, err :=
		c.cowPrimaryMutation(
			state, keyBytes, src, false, found, slot,
			resident, &path,
		)
	if err != nil {
		return false, err
	}
	if becameEmpty && c.primaryRouter.Load().MarkEmpty(resident) {
		c.primaryEmptyLeaves.Add(1)
	}
	if filledEmpty && c.primaryRouter.Load().ClearEmpty(resident) {
		c.removePrimaryEmptyLeaf()
	}
	// The journal-backed sync lane made this mutation durable with an append+sync
	// inside cowPrimaryMutation, so it never takes the committer root fence; the
	// outer generation stays zero. Async and buffered-with-reader publish through
	// the committer and record their applied generation here.
	if !c.syncJournalLane() {
		generation = state.root.Generation + 1
	}
	return created, nil
}

func (c *Collection) deletePrimary(
	key []byte,
) (deleted bool, err error) {
	c.writer.Lock()
	var generation uint64
	var journalTarget uint64
	defer func() {
		syncWait := generation != 0 && c.synchronous()
		groupWait := journalTarget != 0
		if syncWait || groupWait {
			c.durabilityWait.Add(1)
		}
		c.writer.Unlock()
		if syncWait {
			err = errors.Join(err, c.waitPublished(generation))
		} else if groupWait {
			err = errors.Join(err, c.journalGroupAwait(journalTarget))
		}
		if syncWait || groupWait {
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
retryAfterUnifiedDeleteFold:
	state := c.state.Load()
	if state == nil || state.root.PrimaryRoot == (storeio.PageRef{}) {
		return false, nil
	}
	// key is borrowed for this call; stages that persist it copy it.
	keyBytes := key
	resident, err := c.currentPrimaryResidentRoute(state, keyBytes)
	if err != nil {
		return false, err
	}
	canonicalPath := c.canonicalFramePathEligible()
	if canonicalPath {
		leafLease, acquireErr :=
			c.primaryRouter.Load().AcquireLeaf(c.cache, resident)
		if acquireErr != nil {
			return false, acquireErr
		}
		handled, overlayDeleted, pressure, overlayErr :=
			c.tryPrimaryUnifiedOverlayDelete(
				state, resident, leafLease.Page(), keyBytes,
			)
		leafLease.Release()
		if overlayErr != nil {
			return false, fmt.Errorf("delete unified overlay: %w", overlayErr)
		}
		if handled {
			if overlayDeleted && c.buffered() {
				target, ackErr := c.journalDepositLocked(
					storeio.RecoveryRecordKindDelete,
					state.root.Generation+1, keyBytes, nil,
				)
				if ackErr != nil {
					return true, ackErr
				}
				journalTarget = target
			}
			return overlayDeleted, nil
		}
		if pressure || c.primaryUnifiedOverlay.hasPending() {
			if err := c.materializePrimaryOverlayPressureLocked(); err != nil {
				return false, fmt.Errorf(
					"materialize before unified delete: %w", err,
				)
			}
			goto retryAfterUnifiedDeleteFold
		}
	} else if c.primaryUnifiedOverlay.hasPending() {
		if err := c.materializePrimaryOverlayPressureLocked(); err != nil {
			return false, err
		}
		goto retryAfterUnifiedDeleteFold
	}
	if canonicalPath {
		if err := c.ensureBufferedPrimaryMutationCapacity(
			resident, 0,
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
		leaf, workspaceErr := storeio.AdmittedPrimaryLeafForMutationWithScratch(
			leafLease.Page(), c.storeID, resident.Bucket,
			storeio.CommonPrimaryLeafBounds{
				FileEnd:           state.super.FileEnd,
				NextLogicalID:     state.root.NextLogicalID,
				AllocationQuantum: state.root.PageSize,
			},
			c.primaryLeafMutationScratch,
		)
		if workspaceErr != nil {
			leafLease.Release()
			return false, fmt.Errorf(
				"admit unified delete fallback: %w", workspaceErr,
			)
		}
		// A deleted out-of-line value's chain retires on the canonical lane too:
		// cowBufferedPrimaryMutation drops it if still volatile or defers it to the
		// checkpoint if the base already made it durable, so the delete never has to
		// leave the deferred lane for the transactional path.
		slot, _, _, found := leaf.LookupRawHashed(
			resident.Hash, keyBytes,
		)
		if !found {
			leafLease.Release()
			return false, nil
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
		ackGeneration := state.root.Generation + 1
		// The journal-backed sync lane journaled this delete before publish inside
		// cowBufferedPrimaryMutation, so the outer generation stays zero and it never
		// takes the retired chain fence. Buffered-visible deposits its redo record
		// here for the shared group sync after the writer is released.
		if c.buffered() {
			target, ackErr := c.journalDepositLocked(
				storeio.RecoveryRecordKindDelete, ackGeneration, keyBytes, nil,
			)
			if ackErr != nil {
				return true, ackErr
			}
			journalTarget = target
		}
		return true, nil
	}
	if c.deferredCanonicalLane() && len(c.primaryPendingParents) != 0 {
		if err := c.materializePrimaryParentsLocked(); err != nil {
			return false, fmt.Errorf(
				"materialize before transactional delete: %w", err,
			)
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
		return false, fmt.Errorf("acquire transactional delete path: %w", err)
	}
	defer path.Release()
	slot, _, _, found := path.leaf.LookupRawHashed(
		path.leafRoute.Hash, keyBytes,
	)
	if !found {
		return false, nil
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
		return false, fmt.Errorf("rewrite transactional delete leaf: %w", err)
	}
	if becameEmpty && c.primaryRouter.Load().MarkEmpty(resident) {
		c.primaryEmptyLeaves.Add(1)
	}
	// The journal-backed sync lane made this delete durable inside
	// cowPrimaryMutation; the outer generation stays zero and never takes the
	// committer fence.
	if !c.syncJournalLane() {
		generation = state.root.Generation + 1
	}
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
		freeHead: state.freeHead,
	}
	c.bufferedValueBefore = append(c.bufferedValueBefore[:0], old...)
	before := c.bufferedValueBefore

	c.snapshotGate.Lock()
	c.beginReaderFence()
	if c.anyActiveReaders() {
		c.endReaderFence()
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
		c.endReaderFence()
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
	c.endReaderFence()
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
	if state == nil || leaf == nil || !c.deferredCanonicalLane() {
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

	// The new pages this mutation admits occupy the collection's volatile file
	// region above the current FileEnd. The first pending parent reserves a gap
	// wide enough for one worst-case checkpoint transaction below it, so a later
	// materialize allocates its durable pages from the checkpoint base's FileEnd
	// without colliding with any volatile page.
	baseOffset := state.super.FileEnd
	if len(c.primaryPendingParents) == 0 {
		gap := uint64(c.options.maxTransactionPages) *
			uint64(c.options.MaxPageSize)
		if baseOffset > math.MaxUint64-gap {
			return storeio.PageRef{}, false, false,
				storeio.ErrInvalidWrite
		}
		baseOffset += gap
	}

	// Every frame admitted from here to the publish is tracked so any error
	// return before the point of no return hands it back to the cache; the
	// tracking slice must therefore start empty for this mutation.
	c.primaryMutationAdmitted = c.primaryMutationAdmitted[:0]

	// An oversized value is stored out of line as a VOLATILE overflow chain minted
	// just below the new leaf. The leaf then carries only the chain head, exactly
	// as the transactional path does; the visible FileEnd/NextLogicalID advance to
	// cover the chain so the head and every link validate, and the checkpoint
	// re-mints the chain durable and patches the head in place. No device bytes are
	// written at Put.
	value := storeio.CommonPrimaryLeafValue{Inline: src}
	var overflowBytes uint64
	var overflowPages int
	preparePath := filePrimaryMutationPath{leaf: *leaf}
	leafBounds := c.primaryLeafBounds(state)
	if !deleting && !c.primaryOverflowValueIsInline(len(src)) {
		head, ovBytes, ovPages, mintErr := c.mintBufferedPrimaryOverflowChain(
			src, generation, baseOffset, state.root.NextLogicalID,
		)
		if mintErr != nil {
			// A mid-chain mint failure has already admitted the earlier extents;
			// hand them back so the failed attempt leaves no dirty-capacity residue.
			c.unadmitPrimaryMutationFrames()
			return storeio.PageRef{}, false, false, mintErr
		}
		value = storeio.CommonPrimaryLeafValue{Overflow: head}
		overflowBytes, overflowPages = ovBytes, ovPages
		// The leaf embeds the freshly minted head, so it must be encoded against
		// ceilings that already cover the chain rather than the published state's.
		// The chain ends at the leaf offset, so a FileEnd there and a NextLogicalID
		// past the last extent validate the head and every forward link.
		leafBounds = storeio.CommonPrimaryLeafBounds{
			FileEnd:           baseOffset + overflowBytes,
			NextLogicalID:     state.root.NextLogicalID + uint64(overflowPages),
			AllocationQuantum: state.root.PageSize,
		}
		// leaf is already the writer-owned raw mutation workspace. Rebind its
		// overflow-validation bounds directly; the durable input and output on
		// either side of this bridge remain class 5.
		releaf := storeio.AdmittedCommonPrimaryLeaf(
			leaf.PersistentBytes(), c.storeID, resident.Bucket, leafBounds,
		)
		preparePath.leaf = releaf
	}

	leafImage, leafBytes, _, prepareErr := c.preparePrimaryLeafMutation(
		&preparePath, generation, key, value, deleting, found, slot, leafBounds,
	)
	if prepareErr != nil {
		c.unadmitPrimaryMutationFrames()
		return storeio.PageRef{}, false, false, prepareErr
	}
	becameEmpty = deleting && leaf.Len() == 1
	filledEmpty = !deleting && !found && leaf.Len() == 0

	// A replaced or deleted out-of-line value's old chain retires. A chain still
	// volatile (minted since the checkpoint base) drops with the superseded leaf as
	// a memory-only frame set; a chain the checkpoint base already made durable is
	// retired against that base at the next materialize.
	oldOverflowVolatile := false
	c.overflowRetireScratch = c.overflowRetireScratch[:0]
	if found {
		if _, oldValue, ok := leaf.LookupHashed(
			resident.Hash, key,
		); ok && oldValue.IsOverflow() {
			extents, cErr := c.collectPrimaryOverflowExtents(
				c.overflowRetireScratch[:0], oldValue.Overflow,
				c.primaryLeafBounds(state),
			)
			if cErr != nil {
				c.unadmitPrimaryMutationFrames()
				return storeio.PageRef{}, false, false, cErr
			}
			c.overflowRetireScratch = extents
			base := c.primaryCheckpointBaseState()
			oldOverflowVolatile = base != nil &&
				oldValue.Overflow.Offset >= base.super.FileEnd
			if !oldOverflowVolatile &&
				len(c.primaryPendingOverflowRetire)+len(extents) >
					cap(c.primaryPendingOverflowRetire) {
				// This is a retryable pressure error — the caller checkpoints and
				// retries — so the minted chain must be handed back or every retry
				// would strand another chain's worth of dirty frames.
				c.unadmitPrimaryMutationFrames()
				return storeio.PageRef{}, false, false, fmt.Errorf(
					"%w: buffered primary overflow retirements",
					storeio.ErrRetiredExtentCapacity,
				)
			}
		}
	}

	leafOffset := baseOffset + overflowBytes
	if leafOffset > math.MaxUint64-uint64(leafBytes) {
		c.unadmitPrimaryMutationFrames()
		return storeio.PageRef{}, false, false,
			storeio.ErrInvalidWrite
	}
	nextLeaf = storeio.PageRef{
		Offset: leafOffset, LogicalID: resident.Ref.LogicalID,
		Generation: generation, Length: uint32(leafBytes),
		Kind: storeio.PagePrimaryLeaf,
	}
	if !c.primaryRouter.Load().CanUpdateLeaf(
		resident, nextLeaf, generation,
	) {
		c.unadmitPrimaryMutationFrames()
		return storeio.PageRef{}, false, false,
			storeio.ErrSegmentedTabletRouterCorrupt
	}
	if err := c.cache.AdmitBufferedDirty(
		nextLeaf, leafImage, math.MaxUint64,
	); err != nil {
		c.unadmitPrimaryMutationFrames()
		return storeio.PageRef{}, false, false, err
	}
	c.primaryMutationAdmitted = append(c.primaryMutationAdmitted, nextLeaf)

	nextRoot := state.root
	nextRoot.Generation = generation
	nextRoot.NextLogicalID = state.root.NextLogicalID + uint64(overflowPages)
	if deleting {
		nextRoot.DocumentCount--
	} else if !found {
		nextRoot.DocumentCount++
	}
	nextSuper := state.super
	nextSuper.Generation = generation
	nextSuper.FileEnd = leafOffset + uint64(leafBytes)
	nextState := &fileStoreState{
		root: nextRoot, super: nextSuper,
		freeHead: state.freeHead,
	}

	// Prepare this mutation's exact-index overlay records (§3's write rule)
	// as a fallible step: the point-of-no-return install below only links
	// prepared chain heads, so it cannot fail and leave a journaled mutation
	// with a stale index. The durable posting pages are not written now — a
	// buffered leaf frame is volatile until checkpoint, and the checkpoint
	// fold resolves the overlay into durable pages then. On recovery the
	// journaled Put/Delete replays through this same path, so the records
	// regenerate and the fold rebuilds the index identically.
	preparedExact, err := c.preparePrimaryExactBufferedMutation(
		leafImage, resident,
		generation, c.primaryLeafBounds(nextState),
	)
	if err != nil {
		c.unadmitPrimaryMutationFrames()
		return storeio.PageRef{}, false, false, err
	}

	// Point of no return: every fallible prepare step above has succeeded, the
	// dirty frame is admitted, and nothing is reader-visible or committed to the
	// pending-parent set yet. The journal-backed sync lane appends and syncs its
	// redo record here, so a reader never observes this mutation before it is
	// durable; a sync failure poisons and publishes nothing. Buffered-visible is
	// a no-op here and journals after publishing.
	if err := c.journalBeforePublishLocked(
		deleting, generation, key, src,
	); err != nil {
		// The fence failed with nothing published, so the admitted frames are
		// still unreferenced and must not survive as unowned dirty capacity,
		// and the prepared overlay records return to their bump allocator.
		c.unwindPrimaryExactPrepared(&preparedExact)
		c.unadmitPrimaryMutationFrames()
		return storeio.PageRef{}, false, false, err
	}
	// Point of no return passed: the frames are about to be published, so the
	// tracking slice is retired without unadmitting.
	c.primaryMutationAdmitted = c.primaryMutationAdmitted[:0]

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
	c.beginReaderFence()
	c.primaryRouter.Load().UpdateLeaf(
		resident, nextLeaf, generation,
	)
	c.installPrimaryExactResidentLocked(preparedExact)
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	c.retirePrimaryVolatileRefLocked(previousVolatile)
	if oldOverflowVolatile {
		for _, ref := range c.overflowRetireScratch {
			c.retirePrimaryVolatileRefLocked(ref)
		}
	}
	c.endReaderFence()
	c.snapshotGate.Unlock()
	if !oldOverflowVolatile && len(c.overflowRetireScratch) != 0 {
		c.primaryPendingOverflowRetire = append(
			c.primaryPendingOverflowRetire, c.overflowRetireScratch...,
		)
	}
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
	// The transaction opens before the leaf is encoded so an out-of-line overflow
	// chain can be minted inside it and its head threaded into the leaf image:
	// leaf and overflow extents then publish atomically in one generation, with no
	// window in which the value is allocated but unreachable.
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

	// A value past the inline budget is stored out of line. The chain is minted in
	// this transaction, so the leaf must be encoded against the transaction's
	// advanced FileEnd/NextLogicalID for the freshly minted head reference to
	// validate; the mutation leaf view is re-admitted against those bounds.
	value := storeio.CommonPrimaryLeafValue{Inline: src}
	leafBounds := c.primaryLeafBounds(state)
	if !deleting && !c.primaryOverflowValueIsInline(len(src)) {
		firstRef, ovErr := c.stagePrimaryOverflowChain(tx, src, generation)
		if ovErr != nil {
			return storeio.PageRef{}, false, false, ovErr
		}
		value = storeio.CommonPrimaryLeafValue{Overflow: firstRef}
		leafBounds = c.primaryMutationLeafBounds(tx)
		releaf, admitErr := storeio.AdmittedPrimaryLeafForMutation(
			path.leafLease.Page(), c.storeID, path.leafRoute.Bucket, leafBounds,
		)
		if admitErr != nil {
			return storeio.PageRef{}, false, false, admitErr
		}
		path.leaf = releaf
	}
	// Retire the superseded value's overflow chain, if the replaced or deleted key
	// held one, through the ordinary retirement accounting.
	if found {
		if _, oldValue, ok := path.leaf.LookupHashed(
			path.leafRoute.Hash, key,
		); ok && oldValue.IsOverflow() {
			extents, cErr := c.collectPrimaryOverflowExtents(
				c.overflowRetireScratch[:0], oldValue.Overflow,
				c.primaryLeafBounds(state),
			)
			if cErr != nil {
				return storeio.PageRef{}, false, false, cErr
			}
			c.overflowRetireScratch = extents
			for _, ref := range extents {
				if err := c.appendPrimaryRetirement(state, ref); err != nil {
					return storeio.PageRef{}, false, false, err
				}
			}
		}
	}
	leafImage, leafBytes, _, prepareErr := c.preparePrimaryLeafMutation(
		path, generation, key, value, deleting, found, slot, leafBounds,
	)
	if prepareErr != nil {
		return storeio.PageRef{}, false, false, prepareErr
	}
	becameEmpty = deleting && path.leaf.Len() == 1
	filledEmpty = !deleting && !found && path.leaf.Len() == 0
	// Fold-first lane (§7): run the §3 delta write rule for the common
	// slot-stable shapes and fold only the dirty leaves; a slot-reassigning
	// rewrite re-derives the whole bucket from the staged leaf image against
	// the same bounds it was encoded under. Either way the fresh epoch is
	// prepared here and staged durably by this same transaction, so the
	// index and the graph publish in one atomic generation.
	preparedExact, prepareExactErr := c.preparePrimaryExactLeaf(
		leafImage, resident,
		generation, leafBounds,
	)
	if prepareExactErr != nil {
		return storeio.PageRef{}, false, false, prepareExactErr
	}

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
	// Stage the maintained exact-index pages inside this transaction and retire
	// the superseded ones, so the posting index and the graph publish in one
	// atomic generation with no new commit-window shape.
	var exactRoot storeio.PageRef
	if preparedExact.active {
		exactRoot, err = c.stagePrimaryExactPagesLocked(
			tx, state, generation, preparedExact.epoch.exact,
		)
		if err != nil {
			return storeio.PageRef{}, false, false, err
		}
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
	if preparedExact.active {
		nextState.root.ExactIndexRoot = exactRoot
	}
	if err := c.reserveFileRetirements(); err != nil {
		return storeio.PageRef{}, false, false,
			fmt.Errorf("vibejson: reserve retired extents: %w", err)
	}
	retirementReserved = true
	// The journal-backed sync lane reaches this transactional path only for an
	// out-of-line value (or the retirement of one), which mints page identities
	// the canonical frame lane cannot. Append and sync its redo record at the
	// point of no return — after every fallible prepare and before publish — so no
	// reader observes the mutation before it is durable, exactly as the canonical
	// lane does. It is a no-op for async and buffered-with-reader.
	if err := c.journalBeforePublishLocked(
		deleting, generation, key, src,
	); err != nil {
		return storeio.PageRef{}, false, false, err
	}
	if err := c.publishStagedPrimaryMutation(
		tx, nextState, nextInline, freeLog,
		resident, nextLeaf, preparedExact,
	); err != nil {
		return storeio.PageRef{}, false, false, err
	}
	abort = false
	return nextLeaf, becameEmpty, filledEmpty, nil
}

// primaryCheckpointBaseState returns the state the next materialize will derive
// its checkpoint from — the durable state, or a newer flush-less cut recorded in
// primaryCheckpointBase — without the clearing side effect materialize applies.
// A page whose offset is at or above this base's FileEnd is volatile (minted by a
// buffered mutation since the base and never written to device); one below it is
// durable. It is the discriminator for retiring a superseded overflow chain.
func (c *Collection) primaryCheckpointBaseState() *fileStoreState {
	base := c.durableState.Load()
	if cp := c.primaryCheckpointBase; cp != nil &&
		(base == nil || cp.root.Generation > base.root.Generation) {
		base = cp
	}
	return base
}

func (c *Collection) materializePrimaryParentsLocked() error {
	err := c.materializePrimaryParentsOnceLocked()
	if !c.deferredCanonicalLane() ||
		!errors.Is(err, storeio.ErrCheckpointRequired) {
		return err
	}
	// A deferred cut is intentionally device-silent, but each published cut
	// still occupies one entry in the fixed committer. Snapshot-heavy workloads
	// and long repacks can exhaust those entries before an application Flush.
	// The failed materialization has published nothing and its transaction has
	// already unwound here, so drain the previously published cuts and retry the
	// unchanged class-5 overlay against the newly durable base. The internal
	// sentinel is bounded backpressure, not a user-visible mutation failure.
	if err := c.flushBufferedPublishedLocked(); err != nil {
		return err
	}
	c.automaticCheckpoints.Add(1)
	return c.materializePrimaryParentsOnceLocked()
}

// materializePrimaryOverlayPressureLocked drains a full/non-admissible class-5
// overlay without accidentally discarding a volatile generation interval. The
// ordinary buffered delta lane first carries a complete suffix into the journal
// without syncing; only then may a device-silent fold recycle the overlay. If
// the suffix is incomplete or the bounded journal is full, use the physical
// checkpoint path instead. Other durability lanes retain their existing
// device-silent materialization behavior.
func (c *Collection) materializePrimaryOverlayPressureLocked() error {
	if !c.bufferedJournalDeltaLane() {
		if err := c.materializePrimaryParentsLocked(); err != nil {
			return err
		}
		c.primaryOverlayFolds.Add(1)
		return nil
	}
	handled, err := c.carryBufferedJournalDeltaBeforeFoldLocked()
	if err != nil {
		return err
	}
	if handled {
		if err := c.materializePrimaryParentsLocked(); err != nil {
			return err
		}
		c.primaryOverlayFolds.Add(1)
		return nil
	}
	c.journalDeltaFullFallbacks.Add(1)
	if err := c.checkpointBufferedLocked(); err != nil {
		return err
	}
	c.automaticCheckpoints.Add(1)
	return nil
}

func (c *Collection) materializePrimaryParentsOnceLocked() (err error) {
	if len(c.primaryPendingParents) == 0 &&
		!c.primaryUnifiedOverlay.hasPending() {
		return nil
	}
	if c.primaryUnifiedOverlay.hasPending() {
		visible := c.state.Load()
		if visible == nil {
			return ErrClosed
		}
		if err := c.preparePrimaryUnifiedOverlayParentsLocked(
			visible,
		); err != nil {
			return err
		}
	}
	if len(c.primaryPendingParents) == 0 {
		return nil
	}
	c.clearPrimaryVolatileRetiredLocked()
	if c.anyActiveReaders() &&
		len(c.primaryVolatileRetired)+
			len(c.primaryPendingParents) >
			cap(c.primaryVolatileRetired) {
		// An active reader pins the volatile frames this materialize would retire, so
		// the table cannot drain. Name the pinning snapshot and generation through
		// the shared retirement diagnostic rather than surfacing a bare capacity
		// number an operator cannot act on.
		return c.absorbRetirementPressure(fmt.Errorf(
			"%w: buffered primary volatile-reference capacity %d",
			storeio.ErrRetiredExtentCapacity,
			cap(c.primaryVolatileRetired),
		))
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
	c.primaryCheckpointVolatileOverflow =
		c.primaryCheckpointVolatileOverflow[:0]

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
		if storeio.PrimaryLeafClass(lease.Page()) ==
			storeio.CommonPrimaryLeafUnified {
			unified, ok := storeio.AdmittedCommonPrimaryUnifiedLeaf(
				lease.Page(), c.storeID, pending.leafRoute.Bucket,
				c.primaryLeafBounds(visible),
			)
			if !ok {
				pageBytes := len(lease.Page())
				lease.Release()
				return fmt.Errorf(
					"%w: checkpoint unified bucket=%d ref=%+v header=%+v bytes=%d",
					storeio.ErrCommonPrimaryLeafCorrupt,
					pending.leafRoute.Bucket, pending.volatileRef,
					header, pageBytes,
				)
			}
			if c.primaryUnifiedOverlay.pendingBucket(
				pending.leafRoute.Bucket,
			) && pending.volatileRef.Offset < base.super.FileEnd {
				var replacementStorage [primaryUnifiedOverlayRecords]storeio.CommonPrimaryUnifiedReplacement
				replacements, allPuts, patchErr :=
					c.primaryUnifiedOverlay.primaryUnifiedFixedReplacements(
						replacementStorage[:0],
						pending.leafRoute.Bucket, generation,
					)
				if patchErr != nil {
					lease.Release()
					return patchErr
				}
				leafHeader := unified.Header()
				leafHeader.Generation = generation
				image, patchable, patchErr :=
					unified.PatchPlanStableReplacements(
						c.primaryLeafScratch, leafHeader,
						replacements, c.primaryUnifiedBuilder,
					)
				if patchErr != nil {
					lease.Release()
					return patchErr
				}
				if allPuts && patchable {
					// The codec proved the complete template census, dictionary
					// candidate order, encoded content size, and smallest extent
					// unchanged. The image is a fresh-generation COW copy with
					// only changed row bodies patched and its CRC resealed.
					var page storeio.TransactionPage
					page, acquireErr = tx.AllocateNear(
						storeio.PagePrimaryLeaf, uint32(len(image)),
						pending.volatileRef.LogicalID,
						primaryLeafPlacementHint(
							pending.leafRoute.Bucket,
							layout.DataStart,
						),
					)
					if acquireErr == nil {
						copy(page.Bytes(), image)
					}
					if acquireErr == nil {
						acquireErr = page.Stage()
					}
					lease.Release()
					if acquireErr != nil {
						return acquireErr
					}
					pending.checkpointLeaf = page.Ref()
					if appendErr := c.appendPrimaryRetirement(
						base, pending.leafRoute.Ref,
					); appendErr != nil {
						return appendErr
					}
					continue
				}
			}
			records, renderErr :=
				unified.RenderRecordsWithScratch(
					c.primaryLeafMutationScratch,
				)
			if renderErr == nil && c.primaryUnifiedOverlay.pendingBucket(
				pending.leafRoute.Bucket,
			) {
				records, renderErr = c.primaryUnifiedOverlay.applyBucket(
					records, pending.leafRoute.Bucket, generation,
				)
			}
			// Re-mint memory-only overflow chains inside the checkpoint and
			// replace their row descriptors before sealing the class-5 image.
			if renderErr == nil {
				for rank := range records {
					head := records[rank].Value.Overflow
					if head == (storeio.PageRef{}) ||
						head.Offset < base.super.FileEnd {
						continue
					}
					var resolved []byte
					resolved, renderErr = c.appendPrimaryOverflowValue(
						c.overflowValueScratch[:0], head,
						c.primaryLeafBounds(visible),
					)
					if renderErr != nil {
						break
					}
					c.overflowValueScratch = resolved
					var extents []storeio.PageRef
					extents, renderErr = c.collectPrimaryOverflowExtents(
						c.primaryCheckpointVolatileOverflow, head,
						c.primaryLeafBounds(visible),
					)
					if renderErr != nil {
						break
					}
					c.primaryCheckpointVolatileOverflow = extents
					records[rank].Value.Overflow, renderErr =
						c.stagePrimaryOverflowChain(tx, resolved, generation)
					if renderErr != nil {
						break
					}
				}
			}
			leafHeader := unified.Header()
			leafHeader.Generation = generation
			var image []byte
			if renderErr == nil {
				image, renderErr =
					storeio.EncodeBestCommonPrimaryUnifiedLeaf(
						c.primaryLeafScratch, leafHeader, c.storeID,
						records, c.primaryMutationLeafBounds(tx),
						c.primaryUnifiedBuilder,
					)
			}
			var page storeio.TransactionPage
			if renderErr == nil {
				page, renderErr = tx.AllocateNear(
					storeio.PagePrimaryLeaf, uint32(len(image)),
					pending.volatileRef.LogicalID,
					primaryLeafPlacementHint(
						pending.leafRoute.Bucket,
						layout.DataStart,
					),
				)
			}
			if renderErr == nil {
				copy(page.Bytes(), image)
				renderErr = page.Stage()
			}
			lease.Release()
			if renderErr != nil {
				return renderErr
			}
			pending.checkpointLeaf = page.Ref()
			if appendErr := c.appendPrimaryRetirement(
				base, pending.leafRoute.Ref,
			); appendErr != nil {
				return appendErr
			}
			continue
		}
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
			// A leaf carrying a volatile overflow head must have its chain minted
			// durable in this checkpoint transaction and its head patched to the
			// durable one in place before the leaf seals; a head the base already
			// made durable is carried forward untouched. Reading the volatile chain
			// resolves against the visible bounds it was minted under.
			view := storeio.AdmittedCommonPrimaryLeaf(
				lease.Page(), c.storeID, pending.leafRoute.Bucket,
				c.primaryLeafBounds(visible),
			)
			if view.HasOverflowRows() {
				initErr = view.RewriteOverflowRefs(
					payload, func(head storeio.PageRef) (storeio.PageRef, error) {
						if head.Offset < base.super.FileEnd {
							return head, nil
						}
						resolved, rErr := c.appendPrimaryOverflowValue(
							c.overflowValueScratch[:0], head,
							c.primaryLeafBounds(visible),
						)
						if rErr != nil {
							return storeio.PageRef{}, rErr
						}
						c.overflowValueScratch = resolved
						// The volatile chain is now superseded by the durable one
						// minted below; record its extents so their memory-only frames
						// drop once this checkpoint publishes.
						extents, cErr := c.collectPrimaryOverflowExtents(
							c.primaryCheckpointVolatileOverflow, head,
							c.primaryLeafBounds(visible),
						)
						if cErr != nil {
							return storeio.PageRef{}, cErr
						}
						c.primaryCheckpointVolatileOverflow = extents
						return c.stagePrimaryOverflowChain(
							tx, resolved, generation,
						)
					},
				)
			}
			if initErr == nil {
				_, initErr = storeio.SealPage(page.Bytes())
			}
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

	// Retire the durable overflow chains that buffered Puts and Deletes superseded
	// since the base. Their pages predate this checkpoint, so they retire against
	// the base generation through the same reclaim accounting as the graph pages
	// above; the just-minted durable re-materializations above are unaffected.
	for _, ref := range c.primaryPendingOverflowRetire {
		if appendErr := c.appendPrimaryRetirement(base, ref); appendErr != nil {
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

	// Fold the overlay into durable exact pages in the same checkpoint
	// transaction that seals the graph (§7). Per-mutation buffered edits only
	// append overlay records; this is where they resolve onto the base
	// through the read rule at the fold generation and re-encode ONLY the
	// dirty leaves — the runs and stripes the window's touched terms name —
	// via the same cutter+builder a bulk build uses (byte-identical output —
	// the identity anchor), becoming durable with the state root's
	// ExactIndexRoot advancing to match, so a crash after this checkpoint
	// recovers a consistent index without replay. Untouched leaves keep
	// their durable pages by reference and a quiet window — no overlay
	// records, the same-value replacement shape the mixed harness's update
	// lane is made of — rewrites only the small root and catalog pages,
	// which is what keeps the amortized indexed checkpoint within the
	// unindexed arm's class.
	var exactRoot storeio.PageRef
	var exactPrepared primaryExactPrepared
	var quietExact []primaryExactResident
	defer func() {
		if abort && exactPrepared.epoch != nil {
			c.unwindPrimaryExactPrepared(&exactPrepared)
		}
	}()
	exactActive := c.primaryExactActive()
	if exactActive {
		c.recyclePrimaryExactEpochsLocked()
		staged := c.primaryEpoch.exact
		if !c.primaryEpoch.overlayEmpty() {
			exactPrepared, err = c.prepareDirtyPrimaryExactFold(
				generation, generation, nil,
			)
			if err != nil {
				return err
			}
			staged = exactPrepared.epoch.exact
		} else {
			// Staging assigns fresh root/catalog identities. Keep those writes
			// off the installed epoch until the transaction publishes: a later
			// free-log or state-root failure must leave retry metadata intact.
			quietExact = clonePrimaryExactResidentsForStaging(staged)
			staged = quietExact
		}
		exactRoot, err = c.stagePrimaryExactPagesLocked(
			tx, base, generation, staged,
		)
		if err != nil {
			c.unwindPrimaryExactPrepared(&exactPrepared)
			return err
		}
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
	if exactActive {
		nextState.root.ExactIndexRoot = exactRoot
	}
	if err := c.reserveFileRetirements(); err != nil {
		return fmt.Errorf(
			"vibejson: reserve primary checkpoint retirements: %w",
			err,
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
	for index := range c.primaryPendingParents {
		pending := &c.primaryPendingParents[index]
		c.primaryRouter.Load().UpdateLeaf(
			pending.resident, pending.checkpointLeaf,
			generation,
		)
	}
	// The fold's fresh epoch (empty overlay) publishes in the same gate +
	// fence section as the checkpointed state, retiring the consumed epoch
	// onto the generation-keyed pending list. A quiet window keeps its epoch
	// and overlay but adopts the newly published catalog/root identities now
	// that no fallible transaction step remains.
	if quietExact != nil {
		installPrimaryExactDurableMetadata(
			c.primaryEpoch.exact, quietExact,
		)
	}
	c.installPrimaryExactResidentLocked(exactPrepared)
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	c.primaryUnifiedOverlay.markFolded(generation, retiring)
	if retiring {
		c.cache.MarkUnreachable(c.retireRefScratch)
		c.cache.MarkUnreachable(c.primaryVolatileRetired)
		clear(c.primaryVolatileRetired)
		c.primaryVolatileRetired =
			c.primaryVolatileRetired[:0]
		c.extractNeverDurableRetirements(absorbedStart)
	}
	for index := range c.primaryPendingParents {
		c.retirePrimaryVolatileRefLocked(
			c.primaryPendingParents[index].volatileRef,
		)
	}
	// Drop the memory-only frames of every volatile overflow chain this checkpoint
	// re-minted durable, deferring past an active reader exactly as the volatile
	// leaves above do (a snapshot pinning the pre-checkpoint leaf still resolves the
	// old chain until it releases).
	for _, ref := range c.primaryCheckpointVolatileOverflow {
		c.retirePrimaryVolatileRefLocked(ref)
	}
	c.endReaderFence()
	c.snapshotGate.Unlock()
	c.finalizeReusable()
	c.commitFreeLog(freeLog)
	c.inlineFree = nextInline
	clear(c.primaryPendingParents)
	c.primaryPendingParents = c.primaryPendingParents[:0]
	clear(c.primaryPendingOverflowRetire)
	c.primaryPendingOverflowRetire = c.primaryPendingOverflowRetire[:0]
	// Record the published cut so the next materialize derives its base from it
	// rather than a durableState a flush has not yet advanced. A following
	// checkpointBufferedLocked flush advances durableState past this generation,
	// and the base selection above then drops the pointer.
	c.primaryCheckpointBase = nextState
	return nil
}

// preparePrimaryLeafMutation applies the exceptional copy-on-write mutation
// path and re-encodes the complete row set into the sole class-5 grammar. The
// ordinary class-5 path publishes into the resident overlay; this rewrite is
// reserved for an empty leaf, overlay pressure, overflow values, and batch
// folding. Re-placement is allowed here because indexed callers rebuild the
// affected bucket's posting contribution in the same publication.
func (c *Collection) preparePrimaryLeafMutation(
	path *filePrimaryMutationPath,
	generation uint64,
	key []byte,
	value storeio.CommonPrimaryLeafValue,
	deleting, found bool,
	slot uint8,
	bounds storeio.CommonPrimaryLeafBounds,
) ([]byte, int, uint8, error) {
	if path == nil {
		return nil, 0, 0, storeio.ErrInvalidWrite
	}
	rows := appendLeafRecords(c.structuralRows[:0], &path.leaf)
	at := len(rows)
	for rank := range rows {
		order := bytes.Compare(rows[rank].Key, key)
		if order >= 0 {
			at = rank
			break
		}
	}
	newSlot := slot
	switch {
	case deleting:
		if at >= len(rows) || !bytes.Equal(rows[at].Key, key) {
			return nil, 0, 0, storeio.ErrCommonPrimaryLeafNotFound
		}
		copy(rows[at:], rows[at+1:])
		rows[len(rows)-1] = storeio.CommonPrimaryLeafRecord{}
		rows = rows[:len(rows)-1]
	case found:
		if at >= len(rows) || !bytes.Equal(rows[at].Key, key) {
			return nil, 0, 0, storeio.ErrCommonPrimaryLeafNotFound
		}
		rows[at].Value = value
	default:
		if len(rows) == storeio.CommonPrimaryLeafWideSlots {
			c.primaryLeafSplitRequired.Add(1)
			return nil, 0, 0, errors.Join(
				ErrPrimaryLeafSplitRequired,
				storeio.ErrCommonPrimaryLeafFull,
			)
		}
		rows = append(rows, storeio.CommonPrimaryLeafRecord{})
		copy(rows[at+1:], rows[at:])
		rows[at] = storeio.CommonPrimaryLeafRecord{Key: key, Value: value}
	}
	c.structuralRows = rows
	if err := storeio.PlaceCommonPrimaryLeafRecords(
		storeio.CommonPrimaryLeafWide, c.storeID, rows,
	); err != nil {
		return nil, 0, 0, err
	}
	if !deleting {
		newSlot = rows[at].Slot
	}
	page, err := storeio.EncodeBestCommonPrimaryUnifiedLeaf(
		c.primaryLeafScratch,
		storeio.CommonPrimaryLeafHeader{
			StoreID: c.storeID, Generation: generation,
			Bucket: path.leaf.Header().Bucket,
		},
		c.storeID, rows, bounds, c.primaryUnifiedBuilder,
	)
	if errors.Is(err, storeio.ErrCommonPrimaryLeafFull) {
		c.primaryLeafSplitRequired.Add(1)
		return nil, 0, 0, errors.Join(
			ErrPrimaryLeafSplitRequired,
			storeio.ErrCommonPrimaryLeafFull,
		)
	}
	if err != nil {
		return nil, 0, 0, err
	}
	return page, len(page), newSlot, nil
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

// primaryMutationLeafBounds are the leaf-admission bounds for a leaf encoded
// mid-transaction: a value's overflow chain minted in tx advances FileEnd and
// NextLogicalID, so the leaf that embeds the fresh head reference must validate
// against the transaction's advanced ceilings, not the published state's.
func (c *Collection) primaryMutationLeafBounds(
	tx *storeio.WriteTransaction,
) storeio.CommonPrimaryLeafBounds {
	return storeio.CommonPrimaryLeafBounds{
		FileEnd:           tx.FileEnd(),
		NextLogicalID:     tx.NextLogicalID(),
		AllocationQuantum: uint32(c.options.PageSize),
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

// extractNeverDurableRetirements completes the one-publication-late handoff.
// PublishInlineRetiring appended only records whose exact older pending writes
// it transitioned to superseded under the committer checkpoint mutex. The
// caller has since made those refs unreachable in PageCache while the
// snapshot gate and direct-reader fence still exclude every old reader.
//
// The extents move into retirementAbsorbed rather than directly into the
// allocator. refreshReusableFor merges that bounded batch before the next
// transaction, so ordinary ReuseEdits record the mandatory free-log delete or
// shortened-set if the next generation consumes one.
func (c *Collection) extractNeverDurableRetirements(start int) {
	if c == nil || c.reclaimer == nil ||
		start < 0 || start > len(c.retirementAbsorbed) {
		return
	}
	c.retirementAbsorbed = c.reclaimer.ExtractRetiredExact(
		c.retirementAbsorbed[:start],
		c.retirementAbsorbed[start:],
	)
}

// neverDurableRetirementOutput caps the optimization by both its fixed scratch
// and the allocator's remaining authoritative free-set room. The transaction
// may already have consumed whole reusable entries whose zero lengths are not
// removed until finalizeReusable; counting those entries as still live is a
// conservative O(1) bound. It can delay a candidate by one publication, while
// scanning the entire free set here would tax every mutation merely to recover
// room that finalize creates immediately afterward. Candidates beyond the
// bound stay in the reclaimer and take the ordinary fallback-generation path.
func (c *Collection) neverDurableRetirementOutput() []storeio.FreeExtent {
	if c == nil {
		return nil
	}
	used := len(c.retirementAbsorbed)
	room := min(
		cap(c.retirementAbsorbed)-used,
		c.freeSetLimit-len(c.reusable)-used,
	)
	if room < 0 {
		room = 0
	}
	return c.retirementAbsorbed[: used : used+room]
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
		freeHead: freeHead,
	}, *inlineFree, nil
}

func (c *Collection) publishStagedPrimaryMutation(
	tx *storeio.WriteTransaction,
	nextState *fileStoreState,
	nextInline storeio.InlineFreeDelta,
	freeLog freeLogCommit,
	route storeio.ResidentPrimaryRoute,
	nextLeaf storeio.PageRef,
	preparedExact primaryExactPrepared,
) error {
	absorbedStart := len(c.retirementAbsorbed)
	publish := func(retiring bool) error {
		if retiring {
			absorbed := c.retirementAbsorbed
			var err error
			var extracted []storeio.FreeExtent
			extracted, err = tx.PublishInlineRetiring(
				nextState.root, nextInline,
				c.retireRefScratch, c.retireScratch,
				c.neverDurableRetirementOutput(),
			)
			c.retirementAbsorbed = absorbed[:len(extracted)]
			return err
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
		c.installPrimaryExactResidentLocked(preparedExact)
		c.pageValidator.update(nextState)
		c.publishFileState(nextState)
		c.snapshotGate.Unlock()
		return nil
	}

	c.snapshotGate.Lock()
	c.beginReaderFence()
	retiring := !c.anyActiveReaders()
	if err := publish(retiring); err != nil {
		clear(c.retirementAbsorbed[absorbedStart:])
		c.retirementAbsorbed =
			c.retirementAbsorbed[:absorbedStart]
		c.endReaderFence()
		c.snapshotGate.Unlock()
		return err
	}
	c.primaryRouter.Load().UpdateLeaf(
		route, nextLeaf, nextState.root.Generation,
	)
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
	// A snapshot-contended buffered mutation publishes a fresh primary root with no
	// flush, exactly like a materialize. Record it as the checkpoint base so the
	// next materialize allocates past this cut's FileEnd and retires its root, not
	// a stale durableState's. See primaryCheckpointBase.
	c.primaryCheckpointBase = nextState
	return nil
}
