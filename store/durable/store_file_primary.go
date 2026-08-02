package durable

import (
	"fmt"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson"
)

// resolvePrimaryGraph appends one exact inline value through the ordered
// primary graph selected by state. Every resident view is reconstructed from a
// page whose complete typed validation ran once at cache admission. All leases
// are released before return, and a caller-provided destination with enough
// capacity makes both hits and misses allocation-free.
func (c *Collection) resolvePrimaryGraph(
	dst []byte,
	state *fileStoreState,
	key []byte,
) ([]byte, bool, error) {
	if c == nil || state == nil ||
		state.root.PrimaryRoot == (storeio.PageRef{}) {
		return dst, false, nil
	}
	keyBytes := key
	if len(keyBytes) == 0 ||
		len(keyBytes) > storeio.CommonPrimaryLeafMaxKeyBytes {
		return dst, false, nil
	}
	// The resident router is mutable collection-local acceleration for the
	// newest published generation. A snapshot whose rooted graph predates a
	// handle rewrite must retain its old physical route, so it uses the rooted
	// page-walk oracle instead. That substitution is sound here because every
	// state a Snapshot pins was materialized under the writer first: its rooted
	// graph carries every mutation its generation acknowledges.
	// Load the router pointer once: a structural split or empty reclaim swaps the
	// whole instance, so re-reading the field mid-read could observe two
	// generations. One consistent instance plus the generation guard keeps a
	// concurrent swap correct (a mismatch falls back to the rooted oracle).
	router := c.primaryRouter.Load()
	if router == nil ||
		router.Generation() != state.root.Generation {
		return c.resolvePrimaryGraphPageWalk(dst, state, key)
	}
	dst, found, superseded, err := c.resolvePrimaryGraphRouted(
		dst, state, state.root.Generation, keyBytes, router,
	)
	if superseded {
		// The serialized writer advanced the router after the generation check
		// but while this reader was selecting its handle. The snapshot's cut is
		// materialized, so the rooted oracle serves it exactly.
		return c.resolvePrimaryGraphPageWalk(dst, state, key)
	}
	return dst, found, err
}

func (c *Collection) containsPrimaryGraph(
	state *fileStoreState,
	key []byte,
) (bool, error) {
	if c == nil || state == nil ||
		state.root.PrimaryRoot == (storeio.PageRef{}) {
		return false, nil
	}
	if len(key) == 0 || len(key) > storeio.CommonPrimaryLeafMaxKeyBytes {
		return false, nil
	}
	router := c.primaryRouter.Load()
	if router == nil || router.Generation() != state.root.Generation {
		return c.containsPrimaryGraphPageWalk(state, key)
	}
	found, superseded, err := c.containsPrimaryGraphRouted(
		state, state.root.Generation, key, router,
	)
	if superseded {
		return c.containsPrimaryGraphPageWalk(state, key)
	}
	return found, err
}

// resolvePrimaryGraphRouted reads one key through a resident router whose
// generation matched state's at entry. superseded=true reports that the router
// advanced past the state before a leaf handle was selected, with nothing
// resolved; the caller chooses the recovery — the rooted page walk for a
// materialized snapshot cut, a retry against the newer publication for a live
// read.
func (c *Collection) resolvePrimaryGraphRouted(
	dst []byte,
	state *fileStoreState,
	generation uint64,
	keyBytes []byte,
	router *storeio.ResidentPrimaryRouter,
) (out []byte, found bool, superseded bool, err error) {
	route, ok := router.Route(keyBytes)
	if !ok {
		return dst, false, false, fmt.Errorf(
			"%w: resident primary route",
			storeio.ErrSegmentedTabletRouterCorrupt,
		)
	}
	// Close the race in which the serialized writer advances the router after
	// the generation check but while this reader is selecting its handle.
	if router.Generation() != generation {
		return dst, false, true, nil
	}
	if value, disposition, _ := c.primaryUnifiedOverlay.lookup(
		route.Bucket, route.Hash, keyBytes, generation,
	); disposition == primaryUnifiedOverlayValue {
		return append(dst, value...), true, false, nil
	} else if disposition == primaryUnifiedOverlayDeleted {
		return dst, false, false, nil
	}
	leafLease, err := router.AcquireLeaf(c.cache, route)
	if err != nil {
		return dst, false, false, err
	}
	dst, found, err = c.appendPrimaryLeafValue(
		dst, leafLease.Page(), state.root.StoreID, route.Bucket,
		route.Hash, keyBytes,
		storeio.CommonPrimaryLeafBounds{
			FileEnd:           state.fileEnd,
			NextLogicalID:     state.root.NextLogicalID,
			AllocationQuantum: state.root.PageSize,
		},
	)
	leafLease.Release()
	return dst, found, false, err
}

func (c *Collection) containsPrimaryGraphRouted(
	state *fileStoreState,
	generation uint64,
	key []byte,
	router *storeio.ResidentPrimaryRouter,
) (found bool, superseded bool, err error) {
	route, ok := router.Route(key)
	if !ok {
		return false, false, fmt.Errorf(
			"%w: resident primary route",
			storeio.ErrSegmentedTabletRouterCorrupt,
		)
	}
	if router.Generation() != generation {
		return false, true, nil
	}
	if _, disposition, _ := c.primaryUnifiedOverlay.lookup(
		route.Bucket, route.Hash, key, generation,
	); disposition == primaryUnifiedOverlayValue {
		return true, false, nil
	} else if disposition == primaryUnifiedOverlayDeleted {
		return false, false, nil
	}
	leafLease, err := router.AcquireLeaf(c.cache, route)
	if err != nil {
		return false, false, err
	}
	found, err = primaryLeafContains(
		leafLease.Page(), state.root.StoreID, route.Bucket, route.Hash, key,
		storeio.CommonPrimaryLeafBounds{
			FileEnd:           state.fileEnd,
			NextLogicalID:     state.root.NextLogicalID,
			AllocationQuantum: state.root.PageSize,
		},
	)
	leafLease.Release()
	return found, false, err
}

// resolvePrimaryGraphLive is the point-read resolver for the LIVE visible
// state (AppendRaw's epoch and lease paths). It differs from the snapshot
// resolver in exactly one decision: what a router-generation mismatch means.
//
// A live state is never materialized: between checkpoints its acknowledged
// mutations exist only as volatile leaf frames reachable through the resident
// router, while state.root.PrimaryRoot still names the last checkpoint's
// sealed graph. The rooted page walk therefore serves the CHECKPOINT BASE, not
// the state's own acknowledged content — a key put since the fold reads as
// missing and an updated key reads its pre-fold value. So when the router has
// moved AHEAD of the pinned state (a concurrent publication landed between the
// reader's state pin and its router read), the walk is not a legal fallback;
// the read must instead retry against the newer publication (superseded=true).
//
// The opposite mismatch — the router BEHIND the pinned state — is the
// structural swap window: a split or empty reclaim publishes its state under the
// snapshot gate and rebuilds the router outside it. A structural publication
// is a checkpoint (its pending parents were folded first), so that state's
// rooted graph is complete and the page walk is exactly right; retrying there
// would spin for the whole router rebuild for no benefit.
func (c *Collection) resolvePrimaryGraphLive(
	dst []byte,
	state *fileStoreState,
	generation uint64,
	key []byte,
) (out []byte, found bool, superseded bool, err error) {
	if c == nil || state == nil ||
		state.root.PrimaryRoot == (storeio.PageRef{}) {
		return dst, false, false, nil
	}
	if len(key) == 0 ||
		len(key) > storeio.CommonPrimaryLeafMaxKeyBytes {
		return dst, false, false, nil
	}
	router := c.primaryRouter.Load()
	if router == nil ||
		router.Generation() < generation {
		out, found, err = c.resolvePrimaryGraphPageWalk(dst, state, key)
		return out, found, false, err
	}
	if router.Generation() != generation {
		return dst, false, true, nil
	}
	return c.resolvePrimaryGraphRouted(dst, state, generation, key, router)
}

func (c *Collection) containsPrimaryGraphLive(
	state *fileStoreState,
	generation uint64,
	key []byte,
) (found bool, superseded bool, err error) {
	if c == nil || state == nil ||
		state.root.PrimaryRoot == (storeio.PageRef{}) {
		return false, false, nil
	}
	if len(key) == 0 || len(key) > storeio.CommonPrimaryLeafMaxKeyBytes {
		return false, false, nil
	}
	router := c.primaryRouter.Load()
	if router == nil || router.Generation() < generation {
		found, err = c.containsPrimaryGraphPageWalk(state, key)
		return found, false, err
	}
	if router.Generation() != generation {
		return false, true, nil
	}
	return c.containsPrimaryGraphRouted(state, generation, key, router)
}

// appendPrimaryLeafValue reads one exact value from the sole admitted class-5
// leaf grammar. Inline rows splice their canonical spelling into dst; overflow
// rows resolve the existing chain.
func (c *Collection) appendPrimaryLeafValue(
	dst []byte,
	page []byte,
	storeID [16]byte,
	bucket storeio.BucketID,
	hash uint64,
	keyBytes []byte,
	bounds storeio.CommonPrimaryLeafBounds,
) ([]byte, bool, error) {
	if storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafUnified {
		return dst, false, fmt.Errorf(
			"%w: non-unified primary leaf",
			storeio.ErrCommonPrimaryLeafCorrupt,
		)
	}
	uv, ok := storeio.AdmittedCommonPrimaryUnifiedLeaf(
		page, storeID, bucket, bounds,
	)
	if !ok {
		return dst, false, fmt.Errorf(
			"%w: unified primary leaf",
			storeio.ErrCommonPrimaryLeafCorrupt,
		)
	}
	body, overflow, found := uv.LookupBodyHashed(hash, keyBytes)
	if !found {
		return dst, false, nil
	}
	if overflow {
		out, err := c.appendPrimaryOverflowValue(
			dst, storeio.DecodePrimaryOverflowRef(body), bounds,
		)
		if err != nil {
			return dst, false, err
		}
		return out, true, nil
	}
	return uv.AppendAdmittedRowBody(dst, body), true, nil
}

func primaryLeafContains(
	page []byte,
	storeID [16]byte,
	bucket storeio.BucketID,
	hash uint64,
	key []byte,
	bounds storeio.CommonPrimaryLeafBounds,
) (bool, error) {
	if storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafUnified {
		return false, fmt.Errorf(
			"%w: non-unified primary leaf",
			storeio.ErrCommonPrimaryLeafCorrupt,
		)
	}
	uv, ok := storeio.AdmittedCommonPrimaryUnifiedLeaf(
		page, storeID, bucket, bounds,
	)
	if !ok {
		return false, fmt.Errorf(
			"%w: unified primary leaf",
			storeio.ErrCommonPrimaryLeafCorrupt,
		)
	}
	_, _, found := uv.LookupBodyHashed(hash, key)
	return found, nil
}

// resolvePrimaryGraphPageWalk is the rooted resolver retained both as a
// differential correctness oracle and as the production path for snapshots
// older than the mutable resident router's reflected generation.
func (c *Collection) resolvePrimaryGraphPageWalk(
	dst []byte,
	state *fileStoreState,
	key []byte,
) ([]byte, bool, error) {
	lookup, ok, err := c.acquirePrimaryLeafPageWalk(state, key)
	if err != nil || !ok {
		return dst, false, err
	}
	dst, found, err := c.appendPrimaryLeafValue(
		dst, lookup.lease.Page(), lookup.storeID, lookup.bucket,
		lookup.hash, key, lookup.bounds,
	)
	lookup.lease.Release()
	return dst, found, err
}

func (c *Collection) containsPrimaryGraphPageWalk(
	state *fileStoreState,
	key []byte,
) (bool, error) {
	lookup, ok, err := c.acquirePrimaryLeafPageWalk(state, key)
	if err != nil || !ok {
		return false, err
	}
	found, err := primaryLeafContains(
		lookup.lease.Page(), lookup.storeID, lookup.bucket,
		lookup.hash, key, lookup.bounds,
	)
	lookup.lease.Release()
	return found, err
}

type primaryLeafPageLookup struct {
	lease   storeio.PageLease
	storeID [16]byte
	bucket  storeio.BucketID
	hash    uint64
	bounds  storeio.CommonPrimaryLeafBounds
}

// acquirePrimaryLeafPageWalk resolves the rooted catalog/tablet/anchor route
// once and returns the validated terminal leaf lease. Payload readers and
// existence probes share it so a no-copy primary-key probe has exactly the
// same catalog, route, and leaf checks as AppendRaw. An existence probe does
// not walk an overflow payload chain; corruption confined to document bytes is
// reported when those bytes are actually read.
func (c *Collection) acquirePrimaryLeafPageWalk(
	state *fileStoreState,
	key []byte,
) (primaryLeafPageLookup, bool, error) {
	if c == nil || state == nil ||
		state.root.PrimaryRoot == (storeio.PageRef{}) {
		return primaryLeafPageLookup{}, false, nil
	}
	keyBytes := key
	if len(keyBytes) == 0 ||
		len(keyBytes) > storeio.CommonPrimaryLeafMaxKeyBytes {
		return primaryLeafPageLookup{}, false, nil
	}
	bounds := storeio.GlobalTabletCatalogBounds{
		StoreID:                state.root.StoreID,
		SelectedRootGeneration: state.root.Generation,
		FileEnd:                state.fileEnd,
		NextLogicalID:          state.root.NextLogicalID,
	}

	catalogLease, err := c.cache.Acquire(state.root.PrimaryRoot)
	if err != nil {
		return primaryLeafPageLookup{}, false, err
	}
	catalog := storeio.AdmittedGlobalTabletCatalogNode(
		catalogLease.Page(), bounds,
	)
	childLevel := catalog.ChildLevel()
	route := catalog.Route(keyBytes)
	catalogLease.Release()
	if route.Ref == (storeio.PageRef{}) {
		return primaryLeafPageLookup{}, false, fmt.Errorf(
			"%w: empty primary catalog root route",
			storeio.ErrGlobalTabletCatalogCorrupt,
		)
	}

	catalogLease, err = c.cache.Acquire(route.Ref)
	if err != nil {
		return primaryLeafPageLookup{}, false, err
	}
	catalog = storeio.AdmittedGlobalTabletCatalogNode(
		catalogLease.Page(), bounds,
	)
	if childLevel == storeio.GlobalTabletCatalogBranch {
		if catalog.Level() != storeio.GlobalTabletCatalogBranch {
			catalogLease.Release()
			return primaryLeafPageLookup{}, false, fmt.Errorf(
				"%w: primary catalog branch level",
				storeio.ErrGlobalTabletCatalogCorrupt,
			)
		}
		route = catalog.Route(keyBytes)
		catalogLease.Release()
		if route.Ref == (storeio.PageRef{}) {
			return primaryLeafPageLookup{}, false, fmt.Errorf(
				"%w: empty primary catalog branch route",
				storeio.ErrGlobalTabletCatalogCorrupt,
			)
		}
		catalogLease, err = c.cache.Acquire(route.Ref)
		if err != nil {
			return primaryLeafPageLookup{}, false, err
		}
		catalog = storeio.AdmittedGlobalTabletCatalogNode(
			catalogLease.Page(), bounds,
		)
	}
	if catalog.Level() != storeio.GlobalTabletCatalogLeaf {
		catalogLease.Release()
		return primaryLeafPageLookup{}, false, fmt.Errorf(
			"%w: primary catalog terminal level",
			storeio.ErrGlobalTabletCatalogCorrupt,
		)
	}
	tabletRoute := catalog.Route(keyBytes)
	catalogLease.Release()
	if tabletRoute.Ref == (storeio.PageRef{}) {
		return primaryLeafPageLookup{}, false, fmt.Errorf(
			"%w: empty primary tablet route",
			storeio.ErrGlobalTabletCatalogCorrupt,
		)
	}

	tabletLease, err := c.cache.Acquire(tabletRoute.Ref)
	if err != nil {
		return primaryLeafPageLookup{}, false, err
	}
	tablet := storeio.AdmittedGlobalTabletCatalogTabletRoot(
		tabletLease.Page(), bounds,
	)
	anchorRoute, ok := tablet.RouteAnchor(keyBytes)
	if !ok {
		tabletLease.Release()
		return primaryLeafPageLookup{}, false, fmt.Errorf(
			"%w: primary tablet anchor route",
			storeio.ErrGlobalTabletCatalogCorrupt,
		)
	}
	anchorLease, err := c.cache.Acquire(anchorRoute.Ref)
	if err != nil {
		tabletLease.Release()
		return primaryLeafPageLookup{}, false, err
	}
	anchor := storeio.AdmittedGlobalTabletCatalogAnchor(
		anchorLease.Page(), &tablet, anchorRoute.PageID,
	)
	hash := storeio.KeyHashBytes(state.root.StoreID, keyBytes)
	leafRoute, ok := anchor.RouteHashed(hash, keyBytes)
	anchorLease.Release()
	tabletLease.Release()
	if !ok || leafRoute.Ref == (storeio.PageRef{}) {
		return primaryLeafPageLookup{}, false, fmt.Errorf(
			"%w: primary anchor leaf route",
			storeio.ErrSegmentedTabletRouterCorrupt,
		)
	}

	leafLease, err := c.cache.Acquire(leafRoute.Ref)
	if err != nil {
		return primaryLeafPageLookup{}, false, err
	}
	return primaryLeafPageLookup{
		lease: leafLease, storeID: state.root.StoreID,
		bucket: leafRoute.Bucket, hash: leafRoute.Hash,
		bounds: storeio.CommonPrimaryLeafBounds{
			FileEnd:           state.fileEnd,
			NextLogicalID:     state.root.NextLogicalID,
			AllocationQuantum: state.root.PageSize,
		},
	}, true, nil
}

// validateOpenedPrimaryGraph walks the catalog levels selected by PrimaryRoot
// and validates every referenced tablet root and locator. It proves the fixed
// depth, stable node IDs, child kinds, common StoreID binding, and reference
// bounds without acquiring tablet anchors or primary leaves.
func validateOpenedPrimaryGraph(
	cache *storeio.PageCache,
	root storeio.StateRoot,
	fileEnd uint64,
) error {
	if root.PrimaryRoot == (storeio.PageRef{}) {
		return fmt.Errorf("vibedb: collection has no ordered-primary root")
	}
	bounds := storeio.GlobalTabletCatalogBounds{
		StoreID: root.StoreID, SelectedRootGeneration: root.Generation,
		FileEnd: fileEnd, NextLogicalID: root.NextLogicalID,
	}
	return withOpenedPrimaryCatalogNode(
		cache, root.PrimaryRoot, bounds,
		func(catalog *storeio.GlobalTabletCatalogNodeView) error {
			if catalog.Level() != storeio.GlobalTabletCatalogRoot ||
				catalog.PageID() != 0 || catalog.Count() == 0 {
				return fmt.Errorf("vibedb: ordered primary catalog root shape")
			}
			cursor := catalog.LowerBound(nil)
			for {
				route, ok := cursor.Route()
				if !ok {
					return fmt.Errorf("vibedb: ordered primary catalog root cursor")
				}
				if catalog.ChildLevel() == storeio.GlobalTabletCatalogLeaf {
					if err := validateOpenedPrimaryCatalogLeaf(
						cache, route, bounds,
					); err != nil {
						return err
					}
				} else if catalog.ChildLevel() == storeio.GlobalTabletCatalogBranch {
					if err := validateOpenedPrimaryCatalogBranch(
						cache, route, bounds,
					); err != nil {
						return err
					}
				} else {
					return fmt.Errorf("vibedb: ordered primary catalog child level")
				}
				if !cursor.Next() {
					break
				}
			}
			return nil
		},
	)
}

func validateOpenedPrimaryCatalogBranch(
	cache *storeio.PageCache,
	route storeio.GlobalTabletCatalogNodeRoute,
	bounds storeio.GlobalTabletCatalogBounds,
) error {
	return withOpenedPrimaryCatalogNode(
		cache, route.Ref, bounds,
		func(branch *storeio.GlobalTabletCatalogNodeView) error {
			if branch.Level() != storeio.GlobalTabletCatalogBranch ||
				branch.PageID() != route.ID ||
				branch.ChildLevel() != storeio.GlobalTabletCatalogLeaf ||
				branch.Count() == 0 {
				return fmt.Errorf("vibedb: ordered primary catalog branch shape")
			}
			cursor := branch.LowerBound(nil)
			for {
				leafRoute, ok := cursor.Route()
				if !ok {
					return fmt.Errorf("vibedb: ordered primary catalog branch cursor")
				}
				if err := validateOpenedPrimaryCatalogLeaf(
					cache, leafRoute, bounds,
				); err != nil {
					return err
				}
				if !cursor.Next() {
					break
				}
			}
			return nil
		},
	)
}

func validateOpenedPrimaryCatalogLeaf(
	cache *storeio.PageCache,
	route storeio.GlobalTabletCatalogNodeRoute,
	bounds storeio.GlobalTabletCatalogBounds,
) error {
	return withOpenedPrimaryCatalogNode(
		cache, route.Ref, bounds,
		func(leaf *storeio.GlobalTabletCatalogNodeView) error {
			if leaf.Level() != storeio.GlobalTabletCatalogLeaf ||
				leaf.PageID() != route.ID || leaf.Count() == 0 {
				return fmt.Errorf("vibedb: ordered primary catalog leaf shape")
			}
			cursor := leaf.LowerBound(nil)
			for {
				tabletRoute, ok := cursor.Route()
				if !ok {
					return fmt.Errorf(
						"vibedb: ordered primary catalog leaf cursor",
					)
				}
				if err := validateOpenedPrimaryTablet(
					cache, tabletRoute.Ref, bounds,
				); err != nil {
					return err
				}
				if !cursor.Next() {
					break
				}
			}
			return nil
		},
	)
}

func validateOpenedPrimaryTablet(
	cache *storeio.PageCache,
	ref storeio.PageRef,
	bounds storeio.GlobalTabletCatalogBounds,
) error {
	lease, err := cache.Acquire(ref)
	if err != nil {
		return fmt.Errorf("vibedb: open ordered primary tablet: %w", err)
	}
	tablet, err := storeio.OpenGlobalTabletCatalogTabletRoot(
		lease.Page(), ref, bounds,
	)
	locatorRef, locatorOK := tablet.LocatorRef()
	lease.Release()
	if err != nil {
		return fmt.Errorf("vibedb: open ordered primary tablet: %w", err)
	}
	if !locatorOK {
		return fmt.Errorf(
			"%w: ordered primary tablet locator",
			storeio.ErrGlobalTabletCatalogCorrupt,
		)
	}
	locatorLease, err := cache.Acquire(locatorRef)
	if err != nil {
		return fmt.Errorf("vibedb: open ordered primary locator: %w", err)
	}
	_, err = storeio.OpenGlobalTabletCatalogLocator(
		locatorLease.Page(), locatorRef, bounds,
	)
	locatorLease.Release()
	if err != nil {
		return fmt.Errorf("vibedb: open ordered primary locator: %w", err)
	}
	return nil
}

func withOpenedPrimaryCatalogNode(
	cache *storeio.PageCache,
	ref storeio.PageRef,
	bounds storeio.GlobalTabletCatalogBounds,
	visit func(*storeio.GlobalTabletCatalogNodeView) error,
) error {
	if cache == nil {
		return fmt.Errorf("vibedb: ordered primary catalog without page cache")
	}
	lease, err := cache.Acquire(ref)
	if err != nil {
		return fmt.Errorf("vibedb: open ordered primary catalog: %w", err)
	}
	defer lease.Release()
	node, err := storeio.OpenGlobalTabletCatalogNode(
		lease.Page(), ref, bounds,
	)
	if err != nil {
		return fmt.Errorf("vibedb: open ordered primary catalog: %w", err)
	}
	return visit(&node)
}

// setupResidentPrimaryLocked builds the collection-local acceleration a
// primary-layout store needs once its root is known: the mutation scratch sized
// for the largest leaf, the resident router rebuilt from the catalog, the
// deferred-lane pending-parent scratch, and the resident exact indexes. It is
// shared by Open (recovering a persisted primary root) and the fresh-create
// path (publishing an empty primary root), so both reach an identically wired
// collection. The writer must be held and the collection not yet reachable.
func (c *Collection) setupResidentPrimaryLocked(state *fileStoreState) error {
	// Structural mutation renders class 5 into an owned raw workspace before
	// re-encoding class 5. Large inline rows can require the full 64 KiB
	// transient extent, so the retained scratch covers that ceiling.
	c.primaryLeafScratch = make([]byte, storeio.CommonPrimaryLeafMaxExtentBytes)
	c.primaryLeafMutationScratch = storeio.NewPrimaryLeafMutationScratch(
		storeio.CommonPrimaryLeafMaxExtentBytes,
	)
	c.primaryUnifiedBuilder = storeio.NewUnifiedPrimaryLeafBuilder()
	if overlay := c.primaryUnifiedOverlay; overlay != nil {
		c.primaryUnifiedReplacementScratch = make(
			[]storeio.CommonPrimaryUnifiedReplacement,
			0, storeio.CommonPrimaryLeafWideSlots,
		)
		indexEntries := min(c.options.InlineValueBytes+2, 8192)
		indexEntries = max(indexEntries, 64)
		c.primaryUnifiedIndexScratch = make(
			[]vibejson.IndexEntry, 0, indexEntries,
		)
		canonicalBytes := min(
			len(overlay.arena), max(4096, 2*c.options.InlineValueBytes),
		)
		c.primaryUnifiedCanonical = make([]byte, 0, canonicalBytes)
	}
	c.primaryRootScratch = make([]byte, storeio.SegmentedTabletRouterRootBytes)
	builtRouter, err := storeio.BuildResidentPrimaryRouter(
		c.cache, state.root.PrimaryRoot,
		storeio.GlobalTabletCatalogBounds{
			StoreID: state.root.StoreID, SelectedRootGeneration: state.root.Generation,
			FileEnd: state.fileEnd, NextLogicalID: state.root.NextLogicalID,
		},
	)
	if err != nil {
		return fmt.Errorf("vibedb: build resident primary router: %w", err)
	}
	c.primaryRouter.Store(builtRouter)
	// Both buffered-visible and the journal-backed synchronous lane apply through
	// the deferred canonical-frame path, so both need the pending parent and
	// volatile-retire scratch. Async-visible on a primary graph uses the committer
	// path and does not.
	if c.buffered() || c.synchronous() {
		pendingCapacity := filePrimaryPendingCapacity(c.options)
		c.primaryPendingParents = make(
			[]filePrimaryPendingParent, 0, pendingCapacity,
		)
		c.primaryVolatileRetired = make([]storeio.PageRef, 0, pendingCapacity)
		// The deferred-canonical lane mints out-of-line values as volatile chains and
		// resolves them at read, checkpoint, and exact-index maintenance. Size the
		// chain scratch for one worst-case document so a steady-state overflow Put
		// never grows it, and the durable-retirement queue for one worst-case
		// checkpoint's superseded chains.
		maxOverflowPages := c.primaryOverflowPageCount(c.options.MaxDocumentBytes)
		c.overflowRefScratch = make([]storeio.PageRef, 0, maxOverflowPages)
		c.overflowOffsetScratch = make([]int, 0, maxOverflowPages)
		c.overflowValueScratch = make([]byte, 0, c.options.MaxDocumentBytes)
		c.overflowPageScratch = make([]byte, c.options.MaxPageSize)
		c.primaryPendingOverflowRetire = make(
			[]storeio.PageRef, 0, c.options.MaxRetiredExtents,
		)
	}
	c.setupPrimaryNativeFoldContexts()
	if err := c.openPrimaryExactIndexes(state); err != nil {
		return fmt.Errorf(
			"vibedb: open ordered-primary exact indexes: %w", err,
		)
	}
	return nil
}
