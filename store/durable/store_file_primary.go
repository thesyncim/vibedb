package durable

import (
	"fmt"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func (c *Collection) primaryGraphReadOnly() bool {
	if c == nil {
		return false
	}
	state := c.state.Load()
	return state != nil &&
		state.root.PrimaryRoot != (storeio.PageRef{})
}

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
	// Load the router pointer once: a structural split/merge/reclass swaps the
	// whole instance, so re-reading the field mid-read could observe two
	// generations. One consistent instance plus the generation guard keeps a
	// concurrent swap correct (a mismatch falls back to the rooted oracle).
	router := c.primaryRouter.Load()
	if router == nil ||
		router.Generation() != state.root.Generation {
		return c.resolvePrimaryGraphPageWalk(dst, state, key)
	}
	dst, found, superseded, err := c.resolvePrimaryGraphRouted(
		dst, state, keyBytes, router,
	)
	if superseded {
		// The serialized writer advanced the router after the generation check
		// but while this reader was selecting its handle. The snapshot's cut is
		// materialized, so the rooted oracle serves it exactly.
		return c.resolvePrimaryGraphPageWalk(dst, state, key)
	}
	return dst, found, err
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
	if router.Generation() != state.root.Generation {
		return dst, false, true, nil
	}
	leafLease, err := router.AcquireLeaf(c.cache, route)
	if err != nil {
		return dst, false, false, err
	}
	dst, found, err = c.appendPrimaryLeafValue(
		dst, leafLease.Page(), state.root.StoreID, route.Bucket,
		route.Hash, keyBytes,
		storeio.CommonPrimaryLeafBounds{
			FileEnd:           state.super.FileEnd,
			NextLogicalID:     state.root.NextLogicalID,
			AllocationQuantum: state.root.PageSize,
		},
	)
	leafLease.Release()
	return dst, found, false, err
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
// structural swap window: a split/merge/reclass publishes its state under the
// snapshot gate and rebuilds the router outside it. A structural publication
// is a checkpoint (its pending parents were folded first), so that state's
// rooted graph is complete and the page walk is exactly right; retrying there
// would spin for the whole router rebuild for no benefit.
func (c *Collection) resolvePrimaryGraphLive(
	dst []byte,
	state *fileStoreState,
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
		router.Generation() < state.root.Generation {
		out, found, err = c.resolvePrimaryGraphPageWalk(dst, state, key)
		return out, found, false, err
	}
	if router.Generation() != state.root.Generation {
		return dst, false, true, nil
	}
	return c.resolvePrimaryGraphRouted(dst, state, key, router)
}

// appendPrimaryLeafValue reads one exact value from an admitted primary leaf,
// dispatching on the leaf class recorded in the page. Template leaves splice the
// exact JSON into dst; raw leaves append the borrowed inline bytes, or resolve
// and append the out-of-line value when the record is an overflow reference. It
// allocates nothing for inline hits when dst has capacity for the value.
func (c *Collection) appendPrimaryLeafValue(
	dst []byte,
	page []byte,
	storeID [16]byte,
	bucket storeio.BucketID,
	hash uint64,
	keyBytes []byte,
	bounds storeio.CommonPrimaryLeafBounds,
) ([]byte, bool, error) {
	switch storeio.PrimaryLeafClass(page) {
	case storeio.CommonPrimaryLeafTemplate:
		tv, ok := storeio.AdmittedCommonPrimaryTemplateLeaf(page, bucket, bounds)
		if !ok {
			return dst, false, fmt.Errorf(
				"%w: template primary leaf",
				storeio.ErrCommonPrimaryLeafCorrupt,
			)
		}
		out, found := tv.AppendRawByKey(dst, keyBytes)
		return out, found, nil
	case storeio.CommonPrimaryLeafCompact:
		cv, ok := storeio.AdmittedCommonPrimaryCompactLeaf(page, bucket, bounds)
		if !ok {
			return dst, false, fmt.Errorf(
				"%w: compact primary leaf",
				storeio.ErrCommonPrimaryLeafCorrupt,
			)
		}
		out, found := cv.AppendRawByKey(dst, keyBytes)
		return out, found, nil
	case storeio.CommonPrimaryLeafUnified:
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
			// The overflow chain carries the document's canonical bytes, so
			// the read is the existing chain reassembly at verbatim parity
			// (unified-leaf design §6).
			out, err := c.appendPrimaryOverflowValue(
				dst, storeio.DecodePrimaryOverflowRef(body), bounds,
			)
			if err != nil {
				return dst, false, err
			}
			return out, true, nil
		}
		out, rendered := uv.AppendRowBody(dst, body)
		if !rendered {
			return dst, false, fmt.Errorf(
				"%w: unified row body",
				storeio.ErrCommonPrimaryLeafCorrupt,
			)
		}
		return out, true, nil
	}
	leaf := storeio.AdmittedCommonPrimaryLeaf(page, storeID, bucket, bounds)
	_, value, found := leaf.LookupHashed(hash, keyBytes)
	if !found {
		return dst, false, nil
	}
	if value.IsOverflow() {
		out, err := c.appendPrimaryOverflowValue(dst, value.Overflow, bounds)
		if err != nil {
			return dst, false, err
		}
		return out, true, nil
	}
	return append(dst, value.Inline...), true, nil
}

// resolvePrimaryGraphPageWalk is the rooted resolver retained both as a
// differential correctness oracle and as the production path for snapshots
// older than the mutable resident router's reflected generation.
func (c *Collection) resolvePrimaryGraphPageWalk(
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
	bounds := storeio.GlobalTabletCatalogBounds{
		StoreID:                state.root.StoreID,
		SelectedRootGeneration: state.root.Generation,
		FileEnd:                state.super.FileEnd,
		NextLogicalID:          state.root.NextLogicalID,
	}

	catalogLease, err := c.cache.Acquire(state.root.PrimaryRoot)
	if err != nil {
		return dst, false, err
	}
	catalog := storeio.AdmittedGlobalTabletCatalogNode(
		catalogLease.Page(), bounds,
	)
	childLevel := catalog.ChildLevel()
	route := catalog.Route(keyBytes)
	catalogLease.Release()
	if route.Ref == (storeio.PageRef{}) {
		return dst, false, fmt.Errorf(
			"%w: empty primary catalog root route",
			storeio.ErrGlobalTabletCatalogCorrupt,
		)
	}

	catalogLease, err = c.cache.Acquire(route.Ref)
	if err != nil {
		return dst, false, err
	}
	catalog = storeio.AdmittedGlobalTabletCatalogNode(
		catalogLease.Page(), bounds,
	)
	if childLevel == storeio.GlobalTabletCatalogBranch {
		if catalog.Level() != storeio.GlobalTabletCatalogBranch {
			catalogLease.Release()
			return dst, false, fmt.Errorf(
				"%w: primary catalog branch level",
				storeio.ErrGlobalTabletCatalogCorrupt,
			)
		}
		route = catalog.Route(keyBytes)
		catalogLease.Release()
		if route.Ref == (storeio.PageRef{}) {
			return dst, false, fmt.Errorf(
				"%w: empty primary catalog branch route",
				storeio.ErrGlobalTabletCatalogCorrupt,
			)
		}
		catalogLease, err = c.cache.Acquire(route.Ref)
		if err != nil {
			return dst, false, err
		}
		catalog = storeio.AdmittedGlobalTabletCatalogNode(
			catalogLease.Page(), bounds,
		)
	}
	if catalog.Level() != storeio.GlobalTabletCatalogLeaf {
		catalogLease.Release()
		return dst, false, fmt.Errorf(
			"%w: primary catalog terminal level",
			storeio.ErrGlobalTabletCatalogCorrupt,
		)
	}
	tabletRoute := catalog.Route(keyBytes)
	catalogLease.Release()
	if tabletRoute.Ref == (storeio.PageRef{}) {
		return dst, false, fmt.Errorf(
			"%w: empty primary tablet route",
			storeio.ErrGlobalTabletCatalogCorrupt,
		)
	}

	tabletLease, err := c.cache.Acquire(tabletRoute.Ref)
	if err != nil {
		return dst, false, err
	}
	tablet := storeio.AdmittedGlobalTabletCatalogTabletRoot(
		tabletLease.Page(), bounds,
	)
	anchorRoute, ok := tablet.RouteAnchor(keyBytes)
	if !ok {
		tabletLease.Release()
		return dst, false, fmt.Errorf(
			"%w: primary tablet anchor route",
			storeio.ErrGlobalTabletCatalogCorrupt,
		)
	}
	anchorLease, err := c.cache.Acquire(anchorRoute.Ref)
	if err != nil {
		tabletLease.Release()
		return dst, false, err
	}
	anchor := storeio.AdmittedGlobalTabletCatalogAnchor(
		anchorLease.Page(), &tablet, anchorRoute.PageID,
	)
	hash := storeio.KeyHashBytes(state.root.StoreID, keyBytes)
	leafRoute, ok := anchor.RouteHashed(hash, keyBytes)
	anchorLease.Release()
	tabletLease.Release()
	if !ok || leafRoute.Ref == (storeio.PageRef{}) {
		return dst, false, fmt.Errorf(
			"%w: primary anchor leaf route",
			storeio.ErrSegmentedTabletRouterCorrupt,
		)
	}

	leafLease, err := c.cache.Acquire(leafRoute.Ref)
	if err != nil {
		return dst, false, err
	}
	dst, found, err := c.appendPrimaryLeafValue(
		dst, leafLease.Page(), state.root.StoreID, leafRoute.Bucket,
		leafRoute.Hash, keyBytes,
		storeio.CommonPrimaryLeafBounds{
			FileEnd:           state.super.FileEnd,
			NextLogicalID:     state.root.NextLogicalID,
			AllocationQuantum: state.root.PageSize,
		},
	)
	leafLease.Release()
	return dst, found, err
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
		return nil
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
				return fmt.Errorf("vibejson: ordered primary catalog root shape")
			}
			cursor := catalog.LowerBound(nil)
			for {
				route, ok := cursor.Route()
				if !ok {
					return fmt.Errorf("vibejson: ordered primary catalog root cursor")
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
					return fmt.Errorf("vibejson: ordered primary catalog child level")
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
				return fmt.Errorf("vibejson: ordered primary catalog branch shape")
			}
			cursor := branch.LowerBound(nil)
			for {
				leafRoute, ok := cursor.Route()
				if !ok {
					return fmt.Errorf("vibejson: ordered primary catalog branch cursor")
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
				return fmt.Errorf("vibejson: ordered primary catalog leaf shape")
			}
			cursor := leaf.LowerBound(nil)
			for {
				tabletRoute, ok := cursor.Route()
				if !ok {
					return fmt.Errorf(
						"vibejson: ordered primary catalog leaf cursor",
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
		return fmt.Errorf("vibejson: open ordered primary tablet: %w", err)
	}
	tablet, err := storeio.OpenGlobalTabletCatalogTabletRoot(
		lease.Page(), ref, bounds,
	)
	locatorRef, locatorOK := tablet.LocatorRef()
	lease.Release()
	if err != nil {
		return fmt.Errorf("vibejson: open ordered primary tablet: %w", err)
	}
	if !locatorOK {
		return fmt.Errorf(
			"%w: ordered primary tablet locator",
			storeio.ErrGlobalTabletCatalogCorrupt,
		)
	}
	locatorLease, err := cache.Acquire(locatorRef)
	if err != nil {
		return fmt.Errorf("vibejson: open ordered primary locator: %w", err)
	}
	_, err = storeio.OpenGlobalTabletCatalogLocator(
		locatorLease.Page(), locatorRef, bounds,
	)
	locatorLease.Release()
	if err != nil {
		return fmt.Errorf("vibejson: open ordered primary locator: %w", err)
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
		return fmt.Errorf("vibejson: ordered primary catalog without page cache")
	}
	lease, err := cache.Acquire(ref)
	if err != nil {
		return fmt.Errorf("vibejson: open ordered primary catalog: %w", err)
	}
	defer lease.Release()
	node, err := storeio.OpenGlobalTabletCatalogNode(
		lease.Page(), ref, bounds,
	)
	if err != nil {
		return fmt.Errorf("vibejson: open ordered primary catalog: %w", err)
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
	// A compact leaf packs far more rows than a wide leaf holds raw, so its first
	// mutation de-compacts it into a leaf as large as the 64 KiB maximum leaf
	// extent before the structural path re-fits and splits it back into wide
	// leaves. The mutation scratch must therefore cover the largest leaf, not just
	// the wide class.
	c.primaryLeafScratch = make([]byte, storeio.CommonPrimaryLeafMaxExtentBytes)
	c.primaryRootScratch = make([]byte, storeio.SegmentedTabletRouterRootBytes)
	builtRouter, err := storeio.BuildResidentPrimaryRouter(
		c.cache, state.root.PrimaryRoot,
		storeio.GlobalTabletCatalogBounds{
			StoreID: state.root.StoreID, SelectedRootGeneration: state.root.Generation,
			FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
		},
	)
	if err != nil {
		return fmt.Errorf("vibejson: build resident primary router: %w", err)
	}
	c.primaryRouter.Store(builtRouter)
	// Both buffered-visible and the journal-backed synchronous lane apply through
	// the deferred canonical-frame path, so both need the pending parent and
	// volatile-retire scratch. Async-visible on a primary graph uses the committer
	// path and does not.
	if c.buffered() || c.synchronous() {
		pendingCapacity := min(
			c.options.MaxRetiredExtents, filePrimaryPendingParentLimit,
		)
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
	if err := c.openPrimaryExactIndexes(state); err != nil {
		return fmt.Errorf(
			"vibejson: open ordered-primary exact indexes: %w", err,
		)
	}
	return nil
}

// ResetPrimaryTemplateDiag zeroes the process-global template-columnar mutation
// diagnostics (de-template and re-plan event counters). It is a thin re-export
// of the storeio counter reset so out-of-tree diagnostics can attribute runtime
// cost without importing the internal package.
func ResetPrimaryTemplateDiag() { storeio.ResetTemplateColumnarDiag() }
