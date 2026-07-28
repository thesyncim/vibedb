package durable

import (
	"fmt"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson/x/byteview"
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
	key string,
) ([]byte, bool, error) {
	if c == nil || state == nil ||
		state.root.PrimaryRoot == (storeio.PageRef{}) {
		return dst, false, nil
	}
	keyBytes := byteview.Bytes(key)
	if len(keyBytes) == 0 ||
		len(keyBytes) > storeio.CommonPrimaryLeafMaxKeyBytes {
		return dst, false, nil
	}
	// The resident router is mutable collection-local acceleration for the
	// newest published generation. A snapshot whose rooted graph predates a
	// handle rewrite must retain its old physical route, so it uses the rooted
	// page-walk oracle instead.
	// Load the router pointer once: a structural split/merge/reclass swaps the
	// whole instance, so re-reading the field mid-read could observe two
	// generations. One consistent instance plus the generation guard keeps a
	// concurrent swap correct (a mismatch falls back to the rooted oracle).
	router := c.primaryRouter.Load()
	if router == nil ||
		router.Generation() != state.root.Generation {
		return c.resolvePrimaryGraphPageWalk(dst, state, key)
	}
	route, ok := router.Route(keyBytes)
	if !ok {
		return dst, false, fmt.Errorf(
			"%w: resident primary route",
			storeio.ErrSegmentedTabletRouterCorrupt,
		)
	}
	// Close the race in which the serialized writer advances the router after
	// the generation check but while this reader is selecting its handle.
	if router.Generation() != state.root.Generation {
		return c.resolvePrimaryGraphPageWalk(dst, state, key)
	}
	leafLease, err := router.AcquireLeaf(c.cache, route)
	if err != nil {
		return dst, false, err
	}
	dst, found, err := appendPrimaryLeafValue(
		dst, leafLease.Page(), state.root.StoreID, route.Bucket,
		route.Hash, keyBytes,
		storeio.CommonPrimaryLeafBounds{
			FileEnd:           state.super.FileEnd,
			NextLogicalID:     state.root.NextLogicalID,
			AllocationQuantum: state.root.PageSize,
		},
	)
	leafLease.Release()
	return dst, found, err
}

// appendPrimaryLeafValue reads one exact inline value from an admitted primary
// leaf, dispatching on the leaf class recorded in the page. Template leaves
// splice the exact JSON into dst; raw leaves append the borrowed inline bytes.
// It allocates nothing when dst has capacity for the value.
func appendPrimaryLeafValue(
	dst []byte,
	page []byte,
	storeID [16]byte,
	bucket storeio.BucketID,
	hash uint64,
	keyBytes []byte,
	bounds storeio.CommonPrimaryLeafBounds,
) ([]byte, bool, error) {
	if storeio.PrimaryLeafClass(page) == storeio.CommonPrimaryLeafTemplate {
		tv, ok := storeio.AdmittedCommonPrimaryTemplateLeaf(page, bucket, bounds)
		if !ok {
			return dst, false, fmt.Errorf(
				"%w: template primary leaf",
				storeio.ErrCommonPrimaryLeafCorrupt,
			)
		}
		out, found := tv.AppendRawByKey(dst, keyBytes)
		return out, found, nil
	}
	leaf := storeio.AdmittedCommonPrimaryLeaf(page, storeID, bucket, bounds)
	_, raw, overflow, found := leaf.LookupRawHashed(hash, keyBytes)
	if !found {
		return dst, false, nil
	}
	if overflow {
		return dst, false, fmt.Errorf(
			"%w: overflow ordered-primary value",
			ErrPrimaryCutoverUnsupported,
		)
	}
	return append(dst, raw...), true, nil
}

// resolvePrimaryGraphPageWalk is the rooted resolver retained both as a
// differential correctness oracle and as the production path for snapshots
// older than the mutable resident router's reflected generation.
func (c *Collection) resolvePrimaryGraphPageWalk(
	dst []byte,
	state *fileStoreState,
	key string,
) ([]byte, bool, error) {
	if c == nil || state == nil ||
		state.root.PrimaryRoot == (storeio.PageRef{}) {
		return dst, false, nil
	}
	keyBytes := byteview.Bytes(key)
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
	dst, found, err := appendPrimaryLeafValue(
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
