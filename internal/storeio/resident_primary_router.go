package storeio

import (
	"bytes"
	"fmt"
	"math/bits"
	"sync/atomic"
	"time"
)

const residentPrimaryRouterWords = 4

// ResidentPrimaryRouter is an allocation-free point router built from one
// published primary graph. Its persistent tree shares untouched routing blocks
// and coherent leaf-handle cells across structural images; its bucket index
// provides bounded stable-identity lookup without scanning the tree.
//
// generation is the state-root generation reflected by the mutable handles.
// A snapshot selecting an older generation must use the rooted page-walk
// resolver instead of this newest-generation acceleration.
type ResidentPrimaryRouter struct {
	storeID [16]byte
	// fences, rows, and empty are temporary graph-walk staging owned only while
	// BuildResidentPrimaryRouter constructs the persistent representation.
	fences          []byte
	rows            []uint64
	empty           []atomic.Uint32
	hints           []pageCacheFrameHint
	searchKeys      []uint64
	searchTops      []uint64
	version         atomic.Uint64
	buildNS         int64
	generation      atomic.Uint64
	tree            *residentRouteNode
	buckets         *residentBucketIndex
	treeBytes       int
	floorEntry      residentRouteEntry
	firstRealFence  []byte
	firstRealPacked uint64
}

// buildSearchKeys remains a staging-fixture compatibility hook. The
// persistent tree builds its packed routing summaries in buildPersistentTree.
func (r *ResidentPrimaryRouter) buildSearchKeys() {}

// pageCacheFrameHint is mutable cache-local acceleration beside the router's
// immutable routing payload. packed holds a one-based frame index in its low
// word and an exact-key-derived identity stamp in its high word.
type pageCacheFrameHint struct {
	packed atomic.Uint64
}

// ResidentPrimaryRoute is the exact leaf selection returned by the resident
// router. Hash is carried into the ordered-hash leaf lookup.
type ResidentPrimaryRoute struct {
	Ref    PageRef
	Bucket BucketID
	Hash   uint64
	rank   uint32
	cell   *residentRouteCell
}

// ResidentPrimaryTabletReplacement describes one complete tablet image for
// ReplaceTablets. Leaves use tablet-local floors; Floor is the tablet's global
// catalog floor and replaces the empty floor of Leaves[0].
type ResidentPrimaryTabletReplacement struct {
	TabletID uint32
	Floor    []byte
	Leaves   []SegmentedTabletRouterLeaf
}

// BuildResidentPrimaryRouter walks a fully validated published primary graph
// once and copies only its lexical fences and current leaf handles.
func BuildResidentPrimaryRouter(
	cache *PageCache,
	root PageRef,
	bounds GlobalTabletCatalogBounds,
) (*ResidentPrimaryRouter, error) {
	started := time.Now()
	if cache == nil || root == (PageRef{}) || !bounds.valid() {
		return nil, fmt.Errorf("%w: resident primary build bounds",
			ErrGlobalTabletCatalogCorrupt)
	}
	router := &ResidentPrimaryRouter{storeID: bounds.StoreID}
	if err := router.walkCatalog(cache, root, bounds, nil); err != nil {
		return nil, err
	}
	if router.Len() == 0 || len(router.fence(0)) != 0 {
		return nil, fmt.Errorf("%w: empty resident primary router",
			ErrGlobalTabletCatalogCorrupt)
	}
	router.empty = make([]atomic.Uint32, len(router.rows)/residentPrimaryRouterWords)
	router.buildPersistentTree()
	if router.buckets == nil {
		return nil, fmt.Errorf("%w: resident bucket index", ErrGlobalTabletCatalogCorrupt)
	}
	router.fences = nil
	router.rows = nil
	router.empty = nil
	router.generation.Store(bounds.SelectedRootGeneration)
	router.buildNS = time.Since(started).Nanoseconds()
	return router, nil
}

// packLexicalWindow packs up to eight bytes big-endian with zero padding, so
// unsigned comparison of two windows matches bytes.Compare whenever the
// windows differ; equal windows require the exact compare.
func packLexicalWindow(b []byte) uint64 {
	var window uint64
	limit := min(len(b), 8)
	for at := 0; at < limit; at++ {
		window |= uint64(b[at]) << (56 - 8*at)
	}
	return window
}

func (r *ResidentPrimaryRouter) walkCatalog(
	cache *PageCache,
	ref PageRef,
	bounds GlobalTabletCatalogBounds,
	inheritedFloor []byte,
) error {
	lease, err := cache.Acquire(ref)
	if err != nil {
		return err
	}
	node := AdmittedGlobalTabletCatalogNode(lease.Page(), bounds)
	cursor := node.LowerBound(nil)
	for ordinal := 0; ; ordinal++ {
		route, ok := cursor.Route()
		if !ok {
			lease.Release()
			return fmt.Errorf("%w: resident catalog cursor",
				ErrGlobalTabletCatalogCorrupt)
		}
		floor := inheritedFloor
		if ordinal != 0 {
			common, prefix, suffix := node.floors.fenceParts(ordinal - 1)
			floor = make([]byte, 0, len(common)+len(prefix)+len(suffix))
			floor = append(floor, common...)
			floor = append(floor, prefix...)
			floor = append(floor, suffix...)
		}
		switch node.Level() {
		case GlobalTabletCatalogLeaf:
			if err := r.walkTablet(cache, route.Ref, bounds, floor); err != nil {
				lease.Release()
				return err
			}
		case GlobalTabletCatalogRoot, GlobalTabletCatalogBranch:
			if err := r.walkCatalog(cache, route.Ref, bounds, floor); err != nil {
				lease.Release()
				return err
			}
		default:
			lease.Release()
			return fmt.Errorf("%w: resident catalog level",
				ErrGlobalTabletCatalogCorrupt)
		}
		if !cursor.Next() {
			break
		}
	}
	lease.Release()
	return nil
}

func (r *ResidentPrimaryRouter) walkTablet(
	cache *PageCache,
	ref PageRef,
	bounds GlobalTabletCatalogBounds,
	tabletFloor []byte,
) error {
	tabletLease, err := cache.Acquire(ref)
	if err != nil {
		return err
	}
	tablet := AdmittedGlobalTabletCatalogTabletRoot(tabletLease.Page(), bounds)
	leafRank := 0
	for anchorRank := 0; anchorRank < tablet.AnchorCount(); anchorRank++ {
		anchorRoute, ok := tablet.AnchorAt(anchorRank)
		if !ok {
			tabletLease.Release()
			return fmt.Errorf("%w: resident anchor route",
				ErrGlobalTabletCatalogCorrupt)
		}
		anchorLease, acquireErr := cache.Acquire(anchorRoute.Ref)
		if acquireErr != nil {
			tabletLease.Release()
			return acquireErr
		}
		anchor := AdmittedGlobalTabletCatalogAnchor(
			anchorLease.Page(), &tablet, anchorRoute.PageID,
		)
		for rank := 0; rank < anchor.Count(); rank++ {
			route, routeOK := anchor.RouteAt(rank, 0)
			fence, fenceOK := anchor.page.fenceAtChecked(rank)
			if !routeOK || !fenceOK {
				anchorLease.Release()
				tabletLease.Release()
				return fmt.Errorf("%w: resident anchor row",
					ErrSegmentedTabletRouterCorrupt)
			}
			var fenceBytes uint64
			if leafRank == 0 {
				fenceBytes = uint64(len(tabletFloor))
			} else {
				var sizeOK bool
				fenceBytes, sizeOK = checkedSizeAdd(
					uint64(len(fence.a)),
					uint64(len(fence.b)),
					uint64(^uint32(0)),
				)
				if sizeOK {
					fenceBytes, sizeOK = checkedSizeAdd(
						fenceBytes,
						uint64(len(fence.c)),
						uint64(^uint32(0)),
					)
				}
				if !sizeOK {
					anchorLease.Release()
					tabletLease.Release()
					return fmt.Errorf("%w: resident fence arena",
						ErrSegmentedTabletRouterCorrupt)
				}
			}
			fenceLimit := uint64(^uint32(0))
			if intLimit := uint64(maxIntValue); intLimit < fenceLimit {
				fenceLimit = intLimit
			}
			if _, sizeOK := checkedSizeAdd(
				uint64(len(r.fences)), fenceBytes, fenceLimit,
			); !sizeOK {
				anchorLease.Release()
				tabletLease.Release()
				return fmt.Errorf("%w: resident fence arena",
					ErrSegmentedTabletRouterCorrupt)
			}
			start := len(r.fences)
			if leafRank == 0 {
				r.fences = append(r.fences, tabletFloor...)
			} else {
				r.fences = append(r.fences, fence.a...)
				r.fences = append(r.fences, fence.b...)
				r.fences = append(r.fences, fence.c...)
			}
			staged := len(r.rows) / residentPrimaryRouterWords
			if staged != 0 &&
				bytes.Compare(r.flatFence(staged-1), r.fences[start:]) >= 0 {
				anchorLease.Release()
				tabletLease.Release()
				return fmt.Errorf("%w: resident fence order",
					ErrSegmentedTabletRouterCorrupt)
			}
			r.rows = append(r.rows,
				uint64(uint32(start))|uint64(uint32(len(r.fences)))<<32,
				route.Ref.Offset,
				route.Ref.Generation,
				uint64(route.Ref.Length)|uint64(uint32(route.Bucket))<<32,
			)
			leafRank++
		}
		anchorLease.Release()
	}
	tabletLease.Release()
	if leafRank == 0 {
		return fmt.Errorf("%w: resident empty tablet",
			ErrSegmentedTabletRouterCorrupt)
	}
	return nil
}

// Route hashes key once, confirms its exact lexical interval, and returns the
// current leaf handle. It allocates no memory.
func (r *ResidentPrimaryRouter) Route(key []byte) (ResidentPrimaryRoute, bool) {
	if r == nil || r.tree == nil || r.Len() == 0 {
		return ResidentPrimaryRoute{}, false
	}
	hash := KeyHashBytes(r.storeID, key)
	packed := packLexicalWindow(key)
	if packed < r.firstRealPacked || packed == r.firstRealPacked && bytes.Compare(key, r.firstRealFence) < 0 {
		return residentCellRoute(r.floorEntry, 0, hash)
	}
	entry, rank, ok := r.tree.floor(key)
	if !ok {
		return ResidentPrimaryRoute{}, false
	}
	return residentCellRoute(entry, rank, hash)
}

// RouteAtRank returns the leaf route stored at one router row, reading its
// current mutable handle under the version seqlock. rank is the lexical row
// ordinal, not a bucket identity, so it is stable regardless of local-ID
// contiguity. It is the ordered-enumeration entry the exact-index build and
// live-slot derivation walk every leaf through.
func (r *ResidentPrimaryRouter) RouteAtRank(rank int) (ResidentPrimaryRoute, bool) {
	if r == nil || r.tree == nil || rank < 0 || rank >= r.Len() {
		return ResidentPrimaryRoute{}, false
	}
	entry, ok := r.tree.at(rank)
	if !ok {
		return ResidentPrimaryRoute{}, false
	}
	return residentCellRoute(entry, rank, 0)
}

// ResolveBucketID returns the leaf route for one stable BucketID through the
// immutable radix index, then derives its current lexical rank from the tree.
func (r *ResidentPrimaryRouter) ResolveBucketID(
	bucket BucketID,
) (ResidentPrimaryRoute, bool) {
	if r == nil {
		return ResidentPrimaryRoute{}, false
	}
	if r.buckets != nil {
		entry, ok := r.buckets.lookup(bucket)
		if !ok {
			return ResidentPrimaryRoute{}, false
		}
		_, rank, ok := r.tree.floor(entry.fence)
		if !ok {
			return ResidentPrimaryRoute{}, false
		}
		current, ok := r.tree.at(rank)
		if !ok || current.cell != entry.cell {
			return ResidentPrimaryRoute{}, false
		}
		return residentCellRoute(entry, rank, 0)
	}
	return ResidentPrimaryRoute{}, false
}

// ResolveBucketFloor returns one coherent mutable leaf handle together with
// its immutable lexical floor. A snapshot may use the handle directly when
// its generation is not newer than the captured state; otherwise the floor is
// the coordinate for a rooted fallback.
func (r *ResidentPrimaryRouter) ResolveBucketFloor(
	bucket BucketID,
) (ResidentPrimaryRoute, []byte, bool) {
	route, ok := r.ResolveBucketID(bucket)
	if !ok {
		return ResidentPrimaryRoute{}, nil, false
	}
	return route, r.fence(int(route.rank)), true
}

// Generation reports the state-root generation represented by the current
// mutable leaf handles.
func (r *ResidentPrimaryRouter) Generation() uint64 {
	if r == nil {
		return 0
	}
	return r.generation.Load()
}

// CanUpdateLeaf validates one serialized-writer, non-structural handle update
// before the corresponding write transaction is published.
func (r *ResidentPrimaryRouter) CanUpdateLeaf(
	route ResidentPrimaryRoute,
	next PageRef,
	generation uint64,
) bool {
	if r == nil || route.cell == nil || int(route.rank) >= r.Len() ||
		generation <= r.Generation() ||
		next == (PageRef{}) || next.Kind != PagePrimaryLeaf ||
		next.Generation > generation {
		return false
	}
	if route.cell != nil {
		entry, member := r.tree.at(int(route.rank))
		if !member || entry.cell != route.cell {
			return false
		}
		current, ok := residentCellRoute(entry, int(route.rank), route.Hash)
		return ok && current.Ref == route.Ref && current.Bucket == route.Bucket && next.LogicalID == route.Ref.LogicalID
	}
	at := int(route.rank) * residentPrimaryRouterWords
	meta := atomic.LoadUint64(&r.rows[at+3])
	bucket := BucketID(uint32(meta >> 32))
	logicalID, ok := CommonPrimaryLeafLogicalID(bucket)
	return ok && bucket == route.Bucket &&
		logicalID == route.Ref.LogicalID &&
		next.LogicalID == logicalID
}

// UpdateLeaf installs one already-validated COW handle and advances the
// reflected state generation. The collection's single writer calls this after
// committer admission and before publishing the matching visible state.
func (r *ResidentPrimaryRouter) UpdateLeaf(
	route ResidentPrimaryRoute,
	next PageRef,
	generation uint64,
) {
	if r == nil || route.cell == nil {
		return
	}
	if route.cell != nil {
		meta := uint64(next.Length) | uint64(uint32(route.Bucket))<<32
		route.cell.seq.Add(1)
		route.cell.offset.Store(next.Offset)
		route.cell.generation.Store(next.Generation)
		route.cell.meta.Store(meta)
		route.cell.seq.Add(1)
		route.cell.hint.packed.Store(0)
		r.generation.Store(generation)
		return
	}
	at := int(route.rank) * residentPrimaryRouterWords
	meta := uint64(next.Length) | uint64(uint32(route.Bucket))<<32
	r.version.Add(1)
	atomic.StoreUint64(&r.rows[at+1], next.Offset)
	atomic.StoreUint64(&r.rows[at+2], next.Generation)
	atomic.StoreUint64(&r.rows[at+3], meta)
	r.generation.Store(generation)
	r.version.Add(1)
	r.hints[route.rank].packed.Store(0)
}

// SplitLeaf path-copies the affected routing block and its ancestors. It does
// no page-cache acquisition or graph walk, and the old image remains valid.
func (r *ResidentPrimaryRouter) SplitLeaf(
	route ResidentPrimaryRoute,
	leftRef PageRef,
	rightBucket BucketID,
	rightFence []byte,
	rightRef PageRef,
	generation uint64,
) (*ResidentPrimaryRouter, error) {
	started := time.Now()
	if r == nil || int(route.rank) >= r.Len() || len(rightFence) == 0 ||
		generation <= r.Generation() || generation >= uint64(1)<<48 ||
		segmentedTabletRouterValidateLeafRef(leftRef, route.Bucket, PagePrimaryLeaf, generation) != nil ||
		segmentedTabletRouterValidateLeafRef(rightRef, rightBucket, PagePrimaryLeaf, generation) != nil {
		return nil, fmt.Errorf("%w: resident split identity", ErrInvalidWrite)
	}
	current, ok := r.RouteAtRank(int(route.rank))
	if !ok || current.Ref != route.Ref || current.Bucket != route.Bucket ||
		bytes.Compare(r.fence(int(route.rank)), rightFence) >= 0 ||
		int(route.rank)+1 < r.Len() && bytes.Compare(rightFence, r.fence(int(route.rank)+1)) >= 0 {
		return nil, fmt.Errorf("%w: resident split route", ErrInvalidWrite)
	}
	if _, exists := r.buckets.lookup(rightBucket); exists {
		return nil, fmt.Errorf("%w: resident split bucket", ErrInvalidWrite)
	}
	if uint64(len(r.fences)) > uint64(maxIntValue-len(rightFence)) ||
		r.Len() == maxIntValue/residentPrimaryRouterWords {
		return nil, fmt.Errorf("%w: resident split capacity", ErrInvalidWrite)
	}
	left := newResidentRouteEntry(r.fence(int(route.rank)), leftRef, route.Bucket)
	right := newResidentRouteEntry(rightFence, rightRef, rightBucket)
	next := r.replacePersistent(int(route.rank), []residentRouteEntry{left, right}, generation)
	next.buildNS = time.Since(started).Nanoseconds()
	return next, nil
}

// SplitLeafPartition builds the next immutable resident routing image by
// replacing one leaf with several already validated lexical leaves. The old
// router remains valid for readers that loaded it before the collection's
// atomic pointer swap; this method performs no page-cache acquisition or graph
// walk.
func (r *ResidentPrimaryRouter) SplitLeafPartition(
	route ResidentPrimaryRoute,
	replacements []SegmentedTabletRouterLeaf,
	generation uint64,
) (*ResidentPrimaryRouter, error) {
	started := time.Now()
	if r == nil || int(route.rank) >= r.Len() || len(replacements) < 2 ||
		generation <= r.Generation() || generation >= uint64(1)<<48 {
		return nil, fmt.Errorf("%w: resident partition identity", ErrInvalidWrite)
	}
	sourceRank := int(route.rank)
	current, ok := r.RouteAtRank(sourceRank)
	if !ok || current.Ref != route.Ref || current.Bucket != route.Bucket {
		return nil, fmt.Errorf("%w: resident partition route", ErrInvalidWrite)
	}
	tabletID, sourceLocalID, ok := SplitTabletLocalIdentityBucket(
		uint32(route.Bucket),
	)
	if !ok || replacements[0].LocalID != uint16(sourceLocalID) {
		return nil, fmt.Errorf("%w: resident partition source", ErrInvalidWrite)
	}
	residentSourceFence := r.fence(sourceRank)
	// Build one tablet-local identity set for the existing router. Checking the
	// whole resident slice once keeps the K-way validation bounded by O(N+K),
	// rather than rescanning every resident leaf for each replacement.
	var used [TabletLocalIdentityLocalCount / 64]uint64
	selectedTabletHasPriorLeaf := false
	if locals, found := r.buckets.tabletLocals(tabletID); found {
		used = locals.words
		used[sourceLocalID>>6] &^= uint64(1) << (sourceLocalID & 63)
		for rank := sourceRank - 1; rank >= 0; rank-- {
			old, _ := r.RouteAtRank(rank)
			oldTablet, _, valid := SplitTabletLocalIdentityBucket(uint32(old.Bucket))
			if valid && oldTablet == tabletID {
				selectedTabletHasPriorLeaf = true
			}
			break
		}
	}
	// A tablet's first persistent leaf has an empty local floor, while the
	// resident router stores the catalog's global tablet floor. Preserve the
	// resident floor in the global splice and accept an empty replacement floor
	// only when this is the selected tablet's first resident leaf. Non-first
	// local fences must already equal their global resident fence.
	if len(replacements[0].Fence) != 0 {
		if !bytes.Equal(replacements[0].Fence, residentSourceFence) {
			return nil, fmt.Errorf("%w: resident partition source fence", ErrInvalidWrite)
		}
	} else if selectedTabletHasPriorLeaf {
		return nil, fmt.Errorf("%w: resident partition local floor", ErrInvalidWrite)
	}
	previous := residentSourceFence
	for rank, replacement := range replacements {
		globalFence := replacement.Fence
		if rank == 0 {
			globalFence = residentSourceFence
		}
		if replacement.LocalID >= TabletLocalIdentityLocalCount ||
			replacement.Ref.Generation != generation {
			return nil, fmt.Errorf("%w: resident partition leaf", ErrInvalidWrite)
		}
		bucket, bucketOK := MakeTabletLocalIdentityBucket(
			tabletID, uint32(replacement.LocalID),
		)
		if !bucketOK ||
			segmentedTabletRouterValidateLeafRef(
				replacement.Ref, BucketID(bucket), PagePrimaryLeaf, generation,
			) != nil {
			return nil, fmt.Errorf("%w: resident partition leaf", ErrInvalidWrite)
		}
		if rank > 0 {
			if len(replacement.Fence) == 0 ||
				bytes.Compare(previous, globalFence) >= 0 {
				return nil, fmt.Errorf("%w: resident partition fence", ErrInvalidWrite)
			}
		}
		if rank == 0 && replacement.LocalID != uint16(sourceLocalID) {
			return nil, fmt.Errorf("%w: resident partition source", ErrInvalidWrite)
		}
		word, bit := replacement.LocalID>>6, uint64(1)<<(replacement.LocalID&63)
		if used[word]&bit != 0 {
			return nil, fmt.Errorf("%w: resident partition LocalID", ErrInvalidWrite)
		}
		used[word] |= bit
		previous = globalFence
	}
	if sourceRank+1 < r.Len() &&
		bytes.Compare(previous, r.fence(sourceRank+1)) >= 0 {
		return nil, fmt.Errorf("%w: resident partition fence", ErrInvalidWrite)
	}
	newLen := r.Len() - 1 + len(replacements)
	if newLen <= 0 || newLen > maxIntValue/residentPrimaryRouterWords {
		return nil, fmt.Errorf("%w: resident partition capacity", ErrInvalidWrite)
	}
	repl := make([]residentRouteEntry, len(replacements))
	for i, x := range replacements {
		bucket, _ := MakeTabletLocalIdentityBucket(tabletID, uint32(x.LocalID))
		f := x.Fence
		if i == 0 {
			f = residentSourceFence
		}
		repl[i] = newResidentRouteEntry(f, x.Ref, BucketID(bucket))
	}
	next := r.replacePersistent(sourceRank, repl, generation)
	next.buildNS = time.Since(started).Nanoseconds()
	return next, nil
}

// RemoveLeaf builds the next immutable routing image by splicing one already
// published structural leaf removal into this router. The selected leaf's
// interval falls through to its lexical predecessor. Removing the global
// first leaf instead promotes its successor to the empty negative-infinity
// floor. The old router remains valid for concurrent readers.
func (r *ResidentPrimaryRouter) RemoveLeaf(
	route ResidentPrimaryRoute,
	generation uint64,
) (*ResidentPrimaryRouter, error) {
	started := time.Now()
	if r == nil || r.Len() <= 1 || int(route.rank) >= r.Len() ||
		generation <= r.Generation() || generation >= uint64(1)<<48 {
		return nil, fmt.Errorf("%w: resident remove identity", ErrInvalidWrite)
	}
	current, ok := r.RouteAtRank(int(route.rank))
	if !ok || current.Ref != route.Ref || current.Bucket != route.Bucket {
		return nil, fmt.Errorf("%w: resident remove route", ErrInvalidWrite)
	}
	if route.rank == 0 {
		successor, _ := r.tree.at(1)
		replacement := residentRouteEntry{fence: nil, cell: successor.cell}
		next := r.replacePersistentRange(0, 2, []residentRouteEntry{replacement}, generation)
		next.buildNS = time.Since(started).Nanoseconds()
		return next, nil
	}
	next := r.replacePersistent(int(route.rank), nil, generation)
	next.buildNS = time.Since(started).Nanoseconds()
	return next, nil
}

// ReplaceTablets atomically constructs a new routing image by replacing every
// route belonging to oldTabletID with one or more complete tablet images. Its
// work is bounded by the tablet identity space and the replacement leaves;
// routes outside that lexical range are structurally shared.
func (r *ResidentPrimaryRouter) ReplaceTablets(oldTabletID uint32, tablets []ResidentPrimaryTabletReplacement, generation uint64) (*ResidentPrimaryRouter, error) {
	started := time.Now()
	if r == nil || r.tree == nil || len(tablets) == 0 || generation <= r.Generation() || generation >= uint64(1)<<48 {
		return nil, fmt.Errorf("%w: resident tablet replacement", ErrInvalidWrite)
	}
	locals, ok := r.buckets.tabletLocals(oldTabletID)
	if !ok {
		return nil, fmt.Errorf("%w: resident tablet missing", ErrInvalidWrite)
	}
	start, end := r.Len(), -1
	for local := uint32(0); local < TabletLocalIdentityLocalCount; local++ {
		if !locals.contains(local) {
			continue
		}
		bucket, _ := MakeTabletLocalIdentityBucket(oldTabletID, local)
		entry, found := r.buckets.lookup(BucketID(bucket))
		if !found {
			return nil, fmt.Errorf("%w: resident tablet index", ErrInvalidWrite)
		}
		_, rank, found := r.tree.floor(entry.fence)
		if !found {
			return nil, fmt.Errorf("%w: resident tablet rank", ErrInvalidWrite)
		}
		start, end = min(start, rank), max(end, rank)
	}
	if end < start || end-start+1 != residentTabletLocalCount(locals) {
		return nil, fmt.Errorf("%w: resident tablet range", ErrInvalidWrite)
	}
	repl := make([]residentRouteEntry, 0)
	seenTablets := make(map[uint32]struct{}, len(tablets))
	seenBuckets := make(map[BucketID]struct{})
	for _, tablet := range tablets {
		if _, exists := seenTablets[tablet.TabletID]; exists || len(tablet.Leaves) == 0 {
			return nil, fmt.Errorf("%w: resident replacement tablet", ErrInvalidWrite)
		}
		seenTablets[tablet.TabletID] = struct{}{}
		if tablet.TabletID != oldTabletID {
			if _, exists := r.buckets.tabletLocals(tablet.TabletID); exists {
				return nil, fmt.Errorf("%w: resident replacement tablet exists", ErrInvalidWrite)
			}
		}
		floor := tablet.Floor
		if tablet.TabletID == oldTabletID && floor == nil {
			floor = r.fence(start)
		}
		for i, leaf := range tablet.Leaves {
			if i == 0 && len(leaf.Fence) != 0 {
				return nil, fmt.Errorf("%w: resident replacement floor", ErrInvalidWrite)
			}
			bucket, made := MakeTabletLocalIdentityBucket(tablet.TabletID, uint32(leaf.LocalID))
			if !made || segmentedTabletRouterValidateLeafRef(leaf.Ref, BucketID(bucket), PagePrimaryLeaf, generation) != nil {
				return nil, fmt.Errorf("%w: resident replacement leaf", ErrInvalidWrite)
			}
			bucketID := BucketID(bucket)
			if _, duplicate := seenBuckets[bucketID]; duplicate {
				return nil, fmt.Errorf("%w: resident replacement bucket", ErrInvalidWrite)
			}
			seenBuckets[bucketID] = struct{}{}
			if _, exists := r.buckets.lookup(bucketID); exists && tablet.TabletID != oldTabletID {
				return nil, fmt.Errorf("%w: resident replacement bucket", ErrInvalidWrite)
			}
			fence := leaf.Fence
			if i == 0 {
				fence = floor
			}
			if len(repl) != 0 && bytes.Compare(repl[len(repl)-1].fence, fence) >= 0 {
				return nil, fmt.Errorf("%w: resident replacement order", ErrInvalidWrite)
			}
			repl = append(repl, newResidentRouteEntry(fence, leaf.Ref, bucketID))
		}
	}
	if !bytes.Equal(repl[0].fence, r.fence(start)) {
		return nil, fmt.Errorf("%w: resident replacement first floor", ErrInvalidWrite)
	}
	if start > 0 && bytes.Compare(r.fence(start-1), repl[0].fence) >= 0 || end+1 < r.Len() && bytes.Compare(repl[len(repl)-1].fence, r.fence(end+1)) >= 0 {
		return nil, fmt.Errorf("%w: resident replacement bounds", ErrInvalidWrite)
	}
	next := r.replacePersistentRange(start, end-start+1, repl, generation)
	next.buildNS = time.Since(started).Nanoseconds()
	return next, nil
}

func residentTabletLocalCount(set *residentTabletLocalSet) int {
	count := 0
	for _, word := range set.words {
		count += bits.OnesCount64(word)
	}
	return count
}

// NextTabletID returns the first never-issued monotonic tablet identity above
// every tablet represented by this immutable router. Durable reopen rebuilds
// the same high-water directly from the authenticated graph, so a separate
// mutable allocator record is unnecessary and crashed structural attempts do
// not make an identity reusable.
func (r *ResidentPrimaryRouter) NextTabletID() (uint32, bool) {
	if r == nil || r.Len() == 0 {
		return 0, false
	}
	maximum, ok := r.buckets.max()
	if !ok {
		return 0, false
	}
	high, _, ok := SplitTabletLocalIdentityBucket(uint32(maximum))
	if !ok {
		return 0, false
	}
	if high+1 >= TabletLocalIdentityTabletCount {
		return 0, false
	}
	return high + 1, true
}

// AdvanceGeneration records a canonical-frame mutation whose stable leaf
// handle did not change.
func (r *ResidentPrimaryRouter) AdvanceGeneration(generation uint64) {
	// version is exclusively the row-handle seqlock. No ref word changes here,
	// so perturbing it would make concurrent routes retry without protecting any
	// additional state; readers that require a stable generation sample the
	// independent generation atomic explicitly.
	r.generation.Store(generation)
}

// MarkEmpty records one phase-7 empty leaf for session-local accounting.
func (r *ResidentPrimaryRouter) MarkEmpty(
	route ResidentPrimaryRoute,
) bool {
	if r != nil && route.cell != nil {
		entry, ok := r.tree.at(int(route.rank))
		if !ok || entry.cell != route.cell {
			return false
		}
		return route.cell.empty.CompareAndSwap(0, 1)
	}
	return r != nil && int(route.rank) < len(r.empty) &&
		r.empty[route.rank].CompareAndSwap(0, 1)
}

// ClearEmpty clears a session-local empty marker when an insertion refills the
// same routed leaf.
func (r *ResidentPrimaryRouter) ClearEmpty(
	route ResidentPrimaryRoute,
) bool {
	if r != nil && route.cell != nil {
		entry, ok := r.tree.at(int(route.rank))
		if !ok || entry.cell != route.cell {
			return false
		}
		return route.cell.empty.CompareAndSwap(1, 0)
	}
	return r != nil && int(route.rank) < len(r.empty) &&
		r.empty[route.rank].CompareAndSwap(1, 0)
}

// AcquireLeaf pins route's selected leaf, consulting its per-router frame hint
// before the cache table. A stale hint is harmless: PageCache rechecks the
// complete PageRef identity while holding the frame's existing pin lock.
func (r *ResidentPrimaryRouter) AcquireLeaf(
	cache *PageCache,
	route ResidentPrimaryRoute,
) (PageLease, error) {
	if cache == nil {
		return PageLease{}, ErrPageCacheReference
	}
	if route.cell != nil {
		return cache.acquireFrameHinted(route.Ref, &route.cell.hint)
	}
	if r == nil || int(route.rank) >= len(r.hints) {
		return cache.Acquire(route.Ref)
	}
	return cache.acquireFrameHinted(route.Ref, &r.hints[route.rank])
}

func (r *ResidentPrimaryRouter) fence(rank int) []byte {
	if r.tree != nil {
		e, ok := r.tree.at(rank)
		if ok {
			return e.fence
		}
		return nil
	}
	word := r.rows[rank*residentPrimaryRouterWords]
	return r.fences[uint32(word):uint32(word>>32)]
}

func (r *ResidentPrimaryRouter) Len() int {
	if r == nil {
		return 0
	}
	if r.tree != nil {
		return r.tree.total
	}
	return len(r.rows) / residentPrimaryRouterWords
}

// ResidentBytes estimates this image's logical footprint, including reachable
// tree and bucket-index nodes plus cell and fence payloads. It excludes unused
// capacity retained by shared arenas and images held separately by snapshots.
func (r *ResidentPrimaryRouter) ResidentBytes() int {
	if r == nil {
		return 0
	}
	if r.tree != nil {
		return r.treeBytes + r.buckets.retainedBytes()
	}
	limit := uint64(maxIntValue)
	total := uint64(cap(r.fences))
	terms := [5][2]uint64{
		{uint64(cap(r.rows)), 8},
		{uint64(cap(r.hints)), 8},
		{uint64(cap(r.searchKeys)), 8},
		{uint64(cap(r.searchTops)), 8},
		{uint64(cap(r.empty)), 4},
	}
	for _, term := range terms {
		bytes, ok := checkedSizeMul(term[0], term[1], limit)
		if ok {
			total, ok = checkedSizeAdd(total, bytes, limit)
		}
		if !ok {
			return maxIntValue
		}
	}
	return int(total)
}

// BuildDuration reports the wall time spent walking and packing the graph,
// including PageCache acquisitions made by the Open-time build.
func (r *ResidentPrimaryRouter) BuildDuration() time.Duration {
	if r == nil {
		return 0
	}
	return time.Duration(r.buildNS)
}
