package storeio

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"time"
)

const residentPrimaryRouterWords = 4

// ResidentPrimaryRouter is an allocation-free point router built from one
// published primary graph. Fences and bucket identities are immutable. The
// serialized collection writer may replace one leaf handle after publishing a
// non-structural COW generation; version makes the three atomic handle words
// one coherent reader sample. Each leaf occupies four packed words: fence
// bounds, physical offset, generation, and length/bucket identity. Fences share
// one byte arena. Logical IDs and page kind are derived rather than repeated.
//
// generation is the state-root generation reflected by the mutable handles.
// A snapshot selecting an older generation must use the rooted page-walk
// resolver instead of this newest-generation acceleration.
type ResidentPrimaryRouter struct {
	storeID [16]byte
	fences  []byte
	rows    []uint64
	hints   []pageCacheFrameHint
	empty   []atomic.Uint32
	// searchKeys accelerates the fence binary search: one big-endian packed
	// word per rank holding the first eight fence bytes past searchSkip, the
	// prefix every routed fence shares. A probe is then one integer compare
	// instead of a bytes.Compare against a cold fence-arena line; only packed
	// equality falls back to the exact bytes. Zero-padding is order-safe
	// because zero is the minimum byte: strict packed inequality always
	// matches strict lexical order, and ties defer to the full compare.
	// Immutable after build, like the fences it summarizes: UpdateLeaf swaps
	// handles, never fences. Costs 8 bytes per leaf, bought against the
	// measured ~90ns per point read the byte-wise search cost at 100k keys.
	searchKeys []uint64
	// searchTops holds every searchTopGroup'th packed window (rank 1, 1+g,
	// 1+2g, ...). A point lookup binary-searches this small dense array first
	// — it stays L1-resident under random probing where the full searchKeys
	// span does not — and finishes inside one eight-word group, cutting the
	// search's cache-missing probes from log2(leaves) to three.
	searchTops []uint64
	searchSkip int
	buildNS    int64
	generation atomic.Uint64
	version    atomic.Uint64
}

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
	router.hints = make([]pageCacheFrameHint, router.Len())
	router.empty = make([]atomic.Uint32, router.Len())
	router.buildSearchKeys()
	router.generation.Store(bounds.SelectedRootGeneration)
	router.buildNS = time.Since(started).Nanoseconds()
	return router, nil
}

// buildSearchKeys derives the shared fence prefix and packs each fence's next
// eight bytes. Rank zero's fence is the empty floor and is never probed, so
// the prefix is the common prefix of the first and last real fences; lexical
// ordering guarantees every fence between them shares it and is at least that
// long (a shorter fence would be a proper prefix and sort before rank one).
func (r *ResidentPrimaryRouter) buildSearchKeys() {
	count := r.Len()
	r.searchKeys = make([]uint64, count)
	if count < 2 {
		return
	}
	first := r.fence(1)
	last := r.fence(count - 1)
	skip := 0
	for skip < len(first) && skip < len(last) && first[skip] == last[skip] {
		skip++
	}
	r.searchSkip = skip
	for rank := 1; rank < count; rank++ {
		r.searchKeys[rank] = packLexicalWindow(r.fence(rank)[skip:])
	}
	groups := (count - 2 + searchTopGroup) / searchTopGroup
	r.searchTops = make([]uint64, groups)
	for group := range groups {
		r.searchTops[group] = r.searchKeys[1+group*searchTopGroup]
	}
}

// searchTopGroup is eight packed windows — exactly one cache line — so the
// second-level search after the top-index narrowing touches one line.
const searchTopGroup = 8

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
			if r.Len() != 0 &&
				bytes.Compare(r.fence(r.Len()-1), r.fences[start:]) >= 0 {
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
	if r == nil || len(r.rows) == 0 {
		return ResidentPrimaryRoute{}, false
	}
	hash := KeyHashBytes(r.storeID, key)
	rank := r.searchRank(key)
	if rank < 0 || bytes.Compare(r.fence(rank), key) > 0 ||
		rank+1 < r.Len() && bytes.Compare(key, r.fence(rank+1)) >= 0 {
		return ResidentPrimaryRoute{}, false
	}
	at := rank * residentPrimaryRouterWords
	return r.routeAtRankHashed(at, rank, hash)
}

// searchRank finds the last fence at or below key. The exact full-byte
// interval check in Route re-verifies the answer, so a packed-search defect
// can only surface as a routing miss, never a wrong leaf.
func (r *ResidentPrimaryRouter) searchRank(key []byte) int {
	count := r.Len()
	if count < 2 {
		return 0
	}
	skip := r.searchSkip
	if skip != 0 {
		prefix := r.fence(1)[:skip]
		head := key
		if len(head) > skip {
			head = head[:skip]
		}
		if c := bytes.Compare(head, prefix); c != 0 {
			if c < 0 {
				// Below every real fence; the empty rank-zero floor holds it.
				return 0
			}
			return count - 1
		}
		if len(key) < skip {
			// key is a proper prefix of the shared fence prefix: below rank 1.
			return 0
		}
	}
	window := packLexicalWindow(key[skip:])
	// Level one: the predicate fence(rank) <= key is monotone in rank, so a
	// binary search over the group heads and one in-group finish computes the
	// same rank the flat search did; only the memory it touches changes.
	lowGroup, highGroup := 0, len(r.searchTops)
	for lowGroup < highGroup {
		middle := int(uint(lowGroup+highGroup) >> 1)
		if r.fenceBelowOrEqual(1+middle*searchTopGroup, window, key) {
			lowGroup = middle + 1
		} else {
			highGroup = middle
		}
	}
	if lowGroup == 0 {
		// key sits below the first real fence; the empty floor routes it.
		return 0
	}
	low := 1 + (lowGroup-1)*searchTopGroup
	high := min(low+searchTopGroup, count)
	for low < high {
		middle := int(uint(low+high) >> 1)
		if r.fenceBelowOrEqual(middle, window, key) {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low - 1
}

// fenceBelowOrEqual reports fence(rank) <= key using the packed window and
// falling back to exact bytes only on packed equality, where zero-padded
// big-endian packing cannot order the pair.
func (r *ResidentPrimaryRouter) fenceBelowOrEqual(
	rank int, window uint64, key []byte,
) bool {
	packed := r.searchKeys[rank]
	if packed != window {
		return packed < window
	}
	return bytes.Compare(r.fence(rank), key) <= 0
}

func (r *ResidentPrimaryRouter) routeAtRankHashed(
	at, rank int, hash uint64,
) (ResidentPrimaryRoute, bool) {
	for {
		before := r.version.Load()
		if before&1 != 0 {
			continue
		}
		offset := atomic.LoadUint64(&r.rows[at+1])
		generation := atomic.LoadUint64(&r.rows[at+2])
		meta := atomic.LoadUint64(&r.rows[at+3])
		if before != r.version.Load() {
			continue
		}
		bucket := BucketID(uint32(meta >> 32))
		logicalID, ok := CommonPrimaryLeafLogicalID(bucket)
		if !ok {
			return ResidentPrimaryRoute{}, false
		}
		return ResidentPrimaryRoute{
			Ref: PageRef{
				Offset: offset, LogicalID: logicalID,
				Generation: generation, Length: uint32(meta),
				Kind: PagePrimaryLeaf,
			},
			Bucket: bucket,
			Hash:   hash,
			rank:   uint32(rank),
		}, true
	}
}

// RouteAtRank returns the leaf route stored at one router row, reading its
// current mutable handle under the version seqlock. rank is the lexical row
// ordinal, not a bucket identity, so it is stable regardless of local-ID
// contiguity. It is the ordered-enumeration entry the exact-index build and
// live-slot derivation walk every leaf through.
func (r *ResidentPrimaryRouter) RouteAtRank(rank int) (ResidentPrimaryRoute, bool) {
	if r == nil || rank < 0 || rank >= r.Len() {
		return ResidentPrimaryRoute{}, false
	}
	at := rank * residentPrimaryRouterWords
	for {
		before := r.version.Load()
		if before&1 != 0 {
			continue
		}
		offset := atomic.LoadUint64(&r.rows[at+1])
		generation := atomic.LoadUint64(&r.rows[at+2])
		meta := atomic.LoadUint64(&r.rows[at+3])
		if before != r.version.Load() {
			continue
		}
		bucket := BucketID(uint32(meta >> 32))
		logicalID, ok := CommonPrimaryLeafLogicalID(bucket)
		if !ok {
			return ResidentPrimaryRoute{}, false
		}
		return ResidentPrimaryRoute{
			Ref: PageRef{
				Offset: offset, LogicalID: logicalID,
				Generation: generation, Length: uint32(meta),
				Kind: PagePrimaryLeaf,
			},
			Bucket: bucket, rank: uint32(rank),
		}, true
	}
}

// ResolveBucketID returns the leaf route for one stable BucketID. A bottom-up
// bulk build assigns bucket == row ordinal, so the packed row is found in O(1);
// a graph reshaped by splits/merges may hold non-contiguous local IDs, so a
// mismatch falls back to a lexical scan for the matching identity. It is the
// posting-driven route the exact-index read path selects a tile's leaf by.
func (r *ResidentPrimaryRouter) ResolveBucketID(
	bucket BucketID,
) (ResidentPrimaryRoute, bool) {
	if r == nil {
		return ResidentPrimaryRoute{}, false
	}
	if int(bucket) < r.Len() {
		if route, ok := r.RouteAtRank(int(bucket)); ok && route.Bucket == bucket {
			return route, true
		}
	}
	for rank := 0; rank < r.Len(); rank++ {
		if route, ok := r.RouteAtRank(rank); ok && route.Bucket == bucket {
			return route, true
		}
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
	if r == nil || int(route.rank) >= r.Len() ||
		generation <= r.Generation() ||
		next == (PageRef{}) || next.Kind != PagePrimaryLeaf ||
		next.Generation > generation {
		return false
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
	return r != nil && int(route.rank) < len(r.empty) &&
		r.empty[route.rank].CompareAndSwap(0, 1)
}

// ClearEmpty clears a session-local empty marker when an insertion refills the
// same routed leaf.
func (r *ResidentPrimaryRouter) ClearEmpty(
	route ResidentPrimaryRoute,
) bool {
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
	if r == nil || cache == nil || int(route.rank) >= len(r.hints) {
		if cache == nil {
			return PageLease{}, ErrPageCacheReference
		}
		return cache.Acquire(route.Ref)
	}
	return cache.acquireFrameHinted(route.Ref, &r.hints[route.rank])
}

func (r *ResidentPrimaryRouter) fence(rank int) []byte {
	word := r.rows[rank*residentPrimaryRouterWords]
	return r.fences[uint32(word):uint32(word>>32)]
}

func (r *ResidentPrimaryRouter) Len() int {
	if r == nil {
		return 0
	}
	return len(r.rows) / residentPrimaryRouterWords
}

// ResidentBytes is the exact packed payload capacity retained by the router.
func (r *ResidentPrimaryRouter) ResidentBytes() int {
	if r == nil {
		return 0
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
