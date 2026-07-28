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
	storeID    [16]byte
	fences     []byte
	rows       []uint64
	hints      []pageCacheFrameHint
	empty      []atomic.Uint32
	mergeAbort []atomic.Uint32
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
	router.mergeAbort = make([]atomic.Uint32, router.Len())
	router.generation.Store(bounds.SelectedRootGeneration)
	router.buildNS = time.Since(started).Nanoseconds()
	return router, nil
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
			start := len(r.fences)
			if leafRank == 0 {
				r.fences = append(r.fences, tabletFloor...)
			} else {
				r.fences = append(r.fences, fence.a...)
				r.fences = append(r.fences, fence.b...)
				r.fences = append(r.fences, fence.c...)
			}
			if len(r.fences) > int(^uint32(0)) ||
				len(r.fences)-start > int(^uint32(0)) {
				anchorLease.Release()
				tabletLease.Release()
				return fmt.Errorf("%w: resident fence arena",
					ErrSegmentedTabletRouterCorrupt)
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
	low, high := 1, r.Len()
	for low < high {
		middle := int(uint(low+high) >> 1)
		if bytes.Compare(r.fence(middle), key) <= 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	rank := low - 1
	if rank < 0 || bytes.Compare(r.fence(rank), key) > 0 ||
		rank+1 < r.Len() && bytes.Compare(key, r.fence(rank+1)) >= 0 {
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
			Bucket: bucket,
			Hash:   hash,
			rank:   uint32(rank),
		}, true
	}
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
	r.version.Add(1)
	r.generation.Store(generation)
	r.version.Add(1)
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

// NeighborRoute returns the current leaf handle at the lexical rank offset delta
// (-1 or +1) from route, for a read-only merge-viability peek: the caller
// already holds the routed leaf and only needs a neighbour's live occupancy, so
// no key is hashed. The handle words are sampled under the same version protocol
// as Route so a concurrent single-writer COW handle swap is observed coherently.
func (r *ResidentPrimaryRouter) NeighborRoute(
	route ResidentPrimaryRoute, delta int,
) (ResidentPrimaryRoute, bool) {
	if r == nil {
		return ResidentPrimaryRoute{}, false
	}
	rank := int(route.rank) + delta
	if rank < 0 || rank >= r.Len() {
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
			Bucket: bucket,
			rank:   uint32(rank),
		}, true
	}
}

// MergeAbortedAt reports the live count at which route's leaf last aborted a
// merge evaluation, or zero if none is recorded. The merge floor uses it for
// hysteresis: a below-floor leaf that already found no viable neighbour at its
// current count is skipped read-only until its count changes.
func (r *ResidentPrimaryRouter) MergeAbortedAt(
	route ResidentPrimaryRoute,
) uint32 {
	if r == nil || int(route.rank) >= len(r.mergeAbort) {
		return 0
	}
	return r.mergeAbort[route.rank].Load()
}

// RecordMergeAbort stamps the live count at which route's leaf found no viable
// merge, so the next equal-count evaluation is skipped. A subsequent insert or
// delete changes the leaf's count and the stamp no longer matches, re-arming the
// evaluation. A structural rebuild allocates a fresh stamp array, clearing all.
func (r *ResidentPrimaryRouter) RecordMergeAbort(
	route ResidentPrimaryRoute, liveCount uint32,
) {
	if r == nil || int(route.rank) >= len(r.mergeAbort) {
		return
	}
	r.mergeAbort[route.rank].Store(liveCount)
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
	return cap(r.fences) + cap(r.rows)*8 + cap(r.hints)*8 +
		cap(r.empty)*4 + cap(r.mergeAbort)*4
}

// BuildDuration reports the wall time spent walking and packing the graph,
// including PageCache acquisitions made by the Open-time build.
func (r *ResidentPrimaryRouter) BuildDuration() time.Duration {
	if r == nil {
		return 0
	}
	return time.Duration(r.buildNS)
}
