package durable

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	mathbits "math/bits"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson/x/byteview"
)

// ErrIndexBuildInProgress reports a second online index declaration against a
// collection that is already reconciling and publishing another one.
var ErrIndexBuildInProgress = errors.New(
	"vibejson: durable collection index build is already in progress",
)

type onlineIndexBucket struct {
	ref            storeio.PageRef
	overlayVersion uint64
	live           [4]uint64
	terms          map[string]uint16
	termMasks      [][4]uint64
	keyArena       []byte
}

type onlineIndexTermSource struct {
	bucket storeio.BucketID
	keys   []string
	at     int
}

type onlineIndexTermHeap struct {
	sources []onlineIndexTermSource
	order   []int
}

func (h *onlineIndexTermHeap) less(left, right int) bool {
	a := &h.sources[h.order[left]]
	b := &h.sources[h.order[right]]
	aKey := a.keys[a.at]
	bKey := b.keys[b.at]
	return aKey < bKey || aKey == bKey && a.bucket < b.bucket
}

func (h *onlineIndexTermHeap) push(source int) {
	h.order = append(h.order, source)
	at := len(h.order) - 1
	for at > 0 {
		parent := (at - 1) >> 1
		if !h.less(at, parent) {
			break
		}
		h.order[at], h.order[parent] = h.order[parent], h.order[at]
		at = parent
	}
}

func (h *onlineIndexTermHeap) pop() int {
	source := h.order[0]
	last := len(h.order) - 1
	h.order[0] = h.order[last]
	h.order = h.order[:last]
	at := 0
	for {
		left := at*2 + 1
		if left >= len(h.order) {
			break
		}
		smallest := left
		if right := left + 1; right < len(h.order) &&
			h.less(right, left) {
			smallest = right
		}
		if !h.less(smallest, at) {
			break
		}
		h.order[at], h.order[smallest] =
			h.order[smallest], h.order[at]
		at = smallest
	}
	return source
}

type onlineIndexBuild struct {
	exact    *store.ExactIndex
	buckets  map[storeio.BucketID]onlineIndexBucket
	nextRank int
}

const onlineIndexRouteCheckBudget = 64

// CreateIndex builds and atomically publishes an exact index over a live
// collection. Reads and writes continue during the scan. The builder repeatedly
// reconciles one primary leaf per bounded writer hold and retains leaves whose
// immutable references remain current. Publication proceeds only while the
// writer observes the same leaf vector, so no mutation-side change log or
// steady-state branch is required. The collection's fixed transaction and
// resident arenas must already admit maintenance of the additional physical
// index; a deliberately tight configuration fails with
// ErrPrimaryCutoverUnsupported instead of growing steady-state resources.
func (c *Collection) CreateIndex(
	definition store.IndexDefinition,
) (store.IndexInfo, error) {
	return c.CreateIndexContext(context.Background(), definition)
}

// CreateIndexContext is CreateIndex with cancellation between bounded leaf
// visits and before the atomic publication transaction.
func (c *Collection) CreateIndexContext(
	ctx context.Context,
	definition store.IndexDefinition,
) (store.IndexInfo, error) {
	if c == nil {
		return store.IndexInfo{}, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return store.IndexInfo{}, err
	}
	if !c.onlineIndexBuild.CompareAndSwap(false, true) {
		return store.IndexInfo{}, ErrIndexBuildInProgress
	}
	defer c.onlineIndexBuild.Store(false)

	c.writer.Lock()
	if c.closed {
		c.writer.Unlock()
		return store.IndexInfo{}, ErrClosed
	}
	if _, exists := c.options.indexNameIDs[definition.Name]; exists {
		c.writer.Unlock()
		return store.IndexInfo{}, store.ErrIndexExists
	}
	candidateOptions := c.options.Options
	candidateOptions.Indexes = append(
		slices.Clone(c.options.Indexes), store.IndexDefinition{
			Name: definition.Name, Paths: slices.Clone(definition.Paths),
		},
	)
	candidate, err := candidateOptions.normalized()
	if err != nil {
		c.writer.Unlock()
		return store.IndexInfo{}, err
	}
	// The committer and fixed free-space arenas were constructed with the
	// original batch ceiling. Online publication and all currently supported
	// indexed mutations fit below that ceiling; retaining it avoids pretending
	// that immutable process resources grew with the catalog.
	if candidate.singleDocumentTransactionPages > c.options.maxTransactionPages {
		c.writer.Unlock()
		return store.IndexInfo{}, fmt.Errorf(
			"%w: existing transaction arena cannot admit this index",
			ErrPrimaryCutoverUnsupported,
		)
	}
	candidate.maxTransactionPages = c.options.maxTransactionPages
	// Free-fold arenas are fixed, pointer-free process resources. A lower fold
	// ceiling is always safe (it rebuilds fewer free-image pages per
	// transaction). Indexed batches use the same arenas admitted at Create, so
	// retain both ceilings rather than growing heap-backed scratch at cutover.
	candidate.singleDocumentFreeFoldLimit =
		c.options.singleDocumentFreeFoldLimit
	candidate.freeFoldLimit = c.options.freeFoldLimit
	targetID := candidate.indexNameIDs[definition.Name]
	target := candidate.indexes[targetID]
	oldTarget := exactIndexID(c.options.indexes, target)
	router := c.primaryRouter.Load()
	if router == nil {
		c.writer.Unlock()
		return store.IndexInfo{}, storeio.ErrSegmentedTabletRouterCorrupt
	}
	routeCount := router.Len()
	c.writer.Unlock()

	if oldTarget >= 0 {
		// A differently named index over the same ordered path vector is a
		// logical alias. It needs only a catalog/root transaction; no document
		// scan and no duplicate physical bytes.
		if err := ctx.Err(); err != nil {
			return store.IndexInfo{}, err
		}
		c.writer.Lock()
		err = c.publishOnlineIndexLocked(candidate, nil, targetID)
		c.writer.Unlock()
		if err != nil {
			return store.IndexInfo{}, err
		}
		return onlineIndexInfo(definition), nil
	}

	build := &onlineIndexBuild{
		exact: target,
		buckets: make(
			map[storeio.BucketID]onlineIndexBucket, routeCount,
		),
	}
	for {
		if err := ctx.Err(); err != nil {
			return store.IndexInfo{}, err
		}
		c.writer.Lock()
		complete, reconcileErr := build.reconcileOneLocked(c)
		validatedRouter := c.primaryRouter.Load()
		if reconcileErr != nil {
			c.writer.Unlock()
			return store.IndexInfo{}, reconcileErr
		}
		c.writer.Unlock()
		if !complete {
			continue
		}

		validatedGeneration, stable := build.matchesStable(
			c, validatedRouter,
		)
		if !stable {
			continue
		}

		c.writer.Lock()
		if c.primaryRouter.Load() != validatedRouter ||
			validatedRouter.Generation() != validatedGeneration {
			c.writer.Unlock()
			continue
		}
		if err = c.flushPendingForStructural(); err != nil {
			c.writer.Unlock()
			return store.IndexInfo{}, err
		}
		// A pending-parent checkpoint advances the router and folds the exact
		// overlay. Reconcile against that new immutable leaf vector before
		// preparing the cutover epoch.
		if c.primaryRouter.Load() != validatedRouter ||
			validatedRouter.Generation() != validatedGeneration {
			c.writer.Unlock()
			continue
		}
		currentIndexes := c.options.indexes
		currentEpoch := c.primaryEpoch
		if currentEpoch != nil && !currentEpoch.overlayEmpty() {
			c.writer.Unlock()
			return store.IndexInfo{}, storeio.ErrPrimaryExactIndexCorrupt
		}
		var epochLease storeio.GenerationLease
		if currentEpoch != nil {
			// The expensive resident preparation runs without the writer. Pin
			// this generation so a concurrent fold may retire currentEpoch but
			// cannot recycle/reset its immutable base while it is being copied.
			var leaseErr error
			epochLease, leaseErr = c.leases.Acquire(validatedGeneration)
			if leaseErr != nil {
				c.writer.Unlock()
				return store.IndexInfo{}, leaseErr
			}
		}
		fresh := c.takePrimaryExactEpochLocked(len(candidate.indexes))
		c.writer.Unlock()

		prepared, err := build.prepareResident(
			c, currentIndexes, currentEpoch, candidate, targetID, fresh,
		)
		epochLease.Release()
		if err != nil {
			c.writer.Lock()
			c.recycleUnpublishedOnlineEpochLocked(fresh)
			c.writer.Unlock()
			return store.IndexInfo{}, err
		}
		if err := ctx.Err(); err != nil {
			c.writer.Lock()
			c.recycleUnpublishedOnlineEpochLocked(fresh)
			c.writer.Unlock()
			return store.IndexInfo{}, err
		}

		c.writer.Lock()
		if c.primaryRouter.Load() == validatedRouter &&
			validatedRouter.Generation() == validatedGeneration {
			err = c.publishOnlineIndexLocked(candidate, &prepared, targetID)
			c.writer.Unlock()
			if err != nil {
				if prepared.epoch != nil {
					c.writer.Lock()
					c.recycleUnpublishedOnlineEpochLocked(prepared.epoch)
					c.writer.Unlock()
				}
				return store.IndexInfo{}, err
			}
			return onlineIndexInfo(definition), nil
		}
		c.recycleUnpublishedOnlineEpochLocked(prepared.epoch)
		c.writer.Unlock()
	}
}

func (c *Collection) recycleUnpublishedOnlineEpochLocked(
	epoch *primaryExactEpoch,
) {
	if epoch == nil {
		return
	}
	epoch.reset()
	c.primaryEpochPool = append(c.primaryEpochPool, epoch)
}

func onlineIndexInfo(definition store.IndexDefinition) store.IndexInfo {
	info := store.IndexInfo{
		Name: definition.Name, Kind: store.IndexExact, State: store.IndexReady,
		TotalChunks: 1, CoveredChunks: 1,
		ColumnCount: uint8(len(definition.Paths)),
	}
	copy(info.Columns[:], definition.Paths)
	return info
}

func exactIndexID(indexes []*store.ExactIndex, target *store.ExactIndex) int {
	for indexID, exact := range indexes {
		if exactIndexSpecsEqual(exact, target) {
			return indexID
		}
	}
	return -1
}

func exactIndexSpecsEqual(a, b *store.ExactIndex) bool {
	if a == nil || b == nil || a.N != b.N {
		return false
	}
	for i := 0; i < int(a.N); i++ {
		if a.Specs[i] != b.Specs[i] {
			return false
		}
	}
	return true
}

func (b *onlineIndexBuild) reconcileOneLocked(
	c *Collection,
) (bool, error) {
	if c.closed {
		return false, ErrClosed
	}
	state := c.state.Load()
	router := c.primaryRouter.Load()
	if router == nil {
		return false, storeio.ErrSegmentedTabletRouterCorrupt
	}
	checked := 0
	for b.nextRank < router.Len() &&
		checked < onlineIndexRouteCheckBudget {
		route, ok := router.RouteAtRank(b.nextRank)
		if !ok {
			return false, storeio.ErrSegmentedTabletRouterCorrupt
		}
		b.nextRank++
		checked++
		overlayVersion := c.primaryUnifiedOverlay.bucketVersion(
			route.Bucket, state.root.Generation,
		)
		if previous, exists := b.buckets[route.Bucket]; exists &&
			previous.ref == route.Ref &&
			previous.overlayVersion == overlayVersion {
			continue
		}
		bucket, err := b.scanBucket(c, state, router, route)
		if err != nil {
			return false, err
		}
		b.buckets[route.Bucket] = bucket
		return false, nil
	}
	if b.nextRank < router.Len() {
		return false, nil
	}
	b.nextRank = 0
	if len(b.buckets) != router.Len() {
		present := make(map[storeio.BucketID]struct{}, router.Len())
		for rank := 0; rank < router.Len(); rank++ {
			route, ok := router.RouteAtRank(rank)
			if !ok {
				return false, storeio.ErrSegmentedTabletRouterCorrupt
			}
			present[route.Bucket] = struct{}{}
		}
		for bucketID := range b.buckets {
			if _, ok := present[bucketID]; !ok {
				delete(b.buckets, bucketID)
			}
		}
	}
	return true, nil
}

func (b *onlineIndexBuild) scanBucket(
	c *Collection,
	state *fileStoreState,
	router *storeio.ResidentPrimaryRouter,
	route storeio.ResidentPrimaryRoute,
) (onlineIndexBucket, error) {
	bucket := onlineIndexBucket{
		ref: route.Ref,
		overlayVersion: c.primaryUnifiedOverlay.bucketVersion(
			route.Bucket, state.root.Generation,
		),
		terms:     make(map[string]uint16, 32),
		termMasks: make([][4]uint64, 0, 32),
		keyArena:  make([]byte, 0, 1024),
	}
	lease, err := router.AcquireLeaf(c.cache, route)
	if err != nil {
		return onlineIndexBucket{}, err
	}
	defer lease.Release()
	var (
		components [store.MaxIndexColumns]storeio.IndexTermComponent
		canonical  [storeio.IndexTermMaxKeyBytes]byte
		overflow   []byte
		scratch    []byte
	)
	bounds := c.primaryLeafBounds(state)
	visit := func(slot uint8, raw []byte, outOfLine bool) error {
		if outOfLine {
			var resolveErr error
			overflow, resolveErr = c.appendPrimaryOverflowValue(
				overflow[:0],
				storeio.DecodePrimaryOverflowRef(raw), bounds,
			)
			if resolveErr != nil {
				return resolveErr
			}
			raw = overflow
		}
		quadrant := slot >> 6
		bit := uint64(1) << uint(slot&63)
		bucket.live[quadrant] |= bit
		key, present, termErr := appendPrimaryExactDocumentTerm(
			canonical[:0], components[:], b.exact, raw,
		)
		if termErr != nil || !present {
			return termErr
		}
		identity := byteview.String(key)
		termID, exists := bucket.terms[identity]
		if !exists {
			if len(bucket.termMasks) == int(^uint16(0)) {
				return storeio.ErrInvalidWrite
			}
			termID = uint16(len(bucket.termMasks))
			start := len(bucket.keyArena)
			bucket.keyArena = append(bucket.keyArena, key...)
			stored := byteview.String(
				bucket.keyArena[start:len(bucket.keyArena):len(bucket.keyArena)],
			)
			bucket.terms[stored] = termID
			bucket.termMasks = append(
				bucket.termMasks, [4]uint64{},
			)
		}
		bucket.termMasks[termID][quadrant] |= bit
		return nil
	}
	unified, ok := storeio.AdmittedCommonPrimaryUnifiedLeaf(
		lease.Page(), state.root.StoreID, route.Bucket, bounds,
	)
	if !ok {
		return onlineIndexBucket{}, storeio.ErrPrimaryExactIndexCorrupt
	}
	slots, ok := unified.PostingSlots()
	if !ok {
		return onlineIndexBucket{}, storeio.ErrPrimaryExactIndexCorrupt
	}
	overlay := c.primaryUnifiedOverlay
	var overlayIndexes [storeio.CommonPrimaryLeafWideSlots]uint16
	overlayCount, overlayErr := overlay.latestBucketRecords(
		&overlayIndexes, route.Bucket, state.root.Generation,
	)
	if overlayErr != nil {
		return onlineIndexBucket{}, errors.Join(
			storeio.ErrPrimaryExactIndexCorrupt, overlayErr,
		)
	}
	baseRank, overlayAt := 0, 0
	for baseRank < unified.Len() || overlayAt < overlayCount {
		var baseKey, baseBody []byte
		var baseOverflow bool
		if baseRank < unified.Len() {
			var rowOK bool
			baseKey, baseBody, baseOverflow, rowOK =
				unified.RowRawAt(baseRank)
			if !rowOK {
				return onlineIndexBucket{},
					storeio.ErrPrimaryExactIndexCorrupt
			}
		}
		var record *primaryUnifiedOverlayRecord
		var overlayKey []byte
		if overlayAt < overlayCount {
			record = &overlay.records[overlayIndexes[overlayAt]]
			keyEnd := record.keyOffset + uint32(record.keyLen)
			if keyEnd > uint32(len(overlay.arena)) {
				return onlineIndexBucket{},
					storeio.ErrPrimaryExactIndexCorrupt
			}
			overlayKey = overlay.arena[record.keyOffset:keyEnd:keyEnd]
		}
		order := 0
		switch {
		case baseRank >= unified.Len():
			order = 1
		case overlayAt >= overlayCount:
			order = -1
		default:
			order = bytes.Compare(baseKey, overlayKey)
		}
		if order < 0 {
			if baseOverflow {
				err = visit(slots[baseRank], baseBody, true)
			} else {
				scratch = unified.AppendAdmittedRowBody(
					scratch[:0], baseBody,
				)
				err = visit(slots[baseRank], scratch, false)
			}
			if err != nil {
				return onlineIndexBucket{}, err
			}
			baseRank++
			continue
		}
		if record.kind == primaryUnifiedOverlayPut {
			valueEnd := record.valueOff + record.valueLen
			if record.valueLen == 0 ||
				valueEnd > uint32(len(overlay.arena)) {
				return onlineIndexBucket{},
					storeio.ErrPrimaryExactIndexCorrupt
			}
			err = visit(
				record.slot,
				overlay.arena[record.valueOff:valueEnd:valueEnd],
				false,
			)
			if err != nil {
				return onlineIndexBucket{}, err
			}
		} else if record.kind != primaryUnifiedOverlayDelete {
			return onlineIndexBucket{}, storeio.ErrPrimaryExactIndexCorrupt
		}
		overlayAt++
		if order == 0 {
			baseRank++
		}
	}
	_ = scratch
	return bucket, nil
}

func (b *onlineIndexBuild) prepareResident(
	c *Collection,
	currentIndexes []*store.ExactIndex,
	currentEpoch *primaryExactEpoch,
	candidate normalizedFileStoreOptions,
	targetID uint32,
	fresh *primaryExactEpoch,
) (primaryExactPrepared, error) {
	if fresh == nil {
		return primaryExactPrepared{}, storeio.ErrInvalidWrite
	}
	live := make(map[uint32]*[storeio.TermPostingTileChunks]uint64)
	if currentEpoch != nil && currentEpoch.live != nil {
		for at := range currentEpoch.live.slots {
			slot := &currentEpoch.live.slots[at]
			if slot.mask != nil {
				live[slot.tileID] = slot.mask
			}
		}
		for bucketID, bucket := range b.buckets {
			for quadrant, bits := range bucket.live {
				tileID := uint32(bucketID)<<2 | uint32(quadrant)
				mask := live[tileID]
				var got uint64
				if mask != nil {
					got = mask[0]
				}
				if got != bits {
					return primaryExactPrepared{},
						storeio.ErrPrimaryExactIndexCorrupt
				}
			}
		}
	} else {
		for bucketID, bucket := range b.buckets {
			for quadrant, bits := range bucket.live {
				if bits == 0 {
					continue
				}
				tileID := uint32(bucketID)<<2 | uint32(quadrant)
				mask := new([storeio.TermPostingTileChunks]uint64)
				mask[0] = bits
				live[tileID] = mask
			}
		}
	}
	fresh.live = newPrimaryLiveTable(live)
	terms, err := c.buildOnlineIndexTerms(
		b.buckets, fresh.live.lookup,
	)
	if err != nil {
		return primaryExactPrepared{}, err
	}
	fresh.baseGen = 0
	for candidateID, candidateIndex := range candidate.indexes {
		if uint32(candidateID) == targetID {
			leaves, cutErr := c.foldEmitCutLeaves(
				fresh, terms,
				storeio.IndexTermLeafCutBudget(uint32(c.options.MaxPageSize)),
				fresh.exact[candidateID].leaves[:0], false,
			)
			if cutErr != nil {
				return primaryExactPrepared{}, cutErr
			}
			fresh.exact[candidateID].leaves = leaves
			continue
		}
		oldID := exactIndexID(
			currentIndexes, candidateIndex,
		)
		if oldID < 0 || currentEpoch == nil ||
			oldID >= len(currentEpoch.exact) {
			continue
		}
		old := &currentEpoch.exact[oldID]
		resident := &fresh.exact[candidateID]
		resident.leaves = resident.leaves[:0]
		for leafAt := range old.leaves {
			carried := old.leaves[leafAt]
			carried.view = carried.view.WithLive(fresh.live.lookup)
			resident.leaves = append(resident.leaves, carried)
		}
		resident.catalog = append(
			resident.catalog[:0], old.catalog...,
		)
	}
	return primaryExactPrepared{active: true, epoch: fresh}, nil
}

// buildOnlineIndexTerms merges sorted per-bucket contributions directly
// into the canonical codec input. It deliberately avoids constructing a second
// complete term->tile map: temporary space is the sorted string headers, the
// merge heap (one entry per leaf), and the final cutter input.
func (c *Collection) buildOnlineIndexTerms(
	buckets map[storeio.BucketID]onlineIndexBucket,
	liveLookup storeio.IndexTermLeafLiveLookup,
) ([]storeio.IndexTermLeafTerm, error) {
	heap := onlineIndexTermHeap{
		sources: make([]onlineIndexTermSource, 0, len(buckets)),
		order:   make([]int, 0, len(buckets)),
	}
	totalContributions := 0
	totalPostings := 0
	for bucketID, bucket := range buckets {
		if len(bucket.terms) == 0 {
			continue
		}
		keys := make([]string, 0, len(bucket.terms))
		for key := range bucket.terms {
			keys = append(keys, key)
			termID := bucket.terms[key]
			if int(termID) >= len(bucket.termMasks) {
				return nil, storeio.ErrPrimaryExactIndexCorrupt
			}
			for _, bits := range bucket.termMasks[termID] {
				if bits != 0 {
					totalPostings++
				}
			}
		}
		slices.Sort(keys)
		totalContributions += len(keys)
		sourceID := len(heap.sources)
		heap.sources = append(heap.sources, onlineIndexTermSource{
			bucket: bucketID, keys: keys,
		})
		heap.push(sourceID)
	}
	if len(heap.order) == 0 {
		return nil, nil
	}

	terms := make(
		[]storeio.IndexTermLeafTerm, 0, min(totalContributions, 1024),
	)
	// Postings outnumber unique terms but their exact count is already known
	// from the contribution walk. One flat arena removes an allocation per
	// high-cardinality term without reserving a worst-case term×tile matrix.
	allPostings := make(
		[]storeio.IndexTermLeafPosting, 0, totalPostings,
	)
	for len(heap.order) != 0 {
		sourceID := heap.pop()
		source := &heap.sources[sourceID]
		key := source.keys[source.at]
		postingStart := len(allPostings)
		for {
			bucket := buckets[source.bucket]
			termID, ok := bucket.terms[key]
			if !ok || int(termID) >= len(bucket.termMasks) {
				return nil, storeio.ErrPrimaryExactIndexCorrupt
			}
			masks := bucket.termMasks[termID]
			for quadrant, bits := range masks {
				if bits == 0 {
					continue
				}
				tileID := uint32(source.bucket)<<2 | uint32(quadrant)
				liveMask := liveLookup(tileID)
				if liveMask == nil {
					return nil, storeio.ErrPrimaryExactIndexCorrupt
				}
				// Ordered-primary postings are chunk-0-only by construction.
				// Supplying that fact lets the canonical leaf builder choose
				// its direct codec without materializing a component payload,
				// exactly like the streamed checkpoint fold.
				allPostings = append(allPostings, storeio.IndexTermLeafPosting{
					Posting: storeio.TermPosting{
						TileID: tileID,
						Rows:   uint16(mathbits.OnesCount64(bits)),
					},
					Live: liveMask, Chunk0Bits: bits, Chunk0Only: true,
				})
			}
			source.at++
			if source.at < len(source.keys) {
				heap.push(sourceID)
			}
			if len(heap.order) == 0 {
				break
			}
			next := &heap.sources[heap.order[0]]
			if next.keys[next.at] != key {
				break
			}
			sourceID = heap.pop()
			source = &heap.sources[sourceID]
		}
		record, ok := storeio.OpenIndexTermKeyRecord(
			c.storeID, byteview.Bytes(key),
		)
		if !ok {
			return nil, fmt.Errorf(
				"%w: canonical exact term", storeio.ErrInvalidWrite,
			)
		}
		terms = append(terms, storeio.IndexTermLeafTerm{
			Key:      record,
			Postings: allPostings[postingStart:len(allPostings):len(allPostings)],
		})
	}
	return terms, nil
}

// encodeOnlineIndexBuckets preserves the single-leaf oracle used by focused
// merge tests. Production online index creation sends the same ordered terms
// through the shared deterministic cutter.
func (c *Collection) encodeOnlineIndexBuckets(
	buckets map[storeio.BucketID]onlineIndexBucket,
	liveLookup storeio.IndexTermLeafLiveLookup,
) ([]byte, error) {
	terms, err := c.buildOnlineIndexTerms(buckets, liveLookup)
	if err != nil {
		return nil, err
	}
	return storeio.AppendIndexTermLeaf(nil, c.storeID, terms)
}

func (b *onlineIndexBuild) matches(
	c *Collection, router *storeio.ResidentPrimaryRouter,
) bool {
	if router == nil || router.Len() != len(b.buckets) {
		return false
	}
	for rank := 0; rank < router.Len(); rank++ {
		route, ok := router.RouteAtRank(rank)
		if !ok {
			return false
		}
		bucket, exists := b.buckets[route.Bucket]
		if !exists || bucket.ref != route.Ref ||
			bucket.overlayVersion != c.primaryUnifiedOverlay.bucketVersion(
				route.Bucket, router.Generation(),
			) {
			return false
		}
	}
	return true
}

// matchesStable validates the complete immutable-ref vector without holding
// the writer. Resident router handles are coherently sampled, and every primary
// mutation advances its generation; equal generations before and after the
// walk, plus pointer identity, prove that the vector did not change. The final
// publication rechecks both under the writer.
func (b *onlineIndexBuild) matchesStable(
	c *Collection,
	router *storeio.ResidentPrimaryRouter,
) (uint64, bool) {
	if router == nil {
		return 0, false
	}
	before := router.Generation()
	if !b.matches(c, router) {
		return 0, false
	}
	after := router.Generation()
	return after, before == after && c.primaryRouter.Load() == router
}

func (c *Collection) publishOnlineIndexLocked(
	candidate normalizedFileStoreOptions,
	prepared *primaryExactPrepared,
	targetID uint32,
) (err error) {
	if c.closed {
		return ErrClosed
	}
	for name := range candidate.indexNameIDs {
		if _, existed := c.options.indexNameIDs[name]; !existed {
			goto catalogAddsName
		}
	}
	return store.ErrIndexExists

catalogAddsName:
	if err := c.flushPendingForStructural(); err != nil {
		return err
	}
	state := c.state.Load()
	generation := state.root.Generation + 1
	if generation == 0 || generation >= uint64(1)<<48 {
		return storeio.ErrGenerationOrder
	}
	if err := c.refreshReusableFor(
		state, c.options.maxTransactionPages, c.options.freeFoldLimit,
	); err != nil {
		return err
	}
	tx, err := c.beginWriteTransaction(
		c.options.maxTransactionPages,
		storeio.WriteTransactionOptions{
			StoreID: c.storeID, Generation: generation,
			PageSize: uint32(c.options.PageSize),
			FileEnd:  state.fileEnd, NextLogicalID: state.root.NextLogicalID,
			Reusable: c.reusable, ReuseJournal: c.reuseJournal,
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
					state.root.Generation,
				)
			}
			err = errors.Join(err, tx.Abort())
		}
	}()
	c.retireScratch = c.retireScratch[:0]
	c.retireRefScratch = c.retireRefScratch[:0]

	if prepared != nil {
		if prepared.epoch == nil {
			return storeio.ErrInvalidWrite
		}
	} else {
		// Alias-only publication can reuse the current physical residents and
		// liveness; canonical physical IDs are unchanged when no new path vector
		// is introduced.
		if int(targetID) >= len(candidate.indexes) {
			return storeio.ErrInvalidWrite
		}
	}
	exactRoot, err := c.stageOnlineExactRootLocked(
		tx, state, generation, candidate, prepared, targetID,
	)
	if err != nil {
		return err
	}
	catalogHead, err := c.stageOnlineCatalogLocked(
		tx, state, generation, candidate.pageCatalog,
	)
	if err != nil {
		return err
	}
	freeLog, err := c.syncFreeLogFor(
		tx, state, c.options.freeFoldLimit,
	)
	if err != nil {
		return err
	}
	nextState, nextInline, err := c.stagePrimaryState(
		tx, state, generation, state.root.PrimaryRoot,
		freeLog.head, freeLog.inline,
		state.root.DocumentCount,
	)
	if err != nil {
		return err
	}
	nextState.root.ExactIndexRoot = exactRoot
	nextState.root.PageCatalogHead = catalogHead
	nextState.root.PageCatalogBytes =
		uint32(candidate.pageCatalog.CanonicalSize())
	nextState.root.PageCatalogDigest = candidate.pageCatalog.Digest()
	nextState.root.IndexCount = uint32(len(candidate.indexes))
	nextState.root.IndexCatalogHash = candidate.indexCatalogHash

	if prepared != nil {
		if c.primaryEpoch == nil && c.primaryEpochRetired == nil {
			// Match Open's warm epoch lifecycle shape for a collection whose
			// first physical index is appearing now.
			c.primaryEpochRetired = make(
				[]retiredPrimaryExactEpoch, 0, 8,
			)
			c.primaryEpochPool = make([]*primaryExactEpoch, 0, 8)
		}
		if c.primaryEpoch != nil {
			// Keep the publish-gate epoch swap allocation-free even when a
			// long-lived snapshot delays recycling several DDL epochs.
			c.primaryEpochRetired = slices.Grow(c.primaryEpochRetired, 1)
		}
	}
	if err := c.reserveFileRetirements(); err != nil {
		return err
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
	if prepared != nil {
		prepared.gen = generation
		prepared.epoch.baseGen = generation
	}
	// Only catalog-dependent fields change. Assigning the whole normalized
	// struct would needlessly race lock-free readers of frozen admission limits
	// such as MaxDocumentBytes; those fields are intentionally untouched.
	c.options.Options.Indexes = candidate.Options.Indexes
	c.options.maxTransactionBytes = candidate.maxTransactionBytes
	c.options.singleDocumentTransactionPages =
		candidate.singleDocumentTransactionPages
	c.options.singleDocumentTransactionBytes =
		candidate.singleDocumentTransactionBytes
	c.options.pageCatalog = candidate.pageCatalog
	c.options.indexes = candidate.indexes
	c.options.indexNameIDs = candidate.indexNameIDs
	c.options.indexCatalogHash = candidate.indexCatalogHash
	if prepared != nil {
		c.installPrimaryExactResidentLocked(*prepared)
		// Ownership moved to the installed/retired epoch graph. Leaving nil is
		// also how the caller distinguishes an early publication failure from a
		// post-commit durability error.
		prepared.epoch = nil
	}
	c.primaryRouter.Load().AdvanceGeneration(generation)
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

	if c.deferredCanonicalLane() {
		if err := c.committer.Flush(); err != nil {
			return err
		}
		c.cache.MarkDurable(c.committer.DurableGeneration())
	}
	return nil
}

// stageOnlineExactRootLocked performs a persistent-root update: every existing
// immutable exact leaf set and catalog remains referenced, only the newly built
// physical index receives cutter-produced leaves and a catalog, and aliases
// reuse the complete old root. This makes index creation write amplification
// proportional to the new index, independent of how many indexes the
// collection already has.
func (c *Collection) stageOnlineExactRootLocked(
	tx *storeio.WriteTransaction,
	state *fileStoreState,
	generation uint64,
	candidate normalizedFileStoreOptions,
	prepared *primaryExactPrepared,
	targetID uint32,
) (storeio.PageRef, error) {
	if prepared == nil {
		return state.root.ExactIndexRoot, nil
	}
	if int(targetID) >= len(candidate.indexes) ||
		prepared.epoch == nil ||
		len(prepared.epoch.exact) != len(candidate.indexes) {
		return storeio.PageRef{}, storeio.ErrInvalidWrite
	}

	var oldRoot storeio.PrimaryExactRootView
	if state.root.ExactIndexRoot != (storeio.PageRef{}) {
		lease, err := c.cache.Acquire(state.root.ExactIndexRoot)
		if err != nil {
			return storeio.PageRef{}, err
		}
		var openErr error
		oldRoot, openErr = storeio.OpenPrimaryExactRootPage(
			lease.Page(), state.root.ExactIndexRoot, c.primaryExactBounds(state),
		)
		lease.Release()
		if openErr != nil {
			return storeio.PageRef{}, openErr
		}
	}

	rootEntries := make(
		[]storeio.PrimaryExactRootEntry, len(candidate.indexes),
	)
	for candidateID, index := range candidate.indexes {
		if uint32(candidateID) == targetID {
			resident := &prepared.epoch.exact[candidateID]
			if !resident.present() {
				resident.catalog = resident.catalog[:0]
				continue
			}
			staged := make(
				[]primaryExactStagedLeaf, 0, len(resident.leaves),
			)
			for leafAt := range resident.leaves {
				leaf := &resident.leaves[leafAt]
				ref, err := stagePrimaryExactLeafPage(
					tx, leaf.encoded, uint32(c.options.PageSize),
					uint32(c.options.MaxPageSize),
				)
				if err != nil {
					return storeio.PageRef{}, err
				}
				leaf.ref = ref
				staged = append(staged, primaryExactStagedLeaf{
					ref: ref, firstKey: leaf.firstKey,
					firstTile: leaf.firstTile, piece: leaf.piece,
					runCut: leaf.runCut,
				})
			}
			catalogRef, pages, err := stagePrimaryExactCatalog(
				tx, uint32(c.options.PageSize),
				uint32(c.options.MaxPageSize), staged,
				resident.catalog[:0],
			)
			if err != nil {
				return storeio.PageRef{}, err
			}
			resident.catalog = pages
			rootEntries[candidateID] = storeio.PrimaryExactRootEntry{
				Catalog: catalogRef, LeafCount: uint32(len(staged)),
			}
			continue
		}

		oldID := exactIndexID(c.options.indexes, index)
		if oldID < 0 || oldID >= oldRoot.Len() {
			return storeio.PageRef{}, storeio.ErrPrimaryExactIndexCorrupt
		}
		entry, ok := oldRoot.Entry(uint32(oldID))
		if !ok {
			return storeio.PageRef{}, storeio.ErrPrimaryExactIndexCorrupt
		}
		rootEntries[candidateID] = entry
	}

	rootPage, err := tx.Allocate(
		storeio.PagePrimaryExactRoot, uint32(c.options.PageSize), 0,
	)
	if err != nil {
		return storeio.PageRef{}, err
	}
	if _, err := storeio.EncodePrimaryExactRootPage(
		rootPage.Bytes(), c.storeID, generation,
		rootPage.Ref().LogicalID, rootEntries,
	); err != nil {
		return storeio.PageRef{}, err
	}
	if err := rootPage.Stage(); err != nil {
		return storeio.PageRef{}, err
	}
	if state.root.ExactIndexRoot != (storeio.PageRef{}) {
		if err := c.appendPrimaryRetirement(
			state, state.root.ExactIndexRoot,
		); err != nil {
			return storeio.PageRef{}, err
		}
	}
	return rootPage.Ref(), nil
}

func (c *Collection) stageOnlineCatalogLocked(
	tx *storeio.WriteTransaction,
	state *fileStoreState,
	generation uint64,
	catalog *storeio.CanonicalPageCatalog,
) (storeio.PageRef, error) {
	count, ok := catalog.SegmentCountFor(uint32(c.options.PageSize))
	if !ok || count == 0 {
		return storeio.PageRef{}, storeio.ErrInvalidWrite
	}
	// A one-page catalog can consume any best-fit free extent. Multi-page
	// catalogs are streamed as one contiguous run, so only that uncommon case
	// disables reuse before allocation.
	if count > 1 {
		tx.DisableReuse()
	}
	pages := make([]storeio.TransactionPage, count)
	for i := range pages {
		page, err := tx.Allocate(
			storeio.PageCatalogSegment,
			uint32(c.options.PageSize), 0,
		)
		if err != nil {
			return storeio.PageRef{}, err
		}
		pages[i] = page
	}
	layout, err := storeio.MutableStoreLayout(uint32(c.options.PageSize))
	if err != nil {
		return storeio.PageRef{}, err
	}
	bounds := storeio.PageCatalogBounds{
		StoreID: c.storeID, Generation: generation,
		PageSize: uint32(c.options.PageSize), DataStart: layout.DataStart,
		FileEnd: tx.FileEnd(), NextLogicalID: tx.NextLogicalID(),
		TotalBytes:     uint32(catalog.CanonicalSize()),
		ExpectedDigest: catalog.Digest(),
	}
	for i := range pages {
		var next storeio.PageRef
		if i+1 < len(pages) {
			next = pages[i+1].Ref()
		}
		if _, err := storeio.EncodePageCatalogSegment(
			pages[i].Bytes(),
			storeio.PageCatalogSegmentHeader{
				StoreID: c.storeID, Generation: generation,
				LogicalID: pages[i].Ref().LogicalID,
				Ordinal:   uint16(i), Next: next,
			},
			catalog, bounds,
		); err != nil {
			return storeio.PageRef{}, err
		}
		if err := pages[i].Stage(); err != nil {
			return storeio.PageRef{}, err
		}
	}
	oldCount, ok := c.options.pageCatalog.SegmentCountFor(
		uint32(c.options.PageSize),
	)
	if !ok {
		return storeio.PageRef{}, storeio.ErrInvalidWrite
	}
	for i := 0; i < oldCount; i++ {
		ref := state.root.PageCatalogHead
		ref.Offset += uint64(i * c.options.PageSize)
		ref.LogicalID += uint64(i)
		if err := c.appendPrimaryRetirement(state, ref); err != nil {
			return storeio.PageRef{}, err
		}
	}
	return pages[0].Ref(), nil
}

// The build guard is deliberately absent from every mutation path. It is used
// only to serialize DDL; optimistic leaf-reference reconciliation is what makes
// ordinary reads and writes pay exactly zero for an online build.
func (c *Collection) onlineIndexBuilding() bool {
	return c != nil && c.onlineIndexBuild.Load()
}
