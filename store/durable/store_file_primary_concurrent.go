package durable

import (
	"errors"
	"sync"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

const (
	// primaryConcurrentStripeCount keeps the lock directory bounded while
	// providing one hash domain as wide as a tablet's complete local-leaf ID
	// space. False collisions are harmless; unlike a tablet token, they do not
	// deliberately serialize all 4,096 leaves in one tablet.
	primaryConcurrentStripeCount = 4096
	// The scratch pool is a bounded admission queue, not a goroutine-local cache.
	// Thirty-two contexts cover the first qualification target without letting a
	// very large QueueSlots setting multiply retained JSON workspaces without
	// bound. Smaller queues retain their existing configured/default ceiling.
	primaryConcurrentContextLimit = 32
	// A compact class-5 value can never occupy more than one 64 KiB leaf.
	// Canonical escaping expands valid JSON by at most 2x (raw U+2028/U+2029),
	// so admitting at most half a leaf of source bounds every retained output
	// buffer to one leaf. Larger or exceptionally token-dense documents take the
	// established exclusive path.
	primaryConcurrentRawScratchLimit = storeio.CommonPrimaryLeafMaxExtentBytes / 2
	primaryConcurrentIndexLimit      = 8192
	primaryConcurrentCacheLine       = 64
	primaryConcurrentStripePad       = primaryConcurrentCacheLine - int(unsafe.Sizeof(sync.Mutex{}))
)

// Hooks are package-private deterministic test seams. Production leaves them
// nil, so the hot path pays one predictable nil branch at each boundary.
var (
	errConcurrentPrimaryPressure = errors.New("vibejson: concurrent primary overlay pressure")

	concurrentPrimaryReplaceStagedHook  func(storeio.BucketID)
	concurrentPrimaryReplacePublishHook func(storeio.BucketID, uint64)
	concurrentPrimaryExclusiveWaitHook  func(string)
)

// primaryConcurrentStripe keeps adjacent bucket locks on different cache
// lines. sync.Mutex is eight bytes on the supported Go targets; deriving the
// remainder from unsafe.Sizeof keeps the declaration honest if that changes.
type primaryConcurrentStripe struct {
	mu sync.Mutex
	_  [primaryConcurrentStripePad]byte
}

func primaryConcurrentStripeIndex(bucket storeio.BucketID) uint32 {
	return primaryUnifiedOverlayBucketHash(bucket) &
		(primaryConcurrentStripeCount - 1)
}

// primaryConcurrentContext owns every mutable byte touched while one caller
// validates and canonicalizes a candidate replacement. The collection's
// historical scratch remains exclusive-writer-owned for every fallback lane.
type primaryConcurrentContext struct {
	index     []vibejson.IndexEntry
	canonical []byte
	workspace storeio.CanonicalWorkspace
	publish   primaryConcurrentPublishRequest
}

func (c *primaryConcurrentContext) canonicalize(
	src []byte,
) (canonical []byte, eligible bool, err error) {
	index, err := vibejson.BuildIndex(src, c.index[:cap(c.index)])
	if errors.Is(err, document.ErrIndexFull) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	c.index = index.Entries
	if storeio.IndexIsCanonical(index, &c.workspace) {
		return src, true, nil
	}
	out, err := storeio.AppendCanonicalIndexed(
		c.canonical[:0], index, &c.workspace,
	)
	if err != nil {
		return nil, false, err
	}
	if cap(out) > cap(c.canonical) {
		// The 2x source admission bound should make this unreachable. Do not
		// retain an unexpectedly grown buffer if the canonical encoder evolves.
		return nil, false, nil
	}
	c.canonical = out
	return out, true, nil
}

// primaryConcurrentContextPool is fixed at collection construction. Context
// slices warm only to documents admitted by the inline fast path and are then
// reused; unlike sync.Pool, GC cannot silently discard and recreate them.
type primaryConcurrentContextPool struct {
	contexts  []primaryConcurrentContext
	available chan *primaryConcurrentContext
	rawLimit  int
}

func newPrimaryConcurrentContextPool(
	options normalizedFileStoreOptions,
) *primaryConcurrentContextPool {
	if options.Durability != DurabilityBufferedVisible ||
		options.RecoveryJournal ||
		options.Collection.Schema != nil ||
		len(options.indexes) != 0 ||
		options.primaryUnifiedOverlayBytes == 0 {
		return nil
	}
	count := min(
		fileVisibilitySlots(options.QueueSlots),
		primaryConcurrentContextLimit,
	)
	rawLimit := min(options.InlineValueBytes, primaryConcurrentRawScratchLimit)
	indexEntries := min(rawLimit+2, primaryConcurrentIndexLimit)
	pool := &primaryConcurrentContextPool{
		contexts:  make([]primaryConcurrentContext, count),
		available: make(chan *primaryConcurrentContext, count),
		rawLimit:  rawLimit,
	}
	for i := range pool.contexts {
		pool.contexts[i].index = make(
			[]vibejson.IndexEntry, 0, indexEntries,
		)
		pool.contexts[i].canonical = make([]byte, 0, 2*rawLimit)
		pool.contexts[i].workspace = storeio.NewCanonicalWorkspace(
			indexEntries, rawLimit,
		)
		pool.contexts[i].publish.signal = make(
			chan primaryConcurrentPublishSignal, 1,
		)
		pool.available <- &pool.contexts[i]
	}
	return pool
}

func (p *primaryConcurrentContextPool) capacityBytes() uint64 {
	if p == nil {
		return 0
	}
	bytes := uint64(cap(p.contexts)) *
		uint64(unsafe.Sizeof(primaryConcurrentContext{}))
	for i := range p.contexts {
		context := &p.contexts[i]
		bytes += uint64(cap(context.index)) *
			uint64(unsafe.Sizeof(vibejson.IndexEntry{}))
		bytes += uint64(cap(context.canonical))
		bytes += context.workspace.CapacityBytes()
	}
	return bytes
}

func (p *primaryConcurrentContextPool) acquire() *primaryConcurrentContext {
	if p == nil {
		return nil
	}
	return <-p.available
}

func (p *primaryConcurrentContextPool) release(
	context *primaryConcurrentContext,
) {
	if p == nil || context == nil {
		return
	}
	p.available <- context
}

type primaryConcurrentPublishSignal struct {
	leader  bool
	handled bool
	err     error
}

type primaryConcurrentPublishRequest struct {
	key        []byte
	canonical  []byte
	route      storeio.ResidentPrimaryRoute
	rawDelta   int
	stableSlot uint8
	signal     chan primaryConcurrentPublishSignal
}

// primaryConcurrentPublisher is a bounded flat-combining handoff. Every
// request already owns one of the fixed contexts, so at most that many request
// pointers can exist and this fixed queue cannot overflow. Exactly one caller
// is the leader; it drains the requests that reached publication together and
// hands leadership to one later arrival rather than staying trapped in a
// sustained producer stream.
type primaryConcurrentPublisher struct {
	mu    sync.Mutex
	queue [primaryConcurrentContextLimit]*primaryConcurrentPublishRequest
	// batch is owned by the sole running publisher. Retaining it here keeps the
	// flat-combiner drain allocation-free even when every call forms a group of
	// one; arrivals use queue while publication is in progress.
	batch   [primaryConcurrentContextLimit]*primaryConcurrentPublishRequest
	count   int
	running bool
}

func (p *primaryConcurrentPublisher) submit(
	collection *Collection,
	request *primaryConcurrentPublishRequest,
) (bool, error) {
	p.mu.Lock()
	if p.count == len(p.queue) {
		p.mu.Unlock()
		return false, nil
	}
	p.queue[p.count] = request
	p.count++
	leader := !p.running
	if leader {
		p.running = true
	}
	p.mu.Unlock()

	if leader {
		p.runOne(collection)
	}
	for {
		signal := <-request.signal
		if signal.leader {
			p.runOne(collection)
			continue
		}
		return signal.handled, signal.err
	}
}

func (p *primaryConcurrentPublisher) runOne(collection *Collection) {
	p.mu.Lock()
	count := p.count
	copy(p.batch[:count], p.queue[:count])
	clear(p.queue[:count])
	p.count = 0
	p.mu.Unlock()

	collection.publishConcurrentPrimaryReplaces(p.batch[:count])
	clear(p.batch[:count])

	p.mu.Lock()
	var next *primaryConcurrentPublishRequest
	if p.count == 0 {
		p.running = false
	} else {
		next = p.queue[0]
	}
	p.mu.Unlock()
	if next != nil {
		next.signal <- primaryConcurrentPublishSignal{leader: true}
	}
}

func (c *Collection) publishConcurrentPrimaryReplaces(
	requests []*primaryConcurrentPublishRequest,
) {
	if len(requests) == 0 {
		return
	}
	latest := c.state.Load()
	router := c.primaryRouter.Load()
	if latest == nil || router == nil ||
		router.Generation() != latest.root.Generation {
		for _, request := range requests {
			request.signal <- primaryConcurrentPublishSignal{
				err: storeio.ErrSegmentedTabletRouterCorrupt,
			}
		}
		return
	}
	for _, request := range requests {
		route, ok := router.Route(request.key)
		if !ok || route.Ref != request.route.Ref ||
			route.Bucket != request.route.Bucket ||
			route.Hash != request.route.Hash {
			for _, pending := range requests {
				pending.signal <- primaryConcurrentPublishSignal{
					err: storeio.ErrSegmentedTabletRouterCorrupt,
				}
			}
			return
		}
	}

	var results [primaryConcurrentContextLimit]primaryConcurrentPublishSignal
	published := 0
	c.primaryUnifiedSeen = true
	for index, request := range requests {
		// A group can begin below the on-disk generation ceiling but reach it
		// before every member is assigned. Stop at the largest representable
		// prefix and let the untouched suffix take the established path, which
		// owns the collection's normal generation-exhaustion result.
		if latest.root.Generation+uint64(published) >= uint64(1)<<48-1 {
			break
		}
		generation := latest.root.Generation + uint64(published) + 1
		if concurrentPrimaryReplacePublishHook != nil {
			concurrentPrimaryReplacePublishHook(
				request.route.Bucket, generation,
			)
		}
		prepared, err := c.primaryUnifiedOverlay.prepare(
			request.route.Bucket, request.route.Hash, generation,
			request.key, request.canonical, request.rawDelta, 0,
			primaryUnifiedOverlayPut, request.stableSlot,
		)
		if err != nil {
			// Record/arena pressure is global. Nothing later in this drained
			// group can create capacity, so leave this request and the suffix for
			// one coordinated exclusive fold/retry path.
			for pressure := index; pressure < len(requests); pressure++ {
				results[pressure].err = errConcurrentPrimaryPressure
			}
			break
		}
		// Link the fully initialized future-generation record before publishing
		// the group's final state. Existing readers carry an older generation and
		// filter it; no new reader can select the future generation yet.
		c.primaryUnifiedOverlay.publish(prepared)
		results[index].handled = true
		published++
	}
	if published != 0 {
		finalRoot := latest.root
		finalRoot.Generation += uint64(published)
		finalState := &fileStoreState{
			root: finalRoot, fileEnd: latest.fileEnd,
			freeHead: latest.freeHead,
		}
		// The group is one visibility cut. Overlay records retain one consecutive
		// generation per logical replacement for journal-delta replay, while the
		// router and public state need only expose the final generation.
		c.snapshotGate.Lock()
		c.beginReaderFence()
		router.AdvanceGeneration(finalRoot.Generation)
		c.pageValidator.update(finalState)
		c.publishFileState(finalState)
		c.endReaderFence()
		c.snapshotGate.Unlock()
		c.concurrentPrimaryReplaces.Add(uint64(published))
		c.concurrentPrimaryPublishGroups.Add(1)
		groupSize := uint64(published)
		for largest := c.concurrentPrimaryLargestPublishGroup.Load(); groupSize > largest &&
			!c.concurrentPrimaryLargestPublishGroup.CompareAndSwap(largest, groupSize); largest = c.concurrentPrimaryLargestPublishGroup.Load() {
		}
	}
	for index, request := range requests {
		request.signal <- results[index]
	}
}

// tryConcurrentPrimaryReplace applies the one mutation shape whose fallible
// work is leaf-local: an existing inline row replacement in an ordinary
// buffered-visible, schemaless, unindexed collection. handled=false means the
// caller must release this function's defers and enter the unchanged exclusive
// path; this function never attempts an RWMutex upgrade.
func (c *Collection) tryConcurrentPrimaryReplace(
	key, src []byte,
) (handled bool, err error) {
	pool := c.primaryConcurrentContexts
	if pool == nil {
		return false, nil
	}
	context := pool.acquire()
	defer pool.release(context)

	c.writer.RLock()
	defer c.writer.RUnlock()
	if c.closed {
		return false, ErrClosed
	}
	if failure := c.PersistenceError(); failure != nil {
		return false, failure
	}
	if !c.buffered() || c.options.RecoveryJournal ||
		c.options.Collection.Schema != nil ||
		len(c.options.indexes) != 0 || c.primaryEpoch != nil ||
		c.onlineIndexBuild.Load() || c.journalReplaying ||
		c.primaryUnifiedOverlay == nil {
		return false, nil
	}
	// Preserve the established buffered-visible reader veto. A deferred
	// Snapshot acquires its generation lease while holding writer exclusively,
	// so this sample is stable for snapshot leases for the lifetime of our
	// shared hold: an existing lease declines here, and a new one cannot be
	// pinned until RUnlock. Direct read epochs are a conservative sampled veto;
	// a reader arriving after the sample remains safe through generation-filtered
	// overlay publication and the publisher's reader fence.
	if c.anyActiveReaders() {
		return false, nil
	}
	if len(key) == 0 || len(key) > c.options.MaxKeyBytes ||
		len(key) > storeio.CommonPrimaryLeafMaxKeyBytes {
		return false, ErrKeyTooLarge
	}
	if len(src) == 0 || len(src) > c.options.MaxDocumentBytes {
		return false, ErrDocumentTooLarge
	}
	// Values that cannot possibly enter the inline overlay are left to the
	// established path without paying for a parallel tape build.
	if len(src) > c.options.InlineValueBytes || len(src) > pool.rawLimit {
		return false, nil
	}
	canonical, eligible, err := context.canonicalize(src)
	if err != nil {
		return false, err
	}
	if !eligible {
		return false, nil
	}
	if len(canonical) > c.options.InlineValueBytes ||
		len(key)+len(canonical) > len(c.primaryUnifiedOverlay.arena) {
		return false, nil
	}

	state := c.state.Load()
	if state == nil || state.root.PrimaryRoot == (storeio.PageRef{}) {
		return false, ErrClosed
	}
	router := c.primaryRouter.Load()
	if router == nil {
		return false, storeio.ErrSegmentedTabletRouterCorrupt
	}
	route, ok := router.Route(key)
	if !ok || route.Ref == (storeio.PageRef{}) {
		return false, storeio.ErrSegmentedTabletRouterCorrupt
	}

	stripe := &c.primaryConcurrentStripes[primaryConcurrentStripeIndex(route.Bucket)]
	if !stripe.mu.TryLock() {
		// A same-bucket (or rare hash-collision) waiter is cheaper on the mature
		// exclusive path than parked here holding a shared writer admission and a
		// canonicalization context. Dropping RLock through the defer before the
		// fallback also lets Go's RWMutex writer preference stop a hot-bucket
		// stampede from starving that path.
		return false, nil
	}
	defer stripe.mu.Unlock()

	// A prior writer for this bucket publishes state before releasing the same
	// stripe. Reload after acquiring it so overlay lookup includes that writer;
	// unrelated buckets may continue to advance the global generation without
	// invalidating this bucket-local size calculation.
	state = c.state.Load()
	if state == nil {
		return false, ErrClosed
	}
	leafLease, err := router.AcquireLeaf(c.cache, route)
	if err != nil {
		return false, err
	}
	page := leafLease.Page()
	if storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafUnified {
		leafLease.Release()
		return false, nil
	}
	leaf, ok := storeio.AdmittedCommonPrimaryUnifiedLeaf(
		page, c.storeID, route.Bucket, c.primaryLeafBounds(state),
	)
	if !ok {
		leafLease.Release()
		return false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	baseSlot, body, overflow, baseFound :=
		leaf.LookupBodySlotHashed(route.Hash, key)
	current, disposition, overlaySlot := c.primaryUnifiedOverlay.lookup(
		route.Bucket, route.Hash, key, state.root.Generation,
	)
	oldLen := 0
	stableSlot := baseSlot
	switch disposition {
	case primaryUnifiedOverlayValue:
		oldLen = len(current)
		stableSlot = overlaySlot
	case primaryUnifiedOverlayDeleted:
		leafLease.Release()
		return false, nil
	case primaryUnifiedOverlayMissing:
		if !baseFound || overflow {
			leafLease.Release()
			return false, nil
		}
		oldLen = leaf.AdmittedRowBodyLen(body)
	default:
		leafLease.Release()
		return false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	pendingRaw, pendingRows :=
		c.primaryUnifiedOverlay.pendingBucketDeltas(route.Bucket)
	rawDelta := len(canonical) - oldLen
	fits := storeio.CommonPrimaryUnifiedTrivialFits(
		leaf.Len()+pendingRows,
		leaf.TrivialContentBytes()+pendingRaw+rawDelta,
	)
	leafLease.Release()
	if !fits {
		return false, nil
	}
	if concurrentPrimaryReplaceStagedHook != nil {
		concurrentPrimaryReplaceStagedHook(route.Bucket)
	}

	request := &context.publish
	request.key = key
	request.canonical = canonical
	request.route = route
	request.rawDelta = rawDelta
	request.stableSlot = stableSlot
	handled, publishErr := c.primaryOverlayPublish.submit(c, request)
	request.key = nil
	request.canonical = nil
	request.route = storeio.ResidentPrimaryRoute{}
	request.rawDelta = 0
	request.stableSlot = 0
	return handled, publishErr
}
