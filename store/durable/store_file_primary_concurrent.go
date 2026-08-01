package durable

import (
	"errors"
	"math/bits"
	"sync"
	"sync/atomic"
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

	concurrentPrimaryReplaceStagedHook   func(storeio.BucketID)
	concurrentPrimaryReplacePublishHook  func(storeio.BucketID, uint64)
	concurrentPrimaryStripeContendedHook func(storeio.BucketID)
	concurrentPrimaryExclusiveWaitHook   func(string)
	// concurrentPrimaryPutCanonicalizeHook brackets only the writer-private
	// canonicalization phase. Tests use it to prove an exclusive writer can
	// pass while that phase is in flight; production leaves it nil.
	concurrentPrimaryPutCanonicalizeHook func(done bool)
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

func primaryConcurrentDocumentCount(
	base uint64, delta int64,
) (uint64, bool) {
	if delta < 0 {
		magnitude := uint64(-(delta + 1)) + 1
		if magnitude > base {
			return 0, false
		}
		return base - magnitude, true
	}
	magnitude := uint64(delta)
	if magnitude > ^uint64(0)-base {
		return 0, false
	}
	return base + magnitude, true
}

// primaryConcurrentContext owns every mutable byte touched while one caller
// validates and canonicalizes a candidate replacement. The collection's
// historical scratch remains exclusive-writer-owned for every fallback lane.
type primaryConcurrentContext struct {
	index      []vibejson.IndexEntry
	patchSpans []storeio.UnifiedTokenSpan
	canonical  []byte
	workspace  storeio.CanonicalWorkspace
	publish    primaryConcurrentPublishRequest
	poolSlot   uint8
}

func (c *primaryConcurrentContext) canonicalize(
	src []byte,
) (
	canonical []byte,
	spanned storeio.CanonicalSpanIndex,
	eligible bool,
	err error,
) {
	index, err := vibejson.BuildIndex(src, c.index[:cap(c.index)])
	if errors.Is(err, document.ErrIndexFull) {
		return nil, storeio.CanonicalSpanIndex{}, false, nil
	}
	if err != nil {
		return nil, storeio.CanonicalSpanIndex{}, false, err
	}
	c.index = index.Entries
	if canonicalIndex, ok := storeio.CanonicalSpanIndexOf(
		index, &c.workspace, c.patchSpans[:0],
	); ok {
		return src, canonicalIndex, true, nil
	}
	out, canonicalIndex, err := storeio.AppendCanonicalIndexedSpans(
		c.canonical[:0], index, &c.workspace, c.patchSpans[:0],
	)
	if errors.Is(err, document.ErrIndexFull) {
		return nil, storeio.CanonicalSpanIndex{}, false, nil
	}
	if err != nil {
		return nil, storeio.CanonicalSpanIndex{}, false, err
	}
	if cap(out) > cap(c.canonical) {
		// The 2x source admission bound should make this unreachable. Do not
		// retain an unexpectedly grown buffer if the canonical encoder evolves.
		return nil, storeio.CanonicalSpanIndex{}, false, nil
	}
	c.canonical = out
	return out, canonicalIndex, true, nil
}

// primaryConcurrentContextPool is fixed at collection construction. Every
// context receives its maximum admitted tape, span, canonical, and workspace
// capacity up front and is then reused; unlike sync.Pool, GC cannot silently
// discard and recreate it.
type primaryConcurrentContextPool struct {
	contexts []primaryConcurrentContext
	// free contains one bit per context. The ordinary path claims a bit with a
	// bounded CAS attempt and never enters a channel or mutex. A caller that
	// observes exhaustion joins wait below; later fast callers also join that
	// lane so an already-blocked caller cannot be starved by barging.
	free     atomic.Uint32
	waiters  atomic.Uint32
	waitMu   sync.Mutex
	wait     sync.Cond
	rawLimit int
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
		contexts: make([]primaryConcurrentContext, count),
		rawLimit: rawLimit,
	}
	pool.wait.L = &pool.waitMu
	free := ^uint32(0)
	if count < primaryConcurrentContextLimit {
		free = uint32(1)<<uint(count) - 1
	}
	pool.free.Store(free)
	for i := range pool.contexts {
		pool.contexts[i].poolSlot = uint8(i)
		pool.contexts[i].index = make(
			[]vibejson.IndexEntry, 0, indexEntries,
		)
		pool.contexts[i].patchSpans = make(
			[]storeio.UnifiedTokenSpan, 0, 2*indexEntries,
		)
		pool.contexts[i].canonical = make([]byte, 0, 2*rawLimit)
		pool.contexts[i].workspace = storeio.NewCanonicalWorkspace(
			indexEntries, rawLimit,
		)
		pool.contexts[i].publish.signal = make(
			chan primaryConcurrentPublishSignal, 1,
		)
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
		bytes += uint64(cap(context.patchSpans)) *
			uint64(unsafe.Sizeof(storeio.UnifiedTokenSpan{}))
		bytes += uint64(cap(context.canonical))
		bytes += context.workspace.CapacityBytes()
	}
	return bytes
}

func (p *primaryConcurrentContextPool) acquire() *primaryConcurrentContext {
	if p == nil {
		return nil
	}
	// At most one claim can win per context. Retrying once per fixed slot absorbs
	// every claimant already in flight without an unbounded CAS spin; genuine
	// exhaustion or a sustained collision then enters the sleeping lane.
	for attempt := 0; attempt < primaryConcurrentContextLimit &&
		p.waiters.Load() == 0; attempt++ {
		available := p.free.Load()
		if available == 0 {
			break
		}
		slot := uint32(bits.TrailingZeros32(available))
		bit := uint32(1) << slot
		if p.free.CompareAndSwap(available, available&^bit) {
			return &p.contexts[slot]
		}
	}

	p.waitMu.Lock()
	p.waiters.Add(1)
	for {
		available := p.free.Load()
		if available != 0 {
			slot := uint32(bits.TrailingZeros32(available))
			bit := uint32(1) << slot
			if p.free.CompareAndSwap(available, available&^bit) {
				p.waiters.Add(^uint32(0))
				p.waitMu.Unlock()
				return &p.contexts[slot]
			}
			// A fast claimant that passed the waiter check before we published
			// it made progress. Re-read the predicate under waitMu; once those
			// bounded in-flight claims finish, fast callers must join this lane.
			continue
		}
		p.wait.Wait()
	}
}

func (p *primaryConcurrentContextPool) release(
	context *primaryConcurrentContext,
) {
	if p == nil || context == nil {
		return
	}
	slot := uint32(context.poolSlot)
	if slot >= uint32(len(p.contexts)) || &p.contexts[slot] != context {
		panic("vibejson: foreign concurrent primary context release")
	}
	bit := uint32(1) << slot
	if old := p.free.Or(bit); old&bit != 0 {
		panic("vibejson: duplicate concurrent primary context release")
	}
	if p.waiters.Load() != 0 {
		// Locking around Signal closes the check/sleep race: a waiter either
		// observes the returned bit while holding waitMu or is already queued
		// when this signal is issued.
		p.waitMu.Lock()
		p.wait.Signal()
		p.waitMu.Unlock()
	}
}

type primaryConcurrentPublishSignal struct {
	leader  bool
	handled bool
	err     error
}

type primaryConcurrentPublishRequest struct {
	key         []byte
	canonical   []byte
	route       storeio.ResidentPrimaryRoute
	scalarPatch storeio.CommonPrimaryUnifiedScalarPatch
	rawDelta    int
	countDelta  int
	kind        uint8
	stableSlot  uint8
	fixedExtent bool
	fillsEmpty  bool
	signal      chan primaryConcurrentPublishSignal
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

	collection.publishConcurrentPrimaryMutations(p.batch[:count])
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

func (c *Collection) publishConcurrentPrimaryMutations(
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

	var results [primaryConcurrentContextLimit]primaryConcurrentPublishSignal
	published := 0
	c.primaryUnifiedSeen = true
	journalCovered := c.journalDeltaGeneration.Load()
	reuseCandidate := false
	for _, request := range requests {
		if request.kind == primaryUnifiedOverlayPut &&
			request.countDelta == 0 && request.rawDelta == 0 &&
			c.primaryUnifiedOverlay.canReuseSameSizeArena(
				request.route.Bucket, request.route.Hash,
				request.key, request.canonical, request.stableSlot,
				journalCovered,
			) {
			reuseCandidate = true
			break
		}
	}
	// Overwriting a journal-covered arena value is safe only after excluding
	// every reader of the older visible generation. Snapshot leases are frozen
	// by snapshotGate and new direct epochs are diverted by the fence. Keep both
	// through state publication so no reader can observe the overwritten old
	// bytes under the old generation.
	reuseFence := false
	if reuseCandidate {
		c.snapshotGate.Lock()
		c.beginReaderFence()
		if !c.anyActiveReaders() {
			reuseFence = true
		} else {
			c.endReaderFence()
			c.snapshotGate.Unlock()
		}
	}
	documentDelta := int64(0)
	replacements := uint64(0)
	for index, request := range requests {
		// A group can begin below the on-disk generation ceiling but reach it
		// before every member is assigned. Stop at the largest representable
		// prefix and let the untouched suffix take the established path, which
		// owns the collection's normal generation-exhaustion result.
		if latest.root.Generation+uint64(published) >= uint64(1)<<48-1 {
			break
		}
		generation := latest.root.Generation + uint64(published) + 1
		nextDocumentDelta := documentDelta + int64(request.countDelta)
		if _, valid := primaryConcurrentDocumentCount(
			latest.root.DocumentCount, nextDocumentDelta,
		); !valid {
			for invalid := index; invalid < len(requests); invalid++ {
				results[invalid].err = storeio.ErrInvalidWrite
			}
			break
		}
		if concurrentPrimaryReplacePublishHook != nil {
			concurrentPrimaryReplacePublishHook(
				request.route.Bucket, generation,
			)
		}
		var prepared primaryUnifiedOverlayPrepared
		reused := false
		leafBytes := c.primaryUnifiedOverlay.maxLeafBytes
		if request.fixedExtent {
			leafBytes = request.route.Ref.Length
		}
		if reuseFence && request.kind == primaryUnifiedOverlayPut &&
			request.countDelta == 0 && request.rawDelta == 0 {
			prepared, reused =
				c.primaryUnifiedOverlay.prepareSameSizeArenaReuse(
					request.route.Bucket, request.route.Hash, generation,
					request.key, request.canonical, request.stableSlot,
					journalCovered, leafBytes, request.scalarPatch,
				)
		}
		var err error
		if !reused {
			prepared, err = c.primaryUnifiedOverlay.prepareWithLeafReservation(
				request.route.Bucket, request.route.Hash, generation,
				request.key, request.canonical, request.rawDelta,
				request.countDelta, request.kind, request.stableSlot,
				request.route.Ref.Length, !request.fixedExtent,
				request.scalarPatch,
			)
		}
		if err != nil {
			// Record/arena pressure is global. Nothing later in this drained
			// group can create capacity, so leave this request and the suffix for
			// one coordinated exclusive fold/retry path. Invariant and validation
			// errors are not capacity signals: preserve them so callers fail closed
			// instead of performing an unrelated fold and hiding the original cause.
			publishErr := err
			if errors.Is(err, storeio.ErrPageCachePinned) {
				publishErr = errConcurrentPrimaryPressure
			}
			for pending := index; pending < len(requests); pending++ {
				results[pending].err = publishErr
			}
			break
		}
		// Link the fully initialized future-generation record before publishing
		// the group's final state. Existing readers carry an older generation and
		// filter it; no new reader can select the future generation yet.
		c.primaryUnifiedOverlay.publish(prepared)
		results[index].handled = true
		documentDelta = nextDocumentDelta
		if request.kind == primaryUnifiedOverlayPut &&
			request.countDelta == 0 {
			replacements++
		}
		published++
	}
	if published != 0 {
		finalRoot := latest.root
		finalRoot.Generation += uint64(published)
		finalRoot.DocumentCount, _ = primaryConcurrentDocumentCount(
			latest.root.DocumentCount, documentDelta,
		)
		finalState := &fileStoreState{
			root: finalRoot, fileEnd: latest.fileEnd,
			freeHead: latest.freeHead,
		}
		// The group is one visibility cut. Overlay records retain one consecutive
		// generation per logical replacement for journal-delta replay, while the
		// router and public state need only expose the final generation.
		if !reuseFence {
			c.snapshotGate.Lock()
			c.beginReaderFence()
		}
		router.AdvanceGeneration(finalRoot.Generation)
		for _, request := range requests[:published] {
			if request.fillsEmpty && router.ClearEmpty(request.route) {
				c.removePrimaryEmptyLeaf()
			}
		}
		c.pageValidator.update(finalState)
		c.publishFileState(finalState)
		c.endReaderFence()
		c.snapshotGate.Unlock()
		reuseFence = false
		if replacements != 0 {
			c.concurrentPrimaryReplaces.Add(replacements)
		}
		groupSize := uint64(published)
		c.concurrentPrimaryPublishGroups.Add(1)
		for largest := c.concurrentPrimaryLargestPublishGroup.Load(); groupSize > largest &&
			!c.concurrentPrimaryLargestPublishGroup.CompareAndSwap(largest, groupSize); largest = c.concurrentPrimaryLargestPublishGroup.Load() {
		}
	}
	if reuseFence {
		c.endReaderFence()
		c.snapshotGate.Unlock()
	}
	for index, request := range requests {
		request.signal <- results[index]
	}
}

// tryConcurrentPrimaryPut applies every Put whose fallible work is leaf-local:
// an inline replacement, a resurrection, or an insert that can claim one free
// stable envelope slot and still satisfy the leaf's exact trivial-content
// bound. Splits, overflow, and slot exhaustion remain structural fallbacks.
// handled=false means the caller must release this function's defers and enter
// that unchanged lane; this function never attempts an RWMutex upgrade.
func (c *Collection) tryConcurrentPrimaryPut(
	key, src []byte,
) (handled, created bool, err error) {
	pool := c.primaryConcurrentContexts
	if pool == nil {
		return false, false, nil
	}
	context := pool.acquire()
	defer pool.release(context)

	// These bounds and the context belong exclusively to this caller and are
	// immutable for the collection's lifetime. Keep validation and JSON tape /
	// canonical construction outside writer so an exclusive checkpoint or
	// structural writer cannot be convoyed behind CPU-private work. Any result
	// remains provisional until the locked closed, persistence, and lane checks
	// below have re-established their public error precedence.
	eligible := true
	var preflightErr error
	var canonical []byte
	var canonicalIndex storeio.CanonicalSpanIndex
	switch {
	case len(key) == 0 || len(key) > c.options.MaxKeyBytes ||
		len(key) > storeio.CommonPrimaryLeafMaxKeyBytes:
		preflightErr = ErrKeyTooLarge
	case len(src) == 0 || len(src) > c.options.MaxDocumentBytes:
		preflightErr = ErrDocumentTooLarge
	case len(src) > c.options.InlineValueBytes || len(src) > pool.rawLimit:
		// Values that cannot possibly enter the inline overlay are left to the
		// established path without paying for a parallel tape build.
		eligible = false
	default:
		if hook := concurrentPrimaryPutCanonicalizeHook; hook == nil {
			canonical, canonicalIndex, eligible, preflightErr =
				context.canonicalize(src)
		} else {
			hook(false)
			canonical, canonicalIndex, eligible, preflightErr =
				context.canonicalize(src)
			hook(true)
		}
		if preflightErr == nil && eligible &&
			len(canonical) > c.options.InlineValueBytes {
			eligible = false
		}
	}

	c.writer.RLock()
	defer c.writer.RUnlock()
	if c.closed {
		return false, false, ErrClosed
	}
	if failure := c.PersistenceError(); failure != nil {
		return false, false, failure
	}
	if !c.buffered() || c.options.RecoveryJournal ||
		c.options.Collection.Schema != nil ||
		len(c.options.indexes) != 0 || c.primaryEpoch != nil ||
		c.onlineIndexBuild.Load() || c.journalReplaying ||
		c.primaryUnifiedOverlay == nil {
		return false, false, nil
	}
	if preflightErr != nil {
		return false, false, preflightErr
	}
	if !eligible {
		return false, false, nil
	}
	// Readers do not veto append-only overlay publication: they filter immutable
	// records by generation, and the publisher fences router/state advancement.
	// The only destructive optimization, arena reuse, independently raises a
	// reader fence and declines while either a Snapshot lease or direct epoch is
	// active. A long-lived Snapshot may therefore consume the bounded append
	// window, but pressure still falls back through the established fenced fold.
	if len(key)+len(canonical) > len(c.primaryUnifiedOverlay.arena) {
		return false, false, nil
	}

	state := c.state.Load()
	if state == nil || state.root.PrimaryRoot == (storeio.PageRef{}) {
		return false, false, ErrClosed
	}
	router := c.primaryRouter.Load()
	if router == nil {
		return false, false, storeio.ErrSegmentedTabletRouterCorrupt
	}
	route, ok := router.Route(key)
	if !ok || route.Ref == (storeio.PageRef{}) {
		return false, false, storeio.ErrSegmentedTabletRouterCorrupt
	}

	stripe := &c.primaryConcurrentStripes[primaryConcurrentStripeIndex(route.Bucket)]
	if !stripe.mu.TryLock() {
		// Wait only for this bounded leaf stripe. Falling back to the exclusive
		// writer here makes Go's RWMutex writer preference convoy every unrelated
		// shared writer behind a single same-leaf collision.
		if concurrentPrimaryStripeContendedHook != nil {
			concurrentPrimaryStripeContendedHook(route.Bucket)
		}
		stripe.mu.Lock()
	}
	defer stripe.mu.Unlock()

	// A prior writer for this bucket publishes state before releasing the same
	// stripe. Reload after acquiring it so overlay lookup includes that writer;
	// unrelated buckets may continue to advance the global generation without
	// invalidating this bucket-local size calculation.
	state = c.state.Load()
	if state == nil {
		return false, false, ErrClosed
	}
	leafLease, err := router.AcquireLeaf(c.cache, route)
	if err != nil {
		return false, false, err
	}
	page := leafLease.Page()
	if storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafUnified {
		leafLease.Release()
		return false, false, nil
	}
	leaf, ok := storeio.AdmittedCommonPrimaryUnifiedLeaf(
		page, c.storeID, route.Bucket, c.primaryLeafBounds(state),
	)
	if !ok {
		leafLease.Release()
		return false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	baseSlot, body, overflow, baseFound :=
		leaf.LookupBodySlotHashed(route.Hash, key)
	current, disposition, overlaySlot := c.primaryUnifiedOverlay.lookup(
		route.Bucket, route.Hash, key, state.root.Generation,
	)
	pendingRaw, pendingRows :=
		c.primaryUnifiedOverlay.pendingBucketDeltas(route.Bucket)
	leafWasEmpty := leaf.Len()+pendingRows == 0
	oldLen := 0
	stableSlot := baseSlot
	countDelta := 0
	switch disposition {
	case primaryUnifiedOverlayValue:
		oldLen = len(current)
		stableSlot = overlaySlot
	case primaryUnifiedOverlayDeleted:
		created = true
		countDelta = 1
		stableSlot = overlaySlot
		overflow = false
	case primaryUnifiedOverlayMissing:
		if baseFound {
			if overflow {
				leafLease.Release()
				return false, false, nil
			}
			oldLen = leaf.AdmittedRowBodyLen(body)
			break
		}
		var slotOK bool
		stableSlot, slotOK = leaf.ChooseInsertSlotHashed(
			route.Hash,
			c.primaryUnifiedOverlay.pendingInsertSlots(route.Bucket),
		)
		if !slotOK {
			leafLease.Release()
			return false, false, nil
		}
		created = true
		countDelta = 1
	default:
		leafLease.Release()
		return false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	rawDelta := len(canonical) - oldLen
	if created {
		rawDelta = storeio.CommonPrimaryUnifiedInsertedTrivialBytes(
			key, len(canonical),
		)
		if rawDelta == 0 {
			leafLease.Release()
			return false, false, storeio.ErrInvalidWrite
		}
	}
	fits := storeio.CommonPrimaryUnifiedTrivialFits(
		leaf.Len()+pendingRows+countDelta,
		leaf.TrivialContentBytes()+pendingRaw+rawDelta,
	)
	fixedExtent := false
	var scalarPatch storeio.CommonPrimaryUnifiedScalarPatch
	if fits && baseFound && !overflow {
		var resolved bool
		scalarPatch, fixedExtent, resolved, err =
			leaf.PatchStableCanonicalReplacementScalarPatch(
				key, stableSlot, canonicalIndex,
			)
		if err == nil && !resolved {
			fixedExtent, err = leaf.PatchStableCanonicalReplacementKeepsExtent(
				key, stableSlot, canonicalIndex,
				context.index, &context.workspace,
			)
		}
		if err != nil {
			leafLease.Release()
			return false, false, err
		}
	}
	leafLease.Release()
	if !fits {
		return false, false, nil
	}
	if concurrentPrimaryReplaceStagedHook != nil {
		concurrentPrimaryReplaceStagedHook(route.Bucket)
	}

	request := &context.publish
	request.key = key
	request.canonical = canonical
	request.route = route
	request.scalarPatch = scalarPatch
	request.rawDelta = rawDelta
	request.countDelta = countDelta
	request.kind = primaryUnifiedOverlayPut
	request.stableSlot = stableSlot
	request.fixedExtent = fixedExtent
	request.fillsEmpty = created && leafWasEmpty
	handled, publishErr := c.primaryOverlayPublish.submit(c, request)
	request.key = nil
	request.canonical = nil
	request.route = storeio.ResidentPrimaryRoute{}
	request.scalarPatch = storeio.CommonPrimaryUnifiedScalarPatch{}
	request.rawDelta = 0
	request.countDelta = 0
	request.kind = 0
	request.stableSlot = 0
	request.fixedExtent = false
	request.fillsEmpty = false
	return handled, handled && created, publishErr
}

// tryConcurrentPrimaryDelete publishes a tombstone for an existing inline row
// while retaining its stable slot for a later resurrection. Deleting the final
// row in a leaf deliberately falls back: empty-route marking and eager leaf
// reclaim remain structural operations fenced by the exclusive writer.
func (c *Collection) tryConcurrentPrimaryDelete(
	key []byte,
) (handled, deleted bool, err error) {
	pool := c.primaryConcurrentContexts
	if pool == nil {
		return false, false, nil
	}
	context := pool.acquire()
	defer pool.release(context)

	c.writer.RLock()
	defer c.writer.RUnlock()
	if c.closed {
		return false, false, ErrClosed
	}
	if failure := c.PersistenceError(); failure != nil {
		return false, false, failure
	}
	if !c.buffered() || c.options.RecoveryJournal ||
		c.options.Collection.Schema != nil ||
		len(c.options.indexes) != 0 || c.primaryEpoch != nil ||
		c.onlineIndexBuild.Load() || c.journalReplaying ||
		c.primaryUnifiedOverlay == nil {
		return false, false, nil
	}
	// See tryConcurrentPrimaryPut: immutable tombstones may overlap both direct
	// point-read epochs and long-lived Snapshot leases.
	if len(key) == 0 || len(key) > c.options.MaxKeyBytes ||
		len(key) > storeio.CommonPrimaryLeafMaxKeyBytes {
		return false, false, ErrKeyTooLarge
	}
	if len(key) > len(c.primaryUnifiedOverlay.arena) {
		return false, false, nil
	}

	state := c.state.Load()
	if state == nil || state.root.PrimaryRoot == (storeio.PageRef{}) {
		return false, false, ErrClosed
	}
	router := c.primaryRouter.Load()
	if router == nil {
		return false, false, storeio.ErrSegmentedTabletRouterCorrupt
	}
	route, ok := router.Route(key)
	if !ok || route.Ref == (storeio.PageRef{}) {
		return false, false, storeio.ErrSegmentedTabletRouterCorrupt
	}

	stripe := &c.primaryConcurrentStripes[primaryConcurrentStripeIndex(route.Bucket)]
	if !stripe.mu.TryLock() {
		if concurrentPrimaryStripeContendedHook != nil {
			concurrentPrimaryStripeContendedHook(route.Bucket)
		}
		stripe.mu.Lock()
	}
	defer stripe.mu.Unlock()

	state = c.state.Load()
	if state == nil {
		return false, false, ErrClosed
	}
	leafLease, err := router.AcquireLeaf(c.cache, route)
	if err != nil {
		return false, false, err
	}
	page := leafLease.Page()
	if storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafUnified {
		leafLease.Release()
		return false, false, nil
	}
	leaf, ok := storeio.AdmittedCommonPrimaryUnifiedLeaf(
		page, c.storeID, route.Bucket, c.primaryLeafBounds(state),
	)
	if !ok {
		leafLease.Release()
		return false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	baseSlot, body, overflow, baseFound :=
		leaf.LookupBodySlotHashed(route.Hash, key)
	current, disposition, overlaySlot := c.primaryUnifiedOverlay.lookup(
		route.Bucket, route.Hash, key, state.root.Generation,
	)
	stableSlot := baseSlot
	oldLen := 0
	switch disposition {
	case primaryUnifiedOverlayValue:
		stableSlot = overlaySlot
		oldLen = len(current)
	case primaryUnifiedOverlayDeleted:
		leafLease.Release()
		return true, false, nil
	case primaryUnifiedOverlayMissing:
		if !baseFound {
			leafLease.Release()
			return true, false, nil
		}
		if overflow {
			leafLease.Release()
			return false, false, nil
		}
		oldLen = leaf.AdmittedRowBodyLen(body)
	default:
		leafLease.Release()
		return false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}

	pendingRaw, pendingRows :=
		c.primaryUnifiedOverlay.pendingBucketDeltas(route.Bucket)
	currentRows := leaf.Len() + pendingRows
	// Keep the empty-leaf router transition and reclaim on the mature path.
	if currentRows <= 1 {
		leafLease.Release()
		return false, false, nil
	}
	rawDelta := -storeio.CommonPrimaryUnifiedInsertedTrivialBytes(key, oldLen)
	if rawDelta == 0 || !storeio.CommonPrimaryUnifiedTrivialFits(
		currentRows-1,
		leaf.TrivialContentBytes()+pendingRaw+rawDelta,
	) {
		leafLease.Release()
		return false, false, nil
	}
	leafLease.Release()
	if concurrentPrimaryReplaceStagedHook != nil {
		concurrentPrimaryReplaceStagedHook(route.Bucket)
	}

	request := &context.publish
	request.key = key
	request.canonical = nil
	request.route = route
	request.scalarPatch = storeio.CommonPrimaryUnifiedScalarPatch{}
	request.rawDelta = rawDelta
	request.countDelta = -1
	request.kind = primaryUnifiedOverlayDelete
	request.stableSlot = stableSlot
	request.fixedExtent = false
	request.fillsEmpty = false
	handled, publishErr := c.primaryOverlayPublish.submit(c, request)
	request.key = nil
	request.canonical = nil
	request.route = storeio.ResidentPrimaryRoute{}
	request.scalarPatch = storeio.CommonPrimaryUnifiedScalarPatch{}
	request.rawDelta = 0
	request.countDelta = 0
	request.kind = 0
	request.stableSlot = 0
	request.fixedExtent = false
	request.fillsEmpty = false
	return handled, handled, publishErr
}
