package durable

import (
	"bytes"
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
	primaryConcurrentPoolClosed      = uint64(1) << 63
	primaryConcurrentContextMask     = uint64(1)<<primaryConcurrentContextLimit - 1
)

// Hooks are package-private deterministic test seams. Production leaves them
// nil, so the hot path pays one predictable nil branch at each boundary.
var (
	errConcurrentPrimaryPressure = errors.New("vibedb: concurrent primary overlay pressure")

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

// primaryConcurrentContext owns every mutable byte touched while one caller
// validates and canonicalizes a candidate replacement. The collection's
// historical scratch remains exclusive-writer-owned for every fallback lane.
type primaryConcurrentContext struct {
	index      []vibejson.IndexEntry
	patchSpans []storeio.UnifiedTokenSpan
	canonical  []byte
	value      []byte
	workspace  storeio.CanonicalWorkspace
	publish    primaryConcurrentPublishRequest
	poolSlot   uint8
}

func (c *primaryConcurrentContext) canonicalize(
	src []byte, options document.IndexOptions,
) (
	canonical []byte,
	spanned storeio.CanonicalSpanIndex,
	eligible bool,
	err error,
) {
	index, err := vibejson.BuildIndexOptions(
		src, c.index[:cap(c.index)], options,
	)
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
	// contextOnce defers the sizeable per-caller JSON/canonical workspaces
	// until an eligible mutation first claims the lane. initialized publishes
	// those immutable slice/channel headers to lock-free Stats readers.
	contextOnce  sync.Once
	initialized  atomic.Bool
	indexEntries int
	// free contains one bit per context. The ordinary path claims a bit with a
	// bounded CAS attempt and never enters a channel or mutex. A caller that
	// observes exhaustion joins wait below; later fast callers also join that
	// lane so an already-blocked caller cannot be starved by barging.
	// free packs the context bits with one sticky close bit. Close invalidates
	// every in-flight CAS expected word, so acquisition needs no separate
	// pre/post close loads around its one claim CAS.
	free     atomic.Uint64
	allFree  uint64
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
		rawLimit: rawLimit, indexEntries: indexEntries,
	}
	pool.wait.L = &pool.waitMu
	free := primaryConcurrentContextMask
	if count < primaryConcurrentContextLimit {
		free = uint64(1)<<uint(count) - 1
	}
	pool.free.Store(free)
	pool.allFree = free
	for i := range pool.contexts {
		pool.contexts[i].poolSlot = uint8(i)
	}
	return pool
}

func (p *primaryConcurrentContextPool) ensureContexts() bool {
	if p == nil {
		return false
	}
	p.contextOnce.Do(func() {
		if p.free.Load()&primaryConcurrentPoolClosed != 0 {
			return
		}
		for i := range p.contexts {
			context := &p.contexts[i]
			context.index = make(
				[]vibejson.IndexEntry, 0, p.indexEntries,
			)
			context.patchSpans = make(
				[]storeio.UnifiedTokenSpan, 0, 2*p.indexEntries,
			)
			context.canonical = make([]byte, 0, 2*p.rawLimit)
			context.value = make([]byte, 0, p.rawLimit)
			context.workspace = storeio.NewCanonicalWorkspace(
				p.indexEntries, p.rawLimit,
			)
			context.publish.signal = make(
				chan primaryConcurrentPublishSignal, 1,
			)
		}
		p.initialized.Store(true)
	})
	return p.initialized.Load()
}

func (p *primaryConcurrentContextPool) capacityBytes() uint64 {
	if p == nil {
		return 0
	}
	bytes := uint64(cap(p.contexts)) *
		uint64(unsafe.Sizeof(primaryConcurrentContext{}))
	if !p.initialized.Load() {
		return bytes
	}
	for i := range p.contexts {
		context := &p.contexts[i]
		bytes += uint64(cap(context.index)) *
			uint64(unsafe.Sizeof(vibejson.IndexEntry{}))
		bytes += uint64(cap(context.patchSpans)) *
			uint64(unsafe.Sizeof(storeio.UnifiedTokenSpan{}))
		bytes += uint64(cap(context.canonical))
		bytes += uint64(cap(context.value))
		bytes += context.workspace.CapacityBytes()
	}
	return bytes
}

func (p *primaryConcurrentContextPool) acquire() *primaryConcurrentContext {
	if !p.ensureContexts() {
		return nil
	}
	// At most one claim can win per context. Retrying once per fixed slot absorbs
	// every claimant already in flight without an unbounded CAS spin; genuine
	// exhaustion or a sustained collision then enters the sleeping lane.
	for attempt := 0; attempt < primaryConcurrentContextLimit &&
		p.waiters.Load() == 0; attempt++ {
		available := p.free.Load()
		if available&primaryConcurrentPoolClosed != 0 {
			return nil
		}
		if available&p.allFree == 0 {
			break
		}
		slot := uint32(bits.TrailingZeros64(available))
		bit := uint64(1) << slot
		if p.free.CompareAndSwap(available, available&^bit) {
			return &p.contexts[slot]
		}
	}

	p.waitMu.Lock()
	p.waiters.Add(1)
	for {
		available := p.free.Load()
		if available&primaryConcurrentPoolClosed != 0 {
			p.waiters.Add(^uint32(0))
			p.waitMu.Unlock()
			return nil
		}
		if available&p.allFree != 0 {
			slot := uint32(bits.TrailingZeros64(available))
			bit := uint64(1) << slot
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

// close rejects future claims and wakes every exhausted-pool waiter. Existing
// holders drain naturally; waitDrained is called without writer held before
// any collection-owned resources are released.
func (p *primaryConcurrentContextPool) close() {
	if p == nil {
		return
	}
	if old := p.free.Or(primaryConcurrentPoolClosed); old&primaryConcurrentPoolClosed != 0 {
		return
	}
	p.waitMu.Lock()
	p.wait.Broadcast()
	p.waitMu.Unlock()
}

func (p *primaryConcurrentContextPool) waitDrained() {
	if p == nil {
		return
	}
	p.waitMu.Lock()
	for p.free.Load()&p.allFree != p.allFree {
		p.wait.Wait()
	}
	p.waitMu.Unlock()
}

func (p *primaryConcurrentContextPool) release(
	context *primaryConcurrentContext,
) {
	if p == nil || context == nil {
		return
	}
	slot := uint32(context.poolSlot)
	if slot >= uint32(len(p.contexts)) || &p.contexts[slot] != context {
		panic("vibedb: foreign concurrent primary context release")
	}
	bit := uint64(1) << slot
	if old := p.free.Or(bit); old&bit != 0 {
		panic("vibedb: duplicate concurrent primary context release")
	}
	if p.waiters.Load() != 0 ||
		p.free.Load()&primaryConcurrentPoolClosed != 0 {
		// Locking around Signal closes the check/sleep race: a waiter either
		// observes the returned bit while holding waitMu or is already queued
		// when this signal is issued.
		p.waitMu.Lock()
		p.wait.Broadcast()
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
	latestView, logicalOK := c.writerLogicalView()
	latest := latestView.state
	router := c.primaryRouter.Load()
	if !logicalOK || latest == nil || router == nil ||
		router.Generation() != latestView.generation {
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
	// The signed delta remains relative to the unchanged physical state across
	// every allocation-free publication in this fold window.
	documentDelta := latestView.delta
	replacements := uint64(0)
	for index, request := range requests {
		// A group can begin below the on-disk generation ceiling but reach it
		// before every member is assigned. Stop at the largest representable
		// prefix and let the untouched suffix take the established path, which
		// owns the collection's normal generation-exhaustion result.
		if latestView.generation+uint64(published) >=
			fileLogicalCutGenerationMask {
			break
		}
		generation := latestView.generation + uint64(published) + 1
		nextDocumentDelta := documentDelta + request.countDelta
		if nextDocumentDelta < fileLogicalCutMinDelta ||
			nextDocumentDelta > fileLogicalCutMaxDelta {
			// The packed count is an exact signed value, never a saturating one.
			// Leave this request and its suffix untouched; the caller enters the
			// exclusive path, folds this cut, and retries against delta zero.
			break
		}
		if _, valid := fileLogicalDocumentCount(
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
		finalGeneration := latestView.generation + uint64(published)
		finalCut, ok := packFileLogicalCut(finalGeneration, documentDelta)
		if !ok {
			panic("vibedb: invalid prepared packed logical cut")
		}
		// The group is one visibility cut. Overlay records retain consecutive
		// generations for journal-delta replay. The router is initialized first;
		// the single packed Store below is the commit point. Append-only records do
		// not need the snapshot gate: an old-cut point reader sees the router ahead
		// and retries, while current scans filter immutable chains by their pinned
		// generation. Destructive arena reuse still holds its reader fence.
		router.AdvanceGeneration(finalGeneration)
		for _, request := range requests[:published] {
			if request.fillsEmpty && router.ClearEmpty(request.route) {
				c.removePrimaryEmptyLeaf()
			}
		}
		c.pageValidator.advanceGeneration(finalGeneration)
		c.logicalCut.Store(finalCut)
		if reuseFence {
			c.endReaderFence()
			c.snapshotGate.Unlock()
			reuseFence = false
		}
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
	if !c.packedLogicalCutEnabled() || c.onlineIndexBuild.Load() {
		return false, false, nil
	}
	pool := c.primaryConcurrentContexts
	if pool == nil {
		return false, false, nil
	}

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
	var context *primaryConcurrentContext
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
		context = pool.acquire()
		if context == nil {
			return false, false, ErrClosed
		}
		defer pool.release(context)
		if hook := concurrentPrimaryPutCanonicalizeHook; hook == nil {
			canonical, canonicalIndex, eligible, preflightErr =
				context.canonicalize(src, c.options.Collection.IndexOptions)
		} else {
			hook(false)
			canonical, canonicalIndex, eligible, preflightErr =
				context.canonicalize(src, c.options.Collection.IndexOptions)
			hook(true)
		}
		if preflightErr == nil && eligible &&
			len(canonical) > c.options.InlineValueBytes {
			eligible = false
		}
	}
	if preflightErr == nil && eligible {
		if err := c.ensureOrdinaryBufferedRecoveryJournal(); err != nil {
			return false, false, err
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
	if len(key)+len(canonical) > c.primaryUnifiedOverlay.capacityBytes() {
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
	logicalView, logicalOK := c.writerLogicalView()
	state = logicalView.state
	if !logicalOK || state == nil {
		return false, false, ErrClosed
	}
	leafLease, err := router.AcquireLeaf(c.cache, route)
	if err != nil {
		return false, false, err
	}
	page := leafLease.Page()
	if storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafCompact {
		leafLease.Release()
		return false, false, nil
	}
	leaf, ok := storeio.AdmittedCompactPrimaryStripe(
		page, c.storeID, route.Bucket,
	)
	if !ok {
		leafLease.Release()
		return false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	baseRank, baseFound := leaf.FindKey(key)
	largeUnindexed := leaf.Len() > storeio.CommonPrimaryLeafWideSlots ||
		!baseFound && leaf.Len() == storeio.CommonPrimaryLeafWideSlots
	baseSlot, slotOK := uint8(0), largeUnindexed
	if !largeUnindexed {
		baseSlot, slotOK = leaf.PostingSlot(baseRank)
	}
	if baseFound && !slotOK {
		leafLease.Release()
		return false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	current, disposition, overlaySlot := c.primaryUnifiedOverlay.lookup(
		route.Bucket, route.Hash, key, logicalView.generation,
	)
	pendingRaw, pendingRows :=
		c.primaryUnifiedOverlay.pendingBucketDeltas(route.Bucket)
	leafWasEmpty := leaf.Len()+pendingRows == 0
	oldLen := 0
	var baseValue []byte
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
	case primaryUnifiedOverlayMissing:
		if baseFound {
			if _, overflow := leaf.OverflowRef(baseRank); overflow {
				leafLease.Release()
				return false, false, nil
			}
			var decoded bool
			context.value, decoded = leaf.AppendValue(context.value[:0], baseRank)
			if !decoded {
				leafLease.Release()
				return false, false, storeio.ErrCommonPrimaryLeafCorrupt
			}
			oldLen = len(context.value)
			baseValue = context.value
			if largeUnindexed {
				stableSlot, slotOK = c.primaryUnifiedOverlay.chooseLargeUnindexedSlot(
					route.Bucket, route.Hash,
				)
				if !slotOK {
					leafLease.Release()
					return false, false, errConcurrentPrimaryPressure
				}
			}
			break
		}
		if largeUnindexed {
			stableSlot, slotOK = c.primaryUnifiedOverlay.chooseLargeUnindexedSlot(
				route.Bucket, route.Hash,
			)
		} else {
			stableSlot, slotOK = leaf.ChooseInsertSlotHashed(
				route.Hash,
				c.primaryUnifiedOverlay.pendingInsertSlots(route.Bucket),
			)
		}
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
	if baseFound && baseValue == nil {
		if _, overflow := leaf.OverflowRef(baseRank); overflow {
			leafLease.Release()
			return false, false, nil
		}
		var decoded bool
		context.value, decoded = leaf.AppendValue(context.value[:0], baseRank)
		if !decoded {
			leafLease.Release()
			return false, false, storeio.ErrCommonPrimaryLeafCorrupt
		}
		baseValue = context.value
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
	rowLimit := storeio.CommonPrimaryLeafWideSlots
	if largeUnindexed {
		rowLimit = storeio.CompactPrimaryStripeMaxRows
	}
	fits := leaf.Len()+pendingRows+countDelta <= rowLimit &&
		leaf.EncodedPayloadBytes()+pendingRaw+rawDelta <=
			storeio.CommonPrimaryLeafMaxExtentBytes-storeio.PageHeaderSize-storeio.PageTrailerSize
	// A final value byte-identical to the immutable compact base cannot change
	// any stream, shape, dictionary, or extent. This includes delete+restore.
	// Content that differs from the base remains conservatively wide: even a
	// same-length integer can widen a leaf-wide FOR/delta stream.
	fixedExtent := baseFound && bytes.Equal(canonical, baseValue)
	var scalarPatch storeio.CommonPrimaryUnifiedScalarPatch
	_ = canonicalIndex
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
	if !c.packedLogicalCutEnabled() || c.onlineIndexBuild.Load() {
		return false, false, nil
	}
	pool := c.primaryConcurrentContexts
	if pool == nil {
		return false, false, nil
	}
	context := pool.acquire()
	if context == nil {
		return false, false, ErrClosed
	}
	defer pool.release(context)
	if len(key) != 0 && len(key) <= c.options.MaxKeyBytes &&
		len(key) <= storeio.CommonPrimaryLeafMaxKeyBytes {
		if err := c.ensureOrdinaryBufferedRecoveryJournal(); err != nil {
			return false, false, err
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
	// See tryConcurrentPrimaryPut: immutable tombstones may overlap both direct
	// point-read epochs and long-lived Snapshot leases.
	if len(key) == 0 || len(key) > c.options.MaxKeyBytes ||
		len(key) > storeio.CommonPrimaryLeafMaxKeyBytes {
		return false, false, ErrKeyTooLarge
	}
	if len(key) > c.primaryUnifiedOverlay.capacityBytes() {
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

	logicalView, logicalOK := c.writerLogicalView()
	state = logicalView.state
	if !logicalOK || state == nil {
		return false, false, ErrClosed
	}
	leafLease, err := router.AcquireLeaf(c.cache, route)
	if err != nil {
		return false, false, err
	}
	page := leafLease.Page()
	if storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafCompact {
		leafLease.Release()
		return false, false, nil
	}
	leaf, ok := storeio.AdmittedCompactPrimaryStripe(
		page, c.storeID, route.Bucket,
	)
	if !ok {
		leafLease.Release()
		return false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	largeUnindexed := leaf.Len() > storeio.CommonPrimaryLeafWideSlots
	baseRank, baseFound := leaf.FindKey(key)
	baseSlot, slotOK := uint8(0), largeUnindexed
	if !largeUnindexed {
		baseSlot, slotOK = leaf.PostingSlot(baseRank)
	}
	if baseFound && !slotOK {
		leafLease.Release()
		return false, false, storeio.ErrCommonPrimaryLeafCorrupt
	}
	current, disposition, overlaySlot := c.primaryUnifiedOverlay.lookup(
		route.Bucket, route.Hash, key, logicalView.generation,
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
		if _, overflow := leaf.OverflowRef(baseRank); overflow {
			leafLease.Release()
			return false, false, nil
		}
		var decoded bool
		context.value, decoded = leaf.AppendValue(context.value[:0], baseRank)
		if !decoded {
			leafLease.Release()
			return false, false, storeio.ErrCommonPrimaryLeafCorrupt
		}
		oldLen = len(context.value)
		if largeUnindexed {
			stableSlot, slotOK = c.primaryUnifiedOverlay.chooseLargeUnindexedSlot(
				route.Bucket, route.Hash,
			)
			if !slotOK {
				leafLease.Release()
				return false, false, errConcurrentPrimaryPressure
			}
		}
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
	if rawDelta == 0 || currentRows-1 < 0 ||
		leaf.EncodedPayloadBytes()+pendingRaw+rawDelta < 0 {
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
