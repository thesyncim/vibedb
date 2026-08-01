package durable

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// Put inserts or replaces one document. Every collection is an ordered primary
// graph. Eligible inline mutations use its bounded row overlay; structural,
// indexed, overflow, and pressure paths use routed copy-on-write.
//
// key is borrowed for the duration of the call and not retained after it
// returns: the store copies it wherever it stages the key (leaf frame,
// recovery-journal record). Callers may reuse or mutate the backing array as
// soon as Put returns.
func (c *Collection) Put(key []byte, src []byte) (created bool, err error) {
	if c == nil {
		return false, ErrClosed
	}
	handled, concurrentCreated, concurrentErr :=
		c.tryConcurrentPrimaryPut(key, src)
	if handled {
		return concurrentCreated, nil
	}
	if errors.Is(concurrentErr, errConcurrentPrimaryPressure) {
		// Exactly one member of a pressure cohort takes the exclusive fold/retry.
		// Later members first retry the fast lane after that fold, so a full
		// overlay cannot turn one combined group into N queued writer.Lock calls.
		c.primaryConcurrentPressure.Lock()
		handled, concurrentCreated, concurrentErr =
			c.tryConcurrentPrimaryPut(key, src)
		if handled {
			c.primaryConcurrentPressure.Unlock()
			return concurrentCreated, nil
		}
		if concurrentErr != nil &&
			!errors.Is(concurrentErr, errConcurrentPrimaryPressure) {
			c.primaryConcurrentPressure.Unlock()
			return false, concurrentErr
		}
		created, err = c.putPrimaryWithSplit(key, src)
		if err == nil {
			c.concurrentPrimaryFallbacks.Add(1)
		}
		c.primaryConcurrentPressure.Unlock()
		return created, err
	}
	if concurrentErr != nil {
		return false, concurrentErr
	}
	if len(src) > 0 && c.shouldQueuePrimaryMutation(key, src) {
		created, _, err = c.submitPrimaryMutation(
			primaryMutationPut, key, src,
		)
		return created, err
	}
	return c.putPrimaryWithSplit(key, src)
}

// Delete removes one document by key through the routed ordered-primary path.
// key is borrowed for the call only; the store copies it where it stages a
// tombstone (recovery-journal record).
func (c *Collection) Delete(key []byte) (deleted bool, err error) {
	if c == nil {
		return false, ErrClosed
	}
	handled, concurrentDeleted, concurrentErr :=
		c.tryConcurrentPrimaryDelete(key)
	if handled {
		return concurrentDeleted, nil
	}
	if errors.Is(concurrentErr, errConcurrentPrimaryPressure) {
		c.primaryConcurrentPressure.Lock()
		handled, concurrentDeleted, concurrentErr =
			c.tryConcurrentPrimaryDelete(key)
		if handled {
			c.primaryConcurrentPressure.Unlock()
			return concurrentDeleted, nil
		}
		if concurrentErr != nil &&
			!errors.Is(concurrentErr, errConcurrentPrimaryPressure) {
			c.primaryConcurrentPressure.Unlock()
			return false, concurrentErr
		}
		deleted, err = c.deletePrimaryWithEmptyReclaim(key)
		if err == nil && deleted {
			c.concurrentPrimaryFallbacks.Add(1)
		}
		c.primaryConcurrentPressure.Unlock()
		return deleted, err
	}
	if concurrentErr != nil {
		return false, concurrentErr
	}
	if c.shouldQueuePrimaryMutation(key, nil) {
		_, deleted, err = c.submitPrimaryMutation(
			primaryMutationDelete, key, nil,
		)
		return deleted, err
	}
	return c.deletePrimaryWithEmptyReclaim(key)
}

// Snapshot pins one immutable reader-visible rooted generation after any
// bounded foreground fold needed to seal it. Close must be called; copy-out
// methods remain valid independently of page eviction.
//
// A snapshot is cheap to take and expensive to keep. Holding one open blocks
// reuse of every extent the writer retires after it was taken, so a snapshot
// held across a sustained write loop fills Options.MaxRetiredExtents — roughly
// twenty thousand replacements at the default bound and geometry — and writes
// then fail with ErrRetiredExtentCapacity until it is closed. Prefer one
// snapshot per query over one per request handler or per connection. See
// Options.MaxRetiredExtents for the arithmetic and the recovery behaviour.
type Snapshot struct {
	collection *Collection
	state      *fileStoreState
	// primaryRouter is the router pointer captured with state. Its physical
	// leaf handles may advance after this snapshot is published, but its fences
	// and bucket identities are immutable. Mask scans use only those immutable
	// coordinates to route through this snapshot's rooted primary graph.
	primaryRouter *storeio.ResidentPrimaryRouter
	// indexes and indexNameIDs pin the immutable logical/physical catalog
	// published with epoch. Online CREATE INDEX replaces both under
	// snapshotGate, so an older snapshot keeps resolving names against the
	// same physical catalog generation as its postings.
	indexes          []*store.ExactIndex
	indexNameIDs     map[string]uint32
	indexDefinitions []store.IndexDefinition
	// epoch is the index epoch captured at Snapshot creation under
	// snapshotGate: one pointer pins the fold base and the overlay chains for
	// this reader's whole lifetime, and the snapshot's generation filters the
	// records — records linked later carry strictly newer generations, so
	// this view resolves exactly its generation's postings even as the writer
	// keeps appending.
	epoch *primaryExactEpoch
	lease storeio.GenerationLease
	once  sync.Once
	// overflowScanValue reassembles each out-of-line value a range or mask scan
	// encounters into one reused buffer. A Snapshot is single-consumer, so the
	// buffer needs no synchronization; keeping it on the Snapshot rather than a
	// captured local keeps the scan callbacks free of a boxed-capture allocation.
	overflowScanValue []byte
	// scanSpliceScratch is the ordered full-scan cursor's document-reconstruction
	// buffer, retained here across scans. The cursor is a fresh stack local per
	// rangePrimaryGraph call and would otherwise grow a splice buffer from nil on
	// the first template/compact row of every scan — one allocation per scan,
	// which a snapshot reused across executions (a warm join's inner side) repeats
	// forever. The mask scan already threads its reconstruction buffer through
	// caller-owned scratch; this gives the full scan the same retention.
	scanSpliceScratch []byte
	// maskGroups retains the bucket-to-floor routing plan for exact-index mask
	// scans. Reuse keeps warmed sparse probes allocation-free while sorting by
	// lexical floor preserves the same callback order as RangeRaw even after a
	// structural split assigns a non-contiguous bucket identity.
	maskGroups []primarySnapshotMaskGroup
}

// IndexWorkspace is the expert, low-level scratch for callers that invoke a
// Snapshot's AppendIndex*Into methods directly. Normal query execution uses
// [IndexSession], which owns this workspace privately and exposes only detached
// metrics. IndexWorkspace retains the transient routing entries, copied
// certificates, ordered probe decisions, and document bytes and tape used by
// one durable exact-index probe. Its zero value is ready to use. The
// certificate arena exists because a leaf representative borrows an evictable
// page frame: the traversal copies it out rather than letting a slice escape
// its lease. Reusing one workspace with AppendIndexMasksInto makes a warmed
// probe allocation-free when caller dst and the observed candidate and
// document high-water marks fit retained capacity.
//
// A workspace is single-consumer and must not be used concurrently. Release
// drops retained storage when a rare broad probe should not pin its high-water
// capacity.
type IndexWorkspace struct {
	lastProbe IndexProbeStats
	// overlayTiles and baseTiles are the mid-window probe's merge scratch:
	// the needle term's overlay contributions (newest record ≤ G per tile)
	// and its decoded base postings, merged as two sorted arrays. Retained
	// here so a warmed merged probe is allocation-free; the overlay side is
	// one term's churn in a fold window and the base side is one term's
	// postings, so the high-water marks stay small.
	overlayTiles []primaryExactProbeTile
	baseTiles    []primaryExactProbeTile
	// needle is the canonicalized probe term's scratch, retained so a warmed
	// probe neither heap-allocates nor re-zeroes a maximum-size stack array.
	needle []byte
}

// primaryExactProbeTile is one resolved overlay contribution for a probe:
// the tile and its absolute record bits at the snapshot's generation.
type primaryExactProbeTile struct {
	tileID uint32
	bits   uint64
}

// IndexProbeStats reports the physical work of the most recent exact or
// candidate-only probe performed with an IndexWorkspace. CandidateRows is
// the number of stable-slot bits read from posting pages. CertificateRows were
// decided from a collision-free scalar or compound-tuple representative
// without opening the documents; DocumentRecheckRows required exact
// comparison against stored JSON. PostingPages counts the index-directory
// leaf pages the probe admitted, which is its physical read work now that the
// masks live in those leaves. MatchedRows is populated only by an exact
// probe.
type IndexProbeStats struct {
	CandidateRows       uint64
	CertificateRows     uint64
	DocumentRecheckRows uint64
	MatchedRows         uint64
	CandidateChunks     int
	PostingPages        int
}

// LastProbeStats returns value-only counters for the most recent probe.
func (w *IndexWorkspace) LastProbeStats() IndexProbeStats {
	if w == nil {
		return IndexProbeStats{}
	}
	return w.lastProbe
}

// Release drops all storage retained by the workspace.
func (w *IndexWorkspace) Release() {
	if w == nil {
		return
	}
	w.lastProbe = IndexProbeStats{}
	w.overlayTiles = nil
	w.baseTiles = nil
	w.needle = nil
}

// Snapshot acquires an explicit generation lease.
func (c *Collection) Snapshot() (*Snapshot, error) {
	snapshot := new(Snapshot)
	if err := c.SnapshotInto(snapshot); err != nil {
		return nil, err
	}
	return snapshot, nil
}

// SnapshotInto is Snapshot using caller-owned reusable snapshot storage. It
// closes any generation currently bound to dst before acquiring the next one,
// while retaining dst's scan reconstruction and routing buffers. A warmed
// take/query/close loop therefore allocates no snapshot object and regrows no
// per-snapshot scratch. dst is single-consumer and must not be copied.
func (c *Collection) SnapshotInto(dst *Snapshot) error {
	if dst == nil {
		return fmt.Errorf("vibedb: SnapshotInto requires non-nil destination")
	}
	if err := dst.Close(); err != nil {
		return err
	}
	// Rebinding has move-like ownership semantics: the old generation is no
	// longer retained even when the source collection cannot provide a new
	// generation. Check the source only after releasing dst's prior lease.
	if c == nil {
		return ErrClosed
	}
	// Close's sync.Once is per binding. Reset it only after the prior binding is
	// completely released and before this single consumer publishes a new one.
	dst.once = sync.Once{}
	// Any deferred canonical lane's primary router may be newer than its
	// sealed parent graph — buffered and journal-backed synchronous alike.
	// A snapshot's point reads and ordered scans must agree, so the pending
	// parents are materialized and the state captured under one writer hold:
	// releasing the lock between the two hands a concurrent publication a
	// window to advance the router past the root the snapshot walks — and
	// that stray volatile publication handed the snapshot a root whose
	// DocumentCount counted a row its still-unsealed rooted graph did not, so
	// a scan came up one row short of Len. This stages the cut but does not
	// make it durable; Flush/Close keep that boundary.
	if c.deferredCanonicalLane() && c.primaryRouter.Load() != nil {
		if concurrentPrimaryExclusiveWaitHook != nil {
			concurrentPrimaryExclusiveWaitHook("snapshot")
		}
		c.writer.Lock()
		if c.closed {
			c.writer.Unlock()
			return ErrClosed
		}
		if len(c.primaryPendingParents) != 0 ||
			c.primaryUnifiedOverlay.hasPending() {
			if err := c.materializePrimaryParentsLocked(); err != nil {
				c.writer.Unlock()
				return err
			}
		}
		err := c.pinSnapshotIntoLocked(dst)
		c.writer.Unlock()
		return err
	}
	return c.pinSnapshotInto(dst)
}

// pinSnapshot captures the reader state and acquires its lease under the
// snapshot gate alone; deferred canonical lanes use pinSnapshotLocked under
// the writer instead. Snapshots hold a long-lived generation lease, not an
// epoch slot, so the reader-fence protocol does not enter here.
func (c *Collection) pinSnapshot() (*Snapshot, error) {
	snapshot := new(Snapshot)
	if err := c.pinSnapshotInto(snapshot); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (c *Collection) pinSnapshotInto(snapshot *Snapshot) error {
	c.snapshotGate.RLock()
	state, stateErr := c.readerFileState()
	if stateErr != nil {
		c.snapshotGate.RUnlock()
		if c.leases.Closing() {
			return ErrClosed
		}
		return stateErr
	}
	epoch := c.primaryEpoch
	primaryRouter := c.primaryRouter.Load()
	indexes := c.options.indexes
	indexNameIDs := c.options.indexNameIDs
	indexDefinitions := c.options.Indexes
	lease, err := c.leases.Acquire(state.root.Generation)
	c.snapshotGate.RUnlock()
	if err != nil {
		return publicReaderLeaseError(err)
	}
	snapshot.collection = c
	snapshot.state = state
	snapshot.primaryRouter = primaryRouter
	snapshot.indexes = indexes
	snapshot.indexNameIDs = indexNameIDs
	snapshot.indexDefinitions = indexDefinitions
	snapshot.epoch = epoch
	snapshot.lease = lease
	return nil
}

// pinSnapshotLocked is pinSnapshot for callers already holding the writer
// lock, which no gate writer can be inside.
func (c *Collection) pinSnapshotLocked() (*Snapshot, error) {
	return c.pinSnapshot()
}

func (c *Collection) pinSnapshotIntoLocked(snapshot *Snapshot) error {
	return c.pinSnapshotInto(snapshot)
}

// Close releases the snapshot generation. It is idempotent.
func (s *Snapshot) Close() error {
	if s == nil {
		return nil
	}
	s.once.Do(func() {
		s.lease.Release()
		s.collection = nil
		s.state = nil
		s.epoch = nil
		s.primaryRouter = nil
		s.indexes = nil
		s.indexNameIDs = nil
		s.indexDefinitions = nil
		clear(s.maskGroups)
		s.maskGroups = s.maskGroups[:0]
	})
	return nil
}

// Len returns the number of keys visible to the snapshot.
func (s *Snapshot) Len() uint64 {
	if s == nil || s.state == nil {
		return 0
	}
	return s.state.root.DocumentCount
}

// Generation returns the pinned physical publication generation.
func (s *Snapshot) Generation() uint64 {
	if s == nil || s.state == nil {
		return 0
	}
	return s.state.root.Generation
}

// AppendRaw appends key's canonical JSON document into dst. It never returns a
// borrowed page slice. key is borrowed for the call only; it is not retained.
func (s *Snapshot) AppendRaw(dst []byte, key []byte) ([]byte, bool, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return dst, false, ErrClosed
	}
	return s.collection.appendRawAtState(dst, key, s.state)
}

func (c *Collection) appendRawAtState(
	dst []byte, key []byte, state *fileStoreState,
) ([]byte, bool, error) {
	return c.resolvePrimaryGraph(dst, state, key)
}

// PrefetchKeys resolves keys through the pinned directories and submits their
// document extents to the bounded asynchronous read queue in physical order.
// It returns the number submitted; missing keys are ignored and queue pressure
// is visible through Stats.PrefetchDropped.
func (s *Snapshot) PrefetchKeys(keys [][]byte) (int, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return 0, ErrClosed
	}
	var refs [64]storeio.PageRef
	count := 0
	queued := 0
	flush := func() error {
		if count == 0 {
			return nil
		}
		batch := refs[:count]
		slices.SortFunc(batch, func(a, b storeio.PageRef) int {
			if a.Offset < b.Offset {
				return -1
			}
			if a.Offset > b.Offset {
				return 1
			}
			return 0
		})
		unique := batch[:0]
		for _, ref := range batch {
			if len(unique) == 0 || unique[len(unique)-1].Offset != ref.Offset {
				unique = append(unique, ref)
			}
		}
		n, err := s.collection.cache.Prefetch(unique)
		queued += n
		count = 0
		return err
	}
	// Prefetch is a pure read-ahead accelerator, so a snapshot whose ordered
	// graph predates the resident router queues nothing rather than page-walking
	// the rooted oracle. Correctness never depends on a prefetch.
	router := s.collection.primaryRouter.Load()
	if router == nil || router.Generation() != s.state.root.Generation {
		return 0, nil
	}
	for _, key := range keys {
		route, ok := router.Route(key)
		if !ok || route.Ref == (storeio.PageRef{}) {
			continue
		}
		refs[count] = route.Ref
		count++
		if count == len(refs) {
			if flushErr := flush(); flushErr != nil {
				return queued, flushErr
			}
		}
	}
	return queued, flush()
}

// liveReadSupersededRetries bounds how many times a live point read restarts
// after a concurrent publication advanced the resident router past its pinned
// state mid-read. Each restart re-pins the newest visible state, so a retry
// fails again only if another publication lands inside the next entry's
// nanosecond-scale pin-to-route window; the bound exists so an adversarial
// publish cadence still cannot livelock a reader — the terminal gate-held
// resolve in appendRawLeased always completes.
const liveReadSupersededRetries = 4

// liveReadLeasedPinnedHook is a package-test seam invoked after each slow-path
// logical cut is pinned and once more immediately before the terminal writer
// fence. Production leaves it nil.
var liveReadLeasedPinnedHook func(int)

// AppendRaw is the current-snapshot convenience form. It protects the read
// with one epoch slot — no lock, no per-call generation lease — and falls back
// to the gated lease path only when the epoch table declines the entry (full
// table, an active writer fence, Close, or a persistence failure that needs
// the slow path's exact error).
//
// A read whose pinned state is superseded mid-read (the resident router moved
// ahead of it) restarts against the newer publication instead of resolving:
// the live state's rooted graph is the last checkpoint's cut, so falling back
// to it would un-see acknowledged mutations that are visible only through the
// router (see resolvePrimaryGraphLive).
func (c *Collection) AppendRaw(dst []byte, key []byte) ([]byte, bool, error) {
	if c == nil {
		return dst, false, ErrClosed
	}
	if !c.packedLogicalCutEnabled() {
		for range liveReadSupersededRetries {
			state, epoch, ok := c.enterPhysicalReadEpoch()
			if !ok {
				break
			}
			out, found, superseded, err := c.resolvePrimaryGraphLive(
				dst, state, state.root.Generation, key,
			)
			epoch.Exit()
			if !superseded {
				return out, found, err
			}
		}
		return c.appendRawLeased(dst, key)
	}
	for range liveReadSupersededRetries {
		view, epoch, ok := c.enterPackedReadEpoch()
		if !ok {
			break
		}
		out, found, superseded, err := c.resolvePrimaryGraphLive(
			dst, view.state, view.generation, key,
		)
		epoch.Exit()
		if !superseded {
			return out, found, err
		}
	}
	return c.appendRawLeased(dst, key)
}

// appendRawLeased is the pre-epoch read entry, retained as the slow path every
// declined or diverted epoch entry falls back to. A writer fence points new
// readers here exactly because this path blocks on the snapshot gate until the
// writer's decision window closes.
func (c *Collection) appendRawLeased(dst []byte, key []byte) ([]byte, bool, error) {
	for attempt := range liveReadSupersededRetries {
		c.snapshotGate.RLock()
		view, stateErr := c.visibleLogicalView()
		if stateErr != nil {
			c.snapshotGate.RUnlock()
			if c.leases.Closing() {
				return dst, false, ErrClosed
			}
			return dst, false, stateErr
		}
		lease, err := c.leases.Acquire(view.retentionGeneration())
		c.snapshotGate.RUnlock()
		if err != nil {
			return dst, false, publicReaderLeaseError(err)
		}
		if liveReadLeasedPinnedHook != nil {
			liveReadLeasedPinnedHook(attempt)
		}
		out, ok, superseded, err := c.resolvePrimaryGraphLive(
			dst, view.state, view.generation, key,
		)
		lease.Release()
		if !superseded {
			return out, ok, err
		}
	}
	// Terminal, guaranteed-progress resolve: take writer exclusively, the one
	// fence every publication obeys. Packed overlay publishers intentionally do
	// not take snapshotGate, so that gate alone cannot close a fifth router-ahead
	// race after the bounded optimistic retries. The writer hold makes the view
	// and routed resolve one stable cut; snapshotGate still protects failure-state
	// selection and lease acquisition from the persistence callback.
	if liveReadLeasedPinnedHook != nil {
		liveReadLeasedPinnedHook(liveReadSupersededRetries)
	}
	c.writer.Lock()
	defer c.writer.Unlock()
	c.snapshotGate.RLock()
	view, stateErr := c.visibleLogicalView()
	if stateErr != nil {
		c.snapshotGate.RUnlock()
		if c.leases.Closing() {
			return dst, false, ErrClosed
		}
		return dst, false, stateErr
	}
	lease, err := c.leases.Acquire(view.retentionGeneration())
	if err != nil {
		c.snapshotGate.RUnlock()
		return dst, false, publicReaderLeaseError(err)
	}
	out, ok, superseded, err := c.resolvePrimaryGraphLive(
		dst, view.state, view.generation, key,
	)
	c.snapshotGate.RUnlock()
	lease.Release()
	if superseded && err == nil {
		err = storeio.ErrSegmentedTabletRouterCorrupt
	}
	return out, ok, err
}

// PrefetchKeys submits current-snapshot document reads to the bounded
// asynchronous prefetch queue.
func (c *Collection) PrefetchKeys(keys [][]byte) (int, error) {
	snapshot, err := c.Snapshot()
	if err != nil {
		return 0, err
	}
	defer snapshot.Close()
	return snapshot.PrefetchKeys(keys)
}

// Len returns the current reader-visible key count.
func (c *Collection) Len() uint64 {
	if c == nil {
		return 0
	}
	return c.visibleLogicalViewNoError().documentCount
}

// Generation returns the current reader-visible generation.
func (c *Collection) Generation() uint64 {
	if c == nil {
		return 0
	}
	return c.visibleLogicalViewNoError().generation
}

// StateMetrics is one coherent, constant-size observation of reader-visible
// collection state and its crash-safe watermark.
type StateMetrics struct {
	Documents           uint64
	PublishedGeneration uint64
	DurableGeneration   uint64
}

// StateMetrics samples one coherent logical view while holding only the narrow
// visibility mutex. A packed buffered overlay may advance publication and
// document count ahead of the physical root, so the logical cut—not the root
// alone—is authoritative. The durability watermark is capped at that sampled
// publication. This method never takes the writer lock or gathers heavyweight
// cache, queue, lease, or reclaimer statistics.
func (c *Collection) StateMetrics() StateMetrics {
	if c == nil {
		return StateMetrics{}
	}
	c.visibilityMu.Lock()
	defer c.visibilityMu.Unlock()
	view := c.visibleLogicalViewNoError()
	if view.state == nil {
		return StateMetrics{}
	}
	published := view.generation
	durable := uint64(0)
	if c.committer != nil {
		durable = max(
			c.committer.DurableGeneration(),
			c.journalDeltaGeneration.Load(),
		)
	}
	return StateMetrics{
		Documents:           view.documentCount,
		PublishedGeneration: published,
		DurableGeneration:   min(durable, published),
	}
}

// DurableGeneration returns the newest crash-safe generation.
func (c *Collection) DurableGeneration() uint64 {
	if c == nil || c.committer == nil {
		return 0
	}
	return max(
		c.committer.DurableGeneration(),
		c.journalDeltaGeneration.Load(),
	)
}

// Stats reports configured residency, page I/O, prefetch, durability queue,
// snapshot, and reclamation pressure without performing file I/O.
func (c *Collection) Stats() Stats {
	if c == nil {
		return Stats{}
	}
	c.writer.Lock()
	defer c.writer.Unlock()
	if c.cache == nil || c.committer == nil || c.reclaimer == nil {
		return Stats{}
	}
	cache := c.cache.Stats()
	commit := c.committer.Stats()
	view := c.visibleLogicalViewNoError()
	state := view.state
	current := uint64(0)
	if state != nil {
		current = view.generation
	}
	leases := c.leases.Stats(current)
	retired := c.reclaimer.Stats()
	overlayCapacity, overlayResident, overlayDirty :=
		c.primaryUnifiedOverlay.stats()
	concurrentPrimaryScratch := c.primaryConcurrentContexts.capacityBytes()
	if c.primaryConcurrentStripes != nil {
		concurrentPrimaryScratch += uint64(unsafe.Sizeof(*c.primaryConcurrentStripes))
		concurrentPrimaryScratch += uint64(unsafe.Sizeof(c.primaryOverlayPublish))
	}
	stats := Stats{
		CapacityBytes:       cache.CapacityBytes + overlayCapacity,
		ResidentBytes:       cache.ResidentBytes + overlayResident,
		ReservedBytes:       cache.ReservedBytes + overlayCapacity,
		CommitCapacityBytes: c.committer.StagingCapacityBytes(),
		PinnedPages:         cache.PinnedPages,
		DirtyBytes:          cache.DirtyBytes + overlayDirty,
		PageReads:           cache.PageReads, ReadBytes: cache.ReadBytes, CacheHits: cache.CacheHits,
		CacheMisses: cache.Misses, CoalescedReads: cache.Coalesced, ReadErrors: cache.ReadErrors,
		PrefetchHits: cache.PrefetchHits, Evictions: cache.Evictions,
		PrefetchQueued: cache.PrefetchQueued, PrefetchDropped: cache.PrefetchDropped,
		PrefetchQueueDepth: cache.QueueDepth, ReadQueueDepth: cache.ReadQueueDepth,
		AsyncReadBatches: cache.AsyncReadBatches, LargestReadBatch: cache.LargestReadBatch,
		PublishedGeneration: max(commit.PublishedGeneration, current),
		DurableGeneration: max(
			commit.DurableGeneration,
			c.journalDeltaGeneration.Load(),
		),
		CommitQueueDepth: commit.QueuedGenerations, DeviceCommits: commit.DeviceCommits,
		CommittedBatches: commit.CommittedBatches, LargestCommitGroup: commit.LargestGroup,
		SupersededRootWrites:                 commit.SupersededRootWrites,
		SupersededRootBytes:                  commit.SupersededRootBytes,
		TailWitnessWrites:                    commit.TailWitnessWrites,
		TailWitnessBytes:                     commit.TailWitnessBytes,
		PrewrittenPageWrites:                 commit.PrewrittenPageWrites,
		PrewrittenPageBytes:                  commit.PrewrittenPageBytes,
		AutomaticCheckpoints:                 c.automaticCheckpoints.Load(),
		PrimaryOverlayFolds:                  c.primaryOverlayFolds.Load(),
		ConcurrentPrimaryReplaces:            c.concurrentPrimaryReplaces.Load(),
		ConcurrentPrimaryFallbacks:           c.concurrentPrimaryFallbacks.Load(),
		ConcurrentPrimaryPublishGroups:       c.concurrentPrimaryPublishGroups.Load(),
		ConcurrentPrimaryLargestPublishGroup: c.concurrentPrimaryLargestPublishGroup.Load(),
		RetirementPressureCheckpoints:        c.retirementPressureCheckpoints.Load(),
		DeviceBytes:                          commit.DeviceBytes,
		MaterializedBatches:                  commit.MaterializedBatches,
		MaterializationJournalBytes:          commit.MaterializationJournalBytes,
		MaterializationTargetBytes:           commit.MaterializationTargetBytes,
		MaterializationFullWriteBytes:        commit.MaterializationFullWriteBytes,
		MaterializationBarriers:              commit.MaterializationBarriers,
		MaterializationAttempts:              c.materializationAttempts.Load(),
		MaterializationUpdates:               c.materializationUpdates.Load(),
		MaterializationFallbacks:             c.materializationFallbacks.Load(),
		MaterializationSnapshotSkips:         c.materializationSnapshotSkips.Load(),
		MaterializationBusySkips:             c.materializationBusySkips.Load(),
		JournalAcks:                          c.journalAcks.Load(),
		ChainAcks:                            c.chainAcks.Load(),
		JournalSyncs:                         c.journalSyncs.Load(),
		JournalLargestGroup:                  c.journalLargestGroup.Load(),
		JournalDeltaCheckpoints:              c.journalDeltaCheckpoints.Load(),
		JournalDeltaRecords:                  c.journalDeltaRecords.Load(),
		JournalDeltaBytes:                    c.journalDeltaBytes.Load(),
		JournalDeltaFullFallbacks:            c.journalDeltaFullFallbacks.Load(),
		PrimaryLeafSplitRequired:             c.primaryLeafSplitRequired.Load(),
		PrimaryEmptyLeaves:                   c.primaryEmptyLeaves.Load(),
		PrimaryLeafSplits:                    c.primaryLeafSplits.Load(),
		PrimaryEmptyReclaims:                 c.primaryEmptyReclaims.Load(),
		PrimaryMacroSplitRequired:            c.primaryMacroSplitRequired.Load(),
		PrimarySplitMaxNS:                    c.primarySplitMaxNS.Load(),
		PrimaryEmptyReclaimMaxNS:             c.primaryEmptyReclaimMaxNS.Load(),
		HolePunchRanges:                      c.holePunchRanges.Load(),
		HolePunchBytes:                       c.holePunchBytes.Load(),
		HolePunchSkippedRanges:               c.holePunchSkippedRanges.Load(),
		HolePunchUnsupported:                 c.holePunchUnsupported.Load(),
		HolePunchErrors:                      c.holePunchErrors.Load(),
		PrimaryMutationScratchBytes: uint64(
			len(c.primaryLeafScratch)+len(c.primaryRootScratch),
		) + uint64(cap(c.primaryUnifiedReplacementScratch))*
			uint64(unsafe.Sizeof(storeio.CommonPrimaryUnifiedReplacement{})) +
			c.primaryNativeFoldAdditionalScratchBytes(),
		ConcurrentPrimaryScratchBytes: concurrentPrimaryScratch,
		Backend:                       Backend(commit.Backend),
		Durability:                    c.options.Durability,
		CheckpointStrength:            c.options.CheckpointStrength,
		ReadBackend:                   Backend(cache.ReadBackend),
		DirectReads:                   c.directRead,
		DirectWrites:                  c.directWrite,
		SnapshotCapacity:              leases.Capacity, ActiveSnapshots: leases.Active,
		OldestSnapshotGeneration: leases.MinimumGeneration,
		RetiredExtentCapacity:    retired.Capacity, PendingRetiredExtents: retired.Pending,
		PendingRetiredBytes: retired.PendingBytes, ReusableExtents: uint64(len(c.reusable)),
	}
	if leases.Active != 0 && current > leases.MinimumGeneration {
		stats.OldestSnapshotAgeGenerations = current - leases.MinimumGeneration
	}
	if c.reusableBlock != nil {
		stats.ReusableCapacityBytes =
			uint64(cap(c.reusable)) * uint64(unsafe.Sizeof(storeio.FreeExtent{}))
		stats.ReusableIndexBytes = uint64(len(c.freeExtentMaxima)) * 8
		stats.RetiredIntervalIndexBytes = uint64(
			storeio.RetiredIntervalIndexStorageBytes(
				c.options.MaxRetiredExtents,
			),
		)
		stats.RetiredExtentArenaBytes = uint64(
			storeio.RetiredExtentStorageBytes(
				c.options.MaxRetiredExtents,
			),
		)
		if c.reusableBlock.OutsideHeap() {
			stats.ReusableExternalBytes = stats.ReusableCapacityBytes
			stats.ReusableIndexExternalBytes = stats.ReusableIndexBytes
			stats.RetiredIntervalIndexExternalBytes =
				stats.RetiredIntervalIndexBytes
			stats.RetiredExtentArenaExternalBytes =
				stats.RetiredExtentArenaBytes
		}
	}
	if c.freeScratchBlock != nil {
		stats.FreeScratchCapacityBytes = uint64(c.freeScratchBlock.Len())
		if c.freeScratchBlock.OutsideHeap() {
			stats.FreeScratchExternalBytes = stats.FreeScratchCapacityBytes
		}
		stats.FreeScratchLiveBytes =
			uint64(len(c.freeFenced)+len(c.freeImageScratch))*
				uint64(unsafe.Sizeof(storeio.FreeExtent{})) +
				uint64(len(c.freeFoldRanges))*uint64(unsafe.Sizeof([2]int{})) +
				uint64(len(c.freeFoldOrder))*uint64(unsafe.Sizeof(freeFoldSlot{}))
	}
	if c.materializationBlock != nil {
		stats.MaterializationScratchBytes =
			uint64(c.materializationBlock.Len())
	}
	for _, extent := range c.reusable {
		stats.ReusableBytes += extent.Length
	}
	if state != nil {
		stats.DocumentCount = view.documentCount
		stats.FileEnd = state.fileEnd
	}
	return stats
}
