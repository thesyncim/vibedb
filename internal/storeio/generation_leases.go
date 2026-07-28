package storeio

import (
	"errors"
	"fmt"
	"slices"
	"sync"
)

var (
	// ErrLeaseCapacity applies bounded backpressure when every configured
	// snapshot-generation lease is active.
	ErrLeaseCapacity = errors.New("vibejson: Store snapshot lease capacity exhausted")
	// ErrGenerationLeasesClosed reports acquisition after shutdown starts.
	ErrGenerationLeasesClosed = errors.New("vibejson: Store snapshot leases are closed")
	// ErrLeasesActive reports an attempted close while readers still protect
	// generations and their physical extents.
	ErrLeasesActive = errors.New("vibejson: Store snapshot leases are still active")
	// ErrRetiredExtentCapacity reports that reclamation metadata reached its
	// configured bound before readers or recovery roots released old extents.
	ErrRetiredExtentCapacity = errors.New("vibejson: Store retired extent capacity exhausted")
)

// GenerationLeaseOptions fixes snapshot tracking memory at construction.
type GenerationLeaseOptions struct {
	// MaxLeases is the maximum number of concurrently retained snapshots.
	MaxLeases int
}

func (o GenerationLeaseOptions) normalized() (GenerationLeaseOptions, error) {
	if o.MaxLeases == 0 {
		o.MaxLeases = 1024
	}
	if o.MaxLeases < 1 || o.MaxLeases > 1<<20 {
		return GenerationLeaseOptions{}, fmt.Errorf("%w: maximum leases %d", ErrInvalidWrite, o.MaxLeases)
	}
	return o, nil
}

type generationLeaseSlot struct {
	generation uint64
	token      uint64
	active     bool
}

// GenerationLeaseStats is an O(configured lease slots) accounting snapshot.
type GenerationLeaseStats struct {
	Capacity          uint64
	Active            uint64
	MinimumGeneration uint64
}

// GenerationLeases owns a fixed snapshot table. Explicit leases make page
// lifetime independent of Go GC timing and let copy-on-write reclamation prove
// that no reader can still dereference a retired extent.
type GenerationLeases struct {
	mu              sync.Mutex
	slots           []generationLeaseSlot
	free            []uint32
	next            uint64
	observedThrough uint64
	closing         bool
	closed          bool
}

// NewGenerationLeases allocates the complete fixed lease table.
func NewGenerationLeases(options GenerationLeaseOptions) (*GenerationLeases, error) {
	normalized, err := options.normalized()
	if err != nil {
		return nil, err
	}
	l := &GenerationLeases{
		slots: make([]generationLeaseSlot, normalized.MaxLeases),
		free:  make([]uint32, normalized.MaxLeases),
	}
	for i := range l.free {
		l.free[i] = uint32(len(l.free) - 1 - i)
	}
	return l, nil
}

// GenerationLease is a single-owner snapshot token. Do not copy it after
// first use. Release is idempotent for one value.
type GenerationLease struct {
	owner      *GenerationLeases
	index      uint32
	generation uint64
	token      uint64
}

// Generation returns the protected immutable Store generation.
func (l *GenerationLease) Generation() uint64 {
	if l == nil || l.owner == nil {
		return 0
	}
	return l.generation
}

// Release stops protecting the generation.
func (l *GenerationLease) Release() {
	if l == nil || l.owner == nil {
		return
	}
	owner := l.owner
	owner.release(l.index, l.token)
	l.owner = nil
	l.generation = 0
	l.token = 0
}

// Acquire protects generation until the returned lease is released. It never
// grows tracking memory.
func (l *GenerationLeases) Acquire(generation uint64) (GenerationLease, error) {
	if l == nil {
		return GenerationLease{}, ErrGenerationLeasesClosed
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closing || l.closed {
		return GenerationLease{}, ErrGenerationLeasesClosed
	}
	if len(l.free) == 0 {
		return GenerationLease{}, ErrLeaseCapacity
	}
	last := len(l.free) - 1
	index := l.free[last]
	l.free = l.free[:last]
	l.next++
	if l.next == 0 {
		l.next++
	}
	token := l.next
	l.slots[index] = generationLeaseSlot{generation: generation, token: token, active: true}
	if generation > l.observedThrough {
		l.observedThrough = generation
	}
	return GenerationLease{owner: l, index: index, generation: generation, token: token}, nil
}

func (l *GenerationLeases) release(index uint32, token uint64) {
	l.mu.Lock()
	if int(index) < len(l.slots) {
		slot := &l.slots[index]
		if slot.active && slot.token == token {
			*slot = generationLeaseSlot{}
			l.free = append(l.free, index)
		}
	}
	l.mu.Unlock()
}

// Minimum returns the oldest active generation. If no reader is active it
// returns successor, denoting that every generation through current is free of
// reader references.
func (l *GenerationLeases) Minimum(current uint64) uint64 {
	if l == nil {
		return generationSuccessor(current)
	}
	l.mu.Lock()
	minimum := generationSuccessor(current)
	for i := range l.slots {
		if l.slots[i].active && l.slots[i].generation < minimum {
			minimum = l.slots[i].generation
		}
	}
	l.mu.Unlock()
	return minimum
}

// AnyActive reports whether any generation lease is currently held. The
// result is linearized with Acquire and Release. A caller using a false result
// as a storage-retirement proof must separately prevent a new Acquire until
// the corresponding root publication is complete.
func (l *GenerationLeases) AnyActive() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	active := len(l.free) != len(l.slots)
	l.mu.Unlock()
	return active
}

// SafeFromSnapshots reports whether a page first published at generation can
// be unreachable from every currently active reader. A lease can retain a page
// whose generation is less than or equal to its own, so safety requires every
// active lease generation to be strictly less than generation. Equality is
// unsafe.
//
// The lease table retains two constant-size summaries under the same mutex as
// its slots. No active lease, or a page born after the newest generation ever
// handed to a lease, proves safety without walking the fixed slot table. Only
// an active historical reader needs the exact bounded scan. Keeping the
// summary here, instead of beside the public Snapshot API, includes temporary
// reader leases such as current-state point lookups.
//
// The result is linearized with Acquire and Release by the lease mutex. It is
// a point-in-time observation: a writer that needs safety to remain true while
// it materializes or rewrites storage must already prevent new lease
// acquisition with its publication gate and keep that gate held through the
// operation. This method does not cover alternate recovery roots or page-cache
// pins, which have separate ownership fences.
//
// Generation zero is never a valid page generation and returns false. A nil
// table has no active snapshots and returns true for every non-zero generation.
func (l *GenerationLeases) SafeFromSnapshots(generation uint64) bool {
	if generation == 0 {
		return false
	}
	if l == nil {
		return true
	}
	l.mu.Lock()
	if len(l.free) == len(l.slots) || generation > l.observedThrough {
		l.mu.Unlock()
		return true
	}
	safe := true
	for i := range l.slots {
		if l.slots[i].active && l.slots[i].generation >= generation {
			safe = false
			break
		}
	}
	l.mu.Unlock()
	return safe
}

// Stats returns bounded lease usage and the current reclamation floor.
func (l *GenerationLeases) Stats(current uint64) GenerationLeaseStats {
	if l == nil {
		return GenerationLeaseStats{MinimumGeneration: generationSuccessor(current)}
	}
	l.mu.Lock()
	stats := GenerationLeaseStats{
		Capacity:          uint64(len(l.slots)),
		Active:            uint64(len(l.slots) - len(l.free)),
		MinimumGeneration: generationSuccessor(current),
	}
	for i := range l.slots {
		if l.slots[i].active &&
			l.slots[i].generation < stats.MinimumGeneration {
			stats.MinimumGeneration = l.slots[i].generation
		}
	}
	l.mu.Unlock()
	return stats
}

// Close prevents new leases. It returns ErrLeasesActive until every existing
// lease is released, then becomes idempotent.
func (l *GenerationLeases) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closing = true
	if len(l.free) != len(l.slots) {
		return ErrLeasesActive
	}
	l.closed = true
	return nil
}

func generationSuccessor(generation uint64) uint64 {
	if generation == ^uint64(0) {
		return generation
	}
	return generation + 1
}

// ExtentReclaimerOptions fixes retained copy-on-write metadata.
type ExtentReclaimerOptions struct {
	MaxRetiredExtents int
	// Epochs optionally adds the direct-read epoch table to the reclamation
	// floor. Nil keeps the lease-only floor for standalone users. When set,
	// no extent is reused while an epoch reader still protects a generation
	// that could reference it, exactly as for lease holders.
	Epochs *ReadEpochs
	// IntervalIndexStorage optionally supplies exact, aligned, pointer-free
	// backing from the collection's existing metadata arena. Nil keeps the
	// standalone constructor self-contained. A non-nil slice transfers
	// exclusive mutable ownership to the reclaimer: the caller must retain its
	// backing allocation, but must not read, write, reuse, resize, or release it
	// until the reclaimer can no longer be called.
	IntervalIndexStorage []byte
	// RetiredExtentStorage optionally supplies exact, aligned, pointer-free
	// backing for the generation-ordered pending set. Durable stores put both
	// arenas in one mmap-backed block; nil preserves a self-contained fallback
	// for standalone users. The same exclusive-ownership and lifetime contract
	// as IntervalIndexStorage applies.
	RetiredExtentStorage []byte
}

func (o ExtentReclaimerOptions) normalized() (ExtentReclaimerOptions, error) {
	if o.MaxRetiredExtents == 0 {
		o.MaxRetiredExtents = 1 << 16
	}
	if o.MaxRetiredExtents < 1 || o.MaxRetiredExtents > 1<<24 {
		return ExtentReclaimerOptions{}, fmt.Errorf("%w: maximum retired extents %d", ErrInvalidWrite, o.MaxRetiredExtents)
	}
	required := RetiredIntervalIndexStorageBytes(o.MaxRetiredExtents)
	if o.IntervalIndexStorage != nil && len(o.IntervalIndexStorage) < required {
		return ExtentReclaimerOptions{}, fmt.Errorf(
			"%w: retired interval index storage has %d bytes, need %d",
			ErrInvalidWrite, len(o.IntervalIndexStorage), required,
		)
	}
	required = RetiredExtentStorageBytes(o.MaxRetiredExtents)
	if o.RetiredExtentStorage != nil && len(o.RetiredExtentStorage) < required {
		return ExtentReclaimerOptions{}, fmt.Errorf(
			"%w: retired extent storage has %d bytes, need %d",
			ErrInvalidWrite, len(o.RetiredExtentStorage), required,
		)
	}
	return o, nil
}

// ExtentReclaimerStats accounts for extents waiting on snapshots or the
// alternate recovery root.
type ExtentReclaimerStats struct {
	Capacity      uint64
	Pending       uint64
	PendingBytes  uint64
	OldestRetired uint64
}

// Small pending sets are cheaper to validate with one contiguous scan than by
// maintaining a pointer-chasing tree. The index is built once when pressure
// crosses this bound and retained until the set drains completely.
const retiredIntervalIndexThreshold = 256

// ExtentReclaimer retains a fixed number of retired extents. The serialized
// Store writer calls Retire and Reclaim; readers only touch GenerationLeases.
type ExtentReclaimer struct {
	mu           sync.Mutex
	leases       *GenerationLeases
	epochs       *ReadEpochs
	pending      []FreeExtent
	pendingHead  int
	pendingBytes uint64
	limit        int
	intervals    retiredIntervalIndex
	indexed      bool
}

// NewExtentReclaimer creates bounded retirement tracking over leases.
//
// With nil external arenas, the standalone fallback owns two pointer-free Go
// arrays totaling exactly 48 bytes per configured extent (24 bytes for
// generation order and 24 bytes for physical interval order), plus the small
// reclaimer object. Supplying arenas moves both arrays out of the Go heap but
// transfers their exclusive mutable ownership for the reclaimer's full
// lifetime as documented by ExtentReclaimerOptions.
func NewExtentReclaimer(leases *GenerationLeases, options ExtentReclaimerOptions) (*ExtentReclaimer, error) {
	if leases == nil {
		return nil, fmt.Errorf("%w: nil generation leases", ErrInvalidWrite)
	}
	normalized, err := options.normalized()
	if err != nil {
		return nil, err
	}
	if normalized.IntervalIndexStorage != nil &&
		normalized.RetiredExtentStorage != nil &&
		fixedStorageRangesOverlap(
			normalized.IntervalIndexStorage,
			RetiredIntervalIndexStorageBytes(normalized.MaxRetiredExtents),
			normalized.RetiredExtentStorage,
			RetiredExtentStorageBytes(normalized.MaxRetiredExtents),
		) {
		return nil, fmt.Errorf(
			"%w: retired metadata storage overlaps",
			ErrInvalidWrite,
		)
	}
	var intervals retiredIntervalIndex
	if normalized.IntervalIndexStorage == nil {
		intervals = newRetiredIntervalIndex(normalized.MaxRetiredExtents)
	} else {
		var ok bool
		intervals, ok = newRetiredIntervalIndexIn(
			normalized.MaxRetiredExtents,
			normalized.IntervalIndexStorage,
		)
		if !ok {
			return nil, fmt.Errorf(
				"%w: retired interval index storage alignment",
				ErrInvalidWrite,
			)
		}
	}
	var pending []FreeExtent
	if normalized.RetiredExtentStorage == nil {
		pending = make([]FreeExtent, 0, normalized.MaxRetiredExtents)
	} else {
		var ok bool
		pending, ok = newRetiredExtentArenaIn(
			normalized.MaxRetiredExtents,
			normalized.RetiredExtentStorage,
		)
		if !ok {
			return nil, fmt.Errorf(
				"%w: retired extent storage alignment",
				ErrInvalidWrite,
			)
		}
	}
	return &ExtentReclaimer{
		leases: leases, epochs: normalized.Epochs, pending: pending,
		limit:     normalized.MaxRetiredExtents,
		intervals: intervals,
	}, nil
}

// Retire records an extent made unreachable by the next root. Overlap with an
// already-pending extent is rejected before publication.
func (r *ExtentReclaimer) Retire(extent FreeExtent) error {
	return r.RetireBatch([]FreeExtent{extent})
}

// RetireBatch atomically reserves retirement metadata for a publication. No
// extent is recorded if capacity, shape, or overlap validation fails.
func (r *ExtentReclaimer) RetireBatch(extents []FreeExtent) error {
	if r == nil {
		return fmt.Errorf("%w: nil extent reclaimer", ErrInvalidWrite)
	}
	if len(extents) == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	pending := r.activePendingLocked()
	if len(extents) > r.limit-len(pending) {
		return ErrRetiredExtentCapacity
	}
	if !r.indexed && len(pending) == 0 && len(extents) == 1 {
		extent := extents[0]
		if extent.Offset == 0 || extent.Length == 0 ||
			extent.RetiredGeneration == 0 ||
			extent.Offset > ^uint64(0)-extent.Length {
			return fmt.Errorf("%w: retired extent", ErrInvalidWrite)
		}
		if r.pendingBytes > ^uint64(0)-extent.Length {
			return fmt.Errorf("%w: retired byte count", ErrInvalidWrite)
		}
		r.compactPendingLocked(1)
		r.pending = append(r.pending, extent)
		r.pendingBytes += extent.Length
		return nil
	}
	if r.indexed && r.intervals.len() != len(pending) ||
		!r.indexed && r.intervals.len() != 0 {
		return fmt.Errorf("%w: retired interval index", ErrInvalidWrite)
	}
	addedBytes := uint64(0)
	for _, extent := range extents {
		if extent.Offset == 0 || extent.Length == 0 || extent.RetiredGeneration == 0 ||
			extent.Offset > ^uint64(0)-extent.Length {
			return fmt.Errorf("%w: retired extent", ErrInvalidWrite)
		}
		if addedBytes > ^uint64(0)-extent.Length {
			return fmt.Errorf("%w: retired byte count", ErrInvalidWrite)
		}
		addedBytes += extent.Length
	}
	if r.pendingBytes > ^uint64(0)-addedBytes {
		return fmt.Errorf("%w: retired byte count", ErrInvalidWrite)
	}
	nextCount := len(pending) + len(extents)
	switch {
	case r.indexed:
		if !r.insertRetiredIntervalsLocked(extents) {
			return fmt.Errorf("%w: overlapping retired extent", ErrInvalidWrite)
		}
	case nextCount <= retiredIntervalIndexThreshold:
		if retiredExtentsOverlap(pending, extents) {
			return fmt.Errorf("%w: overlapping retired extent", ErrInvalidWrite)
		}
	default:
		if !r.insertRetiredIntervalsLocked(pending) ||
			!r.insertRetiredIntervalsLocked(extents) {
			r.intervals.reset()
			return fmt.Errorf("%w: overlapping retired extent", ErrInvalidWrite)
		}
		r.indexed = true
	}
	r.compactPendingLocked(len(extents))
	start := len(r.pending)
	r.pending = append(r.pending, extents...)
	r.pendingBytes += addedBytes
	added := r.pending[start:]
	if len(added) > 1 && !reclamationGenerationOrdered(added) {
		slices.SortStableFunc(added, compareReclamationGeneration)
	}
	if start != 0 &&
		r.pending[start-1].RetiredGeneration > r.pending[start].RetiredGeneration {
		// A bounded reclamation batch may be returned after a fold-budget trim.
		// That rare path can insert an older generation behind still-fenced
		// generations. Restore ordering here; ordinary serialized retirements
		// append in O(batch) time.
		slices.SortStableFunc(
			r.activePendingLocked(), compareReclamationGeneration,
		)
	}
	return nil
}

// CancelRetiredGeneration rolls back an unpublished serialized-writer
// reservation. The generation must occupy the complete pending tail.
func (r *ExtentReclaimer) CancelRetiredGeneration(generation uint64) error {
	if r == nil || generation == 0 {
		return fmt.Errorf("%w: retired generation", ErrInvalidWrite)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	pending := r.activePendingLocked()
	first := len(r.pending)
	for first > r.pendingHead && r.pending[first-1].RetiredGeneration == generation {
		first--
	}
	if first == len(r.pending) {
		pending := r.activePendingLocked()
		low, high := 0, len(pending)
		for low < high {
			middle := int(uint(low+high) >> 1)
			if pending[middle].RetiredGeneration < generation {
				low = middle + 1
			} else {
				high = middle
			}
		}
		if low < len(pending) &&
			pending[low].RetiredGeneration == generation {
			return fmt.Errorf("%w: non-tail retired generation", ErrInvalidWrite)
		}
		return nil
	}
	if r.indexed {
		cancelled := r.pending[first:]
		if !r.removeRetiredIntervalsLocked(cancelled) {
			return fmt.Errorf("%w: retired interval index", ErrInvalidWrite)
		}
	}
	for _, extent := range r.pending[first:] {
		r.pendingBytes -= extent.Length
	}
	clear(r.pending[first:])
	r.pending = r.pending[:first]
	if len(pending) == first-r.pendingHead {
		return nil
	}
	if first == r.pendingHead {
		r.resetPendingLocked()
	}
	return nil
}

// AppendReusable appends and removes extents whose retired generation is
// older than both every active reader and oldestRecoveryGeneration. dst lets
// the serialized allocator reuse its own scratch without allocation.
//
// limit caps how many extents are moved, because dst is backed by a fixed
// off-heap arena that must never be reallocated onto the Go heap. Extents past
// the limit stay pending and are offered again at the next call, so a caller
// with no room left reclaims nothing rather than losing anything. A
// non-positive limit moves nothing.
//
// Taking a limit is what lets the caller stop declining the whole batch when
// its arena is nearly full. Refusing to reclaim anything is not a smaller
// version of reclaiming: this is the only drain of the pending set, so a
// caller that declines stops the one process that would create the room it is
// waiting for, and the pending set then grows to its own capacity and fails
// every subsequent retirement.
func (r *ExtentReclaimer) AppendReusable(
	dst []FreeExtent,
	currentGeneration, oldestRecoveryGeneration uint64,
	limit int,
) ([]FreeExtent, error) {
	if r == nil || limit <= 0 || len(dst) == cap(dst) {
		return dst, nil
	}
	readerFloor := r.leases.Minimum(currentGeneration)
	// Epoch readers publish before re-validating the visible state, so any
	// reader this scan misses observed a root at least as new as the one whose
	// retirements are being drained; see the ReadEpochs reclaim protocol.
	floor := min(
		readerFloor,
		r.epochs.Minimum(currentGeneration),
		oldestRecoveryGeneration,
	)
	r.mu.Lock()
	pending := r.activePendingLocked()
	eligible := len(pending)
	if eligible != 0 {
		switch {
		case pending[0].RetiredGeneration >= floor:
			eligible = 0
		case pending[eligible-1].RetiredGeneration >= floor:
			for low, high := 0, eligible; low < high; {
				middle := int(uint(low+high) >> 1)
				if pending[middle].RetiredGeneration < floor {
					low = middle + 1
				} else {
					high = middle
				}
				eligible = low
			}
		}
	}
	moved := min(eligible, limit, cap(dst)-len(dst))
	if moved == 0 {
		r.mu.Unlock()
		return dst, nil
	}
	if r.indexed {
		reclaimed := pending[:moved]
		if !r.removeRetiredIntervalsLocked(reclaimed) {
			r.mu.Unlock()
			return dst, fmt.Errorf(
				"%w: retired interval index", ErrInvalidWrite,
			)
		}
	}
	dst = append(dst, pending[:moved]...)
	switch {
	case moved == len(pending):
		r.resetPendingLocked()
	default:
		for _, extent := range pending[:moved] {
			r.pendingBytes -= extent.Length
		}
		clear(pending[:moved])
		r.pendingHead += moved
	}
	r.mu.Unlock()
	return dst, nil
}

// AppendPending copies the extents still waiting on a reader or the alternate
// recovery root in nondecreasing retirement-generation order. There is no
// physical-offset ordering contract within one generation: consumers that
// merge physical ranges must sort the result by offset first. The durable free
// log carries them alongside the
// reusable set — they are free space, merely fenced — so a fold needs them to
// write a complete image, and dropping them from that image is what used to
// lose them at every restart.
func (r *ExtentReclaimer) AppendPending(dst []FreeExtent) []FreeExtent {
	if r == nil {
		return dst
	}
	r.mu.Lock()
	dst = append(dst, r.activePendingLocked()...)
	r.mu.Unlock()
	return dst
}

// Restore re-establishes a pending set replayed from the durable free log.
//
// The durable log should already be disjoint, but Restore still verifies that
// boundary before taking ownership: bounded sets use the linear oracle and
// larger sets build the interval index while checking every insertion. This
// avoids a quadratic hundred-thousand-extent replay without trusting corrupt
// free-space metadata. Restoring into a non-empty reclaimer is refused rather
// than merged, because the only caller is the open path and a second call
// would mean the store replayed twice.
func (r *ExtentReclaimer) Restore(extents []FreeExtent) error {
	if r == nil {
		return fmt.Errorf("%w: nil extent reclaimer", ErrInvalidWrite)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	pending := r.activePendingLocked()
	if len(pending) != 0 {
		return fmt.Errorf("%w: reclaimer already holds %d extents", ErrInvalidWrite, len(pending))
	}
	if len(extents) > r.limit {
		return ErrRetiredExtentCapacity
	}
	pendingBytes := uint64(0)
	for _, extent := range extents {
		if extent.Offset == 0 || extent.Length == 0 || extent.RetiredGeneration == 0 ||
			extent.Offset > ^uint64(0)-extent.Length {
			return fmt.Errorf("%w: restored retired extent", ErrInvalidWrite)
		}
		if pendingBytes > ^uint64(0)-extent.Length {
			return fmt.Errorf("%w: restored retired byte count", ErrInvalidWrite)
		}
		pendingBytes += extent.Length
	}
	r.resetPendingLocked()
	if len(extents) <= retiredIntervalIndexThreshold {
		if retiredExtentsOverlap(nil, extents) {
			return fmt.Errorf("%w: restored retired extent overlap", ErrInvalidWrite)
		}
	} else {
		if !r.insertRetiredIntervalsLocked(extents) {
			r.intervals.reset()
			return fmt.Errorf("%w: restored retired extent overlap", ErrInvalidWrite)
		}
		r.indexed = true
	}
	r.pending = append(r.pending, extents...)
	slices.SortStableFunc(r.pending, compareReclamationGeneration)
	r.pendingBytes = pendingBytes
	return nil
}

// PendingCount reports how many extents are still fenced.
func (r *ExtentReclaimer) PendingCount() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending) - r.pendingHead
}

// Stats reports bounded retirement pressure.
func (r *ExtentReclaimer) Stats() ExtentReclaimerStats {
	if r == nil {
		return ExtentReclaimerStats{}
	}
	r.mu.Lock()
	pending := r.activePendingLocked()
	stats := ExtentReclaimerStats{
		Capacity: uint64(r.limit), Pending: uint64(len(pending)),
		PendingBytes: r.pendingBytes,
	}
	if len(pending) != 0 {
		stats.OldestRetired = pending[0].RetiredGeneration
	}
	r.mu.Unlock()
	return stats
}

func compareReclamationGeneration(a, b FreeExtent) int {
	switch {
	case a.RetiredGeneration < b.RetiredGeneration:
		return -1
	case a.RetiredGeneration > b.RetiredGeneration:
		return 1
	case a.Offset < b.Offset:
		return -1
	case a.Offset > b.Offset:
		return 1
	default:
		return 0
	}
}

func reclamationGenerationOrdered(extents []FreeExtent) bool {
	for i := 1; i < len(extents); i++ {
		if extents[i-1].RetiredGeneration > extents[i].RetiredGeneration {
			return false
		}
	}
	return true
}

func (r *ExtentReclaimer) activePendingLocked() []FreeExtent {
	return r.pending[r.pendingHead:]
}

// compactPendingLocked recovers reclaimed prefix capacity only when the next
// append needs it. The common pinned-snapshot query and bounded drain paths
// therefore move no retained entries.
func (r *ExtentReclaimer) compactPendingLocked(appendCount int) {
	if appendCount <= cap(r.pending)-len(r.pending) {
		return
	}
	pending := r.activePendingLocked()
	moved := copy(r.pending, pending)
	clear(r.pending[moved:])
	r.pending = r.pending[:moved]
	r.pendingHead = 0
}

func (r *ExtentReclaimer) resetPendingLocked() {
	clear(r.pending)
	r.pending = r.pending[:0]
	r.pendingHead = 0
	r.pendingBytes = 0
	// Full drains and cancellation remove every interval before reaching this
	// point. Preserve that already-complete O(batch log n) free list instead of
	// rewriting all MaxRetiredExtents nodes merely to represent the same empty
	// tree. Restore calls this only when the active set is already empty.
	if r.intervals.len() != 0 {
		r.intervals.reset()
	}
	r.indexed = false
}

func (r *ExtentReclaimer) insertRetiredIntervalsLocked(
	extents []FreeExtent,
) bool {
	inserted := 0
	for ; inserted < len(extents); inserted++ {
		extent := extents[inserted]
		if !r.intervals.insert(extent.Offset, extent.Length) {
			break
		}
	}
	if inserted == len(extents) {
		return true
	}
	for _, extent := range extents[:inserted] {
		_ = r.intervals.remove(extent.Offset, extent.Length)
	}
	return false
}

func (r *ExtentReclaimer) removeRetiredIntervalsLocked(
	extents []FreeExtent,
) bool {
	if len(extents) == 1 {
		extent := extents[0]
		if r.intervals.remove(extent.Offset, extent.Length) {
			return true
		}
		r.rebuildRetiredIntervalsLocked()
		return false
	}
	for _, extent := range extents {
		if !r.intervals.contains(extent.Offset, extent.Length) {
			r.rebuildRetiredIntervalsLocked()
			return false
		}
	}
	removed := 0
	for ; removed < len(extents); removed++ {
		extent := extents[removed]
		if !r.intervals.remove(extent.Offset, extent.Length) {
			break
		}
	}
	if removed == len(extents) {
		return true
	}
	// Preflight makes this reachable only if an internal invariant failed
	// during removal. Rebuild from the still-unchanged authoritative pending
	// set before exposing the error; this also repairs any nodes mutated by the
	// failed removal rather than assuming only its earlier operands changed.
	r.rebuildRetiredIntervalsLocked()
	return false
}

func (r *ExtentReclaimer) rebuildRetiredIntervalsLocked() {
	r.intervals.reset()
	if !r.insertRetiredIntervalsLocked(r.activePendingLocked()) {
		r.intervals.reset()
	}
}

// retiredExtentsOverlap validates a small incoming batch against the held set
// and against its own earlier entries. Both sets already passed shape and
// overflow validation.
func retiredExtentsOverlap(held, incoming []FreeExtent) bool {
	for rank, extent := range incoming {
		end := extent.Offset + extent.Length
		for _, other := range held {
			if extent.Offset < other.Offset+other.Length &&
				other.Offset < end {
				return true
			}
		}
		for _, other := range incoming[:rank] {
			if extent.Offset < other.Offset+other.Length &&
				other.Offset < end {
				return true
			}
		}
	}
	return false
}
