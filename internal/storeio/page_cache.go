package storeio

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
)

const defaultPrefetchQueue = 64

const defaultReadConcurrency = 4

const pageCacheEvictionScanZones = 64

var (
	// ErrPageCacheClosed reports use after Close has started.
	ErrPageCacheClosed = errors.New("vibejson: Store page cache is closed")
	// ErrPageCachePinned reports that no clean, unpinned contiguous slot span
	// can admit the requested extent. Releasing leases or fencing dirty pages
	// can make a victim available without growing the cache.
	ErrPageCachePinned = errors.New("vibejson: no clean unpinned Store page-cache extent is available")
	// ErrPageCacheReference reports a malformed or physically unordered page
	// reference before any file I/O is attempted.
	ErrPageCacheReference = errors.New("vibejson: invalid Store page cache reference")
	// Compatibility names used by the immutable StorePageReader surface.
	ErrPageCacheFull = ErrPageCachePinned
	// ErrPageReference is the StorePageReader-surface alias for
	// ErrPageCacheReference.
	ErrPageReference = ErrPageCacheReference
	// ErrPageLeaseClosed reports a read or release against a page lease that has
	// already been closed.
	ErrPageLeaseClosed = errors.New("vibejson: Store page lease already closed")
)

// PageCacheOptions fixes cache residency and every explicit prefetch bound.
// ResidentBytes is rounded down to Store allocation quanta. Native ring/control
// mappings and worker stacks are separate bounded runtime overhead. A logical
// extent reads exactly Length bytes and reserves exactly ceil(Length/PageSize)
// arena slots. StoreID binds every admitted page to one file.
type PageCacheOptions struct {
	// Validate optionally applies a kind-specific payload check before a page
	// becomes visible, so a reader that later reconstructs a zero-copy view over
	// the resident bytes does not have to repeat whole-node work on every hit.
	//
	// A supplied Validate must be complete for every page kind whose reader
	// reconstructs an unchecked view — today the ordered-primary descents,
	// whose cache hits call the Admitted* reconstructors (primary leaves,
	// tablet catalog nodes and roots, anchor maps). Those views index into the
	// payload with offsets read from the page itself, so a page admitted
	// without its structural check is not merely untrusted, it is a panic
	// waiting for the first malformed offset. Leaving Validate nil is safe and
	// simply keeps the descents on the fully validating openers.
	Validate func([]byte, PageRef) error
	// PageSize is the Store allocation quantum and the exact size of metadata
	// pages. Document and overflow extents may be larger whole multiples.
	PageSize int
	// MaxPageSize is the largest contiguous extent admitted into the arena.
	// Zero selects PageSize. Value extents may use any whole number of PageSize
	// slots, and cache misses perform no allocation.
	MaxPageSize   int
	ResidentBytes int64
	StoreID       [16]byte
	PrefetchQueue int
	// ReadConcurrency is the fixed number of prefetch workers. Zero selects
	// four. Demand misses remain synchronous to their caller, while concurrent
	// misses and prefetches use positional reads safely in parallel.
	ReadConcurrency int
	// ReadQueueDepth bounds one native asynchronous submission. Zero selects
	// PrefetchQueue. It is independent from portable worker concurrency.
	ReadQueueDepth int
	// Backend selects the speculative-read engine. Auto tries one pure-Go
	// io_uring issuer on supported Linux kernels and falls back to portable
	// positional reads. Demand misses always retain a synchronous correctness
	// path.
	Backend Backend
}

func (o PageCacheOptions) normalized() (PageCacheOptions, int, error) {
	if o.PageSize == 0 {
		o.PageSize = defaultBufferSize
	}
	if o.StoreID == ([16]byte{}) || o.PageSize < 0 ||
		uint64(o.PageSize) > uint64(^uint32(0)) || !validPhysicalPageSize(uint32(o.PageSize)) {
		return PageCacheOptions{}, 0, fmt.Errorf("%w: Store id or page size", ErrPageCacheReference)
	}
	if o.MaxPageSize == 0 {
		o.MaxPageSize = o.PageSize
	}
	if o.MaxPageSize < o.PageSize || uint64(o.MaxPageSize) > uint64(^uint32(0)) ||
		!validPhysicalPageSize(uint32(o.MaxPageSize)) || o.MaxPageSize%o.PageSize != 0 {
		return PageCacheOptions{}, 0, fmt.Errorf("%w: maximum page size", ErrPageCacheReference)
	}
	if o.ResidentBytes < int64(o.MaxPageSize) {
		return PageCacheOptions{}, 0, fmt.Errorf("%w: resident budget %d is smaller than one %d-byte page",
			ErrPageCacheReference, o.ResidentBytes, o.MaxPageSize)
	}
	slots64 := o.ResidentBytes / int64(o.PageSize)
	maxInt := int64(^uint(0) >> 1)
	if slots64 <= 0 || slots64 > maxInt/int64(o.PageSize) || slots64 > maxInt/2 ||
		slots64 >= int64(cacheTableTombstone-1) {
		return PageCacheOptions{}, 0, fmt.Errorf("%w: resident budget overflows address space", ErrPageCacheReference)
	}
	if o.PrefetchQueue == 0 {
		o.PrefetchQueue = defaultPrefetchQueue
	}
	if o.PrefetchQueue < 1 || o.PrefetchQueue > maxDeviceQueueDepth {
		return PageCacheOptions{}, 0, fmt.Errorf("%w: prefetch queue %d", ErrPageCacheReference, o.PrefetchQueue)
	}
	if o.ReadConcurrency == 0 {
		o.ReadConcurrency = defaultReadConcurrency
	}
	if o.ReadConcurrency < 1 || o.ReadConcurrency > maxDeviceQueueDepth {
		return PageCacheOptions{}, 0, fmt.Errorf("%w: read concurrency %d", ErrPageCacheReference, o.ReadConcurrency)
	}
	if o.ReadQueueDepth == 0 {
		o.ReadQueueDepth = o.PrefetchQueue
	}
	if o.ReadQueueDepth < 1 || o.ReadQueueDepth > maxDeviceQueueDepth {
		return PageCacheOptions{}, 0, fmt.Errorf("%w: read queue depth %d", ErrPageCacheReference, o.ReadQueueDepth)
	}
	if o.Backend > BackendIOUring {
		return PageCacheOptions{}, 0, fmt.Errorf("%w: read backend %d", ErrPageCacheReference, o.Backend)
	}
	o.ResidentBytes = slots64 * int64(o.PageSize)
	return o, int(slots64), nil
}

type pageCacheKey struct {
	offset     uint64
	logicalID  uint64
	generation uint64
	length     uint32
	kind       PageKind
	flags      uint8
	aux        uint16
}

const (
	pageCacheEmpty uint8 = iota
	pageCacheLoading
	pageCacheReady
	pageCacheTail
	// pageCacheExclusive is a writer-held READY -> exclusive -> READY window.
	// It is never published without the frame lock; its distinct value makes
	// every state-machine check fail closed if that locking discipline changes.
	pageCacheExclusive

	cacheTableEmpty     = uint32(0)
	cacheTableTombstone = ^uint32(0)
)

const (
	pageCacheFramePrefetched uint8 = 1 << iota
	pageCacheFrameDoomed
	pageCacheFrameWritePinned
	pageCacheFrameNeedsReseal
	pageCacheFrameBufferedOwned
)

type pageCacheFrame struct {
	key           pageCacheKey
	dirty         uint64
	lock          sync.Mutex
	hits          uint32
	payloadLength uint32
	pins          uint32
	// reservationSpan is the exact number of owned allocation-quantum slots.
	// key.length remains the exact logical extent length exposed to readers and
	// used for I/O/accounting. The byte fits in the frame's existing cache-line
	// tail; MaxPageSize bounds it far below 255.
	reservationSpan uint8
	state           uint8
	referenced      bool
	// flags shares the former prefetched byte with the doomed bit so the frame
	// control record remains one cache line.
	flags uint8
}

type pageCacheDirtyFrame struct {
	generation uint64
	frame      uint32
}

// pageCacheRingLoad is one pointer-free in-flight read descriptor owned by the
// single io_uring worker. frame identifies stable mmap storage; no Go pointer
// is retained by a kernel request.
type pageCacheRingLoad struct {
	ref   PageRef
	key   pageCacheKey
	frame int
	done  bool
}

// PageCacheStats is a point-in-time accounting snapshot. ResidentBytes counts
// exact logical extent bytes and ReservedBytes counts their allocation-quantum
// reservations, both including reads in progress. QueueDepth is sampled from
// the bounded prefetch queue.
type PageCacheStats struct {
	CapacityBytes uint64
	ResidentBytes uint64
	ReservedBytes uint64
	FrameSize     uint32
	// Frames counts allocation-quantum slots; ReadyFrames and LoadingFrames
	// count logical whole extents.
	Frames          uint32
	ReadyFrames     uint32
	LoadingFrames   uint32
	FailedFrames    uint32
	PinnedFrames    uint32
	Pins            uint64
	Hits            uint64
	Misses          uint64
	Coalesced       uint64
	ReadErrors      uint64
	Prefetches      uint64
	CopyOuts        uint64
	PinnedPages     uint64
	DirtyBytes      uint64
	PageReads       uint64
	ReadBytes       uint64
	CacheHits       uint64
	PrefetchHits    uint64
	Evictions       uint64
	PrefetchQueued  uint64
	PrefetchDropped uint64
	// CrossClassTakes counts allocator skew fallbacks that place a reservation
	// in a zone assigned to the other size class.
	CrossClassTakes uint64
	// QueueDepth samples references waiting for a read engine.
	QueueDepth uint64
	// ReadQueueDepth is the configured native submission bound.
	ReadQueueDepth uint32
	ReadBackend    Backend
	// AsyncReadBatches and LargestReadBatch count successful io_uring
	// submissions. Portable worker reads do not increment them.
	AsyncReadBatches uint64
	LargestReadBatch uint32
}

// PageCache owns a fixed off-heap slot arena on common Unix platforms and a
// portable pointer-free byte arena elsewhere. Its control slice contains no
// Go pointers: page and payload views are reconstructed only in a lease.
// It performs explicit positional reads, validates every common page before
// publication, and applies CLOCK replacement to whole extents. It never relies
// on demand-paged mmap for admission or eviction decisions.
type PageCache struct {
	file    *os.File
	options PageCacheOptions
	// dataStart is fixed once at construction so the Acquire hot path rejects
	// allocator-excluded roots and journals with one comparison.
	dataStart uint64
	arena     []byte
	frames    []pageCacheFrame
	table     []atomic.Uint32
	blocks    pageCacheBlocks
	tombs     int
	hand      int
	// evictionScratch holds the head-frame indexes in one exact-span victim
	// window. It is fixed at construction so reservation-driven replacement
	// remains allocation-free.
	evictionScratch []uint32
	// dirtyBytes is the exact logical byte sum reported in Stats. The writer's
	// capacity check uses dirtyReservedBytes to account in arena quanta through
	// each extent's durability fence.
	dirtyBytes         uint64
	dirtyReservedBytes uint64
	// dirtyFrames contains only frames that transitioned from clean to dirty.
	// It lets durability and abort fences visit outstanding writes rather than
	// scan the complete resident arena on every small mutation.
	dirtyFrames []pageCacheDirtyFrame

	mu                sync.Mutex
	cond              *sync.Cond
	closing           atomic.Bool
	closed            bool
	activeLoads       int
	prefetchCloseOnce sync.Once
	prefetch          chan PageRef
	done              chan struct{}
	workers           sync.WaitGroup
	readBackend       atomic.Uint32

	pageReads        uint64
	readBytes        uint64
	cacheHitsBase    atomic.Uint64
	cacheMisses      uint64
	coalesced        uint64
	readErrors       uint64
	copyOuts         uint64
	prefetchHits     atomic.Uint64
	evictions        uint64
	prefetchQueued   uint64
	prefetchDropped  uint64
	asyncReadBatches uint64
	largestReadBatch uint32
}

// NewPageCache creates a bounded read cache over file. The file remains
// caller-owned and must outlive the cache. Construction allocates the complete
// slot arena, then starts either one bounded native issuer or the fixed
// portable prefetch worker set.
func NewPageCache(file *os.File, options PageCacheOptions) (*PageCache, error) {
	if file == nil {
		return nil, fmt.Errorf("%w: nil file", ErrPageCacheReference)
	}
	normalized, slotCount, err := options.normalized()
	if err != nil {
		return nil, err
	}
	layout, err := MutableStoreLayout(uint32(normalized.PageSize))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid mutable Store layout", ErrPageCacheReference)
	}
	arena, err := allocateArena(slotCount * normalized.PageSize)
	if err != nil {
		return nil, fmt.Errorf("vibejson: allocate Store page cache: %w", err)
	}
	c := &PageCache{
		file:        file,
		options:     normalized,
		dataStart:   layout.DataStart,
		arena:       arena,
		frames:      make([]pageCacheFrame, slotCount),
		dirtyFrames: make([]pageCacheDirtyFrame, 0, slotCount),
		blocks:      newPageCacheBlocks(slotCount, normalized.MaxPageSize/normalized.PageSize),
		evictionScratch: make(
			[]uint32, normalized.MaxPageSize/normalized.PageSize,
		),
		prefetch: make(chan PageRef, normalized.PrefetchQueue),
		done:     make(chan struct{}),
	}
	tableSize := 2
	for tableSize < slotCount*2 {
		tableSize <<= 1
	}
	c.table = make([]atomic.Uint32, tableSize)
	c.cond = sync.NewCond(&c.mu)
	if err := c.startPrefetchWorkers(); err != nil {
		_ = releaseArena(arena)
		return nil, err
	}
	go func() {
		c.workers.Wait()
		close(c.done)
	}()
	return c, nil
}

func (c *PageCache) startPrefetchWorkers() error {
	if c.options.Backend != BackendPortable {
		initialized := make(chan error)
		c.workers.Add(1)
		go c.runRingPrefetch(initialized)
		if err := <-initialized; err == nil {
			return nil
		} else if c.options.Backend == BackendIOUring {
			c.workers.Wait()
			return err
		}
		c.workers.Wait()
	}
	c.readBackend.Store(uint32(BackendPortable))
	c.workers.Add(c.options.ReadConcurrency)
	for range c.options.ReadConcurrency {
		go c.runPrefetch()
	}
	return nil
}

// PageLease pins one validated frame. The value is single-owner and must not
// be copied after first use. Payload and Header remain valid until Release.
type PageLease struct {
	cache         *PageCache
	frame         int
	key           pageCacheKey
	payloadLength uint32
	page          []byte
}

// Header returns the immutable identity of the leased page.
func (l *PageLease) Header() PageHeader {
	if l == nil || l.cache == nil {
		return PageHeader{}
	}
	return PageHeader{
		StoreID: l.cache.options.StoreID, Generation: l.key.generation, LogicalID: l.key.logicalID,
		PageSize: l.key.length, PayloadLength: l.payloadLength, Kind: l.key.kind,
		Flags: l.key.flags,
	}
}

// Payload returns a capacity-clipped view of the validated page payload. The
// view becomes invalid after Release.
func (l *PageLease) Payload() []byte {
	if l == nil || l.page == nil {
		return nil
	}
	end := PageHeaderSize + int(l.payloadLength)
	return l.page[PageHeaderSize:end:end]
}

// Page returns the complete capacity-clipped common page for typed page
// decoders. It becomes invalid after Release.
func (l *PageLease) Page() []byte {
	if l == nil {
		return nil
	}
	return l.page
}

// Bytes is the StorePageReader compatibility spelling of Page.
func (l *PageLease) Bytes() []byte { return l.Page() }

// Release unpins the frame. It is idempotent for one PageLease value.
func (l *PageLease) Release() {
	if l == nil || l.cache == nil {
		return
	}
	cache := l.cache
	cache.release(l.frame, l.key)
	l.cache = nil
	l.page = nil
	l.payloadLength = 0
}

// Close releases one StorePageReader lease and diagnoses a repeated close.
// The durable Store uses the idempotent Release form for defer-friendly cleanup.
func (l *PageLease) Close() error {
	if l == nil || l.cache == nil {
		return ErrPageLeaseClosed
	}
	l.Release()
	return nil
}

// Acquire returns a lease over ref. Concurrent misses for the same ref share
// one physical read. A miss returns ErrPageCachePinned when the fixed budget
// contains no clean, unpinned contiguous span; it never grows the resident set.
func (c *PageCache) Acquire(ref PageRef) (PageLease, error) {
	return c.load(ref, true, false)
}

// acquireFrameHinted is the narrow resident-router entry point. The hint never
// grants access by itself: tryPinFrameReady validates the complete cache key
// under the frame lock before incrementing pins. A miss follows ordinary
// Acquire semantics and publishes the resulting stable frame for later reads.
func (c *PageCache) acquireFrameHinted(
	ref PageRef,
	hint *pageCacheFrameHint,
) (PageLease, error) {
	if hint == nil {
		return c.Acquire(ref)
	}
	key, err := c.validateRef(ref)
	if err != nil {
		return PageLease{}, err
	}
	stamp := pageCacheKeyStamp(key)
	packed := hint.packed.Load()
	if uint32(packed>>32) == stamp {
		var lease PageLease
		if c.tryPinFrameReady(int(uint32(packed))-1, key, &lease) {
			return lease, nil
		}
	}
	lease, err := c.Acquire(ref)
	if err != nil {
		return PageLease{}, err
	}
	hint.packed.Store(
		uint64(stamp)<<32 | uint64(uint32(lease.frame)+1),
	)
	return lease, nil
}

// Pin is the StorePageReader compatibility spelling of Acquire.
func (c *PageCache) Pin(ref PageRef) (PageLease, error) { return c.Acquire(ref) }

// ReservationBytes returns the cache-arena bytes a whole logical extent owns.
// It returns zero when length is not a non-zero, allocation-quantum-aligned
// extent within MaxPageSize. The calculation takes no lock and performs no
// allocation, so read-ahead planners can budget a prospective batch before
// submitting any reads.
func (c *PageCache) ReservationBytes(length uint32) uint64 {
	if c == nil {
		return 0
	}
	quantum := uint64(c.options.PageSize)
	logical := uint64(length)
	if logical < quantum || logical > uint64(c.options.MaxPageSize) ||
		logical%quantum != 0 {
		return 0
	}
	return uint64(pageCacheReservedSpan(int(logical/quantum))) * quantum
}

// ValidatesOnAdmission reports whether a typed payload check runs once, before
// a page becomes visible, so that resident lookups may reconstruct a view
// instead of revalidating it. It is the guard on every Admitted* reconstruction
// reached from a cached page: without it a descent must keep paying the
// checksum and whole-node walk on every level of every read, which is correct
// but was measured at roughly three quarters of a warm point read.
//
// Dirty pages are covered even though AdmitDirty does not call Validate: they
// were produced by this process's encoders, which validate everything they
// write, and they are checksum-verified on the way in.
func (c *PageCache) ValidatesOnAdmission() bool {
	return c != nil && c.options.Validate != nil
}

// AppendPage copies one validated page into dst and releases its frame.
func (c *PageCache) AppendPage(dst []byte, ref PageRef) ([]byte, error) {
	lease, err := c.Acquire(ref)
	if err != nil {
		return dst, err
	}
	dst = append(dst, lease.Page()...)
	lease.Release()
	c.mu.Lock()
	c.copyOuts++
	c.mu.Unlock()
	return dst, nil
}

// Invalidate removes one clean, unpinned admitted reference. Loading, dirty,
// or leased frames remain in place.
func (c *PageCache) Invalidate(ref PageRef) bool {
	if c == nil {
		return false
	}
	key, err := c.validateRef(ref)
	if err != nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	index, ok := c.lookupLocked(cacheKeyHash(key), key)
	if !ok {
		return false
	}
	frame := &c.frames[index]
	frame.lock.Lock()
	defer frame.lock.Unlock()
	if frame.state == pageCacheLoading || frame.dirty != 0 ||
		frame.pins != 0 || frame.flags&pageCacheFrameWritePinned != 0 {
		return false
	}
	c.resetExtentLocked(index)
	return true
}

// reserveDirtyFrame reserves the cache extent before encoding and returns its
// exact capacity-clipped bytes. The frame is published in loading state, so it
// cannot be evicted or observed by a reader before sealDirtyFrame validates
// the encoder's seal and switches it to ready.
func (c *PageCache) reserveDirtyFrame(
	ref PageRef, dirtyGeneration uint64,
) (int, []byte, error) {
	key, err := c.validateRef(ref)
	if err != nil || dirtyGeneration == 0 ||
		dirtyGeneration < ref.Generation {
		return 0, nil, fmt.Errorf(
			"%w: dirty page reference or generation",
			ErrPageCacheReference,
		)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing.Load() || c.closed {
		return 0, nil, ErrPageCacheClosed
	}
	hash := cacheKeyHash(key)
	if _, ok := c.lookupLocked(hash, key); ok {
		return 0, nil, fmt.Errorf(
			"%w: duplicate dirty page reservation",
			ErrPageCacheReference,
		)
	}
	logicalSpan := int(ref.Length) / c.options.PageSize
	reservedSpan := pageCacheReservedSpan(logicalSpan)
	var index int
	for {
		var ok bool
		index, ok = c.reserveLocked(reservedSpan)
		if ok {
			break
		}
		if c.activeLoads == 0 {
			return 0, nil, ErrPageCachePinned
		}
		c.cond.Wait()
		if c.closing.Load() || c.closed {
			return 0, nil, ErrPageCacheClosed
		}
	}
	frame := &c.frames[index]
	frame.lock.Lock()
	c.beginExtentLocked(index, reservedSpan, key, hash)
	frame.dirty = dirtyGeneration
	frame.flags |= pageCacheFrameWritePinned
	c.dirtyBytes += uint64(frame.key.length)
	c.dirtyReservedBytes +=
		uint64(frame.reservationSpan) * uint64(c.options.PageSize)
	c.recordDirtyFrameLocked(index, dirtyGeneration)
	page := c.extentBytes(index, ref.Length)
	frame.lock.Unlock()
	return index, page, nil
}

// sealDirtyFrame completes a writer-owned reservation without copying. The
// caller has already authenticated the exact frame slice and checked the seal
// marker; identity remains enforced here before readers can observe the page.
func (c *PageCache) sealDirtyFrame(
	index int,
	ref PageRef,
	dirtyGeneration uint64,
	header PageHeader,
	page []byte,
) error {
	key, err := c.validateRef(ref)
	if err != nil || index < 0 || index >= len(c.frames) ||
		len(page) != int(ref.Length) || cap(page) != int(ref.Length) ||
		header.StoreID != c.options.StoreID ||
		header.PageSize != ref.Length ||
		header.LogicalID != ref.LogicalID ||
		header.Generation != ref.Generation ||
		header.Kind != ref.Kind || header.Flags != ref.Flags {
		return fmt.Errorf(
			"%w: dirty page identity does not match reservation",
			ErrPageCacheReference,
		)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing.Load() || c.closed {
		return ErrPageCacheClosed
	}
	frame := &c.frames[index]
	frame.lock.Lock()
	defer frame.lock.Unlock()
	exact := c.extentBytes(index, ref.Length)
	if frame.state != pageCacheLoading ||
		frame.key != key || frame.dirty != dirtyGeneration ||
		&page[0] != &exact[0] {
		return fmt.Errorf(
			"%w: dirty page reservation changed",
			ErrPageCacheReference,
		)
	}
	frame.payloadLength = header.PayloadLength
	frame.state = pageCacheReady
	frame.referenced = true
	c.cond.Broadcast()
	return nil
}

// releaseFrameWrite drops one committer descriptor's physical-write ownership.
// Reader visibility remains governed by MarkUnreachable/MarkDurable, which run
// at their snapshot-gated lifecycle points.
func (c *PageCache) releaseFrameWrite(write Write, generation uint64) {
	if c == nil || !write.frameNative() ||
		int(write.frameIndex) >= len(c.frames) {
		return
	}
	c.mu.Lock()
	index := int(write.frameIndex)
	frame := &c.frames[index]
	frame.lock.Lock()
	available := false
	reset := false
	if frame.state == pageCacheReady &&
		frame.flags&pageCacheFrameWritePinned != 0 &&
		frame.key.offset == uint64(write.Offset) &&
		frame.key.length == write.Length && frame.key.kind == write.kind &&
		frame.key.generation == generation {
		frame.flags &^= pageCacheFrameWritePinned
		available = true
		if frame.pins == 0 &&
			(frame.flags&pageCacheFrameDoomed != 0 ||
				frame.key.kind == PageStateRoot && frame.dirty == 0) {
			c.resetExtentLocked(index)
			reset = true
		}
	}
	frame.lock.Unlock()
	if reset {
		c.compactDirtyFramesLocked()
	}
	if available {
		c.cond.Broadcast()
	}
	c.mu.Unlock()
}

// AdmitDirty copies one newly encoded immutable page into the bounded cache
// before its asynchronous commit becomes durable. dirtyGeneration is the
// publication whose final root will make ref reachable. Dirty frames are not
// eviction candidates until MarkDurable advances past that generation, so a
// following mutation never has to read an as-yet-unwritten page from disk.
func (c *PageCache) AdmitDirty(ref PageRef, src []byte, dirtyGeneration uint64) error {
	key, err := c.validateRef(ref)
	if err != nil || dirtyGeneration == 0 || dirtyGeneration < ref.Generation || len(src) < int(ref.Length) {
		return fmt.Errorf("%w: dirty page reference, generation, or bytes", ErrPageCacheReference)
	}
	src = src[:int(ref.Length)]
	header, _, err := OpenPage(src)
	if err != nil {
		return err
	}
	return c.admitDirtyValidated(
		key, ref, src, dirtyGeneration, header, false,
	)
}

// AdmitBufferedDirty admits one memory-only buffered-visible frame. It has no
// committer descriptor, but remains writer-owned for the canonical in-place
// lane until the collection rebases or discards its router handle.
func (c *PageCache) AdmitBufferedDirty(
	ref PageRef, src []byte, dirtyGeneration uint64,
) error {
	key, err := c.validateRef(ref)
	if err != nil || dirtyGeneration == 0 ||
		dirtyGeneration < ref.Generation ||
		len(src) < int(ref.Length) {
		return fmt.Errorf(
			"%w: buffered dirty page reference, generation, or bytes",
			ErrPageCacheReference,
		)
	}
	src = src[:int(ref.Length)]
	header, _, err := OpenPage(src)
	if err != nil {
		return err
	}
	return c.admitDirtyValidated(
		key, ref, src, dirtyGeneration, header, true,
	)
}

// admitDirtyTrusted is the narrow writer-owned admission lane. The transaction
// authenticates src as its exact reserved, sealed buffer before calling; device
// reads and generic callers continue through AdmitDirty and full CRC
// verification. Reference and decoded-header consistency remain checked here.
func (c *PageCache) admitDirtyTrusted(
	ref PageRef, src []byte, dirtyGeneration uint64, header PageHeader,
) error {
	key, err := c.validateRef(ref)
	if err != nil || dirtyGeneration == 0 || dirtyGeneration < ref.Generation ||
		len(src) < int(ref.Length) {
		return fmt.Errorf(
			"%w: dirty page reference, generation, or bytes",
			ErrPageCacheReference,
		)
	}
	src = src[:int(ref.Length)]
	return c.admitDirtyValidated(
		key, ref, src, dirtyGeneration, header, false,
	)
}

func (c *PageCache) admitDirtyValidated(
	key pageCacheKey,
	ref PageRef,
	src []byte,
	dirtyGeneration uint64,
	header PageHeader,
	bufferedOwned bool,
) error {
	if header.StoreID != c.options.StoreID || header.PageSize != ref.Length ||
		header.LogicalID != ref.LogicalID || header.Generation != ref.Generation ||
		header.Kind != ref.Kind || header.Flags != ref.Flags {
		return fmt.Errorf("%w: dirty page identity does not match reference", ErrPageCacheReference)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing.Load() || c.closed {
		return ErrPageCacheClosed
	}
	hash := cacheKeyHash(key)
	if index, ok := c.lookupLocked(hash, key); ok {
		frame := &c.frames[index]
		frame.lock.Lock()
		defer frame.lock.Unlock()
		if frame.state != pageCacheReady || frame.dirty != dirtyGeneration ||
			!bytes.Equal(c.extentBytes(index, ref.Length), src) {
			return fmt.Errorf("%w: conflicting dirty page", ErrPageCacheReference)
		}
		if bufferedOwned {
			frame.flags |= pageCacheFrameBufferedOwned
		}
		return nil
	}
	logicalSpan := int(ref.Length) / c.options.PageSize
	reservedSpan := pageCacheReservedSpan(logicalSpan)
	var index int
	for {
		var ok bool
		index, ok = c.reserveLocked(reservedSpan)
		if ok {
			break
		}
		if c.activeLoads == 0 {
			return ErrPageCachePinned
		}
		c.cond.Wait()
		if c.closing.Load() || c.closed {
			return ErrPageCacheClosed
		}
	}
	frame := &c.frames[index]
	frame.lock.Lock()
	defer frame.lock.Unlock()
	c.beginExtentLocked(index, reservedSpan, key, hash)
	page := c.extentBytes(index, ref.Length)
	copy(page, src)
	frame.payloadLength = header.PayloadLength
	c.dirtyBytes += uint64(frame.key.length)
	c.dirtyReservedBytes += uint64(frame.reservationSpan) * uint64(c.options.PageSize)
	frame.dirty = dirtyGeneration
	if bufferedOwned {
		frame.flags |= pageCacheFrameBufferedOwned
	}
	c.recordDirtyFrameLocked(index, dirtyGeneration)
	frame.state = pageCacheReady
	frame.referenced = true
	return nil
}

// MarkDurable makes admitted pages through generation ordinary eviction
// candidates. It performs no file I/O.
func (c *PageCache) MarkDurable(generation uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	kept := c.dirtyFrames[:0]
	for _, dirty := range c.dirtyFrames {
		if int(dirty.frame) >= len(c.frames) {
			continue
		}
		frame := &c.frames[dirty.frame]
		frame.lock.Lock()
		if frame.dirty != dirty.generation {
			frame.lock.Unlock()
			continue
		}
		if dirty.generation <= generation {
			if frame.key.kind == PageStateRoot && frame.pins == 0 &&
				frame.flags&pageCacheFrameWritePinned == 0 {
				// State roots are checkpoint data but readers retain their
				// decoded state and never fault these pages through PageCache.
				// Return their staging-only frame immediately at the fence.
				c.resetExtentLocked(int(dirty.frame))
			} else {
				c.dirtyBytes -= uint64(frame.key.length)
				c.dirtyReservedBytes -=
					uint64(frame.reservationSpan) * uint64(c.options.PageSize)
				frame.dirty = 0
			}
		} else {
			kept = append(kept, dirty)
		}
		frame.lock.Unlock()
	}
	clear(c.dirtyFrames[len(kept):])
	c.dirtyFrames = kept
	c.mu.Unlock()
}

// MarkUnreachable releases exact page versions that a successfully published
// copy-on-write root made unreachable while snapshot acquisition was excluded.
// It is deliberately reference-based rather than generation-based: unchanged
// pages from older generations may remain reachable from the new root.
//
// A pinned frame keeps its bytes until the current internal user releases it,
// but stops consuming dirty capacity immediately and is doomed for removal on
// its last unpin. An unpinned ready frame is removed at once. The caller must
// prove that no current or future snapshot can acquire any ref.
func (c *PageCache) MarkUnreachable(refs []PageRef) {
	if c == nil || len(refs) == 0 {
		return
	}
	c.mu.Lock()
	released := false
	for _, ref := range refs {
		key, err := c.validateRef(ref)
		if err != nil {
			continue
		}
		index, ok := c.lookupLocked(cacheKeyHash(key), key)
		if !ok {
			continue
		}
		frame := &c.frames[index]
		frame.lock.Lock()
		if frame.state != pageCacheReady {
			frame.lock.Unlock()
			continue
		}
		if frame.flags&pageCacheFrameWritePinned != 0 {
			frame.flags |= pageCacheFrameDoomed
			frame.referenced = false
		} else if frame.pins == 0 {
			c.resetExtentLocked(index)
			released = true
		} else {
			if frame.dirty != 0 {
				c.dirtyBytes -= uint64(frame.key.length)
				c.dirtyReservedBytes -=
					uint64(frame.reservationSpan) * uint64(c.options.PageSize)
				frame.dirty = 0
			}
			frame.flags |= pageCacheFrameDoomed
			frame.referenced = false
		}
		frame.lock.Unlock()
	}
	c.compactDirtyFramesLocked()
	if released {
		c.cond.Broadcast()
	}
	c.mu.Unlock()
}

// DiscardDirty removes unreachable pages from an aborted publication. Callers
// must release any internal planning lease first.
func (c *PageCache) DiscardDirty(generation uint64) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	released := false
	for _, dirty := range c.dirtyFrames {
		if dirty.generation != generation || int(dirty.frame) >= len(c.frames) {
			continue
		}
		frame := &c.frames[dirty.frame]
		frame.lock.Lock()
		pinned := frame.dirty == dirty.generation && frame.pins != 0
		frame.lock.Unlock()
		if pinned {
			return ErrPageCachePinned
		}
	}
	for _, dirty := range c.dirtyFrames {
		if dirty.generation != generation || int(dirty.frame) >= len(c.frames) {
			continue
		}
		frame := &c.frames[dirty.frame]
		frame.lock.Lock()
		if frame.dirty == dirty.generation {
			c.resetExtentLocked(int(dirty.frame))
			released = true
		}
		frame.lock.Unlock()
	}
	c.compactDirtyFramesLocked()
	if released {
		c.cond.Broadcast()
	}
	return nil
}

func (c *PageCache) recordDirtyFrameLocked(index int, generation uint64) {
	if index < 0 || index >= len(c.frames) || generation == 0 {
		return
	}
	if len(c.dirtyFrames) == cap(c.dirtyFrames) {
		c.compactDirtyFramesLocked()
	}
	// Every live dirty frame consumes at least one cache slot, so compaction
	// must leave room for a clean-to-dirty transition.
	c.dirtyFrames = append(c.dirtyFrames, pageCacheDirtyFrame{
		generation: generation, frame: uint32(index),
	})
}

func (c *PageCache) removeDirtyFrameLocked(index int, generation uint64) {
	out := c.dirtyFrames[:0]
	for _, dirty := range c.dirtyFrames {
		if int(dirty.frame) == index && dirty.generation == generation {
			continue
		}
		out = append(out, dirty)
	}
	clear(c.dirtyFrames[len(out):])
	c.dirtyFrames = out
}

func (c *PageCache) compactDirtyFramesLocked() {
	out := c.dirtyFrames[:0]
	for _, dirty := range c.dirtyFrames {
		if int(dirty.frame) >= len(c.frames) {
			continue
		}
		// Every dirty transition is serialized by c.mu; frame.lock protects
		// concurrent pins and page bytes, neither of which this compaction reads.
		if c.frames[dirty.frame].dirty == dirty.generation {
			out = append(out, dirty)
		}
	}
	clear(c.dirtyFrames[len(out):])
	c.dirtyFrames = out
}

func (c *PageCache) load(ref PageRef, pin, prefetch bool) (PageLease, error) {
	key, err := c.validateRef(ref)
	if err != nil {
		return PageLease{}, err
	}
	hash := cacheKeyHash(key)
	if pin {
		var lease PageLease
		if c.tryPinReady(hash, key, &lease) {
			return lease, nil
		}
	}

	c.mu.Lock()
	logicalSpan := int(ref.Length) / c.options.PageSize
	reservedSpan := pageCacheReservedSpan(logicalSpan)
	for {
		if c.closing.Load() || c.closed {
			c.mu.Unlock()
			return PageLease{}, ErrPageCacheClosed
		}
		if index, ok := c.lookupLocked(hash, key); ok {
			frame := &c.frames[index]
			frame.lock.Lock()
			switch frame.state {
			case pageCacheLoading:
				frame.lock.Unlock()
				if !pin {
					c.mu.Unlock()
					return PageLease{}, nil
				}
				c.coalesced++
				c.cond.Wait()
				continue
			case pageCacheReady:
				if !pin {
					frame.lock.Unlock()
					c.mu.Unlock()
					return PageLease{}, nil
				}
				if frame.pins == ^uint32(0) {
					frame.lock.Unlock()
					c.mu.Unlock()
					return PageLease{}, ErrPageCachePinned
				}
				frame.pins++
				frame.referenced = true
				c.recordFrameHit(frame)
				if frame.flags&pageCacheFramePrefetched != 0 {
					frame.flags &^= pageCacheFramePrefetched
					c.prefetchHits.Add(1)
				}
				page := c.extentBytes(index, key.length)
				payloadLength := frame.payloadLength
				lease := PageLease{cache: c, frame: index, key: key, payloadLength: payloadLength,
					page: page}
				frame.lock.Unlock()
				c.mu.Unlock()
				return lease, nil
			default:
				frame.lock.Unlock()
			}
		}

		index, ok := c.reserveLocked(reservedSpan)
		if !ok {
			if prefetch {
				c.prefetchDropped++
				c.mu.Unlock()
				return PageLease{}, nil
			}
			if c.activeLoads != 0 {
				c.cond.Wait()
				continue
			}
			c.mu.Unlock()
			return PageLease{}, ErrPageCachePinned
		}
		c.cacheMisses++
		frame := &c.frames[index]
		frame.lock.Lock()
		c.beginExtentLocked(index, reservedSpan, key, hash)
		frame.referenced = pin
		if prefetch {
			frame.flags |= pageCacheFramePrefetched
		}
		c.activeLoads++
		data := c.extentBytes(index, ref.Length)
		frame.lock.Unlock()
		c.mu.Unlock()

		page := data[:int(ref.Length):int(ref.Length)]
		n, readErr := c.file.ReadAt(page, int64(ref.Offset))
		if readErr == nil && n != len(page) {
			readErr = io.ErrUnexpectedEOF
		}
		if readErr == nil {
			var header PageHeader
			header, readErr = c.validateLoadedPage(page, ref)
			c.mu.Lock()
			c.pageReads++
			c.readBytes += uint64(n)
			c.activeLoads--
			frame = &c.frames[index]
			frame.lock.Lock()
			if readErr == nil {
				frame.payloadLength = header.PayloadLength
				frame.state = pageCacheReady
				if pin {
					frame.pins = 1
				}
				c.cond.Broadcast()
				if !pin {
					frame.lock.Unlock()
					c.mu.Unlock()
					return PageLease{}, nil
				}
				lease := PageLease{cache: c, frame: index, key: key, payloadLength: header.PayloadLength,
					page: data}
				frame.lock.Unlock()
				c.mu.Unlock()
				return lease, nil
			}
			c.readErrors++
			c.resetExtentLocked(index)
			frame.lock.Unlock()
			c.cond.Broadcast()
			c.mu.Unlock()
			return PageLease{}, readErr
		}

		c.mu.Lock()
		c.pageReads++
		c.readBytes += uint64(n)
		c.activeLoads--
		frame = &c.frames[index]
		c.readErrors++
		frame.lock.Lock()
		c.resetExtentLocked(index)
		frame.lock.Unlock()
		c.cond.Broadcast()
		c.mu.Unlock()
		return PageLease{}, readErr
	}
}

func (c *PageCache) validateLoadedPage(page []byte, ref PageRef) (PageHeader, error) {
	header, _, err := OpenPage(page)
	if err == nil && (header.StoreID != c.options.StoreID || header.PageSize != ref.Length ||
		header.LogicalID != ref.LogicalID || header.Generation != ref.Generation ||
		header.Kind != ref.Kind || header.Flags != ref.Flags) {
		err = fmt.Errorf("%w: physical page identity does not match reference", ErrPageCacheReference)
	}
	if err == nil && c.options.Validate != nil {
		err = c.options.Validate(page, ref)
	}
	return header, err
}

func (c *PageCache) reserveLocked(span int) (int, bool) {
	if start, ok := c.blocks.take(span); ok {
		return start, true
	}
	if span > 1 {
		if start, ok := c.reserveWindowLocked(span); ok {
			return start, true
		}
	}
	return c.reserveClockLocked(span)
}

type pageCacheWindowScore struct {
	empty      int
	referenced int
	hits       uint64
}

func (c *PageCache) reserveWindowLocked(span int) (int, bool) {
	class := pageCacheBlockClassForSpan(span)
	bestStart, found := c.findWindowLocked(span, class, true)
	if !found {
		bestStart, found = c.findWindowLocked(span, class, false)
	}
	if !found || !c.evictWindowLocked(bestStart, span) {
		return 0, false
	}
	start, ok := c.blocks.take(span)
	if !ok {
		panic("storeio: page-cache eviction did not free its reservation")
	}
	c.hand = start + span
	if c.hand == len(c.frames) {
		c.hand = 0
	}
	return start, true
}

func (c *PageCache) findWindowLocked(
	span int, class pageCacheBlockClass, matchingClassOnly bool,
) (int, bool) {
	zoneSlots := int(c.blocks.zoneSlots)
	zoneCount := len(c.blocks.zoneClasses)
	if span <= 0 || span > zoneSlots || zoneCount == 0 ||
		len(c.evictionScratch) < span {
		return 0, false
	}
	scanZones := min(zoneCount, pageCacheEvictionScanZones)
	startZone := c.hand / zoneSlots
	if startZone >= zoneCount {
		startZone = 0
	}

	bestStart := 0
	best := pageCacheWindowScore{}
	found := false
	for scannedZone := range scanZones {
		zone := startZone + scannedZone
		if zone >= zoneCount {
			zone -= zoneCount
		}
		if matchingClassOnly {
			zoneClass := c.blocks.zoneClasses[zone]
			if zoneClass != class && zoneClass != pageCacheBlockUnassigned {
				continue
			}
		}
		zoneStart := zone * zoneSlots
		zoneEnd := min(zoneStart+zoneSlots, len(c.frames))
		lastStart := zoneEnd - span
		if lastStart < zoneStart {
			continue
		}
		firstOffset := 0
		if scannedZone == 0 && c.hand >= zoneStart && c.hand <= lastStart {
			firstOffset = c.hand - zoneStart
		}
		startCount := lastStart - zoneStart + 1
		for scannedStart := range startCount {
			offset := firstOffset + scannedStart
			if offset >= startCount {
				offset -= startCount
			}
			start := zoneStart + offset
			score, ok := c.scoreWindowLocked(start, span)
			if !ok {
				continue
			}
			if !found || score.empty > best.empty ||
				score.empty == best.empty && score.referenced < best.referenced ||
				score.empty == best.empty && score.referenced == best.referenced &&
					score.hits < best.hits {
				bestStart = start
				best = score
				found = true
			}
		}
	}
	return bestStart, found
}

func (c *PageCache) scoreWindowLocked(start, span int) (pageCacheWindowScore, bool) {
	end := start + span
	score := pageCacheWindowScore{}
	for index := start; index < end; {
		frame := &c.frames[index]
		switch frame.state {
		case pageCacheEmpty:
			score.empty++
			index++
		case pageCacheReady:
			frame.lock.Lock()
			if frame.state != pageCacheReady || frame.dirty != 0 ||
				frame.pins != 0 ||
				frame.flags&pageCacheFrameWritePinned != 0 {
				frame.lock.Unlock()
				return pageCacheWindowScore{}, false
			}
			extentSpan := int(frame.reservationSpan)
			if extentSpan > end-index {
				frame.lock.Unlock()
				return pageCacheWindowScore{}, false
			}
			if frame.referenced {
				score.referenced++
				frame.referenced = false
			}
			if ^uint64(0)-score.hits < uint64(frame.hits) {
				score.hits = ^uint64(0)
			} else {
				score.hits += uint64(frame.hits)
			}
			frame.lock.Unlock()
			index += extentSpan
		default:
			// A tail reached without its ready head belongs to an extent that
			// crosses the window. Loading extents are likewise ineligible.
			return pageCacheWindowScore{}, false
		}
	}
	return score, true
}

func (c *PageCache) evictWindowLocked(start, span int) bool {
	end := start + span
	count := 0
	for index := start; index < end; {
		frame := &c.frames[index]
		switch frame.state {
		case pageCacheEmpty:
			index++
		case pageCacheReady:
			extentSpan := int(frame.reservationSpan)
			if extentSpan > end-index || count == len(c.evictionScratch) {
				return false
			}
			c.evictionScratch[count] = uint32(index)
			count++
			index += extentSpan
		default:
			return false
		}
	}

	for locked := 0; locked < count; locked++ {
		c.frames[c.evictionScratch[locked]].lock.Lock()
	}
	for checked := 0; checked < count; checked++ {
		index := int(c.evictionScratch[checked])
		frame := &c.frames[index]
		extentSpan := int(frame.reservationSpan)
		if frame.state != pageCacheReady || frame.dirty != 0 ||
			frame.pins != 0 ||
			frame.flags&pageCacheFrameWritePinned != 0 ||
			index < start || extentSpan > end-index {
			for locked := count - 1; locked >= 0; locked-- {
				c.frames[c.evictionScratch[locked]].lock.Unlock()
			}
			return false
		}
	}
	for evicted := 0; evicted < count; evicted++ {
		c.resetExtentLocked(int(c.evictionScratch[evicted]))
		c.evictions++
	}
	for locked := count - 1; locked >= 0; locked-- {
		c.frames[c.evictionScratch[locked]].lock.Unlock()
	}
	return true
}

func (c *PageCache) reserveClockLocked(span int) (int, bool) {
	for scanned := 0; scanned < len(c.frames)*2; scanned++ {
		index := c.hand
		c.hand++
		if c.hand == len(c.frames) {
			c.hand = 0
		}
		frame := &c.frames[index]
		if frame.state != pageCacheReady {
			continue
		}
		frame.lock.Lock()
		if frame.state != pageCacheReady || frame.dirty != 0 ||
			frame.pins != 0 ||
			frame.flags&pageCacheFrameWritePinned != 0 {
			frame.lock.Unlock()
			continue
		}
		if frame.referenced {
			frame.referenced = false
			frame.lock.Unlock()
			continue
		}
		c.resetExtentLocked(index)
		frame.lock.Unlock()
		c.evictions++
		if start, ok := c.blocks.take(span); ok {
			return start, true
		}
	}
	return 0, false
}

func pageCacheReservedSpan(logicalSpan int) int {
	return logicalSpan
}

func (c *PageCache) beginExtentLocked(index, reservedSpan int, key pageCacheKey, hash uint64) {
	frame := &c.frames[index]
	// A reserved frame is either free or was evicted, and eviction refuses
	// dirty frames, so this cannot be dropping unfenced bytes. Discount it
	// anyway: the counter must never survive a frame it no longer describes.
	if frame.dirty != 0 {
		c.dirtyBytes -= uint64(frame.key.length)
		c.dirtyReservedBytes -= uint64(frame.reservationSpan) * uint64(c.options.PageSize)
	}
	frame.key = key
	frame.dirty = 0
	frame.hits = 0
	frame.payloadLength = 0
	frame.reservationSpan = uint8(reservedSpan)
	frame.pins = 0
	frame.state = pageCacheLoading
	frame.referenced = false
	frame.flags = 0
	for slot := 1; slot < reservedSpan; slot++ {
		tail := &c.frames[index+slot]
		tail.key = pageCacheKey{}
		tail.dirty = 0
		tail.hits = 0
		tail.payloadLength = 0
		tail.reservationSpan = 0
		tail.pins = 0
		tail.state = pageCacheTail
		tail.referenced = false
		tail.flags = 0
	}
	c.insertLocked(hash, index)
}

// resetExtentLocked removes one complete extent. The caller holds c.mu and
// the head frame lock; tail slots are never published in the lookup table.
func (c *PageCache) resetExtentLocked(index int) {
	frame := &c.frames[index]
	c.removeLocked(cacheKeyHash(frame.key), frame.key)
	reservedSpan := int(frame.reservationSpan)
	if frame.dirty != 0 {
		c.dirtyBytes -= uint64(frame.key.length)
		c.dirtyReservedBytes -= uint64(reservedSpan) * uint64(c.options.PageSize)
	}
	frame.key = pageCacheKey{}
	frame.dirty = 0
	c.cacheHitsBase.Add(uint64(frame.hits))
	frame.hits = 0
	frame.payloadLength = 0
	frame.reservationSpan = 0
	frame.pins = 0
	frame.state = pageCacheEmpty
	frame.referenced = false
	frame.flags = 0
	for slot := 1; slot < reservedSpan; slot++ {
		tail := &c.frames[index+slot]
		tail.key = pageCacheKey{}
		tail.dirty = 0
		tail.hits = 0
		tail.payloadLength = 0
		tail.reservationSpan = 0
		tail.pins = 0
		tail.state = pageCacheEmpty
		tail.referenced = false
		tail.flags = 0
	}
	c.blocks.put(index, reservedSpan)
	if c.hand > index && c.hand < index+reservedSpan {
		c.hand = index
	}
}

func (c *PageCache) extentBytes(index int, length uint32) []byte {
	start := index * c.options.PageSize
	end := start + int(length)
	return c.arena[start:end:end]
}

// tryPinReady is the allocation-free resident path. The table can briefly
// name a frame being replaced, so the per-frame lock always rechecks the full
// immutable key and state. Replacement takes the same lock; after pins rises,
// the complete extent remains stable until Release.
func (c *PageCache) tryPinReady(hash uint64, key pageCacheKey, lease *PageLease) bool {
	if c.closing.Load() {
		return false
	}
	mask := uint64(len(c.table) - 1)
	for probe := uint64(0); probe < uint64(len(c.table)); probe++ {
		entry := c.table[(hash+probe)&mask].Load()
		if entry == cacheTableEmpty {
			return false
		}
		if entry == cacheTableTombstone {
			continue
		}
		index := int(entry - 1)
		if c.tryPinFrameReady(index, key, lease) {
			return true
		}
	}
	return false
}

// tryPinFrameReady mirrors tryPinReady's frame discipline without consulting
// the lookup table. The full immutable identity check is what makes stale,
// collided, and frame-reuse hints fail closed.
func (c *PageCache) tryPinFrameReady(
	index int,
	key pageCacheKey,
	lease *PageLease,
) bool {
	if c.closing.Load() || index < 0 || index >= len(c.frames) {
		return false
	}
	frame := &c.frames[index]
	frame.lock.Lock()
	// Spell out the immutable identity to avoid generic padded-struct
	// equality while still rejecting corrupt references, hint collisions,
	// and safely reused offsets.
	if c.closing.Load() || frame.state != pageCacheReady ||
		frame.key.offset != key.offset || frame.key.generation != key.generation ||
		frame.key.logicalID != key.logicalID || frame.key.length != key.length ||
		frame.key.kind != key.kind || frame.key.flags != key.flags ||
		frame.key.aux != key.aux || frame.pins == ^uint32(0) {
		frame.lock.Unlock()
		return false
	}
	frame.pins++
	frame.referenced = true
	c.recordFrameHit(frame)
	if frame.flags&pageCacheFramePrefetched != 0 {
		frame.flags &^= pageCacheFramePrefetched
		c.prefetchHits.Add(1)
	}
	page := c.extentBytes(index, key.length)
	payloadLength := frame.payloadLength
	*lease = PageLease{cache: c, frame: index, key: key, payloadLength: payloadLength,
		page: page}
	frame.lock.Unlock()
	return true
}

// recordFrameHit keeps the resident path's accounting on the frame lock that
// it already owns. The practically unreachable overflow path folds into an
// atomic lifetime total without making every hit contend on one cache line.
func (c *PageCache) recordFrameHit(frame *pageCacheFrame) {
	if frame.hits != ^uint32(0) {
		frame.hits++
		return
	}
	c.cacheHitsBase.Add(uint64(frame.hits))
	frame.hits = 1
}

func (c *PageCache) lookupLocked(hash uint64, key pageCacheKey) (int, bool) {
	mask := uint64(len(c.table) - 1)
	for probe := uint64(0); probe < uint64(len(c.table)); probe++ {
		entry := c.table[(hash+probe)&mask].Load()
		if entry == cacheTableEmpty {
			return 0, false
		}
		if entry != cacheTableTombstone {
			index := int(entry - 1)
			if c.frames[index].key == key {
				return index, true
			}
		}
	}
	return 0, false
}

func (c *PageCache) insertLocked(hash uint64, index int) {
	if c.tombs > len(c.table)/4 {
		c.rebuildTableLocked()
	}
	mask := uint64(len(c.table) - 1)
	firstTomb := -1
	for probe := uint64(0); probe < uint64(len(c.table)); probe++ {
		slot := int((hash + probe) & mask)
		switch c.table[slot].Load() {
		case cacheTableEmpty:
			if firstTomb >= 0 {
				slot = firstTomb
				c.tombs--
			}
			c.table[slot].Store(uint32(index) + 1)
			return
		case cacheTableTombstone:
			if firstTomb < 0 {
				firstTomb = slot
			}
		}
	}
	if firstTomb >= 0 {
		c.table[firstTomb].Store(uint32(index) + 1)
		c.tombs--
		return
	}
	panic("storeio: page-cache table capacity invariant")
}

func (c *PageCache) removeLocked(hash uint64, key pageCacheKey) {
	mask := uint64(len(c.table) - 1)
	for probe := uint64(0); probe < uint64(len(c.table)); probe++ {
		slot := (hash + probe) & mask
		entry := c.table[slot].Load()
		if entry == cacheTableEmpty {
			return
		}
		if entry != cacheTableTombstone && c.frames[entry-1].key == key {
			c.table[slot].Store(cacheTableTombstone)
			c.tombs++
			return
		}
	}
}

func (c *PageCache) rebuildTableLocked() {
	for index := range c.table {
		c.table[index].Store(cacheTableEmpty)
	}
	c.tombs = 0
	for index := range c.frames {
		state := c.frames[index].state
		if state == pageCacheLoading || state == pageCacheReady {
			c.insertLocked(cacheKeyHash(c.frames[index].key), index)
		}
	}
}

func cacheKeyHash(key pageCacheKey) uint64 {
	// Physical extents are at least 4 KiB aligned. Generation must participate:
	// a large cache can retain a clean old page after its offset is safely reused,
	// and offset-only hashing would turn that history into one long probe chain.
	x := key.offset>>12 ^ key.generation*0x9e3779b97f4a7c15 ^
		uint64(key.kind)<<48 ^ uint64(key.flags)<<56 ^ uint64(key.aux)<<32
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	return x ^ x>>27
}

func pageCacheKeyStamp(key pageCacheKey) uint32 {
	x := key.offset ^ key.logicalID ^ key.generation
	x ^= uint64(key.length)<<32 | uint64(key.kind)<<24 |
		uint64(key.flags)<<16 | uint64(key.aux)
	return uint32(x ^ x>>32)
}

func (c *PageCache) validateRef(ref PageRef) (pageCacheKey, error) {
	pageSize := uint64(c.options.PageSize)
	length := uint64(ref.Length)
	validRouting := ref.Aux == 0
	if length < pageSize || length > uint64(c.options.MaxPageSize) ||
		!validPageExtentSize(ref.Kind, ref.Length) || length%pageSize != 0 ||
		ref.Length != uint32(c.options.PageSize) &&
			ref.Kind != PageOverflow && ref.Kind != PagePrimaryCatalog &&
			ref.Kind != PageTabletDirectory && ref.Kind != PagePrimaryLocator &&
			ref.Kind != PageTabletRoute && ref.Kind != PagePrimaryAnchor &&
			ref.Kind != PagePrimaryLeaf && ref.Kind != PagePrimaryExactLeaf ||
		!validPageFlags(ref.Kind, ref.Flags) || !validRouting || !validPageKind(ref.Kind) ||
		(ref.Kind == PageStateRoot && ref.LogicalID != StateRootLogicalID) ||
		(ref.Kind != PageStateRoot && ref.LogicalID <= StateRootLogicalID) ||
		ref.Generation == 0 ||
		ref.Offset < c.dataStart || ref.Offset%pageSize != 0 ||
		ref.Offset > uint64(^uint64(0)>>1)-length {
		return pageCacheKey{}, fmt.Errorf("%w: offset, identity, kind, or length", ErrPageCacheReference)
	}
	return pageCacheKey{
		offset: ref.Offset, logicalID: ref.LogicalID, generation: ref.Generation,
		length: ref.Length, kind: ref.Kind, flags: ref.Flags, aux: ref.Aux,
	}, nil
}

func (c *PageCache) release(index int, key pageCacheKey) {
	if index < 0 || index >= len(c.frames) {
		return
	}
	frame := &c.frames[index]
	releaseDoomed := false
	frame.lock.Lock()
	// Offset plus generation uniquely names a physical immutable extent. The
	// stale-copy check remains cheap and cannot decrement a reused frame.
	if frame.key.offset == key.offset && frame.key.generation == key.generation && frame.pins != 0 {
		frame.pins--
		releaseDoomed = frame.pins == 0 && frame.flags&pageCacheFrameDoomed != 0
	}
	frame.lock.Unlock()
	if releaseDoomed {
		c.releaseDoomed(index, key)
	}
}

// releaseDoomed is the last-unpin slow path. Ordinary releases retain their
// frame-lock-only path. A reset mutates the lookup table and block allocator,
// so the slow path drops the frame lock before taking c.mu and then reacquires
// the locks in the cache-wide c.mu -> frame.lock order. The recheck tolerates a
// stale lookup briefly repinning the doomed bytes or replacement recycling the
// frame while the locks were dropped.
func (c *PageCache) releaseDoomed(index int, key pageCacheKey) {
	c.mu.Lock()
	frame := &c.frames[index]
	frame.lock.Lock()
	if frame.state == pageCacheReady && frame.key == key &&
		frame.pins == 0 &&
		frame.flags&pageCacheFrameWritePinned == 0 &&
		frame.flags&pageCacheFrameDoomed != 0 {
		c.resetExtentLocked(index)
		c.cond.Broadcast()
	}
	frame.lock.Unlock()
	c.mu.Unlock()
}

// Prefetch enqueues physically ordered refs without blocking on I/O. The input
// must be monotonically ordered by non-overlapping physical offset so query
// planning, rather than the cache, owns sorting scratch. Invalid order queues
// nothing. Queue exhaustion drops the remaining refs and is observable in
// Stats.
func (c *PageCache) Prefetch(refs []PageRef) (int, error) {
	var previousEnd uint64
	for i, ref := range refs {
		if _, err := c.validateRef(ref); err != nil {
			return 0, err
		}
		if i != 0 && ref.Offset < previousEnd {
			return 0, fmt.Errorf("%w: prefetch references are not physically ordered", ErrPageCacheReference)
		}
		previousEnd = ref.Offset + uint64(ref.Length)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing.Load() || c.closed {
		return 0, ErrPageCacheClosed
	}
	queued := 0
	for _, ref := range refs {
		select {
		case c.prefetch <- ref:
			queued++
			c.prefetchQueued++
		default:
			c.prefetchDropped += uint64(len(refs) - queued)
			return queued, nil
		}
	}
	return queued, nil
}

func (c *PageCache) runPrefetch() {
	defer c.workers.Done()
	c.runPortablePrefetch()
}

func (c *PageCache) runPortablePrefetch() {
	for ref := range c.prefetch {
		_, _ = c.load(ref, false, true)
	}
}

// runRingPrefetch owns one ring and one OS thread for its complete lifetime.
// It drains the bounded reference channel into ReadQueueDepth-sized batches
// and submits non-fixed reads directly into reserved page-cache mmap spans.
// Demand readers that reach the same page coalesce on the ordinary loading
// state. A ring accounting or submission failure resets every affected frame
// before this worker continues on the portable correctness path.
func (c *PageCache) runRingPrefetch(initialized chan<- error) {
	defer c.workers.Done()
	runtime.LockOSThread()
	ring, err := Open(Config{
		Entries: uint32(c.options.ReadQueueDepth), SingleIssuer: true,
	})
	if err == nil {
		if registerErr := ring.RegisterFiles([]int{int(c.file.Fd())}); registerErr != nil {
			err = classifyRingSetupError("register read file", registerErr)
		}
	}
	if err == nil && !ring.Features().AsyncRead {
		err = ErrUnsupported
	}
	if err == nil {
		err = ring.useReadArena(c.arena)
	}
	if err != nil {
		if ring != nil {
			_ = ring.Close()
		}
		runtime.UnlockOSThread()
		initialized <- err
		return
	}
	c.readBackend.Store(uint32(BackendIOUring))
	initialized <- nil

	loads := make([]pageCacheRingLoad, c.options.ReadQueueDepth)
	for {
		ref, ok := <-c.prefetch
		if !ok {
			_ = ring.Close()
			runtime.UnlockOSThread()
			return
		}
		count, prepareErr := c.prepareRingPrefetch(ring, loads, 0, ref)
	drain:
		for prepareErr == nil && count < len(loads) {
			select {
			case next, open := <-c.prefetch:
				if !open {
					ok = false
					break drain
				}
				count, prepareErr = c.prepareRingPrefetch(ring, loads, count, next)
			default:
				break drain
			}
		}
		if count == 0 {
			if !ok {
				_ = ring.Close()
				runtime.UnlockOSThread()
				return
			}
			continue
		}
		if prepareErr == nil {
			prepareErr = ring.SubmitAndWait(uint32(count))
		}
		if prepareErr == nil {
			c.recordAsyncReadBatch(count)
			prepareErr = c.completeRingPrefetch(ring, loads[:count])
		}
		if prepareErr != nil {
			// Close drains any SQEs that were prepared or submitted before the
			// failure, so the kernel no longer owns arena bytes when frames are
			// reset and demand reads retry them.
			_ = ring.Close()
			for i := range count {
				if !loads[i].done {
					c.completePrefetch(loads[i], 0, prepareErr)
				}
				loads[i] = pageCacheRingLoad{}
			}
			c.readBackend.Store(uint32(BackendPortable))
			runtime.UnlockOSThread()
			c.runPortablePrefetch()
			return
		}
		clear(loads[:count])
		if !ok {
			_ = ring.Close()
			runtime.UnlockOSThread()
			return
		}
	}
}

func (c *PageCache) prepareRingPrefetch(
	ring *Ring,
	loads []pageCacheRingLoad,
	count int,
	ref PageRef,
) (int, error) {
	load, ok := c.beginPrefetch(ref)
	if !ok {
		return count, nil
	}
	loads[count] = load
	arenaOffset := load.frame * c.options.PageSize
	if err := ring.prepareReadArena(
		0, arenaOffset, int(ref.Length), int64(ref.Offset), uint64(count),
	); err != nil {
		return count + 1, err
	}
	return count + 1, nil
}

func (c *PageCache) beginPrefetch(ref PageRef) (pageCacheRingLoad, bool) {
	key, err := c.validateRef(ref)
	if err != nil {
		return pageCacheRingLoad{}, false
	}
	hash := cacheKeyHash(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing.Load() || c.closed {
		return pageCacheRingLoad{}, false
	}
	if _, ok := c.lookupLocked(hash, key); ok {
		return pageCacheRingLoad{}, false
	}
	logicalSpan := int(ref.Length) / c.options.PageSize
	reservedSpan := pageCacheReservedSpan(logicalSpan)
	index, ok := c.reserveLocked(reservedSpan)
	if !ok {
		c.prefetchDropped++
		return pageCacheRingLoad{}, false
	}
	c.cacheMisses++
	frame := &c.frames[index]
	frame.lock.Lock()
	c.beginExtentLocked(index, reservedSpan, key, hash)
	frame.flags |= pageCacheFramePrefetched
	c.activeLoads++
	frame.lock.Unlock()
	return pageCacheRingLoad{ref: ref, key: key, frame: index}, true
}

func (c *PageCache) completeRingPrefetch(ring *Ring, loads []pageCacheRingLoad) error {
	for range loads {
		completion, ok, err := ring.Pop()
		if err != nil {
			return err
		}
		if !ok || completion.UserData >= uint64(len(loads)) {
			return ErrOverflow
		}
		index := int(completion.UserData)
		load := &loads[index]
		if load.done {
			return ErrOverflow
		}
		load.done = true
		readErr := completion.Err()
		n := 0
		if readErr == nil {
			n = int(completion.Result)
			if n != int(load.ref.Length) {
				readErr = io.ErrUnexpectedEOF
			}
		}
		c.completePrefetch(*load, n, readErr)
	}
	return nil
}

func (c *PageCache) completePrefetch(load pageCacheRingLoad, n int, readErr error) {
	var header PageHeader
	if readErr == nil {
		header, readErr = c.validateLoadedPage(c.extentBytes(load.frame, load.ref.Length), load.ref)
	}
	c.mu.Lock()
	c.pageReads++
	if n > 0 {
		c.readBytes += uint64(n)
	}
	c.activeLoads--
	frame := &c.frames[load.frame]
	frame.lock.Lock()
	if frame.state != pageCacheLoading || frame.key != load.key {
		c.readErrors++
		frame.lock.Unlock()
		c.cond.Broadcast()
		c.mu.Unlock()
		return
	}
	if readErr == nil {
		frame.payloadLength = header.PayloadLength
		frame.state = pageCacheReady
	} else {
		c.readErrors++
		c.resetExtentLocked(load.frame)
	}
	frame.lock.Unlock()
	c.cond.Broadcast()
	c.mu.Unlock()
}

func (c *PageCache) recordAsyncReadBatch(count int) {
	c.mu.Lock()
	c.asyncReadBatches++
	if uint32(count) > c.largestReadBatch {
		c.largestReadBatch = uint32(count)
	}
	c.mu.Unlock()
}

// ReadBackend reports the active speculative-read engine without locking or
// performing I/O.
func (c *PageCache) ReadBackend() Backend {
	if c == nil {
		return BackendPortable
	}
	return Backend(c.readBackend.Load())
}

// Stats returns bounded residency, lease, I/O, eviction, and prefetch
// accounting without performing file I/O.
func (c *PageCache) Stats() PageCacheStats {
	c.mu.Lock()
	hits := c.cacheHitsBase.Load()
	stats := PageCacheStats{
		CapacityBytes:    uint64(len(c.frames)) * uint64(c.options.PageSize),
		FrameSize:        uint32(c.options.MaxPageSize),
		Frames:           uint32(len(c.frames)),
		PageReads:        c.pageReads,
		ReadBytes:        c.readBytes,
		Misses:           c.cacheMisses,
		Coalesced:        c.coalesced,
		ReadErrors:       c.readErrors,
		Prefetches:       c.prefetchQueued,
		CopyOuts:         c.copyOuts,
		PrefetchHits:     c.prefetchHits.Load(),
		Evictions:        c.evictions,
		PrefetchQueued:   c.prefetchQueued,
		PrefetchDropped:  c.prefetchDropped,
		CrossClassTakes:  c.blocks.crossClassTakes,
		QueueDepth:       uint64(len(c.prefetch)),
		ReadQueueDepth:   uint32(c.options.ReadQueueDepth),
		ReadBackend:      c.ReadBackend(),
		AsyncReadBatches: c.asyncReadBatches,
		LargestReadBatch: c.largestReadBatch,
	}
	for i := range c.frames {
		frame := &c.frames[i]
		state := frame.state
		if state == pageCacheTail {
			continue
		}
		frame.lock.Lock()
		state = frame.state
		if state == pageCacheTail {
			frame.lock.Unlock()
			continue
		}
		switch state {
		case pageCacheLoading:
			stats.LoadingFrames++
		case pageCacheReady:
			stats.ReadyFrames++
		}
		if state != pageCacheEmpty {
			stats.ResidentBytes += uint64(frame.key.length)
			stats.ReservedBytes += uint64(frame.reservationSpan) * uint64(c.options.PageSize)
		}
		if frame.pins != 0 {
			stats.PinnedPages++
			stats.PinnedFrames++
			stats.Pins += uint64(frame.pins)
		}
		hits += uint64(frame.hits)
		frame.lock.Unlock()
	}
	stats.DirtyBytes = c.dirtyBytes
	stats.CacheHits = hits
	stats.Hits = hits
	c.mu.Unlock()
	return stats
}

// DirtyCapacityAvailable reports the bytes of the fixed frame arena not
// currently held by pages awaiting their durability fence. It is the one
// question the writer asks before every mutation, and it is answered from a
// maintained counter under a single lock rather than by walking the arena, so
// its cost does not grow with the configured cache size.
func (c *PageCache) DirtyCapacityAvailable() uint64 {
	if c == nil {
		return 0
	}
	// Widen before multiplying: on a 32-bit build the frame count times the page
	// size overflows int well before it overflows the byte counter it feeds.
	capacity := uint64(len(c.frames)) * uint64(c.options.PageSize)
	c.mu.Lock()
	dirty := c.dirtyReservedBytes
	c.mu.Unlock()
	if dirty >= capacity {
		return 0
	}
	return capacity - dirty
}

// Close stops admission and prefetch, then releases the fixed arena. If a
// caller still owns a lease, Close returns ErrPageCachePinned without releasing
// the arena; release those leases and call Close again.
func (c *PageCache) Close() error {
	if c == nil {
		return nil
	}
	c.closing.Store(true)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.prefetchCloseOnce.Do(func() { close(c.prefetch) })
	c.cond.Broadcast()
	c.mu.Unlock()
	<-c.done

	c.mu.Lock()
	for c.activeLoads != 0 {
		c.cond.Wait()
	}
	for i := range c.frames {
		frame := &c.frames[i]
		if frame.state == pageCacheTail {
			continue
		}
		frame.lock.Lock()
		pinned := frame.pins != 0
		frame.lock.Unlock()
		if pinned {
			c.mu.Unlock()
			return ErrPageCachePinned
		}
	}
	arena := c.arena
	c.arena = nil
	for i := range c.frames {
		frame := &c.frames[i]
		frame.lock.Lock()
		c.cacheHitsBase.Add(uint64(frame.hits))
		frame.key = pageCacheKey{}
		frame.dirty = 0
		frame.hits = 0
		frame.payloadLength = 0
		frame.reservationSpan = 0
		frame.pins = 0
		frame.state = pageCacheEmpty
		frame.referenced = false
		frame.flags = 0
		frame.lock.Unlock()
	}
	for i := range c.table {
		c.table[i].Store(cacheTableEmpty)
	}
	c.tombs = 0
	c.dirtyBytes = 0
	c.dirtyReservedBytes = 0
	clear(c.dirtyFrames)
	c.dirtyFrames = c.dirtyFrames[:0]
	c.closed = true
	c.mu.Unlock()
	if err := releaseArena(arena); err != nil {
		return fmt.Errorf("vibejson: release Store page cache: %w", err)
	}
	return nil
}
