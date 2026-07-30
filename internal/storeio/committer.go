package storeio

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultCommitQueueSlots = 64
	defaultCommitGroupLimit = 32
	maxCommitDescriptors    = 1 << 20
	maxCommitCoalesce       = time.Second
	// frameNativeCommitBuffers cover one alternate root plus the maximum
	// journal/patch working set with spare producer/worker overlap. Immutable
	// transaction data pages live in PageCache and do not consume this arena.
	frameNativeCommitBuffers = 16
)

var (
	// ErrBatchState reports use of a free, aborted, or already-published batch.
	ErrBatchState = errors.New("vibejson: Store persistence batch is not owned")
	// ErrTooManyPages reports a transaction larger than its configured bounded
	// page-descriptor capacity.
	ErrTooManyPages = errors.New("vibejson: Store persistence batch has too many pages")
	// ErrGenerationOrder reports a persistence generation that does not advance
	// the preceding published generation.
	ErrGenerationOrder = errors.New("vibejson: Store persistence generation is not increasing")
	// ErrCheckpointRequired reports bounded buffered-visible staging pressure.
	// The caller must checkpoint the generations it has already published
	// before retrying. No rejected batch has been accepted or made visible.
	ErrCheckpointRequired = errors.New("vibejson: Store buffered checkpoint required")
)

// CommitterOptions fixes automatic persistence queue memory. All descriptors
// are allocated during construction and reused until Close.
type CommitterOptions struct {
	// FrameNativeStaging declares that WriteTransaction data pages are backed
	// by PageCache frames. It lets NewCommitter cap the legacy Device arena to
	// the fixed root/journal/patch working set. Generic Batch callers that need
	// page buffers leave it false.
	FrameNativeStaging bool
	// QueueSlots bounds reader-visible generations awaiting persistence. Zero
	// selects 64 and explicit values are rounded up to a power of two.
	QueueSlots int
	// MaxPagesPerBatch bounds changed data/directory pages in one generation;
	// the alternate root uses one additional buffer. Zero uses every Device
	// buffer except one.
	MaxPagesPerBatch int
	// GroupLimit bounds adjacent generations collapsed into one durable root.
	// Zero selects 32. Grouping never crosses the available buffer/page scratch,
	// and in practice that scratch and the arrival rate bind long before this
	// cap does.
	GroupLimit int
	// CoalesceDelay is the maximum time the background worker waits after the
	// first queued generation for adjacent publications to arrive. Publication
	// itself remains immediate; zero preserves eager durability. The wait is
	// skipped when nothing is queued and a caller is already blocked in Wait,
	// because that caller's acknowledgement is what the delay would be charged
	// to and it cannot produce the neighbour being waited for.
	CoalesceDelay time.Duration
	// MaterializationDamageGranule explicitly qualifies the largest complete
	// sector a power loss may damage. Zero disables canonical in-place
	// materialization. A non-zero value must be supported by the journal codec;
	// callers must obtain it from the storage stack rather than infer it from
	// logical or filesystem block sizes.
	MaterializationDamageGranule uint32
	// ManualCheckpoint retains accepted copy-on-write generations in the
	// bounded staging pool until Flush or Close requests one physical commit.
	// It is the persistence boundary for buffered-visible stores: Publish
	// performs no device operation, and exhausted staging returns
	// ErrCheckpointRequired instead of waiting on a worker that has not been
	// authorized to write.
	ManualCheckpoint bool
	// prewriteGenerationWatermark preserves the caller's requested grouping
	// pulse before manual checkpoints widen GroupLimit to the complete queue.
	prewriteGenerationWatermark int
}

func (o CommitterOptions) normalized(bufferCount int) (CommitterOptions, error) {
	if o.QueueSlots == 0 {
		o.QueueSlots = defaultCommitQueueSlots
	}
	if o.QueueSlots < 1 || o.QueueSlots > 1<<16 {
		return CommitterOptions{}, fmt.Errorf("%w: queue slots %d", ErrInvalidWrite, o.QueueSlots)
	}
	o.QueueSlots = nextPowerOfTwo(o.QueueSlots)
	if o.MaxPagesPerBatch == 0 {
		o.MaxPagesPerBatch = bufferCount - 1
	}
	if o.MaxPagesPerBatch < 0 || o.MaxPagesPerBatch >= bufferCount {
		return CommitterOptions{}, fmt.Errorf("%w: max pages %d for %d buffers", ErrTooManyPages, o.MaxPagesPerBatch, bufferCount)
	}
	if o.MaxPagesPerBatch != 0 && o.QueueSlots > maxCommitDescriptors/o.MaxPagesPerBatch {
		return CommitterOptions{}, fmt.Errorf("%w: %d descriptor slots", ErrTooManyPages, uint64(o.QueueSlots)*uint64(o.MaxPagesPerBatch))
	}
	if o.GroupLimit == 0 {
		o.GroupLimit = min(defaultCommitGroupLimit, o.QueueSlots)
	}
	if o.GroupLimit < 1 || o.GroupLimit > o.QueueSlots {
		return CommitterOptions{}, fmt.Errorf("%w: group limit %d", ErrInvalidWrite, o.GroupLimit)
	}
	if o.CoalesceDelay < 0 || o.CoalesceDelay > maxCommitCoalesce {
		return CommitterOptions{}, fmt.Errorf("%w: coalesce delay %s", ErrInvalidWrite, o.CoalesceDelay)
	}
	if o.MaterializationDamageGranule != 0 &&
		(o.MaterializationDamageGranule < MaterializationJournalMinSectorSize ||
			o.MaterializationDamageGranule&(o.MaterializationDamageGranule-1) != 0 ||
			o.MaterializationDamageGranule > MaterializationJournalMaxData) {
		return CommitterOptions{}, fmt.Errorf(
			"%w: materialization damage granule %d",
			ErrInvalidWrite, o.MaterializationDamageGranule,
		)
	}
	if o.ManualCheckpoint {
		if o.MaterializationDamageGranule != 0 {
			return CommitterOptions{}, fmt.Errorf(
				"%w: manual checkpoint with canonical materialization",
				ErrInvalidWrite,
			)
		}
		o.prewriteGenerationWatermark = o.GroupLimit/2 + 1
		// A requested checkpoint is one root cut, not a sequence of partially
		// durable logical generations. Every retained batch therefore belongs
		// to the same maximal group.
		o.GroupLimit = o.QueueSlots
		o.CoalesceDelay = 0
	}
	return o, nil
}

func nextPowerOfTwo(value int) int {
	value--
	value |= value >> 1
	value |= value >> 2
	value |= value >> 4
	value |= value >> 8
	value |= value >> 16
	if ^uint(0)>>32 != 0 {
		value |= value >> 32
	}
	return value + 1
}

type commitFailure struct{ err error }

type committerCallbacks struct {
	failed  func(error)
	durable func(uint64)
}

const (
	batchFree uint32 = iota
	batchOwned
	batchPublished
)

// Batch is one preallocated persistence generation. Its methods belong to the
// Committer's single producer. After Publish or Abort, every Batch method is
// invalid until Begin returns that slot again.
type Batch struct {
	committer                     *Committer
	pages                         []Write
	bufferIndexes                 []uint32
	root                          Write
	rootGeneration                uint64
	journal                       Write
	journalSequence               uint64
	journalSlot                   uint32
	journalCapsuleChecksum        uint32
	materializationPatchCount     uint16
	materializationTargetMask     uint8
	materializationPatchChecksums [MaterializationJournalMaxPatches]uint32
	materialized                  bool
	dataBufferCount               uint16
	generation                    uint64
	index                         uint32
	state                         atomic.Uint32
}

// PageBuffer returns page's reusable staging buffer.
func (b *Batch) PageBuffer(page int) ([]byte, error) {
	if b == nil || b.state.Load() != batchOwned || b.materialized ||
		page < 0 || page >= len(b.pages) {
		return nil, ErrBatchState
	}
	if b.pages[page].frameNative() {
		cache := b.committer.frameCache.Load()
		if cache == nil || int(b.pages[page].frameIndex) >= len(cache.frames) {
			return nil, ErrBatchState
		}
		return cache.extentBytes(
			int(b.pages[page].frameIndex), b.pages[page].Length,
		), nil
	}
	return b.committer.buffers[b.pages[page].Buffer], nil
}

// SetPage records the initialized prefix and physical offset of one page.
func (b *Batch) SetPage(page int, offset int64, length int) error {
	return b.setPage(page, offset, length, 0)
}

// setStorePage records the kind of a page already verified by
// TransactionPage.Stage. The marker is private to the durable batching
// optimizer; generic Batch users retain byte-for-byte write semantics.
func (b *Batch) setStorePage(
	page int,
	offset int64,
	length int,
	kind PageKind,
) error {
	return b.setPage(page, offset, length, kind)
}

func (b *Batch) setPage(
	page int,
	offset int64,
	length int,
	kind PageKind,
) error {
	if b == nil || b.state.Load() != batchOwned || b.materialized ||
		page < 0 || page >= len(b.pages) {
		return ErrBatchState
	}
	if length < 0 || uint64(length) > uint64(^uint32(0)) {
		return ErrInvalidWrite
	}
	b.pages[page].Offset = offset
	b.pages[page].Length = uint32(length)
	b.pages[page].kind = kind
	return nil
}

// ResizePages returns unused trailing page buffers before publication. It is
// useful when a bounded copy-on-write operation reserves its worst-case tree
// height before it knows whether a split or prune is required. A batch cannot
// grow after Begin; every retained page must still be initialized with
// SetPage.
func (b *Batch) ResizePages(pageCount int) error {
	if b == nil || b.state.Load() != batchOwned || b.materialized ||
		pageCount < 0 || pageCount > len(b.pages) {
		return ErrBatchState
	}
	releaseStart := min(pageCount, int(b.dataBufferCount))
	releaseEnd := int(b.dataBufferCount)
	released := b.bufferIndexes[:releaseEnd-releaseStart]
	for i := range released {
		released[i] = uint32(b.pages[releaseStart+i].Buffer)
	}
	b.committer.freeBuffers.pushN(released)
	for i := pageCount; i < len(b.pages); i++ {
		b.pages[i] = Write{}
	}
	b.pages = b.pages[:pageCount]
	if b.dataBufferCount > uint16(pageCount) {
		b.dataBufferCount = uint16(pageCount)
	}
	return nil
}

// RootBuffer returns the alternate-root staging buffer.
func (b *Batch) RootBuffer() ([]byte, error) {
	if b == nil || b.state.Load() != batchOwned {
		return nil, ErrBatchState
	}
	return b.committer.buffers[b.root.Buffer], nil
}

// SetRoot records the initialized root prefix and physical offset.
func (b *Batch) SetRoot(offset int64, length int) error {
	if b == nil || b.state.Load() != batchOwned {
		return ErrBatchState
	}
	if length < 0 || uint64(length) > uint64(^uint32(0)) {
		return ErrInvalidWrite
	}
	b.root.Offset = offset
	b.root.Length = uint32(length)
	b.rootGeneration = 0
	return nil
}

// SetSuperblock encodes a checksummed double-root record into the reusable root
// buffer. The worker selects the physical slot opposite the last durable root:
// generation parity is only a provisional offset because group commit may skip
// one or more generations. The complete page is cleared and committed: besides
// removing stale tail bytes, page-sized root writes retain the offset/length
// alignment required by direct I/O. Recovery decodes only the fixed record
// prefix. Publish must receive the same generation, preventing a durable-
// generation counter from naming different on-disk state. No allocation is
// performed.
func (b *Batch) SetSuperblock(root Superblock) error {
	if b == nil || b.state.Load() != batchOwned {
		return ErrBatchState
	}
	buffer := b.committer.buffers[b.root.Buffer]
	if uint64(root.PageSize) > uint64(len(buffer)) {
		return ErrInvalidWrite
	}
	page := buffer[:root.PageSize]
	clear(page)
	_, err := EncodeSuperblock(page, root)
	if err != nil {
		return err
	}
	offset, err := SuperblockOffset(root.Generation, root.PageSize)
	if err != nil {
		return err
	}
	b.root.Offset = offset
	b.root.Length = root.PageSize
	b.rootGeneration = root.Generation
	return nil
}

// Publish transfers the batch to the background writer without allocating.
// generation must be greater than every previously published generation.
func (b *Batch) Publish(generation uint64) error {
	if b == nil || b.state.Load() != batchOwned {
		return ErrBatchState
	}
	return b.committer.publish(b, generation, nil)
}

// Abort returns every buffer and descriptor without publishing the batch.
func (b *Batch) Abort() error {
	if b == nil || b.state.Load() != batchOwned {
		return ErrBatchState
	}
	b.committer.release(b)
	return nil
}

// CommitterStats is a lock-free snapshot of automatic persistence progress.
type CommitterStats struct {
	Backend                     Backend
	PublishedGeneration         uint64
	DurableGeneration           uint64
	FallbackGeneration          uint64
	QueuedGenerations           uint64
	DeviceCommits               uint64
	CommittedBatches            uint64
	MaterializedBatches         uint64
	MaterializationJournalBytes uint64
	MaterializationTargetBytes  uint64
	// MaterializationBarriers reports completed durability fences. Patch-only
	// batches complete with two; hybrids containing unjournaled COW pages
	// complete with three.
	MaterializationBarriers uint64
	// MaterializationFullWriteBytes counts immutable copy-on-write pages
	// published in the same data phase as journal-covered canonical patches.
	// Keeping this separate from TargetBytes exposes the exact split between
	// newly allocated data and in-place bytes.
	MaterializationFullWriteBytes uint64
	LargestGroup                  uint32
	SuppressedRootWrites          uint64
	SuppressedRootBytes           uint64
	SupersededRootWrites          uint64
	SupersededRootBytes           uint64
	// SupersededPageWrites counts buffered copy-on-write pages proved
	// unreachable before their checkpoint and therefore omitted from device
	// submission. TailWitnessWrites are equally unreachable pages retained
	// solely because their write is still needed to extend the file through
	// the newest published FileEnd.
	SupersededPageWrites uint64
	SupersededPageBytes  uint64
	TailWitnessWrites    uint64
	TailWitnessBytes     uint64
	// PrewrittenPageWrites/Bytes count sealed frame-native pages written
	// opportunistically while a manual-checkpoint worker was otherwise idle.
	// They exclude barriers and roots and therefore do not advance durability.
	PrewrittenPageWrites uint64
	PrewrittenPageBytes  uint64
	// DeviceBytes counts every payload byte handed to the Device: pre-writes,
	// checkpoint data pages, and the alternate root. It is the
	// write-amplification number: dividing it by CommittedBatches gives bytes
	// per published generation, which is what makes a directory-shape,
	// grouping, or pre-write change visible. File length cannot substitute,
	// because copy-on-write reuses retired extents and so stops growing long
	// before amplification does.
	DeviceBytes uint64
}

// Committer turns synchronous Device commits into automatic asynchronous
// persistence. One serialized Store writer is the producer; one private
// background worker is the consumer and sole Device owner. Readers never load
// Committer state.
type Committer struct {
	deviceOptions DeviceOptions
	options       CommitterOptions
	device        Device
	backend       Backend

	buffers      [][]byte
	bufferSize   int
	bufferCount  int
	freeBuffers  *indexPool
	freeBatches  *indexPool
	batches      []Batch
	writeStorage []Write
	indexStorage []uint32
	producerSeen []uint64

	pending     []*Batch
	pendingMask uint64
	// manualMu protects producer-side pending-page supersession against a
	// concurrent checkpoint cut and worker release. Automatic durability never
	// takes it.
	manualMu    sync.Mutex
	pendingRoot *Batch
	head        atomic.Uint64
	tail        atomic.Uint64
	wake        chan struct{}
	workerWait  atomic.Uint32
	stop        chan struct{}
	done        chan struct{}
	closeOnce   sync.Once
	closing     atomic.Bool
	publishers  atomic.Uint32

	published atomic.Uint64
	durable   atomic.Uint64
	settled   atomic.Uint64
	fallback  atomic.Uint64
	// checkpointThrough is the newest manually buffered generation a Flush or
	// Close has authorized the worker to persist.
	checkpointThrough atomic.Uint64
	// nextRootSlot is the physical superblock page opposite the last durable
	// root. The worker advances it only after the root fence succeeds. This is
	// deliberately independent of generation parity: a grouped commit can
	// publish generation N+2 directly over durable generation N.
	nextRootSlot    atomic.Uint32
	failure         atomic.Pointer[commitFailure]
	callbacks       atomic.Pointer[committerCallbacks]
	frameCache      atomic.Pointer[PageCache]
	failed          chan struct{}
	failureNotified chan struct{}
	failOnce        sync.Once

	waitMu sync.Mutex
	wait   *sync.Cond

	commitScratch                      []Write
	groupScratch                       []*Batch
	deviceCommits                      atomic.Uint64
	batchesDone                        atomic.Uint64
	materializedDone                   atomic.Uint64
	materializationJournalBytes        atomic.Uint64
	materializationTargetBytes         atomic.Uint64
	materializationFullWriteBytes      atomic.Uint64
	materializationBarriers            atomic.Uint64
	largestGroup                       atomic.Uint32
	suppressedRootWrites               atomic.Uint64
	suppressedRootBytes                atomic.Uint64
	supersededRootWrites               atomic.Uint64
	supersededRootBytes                atomic.Uint64
	supersededPageWrites               atomic.Uint64
	supersededPageBytes                atomic.Uint64
	tailWitnessWrites                  atomic.Uint64
	tailWitnessBytes                   atomic.Uint64
	prewrittenPageWrites               atomic.Uint64
	prewrittenPageBytes                atomic.Uint64
	prewriteDisabled                   atomic.Bool
	prewritePulseDone                  atomic.Bool
	deviceBytes                        atomic.Uint64
	materializationNextSequence        atomic.Uint64
	materializationNextSlot            atomic.Uint32
	materializationPublished           atomic.Bool
	materializationRecoveryInitialized atomic.Bool
	// waiters counts callers blocked in Wait. It distinguishes the two writers
	// that group commit must treat differently: one whose Put has already
	// returned, for whom a coalescing window costs nothing, and one still
	// blocked on this exact fence, for whom the window is added latency it can
	// never recover.
	waiters atomic.Int64
}

type committerInit struct{ err error }

type deviceOpener func(*os.File, DeviceOptions) (Device, error)

// NewCommitter starts a bounded background writer over file. Construction
// waits until the selected Device and every reusable buffer are ready.
func NewCommitter(file *os.File, deviceOptions DeviceOptions, options CommitterOptions) (*Committer, error) {
	return newCommitter(file, deviceOptions, options, OpenDevice)
}

// NewCommitterWithDevice is NewCommitter with a caller-supplied Device opener.
// It exists so durability crash tests can interpose a fault-injecting Device
// between the committer and the platform backend; a nil opener falls back to
// the platform Device. Production uses NewCommitter.
func NewCommitterWithDevice(
	file *os.File, deviceOptions DeviceOptions, options CommitterOptions,
	open func(*os.File, DeviceOptions) (Device, error),
) (*Committer, error) {
	if open == nil {
		open = OpenDevice
	}
	return newCommitter(file, deviceOptions, options, open)
}

func newCommitter(file *os.File, deviceOptions DeviceOptions, options CommitterOptions, open deviceOpener) (*Committer, error) {
	normalizedDevice, err := deviceOptions.normalized()
	if err != nil {
		return nil, err
	}
	normalizedCommitter, err := options.normalized(normalizedDevice.BufferCount)
	if err != nil {
		return nil, err
	}
	if normalizedCommitter.MaterializationDamageGranule != 0 &&
		uint32(normalizedDevice.BufferSize)%normalizedCommitter.MaterializationDamageGranule != 0 {
		return nil, fmt.Errorf(
			"%w: buffer size %d is not a multiple of materialization damage granule %d",
			ErrInvalidWrite, normalizedDevice.BufferSize,
			normalizedCommitter.MaterializationDamageGranule,
		)
	}
	arenaDevice := normalizedDevice
	if normalizedCommitter.FrameNativeStaging {
		arenaDevice.BufferCount = min(
			normalizedDevice.BufferCount, frameNativeCommitBuffers,
		)
	}
	c := &Committer{
		deviceOptions:   arenaDevice,
		options:         normalizedCommitter,
		bufferSize:      normalizedDevice.BufferSize,
		bufferCount:     arenaDevice.BufferCount,
		freeBuffers:     newIndexPool(arenaDevice.BufferCount),
		freeBatches:     newIndexPool(normalizedCommitter.QueueSlots),
		batches:         make([]Batch, normalizedCommitter.QueueSlots),
		writeStorage:    make([]Write, normalizedCommitter.QueueSlots*normalizedCommitter.MaxPagesPerBatch),
		indexStorage:    make([]uint32, normalizedCommitter.QueueSlots*(normalizedCommitter.MaxPagesPerBatch+2)),
		producerSeen:    make([]uint64, (arenaDevice.BufferCount+63)/64),
		pending:         make([]*Batch, normalizedCommitter.QueueSlots),
		pendingMask:     uint64(normalizedCommitter.QueueSlots - 1),
		wake:            make(chan struct{}, 1),
		stop:            make(chan struct{}),
		done:            make(chan struct{}),
		failed:          make(chan struct{}),
		failureNotified: make(chan struct{}),
		commitScratch:   make([]Write, 0, normalizedDevice.BufferCount),
	}
	groupCapacity := normalizedCommitter.GroupLimit
	if normalizedCommitter.ManualCheckpoint {
		// A manual checkpoint is one exact root cut. Buffered alternate-root
		// supersession makes every older descriptor unreachable, so the worker
		// must drain the authorized cut under its newest root even when the
		// ordinary automatic group limit is smaller.
		groupCapacity = normalizedCommitter.QueueSlots
	}
	c.groupScratch = make([]*Batch, 0, groupCapacity)
	c.wait = sync.NewCond(&c.waitMu)
	c.materializationNextSequence.Store(1)
	for i := range c.batches {
		start := i * normalizedCommitter.MaxPagesPerBatch
		indexStart := i * (normalizedCommitter.MaxPagesPerBatch + 2)
		batch := &c.batches[i]
		batch.committer = c
		batch.index = uint32(i)
		batch.pages = c.writeStorage[start : start : start+normalizedCommitter.MaxPagesPerBatch]
		batch.bufferIndexes = c.indexStorage[indexStart : indexStart : indexStart+normalizedCommitter.MaxPagesPerBatch+2]
	}
	initialized := make(chan committerInit, 1)
	go c.run(file, initialized, open)
	if result := <-initialized; result.err != nil {
		return nil, result.err
	}
	return c, nil
}

// Begin acquires one reusable descriptor and pageCount+1 staging buffers. It
// applies bounded backpressure when the persistence worker owns all capacity.
func (c *Committer) Begin(pageCount int) (*Batch, error) {
	return c.begin(pageCount, pageCount)
}

// beginFrameNative acquires descriptors for cache-backed data pages and only
// one committer buffer for the alternate root.
func (c *Committer) beginFrameNative(pageCount int) (*Batch, error) {
	return c.begin(pageCount, 0)
}

func (c *Committer) bindFrameCache(cache *PageCache) error {
	if c == nil || cache == nil {
		return ErrInvalidWrite
	}
	current := c.frameCache.Load()
	if current == cache {
		return nil
	}
	if current != nil || !c.frameCache.CompareAndSwap(nil, cache) {
		if c.frameCache.Load() == cache {
			return nil
		}
		return fmt.Errorf("%w: committer page cache changed", ErrInvalidWrite)
	}
	return nil
}

// begin reserves buffers for the first bufferedPageCount page descriptors and
// leaves the remainder for cache-frame-native sources.
func (c *Committer) begin(
	pageCount, bufferedPageCount int,
) (*Batch, error) {
	if c == nil {
		return nil, ErrClosed
	}
	if failure := c.currentFailure(); failure != nil {
		return nil, failure
	}
	if c.closing.Load() {
		return nil, ErrClosed
	}
	if pageCount < 0 || pageCount > c.options.MaxPagesPerBatch ||
		bufferedPageCount < 0 || bufferedPageCount > pageCount ||
		bufferedPageCount+1 > c.bufferCount {
		return nil, ErrTooManyPages
	}
	batchIndex, err := c.acquire(c.freeBatches)
	if err != nil {
		return nil, err
	}
	batch := &c.batches[batchIndex]
	batch.pages = batch.pages[:pageCount]
	clear(batch.pages)
	batch.dataBufferCount = uint16(bufferedPageCount)
	indexes := batch.bufferIndexes[:bufferedPageCount+1]
	if err := c.acquireN(c.freeBuffers, indexes); err != nil {
		batch.pages = batch.pages[:0]
		batch.dataBufferCount = 0
		c.freeBatches.push(batch.index)
		return nil, err
	}
	for i, buffer := range indexes {
		if i == bufferedPageCount {
			batch.root = Write{Buffer: uint16(buffer)}
		} else {
			batch.pages[i] = Write{Buffer: uint16(buffer)}
		}
	}
	if failure := c.currentFailure(); failure != nil {
		c.release(batch)
		return nil, failure
	}
	if c.closing.Load() {
		c.release(batch)
		return nil, ErrClosed
	}
	batch.state.Store(batchOwned)
	return batch, nil
}

// NeedsCheckpointFor reports whether a manual committer lacks the descriptor
// and legacy Device staging buffers needed by a generic Batch.
func (c *Committer) NeedsCheckpointFor(pageCount int) bool {
	if c == nil || !c.options.ManualCheckpoint {
		return false
	}
	if pageCount < 0 || pageCount > c.options.MaxPagesPerBatch {
		return true
	}
	return c.freeBatches.availableCount() < 1 ||
		c.freeBuffers.availableCount() < int64(pageCount+1)
}

// NeedsFrameCheckpointFor is the cache-frame-native transaction preflight.
// Data capacity belongs to PageCache; only a descriptor and alternate-root
// buffer remain in the committer pool.
func (c *Committer) NeedsFrameCheckpointFor(pageCount int) bool {
	if c == nil || !c.options.ManualCheckpoint {
		return false
	}
	if pageCount < 0 || pageCount > c.options.MaxPagesPerBatch {
		return true
	}
	return c.freeBatches.availableCount() < 1 ||
		c.freeBuffers.availableCount() < 1
}

func (c *Committer) acquire(pool *indexPool) (uint32, error) {
	for {
		if failure := c.currentFailure(); failure != nil {
			return 0, failure
		}
		if c.closing.Load() {
			return 0, ErrClosed
		}
		if index, ok := pool.pop(); ok {
			return index, nil
		}
		if c.options.ManualCheckpoint {
			return 0, ErrCheckpointRequired
		}
		notify := pool.prepareWait()
		if index, ok := pool.pop(); ok {
			pool.finishWait()
			return index, nil
		}
		select {
		case <-notify:
		case <-c.failed:
			pool.finishWait()
			return 0, c.currentFailure()
		case <-c.stop:
			pool.finishWait()
			return 0, ErrClosed
		}
		pool.finishWait()
	}
}

// acquireN is acquire's exact bulk counterpart. The pool either transfers the
// complete reservation under one lock or transfers nothing before the caller
// parks, preserving bounded backpressure without a partially held buffer set.
func (c *Committer) acquireN(pool *indexPool, indexes []uint32) error {
	for {
		if failure := c.currentFailure(); failure != nil {
			return failure
		}
		if c.closing.Load() {
			return ErrClosed
		}
		if pool.popN(indexes) {
			return nil
		}
		if c.options.ManualCheckpoint {
			return ErrCheckpointRequired
		}
		notify := pool.prepareWait()
		if pool.popN(indexes) {
			pool.finishWait()
			return nil
		}
		select {
		case <-notify:
		case <-c.failed:
			pool.finishWait()
			return c.currentFailure()
		case <-c.stop:
			pool.finishWait()
			return ErrClosed
		}
		pool.finishWait()
	}
}

func (c *Committer) publish(
	batch *Batch,
	generation uint64,
	retired []PageRef,
) error {
	_, err := c.publishRetiring(
		batch, generation, retired, nil, nil,
	)
	return err
}

// publishRetiring is publish with a bounded, caller-owned result for exact
// retirements whose older pending writes this publication transitions to
// pendingWriteSuperseded. The result is produced under manualMu, in the same
// critical section that excludes checkpoint capture and worker dequeue.
func (c *Committer) publishRetiring(
	batch *Batch,
	generation uint64,
	retired []PageRef,
	retirements []FreeExtent,
	superseded []FreeExtent,
) ([]FreeExtent, error) {
	if failure := c.currentFailure(); failure != nil {
		return superseded, failure
	}
	if !c.enterPublish() {
		if failure := c.currentFailure(); failure != nil {
			return superseded, failure
		}
		return superseded, ErrClosed
	}
	defer c.publishers.Add(^uint32(0))
	if failure := c.currentFailure(); failure != nil {
		return superseded, failure
	}
	if generation == 0 || generation <= c.published.Load() {
		return superseded, ErrGenerationOrder
	}
	if batch.rootGeneration != 0 && batch.rootGeneration != generation {
		return superseded, ErrGenerationOrder
	}
	if batch.materialized {
		sequence, err := c.validateMaterializedBatch(batch, generation)
		if err != nil {
			return superseded, err
		}
		batch.journalSequence = sequence
	} else if err := validateCommit(
		c.bufferCount, c.bufferSize, c.producerSeen, batch.pages, batch.root,
	); err != nil {
		return superseded, err
	}
	if c.closing.Load() {
		return superseded, ErrClosed
	}
	if c.options.ManualCheckpoint {
		c.manualMu.Lock()
		defer c.manualMu.Unlock()
	}
	tail := c.tail.Load()
	if tail-c.head.Load() >= uint64(len(c.pending)) {
		if c.options.ManualCheckpoint {
			return superseded, ErrCheckpointRequired
		}
		return superseded, ErrQueueFull
	}
	if batch.materialized {
		batch.journalSlot = c.materializationNextSlot.Load()
		c.materializationNextSequence.Store(batch.journalSequence + 1)
		c.materializationNextSlot.Store(batch.journalSlot ^ 1)
		c.materializationPublished.Store(true)
	}
	batch.generation = generation
	if c.options.ManualCheckpoint {
		superseded = c.coalesceManualPagesLocked(
			batch, generation, retired, retirements, superseded,
		)
		c.coalesceManualBatchLocked(batch, generation)
	}
	batch.state.Store(batchPublished)
	c.pending[tail&c.pendingMask] = batch
	c.published.Store(generation)
	c.tail.Store(tail + 1)
	// A normal publication wakes the automatic worker. A manual publication
	// wakes it only when a concurrent Flush already captured this generation;
	// otherwise acknowledgement remains completely device-silent.
	authorized := !c.options.ManualCheckpoint ||
		generation <= c.checkpointThrough.Load()
	prewritePressure := c.prewritePressure()
	if (authorized || prewritePressure) && c.workerWait.Load() != 0 {
		select {
		case c.wake <- struct{}{}:
		default:
		}
	}
	return superseded, nil
}

// requestCurrentCheckpoint captures and authorizes one exact publication cut.
// Manual publication and page/root supersession take the same mutex, so no
// later generation can recycle a write needed by the captured root between
// those two actions.
func (c *Committer) requestCurrentCheckpoint() uint64 {
	if c == nil {
		return 0
	}
	if !c.options.ManualCheckpoint {
		return c.published.Load()
	}
	c.manualMu.Lock()
	generation := c.published.Load()
	if c.checkpointThrough.Load() < generation {
		c.checkpointThrough.Store(generation)
	}
	c.manualMu.Unlock()
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return generation
}

func (c *Committer) enterPublish() bool {
	if c.closing.Load() {
		return false
	}
	c.publishers.Add(1)
	if c.closing.Load() {
		c.publishers.Add(^uint32(0))
		return false
	}
	return true
}

// PublishedGeneration returns the newest generation accepted by Publish.
func (c *Committer) PublishedGeneration() uint64 {
	if c == nil {
		return 0
	}
	return c.published.Load()
}

// DurableGeneration returns the newest generation whose root passed the final
// data-integrity barrier.
func (c *Committer) DurableGeneration() uint64 {
	if c == nil {
		return 0
	}
	return c.durable.Load()
}

// StagingCapacityBytes reports the remaining fixed Device arena. Transaction
// data pages are staged in PageCache and are intentionally excluded.
func (c *Committer) StagingCapacityBytes() uint64 {
	if c == nil {
		return 0
	}
	return uint64(c.bufferCount) * uint64(c.bufferSize)
}

// Failure returns the sticky persistence failure that poisoned the committer,
// or nil while persistence remains healthy. Once non-nil it never changes and
// Begin, Publish, Wait, Flush, and Close all reject further work.
func (c *Committer) Failure() error {
	if c == nil {
		return ErrClosed
	}
	return c.currentFailure()
}

func (c *Committer) currentFailure() error {
	failure := c.failure.Load()
	if failure == nil {
		return nil
	}
	return failure.err
}

func (c *Committer) waitFailure(failure *commitFailure) error {
	if c.failureNotified != nil {
		<-c.failureNotified
	}
	return failure.err
}

// SetCallbacks installs lifecycle notifications used by the durable
// collection to move its reader-visible state without polling on read paths.
// It must be called before the first Begin. The callbacks run on the private
// persistence worker and must not call back into Committer methods that wait
// for worker progress.
func (c *Committer) SetCallbacks(
	durable func(uint64),
	failed func(error),
) error {
	if c == nil || c.closing.Load() ||
		c.published.Load() != 0 || c.head.Load() != c.tail.Load() {
		return ErrBatchState
	}
	callbacks := &committerCallbacks{durable: durable, failed: failed}
	if !c.callbacks.CompareAndSwap(nil, callbacks) {
		return ErrBatchState
	}
	return nil
}

// FallbackGeneration returns the generation of the other independently valid
// recovery root after the latest successful physical commit. It can lag the
// durable generation by more than one when group commit collapses logical
// generations. Reclaimers must fence against this exact value.
func (c *Committer) FallbackGeneration() uint64 {
	if c == nil {
		return 0
	}
	return c.fallback.Load()
}

// InitializeGeneration seeds a newly opened Committer with the generation
// already selected by crash recovery, inferring the recovery slot from the
// legacy generation-parity convention. New recovery callers should use
// InitializeGenerationAt so files written across grouped generations continue
// alternating from the physical slot actually selected by recovery.
func (c *Committer) InitializeGeneration(generation uint64) error {
	if generation == 0 {
		return ErrGenerationOrder
	}
	fallbackGeneration := generation
	if generation > 1 {
		fallbackGeneration--
	}
	return c.InitializeRecovery(
		generation, int((generation-1)&(superblockCopies-1)), fallbackGeneration,
	)
}

// InitializeGenerationAt seeds a newly opened Committer with the generation
// and physical superblock slot selected by crash recovery. The next successful
// superblock commit writes the other slot even when logical generations skip.
// It must be called before Begin or Publish.
func (c *Committer) InitializeGenerationAt(generation uint64, rootSlot int) error {
	fallbackGeneration := generation
	if generation > 1 {
		fallbackGeneration--
	}
	return c.InitializeRecovery(generation, rootSlot, fallbackGeneration)
}

// InitializeRecovery seeds the selected generation, its physical slot, and
// the actual validated fallback generation returned by crash recovery.
func (c *Committer) InitializeRecovery(
	generation uint64, rootSlot int, fallbackGeneration uint64,
) error {
	if c == nil || generation == 0 || c.closing.Load() || c.head.Load() != c.tail.Load() ||
		c.published.Load() != 0 || c.durable.Load() != 0 ||
		rootSlot < 0 || rootSlot >= superblockCopies ||
		fallbackGeneration == 0 || fallbackGeneration > generation {
		return ErrGenerationOrder
	}
	c.published.Store(generation)
	c.durable.Store(generation)
	c.settled.Store(generation)
	c.fallback.Store(fallbackGeneration)
	c.nextRootSlot.Store(uint32(rootSlot ^ 1))
	return nil
}

// Wait blocks until generation is durable and its owner lifecycle callback has
// completed, or persistence fails/closes. DurableGeneration may observe the
// physical fence slightly earlier; Wait is the acknowledgement boundary.
func (c *Committer) Wait(generation uint64) error {
	if c == nil {
		return ErrClosed
	}
	if failure := c.failure.Load(); failure != nil {
		return c.waitFailure(failure)
	}
	if generation > c.published.Load() {
		return ErrGenerationOrder
	}
	c.waiters.Add(1)
	defer c.waiters.Add(-1)
	c.waitMu.Lock()
	defer c.waitMu.Unlock()
	for {
		if failure := c.failure.Load(); failure != nil {
			return c.waitFailure(failure)
		}
		if c.settled.Load() >= generation {
			return nil
		}
		select {
		case <-c.done:
			return ErrClosed
		default:
		}
		c.wait.Wait()
	}
}

// Flush waits for the newest generation published before the call. A manual
// committer first authorizes exactly that buffered root cut for persistence.
func (c *Committer) Flush() error {
	if c == nil {
		return ErrClosed
	}
	generation := c.requestCurrentCheckpoint()
	return c.Wait(generation)
}

// Stats returns current queue and group-commit counters.
func (c *Committer) Stats() CommitterStats {
	if c == nil {
		return CommitterStats{}
	}
	published := c.published.Load()
	durable := c.durable.Load()
	queued := c.tail.Load() - c.head.Load()
	return CommitterStats{
		Backend:                       c.backend,
		PublishedGeneration:           published,
		DurableGeneration:             durable,
		FallbackGeneration:            c.fallback.Load(),
		QueuedGenerations:             queued,
		DeviceCommits:                 c.deviceCommits.Load(),
		CommittedBatches:              c.batchesDone.Load(),
		MaterializedBatches:           c.materializedDone.Load(),
		MaterializationJournalBytes:   c.materializationJournalBytes.Load(),
		MaterializationTargetBytes:    c.materializationTargetBytes.Load(),
		MaterializationFullWriteBytes: c.materializationFullWriteBytes.Load(),
		MaterializationBarriers:       c.materializationBarriers.Load(),
		LargestGroup:                  c.largestGroup.Load(),
		SuppressedRootWrites:          c.suppressedRootWrites.Load(),
		SuppressedRootBytes:           c.suppressedRootBytes.Load(),
		SupersededRootWrites:          c.supersededRootWrites.Load(),
		SupersededRootBytes:           c.supersededRootBytes.Load(),
		SupersededPageWrites:          c.supersededPageWrites.Load(),
		SupersededPageBytes:           c.supersededPageBytes.Load(),
		TailWitnessWrites:             c.tailWitnessWrites.Load(),
		TailWitnessBytes:              c.tailWitnessBytes.Load(),
		PrewrittenPageWrites:          c.prewrittenPageWrites.Load(),
		PrewrittenPageBytes:           c.prewrittenPageBytes.Load(),
		DeviceBytes:                   c.deviceBytes.Load(),
	}
}

// Close drains every published batch, closes the Device, and returns any
// sticky persistence failure. It is idempotent.
func (c *Committer) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.stopAccepting()
		c.requestCurrentCheckpoint()
		close(c.stop)
	})
	<-c.done
	if failure := c.currentFailure(); failure != nil {
		return failure
	}
	return nil
}
