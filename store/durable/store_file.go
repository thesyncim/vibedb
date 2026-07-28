package durable

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/internal/storemem"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

var (
	// ErrClosed reports use after Collection.Close has started.
	ErrClosed = errors.New("vibejson: collection is closed")
	// ErrNotEmpty requires Create to receive an empty file.
	ErrNotEmpty = errors.New("vibejson: collection create requires an empty file")
	// ErrKeyTooLarge reports a key beyond the configured durable page
	// bound.
	ErrKeyTooLarge = errors.New("vibejson: collection key exceeds configured bound")
	// ErrDocumentTooLarge reports a JSON value beyond the configured
	// transaction bound.
	ErrDocumentTooLarge = errors.New("vibejson: collection document exceeds configured bound")
	// ErrPrimaryReadOnly reports a mutation surface not yet wired to the
	// ordered-primary graph, such as WriteBatch. Point Put/Delete use the
	// primary COW path.
	ErrPrimaryReadOnly = errors.New(
		"vibejson: ordered primary graph is read-only during cutover",
	)
	// ErrPrimaryLeafSplitRequired reports a correct deferred structural insert.
	// The mutation was not published; the next ordered-primary phase replaces
	// this signal with an atomic leaf split.
	ErrPrimaryLeafSplitRequired = errors.New(
		"vibejson: ordered primary leaf split required",
	)
	// ErrPrimaryCutoverUnsupported reports a CreateFromPrimary option or
	// source shape whose durable companion structure is not built yet.
	ErrPrimaryCutoverUnsupported = errors.New(
		"vibejson: ordered primary cutover feature is unsupported",
	)
	// ErrWriterLocked reports that another mutable collection owns the
	// page file. A durable file has exactly one generation publisher.
	ErrWriterLocked = storeio.ErrWriterLocked
	// ErrWriterLockUnsupported rejects mutable durable collections on a
	// platform without a safe exclusive file-lock implementation.
	ErrWriterLockUnsupported = storeio.ErrWriterLockUnsupported
	// ErrCommitOutcomeUnknown reports a persistence failure after the new root
	// may have reached storage. Reopen to let root recovery determine whether
	// the old or new generation won before retrying the mutation.
	ErrCommitOutcomeUnknown = storeio.ErrCommitOutcomeUnknown
	// ErrCheckpointRequired reports that buffered-visible generations own the
	// configured staging bound. Call Flush to checkpoint them, then retry the
	// mutation; the rejected mutation was not made reader-visible.
	ErrCheckpointRequired = storeio.ErrCheckpointRequired
)

// Backend selects the durable commit and speculative-read engines.
type Backend uint8

const (
	// BackendAuto is the safe zero value. It selects the native asynchronous
	// engine (io_uring on Linux) when the platform and build support it and
	// otherwise falls back to BackendPortable, so callers need not probe.
	BackendAuto Backend = iota
	// BackendPortable forces the dependency-free engine that runs on every
	// supported platform, trading peak throughput for portability.
	BackendPortable
	// BackendIOUring forces the Linux io_uring engine and fails construction
	// where it is unavailable, rather than silently degrading to portable.
	BackendIOUring
)

// ReadMode selects how cache misses reach the file. Direct modes are
// Linux-only and leave the caller's descriptor untouched.
type ReadMode uint8

const (
	// ReadBuffered is the safe zero value. Cache misses use ordinary buffered
	// file reads served through the operating system page cache.
	ReadBuffered ReadMode = iota
	// ReadDirectTry uses O_DIRECT when the platform and filesystem
	// accept it, otherwise Stats reports the observable buffered fallback.
	ReadDirectTry
	// ReadDirectRequire fails construction rather than falling back.
	ReadDirectRequire
)

// WriteMode selects how durable page commits reach the file. Direct
// modes are Linux-only and use an independently owned descriptor, so the
// caller's descriptor flags and file offset remain untouched.
type WriteMode uint8

const (
	// WriteBuffered is the safe zero value. Durable commits use ordinary
	// buffered writes made durable by the configured checkpoint barrier. It is
	// the only mode that currently accepts canonical sparse writes.
	WriteBuffered WriteMode = iota
	// WriteDirectTry uses O_DIRECT when the platform and filesystem
	// accept it, otherwise Stats reports the observable buffered fallback.
	WriteDirectTry
	// WriteDirectRequire fails construction rather than falling back.
	WriteDirectRequire
)

// DurabilityMode selects when a successful mutation becomes reader-visible
// and when its caller receives acknowledgement.
type DurabilityMode uint8

const (
	// DurabilitySync is the safe zero value. A mutation becomes visible and
	// returns success only after it is durable. On a primary-layout store that
	// durability is a bounded recovery-journal record appended and synced before
	// the mutation is applied and published — its root is folded in at the next
	// checkpoint — so visibility strictly follows durability; on the chunk layout
	// it is the committer's data and root barriers. Either way no reader observes
	// an un-durable acknowledged mutation.
	DurabilitySync DurabilityMode = iota
	// DurabilityAsyncVisible explicitly publishes after bounded queue
	// admission while persistence continues in the background.
	DurabilityAsyncVisible
	// DurabilityBufferedVisible publishes a fresh copy-on-write generation
	// after bounded memory admission without issuing device writes. Flush and
	// Close checkpoint every generation accepted before their cut through the
	// existing alternate-root recovery protocol. A process or machine failure
	// before that checkpoint loses those acknowledged mutations.
	DurabilityBufferedVisible
)

// CheckpointStrength selects what a successful buffered-visible Flush or Close
// promises. It does not change mutation acknowledgement or reader visibility.
type CheckpointStrength uint8

const (
	// CheckpointPowerSafe is the safe zero value. It preserves the strongest
	// platform checkpoint primitive, including F_FULLFSYNC on Darwin.
	CheckpointPowerSafe CheckpointStrength = iota
	// CheckpointFilesystem uses ordinary filesystem sync for both the data
	// ordering barrier and the final alternate-root barrier. It matches the
	// usual fsync/msync checkpoint class and survives process failure, but on
	// Darwin it does not promise that volatile drive caches survive sudden
	// power loss. It is accepted only with DurabilityBufferedVisible,
	// BackendPortable, and WriteBuffered.
	CheckpointFilesystem
)

// Options fixes every collection-owned resident and in-flight memory
// bound. The zero value selects 4 KiB metadata pages, 64 KiB
// document/overflow extents, a 64 MiB read cache, and 4 MiB maximum documents.
// DocumentFormat selects the physical representation CreateFrom gives document
// bytes. It is a space-against-scan-speed decision, the two options are far
// apart on both axes, and which one a workload wants is not obvious, so it is
// stated explicitly rather than chosen silently.
//
// Measured on 100,000 documents averaging 299 bytes, Apple M4 Max, medians of
// -count=6, both files written by CreateFrom from the same source collection:
//
//	                          verbatim   compact
//	file size                 52.7 MiB   27.1 MiB
//	... as a multiple of raw JSON  1.85x     0.95x
//	full scan, warm, ns/doc         6.2      81.3
//	point read, warm, ns            904      1234
//	full scan, cold, ns/doc         287       450
//	cold scan device reads         1586       805
//
// So compact costs roughly thirteen times the resident scan and a third more
// per point read, and returns just under half the bytes on disk.
//
// The cold row is the one that is easy to get backwards. Compact reads half as
// much from the device, and it is still slower cold, because on this hardware
// the token decoding costs more than the I/O it saves. That balance is a
// property of the storage, not of the format: on a slow or contended device,
// or with a resident budget far below the corpus, halving the bytes read can
// invert it. Measure it on the device you will deploy on rather than assuming
// either direction. Note also that this cold measurement empties the buffer
// pool but not the operating system's page cache.
//
// One more caution about the space number, because two different comparisons
// are in circulation and they flatter differently. The 1.95x above is compact
// against this package's own verbatim output, and a similar ratio holds against
// a Put-built file. Against other embedded engines the result is much less
// impressive: on a shape-matched high-cardinality corpus the compact file grew
// 77% (15.9 -> 28.2 MiB) while bbolt, badger, and SQLite moved under 3%, which
// leaves compact level with them rather than ahead. Both statements are true
// and they answer different questions; quote the one that matches the question
// being asked.
//
// Verbatim is the default because a caller who reaches for CreateFrom is
// usually after "write every page exactly once", and should not silently
// receive an order-of-magnitude resident-scan regression they did not ask for.
type DocumentFormat uint8

const (
	// DocumentFormatVerbatim stores each chunk as one PageDocument extent
	// holding contiguous JSON, which a scan hands to its callback as a
	// borrowed slice with no decoding at all.
	DocumentFormatVerbatim DocumentFormat = iota
	// DocumentFormatCompact packs consecutive chunks into PageDocumentGroup
	// extents that store one shape template and one value dictionary per page
	// and reduce each row to a token stream. Reading a row means reassembling
	// it from roughly two dozen fragments, which is where the scan cost is; see
	// DocumentFormat for the numbers.
	DocumentFormatCompact
)

// Options fixes the durable collection's schema, exact indexes, memory bounds,
// engine selection, and durability contract at construction. It embeds the
// in-memory store.Options for shared collection settings and adds the on-disk
// concerns the resident engine does not have. The zero value is a working
// power-safe configuration; every field below documents what it overrides.
type Options struct {
	Collection store.Options
	// Indexes are frozen exact scalar definitions maintained from the first
	// durable generation. Canonical ordered path vectors assign stable on-disk
	// physical IDs independently of caller order. Differently named definitions
	// with identical ordered paths are logical aliases of one physical index:
	// they share posting maintenance and durable bytes while remaining
	// independently discoverable and queryable.
	Indexes []store.IndexDefinition
	// Float64Columns are frozen RFC 6901 paths stored beside each document
	// micro-page as typed covering columns. Predicate-free numeric aggregates
	// can reduce these values without parsing JSON. Missing, non-numeric, and
	// non-finite values are omitted from the column.
	Float64Columns []string
	// DocumentFormat selects how CreateFrom stores document bytes. It has no
	// effect on Open, Put, or Update: a collection reads both representations
	// unconditionally, and ordinary writes always produce the verbatim one.
	DocumentFormat DocumentFormat

	PageSize      int
	MaxPageSize   int
	ResidentBytes int64
	// ReadConcurrency bounds portable positional-read workers.
	ReadConcurrency int
	// ReadQueueDepth bounds one native asynchronous read submission.
	ReadQueueDepth int
	// PrefetchQueue bounds references waiting for either read engine.
	PrefetchQueue    int
	MaxKeyBytes      int
	InlineValueBytes int
	MaxDocumentBytes int
	// BufferCount is retained for configuration compatibility through the
	// storage unification cutover. It normalizes the maximum transaction and
	// checkpoint descriptor geometry, but immutable data pages now encode
	// directly in PageCache frames. The committer owns only a small fixed arena
	// for alternate roots, recovery journals, and sparse canonical patches.
	//
	// An explicit Update reserves the worst case for the configured
	// MaxDocumentBytes and MaxBatchDocuments — overflow, indexes, free-space
	// folds, directories, and one root — before it writes anything. Put/Delete
	// reserve the same consumers at the single-document geometry. Unused page
	// descriptors are returned immediately before publication.
	//
	// Zero chooses the smallest legal power of two and grows toward four
	// worst-case transactions while the pool remains within a 32 MiB target.
	// The legal minimum wins when one transaction already exceeds that target.
	// With today's zero-value 64-document batch bound, the worst case is 663
	// pages, so zero still normalizes to 1,024 descriptors. This knob is
	// deprecated and may become fully vestigial at that cutover; explicit
	// values remain accepted and validated meanwhile.
	BufferCount int
	QueueSlots  int
	// GroupLimit caps how many adjacent generations share one durability
	// fence; zero selects 32. It is a ceiling, not a target, and measurement
	// shows it is almost never the binding constraint: how many generations are
	// queued when the worker picks one up is. Raising it from 2 to 64 changes
	// neither the achieved group size nor throughput on any writer shape tested.
	// Reach for CommitCoalesce instead.
	GroupLimit int
	// CommitCoalesce bounds how long the background durability worker waits
	// after taking one generation for adjacent ones to join it. Zero, the
	// default, commits each generation as soon as it is picked up.
	//
	// The window is only entered when another generation is already queued or a
	// producer is mid-transaction, so a lone synchronous writer — whose next Put
	// cannot start until this one is durable — never pays it. When grouping is
	// possible the cost is real and bounded: a DurabilitySync caller's
	// acknowledgement is delayed by up to this duration so that its fence can be
	// shared. On an Apple M4 Max, where one file.Sync costs several
	// milliseconds, a 1 ms window took roughly three generations per fence
	// instead of one and a half, cutting per-operation cost by about a third
	// with eight concurrent writers. It is left at zero by default because that
	// trade belongs to the caller: it buys throughput with acknowledged-commit
	// latency, and only a caller knows which it is short of.
	CommitCoalesce time.Duration
	// Backend selects both engines; Stats reports the actual read and write
	// choices independently after Auto fallback.
	Backend Backend
	// ReadMode controls cache-miss reads independently from durable writes.
	// DirectTry has observable fallback; DirectRequire fails when unavailable.
	ReadMode ReadMode
	// WriteMode controls durable data and root writes independently from cache
	// misses. Direct modes keep sustained ingestion out of the kernel page
	// cache while retaining the same ordered durability barriers.
	WriteMode WriteMode
	// Durability defaults to DurabilitySync. Volatile acknowledgement and
	// immediate visibility require an explicit asynchronous or buffered value.
	Durability DurabilityMode
	// CheckpointStrength defaults to power-safe. The weaker ordinary-filesystem
	// option is explicit and restricted to buffered-visible copy-on-write
	// checkpoints; it can never weaken DurabilitySync or AsyncVisible.
	CheckpointStrength CheckpointStrength
	// RecoveryJournal is the opt-in that gives a DurabilityBufferedVisible
	// collection every frame-deferred acknowledgement a bounded redo record
	// synced to a sibling journal file, turning buffered-visible's volatile
	// acknowledgement into a durable one at one bounded append plus one sync — no
	// page copy-on-write and no root fence. Readers are untouched: visibility
	// still comes from the canonical frames, and the journal is write-only until
	// crash recovery replays it through the ordinary mutation path. The journal's
	// sync strength follows CheckpointStrength (power-safe issues the
	// F_FULLFSYNC-class barrier, filesystem the ordinary fdatasync-class one).
	//
	// It has no effect on DurabilityAsyncVisible, and none on DurabilitySync: a
	// primary-layout synchronous store is journal-backed UNCONDITIONALLY (the
	// journal is how sync acknowledges, appended and synced before visibility, at
	// the power-safe strength sync always uses), so this flag is neither required
	// nor consulted there.
	RecoveryJournal bool
	// MaterializationDamageGranule enables recovery-journaled canonical page
	// replacement for mutations whose complete before-image sectors fit the
	// fixed capsule. Zero disables it. A non-zero value is a storage-stack
	// capability assertion: it must be the largest complete region that a
	// power failure can damage, not an inferred filesystem or device block
	// size. The value is frozen into the file and checked on every Open.
	// Canonical sparse writes currently require WriteBuffered; direct-I/O
	// alignment is device-specific and is rejected rather than risking a
	// sticky EINVAL after publication.
	MaterializationDamageGranule int
	MaxSnapshotLeases            int
	// MaxRetiredExtents bounds the copy-on-write extents held back from reuse
	// because some reader might still dereference them. Zero selects 65,536.
	//
	// This is the bound that decides how long a Snapshot may be held open while
	// the collection is being written. An extent retired at generation G cannot
	// be reused until no lease sits at or below G, so one snapshot held across a
	// write loop pins everything retired after it was taken. On reaching the
	// bound, the writer first forces the existing checkpoint/reclamation path
	// and retries the unpublished reservation. ErrRetiredExtentCapacity is
	// returned only when that cannot create enough room; the default and its
	// memory bound are unchanged.
	// Legal values are 1 through 16,777,216; larger tables cannot be addressed
	// by the packed pointer-free interval arena and are rejected before any
	// storage allocation.
	//
	// That failure is bounded backpressure and is fully recoverable: closing the
	// snapshot lets the next commit drain the pending set and the writer
	// resumes with no restart and nothing lost. It is not a wedge. But a reader
	// that keeps one snapshot for the lifetime of a long-lived request handler
	// will meet it, so take snapshots per query rather than per connection, or
	// raise this bound and accept the proportional tracking memory.
	//
	// The bound never permits a commit to forget an extent. If one transaction
	// would overflow the retirement table without a reader pinning it, the
	// unpublished write fails with ErrRetiredExtentCapacity. Raise this bound
	// for a larger worst-case transaction; no restart is required and no space
	// is abandoned.
	MaxRetiredExtents int
	// MaxBatchDocuments bounds how many distinct keys one Update may mutate;
	// zero selects store.MaxChunkDocuments. It sizes the durable transaction's
	// worst-case page reservation, so raising it raises the staging arena's
	// address-space reservation (lazily backed on every Unix, eagerly allocated
	// elsewhere) and lowers nothing. Update reports ErrBatchTooLarge rather than
	// silently splitting: a batch that spans two commits is not the atomic unit
	// its caller asked for, and a crash between them would publish half of it.
	MaxBatchDocuments int
	// MaxBatchBytes bounds the key and current-value bytes copied by one Update.
	// Zero reserves every maximum-size key plus up to 16 MiB of values, or every
	// maximum-size value when that is smaller. Rewriting one key replaces its
	// previous bytes in this budget instead of accumulating callback history.
	MaxBatchBytes int
	// DisableMutationCombining keeps concurrent Put/Delete calls on the
	// single-document materializer. It exists for controlled latency/benchmark
	// comparisons; the default combines only an observed writer backlog and
	// never delays a lone mutation to wait for company.
	DisableMutationCombining bool
}

// batchMetadataBasePages is the worst-case non-overflow page reservation for
// one batched publication before its free-log fold grows past the
// single-document baseline. Each term names the structure it pays for:
//
//   - one rebuilt document page per chunk the batch touches, plus one for a
//     chunk it creates;
//   - one batched chunk-directory descent over every touched chunk;
//   - one batched fingerprint-directory descent over every mutated key;
//   - one batched index-directory descent per configured index, over at most
//     two routing edits per document, because a replaced value leaves one
//     posting and joins another;
//   - the free log's fold ceiling and the publication root.
//
// It is deliberately a reservation and not an invariant. A pathological tree
// shape can exceed it, in which case the transaction's allocator refuses and
// Update returns ErrBatchTooLarge with nothing published; the caller retries
// with a smaller batch. Making it exact would require reserving for a
// ten-level directory over every key, which is hundreds of times the pages any
// real store uses.
func batchMetadataBasePages(o Options, indexes int) int {
	documents := o.MaxBatchDocuments
	chunks := (documents+o.Collection.ChunkDocuments-1)/o.Collection.ChunkDocuments + 1
	pages := chunks
	pages += storeio.ChunkTreeBatchPages(chunks)
	pages += fileStoreFingerprintBatchReservePages(documents)
	if indexes != 0 {
		pages += indexes * storeio.IndexTreeBatchPages(2*documents)
	}
	pages += fileStoreMetadataReservePages
	return pages
}

// fileStoreFingerprintBatchReservePages sizes the ordinary atomic-batch
// staging arena without charging every edit for all sixteen theoretical tree
// levels. One edit can contribute a rewritten leaf, a branch, and a split
// output; the fixed point-mutation allowance covers root promotion and the
// empty-tree boundary.
//
// This is intentionally a practical reservation, not
// storeio.PageKeyTreeBatchPages' format-wide adversarial bound. A directory
// deep enough for widely scattered edits to exceed it fails the unpublished
// transaction with ErrBatchTooLarge, as documented by
// batchMetadataBasePages, and succeeds when the caller retries with a smaller
// atomic batch. Reserving the format-wide bound for the default 64-document
// batch would otherwise consume thousands of staging buffers for a mutation
// that normally publishes only a handful of pages.
func fileStoreFingerprintBatchReservePages(edits int) int {
	if edits <= 0 {
		return 0
	}
	const fixed = fileStorePointFingerprintStagePages
	if edits > (math.MaxInt-fixed)/3 {
		return math.MaxInt
	}
	return 3*edits + fixed
}

func batchFreeFoldLimit(o Options, indexes int) int {
	// No fold can contain more segments than the complete segment index names.
	// Within that format bound, the batch's existing metadata reservation is a
	// conservative bound on how many independently placed pages it can retire
	// or consume from the free set.
	maxSegments := storeio.FreeLogMaxIndexPages *
		storeio.FreeIndexRecordCapacity(uint32(o.PageSize))
	return min(maxSegments, max(
		storeio.FreeLogMaxFoldSegments, batchMetadataBasePages(o, indexes),
	))
}

func batchMetadataPages(o Options, indexes int) int {
	base := batchMetadataBasePages(o, indexes)
	return base + batchFreeFoldLimit(o, indexes) - storeio.FreeLogMaxFoldSegments
}

const (
	// A point fingerprint mutation has a sixteen-page maximum path. Root
	// promotion can stage two more pages, while delete compaction may retire
	// the selected page plus one sibling at every level: 2*16-1 = 31.
	// Keeping the staged and retired geometries separate avoids charging the
	// commit-buffer arena for immutable pages that only enter the reclaimer.
	fileStorePointFingerprintStagePages  = 18
	fileStorePointFingerprintRetirePages = 31

	// fileStoreMetadataReservePages is the fixed share of a transaction's page
	// reservation that is not proportional to the batch: the fingerprint
	// tree's 18-page point ceiling, state root, chunk/index paths, and the
	// single-document free log's worst commit. A wider batch adds fold pages in
	// batchMetadataPages because it can dirty more free-image segments
	// atomically. Both geometries spend this baseline, which is why it is named
	// once rather than repeated as a literal in each.
	fileStoreMetadataReservePages = 56
)

type normalizedFileStoreOptions struct {
	Options
	maxTransactionPages            int
	maxTransactionBytes            uint64
	singleDocumentTransactionPages int
	singleDocumentTransactionBytes uint64
	singleDocumentFreeFoldLimit    int
	freeFoldLimit                  int
	pageCatalog                    *storeio.CanonicalPageCatalog
	indexes                        []*store.ExactIndex
	indexNameIDs                   map[string]uint32
	float64Columns                 []fileStoreFloat64Column
	indexCatalogHash               uint64
}

const (
	// Physical index IDs are encoded into a uint64 bitmap by the packed
	// scalar-group catalog. Logical names do not consume a bit: aliases resolve
	// to the canonical physical definition with the same ordered paths.
	fileStoreMaxPhysicalIndexes = 64
	// Logical aliases are memory-only catalog entries, but still need a finite
	// bound so an untrusted configuration cannot force unbounded compilation,
	// hashing, and lookup-map growth.
	fileStoreMaxLogicalIndexes = 4096
	fileStoreMaxFloat64Columns = 256
)

type fileStoreFloat64Column struct {
	spec    string
	pointer vibejson.CompiledPointer
}

func (o Options) normalized() (normalizedFileStoreOptions, error) {
	storeOptions, err := o.Collection.Normalized()
	if err != nil {
		return normalizedFileStoreOptions{}, err
	}
	o.Collection = storeOptions
	if o.PageSize == 0 {
		o.PageSize = 4096
	}
	if o.MaxPageSize == 0 {
		o.MaxPageSize = 64 << 10
	}
	if o.ResidentBytes == 0 {
		o.ResidentBytes = 64 << 20
	}
	if o.ReadConcurrency == 0 {
		o.ReadConcurrency = 4
	}
	if o.PrefetchQueue == 0 {
		o.PrefetchQueue = 64
	}
	if o.ReadQueueDepth == 0 {
		o.ReadQueueDepth = o.PrefetchQueue
	}
	if o.MaxKeyBytes == 0 {
		o.MaxKeyBytes = 256
	}
	if o.InlineValueBytes == 0 {
		o.InlineValueBytes = 512
	}
	if o.MaxDocumentBytes == 0 {
		o.MaxDocumentBytes = 4 << 20
	}
	if o.MaxSnapshotLeases == 0 {
		o.MaxSnapshotLeases = 1024
	}
	if o.MaxRetiredExtents == 0 {
		o.MaxRetiredExtents = 1 << 16
	}
	if o.MaxRetiredExtents < 1 || o.MaxRetiredExtents > 1<<24 {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"vibejson: collection MaxRetiredExtents must be between 1 and %d",
			1<<24,
		)
	}
	if o.MaxBatchDocuments == 0 {
		o.MaxBatchDocuments = store.MaxChunkDocuments
	}
	if o.MaxBatchDocuments < 1 {
		return normalizedFileStoreOptions{}, fmt.Errorf("vibejson: collection MaxBatchDocuments must be positive")
	}
	if o.MaxBatchDocuments > (math.MaxInt-o.MaxDocumentBytes)/o.MaxKeyBytes {
		return normalizedFileStoreOptions{}, fmt.Errorf("vibejson: collection batch byte bound overflows")
	}
	minBatchBytes := o.MaxDocumentBytes + o.MaxBatchDocuments*o.MaxKeyBytes
	if o.MaxBatchBytes == 0 {
		valueBytes := defaultBatchValueBytes
		if o.MaxBatchDocuments <= math.MaxInt/o.MaxDocumentBytes {
			valueBytes = min(valueBytes, o.MaxBatchDocuments*o.MaxDocumentBytes)
		}
		o.MaxBatchBytes = o.MaxBatchDocuments*o.MaxKeyBytes +
			max(o.MaxDocumentBytes, valueBytes)
	}
	if o.MaxBatchBytes < minBatchBytes {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"vibejson: collection MaxBatchBytes must hold one maximum document and every batch key",
		)
	}
	if o.DocumentFormat > DocumentFormatCompact ||
		o.Backend > BackendIOUring || o.ReadMode > ReadDirectRequire ||
		o.WriteMode > WriteDirectRequire ||
		o.Durability > DurabilityBufferedVisible ||
		o.CheckpointStrength > CheckpointFilesystem ||
		o.CheckpointStrength == CheckpointFilesystem &&
			(o.Durability != DurabilityBufferedVisible ||
				o.Backend != BackendPortable ||
				o.WriteMode != WriteBuffered) ||
		o.CommitCoalesce < 0 || o.CommitCoalesce > time.Second ||
		o.PageSize < 4096 || o.PageSize&(o.PageSize-1) != 0 ||
		o.MaxPageSize < o.PageSize || o.MaxPageSize&(o.MaxPageSize-1) != 0 || o.MaxPageSize%o.PageSize != 0 ||
		o.MaxKeyBytes < 1 || o.InlineValueBytes < 1 || o.MaxDocumentBytes < 1 ||
		o.InlineValueBytes > o.MaxDocumentBytes ||
		uint64(o.MaxPageSize) > uint64(storeio.MaxPhysicalPageSize) ||
		uint64(o.MaxKeyBytes) > math.MaxUint32 ||
		uint64(o.InlineValueBytes) > math.MaxUint32 ||
		uint64(o.MaxDocumentBytes) > math.MaxUint32 ||
		o.MaterializationDamageGranule < 0 ||
		o.Durability == DurabilityBufferedVisible &&
			o.MaterializationDamageGranule != 0 ||
		o.MaterializationDamageGranule != 0 &&
			(o.MaterializationDamageGranule < storeio.MaterializationJournalMinSectorSize ||
				o.MaterializationDamageGranule&(o.MaterializationDamageGranule-1) != 0 ||
				o.MaterializationDamageGranule > storeio.MaterializationJournalMaxData ||
				o.PageSize%o.MaterializationDamageGranule != 0 ||
				o.WriteMode != WriteBuffered) ||
		o.ReadConcurrency < 1 || o.ReadConcurrency > 32768 ||
		o.ReadQueueDepth < 1 || o.ReadQueueDepth > 32768 ||
		o.PrefetchQueue < 1 || o.PrefetchQueue > 32768 {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"vibejson: invalid Store page, key, value, backend, durability, checkpoint, or read option",
		)
	}
	if len(o.Indexes) > fileStoreMaxLogicalIndexes {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"%w: collection supports at most %d logical index names",
			store.ErrIndexDefinition, fileStoreMaxLogicalIndexes,
		)
	}
	if len(o.Float64Columns) > fileStoreMaxFloat64Columns {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"vibejson: collection supports at most %d float64 columns", fileStoreMaxFloat64Columns,
		)
	}
	inputIndexes := make([]storeio.PageCatalogIndex, len(o.Indexes))
	seenIndexes := make(map[string]struct{}, len(o.Indexes))
	for i, definition := range o.Indexes {
		if _, exists := seenIndexes[definition.Name]; exists {
			return normalizedFileStoreOptions{}, store.ErrIndexExists
		}
		exact, compileErr := store.CompileExactIndex(definition)
		if compileErr != nil {
			return normalizedFileStoreOptions{}, compileErr
		}
		seenIndexes[definition.Name] = struct{}{}
		inputIndexes[i] = storeio.PageCatalogIndex{
			Name:  strings.Clone(definition.Name),
			Paths: slices.Clone(exact.Specs[:exact.N]),
		}
	}
	inputColumns := make([]string, len(o.Float64Columns))
	seenColumns := make(map[string]struct{}, len(o.Float64Columns))
	for i, spec := range o.Float64Columns {
		owned := strings.Clone(spec)
		if _, exists := seenColumns[owned]; exists {
			return normalizedFileStoreOptions{}, fmt.Errorf(
				"%w: duplicate float64 column %q", store.ErrIndexDefinition, owned,
			)
		}
		if _, compileErr := vibejson.CompilePointer(owned); compileErr != nil {
			return normalizedFileStoreOptions{}, fmt.Errorf(
				"%w: float64 column %d: %v", store.ErrIndexDefinition, i, compileErr,
			)
		}
		seenColumns[owned] = struct{}{}
		inputColumns[i] = owned
	}
	var catalogSchema *storeio.PageCatalogSchema
	if o.Collection.Schema != nil {
		definition := o.Collection.Schema.Definition()
		catalogSchema = &storeio.PageCatalogSchema{
			Root:   uint16(definition.Root),
			Fields: make([]storeio.PageCatalogSchemaField, len(definition.Fields)),
		}
		for i, field := range definition.Fields {
			catalogSchema.Fields[i] = storeio.PageCatalogSchemaField{
				Path: field.Path, Types: uint16(field.Types),
				Required: field.Required,
			}
		}
		if schemaErr := validateFilePageCatalogSchema(catalogSchema); schemaErr != nil {
			return normalizedFileStoreOptions{}, fmt.Errorf(
				"%w: %v", store.ErrSchemaDefinition, schemaErr,
			)
		}
	}
	pageCatalog, catalogErr := storeio.BuildCanonicalPageCatalog(
		storeio.PageCatalogDefinition{
			Indexes: inputIndexes, Float64Paths: inputColumns,
			Schema: catalogSchema,
		},
	)
	if catalogErr != nil {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"%w: %v", store.ErrIndexDefinition, catalogErr,
		)
	}
	canonical := pageCatalog.Definition()
	physicalDefinitions := pageCatalog.PhysicalIndexes()
	if len(physicalDefinitions) > fileStoreMaxPhysicalIndexes {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"%w: collection supports at most %d distinct physical index definitions",
			store.ErrIndexDefinition, fileStoreMaxPhysicalIndexes,
		)
	}
	compiled := make([]*store.ExactIndex, len(physicalDefinitions))
	for physicalID, paths := range physicalDefinitions {
		name := ""
		for _, alias := range canonical.Indexes {
			if slices.Equal(alias.Paths, paths) {
				name = alias.Name
				break
			}
		}
		exact, compileErr := store.CompileExactIndex(store.IndexDefinition{
			Name: name, Paths: paths,
		})
		if compileErr != nil {
			return normalizedFileStoreOptions{}, compileErr
		}
		compiled[physicalID] = exact
	}
	definitions := make([]store.IndexDefinition, len(canonical.Indexes))
	indexNameIDs := make(map[string]uint32, len(canonical.Indexes))
	catalogHash := uint64(14695981039346656037)
	for i, alias := range canonical.Indexes {
		definitions[i] = store.IndexDefinition{
			Name: alias.Name, Paths: slices.Clone(alias.Paths),
		}
		physicalID := -1
		for candidateID, paths := range physicalDefinitions {
			if slices.Equal(paths, alias.Paths) {
				physicalID = candidateID
				break
			}
		}
		if physicalID < 0 {
			return normalizedFileStoreOptions{}, fmt.Errorf(
				"%w: missing canonical physical definition",
				store.ErrIndexDefinition,
			)
		}
		indexNameIDs[alias.Name] = uint32(physicalID)
		catalogHash = fileIndexHashBytes(catalogHash, []byte(alias.Name))
		catalogHash = fileIndexHashBytes(
			catalogHash, []byte{0xff, byte(len(alias.Paths))},
		)
		for _, path := range alias.Paths {
			catalogHash = fileIndexHashBytes(catalogHash, []byte(path))
			catalogHash = fileIndexHashBytes(catalogHash, []byte{0})
		}
	}
	o.Indexes = definitions
	columns := make([]fileStoreFloat64Column, len(canonical.Float64Paths))
	for i, spec := range canonical.Float64Paths {
		pointer, compileErr := vibejson.CompilePointer(spec)
		if compileErr != nil {
			return normalizedFileStoreOptions{}, fmt.Errorf(
				"%w: canonical float64 column %d: %v",
				store.ErrIndexDefinition, i, compileErr,
			)
		}
		columns[i] = fileStoreFloat64Column{spec: spec, pointer: pointer}
	}
	o.Float64Columns = slices.Clone(canonical.Float64Paths)
	if len(columns) != 0 {
		catalogHash = fileIndexHashBytes(catalogHash, []byte{0xfc, 0x64})
		for _, column := range columns {
			catalogHash = fileIndexHashBytes(catalogHash, []byte(column.spec))
			catalogHash = fileIndexHashBytes(catalogHash, []byte{0})
		}
	}
	if o.Collection.Schema != nil {
		catalogHash = fileIndexHashBytes(
			catalogHash, []byte{0x53, 0x43, 0x48},
		)
		var identity [8]byte
		binary.LittleEndian.PutUint64(
			identity[:], o.Collection.Schema.Hash,
		)
		catalogHash = fileIndexHashBytes(
			catalogHash, identity[:],
		)
	}
	if len(compiled) == 0 && len(columns) == 0 &&
		o.Collection.Schema == nil {
		catalogHash = 0
	} else if catalogHash == 0 {
		// StateRoot reserves zero to mean that no exact catalog exists. FNV is
		// only a fast rejection key and an adversarial valid definition can
		// drive it to zero, so keep that sentinel out of the populated domain;
		// canonical bytes and their digest remain the authority.
		catalogHash = 1
	}
	maxRowBytes := o.MaxKeyBytes + max(o.InlineValueBytes, storeio.DocumentOverflowDescriptorSize)
	worstDocumentPage := storeio.PageHeaderSize + storeio.PageTrailerSize + storeio.DocumentPagePayloadHeaderSize +
		o.Collection.ChunkDocuments*storeio.DocumentPageRecordSize + o.Collection.ChunkDocuments*maxRowBytes +
		len(columns)*(8+o.Collection.ChunkDocuments*8)
	if worstDocumentPage > o.MaxPageSize {
		return normalizedFileStoreOptions{}, fmt.Errorf("vibejson: collection MaxPageSize cannot hold configured chunk/key/inline bounds")
	}
	overflowPayload := o.MaxPageSize - storeio.PageHeaderSize - storeio.PageTrailerSize - storeio.OverflowPagePayloadHeaderSize
	if overflowPayload <= 0 {
		return normalizedFileStoreOptions{}, fmt.Errorf("vibejson: collection overflow page has no payload")
	}
	overflowPages := 1 + (o.MaxDocumentBytes-1)/overflowPayload
	// Derive Put/Delete from the exact same overflow, tree, retirement, and
	// free-fold inputs as Update, changing only the admitted document count to
	// one. Keeping both calculations together prevents a new batch consumer
	// from silently disappearing from the single-document worst-case bound.
	singleDocumentOptions := o
	singleDocumentOptions.MaxBatchDocuments = 1
	singleDocumentFreeFoldLimit := batchFreeFoldLimit(
		singleDocumentOptions, len(compiled),
	)
	singleDocumentMetadataPageLimit := batchMetadataPages(
		singleDocumentOptions, len(compiled),
	)
	freeFoldLimit := batchFreeFoldLimit(o, len(compiled))
	metadataPageLimit := batchMetadataPages(o, len(compiled))
	// Buffer indexes are uint16 today and the configured device ceiling is
	// 32,768. Reject the transaction geometry before int addition or byte
	// multiplication can wrap on adversarial maximum-document options.
	if metadataPageLimit < 0 || singleDocumentMetadataPageLimit < 0 ||
		overflowPages >= 32768-max(
			metadataPageLimit, singleDocumentMetadataPageLimit,
		) {
		return normalizedFileStoreOptions{}, fmt.Errorf("vibejson: collection MaxBatchDocuments or maximum document requires too many transaction pages")
	}
	docMaxTransactionPages := overflowPages + metadataPageLimit
	docSingleDocumentPages :=
		overflowPages + singleDocumentMetadataPageLimit
	// One document and its overflow chain may use maximum-size extents. A
	// categorical cover can replace one packed catalog, while a numeric
	// projection replaces one packed stripe plus a bounded path of PageSize
	// directory nodes. Every tree/root page remains exactly PageSize. The slot
	// cache therefore reserves the actual worst-case dirty bytes instead of
	// charging MaxPageSize for every metadata descriptor.
	largePages := overflowPages + 1
	if len(compiled) != 0 {
		largePages++
	}
	if len(columns) != 0 {
		largePages++
	}
	metadataPages := docMaxTransactionPages - largePages
	docMaxTransactionBytes := uint64(largePages)*uint64(o.MaxPageSize) +
		uint64(metadataPages)*uint64(o.PageSize)
	singleDocumentMetadataPages :=
		docSingleDocumentPages - largePages
	docSingleDocumentBytes :=
		uint64(largePages)*uint64(o.MaxPageSize) +
			uint64(singleDocumentMetadataPages)*uint64(o.PageSize)
	// Incremental exact-index maintenance stages, in the same transaction as the
	// document mutation, the term leaves that mutation can touch plus the
	// reference-root page. A leaf owns four posting tiles, so a mutation's whole
	// posting effect is one term leaf per physical index (an in-place value change
	// removes the old posting and inserts the new one in that same leaf), and a
	// single document can carry a value for every index -- so the worst case is
	// one term leaf for each of the fileStoreMaxPhysicalIndexes-bounded physical
	// indexes plus one PagePrimaryExactRoot. Each term leaf may reach
	// IndexTermLeafMaxBytes and so rounds up to a full MaxPageSize extent; the
	// root is exactly PageSize. This is the term-leaf reservation the design's
	// bounded exact-tile rebuild names, derived rather than fudged. It is zero for
	// an unindexed collection, so the unindexed mutation reserves exactly what it
	// did before.
	exactIndexLeafPages := len(compiled)
	exactIndexRootPages := 0
	if len(compiled) != 0 {
		exactIndexRootPages = 1
	}
	exactIndexPages := exactIndexLeafPages + exactIndexRootPages
	exactIndexBytes := uint64(exactIndexLeafPages)*uint64(o.MaxPageSize) +
		uint64(exactIndexRootPages)*uint64(o.PageSize)
	maxTransactionPages := docMaxTransactionPages + exactIndexPages
	singleDocumentTransactionPages := docSingleDocumentPages + exactIndexPages
	maxTransactionBytes := docMaxTransactionBytes + exactIndexBytes
	singleDocumentTransactionBytes := docSingleDocumentBytes + exactIndexBytes
	if o.MaxRetiredExtents < maxTransactionPages {
		return normalizedFileStoreOptions{}, fmt.Errorf("vibejson: collection MaxRetiredExtents must retain one worst-case transaction")
	}
	if o.BufferCount == 0 {
		o.BufferCount = defaultBufferCount(maxTransactionPages, o.MaxPageSize)
	}
	if o.BufferCount <= maxTransactionPages || o.BufferCount > maxCollectionBuffers {
		return normalizedFileStoreOptions{}, fmt.Errorf("vibejson: collection BufferCount must exceed worst-case %d-page transaction", maxTransactionPages)
	}
	if o.ResidentBytes < 0 || uint64(o.ResidentBytes) < maxTransactionBytes {
		return normalizedFileStoreOptions{}, fmt.Errorf("vibejson: collection ResidentBytes cannot retain one worst-case dirty transaction")
	}
	return normalizedFileStoreOptions{
		Options:                        o,
		maxTransactionPages:            maxTransactionPages,
		maxTransactionBytes:            maxTransactionBytes,
		singleDocumentTransactionPages: singleDocumentTransactionPages,
		singleDocumentTransactionBytes: singleDocumentTransactionBytes,
		singleDocumentFreeFoldLimit:    singleDocumentFreeFoldLimit,
		freeFoldLimit:                  freeFoldLimit,
		pageCatalog:                    pageCatalog,
		indexes:                        compiled, indexNameIDs: indexNameIDs,
		float64Columns: columns, indexCatalogHash: catalogHash,
	}, nil
}

// validateFilePageCatalogSchema preserves the public error category at the
// boundary where a valid in-memory schema enters the narrower durable format.
// It mirrors the schema-only byte bounds without constructing and immediately
// discarding a second canonical image during Open.
func validateFilePageCatalogSchema(schema *storeio.PageCatalogSchema) error {
	if schema == nil {
		return nil
	}
	if len(schema.Fields) > storeio.PageCatalogMaxSchemaFields {
		return fmt.Errorf(
			"schema has %d fields, durable maximum is %d",
			len(schema.Fields), storeio.PageCatalogMaxSchemaFields,
		)
	}
	canonicalBytes := uint64(storeio.PageCatalogCanonicalHeaderSize) +
		uint64(len(schema.Fields))*6
	previous := ""
	for fieldIndex, field := range schema.Fields {
		if len(field.Path) > storeio.PageCatalogMaxStringBytes {
			return fmt.Errorf(
				"schema field %d path has %d bytes, durable maximum is %d",
				fieldIndex, len(field.Path), storeio.PageCatalogMaxStringBytes,
			)
		}
		prefix := 0
		for prefix < len(previous) && prefix < len(field.Path) &&
			previous[prefix] == field.Path[prefix] {
			prefix++
		}
		canonicalBytes += uint64(4 + len(field.Path) - prefix)
		previous = field.Path
	}
	if canonicalBytes > storeio.PageCatalogMaxCanonicalBytes {
		return fmt.Errorf(
			"durable schema image has %d bytes, maximum is %d",
			canonicalBytes, storeio.PageCatalogMaxCanonicalBytes,
		)
	}
	return nil
}

const (
	// maxCollectionBuffers matches the durability device's own staging ceiling.
	// Buffer indexes are uint16 on the wire between the writer and the device.
	maxCollectionBuffers = 32768
	// defaultCommitDepth is how many worst-case transactions the shipped
	// staging pool holds at once. Depth is the quantity that matters, not the
	// buffer count: at depth one a serialized writer waits for its own
	// predecessor's durability fence before it may begin, so it pays one fence
	// per Put no matter what Durability or the group-commit knobs say.
	defaultCommitDepth = 4
	// defaultCommitStageBytes caps what that depth is allowed to cost. Without
	// it the pool would scale with MaxDocumentBytes, and a store configured for
	// 64 MiB documents would silently reserve half a gigabyte of staging. A
	// configuration whose single worst-case transaction already exceeds this
	// budget keeps the old depth-one geometry, which is the correct
	// degradation: it is the one that fits.
	defaultCommitStageBytes = 32 << 20
	// defaultBatchValueBytes keeps automatic and explicit mutation admission
	// bounded even when the collection permits multi-megabyte documents.
	defaultBatchValueBytes = 16 << 20
)

// defaultBufferCount sizes the commit-buffer pool when the caller leaves
// BufferCount zero. It grows the smallest legal pool — a power of two strictly
// greater than one worst-case transaction — toward defaultCommitDepth
// transactions, stopping at the staging budget or the device ceiling.
func defaultBufferCount(maxTransactionPages, maxPageSize int) int {
	count := 1
	for count <= maxTransactionPages {
		count <<= 1
	}
	// maxTransactionPages+1 counts the alternate root buffer a transaction
	// reserves alongside its pages; leaving it out would size the pool a hair
	// short of the depth it claims to provide.
	target := defaultCommitDepth * (maxTransactionPages + 1)
	for count < target && count*2 <= maxCollectionBuffers &&
		uint64(count*2)*uint64(maxPageSize) <= defaultCommitStageBytes {
		count <<= 1
	}
	return count
}

func fileIndexHashBytes(hash uint64, src []byte) uint64 {
	for _, value := range src {
		hash = (hash ^ uint64(value)) * 1099511628211
	}
	return hash
}

type fileStoreState struct {
	root      storeio.StateRoot
	super     storeio.Superblock
	keyRoot   storeio.PageRef
	chunkRoot storeio.PageRef
	indexRoot storeio.PageRef
	// freeHead is the newest delta page of the free log, or the zero reference
	// when the durable free set is empty. It is reached through the superblock
	// rather than the state root, so the whole free set is replaceable without
	// rewriting a directory.
	freeHead storeio.PageRef
}

// Collection is a bounded-residency, page-oriented JSON document store. It owns
// no caller file lifetime: file must remain open through Close. Structural
// mutations are copy-on-write and automatically persisted through a checksummed
// double root. Reads use explicit Snapshot leases and caller-owned copy-out
// buffers.
type Collection struct {
	file         *os.File
	writerLocked bool
	options      normalizedFileStoreOptions
	storeID      [16]byte

	writer         sync.Mutex
	durabilityWait sync.WaitGroup
	snapshotGate   sync.RWMutex
	closed         bool
	closeDone      bool
	// state is the writer's newest applied generation. Readers use
	// visibleState so synchronous commits cannot leak before their fence.
	state          atomic.Pointer[fileStoreState]
	visibleState   atomic.Pointer[fileStoreState]
	durableState   atomic.Pointer[fileStoreState]
	visibilityMu   sync.Mutex
	pendingVisible []filePendingState

	committer *storeio.Committer
	cache     *storeio.PageCache
	// primaryRouter is swapped wholesale by a structural split/merge/reclass
	// transaction and mutated in place by ordinary COW UpdateLeaf. Lock-free
	// point reads load it once; an atomic pointer keeps that swap race-free.
	primaryRouter atomic.Pointer[storeio.ResidentPrimaryRouter]
	// primaryExact holds the resident exact-term leaves published on the ordered
	// graph, one per physical index; primaryLive is the live posting-slot map the
	// read-side posting recheck validates against. Both are non-nil only for an
	// indexed ordered-primary collection and are rebuilt wholesale after a
	// mutation changes the graph (see rebuildPrimaryExactIndexes). They are
	// protected by the same writer/snapshotGate discipline as primaryRouter.
	primaryExact []primaryExactResident
	primaryLive  map[uint32]*[storeio.TermPostingTileChunks]uint64
	// structuralExactReencoded and structuralExactRemoved accumulate, within one
	// bounded structural transaction, the exact-index contribution of every leaf
	// the transaction re-encodes plus the buckets it removes outright, so the
	// affected postings are rebuilt atomically with the tablet (see
	// structuralRepairPostingsHook). They are writer-owned scratch reset at the
	// start of each structural transaction.
	structuralExactReencoded map[storeio.BucketID]*structuralBucketContribution
	structuralExactRemoved   []storeio.BucketID
	readFile      *os.File
	writeFile     *os.File
	directRead    bool
	directWrite   bool
	leases        *storeio.GenerationLeases
	reclaimer     *storeio.ExtentReclaimer
	pageValidator *fileStorePageValidator
	combiner      *fileMutationCombiner
	// journal is the bounded redo log paired with this store. It is non-nil only
	// for a DurabilityBufferedVisible collection opened or created with
	// Options.RecoveryJournal. It is owned by the serialized writer exactly like
	// the committer, appended and synced under c.writer, and recycled inside the
	// checkpoint's root publication. journalID mirrors the header identity written
	// into the state root so recovery cannot pair a stray file; journalPowerSafe
	// selects the acknowledgement barrier strength from CheckpointStrength.
	journal          *storeio.RecoveryJournal
	journalID        [16]byte
	journalPowerSafe bool
	// journalReplaying suppresses journal appends while Open re-applies recovered
	// records through the ordinary mutation path: those records are already
	// durable, and the recycle that follows replay discards them regardless.
	journalReplaying bool
	// journalFailure is the sticky poison set when a journal append or sync fails.
	// A journal fsync-class error is terminal — the platform may drop the very
	// dirty pages a retry would need — so like the committer's own poisoning it is
	// die-don't-retry: every later mutation, checkpoint, and Close is rejected
	// until the collection is reopened and recovers through replay.
	journalFailure atomic.Pointer[journalFailureBox]
	// writeTransaction and the point-mutation scratch below are protected by
	// writer. The automatic mutation combiner's leader also holds writer while
	// applying its batch, so no transaction can overlap a Reset.
	writeTransaction storeio.WriteTransaction

	automaticMutationGroups       atomic.Uint64
	automaticMutationRequests     atomic.Uint64
	automaticMutationWaits        atomic.Uint64
	automaticMutationQueueHigh    atomic.Uint32
	automaticMutationBytesHigh    atomic.Uint64
	automaticMutationGroupHigh    atomic.Uint32
	automaticCheckpoints          atomic.Uint64
	retirementPressureCheckpoints atomic.Uint64
	materializationAttempts       atomic.Uint64
	materializationUpdates        atomic.Uint64
	materializationFallbacks      atomic.Uint64
	materializationSnapshotSkips  atomic.Uint64
	materializationBusySkips      atomic.Uint64
	bufferedInplaceAttempts       atomic.Uint64
	bufferedInplaceUpdates        atomic.Uint64
	bufferedInplaceFallbacks      atomic.Uint64
	bufferedFirstTouchOverflows   atomic.Uint64
	// journalAcks counts frame-deferred mutations made durable by a single journal
	// append plus one sync, the redo lane's fast acknowledgement. chainAcks counts
	// mutations whose durability instead came from a committer root fence — the
	// snapshot-contended chain path and every forced or explicit checkpoint. The
	// split is how a bench distinguishes the bounded-append lane from the
	// full-publication lane at the store level.
	journalAcks              atomic.Uint64
	chainAcks                atomic.Uint64
	primaryLeafSplitRequired atomic.Uint64
	primaryEmptyLeaves       atomic.Uint64
	// Structural-transaction accounting for phase-8 split/merge/reclass. The
	// *MaxNS fields are the observed high-water bounded-transaction latency so a
	// harness can gate p-max without a full histogram allocation.
	primaryLeafSplits         atomic.Uint64
	primaryLeafMerges         atomic.Uint64
	primaryLeafReclass        atomic.Uint64
	primaryMacroSplitRequired atomic.Uint64
	// mergeReclassEvaluations counts every post-delete merge/reclass evaluation
	// (the second writer-lock acquisition after a routed delete). The rest
	// decompose its outcome, and the whole point of the capacity-relative floor is
	// that evaluations vastly exceeds the flush count on a byte-full corpus:
	//   - mergeReclassWarrantedHits: passed every read-only gate and PAID A FLUSH.
	//     This is the only counter that implies a checkpoint. It equals commits
	//     plus the rare post-flush abort (a count-viable neighbour that turned out
	//     to be on a different anchor page).
	//   - mergeReclassCommits: published a structural transaction.
	//   - mergeReclassAborts: identified a candidate but committed nothing --
	//     mostly the read-only no-viable-neighbour abort (checkpoint-free), plus
	//     the rare post-flush abort. Recorded as a hysteresis stamp.
	//   - mergeReclassSkips: below-floor evaluations elided read-only because the
	//     leaf already aborted at this exact live count (checkpoint-free).
	// evaluations - warranted is the read-only fraction; only warranted flushes.
	mergeReclassEvaluations   atomic.Uint64
	mergeReclassWarrantedHits atomic.Uint64
	mergeReclassCommits       atomic.Uint64
	mergeReclassAborts        atomic.Uint64
	mergeReclassSkips         atomic.Uint64
	primarySplitMaxNS         atomic.Uint64
	primaryMergeMaxNS         atomic.Uint64
	primaryReclassMaxNS       atomic.Uint64

	parseScratch            []vibejson.IndexEntry
	oldParseScratch         []vibejson.IndexEntry
	indexValueScratch       []byte
	indexNewCertificate     []byte
	indexCertificateScratch []byte
	indexGroupSource        []storeio.IndexGroupCatalogEntry
	indexGroupEntries       []storeio.IndexGroupCatalogEntry
	documentValueScratch    []byte
	rowScratch              []storeio.DocumentRecord
	retireScratch           []storeio.FreeExtent
	// retireRefScratch mirrors exact PageRefs opportunistically for cache
	// cleanup. retireScratch remains the authoritative durable/reclaimer list
	// and may coalesce adjacent refs; this list never affects correctness when
	// its fixed capacity is exhausted.
	retireRefScratch       []storeio.PageRef
	reusable               []storeio.FreeExtent
	reuseJournal           []storeio.ReuseEdit
	reusableBlock          *storemem.Block
	freeExtentIndex        storeio.FreeExtentIndex
	freeExtentMaxima       []uint64
	freeScratchBlock       *storemem.Block
	materializationBlock   *storemem.Block
	materializationBefore  []byte
	materializationAfter   []byte
	bufferedFirstTouches   []storeio.PageRef
	bufferedValueBefore    []byte
	primaryLeafScratch     []byte
	primaryRootScratch     []byte
	primaryPendingParents  []filePrimaryPendingParent
	primaryVolatileRetired []storeio.PageRef
	// primaryCheckpointBase is the last buffered primary checkpoint materialize
	// published, held only while it is newer than durableState. A materialize
	// derives its whole base -- the parent graph it re-checkpoints, the FileEnd it
	// allocates past, and the generation it stamps on the extents it retires --
	// from the previous checkpoint. checkpointBufferedLocked keeps durableState
	// current by flushing after it materializes, but Snapshot() and a
	// snapshot-contended mutation materialize with no flush, so durableState is
	// left behind. Without this field the next materialize would re-derive from the
	// stale durableState: re-retire that base's root a second time (the
	// "overlapping retired extent" the reclaimer rejects), allocate past its stale
	// FileEnd over the un-flushed checkpoint's pages, and rebuild from its stale
	// root, silently reverting the intervening materialize. Advancing this base in
	// memory instead of forcing a device flush keeps the buffered cut off the
	// steady-state persistence path; the crash-recovery fence is unaffected because
	// it keys off the committer's on-disk FallbackGeneration, not this pointer, so
	// every un-flushed checkpoint's extents stay fenced from reuse until a real
	// flush advances the durable floor. Writer-lock owned; a flush advancing
	// durableState past it clears it lazily in the next materialize.
	primaryCheckpointBase *fileStoreState
	// structuralRows is reused row scratch for a leaf split/merge re-encode. Its
	// records borrow the source leaf page and are valid only while that page is
	// leased inside the structural transaction.
	structuralRows       []storeio.CommonPrimaryLeafRecord
	float64Masks         []uint64
	float64Values        []float64
	float64StripeBytes   []byte
	float64StripeColumns []storeio.Float64StripeColumn
	pointKeyScratch      []byte
	pointChunkEdit       [1]fileChunkEdit
	// inlineFree is writer-only durable free-log lineage. Snapshots never need
	// it, so keeping its fixed record arena off fileStoreState avoids copying a
	// multi-kilobyte value into every tiny published state object.
	inlineFree     storeio.InlineFreeDelta
	nextInlineFree storeio.InlineFreeDelta

	// Durable free-set bookkeeping. freeSegments is the published segment index;
	// freeIndexPages and freeDeltaPages are the pages the published index and
	// chain occupy, kept so a fold can retire exactly what it supersedes.
	// freeDirty marks, per published segment, that its durable page no longer
	// matches memory, which is what lets a fold rewrite those and carry the rest
	// forward by reference instead of rewriting the whole image. freePending
	// holds free-set changes made outside a transaction — reclamation, which is
	// not rolled back by Abort — and so must survive an aborted commit or those
	// extents would never be written down.

	freeSegments    []storeio.FreeSegment
	freeNewSegments []storeio.FreeSegment
	freeIndexPages  []storeio.PageRef
	freeNewIndex    []storeio.PageRef
	freeDeltaPages  []storeio.PageRef
	freeNewDelta    []storeio.PageRef
	freeDirty       []bool
	freeResident    []bool
	freeNewResident []bool
	freeReadBack    []bool
	freeRetired     []bool
	freeDirtyCount  int
	freeDirtyAll    bool
	freeFoldRanges  [][2]int
	freeFoldOrder   []freeFoldSlot
	freeFoldPages   []storeio.TransactionPage

	freePending        []storeio.FreeDelta
	freeDeltas         []storeio.FreeDelta
	freeSpill          []storeio.FreeDelta
	freeReclaimed      []storeio.FreeExtent
	retirementAbsorbed []storeio.FreeExtent
	freeFenced         []storeio.FreeExtent
	freeImageScratch   []storeio.FreeExtent
	freeAllocMark      []uint32
	freeAllocStamp     uint32
	freeSetLimit       int
	freeResidentBudget int
	freeFoldLimit      int
	freeDeltaPerPage   int
	freeImagePerPage   int
	freeIndexPerPage   int
	freeFoldRequired   bool
	freeLoaded         bool
	freeNonResident    int

	appendChunk uint32
	appendLive  uint64

	// zoneScratch is the reusable chunk-summary merger handed to the chunk-tree
	// rewrite. It is serialized-writer state like every other scratch field
	// here, and living on the Collection is what keeps passing it as an
	// interface allocation-free.
	zoneScratch fileZoneMerger

	// Batched-publication scratch. Every one of these is reset at the start of
	// an Update and reused across calls, so a batch's steady-state cost is the
	// pages it publishes rather than the slices it would otherwise allocate.
	batch               *WriteBatch
	batchMutations      []fileBatchMutation
	batchPlacement      []fileBatchPlacement
	batchChunkEdits     []fileChunkEdit
	batchChunkTreeEdits []storeio.ChunkTreeEdit
	batchPageKeyEdits   []storeio.PageKeyTreeEdit
	batchIndexEdits     []fileIndexBatchEdit
	batchTreeEdits      []storeio.IndexTreeEdit
	batchTreeCerts      []byte
	batchCertArena      []byte
	batchRetired        []storeio.PageRef
}

// Stats is a point-in-time resource and I/O accounting snapshot.
// Every byte and queue counter corresponds to a configured finite budget.
type Stats struct {
	CapacityBytes uint64
	ResidentBytes uint64
	// ReservedBytes is the cache arena actually owned by resident extents.
	// It can exceed ResidentBytes when an exact on-disk extent occupies the
	// next buddy size class in RAM, but never exceeds CapacityBytes.
	ReservedBytes uint64
	// CommitCapacityBytes is the small fixed root/journal/patch arena owned by
	// the durability device. Immutable data-page staging is already included
	// in the cache capacity above.
	CommitCapacityBytes uint64
	PinnedPages         uint64
	DirtyBytes          uint64
	PageReads           uint64
	ReadBytes           uint64
	CacheHits           uint64
	CacheMisses         uint64
	CoalescedReads      uint64
	ReadErrors          uint64
	PrefetchHits        uint64
	Evictions           uint64
	PrefetchQueued      uint64
	PrefetchDropped     uint64
	// PrefetchQueueDepth samples references waiting for either read engine.
	PrefetchQueueDepth uint64
	// ReadQueueDepth is the configured native submission bound.
	ReadQueueDepth uint32
	// AsyncReadBatches counts successful native submissions.
	AsyncReadBatches uint64
	// LargestReadBatch is the native submission high-water.
	LargestReadBatch uint32

	PublishedGeneration uint64
	DurableGeneration   uint64
	CommitQueueDepth    uint64
	DeviceCommits       uint64
	CommittedBatches    uint64
	LargestCommitGroup  uint32
	// SuppressedRootWrites/Bytes count intermediate state pages omitted when
	// several generations share one newest durable superblock.
	SuppressedRootWrites uint64
	SuppressedRootBytes  uint64
	// SupersededRootWrites/Bytes count buffered alternate-superblock staging
	// buffers returned before checkpoint because only a newer root can be
	// selected.
	SupersededRootWrites uint64
	SupersededRootBytes  uint64
	// SupersededPageWrites/Bytes count exact buffered page writes omitted after
	// a later publication retired them while no snapshot was active.
	SupersededPageWrites uint64
	SupersededPageBytes  uint64
	// TailWitnessWrites/Bytes count unreachable pages still submitted because
	// they alone extended the file through the published FileEnd.
	TailWitnessWrites uint64
	TailWitnessBytes  uint64
	// PrewrittenPageWrites/Bytes count sealed buffered pages written without a
	// barrier or root publication while the checkpoint worker was idle.
	PrewrittenPageWrites uint64
	PrewrittenPageBytes  uint64
	// AutomaticCheckpoints counts successful Flush calls forced internally by
	// bounded dirty-cache or buffered-visible staging pressure.
	AutomaticCheckpoints uint64
	// RetirementPressureCheckpoints counts retirement-capacity events that
	// forced an otherwise-unrequested checkpoint before retry.
	RetirementPressureCheckpoints uint64
	// DeviceBytes counts payload bytes handed to the durability device since
	// open, including opportunistic pre-writes. Divided by CommittedBatches it
	// is write amplification per generation. FileEnd cannot answer that
	// question: copy-on-write reuses retired extents, so the file stops growing
	// while amplification does not.
	DeviceBytes                   uint64
	MaterializedBatches           uint64
	MaterializationJournalBytes   uint64
	MaterializationTargetBytes    uint64
	MaterializationFullWriteBytes uint64
	MaterializationBarriers       uint64
	MaterializationAttempts       uint64
	MaterializationUpdates        uint64
	MaterializationFallbacks      uint64
	MaterializationSnapshotSkips  uint64
	MaterializationBusySkips      uint64
	MaterializationScratchBytes   uint64
	// BufferedInplace* accounts for the narrow same-size current-chunk
	// canonical-frame lane. Fallbacks remain ordinary COW publications.
	BufferedInplaceAttempts  uint64
	BufferedInplaceUpdates   uint64
	BufferedInplaceFallbacks uint64
	// BufferedFirstTouchOverflows counts eligible ordinary COW publications
	// that could not be remembered because the bounded per-checkpoint set was
	// full. Those frames remain on the ordinary COW path.
	BufferedFirstTouchOverflows uint64
	// JournalAcks counts frame-deferred mutations acknowledged through a single
	// recovery-journal append plus one sync, with the root publication deferred to
	// the next checkpoint. ChainAcks counts mutations whose durability instead
	// came from a committer root fence: the snapshot-contended chain path and
	// every forced or explicit checkpoint. Both are zero unless a recovery journal
	// is configured.
	JournalAcks uint64
	ChainAcks   uint64
	// PrimaryLeafSplitRequired counts inserts rejected before publication
	// because the selected wide leaf needs the deferred structural split.
	PrimaryLeafSplitRequired uint64
	// PrimaryEmptyLeaves counts routed leaves currently empty (made empty and
	// not yet refilled or reclaimed by a merge). A merge that retires an empty
	// leaf decrements it. The counter is rebuilt from zero on Open.
	PrimaryEmptyLeaves uint64
	// PrimaryLeafSplits, PrimaryLeafMerges, and PrimaryLeafReclass count the
	// phase-8 bounded structural transactions performed this session. Reclass
	// counts wide->narrow slot-class rewrites folded into a split or merge.
	PrimaryLeafSplits  uint64
	PrimaryLeafMerges  uint64
	PrimaryLeafReclass uint64
	// MergeReclassEvaluations counts post-delete merge/reclass evaluations (the
	// second writer-lock acquisition each routed delete pays);
	// MergeReclassWarranted counts the subset that proceeded to a structural
	// transaction. PrimaryTemplateDetemplateEvents counts template-leaf
	// de-template splices for mutation; PrimaryTemplateReplanEvents counts full
	// template dictionary re-dedup encodes. These are process-global storeio
	// counters, reset by ResetTemplateColumnarDiag.
	MergeReclassEvaluations         uint64
	MergeReclassWarranted           uint64
	// MergeReclassCommits, MergeReclassAborts, and MergeReclassSkips decompose the
	// merge-floor engagement. Commits published a structural transaction; aborts
	// were warranted but found nothing viable (a checkpoint-free abort recorded
	// for hysteresis); skips were elided read-only because the leaf already
	// aborted at this live count. Only commits pay a flush.
	MergeReclassCommits             uint64
	MergeReclassAborts              uint64
	MergeReclassSkips               uint64
	PrimaryTemplateDetemplateEvents uint64
	PrimaryTemplateReplanEvents     uint64
	// PrimaryMacroSplitRequired counts structural transactions that could not
	// proceed because a tablet's 4096 local IDs or 16 anchor pages are exhausted
	// and a macro-tablet split (the next phase) is required.
	PrimaryMacroSplitRequired uint64
	// PrimarySplitMaxNS, PrimaryMergeMaxNS, and PrimaryReclassMaxNS report the
	// high-water bounded-transaction latency in nanoseconds for each structural
	// kind, so a harness can bound p-max without a resident histogram.
	PrimarySplitMaxNS   uint64
	PrimaryMergeMaxNS   uint64
	PrimaryReclassMaxNS uint64
	// PrimaryMutationScratchBytes is the fixed leaf-promotion and raw
	// segmented-root writer scratch, allocated only for PrimaryRoot stores.
	PrimaryMutationScratchBytes uint64
	// AutomaticMutation* accounts for ordinary Put/Delete calls collapsed
	// before page materialization. Reads never consult this queue.
	AutomaticMutationGroups       uint64
	AutomaticMutationRequests     uint64
	AutomaticMutationWaits        uint64
	AutomaticMutationQueueHigh    uint32
	AutomaticMutationBytesHigh    uint64
	LargestAutomaticMutationGroup uint32
	// Backend reports the durable write engine.
	Backend Backend
	// Durability reports acknowledgement and reader-visibility semantics.
	Durability DurabilityMode
	// CheckpointStrength reports the configured Flush/Close persistence class.
	CheckpointStrength CheckpointStrength
	// ReadBackend reports the active speculative-read engine. Demand misses
	// remain correct through positional reads regardless of this value.
	ReadBackend Backend
	// DirectReads reports actual O_DIRECT cache-miss reads, not merely a
	// requested try-direct policy.
	DirectReads bool
	// DirectWrites reports actual O_DIRECT durable writes. It is independent
	// from DirectReads and the selected portable or io_uring commit backend.
	DirectWrites bool

	SnapshotCapacity             uint64
	ActiveSnapshots              uint64
	OldestSnapshotGeneration     uint64
	OldestSnapshotAgeGenerations uint64
	RetiredExtentCapacity        uint64
	// ReusableCapacityBytes is the fixed pointer-free extent arena. Common
	// Unix platforms keep it outside the Go heap.
	ReusableCapacityBytes uint64
	// ReusableExternalBytes is the portion of ReusableCapacityBytes outside
	// the Go heap on this platform.
	ReusableExternalBytes uint64
	// ReusableIndexBytes is the fixed caller-backed first-fit hierarchy.
	// ReusableIndexExternalBytes is the portion outside the Go heap.
	ReusableIndexBytes         uint64
	ReusableIndexExternalBytes uint64
	// RetiredIntervalIndexBytes is the bounded large-fragmentation overlap
	// index. Its mmap-backed arena is reserved at open without touching its
	// node pages; they become resident only if fragmentation first crosses the
	// linear threshold.
	RetiredIntervalIndexBytes         uint64
	RetiredIntervalIndexExternalBytes uint64
	// RetiredExtentArenaBytes is the fixed generation-ordered retirement
	// table. Durable stores keep it pointer-free and outside the Go heap on
	// platforms where the shared metadata block is mmap-backed.
	RetiredExtentArenaBytes         uint64
	RetiredExtentArenaExternalBytes uint64
	// FreeScratchCapacityBytes is the one fixed pointer-free arena used to
	// plan free-image folds. FreeScratchExternalBytes is the portion outside
	// the Go heap on this platform.
	FreeScratchCapacityBytes uint64
	FreeScratchExternalBytes uint64
	// FreeScratchLiveBytes is the portion occupied by the current fold's
	// fenced/image/range/order slices. It returns to zero or a small retained
	// plan without fragmenting the general heap.
	FreeScratchLiveBytes uint64
	// Float64ScratchBytes is the fixed pointer-free writer scratch used to
	// rebuild typed covering columns during one chunk replacement.
	Float64ScratchBytes   uint64
	PendingRetiredExtents uint64
	PendingRetiredBytes   uint64
	// AbandonedExtents and AbandonedBytes are retained for source compatibility
	// and are always zero. Commits now fail before publication rather than
	// forgetting reusable-space metadata.
	AbandonedExtents uint64
	AbandonedBytes   uint64
	ReusableExtents  uint64
	ReusableBytes    uint64
	DocumentCount    uint64
	// ChunkSlots is the stable-slot capacity in live chunks. VacantChunkSlots
	// is the immediately reusable logical space inside those chunks; it excludes
	// absent chunks below ChunkHighWater, which an insert can also reclaim.
	ChunkSlots       uint64
	VacantChunkSlots uint64
	LiveChunks       uint32
	// ChunkHighWater is the logical placement high-water. The difference from
	// LiveChunks exposes completely empty historical chunks without walking the
	// chunk directory or touching document pages.
	ChunkHighWater uint32
	FileEnd        uint64
}

// Create initializes an empty durable collection in file and fences its
// first root before returning.
func Create(file *os.File, options Options) (*Collection, error) {
	if file == nil {
		return nil, fmt.Errorf("vibejson: nil collection file")
	}
	if err := storeio.LockWriter(file); err != nil {
		return nil, err
	}
	locked := true
	defer func() {
		if locked {
			_ = storeio.UnlockWriter(file)
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() != 0 {
		return nil, ErrNotEmpty
	}
	normalized, err := options.normalized()
	if err != nil {
		return nil, err
	}
	var storeID [16]byte
	if _, err := rand.Read(storeID[:]); err != nil {
		return nil, fmt.Errorf("vibejson: create collection identity: %w", err)
	}
	// A freshly created store is always chunk-layout (createInitialState leaves
	// PrimaryRoot zero); a journaled primary store is built by CreateFromPrimary
	// and reached through Open.
	collection, err := newCollectionResources(file, normalized, storeID, false)
	if err != nil {
		return nil, err
	}
	collection.writerLocked = true
	locked = false
	if err := collection.createInitialState(); err != nil {
		_ = collection.closeResources()
		return nil, err
	}
	return collection, nil
}

// Open performs bounded recovery: it reads the two superblocks, the
// selected state root, and its top-level directory pages, then starts with an
// empty read cache. It does not scan keys, documents, or postings.
func Open(file *os.File, options Options) (*Collection, error) {
	if file == nil {
		return nil, fmt.Errorf("vibejson: nil collection file")
	}
	if err := storeio.LockWriter(file); err != nil {
		return nil, err
	}
	locked := true
	defer func() {
		if locked {
			_ = storeio.UnlockWriter(file)
		}
	}()
	bootstrap, err := storeio.DiscoverMutableInlineBootstrap(file)
	if err != nil {
		return nil, err
	}
	if options.PageSize != 0 &&
		options.PageSize != int(bootstrap.PageSize) ||
		options.MaxPageSize != 0 &&
			options.MaxPageSize != int(bootstrap.MaxPageSize) ||
		options.MaterializationDamageGranule !=
			int(bootstrap.MaterializationDamageGranule) {
		return nil, fmt.Errorf(
			"vibejson: collection persisted geometry mismatch",
		)
	}
	scratch := make([]byte, int(bootstrap.MaxPageSize))
	recovery, err := storeio.RecoverMutableInlineStateRoot(
		file, bootstrap.PageSize,
		bootstrap.MaterializationDamageGranule, scratch,
	)
	if err != nil {
		return nil, err
	}
	inline, root := recovery.Root, recovery.State
	rootSlot, fallbackGeneration :=
		recovery.RootSlot, recovery.FallbackGeneration
	normalized, err := normalizeOpenedFileStoreOptions(
		options, root, recovery.Catalog,
	)
	if err != nil {
		return nil, err
	}
	if root.PrimaryRoot == (storeio.PageRef{}) &&
		root.DocumentCount != 0 &&
		root.KeyDirectory.Kind != storeio.PageFingerprintDirectory {
		return nil, fmt.Errorf(
			"vibejson: collection options or unsupported durable catalog mismatch",
		)
	}
	if root.PageSize != uint32(normalized.PageSize) ||
		root.MaxPageSize != uint32(normalized.MaxPageSize) {
		return nil, fmt.Errorf("vibejson: collection options or unsupported durable catalog mismatch")
	}
	collection, err := newCollectionResources(
		file, normalized, root.StoreID,
		root.PrimaryRoot != (storeio.PageRef{}),
	)
	if err != nil {
		return nil, err
	}
	collection.writerLocked = true
	locked = false
	if err := collection.committer.InitializeRecovery(
		root.Generation, rootSlot, fallbackGeneration,
	); err != nil {
		_ = collection.closeResources()
		return nil, err
	}
	if recovery.JournalSequence != 0 {
		if err := collection.committer.InitializeMaterializationRecovery(
			recovery.JournalSequence, recovery.JournalSlot,
		); err != nil {
			_ = collection.closeResources()
			return nil, err
		}
	}
	// Keep the old internal carrier for FileEnd and statistics while the public
	// format uses the state-bearing inline root exclusively.
	super := storeio.Superblock{
		StoreID: inline.StoreID, Generation: inline.Generation,
		FileEnd: inline.FileEnd, PageSize: inline.PageSize,
	}
	freeHead := inline.FreeDelta.ExternalPrev()
	if freeHead != (storeio.PageRef{}) {
		super.FreeOffset = freeHead.Offset
		super.FreeLength = freeHead.Length
	}
	state := &fileStoreState{
		root: root, super: super,
		keyRoot: root.KeyDirectory, chunkRoot: root.ChunkDirectory,
		indexRoot: root.IndexDirectory, freeHead: freeHead,
	}
	collection.inlineFree = inline.FreeDelta
	collection.pageValidator.update(state)
	if err := validateOpenedPrimaryGraph(
		collection.cache, root, super.FileEnd,
	); err != nil {
		_ = collection.closeResources()
		return nil, err
	}
	if root.PrimaryRoot != (storeio.PageRef{}) {
		collection.primaryLeafScratch = make(
			[]byte, storeio.CommonPrimaryLeafWideBytes,
		)
		collection.primaryRootScratch = make(
			[]byte, storeio.SegmentedTabletRouterRootBytes,
		)
		builtRouter, buildErr := storeio.BuildResidentPrimaryRouter(
			collection.cache, root.PrimaryRoot,
			storeio.GlobalTabletCatalogBounds{
				StoreID: root.StoreID, SelectedRootGeneration: root.Generation,
				FileEnd: super.FileEnd, NextLogicalID: root.NextLogicalID,
			},
		)
		err = buildErr
		if err == nil {
			collection.primaryRouter.Store(builtRouter)
		}
		if err != nil {
			_ = collection.closeResources()
			return nil, fmt.Errorf("vibejson: build resident primary router: %w", err)
		}
		// Both buffered-visible and the journal-backed synchronous lane apply
		// through the deferred canonical-frame path, so both need the pending
		// parent and volatile-retire scratch. Async-visible on a primary graph
		// uses the committer path and does not.
		if collection.buffered() || collection.synchronous() {
			pendingCapacity := min(
				normalized.MaxRetiredExtents,
				filePrimaryPendingParentLimit,
			)
			collection.primaryPendingParents = make(
				[]filePrimaryPendingParent, 0, pendingCapacity,
			)
			collection.primaryVolatileRetired = make(
				[]storeio.PageRef, 0,
				pendingCapacity,
			)
		}
		if err := collection.openPrimaryExactIndexes(state); err != nil {
			_ = collection.closeResources()
			return nil, fmt.Errorf(
				"vibejson: open ordered-primary exact indexes: %w", err,
			)
		}
	}
	collection.initializeFileState(state)
	collection.appendChunk = root.ChunkHighWater
	if err := collection.restoreAppendChunk(state); err != nil {
		_ = collection.closeResources()
		return nil, err
	}
	// The recovery journal only has a mutation lane on the ordered primary graph.
	// Opening a chunk-layout store with Options.RecoveryJournal would acknowledge
	// nothing through the journal, and a root that names a journal but carries no
	// primary graph is corrupt: both fail closed rather than lie about durability.
	if root.PrimaryRoot == (storeio.PageRef{}) &&
		(collection.journalConfigured() || root.JournalID != ([16]byte{})) {
		_ = collection.closeResources()
		return nil, ErrRecoveryJournalRequiresPrimary
	}
	// A root that names a journal must pair, replay, and recycle it before the
	// collection is reachable. A non-zero JournalID is authoritative regardless of
	// the caller's option: the store may have acknowledged mutations only the
	// journal records, so a missing or mismatched file fails closed here.
	if root.JournalID != ([16]byte{}) {
		// A journaled root reopens only on a lane that consults the journal:
		// buffered-visible (its opt-in) or the synchronous lane (where the journal
		// is how sync acknowledges). Async-visible has no journal lane, so it must
		// not adopt a root that may reference acknowledgements only the journal
		// records.
		if !collection.buffered() && !collection.synchronous() {
			_ = collection.closeResources()
			return nil, fmt.Errorf(
				"vibejson: journaled store must reopen buffered-visible or synchronous")
		}
		if err := collection.openRecoveryJournalLocked(root.JournalID); err != nil {
			_ = collection.closeResources()
			return nil, err
		}
		if err := collection.replayRecoveryJournalLocked(root.Generation); err != nil {
			_ = collection.closeResources()
			return nil, err
		}
	}
	return collection, nil
}

// storeCommitterFactory constructs the persistence committer for a collection.
// It is a package variable so durability crash tests can interpose a
// fault-injecting Device between the committer and the platform backend;
// production keeps the platform committer.
var storeCommitterFactory = storeio.NewCommitter

// newCollectionResources builds the committer and cache for a collection.
// primaryLayout reports whether the store carries an ordered primary graph,
// known from the recovered root at Open (and always false for a freshly created
// chunk store). It selects the committer's deferred-root manual checkpoint mode
// for both buffered-visible and the journal-backed synchronous lane, which
// stages canonical frames and publishes the root at checkpoints rather than
// fencing one per mutation.
func newCollectionResources(file *os.File, options normalizedFileStoreOptions, storeID [16]byte, primaryLayout bool) (*Collection, error) {
	writeFile, directWrite, err := storeio.OpenPageCommitFile(file, storeio.DirectMode(options.WriteMode))
	if err != nil {
		return nil, err
	}
	committer, err := storeCommitterFactory(writeFile, storeio.DeviceOptions{
		Backend: storeio.Backend(options.Backend), BufferCount: options.BufferCount,
		BufferSize: max(options.MaxPageSize, os.Getpagesize()), QueueDepth: options.BufferCount,
		CheckpointSync: storeio.CheckpointSync(options.CheckpointStrength),
	}, storeio.CommitterOptions{
		FrameNativeStaging: true,
		QueueSlots:         options.QueueSlots, MaxPagesPerBatch: options.maxTransactionPages,
		GroupLimit: options.GroupLimit, CoalesceDelay: options.CommitCoalesce,
		MaterializationDamageGranule: uint32(options.MaterializationDamageGranule),
		ManualCheckpoint: options.Durability == DurabilityBufferedVisible ||
			options.Durability == DurabilitySync && primaryLayout,
	})
	if err != nil {
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, err
	}
	readFile, directRead, err := storeio.OpenPageCacheFile(file, storeio.DirectMode(options.ReadMode))
	if err != nil {
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, err
	}
	pageValidator := newFileStorePageValidator(
		uint32(options.PageSize), uint32(len(options.indexes)),
		uint32(options.Collection.ChunkDocuments),
	)
	cache, err := storeio.NewPageCache(readFile, storeio.PageCacheOptions{
		PageSize: options.PageSize, MaxPageSize: options.MaxPageSize,
		ResidentBytes: options.ResidentBytes, StoreID: storeID,
		PrefetchQueue: options.PrefetchQueue, ReadConcurrency: options.ReadConcurrency,
		ReadQueueDepth: options.ReadQueueDepth,
		Backend:        storeio.Backend(options.Backend),
		Validate:       pageValidator.validate,
	})
	if err != nil {
		if readFile != file {
			_ = readFile.Close()
		}
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, err
	}
	leases, err := storeio.NewGenerationLeases(storeio.GenerationLeaseOptions{MaxLeases: options.MaxSnapshotLeases})
	if err != nil {
		_ = cache.Close()
		if readFile != file {
			_ = readFile.Close()
		}
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, err
	}
	extentSize := int(unsafe.Sizeof(storeio.FreeExtent{}))
	// Keep one bounded handoff batch beyond the retirement-table capacity. When
	// a long-held snapshot is released, refreshReusable can drain the full table
	// into this reserve before the next transaction consumes those extents. The
	// old equal-sized arenas deadlocked at exactly that boundary: neither side
	// had a slot in which to move the first extent.
	reusableCapacity := options.MaxRetiredExtents +
		min(options.MaxRetiredExtents, freeReclaimBatch)
	freeExtentMaximaCapacity :=
		storeio.FreeExtentIndexCapacity(reusableCapacity)
	retiredIntervalIndexBytes :=
		storeio.RetiredIntervalIndexStorageBytes(options.MaxRetiredExtents)
	retiredExtentArenaBytes :=
		storeio.RetiredExtentStorageBytes(options.MaxRetiredExtents)
	if reusableCapacity > math.MaxInt/extentSize ||
		freeExtentMaximaCapacity > (math.MaxInt-reusableCapacity*extentSize)/8 ||
		retiredIntervalIndexBytes == 0 || retiredExtentArenaBytes == 0 {
		_ = leases.Close()
		_ = cache.Close()
		if readFile != file {
			_ = readFile.Close()
		}
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, store.ErrCheckpointTooLarge
	}
	reusableExtentBytes := reusableCapacity * extentSize
	reusableIndexBytes := freeExtentMaximaCapacity * 8
	reusableMetadataBytes := reusableExtentBytes + reusableIndexBytes
	if retiredExtentArenaBytes > math.MaxInt-reusableMetadataBytes ||
		retiredIntervalIndexBytes >
			math.MaxInt-reusableMetadataBytes-retiredExtentArenaBytes {
		_ = leases.Close()
		_ = cache.Close()
		if readFile != file {
			_ = readFile.Close()
		}
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, store.ErrCheckpointTooLarge
	}
	reusableBlock, err := storemem.Allocate(
		reusableMetadataBytes + retiredExtentArenaBytes +
			retiredIntervalIndexBytes,
	)
	if err != nil {
		_ = leases.Close()
		_ = cache.Close()
		if readFile != file {
			_ = readFile.Close()
		}
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, err
	}
	reusableArena := unsafe.Slice(
		(*storeio.FreeExtent)(unsafe.Pointer(unsafe.SliceData(reusableBlock.Bytes()))),
		reusableCapacity,
	)
	freeExtentMaxima := unsafe.Slice(
		(*uint64)(unsafe.Pointer(
			unsafe.SliceData(
				reusableBlock.Bytes()[reusableExtentBytes:reusableMetadataBytes],
			),
		)),
		freeExtentMaximaCapacity,
	)
	retiredExtentStorage := reusableBlock.Bytes()[reusableMetadataBytes : reusableMetadataBytes+retiredExtentArenaBytes]
	retiredIntervalIndexStorage := reusableBlock.Bytes()[reusableMetadataBytes+retiredExtentArenaBytes:]
	reclaimer, err := storeio.NewExtentReclaimer(
		leases,
		storeio.ExtentReclaimerOptions{
			MaxRetiredExtents:    options.MaxRetiredExtents,
			IntervalIndexStorage: retiredIntervalIndexStorage,
			RetiredExtentStorage: retiredExtentStorage,
		},
	)
	if err != nil {
		_ = reusableBlock.Close()
		_ = leases.Close()
		_ = cache.Close()
		if readFile != file {
			_ = readFile.Close()
		}
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, err
	}
	var ownedRead *os.File
	if readFile != file {
		ownedRead = readFile
	}
	var ownedWrite *os.File
	if writeFile != file {
		ownedWrite = writeFile
	}
	pageSize := uint32(options.PageSize)
	deltaPerPage := storeio.FreeDeltaRecordCapacity(pageSize)
	imagePerPage := storeio.FreeImageRecordCapacity(pageSize)
	indexPerPage := storeio.FreeIndexRecordCapacity(pageSize)
	// The free set is capped at what the segment index can name, not at what one
	// image rewrite can hold. That is the whole change: the old cap was
	// FreeLogMaxImagePages*imagePerPage — sixteen pages, about 2,700 extents,
	// roughly 11 MiB of trackable free space at the 4 KiB page size — because a
	// fold rewrote the entire linked image and had to fit in one transaction.
	// At 4 KiB the index costs one page per 70 segments of 165 extents, so eight
	// index pages describe exactly 92,400 extents. The worst-case fold reserve
	// is 28 pages (8 index + 16 segment + 4 delta), and what must fit inside a
	// commit is a directory of the free set rather than the free set.
	//
	// A collection that fragments past this ceiling still stalls reclamation and
	// eventually fails writes with ErrRetiredExtentCapacity, exactly as before;
	// the ceiling is simply much further away, and raising it again
	// is a policy edit against FreeLogMaxIndexPages rather than a redesign.
	// Half the index's capacity, because the durable set now carries the fenced
	// extents as well as the reusable ones: a retirement is written down by the
	// commit that makes it, so both halves have to fit the same image.
	freeSetLimit := min(reusableCapacity,
		storeio.FreeLogMaxIndexPages*indexPerPage*imagePerPage/2)
	// How many segments an open reads before it stops. Everything past this stays
	// on disk until something needs it, which is what keeps open time a function
	// of the working set rather than of the free set: at 165 extents per segment a
	// store with ninety thousand free extents has five hundred and forty-six of
	// them, and reading all of them costs half a megabyte before the first write.
	//
	// The arena is the natural bound. A resident segment's extents live in
	// c.reusable, so residency can never usefully exceed what that holds, and
	// capping it there means a store configured for a small free set does not
	// read a large one. The floor of four is so that a fresh store, whose whole
	// free set is a handful of segments, behaves exactly as it did before.
	freeResidentBudget := max(4, freeSetLimit/imagePerPage)
	maxFreeSegments := storeio.FreeLogMaxIndexPages * indexPerPage
	freeFencedCapacity, freeImageScratchCapacity, ok :=
		checkedFileFreeScratchCounts(
			options.freeFoldLimit,
			imagePerPage,
			freeSetLimit,
			options.MaxRetiredExtents,
			options.maxTransactionPages,
		)
	if !ok {
		_ = reusableBlock.Close()
		_ = leases.Close()
		_ = cache.Close()
		if readFile != file {
			_ = readFile.Close()
		}
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, store.ErrCheckpointTooLarge
	}
	freeScratchBlock, freeScratch, err := newFileFreeScratch(
		freeFencedCapacity,
		freeImageScratchCapacity,
		options.freeFoldLimit,
		maxFreeSegments,
	)
	if err != nil {
		_ = reusableBlock.Close()
		_ = leases.Close()
		_ = cache.Close()
		if readFile != file {
			_ = readFile.Close()
		}
		_ = committer.Close()
		if writeFile != file {
			_ = writeFile.Close()
		}
		return nil, err
	}
	collection := &Collection{
		file: file, options: options, storeID: storeID, committer: committer, cache: cache,
		readFile: ownedRead, writeFile: ownedWrite,
		directRead: directRead, directWrite: directWrite,
		leases: leases, reclaimer: reclaimer,
		// A fold retires the whole superseded chain on top of the commit's own
		// retirements, so the scratch reserves both.
		retireScratch: make([]storeio.FreeExtent, 0, options.maxTransactionPages+
			fileStorePointFingerprintRetirePages+1+
			storeio.FreeLogMaxChainPages+storeio.FreeLogMaxIndexPages+
			options.freeFoldLimit),
		retireRefScratch: make([]storeio.PageRef, 0, options.maxTransactionPages+
			fileStorePointFingerprintRetirePages+1+
			storeio.FreeLogMaxChainPages+storeio.FreeLogMaxIndexPages+
			options.freeFoldLimit),
		reusable:         reusableArena[:0],
		reuseJournal:     make([]storeio.ReuseEdit, 0, options.maxTransactionPages),
		reusableBlock:    reusableBlock,
		freeExtentMaxima: freeExtentMaxima,
		freeScratchBlock: freeScratchBlock,
		float64Masks:     make([]uint64, len(options.float64Columns)),
		float64Values:    make([]float64, len(options.float64Columns)*64),
		pageValidator:    pageValidator,

		freeSegments:    make([]storeio.FreeSegment, 0, maxFreeSegments),
		freeNewSegments: make([]storeio.FreeSegment, 0, maxFreeSegments),
		freeIndexPages:  make([]storeio.PageRef, 0, storeio.FreeLogMaxIndexPages),
		freeNewIndex:    make([]storeio.PageRef, 0, storeio.FreeLogMaxIndexPages),
		freeDeltaPages:  make([]storeio.PageRef, 0, storeio.FreeLogMaxChainPages),
		freeNewDelta:    make([]storeio.PageRef, 0, storeio.FreeLogMaxDeltaPages),
		freeDirty:       make([]bool, 0, maxFreeSegments),
		freeResident:    make([]bool, 0, maxFreeSegments),
		freeReadBack:    make([]bool, 0, maxFreeSegments),
		freeNewResident: make([]bool, 0, maxFreeSegments),
		freeRetired:     make([]bool, 0, maxFreeSegments),
		freeFoldRanges:  freeScratch.ranges[:0],
		freeFoldOrder:   freeScratch.order[:0],
		freeFoldPages:   make([]storeio.TransactionPage, 0, options.freeFoldLimit),
		freeDirtyAll:    true,
		// Half the diff capacity belongs to changes made outside a transaction;
		// the rest is left for what the commit itself consumes. Overflowing the
		// half is not a failure, it schedules a fold.
		freePending: make([]storeio.FreeDelta, 0,
			storeio.FreeLogMaxDeltaPages*deltaPerPage/2),
		freeDeltas: make([]storeio.FreeDelta, 0,
			storeio.FreeLogMaxDeltaPages*deltaPerPage+options.maxTransactionPages),
		freeSpill: make([]storeio.FreeDelta, 0,
			storeio.InlineFreeDeltaCapacity),
		freeReclaimed:      make([]storeio.FreeExtent, 0, freeReclaimBatch),
		retirementAbsorbed: make([]storeio.FreeExtent, 0, freeReclaimBatch),
		// The fold image is the reusable set plus everything still fenced plus
		// what this commit just retired, so its scratch has to hold all three.
		freeFenced:         freeScratch.fenced[:0],
		freeImageScratch:   freeScratch.image[:0],
		freeAllocMark:      make([]uint32, freeSetLimit),
		freeSetLimit:       freeSetLimit,
		freeResidentBudget: freeResidentBudget,
		freeFoldLimit:      options.freeFoldLimit,
		freeDeltaPerPage:   deltaPerPage,
		freeImagePerPage:   imagePerPage,
		freeIndexPerPage:   indexPerPage,
		batchPlacement:     make([]fileBatchPlacement, 0, options.MaxBatchDocuments),
		pendingVisible:     make([]filePendingState, fileVisibilitySlots(options.QueueSlots)),
	}
	if options.Durability == DurabilityBufferedVisible {
		collection.bufferedFirstTouches = make(
			[]storeio.PageRef, 0, fileVisibilitySlots(options.QueueSlots),
		)
	}
	if !options.DisableMutationCombining &&
		options.MaxBatchDocuments > 1 &&
		options.Collection.Schema == nil &&
		options.Collection.IndexOptions.MaxDepth <= 0 {
		collection.combiner = newFileMutationCombiner(
			options.MaxBatchDocuments, options.MaxBatchBytes,
		)
	}
	if options.MaterializationDamageGranule != 0 {
		imageArenaBytes := options.MaxPageSize + options.PageSize
		block, allocateErr := storemem.Allocate(2 * imageArenaBytes)
		if allocateErr != nil {
			_ = collection.closeResources()
			return nil, allocateErr
		}
		collection.materializationBlock = block
		bytes := block.Bytes()
		collection.materializationBefore = bytes[:imageArenaBytes]
		collection.materializationAfter =
			bytes[imageArenaBytes : 2*imageArenaBytes]
	}
	if err := committer.SetCallbacks(
		collection.promoteDurableState,
		collection.poisonPersistence,
	); err != nil {
		_ = collection.closeResources()
		return nil, err
	}
	return collection, nil
}

func (c *Collection) beginWriteTransaction(
	maxPages int,
	options storeio.WriteTransactionOptions,
) (*storeio.WriteTransaction, error) {
	if err := c.writeTransaction.Reset(
		c.committer, c.cache, maxPages, options,
	); err != nil {
		return nil, err
	}
	return &c.writeTransaction, nil
}

func (c *Collection) createInitialState() error {
	layout, err := storeio.MutableStoreLayout(uint32(c.options.PageSize))
	if err != nil {
		return err
	}
	catalog, err := planFilePageCatalog(
		c.options.pageCatalog, c.cacheStoreID(), 1,
		uint32(c.options.PageSize), layout.DataStart,
		storeio.StateRootLogicalID+1,
	)
	if err != nil {
		return err
	}
	initialFileEnd := catalog.fileEnd
	if err := c.file.Truncate(int64(initialFileEnd)); err != nil {
		return err
	}
	if catalog.segments != 0 {
		catalogScratch := make([]byte, c.options.PageSize)
		if err := catalog.write(
			c.file, initialFileEnd, catalog.nextID, catalogScratch,
		); err != nil {
			return err
		}
		if err := c.file.Sync(); err != nil {
			return err
		}
	}
	tx, err := c.beginWriteTransaction(1, storeio.WriteTransactionOptions{
		StoreID: c.cacheStoreID(), Generation: 1, PageSize: uint32(c.options.PageSize),
		FileEnd: initialFileEnd, NextLogicalID: catalog.nextID,
	})
	if err != nil {
		return err
	}
	root := storeio.StateRoot{
		StoreID: c.cacheStoreID(), Generation: 1, PageSize: uint32(c.options.PageSize),
		NextLogicalID: tx.NextLogicalID(), ChunkDocuments: uint32(c.options.Collection.ChunkDocuments),
		IndexCount: uint32(len(c.options.indexes)), IndexCatalogHash: c.options.indexCatalogHash,
		IndexMaxDepth:    uint32(max(c.options.Collection.IndexOptions.MaxDepth, 0)),
		MaxKeyBytes:      uint32(c.options.MaxKeyBytes),
		InlineValueBytes: uint32(c.options.InlineValueBytes),
		MaxDocumentBytes: uint32(c.options.MaxDocumentBytes),
	}
	root.Options = fileStoreCollectionOptionFlags(c.options.Collection)
	if len(c.options.float64Columns) != 0 {
		root.Options |= storeio.StateOptionFloat64Columns
	}
	if c.options.Collection.Schema != nil {
		root.Options |= storeio.StateOptionSchema
	}
	if c.options.MaterializationDamageGranule != 0 {
		root.Options |= storeio.StateOptionCanonicalMaterialization
		root.MaterializationDamageGranule =
			uint32(c.options.MaterializationDamageGranule)
	}
	if err := catalog.apply(&root, uint32(c.options.MaxPageSize)); err != nil {
		_ = tx.Abort()
		return err
	}
	// createInitialState builds a chunk-layout store (StateRoot.PrimaryRoot stays
	// zero). The recovery journal has no mutation lane there -- journalAckLocked
	// fires only from putPrimary/deletePrimary -- so minting one here would pair a
	// file that no acknowledgement ever appends to, a silent durability lie. Fail
	// closed; a journaled store is built through CreateFromPrimary.
	if c.journalConfigured() {
		_ = tx.Abort()
		return ErrRecoveryJournalRequiresPrimary
	}
	inlineFree := storeio.NewInlineFreeDelta(storeio.PageRef{}, storeio.PageRef{})
	if err := tx.PublishInline(root, inlineFree); err != nil {
		_ = tx.Abort()
		return err
	}
	if err := c.committer.Flush(); err != nil {
		return err
	}
	c.cache.MarkDurable(1)
	super := storeio.Superblock{
		StoreID: root.StoreID, Generation: 1,
		FileEnd: tx.FileEnd(), PageSize: uint32(c.options.PageSize),
	}
	state := &fileStoreState{root: root, super: super}
	c.inlineFree = inlineFree
	c.pageValidator.update(state)
	c.initializeFileState(state)
	c.freeLoaded = true
	return nil
}

func (c *Collection) cacheStoreID() [16]byte {
	return c.storeID
}

// Snapshot pins one immutable durable root generation. Close must be
// called; copy-out methods remain valid independently of page eviction.
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
	// exact and live are the immutable resident exact-index posting snapshot
	// captured at Snapshot creation under snapshotGate. An incremental mutation
	// installs a fresh pair by swapping the collection fields, so capturing them
	// once here pins a consistent (term leaves, live map) view for this reader's
	// whole lifetime even as the writer maintains the index.
	exact []primaryExactResident
	live  map[uint32]*[storeio.TermPostingTileChunks]uint64
	lease storeio.GenerationLease
	once  sync.Once
}

// IndexWorkspace retains the transient routing entries, their copied
// certificates, the ordered probe decisions, and the document bytes and tape
// used by one durable exact-index probe. Its zero value is ready to use. The
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
	probe             storeio.IndexTreeProbe
	postings          []fileIndexProbePosting
	document          []byte
	tape              []vibejson.IndexEntry
	groupArena        []byte
	groupState        []fileIndexScalarGroupState
	indexCoverage     []uint64
	certifiedCoverage []uint64
	lastProbe         IndexProbeStats
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
	w.probe = storeio.IndexTreeProbe{}
	w.postings = nil
	w.document = nil
	w.tape = nil
	w.groupArena = nil
	w.groupState = nil
	w.indexCoverage = nil
	w.certifiedCoverage = nil
	w.lastProbe = IndexProbeStats{}
}

// Snapshot acquires an explicit generation lease.
func (c *Collection) Snapshot() (*Snapshot, error) {
	if c == nil {
		return nil, ErrClosed
	}
	// Any deferred canonical lane's primary router may be newer than its
	// sealed parent graph — buffered and journal-backed synchronous alike.
	// A snapshot's point reads and ordered scans must agree, so the pending
	// parents are materialized and the state captured under one writer hold:
	// releasing the lock between the two hands a concurrent publication a
	// window to advance the router past the root the snapshot walks. This
	// stages the cut but does not make it durable; Flush/Close keep that
	// boundary.
	if c.deferredCanonicalLane() && c.primaryRouter.Load() != nil {
		c.writer.Lock()
		if len(c.primaryPendingParents) != 0 {
			if err := c.materializePrimaryParentsLocked(); err != nil {
				c.writer.Unlock()
				return nil, err
			}
		}
		snap, err := c.pinSnapshotLocked()
		c.writer.Unlock()
		return snap, err
	}
	return c.pinSnapshot()
}

// pinSnapshot captures the reader state and acquires its lease under the
// snapshot gate alone; deferred canonical lanes use pinSnapshotLocked under
// the writer instead.
func (c *Collection) pinSnapshot() (*Snapshot, error) {
	c.snapshotGate.RLock()
	state, stateErr := c.readerFileState()
	if stateErr != nil {
		c.snapshotGate.RUnlock()
		return nil, stateErr
	}
	exact := c.primaryExact
	live := c.primaryLive
	lease, err := c.leases.Acquire(state.root.Generation)
	c.snapshotGate.RUnlock()
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		collection: c, state: state, exact: exact, live: live, lease: lease,
	}, nil
}

// pinSnapshotLocked is pinSnapshot for callers already holding the writer
// lock, which no gate writer can be inside.
func (c *Collection) pinSnapshotLocked() (*Snapshot, error) {
	return c.pinSnapshot()
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
		s.exact = nil
		s.live = nil
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

// Generation returns the pinned durable publication generation.
func (s *Snapshot) Generation() uint64 {
	if s == nil || s.state == nil {
		return 0
	}
	return s.state.root.Generation
}

// AppendRaw appends key's exact JSON spelling into dst. It never returns a
// borrowed page slice.
func (s *Snapshot) AppendRaw(dst []byte, key string) ([]byte, bool, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return dst, false, ErrClosed
	}
	return s.collection.appendRawAtState(dst, key, s.state)
}

func (c *Collection) appendRawAtState(
	dst []byte, key string, state *fileStoreState,
) ([]byte, bool, error) {
	if state.root.PrimaryRoot != (storeio.PageRef{}) {
		return c.resolvePrimaryGraph(dst, state, key)
	}
	match, ok, err := c.resolveFileFingerprint(state, []byte(key))
	if err != nil || !ok {
		return dst, false, err
	}
	if match.value.grouped || match.value.value.Overflow == (storeio.PageRef{}) {
		dst, ok = match.view.appendJSON(dst, match.value)
		match.Release()
		if !ok {
			return dst, false, storeio.ErrDocumentGroupCorrupt
		}
		return dst, true, nil
	}
	value := match.value.value
	location := match.keyLocation()
	match.Release()
	dst, err = c.appendOverflowAtState(dst, value, location, state)
	return dst, err == nil, err
}

// PrefetchKeys resolves keys through the pinned directories and submits their
// document extents to the bounded asynchronous read queue in physical order.
// It returns the number submitted; missing keys are ignored and queue pressure
// is visible through Stats.PrefetchDropped.
func (s *Snapshot) PrefetchKeys(keys []string) (int, error) {
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
	state := s.state
	chunkBounds := storeio.ChunkTreeBounds{FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID}
	for _, key := range keys {
		cursor, err := storeio.OpenPageKeyTreeCursor(
			s.collection.cache, state.keyRoot,
			storeio.KeyHash(state.root.StoreID, key),
			filePageKeyTreeBounds(state),
		)
		if err != nil {
			return queued, err
		}
		for {
			location, ok, nextErr := cursor.Next()
			if nextErr != nil {
				cursor.Close()
				return queued, nextErr
			}
			if !ok {
				break
			}
			ref, found, lookupErr := storeio.LookupChunkTree(
				s.collection.cache, state.chunkRoot, location.Chunk, chunkBounds,
			)
			if lookupErr != nil {
				cursor.Close()
				return queued, lookupErr
			}
			if !found {
				cursor.Close()
				return queued, storeio.ErrChunkDirectoryCorrupt
			}
			refs[count] = ref
			count++
			if count == len(refs) {
				if flushErr := flush(); flushErr != nil {
					cursor.Close()
					return queued, flushErr
				}
			}
		}
		cursor.Close()
	}
	return queued, flush()
}

func (c *Collection) appendOverflowAtState(
	dst []byte, value storeio.DocumentValue, location storeio.KeyLocation,
	state *fileStoreState,
) ([]byte, error) {
	ref := value.Overflow
	offset := uint64(0)
	for ref != (storeio.PageRef{}) {
		lease, err := c.cache.Acquire(ref)
		if err != nil {
			return dst, err
		}
		view, err := storeio.OpenOverflowPage(
			lease.Page(), state.super.FileEnd, state.root.NextLogicalID,
			state.root.PageSize, state.root.ChunkHighWater, uint8(state.root.ChunkDocuments),
		)
		if err != nil {
			lease.Release()
			return dst, err
		}
		header := view.Header()
		if header.Chunk != location.Chunk || header.Slot != location.Slot ||
			header.Total != value.Length || header.Offset != offset {
			lease.Release()
			return dst, storeio.ErrOverflowPageCorrupt
		}
		dst = append(dst, view.Data()...)
		offset += uint64(len(view.Data()))
		next := header.Next
		lease.Release()
		if next != (storeio.PageRef{}) {
			_, _ = c.cache.Prefetch([]storeio.PageRef{next})
		}
		ref = next
	}
	if offset != value.Length {
		return dst, storeio.ErrOverflowPageCorrupt
	}
	return dst, nil
}

// AppendRaw is the current-snapshot convenience form.
func (c *Collection) AppendRaw(dst []byte, key string) ([]byte, bool, error) {
	if c == nil {
		return dst, false, ErrClosed
	}
	c.snapshotGate.RLock()
	state, stateErr := c.readerFileState()
	if stateErr != nil {
		c.snapshotGate.RUnlock()
		return dst, false, stateErr
	}
	lease, err := c.leases.Acquire(state.root.Generation)
	c.snapshotGate.RUnlock()
	if err != nil {
		return dst, false, err
	}
	out, ok, err := c.appendRawAtState(dst, key, state)
	lease.Release()
	return out, ok, err
}

// PrefetchKeys submits current-snapshot document reads to the bounded
// asynchronous prefetch queue.
func (c *Collection) PrefetchKeys(keys []string) (int, error) {
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
	state := c.readerFileStateNoError()
	if state == nil {
		return 0
	}
	return state.root.DocumentCount
}

// Generation returns the current reader-visible generation.
func (c *Collection) Generation() uint64 {
	if c == nil {
		return 0
	}
	state := c.readerFileStateNoError()
	if state == nil {
		return 0
	}
	return state.root.Generation
}

// DurableGeneration returns the newest crash-safe generation.
func (c *Collection) DurableGeneration() uint64 {
	if c == nil || c.committer == nil {
		return 0
	}
	return c.committer.DurableGeneration()
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
	state := c.readerFileStateNoError()
	current := uint64(0)
	if state != nil {
		current = state.root.Generation
	}
	leases := c.leases.Stats(current)
	retired := c.reclaimer.Stats()
	stats := Stats{
		CapacityBytes: cache.CapacityBytes, ResidentBytes: cache.ResidentBytes,
		ReservedBytes:       cache.ReservedBytes,
		CommitCapacityBytes: c.committer.StagingCapacityBytes(),
		PinnedPages:         cache.PinnedPages, DirtyBytes: cache.DirtyBytes,
		PageReads: cache.PageReads, ReadBytes: cache.ReadBytes, CacheHits: cache.CacheHits,
		CacheMisses: cache.Misses, CoalescedReads: cache.Coalesced, ReadErrors: cache.ReadErrors,
		PrefetchHits: cache.PrefetchHits, Evictions: cache.Evictions,
		PrefetchQueued: cache.PrefetchQueued, PrefetchDropped: cache.PrefetchDropped,
		PrefetchQueueDepth: cache.QueueDepth, ReadQueueDepth: cache.ReadQueueDepth,
		AsyncReadBatches: cache.AsyncReadBatches, LargestReadBatch: cache.LargestReadBatch,
		PublishedGeneration: commit.PublishedGeneration, DurableGeneration: commit.DurableGeneration,
		CommitQueueDepth: commit.QueuedGenerations, DeviceCommits: commit.DeviceCommits,
		CommittedBatches: commit.CommittedBatches, LargestCommitGroup: commit.LargestGroup,
		SuppressedRootWrites:            commit.SuppressedRootWrites,
		SuppressedRootBytes:             commit.SuppressedRootBytes,
		SupersededRootWrites:            commit.SupersededRootWrites,
		SupersededRootBytes:             commit.SupersededRootBytes,
		SupersededPageWrites:            commit.SupersededPageWrites,
		SupersededPageBytes:             commit.SupersededPageBytes,
		TailWitnessWrites:               commit.TailWitnessWrites,
		TailWitnessBytes:                commit.TailWitnessBytes,
		PrewrittenPageWrites:            commit.PrewrittenPageWrites,
		PrewrittenPageBytes:             commit.PrewrittenPageBytes,
		AutomaticCheckpoints:            c.automaticCheckpoints.Load(),
		RetirementPressureCheckpoints:   c.retirementPressureCheckpoints.Load(),
		DeviceBytes:                     commit.DeviceBytes,
		MaterializedBatches:             commit.MaterializedBatches,
		MaterializationJournalBytes:     commit.MaterializationJournalBytes,
		MaterializationTargetBytes:      commit.MaterializationTargetBytes,
		MaterializationFullWriteBytes:   commit.MaterializationFullWriteBytes,
		MaterializationBarriers:         commit.MaterializationBarriers,
		MaterializationAttempts:         c.materializationAttempts.Load(),
		MaterializationUpdates:          c.materializationUpdates.Load(),
		MaterializationFallbacks:        c.materializationFallbacks.Load(),
		MaterializationSnapshotSkips:    c.materializationSnapshotSkips.Load(),
		MaterializationBusySkips:        c.materializationBusySkips.Load(),
		BufferedInplaceAttempts:         c.bufferedInplaceAttempts.Load(),
		BufferedInplaceUpdates:          c.bufferedInplaceUpdates.Load(),
		BufferedInplaceFallbacks:        c.bufferedInplaceFallbacks.Load(),
		BufferedFirstTouchOverflows:     c.bufferedFirstTouchOverflows.Load(),
		JournalAcks:                     c.journalAcks.Load(),
		ChainAcks:                       c.chainAcks.Load(),
		PrimaryLeafSplitRequired:        c.primaryLeafSplitRequired.Load(),
		PrimaryEmptyLeaves:              c.primaryEmptyLeaves.Load(),
		PrimaryLeafSplits:               c.primaryLeafSplits.Load(),
		PrimaryLeafMerges:               c.primaryLeafMerges.Load(),
		PrimaryLeafReclass:              c.primaryLeafReclass.Load(),
		MergeReclassEvaluations:         c.mergeReclassEvaluations.Load(),
		MergeReclassWarranted:           c.mergeReclassWarrantedHits.Load(),
		MergeReclassCommits:             c.mergeReclassCommits.Load(),
		MergeReclassAborts:              c.mergeReclassAborts.Load(),
		MergeReclassSkips:               c.mergeReclassSkips.Load(),
		PrimaryTemplateDetemplateEvents: storeio.TemplateColumnarDetemplateEvents(),
		PrimaryTemplateReplanEvents:     storeio.TemplateColumnarReplanEvents(),
		PrimaryMacroSplitRequired:       c.primaryMacroSplitRequired.Load(),
		PrimarySplitMaxNS:               c.primarySplitMaxNS.Load(),
		PrimaryMergeMaxNS:               c.primaryMergeMaxNS.Load(),
		PrimaryReclassMaxNS:             c.primaryReclassMaxNS.Load(),
		PrimaryMutationScratchBytes: uint64(
			len(c.primaryLeafScratch) + len(c.primaryRootScratch),
		),
		AutomaticMutationGroups:       c.automaticMutationGroups.Load(),
		AutomaticMutationRequests:     c.automaticMutationRequests.Load(),
		AutomaticMutationWaits:        c.automaticMutationWaits.Load(),
		AutomaticMutationQueueHigh:    c.automaticMutationQueueHigh.Load(),
		AutomaticMutationBytesHigh:    c.automaticMutationBytesHigh.Load(),
		LargestAutomaticMutationGroup: c.automaticMutationGroupHigh.Load(),
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
		Float64ScratchBytes: uint64(len(c.float64Masks))*8 + uint64(len(c.float64Values))*8,
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
		stats.DocumentCount = state.root.DocumentCount
		stats.LiveChunks = state.root.LiveChunks
		stats.ChunkHighWater = state.root.ChunkHighWater
		stats.ChunkSlots = uint64(state.root.LiveChunks) * uint64(state.root.ChunkDocuments)
		if state.root.PrimaryRoot == (storeio.PageRef{}) {
			stats.VacantChunkSlots = stats.ChunkSlots - state.root.DocumentCount
		}
		stats.FileEnd = state.super.FileEnd
	}
	return stats
}

// Put validates and copies src, then atomically publishes a copy-on-write file
// generation. created reports whether key was absent. DurabilityAsyncVisible
// and DurabilityBufferedVisible return after bounded admission; DurabilitySync
// waits for the double-root durability fence. Buffered-visible admission does
// not issue device writes; Flush and Close checkpoint explicitly, cache
// pressure may checkpoint before admission, and exhausted committer staging
// returns ErrCheckpointRequired.
func (c *Collection) Put(key string, src []byte) (created bool, err error) {
	if c == nil {
		return false, ErrClosed
	}
	if c.primaryGraphReadOnly() {
		return c.putPrimaryWithSplit(key, src)
	}
	writerAcquired := false
	if c.combiner != nil {
		writerAcquired = !c.combiner.active.Load() && c.writer.TryLock()
		if writerAcquired && c.combiner.active.Load() {
			c.writer.Unlock()
			writerAcquired = false
		}
		if !writerAcquired {
			if len(key) > c.options.MaxKeyBytes {
				return false, ErrKeyTooLarge
			}
			if len(src) > c.options.MaxDocumentBytes {
				return false, ErrDocumentTooLarge
			}
			if err := vibejson.Validate(src); err != nil {
				return false, err
			}
			return c.combineFileMutation(key, src, false)
		}
	}
	if !writerAcquired {
		c.writer.Lock()
	}
	var generation uint64
	defer func() {
		wait := generation != 0 && c.synchronous()
		if wait {
			c.durabilityWait.Add(1)
		}
		c.writer.Unlock()
		if wait {
			err = errors.Join(err, c.waitPublished(generation))
			c.durabilityWait.Done()
		}
	}()
	if c.closed {
		return false, ErrClosed
	}
	if failure := c.PersistenceError(); failure != nil {
		return false, failure
	}
	if len(key) > c.options.MaxKeyBytes {
		return false, ErrKeyTooLarge
	}
	if len(src) > c.options.MaxDocumentBytes {
		return false, ErrDocumentTooLarge
	}
	index, err := c.validateDocument(src)
	if err != nil {
		return false, err
	}
	state := c.state.Load()
	if state == nil {
		return false, ErrClosed
	}
	c.pointKeyScratch = append(c.pointKeyScratch[:0], key...)
	keyBytes := c.pointKeyScratch
	var match fileFingerprintMatch
	var location storeio.KeyLocation
	found := false
	if state.keyRoot != (storeio.PageRef{}) {
		match, found, err = c.resolveFileFingerprint(state, keyBytes)
		if err != nil {
			return false, err
		}
		defer match.Release()
		if found {
			location = match.keyLocation()
		}
	}
	created = !found
	trackFirstTouch := false
	prospectiveHighWater := state.root.ChunkHighWater
	if !found {
		location, err = c.findFileInsertSlot(state)
		if err != nil {
			return false, err
		}
		if location.Chunk == state.root.ChunkHighWater {
			prospectiveHighWater++
		}
	}
	if found {
		materialized, materializeErr := c.tryMaterializeFileUpdate(
			state, keyBytes, src, index, location, &match,
		)
		if materializeErr != nil {
			return false, materializeErr
		}
		if materialized {
			generation = state.root.Generation + 1
			return false, nil
		}
		var inplace bool
		var inplaceErr error
		inplace, trackFirstTouch, inplaceErr = c.tryBufferedFileInplace(
			state, src, &match,
		)
		if inplaceErr != nil {
			return false, inplaceErr
		}
		if inplace {
			generation = state.root.Generation + 1
			return false, nil
		}
		// Canonical replacement must release its resolving cache pin before
		// checking exclusive-frame eligibility. A snapshot or competing cache
		// pin can still force a late COW fallback after that release. Re-resolve
		// here so putLocked never consumes the cleared borrowed view.
		if match.documentRef == (storeio.PageRef{}) {
			match, found, err = c.resolveFileFingerprint(state, keyBytes)
			if err != nil {
				return false, err
			}
			if !found {
				return false, storeio.ErrKeyDirectoryCorrupt
			}
			location = match.keyLocation()
		}
	}
	if err := c.ensureDirtyCapacityFor(
		c.options.singleDocumentTransactionPages,
		c.options.singleDocumentTransactionBytes,
	); err != nil {
		return false, err
	}
	var cowDocumentRef storeio.PageRef
	created, err = c.putLocked(
		state, keyBytes, src, index, location, created, prospectiveHighWater, &match,
		&cowDocumentRef,
	)
	if err == nil {
		if trackFirstTouch {
			c.rememberBufferedFirstTouch(cowDocumentRef)
		}
		generation = state.root.Generation + 1
	}
	return created, err
}

// findFileInsertSlot resolves the first reusable stable slot at or above the
// persistent writer-only hint. Point reads never consult this evidence: it is
// carried in the already-published state root and paid for only by an insert.
//
// Delete lowers the hint in O(1). An insert advances past full chunks and
// leaves the first partially occupied chunk as the next hint, so a delete-heavy
// store fills old holes instead of extending the chunk high-water forever.
func (c *Collection) findFileInsertSlot(state *fileStoreState) (storeio.KeyLocation, error) {
	limit := fileStoreLiveMask(state.root.ChunkDocuments)
	for chunk := state.root.FreeChunkHint; chunk < state.root.ChunkHighWater; chunk++ {
		_, view, leases, err := c.loadFileChunk(state, chunk)
		if err != nil {
			return storeio.KeyLocation{}, err
		}
		live := uint64(0)
		if view != nil {
			live = view.live()
		}
		if leases != nil {
			leases.Release()
		}
		free := ^live & limit
		if free != 0 {
			return storeio.KeyLocation{
				Chunk: chunk, Slot: uint8(bits.TrailingZeros64(free)),
			}, nil
		}
	}
	if state.root.ChunkHighWater == ^uint32(0) {
		return storeio.KeyLocation{}, store.ErrTooLarge
	}
	return storeio.KeyLocation{Chunk: state.root.ChunkHighWater}, nil
}

func (c *Collection) putLocked(
	state *fileStoreState,
	key, src []byte,
	newIndex vibejson.Index,
	location storeio.KeyLocation,
	created bool,
	prospectiveHighWater uint32,
	resolved *fileFingerprintMatch,
	publishedDocumentRef *storeio.PageRef,
) (bool, error) {
	generation := state.root.Generation + 1
	if generation == 0 {
		return false, storeio.ErrGenerationOrder
	}
	if err := c.refreshReusableFor(
		state,
		c.options.singleDocumentTransactionPages,
		c.options.singleDocumentFreeFoldLimit,
	); err != nil {
		return false, fmt.Errorf("vibejson: refresh reusable extents: %w", err)
	}
	tx, err := c.beginWriteTransaction(c.options.singleDocumentTransactionPages, storeio.WriteTransactionOptions{
		StoreID: c.storeID, Generation: generation, PageSize: uint32(c.options.PageSize),
		FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
		Reusable: c.reusable, ReuseJournal: c.reuseJournal,
		ReusableIndex:    &c.freeExtentIndex,
		ReusablePromoter: c.reusableExtentPromoter(),
	})
	if err != nil {
		return false, err
	}
	abort := true
	retirementReserved := false
	defer func() {
		if abort {
			if retirementReserved {
				_ = c.reclaimer.CancelRetiredGeneration(state.root.Generation)
			}
			_ = tx.Abort()
		}
	}()
	c.retireScratch = c.retireScratch[:0]
	c.retireRefScratch = c.retireRefScratch[:0]

	var oldRef storeio.PageRef
	var oldView *fileDocumentChunk
	if !created && resolved != nil && resolved.documentRef != (storeio.PageRef{}) {
		if err := resolved.attachColumns(c); err != nil {
			return false, err
		}
		oldRef, oldView = resolved.documentRef, &resolved.view
	} else {
		var oldLease *fileDocumentLeases
		oldRef, oldView, oldLease, err = c.loadFileChunk(state, location.Chunk)
		if err != nil {
			return false, err
		}
		if oldLease != nil {
			defer oldLease.Release()
		}
	}
	var oldIndex vibejson.Index
	hasOldIndex := false
	if created {
		if oldView != nil {
			if _, occupied := oldView.lookup(location.Slot); occupied {
				return false, storeio.ErrDocumentPageCorrupt
			}
		}
	} else {
		if oldView == nil {
			return false, storeio.ErrDocumentPageCorrupt
		}
		oldValue, ok := oldView.lookupKey(location.Slot, key)
		if !ok {
			return false, storeio.ErrDocumentPageCorrupt
		}
		if !oldValue.grouped {
			if err := c.appendOverflowRetirements(state, oldValue.value, location); err != nil {
				return false, err
			}
		}
		if len(c.options.indexes) != 0 ||
			len(c.options.float64Columns) != 0 {
			raw, valueErr := c.appendFileDocumentValue(
				c.indexValueScratch[:0], state, *oldView, oldValue, location,
			)
			if valueErr != nil {
				return false, valueErr
			}
			c.indexValueScratch = raw
			oldIndex, err = c.buildOldFileIndex(raw)
			if err != nil {
				return false, err
			}
			hasOldIndex = true
		}
	}
	newRecord, err := c.stageFileValue(tx, location, key, src)
	if err != nil {
		return false, err
	}
	c.pointChunkEdit[0] = fileChunkEdit{
		slot: location.Slot, record: newRecord, keep: true,
	}
	zoneEdits := c.pointChunkEdit[:]
	rows, live, err := c.buildFileRows(state, oldView, zoneEdits)
	if err != nil {
		return false, err
	}
	freeChunkHint := state.root.FreeChunkHint
	if created {
		freeChunkHint = location.Chunk
		if live == fileStoreLiveMask(state.root.ChunkDocuments) {
			freeChunkHint++
		}
	}
	columns, err := c.buildFileFloat64Columns(state, oldView, location.Slot, &newIndex, true)
	if err != nil {
		return false, err
	}
	documentSize, err := c.fileDocumentPageSize(rows, columns)
	if err != nil {
		return false, err
	}
	documentLogicalID := uint64(0)
	if oldRef.Kind == storeio.PageDocument {
		documentLogicalID = oldRef.LogicalID
	}
	documentPage, err := tx.Allocate(storeio.PageDocument, documentSize, documentLogicalID)
	if err != nil {
		return false, err
	}
	if _, err := storeio.EncodeDocumentPageWithColumns(documentPage.Bytes(), storeio.DocumentPageHeader{
		StoreID: c.storeID, Generation: generation, LogicalID: documentPage.Ref().LogicalID,
		PageSize: documentPage.Ref().Length, ChunkID: location.Chunk, Live: live,
	}, rows, columns, tx.NextLogicalID(), tx.FileEnd(), uint32(c.options.PageSize)); err != nil {
		return false, err
	}
	if err := documentPage.Stage(); err != nil {
		return false, err
	}
	chunkMutation, err := storeio.UpsertChunkTreeZone(
		c.cache, tx, state.chunkRoot, location.Chunk, documentPage.Ref(),
		c.zoneMerger(rows, zoneEdits, zonePriorDocs(oldView)),
		storeio.ChunkTreeBounds{
			FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
		})
	if err != nil {
		return false, err
	}
	keyRoot := state.keyRoot
	var keyMutation storeio.PageKeyTreeMutation
	if created {
		keyMutation, err = storeio.InsertPageKeyTree(
			c.cache, tx, state.keyRoot, filePageKeyLocation(c.storeID, key, location),
			storeio.PageKeyTreeBounds{
				FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
				ChunkHighWater: prospectiveHighWater, ChunkDocuments: state.root.ChunkDocuments,
			})
		if err != nil || keyMutation.Found {
			if err == nil {
				err = storeio.ErrKeyDirectoryCorrupt
			}
			return false, err
		}
		keyRoot = keyMutation.Root
	}
	var oldIndexPointer *vibejson.Index
	if hasOldIndex {
		oldIndexPointer = &oldIndex
	}
	indexRoot, err := c.updateFileIndexes(tx, state, location, oldIndexPointer, &newIndex)
	if err != nil {
		return false, err
	}
	documentCount := state.root.DocumentCount
	liveChunks := state.root.LiveChunks
	if created {
		documentCount++
		if oldRef == (storeio.PageRef{}) {
			liveChunks++
		}
	}
	indexGroupHead, retireIndexGroup, err := c.maintainFileIndexGroups(
		tx, state, location, oldIndexPointer, &newIndex,
		documentCount, prospectiveHighWater,
	)
	if err != nil {
		return false, err
	}
	float64ScanHead, retireFloat64Scan, err :=
		c.maintainFileFloat64Scan(
			tx, state, chunkMutation.Root, location,
			oldIndexPointer, &newIndex, created,
		)
	if err != nil {
		return false, err
	}
	if err := c.collectFileRetirements(
		state, oldRef, oldView, keyMutation, chunkMutation,
		retireFloat64Scan, retireIndexGroup,
	); err != nil {
		return false, fmt.Errorf("vibejson: collect retired extents: %w", err)
	}
	freeLog, err := c.syncFreeLogFor(
		tx, state, c.options.singleDocumentFreeFoldLimit,
	)
	if err != nil {
		return false, fmt.Errorf("vibejson: persist reusable extents: %w", err)
	}
	nextState, nextInline, err := c.stageFileState(
		tx, state, generation, prospectiveHighWater, freeChunkHint,
		documentCount,
		liveChunks, chunkMutation.Root, keyRoot, indexRoot,
		float64ScanHead, indexGroupHead, freeLog.head, freeLog.checksum,
		freeLog.inline,
	)
	if err != nil {
		return false, err
	}
	if err := c.reserveFileRetirements(); err != nil {
		return false, fmt.Errorf("vibejson: reserve retired extents: %w", err)
	}
	retirementReserved = true
	if err := c.publishStagedFileMutation(
		tx, nextState, nextInline, freeLog,
	); err != nil {
		return false, err
	}
	abort = false
	if publishedDocumentRef != nil {
		*publishedDocumentRef = documentPage.Ref()
	}
	if location.Chunk >= state.root.ChunkHighWater || location.Chunk == c.appendChunk {
		c.appendChunk = location.Chunk
		c.appendLive = live
	}
	if live == fileStoreLiveMask(state.root.ChunkDocuments) {
		c.appendChunk = prospectiveHighWater
		c.appendLive = 0
	}
	return created, nil
}

// Delete removes key through the same failure-atomic page publication.
func (c *Collection) Delete(key string) (deleted bool, err error) {
	if c == nil {
		return false, ErrClosed
	}
	if c.primaryGraphReadOnly() {
		return c.deletePrimaryWithMerge(key)
	}
	writerAcquired := false
	if c.combiner != nil {
		writerAcquired = !c.combiner.active.Load() && c.writer.TryLock()
		if writerAcquired && c.combiner.active.Load() {
			c.writer.Unlock()
			writerAcquired = false
		}
		if !writerAcquired {
			if len(key) > c.options.MaxKeyBytes {
				return false, ErrKeyTooLarge
			}
			return c.combineFileMutation(key, nil, true)
		}
	}
	if !writerAcquired {
		c.writer.Lock()
	}
	var generation uint64
	defer func() {
		wait := generation != 0 && c.synchronous()
		if wait {
			c.durabilityWait.Add(1)
		}
		c.writer.Unlock()
		if wait {
			err = errors.Join(err, c.waitPublished(generation))
			c.durabilityWait.Done()
		}
	}()
	if c.closed {
		return false, ErrClosed
	}
	if failure := c.PersistenceError(); failure != nil {
		return false, failure
	}
	if len(key) > c.options.MaxKeyBytes {
		return false, ErrKeyTooLarge
	}
	state := c.state.Load()
	if state == nil || state.keyRoot == (storeio.PageRef{}) {
		return false, nil
	}
	keyBytes := []byte(key)
	match, found, err := c.resolveFileFingerprint(state, keyBytes)
	if err != nil || !found {
		return false, err
	}
	defer match.Release()
	if err := c.ensureDirtyCapacityFor(
		c.options.singleDocumentTransactionPages,
		c.options.singleDocumentTransactionBytes,
	); err != nil {
		return false, err
	}
	deleted, err = c.deleteLocked(state, keyBytes, match.keyLocation(), match.location, &match)
	if err == nil && deleted {
		generation = state.root.Generation + 1
	}
	return deleted, err
}

func (c *Collection) deleteLocked(
	state *fileStoreState,
	key []byte,
	location storeio.KeyLocation,
	fingerprint storeio.PageKeyLocation,
	resolved *fileFingerprintMatch,
) (bool, error) {
	generation := state.root.Generation + 1
	if err := c.refreshReusableFor(
		state,
		c.options.singleDocumentTransactionPages,
		c.options.singleDocumentFreeFoldLimit,
	); err != nil {
		return false, err
	}
	tx, err := c.beginWriteTransaction(c.options.singleDocumentTransactionPages, storeio.WriteTransactionOptions{
		StoreID: c.storeID, Generation: generation, PageSize: uint32(c.options.PageSize),
		FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
		Reusable: c.reusable, ReuseJournal: c.reuseJournal,
		ReusableIndex:    &c.freeExtentIndex,
		ReusablePromoter: c.reusableExtentPromoter(),
	})
	if err != nil {
		return false, err
	}
	abort := true
	retirementReserved := false
	defer func() {
		if abort {
			if retirementReserved {
				_ = c.reclaimer.CancelRetiredGeneration(state.root.Generation)
			}
			_ = tx.Abort()
		}
	}()
	c.retireScratch = c.retireScratch[:0]
	c.retireRefScratch = c.retireRefScratch[:0]
	var oldRef storeio.PageRef
	var oldView *fileDocumentChunk
	if resolved != nil && resolved.documentRef != (storeio.PageRef{}) {
		if err := resolved.attachColumns(c); err != nil {
			return false, err
		}
		oldRef, oldView = resolved.documentRef, &resolved.view
	} else {
		var oldLease *fileDocumentLeases
		oldRef, oldView, oldLease, err = c.loadFileChunk(state, location.Chunk)
		if err != nil || oldView == nil {
			return false, err
		}
		defer oldLease.Release()
	}
	if oldView == nil {
		return false, storeio.ErrDocumentPageCorrupt
	}
	oldValue, ok := oldView.lookupKey(location.Slot, key)
	if !ok {
		return false, storeio.ErrDocumentPageCorrupt
	}
	if !oldValue.grouped {
		if err := c.appendOverflowRetirements(state, oldValue.value, location); err != nil {
			return false, err
		}
	}
	var oldIndex vibejson.Index
	if len(c.options.indexes) != 0 ||
		len(c.options.float64Columns) != 0 {
		raw, valueErr := c.appendFileDocumentValue(
			c.indexValueScratch[:0], state, *oldView, oldValue, location,
		)
		if valueErr != nil {
			return false, valueErr
		}
		c.indexValueScratch = raw
		oldIndex, err = c.buildOldFileIndex(raw)
		if err != nil {
			return false, err
		}
	}
	deleteEdits := [1]fileChunkEdit{{slot: location.Slot}}
	rows, live, err := c.buildFileRows(state, oldView, deleteEdits[:])
	if err != nil {
		return false, err
	}
	var chunkMutation storeio.ChunkTreeMutation
	if live == 0 {
		chunkMutation, err = storeio.DeleteChunkTree(c.cache, tx, state.chunkRoot, location.Chunk, storeio.ChunkTreeBounds{
			FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
		})
	} else {
		columns, coverErr := c.buildFileFloat64Columns(state, oldView, location.Slot, nil, false)
		if coverErr != nil {
			return false, coverErr
		}
		documentSize, sizeErr := c.fileDocumentPageSize(rows, columns)
		if sizeErr != nil {
			return false, sizeErr
		}
		documentLogicalID := uint64(0)
		if oldRef.Kind == storeio.PageDocument {
			documentLogicalID = oldRef.LogicalID
		}
		documentPage, allocateErr := tx.Allocate(storeio.PageDocument, documentSize, documentLogicalID)
		if allocateErr != nil {
			return false, allocateErr
		}
		if _, encodeErr := storeio.EncodeDocumentPageWithColumns(documentPage.Bytes(), storeio.DocumentPageHeader{
			StoreID: c.storeID, Generation: generation, LogicalID: documentPage.Ref().LogicalID,
			PageSize: documentPage.Ref().Length, ChunkID: location.Chunk, Live: live,
		}, rows, columns, tx.NextLogicalID(), tx.FileEnd(), uint32(c.options.PageSize)); encodeErr != nil {
			return false, encodeErr
		}
		if stageErr := documentPage.Stage(); stageErr != nil {
			return false, stageErr
		}
		// A delete folds nothing: the surviving documents are already covered by
		// the carried-forward summary, and merge-only bounds do not narrow when
		// a row leaves. The merger is still handed over so a stale summary gets
		// rebuilt out of the rows this commit is publishing anyway.
		chunkMutation, err = storeio.UpsertChunkTreeZone(
			c.cache, tx, state.chunkRoot, location.Chunk, documentPage.Ref(),
			c.zoneMerger(rows, nil, zonePriorDocs(oldView)),
			storeio.ChunkTreeBounds{
				FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
			})
	}
	if err != nil {
		return false, err
	}
	chunkRoot := chunkMutation.Root
	keyMutation, err := storeio.DeletePageKeyTree(
		c.cache, tx, state.keyRoot, fingerprint, storeio.PageKeyTreeBounds{
			FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
			ChunkHighWater: state.root.ChunkHighWater, ChunkDocuments: state.root.ChunkDocuments,
		})
	if err != nil || !keyMutation.Found || !keyMutation.Changed {
		if err == nil {
			err = storeio.ErrKeyDirectoryCorrupt
		}
		return false, err
	}
	indexRoot, err := c.updateFileIndexes(tx, state, location, &oldIndex, nil)
	if err != nil {
		return false, err
	}
	liveChunks := state.root.LiveChunks
	if live == 0 {
		liveChunks--
	}
	float64ScanHead, retireFloat64Scan, err :=
		c.maintainFileFloat64Scan(
			tx, state, chunkRoot, location, &oldIndex, nil, false,
		)
	if err != nil {
		return false, err
	}
	indexGroupHead, retireIndexGroup, err := c.maintainFileIndexGroups(
		tx, state, location, &oldIndex, nil,
		state.root.DocumentCount-1, state.root.ChunkHighWater,
	)
	if err != nil {
		return false, err
	}
	if err := c.collectFileRetirements(
		state, oldRef, oldView, keyMutation, chunkMutation,
		retireFloat64Scan, retireIndexGroup,
	); err != nil {
		return false, err
	}
	freeLog, err := c.syncFreeLogFor(
		tx, state, c.options.singleDocumentFreeFoldLimit,
	)
	if err != nil {
		return false, err
	}
	nextState, nextInline, err := c.stageFileState(
		tx, state, generation, state.root.ChunkHighWater,
		min(state.root.FreeChunkHint, location.Chunk),
		state.root.DocumentCount-1, liveChunks,
		chunkRoot, keyMutation.Root, indexRoot,
		float64ScanHead, indexGroupHead, freeLog.head, freeLog.checksum,
		freeLog.inline,
	)
	if err != nil {
		return false, err
	}
	if err := c.reserveFileRetirements(); err != nil {
		return false, err
	}
	retirementReserved = true
	if err := c.publishStagedFileMutation(
		tx, nextState, nextInline, freeLog,
	); err != nil {
		return false, err
	}
	abort = false
	if location.Chunk == c.appendChunk {
		c.appendLive = live
	}
	return true, nil
}

func (c *Collection) validateDocument(src []byte) (vibejson.Index, error) {
	estimate := max(len(src)/8+8, 8)
	if cap(c.parseScratch) < estimate {
		c.parseScratch = make([]vibejson.IndexEntry, estimate)
	}
	for {
		index, err := vibejson.BuildIndexOptions(src, c.parseScratch[:cap(c.parseScratch)], c.options.Collection.IndexOptions)
		if err != document.ErrIndexFull {
			if err != nil {
				return index, err
			}
			if schema := c.options.Collection.Schema; schema != nil {
				if schemaErr := schema.ValidateIndex(index); schemaErr != nil {
					return vibejson.Index{}, schemaErr
				}
			}
			return index, nil
		}
		if cap(c.parseScratch) > c.options.MaxDocumentBytes {
			return vibejson.Index{}, ErrDocumentTooLarge
		}
		c.parseScratch = make([]vibejson.IndexEntry, cap(c.parseScratch)*2)
	}
}

// ensureDirtyCapacity fences prior generations when either the frame arena or
// a manual committer's staging pools cannot hold one more worst-case
// transaction. Both checks are lock-free capacity hints under the serialized
// writer; Begin remains the authoritative backstop against unexpected races.
// Asking the cache directly avoids a full Stats snapshot, which walks every
// frame under its lock to build counters this check does not read and made a
// bound that is O(1) by construction cost O(cache size) per Put.
func (c *Collection) ensureDirtyCapacity() error {
	return c.ensureDirtyCapacityFor(
		c.options.maxTransactionPages, c.options.maxTransactionBytes,
	)
}

func (c *Collection) ensureDirtyCapacityFor(
	transactionPages int, transactionBytes uint64,
) error {
	required := transactionBytes
	if c.cache.DirtyCapacityAvailable() >= required &&
		!c.committer.NeedsFrameCheckpointFor(transactionPages) {
		return nil
	}
	var err error
	if c.buffered() {
		err = c.checkpointBufferedLocked()
	} else {
		err = c.committer.Flush()
	}
	if err != nil {
		return err
	}
	c.automaticCheckpoints.Add(1)
	if !c.buffered() {
		c.cache.MarkDurable(c.committer.DurableGeneration())
	}
	return nil
}

// overflowPageSize returns the smallest allocation-quantum multiple holding
// one overflow piece. A full piece lands exactly on MaxPageSize, so the
// multi-page path is unchanged; only the final piece, which is the whole value
// for anything under one maximum-size page, shrinks.
//
// A smaller extent stays within the reader's contract: validateOverflowPage
// requires a whole allocation-quantum multiple that holds the piece, and every
// page records its own size in its header, so pieces of one value need not
// agree.
func (c *Collection) overflowPageSize(piece int) uint32 {
	needed := storeio.PageHeaderSize + storeio.PageTrailerSize +
		storeio.OverflowPagePayloadHeaderSize + piece
	quantum := c.options.PageSize
	size := needed / quantum * quantum
	if needed%quantum != 0 {
		size += quantum
	}
	return uint32(size)
}

func (c *Collection) stageFileValue(tx *storeio.WriteTransaction, location storeio.KeyLocation, key, src []byte) (storeio.DocumentRecord, error) {
	record := storeio.DocumentRecord{Key: key, Slot: location.Slot}
	if len(src) <= c.options.InlineValueBytes {
		record.JSON = src
		return record, nil
	}
	payloadBytes := c.options.MaxPageSize - storeio.PageHeaderSize - storeio.PageTrailerSize - storeio.OverflowPagePayloadHeaderSize
	pageCount := (len(src) + payloadBytes - 1) / payloadBytes
	pages := make([]storeio.TransactionPage, pageCount)
	for i := range pages {
		// Each piece gets the smallest extent that holds it rather than
		// MaxPageSize. Only a full piece needs MaxPageSize; the final piece of
		// any value — and the only piece of every value just past
		// InlineValueBytes — is usually far smaller. Allocating the maximum
		// unconditionally cost a 64 KiB extent for a 513-byte document under
		// the default options, a 128x amplification on exactly the document
		// sizes that overflow first.
		piece := min(payloadBytes, len(src)-i*payloadBytes)
		page, err := tx.Allocate(storeio.PageOverflow, c.overflowPageSize(piece), 0)
		if err != nil {
			return storeio.DocumentRecord{}, err
		}
		pages[i] = page
	}
	position := 0
	for i, page := range pages {
		end := min(position+payloadBytes, len(src))
		var next storeio.PageRef
		if i+1 < len(pages) {
			next = pages[i+1].Ref()
		}
		header := storeio.OverflowPageHeader{
			StoreID: c.storeID, Generation: tx.Generation(), LogicalID: page.Ref().LogicalID,
			PageSize: page.Ref().Length, Chunk: location.Chunk, Slot: location.Slot,
			Total: uint64(len(src)), Offset: uint64(position), Next: next,
		}
		if _, err := storeio.EncodeOverflowPage(
			page.Bytes(), header, src[position:end], tx.FileEnd(), tx.NextLogicalID(),
			uint32(c.options.PageSize), location.Chunk+1, uint8(c.options.Collection.ChunkDocuments),
		); err != nil {
			return storeio.DocumentRecord{}, err
		}
		if err := page.Stage(); err != nil {
			return storeio.DocumentRecord{}, err
		}
		position = end
	}
	record.Overflow = pages[0].Ref()
	record.JSONLength = uint64(len(src))
	return record, nil
}

type fileDocumentLeases struct {
	document storeio.PageLease
	columns  storeio.PageLease
	detached bool
}

func (l *fileDocumentLeases) Release() {
	if l == nil {
		return
	}
	if l.detached {
		l.columns.Release()
	}
	l.document.Release()
}

func (c *Collection) loadFileChunk(state *fileStoreState, chunkID uint32) (storeio.PageRef, *fileDocumentChunk, *fileDocumentLeases, error) {
	ref, _, view, leases, err := c.loadFileChunkZone(state, chunkID)
	return ref, view, leases, err
}

func (c *Collection) loadFileChunkZone(
	state *fileStoreState,
	chunkID uint32,
) (
	storeio.PageRef,
	storeio.ChunkZone,
	*fileDocumentChunk,
	*fileDocumentLeases,
	error,
) {
	var zone storeio.ChunkZone
	if chunkID >= state.root.ChunkHighWater || state.chunkRoot == (storeio.PageRef{}) {
		return storeio.PageRef{}, zone, nil, nil, nil
	}
	ref, zone, ok, err := storeio.LookupChunkTreeDocumentZone(
		c.cache, state.chunkRoot, chunkID, storeio.ChunkTreeBounds{
			FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
		},
	)
	if err != nil || !ok {
		return storeio.PageRef{}, zone, nil, nil, err
	}
	lease, err := c.cache.Acquire(ref)
	if err != nil {
		return storeio.PageRef{}, zone, nil, nil, err
	}
	view, err := admittedFileDocumentChunk(lease.Page(), ref, chunkID)
	if err != nil {
		lease.Release()
		return storeio.PageRef{}, zone, nil, nil, err
	}
	leases := fileDocumentLeases{document: lease}
	columnsRef, detached, err := storeio.DocumentGroupFloat64Sidecar(
		ref, uint32(c.options.PageSize),
	)
	if err != nil {
		leases.Release()
		return storeio.PageRef{}, zone, nil, nil, err
	}
	if detached {
		columns, acquireErr := c.cache.Acquire(columnsRef)
		if acquireErr != nil {
			leases.Release()
			return storeio.PageRef{}, zone, nil, nil, acquireErr
		}
		leases.columns = columns
		leases.detached = true
		if attachErr := view.attachFloat64Group(columns.Page()); attachErr != nil {
			leases.Release()
			return storeio.PageRef{}, zone, nil, nil, attachErr
		}
	}
	return ref, zone, &view, &leases, nil
}

// fileChunkEdit is one slot of a chunk rebuild. A batch supplies several, in
// strictly ascending slot order; keep false removes the slot.
type fileChunkEdit struct {
	record storeio.DocumentRecord
	slot   uint8
	keep   bool
}

// buildFileRows materialises the replacement row set for one chunk page,
// applying every edit that lands in it in a single pass.
//
// Taking a slice of edits rather than one target slot is what lets a batched
// commit rebuild a 64-document page once instead of once per document. The
// per-slot work is unchanged; only the number of times the page is rebuilt is.
func (c *Collection) buildFileRows(state *fileStoreState, old *fileDocumentChunk, edits []fileChunkEdit) ([]storeio.DocumentRecord, uint64, error) {
	if cap(c.rowScratch) < store.MaxChunkDocuments {
		c.rowScratch = make([]storeio.DocumentRecord, store.MaxChunkDocuments)
	}
	storage := c.rowScratch[:store.MaxChunkDocuments]
	c.documentValueScratch = c.documentValueScratch[:0]
	position := 0
	var live uint64
	at := 0
	for slot := uint8(0); slot < uint8(c.options.Collection.ChunkDocuments); slot++ {
		if at < len(edits) && edits[at].slot == slot {
			edit := edits[at]
			at++
			if edit.keep {
				storage[position] = edit.record
				position++
				live |= uint64(1) << slot
				continue
			}
			if old != nil {
				if _, existed := old.lookup(slot); !existed {
					return nil, 0, storeio.ErrDocumentPageCorrupt
				}
			}
			continue
		}
		if old == nil {
			continue
		}
		record, ok := old.lookup(slot)
		if !ok {
			continue
		}
		json := record.value.value.Inline
		if record.value.grouped {
			var appendErr error
			start := len(c.documentValueScratch)
			c.documentValueScratch, appendErr = c.appendFileDocumentValue(
				c.documentValueScratch, state, *old, record.value,
				storeio.KeyLocation{Chunk: old.chunk, Slot: slot},
			)
			if appendErr != nil {
				return nil, 0, appendErr
			}
			json = c.documentValueScratch[start:]
		}
		stored := storeio.DocumentRecord{
			Key: record.key, JSON: json, Overflow: record.value.value.Overflow, Slot: slot,
		}
		if stored.Overflow != (storeio.PageRef{}) {
			stored.JSONLength = record.value.value.Length
		}
		storage[position] = stored
		position++
		live |= uint64(1) << slot
	}
	if at != len(edits) {
		return nil, 0, storeio.ErrDocumentPageCorrupt
	}
	return storage[:position], live, nil
}

// seedFileFloat64Columns loads a chunk's surviving typed covering values into
// the writer's fixed column scratch, dropping every slot the caller is about to
// rewrite. Callers then record each replacement with setFileFloat64Column.
//
// Seeding and recording are separate steps so that a batch never has to hold
// every replacement document's parsed index alive at once: each document's
// projection is extracted while its index is still on the stack and discarded
// immediately, which keeps a 64-document batch's peak memory the same as a
// single Put's.
func (c *Collection) seedFileFloat64Columns(state *fileStoreState, old *fileDocumentChunk, edited uint64) error {
	if state == nil || state.root.Options&storeio.StateOptionFloat64Columns == 0 {
		return nil
	}
	if len(c.float64Masks) != len(c.options.float64Columns) ||
		len(c.float64Values) != len(c.options.float64Columns)*64 {
		return storeio.ErrDocumentPageCorrupt
	}
	clear(c.float64Masks)
	if old == nil {
		return nil
	}
	if old.float64ColumnCount() != len(c.options.float64Columns) {
		return storeio.ErrDocumentPageCorrupt
	}
	for column := range c.options.float64Columns {
		view, ok := old.float64Column(column)
		if !ok {
			return storeio.ErrDocumentPageCorrupt
		}
		iterator := view.Iterator()
		for {
			slot, value, present := iterator.Next()
			if !present {
				break
			}
			if edited&(uint64(1)<<slot) != 0 {
				continue
			}
			c.float64Masks[column] |= uint64(1) << slot
			c.float64Values[column*64+int(slot)] = value
		}
	}
	return nil
}

// setFileFloat64Column records one replacement document's typed projection into
// the seeded column scratch.
func (c *Collection) setFileFloat64Column(state *fileStoreState, slot uint8, replacement *vibejson.Index) error {
	if state == nil || state.root.Options&storeio.StateOptionFloat64Columns == 0 {
		return nil
	}
	if replacement == nil {
		return storeio.ErrDocumentPageCorrupt
	}
	for column, definition := range c.options.float64Columns {
		node, ok, err := replacement.PointerCompiled(definition.pointer)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		value, ok := node.Raw().Float64()
		if !ok || math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		c.float64Masks[column] |= uint64(1) << slot
		c.float64Values[column*64+int(slot)] = value
	}
	return nil
}

// fileFloat64Columns exposes the seeded scratch as the encoder's column input.
func (c *Collection) fileFloat64Columns(state *fileStoreState) storeio.DocumentFloat64Columns {
	if state == nil || state.root.Options&storeio.StateOptionFloat64Columns == 0 {
		return storeio.DocumentFloat64Columns{}
	}
	return storeio.DocumentFloat64Columns{Masks: c.float64Masks, Values: c.float64Values}
}

func (c *Collection) buildFileFloat64Columns(state *fileStoreState, old *fileDocumentChunk, target uint8, replacement *vibejson.Index, keep bool) (storeio.DocumentFloat64Columns, error) {
	if err := c.seedFileFloat64Columns(state, old, uint64(1)<<target); err != nil {
		return storeio.DocumentFloat64Columns{}, err
	}
	if keep {
		if err := c.setFileFloat64Column(state, target, replacement); err != nil {
			return storeio.DocumentFloat64Columns{}, err
		}
	}
	return c.fileFloat64Columns(state), nil
}

func (c *Collection) fileDocumentPageSize(rows []storeio.DocumentRecord, columns storeio.DocumentFloat64Columns) (uint32, error) {
	needed := storeio.PageHeaderSize + storeio.PageTrailerSize + storeio.DocumentPagePayloadHeaderSize + len(rows)*storeio.DocumentPageRecordSize
	for _, row := range rows {
		needed += len(row.Key)
		if row.Overflow == (storeio.PageRef{}) {
			needed += len(row.JSON)
		} else {
			needed += storeio.DocumentOverflowDescriptorSize
		}
	}
	for _, mask := range columns.Masks {
		needed += 8 + bits.OnesCount64(mask)*8
	}
	if needed > c.options.MaxPageSize {
		return 0, ErrDocumentTooLarge
	}
	quantum := c.options.PageSize
	size := needed / quantum * quantum
	if needed%quantum != 0 {
		size += quantum
	}
	return uint32(size), nil
}

// stageFileState constructs the complete state-bearing alternate root. State
// no longer consumes an allocator extent: all allocations therefore precede
// syncFreeLog, and the root can carry the resulting external free-log anchor
// without creating another allocation that the log must describe.
func (c *Collection) stageFileState(
	tx *storeio.WriteTransaction,
	old *fileStoreState,
	generation uint64,
	chunkHighWater uint32,
	freeChunkHint uint32,
	documentCount uint64,
	liveChunks uint32,
	chunkRoot, keyRoot, indexRoot, float64ScanHead, indexGroupHead,
	freeHead storeio.PageRef,
	freeChecksum uint32,
	inlineFree *storeio.InlineFreeDelta,
) (*fileStoreState, storeio.InlineFreeDelta, error) {
	root := storeio.StateRoot{
		StoreID: c.storeID, Generation: generation, PageSize: uint32(c.options.PageSize),
		Options:       old.root.Options,
		DocumentCount: documentCount, NextLogicalID: tx.NextLogicalID(),
		ChunkHighWater: chunkHighWater, LiveChunks: liveChunks,
		FreeChunkHint:  freeChunkHint,
		ChunkDocuments: uint32(c.options.Collection.ChunkDocuments),
		IndexCount:     uint32(len(c.options.indexes)), IndexCatalogHash: c.options.indexCatalogHash,
		IndexMaxDepth:                old.root.IndexMaxDepth,
		MaterializationDamageGranule: old.root.MaterializationDamageGranule,
		MaxPageSize:                  old.root.MaxPageSize,
		MaxKeyBytes:                  old.root.MaxKeyBytes,
		InlineValueBytes:             old.root.InlineValueBytes,
		MaxDocumentBytes:             old.root.MaxDocumentBytes,
		PageCatalogHead:              old.root.PageCatalogHead,
		PageCatalogDigest:            old.root.PageCatalogDigest,
		PageCatalogBytes:             old.root.PageCatalogBytes,
		ChunkDirectory:               chunkRoot, KeyDirectory: keyRoot, IndexDirectory: indexRoot,
		Float64ScanHead: float64ScanHead,
		IndexGroupHead:  indexGroupHead,
	}
	super := storeio.Superblock{
		StoreID: c.storeID, Generation: generation,
		FileEnd:  tx.FileEnd(),
		PageSize: uint32(c.options.PageSize),
	}
	if freeHead != (storeio.PageRef{}) {
		super.FreeOffset = freeHead.Offset
		super.FreeLength = freeHead.Length
		super.FreeChecksum = freeChecksum
	}
	if inlineFree == nil || inlineFree.ExternalPrev() != freeHead {
		return nil, storeio.InlineFreeDelta{}, storeio.ErrFreeLogCorrupt
	}
	// This is the one intentional steady-state point-mutation allocation.
	// Reuse is unsafe until the state is absent from state, visibleState,
	// durableState, pendingVisible, and every generation lease at or below its
	// generation. GenerationLeases.MinimumGeneration is the authority for the
	// last condition; a correct freelist therefore needs retirement machinery
	// rather than an eager writer-local pool.
	return &fileStoreState{
		root: root, super: super,
		keyRoot: keyRoot, chunkRoot: chunkRoot, indexRoot: indexRoot, freeHead: freeHead,
	}, *inlineFree, nil
}

// publishStagedFileMutation adopts a completely prepared copy-on-write
// generation. Buffered-visible publication takes the snapshot gate before its
// no-reader proof and holds it through both committer admission and the
// reader-visible pointer swap. That closes the only race in which a snapshot
// could otherwise capture the old state after its exact queued page writes had
// been recycled.
func (c *Collection) publishStagedFileMutation(
	tx *storeio.WriteTransaction,
	nextState *fileStoreState,
	nextInline storeio.InlineFreeDelta,
	freeLog freeLogCommit,
) error {
	publish := func(retiring bool) error {
		if retiring {
			return tx.PublishInlineRetiring(
				nextState.root, nextInline, c.retireRefScratch,
			)
		}
		return tx.PublishInline(nextState.root, nextInline)
	}
	if !c.buffered() {
		if err := publish(false); err != nil {
			return err
		}
		c.finalizeReusable()
		c.commitFreeLog(freeLog)
		c.inlineFree = nextInline
		c.snapshotGate.Lock()
		c.pageValidator.update(nextState)
		c.publishFileState(nextState)
		c.snapshotGate.Unlock()
		return nil
	}

	c.snapshotGate.Lock()
	retiring := !c.leases.AnyActive()
	if err := publish(retiring); err != nil {
		c.snapshotGate.Unlock()
		return err
	}
	// No fallible or unbounded writer bookkeeping belongs between accepted
	// admission and this swap. Readers do not consult the allocator/free-log
	// working state finalized below.
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	if retiring {
		c.cache.MarkUnreachable(c.retireRefScratch)
	}
	c.snapshotGate.Unlock()
	c.finalizeReusable()
	c.commitFreeLog(freeLog)
	c.inlineFree = nextInline
	return nil
}

func (c *Collection) rememberRetiredRef(ref storeio.PageRef) {
	if ref == (storeio.PageRef{}) ||
		len(c.retireRefScratch) == cap(c.retireRefScratch) {
		return
	}
	c.retireRefScratch = append(c.retireRefScratch, ref)
}

// collectFileRetirements lists the extents this commit makes unreachable. It
// runs before syncFreeLog rather than after, because the free log now writes
// those extents down in the same commit that retires them: a retirement that
// only reached memory was abandoned at the next Close, which cost a fixed three
// pages per open-write-close cycle and grew the file without bound across
// restarts. It reserves nothing — reserveFileRetirements does that after the
// free log has added its own superseded pages to the same list.
func (c *Collection) collectFileRetirements(
	old *fileStoreState,
	oldDocument storeio.PageRef,
	oldView *fileDocumentChunk,
	key storeio.PageKeyTreeMutation,
	chunk storeio.ChunkTreeMutation,
	retireFloat64Scan bool,
	retireIndexGroup bool,
) error {
	appendRef := func(ref storeio.PageRef) error {
		if ref == (storeio.PageRef{}) {
			return nil
		}
		if len(c.retireScratch) == cap(c.retireScratch) {
			return storeio.ErrRetiredExtentCapacity
		}
		c.retireScratch = append(c.retireScratch, storeio.FreeExtent{
			Offset: ref.Offset, Length: uint64(ref.Length), RetiredGeneration: old.root.Generation,
		})
		c.rememberRetiredRef(ref)
		return nil
	}
	if retireFloat64Scan && old.root.Float64ScanHead != (storeio.PageRef{}) {
		if err := c.appendFloat64ScanRetirements(old); err != nil {
			return err
		}
	}
	if retireIndexGroup {
		if err := c.appendIndexGroupRetirements(old); err != nil {
			return err
		}
	}
	if err := c.appendDocumentRetirement(old, oldDocument, oldView); err != nil {
		return err
	}
	for i := 0; i < int(key.RetiredCount); i++ {
		if err := appendRef(key.Retired[i]); err != nil {
			return err
		}
	}
	for i := 0; i < int(chunk.RetiredCount); i++ {
		if err := appendRef(chunk.Retired[i]); err != nil {
			return err
		}
	}
	return nil
}

// appendDocumentRetirement releases the extent a replaced chunk page occupied.
// A bulk-built PageDocumentGroup covers several chunks at once, so it may only
// be retired when no other chunk still names it; an ordinary PageDocument is
// owned by exactly one chunk and is released unconditionally.
func (c *Collection) appendDocumentRetirement(
	old *fileStoreState, oldDocument storeio.PageRef, oldView *fileDocumentChunk,
) error {
	appendRef := func(ref storeio.PageRef) error {
		if ref == (storeio.PageRef{}) {
			return nil
		}
		if len(c.retireScratch) == cap(c.retireScratch) {
			return storeio.ErrRetiredExtentCapacity
		}
		c.retireScratch = append(c.retireScratch, storeio.FreeExtent{
			Offset: ref.Offset, Length: uint64(ref.Length), RetiredGeneration: old.root.Generation,
		})
		c.rememberRetiredRef(ref)
		return nil
	}
	if oldDocument.Kind == storeio.PageDocumentGroup {
		if oldView == nil {
			return storeio.ErrDocumentGroupCorrupt
		}
		header, ok := oldView.groupHeader()
		if !ok {
			return storeio.ErrDocumentGroupCorrupt
		}
		shared, err := storeio.ChunkTreeHasOtherReference(
			c.cache, old.chunkRoot, header.FirstChunk, header.ChunkCount,
			oldView.chunk, oldDocument, storeio.ChunkTreeBounds{
				FileEnd: old.super.FileEnd, NextLogicalID: old.root.NextLogicalID,
			},
		)
		if err != nil {
			return err
		}
		if !shared {
			if err := appendRef(oldDocument); err != nil {
				return err
			}
			columns, detached, deriveErr := storeio.DocumentGroupFloat64Sidecar(
				oldDocument, uint32(c.options.PageSize),
			)
			if deriveErr != nil {
				return deriveErr
			}
			if detached {
				columnsHeader, ok := oldView.detachedFloat64Header()
				if !ok || columnsHeader.LogicalID != columns.LogicalID {
					return storeio.ErrFloat64GroupCorrupt
				}
				sharedColumns, referenceErr := storeio.ChunkTreeHasOtherFloat64Sidecar(
					c.cache, old.chunkRoot, columnsHeader.FirstChunk, columnsHeader.ChunkCount,
					oldView.chunk, columns, uint32(c.options.PageSize), storeio.ChunkTreeBounds{
						FileEnd: old.super.FileEnd, NextLogicalID: old.root.NextLogicalID,
					},
				)
				if referenceErr != nil {
					return referenceErr
				}
				if !sharedColumns {
					if err := appendRef(columns); err != nil {
						return err
					}
				}
			}
		}
	} else if err := appendRef(oldDocument); err != nil {
		return err
	}
	return nil
}

// absorbRetirementPressure turns a bare full-table sentinel into actionable
// bounded backpressure without ever allowing a commit to forget an extent.
//
// With a snapshot open, the extents genuinely might be dereferenced again, so
// the write fails. That is bounded, recoverable backpressure — closing the
// snapshot lets the next commit drain the set and the writer resumes with
// nothing lost — but the operator has to be told which snapshot to close, and
// "retired extent capacity exhausted" reads like corruption rather than like a
// reader holding a lease. The message now names the pinned generation.
//
// The sentinel is wrapped, not replaced, so errors.Is keeps working.
func (c *Collection) absorbRetirementPressure(err error) error {
	if c == nil || !errors.Is(err, storeio.ErrRetiredExtentCapacity) {
		return err
	}
	retired := c.reclaimer.Stats()
	current := uint64(0)
	if state := c.state.Load(); state != nil {
		current = state.root.Generation
	}
	leases := c.leases.Stats(current)
	if leases.Active != 0 && leases.MinimumGeneration <= current {
		return fmt.Errorf(
			"%w: %d of %d retired extents (%d bytes) are pinned by %d open snapshot(s), "+
				"the oldest at generation %d against current generation %d; "+
				"close those snapshots or raise Options.MaxRetiredExtents",
			err, retired.Pending, retired.Capacity, retired.PendingBytes,
			leases.Active, leases.MinimumGeneration, current)
	}
	return fmt.Errorf(
		"%w: committing %d retired extents would exceed the capacity of %d; "+
			"nothing was published or abandoned; raise Options.MaxRetiredExtents",
		err, len(c.retireScratch), retired.Capacity)
}

// retryRetirementAfterPressure checkpoints generations already accepted, then
// removes newly safe retirements into a holding scratch. The scratch is merged
// at the next transaction preflight, after the current allocator is inactive.
func (c *Collection) retryRetirementAfterPressure() error {
	current := uint64(0)
	if state := c.state.Load(); state != nil {
		current = state.root.Generation
	}
	if c.leases.Stats(current).Active == 0 {
		return storeio.ErrRetiredExtentCapacity
	}
	c.retirementPressureCheckpoints.Add(1)
	var err error
	if c.buffered() {
		err = c.checkpointBufferedLocked()
	} else {
		err = c.committer.Flush()
		if err == nil {
			c.cache.MarkDurable(c.committer.DurableGeneration())
		}
	}
	if err != nil {
		return err
	}
	absorbed, err := c.reclaimer.AppendReusable(
		c.retirementAbsorbed[:0], current,
		c.committer.FallbackGeneration(), cap(c.retirementAbsorbed),
	)
	if err != nil {
		return err
	}
	c.retirementAbsorbed = absorbed
	if len(absorbed) == 0 {
		return storeio.ErrRetiredExtentCapacity
	}
	return c.reclaimer.RetireBatch(c.retireScratch)
}

// reserveFileRetirements hands the complete list to the reclaimer. It runs after
// syncFreeLog so that the free log's own superseded pages — which a fold only
// knows once it has decided to fold — are reserved with everything else, and so
// that a failure here still precedes Publish and rolls the whole commit back.
//
// A full retirement table is routed through absorbRetirementPressure so the
// error identifies either the reader pin or the undersized transaction bound.
func (c *Collection) reserveFileRetirements() error {
	if err := c.reclaimer.RetireBatch(c.retireScratch); err != nil {
		if errors.Is(err, storeio.ErrRetiredExtentCapacity) {
			if retryErr := c.retryRetirementAfterPressure(); retryErr == nil {
				return nil
			} else if !errors.Is(retryErr, storeio.ErrRetiredExtentCapacity) {
				return retryErr
			}
		}
		return c.absorbRetirementPressure(err)
	}
	return nil
}

func (c *Collection) appendIndexGroupRetirements(
	old *fileStoreState,
) error {
	var previous storeio.PageRef
	for ref := old.root.IndexGroupHead; ref != (storeio.PageRef{}); {
		lease, err := c.cache.Acquire(ref)
		if err != nil {
			return err
		}
		catalog := storeio.AdmittedIndexGroupCatalog(lease.Page())
		next := catalog.Header().Next
		if previous != (storeio.PageRef{}) &&
			(ref.Offset <= previous.Offset ||
				ref.LogicalID <= previous.LogicalID) {
			lease.Release()
			return storeio.ErrIndexGroupCatalogCorrupt
		}
		lease.Release()
		length := uint64(ref.Length)
		if len(c.retireScratch) != 0 {
			last := &c.retireScratch[len(c.retireScratch)-1]
			if last.RetiredGeneration == old.root.Generation &&
				last.Offset <= ^uint64(0)-last.Length &&
				last.Offset+last.Length == ref.Offset &&
				last.Length <= ^uint64(0)-length {
				last.Length += length
				c.rememberRetiredRef(ref)
			} else if err := c.appendIndexRetiredRef(old, ref); err != nil {
				return err
			}
		} else if err := c.appendIndexRetiredRef(old, ref); err != nil {
			return err
		}
		previous = ref
		ref = next
	}
	return nil
}

// appendFloat64ScanRetirements releases a complete aggregate-only projection
// after an out-of-range insert or incremental-rebuild fallback. Bulk stripes
// and ordered-directory levels are allocated as one physical run. Adjacent
// refs are folded into one retirement record so reclamation metadata remains
// O(1) for a large compact generation. Projection-neutral
// publications retain the projection.
//
// Authoritative detached PageFloat64Group sidecars are not catalog entries
// and remain reachable from document refs.
func (c *Collection) appendFloat64ScanRetirements(old *fileStoreState) error {
	appendRef := func(ref storeio.PageRef) error {
		if ref == (storeio.PageRef{}) {
			return nil
		}
		c.rememberRetiredRef(ref)
		length := uint64(ref.Length)
		if len(c.retireScratch) != 0 {
			last := &c.retireScratch[len(c.retireScratch)-1]
			if last.RetiredGeneration == old.root.Generation &&
				last.Offset <= ^uint64(0)-last.Length &&
				last.Offset+last.Length == ref.Offset &&
				last.Length <= ^uint64(0)-length {
				last.Length += length
				return nil
			}
		}
		if len(c.retireScratch) == cap(c.retireScratch) {
			return storeio.ErrRetiredExtentCapacity
		}
		c.retireScratch = append(c.retireScratch, storeio.FreeExtent{
			Offset: ref.Offset, Length: length, RetiredGeneration: old.root.Generation,
		})
		return nil
	}
	bounds := storeio.Float64DirectoryBounds{
		FileEnd:       old.super.FileEnd,
		NextLogicalID: old.root.NextLogicalID,
	}
	err := storeio.WalkFloat64DirectoryLeaves(
		c.cache, old.root.Float64ScanHead, bounds,
		uint32(c.options.PageSize),
		func(leaf storeio.Float64DirectoryView) error {
			for i := 0; i < leaf.Len(); i++ {
				entry, _ := leaf.EntryAt(i)
				if err := appendRef(entry.Ref); err != nil {
					return err
				}
			}
			return nil
		},
	)
	if err != nil {
		return err
	}
	return storeio.WalkFloat64DirectoryPages(
		c.cache, old.root.Float64ScanHead, bounds,
		uint32(c.options.PageSize), appendRef,
	)
}

func (c *Collection) appendOverflowRetirements(state *fileStoreState, value storeio.DocumentValue, location storeio.KeyLocation) error {
	ref := value.Overflow
	if ref == (storeio.PageRef{}) {
		return nil
	}
	offset := uint64(0)
	for ref != (storeio.PageRef{}) {
		if len(c.retireScratch) == cap(c.retireScratch) {
			return storeio.ErrRetiredExtentCapacity
		}
		lease, err := c.cache.Acquire(ref)
		if err != nil {
			return err
		}
		view, err := storeio.OpenOverflowPage(
			lease.Page(), state.super.FileEnd, state.root.NextLogicalID,
			state.root.PageSize, state.root.ChunkHighWater, uint8(state.root.ChunkDocuments),
		)
		if err != nil {
			lease.Release()
			return err
		}
		header := view.Header()
		if header.Chunk != location.Chunk || header.Slot != location.Slot ||
			header.Total != value.Length || header.Offset != offset {
			lease.Release()
			return storeio.ErrOverflowPageCorrupt
		}
		c.retireScratch = append(c.retireScratch, storeio.FreeExtent{
			Offset: ref.Offset, Length: uint64(ref.Length), RetiredGeneration: state.root.Generation,
		})
		c.rememberRetiredRef(ref)
		offset += uint64(len(view.Data()))
		ref = header.Next
		lease.Release()
	}
	if offset != value.Length {
		return storeio.ErrOverflowPageCorrupt
	}
	return nil
}

func (c *Collection) appendFileValue(dst []byte, state *fileStoreState, value storeio.DocumentValue, location storeio.KeyLocation) ([]byte, error) {
	if value.Overflow == (storeio.PageRef{}) {
		return append(dst, value.Inline...), nil
	}
	ref := value.Overflow
	offset := uint64(0)
	for ref != (storeio.PageRef{}) {
		lease, err := c.cache.Acquire(ref)
		if err != nil {
			return dst, err
		}
		view, err := storeio.OpenOverflowPage(
			lease.Page(), state.super.FileEnd, state.root.NextLogicalID,
			state.root.PageSize, state.root.ChunkHighWater, uint8(state.root.ChunkDocuments),
		)
		if err != nil {
			lease.Release()
			return dst, err
		}
		header := view.Header()
		if header.Chunk != location.Chunk || header.Slot != location.Slot ||
			header.Total != value.Length || header.Offset != offset {
			lease.Release()
			return dst, storeio.ErrOverflowPageCorrupt
		}
		dst = append(dst, view.Data()...)
		offset += uint64(len(view.Data()))
		ref = header.Next
		lease.Release()
	}
	if offset != value.Length {
		return dst, storeio.ErrOverflowPageCorrupt
	}
	return dst, nil
}

// buildOldFileIndex indexes the pre-update copy of one document so an index
// maintenance step can retract its old keys. It runs once per updated
// document, so it takes the growing build rather than a sizing pass; see
// buildIndexGrowing.
func (c *Collection) buildOldFileIndex(src []byte) (vibejson.Index, error) {
	return buildIndexGrowing(&c.oldParseScratch, src, c.options.Collection.IndexOptions)
}

func fileIndexTupleHash(exact *store.ExactIndex, index vibejson.Index) (uint64, bool, error) {
	hash := uint64(14695981039346656037)
	for i := 0; i < int(exact.N); i++ {
		node, ok, err := index.PointerCompiled(exact.Paths[i])
		if err != nil || !ok {
			return 0, false, err
		}
		hash, ok = fileIndexHashNode(hash, node)
		if !ok {
			return 0, false, nil
		}
	}
	return hash, true, nil
}

func fileIndexTuplesEqual(exact *store.ExactIndex, left, right vibejson.Index) (bool, error) {
	if exact == nil || exact.N == 0 {
		return false, nil
	}
	for column := range int(exact.N) {
		leftNode, leftOK, err := left.PointerCompiled(exact.Paths[column])
		if err != nil {
			return false, err
		}
		rightNode, rightOK, err := right.PointerCompiled(exact.Paths[column])
		if err != nil {
			return false, err
		}
		if !leftOK || !rightOK ||
			!fileIndexRawValuesEqual(leftNode.Raw(), rightNode.Raw()) {
			return false, nil
		}
	}
	return true, nil
}

func fileIndexNeedleHash(exact *store.ExactIndex, values []vibejson.Index) (uint64, error) {
	if len(values) != int(exact.N) {
		return 0, store.ErrIndexArity
	}
	hash := uint64(14695981039346656037)
	for _, value := range values {
		var ok bool
		hash, ok = fileIndexHashNode(hash, value.Root())
		if !ok {
			return 0, store.ErrIndexScalar
		}
	}
	return hash, nil
}

func fileIndexHashNode(hash uint64, node vibejson.Node) (uint64, bool) {
	raw := node.Raw()
	hash = fileIndexHashBytes(hash, []byte{byte(raw.Kind()), 0xff})
	switch raw.Kind() {
	case document.Null:
	case document.Bool:
		value, _ := raw.Bool()
		if value {
			hash = fileIndexHashBytes(hash, []byte{1})
		} else {
			hash = fileIndexHashBytes(hash, []byte{0})
		}
	case document.Number:
		if value, ok := raw.Float64(); ok {
			if value == 0 {
				value = 0
			}
			var encoded [8]byte
			binary.LittleEndian.PutUint64(encoded[:], math.Float64bits(value))
			hash = fileIndexHashBytes(hash, encoded[:])
		} else {
			hash = fileIndexHashBytes(hash, []byte{0x7f})
		}
	case document.String:
		if text, clean := raw.StringBytes(); clean {
			hash = fileIndexHashBytes(hash, text)
		} else {
			text, ok := node.AppendText(nil)
			if !ok {
				return 0, false
			}
			hash = fileIndexHashBytes(hash, text)
		}
	default:
		return 0, false
	}
	hash = fileIndexHashBytes(hash, []byte{0xfe})
	return hash, true
}

func (c *Collection) updateFileIndexes(tx *storeio.WriteTransaction, state *fileStoreState, location storeio.KeyLocation, oldIndex, newIndex *vibejson.Index) (storeio.PageRef, error) {
	root := state.indexRoot
	for indexID, exact := range c.options.indexes {
		var oldHash, newHash uint64
		var oldOK, newOK bool
		var err error
		if oldIndex != nil {
			oldHash, oldOK, err = fileIndexTupleHash(exact, *oldIndex)
			if err != nil {
				return storeio.PageRef{}, err
			}
		}
		if newIndex != nil {
			newHash, newOK, err = fileIndexTupleHash(exact, *newIndex)
			if err != nil {
				return storeio.PageRef{}, err
			}
		}
		if oldOK && newOK && oldHash == newHash {
			equal, equalErr := fileIndexTuplesEqual(exact, *oldIndex, *newIndex)
			if equalErr != nil {
				return storeio.PageRef{}, equalErr
			}
			if equal {
				continue
			}
			newCertificate, certificateErr := c.fileIndexCertificate(
				c.indexNewCertificate[:0], exact, *newIndex,
			)
			if certificateErr != nil {
				return storeio.PageRef{}, certificateErr
			}
			c.indexNewCertificate = newCertificate
			root, err = c.mutateFilePosting(
				tx, state, root, uint32(indexID), oldHash, location, true,
				newCertificate,
			)
			if err != nil {
				return storeio.PageRef{}, err
			}
			continue
		}
		if oldOK {
			root, err = c.mutateFilePosting(
				tx, state, root, uint32(indexID), oldHash, location, false, nil,
			)
			if err != nil {
				return storeio.PageRef{}, err
			}
		}
		if newOK {
			newCertificate, certificateErr := c.fileIndexCertificate(
				c.indexNewCertificate[:0], exact, *newIndex,
			)
			if certificateErr != nil {
				return storeio.PageRef{}, certificateErr
			}
			c.indexNewCertificate = newCertificate
			root, err = c.mutateFilePosting(
				tx, state, root, uint32(indexID), newHash, location, true,
				newCertificate,
			)
			if err != nil {
				return storeio.PageRef{}, err
			}
		}
	}
	return root, nil
}

func (c *Collection) fileIndexCertificate(dst []byte, exact *store.ExactIndex, index vibejson.Index) ([]byte, error) {
	if exact == nil || exact.N == 0 {
		return nil, nil
	}
	var values [store.MaxIndexColumns]vibejson.RawValue
	for column := range int(exact.N) {
		node, ok, err := index.PointerCompiled(exact.Paths[column])
		if err != nil || !ok {
			return nil, err
		}
		values[column] = node.Raw()
	}
	certificate, ok := appendFileIndexCertificate(
		dst, values[:exact.N],
		storeio.IndexDirectoryMaxCertificate(uint32(c.options.PageSize)),
	)
	if !ok {
		return nil, nil
	}
	return certificate, nil
}

func (c *Collection) mutateFilePosting(
	tx *storeio.WriteTransaction,
	state *fileStoreState,
	root storeio.PageRef,
	indexID uint32,
	tupleHash uint64,
	location storeio.KeyLocation,
	present bool,
	newCertificate []byte,
) (storeio.PageRef, error) {
	key := storeio.IndexDirectoryKey{IndexID: indexID, TupleHash: tupleHash, Chunk: location.Chunk}
	bounds := storeio.IndexTreeBounds{
		FileEnd: tx.FileEnd(), NextLogicalID: tx.NextLogicalID(), IndexHighWater: uint32(len(c.options.indexes)),
	}
	// LookupIndexTree copies the representative out before it drops the leaf
	// lease, so the scratch below never aliases an evictable page frame.
	entry, certificate, found, err := storeio.LookupIndexTree(
		c.cache, root, key, bounds, c.indexCertificateScratch[:0],
	)
	c.indexCertificateScratch = certificate
	if err != nil {
		return storeio.PageRef{}, err
	}
	mask := uint64(0)
	collision := false
	if found {
		if len(c.indexCertificateScratch) != 0 &&
			!fileIndexCertificateValid(
				c.indexCertificateScratch, int(c.options.indexes[indexID].N),
			) {
			return storeio.PageRef{}, storeio.ErrIndexDirectoryCorrupt
		}
		collision = entry.Flags&storeio.IndexEntryCollision != 0
		mask = entry.Bits
	}
	bit := uint64(1) << location.Slot
	if present {
		if len(newCertificate) == 0 {
			c.indexCertificateScratch = c.indexCertificateScratch[:0]
			collision = false
		} else if len(c.indexCertificateScratch) == 0 {
			if found {
				// An existing entry without a representative cannot prove that
				// its bits all belong to the new value, and a live entry always
				// carries a non-empty mask, so those bits really are at stake.
				collision = true
			}
			c.indexCertificateScratch = append(
				c.indexCertificateScratch, newCertificate...,
			)
		} else if !fileIndexCertificatesEqual(
			c.indexCertificateScratch, newCertificate,
			int(c.options.indexes[indexID].N),
		) {
			collision = true
		}
		mask |= bit
	} else {
		mask &^= bit
	}
	if mask == 0 {
		mutation, deleteErr := storeio.DeleteIndexTree(c.cache, tx, root, key, bounds)
		if deleteErr != nil {
			return storeio.PageRef{}, deleteErr
		}
		if !mutation.Found {
			return storeio.PageRef{}, storeio.ErrIndexDirectoryCorrupt
		}
		if err := c.appendIndexRetirements(state, mutation); err != nil {
			return storeio.PageRef{}, err
		}
		return mutation.Root, nil
	}
	// The mask now lives in the routing record itself, so one changed value
	// costs one rewritten leaf rather than a leaf plus a whole page holding a
	// single 64-bit word. There is nothing left to retire here either: the leaf
	// rewrite reports its own retirements below.
	flags := uint16(0)
	if collision {
		flags |= storeio.IndexEntryCollision
	}
	mutation, err := storeio.UpsertIndexTree(c.cache, tx, root, storeio.IndexDirectoryEntry{
		Key: key, Bits: mask, Flags: flags, Kind: storeio.IndexEntryInlineMask,
		Cert: storeio.CertSpan{Length: uint16(len(c.indexCertificateScratch))},
	}, c.indexCertificateScratch, bounds)
	if err != nil {
		return storeio.PageRef{}, err
	}
	if err := c.appendIndexRetirements(state, mutation); err != nil {
		return storeio.PageRef{}, err
	}
	return mutation.Root, nil
}

func (c *Collection) appendIndexRetiredRef(state *fileStoreState, ref storeio.PageRef) error {
	if len(c.retireScratch) == cap(c.retireScratch) {
		return storeio.ErrRetiredExtentCapacity
	}
	c.retireScratch = append(c.retireScratch, storeio.FreeExtent{
		Offset: ref.Offset, Length: uint64(ref.Length), RetiredGeneration: state.root.Generation,
	})
	c.rememberRetiredRef(ref)
	return nil
}

func (c *Collection) appendIndexRetirements(state *fileStoreState, mutation storeio.IndexTreeMutation) error {
	for i := 0; i < int(mutation.RetiredCount); i++ {
		if err := c.appendIndexRetiredRef(state, mutation.Retired[i]); err != nil {
			return err
		}
	}
	return nil
}

func fileStoreLiveMask(chunkDocuments uint32) uint64 {
	if chunkDocuments >= 64 {
		return ^uint64(0)
	}
	return uint64(1)<<chunkDocuments - 1
}

func (c *Collection) restoreAppendChunk(state *fileStoreState) error {
	if state.root.ChunkHighWater == 0 || state.chunkRoot == (storeio.PageRef{}) {
		return nil
	}
	last := state.root.ChunkHighWater - 1
	ref, ok, err := storeio.LookupChunkTree(c.cache, state.chunkRoot, last, storeio.ChunkTreeBounds{
		FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
	})
	if err != nil || !ok {
		return err
	}
	lease, err := c.cache.Acquire(ref)
	if err != nil {
		return err
	}
	view, viewErr := admittedFileDocumentChunk(lease.Page(), ref, last)
	lease.Release()
	if viewErr != nil {
		return viewErr
	}
	limit := ^uint64(0)
	if state.root.ChunkDocuments < 64 {
		limit = uint64(1)<<state.root.ChunkDocuments - 1
	}
	if view.live() != limit {
		c.appendChunk = last
		c.appendLive = view.live()
	}
	return nil
}

func (c *Collection) waitPublished(generation uint64) error {
	if err := c.committer.Wait(generation); err != nil {
		return err
	}
	c.cache.MarkDurable(generation)
	return nil
}

// Flush waits until the current reader-visible generation is crash-safe.
func (c *Collection) Flush() error {
	if c == nil || c.committer == nil {
		return ErrClosed
	}
	// The deferred canonical-frame lanes — buffered-visible and the
	// journal-backed synchronous lane — hold acknowledged mutations in staged
	// frames plus (on the sync lane) durable journal records, so Flush folds them
	// into a checkpointed root and recycles the journal. The chunk sync and
	// async-visible lanes already fence per generation, so Flush waits on the
	// current one.
	if c.buffered() || c.syncJournalLane() {
		c.writer.Lock()
		defer c.writer.Unlock()
		if c.closed {
			return ErrClosed
		}
		return c.checkpointBufferedLocked()
	}
	generation := c.Generation()
	if err := c.committer.Wait(generation); err != nil {
		return err
	}
	c.cache.MarkDurable(generation)
	return nil
}

// Close fences every publication and releases bounded I/O resources. It does
// not close the caller-owned file. Active snapshots must be closed first.
func (c *Collection) Close() error {
	if c == nil {
		return nil
	}
	c.writer.Lock()
	if c.closeDone {
		c.writer.Unlock()
		return nil
	}
	c.closed = true
	c.writer.Unlock()
	// DurabilitySync publishers release the construction lock before their
	// durability wait so independent writers can share one device commit.
	// Closed prevents any new waiter from registering before this drain.
	c.durabilityWait.Wait()
	c.writer.Lock()
	defer c.writer.Unlock()
	// Concurrent Close calls may both have observed closeDone before waiting
	// for the last synchronous publisher. Recheck under the resource lock so
	// only one caller detaches and closes the mmap-backed arenas.
	if c.closeDone {
		return nil
	}
	// Fold the final deferred window into a durable root and recycle the journal
	// so a reopen replays nothing, for both buffered-visible and the
	// journal-backed synchronous lane.
	if c.buffered() || c.syncJournalLane() {
		if err := c.checkpointBufferedLocked(); err != nil {
			return err
		}
	}
	if err := c.leases.Close(); err != nil {
		return err
	}
	if err := c.closeResourcesLocked(); err != nil {
		return err
	}
	c.closeDone = true
	return nil
}

func (c *Collection) closeResources() error {
	c.writer.Lock()
	defer c.writer.Unlock()
	return c.closeResourcesLocked()
}

// closeResourcesLocked detaches every view into an external block before
// releasing that block. Stats uses the same writer lock, so it can observe
// either a complete live resource set or the detached state, never a slice or
// reclaimer whose backing mmap has already been unmapped.
func (c *Collection) closeResourcesLocked() error {
	var result error
	if err := c.closeRecoveryJournalLocked(); err != nil {
		result = errors.Join(result, err)
	}
	if c.committer != nil {
		if err := c.committer.Close(); err != nil {
			result = errors.Join(result, err)
		} else if c.buffered() {
			c.clearBufferedInplaceLocked()
		}
		c.cache.MarkDurable(c.committer.DurableGeneration())
	}
	if c.cache != nil {
		if err := c.cache.Close(); err != nil {
			result = errors.Join(result, err)
		}
	}
	if c.readFile != nil {
		readFile := c.readFile
		c.readFile = nil
		if err := readFile.Close(); err != nil {
			result = errors.Join(result, err)
		}
	}
	if c.writeFile != nil {
		writeFile := c.writeFile
		c.writeFile = nil
		if err := writeFile.Close(); err != nil {
			result = errors.Join(result, err)
		}
	}
	reusableBlock := c.reusableBlock
	c.reclaimer = nil
	c.reusableBlock = nil
	c.reusable = nil
	c.freeExtentIndex = storeio.FreeExtentIndex{}
	c.freeExtentMaxima = nil
	if reusableBlock != nil {
		if err := reusableBlock.Close(); err != nil {
			result = errors.Join(result, err)
		}
	}
	if c.freeScratchBlock != nil {
		if err := c.freeScratchBlock.Close(); err != nil {
			result = errors.Join(result, err)
		}
		c.freeScratchBlock = nil
		c.freeFenced = nil
		c.freeImageScratch = nil
		c.freeFoldRanges = nil
		c.freeFoldOrder = nil
	}
	if c.materializationBlock != nil {
		if err := c.materializationBlock.Close(); err != nil {
			result = errors.Join(result, err)
		}
		c.materializationBlock = nil
		c.materializationBefore = nil
		c.materializationAfter = nil
	}
	if c.writerLocked {
		if err := storeio.UnlockWriter(c.file); err != nil {
			result = errors.Join(result, err)
		} else {
			c.writerLocked = false
		}
	}
	return result
}
