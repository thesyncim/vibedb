package durable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

var (
	// ErrClosed reports use after Collection.Close has started.
	ErrClosed = errors.New("vibedb: collection is closed")
	// ErrNotEmpty requires Create to receive an empty file.
	ErrNotEmpty = errors.New("vibedb: collection create requires an empty file")
	// ErrKeyTooLarge reports a key beyond the configured durable page
	// bound.
	ErrKeyTooLarge = errors.New("vibedb: collection key exceeds configured bound")
	// ErrDocumentTooLarge reports a JSON value beyond the configured
	// transaction bound.
	ErrDocumentTooLarge = errors.New("vibedb: collection document exceeds configured bound")
	// ErrUnsupportedPageSize rejects any page size other than the fixed 4096-byte
	// base page. The ordered primary graph is a 4 KiB-page structure (leaf extents
	// grow to MaxPageSize); variable base page sizes are no longer supported, so
	// Create refuses a non-4096 PageSize and Open refuses a store that recorded one.
	ErrUnsupportedPageSize = errors.New("vibedb: collection page size must be 4096")
	// ErrPrimaryLeafSplitRequired reports that an insert landed on a leaf with
	// no room for it. It is an internal retry signal: the mutation path catches
	// it, commits an atomic leaf split as its own structural transaction, and
	// retries the insert, so a successful mutation never surfaces it. It reaches
	// a caller only when the bounded split-and-retry loop cannot make progress.
	ErrPrimaryLeafSplitRequired = errors.New(
		"vibedb: ordered primary leaf split required",
	)
	// ErrPrimaryCutoverUnsupported reports a CreateFromPrimary option or
	// source shape whose durable companion structure is not built yet.
	ErrPrimaryCutoverUnsupported = errors.New(
		"vibedb: ordered primary cutover feature is unsupported",
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
	// ErrPhysicalCapacity reports that a capped collection has not yet strictly
	// allocated enough of its sealed main-file ceiling for a requested write.
	// EnsurePhysicalAllocation may advance that proof before the caller retries.
	ErrPhysicalCapacity = storeio.ErrPhysicalCapacity
	// ErrSealedJournalCapacity reports that a recovery journal or transaction
	// marker cannot satisfy an exact immutable physical-capacity profile.
	ErrSealedJournalCapacity = storeio.ErrSealedCapacityMismatch
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
	// returns success only after it is durable: a bounded recovery-journal
	// record is appended and synced before the mutation is applied and
	// published — its root is folded in at the next checkpoint — so visibility
	// strictly follows durability and no reader observes an un-durable
	// acknowledged mutation.
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

// Options fixes the durable collection's schema, exact indexes, memory bounds,
// engine selection, and durability contract at construction. It embeds the
// in-memory store.Options for shared collection settings and adds the on-disk
// concerns the resident engine does not have. The zero value is a working
// power-safe configuration; every field below documents what it overrides.
type Options struct {
	Collection store.Options
	// OpaqueValues stores primary values as uninterpreted, non-empty byte
	// strings. It disables JSON parsing and canonicalization and therefore
	// cannot be combined with schemas, exact or skip indexes, or the resident
	// store's JSON-only representation options. The mode is persisted and a
	// zero-options Open reconstructs it from the selected state root.
	OpaqueValues bool
	// Indexes are exact scalar definitions maintained from their atomic durable
	// publication onward. CreateIndex can add definitions to a live collection
	// when its fixed transaction/resident arenas admit the resulting write path.
	// Canonical ordered path vectors assign stable on-disk physical IDs
	// independently of caller order. Differently named definitions with
	// identical ordered paths are logical aliases of one physical index: they
	// share posting maintenance and durable bytes while remaining independently
	// discoverable and queryable.
	Indexes []store.IndexDefinition
	// SkipIndexes are RFC 6901 paths with compact per-primary-stripe min/max
	// summaries. They accelerate equality and ordered scalar predicates without
	// materializing posting lists. Paths are canonicalized and persisted; at
	// most storeio.PageCatalogMaxSkipIndexes are accepted so write and space
	// amplification stay strictly bounded. Missing values summarize as null;
	// a container, oversized scalar, or overflow row makes only that stripe/path
	// unprunable and never affects query correctness.
	SkipIndexes []string
	// PageSize is the base page of the ordered primary graph. It must be 4096:
	// every tree, root, directory, and metadata page is exactly one base page.
	// Zero defaults to 4096; any other value is rejected on Create with
	// ErrUnsupportedPageSize, and Open of a store that recorded a different base
	// page fails the same way. It is no longer discovered from the file.
	PageSize int
	// MaxPageSize bounds a leaf/overflow extent, which grows from PageSize up to
	// this value (default 64 KiB). It must be a power-of-two multiple of PageSize.
	MaxPageSize int
	// PhysicalCapacityBytes seals the maximum main-file high-water. Zero keeps
	// elastic growth. A non-zero value is immutable, page aligned, and does not
	// eagerly allocate the whole ceiling; EnsurePhysicalAllocation grows the
	// currently proven prefix monotonically when future work needs it. While a
	// capped collection is open, no other descriptor or process may truncate,
	// punch, reflink-clone, or otherwise change allocation on its main file; the
	// cooperative writer lease cannot prevent those external mutations.
	PhysicalCapacityBytes uint64
	// SealedRecoveryJournalBytes fixes the paired recovery journal's immutable,
	// strictly allocated record-region capacity. It is Linux-only at create/open,
	// requires DurabilitySync, and includes neither of the two 512-byte headers.
	// A sealed journal never grows; pressure recycles its existing region or
	// refuses a record that cannot fit in an empty region.
	SealedRecoveryJournalBytes uint64
	ResidentBytes              int64
	// ReadConcurrency bounds portable positional-read workers.
	ReadConcurrency int
	// ReadQueueDepth bounds one native asynchronous read submission.
	ReadQueueDepth int
	// PrefetchQueue bounds references waiting for either read engine.
	PrefetchQueue int
	// MaxKeyBytes bounds encoded primary keys. Zero selects the format maximum,
	// 256 bytes; explicit values must be between 1 and 256 inclusive.
	MaxKeyBytes      int
	InlineValueBytes int
	MaxDocumentBytes int
	// BufferCount normalizes the maximum transaction and checkpoint descriptor
	// geometry. Immutable data pages encode directly in PageCache frames; the
	// committer owns only a small fixed arena for alternate roots, recovery
	// journals, and sparse canonical patches, and BufferCount bounds how many
	// page descriptors one transaction may stage.
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
	// pages, so zero normalizes to 1,024 descriptors. Explicit values are
	// accepted and validated.
	BufferCount int
	QueueSlots  int
	// GroupLimit caps how many adjacent generations share one durability
	// fence; zero selects 32. It limits an already formed group rather than
	// waiting to create one; CommitCoalesce is the option that permits such a
	// wait. Measured tuning results live in docs/performance.md.
	GroupLimit int
	// CommitCoalesce bounds optional durability grouping. The background
	// committer uses it to wait for adjacent accepted generations; the opt-in
	// buffered-journal lane uses it to let concurrent published records share
	// one sync. Zero, the default, adds no intentional wait. It has no effect on
	// the journal-before-publish DurabilitySync path, whose record must be
	// fenced before the mutation becomes visible.
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
	// RecoveryJournal upgrades DurabilityBufferedVisible from volatile mutation
	// acknowledgements to a bounded redo append and sync for every mutation.
	// Readers are untouched: visibility still comes from canonical resident
	// state, and crash recovery replays redo through the ordinary mutation path.
	// The sync strength follows CheckpointStrength (power-safe issues the
	// F_FULLFSYNC-class barrier, filesystem the ordinary fdatasync-class one).
	//
	// When false, a fresh or bulk-built buffered-visible store has no sibling
	// journal. Its first valid mutation mints one synchronously; the first physical
	// Flush/Close roots that identity, and later Flush calls can append and sync one
	// ordered batch for a complete bounded class-5 overlay instead of copying its
	// physical leaves. Exceptional mutations, pressure, and Close take a physical
	// checkpoint and recycle the journal. No background task creates or compacts
	// the file.
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
}

// TxnLimits bounds one multi-collection commit across participants. Accounting
// completes before any durable work; exceeding any bound is a typed refusal
// with nothing staged, journaled, or published.
//
// The zero value is fail-closed at the caller-owned UpdateCollections
// primitive: any zero dimension refuses a K ≥ 2 commit. Zero-to-default
// substitution happens only in layers that own defaults — Database.Update,
// facade AdvancedOptions, and SQL driver option normalization — never inside
// the primitive.
type TxnLimits struct {
	// MaxCollections bounds how many dirty participants one commit may name.
	// Zero is fail-closed at UpdateCollections. Database.Update normalizes
	// zero to defaultTxnMaxCollections (16), hard-capped by
	// storeio.TxnMarkerMaxParticipants.
	MaxCollections int
	// MaxDocuments bounds the total distinct keys staged across every
	// participant. Zero is fail-closed at UpdateCollections; Database.Update
	// normalizes zero to defaultTxnMaxDocuments (4× the single-collection
	// MaxBatchDocuments default).
	MaxDocuments int
	// MaxBytes bounds the total key and value bytes staged across every
	// participant. Zero is fail-closed at UpdateCollections; Database.Update
	// normalizes zero to defaultTxnMaxBytes (4× the single-collection
	// MaxBatchBytes default).
	MaxBytes int64
}

const (
	// defaultTxnMaxCollections is the Database.Update / package-default
	// MaxCollections. It matches the design's default of 16 and sits below
	// storeio.TxnMarkerMaxParticipants (64), the hard encode-time ceiling.
	defaultTxnMaxCollections = 16
	// defaultTxnMaxDocuments is 4× store.MaxChunkDocuments, the zero-value
	// Options.MaxBatchDocuments default.
	defaultTxnMaxDocuments = 4 * store.MaxChunkDocuments
	// defaultTxnMaxBytes is 4× the zero-value Options.MaxBatchBytes:
	// MaxBatchDocuments×MaxKeyBytes + defaultBatchValueBytes with the
	// zero-value MaxKeyBytes of 256.
	defaultTxnMaxBytes = 4 * (int64(store.MaxChunkDocuments)*256 + defaultBatchValueBytes)
)

// defaultTxnLimits returns the pinned package defaults Database.Update applies
// when normalizing a zero TxnLimits dimension.
func defaultTxnLimits() TxnLimits {
	return TxnLimits{
		MaxCollections: defaultTxnMaxCollections,
		MaxDocuments:   defaultTxnMaxDocuments,
		MaxBytes:       defaultTxnMaxBytes,
	}
}

// ValidateOptions reports whether options can be normalized and represented by
// the durable on-disk format, without opening or creating a collection.
//
// This is useful to catalogs that persist collection definitions before they
// allocate a data file: validating first prevents acknowledging metadata that a
// later Create would reject because of an index-count, string-size, schema, or
// geometry bound.
func ValidateOptions(options Options) error {
	_, err := options.normalized()
	return err
}

func fileStoreCheckedAdd(left, right int) (int, bool) {
	if left < 0 || right < 0 || left > math.MaxInt-right {
		return 0, false
	}
	return left + right, true
}

func fileStoreCheckedMul(left, right int) (int, bool) {
	if left < 0 || right < 0 || left != 0 && right > math.MaxInt/left {
		return 0, false
	}
	return left * right, true

}

func fileStoreSaturatingAdd(left, right int) int {
	if sum, ok := fileStoreCheckedAdd(left, right); ok {
		return sum
	}
	return math.MaxInt
}

func fileStoreSaturatingMul(left, right int) int {
	if product, ok := fileStoreCheckedMul(left, right); ok {
		return product
	}
	return math.MaxInt
}

func fileStoreSaturatingByteProduct(count, width int) uint64 {
	if count < 0 || width < 0 ||
		width != 0 && uint64(count) > math.MaxUint64/uint64(width) {
		return math.MaxUint64
	}
	return uint64(count) * uint64(width)
}

func fileStoreSaturatingByteAdd(left, right uint64) uint64 {
	if left > math.MaxUint64-right {
		return math.MaxUint64
	}
	return left + right
}

// batchMetadataBasePages is the worst-case non-overflow page reservation for
// one batched publication before its free-log fold grows past the
// single-document baseline. Each term names the structure it pays for:
//
//   - one rewritten primary leaf per document;
//   - the bounded structural primary path for every mutated key;
//   - rewritten term leaves and catalog paths for each configured exact index;
//   - the free log's fold ceiling.
//
// The alternate publication root has its own Batch buffer and is not part of
// this transaction-page reservation.
//
// It is deliberately a reservation and not an invariant. A pathological tree
// shape can exceed it, in which case the transaction's allocator refuses and
// Update returns ErrBatchTooLarge with nothing published; the caller retries
// with a smaller batch. Making it exact would require reserving for a
// ten-level directory over every key, which is hundreds of times the pages any
// real store uses.
func batchMetadataBasePages(o Options, indexes int) int {
	documents := o.MaxBatchDocuments
	// One rewritten leaf frame per document in the worst case, plus the bounded
	// parent-chain and free-set reservation the point path already sizes, plus,
	// per index, its rewritten term leaves and canonical root. The ordered-primary
	// batch computes its exact reservation at apply time; this is the conservative
	// commit-buffer cap.
	pages := fileStoreSaturatingAdd(
		documents, fileStorePrimaryBatchReservePages(documents),
	)
	if indexes != 0 {
		perIndex := fileStoreSaturatingAdd(documents, 2)
		pages = fileStoreSaturatingAdd(
			pages, fileStoreSaturatingMul(indexes, perIndex),
		)
	}
	return fileStoreSaturatingAdd(pages, fileStoreMetadataReservePages)
}

// fileStorePrimaryBatchReservePages sizes the ordinary atomic-batch
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
func fileStorePrimaryBatchReservePages(edits int) int {
	if edits <= 0 {
		return 0
	}
	const fixed = fileStorePointPrimaryStagePages
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
	extra := batchFreeFoldLimit(o, indexes) - storeio.FreeLogMaxFoldSegments
	return fileStoreSaturatingAdd(base, extra)
}

const (
	// A point primary mutation has a sixteen-page maximum path. Root
	// promotion can stage two more pages, while delete compaction may retire
	// the selected page plus one sibling at every level: 2*16-1 = 31.
	// Keeping the staged and retired geometries separate avoids charging the
	// commit-buffer arena for immutable pages that only enter the reclaimer.
	fileStorePointPrimaryStagePages  = 18
	fileStorePointPrimaryRetirePages = 31

	// fileStoreMetadataReservePages is the fixed share of a transaction's page
	// reservation that is not proportional to the batch: the primary tree's
	// 18-page point ceiling, exact-index paths, and the
	// single-document free log's worst commit. A wider batch adds fold pages in
	// batchMetadataPages because it can dirty more free-image segments
	// atomically. Both geometries spend this baseline, which is why it is named
	// once rather than repeated as a literal in each.
	fileStoreMetadataReservePages = 55
)

// fileStoreTransactionExtentBytes converts a descriptor ceiling into a
// physical-byte ceiling by assigning the widest independently possible extent
// classes first. Maximum extents cover overflow, primary leaves, and exact
// pages. One global catalog root is 64 KiB. Each independently dirty primary
// bucket can then own four 8 KiB routing parents. Remaining free-log and state
// metadata are PageSize. Sharing or reusable placement can only reduce the
// resulting append span.
func fileStoreTransactionExtentBytes(
	totalPages, maximumExtentPages, routingParentPages, pageSize, maxPageSize int,
) uint64 {
	remaining := max(0, totalPages)
	take := func(want int) int {
		got := min(max(0, want), remaining)
		remaining -= got
		return got
	}
	root := take(1)
	maximum := take(maximumExtentPages)
	routing := take(routingParentPages)
	bytes := fileStoreSaturatingByteProduct(maximum, maxPageSize)
	bytes = fileStoreSaturatingByteAdd(bytes,
		fileStoreSaturatingByteProduct(root, storeio.GlobalTabletCatalogRootBytes))
	bytes = fileStoreSaturatingByteAdd(bytes,
		fileStoreSaturatingByteProduct(routing, storeio.GlobalTabletCatalogNodeBytes))
	return fileStoreSaturatingByteAdd(bytes,
		fileStoreSaturatingByteProduct(remaining, pageSize))
}

// fileStoreTransactionResidentBytes charges maximum-size dirty frames only for
// page classes that can actually retain that width; every other descriptor is
// a base-page metadata frame. Unlike persisted extent accounting, this excludes
// alignment and routing-parent padding that never enters the resident cache.
func fileStoreTransactionResidentBytes(
	totalPages, maximumPages, pageSize, maxPageSize int,
) uint64 {
	maximum := min(max(0, maximumPages), max(0, totalPages))
	return fileStoreSaturatingByteAdd(
		fileStoreSaturatingByteProduct(maximum, maxPageSize),
		fileStoreSaturatingByteProduct(
			max(0, totalPages-maximum), pageSize,
		),
	)
}

type normalizedFileStoreOptions struct {
	Options
	// maxTransactionBytes bounds resident dirty frame bytes. Persisted extent
	// placement has wider metadata classes and is bounded independently below.
	maxTransactionPages              int
	maxTransactionBytes              uint64
	maxTransactionPhysicalBytes      uint64
	singleDocumentTransactionPages   int
	singleDocumentTransactionBytes   uint64
	singleDocumentFreeFoldLimit      int
	freeFoldLimit                    int
	pageCatalog                      *storeio.CanonicalPageCatalog
	indexes                          []*store.ExactIndex
	skipIndexes                      []vibejson.CompiledPointer
	indexNameIDs                     map[string]uint32
	indexCatalogHash                 uint64
	primaryUnifiedOverlayBytes       int
	primaryUnifiedOverlayBuckets     int
	primaryUnifiedOverlayDirtyBytes  uint64
	primaryUnifiedOverlayParentBytes uint32
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

	// fileStoreExactStagePagesMin/Max bound the per-transaction staging
	// allowance for dirty spanned exact-index term leaves (see the
	// exactIndexPages derivation in normalized()). The allowance scales with
	// ResidentBytes — a quarter of residency at one MaxPageSize per leaf —
	// because the worst-case dirty transaction must remain resident; 256
	// leaves ≈ a full re-cut of a ~1M-posting index, far past what a bounded
	// overlay window can dirty.
	fileStoreExactStagePagesMin = 16
	fileStoreExactStagePagesMax = 256
)

func (o Options) normalized() (normalizedFileStoreOptions, error) {
	storeOptions, err := o.Collection.Normalized()
	if err != nil {
		return normalizedFileStoreOptions{}, err
	}
	o.Collection = storeOptions
	if o.OpaqueValues &&
		(len(o.Indexes) != 0 || len(o.SkipIndexes) != 0 ||
			o.Collection.Schema != nil || o.Collection.ShapeTapes ||
			o.Collection.Postings || o.Collection.ValueDict ||
			o.Collection.IndexOptions.MaxDepth != 0 ||
			o.Collection.IndexOptions.HashKeys) {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"vibedb: opaque values cannot enable JSON schema, index, or representation options",
		)
	}
	if o.PageSize == 0 {
		o.PageSize = 4096
	}
	if o.PageSize != 4096 {
		// The ordered primary graph fixes the base page at 4096; leaf extents grow
		// to MaxPageSize but every tree/root/metadata page is exactly one base page.
		return normalizedFileStoreOptions{}, ErrUnsupportedPageSize
	}
	if o.PhysicalCapacityBytes != 0 &&
		(o.PhysicalCapacityBytes > math.MaxInt64 ||
			o.PhysicalCapacityBytes%uint64(o.PageSize) != 0) {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"%w: PhysicalCapacityBytes must be page aligned and representable",
			ErrPhysicalCapacity,
		)
	}
	if o.PhysicalCapacityBytes != 0 {
		layout, layoutErr := storeio.MutableStoreLayout(uint32(o.PageSize))
		if layoutErr != nil || o.PhysicalCapacityBytes < layout.DataStart {
			return normalizedFileStoreOptions{}, fmt.Errorf(
				"%w: PhysicalCapacityBytes is smaller than the Store prefix",
				ErrPhysicalCapacity,
			)
		}
		// Sealed capacity serves rooted copy-on-write only. Deferred
		// canonical/journal acknowledgements can require more than one replay-time
		// checkpoint window when an opener selects smaller runtime batch arenas;
		// without a persisted physical-suffix certificate they are not safe to
		// serve.
		if o.Durability != DurabilityAsyncVisible || o.RecoveryJournal ||
			o.MaterializationDamageGranule != 0 {
			return normalizedFileStoreOptions{}, fmt.Errorf(
				"%w: sealed physical capacity requires rooted DurabilityAsyncVisible without a recovery journal or canonical materialization",
				ErrPhysicalCapacity,
			)
		}
	}
	if o.SealedRecoveryJournalBytes != 0 &&
		(o.SealedRecoveryJournalBytes > storeio.RecoveryJournalMaxCapacityBytes ||
			o.SealedRecoveryJournalBytes%storeio.RecoveryJournalMinSectorSize != 0 ||
			o.Durability != DurabilitySync) {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"%w: sealed recovery journal requires synchronous durability and a bounded sector-aligned capacity",
			ErrSealedJournalCapacity,
		)
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
			"vibedb: collection MaxRetiredExtents must be between 1 and %d",
			1<<24,
		)
	}
	if o.MaxBatchDocuments == 0 {
		o.MaxBatchDocuments = store.MaxChunkDocuments
	}
	if o.MaxBatchDocuments < 1 {
		return normalizedFileStoreOptions{}, fmt.Errorf("vibedb: collection MaxBatchDocuments must be positive")
	}
	batchKeyBytes, batchKeyBytesOK := fileStoreCheckedMul(
		o.MaxBatchDocuments, o.MaxKeyBytes,
	)
	minBatchBytes, minBatchBytesOK := fileStoreCheckedAdd(
		o.MaxDocumentBytes, batchKeyBytes,
	)
	if !batchKeyBytesOK || !minBatchBytesOK {
		return normalizedFileStoreOptions{}, fmt.Errorf("vibedb: collection batch byte bound overflows")
	}
	if o.MaxBatchBytes == 0 {
		valueBytes := defaultBatchValueBytes
		valueBytes = min(valueBytes, fileStoreSaturatingMul(
			o.MaxBatchDocuments, o.MaxDocumentBytes,
		))
		o.MaxBatchBytes = fileStoreSaturatingAdd(
			batchKeyBytes, max(o.MaxDocumentBytes, valueBytes),
		)
	}
	if o.MaxBatchBytes < minBatchBytes {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"vibedb: collection MaxBatchBytes must hold one maximum document and every batch key",
		)
	}
	if o.SealedRecoveryJournalBytes != 0 {
		if o.MaxBatchBytes > math.MaxInt-storeio.RecoveryConditionalHeaderSize {
			return normalizedFileStoreOptions{}, fmt.Errorf(
				"%w: conditional journal record bound overflows",
				ErrSealedJournalCapacity,
			)
		}
		required := storeio.RecoveryBatchRecordPaddedSizeForPayload(
			storeio.RecoveryJournalMinSectorSize,
			o.MaxBatchDocuments,
			o.MaxBatchBytes+storeio.RecoveryConditionalHeaderSize,
		)
		if required <= 0 || uint64(required) > o.SealedRecoveryJournalBytes {
			return normalizedFileStoreOptions{}, fmt.Errorf(
				"%w: largest conditional batch needs=%d capacity=%d",
				ErrSealedJournalCapacity, required,
				o.SealedRecoveryJournalBytes,
			)
		}
	}
	if o.Backend > BackendIOUring || o.ReadMode > ReadDirectRequire ||
		o.WriteMode > WriteDirectRequire ||
		o.Durability > DurabilityBufferedVisible ||
		o.CheckpointStrength > CheckpointFilesystem ||
		o.CheckpointStrength == CheckpointFilesystem &&
			(o.Durability != DurabilityBufferedVisible ||
				o.Backend != BackendPortable ||
				o.WriteMode != WriteBuffered) ||
		o.CommitCoalesce < 0 || o.CommitCoalesce > time.Second ||
		o.MaxPageSize < o.PageSize || o.MaxPageSize&(o.MaxPageSize-1) != 0 || o.MaxPageSize%o.PageSize != 0 ||
		o.MaxKeyBytes < 1 ||
		o.MaxKeyBytes > storeio.CommonPrimaryLeafMaxKeyBytes ||
		o.InlineValueBytes < 1 || o.MaxDocumentBytes < 1 ||
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
		o.PrefetchQueue < 1 || o.PrefetchQueue > 32768 ||
		o.QueueSlots < 0 || o.QueueSlots > 1<<16 {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"vibedb: invalid Store page, key, value, backend, durability, checkpoint, or read option",
		)
	}
	if len(o.Indexes) > fileStoreMaxLogicalIndexes {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"%w: collection supports at most %d logical index names",
			store.ErrIndexDefinition, fileStoreMaxLogicalIndexes,
		)
	}
	if len(o.SkipIndexes) > storeio.PageCatalogMaxSkipIndexes {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"vibedb: collection supports at most %d skip indexes",
			storeio.PageCatalogMaxSkipIndexes,
		)
	}
	inputSkipPaths := make([]string, len(o.SkipIndexes))
	for i, path := range o.SkipIndexes {
		owned := strings.Clone(path)
		if _, compileErr := vibejson.CompilePointer(owned); compileErr != nil {
			return normalizedFileStoreOptions{}, fmt.Errorf(
				"%w: skip path %d: %v", store.ErrIndexDefinition, i, compileErr,
			)
		}
		inputSkipPaths[i] = owned
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
			Indexes: inputIndexes, SkipPaths: inputSkipPaths, Schema: catalogSchema,
		},
	)
	if catalogErr != nil {
		return normalizedFileStoreOptions{}, fmt.Errorf(
			"%w: %v", store.ErrIndexDefinition, catalogErr,
		)
	}
	canonical := pageCatalog.Definition()
	compiledSkipIndexes := make([]vibejson.CompiledPointer, len(canonical.SkipPaths))
	for i, path := range canonical.SkipPaths {
		pointer, compileErr := vibejson.CompilePointer(path)
		if compileErr != nil {
			return normalizedFileStoreOptions{}, fmt.Errorf(
				"%w: canonical skip path %q: %v",
				store.ErrIndexDefinition, path, compileErr,
			)
		}
		compiledSkipIndexes[i] = pointer
	}
	o.SkipIndexes = slices.Clone(canonical.SkipPaths)
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
	for _, path := range o.SkipIndexes {
		catalogHash = fileIndexHashBytes(catalogHash, []byte{0x53, 0x4b, 0x50})
		catalogHash = fileIndexHashBytes(catalogHash, []byte(path))
		catalogHash = fileIndexHashBytes(catalogHash, []byte{0})
	}
	if len(compiled) == 0 && len(compiledSkipIndexes) == 0 &&
		o.Collection.Schema == nil {
		catalogHash = 0
	} else if catalogHash == 0 {
		// StateRoot reserves zero to mean that no exact catalog exists. FNV is
		// only a fast rejection key and an adversarial valid definition can
		// drive it to zero, so keep that sentinel out of the populated domain;
		// canonical bytes and their digest remain the authority.
		catalogHash = 1
	}
	overflowPayload := o.MaxPageSize - storeio.PageHeaderSize - storeio.PageTrailerSize - storeio.OverflowPagePayloadHeaderSize
	if overflowPayload <= 0 {
		return normalizedFileStoreOptions{}, fmt.Errorf("vibedb: collection overflow page has no payload")
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
	largestMetadataPageLimit := max(
		metadataPageLimit, singleDocumentMetadataPageLimit,
	)
	if metadataPageLimit < 0 || singleDocumentMetadataPageLimit < 0 ||
		largestMetadataPageLimit >= 32768 ||
		overflowPages >= 32768-largestMetadataPageLimit {
		return normalizedFileStoreOptions{}, fmt.Errorf("vibedb: collection MaxBatchDocuments or maximum document requires too many transaction pages")
	}
	docMaxTransactionPages := overflowPages + metadataPageLimit
	docSingleDocumentPages :=
		overflowPages + singleDocumentMetadataPageLimit
	// Dirty admission accounts for the bytes actually retained in cache. The
	// persisted checkpoint namespace is deliberately separate: routing parents
	// can occupy wider physical extent classes without retaining those padded
	// bytes in resident frames.
	batchLargePages := fileStoreSaturatingAdd(
		overflowPages, o.MaxBatchDocuments,
	)
	singleDocumentLargePages := fileStoreSaturatingAdd(overflowPages, 1)
	if len(compiled) != 0 {
		batchLargePages = fileStoreSaturatingAdd(batchLargePages, 1)
		singleDocumentLargePages = fileStoreSaturatingAdd(
			singleDocumentLargePages, 1,
		)
	}
	docMaxTransactionBytes := fileStoreTransactionResidentBytes(
		docMaxTransactionPages, batchLargePages, o.PageSize, o.MaxPageSize,
	)
	docSingleDocumentBytes := fileStoreTransactionResidentBytes(
		docSingleDocumentPages, singleDocumentLargePages,
		o.PageSize, o.MaxPageSize,
	)
	// Exact-index maintenance stages, in the same transaction as the document
	// mutation or checkpoint, the dirty term leaves the fold re-encoded plus
	// each physical index's ordered catalog pages and one
	// PagePrimaryExactRoot. Since the spanned-leaf format the dirty set is
	// variable — a window's touched terms name it — so the reservation is a
	// static allowance (fileStoreExactStagePages) covering every leaf a
	// bounded overlay window or a full re-cut of a multi-hundred-thousand-row
	// index can dirty, plus one catalog page per index and the root. A fold
	// whose dirty set ever exceeds the allowance fails its transaction closed
	// with ErrTooManyPages rather than corrupting; quiet windows stage only
	// the root and catalogs regardless of index size. Zero for an unindexed
	// collection, so the unindexed mutation reserves exactly what it did
	// before.
	exactIndexPages := 0
	exactIndexBytes := uint64(0)
	if len(compiled) != 0 {
		allowance := int(o.ResidentBytes / int64(4*o.MaxPageSize))
		allowance = min(
			max(allowance, fileStoreExactStagePagesMin),
			fileStoreExactStagePagesMax,
		)
		// Online index creation inherits the already allocated commit-buffer
		// pool. Admit it with the largest dirty-leaf window that fits that
		// fixed pool (never below the supported minimum) instead of requiring
		// the unindexed collection to have pre-sized for the maximum 256-leaf
		// window. A fold exceeding the admitted window still fails closed at
		// transaction staging; the common bounded window scales to the arena
		// actually owned by this collection.
		if o.BufferCount != 0 {
			available := o.BufferCount - docMaxTransactionPages -
				len(compiled) - 2 // catalogs, exact root, alternate root
			if available >= fileStoreExactStagePagesMin {
				allowance = min(allowance, available)
			}
		}
		exactIndexPages = allowance + len(compiled) + 1
		exactIndexBytes = uint64(exactIndexPages) * uint64(o.MaxPageSize)
	}
	maxTransactionPages := fileStoreSaturatingAdd(
		docMaxTransactionPages, exactIndexPages,
	)
	singleDocumentTransactionPages := fileStoreSaturatingAdd(
		docSingleDocumentPages, exactIndexPages,
	)
	compiledPackedPages := 0
	if len(compiled) != 0 {
		compiledPackedPages = 1
	}
	maxTransactionBytes := docMaxTransactionBytes + exactIndexBytes
	singleDocumentTransactionBytes := docSingleDocumentBytes + exactIndexBytes
	maxTransactionPhysicalBytes := fileStoreTransactionExtentBytes(
		maxTransactionPages,
		fileStoreSaturatingAdd(
			fileStoreSaturatingAdd(
				fileStoreSaturatingAdd(overflowPages, o.MaxBatchDocuments),
				compiledPackedPages,
			),
			exactIndexPages,
		),
		4*min(o.MaxBatchDocuments, filePrimaryPendingParentLimit),
		o.PageSize, o.MaxPageSize,
	)
	// The class-5 overlay is bounded independently of MaxBatchDocuments. Its
	// runtime dirty-bucket limit is the largest prefix of the fixed 1,024-bucket
	// directory that fits the collection's descriptor, retirement, and resident
	// byte budgets. That fold stages one maximum-size leaf per bucket, up to four
	// distinct rooted parent pages per bucket, the catalog root,
	// and the configured free-log fold reserve. The exact-index allowance is
	// shared with the ordinary batch geometry but must coexist with this wider
	// primary cut.
	//
	// Previously MaxBatchDocuments=1 sized the committer for only the point
	// transaction (about 141 descriptors in the competitive configuration).
	// Mature churn could then reach syncFreeLogFor after staging the primary
	// graph and run out of descriptors while writing its reusable extents. This
	// explicit overlay geometry makes the pressure fold a first-class bounded
	// transaction instead of relying on spare buffers.
	const primaryOverlayParentLevels = 4
	const primaryOverlayPagesPerBucket = primaryOverlayParentLevels + 1
	primaryOverlayFixedMetadataPages :=
		1 + freeFoldLimit + storeio.FreeLogMaxIndexPages +
			storeio.FreeLogMaxDeltaPages
	primaryOverlayFixedPages :=
		primaryOverlayFixedMetadataPages + exactIndexPages
	primaryOverlayFixedBytes :=
		uint64(primaryOverlayFixedMetadataPages)*uint64(o.PageSize) +
			exactIndexBytes
	primaryOverlayParentBytes :=
		uint64(primaryOverlayParentLevels) * uint64(o.PageSize)
	primaryOverlayPhysicalFixedBytes :=
		uint64(primaryOverlayFixedMetadataPages-1)*uint64(o.PageSize) +
			uint64(storeio.GlobalTabletCatalogRootBytes) + exactIndexBytes
	primaryOverlayPhysicalParentBytes := uint64(
		storeio.SegmentedTabletRouterAnchorPageBytes +
			storeio.GlobalTabletCatalogTabletBytes +
			2*storeio.GlobalTabletCatalogNodeBytes,
	)
	primaryOverlayTargetBytes := uint64(primaryUnifiedOverlayTargetBytes(
		o.PageSize, o.MaxKeyBytes, o.InlineValueBytes,
	))
	primaryOverlayBucketLimit := primaryUnifiedOverlayBuckets
	if o.OpaqueValues {
		primaryOverlayTargetBytes = 0
		primaryOverlayBucketLimit = 0
	}
	limitByPages := func(pageLimit int) {
		if pageLimit <= primaryOverlayFixedPages {
			primaryOverlayBucketLimit = 0
			return
		}
		primaryOverlayBucketLimit = min(
			primaryOverlayBucketLimit,
			(pageLimit-primaryOverlayFixedPages)/primaryOverlayPagesPerBucket,
		)
	}
	if o.BufferCount != 0 {
		// One descriptor remains for the alternate root.
		limitByPages(o.BufferCount - 1)
	}
	// NewCommitter retains QueueSlots*MaxPagesPerBatch descriptors. Apply its
	// exact arena bound here so widening the overlay cannot turn a previously
	// valid explicit BufferCount/QueueSlots pair into a construction-time error.
	limitByPages(storeio.MaxCommitDescriptors / fileVisibilitySlots(o.QueueSlots))
	limitByPages(o.MaxRetiredExtents)
	// Resident admission is byte-exact at runtime. The fixed share covers free
	// log/exact scratch and the overlay arena; every first dirty bucket then
	// reserves four parent pages plus either its certified routed extent or one
	// conservative MaxPageSize extent. This lets a 64 MiB collection represent
	// many compact 4/8/32 KiB leaves without pretending 1,024 leaves are all
	// 64 KiB, while shape-changing paths remain fully covered.
	primaryOverlayDirtyBytes := uint64(0)
	minimumDirtyBytes := uint64(o.PageSize) + primaryOverlayParentBytes
	// The preferred arena is not an admission cliff. Preserve room for one
	// complete dirty bucket, the fixed fold state, and the ordinary point-write
	// transaction, then take the largest page-aligned arena up to the preferred
	// window. This keeps small valid ResidentBytes configurations functional
	// while 64 MiB production configurations retain the full fast window.
	if o.ResidentBytes >= 0 {
		resident := uint64(o.ResidentBytes)
		reservedWithoutArena := max(
			primaryOverlayFixedBytes+uint64(o.MaxPageSize)+minimumDirtyBytes,
			maxTransactionBytes+uint64(o.MaxPageSize),
		)
		if resident <= reservedWithoutArena {
			primaryOverlayTargetBytes = 0
		} else {
			primaryOverlayTargetBytes = min(
				primaryOverlayTargetBytes,
				(resident-reservedWithoutArena)/uint64(o.PageSize)*uint64(o.PageSize),
			)
		}
	}
	residentFixed := primaryOverlayFixedBytes + primaryOverlayTargetBytes +
		uint64(o.MaxPageSize)
	if o.ResidentBytes < 0 || primaryOverlayTargetBytes < 64<<10 ||
		primaryOverlayBucketLimit == 0 ||
		uint64(o.ResidentBytes) <= residentFixed ||
		maxTransactionBytes+primaryOverlayTargetBytes+
			uint64(o.MaxPageSize) > uint64(o.ResidentBytes) {
		primaryOverlayBucketLimit = 0
	} else {
		available := uint64(o.ResidentBytes) - residentFixed
		worst := uint64(primaryOverlayBucketLimit) *
			(uint64(o.MaxPageSize) + primaryOverlayParentBytes)
		primaryOverlayDirtyBytes = min(available, worst)
		if primaryOverlayDirtyBytes < minimumDirtyBytes {
			primaryOverlayBucketLimit = 0
			primaryOverlayDirtyBytes = 0
		}
	}
	if primaryOverlayBucketLimit == 0 {
		// Overlay admission is one coherent geometry. Never retain an arena or
		// construct concurrent scratch when descriptor/retirement/resident limits
		// have disabled the dirty-bucket window.
		primaryOverlayTargetBytes = 0
		primaryOverlayDirtyBytes = 0
		primaryOverlayParentBytes = 0
	}
	primaryOverlayMetadataPages :=
		primaryOverlayParentLevels*primaryOverlayBucketLimit +
			primaryOverlayFixedMetadataPages
	primaryOverlayTransactionPages :=
		primaryOverlayBucketLimit + primaryOverlayMetadataPages +
			exactIndexPages
	primaryOverlayTransactionBytes := primaryOverlayFixedBytes +
		primaryOverlayDirtyBytes
	if primaryOverlayBucketLimit != 0 {
		maxTransactionPages = max(
			maxTransactionPages, primaryOverlayTransactionPages,
		)
		maxTransactionBytes = max(
			maxTransactionBytes, primaryOverlayTransactionBytes,
		)
		// The resident dirty ceiling already covers each leaf and four PageSize
		// parents. Persisted parents use wider extent classes, so add only that
		// per-bucket delta. No transaction can dirty more buckets than fit its
		// minimum resident leaf+parent charge. This persisted-offset ceiling is
		// intentionally not a cache admission charge: physical padding/alignment
		// is never retained as dirty resident memory.
		physicalDirtyBuckets := min(
			uint64(primaryOverlayBucketLimit),
			primaryOverlayDirtyBytes/minimumDirtyBytes,
		)
		primaryOverlayPhysicalBytes := primaryOverlayPhysicalFixedBytes +
			primaryOverlayDirtyBytes + physicalDirtyBuckets*(primaryOverlayPhysicalParentBytes-primaryOverlayParentBytes)
		maxTransactionPhysicalBytes = max(
			maxTransactionPhysicalBytes, primaryOverlayPhysicalBytes,
		)
	}
	if o.MaxRetiredExtents < maxTransactionPages {
		return normalizedFileStoreOptions{}, fmt.Errorf("vibedb: collection MaxRetiredExtents must retain one worst-case transaction")
	}
	if o.BufferCount == 0 {
		o.BufferCount = defaultBufferCount(maxTransactionPages, o.MaxPageSize)
	}
	if o.BufferCount <= maxTransactionPages || o.BufferCount > maxCollectionBuffers {
		return normalizedFileStoreOptions{}, fmt.Errorf("vibedb: collection BufferCount must exceed worst-case %d-page transaction", maxTransactionPages)
	}
	if o.ResidentBytes < 0 || uint64(o.ResidentBytes) < maxTransactionBytes {
		return normalizedFileStoreOptions{}, fmt.Errorf("vibedb: collection ResidentBytes cannot retain one worst-case dirty transaction")
	}
	primaryUnifiedOverlayBytes := int(primaryOverlayTargetBytes)
	normalized := normalizedFileStoreOptions{
		Options:                        o,
		maxTransactionPages:            maxTransactionPages,
		maxTransactionBytes:            maxTransactionBytes,
		maxTransactionPhysicalBytes:    maxTransactionPhysicalBytes,
		singleDocumentTransactionPages: singleDocumentTransactionPages,
		singleDocumentTransactionBytes: singleDocumentTransactionBytes,
		singleDocumentFreeFoldLimit:    singleDocumentFreeFoldLimit,
		freeFoldLimit:                  freeFoldLimit,
		pageCatalog:                    pageCatalog,
		indexes:                        compiled, indexNameIDs: indexNameIDs,
		skipIndexes:                      compiledSkipIndexes,
		indexCatalogHash:                 catalogHash,
		primaryUnifiedOverlayBytes:       primaryUnifiedOverlayBytes,
		primaryUnifiedOverlayBuckets:     primaryOverlayBucketLimit,
		primaryUnifiedOverlayDirtyBytes:  primaryOverlayDirtyBytes,
		primaryUnifiedOverlayParentBytes: uint32(primaryOverlayParentBytes),
	}
	if o.PhysicalCapacityBytes != 0 {
		initial, initialErr := initialCollectionPhysicalFileEnd(normalized)
		if initialErr != nil {
			return normalizedFileStoreOptions{}, initialErr
		}
		if initial > o.PhysicalCapacityBytes {
			return normalizedFileStoreOptions{}, fmt.Errorf(
				"%w: initial collection needs=%d ceiling=%d",
				ErrPhysicalCapacity, initial, o.PhysicalCapacityBytes,
			)
		}
	}
	return normalized, nil
}

// initialCollectionPhysicalFileEnd is the exact generation-one high-water for
// Create's empty ordered-primary graph. Keeping this derivation shared between
// option preflight and creation prevents a too-small sealed ceiling from
// touching the file before the builder discovers it cannot publish.
func initialCollectionPhysicalFileEnd(
	o normalizedFileStoreOptions,
) (uint64, error) {
	layout, err := storeio.MutableStoreLayout(uint32(o.PageSize))
	if err != nil {
		return 0, err
	}
	segments, ok := o.pageCatalog.SegmentCountFor(uint32(o.PageSize))
	if !ok || segments < 0 {
		return 0, fmt.Errorf(
			"%w: cannot derive initial collection catalog geometry",
			ErrPhysicalCapacity,
		)
	}
	result := layout.DataStart + uint64(segments)*uint64(o.PageSize) +
		uint64(storeio.CommonPrimaryLeafNarrowBytes) +
		uint64(storeio.SegmentedTabletRouterAnchorPageBytes) +
		uint64(storeio.GlobalTabletCatalogLocatorBytes) +
		uint64(storeio.GlobalTabletCatalogTabletBytes) +
		uint64(storeio.GlobalTabletCatalogNodeBytes) +
		uint64(storeio.GlobalTabletCatalogRootBytes)
	if len(o.indexes) != 0 {
		result += uint64(o.PageSize) // empty exact-index root
	}
	return result, nil
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
