package durable

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
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
	// ErrUnsupportedPageSize rejects any page size other than the fixed 4096-byte
	// base page. The ordered primary graph is a 4 KiB-page structure (leaf extents
	// grow to MaxPageSize); variable base page sizes are no longer supported, so
	// Create refuses a non-4096 PageSize and Open refuses a store that recorded one.
	ErrUnsupportedPageSize = errors.New("vibejson: collection page size must be 4096")
	// ErrPrimaryLeafSplitRequired reports that an insert landed on a leaf with
	// no room for it. It is an internal retry signal: the mutation path catches
	// it, commits an atomic leaf split as its own structural transaction, and
	// retries the insert, so a successful mutation never surfaces it. It reaches
	// a caller only when the bounded split-and-retry loop cannot make progress.
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
	// Indexes are exact scalar definitions maintained from their atomic durable
	// publication onward. CreateIndex can add definitions to a live collection
	// when its fixed transaction/resident arenas admit the resulting write path.
	// Canonical ordered path vectors assign stable on-disk physical IDs
	// independently of caller order. Differently named definitions with
	// identical ordered paths are logical aliases of one physical index: they
	// share posting maintenance and durable bytes while remaining independently
	// discoverable and queryable.
	Indexes []store.IndexDefinition
	// PageSize is the base page of the ordered primary graph. It must be 4096:
	// every tree, root, directory, and metadata page is exactly one base page.
	// Zero defaults to 4096; any other value is rejected on Create with
	// ErrUnsupportedPageSize, and Open of a store that recorded a different base
	// page fails the same way. It is no longer discovered from the file.
	PageSize int
	// MaxPageSize bounds a leaf/overflow extent, which grows from PageSize up to
	// this value (default 64 KiB). It must be a power-of-two multiple of PageSize.
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
	// fence; zero selects 32. It is a ceiling, not a target, and measurement
	// shows it is almost never the binding constraint: how many generations are
	// queued when the worker picks one up is. Raising it from 2 to 64 changes
	// neither the achieved group size nor throughput on any writer shape tested.
	// Reach for CommitCoalesce instead.
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
	// When false, a buffered-visible store still owns a smaller sibling journal.
	// Mutations remain volatile, but Flush can append and sync one ordered batch
	// for a complete bounded class-5 overlay instead of copying its physical
	// leaves. Exceptional mutations, pressure, and Close take a physical
	// checkpoint and recycle the journal.
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
	pages := documents
	pages += fileStorePrimaryBatchReservePages(documents)
	if indexes != 0 {
		pages += indexes * (documents + 2)
	}
	pages += fileStoreMetadataReservePages
	return pages
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
	return base + batchFreeFoldLimit(o, indexes) - storeio.FreeLogMaxFoldSegments
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
	indexCatalogHash               uint64
	primaryUnifiedOverlayBytes     int
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
	if o.PageSize == 0 {
		o.PageSize = 4096
	}
	if o.PageSize != 4096 {
		// The ordered primary graph fixes the base page at 4096; leaf extents grow
		// to MaxPageSize but every tree/root/metadata page is exactly one base page.
		return normalizedFileStoreOptions{}, ErrUnsupportedPageSize
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
			Indexes: inputIndexes, Schema: catalogSchema,
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
	if len(compiled) == 0 && o.Collection.Schema == nil {
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
	metadataPages := docMaxTransactionPages - largePages
	docMaxTransactionBytes := uint64(largePages)*uint64(o.MaxPageSize) +
		uint64(metadataPages)*uint64(o.PageSize)
	singleDocumentMetadataPages :=
		docSingleDocumentPages - largePages
	docSingleDocumentBytes :=
		uint64(largePages)*uint64(o.MaxPageSize) +
			uint64(singleDocumentMetadataPages)*uint64(o.PageSize)
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
	maxTransactionPages := docMaxTransactionPages + exactIndexPages
	singleDocumentTransactionPages := docSingleDocumentPages + exactIndexPages
	maxTransactionBytes := docMaxTransactionBytes + exactIndexBytes
	singleDocumentTransactionBytes := docSingleDocumentBytes + exactIndexBytes
	// The class-5 overlay is bounded independently of MaxBatchDocuments: point
	// mutations may touch as many as 64 routed buckets before pressure folds the
	// window. That fold stages one maximum-size leaf per bucket, up to four
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
	primaryOverlayMetadataPages :=
		primaryOverlayParentLevels*primaryUnifiedOverlayBuckets + 1 +
			freeFoldLimit + storeio.FreeLogMaxIndexPages +
			storeio.FreeLogMaxDeltaPages
	primaryOverlayTransactionPages :=
		primaryUnifiedOverlayBuckets + primaryOverlayMetadataPages +
			exactIndexPages
	primaryOverlayTransactionBytes :=
		uint64(primaryUnifiedOverlayBuckets)*uint64(o.MaxPageSize) +
			uint64(primaryOverlayMetadataPages)*uint64(o.PageSize) +
			exactIndexBytes
	maxTransactionPages = max(
		maxTransactionPages, primaryOverlayTransactionPages,
	)
	maxTransactionBytes = max(
		maxTransactionBytes, primaryOverlayTransactionBytes,
	)
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
	primaryUnifiedOverlayBytes := primaryUnifiedOverlayBudget(
		uint64(o.ResidentBytes), maxTransactionBytes,
		o.PageSize, o.MaxPageSize, o.MaxKeyBytes, o.InlineValueBytes,
	)
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
		indexCatalogHash:           catalogHash,
		primaryUnifiedOverlayBytes: primaryUnifiedOverlayBytes,
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
	root    storeio.StateRoot
	fileEnd uint64
	// freeHead is the newest delta page of the free log, or the zero reference
	// when the durable free set is empty. The inline root's cumulative free
	// delta reaches it directly, so the whole free set is replaceable without
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

	// writer is the collection-wide mutation gate. Ordinary mutation,
	// checkpoint, structural, and lifecycle paths retain its exclusive side;
	// the narrow buffered existing-row overlay lane takes the shared side so its
	// fallible staging can overlap across independent leaf buckets. Converting
	// the existing gate, rather than adding a second checkpoint lock, makes every
	// established writer.Lock site automatically fence the concurrent lane.
	writer sync.RWMutex
	// primaryOverlayPublish retains the unified overlay's single-producer
	// contract while allowing validation, canonicalization, routing, and leaf
	// inspection to run in parallel. Contended staged requests are flat-combined
	// into consecutive generations under one reader fence; structural writers
	// remain excluded by writer.
	primaryOverlayPublish primaryConcurrentPublisher
	// primaryConcurrentPressure elects one overflowed publisher request to take
	// the exclusive fold/fallback path. The remaining pressure cohort retries the
	// concurrent lane after that fold instead of stampeding writer.Lock.
	primaryConcurrentPressure sync.Mutex
	// primaryConcurrentStripes serialize the shared size/accounting state of one
	// routed leaf. The full BucketID is mixed before selecting a stripe, so leaves
	// inside one tablet do not collapse onto one writer token.
	primaryConcurrentStripes *[primaryConcurrentStripeCount]primaryConcurrentStripe
	// primaryConcurrentContexts is a fixed-count pool of writer-private JSON
	// tape/canonicalization workspaces. It is nil for collections that cannot use
	// the concurrent overlay lane.
	primaryConcurrentContexts *primaryConcurrentContextPool
	// mutationCombiner is a short, bounded arrival lane for synchronous
	// unindexed primary mutations. It shares one journal barrier across a
	// contended group and hands the group to the existing atomic Update path.
	mutationCombiner *primaryMutationCombiner
	mutationWait     sync.WaitGroup
	onlineIndexBuild atomic.Bool
	durabilityWait   sync.WaitGroup
	snapshotGate     sync.RWMutex
	// snapshotOrder is a process-local, lazily assigned identity used to
	// acquire several collections' snapshot gates in one global order. Names
	// are catalog-local and cannot provide that order when the same handles are
	// exposed through different catalogs.
	snapshotOrder atomic.Uint64
	closed        bool
	closeDone     bool
	// state is the writer's newest applied generation. Readers use
	// visibleState so synchronous commits cannot leak before their fence.
	state          atomic.Pointer[fileStoreState]
	visibleState   atomic.Pointer[fileStoreState]
	durableState   atomic.Pointer[fileStoreState]
	visibilityMu   sync.Mutex
	pendingVisible []filePendingState

	committer *storeio.Committer
	cache     *storeio.PageCache
	// primaryUnifiedOverlay is the bounded, generation-stamped mutable row
	// window for class-5 replacement updates. Its byte arena is carved out of
	// Options.ResidentBytes; PageCache owns the remainder.
	primaryUnifiedOverlay *primaryUnifiedOverlay
	// primaryUnifiedReplacementScratch is the exclusive writer's bounded fold
	// workspace. Keeping it on the collection avoids promoting a 4,096-entry
	// stack array to the heap each time overlay pressure materializes a leaf.
	primaryUnifiedReplacementScratch []storeio.CommonPrimaryUnifiedReplacement
	// primaryUnifiedSeen is writer-owned lazy route metadata. Until the first
	// class-5 leaf is observed, ordinary stores preserve their established
	// capacity-before-acquire mutation order; afterwards class-5 routes may try
	// the allocation-free overlay before reserving structural COW capacity.
	primaryUnifiedSeen bool
	// primaryRouter is swapped wholesale by a structural split or empty reclaim
	// transaction and mutated in place by ordinary COW UpdateLeaf. Lock-free
	// point reads load it once; an atomic pointer keeps that swap race-free.
	primaryRouter atomic.Pointer[storeio.ResidentPrimaryRouter]
	// primaryEpoch is the resident exact-index epoch: the immutable fold base
	// (encoded term leaves plus flat live table) under the generation-stamped
	// overlay. Non-nil only for an indexed ordered-primary collection. The
	// pointer is swapped whole under
	// the writer/snapshotGate discipline at fold points; between folds the
	// exclusive writer appends overlay records inside the publish fence.
	// Retired epochs wait on the generation-keyed pending list until the
	// reclaim floor passes them, then recycle through the pool so steady-state
	// fold cycles reuse overlay storage instead of allocating it.
	primaryEpoch        *primaryExactEpoch
	primaryEpochRetired []retiredPrimaryExactEpoch
	primaryEpochPool    []*primaryExactEpoch
	// exactTermLinkScratch/exactTileLinkScratch are writer-owned staging for
	// one mutation's prepared overlay chain heads (fallible prepare,
	// infallible link at publish).
	exactTermLinkScratch []primaryExactTermLink
	exactTileLinkScratch []primaryExactTileLink
	// fold* slices are the streamed checkpoint fold's writer-owned scratch:
	// the resolved changed-tile set, per-index entry ordering, per-term
	// overlay tiles, the flat term/posting builder inputs, and the base-key
	// arena with its rebind offsets. All transient within one fold call
	// (AppendIndexTermLeaf copies everything), so reuse is safe and keeps
	// the steady-state fold free of resolve-side allocation.
	foldChangedTiles   []primaryExactProbeTile
	foldEntryScratch   []*primaryExactTermEntry
	foldTileScratch    []primaryExactProbeTile
	foldTermScratch    []storeio.IndexTermLeafTerm
	foldPostingScratch []storeio.IndexTermLeafPosting
	foldKeyArena       []byte
	foldKeyOffsets     []int
	// foldPlanScratch/foldRunScratch/foldDirtyRunScratch are the dirty-leaf
	// fold's classification scratch: one plan per touched (index, term), the
	// rule-1 run decomposition of the current leaf set, and the dirty-run
	// marks. Transient within one per-index fold pass.
	foldPlanScratch     []primaryExactFoldPlan
	foldRunScratch      []int
	foldDirtyRunScratch []bool
	// structuralExactReencoded and structuralExactRemoved accumulate, within one
	// bounded structural transaction, the exact-index contribution of every leaf
	// the transaction re-encodes plus the buckets it removes outright, so the
	// affected postings are rebuilt atomically with the tablet (see
	// structuralRepairPostingsHook). They are writer-owned scratch reset at the
	// start of each structural transaction.
	structuralExactReencoded map[storeio.BucketID]*structuralBucketContribution
	structuralExactRemoved   []storeio.BucketID
	readFile                 *os.File
	writeFile                *os.File
	directRead               bool
	directWrite              bool
	leases                   *storeio.GenerationLeases
	// readEpochs is the direct-read fast path's reader registry. A point read
	// claims one epoch slot instead of a snapshot-gate round trip plus a
	// mutex-guarded generation lease; long-lived Snapshots keep their leases.
	// Writer-side decisions that consult reader presence must combine both
	// tables (anyActiveReaders) inside a reader fence.
	readEpochs    *storeio.ReadEpochs
	reclaimer     *storeio.ExtentReclaimer
	pageValidator *fileStorePageValidator
	// journal is the bounded redo log paired with every primary
	// DurabilityBufferedVisible and DurabilitySync store. It is owned by the
	// serialized writer exactly like the committer and appended and synced under
	// c.writer. Options.RecoveryJournal selects per-mutation durable
	// acknowledgement for buffered-visible; without it, Flush may append one
	// complete class-5 delta batch. Full checkpoints fold and recycle the log.
	// journalID mirrors the root identity so recovery cannot pair a stray file;
	// journalPowerSafe selects the barrier strength from CheckpointStrength.
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
	// journalGroup is the buffered-journal lane's flat-combining sync sequencer.
	// A caller appends its redo record under writer, releases writer, and blocks
	// on this group's fence; one leader shares one journal sync across every
	// caller whose record it covers. See store_file_journal_group.go.
	journalGroup journalCommitGroup
	// journalDeltaGeneration is the newest ordinary buffered-visible generation
	// made crash-safe by a checkpoint batch in the recovery journal. It is a
	// logical durability watermark: allocator/recycle decisions continue to use
	// the committer's physical root generation, while DurableGeneration reports
	// the maximum of the two. It advances only after the journal sync succeeds.
	journalDeltaGeneration atomic.Uint64
	// journalDeltaAppendedGeneration is the newest complete ordinary-buffered
	// overlay suffix appended to the journal. It may be newer than
	// journalDeltaGeneration when non-aligned overlay pressure carries a suffix
	// before a device-silent fold. A later explicit Flush syncs every append
	// through this watermark with one fence, then advances the durable watermark.
	journalDeltaAppendedGeneration atomic.Uint64
	// journalDeltaEntries is fixed writer-owned framing scratch for one cheap
	// checkpoint. The class-5 overlay itself is capped at this record count, so
	// every eligible interval fits without allocation.
	journalDeltaEntries [primaryUnifiedOverlayRecords]storeio.RecoveryBatchEntry
	// writeTransaction and the point-mutation scratch below are protected by
	// writer, so no transaction can overlap a Reset.
	writeTransaction storeio.WriteTransaction

	automaticCheckpoints                 atomic.Uint64
	primaryOverlayFolds                  atomic.Uint64
	concurrentPrimaryReplaces            atomic.Uint64
	concurrentPrimaryFallbacks           atomic.Uint64
	concurrentPrimaryPublishGroups       atomic.Uint64
	concurrentPrimaryLargestPublishGroup atomic.Uint64
	retirementPressureCheckpoints        atomic.Uint64
	materializationAttempts              atomic.Uint64
	materializationUpdates               atomic.Uint64
	materializationFallbacks             atomic.Uint64
	materializationSnapshotSkips         atomic.Uint64
	materializationBusySkips             atomic.Uint64
	// journalAcks counts frame-deferred mutations made durable by a single journal
	// append plus one sync, the redo lane's fast acknowledgement. chainAcks counts
	// mutations whose durability instead came from a committer root fence — the
	// snapshot-contended chain path and every forced or explicit checkpoint. The
	// split is how a bench distinguishes the bounded-append lane from the
	// full-publication lane at the store level.
	journalAcks atomic.Uint64
	chainAcks   atomic.Uint64
	// journalSyncs counts shared journal syncs a group-commit leader issued;
	// journalLargestGroup is the most records one such sync covered. JournalAcks
	// divided by JournalSyncs is the average group size provided by journal
	// group commit.
	journalSyncs              atomic.Uint64
	journalLargestGroup       atomic.Uint32
	journalDeltaCheckpoints   atomic.Uint64
	journalDeltaRecords       atomic.Uint64
	journalDeltaBytes         atomic.Uint64
	journalDeltaFullFallbacks atomic.Uint64
	primaryLeafSplitRequired  atomic.Uint64
	primaryEmptyLeaves        atomic.Uint64
	// Structural-transaction accounting for split and empty-leaf reclaim. The
	// *MaxNS fields are the observed high-water bounded-transaction latency so a
	// harness can gate p-max without a full histogram allocation.
	primaryLeafSplits         atomic.Uint64
	primaryEmptyReclaims      atomic.Uint64
	primaryMacroSplitRequired atomic.Uint64
	primarySplitMaxNS         atomic.Uint64
	primaryEmptyReclaimMaxNS  atomic.Uint64

	retireScratch []storeio.FreeExtent
	// retireRefScratch mirrors exact PageRefs opportunistically for cache
	// cleanup. retireScratch remains the authoritative durable/reclaimer list
	// and may coalesce adjacent refs; this list never affects correctness when
	// its fixed capacity is exhausted.
	retireRefScratch      []storeio.PageRef
	reusable              []storeio.FreeExtent
	reuseJournal          []storeio.ReuseEdit
	reusableBlock         *storemem.Block
	freeExtentIndex       storeio.FreeExtentIndex
	freeExtentMaxima      []uint64
	freeScratchBlock      *storemem.Block
	materializationBlock  *storemem.Block
	materializationBefore []byte
	materializationAfter  []byte
	primaryLeafScratch    []byte
	// primaryLeafMutationScratch is the second writer-owned leaf buffer plus
	// row/render workspace used when the structural mutation bridge opens a compressed
	// leaf for mutation. It must be distinct from primaryLeafScratch because
	// the decoded view remains the source while that buffer receives the
	// mutated image.
	primaryLeafMutationScratch *storeio.PrimaryLeafMutationScratch
	primaryUnifiedBuilder      *storeio.UnifiedPrimaryLeafBuilder
	primaryUnifiedIndexScratch []vibejson.IndexEntry
	primaryUnifiedCanonical    []byte
	primaryUnifiedCanonicalWS  storeio.CanonicalWorkspace
	primaryRootScratch         []byte
	primaryPendingParents      []filePrimaryPendingParent
	primaryVolatileRetired     []storeio.PageRef
	// overflowChainScratch and overflowOffsetScratch stage one out-of-line value's
	// overflow-extent chain during a mutation transaction: the reserved transaction
	// pages and each piece's start offset in the value. They are writer-private and
	// reused per Put so a steady-state overflow write allocates nothing here.
	overflowChainScratch  []storeio.TransactionPage
	overflowOffsetScratch []int
	// overflowRetireScratch enumerates a superseded value's overflow-extent chain
	// for retirement when a Put replaces or a Delete removes an out-of-line value.
	overflowRetireScratch []storeio.PageRef
	// overflowRefScratch and overflowPageScratch stage one out-of-line value's
	// VOLATILE overflow chain on the deferred-canonical lane: the freshly minted
	// extent identities and one reusable MaxPageSize encode buffer admitted through
	// AdmitBufferedDirty. overflowValueScratch reassembles a volatile chain's value
	// when the checkpoint re-mints it durable or the exact index derives its terms.
	// All three are writer-private and reused, so a steady-state buffered overflow
	// Put allocates nothing here.
	overflowRefScratch   []storeio.PageRef
	overflowPageScratch  []byte
	overflowValueScratch []byte
	// primaryMutationAdmitted tracks every buffered-dirty frame one
	// single-document deferred-canonical mutation admits before its point of no
	// return: the minted overflow-chain extents, then the rewritten leaf. A
	// fallible step after the first admission (collecting the superseded chain,
	// the retired-extent capacity check, the exact-index prepare, the sync-lane
	// journal fence) must hand these frames back to the cache on error, or a
	// retryable failure would leak up to a whole MaxDocumentBytes chain of
	// unreachable dirty frames per attempt until Close. Writer-lock owned and
	// reused per mutation, so the steady-state cost is zero allocations — the
	// single-document mirror of batchPrimaryAdmitted.
	primaryMutationAdmitted []storeio.PageRef
	// primaryPendingOverflowRetire holds the durable overflow-extent chains a
	// buffered Put or Delete superseded since the last checkpoint. Their pages are
	// on device (a prior checkpoint minted them), so unlike a superseded volatile
	// chain they cannot simply be dropped: the next materialize retires them
	// against the checkpoint base through the ordinary reclaim accounting. Writer-
	// lock owned; drained and truncated by materializePrimaryParentsLocked.
	primaryPendingOverflowRetire []storeio.PageRef
	// primaryCheckpointVolatileOverflow accumulates the extents of every volatile
	// overflow chain a materialize re-minted durable, so their memory-only frames
	// are dropped once the checkpoint publishes -- the overflow analogue of a
	// superseded volatile leaf. Writer-lock owned; reused each materialize.
	primaryCheckpointVolatileOverflow []storeio.PageRef
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
	// structuralRows is reused row scratch for leaf re-encoding. Its
	// records borrow the source leaf page and are valid only while that page is
	// leased inside the structural transaction.
	structuralRows  []storeio.CommonPrimaryLeafRecord
	pointKeyScratch []byte
	// schemaIndexScratch is the writer-only IndexEntry arena reused to build the
	// per-document index that schema enforcement validates. It is only touched
	// when the collection carries a declared schema, so the common (schemaless)
	// hot path never allocates it.
	schemaIndexScratch []vibejson.IndexEntry
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

	// batch is the reusable transactional WriteBatch handle. The batch type and
	// its options are shared; only the primary apply path remains.
	batch *WriteBatch

	// Ordered-primary batch scratch. One Update over the primary graph resolves
	// every mutation, rewrites one frame per touched leaf, and publishes them all
	// under one generation; these are reset per Update and reused so a batch's
	// steady-state cost is the frames it publishes, not the slices it plans with.
	// batchPrimaryLeafArena holds the finalized image of every touched leaf at
	// once (they must coexist until the single admit-all step), and
	// batchPrimaryLeafImage is the rolling accumulator for one leaf whose several
	// mutations fold onto the same frame.
	batchPrimaryLeaves       []primaryBatchLeaf
	batchPrimaryMutations    []primaryBatchMutation
	batchJournalEntries      []storeio.RecoveryBatchEntry
	batchPrimaryAdmitted     []storeio.PageRef
	batchPrimaryPrevVolatile []storeio.PageRef
	batchPrimaryLeafArena    []byte
	batchPrimaryLeafImage    []byte
	batchPrimaryFileEnd      uint64
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
	// SupersededRootWrites/Bytes count buffered alternate-superblock staging
	// buffers returned before checkpoint because only a newer root can be
	// selected.
	SupersededRootWrites uint64
	SupersededRootBytes  uint64
	// TailWitnessWrites/Bytes count unreachable pages still submitted because
	// they alone extended the file through the published FileEnd.
	TailWitnessWrites uint64
	TailWitnessBytes  uint64
	// PrewrittenPageWrites/Bytes count sealed buffered pages written without a
	// barrier or root publication while the checkpoint worker was idle.
	PrewrittenPageWrites uint64
	PrewrittenPageBytes  uint64
	// AutomaticCheckpoints counts unrequested persistence boundaries forced by
	// bounded staging pressure. Device-silent row-overlay materializations are
	// accounted separately by PrimaryOverlayFolds.
	AutomaticCheckpoints uint64
	// PrimaryOverlayFolds counts device-silent materializations of the bounded
	// class-5 row overlay. These publish a physical root in memory but perform no
	// device checkpoint; a later journal delta, explicit Flush, or Close supplies
	// the crash-safety boundary.
	PrimaryOverlayFolds uint64
	// ConcurrentPrimaryReplaces counts existing inline rows published through
	// the shared-writer, bucket-striped buffered lane. It advances only after the
	// complete overlay/router/state publication, so qualification can detect a
	// benchmark that silently fell back to the exclusive mutation path.
	ConcurrentPrimaryReplaces uint64
	// ConcurrentPrimaryFallbacks counts successful members of a concurrent
	// pressure cohort that were elected to take the exclusive fold/retry path.
	// Together with ConcurrentPrimaryReplaces it accounts for every successful
	// operation admitted by the concurrent replacement entry lane.
	ConcurrentPrimaryFallbacks uint64
	// ConcurrentPrimaryPublishGroups counts non-empty replacement groups made
	// visible by the concurrent publisher. Dividing ConcurrentPrimaryReplaces
	// by this value yields its lifetime mean successful group size.
	ConcurrentPrimaryPublishGroups uint64
	// ConcurrentPrimaryLargestPublishGroup is the largest successful prefix
	// made visible by one concurrent publisher group.
	ConcurrentPrimaryLargestPublishGroup uint64
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
	// JournalAcks counts frame-deferred mutations acknowledged through a single
	// recovery-journal append plus one sync, with the root publication deferred to
	// the next checkpoint. ChainAcks counts mutations whose durability instead
	// came from a committer root fence: the snapshot-contended chain path and
	// every forced or explicit checkpoint. Both are zero unless a recovery journal
	// is configured.
	JournalAcks uint64
	ChainAcks   uint64
	// JournalSyncs counts the shared journal syncs the buffered-journal
	// group-commit leader issued, and JournalLargestGroup the most records a
	// single such sync covered. JournalAcks/JournalSyncs is the average
	// group-commit fan-out: 1 for a lone writer, higher as concurrent callers
	// share one fence. Both are zero unless a recovery journal is configured.
	JournalSyncs        uint64
	JournalLargestGroup uint32
	// JournalDelta* accounts for ordinary buffered-visible journal checkpoints.
	// Checkpoints counts explicit Flush fences. Records is the number of logical
	// overlay mutations appended (including an unsynced pressure carry), Bytes
	// their sector-padded journal traffic, and FullFallbacks counts attempted
	// delta paths that deliberately fell back to the full COW/root path.
	JournalDeltaCheckpoints   uint64
	JournalDeltaRecords       uint64
	JournalDeltaBytes         uint64
	JournalDeltaFullFallbacks uint64
	// PrimaryLeafSplitRequired counts inserts rejected before publication
	// because the selected wide leaf needs the deferred structural split.
	PrimaryLeafSplitRequired uint64
	// PrimaryEmptyLeaves counts routed leaves currently empty (made empty and
	// not yet refilled or reclaimed). The counter is rebuilt from zero on Open.
	PrimaryEmptyLeaves uint64
	// PrimaryLeafSplits and PrimaryEmptyReclaims count the bounded structural
	// transactions performed this session.
	PrimaryLeafSplits    uint64
	PrimaryEmptyReclaims uint64
	// PrimaryMacroSplitRequired counts structural transactions that could not
	// proceed because a tablet's 4096 local IDs or 16 anchor pages are exhausted
	// and a macro-tablet split (the next phase) is required.
	PrimaryMacroSplitRequired uint64
	// PrimarySplitMaxNS and PrimaryEmptyReclaimMaxNS report the high-water
	// bounded-transaction latency in nanoseconds for each structural kind.
	PrimarySplitMaxNS        uint64
	PrimaryEmptyReclaimMaxNS uint64
	// PrimaryMutationScratchBytes is the fixed leaf-promotion and raw
	// segmented-root writer scratch plus the bounded unified-fold replacement
	// vector, allocated only for PrimaryRoot stores.
	PrimaryMutationScratchBytes uint64
	// ConcurrentPrimaryScratchBytes is the retained stripe directory, publisher
	// handoff, and fixed writer-private canonicalization context pool for the
	// eligible concurrent replacement lane. It is zero for ineligible stores.
	ConcurrentPrimaryScratchBytes uint64
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
	FreeScratchLiveBytes  uint64
	PendingRetiredExtents uint64
	PendingRetiredBytes   uint64
	ReusableExtents       uint64
	ReusableBytes         uint64
	DocumentCount         uint64
	FileEnd               uint64
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
	// A freshly created store is ordered-primary from its first byte:
	// createInitialState publishes an empty primary graph, and the committer's
	// deferred-root checkpoint mode is selected for the buffered and journal-backed
	// synchronous lanes exactly as it is for an opened primary store.
	collection, err := newCollectionResources(file, normalized, storeID)
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
	// The ordered-primary root is mandatory in the sole current format.
	if root.PrimaryRoot == (storeio.PageRef{}) {
		return nil, fmt.Errorf(
			"vibejson: collection has no ordered-primary root",
		)
	}
	if root.PageSize != uint32(normalized.PageSize) ||
		root.MaxPageSize != uint32(normalized.MaxPageSize) {
		return nil, fmt.Errorf("vibejson: collection options or unsupported durable catalog mismatch")
	}
	collection, err := newCollectionResources(file, normalized, root.StoreID)
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
	freeHead := inline.FreeDelta.ExternalPrev()
	state := &fileStoreState{
		root: root, fileEnd: inline.FileEnd,
		freeHead: freeHead,
	}
	collection.inlineFree = inline.FreeDelta
	collection.pageValidator.update(state)
	if err := validateOpenedPrimaryGraph(
		collection.cache, root, state.fileEnd,
	); err != nil {
		_ = collection.closeResources()
		return nil, err
	}
	if err := collection.setupResidentPrimaryLocked(state); err != nil {
		_ = collection.closeResources()
		return nil, err
	}
	collection.initializeFileState(state)
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
		if err := collection.openRecoveryJournalLocked(
			root.JournalID, root.Generation,
		); err != nil {
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

// newCollectionResources builds the committer and cache for a collection. The
// committer runs in deferred-root manual checkpoint mode on every lane except
// async-visible: both buffered-visible and the journal-backed synchronous lane
// stage canonical frames and publish the root at checkpoints rather than
// fencing one per mutation.
func newCollectionResources(file *os.File, options normalizedFileStoreOptions, storeID [16]byte) (*Collection, error) {
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
		ManualCheckpoint:             options.Durability != DurabilityAsyncVisible,
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
	)
	cache, err := storeio.NewPageCache(readFile, storeio.PageCacheOptions{
		PageSize: options.PageSize, MaxPageSize: options.MaxPageSize,
		ResidentBytes: options.ResidentBytes -
			int64(options.primaryUnifiedOverlayBytes),
		StoreID:       storeID,
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
	readEpochs := storeio.NewReadEpochs()
	reclaimer, err := storeio.NewExtentReclaimer(
		leases,
		storeio.ExtentReclaimerOptions{
			MaxRetiredExtents:    options.MaxRetiredExtents,
			Epochs:               readEpochs,
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
	concurrentContexts := newPrimaryConcurrentContextPool(options)
	var concurrentStripes *[primaryConcurrentStripeCount]primaryConcurrentStripe
	if concurrentContexts != nil {
		concurrentStripes = new(
			[primaryConcurrentStripeCount]primaryConcurrentStripe,
		)
	}
	collection := &Collection{
		file: file, options: options, storeID: storeID, committer: committer, cache: cache,
		primaryUnifiedOverlay: newPrimaryUnifiedOverlay(
			options.primaryUnifiedOverlayBytes,
		),
		readFile: ownedRead, writeFile: ownedWrite,
		directRead: directRead, directWrite: directWrite,
		leases: leases, readEpochs: readEpochs, reclaimer: reclaimer,
		// A fold retires the whole superseded chain on top of the commit's own
		// retirements, so the scratch reserves both.
		retireScratch: make([]storeio.FreeExtent, 0, options.maxTransactionPages+
			fileStorePointPrimaryRetirePages+1+
			storeio.FreeLogMaxChainPages+storeio.FreeLogMaxIndexPages+
			options.freeFoldLimit),
		retireRefScratch: make([]storeio.PageRef, 0, options.maxTransactionPages+
			fileStorePointPrimaryRetirePages+1+
			storeio.FreeLogMaxChainPages+storeio.FreeLogMaxIndexPages+
			options.freeFoldLimit),
		reusable:         reusableArena[:0],
		reuseJournal:     make([]storeio.ReuseEdit, 0, options.maxTransactionPages),
		reusableBlock:    reusableBlock,
		freeExtentMaxima: freeExtentMaxima,
		freeScratchBlock: freeScratchBlock,
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
		pendingVisible:     make([]filePendingState, fileVisibilitySlots(options.QueueSlots)),
		mutationCombiner: newPrimaryMutationCombiner(
			options.QueueSlots, options.MaxBatchDocuments, options.MaxBatchBytes,
		),
		primaryConcurrentContexts: concurrentContexts,
		primaryConcurrentStripes:  concurrentStripes,
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

// createInitialState builds a fresh, empty ordered-primary collection. A newly
// created store is primary-layout from its first byte: the root names an empty
// primary graph (one empty leaf spanning the whole key range) and, when the
// collection is indexed, an empty exact-index root. The first Put routes to that
// empty leaf and fills it. A configured schema is recorded in the root's option
// flags and catalog and enforced on every primary Put.
func (c *Collection) createInitialState() error {
	if c.options.PageSize != 4096 {
		return fmt.Errorf(
			"vibejson: ordered-primary collection requires 4 KiB pages",
		)
	}
	if uint32(c.options.MaxPageSize) < storeio.GlobalTabletCatalogRootBytes {
		return fmt.Errorf(
			"vibejson: ordered-primary collection requires a %d-byte maximum page",
			storeio.GlobalTabletCatalogRootBytes,
		)
	}
	layout, err := storeio.MutableStoreLayout(uint32(c.options.PageSize))
	if err != nil {
		return err
	}
	catalog, err := planFilePageCatalog(
		c.options.pageCatalog, c.cacheStoreID(), 1,
		uint32(c.options.PageSize), layout.DataStart,
		1,
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
	// One leaf/tablet/catalog for the empty graph, plus one exact-index root when
	// indexed. PublishInline uses the Batch's dedicated alternate-root buffer, so
	// it does not consume a transaction-page slot.
	reserve := storeio.EmptyPrimaryGraphPageCount
	if len(c.options.indexes) != 0 {
		reserve++
	}
	// The primary graph draws its dynamically allocated pages from the reserved
	// primary namespace, so the transaction starts at PrimaryFirstDynamicLogicalID
	// exactly as CreateFromPrimary does; the page catalog's own logical IDs live
	// below that range and are written with catalog.nextID above.
	tx, err := c.beginWriteTransaction(reserve, storeio.WriteTransactionOptions{
		StoreID: c.cacheStoreID(), Generation: 1, PageSize: uint32(c.options.PageSize),
		FileEnd: initialFileEnd, NextLogicalID: storeio.PrimaryFirstDynamicLogicalID,
	})
	if err != nil {
		return err
	}
	primaryRoot, err := storeio.BuildEmptyPrimaryGraph(tx)
	if err != nil {
		_ = tx.Abort()
		return err
	}
	exactIndexRoot, err := buildPrimaryExactIndexes(
		tx, nil, nil, c.options.indexes,
		uint32(c.options.PageSize), uint32(c.options.MaxPageSize),
	)
	if err != nil {
		_ = tx.Abort()
		return err
	}
	root := storeio.StateRoot{
		StoreID: c.cacheStoreID(), Generation: 1, PageSize: uint32(c.options.PageSize),
		NextLogicalID: tx.NextLogicalID(),
		IndexCount:    uint32(len(c.options.indexes)), IndexCatalogHash: c.options.indexCatalogHash,
		IndexMaxDepth:    uint32(max(c.options.Collection.IndexOptions.MaxDepth, 0)),
		MaxKeyBytes:      uint32(c.options.MaxKeyBytes),
		InlineValueBytes: uint32(c.options.InlineValueBytes),
		MaxDocumentBytes: uint32(c.options.MaxDocumentBytes),
		PrimaryRoot:      primaryRoot,
		ExactIndexRoot:   exactIndexRoot,
	}
	root.Options = fileStoreCollectionOptionFlags(c.options.Collection)
	if c.options.MaterializationDamageGranule != 0 {
		root.Options |= storeio.StateOptionCanonicalMaterialization
		root.MaterializationDamageGranule =
			uint32(c.options.MaterializationDamageGranule)
	}
	if err := catalog.apply(&root, uint32(c.options.MaxPageSize)); err != nil {
		_ = tx.Abort()
		return err
	}
	// Mint the paired journal before the root that names it is published, so a
	// crash after the root is durable finds the journal file present. The
	// synchronous lane is journal-backed on the primary graph unconditionally --
	// it is how sync acknowledges -- and every buffered-visible store carries one
	// for either checkpoint deltas or per-mutation acknowledgement.
	// Async-visible never carries one. This is the
	// creation-time counterpart of CreateFromPrimary's journal mint.
	journalRequired := c.options.Durability == DurabilitySync ||
		c.journalConfigured()
	if journalRequired {
		var journalID [16]byte
		if _, err := rand.Read(journalID[:]); err != nil {
			_ = tx.Abort()
			return fmt.Errorf("vibejson: mint recovery journal identity: %w", err)
		}
		if err := createSiblingRecoveryJournal(
			c.file.Name(),
			recoveryJournalHeaderFor(
				c.cacheStoreID(), journalID, uint32(c.options.PageSize),
				c.options.MaxKeyBytes, c.options.InlineValueBytes,
				c.options.MaxDocumentBytes, 1,
				func() int {
					if c.buffered() && !c.options.RecoveryJournal {
						return c.options.primaryUnifiedOverlayBytes
					}
					return 0
				}(),
			),
		); err != nil {
			_ = tx.Abort()
			return err
		}
		root.JournalID = journalID
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
	state := &fileStoreState{root: root, fileEnd: tx.FileEnd()}
	c.inlineFree = inlineFree
	c.pageValidator.update(state)
	c.initializeFileState(state)
	c.freeLoaded = true
	if err := c.setupResidentPrimaryLocked(state); err != nil {
		return err
	}
	// Pair, replay, and recycle the fresh journal exactly as Open does, so a
	// created and an opened journaled collection reach an identical live state.
	// A freshly minted journal holds no records, so replay is a clean no-op.
	if journalRequired {
		if err := c.openRecoveryJournalLocked(root.JournalID, root.Generation); err != nil {
			return err
		}
		if err := c.replayRecoveryJournalLocked(root.Generation); err != nil {
			return err
		}
	}
	return nil
}

func (c *Collection) cacheStoreID() [16]byte {
	return c.storeID
}

// Put inserts or replaces one document. Every collection is an ordered primary
// graph, so the mutation is always resolved through the routed COW path.
//
// key is borrowed for the duration of the call and not retained after it
// returns: the store copies it wherever it stages the key (leaf frame,
// recovery-journal record). Callers may reuse or mutate the backing array as
// soon as Put returns.
func (c *Collection) Put(key []byte, src []byte) (created bool, err error) {
	if c == nil {
		return false, ErrClosed
	}
	handled, concurrentErr := c.tryConcurrentPrimaryReplace(key, src)
	if handled {
		return false, nil
	}
	if errors.Is(concurrentErr, errConcurrentPrimaryPressure) {
		// Exactly one member of a pressure cohort takes the exclusive fold/retry.
		// Later members first retry the fast lane after that fold, so a full
		// overlay cannot turn one combined group into N queued writer.Lock calls.
		c.primaryConcurrentPressure.Lock()
		handled, concurrentErr = c.tryConcurrentPrimaryReplace(key, src)
		if handled {
			c.primaryConcurrentPressure.Unlock()
			return false, nil
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
	if c.shouldQueuePrimaryMutation(key, nil) {
		_, deleted, err = c.submitPrimaryMutation(
			primaryMutationDelete, key, nil,
		)
		return deleted, err
	}
	return c.deletePrimaryWithEmptyReclaim(key)
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
	if c == nil {
		return nil, ErrClosed
	}
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
		if len(c.primaryPendingParents) != 0 ||
			c.primaryUnifiedOverlay.hasPending() {
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
// the writer instead. Snapshots hold a long-lived generation lease, not an
// epoch slot, so the reader-fence protocol does not enter here.
func (c *Collection) pinSnapshot() (*Snapshot, error) {
	c.snapshotGate.RLock()
	state, stateErr := c.readerFileState()
	if stateErr != nil {
		c.snapshotGate.RUnlock()
		return nil, stateErr
	}
	epoch := c.primaryEpoch
	indexes := c.options.indexes
	indexNameIDs := c.options.indexNameIDs
	indexDefinitions := c.options.Indexes
	lease, err := c.leases.Acquire(state.root.Generation)
	c.snapshotGate.RUnlock()
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		collection: c, state: state,
		indexes: indexes, indexNameIDs: indexNameIDs,
		indexDefinitions: indexDefinitions,
		epoch:            epoch, lease: lease,
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
		s.epoch = nil
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
	for range liveReadSupersededRetries {
		state, epoch, ok := c.enterReadEpoch()
		if !ok {
			break
		}
		out, found, superseded, err := c.resolvePrimaryGraphLive(dst, state, key)
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
	for range liveReadSupersededRetries {
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
		out, ok, superseded, err := c.resolvePrimaryGraphLive(dst, state, key)
		lease.Release()
		if !superseded {
			return out, ok, err
		}
	}
	// Terminal, guaranteed-progress resolve: hold the snapshot gate's read side
	// across the read. Every live publication pairs its router advance with its
	// state publish inside one gate write hold, so a state pinned under the
	// gate can never trail the router — the routed read is current, or the
	// router is behind a structural publish and that state's materialized
	// rooted graph serves exactly. This path is effectively unreachable (it
	// needs liveReadSupersededRetries consecutive publications to land inside
	// consecutive nanosecond windows); it exists so reader progress is a
	// guarantee rather than a probability.
	c.snapshotGate.RLock()
	state, stateErr := c.readerFileState()
	if stateErr != nil {
		c.snapshotGate.RUnlock()
		return dst, false, stateErr
	}
	lease, err := c.leases.Acquire(state.root.Generation)
	if err != nil {
		c.snapshotGate.RUnlock()
		return dst, false, err
	}
	out, ok, _, err := c.resolvePrimaryGraphLive(dst, state, key)
	c.snapshotGate.RUnlock()
	lease.Release()
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
	state := c.readerFileStateNoError()
	current := uint64(0)
	if state != nil {
		current = state.root.Generation
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
		PrimaryMutationScratchBytes: uint64(
			len(c.primaryLeafScratch)+len(c.primaryRootScratch),
		) + uint64(cap(c.primaryUnifiedReplacementScratch))*
			uint64(unsafe.Sizeof(storeio.CommonPrimaryUnifiedReplacement{})),
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
		stats.DocumentCount = state.root.DocumentCount
		stats.FileEnd = state.fileEnd
	}
	return stats
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

func (c *Collection) rememberRetiredRef(ref storeio.PageRef) {
	if ref == (storeio.PageRef{}) ||
		len(c.retireRefScratch) == cap(c.retireRefScratch) {
		return
	}
	c.retireRefScratch = append(c.retireRefScratch, ref)
}

// reserveFileRetirements hands the complete list to the reclaimer. It runs after
// syncFreeLogFor so that the free log's own superseded pages — which a fold only
// knows once it has decided to fold — are reserved with everything else, and so
// that a failure here still precedes Publish and rolls the whole commit back.
//
// A full retirement table is routed through absorbRetirementPressure so the
// error identifies either the reader pin or the undersized transaction bound.
// absorbRetirementPressure turns a retired-extent capacity failure into a
// caller-actionable error, distinguishing snapshot-pinned extents (close the
// snapshots) from a genuinely exhausted table (raise MaxRetiredExtents).
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

// retryRetirementAfterPressure forces a checkpoint (buffered) or flush to
// advance the durable floor, absorbs whatever that frees back into the reusable
// set, and retries the retirement once. It reports ErrRetiredExtentCapacity
// when no reader is present to free anything.
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

func (c *Collection) waitPublished(generation uint64) error {
	if err := c.committer.Wait(generation); err != nil {
		return err
	}
	c.cache.MarkDurable(generation)
	return nil
}

// Flush waits until the current reader-visible generation is crash-safe.
// Ordinary buffered-visible stores may satisfy this with a recovery-journal
// batch over the unchanged durable root; pressure, exceptional mutations,
// snapshots, and Close still fold a complete physical root.
func (c *Collection) Flush() error {
	if c == nil || c.committer == nil {
		return ErrClosed
	}
	if c.buffered() {
		if concurrentPrimaryExclusiveWaitHook != nil {
			concurrentPrimaryExclusiveWaitHook("flush")
		}
		c.writer.Lock()
		defer c.writer.Unlock()
		if c.closed {
			return ErrClosed
		}
		deltaLane := c.bufferedJournalDeltaLane()
		handled, err := c.checkpointBufferedJournalDeltaLocked()
		if err != nil || handled {
			return err
		}
		if deltaLane {
			c.journalDeltaFullFallbacks.Add(1)
		}
		return c.checkpointBufferedLocked()
	}
	// The journal-backed synchronous lane already has durable redo records but
	// Flush folds and recycles them. Async-visible and the chain-fence sync lane
	// already publish through the committer and only need to wait.
	if c.syncJournalLane() {
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
	if c.mutationCombiner != nil {
		for _, request := range c.mutationCombiner.stop() {
			request.err = ErrClosed
			close(request.done)
		}
	}
	c.mutationWait.Wait()
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
	var result error
	poisoned := false
	// Fold the final deferred window into a durable root and recycle the journal
	// so a reopen replays nothing, for both buffered-visible and the
	// journal-backed synchronous lane.
	if c.buffered() || c.syncJournalLane() {
		if err := c.checkpointBufferedLocked(); err != nil {
			// A sticky persistence failure cannot be repaired on this live
			// handle, but it must not prevent Close from releasing the writer
			// lock and owned I/O resources. Recovery on reopen is the repair
			// path. Non-persistence checkpoint errors remain retryable.
			if c.PersistenceError() == nil {
				return err
			}
			result = errors.Join(result, err)
			poisoned = true
		}
	}
	// The epoch table closes before the lease table for the same reason the
	// lease table closes before resources: a still-in-flight direct read must
	// fail Close (and divert every later read) rather than race the arena
	// release below.
	if err := c.readEpochs.Close(); err != nil {
		return errors.Join(result, err)
	}
	if err := c.leases.Close(); err != nil {
		return errors.Join(result, err)
	}
	if err := c.closeResourcesLocked(); err != nil {
		result = errors.Join(result, err)
		if !poisoned {
			return result
		}
	}
	// A failed writer unlock is the one cleanup error that must remain
	// retryable: until it succeeds, reopening the file for mutation is not safe.
	if c.writerLocked {
		return result
	}
	c.closeDone = true
	return result
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
