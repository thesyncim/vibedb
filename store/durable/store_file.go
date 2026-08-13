package durable

import (
	"os"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/internal/storemem"
	vibejson "github.com/thesyncim/vibejson"
)

type fileStoreState struct {
	root    storeio.StateRoot
	fileEnd uint64
	// freeHead is the newest delta page of the free log, or the zero reference
	// when the durable free set is empty. The inline root's cumulative free
	// delta reaches it directly, so the whole free set is replaceable without
	// rewriting a directory.
	freeHead storeio.PageRef
}

// collectionClosePhase is the last teardown boundary completed under writer.
// Close advances monotonically so a retry resumes after the exact resource
// that blocked it instead of rerunning persistence against a half-detached
// engine.
type collectionClosePhase uint8

const (
	closePhaseOpen collectionClosePhase = iota
	closePhasePersistence
	closePhaseReadEpochs
	closePhaseLeases
	closePhaseJournal
	closePhaseCommitter
	closePhasePageCache
	closePhaseFiles
	closePhaseBlocks
	closePhaseUnlocked
)

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
	// physicalHighWater is the complete page-aligned main-file prefix whose
	// physical allocation has been strictly proved and synced. It is writer
	// protected and remains zero for elastic collections.
	physicalHighWater uint64

	// writer is the collection-wide mutation gate. Ordinary mutation,
	// checkpoint, structural, and lifecycle paths retain its exclusive side;
	// the narrow buffered inline-row overlay lane takes the shared side so its
	// fallible replacement, insert, resurrection, and tombstone staging can
	// overlap across independent leaf buckets. Converting
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
	// primaryJournalAdmission serializes explicit recovery-journal callers into
	// finite phases without taking overlay stripes. primaryJournalContexts owns
	// preparation scratch only; admitted requests still execute through the
	// established exclusive Put/Delete baseline.
	primaryJournalAdmission *primaryJournalAdmission
	primaryJournalContexts  *primaryConcurrentContextPool
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
	closePhase    collectionClosePhase
	// closeErr is the terminal result of a completed Close, or a sticky
	// persistence error already discovered while retryable cleanup (active
	// readers or writer unlock) still prevents completion. writer protects it.
	closeErr error
	// state and visibleState are the newest published physical roots. On the
	// packed buffered lane logicalCut may lead those roots while mutations live
	// only in the resident overlay. Readers sample the physical pointer and cut
	// as one logical view; synchronous commits still cannot leak before their
	// visibility fence.
	state        atomic.Pointer[fileStoreState]
	visibleState atomic.Pointer[fileStoreState]
	durableState atomic.Pointer[fileStoreState]
	// logicalCut is the allocation-free publication token for the narrow
	// journal-delta buffered overlay lane. Its low 48 bits are the logical
	// generation and its high 16 bits are the signed document-count delta from
	// state/visibleState's physical root. The overlay and resident router are
	// initialized first; one Store here is the reader-visible commit point.
	logicalCut atomic.Uint64
	// packedLogicalCutDisabled is a one-way certificate. A collection built
	// without indexes owns the fixed concurrent contexts for its lifetime, but
	// a successful online index cutover permanently removes it from the packed
	// publication lane. Keeping that state separate avoids charging every later
	// indexed read for a cut load and recheck.
	packedLogicalCutDisabled atomic.Bool
	// primaryJournalCohortCutActive is a one-way, pay-for-use certificate. An
	// explicit RecoveryJournal collection does not tax reads with packed-cut
	// checks until its first cohort append has succeeded and is about to publish.
	primaryJournalCohortCutActive atomic.Bool
	visibilityMu                  sync.Mutex
	pendingVisible                []filePendingState

	committer *storeio.Committer
	cache     *storeio.PageCache
	// primaryUnifiedOverlay is the bounded, generation-stamped mutable row
	// window for class-5 inline puts and tombstones. Its byte arena is carved out of
	// Options.ResidentBytes; PageCache owns the remainder.
	primaryUnifiedOverlay *primaryUnifiedOverlay
	// primaryUnifiedReplacementScratch is the exclusive writer's bounded fold
	// workspace. A class-5 leaf has at most 256 stable slots regardless of how
	// many raw generations the overlay retains, so this final-state vector is
	// leaf-bounded rather than generation-window-bounded.
	primaryUnifiedReplacementScratch []storeio.CommonPrimaryUnifiedReplacement
	// primaryNativeFoldContexts is a fixed, construction-time foreground codec
	// pool. Its goroutines exist only while an exclusive checkpoint is actively
	// precomputing native class-5 leaf images; transaction allocation, staging,
	// retirement, parent rewrites, and publication remain on the writer goroutine.
	primaryNativeFoldContexts []primaryNativeFoldContext
	// primaryNativeFoldPrecomputeHook is a deterministic package-test seam called
	// after a worker has sealed one qualified all-Put leaf image. It may be called
	// concurrently; package tests must synchronize their hook body. Production
	// leaves it nil.
	primaryNativeFoldPrecomputeHook func(storeio.BucketID)
	// primaryNativeFoldAcquire is a narrow package-test seam for the one
	// schedule-dependent cache condition: several simultaneous leases may return
	// ErrPageCachePinned where serial acquisition succeeds. Production leaves it
	// nil. A test override may likewise be called concurrently.
	primaryNativeFoldAcquire func(storeio.PageRef) (storeio.PageLease, error)
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
	// journal is the bounded redo log paired eagerly with DurabilitySync and
	// explicit RecoveryJournal stores, and lazily on the first valid ordinary
	// buffered-visible mutation. It is owned by the serialized writer exactly like
	// the committer and appended and synced under c.writer. RecoveryJournal selects
	// per-mutation durable acknowledgement for buffered-visible; without it, Flush
	// may append one complete class-5 delta batch after a physical root has named
	// the lazy journal. Full checkpoints fold and recycle the log.
	// journalID mirrors the root identity so recovery cannot pair a stray file;
	// journalPowerSafe selects the barrier strength from CheckpointStrength.
	journal          *storeio.RecoveryJournal
	journalID        [16]byte
	journalPowerSafe bool
	journalReady     atomic.Bool
	// journalCatalogOwned is set for collections opened through a database
	// directory (or an equivalent caller-owned catalog). When set, journal mint
	// and recycle write the conditional journal format word so the collection
	// may prepare kind-5 records; standalone opens leave it false and keep the
	// legacy/scalar-patch mint. It is internal wiring, never an Options field.
	journalCatalogOwned bool
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
	// checkpoint. Only the ordinary buffered delta lane retains it; stores that
	// cannot emit an overlay delta leave the slice nil instead of carrying its
	// pointer-rich backing array. The class-5 overlay itself is capped at this
	// record count, so every eligible interval still fits without allocation.
	journalDeltaEntries []storeio.RecoveryBatchEntry
	// writeTransaction and the point-mutation scratch below are protected by
	// writer, so no transaction can overlap a Reset.
	writeTransaction storeio.WriteTransaction

	automaticCheckpoints                  atomic.Uint64
	primaryOverlayFolds                   atomic.Uint64
	primaryOverlayMaterializationAttempts atomic.Uint64
	primaryOverlayMaterializations        atomic.Uint64
	primaryOverlayMaterializationFailures atomic.Uint64
	primaryOverlayFoldNS                  atomicStatsHistogram
	primaryOverlayPressureFolds           atomic.Uint64
	primaryOverlaySnapshotFolds           atomic.Uint64
	primaryOverlayBarrierFolds            atomic.Uint64
	primaryOverlayCheckpointFolds         atomic.Uint64
	primaryCompactColumnPatchAttempts     atomic.Uint64
	primaryCompactColumnPatches           atomic.Uint64
	concurrentPrimaryScalarPatchAttempts  atomic.Uint64
	concurrentPrimaryScalarPatches        atomic.Uint64
	concurrentPrimaryReplaces             atomic.Uint64
	concurrentPrimaryFallbacks            atomic.Uint64
	concurrentPrimaryPublishGroups        atomic.Uint64
	concurrentPrimaryLargestPublishGroup  atomic.Uint64
	concurrentPrimaryStripeWaitNS         atomicStatsHistogram
	concurrentPrimaryPublishGroupSize     atomicStatsHistogram
	journalCohortReplaces                 atomic.Uint64
	journalCohortPublishGroups            atomic.Uint64
	journalCohortLargestPublishGroup      atomic.Uint64
	journalCohortPublishGroupSize         atomicStatsHistogram
	retirementPressureCheckpoints         atomic.Uint64
	materializationAttempts               atomic.Uint64
	materializationUpdates                atomic.Uint64
	materializationFallbacks              atomic.Uint64
	materializationSnapshotSkips          atomic.Uint64
	materializationBusySkips              atomic.Uint64
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
	journalGroupRecords       atomicStatsHistogram
	journalGroupMutations     atomicStatsHistogram
	journalGroupBytes         atomicStatsHistogram
	journalGroupSyncNS        atomicStatsHistogram
	journalStrictSyncs        atomic.Uint64
	journalStrictRecords      atomic.Uint64
	journalStrictMutations    atomic.Uint64
	journalStrictBytes        atomic.Uint64
	journalStrictSyncNS       atomicStatsHistogram
	journalDeltaCheckpoints   atomic.Uint64
	journalDeltaRecords       atomic.Uint64
	journalDeltaBytes         atomic.Uint64
	journalDeltaBatchRecords  atomicStatsHistogram
	journalDeltaBatchBytes    atomicStatsHistogram
	journalDeltaSyncNS        atomicStatsHistogram
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
	// Hole punching is a foreground, post-durability space optimization. The
	// source cursors, physical-generation guard, and disabled flag are
	// writer-owned; atomic counters keep the optional filesystem results
	// independently observable and cheap to sample.
	holePunchRanges           atomic.Uint64
	holePunchBytes            atomic.Uint64
	holePunchSkippedRanges    atomic.Uint64
	holePunchUnsupported      atomic.Uint64
	holePunchErrors           atomic.Uint64
	holePunchReusableCursor   uint64
	holePunchPendingCursor    storeio.PunchableExtentCursor
	holePunchAbsorbedCursor   uint64
	holePunchCandidateSource  uint8
	holePunchDisabled         bool
	holePunchGeneration       uint64
	holePunchCompletionVictim uint32
	holePunchCompletions      [fileStoreHolePunchCompletionSlots]storeio.FreeExtent
	holePunchPartials         [fileStoreHolePunchSourceCount]fileStoreHolePunchPartial
	// Tests may replace the platform helper per collection. Nil selects the
	// production Linux/Darwin implementation (or the unsupported no-op).
	holePunch func(*os.File, uint64, uint64) (bool, error)
	// holePunchCandidateFenced is a deterministic package-test seam invoked after
	// bounded candidate discovery completes but before either reader-admission
	// fence is released. Production leaves it nil.
	holePunchCandidateFenced func()

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
	// freeImageScratchInUse is a writer-owned assertion spanning the complete
	// fold plan/stage call. The post-flush hole-punch hook may borrow the same
	// fixed arena only after this marker clears.
	freeImageScratchInUse bool

	// batch is the reusable transactional WriteBatch handle. The batch type and
	// its options are shared; only the primary apply path remains.
	batch *WriteBatch

	// Ordered-primary batch scratch. One Update over the primary graph resolves
	// every mutation, rewrites one frame per touched leaf, and publishes them all
	// under one generation; these are reset per Update and reused so a batch's
	// steady-state cost is the frames it publishes, not the slices it plans with.
	// batchPrimaryLeafArena holds the finalized image of every touched leaf at
	// once (they must coexist until the single admit-all step).
	batchPrimaryLeaves       []primaryBatchLeaf
	batchPrimaryMutations    []primaryBatchMutation
	batchJournalEntries      []storeio.RecoveryBatchEntry
	batchPrimaryAdmitted     []storeio.PageRef
	batchPrimaryPrevVolatile []storeio.PageRef
	// batchPrimaryOverflowVolatile and batchPrimaryOverflowDurable are the old
	// out-of-line chains superseded by the current batch. The former are
	// memory-only frames retired under the publication reader fence; the latter
	// remain on device and join the next checkpoint's durable retirement set.
	// Both slices are writer-owned, reset for every plan, and retain their
	// high-water storage so a warmed mixed overflow batch allocates no metadata.
	batchPrimaryOverflowVolatile []storeio.PageRef
	batchPrimaryOverflowDurable  []storeio.PageRef
	batchPrimaryLeafArena        []byte
	batchPrimarySplitKey         []byte
	// The overflow pre-plan lays every new chain below the rewritten leaves and
	// records the exact visible high-water marks. These are published only after
	// all chains, leaves, exact-index records, and the WAL fence have succeeded.
	batchPrimaryOverflowFileEnd uint64
	batchPrimaryNextLogicalID   uint64
	batchPrimaryOverflowPages   int
	batchPrimaryOverflowDirty   uint64
	batchPrimaryFileEnd         uint64
}

// Stats is a point-in-time resource and I/O accounting snapshot.
// Every byte and queue counter corresponds to a configured finite budget.
type Stats struct {
	CapacityBytes uint64
	// PhysicalCapacityBytes is the immutable sealed main-file ceiling. Zero
	// denotes elastic allocation. PhysicalHighWaterBytes is the strictly
	// allocated and synced prefix currently available to rooted transactions.
	PhysicalCapacityBytes  uint64
	PhysicalHighWaterBytes uint64
	ResidentBytes          uint64
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
	// PrimaryOverlayMaterializationAttempts counts every non-empty class-5
	// overlay fold attempt, including a bounded-capacity decline that drains a
	// prior staged cut and retries. PrimaryOverlayMaterializations counts the
	// successful physical attempts. PrimaryOverlayMaterializationFailures counts
	// logical wrapper calls that ultimately failed; a handled capacity decline can
	// therefore make Attempts exceed Materializations+Failures. FoldNS measures
	// successful logical foreground calls, including any drain-and-retry latency.
	PrimaryOverlayMaterializationAttempts uint64
	PrimaryOverlayMaterializations        uint64
	PrimaryOverlayMaterializationFailures uint64
	PrimaryOverlayFoldNS                  StatsHistogram
	// PrimaryOverlay*Folds classify successful logical materializations by the
	// foreground boundary that requested them. PrimaryOverlayFolds is the sum of
	// Pressure, Snapshot, and Barrier; physical Checkpoint folds are separate.
	PrimaryOverlayPressureFolds   uint64
	PrimaryOverlaySnapshotFolds   uint64
	PrimaryOverlayBarrierFolds    uint64
	PrimaryOverlayCheckpointFolds uint64
	// PrimaryOverlay* debt fields expose the current bounded fold input without
	// scanning records: retained arena bytes/records, dirty leaf buckets, and the
	// conservative physical bytes reserved for their next materialization.
	// Records can remain retained after a fold while an old reader pins them;
	// DirtyBuckets and ReservedFoldBytes describe only generations newer than the
	// folded base.
	PrimaryOverlayArenaBytes        uint64
	PrimaryOverlayRetainedRecords   uint64
	PrimaryOverlayDirtyBuckets      uint64
	PrimaryOverlayReservedFoldBytes uint64
	PrimaryOverlayDirtyBucketLimit  uint64
	PrimaryOverlayDirtyByteLimit    uint64
	// PrimaryCompactColumnPatchAttempts counts exact compact-stripe replacement
	// qualifications, including safe declines to the complete planner.
	PrimaryCompactColumnPatchAttempts uint64
	// PrimaryCompactColumnPatches counts exact compact-stripe replacements that
	// replanned only the changed scalar column instead of rebuilding every row.
	PrimaryCompactColumnPatches uint64
	// ConcurrentPrimaryScalarPatchAttempts counts existing compact-row
	// replacements checked against the canonical span certificate produced by
	// concurrent admission.
	ConcurrentPrimaryScalarPatchAttempts uint64
	// ConcurrentPrimaryScalarPatches counts those attempts that published an
	// exact or one-scalar certificate for compact-fold reuse.
	ConcurrentPrimaryScalarPatches uint64
	// ConcurrentPrimaryReplaces counts existing inline rows published through
	// the shared-writer, bucket-striped buffered lane. It advances only after the
	// complete overlay/router/state publication, so qualification can detect a
	// benchmark that silently fell back to the exclusive mutation path.
	ConcurrentPrimaryReplaces uint64
	// ConcurrentPrimaryFallbacks counts successful mutations from a concurrent
	// pressure cohort that were elected to take the exclusive fold/retry path.
	ConcurrentPrimaryFallbacks uint64
	// ConcurrentPrimaryPublishGroups counts non-empty mutation groups made
	// visible by the concurrent publisher.
	ConcurrentPrimaryPublishGroups uint64
	// ConcurrentPrimaryLargestPublishGroup is the largest successful prefix
	// made visible by one concurrent publisher group.
	ConcurrentPrimaryLargestPublishGroup uint64
	// ConcurrentPrimaryStripeWaitNS measures only contended bucket-stripe waits;
	// uncontended acquisitions take no clock read. PublishGroupSize records every
	// successful group and therefore has the same Count as PublishGroups.
	ConcurrentPrimaryStripeWaitNS     StatsHistogram
	ConcurrentPrimaryPublishGroupSize StatsHistogram
	// JournalCohort* accounts only no-stripe RecoveryJournal admission groups.
	// Keeping it separate prevents benchmark qualification from mistaking this
	// caller-baton lane for the ordinary bucket-striped concurrent publisher.
	JournalCohortReplaces            uint64
	JournalCohortPublishGroups       uint64
	JournalCohortLargestPublishGroup uint64
	JournalCohortPublishGroupSize    StatsHistogram
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
	// JournalGroup* histograms describe each successful shared buffered-journal
	// fence by redo records, logical mutations, padded bytes, and sync latency.
	// A batch is one record but can contain several logical mutations.
	JournalGroupRecords   StatsHistogram
	JournalGroupMutations StatsHistogram
	JournalGroupBytes     StatsHistogram
	JournalGroupSyncNS    StatsHistogram
	// JournalStrict* accounts for the synchronous journal-before-publish lane,
	// which cannot share the buffered lane's post-publication fence. Syncs counts
	// successful fences; Records/Mutations/Bytes count the records those fences
	// made durable.
	JournalStrictSyncs     uint64
	JournalStrictRecords   uint64
	JournalStrictMutations uint64
	JournalStrictBytes     uint64
	JournalStrictSyncNS    StatsHistogram
	// JournalDelta* accounts for ordinary buffered-visible journal checkpoints.
	// Checkpoints counts explicit Flush fences. Records is the number of logical
	// overlay mutations appended (including an unsynced pressure carry), Bytes
	// their sector-padded journal traffic, and FullFallbacks counts attempted
	// delta paths that deliberately fell back to the full COW/root path.
	JournalDeltaCheckpoints   uint64
	JournalDeltaRecords       uint64
	JournalDeltaBytes         uint64
	JournalDeltaFullFallbacks uint64
	// JournalDeltaBatch* describes every successfully appended checkpoint/carry
	// batch. JournalDeltaSyncNS describes successful explicit delta fences.
	JournalDeltaBatchRecords StatsHistogram
	JournalDeltaBatchBytes   StatsHistogram
	JournalDeltaSyncNS       StatsHistogram
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
	// PrimaryMutationScratchBytes is the retained leaf-promotion and raw
	// segmented-root writer scratch plus the bounded unified-fold replacement
	// vector and compact-column planner workspace. It is allocated only for
	// PrimaryRoot stores and reflects any capacity learned by completed folds.
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
	FreeScratchLiveBytes uint64
	// HolePunchRanges/Bytes count successful filesystem deallocations.
	// HolePunchSkippedRanges counts deferred candidate observations, not unique
	// extents; the same safe range may be observed again after a cap or platform
	// failure. Unsupported and Errors each disable the optional optimization for
	// the process after their first occurrence; active readers instead narrow the
	// generation-safe prefix.
	HolePunchRanges        uint64
	HolePunchBytes         uint64
	HolePunchSkippedRanges uint64
	HolePunchUnsupported   uint64
	HolePunchErrors        uint64
	PendingRetiredExtents  uint64
	PendingRetiredBytes    uint64
	ReusableExtents        uint64
	ReusableBytes          uint64
	DocumentCount          uint64
	FileEnd                uint64
}
