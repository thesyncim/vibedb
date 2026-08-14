package durable

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/thesyncim/vibedb/internal/collectionname"
	"github.com/thesyncim/vibedb/internal/storeio"
)

// The recovery journal serves two buffered-visible contracts without changing
// reads. With Options.RecoveryJournal, each mutation appends and syncs its redo
// before acknowledgement. Without that option, acknowledgements stay volatile
// and an explicit Flush appends one ordered batch covering the bounded class-5
// overlay, then syncs it. Both forms recover through the ordinary mutation path;
// a physical checkpoint eventually folds the records into the root and recycles
// the journal. DurabilitySync uses the per-mutation form unconditionally.

// recoveryJournalCheckpointRecords bounds how many worst-case records the
// preallocated journal holds before a further append reports full and forces a
// checkpoint. It is the journal's checkpoint cadence, chosen to match the
// buffered dirty-cache cadence within an order of magnitude while bounding
// crash-recovery replay to a few thousand records. Worst case is
// MaxKeyBytes+InlineValueBytes per record, so a store with small values keeps
// many more than this many acknowledgements between checkpoints.
const recoveryJournalCheckpointRecords = 2048

// recoveryJournalMinCapacityBytes and recoveryJournalMaxCapacityBytes clamp the
// derived capacity so a tiny-value store still reserves a useful window and a
// large-value store cannot reserve an unbounded file or an unbounded replay.
const (
	recoveryJournalMinCapacityBytes = uint64(512) << 10
	// The ordinary delta lane keeps a one-MiB floor for small overlays. Larger
	// overlays start from two put/delete-framed arena estimates below, subject to the
	// compact cap and exact foreground fallback checks.
	recoveryJournalDeltaMinCapacityBytes = uint64(1) << 20
	// Compact full-value delta redo keeps the ordinary buffered journal fully
	// preallocated with a deterministic 2.5 MiB disk reservation.
	// The Flush guard considers at most 512 KiB of estimated future carry; it does
	// not promise every overlay fits. That leaves the same two-MiB append window
	// used by the qualified CP64 lane, so the smaller file does not increase its
	// physical-drain cadence. Any larger exact window takes the existing bounded
	// physical fallback rather than extending this file.
	recoveryJournalCompactDeltaCapacityBytes = uint64(5) << 19
	recoveryJournalCompactFutureReserveBytes = uint64(512) << 10
	recoveryJournalMaxCapacityBytes          = storeio.RecoveryJournalMaxCapacityBytes
)

// journalFailureBox carries the sticky journal poison behind an atomic pointer.
type journalFailureBox struct{ err error }

func recoveryJournalAcknowledgementFailure(cause error) error {
	return fmt.Errorf(
		"vibedb: recovery journal acknowledgement failed, reopen required: %w",
		cause,
	)
}

// poisonJournal records a terminal journal failure and rolls the reader view
// to the committer's poison rules, then returns the sticky error. It is
// idempotent: the first failure wins so later callers report the original
// cause. It requires no collection lock: the failure box is a first-wins
// CompareAndSwap, and poisonPersistence serializes against every publication
// under snapshotGate and visibilityMu on its own. That is what lets the
// group-commit leader poison from journalGroupAwait after releasing the writer
// (its sync deliberately runs outside c.writer), while the deposit and
// before-publish paths call it with the writer held.
func (c *Collection) poisonJournal(cause error) error {
	if cause == nil {
		return nil
	}
	c.journalFailure.CompareAndSwap(nil, &journalFailureBox{
		err: recoveryJournalAcknowledgementFailure(cause),
	})
	// Roll the reader view exactly as an automatic-persistence failure does. The
	// journal is buffered-only, so this preserves the last admitted immutable view
	// while every later mutation is rejected by PersistenceError.
	c.poisonPersistence(cause)
	return c.journalFailure.Load().err
}

// poisonJournalCommitOutcomeUnknown is the journal-sync counterpart of the
// committer's post-root-fence classification. Callers use it only after a
// complete redo record has been appended and the following durability barrier
// fails: the record may have reached stable storage despite the barrier's
// error, so only reopen/replay can determine whether the mutation committed.
//
// A record append can report a short/error result after its complete
// checksummed body reached the page cache because sector padding is not part of
// the authenticated record. Append and following-sync failures are therefore
// both outcome-unknown. Recycle failures are also treated as unknown by their
// TxnLog/collection lifecycle callers because the alternate header may have
// published despite the error.
func journalCommitOutcomeUnknown(cause error) error {
	if cause == nil {
		return nil
	}
	if !errors.Is(cause, ErrCommitOutcomeUnknown) {
		cause = fmt.Errorf("%w: %w", ErrCommitOutcomeUnknown, cause)
	}
	return cause
}

func (c *Collection) poisonJournalCommitOutcomeUnknown(cause error) error {
	return c.poisonJournal(journalCommitOutcomeUnknown(cause))
}

// poisonJournalCommitOutcomeUnknownForFence returns both diagnostic channels
// needed by group commit. sticky is the collection-wide first poison. fence is
// always this failed sync's independently classified error, even when an
// earlier concurrent append failure won the sticky CompareAndSwap.
func (c *Collection) poisonJournalCommitOutcomeUnknownForFence(
	cause error,
) (sticky, fence error) {
	classified := journalCommitOutcomeUnknown(cause)
	sticky = c.poisonJournal(classified)
	if errors.Is(sticky, classified) {
		return sticky, sticky
	}
	return sticky, recoveryJournalAcknowledgementFailure(classified)
}

// ErrStoreDirectIOUnsupported reports that ReadDirectRequire or
// WriteDirectRequire was configured on a platform or filesystem that cannot
// honor direct Store page I/O.
var ErrStoreDirectIOUnsupported = storeio.ErrDirectIOUnsupported

// ErrCollectionInDoubt reports that a standalone open found an uncovered
// kind-4 conditional batch in the live journal window. The file participates
// in a database transaction and must be opened through its database directory;
// once the collection checkpoints past the record the refusal clears.
var ErrCollectionInDoubt = errors.New(
	"vibedb: collection holds an undecided database transaction; open its database directory",
)

// ErrConditionalPrepareUnsupportedJournal reports a defensive refusal when
// conditional prepare is reached without a supported open journal lane. The
// coordinator refuses ordinary buffered delta before prepare.
var ErrConditionalPrepareUnsupportedJournal = errors.New(
	"vibedb: conditional prepare requires a supported recovery journal",
)

// ErrTransactionMarkerEpochMismatch reports that a kind-4 record's MarkerEpoch
// disagrees with the decision log's current epoch in either direction. A
// recycled log never coexists with live old-epoch records.
var ErrTransactionMarkerEpochMismatch = errors.New(
	"vibedb: conditional journal record epoch disagrees with the decision log",
)

// ErrTransactionParticipantBinding reports a committed decision whose marker
// or participant tuple does not exactly bind the conditional record recovered.
var ErrTransactionParticipantBinding = errors.New(
	"vibedb: transaction decision participant binding mismatch",
)

// recoveryJournalFaultHook, when non-nil, is invoked with each journal this
// collection creates or opens. The exhaustive store-level crash sweep sets it to
// install a FaultJournal over the journal's raw writes; production leaves it nil.
var recoveryJournalFaultHook func(*storeio.RecoveryJournal)

// recoveryJournalPostSyncHook, when non-nil, is invoked on the journal-backed
// synchronous lane immediately after a redo record has been appended and synced
// durable but BEFORE the mutation is applied to memory and published. It exists
// only so the synced-but-unpublished crash-window test can capture the on-disk
// store and journal at exactly that instant — the record is durable, the store
// root still predates it, and no in-memory publish has happened — and prove that
// a crash there replays the record on reopen. Production leaves it nil.
var recoveryJournalPostSyncHook func()

// recoveryJournalDeltaPreSyncHook and recoveryJournalDeltaPostSyncHook bracket
// the ordinary buffered-visible checkpoint fence for crash tests.
// recoveryJournalDeltaCarryHook runs after a pressure suffix has been appended
// without syncing and before its device-silent fold. The pre-sync hook runs
// after every suffix needed by the target has been appended and before Sync;
// the post-sync hook runs after Sync succeeds but before the logical durable
// watermark advances. Production leaves all three nil.
var (
	recoveryJournalDeltaCarryHook    func(target uint64)
	recoveryJournalDeltaPreSyncHook  func(target uint64)
	recoveryJournalDeltaPostSyncHook func(target uint64)
	// recoveryJournalReplayBatchEntryHook is a deterministic second-crash test
	// seam. It runs after one decoded batch entry has been applied but before the
	// next entry. Production leaves it nil.
	recoveryJournalReplayBatchEntryHook func(
		*Collection, storeio.RecoveryRecord, int,
	) error
)

// journalEnabled reports whether this collection has an open recovery journal.
// RecoveryJournal selects per-mutation durable acknowledgement. Ordinary
// buffered-visible stores mint this sibling only on their first valid mutation;
// their immutable/create-from-bulk footprint is therefore just the primary
// file. The synchronous lane is journal-backed unconditionally. A synchronous
// store whose root names no journal (created async-visible, reopened sync) has
// none and stays on the committer fence (chainFenceSync).
func (c *Collection) journalEnabled() bool {
	return c != nil && c.journal != nil
}

// journalConfigured reports whether a newly created buffered collection needs
// the recovery-journal sibling immediately. Ordinary buffered-visible stores
// defer it until their first valid mutation; explicit per-mutation recovery
// cannot defer because Put/Update acknowledge through the journal itself.
func (c *Collection) journalConfigured() bool {
	return c.options.Durability == DurabilityBufferedVisible &&
		c.options.RecoveryJournal
}

// bufferedJournalAckLane is the opt-in buffered mode that appends and syncs a
// redo record for every acknowledged mutation. Merely having a paired journal
// no longer selects this path: a mutated ordinary buffered-visible store also
// has one, but keeps mutation acknowledgements volatile and writes it only from
// Flush after a physical root has named the lazy identity.
func (c *Collection) bufferedJournalAckLane() bool {
	return c.buffered() && c.options.RecoveryJournal &&
		c.journalEnabled()
}

// bufferedJournalDeltaLane is the ordinary buffered-visible checkpoint lane.
// It never runs during recovery replay: Open must physically fold and recycle
// the records it is currently consuming.
func (c *Collection) bufferedJournalDeltaLane() bool {
	if !c.buffered() || c.options.RecoveryJournal ||
		!c.journalEnabled() || c.journalReplaying {
		return false
	}
	durable := c.durableState.Load()
	return durable != nil && c.journalID != ([16]byte{}) &&
		durable.root.JournalID == c.journalID
}

// ensureOrdinaryBufferedRecoveryJournalLocked mints the bounded delta journal
// on the first valid ordinary-buffered mutation. The caller holds writer. The
// current durable root does not name the new identity yet, so
// bufferedJournalDeltaLane remains false until a foreground physical checkpoint
// publishes it. This unpublished state is deliberate: a crash may leave an
// ignored orphan sibling, but can never make recovery depend on an unrooted log.
func (c *Collection) ensureOrdinaryBufferedRecoveryJournalLocked() error {
	if !c.buffered() || c.options.RecoveryJournal || c.journalEnabled() {
		return nil
	}
	durable := c.durableState.Load()
	if durable == nil {
		return ErrClosed
	}
	var journalID [16]byte
	if _, err := rand.Read(journalID[:]); err != nil {
		return fmt.Errorf("vibedb: mint recovery journal identity: %w", err)
	}
	path, err := c.journalSiblingPath()
	if err != nil {
		return err
	}
	header := recoveryJournalHeaderFor(
		c.storeID, journalID, uint32(c.options.PageSize),
		c.options.MaxKeyBytes, c.options.InlineValueBytes,
		c.options.MaxDocumentBytes, durable.root.Generation,
		c.options.primaryUnifiedOverlayBytes,
		c.options.SealedRecoveryJournalBytes,
	)
	if err := createSiblingRecoveryJournal(c.file.Name(), header); err != nil {
		return err
	}
	if err := c.openRecoveryJournalLocked(
		journalID, durable.root.Generation,
	); err != nil {
		// The durable root does not reference this identity, so removing a sibling
		// that could not be opened is safe and keeps a failed first mutation from
		// permanently charging the immutable footprint.
		_ = os.Remove(path)
		return err
	}
	return nil
}

// ensureOrdinaryBufferedRecoveryJournal serializes the one-time journal mint
// before an eligible packed mutation takes writer.RLock. Keeping initialization
// outside the shared section preserves the concurrent fast path for that first
// mutation while still publishing the journal pointer under writer's memory
// ordering.
func (c *Collection) ensureOrdinaryBufferedRecoveryJournal() error {
	if c.journalReady.Load() {
		return nil
	}
	c.writer.Lock()
	defer c.writer.Unlock()
	if c.closed {
		return ErrClosed
	}
	if failure := c.PersistenceError(); failure != nil {
		return failure
	}
	return c.ensureOrdinaryBufferedRecoveryJournalLocked()
}

// newBufferedJournalDeltaEntryScratch retains the complete fixed framing
// window only for stores that can emit ordinary buffered overlay deltas. The
// returned slice intentionally has full length and capacity; callers reset it
// to length zero while preserving an allocation-free append ceiling.
func newBufferedJournalDeltaEntryScratch(
	options normalizedFileStoreOptions,
) []storeio.RecoveryBatchEntry {
	if !bufferedJournalDeltaEntryScratchEnabled(options) {
		return nil
	}
	return make([]storeio.RecoveryBatchEntry, primaryUnifiedOverlayRecords)
}

func bufferedJournalDeltaEntryScratchEnabled(
	options normalizedFileStoreOptions,
) bool {
	return options.Durability == DurabilityBufferedVisible &&
		!options.RecoveryJournal &&
		options.primaryUnifiedOverlayBytes != 0
}

// recoveryJournalSuffix names a store file's paired recovery journal, which
// lives beside the store file and is resolved by path on reopen. It is exported
// through RecoveryJournalPath so a caller that publishes a store file by renaming
// it (a SQL table's atomic temp-to-final publish, say) can relocate the journal
// with it; leaving the journal behind makes a journaled root unopenable.
const recoveryJournalSuffix = collectionname.JournalSuffix

// RecoveryJournalPath returns the recovery-journal sibling path for a store file
// at storePath. Synchronous stores, explicit RecoveryJournal stores, and mutated
// ordinary buffered-visible stores write their journal here. Open resolves a
// root-referenced sibling here, so an external move of a mutable store file must
// move this sibling with it when it exists.
func RecoveryJournalPath(storePath string) string {
	return storePath + recoveryJournalSuffix
}

// journalSiblingPath returns the store file's paired journal path.
func (c *Collection) journalSiblingPath() (string, error) {
	name := c.file.Name()
	if name == "" {
		return "", fmt.Errorf("vibedb: recovery journal requires a named store file")
	}
	return RecoveryJournalPath(name), nil
}

// recoveryJournalCapacityBytesFor derives the preallocated record-region size
// from the store's admission bounds and the checkpoint-record cadence, clamped
// and sector-aligned. It is a free function so both Create and CreateFromPrimary
// size an identical journal.
//
// The cadence-derived size uses the inline worst case (MaxKeyBytes +
// InlineValueBytes) so a small-value store keeps thousands of acknowledgements
// between checkpoints. An out-of-line value's redo record instead carries the
// whole document (up to MaxDocumentBytes) so replay can re-run the Put and
// re-derive its overflow chain. That single record can exceed the inline cadence
// budget entirely, so the capacity is widened — never shrunk — to hold at least
// one worst-case overflow record plus a checkpoint's slack. It is not sized as
// recoveryJournalCheckpointRecords overflow records: one must fit, the
// journal recycles at the next.
func recoveryJournalCapacityBytesFor(
	sectorSize uint32, maxKeyBytes, inlineValueBytes, maxDocumentBytes int,
	deltaOverlayBytes int,
) uint64 {
	if deltaOverlayBytes > 0 {
		// Seed the ordinary buffered-visible reservation from two put/delete-framed
		// record/arena windows. This estimate does not include scalar metadata and
		// the 2.5-MiB policy cap deliberately prevents it from promising that every
		// admissible overlay fits. The exact prepared batch and foreground future-
		// reserve checks decide whether a Flush stays journal-only or takes the
		// bounded physical fallback.
		batch := uint64(storeio.RecoveryBatchRecordPaddedSizeForPayload(
			sectorSize, primaryUnifiedOverlayRecords, deltaOverlayBytes,
		))
		capacity := batch
		if batch <= recoveryJournalMaxCapacityBytes/2 {
			capacity = batch * 2
		} else {
			capacity = recoveryJournalMaxCapacityBytes
		}
		capacity = max(capacity, recoveryJournalDeltaMinCapacityBytes)
		capacity = min(capacity, recoveryJournalMaxCapacityBytes)
		capacity = min(capacity, recoveryJournalCompactDeltaCapacityBytes)
		return capacity
	}
	recordUpper := storeio.RecoveryRecordPaddedSize(
		sectorSize, maxKeyBytes, inlineValueBytes,
	)
	capacity := uint64(recordUpper) * recoveryJournalCheckpointRecords
	if capacity < recoveryJournalMinCapacityBytes {
		capacity = recoveryJournalMinCapacityBytes
	}
	if capacity > recoveryJournalMaxCapacityBytes {
		capacity = recoveryJournalMaxCapacityBytes
	}
	if maxDocumentBytes > inlineValueBytes {
		worstOverflow := uint64(storeio.RecoveryRecordPaddedSize(
			sectorSize, maxKeyBytes, maxDocumentBytes,
		))
		// One worst-case overflow record plus one inline cadence window of slack, so
		// a checkpoint that folds pending mutations still has room to append the
		// record that forced it.
		need := worstOverflow + uint64(recordUpper)
		if capacity < need {
			capacity = need
		}
	}
	sector := uint64(sectorSize)
	return (capacity + sector - 1) / sector * sector
}

// recoveryJournalInitialDocumentBytes keeps the synchronous journal's initial
// reservation proportional to its ordinary inline acknowledgement cadence.
// A larger configured document is an admission bound, not evidence that every
// store will write one; the sync lane grows its crash-safe preallocation before
// the first such record reaches the point of no return. Buffered per-mutation
// group commit retains its construction-time worst-document reservation because
// its journal Sync may run outside the collection writer while later records
// are admitted.
func recoveryJournalInitialDocumentBytes(
	durability DurabilityMode,
	inlineValueBytes, maxDocumentBytes int,
) int {
	if durability == DurabilitySync {
		return inlineValueBytes
	}
	return maxDocumentBytes
}

// growJournalForRecordLocked grows an ordinary mutable acknowledgement journal
// only for a single record that cannot fit even when empty. Ordinary capacity
// pressure still takes the established checkpoint/recycle path, preserving the
// bounded replay and fold cadence instead of letting small records expand the
// journal to its cap. Sealed journals reject growth themselves. The caller holds
// writer and has not reached the mutation's point of no return.
func (c *Collection) growJournalForRecordLocked(recordBytes int) error {
	if !c.syncJournalLane() && !c.bufferedJournalAckLane() || recordBytes <= 0 ||
		uint64(recordBytes) <= c.journal.Header().Capacity {
		return nil
	}
	header := c.journal.Header()
	inline := storeio.RecoveryRecordPaddedSize(
		header.SectorSize,
		c.options.MaxKeyBytes,
		c.options.InlineValueBytes,
	)
	if inline <= 0 || uint64(recordBytes) > recoveryJournalMaxCapacityBytes {
		return storeio.ErrRecoveryJournalFull
	}
	cursor := c.journal.Cursor()
	if cursor != 0 {
		// Preserve the existing checkpoint/recycle cadence. The caller folds the
		// live prefix first, then retries growth for this one oversized record in
		// an empty journal instead of retaining prefix+record amplification.
		return nil
	}
	minimum := uint64(recordBytes)
	if uint64(inline) <= recoveryJournalMaxCapacityBytes-minimum {
		minimum += uint64(inline)
	}
	return c.journal.GrowCapacity(minimum, c.journalPowerSafe)
}

// recoveryJournalHeaderFor builds the header identity and geometry for a store
// paired at baseGeneration.
func recoveryJournalHeaderFor(
	storeID, journalID [16]byte,
	pageSize uint32,
	maxKeyBytes, inlineValueBytes, maxDocumentBytes int,
	baseGeneration uint64, deltaOverlayBytes int,
	sealedCapacityBytes uint64,
) storeio.RecoveryJournalHeader {
	sectorSize := uint32(storeio.RecoveryJournalMinSectorSize)
	header := storeio.RecoveryJournalHeader{
		Format:         storeio.RecoveryJournalFormat,
		StoreID:        storeID,
		JournalID:      journalID,
		PageSize:       pageSize,
		SectorSize:     sectorSize,
		BaseGeneration: baseGeneration,
		BaseSequence:   0,
		Capacity: recoveryJournalCapacityBytesFor(
			sectorSize, maxKeyBytes, inlineValueBytes, maxDocumentBytes,
			deltaOverlayBytes,
		),
	}
	if sealedCapacityBytes != 0 {
		header.Capacity = sealedCapacityBytes
		header.SealedCapacity = true
	}
	return header
}

// createSiblingRecoveryJournal preallocates and syncs a fresh journal file
// beside storePath, closes it, then durably links its directory entry before a
// root may publish the journal identity. A one-shot builder
// (CreateFromPrimary) uses this to mint the journal a later Open will pair,
// replay, and own.
func createSiblingRecoveryJournal(
	storePath string, header storeio.RecoveryJournalHeader,
) error {
	if storePath == "" {
		return fmt.Errorf("vibedb: recovery journal requires a named store file")
	}
	storeInfo, err := os.Stat(storePath)
	if err != nil {
		return fmt.Errorf("vibedb: stat recovery journal store file: %w", err)
	}
	path := storePath + ".rjournal"
	file, err := os.OpenFile(
		path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, storeInfo.Mode().Perm(),
	)
	if err != nil {
		return fmt.Errorf("vibedb: create recovery journal file: %w", err)
	}
	journal, err := storeio.CreateRecoveryJournal(file, header)
	if err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return fmt.Errorf("vibedb: create recovery journal: %w", err)
	}
	if err := journal.Close(); err != nil {
		return fmt.Errorf("vibedb: close recovery journal: %w", err)
	}
	if err := syncRecoveryJournalParent(path); err != nil {
		return fmt.Errorf(
			"vibedb: persist recovery journal directory entry: %w", err,
		)
	}
	return nil
}

// openRecoveryJournalLocked opens and pairs the sibling journal named by a
// recovered root. A referenced-but-absent journal fails closed: the store may
// have acknowledged mutations only the journal records. The caller must hold the
// writer and must not yet have made the collection reachable.
func (c *Collection) openRecoveryJournalLocked(
	journalID [16]byte, rootGeneration uint64,
) error {
	path, err := c.journalSiblingPath()
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return storeio.ErrRecoveryJournalMissing
		}
		return fmt.Errorf("vibedb: open recovery journal file: %w", err)
	}
	journal, err := storeio.OpenRecoveryJournalWithOptions(
		file,
		storeio.RecoveryJournalOpenOptions{
			SealedCapacityBytes: c.options.SealedRecoveryJournalBytes,
			Pairing: &storeio.RecoveryJournalPairing{
				StoreID: c.storeID, JournalID: journalID,
				PageSize:       uint32(c.options.PageSize),
				RootGeneration: rootGeneration,
			},
		},
	)
	if err != nil {
		_ = file.Close()
		return err
	}
	c.journalID = journalID
	c.journalPowerSafe = c.options.CheckpointStrength != CheckpointFilesystem
	c.journal = journal
	c.journalReady.Store(true)
	c.journalDeltaGeneration.Store(rootGeneration)
	c.journalDeltaAppendedGeneration.Store(rootGeneration)
	c.initJournalGroupLocked()
	if recoveryJournalFaultHook != nil {
		recoveryJournalFaultHook(journal)
	}
	return nil
}

// recoveryJournalDeltaReplayStart returns the first entry not already covered
// by the recovered physical root. Delta batches contain exactly one entry for
// every consecutive generation ending at rec.Generation. Replay itself can
// checkpoint under bounded staging pressure while deliberately retaining the
// journal; after a second crash, skipping this durable prefix prevents an
// already-published point generation from being applied twice.
func recoveryJournalDeltaReplayStart(
	rec storeio.RecoveryRecord, rootGeneration uint64,
) (int, error) {
	if rec.Kind != storeio.RecoveryRecordKindDeltaBatch {
		return 0, fmt.Errorf(
			"%w: expected delta batch, got kind %d",
			storeio.ErrRecoveryJournalRecord, rec.Kind,
		)
	}
	count := uint64(len(rec.Entries))
	if count == 0 || rec.Generation < count {
		return 0, fmt.Errorf(
			"%w: invalid delta-batch generation range target=%d entries=%d",
			storeio.ErrRecoveryJournalRecord, rec.Generation, count,
		)
	}
	firstGeneration := rec.Generation - count + 1
	if rootGeneration < firstGeneration {
		return 0, nil
	}
	covered := rootGeneration - firstGeneration + 1
	if covered >= count {
		return len(rec.Entries), nil
	}
	return int(covered), nil
}

// recoveryJournalDecisionResolver answers whether a kind-4 conditional batch
// should apply. Database recovery constructs it per collection, closing over that
// collection's (StoreID, JournalID) so a decision commits the record only when
// its participant list names them. A nil resolver is standalone open.
type recoveryJournalDecisionResolver func(
	markerID [16]byte, epoch, txnID, preparedGeneration uint64,
) (committed bool, err error)

type recoveryConditionalOutcomeIdentity struct {
	markerID   [16]byte
	epoch      uint64
	txnID      uint64
	generation uint64
}

// recoveryJournalReplayResolverHook, when non-nil, supplies the decision
// resolver and marker epoch used by replayRecoveryJournalLocked. Production
// leaves it nil so standalone Open keeps nil-resolver semantics. Tests that
// need Open-driven resolved replay install a hook; database recovery threads the real
// resolver through replayRecoveryJournalResolvedLocked without this seam.
var recoveryJournalReplayResolverHook func(*Collection) (
	resolve recoveryJournalDecisionResolver, markerEpoch uint64,
)

// replayRecoveryJournalLocked re-applies every journaled record newer than the
// recovered root's generation through the ordinary mutation path, then
// checkpoints and recycles so the journal is empty and the store's durable root
// covers every replayed acknowledgement. Replay suppresses its own journal
// appends: the records are already durable, and re-journaling them would be
// redundant work that the immediately-following recycle discards anyway.
//
// Standalone opens pass a nil resolver: any retained kind-4 fails closed with
// ErrCollectionInDoubt. Database opens call
// replayRecoveryJournalResolvedLocked with a participant-binding resolver.
func (c *Collection) replayRecoveryJournalLocked(rootGeneration uint64) error {
	if hook := recoveryJournalReplayResolverHook; hook != nil {
		resolve, epoch := hook(c)
		return c.replayRecoveryJournalResolvedLocked(rootGeneration, resolve, epoch)
	}
	return c.replayRecoveryJournalResolvedLocked(rootGeneration, nil, 0)
}

// preflightRecoveryJournalResolved validates every decision and the resulting
// atomic-generation simulation without mutating collection state. It is the
// catalog phase-one primitive; phase two repeats the bounded scan and consumes
// the frozen, durable decision state only after every member passes.
func (c *Collection) preflightRecoveryJournalResolved(
	resolve recoveryJournalDecisionResolver,
	markerEpoch uint64,
) error {
	if c == nil || !c.journalEnabled() || c.journal.Cursor() == 0 {
		return nil
	}
	logicalGeneration := c.journal.BaseGeneration()
	return c.journal.Replay(logicalGeneration, func(rec storeio.RecoveryRecord) error {
		if rec.Kind == storeio.RecoveryRecordKindDeltaBatch {
			return nil
		}
		if logicalGeneration == ^uint64(0) ||
			rec.Generation != logicalGeneration+1 {
			return fmt.Errorf(
				"%w: atomic generation %d after %d",
				storeio.ErrRecoveryJournalRecord,
				rec.Generation, logicalGeneration,
			)
		}
		if rec.Kind != storeio.RecoveryRecordKindConditionalBatch {
			logicalGeneration = rec.Generation
			return nil
		}
		if resolve == nil {
			return ErrCollectionInDoubt
		}
		if rec.Conditional.MarkerEpoch != markerEpoch {
			return fmt.Errorf(
				"%w: record epoch %d decision epoch %d",
				ErrTransactionMarkerEpochMismatch,
				rec.Conditional.MarkerEpoch, markerEpoch,
			)
		}
		committed, err := resolve(
			rec.Conditional.MarkerID,
			rec.Conditional.MarkerEpoch,
			rec.Conditional.TxnID,
			rec.Generation,
		)
		if err != nil {
			return err
		}
		if committed {
			logicalGeneration = rec.Generation
		}
		return nil
	})
}

// replayRecoveryJournalResolvedLocked is the resolver-aware replay entry point.
// markerEpoch is the decision log's current epoch; a kind-4 record whose
// MarkerEpoch disagrees in either direction fails closed. The resolver is
// consulted for every retained kind-4 record, including one the selected root
// appears to cover. Resolver errors propagate unwrapped.
func (c *Collection) replayRecoveryJournalResolvedLocked(
	rootGeneration uint64,
	resolve recoveryJournalDecisionResolver,
	markerEpoch uint64,
) error {
	c.journalReplaying = true
	defer func() { c.journalReplaying = false }()
	// OpenRecoveryJournal has just validated the complete live region and
	// derived this cursor before the collection becomes reachable. Avoid a
	// second capacity-sized read when that authoritative scan found no record.
	if c.journal.Cursor() == 0 {
		if c.journal.BaseGeneration() < rootGeneration {
			return c.journal.Recycle(rootGeneration, c.journalPowerSafe)
		}
		return nil
	}
	replayAfter := c.journal.BaseGeneration()
	conditionalOutcomes := make(map[uint64]bool)
	recycleOutcomes := make(map[recoveryConditionalOutcomeIdentity]bool)
	logicalGeneration := replayAfter
	// Resolve the whole atomic window and simulate its decision-aware generation
	// chain before the first mutation. An aborted conditional does not consume
	// its prepared generation, so a following record may reuse it; a committed
	// conditional does consume it. This preflight prevents discovering an
	// impossible same-generation commit only after private replay has mutated or
	// pressure-checkpointed the collection.
	if err := c.journal.Replay(replayAfter, func(rec storeio.RecoveryRecord) error {
		if rec.Kind == storeio.RecoveryRecordKindDeltaBatch {
			return nil
		}
		if logicalGeneration == ^uint64(0) ||
			rec.Generation != logicalGeneration+1 {
			return fmt.Errorf(
				"%w: atomic generation %d after %d",
				storeio.ErrRecoveryJournalRecord,
				rec.Generation, logicalGeneration,
			)
		}
		if rec.Kind != storeio.RecoveryRecordKindConditionalBatch {
			logicalGeneration = rec.Generation
			return nil
		}
		if resolve == nil {
			return ErrCollectionInDoubt
		}
		if rec.Conditional.MarkerEpoch != markerEpoch {
			return fmt.Errorf(
				"%w: record epoch %d decision epoch %d",
				ErrTransactionMarkerEpochMismatch,
				rec.Conditional.MarkerEpoch, markerEpoch,
			)
		}
		committed, resolveErr := resolve(
			rec.Conditional.MarkerID,
			rec.Conditional.MarkerEpoch,
			rec.Conditional.TxnID,
			rec.Generation,
		)
		if resolveErr != nil {
			return resolveErr
		}
		conditionalOutcomes[rec.Sequence] = committed
		recycleOutcomes[recoveryConditionalOutcomeIdentity{
			markerID:   rec.Conditional.MarkerID,
			epoch:      rec.Conditional.MarkerEpoch,
			txnID:      rec.Conditional.TxnID,
			generation: rec.Generation,
		}] = committed
		if committed {
			logicalGeneration = rec.Generation
		}
		return nil
	}); err != nil {
		return err
	}
	applied := 0
	// Inspect the complete live suffix from the journal's own base. Atomic
	// records use idempotent logical set/delete replay, while kind-5 delta batches
	// authenticate their consecutive-generation semantics and skip exactly the
	// prefix already covered by the selected root. Every kind-4 record is resolved:
	// a root at its generation may be a checkpoint of only a sequential replay
	// prefix under narrower reopen limits, so "covered" alone cannot prove the
	// complete conditional batch was applied.
	replayCoveredAtomicSuffix := false
	apply := func(kind uint16, key, value []byte) error {
		switch kind {
		case storeio.RecoveryRecordKindPut:
			_, putErr := c.Put(key, value)
			if putErr == nil {
				applied++
			}
			return putErr
		case storeio.RecoveryRecordKindDelete:
			_, delErr := c.Delete(key)
			if delErr == nil {
				applied++
			}
			return delErr
		default:
			return fmt.Errorf("%w: unknown replay kind %d",
				storeio.ErrRecoveryJournalRecord, kind)
		}
	}
	applyAtomicBatchSequential := func(rec storeio.RecoveryRecord) error {
		for i := range rec.Entries {
			entry := rec.Entries[i]
			if err := apply(entry.Kind, entry.Key, entry.Value); err != nil {
				return err
			}
			if hook := recoveryJournalReplayBatchEntryHook; hook != nil {
				if err := hook(c, rec, i); err != nil {
					return err
				}
			}
		}
		return nil
	}
	// applyAtomicBatch replays one one-generation batch through Update.
	// countAtomic adds the ambiguous no-op consume count used by kind-3 atomic
	// batches; kind-4 callers pass false because they already counted the
	// record as consumed-on-decode before resolving.
	applyAtomicBatch := func(rec storeio.RecoveryRecord, countAtomic bool) error {
		// MaxBatchDocuments/MaxBatchBytes are reopen-time admission policy,
		// not persisted journal semantics. When the current process sized its
		// batch arenas below this acknowledged record, replay the entries
		// sequentially while Open is still private. The journal remains intact
		// across any pressure checkpoint; a second recovery re-enters at the
		// first covered batch and restores its complete ordered suffix.
		if !c.atomicRecoveryBatchFitsUpdate(rec) {
			return applyAtomicBatchSequential(rec)
		}
		// An atomic/conditional batch is one transactional WriteBatch, not a
		// sequence of independently visible point mutations. Replaying through
		// Update keeps its primary rows and exact-index postings atomic in
		// memory and prevents a pressure checkpoint from persisting a prefix
		// that a second recovery could mistake for the whole one-generation
		// record.
		batchErr := c.Update(func(batch *WriteBatch) error {
			for i := range rec.Entries {
				entry := rec.Entries[i]
				switch entry.Kind {
				case storeio.RecoveryRecordKindPut:
					if err := batch.appendRecovery(
						entry.Key, entry.Value, false,
					); err != nil {
						return err
					}
				case storeio.RecoveryRecordKindDelete:
					if err := batch.appendRecovery(
						entry.Key, nil, true,
					); err != nil {
						return err
					}
				default:
					return fmt.Errorf(
						"%w: unknown atomic batch kind %d",
						storeio.ErrRecoveryJournalRecord, entry.Kind,
					)
				}
			}
			return nil
		})
		if errors.Is(batchErr, ErrBatchTooLarge) {
			return applyAtomicBatchSequential(rec)
		}
		if batchErr != nil {
			return batchErr
		}
		if countAtomic {
			// Count a successfully consumed atomic batch even when every entry
			// is already a no-op (notably a pure-delete batch checkpointed by an
			// earlier interrupted replay). The final checkpoint/recycle must still
			// consume that ambiguous record instead of replaying it on every Open.
			applied++
		}
		// Preserve the recovery fault seam, but only after the whole batch is
		// published. A hook may checkpoint or interrupt recovery here; it can
		// no longer observe or persist a torn prefix.
		if hook := recoveryJournalReplayBatchEntryHook; hook != nil {
			for i := range rec.Entries {
				if hookErr := hook(c, rec, i); hookErr != nil {
					return hookErr
				}
			}
		}
		return nil
	}
	err := c.journal.Replay(replayAfter, func(rec storeio.RecoveryRecord) error {
		if rec.Kind == storeio.RecoveryRecordKindConditionalBatch {
			// Consumed-on-decode: every decoded kind-4 — applied or skipped —
			// forces the post-replay fold+recycle so it never survives a
			// resolver-backed open.
			applied++
			committed, ok := conditionalOutcomes[rec.Sequence]
			if !ok {
				return fmt.Errorf(
					"%w: conditional sequence %d was not preflighted",
					storeio.ErrRecoveryJournalRecord, rec.Sequence,
				)
			}
			if !committed {
				// Same-epoch undecided or decided-elsewhere: presumed abort.
				return nil
			}
			if rec.Generation <= rootGeneration {
				// The root may cover only a sequential replay prefix. Reapply the
				// complete committed batch and every later covered record in order.
				replayCoveredAtomicSuffix = true
			}
			// Committed: apply like an atomic one-generation batch.
			// Consumed-on-decode already counted this record.
			return applyAtomicBatch(rec, false)
		}
		if rec.Generation <= rootGeneration && !replayCoveredAtomicSuffix {
			if rec.Kind == storeio.RecoveryRecordKindBatch {
				// The recovered root may be a checkpoint of only a prefix of this
				// one-generation batch. Reapply it atomically, then preserve journal
				// order for every later covered record.
				replayCoveredAtomicSuffix = true
			} else if rec.Kind != storeio.RecoveryRecordKindDeltaBatch {
				return nil
			}
		}
		if rec.Kind == storeio.RecoveryRecordKindBatch {
			return applyAtomicBatch(rec, true)
		}
		if rec.Kind == storeio.RecoveryRecordKindDeltaBatch {
			start := 0
			if !replayCoveredAtomicSuffix {
				var startErr error
				start, startErr = recoveryJournalDeltaReplayStart(
					rec, rootGeneration,
				)
				if startErr != nil {
					return startErr
				}
			}
			// A delta batch is a consecutive sequence of point generations. A
			// pressure checkpoint may cover a prefix; start skips exactly that
			// durable prefix before applying the remainder.
			for i := start; i < len(rec.Entries); i++ {
				entry := rec.Entries[i]
				// The mutation path borrows the key; replay hands the record's
				// []byte straight back with no string round-trip.
				entryErr := apply(entry.Kind, entry.Key, entry.Value)
				if entryErr != nil {
					return entryErr
				}
				if hook := recoveryJournalReplayBatchEntryHook; hook != nil {
					if hookErr := hook(c, rec, i); hookErr != nil {
						return hookErr
					}
				}
			}
			return nil
		}
		return apply(rec.Kind, rec.Key, rec.Value)
	})
	if err != nil {
		return err
	}
	// Cursor was non-zero on entry, so Replay validated at least one record.
	// Fold and recycle even when every delta entry was already covered or every
	// atomic mutation was an idempotent no-op. An empty live window is what lets
	// a later writer select the other authenticated record family without
	// weakening the per-window family fence.
	// Fold the replayed acknowledgements into a durable root, then — and only
	// then — empty the journal, so a crash at any point during replay or the
	// fold finds every record still on disk and replays idempotently. Recycling
	// is suppressed while journalReplaying is set, so a mid-replay checkpoint
	// forced by staging pressure cannot consume records the replay cursor has
	// not reached.
	c.writer.Lock()
	defer c.writer.Unlock()
	if err := c.checkpointBufferedLocked(); err != nil {
		return err
	}
	c.journalReplaying = false
	physicalGeneration := c.committer.DurableGeneration()
	if err := c.recycleRecoveryJournalResolvedLocked(
		physicalGeneration,
		func(
			header storeio.RecoveryConditionalHeader, generation uint64,
		) (bool, error) {
			committed, ok := recycleOutcomes[recoveryConditionalOutcomeIdentity{
				markerID: header.MarkerID, epoch: header.MarkerEpoch,
				txnID: header.TxnID, generation: generation,
			}]
			if !ok {
				return false, fmt.Errorf(
					"%w: conditional transaction %d generation %d was not preflighted",
					storeio.ErrRecoveryJournalRecord, header.TxnID, generation,
				)
			}
			return committed, nil
		},
	); err != nil {
		return err
	}
	// Normal physical completion owns this hook. Recovery recycles directly
	// after its final fold, so it performs the same one bounded pass explicitly.
	return c.punchNewPhysicalGenerationLocked(physicalGeneration)
}

func (c *Collection) atomicRecoveryBatchFitsUpdate(
	rec storeio.RecoveryRecord,
) bool {
	if len(rec.Entries) > c.options.MaxBatchDocuments {
		return false
	}
	remaining := c.options.MaxBatchBytes
	for i := range rec.Entries {
		entry := rec.Entries[i]
		bytes := len(entry.Key) + len(entry.Value)
		if bytes > remaining {
			return false
		}
		remaining -= bytes
	}
	return true
}

// recycleRecoveryJournalLocked advances the journal head past a checkpointed
// generation inside the checkpoint's root publication. It is a no-op when no
// journal is configured. The caller holds the writer and has already made the
// checkpointed generation durable.
func (c *Collection) recycleRecoveryJournalLocked(baseGeneration uint64) error {
	if c.journalReplaying {
		// A checkpoint forced mid-replay (staging pressure from the replayed
		// mutations) must leave the journal intact: records past the replay
		// cursor exist nowhere else, and a crash before replay completes must
		// find them again. The post-replay checkpoint recycles explicitly.
		return nil
	}
	if !c.journalEnabled() || baseGeneration == 0 {
		return nil
	}
	// Committer staging pressure may flush an older rooted cut while either an
	// atomic or delta journal window still carries newer logical generations.
	// Retain every such window until the selected physical root covers its
	// authenticated end; Recycle independently enforces the same invariant.
	if c.journal.Cursor() != 0 &&
		baseGeneration < c.journal.LiveEndGeneration() {
		return nil
	}
	if baseGeneration < c.journal.BaseGeneration() {
		// The durable generation never regressed; nothing to recycle past.
		return nil
	}
	return c.finishRecoveryJournalRecycleLocked(
		baseGeneration,
		c.journal.Recycle(baseGeneration, c.journalPowerSafe),
	)
}

func (c *Collection) recycleRecoveryJournalResolvedLocked(
	baseGeneration uint64, resolve storeio.RecoveryConditionalResolver,
) error {
	if !c.journalEnabled() || baseGeneration == 0 {
		return nil
	}
	if baseGeneration < c.journal.BaseGeneration() {
		return nil
	}
	return c.finishRecoveryJournalRecycleLocked(
		baseGeneration,
		c.journal.RecycleResolved(
			baseGeneration, c.journalPowerSafe, resolve,
		),
	)
}

func (c *Collection) finishRecoveryJournalRecycleLocked(
	baseGeneration uint64, recycleErr error,
) error {
	if recycleErr != nil {
		// A failed recycle is a device write or sync failure on the journal
		// header (Recycle never reports full), and it is terminal the same way a
		// failed record append is: the caller's mutation may already be
		// published, so returning a plain error would let every later Put
		// re-publish and re-fail forever with PersistenceError still nil.
		// Poison die-don't-retry and fail the shared group fence so no deposited
		// waiter is acknowledged out of a journal whose head can no longer move.
		poisoned := c.poisonJournal(recycleErr)
		c.journalGroup.fail(poisoned)
		return poisoned
	}
	c.journalDeltaGeneration.Store(baseGeneration)
	c.journalDeltaAppendedGeneration.Store(baseGeneration)
	// The checkpoint folded every deposited record into the durable root before
	// this recycle, so any group-commit waiter parked on an earlier ticket is now
	// durable via the root and must complete without waiting for a sync of the
	// records the recycle just discarded.
	c.journalGroup.recycleAdvance()
	return nil
}

// ensurePrimaryBatchConditionalJournalRoom guarantees a kind-4 record carrying
// entries fits before prepare appends. Capacity shortfalls take the ordinary
// pressure-checkpoint path and are not persistence poisons.
func (c *Collection) ensurePrimaryBatchConditionalJournalRoom(
	entries []storeio.RecoveryBatchEntry,
) error {
	if c.journalReplaying || !c.journalEnabled() {
		return nil
	}
	plan, err := c.journal.PrepareConditionalBatch(entries)
	if err != nil {
		return err
	}
	if c.journal.PreparedBatchFits(plan) {
		return nil
	}
	if err := c.growJournalForRecordLocked(plan.PaddedSize()); err != nil {
		return err
	}
	if c.journal.PreparedBatchFits(plan) {
		return nil
	}
	if err := c.checkpointBufferedLocked(); err != nil {
		return err
	}
	c.automaticCheckpoints.Add(1)
	plan, err = c.journal.PrepareConditionalBatch(entries)
	if err != nil {
		return err
	}
	if err := c.growJournalForRecordLocked(plan.PaddedSize()); err != nil {
		return err
	}
	if !c.journal.PreparedBatchFits(plan) {
		return storeio.ErrRecoveryJournalFull
	}
	return nil
}

// journalHoldsConditional reports whether the journal's live window still holds
// any kind-4 record for markerID at epoch. The caller must hold the writer.
// Used by decision-log recycle legality and collection-retirement barriers.
func (c *Collection) journalHoldsConditional(
	markerID [16]byte, epoch uint64,
) (bool, error) {
	if !c.journalEnabled() || c.journal.Cursor() == 0 {
		return false, nil
	}
	found := false
	err := c.journal.Replay(c.journal.BaseGeneration(), func(rec storeio.RecoveryRecord) error {
		if rec.Kind == storeio.RecoveryRecordKindConditionalBatch &&
			rec.Conditional.MarkerID == markerID &&
			rec.Conditional.MarkerEpoch == epoch {
			found = true
		}
		return nil
	})
	return found, err
}

// checkpointPastConditionalsLocked performs one bounded foreground checkpoint
// plus recycle that folds the live window past every conditional record. Used
// by the collection-retirement barrier and laggard folding under decision-log
// region pressure. The caller holds the writer.
func (c *Collection) checkpointPastConditionalsLocked(
	resolve recoveryJournalDecisionResolver, markerEpoch uint64,
) error {
	if failure := c.PersistenceError(); failure != nil {
		return failure
	}
	if err := c.preflightRecoveryJournalResolved(resolve, markerEpoch); err != nil {
		return err
	}
	if err := c.checkpointBufferedLocked(); err != nil {
		return err
	}
	physicalGeneration := c.committer.DurableGeneration()
	if err := c.recycleRecoveryJournalResolvedLocked(
		physicalGeneration,
		func(
			header storeio.RecoveryConditionalHeader, generation uint64,
		) (bool, error) {
			if resolve == nil {
				return false, ErrCollectionInDoubt
			}
			return resolve(
				header.MarkerID, header.MarkerEpoch, header.TxnID, generation,
			)
		},
	); err != nil {
		return err
	}
	c.automaticCheckpoints.Add(1)
	return nil
}

// journalDepositLocked appends one redo record for a frame-deferred
// (buffered-visible) mutation under the writer and returns the group-commit
// ticket the caller waits on for the shared sync. It does NOT sync: the sync is
// amortized across concurrent callers by journalGroupAwait after the writer is
// released (phase 1 group commit, store_file_journal_group.go). Append order
// under the writer equals publish order equals generation order, so the on-disk
// record stream is byte-for-byte the per-caller path's — only the fsync is
// shared.
//
// A full journal forces a checkpoint — which folds the just-published mutation,
// and every earlier deposit, into a durable root and recycles the journal —
// exactly like dirty-cache pressure, after which the mutation is durable without
// its own record, so the ack is counted against the chain lane and a zero ticket
// is returned (there is no sync to wait for). The caller holds the writer and
// has already published the mutation's state. A returned ticket of zero means
// "already durable, do not wait"; any device append error poisons
// die-don't-retry and fails every waiter parked on the shared fence.
func (c *Collection) journalDepositLocked(
	kind uint16, generation uint64, key, value []byte,
) (uint64, error) {
	if !c.bufferedJournalAckLane() || c.journalReplaying {
		return 0, nil
	}
	if !c.journal.Fits(len(key), len(value)) {
		// No room for this record. Checkpoint folds every deferred mutation —
		// including the one just published — into a durable root and recycles the
		// journal. The mutation is durable afterwards without its own record.
		if err := c.checkpointBufferedLocked(); err != nil {
			return 0, err
		}
		c.automaticCheckpoints.Add(1)
		c.chainAcks.Add(1)
		return 0, nil
	}
	before := c.journal.Cursor()
	if _, err := c.journal.Append(kind, generation, key, value); err != nil {
		if errors.Is(err, storeio.ErrRecoveryJournalFull) {
			if cpErr := c.checkpointBufferedLocked(); cpErr != nil {
				return 0, cpErr
			}
			c.automaticCheckpoints.Add(1)
			c.chainAcks.Add(1)
			return 0, nil
		}
		// The raw checksummed body may have landed even when padding did not, so
		// reopen may accept this record. Fail every waiter with unknown outcome.
		poisoned := c.poisonJournalCommitOutcomeUnknown(err)
		c.journalGroup.fail(poisoned)
		return 0, poisoned
	}
	c.journalAcks.Add(1)
	return c.journalGroup.depositBump(c.journal.Cursor()-before, 1), nil
}

// journalBeforePublishLocked is the journal-backed synchronous lane's
// acknowledgement. It appends one redo record and syncs it at the point of no
// return of a primary canonical-frame mutation — after every fallible prepare
// step (routing, eligibility, split detection, dirty admission) has succeeded
// and before any reader-visible or in-memory-committed state changes — so
// visibility strictly follows durability. It is a no-op for buffered-visible
// (which journals after publishing, per its volatile-window contract) and during
// replay. Journal capacity is ensured before the leaf frame is prepared
// (ensureBufferedPrimaryMutationCapacity), so the append cannot report full
// here; any append or sync error is a device failure and is terminal for the
// journal lane, poisoning die-don't-retry with nothing published. Append and
// Sync failures both have unknown commit outcome because a complete
// checksummed body may replay after reopen even when the padded write failed.
func (c *Collection) journalBeforePublishLocked(
	deleting bool, generation uint64, key, value []byte,
) error {
	if !c.syncJournalLane() || c.journalReplaying {
		return nil
	}
	kind := uint16(storeio.RecoveryRecordKindPut)
	if deleting {
		kind = storeio.RecoveryRecordKindDelete
		value = nil
	}
	before := c.journal.Cursor()
	if _, err := c.journal.Append(kind, generation, key, value); err != nil {
		return c.poisonJournalCommitOutcomeUnknown(err)
	}
	bytes := c.journal.Cursor() - before
	started := time.Now()
	if err := c.journal.Sync(c.journalPowerSafe); err != nil {
		return c.poisonJournalCommitOutcomeUnknown(err)
	}
	syncNS := uint64(time.Since(started))
	if recoveryJournalPostSyncHook != nil {
		recoveryJournalPostSyncHook()
	}
	c.journalAcks.Add(1)
	c.journalStrictSyncs.Add(1)
	c.journalStrictRecords.Add(1)
	c.journalStrictMutations.Add(1)
	c.journalStrictBytes.Add(bytes)
	c.journalStrictSyncNS.observe(syncNS)
	return nil
}

// journalBatchBeforePublishLocked is the sync lane's batch acknowledgement: it
// appends the batch's single redo record and syncs it at the batch's point of no
// return — after every document's fallible prepare and every leaf frame's dirty
// admission, and before any leaf pointer is published — so no reader observes any
// member of the batch before the whole group is durable. Journal capacity is
// ensured for the whole record before prepare (ensurePrimaryBatchJournalRoom), so
// the append cannot report full here; an append or sync error is a device failure
// and poisons die-don't-retry with unknown outcome because recovery may accept
// the checksummed body and replay it atomically. It is a no-op for buffered-
// visible (which journals its batch after publishing) and during replay.
func (c *Collection) journalBatchBeforePublishLocked(
	generation uint64, entries []storeio.RecoveryBatchEntry,
) error {
	if !c.syncJournalLane() || c.journalReplaying {
		return nil
	}
	before := c.journal.Cursor()
	if _, err := c.journal.AppendBatch(generation, entries); err != nil {
		return c.poisonJournalCommitOutcomeUnknown(err)
	}
	bytes := c.journal.Cursor() - before
	started := time.Now()
	if err := c.journal.Sync(c.journalPowerSafe); err != nil {
		return c.poisonJournalCommitOutcomeUnknown(err)
	}
	syncNS := uint64(time.Since(started))
	if recoveryJournalPostSyncHook != nil {
		recoveryJournalPostSyncHook()
	}
	c.journalAcks.Add(1)
	c.journalStrictSyncs.Add(1)
	c.journalStrictRecords.Add(1)
	c.journalStrictMutations.Add(uint64(len(entries)))
	c.journalStrictBytes.Add(bytes)
	c.journalStrictSyncNS.observe(syncNS)
	return nil
}

// journalBatchDepositLocked is the buffered-visible lane's batch acknowledgement:
// it appends the already-published batch's single redo record under the writer
// and returns the group-commit ticket the caller waits on for the shared sync,
// the group-commit analogue of journalDepositLocked. The whole batch is one
// record and thus one ticket. Capacity is ensured before prepare, but the
// full-journal fallback is kept for the same reason the single-record path keeps
// it — a checkpoint folds the just-published batch into a durable root and
// recycles, after which the batch is durable without its own record (zero ticket
// returned). The caller holds the writer and has already published the batch's
// state.
func (c *Collection) journalBatchDepositLocked(
	generation uint64, entries []storeio.RecoveryBatchEntry,
) (uint64, error) {
	if !c.bufferedJournalAckLane() || c.journalReplaying {
		return 0, nil
	}
	before := c.journal.Cursor()
	if _, err := c.journal.AppendBatch(generation, entries); err != nil {
		if errors.Is(err, storeio.ErrRecoveryJournalFull) {
			if cpErr := c.checkpointBufferedLocked(); cpErr != nil {
				return 0, cpErr
			}
			c.automaticCheckpoints.Add(1)
			c.chainAcks.Add(1)
			return 0, nil
		}
		poisoned := c.poisonJournalCommitOutcomeUnknown(err)
		c.journalGroup.fail(poisoned)
		return 0, poisoned
	}
	c.journalAcks.Add(1)
	return c.journalGroup.depositBump(
		c.journal.Cursor()-before, uint64(len(entries)),
	), nil
}

// ensurePrimaryBatchJournalRoom guarantees the whole batch record fits the
// preallocated journal before the batch prepares any frame, folding and recycling
// a full journal now — exactly as the single-document sync lane folds a full
// journal in ensureBufferedPrimaryMutationCapacity — so neither the sync lane's
// point-of-no-return append nor the buffered lane's post-publish append can meet
// a full journal mid-commit. It is a no-op when no journal is configured.
func (c *Collection) ensurePrimaryBatchJournalRoom(
	entries []storeio.RecoveryBatchEntry,
) error {
	if c.journalReplaying ||
		!c.bufferedJournalAckLane() && !c.syncJournalLane() {
		return nil
	}
	if c.journal.FitsBatch(entries) {
		return nil
	}
	if c.bufferedJournalAckLane() {
		plan, err := c.journal.PrepareBatch(entries)
		if err != nil {
			return err
		}
		if uint64(plan.PaddedSize()) > c.journal.Header().Capacity {
			// The buffered acknowledgement lane publishes before depositing its
			// kind-3 record. An oversized record deliberately takes that deposit's
			// physical-checkpoint fallback; pre-publish checkpoint/replan would
			// repeat forever because this compact journal never grows for kind 3.
			return nil
		}
	}
	if c.syncJournalLane() {
		plan, err := c.journal.PrepareBatch(entries)
		if err != nil {
			return err
		}
		if err := c.growJournalForRecordLocked(plan.PaddedSize()); err != nil {
			return err
		}
		if c.journal.PreparedBatchFits(plan) {
			return nil
		}
	}
	if err := c.checkpointBufferedLocked(); err != nil {
		return err
	}
	c.automaticCheckpoints.Add(1)
	if c.syncJournalLane() && !c.journal.FitsBatch(entries) {
		plan, err := c.journal.PrepareBatch(entries)
		if err != nil {
			return err
		}
		if err := c.growJournalForRecordLocked(plan.PaddedSize()); err != nil {
			return err
		}
		if !c.journal.PreparedBatchFits(plan) {
			return storeio.ErrRecoveryJournalFull
		}
	}
	return nil
}

// bufferedJournalDeltaStagingCovered reports whether the committer either has
// no device-silent root or its newest staged root is exactly the heap fold named
// by primaryCheckpointBase and is already represented by complete journal
// appends through coveredGeneration. An arbitrary structural/COW publication
// must never borrow the overlay delta lane.
func (c *Collection) bufferedJournalDeltaStagingCovered(
	coveredGeneration uint64,
) bool {
	published := c.committer.PublishedGeneration()
	physicalDurable := c.committer.DurableGeneration()
	if published == physicalDurable {
		return true
	}
	if published > coveredGeneration {
		return false
	}
	base := c.primaryCheckpointBase
	return base != nil && base.root.Generation == published
}

// bufferedJournalDeltaStateEligible rejects every non-overlay source of a
// visible generation. Exact-index resident deltas are deliberately allowed:
// replaying the same ordered primary mutations rebuilds identical postings.
func (c *Collection) bufferedJournalDeltaStateEligible(
	coveredGeneration uint64,
) bool {
	return c.bufferedJournalDeltaStagingCovered(coveredGeneration) &&
		len(c.primaryPendingParents) == 0 &&
		len(c.primaryVolatileRetired) == 0 &&
		len(c.primaryMutationAdmitted) == 0 &&
		len(c.batchPrimaryAdmitted) == 0
}

// appendBufferedJournalDeltaLocked appends exactly one complete consecutive
// overlay interval without syncing it. Success advances only the appended
// watermark; the durable watermark remains unchanged until an explicit Flush
// fences the journal. complete=false is a conservative physical-fallback
// signal, including a gap, structural staging, or bounded journal capacity.
func (c *Collection) prepareBufferedJournalDeltaLocked(
	after, target uint64,
) (
	entries []storeio.RecoveryBatchEntry,
	plan storeio.RecoveryBatchPlan,
	complete bool,
	err error,
) {
	if target == after {
		return nil, storeio.RecoveryBatchPlan{}, true, nil
	}
	if target < after ||
		after < c.journalDeltaGeneration.Load() {
		return nil, storeio.RecoveryBatchPlan{}, false,
			storeio.ErrGenerationOrder
	}
	if !c.bufferedJournalDeltaStateEligible(after) {
		return nil, storeio.RecoveryBatchPlan{}, false, nil
	}
	if c.journalDeltaEntries == nil {
		// This framing window is needed only by a foreground Flush that found a
		// consecutive overlay delta. Read-only opens and stores that close without
		// a logical delta never materialize it.
		c.journalDeltaEntries = newBufferedJournalDeltaEntryScratch(c.options)
	}
	entries, covered, entryErr :=
		c.primaryUnifiedOverlay.checkpointEntries(
			c.journalDeltaEntries[:0], after, target,
		)
	if entryErr != nil {
		return nil, storeio.RecoveryBatchPlan{}, false, entryErr
	}
	if !covered || len(entries) == 0 {
		return nil, storeio.RecoveryBatchPlan{}, false, nil
	}
	plan, planErr := c.journal.PrepareDeltaBatch(entries)
	if planErr != nil {
		return nil, storeio.RecoveryBatchPlan{}, false, planErr
	}
	if !c.journal.PreparedBatchFits(plan) {
		return nil, storeio.RecoveryBatchPlan{}, false, nil
	}
	return entries, plan, true, nil
}

func (c *Collection) appendPreparedBufferedJournalDeltaLocked(
	target uint64, entries []storeio.RecoveryBatchEntry,
	plan storeio.RecoveryBatchPlan,
) (complete bool, err error) {
	if _, appendErr := c.journal.AppendPreparedDeltaBatch(
		target, entries, plan,
	); appendErr != nil {
		if errors.Is(appendErr, storeio.ErrRecoveryJournalFull) {
			return false, nil
		}
		return false, c.poisonJournalCommitOutcomeUnknown(appendErr)
	}
	c.journalDeltaAppendedGeneration.Store(target)
	c.journalDeltaRecords.Add(uint64(len(entries)))
	c.journalDeltaBytes.Add(uint64(plan.PaddedSize()))
	c.journalDeltaBatchRecords.observe(uint64(len(entries)))
	c.journalDeltaBatchBytes.observe(uint64(plan.PaddedSize()))
	return true, nil
}

func (c *Collection) appendBufferedJournalDeltaLocked(
	after, target uint64,
) (complete bool, err error) {
	entries, plan, complete, prepareErr :=
		c.prepareBufferedJournalDeltaLocked(after, target)
	if prepareErr != nil || !complete || target == after {
		return complete, prepareErr
	}
	return c.appendPreparedBufferedJournalDeltaLocked(
		target, entries, plan,
	)
}

// carryBufferedJournalDeltaBeforeFoldLocked preserves a complete non-aligned
// overlay suffix before pressure materializes and recycles the overlay. It never
// syncs. A later explicit Flush fences this append plus any later suffix with
// one Sync. Gaps, structural state, and journal capacity return handled=false
// so the pressure caller takes the ordinary physical checkpoint instead.
func (c *Collection) carryBufferedJournalDeltaBeforeFoldLocked() (
	handled bool, err error,
) {
	if !c.bufferedJournalDeltaLane() {
		return false, nil
	}
	if failure := c.PersistenceError(); failure != nil {
		return true, failure
	}
	view, logicalOK := c.writerLogicalView()
	if !logicalOK || view.state == nil {
		return true, ErrClosed
	}
	target := view.generation
	after := c.journalDeltaAppendedGeneration.Load()
	complete, appendErr :=
		c.appendBufferedJournalDeltaLocked(after, target)
	if appendErr != nil || !complete {
		return complete, appendErr
	}
	if recoveryJournalDeltaCarryHook != nil && target > after {
		recoveryJournalDeltaCarryHook(target)
	}
	return true, nil
}

type bufferedJournalDeltaDrainReason uint8

const (
	bufferedJournalDeltaDrainResource bufferedJournalDeltaDrainReason = 1 << iota
	bufferedJournalDeltaDrainJournal
)

// bufferedJournalDeltaPhysicalDrainReason classifies why another journal-only
// checkpoint would leave too little bounded staging for the next overlay fold.
// Journal durability may run arbitrarily far ahead of the rooted durability
// floor, but the resources held by those device-silent roots may not: committer
// descriptors, dirty cache frames, the free-log lineage they fence, and the
// bounded redo region are all finite.
//
// Half the descriptor arena, capped at 32 staged roots, is a deterministic
// upper bound independent of page shape. The dirty-byte guard retains one
// complete worst-case materialization. A second simultaneous reserve is not a
// safety requirement: materializePrimaryParentsLocked already drains a prior
// published cut and retries the intact overlay when the next fold cannot acquire
// frames. Reserving two made a wider but still resident-bounded dirty-leaf
// window force every cheap Flush onto the physical path.
func (c *Collection) bufferedJournalDeltaPhysicalDrainReason(
	pendingBytes int,
) bufferedJournalDeltaDrainReason {
	if !c.bufferedJournalDeltaLane() {
		return 0
	}
	var reason bufferedJournalDeltaDrainReason
	queueLimit := min(
		32, max(1, fileVisibilitySlots(c.options.QueueSlots)/2),
	)
	if c.committer.Stats().QueuedGenerations >= uint64(queueLimit) {
		reason |= bufferedJournalDeltaDrainResource
	}
	reserve := c.options.maxTransactionBytes
	if c.cache.DirtyCapacityAvailable() < reserve {
		reason |= bufferedJournalDeltaDrainResource
	}

	// Budget this explicit Flush suffix plus one policy-sized future carry.
	// RecoveryBatchRecordPaddedSizeForPayload describes put/delete framing only;
	// a delta batch can add scalar metadata, and the compact lane caps this
	// estimate at 512 KiB. Exact planning still gates every current append, while
	// a future window larger than the reserve takes the bounded physical fallback.
	futureBytes := storeio.RecoveryBatchRecordPaddedSizeForPayload(
		c.journal.Header().SectorSize, primaryUnifiedOverlayRecords,
		c.primaryUnifiedOverlay.capacityBytes(),
	)
	futureBytes = min(
		futureBytes,
		int(recoveryJournalCompactFutureReserveBytes),
	)
	header := c.journal.Header()
	cursor := c.journal.Cursor()
	if cursor > header.Capacity {
		return reason | bufferedJournalDeltaDrainJournal
	}
	remaining := header.Capacity - cursor
	current := uint64(pendingBytes)
	future := uint64(futureBytes)
	if current > remaining || future > remaining-current {
		reason |= bufferedJournalDeltaDrainJournal
	}
	return reason
}

// bufferedJournalDeltaPhysicalDrainNeeded is the boolean form used by callers
// and tests that do not need to distinguish finite staging from journal room.
func (c *Collection) bufferedJournalDeltaPhysicalDrainNeeded(
	pendingBytes int,
) bool {
	return c.bufferedJournalDeltaPhysicalDrainReason(pendingBytes) != 0
}

// drainBufferedJournalDeltaResourcesLocked releases an older staged root when
// finite committer/cache resources are the only reason the current logical cut
// cannot remain journal-only. The caller restarts delta planning after success:
// recycling may have advanced both journal watermarks when the older root
// covered LiveEndGeneration, or retained them when a newer suffix still exists.
func (c *Collection) drainBufferedJournalDeltaResourcesLocked(
	reason bufferedJournalDeltaDrainReason, alreadyDrained bool,
) (bool, error) {
	if reason != bufferedJournalDeltaDrainResource || alreadyDrained ||
		c.committer.PublishedGeneration() <=
			c.committer.DurableGeneration() {
		return false, nil
	}
	if err := c.flushBufferedPublishedLocked(); err != nil {
		return false, err
	}
	return true, nil
}

// drainBufferedJournalDeltaPhysicalLocked advances the physical durability and
// reclamation floor through the current explicit Flush cut. Ordinarily one
// checkpoint both materializes the pending overlay and drains the committer.
// If prior staged cuts have already consumed the materialization reserve, drain
// those first, then retry the still-intact overlay. Recycling is generation
// guarded, so the first drain cannot discard journal records newer than its
// rooted cut.
func (c *Collection) drainBufferedJournalDeltaPhysicalLocked(
	target uint64,
) error {
	if c.cache.DirtyCapacityAvailable() < c.options.maxTransactionBytes ||
		c.committer.NeedsFrameCheckpointFor(c.options.maxTransactionPages) {
		if err := c.flushBufferedPublishedLocked(); err != nil {
			return err
		}
		if c.committer.DurableGeneration() >= target {
			return nil
		}
	}
	return c.checkpointBufferedLocked()
}

// checkpointBufferedJournalDeltaLocked makes the current ordinary
// buffered-visible cut durable with one journal Sync. Any suffix not already
// carried by non-aligned overlay pressure is appended first; a carried suffix
// needs no second append. It returns handled=false for any partial coverage,
// structural state, or journal-capacity failure, so the caller performs the
// existing full leaf/root checkpoint and recycles the journal.
//
// The batch retains every raw record rather than coalescing repeated keys.
// Recovery applies one entry per logical generation, so the reopened public
// generation lands exactly on target as well as reconstructing the same rows
// and exact-index postings. Once a complete suffix has been appended, a failed
// Sync is an unknown outcome: recovery may replay that suffix even though this
// Flush could not acknowledge it.
func (c *Collection) checkpointBufferedJournalDeltaLocked() (
	handled bool, err error,
) {
	if !c.bufferedJournalDeltaLane() {
		return false, nil
	}
	if failure := c.PersistenceError(); failure != nil {
		return true, failure
	}
	view, logicalOK := c.writerLogicalView()
	if !logicalOK || view.state == nil {
		return true, ErrClosed
	}
	target := view.generation
	resourceDrained := false

restart:
	durableAfter := c.journalDeltaGeneration.Load()
	appendedAfter := c.journalDeltaAppendedGeneration.Load()
	if target < durableAfter || target < appendedAfter ||
		appendedAfter < durableAfter {
		return true, storeio.ErrGenerationOrder
	}
	reason := c.bufferedJournalDeltaPhysicalDrainReason(0)
	// Resource relief is profitable only for a cut this lane can finish with a
	// journal fence. An exceptional/structural cut must retain the old behavior:
	// publish its current physical state first, then let one committer Flush
	// coalesce that root with any older staged cut.
	if target > durableAfter &&
		!c.bufferedJournalDeltaStateEligible(appendedAfter) {
		if reason != 0 {
			return true, c.drainBufferedJournalDeltaPhysicalLocked(target)
		}
		return false, nil
	}
	if reason != 0 {
		drained, drainErr := c.drainBufferedJournalDeltaResourcesLocked(
			reason, resourceDrained,
		)
		if drainErr != nil {
			return true, drainErr
		}
		if drained {
			resourceDrained = true
			goto restart
		}
		return true, c.drainBufferedJournalDeltaPhysicalLocked(target)
	}
	if target == durableAfter {
		return true, nil
	}
	// The cheap cut may coexist with resident exact-index deltas: recovery
	// deterministically rebuilds them by replaying the same primary mutations.
	//
	// A device-silent row-overlay fold is also safe once the appended journal
	// watermark covers its complete generation. The committer's newer root is
	// only staged in memory; after a crash the old durable root plus the retained
	// journal reconstructs the same cut. primaryCheckpointBase identifies that
	// staged root exactly. Do not admit an arbitrary physical gap here: a
	// structural/COW publication newer than the appended watermark is not
	// represented by checkpointEntries and must take the ordinary full
	// checkpoint.
	if target > appendedAfter {
		entries, plan, complete, prepareErr :=
			c.prepareBufferedJournalDeltaLocked(appendedAfter, target)
		if prepareErr != nil {
			return true, prepareErr
		}
		if !complete {
			return false, nil
		}
		reason = c.bufferedJournalDeltaPhysicalDrainReason(
			plan.PaddedSize(),
		)
		if reason != 0 {
			drained, drainErr := c.drainBufferedJournalDeltaResourcesLocked(
				reason, resourceDrained,
			)
			if drainErr != nil {
				return true, drainErr
			}
			if drained {
				resourceDrained = true
				goto restart
			}
			return true, c.drainBufferedJournalDeltaPhysicalLocked(target)
		}
		complete, appendErr := c.appendPreparedBufferedJournalDeltaLocked(
			target, entries, plan,
		)
		if appendErr != nil {
			return true, appendErr
		}
		if !complete {
			return false, nil
		}
	}
	if recoveryJournalDeltaPreSyncHook != nil {
		recoveryJournalDeltaPreSyncHook(target)
	}
	started := time.Now()
	if syncErr := c.journal.Sync(c.journalPowerSafe); syncErr != nil {
		return true, c.poisonJournalCommitOutcomeUnknown(syncErr)
	}
	syncNS := uint64(time.Since(started))
	if recoveryJournalDeltaPostSyncHook != nil {
		recoveryJournalDeltaPostSyncHook(target)
	}

	// The watermark is the point of no return: only a completed journal fence
	// may make this generation observable as crash-safe. The physical committer
	// generation deliberately remains at the old root for allocator/recycle
	// decisions until pressure or Close performs a full fold.
	c.journalDeltaGeneration.Store(target)
	c.journalDeltaCheckpoints.Add(1)
	c.journalDeltaSyncNS.observe(syncNS)
	return true, nil
}

// closeRecoveryJournalLocked closes the journal file. The final checkpoint on
// Close already recycled it to the durable generation, so a reopen replays
// nothing.
func (c *Collection) closeRecoveryJournalLocked() error {
	if c.journal == nil {
		return nil
	}
	journal := c.journal
	c.journal = nil
	return journal.Close()
}
