package durable

import (
	"errors"
	"fmt"
	"os"

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
	// overlays start from two legacy-framed arena estimates below, subject to the
	// compact cap and exact foreground fallback checks.
	recoveryJournalDeltaMinCapacityBytes = uint64(1) << 20
	// Compact scalar-patch redo keeps the ordinary buffered journal fully
	// preallocated while cutting its deterministic disk reservation to 2.5 MiB.
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
// Append failures and recycle failures deliberately stay on poisonJournal.
// An append failure precedes the durability boundary and recovery rejects its
// incomplete tail; a recycle follows an already-durable physical root and
// therefore cannot make that root's commit outcome ambiguous.
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
// Every newly created buffered-visible store carries one: RecoveryJournal
// selects per-mutation durable acknowledgement, while the ordinary lane uses
// it only at Flush boundaries. The synchronous lane is also journal-backed
// unconditionally. A synchronous store whose root names no journal (created
// async-visible, reopened sync) has none and stays on the committer fence
// (chainFenceSync).
func (c *Collection) journalEnabled() bool {
	return c != nil && c.journal != nil
}

// journalConfigured reports whether a newly created collection needs the
// recovery-journal sibling. Every buffered-visible store carries one: the
// RecoveryJournal option selects per-mutation durable acknowledgements, while
// ordinary buffered-visible uses the same file only when Flush publishes a
// bounded delta checkpoint. It is decided from options alone so Create can mint
// the sibling before c.journal is set.
func (c *Collection) journalConfigured() bool {
	return c.options.Durability == DurabilityBufferedVisible
}

// bufferedJournalAckLane is the opt-in buffered mode that appends and syncs a
// redo record for every acknowledged mutation. Merely having a paired journal
// no longer selects this path: ordinary buffered-visible also has a journal,
// but keeps mutation acknowledgements volatile and writes it only from Flush.
func (c *Collection) bufferedJournalAckLane() bool {
	return c.buffered() && c.options.RecoveryJournal &&
		c.journalEnabled()
}

// bufferedJournalDeltaLane is the ordinary buffered-visible checkpoint lane.
// It never runs during recovery replay: Open must physically fold and recycle
// the records it is currently consuming.
func (c *Collection) bufferedJournalDeltaLane() bool {
	return c.buffered() && !c.options.RecoveryJournal &&
		c.journalEnabled() && !c.journalReplaying
}

// newBufferedJournalDeltaEntryScratch retains the complete fixed framing
// window only for stores that can emit ordinary buffered overlay deltas. The
// returned slice intentionally has full length and capacity; callers reset it
// to length zero while preserving an allocation-free append ceiling.
func newBufferedJournalDeltaEntryScratch(
	options normalizedFileStoreOptions,
) []storeio.RecoveryBatchEntry {
	if options.Durability != DurabilityBufferedVisible ||
		options.RecoveryJournal ||
		options.primaryUnifiedOverlayBytes == 0 {
		return nil
	}
	return make([]storeio.RecoveryBatchEntry, primaryUnifiedOverlayRecords)
}

// recoveryJournalSuffix names a store file's paired recovery journal, which
// lives beside the store file and is resolved by path on reopen. It is exported
// through RecoveryJournalPath so a caller that publishes a store file by renaming
// it (a SQL table's atomic temp-to-final publish, say) can relocate the journal
// with it; leaving the journal behind makes a journaled root unopenable.
const recoveryJournalSuffix = collectionname.JournalSuffix

// RecoveryJournalPath returns the recovery-journal sibling path for a store file
// at storePath. Every buffered-visible or synchronous primary store writes its
// journal here, and Open resolves it here, so an external move of the store file
// must move this sibling with it.
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
		// Seed the ordinary buffered-visible reservation from two legacy-framed
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

// recoveryJournalHeaderFor builds the header identity and geometry for a store
// paired at baseGeneration.
func recoveryJournalHeaderFor(
	storeID, journalID [16]byte,
	pageSize uint32,
	maxKeyBytes, inlineValueBytes, maxDocumentBytes int,
	baseGeneration uint64, deltaOverlayBytes int,
) storeio.RecoveryJournalHeader {
	sectorSize := uint32(storeio.RecoveryJournalMinSectorSize)
	formatVersion := storeio.RecoveryJournalFormatLegacy
	if deltaOverlayBytes > 0 {
		formatVersion = storeio.RecoveryJournalFormatScalarPatch
	}
	return storeio.RecoveryJournalHeader{
		FormatVersion:  formatVersion,
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
	journal, err := storeio.OpenRecoveryJournal(file)
	if err != nil {
		_ = file.Close()
		return err
	}
	if err := journal.Pair(
		c.storeID, journalID, uint32(c.options.PageSize), rootGeneration,
	); err != nil {
		_ = journal.Close()
		return err
	}
	if journal.Header().FormatVersion ==
		storeio.RecoveryJournalFormatScalarPatch &&
		(!c.buffered() || c.options.RecoveryJournal ||
			c.primaryUnifiedOverlay == nil || len(c.journalDeltaEntries) == 0) {
		_ = journal.Close()
		return fmt.Errorf(
			"%w: format-1 journal requires ordinary buffered delta mode",
			storeio.ErrRecoveryJournalGeometry,
		)
	}
	c.journalID = journalID
	c.journalPowerSafe = c.options.CheckpointStrength != CheckpointFilesystem
	c.journal = journal
	c.journalDeltaGeneration.Store(rootGeneration)
	c.journalDeltaAppendedGeneration.Store(rootGeneration)
	c.initJournalGroupLocked()
	if recoveryJournalFaultHook != nil {
		recoveryJournalFaultHook(journal)
	}
	return nil
}

// recoveryJournalBatchReplayStart returns the first entry not already covered
// by the recovered physical root. Format-1 journals are used only by the
// ordinary buffered delta lane, whose batches contain exactly one entry for
// every consecutive generation ending at rec.Generation. Replay itself can
// checkpoint under bounded staging pressure while deliberately retaining the
// journal; after a second crash, skipping this durable prefix is what makes a
// length-changing scalar patch replayable rather than applying it twice.
//
// Legacy journals retain their atomic WriteBatch grammar: every entry belongs
// to the record's one generation, so they always replay from entry zero.
func recoveryJournalBatchReplayStart(
	formatVersion uint32, rec storeio.RecoveryRecord, rootGeneration uint64,
) (int, error) {
	if formatVersion != storeio.RecoveryJournalFormatScalarPatch {
		return 0, nil
	}
	count := uint64(len(rec.Entries))
	if count == 0 || rec.Generation < count {
		return 0, fmt.Errorf(
			"%w: invalid scalar-format batch generation range target=%d entries=%d",
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

// replayRecoveryJournalLocked re-applies every journaled record newer than the
// recovered root's generation through the ordinary mutation path, then
// checkpoints and recycles so the journal is empty and the store's durable root
// covers every replayed acknowledgement. Replay suppresses its own journal
// appends: the records are already durable, and re-journaling them would be
// redundant work that the immediately-following recycle discards anyway.
func (c *Collection) replayRecoveryJournalLocked(rootGeneration uint64) error {
	c.journalReplaying = true
	defer func() { c.journalReplaying = false }()
	applied := 0
	formatVersion := c.journal.Header().FormatVersion
	// Legacy records are logical set/delete operations and therefore idempotent.
	// Inspect their complete live suffix from the journal's own base even when a
	// prior interrupted recovery checkpoint advanced the physical root into that
	// suffix. In particular, a legacy batch has one record generation for all of
	// its entries; filtering it against a prefix checkpoint could otherwise skip
	// the un-applied tail on the second recovery. Covered point records before the
	// first ambiguous batch remain safely skipped. Once such a batch is found, its
	// entire following record suffix is replayed in journal order so later covered
	// points restore any key the older batch also touched. Format 1 contains scalar
	// patches that are not idempotent, so it retains its exact prefix filter.
	replayAfter := rootGeneration
	if formatVersion == storeio.RecoveryJournalFormatLegacy {
		replayAfter = c.journal.BaseGeneration()
	}
	replayCoveredLegacySuffix := false
	var scalarPatchScratch []byte
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
	applyLegacyBatchSequential := func(rec storeio.RecoveryRecord) error {
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
	applyScalarPatch := func(entry storeio.RecoveryBatchEntry) error {
		scalarPatchScratch = scalarPatchScratch[:0]
		current, found, readErr := c.AppendRaw(
			scalarPatchScratch, entry.Key,
		)
		if readErr != nil {
			return readErr
		}
		metadata := entry.ScalarPatch
		offset := int(metadata.CanonicalOffset)
		oldLength := int(metadata.OldScalarLength)
		if !found || oldLength == 0 || offset > len(current) ||
			oldLength > len(current)-offset {
			return fmt.Errorf(
				"%w: scalar-patch preimage key=%q offset=%d old=%d bytes=%d",
				storeio.ErrRecoveryJournalRecord, entry.Key, offset,
				oldLength, len(current),
			)
		}
		oldDocumentBytes := len(current)
		newDocumentBytes := oldDocumentBytes - oldLength + len(entry.Value)
		if newDocumentBytes <= 0 ||
			newDocumentBytes > c.options.MaxDocumentBytes {
			return fmt.Errorf(
				"%w: scalar-patch result length %d",
				storeio.ErrRecoveryJournalRecord, newDocumentBytes,
			)
		}
		if newDocumentBytes > oldDocumentBytes {
			current = append(current, make(
				[]byte, newDocumentBytes-oldDocumentBytes,
			)...)
		}
		copy(
			current[offset+len(entry.Value):newDocumentBytes],
			current[offset+oldLength:oldDocumentBytes],
		)
		current = current[:newDocumentBytes]
		copy(current[offset:], entry.Value)
		if storeio.PageChecksum(current) !=
			metadata.ExpectedResultChecksum {
			return fmt.Errorf(
				"%w: scalar-patch result checksum key=%q",
				storeio.ErrRecoveryJournalRecord, entry.Key,
			)
		}
		_, putErr := c.Put(entry.Key, current)
		if putErr == nil {
			applied++
			scalarPatchScratch = current[:0]
		}
		return putErr
	}
	err := c.journal.Replay(replayAfter, func(rec storeio.RecoveryRecord) error {
		if formatVersion == storeio.RecoveryJournalFormatLegacy &&
			rec.Generation <= rootGeneration && !replayCoveredLegacySuffix {
			if rec.Kind != storeio.RecoveryRecordKindBatch {
				return nil
			}
			// The recovered root may be a checkpoint of only a prefix of this
			// one-generation batch. Reapply it atomically, then preserve journal
			// order for every later covered record.
			replayCoveredLegacySuffix = true
		}
		if rec.Kind == storeio.RecoveryRecordKindBatch {
			start, startErr := recoveryJournalBatchReplayStart(
				formatVersion, rec, rootGeneration,
			)
			if startErr != nil {
				return startErr
			}
			if formatVersion == storeio.RecoveryJournalFormatLegacy {
				// MaxBatchDocuments/MaxBatchBytes are reopen-time admission policy,
				// not persisted journal semantics. When the current process sized its
				// batch arenas below this acknowledged record, replay the entries
				// sequentially while Open is still private. The journal remains intact
				// across any pressure checkpoint; a second recovery re-enters at the
				// first covered batch and restores its complete ordered suffix.
				if !c.legacyRecoveryBatchFitsUpdate(rec) {
					return applyLegacyBatchSequential(rec)
				}
				// A legacy batch is one transactional WriteBatch, not a sequence of
				// independently visible point mutations. Replaying through Update keeps
				// its primary rows and exact-index postings atomic in memory and prevents
				// a pressure checkpoint from persisting a prefix that a second recovery
				// could mistake for the whole one-generation record.
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
								"%w: unknown legacy batch kind %d",
								storeio.ErrRecoveryJournalRecord, entry.Kind,
							)
						}
					}
					return nil
				})
				if errors.Is(batchErr, ErrBatchTooLarge) {
					return applyLegacyBatchSequential(rec)
				}
				if batchErr != nil {
					return batchErr
				}
				// Count a successfully consumed legacy batch even when every entry
				// is already a no-op (notably a pure-delete batch checkpointed by an
				// earlier interrupted replay). The final checkpoint/recycle must still
				// consume that ambiguous record instead of replaying it on every Open.
				applied++
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
			// A format-1 delta batch is intentionally a consecutive sequence of
			// point generations. A pressure checkpoint may cover a prefix; start
			// skips exactly that durable prefix before applying the remainder.
			for i := start; i < len(rec.Entries); i++ {
				entry := rec.Entries[i]
				// The mutation path borrows the key; replay hands the record's
				// []byte straight back with no string round-trip.
				var entryErr error
				if entry.Kind == storeio.RecoveryRecordKindScalarPatch {
					entryErr = applyScalarPatch(entry)
				} else {
					entryErr = apply(entry.Kind, entry.Key, entry.Value)
				}
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
	if applied == 0 {
		// A cleanly-recycled journal replays nothing; the durable root already
		// covers every acknowledgement, so there is nothing to fold and no reason
		// to publish a redundant checkpoint. The journal is left as recovered and
		// its next recycle rides the first ordinary checkpoint.
		return nil
	}
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
	if err := c.recycleRecoveryJournalLocked(physicalGeneration); err != nil {
		return err
	}
	// Normal physical completion owns this hook. Recovery recycles directly
	// after its final fold, so it performs the same one bounded pass explicitly.
	return c.punchNewPhysicalGenerationLocked(physicalGeneration)
}

func (c *Collection) legacyRecoveryBatchFitsUpdate(
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
	// A device-silent fold may have appended a newer ordinary-buffered suffix
	// without syncing it yet. Committer staging pressure is allowed to flush an
	// older rooted cut, but that older physical generation cannot recycle the
	// journal: doing so would discard the carried suffix and regress both
	// logical watermarks. Retain the journal until a physical root covers every
	// appended generation. An explicit Flush may sync that suffix, but the
	// records still cannot be discarded until a physical root covers them.
	if c.bufferedJournalDeltaLane() &&
		baseGeneration < max(
			c.journalDeltaGeneration.Load(),
			c.journalDeltaAppendedGeneration.Load(),
		) {
		return nil
	}
	if baseGeneration < c.journal.BaseGeneration() {
		// The durable generation never regressed; nothing to recycle past.
		return nil
	}
	if err := c.journal.Recycle(
		baseGeneration, c.journalPowerSafe,
	); err != nil {
		// A failed recycle is a device write or sync failure on the journal
		// header (Recycle never reports full), and it is terminal the same way a
		// failed record append is: the caller's mutation may already be
		// published, so returning a plain error would let every later Put
		// re-publish and re-fail forever with PersistenceError still nil.
		// Poison die-don't-retry and fail the shared group fence so no deposited
		// waiter is acknowledged out of a journal whose head can no longer move.
		poisoned := c.poisonJournal(err)
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
	if _, err := c.journal.Append(kind, generation, key, value); err != nil {
		if errors.Is(err, storeio.ErrRecoveryJournalFull) {
			if cpErr := c.checkpointBufferedLocked(); cpErr != nil {
				return 0, cpErr
			}
			c.automaticCheckpoints.Add(1)
			c.chainAcks.Add(1)
			return 0, nil
		}
		// A raw append error (a device write failure such as ENOSPC or EIO) is
		// terminal for the journal lane: poison die-don't-retry and fail every
		// waiter of the shared fence, none of which can now be synced.
		poisoned := c.poisonJournal(err)
		c.journalGroup.fail(poisoned)
		return 0, poisoned
	}
	c.journalAcks.Add(1)
	return c.journalGroup.depositBump(), nil
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
// journal lane, poisoning die-don't-retry with nothing published. An append
// failure is a definite rejection; a failure from the following Sync has an
// unknown commit outcome because its complete record may replay after reopen.
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
	if _, err := c.journal.Append(kind, generation, key, value); err != nil {
		return c.poisonJournal(err)
	}
	if err := c.journal.Sync(c.journalPowerSafe); err != nil {
		return c.poisonJournalCommitOutcomeUnknown(err)
	}
	if recoveryJournalPostSyncHook != nil {
		recoveryJournalPostSyncHook()
	}
	c.journalAcks.Add(1)
	return nil
}

// journalBatchBeforePublishLocked is the sync lane's batch acknowledgement: it
// appends the batch's single redo record and syncs it at the batch's point of no
// return — after every document's fallible prepare and every leaf frame's dirty
// admission, and before any leaf pointer is published — so no reader observes any
// member of the batch before the whole group is durable. Journal capacity is
// ensured for the whole record before prepare (ensurePrimaryBatchJournalRoom), so
// the append cannot report full here; an append or sync error is a device failure
// and poisons die-don't-retry with nothing published. Append failure rejects the
// whole batch definitely; Sync failure leaves the whole record's commit outcome
// unknown because recovery may replay it atomically. It is a no-op for buffered-
// visible (which journals its batch after publishing) and during replay.
func (c *Collection) journalBatchBeforePublishLocked(
	generation uint64, entries []storeio.RecoveryBatchEntry,
) error {
	if !c.syncJournalLane() || c.journalReplaying {
		return nil
	}
	if _, err := c.journal.AppendBatch(generation, entries); err != nil {
		return c.poisonJournal(err)
	}
	if err := c.journal.Sync(c.journalPowerSafe); err != nil {
		return c.poisonJournalCommitOutcomeUnknown(err)
	}
	if recoveryJournalPostSyncHook != nil {
		recoveryJournalPostSyncHook()
	}
	c.journalAcks.Add(1)
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
	if _, err := c.journal.AppendBatch(generation, entries); err != nil {
		if errors.Is(err, storeio.ErrRecoveryJournalFull) {
			if cpErr := c.checkpointBufferedLocked(); cpErr != nil {
				return 0, cpErr
			}
			c.automaticCheckpoints.Add(1)
			c.chainAcks.Add(1)
			return 0, nil
		}
		poisoned := c.poisonJournal(err)
		c.journalGroup.fail(poisoned)
		return 0, poisoned
	}
	c.journalAcks.Add(1)
	return c.journalGroup.depositBump(), nil
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
	if err := c.checkpointBufferedLocked(); err != nil {
		return err
	}
	c.automaticCheckpoints.Add(1)
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
	entries, covered, entryErr :=
		c.primaryUnifiedOverlay.checkpointEntriesMode(
			c.journalDeltaEntries[:0], after, target,
			c.journal.Header().FormatVersion ==
				storeio.RecoveryJournalFormatScalarPatch,
		)
	if entryErr != nil {
		return nil, storeio.RecoveryBatchPlan{}, false, entryErr
	}
	if !covered || len(entries) == 0 {
		return nil, storeio.RecoveryBatchPlan{}, false, nil
	}
	plan, planErr := c.journal.PrepareBatch(entries)
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
	if _, appendErr := c.journal.AppendPreparedBatch(
		target, entries, plan,
	); appendErr != nil {
		if errors.Is(appendErr, storeio.ErrRecoveryJournalFull) {
			return false, nil
		}
		return false, c.poisonJournal(appendErr)
	}
	c.journalDeltaAppendedGeneration.Store(target)
	c.journalDeltaRecords.Add(uint64(len(entries)))
	c.journalDeltaBytes.Add(uint64(plan.PaddedSize()))
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

// bufferedJournalDeltaPhysicalDrainNeeded reports whether another journal-only
// checkpoint would leave too little bounded physical staging for the next
// overlay fold. Journal durability may run arbitrarily far ahead of the rooted
// durability floor, but the resources held by those device-silent roots may
// not: committer descriptors, dirty cache frames, the free-log lineage they
// fence, and the bounded redo region are all finite.
//
// Half the descriptor arena, capped at 32 staged roots, is a deterministic
// upper bound independent of page shape. The dirty-byte guard retains one
// complete worst-case materialization. A second simultaneous reserve is not a
// safety requirement: materializePrimaryParentsLocked already drains a prior
// published cut and retries the intact overlay when the next fold cannot acquire
// frames. Reserving two made a wider but still resident-bounded dirty-leaf
// window force every cheap Flush onto the physical path.
func (c *Collection) bufferedJournalDeltaPhysicalDrainNeeded(
	pendingBytes int,
) bool {
	if !c.bufferedJournalDeltaLane() {
		return false
	}
	queueLimit := min(
		32, max(1, fileVisibilitySlots(c.options.QueueSlots)/2),
	)
	if c.committer.Stats().QueuedGenerations >= uint64(queueLimit) {
		return true
	}
	reserve := c.options.maxTransactionBytes
	if c.cache.DirtyCapacityAvailable() < reserve {
		return true
	}

	// Budget this explicit Flush suffix plus one policy-sized future carry.
	// RecoveryBatchRecordPaddedSizeForPayload describes legacy framing only; a v1
	// batch can add scalar metadata, and the compact lane deliberately caps this
	// estimate at 512 KiB. Exact planning still gates every current append, while
	// a future window larger than the reserve takes the bounded physical fallback.
	header := c.journal.Header()
	futureBytes := storeio.RecoveryBatchRecordPaddedSizeForPayload(
		header.SectorSize, primaryUnifiedOverlayRecords,
		len(c.primaryUnifiedOverlay.arena),
	)
	if header.FormatVersion == storeio.RecoveryJournalFormatScalarPatch {
		futureBytes = min(
			futureBytes,
			int(recoveryJournalCompactFutureReserveBytes),
		)
	}
	cursor := c.journal.Cursor()
	if cursor > header.Capacity {
		return true
	}
	remaining := header.Capacity - cursor
	current := uint64(pendingBytes)
	future := uint64(futureBytes)
	return current > remaining || future > remaining-current
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
	durableAfter := c.journalDeltaGeneration.Load()
	appendedAfter := c.journalDeltaAppendedGeneration.Load()
	if target == durableAfter {
		if appendedAfter != durableAfter {
			return true, storeio.ErrGenerationOrder
		}
		if c.bufferedJournalDeltaPhysicalDrainNeeded(0) {
			return true, c.drainBufferedJournalDeltaPhysicalLocked(target)
		}
		return true, nil
	}
	if target < durableAfter || target < appendedAfter ||
		appendedAfter < durableAfter {
		return true, storeio.ErrGenerationOrder
	}
	if c.bufferedJournalDeltaPhysicalDrainNeeded(0) {
		return true, c.drainBufferedJournalDeltaPhysicalLocked(target)
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
	if !c.bufferedJournalDeltaStateEligible(appendedAfter) {
		return false, nil
	}
	if target > appendedAfter {
		entries, plan, complete, prepareErr :=
			c.prepareBufferedJournalDeltaLocked(appendedAfter, target)
		if prepareErr != nil {
			return true, prepareErr
		}
		if !complete {
			return false, nil
		}
		if c.bufferedJournalDeltaPhysicalDrainNeeded(plan.PaddedSize()) {
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
	if syncErr := c.journal.Sync(c.journalPowerSafe); syncErr != nil {
		return true, c.poisonJournalCommitOutcomeUnknown(syncErr)
	}
	if recoveryJournalDeltaPostSyncHook != nil {
		recoveryJournalDeltaPostSyncHook(target)
	}

	// The watermark is the point of no return: only a completed journal fence
	// may make this generation observable as crash-safe. The physical committer
	// generation deliberately remains at the old root for allocator/recycle
	// decisions until pressure or Close performs a full fold.
	c.journalDeltaGeneration.Store(target)
	c.journalDeltaCheckpoints.Add(1)
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
