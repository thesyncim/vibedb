package durable

import (
	"errors"
	"fmt"
	"os"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// The recovery journal wiring makes a DurabilityBufferedVisible acknowledgement
// durable at the price of one bounded append plus one sync, instead of leaving
// it volatile until the next checkpoint. A frame-deferred mutation is applied to
// the canonical frames exactly as buffered-visible already does, then its redo
// record — key, value, generation — is appended to a sibling journal file and
// that file alone is synced. Readers never consult the journal: visibility comes
// from the canonical frames, unchanged. A checkpoint folds the records into the
// ordinary root publication and recycles the journal head in the same fence.
//
// Only buffered-visible carries a journal. Its deferred-root, immediate-visibility
// read path is exactly what a redo lane needs, and adding write-side durability
// does not perturb a single reader decision — the standing "no read-path change"
// gate holds by construction.

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
	recoveryJournalMaxCapacityBytes = storeio.RecoveryJournalMaxCapacityBytes
)

// ErrRecoveryJournalRequiresPrimary reports that Options.RecoveryJournal was
// requested for a store whose root has no ordered primary graph. Only the
// primary mutation path acknowledges through the journal (journalAckLocked fires
// from putPrimary/deletePrimary); a chunk-layout store would pair a journal that
// no acknowledgement ever appends to, so the request fails closed rather than
// mint a file that silently carries no per-mutation durability.
var ErrRecoveryJournalRequiresPrimary = errors.New(
	"vibejson: recovery journal requires an ordered primary-graph store",
)

// journalFailureBox carries the sticky journal poison behind an atomic pointer.
type journalFailureBox struct{ err error }

// poisonJournalLocked records a terminal journal failure and rolls the reader
// view to the committer's poison rules, then returns the sticky error. It is
// idempotent: the first failure wins so later callers report the original cause.
// The caller holds the writer.
func (c *Collection) poisonJournalLocked(cause error) error {
	if cause == nil {
		return nil
	}
	c.journalFailure.CompareAndSwap(nil, &journalFailureBox{
		err: fmt.Errorf(
			"vibejson: recovery journal acknowledgement failed, reopen required: %w",
			cause,
		),
	})
	// Roll the reader view exactly as an automatic-persistence failure does. The
	// journal is buffered-only, so this preserves the last admitted immutable view
	// while every later mutation is rejected by PersistenceError.
	c.poisonPersistence(cause)
	return c.journalFailure.Load().err
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

// journalEnabled reports whether this collection acknowledges through a recovery
// journal. It is true only for a buffered-visible collection configured with
// Options.RecoveryJournal whose journal file is open.
func (c *Collection) journalEnabled() bool {
	return c != nil && c.journal != nil
}

// journalConfigured reports whether the collection's options request a journal.
// It is decided from options alone so Create and Open can size and mint the
// journal before c.journal is set.
func (c *Collection) journalConfigured() bool {
	return c.options.RecoveryJournal &&
		c.options.Durability == DurabilityBufferedVisible
}

// recoveryJournalSuffix names a store file's paired recovery journal, which
// lives beside the store file and is resolved by path on reopen. It is exported
// through RecoveryJournalPath so a caller that publishes a store file by renaming
// it (a SQL table's atomic temp-to-final publish, say) can relocate the journal
// with it; leaving the journal behind makes a journaled root unopenable.
const recoveryJournalSuffix = ".rjournal"

// RecoveryJournalPath returns the recovery-journal sibling path for a store file
// at storePath. A journaled collection (any synchronous collection, and a
// buffered-visible one that opted into RecoveryJournal) writes its journal here,
// and Open resolves it here, so any external move of the store file must move
// this alongside it.
func RecoveryJournalPath(storePath string) string {
	return storePath + recoveryJournalSuffix
}

// journalSiblingPath returns the store file's paired journal path.
func (c *Collection) journalSiblingPath() (string, error) {
	name := c.file.Name()
	if name == "" {
		return "", fmt.Errorf("vibejson: recovery journal requires a named store file")
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
// 2048 overflow records: one must fit, the journal recycles at the next.
func recoveryJournalCapacityBytesFor(
	sectorSize uint32, maxKeyBytes, inlineValueBytes, maxDocumentBytes int,
) uint64 {
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
	baseGeneration uint64,
) storeio.RecoveryJournalHeader {
	sectorSize := uint32(storeio.RecoveryJournalMinSectorSize)
	return storeio.RecoveryJournalHeader{
		StoreID:        storeID,
		JournalID:      journalID,
		PageSize:       pageSize,
		SectorSize:     sectorSize,
		BaseGeneration: baseGeneration,
		BaseSequence:   0,
		Capacity: recoveryJournalCapacityBytesFor(
			sectorSize, maxKeyBytes, inlineValueBytes, maxDocumentBytes,
		),
	}
}

// createSiblingRecoveryJournal preallocates and syncs a fresh journal file
// beside storePath, then closes it. A one-shot builder (CreateFromPrimary) uses
// this to mint the journal a later Open will pair, replay, and own.
func createSiblingRecoveryJournal(
	storePath string, header storeio.RecoveryJournalHeader,
) error {
	if storePath == "" {
		return fmt.Errorf("vibejson: recovery journal requires a named store file")
	}
	path := storePath + ".rjournal"
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("vibejson: create recovery journal file: %w", err)
	}
	journal, err := storeio.CreateRecoveryJournal(file, header)
	if err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return fmt.Errorf("vibejson: create recovery journal: %w", err)
	}
	return journal.Close()
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
		return fmt.Errorf("vibejson: open recovery journal file: %w", err)
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
	c.journalID = journalID
	c.journalPowerSafe = c.options.CheckpointStrength != CheckpointFilesystem
	c.journal = journal
	if recoveryJournalFaultHook != nil {
		recoveryJournalFaultHook(journal)
	}
	return nil
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
	apply := func(kind uint16, key string, value []byte) error {
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
	err := c.journal.Replay(rootGeneration, func(rec storeio.RecoveryRecord) error {
		if rec.Kind == storeio.RecoveryRecordKindBatch {
			// A batch record replays as its entries applied in order through the
			// ordinary mutation path. The record is present in whole (its single CRC
			// validated) or not at all, so this loop never sees a partial batch.
			for i := range rec.Entries {
				entry := rec.Entries[i]
				if err := apply(entry.Kind, string(entry.Key), entry.Value); err != nil {
					return err
				}
			}
			return nil
		}
		return apply(rec.Kind, string(rec.Key), rec.Value)
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
	return c.recycleRecoveryJournalLocked(c.committer.DurableGeneration())
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
	if baseGeneration < c.journal.BaseGeneration() {
		// The durable generation never regressed; nothing to recycle past.
		return nil
	}
	return c.journal.Recycle(baseGeneration)
}

// journalAckLocked appends one redo record for a frame-deferred mutation and
// syncs the journal alone, turning a volatile buffered acknowledgement into a
// durable one. A full journal forces a checkpoint — which recycles it — exactly
// like dirty-cache pressure, after which the just-published mutation is already
// durable in the checkpointed root, so no record is needed and the ack is
// counted against the chain lane instead. The caller holds the writer and has
// already published the mutation's state.
func (c *Collection) journalAckLocked(
	kind uint16, generation uint64, key, value []byte,
) error {
	if !c.journalEnabled() || c.journalReplaying {
		return nil
	}
	if !c.journal.Fits(len(key), len(value)) {
		// No room for this record. Checkpoint folds every deferred mutation —
		// including the one just published — into a durable root and recycles the
		// journal. The mutation is durable afterwards without its own record.
		if err := c.checkpointBufferedLocked(); err != nil {
			return err
		}
		c.automaticCheckpoints.Add(1)
		c.chainAcks.Add(1)
		return nil
	}
	if _, err := c.journal.Append(kind, generation, key, value); err != nil {
		if errors.Is(err, storeio.ErrRecoveryJournalFull) {
			if cpErr := c.checkpointBufferedLocked(); cpErr != nil {
				return cpErr
			}
			c.automaticCheckpoints.Add(1)
			c.chainAcks.Add(1)
			return nil
		}
		// A raw append error (a device write failure such as ENOSPC or EIO) is
		// terminal for the journal lane: poison die-don't-retry.
		return c.poisonJournalLocked(err)
	}
	if err := c.journal.Sync(c.journalPowerSafe); err != nil {
		return c.poisonJournalLocked(err)
	}
	c.journalAcks.Add(1)
	return nil
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
// journal lane, poisoning die-don't-retry with nothing published.
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
		return c.poisonJournalLocked(err)
	}
	if err := c.journal.Sync(c.journalPowerSafe); err != nil {
		return c.poisonJournalLocked(err)
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
// and poisons die-don't-retry with nothing published. It is a no-op for
// buffered-visible (which journals its batch after publishing) and during replay.
func (c *Collection) journalBatchBeforePublishLocked(
	generation uint64, entries []storeio.RecoveryBatchEntry,
) error {
	if !c.syncJournalLane() || c.journalReplaying {
		return nil
	}
	if _, err := c.journal.AppendBatch(generation, entries); err != nil {
		return c.poisonJournalLocked(err)
	}
	if err := c.journal.Sync(c.journalPowerSafe); err != nil {
		return c.poisonJournalLocked(err)
	}
	if recoveryJournalPostSyncHook != nil {
		recoveryJournalPostSyncHook()
	}
	c.journalAcks.Add(1)
	return nil
}

// journalBatchAckLocked is the buffered-visible lane's batch acknowledgement: it
// makes an already-published batch durable with one append plus one sync, the
// group-commit analogue of journalAckLocked. Capacity is ensured before prepare,
// but the full-journal fallback is kept for the same reason the single-record
// path keeps it — a checkpoint folds the just-published batch into a durable root
// and recycles, after which the batch is durable without its own record. The
// caller holds the writer and has already published the batch's state.
func (c *Collection) journalBatchAckLocked(
	generation uint64, entries []storeio.RecoveryBatchEntry,
) error {
	if !c.journalEnabled() || c.journalReplaying {
		return nil
	}
	if _, err := c.journal.AppendBatch(generation, entries); err != nil {
		if errors.Is(err, storeio.ErrRecoveryJournalFull) {
			if cpErr := c.checkpointBufferedLocked(); cpErr != nil {
				return cpErr
			}
			c.automaticCheckpoints.Add(1)
			c.chainAcks.Add(1)
			return nil
		}
		return c.poisonJournalLocked(err)
	}
	if err := c.journal.Sync(c.journalPowerSafe); err != nil {
		return c.poisonJournalLocked(err)
	}
	c.journalAcks.Add(1)
	return nil
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
	if !c.journalEnabled() || c.journalReplaying {
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
