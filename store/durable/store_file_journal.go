package durable

import (
	"crypto/rand"
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
	recoveryJournalMaxCapacityBytes = uint64(16) << 20
)

// recoveryJournalFaultHook, when non-nil, is invoked with each journal this
// collection creates or opens. The exhaustive store-level crash sweep sets it to
// install a FaultJournal over the journal's raw writes; production leaves it nil.
var recoveryJournalFaultHook func(*storeio.RecoveryJournal)

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

// journalSiblingPath returns the store file's paired journal path.
func (c *Collection) journalSiblingPath() (string, error) {
	name := c.file.Name()
	if name == "" {
		return "", fmt.Errorf("vibejson: recovery journal requires a named store file")
	}
	return name + ".rjournal", nil
}

// recoveryJournalCapacityBytesFor derives the preallocated record-region size
// from the store's admission bounds and the checkpoint-record cadence, clamped
// and sector-aligned. It is a free function so both Create and CreateFromPrimary
// size an identical journal.
func recoveryJournalCapacityBytesFor(
	sectorSize uint32, maxKeyBytes, inlineValueBytes int,
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
	sector := uint64(sectorSize)
	return (capacity + sector - 1) / sector * sector
}

// recoveryJournalHeaderFor builds the header identity and geometry for a store
// paired at baseGeneration.
func recoveryJournalHeaderFor(
	storeID, journalID [16]byte,
	pageSize uint32,
	maxKeyBytes, inlineValueBytes int,
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
			sectorSize, maxKeyBytes, inlineValueBytes,
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

// mintRecoveryJournalLocked generates a fresh journal identity and preallocates
// the sibling journal file paired at baseGeneration, keeping it open on the
// collection. It is called during Create before the first root — which records
// JournalID — is published, so a crash after the root is durable always finds
// the journal file present. The caller must hold the writer.
func (c *Collection) mintRecoveryJournalLocked(baseGeneration uint64) error {
	if _, err := rand.Read(c.journalID[:]); err != nil {
		return fmt.Errorf("vibejson: mint recovery journal identity: %w", err)
	}
	path, err := c.journalSiblingPath()
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("vibejson: create recovery journal file: %w", err)
	}
	journal, err := storeio.CreateRecoveryJournal(
		file,
		recoveryJournalHeaderFor(
			c.storeID, c.journalID, uint32(c.options.PageSize),
			c.options.MaxKeyBytes, c.options.InlineValueBytes, baseGeneration,
		),
	)
	if err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return fmt.Errorf("vibejson: create recovery journal: %w", err)
	}
	c.journalPowerSafe = c.options.CheckpointStrength != CheckpointFilesystem
	c.journal = journal
	if recoveryJournalFaultHook != nil {
		recoveryJournalFaultHook(journal)
	}
	return nil
}

// openRecoveryJournalLocked opens and pairs the sibling journal named by a
// recovered root. A referenced-but-absent journal fails closed: the store may
// have acknowledged mutations only the journal records. The caller must hold the
// writer and must not yet have made the collection reachable.
func (c *Collection) openRecoveryJournalLocked(journalID [16]byte) error {
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
	if err := journal.Pair(c.storeID, journalID); err != nil {
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
	err := c.journal.Replay(rootGeneration, func(rec storeio.RecoveryRecord) error {
		key := string(rec.Key)
		switch rec.Kind {
		case storeio.RecoveryRecordKindPut:
			_, putErr := c.Put(key, rec.Value)
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
				storeio.ErrRecoveryJournalRecord, rec.Kind)
		}
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
	// Fold the replayed acknowledgements into a durable root and empty the
	// journal so a second crash before any new write replays nothing.
	c.writer.Lock()
	defer c.writer.Unlock()
	return c.checkpointBufferedLocked()
}

// recycleRecoveryJournalLocked advances the journal head past a checkpointed
// generation inside the checkpoint's root publication. It is a no-op when no
// journal is configured. The caller holds the writer and has already made the
// checkpointed generation durable.
func (c *Collection) recycleRecoveryJournalLocked(baseGeneration uint64) error {
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
		return err
	}
	if err := c.journal.Sync(c.journalPowerSafe); err != nil {
		return err
	}
	c.journalAcks.Add(1)
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
