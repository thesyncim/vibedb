package distributedtxn

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"slices"
)

const (
	// MinimumJournalCompactionReclaimBytes prevents tiny terminal transitions
	// from turning the durability path into a rewrite loop.
	MinimumJournalCompactionReclaimBytes = uint64(1 << 20)
	journalCompactionRatio               = uint64(4)
	journalCompactionPressureBytes       = MaxRetainedJournalBytes * 3 / 4
)

// JournalCompactionOpportunity is an exact snapshot of the current generation
// and its canonical compact replacement. Recommended becomes true after at
// least 1 MiB can be reclaimed and either dead bytes occupy at least one
// quarter of the journal or admission is under 75%-of-capacity pressure.
type JournalCompactionOpportunity struct {
	RetainedBytes    uint64
	CompactedBytes   uint64
	ReclaimableBytes uint64
	Recommended      bool
}

// CompactionOpportunity performs no I/O and allocates no payload copies. It is
// intended for a bounded background driver, never the request hot path.
func (j *Journal) CompactionOpportunity() JournalCompactionOpportunity {
	if j == nil {
		return JournalCompactionOpportunity{}
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	if j.closed || j.sticky != nil {
		return JournalCompactionOpportunity{RetainedBytes: j.retainedBytes}
	}
	compacted, ok := j.compactedBytesLocked()
	if !ok || compacted >= j.retainedBytes {
		return JournalCompactionOpportunity{
			RetainedBytes: j.retainedBytes, CompactedBytes: compacted,
		}
	}
	reclaimable := j.retainedBytes - compacted
	recommended := reclaimable >= MinimumJournalCompactionReclaimBytes &&
		(reclaimable >= (j.retainedBytes+journalCompactionRatio-1)/journalCompactionRatio ||
			j.retainedBytes >= journalCompactionPressureBytes)
	return JournalCompactionOpportunity{
		RetainedBytes: j.retainedBytes, CompactedBytes: compacted,
		ReclaimableBytes: reclaimable, Recommended: recommended,
	}
}

func (j *Journal) compactedBytesLocked() (uint64, bool) {
	var total uint64
	add := func(payload int) bool {
		bytes := journalEncodedEntryBytes(payload)
		if total > MaxRetainedJournalBytes || bytes > MaxRetainedJournalBytes-total {
			return false
		}
		total += bytes
		return true
	}
	for _, record := range j.coordinators {
		if record == nil || len(record.stage) == 0 || !add(len(record.stage)) {
			return total, false
		}
		if record.status.CoordinatorState == CoordinatorRetired {
			continue
		}
		if record.hasManifest {
			manifest := j.manifests[record.status.ID]
			if manifest == nil {
				return total, false
			}
			for _, page := range manifest.segments {
				if !add(len(page)) {
					return total, false
				}
			}
		}
		if record.status.CoordinatorState == CoordinatorCommitted ||
			record.status.CoordinatorState == CoordinatorAborted {
			payload := 0
			if record.hasManifest {
				payload = manifestDescriptorBytes
			}
			if !add(payload) {
				return total, false
			}
		}
	}
	for _, record := range j.participants {
		if record == nil || !add(len(record.stage)) {
			return total, false
		}
		if record.status.ParticipantState == ParticipantAborted ||
			record.status.ParticipantState == ParticipantReleased {
			continue
		}
		transitions := int(record.status.Revision) - 1
		for range transitions {
			if !add(0) {
				return total, false
			}
		}
	}
	return total, true
}

// Compact rewrites the journal's current authoritative state into one
// canonical generation and atomically installs it at the original path.
//
// Active coordinator manifests and participant mutation stages remain intact:
// recovery still needs them. Retired coordinator manifests and superseded
// transition entries do not. Terminal records retain their byte-exact stage in
// one compact entry, preserving delayed retry and lookup semantics without an
// unsafe time- or process-local tombstone horizon.
func (j *Journal) Compact() error {
	if j == nil {
		return ErrJournalClosed
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return ErrJournalClosed
	}
	if j.sticky != nil {
		return j.sticky
	}
	if j.path == "" {
		return ErrJournalConflict
	}

	temporary := j.path + ".compact"
	removed := false
	if err := os.Remove(temporary); err == nil {
		removed = true
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if removed {
		if err := syncJournalDirectory(j.path); err != nil {
			return err
		}
	}
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	renamed := false
	defer func() {
		if !renamed {
			_ = file.Close()
			_ = os.Remove(temporary)
		}
	}()

	retained, err := j.writeCompactedLocked(file)
	if err == nil {
		err = file.Sync()
	}
	if err != nil {
		return err
	}
	candidateClosed, currentClosed, installErr := installJournalCompaction(
		temporary, j.path, file, j.file,
	)
	if installErr != nil {
		if currentClosed {
			reopened, reopenErr := os.OpenFile(j.path, os.O_RDWR, 0o600)
			if reopenErr != nil {
				j.sticky = errors.Join(ErrOutcomeUnknown, installErr, reopenErr)
				return j.sticky
			}
			j.file = reopened
		}
		return installErr
	}
	if candidateClosed {
		file, err = os.OpenFile(j.path, os.O_RDWR, 0o600)
		if err != nil {
			j.sticky = errors.Join(ErrOutcomeUnknown, err)
			return j.sticky
		}
	}
	renamed = true
	if err = syncJournalDirectory(j.path); err != nil {
		j.sticky = errors.Join(ErrOutcomeUnknown, err, file.Close())
		return j.sticky
	}

	old := j.file
	j.file = file
	j.retainedBytes = retained
	if currentClosed {
		return nil
	}
	return old.Close()
}

func (j *Journal) writeCompactedLocked(file *os.File) (uint64, error) {
	coordinatorIDs := make([]ID, 0, len(j.coordinators))
	for id := range j.coordinators {
		coordinatorIDs = append(coordinatorIDs, id)
	}
	slices.SortFunc(coordinatorIDs, compareJournalID)
	participantIDs := make([]ID, 0, len(j.participants))
	for id := range j.participants {
		participantIDs = append(participantIDs, id)
	}
	slices.SortFunc(participantIDs, compareJournalID)

	var retained uint64
	write := func(kind journalEntryKind, state byte, revision uint64, id ID, payload []byte) error {
		entryBytes := journalEncodedEntryBytes(len(payload))
		if retained > MaxRetainedJournalBytes || entryBytes > MaxRetainedJournalBytes-retained ||
			uint64(len(payload)) > MaxParticipantRecordBytes {
			return ErrTooLarge
		}
		entry := make([]byte, int(entryBytes))
		copy(entry[:4], journalEntryMagic[:])
		entry[4], entry[5] = byte(kind), state
		binary.LittleEndian.PutUint32(entry[8:12], uint32(len(payload)))
		binary.LittleEndian.PutUint64(entry[12:20], revision)
		copy(entry[20:36], id[:])
		copy(entry[journalEntryHeaderBytes:], payload)
		binary.LittleEndian.PutUint32(
			entry[len(entry)-4:], crc32.Checksum(entry[:len(entry)-4], castagnoli),
		)
		n, err := file.Write(entry)
		if err != nil {
			return err
		}
		if n != len(entry) {
			return io.ErrShortWrite
		}
		retained += entryBytes
		return nil
	}

	for _, id := range coordinatorIDs {
		record := j.coordinators[id]
		if record == nil || len(record.stage) == 0 {
			return 0, ErrCorrupt
		}
		state := record.status.CoordinatorState
		if state == CoordinatorRetired {
			if err := write(journalCompactedCoordinator, byte(state), record.status.Revision, id, record.stage); err != nil {
				return 0, err
			}
			continue
		}
		stageKind := journalCoordinatorStage
		if record.hasManifest {
			stageKind = journalManifestCoordinatorStage
		}
		if err := write(stageKind, byte(CoordinatorStaging), 1, id, record.stage); err != nil {
			return 0, err
		}
		if record.hasManifest {
			manifest := j.manifests[id]
			if manifest == nil {
				return 0, ErrCorrupt
			}
			for index, page := range manifest.segments {
				if err := write(journalManifestSegment, 0, uint64(index)+1, id, page); err != nil {
					return 0, err
				}
			}
		}
		switch state {
		case CoordinatorStaging:
		case CoordinatorCommitted, CoordinatorAborted:
			if record.status.Revision != 2 {
				return 0, ErrCorrupt
			}
			if record.hasManifest {
				payload, appendErr := appendManifestDescriptor(nil, record.manifest)
				if appendErr != nil {
					return 0, appendErr
				}
				if err := write(journalManifestCoordinatorSeal, byte(state), 2, id, payload); err != nil {
					return 0, err
				}
			} else if err := write(journalCoordinatorTransition, byte(state), 2, id, nil); err != nil {
				return 0, err
			}
		default:
			return 0, ErrCorrupt
		}
	}

	for _, id := range participantIDs {
		record := j.participants[id]
		if record == nil {
			return 0, ErrCorrupt
		}
		state := record.status.ParticipantState
		if state == ParticipantAborted || state == ParticipantReleased {
			if err := write(journalCompactedParticipant, byte(state), record.status.Revision, id, record.stage); err != nil {
				return 0, err
			}
			continue
		}
		if len(record.stage) == 0 || record.status.Revision != uint64(state) {
			return 0, ErrCorrupt
		}
		if err := write(journalParticipantStage, byte(ParticipantStaged), 1, id, record.stage); err != nil {
			return 0, err
		}
		if state >= ParticipantPrepared {
			if err := write(journalParticipantTransition, byte(ParticipantPrepared), 2, id, nil); err != nil {
				return 0, err
			}
		}
		if state == ParticipantApplied {
			if err := write(journalParticipantTransition, byte(ParticipantApplied), 3, id, nil); err != nil {
				return 0, err
			}
		}
	}
	return retained, nil
}

func compareJournalID(left, right ID) int {
	return slices.Compare(left[:], right[:])
}

func removeStaleJournalCompaction(path string) error {
	temporary := path + ".compact"
	if err := os.Remove(temporary); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	return syncJournalDirectory(path)
}
