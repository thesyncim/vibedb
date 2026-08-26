package distributedtxn

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
)

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
	if err = os.Rename(temporary, j.path); err != nil {
		return err
	}
	renamed = true
	if err = syncJournalDirectory(j.path); err != nil {
		j.sticky = errors.Join(ErrOutcomeUnknown, err, file.Close())
		return j.sticky
	}

	old := j.file
	j.file = file
	j.retainedBytes = retained
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

func syncJournalDirectory(path string) error {
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
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
