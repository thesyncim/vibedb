package distributedtxn

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrJournalClosed   = errors.New("distributed transaction journal is closed")
	ErrJournalConflict = errors.New("distributed transaction journal identity conflicts with durable state")
	ErrJournalNotFound = errors.New("distributed transaction journal identity was not found")
	ErrJournalBusy     = errors.New("distributed transaction journal has another active participant")
	ErrOutcomeUnknown  = errors.New("distributed transaction journal durability outcome is unknown")
	journalEntryMagic  = [4]byte{'V', 'T', 'J', '1'}
)

const journalEntryHeaderBytes = 36

type journalEntryKind uint8

const (
	journalCoordinatorStage journalEntryKind = iota + 1
	journalParticipantStage
	journalCoordinatorTransition
	journalParticipantTransition
)

type RecordRole uint8

const (
	RoleInvalid RecordRole = iota
	RoleCoordinator
	RoleParticipant
)

// Status is the allocation-free result of a journal lookup or transition.
// Exactly one typed state is populated according to Role.
type Status struct {
	Role             RecordRole
	ID               ID
	Revision         uint64
	CoordinatorState CoordinatorState
	ParticipantState ParticipantState
	MutationDigest   Digest
}

type journalRecord struct {
	status Status
	stage  []byte
}

// Journal is one shard-local, append-only transaction journal. Stage payloads
// are written once; state changes append fixed-size deltas. The in-memory index
// is published only after fsync succeeds, and a sync failure poisons the handle
// so callers must reopen and resolve the durable outcome from recovery.
type Journal struct {
	mu           sync.RWMutex
	file         *os.File
	coordinators map[ID]*journalRecord
	participants map[ID]*journalRecord
	sticky       error
	closed       bool
	barriers     int
	barrierDone  chan struct{}
}

// OpenJournal opens or creates path, recovers every checksummed entry, and
// removes only a torn final write. Corruption before the tail fails closed.
func OpenJournal(path string) (*Journal, error) {
	if path == "" {
		return nil, errors.New("distributed transaction journal path is empty")
	}
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if created {
		dir, openErr := os.Open(filepath.Dir(path))
		if openErr != nil {
			_ = file.Close()
			return nil, openErr
		}
		err = dir.Sync()
		err = errors.Join(err, dir.Close())
		if err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	barrierDone := make(chan struct{})
	close(barrierDone)
	j := &Journal{
		file: file, coordinators: make(map[ID]*journalRecord),
		participants: make(map[ID]*journalRecord), barrierDone: barrierDone,
	}
	if err := j.recover(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return j, nil
}

func (j *Journal) recover() error {
	info, err := j.file.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	var offset int64
	var header [journalEntryHeaderBytes]byte
	for offset < size {
		remaining := size - offset
		if remaining < journalEntryHeaderBytes {
			return j.truncateTornTail(offset)
		}
		if _, err := j.file.ReadAt(header[:], offset); err != nil {
			return err
		}
		if !equal4(header[:4], journalEntryMagic) {
			return ErrCorrupt
		}
		payloadBytes := int64(binary.LittleEndian.Uint32(header[8:12]))
		entryBytes := int64(journalEntryHeaderBytes) + payloadBytes + 4
		if payloadBytes < 0 || payloadBytes > MaxMutationBytes+512 {
			return ErrTooLarge
		}
		if entryBytes > remaining {
			return j.truncateTornTail(offset)
		}
		entry := make([]byte, entryBytes)
		if _, err := j.file.ReadAt(entry, offset); err != nil {
			return err
		}
		if binary.LittleEndian.Uint32(entry[len(entry)-4:]) !=
			crc32.Checksum(entry[:len(entry)-4], castagnoli) {
			if entryBytes == remaining {
				return j.truncateTornTail(offset)
			}
			return ErrCorrupt
		}
		if err := j.replayEntry(entry); err != nil {
			return err
		}
		offset += entryBytes
	}
	_, err = j.file.Seek(0, io.SeekEnd)
	return err
}

func (j *Journal) truncateTornTail(offset int64) error {
	if err := j.file.Truncate(offset); err != nil {
		return err
	}
	if err := j.file.Sync(); err != nil {
		return err
	}
	_, err := j.file.Seek(0, io.SeekEnd)
	return err
}

func (j *Journal) replayEntry(entry []byte) error {
	kind := journalEntryKind(entry[4])
	state := entry[5]
	revision := binary.LittleEndian.Uint64(entry[12:20])
	var id ID
	copy(id[:], entry[20:36])
	payload := entry[journalEntryHeaderBytes : len(entry)-4]
	switch kind {
	case journalCoordinatorStage:
		record, err := OpenCoordinator(payload)
		if err != nil || record.ID != id || record.Revision != revision || byte(record.State) != state {
			return ErrCorrupt
		}
		return j.replayCoordinatorStage(payload, record)
	case journalParticipantStage:
		record, err := OpenParticipant(payload)
		if err != nil || record.ID != id || record.Revision != revision || byte(record.State) != state {
			return ErrCorrupt
		}
		return j.replayParticipantStage(payload, record)
	case journalCoordinatorTransition:
		if len(payload) != 0 {
			return ErrCorrupt
		}
		return j.replayCoordinatorTransition(id, revision, CoordinatorState(state))
	case journalParticipantTransition:
		if len(payload) != 0 {
			return ErrCorrupt
		}
		return j.replayParticipantTransition(id, revision, ParticipantState(state))
	default:
		return ErrUnsupported
	}
}

func (j *Journal) replayCoordinatorStage(raw []byte, record CoordinatorRecord) error {
	if record.State != CoordinatorStaging {
		return ErrCorrupt
	}
	if prior := j.coordinators[record.ID]; prior != nil {
		if !bytes.Equal(prior.stage, raw) {
			return ErrJournalConflict
		}
		return nil
	}
	j.coordinators[record.ID] = &journalRecord{
		status: Status{Role: RoleCoordinator, ID: record.ID, Revision: record.Revision,
			CoordinatorState: record.State},
		stage: bytes.Clone(raw),
	}
	return nil
}

func (j *Journal) replayParticipantStage(raw []byte, record ParticipantRecord) error {
	if record.State != ParticipantStaged {
		return ErrCorrupt
	}
	if prior := j.participants[record.ID]; prior != nil {
		if !bytes.Equal(prior.stage, raw) {
			return ErrJournalConflict
		}
		return nil
	}
	j.participants[record.ID] = &journalRecord{
		status: Status{Role: RoleParticipant, ID: record.ID, Revision: record.Revision,
			ParticipantState: record.State, MutationDigest: record.MutationDigest},
		stage: bytes.Clone(raw),
	}
	j.addBarrierLocked()
	return nil
}

func (j *Journal) replayCoordinatorTransition(id ID, revision uint64, next CoordinatorState) error {
	record := j.coordinators[id]
	if record == nil || revision != record.status.Revision+1 ||
		!record.status.CoordinatorState.CanTransitionTo(next) {
		return ErrCorrupt
	}
	record.status.Revision = revision
	record.status.CoordinatorState = next
	return nil
}

func (j *Journal) replayParticipantTransition(id ID, revision uint64, next ParticipantState) error {
	record := j.participants[id]
	if record == nil || revision != record.status.Revision+1 ||
		!record.status.ParticipantState.CanTransitionTo(next) {
		return ErrCorrupt
	}
	record.status.Revision = revision
	record.status.ParticipantState = next
	if next == ParticipantAborted || next == ParticipantReleased {
		j.removeBarrierLocked()
	}
	return nil
}

func (j *Journal) addBarrierLocked() {
	if j.barriers == 0 {
		j.barrierDone = make(chan struct{})
	}
	j.barriers++
}

func (j *Journal) removeBarrierLocked() {
	if j.barriers <= 0 {
		return
	}
	j.barriers--
	if j.barriers == 0 {
		close(j.barrierDone)
	}
}

func (j *Journal) writeEntryLocked(kind journalEntryKind, state byte, revision uint64, id ID, payload []byte) error {
	if j.closed {
		return ErrJournalClosed
	}
	if j.sticky != nil {
		return j.sticky
	}
	entry := make([]byte, journalEntryHeaderBytes+len(payload)+4)
	copy(entry[:4], journalEntryMagic[:])
	entry[4], entry[5] = byte(kind), state
	binary.LittleEndian.PutUint32(entry[8:12], uint32(len(payload)))
	binary.LittleEndian.PutUint64(entry[12:20], revision)
	copy(entry[20:36], id[:])
	copy(entry[journalEntryHeaderBytes:], payload)
	binary.LittleEndian.PutUint32(entry[len(entry)-4:], crc32.Checksum(entry[:len(entry)-4], castagnoli))
	n, err := j.file.Write(entry)
	if err == nil && n != len(entry) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = j.file.Sync()
	}
	if err != nil {
		j.sticky = errors.Join(ErrOutcomeUnknown, err)
		return j.sticky
	}
	return nil
}

func (j *Journal) StageCoordinator(raw []byte) (Status, error) {
	var arena [MaxParticipants]ParticipantRef
	record, err := OpenCoordinatorInto(raw, arena[:])
	if err != nil || record.State != CoordinatorStaging {
		return Status{}, errors.Join(ErrCorrupt, err)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if prior := j.coordinators[record.ID]; prior != nil {
		if bytes.Equal(prior.stage, raw) {
			return prior.status, nil
		}
		return Status{}, ErrJournalConflict
	}
	if err := j.writeEntryLocked(journalCoordinatorStage, byte(record.State), record.Revision, record.ID, raw); err != nil {
		return Status{}, err
	}
	owned := bytes.Clone(raw)
	status := Status{Role: RoleCoordinator, ID: record.ID, Revision: record.Revision,
		CoordinatorState: record.State}
	j.coordinators[record.ID] = &journalRecord{status: status, stage: owned}
	return status, nil
}

func (j *Journal) StageParticipant(raw []byte) (Status, error) {
	record, err := OpenParticipant(raw)
	if err != nil || record.State != ParticipantStaged {
		return Status{}, errors.Join(ErrCorrupt, err)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if prior := j.participants[record.ID]; prior != nil {
		if bytes.Equal(prior.stage, raw) {
			return prior.status, nil
		}
		return Status{}, ErrJournalConflict
	}
	// The current visibility barrier is shard-wide, so admitting a second
	// distributed participant would let two dry-run/commit sequences observe the
	// same pre-commit state. Fail fast instead of waiting or deadlocking across
	// shards. Scoped intents replace this conservative exclusion later.
	if j.barriers != 0 {
		return Status{}, ErrJournalBusy
	}
	if err := j.writeEntryLocked(journalParticipantStage, byte(record.State), record.Revision, record.ID, raw); err != nil {
		return Status{}, err
	}
	status := Status{Role: RoleParticipant, ID: record.ID, Revision: record.Revision,
		ParticipantState: record.State, MutationDigest: record.MutationDigest}
	j.participants[record.ID] = &journalRecord{status: status, stage: bytes.Clone(raw)}
	j.addBarrierLocked()
	return status, nil
}

func (j *Journal) Status(id ID) (Status, bool) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	record := j.participants[id]
	if record == nil {
		record = j.coordinators[id]
	}
	if record == nil {
		return Status{}, false
	}
	return record.status, true
}

// CoordinatorStatus returns only the coordinator role for id. A coordinator
// shard may also be a data participant for the same transaction, so protocol
// callers must not use the compatibility Status lookup when the role matters.
func (j *Journal) CoordinatorStatus(id ID) (Status, bool) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	record := j.coordinators[id]
	if record == nil {
		return Status{}, false
	}
	return record.status, true
}

// ParticipantStatus returns only the participant role for id. Coordinator and
// participant records have independent revisions and may coexist on one shard.
func (j *Journal) ParticipantStatus(id ID) (Status, bool) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	record := j.participants[id]
	if record == nil {
		return Status{}, false
	}
	return record.status, true
}

func (j *Journal) TransitionCoordinator(id ID, expected uint64, next CoordinatorState) (Status, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	record := j.coordinators[id]
	if record == nil {
		return Status{}, ErrJournalNotFound
	}
	if record.status.Revision == expected+1 && record.status.CoordinatorState == next {
		return record.status, nil
	}
	if record.status.Revision != expected || !record.status.CoordinatorState.CanTransitionTo(next) {
		return Status{}, ErrJournalConflict
	}
	if err := j.writeEntryLocked(journalCoordinatorTransition, byte(next), expected+1, id, nil); err != nil {
		return Status{}, err
	}
	record.status.Revision++
	record.status.CoordinatorState = next
	return record.status, nil
}

func (j *Journal) TransitionParticipant(id ID, expected uint64, next ParticipantState) (Status, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	record := j.participants[id]
	if record == nil {
		return Status{}, ErrJournalNotFound
	}
	if record.status.Revision == expected+1 && record.status.ParticipantState == next {
		return record.status, nil
	}
	if record.status.Revision != expected || !record.status.ParticipantState.CanTransitionTo(next) {
		return Status{}, ErrJournalConflict
	}
	if err := j.writeEntryLocked(journalParticipantTransition, byte(next), expected+1, id, nil); err != nil {
		return Status{}, err
	}
	record.status.Revision++
	record.status.ParticipantState = next
	if next == ParticipantAborted || next == ParticipantReleased {
		j.removeBarrierLocked()
	}
	return record.status, nil
}

// WaitNoParticipantBarrier waits until every staged participant is aborted or
// released. Transaction commands bypass this gate so recovery can always make
// progress. The channel fast path allocates nothing.
func (j *Journal) WaitNoParticipantBarrier(ctx context.Context) error {
	j.mu.RLock()
	if j.closed {
		j.mu.RUnlock()
		return ErrJournalClosed
	}
	done := j.barrierDone
	j.mu.RUnlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Participant returns a decoded immutable view of the stage payload. Its byte
// slices remain valid until Journal.Close and must not be modified.
func (j *Journal) Participant(id ID) (ParticipantRecord, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	record := j.participants[id]
	if record == nil {
		return ParticipantRecord{}, ErrJournalNotFound
	}
	return OpenParticipant(record.stage)
}

// Coordinator returns a decoded immutable view of the coordinator stage
// payload. Participant slices alias journal-owned storage and remain valid
// until Close.
func (j *Journal) Coordinator(id ID) (CoordinatorRecord, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	record := j.coordinators[id]
	if record == nil {
		return CoordinatorRecord{}, ErrJournalNotFound
	}
	return OpenCoordinator(record.stage)
}

func (j *Journal) Close() error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true
	return j.file.Close()
}
