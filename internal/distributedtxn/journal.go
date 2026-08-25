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
	"slices"
	"sort"
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
	journalParticipantFence
	journalManifestSegment
	journalManifestCoordinatorStage
	journalManifestCoordinatorSeal
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
	status      Status
	stage       []byte
	bucketBits  uint8
	scopes      []IntentScope
	manifest    ManifestDescriptor
	hasManifest bool
}

type journalManifest struct {
	segments            [][]byte
	participantCount    uint64
	encodedBytes        uint64
	chain               Digest
	lastDistribution    [MaxShardIdentityBytes]byte
	lastShard           [MaxShardIdentityBytes]byte
	lastDistributionLen uint8
	lastShardLen        uint8
}

type barrierScope struct {
	start uint32
	end   uint32
	id    ID
}

// Journal is one shard-local, append-only transaction journal. Stage payloads
// are written once; state changes append fixed-size deltas. The in-memory index
// is published only after fsync succeeds, and a sync failure poisons the handle
// so callers must reopen and resolve the durable outcome from recovery.
type Journal struct {
	mu             sync.RWMutex
	file           *os.File
	coordinators   map[ID]*journalRecord
	participants   map[ID]*journalRecord
	manifests      map[ID]*journalManifest
	sticky         error
	closed         bool
	barriers       int
	globalBarriers int
	barrierBits    uint8
	barrierIndex   []barrierScope
	barrierChanged chan struct{}
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
	j := &Journal{
		file: file, coordinators: make(map[ID]*journalRecord),
		participants: make(map[ID]*journalRecord), manifests: make(map[ID]*journalManifest),
		barrierChanged: make(chan struct{}),
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
	var participantScratch []ParticipantRef
	var identityScratch []byte
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
		if payloadBytes < 0 || payloadBytes > MaxParticipantRecordBytes {
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
		if journalEntryKind(entry[4]) == journalManifestSegment && participantScratch == nil {
			participantScratch = make([]ParticipantRef, MaxManifestPageParticipants)
			identityScratch = make([]byte, MaxManifestPageParticipants*MaxShardIdentityBytes*2)
		}
		if err := j.replayEntry(entry, participantScratch, identityScratch); err != nil {
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

func (j *Journal) replayEntry(
	entry []byte,
	participantScratch []ParticipantRef,
	identityScratch []byte,
) error {
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
		var scopes [MaxIntentScopes]IntentScope
		record, err := OpenParticipantInto(payload, scopes[:])
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
	case journalParticipantFence:
		if len(payload) != 0 || ParticipantState(state) != ParticipantAborted || revision != 2 {
			return ErrCorrupt
		}
		return j.replayParticipantFence(id)
	case journalManifestSegment:
		if state != 0 || revision == 0 {
			return ErrCorrupt
		}
		page, err := OpenManifestSegment(payload, participantScratch, identityScratch)
		if err != nil || uint64(page.Segment.Index)+1 != revision {
			return ErrCorrupt
		}
		return j.replayManifestSegment(id, payload, page)
	case journalManifestCoordinatorStage:
		record, err := OpenManifestCoordinator(payload)
		if err != nil || record.ID != id || record.Revision != revision || byte(record.State) != state {
			return ErrCorrupt
		}
		return j.replayManifestCoordinatorStage(payload, record)
	case journalManifestCoordinatorSeal:
		if len(payload) != manifestDescriptorBytes {
			return ErrCorrupt
		}
		descriptor := openManifestDescriptor(payload)
		if !descriptor.valid() {
			return ErrCorrupt
		}
		return j.replayManifestCoordinatorSeal(id, revision, CoordinatorState(state), descriptor)
	default:
		return ErrUnsupported
	}
}

func (j *Journal) replayParticipantFence(id ID) error {
	if id.IsZero() || j.participants[id] != nil {
		return ErrCorrupt
	}
	j.participants[id] = &journalRecord{status: Status{
		Role: RoleParticipant, ID: id, Revision: 2, ParticipantState: ParticipantAborted,
	}}
	return nil
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

func (j *Journal) replayManifestSegment(id ID, raw []byte, page ManifestPage) error {
	if id.IsZero() {
		return ErrCorrupt
	}
	manifest := j.manifests[id]
	if manifest == nil {
		manifest = &journalManifest{}
		j.manifests[id] = manifest
	}
	index := int(page.Segment.Index)
	if index < len(manifest.segments) {
		if !bytes.Equal(manifest.segments[index], raw) {
			return ErrJournalConflict
		}
		return nil
	}
	if index != len(manifest.segments) ||
		page.Segment.FirstParticipant != manifest.participantCount ||
		manifest.encodedBytes+uint64(len(raw)) > MaxManifestBytes {
		return ErrCorrupt
	}
	first := &page.Participants[0]
	if manifest.participantCount != 0 && compareIdentityBytes(
		manifest.lastDistribution[:manifest.lastDistributionLen],
		manifest.lastShard[:manifest.lastShardLen],
		first.Distribution, first.Shard,
	) >= 0 {
		return ErrCorrupt
	}
	last := &page.Participants[len(page.Participants)-1]
	copy(manifest.lastDistribution[:], last.Distribution)
	copy(manifest.lastShard[:], last.Shard)
	manifest.lastDistributionLen = uint8(len(last.Distribution))
	manifest.lastShardLen = uint8(len(last.Shard))
	manifest.chain = appendManifestChain(manifest.chain, page.Segment.Index, page.Segment.Digest)
	manifest.participantCount += uint64(page.Segment.ParticipantCount)
	manifest.encodedBytes += uint64(len(raw))
	manifest.segments = append(manifest.segments, bytes.Clone(raw))
	return nil
}

func (j *Journal) replayManifestCoordinatorStage(
	raw []byte,
	record ManifestCoordinatorRecord,
) error {
	if record.State != CoordinatorStaging || j.manifestDescriptor(record.ID) != record.Manifest {
		return ErrCorrupt
	}
	if prior := j.coordinators[record.ID]; prior != nil {
		if !prior.hasManifest || !bytes.Equal(prior.stage, raw) {
			return ErrJournalConflict
		}
		return nil
	}
	j.coordinators[record.ID] = &journalRecord{
		status: Status{Role: RoleCoordinator, ID: record.ID, Revision: record.Revision,
			CoordinatorState: record.State},
		stage: bytes.Clone(raw), manifest: record.Manifest, hasManifest: true,
	}
	return nil
}

func (j *Journal) replayManifestCoordinatorSeal(
	id ID,
	revision uint64,
	next CoordinatorState,
	descriptor ManifestDescriptor,
) error {
	record := j.coordinators[id]
	if record == nil || !record.hasManifest || record.manifest != descriptor ||
		revision != record.status.Revision+1 ||
		(next != CoordinatorCommitted && next != CoordinatorAborted) ||
		!record.status.CoordinatorState.CanTransitionTo(next) {
		return ErrCorrupt
	}
	record.status.Revision = revision
	record.status.CoordinatorState = next
	return nil
}

func (j *Journal) manifestDescriptor(id ID) ManifestDescriptor {
	manifest := j.manifests[id]
	if manifest == nil || manifest.participantCount == 0 || len(manifest.segments) == 0 {
		return ManifestDescriptor{}
	}
	descriptor := ManifestDescriptor{
		ParticipantCount: manifest.participantCount,
		EncodedBytes:     manifest.encodedBytes,
		SegmentCount:     uint32(len(manifest.segments)),
	}
	descriptor.Root = finishManifestRoot(manifest.chain, descriptor)
	return descriptor
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
		stage: bytes.Clone(raw), bucketBits: record.BucketBits,
		scopes: append([]IntentScope(nil), record.IntentScopes...),
	}
	j.addBarrierLocked(record.ID, record.BucketBits, record.IntentScopes)
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
		j.removeBarrierLocked(id)
	}
	return nil
}

func (j *Journal) addBarrierLocked(id ID, bucketBits uint8, scopes []IntentScope) {
	j.barriers++
	if len(scopes) == 0 {
		j.globalBarriers++
		return
	}
	j.barrierBits = bucketBits
	for i := range scopes {
		position := sort.Search(len(j.barrierIndex), func(index int) bool {
			return j.barrierIndex[index].start >= scopes[i].Start
		})
		j.barrierIndex = append(j.barrierIndex, barrierScope{})
		copy(j.barrierIndex[position+1:], j.barrierIndex[position:])
		j.barrierIndex[position] = barrierScope{
			start: scopes[i].Start, end: scopes[i].End, id: id,
		}
	}
}

func (j *Journal) removeBarrierLocked(id ID) {
	if j.barriers <= 0 {
		return
	}
	record := j.participants[id]
	if record != nil && len(record.scopes) == 0 {
		if j.globalBarriers > 0 {
			j.globalBarriers--
		}
	} else {
		write := 0
		for i := range j.barrierIndex {
			if j.barrierIndex[i].id == id {
				continue
			}
			j.barrierIndex[write] = j.barrierIndex[i]
			write++
		}
		clear(j.barrierIndex[write:])
		j.barrierIndex = j.barrierIndex[:write]
		if len(j.barrierIndex) == 0 {
			j.barrierBits = 0
		}
	}
	j.barriers--
	close(j.barrierChanged)
	j.barrierChanged = make(chan struct{})
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

// StageManifestSegment durably appends one canonical manifest page. Pages must
// arrive in order; retries of the exact page are idempotent. participants and
// identities are caller scratch and may be reused immediately after return.
func (j *Journal) StageManifestSegment(
	id ID,
	raw []byte,
	participants []ParticipantRef,
	identities []byte,
) (ManifestSegment, error) {
	if id.IsZero() {
		return ManifestSegment{}, ErrCorrupt
	}
	page, err := OpenManifestSegment(raw, participants, identities)
	if err != nil {
		return ManifestSegment{}, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	manifest := j.manifests[id]
	if manifest == nil {
		manifest = &journalManifest{}
		j.manifests[id] = manifest
	}
	index := int(page.Segment.Index)
	if index < len(manifest.segments) {
		if bytes.Equal(manifest.segments[index], raw) {
			return page.Segment, nil
		}
		return ManifestSegment{}, ErrJournalConflict
	}
	if index != len(manifest.segments) || page.Segment.FirstParticipant != manifest.participantCount ||
		manifest.encodedBytes+uint64(len(raw)) > MaxManifestBytes || j.coordinators[id] != nil {
		return ManifestSegment{}, ErrJournalConflict
	}
	first := &page.Participants[0]
	if manifest.participantCount != 0 && compareIdentityBytes(
		manifest.lastDistribution[:manifest.lastDistributionLen],
		manifest.lastShard[:manifest.lastShardLen],
		first.Distribution, first.Shard,
	) >= 0 {
		return ManifestSegment{}, ErrCorrupt
	}
	if err := j.writeEntryLocked(
		journalManifestSegment, 0, uint64(page.Segment.Index)+1, id, raw,
	); err != nil {
		return ManifestSegment{}, err
	}
	last := &page.Participants[len(page.Participants)-1]
	copy(manifest.lastDistribution[:], last.Distribution)
	copy(manifest.lastShard[:], last.Shard)
	manifest.lastDistributionLen = uint8(len(last.Distribution))
	manifest.lastShardLen = uint8(len(last.Shard))
	manifest.chain = appendManifestChain(manifest.chain, page.Segment.Index, page.Segment.Digest)
	manifest.participantCount += uint64(page.Segment.ParticipantCount)
	manifest.encodedBytes += uint64(len(raw))
	manifest.segments = append(manifest.segments, bytes.Clone(raw))
	return page.Segment, nil
}

// StageManifestCoordinator seals the complete page set into a fixed-size
// staging record. No additional page can be appended after this point.
func (j *Journal) StageManifestCoordinator(raw []byte) (Status, error) {
	record, err := OpenManifestCoordinator(raw)
	if err != nil || record.State != CoordinatorStaging {
		return Status{}, errors.Join(ErrCorrupt, err)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if prior := j.coordinators[record.ID]; prior != nil {
		if prior.hasManifest && bytes.Equal(prior.stage, raw) {
			return prior.status, nil
		}
		return Status{}, ErrJournalConflict
	}
	if j.manifestDescriptor(record.ID) != record.Manifest {
		return Status{}, ErrJournalConflict
	}
	if err := j.writeEntryLocked(
		journalManifestCoordinatorStage, byte(record.State), record.Revision, record.ID, raw,
	); err != nil {
		return Status{}, err
	}
	status := Status{Role: RoleCoordinator, ID: record.ID, Revision: record.Revision,
		CoordinatorState: record.State}
	j.coordinators[record.ID] = &journalRecord{
		status: status, stage: bytes.Clone(raw), manifest: record.Manifest, hasManifest: true,
	}
	return status, nil
}

// SealManifestCoordinator writes the commit/abort decision together with the
// exact manifest descriptor. A detached or substituted page set therefore
// cannot acquire a valid coordinator decision.
func (j *Journal) SealManifestCoordinator(
	id ID,
	expected uint64,
	next CoordinatorState,
) (Status, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	record := j.coordinators[id]
	if record == nil || !record.hasManifest {
		return Status{}, ErrJournalNotFound
	}
	if record.status.Revision == expected+1 && record.status.CoordinatorState == next {
		return record.status, nil
	}
	if record.status.Revision != expected ||
		(next != CoordinatorCommitted && next != CoordinatorAborted) ||
		!record.status.CoordinatorState.CanTransitionTo(next) {
		return Status{}, ErrJournalConflict
	}
	payload, err := appendManifestDescriptor(nil, record.manifest)
	if err != nil {
		return Status{}, err
	}
	if err := j.writeEntryLocked(
		journalManifestCoordinatorSeal, byte(next), expected+1, id, payload,
	); err != nil {
		return Status{}, err
	}
	record.status.Revision++
	record.status.CoordinatorState = next
	return record.status, nil
}

func (j *Journal) StageParticipant(raw []byte) (Status, error) {
	var scopes [MaxIntentScopes]IntentScope
	record, err := OpenParticipantInto(raw, scopes[:])
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
	// Overlapping participants fail fast instead of waiting and forming a
	// cross-shard deadlock. Disjoint bucket intervals may prepare concurrently.
	if j.scopeConflictLocked(record.BucketBits, record.IntentScopes) {
		return Status{}, ErrJournalBusy
	}
	if err := j.writeEntryLocked(journalParticipantStage, byte(record.State), record.Revision, record.ID, raw); err != nil {
		return Status{}, err
	}
	status := Status{Role: RoleParticipant, ID: record.ID, Revision: record.Revision,
		ParticipantState: record.State, MutationDigest: record.MutationDigest}
	j.participants[record.ID] = &journalRecord{
		status: status, stage: bytes.Clone(raw),
		bucketBits: record.BucketBits, scopes: append([]IntentScope(nil), record.IntentScopes...),
	}
	j.addBarrierLocked(record.ID, record.BucketBits, record.IntentScopes)
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
	if record.hasManifest && record.status.CoordinatorState == CoordinatorStaging &&
		(next == CoordinatorCommitted || next == CoordinatorAborted) {
		return Status{}, ErrJournalConflict
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
		j.removeBarrierLocked(id)
	}
	return record.status, nil
}

// AbortParticipant aborts an existing staged/prepared participant or durably
// installs an ABORTED tombstone for a missing initial revision. The tombstone
// fences a delayed StageParticipant after recovery has chosen abort.
func (j *Journal) AbortParticipant(id ID, expected uint64) (Status, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	record := j.participants[id]
	if record == nil {
		if id.IsZero() || expected != 1 {
			return Status{}, ErrJournalNotFound
		}
		if err := j.writeEntryLocked(
			journalParticipantFence, byte(ParticipantAborted), expected+1, id, nil,
		); err != nil {
			return Status{}, err
		}
		status := Status{
			Role: RoleParticipant, ID: id, Revision: expected + 1,
			ParticipantState: ParticipantAborted,
		}
		j.participants[id] = &journalRecord{status: status}
		return status, nil
	}
	if record.status.Revision == expected+1 && record.status.ParticipantState == ParticipantAborted {
		return record.status, nil
	}
	if record.status.Revision != expected ||
		!record.status.ParticipantState.CanTransitionTo(ParticipantAborted) {
		return Status{}, ErrJournalConflict
	}
	if err := j.writeEntryLocked(
		journalParticipantTransition, byte(ParticipantAborted), expected+1, id, nil,
	); err != nil {
		return Status{}, err
	}
	record.status.Revision++
	record.status.ParticipantState = ParticipantAborted
	j.removeBarrierLocked(id)
	return record.status, nil
}

// WaitNoParticipantBarrier waits only for active participant scopes that
// intersect access. An absent access scope is the fail-safe whole-shard form.
// Transaction commands bypass this gate so recovery can always make progress.
func (j *Journal) WaitNoParticipantBarrier(
	ctx context.Context,
	bucketBits uint8,
	access []IntentScope,
) error {
	for {
		j.mu.RLock()
		if j.closed {
			j.mu.RUnlock()
			return ErrJournalClosed
		}
		if !j.scopeConflictLocked(bucketBits, access) {
			j.mu.RUnlock()
			return nil
		}
		changed := j.barrierChanged
		j.mu.RUnlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ParticipantBarrierConflict is the non-blocking form used while assembling a
// distributed read cut. A caller must first hold its shard-local read gate so a
// participant cannot appear between this check and fence publication.
func (j *Journal) ParticipantBarrierConflict(
	bucketBits uint8,
	access []IntentScope,
) (bool, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	if j.closed {
		return false, ErrJournalClosed
	}
	return j.scopeConflictLocked(bucketBits, access), nil
}

func (j *Journal) scopeConflictLocked(bucketBits uint8, access []IntentScope) bool {
	if j.barriers == 0 {
		return false
	}
	if j.globalBarriers != 0 || len(access) == 0 || bucketBits != j.barrierBits {
		return true
	}
	for i := range access {
		position := sort.Search(len(j.barrierIndex), func(index int) bool {
			return j.barrierIndex[index].end > access[i].Start
		})
		if position < len(j.barrierIndex) && j.barrierIndex[position].start < access[i].End {
			return true
		}
	}
	return false
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

// ParticipantStage returns the immutable checksummed participant stage bytes.
func (j *Journal) ParticipantStage(id ID) ([]byte, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	record := j.participants[id]
	if record == nil {
		return nil, ErrJournalNotFound
	}
	if len(record.stage) == 0 {
		return nil, nil
	}
	return record.stage, nil
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

// ManifestCoordinator returns the fixed-size segmented coordinator stage.
func (j *Journal) ManifestCoordinator(id ID) (ManifestCoordinatorRecord, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	record := j.coordinators[id]
	if record == nil || !record.hasManifest {
		return ManifestCoordinatorRecord{}, ErrJournalNotFound
	}
	return OpenManifestCoordinator(record.stage)
}

// ManifestPage decodes one persisted page into caller-owned scratch. Recovery
// can iterate [0, SegmentCount) without materializing the aggregate manifest.
func (j *Journal) ManifestPage(
	id ID,
	index uint32,
	participants []ParticipantRef,
	identities []byte,
) (ManifestPage, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	record := j.coordinators[id]
	manifest := j.manifests[id]
	if record == nil || !record.hasManifest || manifest == nil || int(index) >= len(manifest.segments) {
		return ManifestPage{}, ErrJournalNotFound
	}
	return OpenManifestSegment(manifest.segments[index], participants, identities)
}

// CoordinatorStage returns the immutable checksummed stage bytes for id. The
// returned slice aliases journal-owned memory and remains valid until Close.
func (j *Journal) CoordinatorStage(id ID) ([]byte, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	record := j.coordinators[id]
	if record == nil {
		return nil, ErrJournalNotFound
	}
	return record.stage, nil
}

// NextCoordinator returns the smallest non-retired coordinator identity after
// the exclusive cursor. It scans the bounded active index without allocating;
// recovery is cold-path work and does not impose a sorted structure on stage
// or status hot paths.
func (j *Journal) NextCoordinator(after ID) (Status, []byte, bool) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	var (
		selected ID
		found    bool
	)
	for id, record := range j.coordinators {
		if record.status.CoordinatorState == CoordinatorRetired ||
			slices.Compare(id[:], after[:]) <= 0 {
			continue
		}
		if !found || slices.Compare(id[:], selected[:]) < 0 {
			selected, found = id, true
		}
	}
	if !found {
		return Status{}, nil, false
	}
	record := j.coordinators[selected]
	return record.status, record.stage, true
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
	close(j.barrierChanged)
	return j.file.Close()
}
