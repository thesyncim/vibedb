// Package distributedtxn owns the compact durable record vocabulary for
// cross-shard transaction coordinators and participants. It deliberately has no
// networking, SQL, or JSON dependency.
package distributedtxn

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"unicode/utf8"
)

const (
	FormatVersion             = 1
	MaxParticipants           = 64
	MaxShardIdentityBytes     = 255
	MaxMutationBytes          = 16 << 20
	MaxCoordinatorRecordBytes = 32 << 10
)

var (
	ErrCorrupt       = errors.New("distributed transaction record is corrupt")
	ErrUnsupported   = errors.New("distributed transaction record format is unsupported")
	ErrTooLarge      = errors.New("distributed transaction record exceeds its bound")
	ErrInvalidState  = errors.New("distributed transaction state transition is invalid")
	castagnoli       = crc32.MakeTable(crc32.Castagnoli)
	coordinatorMagic = [4]byte{'V', 'T', 'C', '1'}
	participantMagic = [4]byte{'V', 'T', 'P', '1'}
)

type ID [16]byte
type Digest [32]byte

func (id ID) IsZero() bool { return id == ID{} }

type CoordinatorState uint8

const (
	CoordinatorInvalid CoordinatorState = iota
	CoordinatorStaging
	CoordinatorCommitted
	CoordinatorAborted
	CoordinatorRetired
)

func (s CoordinatorState) valid() bool {
	return s >= CoordinatorStaging && s <= CoordinatorRetired
}

// CanTransitionTo implements the monotone coordinator state machine. Repeating
// a state is an idempotent retry.
func (s CoordinatorState) CanTransitionTo(next CoordinatorState) bool {
	if s == next {
		return s.valid()
	}
	switch s {
	case CoordinatorStaging:
		return next == CoordinatorCommitted || next == CoordinatorAborted
	case CoordinatorAborted, CoordinatorCommitted:
		return next == CoordinatorRetired
	default:
		return false
	}
}

type ParticipantState uint8

const (
	ParticipantInvalid ParticipantState = iota
	ParticipantStaged
	ParticipantApplied
	ParticipantAborted
	ParticipantReleased
)

func (s ParticipantState) valid() bool {
	return s >= ParticipantStaged && s <= ParticipantReleased
}

func (s ParticipantState) CanTransitionTo(next ParticipantState) bool {
	if s == next {
		return s.valid()
	}
	switch s {
	case ParticipantStaged:
		return next == ParticipantApplied || next == ParticipantAborted
	case ParticipantApplied, ParticipantAborted:
		return next == ParticipantReleased
	default:
		return false
	}
}

// ParticipantRef is one coordinator-owned participant identity. Shard is raw
// UTF-8 bytes so decoding can alias the durable record.
type ParticipantRef struct {
	Shard                []byte
	AllocationGeneration uint64
	OwnershipEpoch       uint64
	MutationDigest       Digest
	State                ParticipantState
}

type CoordinatorRecord struct {
	ID               ID
	State            CoordinatorState
	Revision         uint64
	RoutingVersion   uint64
	RecoveryDeadline int64
	Participants     []ParticipantRef
}

// Coordinator record layout: 48-byte fixed header, packed participant entries,
// and a CRC32C. Each entry costs 50 bytes plus the shard identity.
const coordinatorHeaderBytes = 48

func AppendCoordinator(dst []byte, record CoordinatorRecord) ([]byte, error) {
	if err := validateCoordinator(record); err != nil {
		return dst, err
	}
	total := coordinatorHeaderBytes + 4
	for i := range record.Participants {
		total += 50 + len(record.Participants[i].Shard)
	}
	if total > MaxCoordinatorRecordBytes {
		return dst, ErrTooLarge
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	out := dst[start:]
	copy(out[:4], coordinatorMagic[:])
	out[4] = FormatVersion
	out[5] = byte(record.State)
	binary.LittleEndian.PutUint16(out[6:8], uint16(len(record.Participants)))
	binary.LittleEndian.PutUint64(out[8:16], record.Revision)
	binary.LittleEndian.PutUint64(out[16:24], record.RoutingVersion)
	binary.LittleEndian.PutUint64(out[24:32], uint64(record.RecoveryDeadline))
	copy(out[32:48], record.ID[:])
	cursor := coordinatorHeaderBytes
	for i := range record.Participants {
		p := &record.Participants[i]
		out[cursor] = byte(len(p.Shard))
		out[cursor+1] = byte(p.State)
		binary.LittleEndian.PutUint64(out[cursor+2:cursor+10], p.AllocationGeneration)
		binary.LittleEndian.PutUint64(out[cursor+10:cursor+18], p.OwnershipEpoch)
		copy(out[cursor+18:cursor+50], p.MutationDigest[:])
		cursor += 50
		copy(out[cursor:cursor+len(p.Shard)], p.Shard)
		cursor += len(p.Shard)
	}
	binary.LittleEndian.PutUint32(out[total-4:], crc32.Checksum(out[:total-4], castagnoli))
	return dst, nil
}

func OpenCoordinator(src []byte) (CoordinatorRecord, error) {
	if len(src) < coordinatorHeaderBytes+4 {
		return CoordinatorRecord{}, ErrCorrupt
	}
	count := int(binary.LittleEndian.Uint16(src[6:8]))
	if count <= 0 || count > MaxParticipants {
		return CoordinatorRecord{}, ErrCorrupt
	}
	return OpenCoordinatorInto(src, make([]ParticipantRef, count))
}

// OpenCoordinatorInto decodes into caller-owned participant storage. Hot
// status/recovery loops keep a [MaxParticipants] arena and allocate nothing.
func OpenCoordinatorInto(src []byte, participants []ParticipantRef) (CoordinatorRecord, error) {
	if len(src) < coordinatorHeaderBytes+4 || len(src) > MaxCoordinatorRecordBytes ||
		!equal4(src[:4], coordinatorMagic) || !checksumOK(src) {
		return CoordinatorRecord{}, ErrCorrupt
	}
	if src[4] != FormatVersion {
		return CoordinatorRecord{}, ErrUnsupported
	}
	count := int(binary.LittleEndian.Uint16(src[6:8]))
	if count == 0 || count > MaxParticipants || cap(participants) < count {
		return CoordinatorRecord{}, ErrCorrupt
	}
	participants = participants[:count]
	clear(participants)
	record := CoordinatorRecord{
		State: CoordinatorState(src[5]), Revision: binary.LittleEndian.Uint64(src[8:16]),
		RoutingVersion:   binary.LittleEndian.Uint64(src[16:24]),
		RecoveryDeadline: int64(binary.LittleEndian.Uint64(src[24:32])),
		Participants:     participants,
	}
	copy(record.ID[:], src[32:48])
	cursor, end := coordinatorHeaderBytes, len(src)-4
	for i := 0; i < count; i++ {
		if end-cursor < 50 {
			return CoordinatorRecord{}, ErrCorrupt
		}
		length := int(src[cursor])
		p := &record.Participants[i]
		p.State = ParticipantState(src[cursor+1])
		p.AllocationGeneration = binary.LittleEndian.Uint64(src[cursor+2 : cursor+10])
		p.OwnershipEpoch = binary.LittleEndian.Uint64(src[cursor+10 : cursor+18])
		copy(p.MutationDigest[:], src[cursor+18:cursor+50])
		cursor += 50
		if length == 0 || end-cursor < length {
			return CoordinatorRecord{}, ErrCorrupt
		}
		p.Shard = src[cursor : cursor+length]
		cursor += length
	}
	if cursor != end {
		return CoordinatorRecord{}, ErrCorrupt
	}
	if err := validateCoordinator(record); err != nil {
		return CoordinatorRecord{}, ErrCorrupt
	}
	return record, nil
}

type ParticipantRecord struct {
	ID                    ID
	State                 ParticipantState
	Revision              uint64
	RoutingVersion        uint64
	AllocationGeneration  uint64
	OwnershipEpoch        uint64
	CoordinatorShard      []byte
	CoordinatorAllocation uint64
	MutationDigest        Digest
	Mutation              []byte
}

const participantHeaderBytes = 100

func AppendParticipant(dst []byte, record ParticipantRecord) ([]byte, error) {
	if err := validateParticipant(record); err != nil {
		return dst, err
	}
	total := participantHeaderBytes + len(record.CoordinatorShard) + len(record.Mutation) + 4
	if len(record.Mutation) > MaxMutationBytes || total > MaxMutationBytes+512 {
		return dst, ErrTooLarge
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	out := dst[start:]
	copy(out[:4], participantMagic[:])
	out[4] = FormatVersion
	out[5] = byte(record.State)
	out[6] = byte(len(record.CoordinatorShard))
	binary.LittleEndian.PutUint64(out[8:16], record.Revision)
	binary.LittleEndian.PutUint64(out[16:24], record.RoutingVersion)
	binary.LittleEndian.PutUint64(out[24:32], record.AllocationGeneration)
	binary.LittleEndian.PutUint64(out[32:40], record.OwnershipEpoch)
	binary.LittleEndian.PutUint64(out[40:48], record.CoordinatorAllocation)
	binary.LittleEndian.PutUint32(out[48:52], uint32(len(record.Mutation)))
	copy(out[52:68], record.ID[:])
	copy(out[68:100], record.MutationDigest[:])
	cursor := participantHeaderBytes
	copy(out[cursor:], record.CoordinatorShard)
	cursor += len(record.CoordinatorShard)
	copy(out[cursor:], record.Mutation)
	binary.LittleEndian.PutUint32(out[total-4:], crc32.Checksum(out[:total-4], castagnoli))
	return dst, nil
}

func OpenParticipant(src []byte) (ParticipantRecord, error) {
	if len(src) < participantHeaderBytes+4 || len(src) > MaxMutationBytes+512 ||
		!equal4(src[:4], participantMagic) || !checksumOK(src) {
		return ParticipantRecord{}, ErrCorrupt
	}
	if src[4] != FormatVersion {
		return ParticipantRecord{}, ErrUnsupported
	}
	shardLen := int(src[6])
	mutationLen := int(binary.LittleEndian.Uint32(src[48:52]))
	if shardLen == 0 || mutationLen > MaxMutationBytes ||
		participantHeaderBytes+shardLen+mutationLen+4 != len(src) {
		return ParticipantRecord{}, ErrCorrupt
	}
	record := ParticipantRecord{
		State: ParticipantState(src[5]), Revision: binary.LittleEndian.Uint64(src[8:16]),
		RoutingVersion:        binary.LittleEndian.Uint64(src[16:24]),
		AllocationGeneration:  binary.LittleEndian.Uint64(src[24:32]),
		OwnershipEpoch:        binary.LittleEndian.Uint64(src[32:40]),
		CoordinatorAllocation: binary.LittleEndian.Uint64(src[40:48]),
	}
	copy(record.ID[:], src[52:68])
	copy(record.MutationDigest[:], src[68:100])
	cursor := participantHeaderBytes
	record.CoordinatorShard = src[cursor : cursor+shardLen]
	cursor += shardLen
	record.Mutation = src[cursor : cursor+mutationLen]
	if err := validateParticipant(record); err != nil {
		return ParticipantRecord{}, ErrCorrupt
	}
	return record, nil
}

func validateCoordinator(record CoordinatorRecord) error {
	if record.ID.IsZero() || !record.State.valid() || record.Revision == 0 ||
		record.RoutingVersion == 0 || len(record.Participants) == 0 ||
		len(record.Participants) > MaxParticipants {
		return ErrCorrupt
	}
	for i := range record.Participants {
		p := &record.Participants[i]
		if len(p.Shard) == 0 || len(p.Shard) > MaxShardIdentityBytes ||
			!utf8.Valid(p.Shard) || p.AllocationGeneration == 0 ||
			p.OwnershipEpoch == 0 || !p.State.valid() || p.MutationDigest == (Digest{}) {
			return ErrCorrupt
		}
		if i != 0 && compareBytes(record.Participants[i-1].Shard, p.Shard) >= 0 {
			return ErrCorrupt
		}
	}
	return nil
}

func validateParticipant(record ParticipantRecord) error {
	if record.ID.IsZero() || !record.State.valid() || record.Revision == 0 ||
		record.RoutingVersion == 0 || record.AllocationGeneration == 0 ||
		record.OwnershipEpoch == 0 || record.CoordinatorAllocation == 0 ||
		len(record.CoordinatorShard) == 0 || len(record.CoordinatorShard) > MaxShardIdentityBytes ||
		!utf8.Valid(record.CoordinatorShard) || record.MutationDigest == (Digest{}) ||
		len(record.Mutation) == 0 || len(record.Mutation) > MaxMutationBytes {
		return ErrCorrupt
	}
	return nil
}

func checksumOK(src []byte) bool {
	return len(src) >= 4 && binary.LittleEndian.Uint32(src[len(src)-4:]) ==
		crc32.Checksum(src[:len(src)-4], castagnoli)
}

func equal4(value []byte, magic [4]byte) bool {
	return len(value) >= 4 && value[0] == magic[0] && value[1] == magic[1] &&
		value[2] == magic[2] && value[3] == magic[3]
}

func compareBytes(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
