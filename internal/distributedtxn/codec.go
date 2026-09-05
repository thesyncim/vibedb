// Package distributedtxn owns the compact durable record vocabulary for
// cross-shard transaction coordinators and targets. It deliberately has no
// networking, SQL, or JSON dependency.
package distributedtxn

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"unicode/utf8"
)

const (
	FormatVersion = 1
	// MaxInlineTargets bounds only the legacy single-record VTC1
	// encoding. It is not a distributed transaction target limit;
	// wider transactions use the segmented VTM1 manifest.
	MaxInlineTargets          = 64
	MaxIntentScopes           = 256
	MaxShardIdentityBytes     = 255
	MaxMutationBytes          = 16 << 20
	MaxCoordinatorRecordBytes = 32 << 10
	MaxTargetRecordBytes      = MaxMutationBytes + MaxIntentScopes*8 + 1024
)

var (
	ErrCorrupt       = errors.New("distributed transaction record is corrupt")
	ErrUnsupported   = errors.New("distributed transaction record format is unsupported")
	ErrTooLarge      = errors.New("distributed transaction record exceeds its bound")
	ErrInvalidState  = errors.New("distributed transaction state transition is invalid")
	castagnoli       = crc32.MakeTable(crc32.Castagnoli)
	coordinatorMagic = [4]byte{'V', 'T', 'C', '1'}
	targetMagic      = [4]byte{'V', 'T', 'P', '1'}
)

type ID [16]byte
type Digest [32]byte

// AuthorityWitness is the compact 128-bit prefix of the full canonical route
// authority digest. It provides 128-bit preimage strength, matching VibeDB's
// shard, group, and transaction identifiers, while avoiding a second full
// digest in every wide-manifest target.
// Zero is reserved for the static non-RF3 path, whose complete authority is
// already present in the target record itself.
type AuthorityWitness [16]byte

// IntentScope is one half-open virtual-bucket interval [Start, End). Scopes in
// a target are sorted, disjoint, and coalesced before encoding.
type IntentScope struct {
	Start uint32
	End   uint32
}

// ValidateIntentScope enforces the canonical interval form for one scope.
// It matches ValidateIntentScopes on a single-element slice exactly, without
// allocating the slice.
func ValidateIntentScope(scope IntentScope, bucketBits uint8) bool {
	if 1 > MaxIntentScopes || bucketBits < 8 || bucketBits > 24 {
		return false
	}
	limit := uint32(1) << bucketBits
	return scope.Start < scope.End && scope.End <= limit
}

// ValidateIntentScopes enforces the canonical sorted interval form.
func ValidateIntentScopes(scopes []IntentScope, bucketBits uint8) bool {
	if len(scopes) == 0 {
		return bucketBits == 0
	}
	if len(scopes) > MaxIntentScopes ||
		bucketBits < 8 || bucketBits > 24 {
		return false
	}
	limit := uint32(1) << bucketBits
	for i := range scopes {
		if scopes[i].Start >= scopes[i].End || scopes[i].End > limit ||
			(i != 0 && scopes[i-1].End >= scopes[i].Start) {
			return false
		}
	}
	return true
}

// IntentScopesOverlap reports whether two canonical sorted scope sets touch
// the same bucket. Different bucket widths must be treated as conflicting by
// the caller because their coordinates are not comparable.
func IntentScopesOverlap(a, b []IntentScope) bool {
	for i, j := 0, 0; i < len(a) && j < len(b); {
		if a[i].End <= b[j].Start {
			i++
			continue
		}
		if b[j].End <= a[i].Start {
			j++
			continue
		}
		return true
	}
	return false
}

// TargetDigest binds the exact mutation bytes to their visibility scope.
// A retry cannot retain the same SQL digest while widening or narrowing the
// buckets it blocks.
func TargetDigest(bucketBits uint8, scopes []IntentScope, mutation []byte) Digest {
	hash := sha256.New()
	domain := [5]byte{'V', 'P', 'D', '1', bucketBits}
	_, _ = hash.Write(domain[:])
	var encoded [8]byte
	var count [2]byte
	binary.LittleEndian.PutUint16(count[:], uint16(len(scopes)))
	_, _ = hash.Write(count[:])
	for i := range scopes {
		binary.LittleEndian.PutUint32(encoded[:4], scopes[i].Start)
		binary.LittleEndian.PutUint32(encoded[4:8], scopes[i].End)
		_, _ = hash.Write(encoded[:])
	}
	_, _ = hash.Write(mutation)
	var digest Digest
	_ = hash.Sum(digest[:0])
	return digest
}

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

type TargetState uint8

const (
	TargetInvalid TargetState = iota
	TargetStaged
	TargetPrepared
	TargetApplied
	TargetAborted
	TargetReleased
)

func (s TargetState) valid() bool {
	return s >= TargetStaged && s <= TargetReleased
}

func (s TargetState) CanTransitionTo(next TargetState) bool {
	if s == next {
		return s.valid()
	}
	switch s {
	case TargetStaged:
		return next == TargetPrepared || next == TargetAborted
	case TargetPrepared:
		return next == TargetApplied || next == TargetAborted
	case TargetApplied, TargetAborted:
		return next == TargetReleased
	default:
		return false
	}
}

// TransactionTargetRef is one coordinator-owned target identity. Shard is raw
// UTF-8 bytes so decoding can alias the durable record.
type TransactionTargetRef struct {
	Distribution         []byte
	Shard                []byte
	RoutingVersion       uint64
	AllocationGeneration uint64
	OwnershipEpoch       uint64
	AuthorityWitness     AuthorityWitness
	MutationDigest       Digest
	State                TargetState
}

type CoordinatorRecord struct {
	ID                ID
	State             CoordinatorState
	Revision          uint64
	CatalogGeneration uint64
	// RecoveryDeadline is the legacy field name for the bounded logical pulse
	// limit. It is never a Unix timestamp and is interpreted only as 1..3.
	RecoveryDeadline int64
	Targets          []TransactionTargetRef
}

// Coordinator record layout: 48-byte fixed header, packed target entries,
// and a CRC32C. Each target has a 76-byte fixed identity and digest frame.
const (
	coordinatorHeaderBytes = 48
	coordinatorEntryBytes  = 76
)

func AppendCoordinator(dst []byte, record CoordinatorRecord) ([]byte, error) {
	if err := validateCoordinator(record); err != nil {
		return dst, err
	}
	total := coordinatorHeaderBytes + 4
	for i := range record.Targets {
		total += coordinatorEntryBytes + len(record.Targets[i].Distribution) + len(record.Targets[i].Shard)
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
	binary.LittleEndian.PutUint16(out[6:8], uint16(len(record.Targets)))
	binary.LittleEndian.PutUint64(out[8:16], record.Revision)
	binary.LittleEndian.PutUint64(out[16:24], record.CatalogGeneration)
	binary.LittleEndian.PutUint64(out[24:32], uint64(record.RecoveryDeadline))
	copy(out[32:48], record.ID[:])
	cursor := coordinatorHeaderBytes
	for i := range record.Targets {
		p := &record.Targets[i]
		out[cursor] = byte(len(p.Distribution))
		out[cursor+1] = byte(len(p.Shard))
		out[cursor+2] = byte(p.State)
		binary.LittleEndian.PutUint64(out[cursor+4:cursor+12], p.AllocationGeneration)
		binary.LittleEndian.PutUint64(out[cursor+12:cursor+20], p.OwnershipEpoch)
		binary.LittleEndian.PutUint64(out[cursor+20:cursor+28], p.RoutingVersion)
		copy(out[cursor+28:cursor+60], p.MutationDigest[:])
		copy(out[cursor+60:cursor+76], p.AuthorityWitness[:])
		cursor += coordinatorEntryBytes
		copy(out[cursor:cursor+len(p.Distribution)], p.Distribution)
		cursor += len(p.Distribution)
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
	if count <= 0 || count > MaxInlineTargets {
		return CoordinatorRecord{}, ErrCorrupt
	}
	return OpenCoordinatorInto(src, make([]TransactionTargetRef, count))
}

// OpenCoordinatorInto decodes into caller-owned target storage. Hot
// status/recovery loops keep a [MaxInlineTargets] arena and allocate nothing.
func OpenCoordinatorInto(src []byte, targets []TransactionTargetRef) (CoordinatorRecord, error) {
	if len(src) < coordinatorHeaderBytes+4 || len(src) > MaxCoordinatorRecordBytes ||
		!equal4(src[:4], coordinatorMagic) || !checksumOK(src) {
		return CoordinatorRecord{}, ErrCorrupt
	}
	if src[4] != FormatVersion {
		return CoordinatorRecord{}, ErrUnsupported
	}
	count := int(binary.LittleEndian.Uint16(src[6:8]))
	if count == 0 || count > MaxInlineTargets || cap(targets) < count {
		return CoordinatorRecord{}, ErrCorrupt
	}
	targets = targets[:count]
	clear(targets)
	record := CoordinatorRecord{
		State: CoordinatorState(src[5]), Revision: binary.LittleEndian.Uint64(src[8:16]),
		CatalogGeneration: binary.LittleEndian.Uint64(src[16:24]),
		RecoveryDeadline:  int64(binary.LittleEndian.Uint64(src[24:32])),
		Targets:           targets,
	}
	copy(record.ID[:], src[32:48])
	cursor, end := coordinatorHeaderBytes, len(src)-4
	for i := 0; i < count; i++ {
		if end-cursor < coordinatorEntryBytes {
			return CoordinatorRecord{}, ErrCorrupt
		}
		distributionLength := int(src[cursor])
		shardLength := int(src[cursor+1])
		p := &record.Targets[i]
		p.State = TargetState(src[cursor+2])
		p.AllocationGeneration = binary.LittleEndian.Uint64(src[cursor+4 : cursor+12])
		p.OwnershipEpoch = binary.LittleEndian.Uint64(src[cursor+12 : cursor+20])
		p.RoutingVersion = binary.LittleEndian.Uint64(src[cursor+20 : cursor+28])
		copy(p.MutationDigest[:], src[cursor+28:cursor+60])
		copy(p.AuthorityWitness[:], src[cursor+60:cursor+76])
		cursor += coordinatorEntryBytes
		if distributionLength == 0 || shardLength == 0 ||
			end-cursor < distributionLength+shardLength {
			return CoordinatorRecord{}, ErrCorrupt
		}
		p.Distribution = src[cursor : cursor+distributionLength]
		cursor += distributionLength
		p.Shard = src[cursor : cursor+shardLength]
		cursor += shardLength
	}
	if cursor != end {
		return CoordinatorRecord{}, ErrCorrupt
	}
	if err := validateCoordinator(record); err != nil {
		return CoordinatorRecord{}, ErrCorrupt
	}
	return record, nil
}

type TargetRecord struct {
	ID                        ID
	State                     TargetState
	Revision                  uint64
	RoutingVersion            uint64
	AllocationGeneration      uint64
	OwnershipEpoch            uint64
	CoordinatorDistribution   []byte
	CoordinatorShard          []byte
	CoordinatorAllocation     uint64
	CoordinatorRoutingVersion uint64
	CoordinatorOwnershipEpoch uint64
	BucketBits                uint8
	IntentScopes              []IntentScope
	MutationDigest            Digest
	Mutation                  []byte
}

const targetHeaderBytes = 120

func AppendTarget(dst []byte, record TargetRecord) ([]byte, error) {
	if err := validateTarget(record); err != nil {
		return dst, err
	}
	total := targetHeaderBytes + len(record.CoordinatorDistribution) +
		len(record.CoordinatorShard) + len(record.IntentScopes)*8 + len(record.Mutation) + 4
	if len(record.Mutation) > MaxMutationBytes || total > MaxTargetRecordBytes {
		return dst, ErrTooLarge
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	out := dst[start:]
	copy(out[:4], targetMagic[:])
	out[4] = FormatVersion
	out[5] = byte(record.State)
	out[6] = byte(len(record.CoordinatorDistribution))
	out[7] = byte(len(record.CoordinatorShard))
	binary.LittleEndian.PutUint64(out[8:16], record.Revision)
	binary.LittleEndian.PutUint64(out[16:24], record.RoutingVersion)
	binary.LittleEndian.PutUint64(out[24:32], record.AllocationGeneration)
	binary.LittleEndian.PutUint64(out[32:40], record.OwnershipEpoch)
	binary.LittleEndian.PutUint64(out[40:48], record.CoordinatorAllocation)
	binary.LittleEndian.PutUint32(out[48:52], uint32(len(record.Mutation)))
	copy(out[52:68], record.ID[:])
	copy(out[68:100], record.MutationDigest[:])
	binary.LittleEndian.PutUint64(out[100:108], record.CoordinatorRoutingVersion)
	binary.LittleEndian.PutUint64(out[108:116], record.CoordinatorOwnershipEpoch)
	out[116] = record.BucketBits
	binary.LittleEndian.PutUint16(out[118:120], uint16(len(record.IntentScopes)))
	cursor := targetHeaderBytes
	copy(out[cursor:], record.CoordinatorDistribution)
	cursor += len(record.CoordinatorDistribution)
	copy(out[cursor:], record.CoordinatorShard)
	cursor += len(record.CoordinatorShard)
	for i := range record.IntentScopes {
		binary.LittleEndian.PutUint32(out[cursor:cursor+4], record.IntentScopes[i].Start)
		binary.LittleEndian.PutUint32(out[cursor+4:cursor+8], record.IntentScopes[i].End)
		cursor += 8
	}
	copy(out[cursor:], record.Mutation)
	binary.LittleEndian.PutUint32(out[total-4:], crc32.Checksum(out[:total-4], castagnoli))
	return dst, nil
}

func OpenTarget(src []byte) (TargetRecord, error) {
	if len(src) < targetHeaderBytes+4 {
		return TargetRecord{}, ErrCorrupt
	}
	count := int(binary.LittleEndian.Uint16(src[118:120]))
	if count > MaxIntentScopes {
		return TargetRecord{}, ErrCorrupt
	}
	return OpenTargetInto(src, make([]IntentScope, count))
}

// OpenTargetInto decodes using caller-owned scope storage. Apply,
// admission, and codec validation keep a fixed arena and allocate nothing.
func OpenTargetInto(src []byte, scopes []IntentScope) (TargetRecord, error) {
	if len(src) < targetHeaderBytes+4 || len(src) > MaxTargetRecordBytes ||
		!equal4(src[:4], targetMagic) || !checksumOK(src) {
		return TargetRecord{}, ErrCorrupt
	}
	if src[4] != FormatVersion {
		return TargetRecord{}, ErrUnsupported
	}
	distributionLen := int(src[6])
	shardLen := int(src[7])
	mutationLen := int(binary.LittleEndian.Uint32(src[48:52]))
	scopeCount := int(binary.LittleEndian.Uint16(src[118:120]))
	if distributionLen == 0 || shardLen == 0 || mutationLen > MaxMutationBytes ||
		scopeCount > MaxIntentScopes || cap(scopes) < scopeCount ||
		targetHeaderBytes+distributionLen+shardLen+scopeCount*8+mutationLen+4 != len(src) {
		return TargetRecord{}, ErrCorrupt
	}
	record := TargetRecord{
		State: TargetState(src[5]), Revision: binary.LittleEndian.Uint64(src[8:16]),
		RoutingVersion:            binary.LittleEndian.Uint64(src[16:24]),
		AllocationGeneration:      binary.LittleEndian.Uint64(src[24:32]),
		OwnershipEpoch:            binary.LittleEndian.Uint64(src[32:40]),
		CoordinatorAllocation:     binary.LittleEndian.Uint64(src[40:48]),
		CoordinatorRoutingVersion: binary.LittleEndian.Uint64(src[100:108]),
		CoordinatorOwnershipEpoch: binary.LittleEndian.Uint64(src[108:116]),
		BucketBits:                src[116],
	}
	copy(record.ID[:], src[52:68])
	copy(record.MutationDigest[:], src[68:100])
	cursor := targetHeaderBytes
	record.CoordinatorDistribution = src[cursor : cursor+distributionLen]
	cursor += distributionLen
	record.CoordinatorShard = src[cursor : cursor+shardLen]
	cursor += shardLen
	if scopeCount != 0 {
		record.IntentScopes = scopes[:scopeCount]
		for i := range record.IntentScopes {
			record.IntentScopes[i] = IntentScope{
				Start: binary.LittleEndian.Uint32(src[cursor : cursor+4]),
				End:   binary.LittleEndian.Uint32(src[cursor+4 : cursor+8]),
			}
			cursor += 8
		}
	}
	record.Mutation = src[cursor : cursor+mutationLen]
	if err := validateTarget(record); err != nil {
		return TargetRecord{}, ErrCorrupt
	}
	return record, nil
}

func validateCoordinator(record CoordinatorRecord) error {
	if record.ID.IsZero() || !record.State.valid() || record.Revision == 0 ||
		record.CatalogGeneration == 0 || len(record.Targets) == 0 {
		return ErrCorrupt
	}
	if len(record.Targets) > MaxInlineTargets {
		return ErrTooLarge
	}
	for i := range record.Targets {
		p := &record.Targets[i]
		if len(p.Distribution) == 0 || len(p.Distribution) > MaxShardIdentityBytes ||
			!utf8.Valid(p.Distribution) || len(p.Shard) == 0 ||
			len(p.Shard) > MaxShardIdentityBytes || !utf8.Valid(p.Shard) || p.RoutingVersion == 0 || p.AllocationGeneration == 0 ||
			p.OwnershipEpoch == 0 || !p.State.valid() || p.MutationDigest == (Digest{}) {
			return ErrCorrupt
		}
		if i != 0 {
			prior := &record.Targets[i-1]
			order := compareBytes(prior.Distribution, p.Distribution)
			if order > 0 || (order == 0 && compareBytes(prior.Shard, p.Shard) >= 0) {
				return ErrCorrupt
			}
		}
	}
	return nil
}

func validateTarget(record TargetRecord) error {
	if record.ID.IsZero() || !record.State.valid() || record.Revision == 0 ||
		record.RoutingVersion == 0 || record.AllocationGeneration == 0 ||
		record.OwnershipEpoch == 0 || record.CoordinatorAllocation == 0 ||
		record.CoordinatorRoutingVersion == 0 || record.CoordinatorOwnershipEpoch == 0 ||
		!ValidateIntentScopes(record.IntentScopes, record.BucketBits) ||
		len(record.CoordinatorDistribution) == 0 || len(record.CoordinatorDistribution) > MaxShardIdentityBytes ||
		!utf8.Valid(record.CoordinatorDistribution) || len(record.CoordinatorShard) == 0 ||
		len(record.CoordinatorShard) > MaxShardIdentityBytes || !utf8.Valid(record.CoordinatorShard) ||
		record.MutationDigest == (Digest{}) ||
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
