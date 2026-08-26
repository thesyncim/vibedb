package distributedtxn

import (
	"encoding/binary"
	"hash/crc32"
	"unicode/utf8"
)

const (
	replicatedCommandHeaderBytes   = 128
	replicatedCommandChecksumBytes = 4

	// MaxReplicatedCommandBytes is an encoded-byte admission bound. Relation
	// mutation bytes are carried once by the outer replication command and are
	// deliberately not part of this transaction control body.
	MaxReplicatedCommandBytes = replicatedCommandHeaderBytes +
		MaxIntentScopes*8 + ManifestSegmentBytes + replicatedCommandChecksumBytes
)

var replicatedCommandMagic = [4]byte{'V', 'T', 'R', 'C'}

// ReplicatedRole identifies the transaction state machine that owns a command.
type ReplicatedRole uint8

const (
	ReplicatedRoleInvalid ReplicatedRole = iota
	ReplicatedRoleCoordinator
	ReplicatedRoleParticipant
)

// ReplicatedOperation is one deterministic transaction state transition.
type ReplicatedOperation uint8

const (
	ReplicatedOperationInvalid ReplicatedOperation = iota
	ReplicatedStageCoordinator
	ReplicatedStageManifestCoordinator
	ReplicatedStageManifestSegment
	ReplicatedCommitCoordinator
	ReplicatedAbortCoordinator
	ReplicatedRetireCoordinator
	ReplicatedStageParticipant
	ReplicatedPrepareParticipant
	ReplicatedApplyParticipant
	ReplicatedAbortParticipant
	ReplicatedReleaseParticipant
)

// ReplicatedPayloadKind binds the control body to one existing canonical
// durable record grammar. ParticipantStage is the compact exception: native
// relation mutations remain in the outer replication command.
type ReplicatedPayloadKind uint8

const (
	ReplicatedPayloadNone ReplicatedPayloadKind = iota
	ReplicatedPayloadCoordinator
	ReplicatedPayloadManifestCoordinator
	ReplicatedPayloadManifestSegment
	ReplicatedPayloadParticipantStage
)

// ParticipantStage is the compact durable intent metadata paired with native
// relation batches in the outer replication command. MutationDigest binds
// those exact canonical relation bytes; the outer decoder recomputes it.
type ParticipantStage struct {
	CoordinatorGroup            ID
	CoordinatorShardIncarnation ID
	CoordinatorAllocation       uint64
	BucketBits                  uint8
	IntentScopes                []IntentScope
	MutationDigest              Digest
}

// ReplicatedCommand is the construction form of one self-delimiting
// transaction control body. Payload contains VTC1, VTCM, or VTM1 bytes and is
// empty for participant staging and transitions.
type ReplicatedCommand struct {
	Role             ReplicatedRole
	Operation        ReplicatedOperation
	ID               ID
	ExpectedRevision uint64
	PayloadKind      ReplicatedPayloadKind
	Payload          []byte
	Participant      ParticipantStage
}

// ReplicatedCommandView is checksum- and semantics-validated. Payload aliases
// the input; participant scopes occupy caller scratch in Open...Into.
type ReplicatedCommandView struct {
	ReplicatedCommand
	raw []byte
}

// Bytes returns the exact complete transaction control body. It never includes
// the outer replication command's native relation batches.
func (v ReplicatedCommandView) Bytes() []byte { return v.raw[:len(v.raw):len(v.raw)] }

// Command returns a construction form. Payload remains a borrowed,
// capacity-clamped alias; participant scopes use the decoder scratch.
func (v ReplicatedCommandView) Command() ReplicatedCommand { return v.ReplicatedCommand }

func AppendReplicatedCommand(dst []byte, command ReplicatedCommand) ([]byte, error) {
	if err := validateReplicatedCommand(command); err != nil {
		return dst, err
	}
	total := replicatedCommandHeaderBytes + len(command.Participant.IntentScopes)*8 +
		len(command.Payload) + replicatedCommandChecksumBytes
	if total > MaxReplicatedCommandBytes {
		return dst, ErrTooLarge
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	out := dst[start:]
	copy(out[:4], replicatedCommandMagic[:])
	out[4] = FormatVersion
	out[5] = byte(command.Role)
	out[6] = byte(command.Operation)
	out[7] = byte(command.PayloadKind)
	binary.LittleEndian.PutUint16(out[8:10], replicatedCommandHeaderBytes)
	binary.LittleEndian.PutUint32(out[12:16], uint32(total))
	binary.LittleEndian.PutUint32(out[16:20], uint32(len(command.Payload)))
	binary.LittleEndian.PutUint64(out[24:32], command.ExpectedRevision)
	copy(out[32:48], command.ID[:])
	copy(out[48:64], command.Participant.CoordinatorGroup[:])
	copy(out[64:80], command.Participant.CoordinatorShardIncarnation[:])
	binary.LittleEndian.PutUint64(out[80:88], command.Participant.CoordinatorAllocation)
	copy(out[88:120], command.Participant.MutationDigest[:])
	out[120] = command.Participant.BucketBits
	binary.LittleEndian.PutUint16(out[122:124], uint16(len(command.Participant.IntentScopes)))
	cursor := replicatedCommandHeaderBytes
	for i := range command.Participant.IntentScopes {
		binary.LittleEndian.PutUint32(out[cursor:cursor+4], command.Participant.IntentScopes[i].Start)
		binary.LittleEndian.PutUint32(out[cursor+4:cursor+8], command.Participant.IntentScopes[i].End)
		cursor += 8
	}
	copy(out[cursor:], command.Payload)
	binary.LittleEndian.PutUint32(out[total-4:], crc32.Checksum(out[:total-4], castagnoli))
	return dst, nil
}

// OpenReplicatedCommand returns a borrowed command view. It allocates only the
// compact participant scope slice, and only for participant staging.
func OpenReplicatedCommand(src []byte) (ReplicatedCommandView, error) {
	if len(src) < replicatedCommandHeaderBytes+replicatedCommandChecksumBytes {
		return ReplicatedCommandView{}, ErrCorrupt
	}
	count := int(binary.LittleEndian.Uint16(src[122:124]))
	if count > MaxIntentScopes {
		return ReplicatedCommandView{}, ErrCorrupt
	}
	return OpenReplicatedCommandInto(src, make([]IntentScope, count))
}

// OpenReplicatedCommandInto decodes with caller-owned scope storage. A
// sufficiently sized slice makes participant-stage decode allocation-free.
func OpenReplicatedCommandInto(src []byte, scopes []IntentScope) (ReplicatedCommandView, error) {
	if len(src) < replicatedCommandHeaderBytes+replicatedCommandChecksumBytes ||
		len(src) > MaxReplicatedCommandBytes || !equal4(src[:4], replicatedCommandMagic) ||
		!checksumOK(src) {
		return ReplicatedCommandView{}, ErrCorrupt
	}
	if src[4] != FormatVersion {
		return ReplicatedCommandView{}, ErrUnsupported
	}
	if binary.LittleEndian.Uint16(src[8:10]) != replicatedCommandHeaderBytes ||
		src[10] != 0 || src[11] != 0 || binary.LittleEndian.Uint32(src[12:16]) != uint32(len(src)) ||
		binary.LittleEndian.Uint32(src[20:24]) != 0 || src[121] != 0 ||
		binary.LittleEndian.Uint32(src[124:128]) != 0 {
		return ReplicatedCommandView{}, ErrCorrupt
	}
	payloadLength := uint64(binary.LittleEndian.Uint32(src[16:20]))
	scopeCount := uint64(binary.LittleEndian.Uint16(src[122:124]))
	if scopeCount > MaxIntentScopes || scopeCount > uint64(cap(scopes)) ||
		uint64(replicatedCommandHeaderBytes+replicatedCommandChecksumBytes)+scopeCount*8+payloadLength != uint64(len(src)) {
		return ReplicatedCommandView{}, ErrCorrupt
	}
	view := ReplicatedCommandView{raw: src[:len(src):len(src)]}
	view.Role = ReplicatedRole(src[5])
	view.Operation = ReplicatedOperation(src[6])
	view.PayloadKind = ReplicatedPayloadKind(src[7])
	view.ExpectedRevision = binary.LittleEndian.Uint64(src[24:32])
	copy(view.ID[:], src[32:48])
	copy(view.Participant.CoordinatorGroup[:], src[48:64])
	copy(view.Participant.CoordinatorShardIncarnation[:], src[64:80])
	view.Participant.CoordinatorAllocation = binary.LittleEndian.Uint64(src[80:88])
	copy(view.Participant.MutationDigest[:], src[88:120])
	view.Participant.BucketBits = src[120]
	cursor := replicatedCommandHeaderBytes
	if scopeCount != 0 {
		view.Participant.IntentScopes = scopes[:scopeCount]
		for i := range view.Participant.IntentScopes {
			view.Participant.IntentScopes[i] = IntentScope{
				Start: binary.LittleEndian.Uint32(src[cursor : cursor+4]),
				End:   binary.LittleEndian.Uint32(src[cursor+4 : cursor+8]),
			}
			cursor += 8
		}
	}
	payloadEnd := cursor + int(payloadLength)
	view.Payload = src[cursor:payloadEnd:payloadEnd]
	if err := validateReplicatedCommand(view.ReplicatedCommand); err != nil {
		if err == ErrTooLarge {
			return ReplicatedCommandView{}, err
		}
		return ReplicatedCommandView{}, ErrCorrupt
	}
	return view, nil
}

func validateReplicatedCommand(command ReplicatedCommand) error {
	if command.ID.IsZero() {
		return ErrCorrupt
	}
	wantRole, wantPayload, creation, ok := replicatedOperationShape(command.Operation)
	if !ok || command.Role != wantRole || command.PayloadKind != wantPayload ||
		(creation && command.ExpectedRevision != 0) || (!creation && command.ExpectedRevision == 0) {
		return ErrCorrupt
	}
	if wantPayload != ReplicatedPayloadParticipantStage && !participantStageZero(command.Participant) {
		return ErrCorrupt
	}
	switch wantPayload {
	case ReplicatedPayloadNone:
		if len(command.Payload) != 0 {
			return ErrCorrupt
		}
	case ReplicatedPayloadCoordinator:
		if len(command.Payload) > MaxCoordinatorRecordBytes {
			return ErrTooLarge
		}
		var participantScratch [MaxInlineParticipants]ParticipantRef
		record, err := OpenCoordinatorInto(command.Payload, participantScratch[:])
		if err != nil || !canonicalCoordinatorBytes(command.Payload) || record.ID != command.ID ||
			record.State != CoordinatorStaging || record.Revision != 1 {
			return ErrCorrupt
		}
	case ReplicatedPayloadManifestCoordinator:
		record, err := OpenManifestCoordinator(command.Payload)
		if err != nil || record.ID != command.ID || record.State != CoordinatorStaging || record.Revision != 1 {
			return ErrCorrupt
		}
	case ReplicatedPayloadManifestSegment:
		if len(command.Payload) > ManifestSegmentBytes {
			return ErrTooLarge
		}
		if !canonicalManifestSegment(command.Payload) {
			return ErrCorrupt
		}
	case ReplicatedPayloadParticipantStage:
		if len(command.Payload) != 0 || command.Participant.CoordinatorGroup.IsZero() ||
			command.Participant.CoordinatorShardIncarnation.IsZero() ||
			command.Participant.CoordinatorAllocation == 0 ||
			command.Participant.MutationDigest == (Digest{}) ||
			!ValidateIntentScopes(command.Participant.IntentScopes, command.Participant.BucketBits) {
			return ErrCorrupt
		}
	default:
		return ErrCorrupt
	}
	return nil
}

func replicatedOperationShape(operation ReplicatedOperation) (
	ReplicatedRole,
	ReplicatedPayloadKind,
	bool,
	bool,
) {
	switch operation {
	case ReplicatedStageCoordinator:
		return ReplicatedRoleCoordinator, ReplicatedPayloadCoordinator, true, true
	case ReplicatedStageManifestCoordinator:
		return ReplicatedRoleCoordinator, ReplicatedPayloadManifestCoordinator, true, true
	case ReplicatedStageManifestSegment:
		return ReplicatedRoleCoordinator, ReplicatedPayloadManifestSegment, false, true
	case ReplicatedCommitCoordinator, ReplicatedAbortCoordinator, ReplicatedRetireCoordinator:
		return ReplicatedRoleCoordinator, ReplicatedPayloadNone, false, true
	case ReplicatedStageParticipant:
		return ReplicatedRoleParticipant, ReplicatedPayloadParticipantStage, true, true
	case ReplicatedPrepareParticipant, ReplicatedApplyParticipant,
		ReplicatedAbortParticipant, ReplicatedReleaseParticipant:
		return ReplicatedRoleParticipant, ReplicatedPayloadNone, false, true
	default:
		return ReplicatedRoleInvalid, ReplicatedPayloadNone, false, false
	}
}

func participantStageZero(stage ParticipantStage) bool {
	return stage.CoordinatorGroup.IsZero() && stage.CoordinatorShardIncarnation.IsZero() &&
		stage.CoordinatorAllocation == 0 && stage.BucketBits == 0 &&
		len(stage.IntentScopes) == 0 && stage.MutationDigest == (Digest{})
}

func canonicalCoordinatorBytes(raw []byte) bool {
	if len(raw) < coordinatorHeaderBytes+4 {
		return false
	}
	count := int(binary.LittleEndian.Uint16(raw[6:8]))
	cursor, end := coordinatorHeaderBytes, len(raw)-4
	for i := 0; i < count; i++ {
		if end-cursor < 60 || raw[cursor+3] != 0 {
			return false
		}
		cursor += 60 + int(raw[cursor]) + int(raw[cursor+1])
		if cursor > end {
			return false
		}
	}
	return cursor == end
}

func canonicalManifestSegment(raw []byte) bool {
	if len(raw) < manifestSegmentHeaderBytes+manifestEntryFixedBytes+4 ||
		len(raw) > ManifestSegmentBytes || !equal4(raw[:4], manifestSegmentMagic) ||
		raw[4] != FormatVersion || raw[5] != 0 || raw[6] != 0 || raw[7] != 0 ||
		!checksumOK(raw) {
		return false
	}
	count := int(binary.LittleEndian.Uint32(raw[12:16]))
	payloadBytes := int(binary.LittleEndian.Uint32(raw[24:28]))
	if count <= 0 || count > MaxManifestPageParticipants ||
		binary.LittleEndian.Uint32(raw[28:32]) != 0 ||
		manifestSegmentHeaderBytes+payloadBytes+4 != len(raw) {
		return false
	}
	cursor, end := manifestSegmentHeaderBytes, len(raw)-4
	var priorDistribution, priorShard [MaxShardIdentityBytes]byte
	priorDistributionLength, priorShardLength := 0, 0
	for i := 0; i < count; i++ {
		if end-cursor < manifestEntryFixedBytes {
			return false
		}
		entry := raw[cursor:]
		distributionPrefix, distributionSuffix := int(entry[0]), int(entry[1])
		shardPrefix, shardSuffix := int(entry[2]), int(entry[3])
		distributionLength := distributionPrefix + distributionSuffix
		shardLength := shardPrefix + shardSuffix
		if entry[5] != 0 || entry[6] != 0 || entry[7] != 0 ||
			(i == 0 && (distributionPrefix != 0 || shardPrefix != 0)) ||
			distributionPrefix > priorDistributionLength || shardPrefix > priorShardLength ||
			distributionLength == 0 || distributionLength > MaxShardIdentityBytes ||
			shardLength == 0 || shardLength > MaxShardIdentityBytes ||
			end-cursor-manifestEntryFixedBytes < distributionSuffix+shardSuffix ||
			!ParticipantState(entry[4]).valid() ||
			binary.LittleEndian.Uint64(entry[8:16]) == 0 ||
			binary.LittleEndian.Uint64(entry[16:24]) == 0 ||
			binary.LittleEndian.Uint64(entry[24:32]) == 0 {
			return false
		}
		var mutationDigest Digest
		copy(mutationDigest[:], entry[32:64])
		if mutationDigest == (Digest{}) {
			return false
		}
		cursor += manifestEntryFixedBytes
		var distribution, shard [MaxShardIdentityBytes]byte
		copy(distribution[:distributionPrefix], priorDistribution[:distributionPrefix])
		copy(distribution[distributionPrefix:distributionLength], raw[cursor:cursor+distributionSuffix])
		cursor += distributionSuffix
		copy(shard[:shardPrefix], priorShard[:shardPrefix])
		copy(shard[shardPrefix:shardLength], raw[cursor:cursor+shardSuffix])
		cursor += shardSuffix
		currentDistribution := distribution[:distributionLength]
		currentShard := shard[:shardLength]
		priorDistributionView := priorDistribution[:priorDistributionLength]
		priorShardView := priorShard[:priorShardLength]
		if !utf8.Valid(currentDistribution) || !utf8.Valid(currentShard) ||
			(i != 0 && (compareIdentityBytes(priorDistributionView, priorShardView,
				currentDistribution, currentShard) >= 0 ||
				distributionPrefix != commonPrefix(priorDistributionView, currentDistribution) ||
				shardPrefix != commonPrefix(priorShardView, currentShard))) {
			return false
		}
		priorDistribution, priorShard = distribution, shard
		priorDistributionLength, priorShardLength = distributionLength, shardLength
	}
	return cursor == end
}
