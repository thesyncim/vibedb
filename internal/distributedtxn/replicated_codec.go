package distributedtxn

import (
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"unicode/utf8"
	"unsafe"
)

const (
	replicatedCommandHeaderBytes   = 128
	replicatedCommandChecksumBytes = 4
	// ReplicatedManifestCoordinatorRecordBytes is the exact VTCM prefix length
	// in a manifest-start payload. A canonical sequence of one through
	// MaxManifestSegmentsPerCommand VTM1 pages follows it immediately.
	ReplicatedManifestCoordinatorRecordBytes = manifestCoordinatorHeaderBytes + 4
	// MaxManifestSegmentsPerCommand is a byte-packing bound, not a transaction
	// participant bound. Fifteen worst-case 64 KiB pages keep the manifest page
	// pack below 1 MiB. The outer proposal's native relation mutations are not
	// manifest bytes and may legitimately raise the complete proposal above it.
	MaxManifestSegmentsPerCommand   = 15
	MaxManifestSegmentSequenceBytes = MaxManifestSegmentsPerCommand * ManifestSegmentBytes
	// ReplicatedRetirementSummaryBytes is the sole fixed retirement witness:
	// one canonical validity byte followed by one little-endian nonnegative
	// affected-row count. The enclosing VTRC checksum authenticates these bytes.
	ReplicatedRetirementSummaryBytes = 9

	// MaxReplicatedCommandBytes bounds this transaction control body, not the
	// complete outer proposal. Relation mutation bytes are carried once by the
	// outer replication command and may legitimately make that proposal larger.
	// The maximum control shape is a fused manifest coordinator begin: fixed
	// header, caller-owned intent scopes, VTCM, fifteen VTM1 pages, and checksum.
	MaxReplicatedCommandBytes = replicatedCommandHeaderBytes +
		MaxIntentScopes*8 + ReplicatedManifestCoordinatorRecordBytes +
		MaxManifestSegmentSequenceBytes + replicatedCommandChecksumBytes
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
	// Fused success-path operations use fresh codes. Existing split operations
	// remain readable only while the unreleased legacy gateway is switched as one
	// unit; no old command can acquire stronger fused semantics by aliasing a code.
	ReplicatedBeginPrepareCoordinator
	ReplicatedBeginPrepareManifestCoordinator
	ReplicatedAppendManifestSegments
	ReplicatedStagePrepareParticipant
	ReplicatedApplyReleaseParticipant
	ReplicatedAbortReleaseParticipant
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
	ReplicatedPayloadManifestSegments
	ReplicatedPayloadRetirement
)

// ReplicatedRetirementSummary is the authoritative aggregate retained by a
// retired coordinator. Committed retirement requires a valid nonnegative row
// count; aborted retirement uses the canonical invalid-zero form.
type ReplicatedRetirementSummary struct {
	AffectedRows      int64
	AffectedRowsValid bool
}

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
	// ParticipantOrdinal is the participant's exact position in the canonical
	// coordinator manifest. Zero is a valid ordinal.
	ParticipantOrdinal uint32
}

// ReplicatedCommand is the construction form of one self-delimiting
// transaction control body. A manifest start contains VTCM immediately
// followed by a direct canonical VTM1 page sequence; later pages use the same
// zero-overhead sequence representation.
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

// AppendReplicatedRetirementSummary appends the sole fixed retirement-summary
// grammar. It has no inner checksum because it exists only as a VTRC payload.
func AppendReplicatedRetirementSummary(
	dst []byte,
	summary ReplicatedRetirementSummary,
) ([]byte, error) {
	if summary.AffectedRows < 0 || !summary.AffectedRowsValid && summary.AffectedRows != 0 {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, ReplicatedRetirementSummaryBytes)...)
	if summary.AffectedRowsValid {
		dst[start] = 1
	}
	binary.LittleEndian.PutUint64(dst[start+1:start+9], uint64(summary.AffectedRows))
	return dst, nil
}

// OpenReplicatedRetirementSummary validates and opens one exact fixed
// retirement summary.
func OpenReplicatedRetirementSummary(raw []byte) (ReplicatedRetirementSummary, error) {
	if len(raw) != ReplicatedRetirementSummaryBytes || raw[0] > 1 {
		return ReplicatedRetirementSummary{}, ErrCorrupt
	}
	rows := int64(binary.LittleEndian.Uint64(raw[1:9]))
	valid := raw[0] == 1
	if rows < 0 || !valid && rows != 0 {
		return ReplicatedRetirementSummary{}, ErrCorrupt
	}
	return ReplicatedRetirementSummary{
		AffectedRows: rows, AffectedRowsValid: valid,
	}, nil
}

// ManifestSegmentSequence is a validated, borrowed, zero-overhead concatenation
// of canonical VTM1 pages. Its byte slice and every iterator page are capacity
// clamped aliases of the input.
type ManifestSegmentSequence struct {
	raw              []byte
	count            uint8
	firstIndex       uint32
	firstParticipant uint64
	participantCount uint64
	chain            Digest
}

func (s ManifestSegmentSequence) Bytes() []byte {
	return s.raw[:len(s.raw):len(s.raw)]
}
func (s ManifestSegmentSequence) Count() int               { return int(s.count) }
func (s ManifestSegmentSequence) FirstIndex() uint32       { return s.firstIndex }
func (s ManifestSegmentSequence) FirstParticipant() uint64 { return s.firstParticipant }
func (s ManifestSegmentSequence) ParticipantCount() uint64 { return s.participantCount }
func (s ManifestSegmentSequence) EncodedBytes() uint64     { return uint64(len(s.raw)) }

// ManifestSegmentIterator walks an already validated sequence without
// revalidating or allocating.
type ManifestSegmentIterator struct {
	raw     []byte
	cursor  int
	current ManifestSegment
}

func (s ManifestSegmentSequence) Iterator() ManifestSegmentIterator {
	return ManifestSegmentIterator{raw: s.raw}
}

func (i *ManifestSegmentIterator) Next() bool {
	if i.cursor == len(i.raw) {
		i.current = ManifestSegment{}
		return false
	}
	raw := i.raw[i.cursor:]
	total := manifestSegmentHeaderBytes + int(binary.LittleEndian.Uint32(raw[24:28])) + 4
	page := raw[:total:total]
	i.current = ManifestSegment{
		Index:            binary.LittleEndian.Uint32(page[8:12]),
		FirstParticipant: binary.LittleEndian.Uint64(page[16:24]),
		ParticipantCount: binary.LittleEndian.Uint32(page[12:16]),
		Digest:           sha256.Sum256(page), Raw: page,
	}
	i.cursor += total
	return true
}

func (i *ManifestSegmentIterator) Segment() ManifestSegment { return i.current }

// OpenManifestSegmentSequence validates one direct sequence of one through
// fifteen self-delimiting VTM1 pages. Page ordinals, participant ordinals, and
// identities are strictly increasing across page boundaries.
func OpenManifestSegmentSequence(raw []byte) (ManifestSegmentSequence, error) {
	if len(raw) == 0 || len(raw) > MaxManifestSegmentSequenceBytes {
		if len(raw) > MaxManifestSegmentSequenceBytes {
			return ManifestSegmentSequence{}, ErrTooLarge
		}
		return ManifestSegmentSequence{}, ErrCorrupt
	}
	sequence := ManifestSegmentSequence{raw: raw[:len(raw):len(raw)]}
	cursor := 0
	var nextIndex uint32
	var nextParticipant uint64
	var prior manifestSegmentSummary
	for cursor < len(raw) {
		if sequence.count == MaxManifestSegmentsPerCommand ||
			len(raw)-cursor < manifestSegmentHeaderBytes+manifestEntryFixedBytes+4 {
			return ManifestSegmentSequence{}, ErrCorrupt
		}
		payloadBytes := uint64(binary.LittleEndian.Uint32(raw[cursor+24 : cursor+28]))
		total := uint64(manifestSegmentHeaderBytes+4) + payloadBytes
		if total > ManifestSegmentBytes || total > uint64(len(raw)-cursor) {
			return ManifestSegmentSequence{}, ErrCorrupt
		}
		page := raw[cursor : cursor+int(total) : cursor+int(total)]
		summary, ok := openCanonicalManifestSegment(page)
		if !ok {
			return ManifestSegmentSequence{}, ErrCorrupt
		}
		if sequence.count == 0 {
			sequence.firstIndex = summary.index
			sequence.firstParticipant = summary.firstParticipant
			nextIndex, nextParticipant = summary.index, summary.firstParticipant
		} else if compareIdentityBytes(
			prior.lastDistribution[:prior.lastDistributionLength],
			prior.lastShard[:prior.lastShardLength],
			summary.firstDistribution[:summary.firstDistributionLength],
			summary.firstShard[:summary.firstShardLength],
		) >= 0 {
			return ManifestSegmentSequence{}, ErrCorrupt
		}
		if summary.index != nextIndex || summary.index == ^uint32(0) ||
			summary.firstParticipant != nextParticipant ||
			uint64(summary.participantCount) > ^uint64(0)-nextParticipant {
			return ManifestSegmentSequence{}, ErrCorrupt
		}
		nextIndex++
		nextParticipant += uint64(summary.participantCount)
		sequence.participantCount += uint64(summary.participantCount)
		sequence.chain = appendManifestChain(sequence.chain, summary.index, summary.digest)
		sequence.count++
		prior = summary
		cursor += int(total)
	}
	if cursor != len(raw) || sequence.count == 0 {
		return ManifestSegmentSequence{}, ErrCorrupt
	}
	return sequence, nil
}

// ManifestSegmentSequenceFollows validates previous as one complete canonical
// VTM1 page and requires its final participant identity to sort strictly before
// the first identity in next. The already-opened sequence remains borrowed and
// is inspected without decoding an identity arena or allocating.
func ManifestSegmentSequenceFollows(previous []byte, next ManifestSegmentSequence) error {
	prior, ok := openCanonicalManifestSegment(previous)
	if !ok || len(next.raw) < manifestSegmentHeaderBytes+manifestEntryFixedBytes+4 ||
		next.count == 0 {
		return ErrCorrupt
	}
	payloadBytes := uint64(binary.LittleEndian.Uint32(next.raw[24:28]))
	total := uint64(manifestSegmentHeaderBytes+4) + payloadBytes
	if total > ManifestSegmentBytes || total > uint64(len(next.raw)) {
		return ErrCorrupt
	}
	first, ok := openCanonicalManifestSegment(next.raw[:int(total):int(total)])
	if !ok || compareIdentityBytes(
		prior.lastDistribution[:prior.lastDistributionLength],
		prior.lastShard[:prior.lastShardLength],
		first.firstDistribution[:first.firstDistributionLength],
		first.firstShard[:first.firstShardLength],
	) >= 0 {
		return ErrCorrupt
	}
	return nil
}

// OpenReplicatedManifestStart splits and validates the atomic VTCM plus its
// maximally packed initial VTM1 sequence. Both views borrow the payload.
func OpenReplicatedManifestStart(payload []byte) (
	coordinator []byte,
	segments ManifestSegmentSequence,
	err error,
) {
	if len(payload) <= ReplicatedManifestCoordinatorRecordBytes {
		return nil, ManifestSegmentSequence{}, ErrCorrupt
	}
	coordinatorEnd := ReplicatedManifestCoordinatorRecordBytes
	coordinator = payload[:coordinatorEnd:coordinatorEnd]
	segmentBytes := payload[coordinatorEnd:len(payload):len(payload)]
	record, openErr := OpenManifestCoordinator(coordinator)
	if openErr != nil {
		return nil, ManifestSegmentSequence{}, ErrCorrupt
	}
	segments, openErr = OpenManifestSegmentSequence(segmentBytes)
	if openErr != nil || segments.FirstIndex() != 0 || segments.FirstParticipant() != 0 {
		return nil, ManifestSegmentSequence{}, ErrCorrupt
	}
	descriptor := record.Manifest
	if segments.ParticipantCount() > descriptor.ParticipantCount ||
		segments.EncodedBytes() > descriptor.EncodedBytes {
		return nil, ManifestSegmentSequence{}, ErrCorrupt
	}
	if uint32(segments.Count()) == descriptor.SegmentCount {
		got := ManifestDescriptor{
			ParticipantCount: segments.ParticipantCount(),
			EncodedBytes:     segments.EncodedBytes(), SegmentCount: uint32(segments.Count()),
		}
		got.Root = finishManifestRoot(segments.chain, got)
		if got != descriptor {
			return nil, ManifestSegmentSequence{}, ErrCorrupt
		}
	} else if segments.ParticipantCount() >= descriptor.ParticipantCount ||
		segments.EncodedBytes() >= descriptor.EncodedBytes {
		return nil, ManifestSegmentSequence{}, ErrCorrupt
	}
	return coordinator, segments, nil
}

// ReplicatedCoordinatorBindsParticipant validates an inline or segmented
// coordinator creation payload and exact-matches one participant ordinal
// without allocating. For a segmented coordinator, present is false when the
// requested ordinal is valid for the descriptor but lies after the initial
// packed pages and must be checked when its durable page arrives.
func ReplicatedCoordinatorBindsParticipant(
	payload []byte,
	ordinal uint64,
	want ParticipantRef,
) (present bool, matches bool, err error) {
	if len(payload) < 4 {
		return false, false, ErrCorrupt
	}
	if equal4(payload[:4], coordinatorMagic) {
		var scratch [MaxInlineParticipants]ParticipantRef
		record, openErr := OpenCoordinatorInto(payload, scratch[:])
		if openErr != nil || !canonicalCoordinatorBytes(payload) {
			return false, false, ErrCorrupt
		}
		if ordinal >= uint64(len(record.Participants)) {
			return false, false, ErrCorrupt
		}
		return true, equalParticipantRef(record.Participants[ordinal], want), nil
	}
	if !equal4(payload[:4], manifestCoordinatorMagic) {
		return false, false, ErrCorrupt
	}
	coordinator, segments, openErr := OpenReplicatedManifestStart(payload)
	if openErr != nil {
		return false, false, openErr
	}
	record, openErr := OpenManifestCoordinator(coordinator)
	if openErr != nil || ordinal >= record.Manifest.ParticipantCount {
		return false, false, ErrCorrupt
	}
	iterator := segments.Iterator()
	for iterator.Next() {
		page := iterator.Segment()
		if ordinal < page.FirstParticipant ||
			ordinal >= page.FirstParticipant+uint64(page.ParticipantCount) {
			continue
		}
		return ManifestSegmentMatchesParticipant(page.Raw, ordinal, want)
	}
	return false, false, nil
}

// AppendReplicatedCommand appends one canonical transaction control body.
// Payload must not overlap the writable append region in dst's current backing
// array; aliases are rejected before any destination byte is changed.
func AppendReplicatedCommand(dst []byte, command ReplicatedCommand) ([]byte, error) {
	total, err := ReplicatedCommandSize(command)
	if err != nil {
		return dst, err
	}
	if replicatedPayloadOverlapsAppendRegion(dst, total, command.Payload) {
		return dst, ErrCorrupt
	}
	start := len(dst)
	if total <= cap(dst)-start {
		dst = dst[:start+total]
		clear(dst[start:])
	} else {
		dst = append(dst, make([]byte, total)...)
	}
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
	binary.LittleEndian.PutUint32(out[124:128], command.Participant.ParticipantOrdinal)
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

// ReplicatedCommandSize returns the exact canonical control-body size without
// allocating or encoding. It performs the same semantic and byte-bound
// validation as AppendReplicatedCommand, allowing callers to reserve the
// complete proposal budget before either control or outer-command encoding.
func ReplicatedCommandSize(command ReplicatedCommand) (int, error) {
	if err := validateReplicatedCommand(command); err != nil {
		return 0, err
	}
	return replicatedCommandEncodedSize(command), nil
}

func replicatedCommandEncodedSize(command ReplicatedCommand) int {
	return replicatedCommandHeaderBytes + len(command.Participant.IntentScopes)*8 +
		len(command.Payload) + replicatedCommandChecksumBytes
}

func replicatedPayloadOverlapsAppendRegion(dst []byte, total int, payload []byte) bool {
	if len(payload) == 0 || total <= 0 || total > cap(dst)-len(dst) {
		return false
	}
	region := dst[len(dst) : len(dst)+total : len(dst)+total]
	left := uintptr(unsafe.Pointer(unsafe.SliceData(region)))
	right := uintptr(unsafe.Pointer(unsafe.SliceData(payload)))
	if left <= right {
		return right-left < uintptr(len(region))
	}
	return left-right < uintptr(len(payload))
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

// ValidateReplicatedCommand validates one complete canonical transaction
// control body without retaining a view or materializing participant scopes.
// Replication admission uses this before retaining the immutable raw control
// bytes, keeping the hot path allocation-free.
func ValidateReplicatedCommand(src []byte) error {
	if len(src) < replicatedCommandHeaderBytes+replicatedCommandChecksumBytes ||
		len(src) > MaxReplicatedCommandBytes || !equal4(src[:4], replicatedCommandMagic) ||
		!checksumOK(src) {
		return ErrCorrupt
	}
	if src[4] != FormatVersion {
		return ErrUnsupported
	}
	if binary.LittleEndian.Uint16(src[8:10]) != replicatedCommandHeaderBytes ||
		src[10] != 0 || src[11] != 0 || binary.LittleEndian.Uint32(src[12:16]) != uint32(len(src)) ||
		binary.LittleEndian.Uint32(src[20:24]) != 0 || src[121] != 0 {
		return ErrCorrupt
	}
	payloadLength := uint64(binary.LittleEndian.Uint32(src[16:20]))
	scopeCount := uint64(binary.LittleEndian.Uint16(src[122:124]))
	if scopeCount > MaxIntentScopes ||
		uint64(replicatedCommandHeaderBytes+replicatedCommandChecksumBytes)+scopeCount*8+payloadLength != uint64(len(src)) {
		return ErrCorrupt
	}
	role := ReplicatedRole(src[5])
	operation := ReplicatedOperation(src[6])
	payloadKind := ReplicatedPayloadKind(src[7])
	expectedRevision := binary.LittleEndian.Uint64(src[24:32])
	var id ID
	copy(id[:], src[32:48])
	wantRole, wantPayload, creation, ok := replicatedOperationShape(operation)
	abortFence := operation == ReplicatedAbortReleaseParticipant && expectedRevision == 0
	if abortFence {
		wantPayload, creation = ReplicatedPayloadParticipantStage, true
	}
	if id.IsZero() || !ok || role != wantRole || payloadKind != wantPayload ||
		(creation && expectedRevision != 0) || (!creation && expectedRevision == 0) {
		return ErrCorrupt
	}
	scopeEnd := replicatedCommandHeaderBytes + int(scopeCount)*8
	payloadEnd := scopeEnd + int(payloadLength)
	payload := src[scopeEnd:payloadEnd:payloadEnd]
	if replicatedCommandCarriesParticipant(operation, expectedRevision) {
		var coordinatorGroup, coordinatorShardIncarnation ID
		var mutationDigest Digest
		copy(coordinatorGroup[:], src[48:64])
		copy(coordinatorShardIncarnation[:], src[64:80])
		copy(mutationDigest[:], src[88:120])
		if coordinatorGroup.IsZero() || coordinatorShardIncarnation.IsZero() ||
			binary.LittleEndian.Uint64(src[80:88]) == 0 || mutationDigest == (Digest{}) ||
			!validateIntentScopeBytes(
				src[replicatedCommandHeaderBytes:scopeEnd], src[120], int(scopeCount),
			) {
			return ErrCorrupt
		}
		if operation == ReplicatedStageParticipant &&
			binary.LittleEndian.Uint32(src[124:128]) != 0 {
			return ErrCorrupt
		}
		if abortFence && (src[120] != 0 || scopeCount != 0) {
			return ErrCorrupt
		}
		return validateReplicatedPayload(
			operation, wantPayload, id, payload,
			binary.LittleEndian.Uint32(src[124:128]), mutationDigest,
		)
	}
	if !allZero(src[48:122]) || scopeCount != 0 ||
		binary.LittleEndian.Uint32(src[124:128]) != 0 {
		return ErrCorrupt
	}
	return validateReplicatedPayload(operation, wantPayload, id, payload, 0, Digest{})
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
		binary.LittleEndian.Uint32(src[20:24]) != 0 || src[121] != 0 {
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
	view.Participant.ParticipantOrdinal = binary.LittleEndian.Uint32(src[124:128])
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
	abortFence := command.Operation == ReplicatedAbortReleaseParticipant &&
		command.ExpectedRevision == 0
	if abortFence {
		wantPayload, creation = ReplicatedPayloadParticipantStage, true
	}
	if !ok || command.Role != wantRole || command.PayloadKind != wantPayload ||
		(creation && command.ExpectedRevision != 0) || (!creation && command.ExpectedRevision == 0) {
		return ErrCorrupt
	}
	carriesParticipant := replicatedCommandCarriesParticipant(
		command.Operation, command.ExpectedRevision,
	)
	if !carriesParticipant && !participantStageZero(command.Participant) {
		return ErrCorrupt
	}
	if carriesParticipant {
		if command.Participant.CoordinatorGroup.IsZero() ||
			command.Participant.CoordinatorShardIncarnation.IsZero() ||
			command.Participant.CoordinatorAllocation == 0 ||
			command.Participant.MutationDigest == (Digest{}) ||
			!ValidateIntentScopes(command.Participant.IntentScopes, command.Participant.BucketBits) {
			return ErrCorrupt
		}
		if command.Operation == ReplicatedStageParticipant &&
			command.Participant.ParticipantOrdinal != 0 {
			return ErrCorrupt
		}
		if abortFence && (command.Participant.BucketBits != 0 ||
			len(command.Participant.IntentScopes) != 0) {
			return ErrCorrupt
		}
	}
	if err := validateReplicatedPayload(
		command.Operation, wantPayload, command.ID, command.Payload,
		command.Participant.ParticipantOrdinal, command.Participant.MutationDigest,
	); err != nil {
		return err
	}
	total := replicatedCommandEncodedSize(command)
	if total > MaxReplicatedCommandBytes || !replicatedCommandShapeWithinBound(command) {
		return ErrTooLarge
	}
	return nil
}

func validateReplicatedPayload(
	operation ReplicatedOperation,
	kind ReplicatedPayloadKind,
	id ID,
	payload []byte,
	participantOrdinal uint32,
	mutationDigest Digest,
) error {
	switch kind {
	case ReplicatedPayloadNone:
		if len(payload) != 0 {
			return ErrCorrupt
		}
	case ReplicatedPayloadCoordinator:
		if err := validateReplicatedCoordinatorPayload(
			operation, id, payload, participantOrdinal, mutationDigest,
		); err != nil {
			return err
		}
		if operation == ReplicatedBeginPrepareCoordinator {
			return ValidateReplicatedCoordinatorAuthorityWitnesses(payload)
		}
		return nil
	case ReplicatedPayloadManifestCoordinator:
		coordinator, segments, err := OpenReplicatedManifestStart(payload)
		if err != nil {
			return ErrCorrupt
		}
		record, err := OpenManifestCoordinator(coordinator)
		if err != nil || record.ID != id || record.State != CoordinatorStaging || record.Revision != 1 {
			return ErrCorrupt
		}
		if operation == ReplicatedStageManifestCoordinator {
			if segments.Count() != 1 {
				return ErrCorrupt
			}
		} else {
			want := int(record.Manifest.SegmentCount)
			if want > MaxManifestSegmentsPerCommand {
				want = MaxManifestSegmentsPerCommand
			}
			if operation != ReplicatedBeginPrepareManifestCoordinator || segments.Count() != want {
				return ErrCorrupt
			}
			ordinal := uint64(participantOrdinal)
			if ordinal >= record.Manifest.ParticipantCount {
				return ErrCorrupt
			}
			iterator := segments.Iterator()
			for iterator.Next() {
				page := iterator.Segment()
				if ordinal < page.FirstParticipant ||
					ordinal >= page.FirstParticipant+uint64(page.ParticipantCount) {
					continue
				}
				matched, matchErr := manifestSegmentMatchesParticipantFields(
					page.Raw, ordinal, ParticipantRef{MutationDigest: mutationDigest}, false,
				)
				if matchErr != nil || !matched {
					return ErrCorrupt
				}
				break
			}
			if err = segments.ValidateAuthorityWitnesses(); err != nil {
				return err
			}
		}
	case ReplicatedPayloadManifestSegment:
		if len(payload) > ManifestSegmentBytes {
			return ErrTooLarge
		}
		if !canonicalManifestSegment(payload) ||
			binary.LittleEndian.Uint32(payload[8:12]) == 0 ||
			binary.LittleEndian.Uint64(payload[16:24]) == 0 {
			return ErrCorrupt
		}
	case ReplicatedPayloadManifestSegments:
		segments, err := OpenManifestSegmentSequence(payload)
		if err != nil {
			return err
		}
		if operation != ReplicatedAppendManifestSegments || segments.FirstIndex() == 0 ||
			segments.FirstParticipant() == 0 {
			return ErrCorrupt
		}
		if err = segments.ValidateAuthorityWitnesses(); err != nil {
			return err
		}
	case ReplicatedPayloadParticipantStage:
		if len(payload) != 0 {
			return ErrCorrupt
		}
	case ReplicatedPayloadRetirement:
		if operation != ReplicatedRetireCoordinator {
			return ErrCorrupt
		}
		if _, err := OpenReplicatedRetirementSummary(payload); err != nil {
			return err
		}
	default:
		return ErrCorrupt
	}
	return nil
}

func validateReplicatedCoordinatorPayload(
	operation ReplicatedOperation,
	id ID,
	payload []byte,
	participantOrdinal uint32,
	mutationDigest Digest,
) error {
	if len(payload) > MaxCoordinatorRecordBytes {
		return ErrTooLarge
	}
	var participantScratch [MaxInlineParticipants]ParticipantRef
	record, err := OpenCoordinatorInto(payload, participantScratch[:])
	if err != nil || !canonicalCoordinatorBytes(payload) || record.ID != id ||
		record.State != CoordinatorStaging || record.Revision != 1 {
		return ErrCorrupt
	}
	if operation == ReplicatedBeginPrepareCoordinator {
		ordinal := uint64(participantOrdinal)
		if ordinal >= uint64(len(record.Participants)) ||
			record.Participants[ordinal].MutationDigest != mutationDigest {
			return ErrCorrupt
		}
	}
	return nil
}

func replicatedCommandShapeWithinBound(command ReplicatedCommand) bool {
	scopeBytes := len(command.Participant.IntentScopes) * 8
	switch command.Operation {
	case ReplicatedBeginPrepareManifestCoordinator:
		return len(command.Payload) <= ReplicatedManifestCoordinatorRecordBytes+
			MaxManifestSegmentSequenceBytes && scopeBytes <= MaxIntentScopes*8
	case ReplicatedAppendManifestSegments:
		return scopeBytes == 0 && len(command.Payload) <= MaxManifestSegmentSequenceBytes
	case ReplicatedBeginPrepareCoordinator, ReplicatedStagePrepareParticipant,
		ReplicatedStageParticipant:
		return scopeBytes <= MaxIntentScopes*8
	default:
		return scopeBytes == 0
	}
}

func validateIntentScopeBytes(raw []byte, bucketBits uint8, count int) bool {
	if count == 0 {
		return bucketBits == 0 && len(raw) == 0
	}
	if count > MaxIntentScopes || bucketBits < 8 || bucketBits > 24 || len(raw) != count*8 {
		return false
	}
	limit := uint32(1) << bucketBits
	var priorEnd uint32
	for i := 0; i < count; i++ {
		start := binary.LittleEndian.Uint32(raw[i*8 : i*8+4])
		end := binary.LittleEndian.Uint32(raw[i*8+4 : i*8+8])
		if start >= end || end > limit || (i != 0 && priorEnd >= start) {
			return false
		}
		priorEnd = end
	}
	return true
}

func allZero(raw []byte) bool {
	var combined byte
	for i := range raw {
		combined |= raw[i]
	}
	return combined == 0
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
	case ReplicatedCommitCoordinator, ReplicatedAbortCoordinator:
		return ReplicatedRoleCoordinator, ReplicatedPayloadNone, false, true
	case ReplicatedRetireCoordinator:
		return ReplicatedRoleCoordinator, ReplicatedPayloadRetirement, false, true
	case ReplicatedStageParticipant:
		return ReplicatedRoleParticipant, ReplicatedPayloadParticipantStage, true, true
	case ReplicatedPrepareParticipant, ReplicatedApplyParticipant,
		ReplicatedAbortParticipant, ReplicatedReleaseParticipant:
		return ReplicatedRoleParticipant, ReplicatedPayloadNone, false, true
	case ReplicatedBeginPrepareCoordinator:
		return ReplicatedRoleCoordinator, ReplicatedPayloadCoordinator, true, true
	case ReplicatedBeginPrepareManifestCoordinator:
		return ReplicatedRoleCoordinator, ReplicatedPayloadManifestCoordinator, true, true
	case ReplicatedAppendManifestSegments:
		return ReplicatedRoleCoordinator, ReplicatedPayloadManifestSegments, false, true
	case ReplicatedStagePrepareParticipant:
		return ReplicatedRoleParticipant, ReplicatedPayloadParticipantStage, true, true
	case ReplicatedApplyReleaseParticipant, ReplicatedAbortReleaseParticipant:
		return ReplicatedRoleParticipant, ReplicatedPayloadNone, false, true
	default:
		return ReplicatedRoleInvalid, ReplicatedPayloadNone, false, false
	}
}

func replicatedCommandCarriesParticipant(
	operation ReplicatedOperation,
	expectedRevision uint64,
) bool {
	switch operation {
	case ReplicatedStageParticipant, ReplicatedBeginPrepareCoordinator,
		ReplicatedBeginPrepareManifestCoordinator, ReplicatedStagePrepareParticipant:
		return true
	case ReplicatedAbortReleaseParticipant:
		return expectedRevision == 0
	default:
		return false
	}
}

func participantStageZero(stage ParticipantStage) bool {
	return stage.CoordinatorGroup.IsZero() && stage.CoordinatorShardIncarnation.IsZero() &&
		stage.CoordinatorAllocation == 0 && stage.BucketBits == 0 &&
		len(stage.IntentScopes) == 0 && stage.MutationDigest == (Digest{}) &&
		stage.ParticipantOrdinal == 0
}

func canonicalCoordinatorBytes(raw []byte) bool {
	if len(raw) < coordinatorHeaderBytes+4 {
		return false
	}
	count := int(binary.LittleEndian.Uint16(raw[6:8]))
	cursor, end := coordinatorHeaderBytes, len(raw)-4
	for i := 0; i < count; i++ {
		if end-cursor < coordinatorEntryBytes || raw[cursor+3] != 0 {
			return false
		}
		cursor += coordinatorEntryBytes + int(raw[cursor]) + int(raw[cursor+1])
		if cursor > end {
			return false
		}
	}
	return cursor == end
}

type manifestSegmentSummary struct {
	index            uint32
	firstParticipant uint64
	participantCount uint32
	digest           Digest

	firstDistribution       [MaxShardIdentityBytes]byte
	firstShard              [MaxShardIdentityBytes]byte
	firstDistributionLength int
	firstShardLength        int
	lastDistribution        [MaxShardIdentityBytes]byte
	lastShard               [MaxShardIdentityBytes]byte
	lastDistributionLength  int
	lastShardLength         int
}

type manifestParticipantMatch struct {
	ordinal uint64
	want    ParticipantRef
	exact   bool
	found   bool
	matched bool
}

func canonicalManifestSegment(raw []byte) bool {
	_, ok := inspectCanonicalManifestSegment(raw, nil)
	return ok
}

func openCanonicalManifestSegment(raw []byte) (manifestSegmentSummary, bool) {
	return inspectCanonicalManifestSegment(raw, nil)
}

// ManifestSegmentMatchesParticipant validates a complete VTM1 page and exact-
// matches one absolute participant ordinal without a decoded-page arena.
// present distinguishes an ordinal outside the page from a mismatch.
func ManifestSegmentMatchesParticipant(
	raw []byte,
	ordinal uint64,
	want ParticipantRef,
) (present bool, matches bool, err error) {
	match := manifestParticipantMatch{ordinal: ordinal, want: want, exact: true}
	if _, ok := inspectCanonicalManifestSegment(raw, &match); !ok {
		return false, false, ErrCorrupt
	}
	return match.found, match.matched, nil
}

func manifestSegmentMatchesParticipantFields(
	raw []byte,
	ordinal uint64,
	want ParticipantRef,
	exact bool,
) (bool, error) {
	match := manifestParticipantMatch{ordinal: ordinal, want: want, exact: exact}
	if _, ok := inspectCanonicalManifestSegment(raw, &match); !ok {
		return false, ErrCorrupt
	}
	return match.found && match.matched, nil
}

func inspectCanonicalManifestSegment(
	raw []byte,
	match *manifestParticipantMatch,
) (manifestSegmentSummary, bool) {
	var summary manifestSegmentSummary
	if len(raw) < manifestSegmentHeaderBytes+manifestEntryFixedBytes+4 ||
		len(raw) > ManifestSegmentBytes || !equal4(raw[:4], manifestSegmentMagic) ||
		raw[4] != FormatVersion || raw[5] != 0 || raw[6] != 0 || raw[7] != 0 ||
		!checksumOK(raw) {
		return manifestSegmentSummary{}, false
	}
	summary.index = binary.LittleEndian.Uint32(raw[8:12])
	summary.firstParticipant = binary.LittleEndian.Uint64(raw[16:24])
	count := int(binary.LittleEndian.Uint32(raw[12:16]))
	summary.participantCount = uint32(count)
	payloadBytes := int(binary.LittleEndian.Uint32(raw[24:28]))
	if count <= 0 || count > MaxManifestPageParticipants ||
		uint64(count) > ^uint64(0)-summary.firstParticipant ||
		binary.LittleEndian.Uint32(raw[28:32]) != 0 ||
		manifestSegmentHeaderBytes+payloadBytes+4 != len(raw) {
		return manifestSegmentSummary{}, false
	}
	cursor, end := manifestSegmentHeaderBytes, len(raw)-4
	var priorDistribution, priorShard [MaxShardIdentityBytes]byte
	priorDistributionLength, priorShardLength := 0, 0
	for i := 0; i < count; i++ {
		if end-cursor < manifestEntryFixedBytes {
			return manifestSegmentSummary{}, false
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
			return manifestSegmentSummary{}, false
		}
		var mutationDigest Digest
		copy(mutationDigest[:], entry[32:64])
		var authorityWitness AuthorityWitness
		copy(authorityWitness[:], entry[64:80])
		if mutationDigest == (Digest{}) {
			return manifestSegmentSummary{}, false
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
			return manifestSegmentSummary{}, false
		}
		if i == 0 {
			copy(summary.firstDistribution[:], currentDistribution)
			copy(summary.firstShard[:], currentShard)
			summary.firstDistributionLength = distributionLength
			summary.firstShardLength = shardLength
		}
		absoluteOrdinal := summary.firstParticipant + uint64(i)
		if match != nil && absoluteOrdinal == match.ordinal {
			match.found = true
			current := ParticipantRef{
				Distribution: currentDistribution, Shard: currentShard,
				State:                ParticipantState(entry[4]),
				RoutingVersion:       binary.LittleEndian.Uint64(entry[8:16]),
				AllocationGeneration: binary.LittleEndian.Uint64(entry[16:24]),
				OwnershipEpoch:       binary.LittleEndian.Uint64(entry[24:32]),
				AuthorityWitness:     authorityWitness,
				MutationDigest:       mutationDigest,
			}
			if match.exact {
				match.matched = equalParticipantRef(current, match.want)
			} else {
				match.matched = current.MutationDigest == match.want.MutationDigest
			}
		}
		priorDistribution, priorShard = distribution, shard
		priorDistributionLength, priorShardLength = distributionLength, shardLength
	}
	if cursor != end {
		return manifestSegmentSummary{}, false
	}
	copy(summary.lastDistribution[:], priorDistribution[:priorDistributionLength])
	copy(summary.lastShard[:], priorShard[:priorShardLength])
	summary.lastDistributionLength = priorDistributionLength
	summary.lastShardLength = priorShardLength
	summary.digest = sha256.Sum256(raw)
	return summary, true
}
