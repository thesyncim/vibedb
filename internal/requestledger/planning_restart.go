package requestledger

import "encoding/binary"

const (
	PlanningRestartRequestBytes = 144
	PlanningCleanupRequestBytes = 88
)

var (
	planningRestartMagic = [4]byte{'V', 'R', 'L', 'N'}
	planningCleanupMagic = [4]byte{'V', 'R', 'L', 'J'}
)

type PlanningRestartRequest struct {
	ObservedAppliedIndex uint64
	NextLeaseExpiryIndex uint64
	PriorGeneration      uint64
	NextGeneration       uint64
	PriorPlanBuildID     Digest
	NextPlanBuildID      Digest
	KeyDigest            Digest
}

func NewPlanningRestartRequest(
	head HeadRecord,
	observedAppliedIndex, nextLeaseExpiryIndex uint64,
	nextPlanBuildID Digest,
) (PlanningRestartRequest, error) {
	request := PlanningRestartRequest{
		ObservedAppliedIndex: observedAppliedIndex, NextLeaseExpiryIndex: nextLeaseExpiryIndex,
		PriorGeneration: head.PlanBuildGeneration, NextGeneration: head.PlanBuildGeneration + 1,
		PriorPlanBuildID: head.PlanBuildID, NextPlanBuildID: nextPlanBuildID, KeyDigest: head.KeyDigest,
	}
	if err := ValidatePlanningRestart(head, request); err != nil {
		return PlanningRestartRequest{}, err
	}
	return request, nil
}

func ValidatePlanningRestart(head HeadRecord, request PlanningRestartRequest) error {
	if err := validateHead(head); err != nil || head.Phase != PhaseExpired ||
		head.AppendedPageCount != 0 || head.AppendedPlanBytes != 0 ||
		head.ExpiredCleanupNextPage != 0 || nonzeroDigest(head.PageChain) ||
		request.ObservedAppliedIndex == 0 || request.NextLeaseExpiryIndex <= request.ObservedAppliedIndex ||
		request.PriorGeneration != head.PlanBuildGeneration || request.PriorGeneration == ^uint64(0) ||
		request.NextGeneration != request.PriorGeneration+1 ||
		request.PriorPlanBuildID != head.PlanBuildID || !nonzeroDigest(request.NextPlanBuildID) ||
		request.NextPlanBuildID == request.PriorPlanBuildID || request.KeyDigest != head.KeyDigest {
		return ErrInvalidState
	}
	return nil
}

func RestartPlanning(head HeadRecord, request PlanningRestartRequest, revision uint64) (HeadRecord, error) {
	if err := ValidatePlanningRestart(head, request); err != nil || !nextRevision(head.Revision, revision) {
		return HeadRecord{}, ErrInvalidState
	}
	head.Phase = PhasePlanning
	head.Revision = revision
	head.PlanBuildID = request.NextPlanBuildID
	head.PlanBuildGeneration = request.NextGeneration
	head.PlanningLeaseGeneration = request.NextGeneration
	head.PlanningLeaseExpiryIndex = request.NextLeaseExpiryIndex
	return head, validateHead(head)
}

func AppendPlanningRestartRequest(dst []byte, request PlanningRestartRequest) ([]byte, error) {
	if err := validatePlanningRestartRequest(request); err != nil {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, PlanningRestartRequestBytes-checksumBytes)...)
	out := dst[start:]
	copy(out[:4], planningRestartMagic[:])
	binary.LittleEndian.PutUint64(out[8:16], request.ObservedAppliedIndex)
	binary.LittleEndian.PutUint64(out[16:24], request.NextLeaseExpiryIndex)
	binary.LittleEndian.PutUint64(out[24:32], request.PriorGeneration)
	binary.LittleEndian.PutUint64(out[32:40], request.NextGeneration)
	putDigest(out[40:72], request.PriorPlanBuildID)
	putDigest(out[72:104], request.NextPlanBuildID)
	putDigest(out[104:136], request.KeyDigest)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenPlanningRestartRequest(raw []byte) (PlanningRestartRequest, error) {
	if len(raw) != PlanningRestartRequestBytes || !magicOK(raw, planningRestartMagic) ||
		!zeroBytes(raw[4:8]) || !zeroBytes(raw[136:140]) || !checksumOK(raw) {
		return PlanningRestartRequest{}, ErrCorrupt
	}
	request := PlanningRestartRequest{
		ObservedAppliedIndex: binary.LittleEndian.Uint64(raw[8:16]),
		NextLeaseExpiryIndex: binary.LittleEndian.Uint64(raw[16:24]),
		PriorGeneration:      binary.LittleEndian.Uint64(raw[24:32]), NextGeneration: binary.LittleEndian.Uint64(raw[32:40]),
		PriorPlanBuildID: readDigest(raw[40:72]), NextPlanBuildID: readDigest(raw[72:104]),
		KeyDigest: readDigest(raw[104:136]),
	}
	if err := validatePlanningRestartRequest(request); err != nil {
		return PlanningRestartRequest{}, ErrCorrupt
	}
	return request, nil
}

func validatePlanningRestartRequest(request PlanningRestartRequest) error {
	if request.ObservedAppliedIndex == 0 || request.NextLeaseExpiryIndex <= request.ObservedAppliedIndex ||
		request.PriorGeneration == 0 || request.PriorGeneration == ^uint64(0) ||
		request.NextGeneration != request.PriorGeneration+1 || !nonzeroDigest(request.PriorPlanBuildID) ||
		!nonzeroDigest(request.NextPlanBuildID) || request.PriorPlanBuildID == request.NextPlanBuildID ||
		!nonzeroDigest(request.KeyDigest) {
		return ErrCorrupt
	}
	return nil
}

type PlanningCleanupRequest struct {
	BuildGeneration uint64
	PlanBuildID     Digest
	MaxRows         uint16
	MaxBytes        uint32
}

type PlanningCleanupChunk struct {
	FirstOrdinal, PageCount, ReclaimedBytes uint64
	Final                                   bool
}

func NewPlanningCleanupRequest(head HeadRecord, maxRows uint16, maxBytes uint32) (PlanningCleanupRequest, error) {
	request := PlanningCleanupRequest{BuildGeneration: head.PlanBuildGeneration,
		PlanBuildID: head.PlanBuildID, MaxRows: maxRows, MaxBytes: maxBytes}
	if err := validateHead(head); err != nil || errOrNil(validatePlanningCleanupRequest(request)) != nil ||
		head.Phase != PhaseExpired {
		return PlanningCleanupRequest{}, ErrInvalidState
	}
	return request, nil
}

func AppendPlanningCleanupRequest(dst []byte, request PlanningCleanupRequest) ([]byte, error) {
	if err := validatePlanningCleanupRequest(request); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, PlanningCleanupRequestBytes-checksumBytes)...)
	out := dst[start:]
	copy(out[:4], planningCleanupMagic[:])
	binary.LittleEndian.PutUint64(out[8:16], request.BuildGeneration)
	binary.LittleEndian.PutUint16(out[16:18], request.MaxRows)
	binary.LittleEndian.PutUint32(out[20:24], request.MaxBytes)
	putDigest(out[24:56], request.PlanBuildID)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenPlanningCleanupRequest(raw []byte) (PlanningCleanupRequest, error) {
	if len(raw) != PlanningCleanupRequestBytes || !magicOK(raw, planningCleanupMagic) ||
		!zeroBytes(raw[4:8]) || !zeroBytes(raw[18:20]) || !zeroBytes(raw[56:84]) || !checksumOK(raw) {
		return PlanningCleanupRequest{}, ErrCorrupt
	}
	request := PlanningCleanupRequest{BuildGeneration: binary.LittleEndian.Uint64(raw[8:16]),
		MaxRows: binary.LittleEndian.Uint16(raw[16:18]), MaxBytes: binary.LittleEndian.Uint32(raw[20:24]),
		PlanBuildID: readDigest(raw[24:56])}
	if err := validatePlanningCleanupRequest(request); err != nil {
		return PlanningCleanupRequest{}, ErrCorrupt
	}
	return request, nil
}

func validatePlanningCleanupRequest(request PlanningCleanupRequest) error {
	if request.BuildGeneration == 0 || !nonzeroDigest(request.PlanBuildID) ||
		request.MaxRows == 0 || request.MaxRows > MaxAckGCDeleteRows || request.MaxBytes == 0 {
		return ErrCorrupt
	}
	return nil
}

func PlanPlanningCleanup(head HeadRecord, request PlanningCleanupRequest) (PlanningCleanupChunk, error) {
	if err := validateHead(head); err != nil || errOrNil(validatePlanningCleanupRequest(request)) != nil ||
		head.Phase != PhaseExpired || request.BuildGeneration != head.PlanBuildGeneration ||
		request.PlanBuildID != head.PlanBuildID || head.ExpiredCleanupNextPage >= head.AppendedPageCount {
		return PlanningCleanupChunk{}, ErrInvalidState
	}
	chunk := PlanningCleanupChunk{FirstOrdinal: head.ExpiredCleanupNextPage}
	remainingRows, remainingBytes := uint64(request.MaxRows), uint64(request.MaxBytes)
	for ordinal := head.ExpiredCleanupNextPage; ordinal < head.AppendedPageCount && remainingRows > 0; ordinal++ {
		offset := ordinal * MaxPlanPageBytes
		dataBytes := min(uint64(MaxPlanPageBytes), head.TotalPlanBytes-offset)
		rowBytes := uint64(PageStorageKeyBytes+pageHeaderBytes+checksumBytes) + dataBytes
		if rowBytes > remainingBytes {
			break
		}
		chunk.PageCount++
		chunk.ReclaimedBytes += rowBytes
		remainingRows--
		remainingBytes -= rowBytes
	}
	if chunk.PageCount == 0 {
		return PlanningCleanupChunk{}, ErrIncomplete
	}
	chunk.Final = chunk.FirstOrdinal+chunk.PageCount == head.AppendedPageCount
	return chunk, nil
}

func AdvancePlanningCleanup(
	head HeadRecord,
	request PlanningCleanupRequest,
	chunk PlanningCleanupChunk,
	revision uint64,
) (HeadRecord, error) {
	expected, err := PlanPlanningCleanup(head, request)
	if err != nil || expected != chunk || !nextRevision(head.Revision, revision) {
		return HeadRecord{}, ErrInvalidState
	}
	head.Revision = revision
	head.ExpiredCleanupNextPage += chunk.PageCount
	if chunk.Final {
		head.AppendedPageCount = 0
		head.AppendedPlanBytes = 0
		head.ExpiredCleanupNextPage = 0
		head.PageChain = Digest{}
		head.PlanCRC32C = 0
		head.PlanCRCBytes = 0
		head.PlanFramingValid = false
	}
	return head, validateHead(head)
}
