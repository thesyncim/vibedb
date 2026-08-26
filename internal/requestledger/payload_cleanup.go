package requestledger

import "encoding/binary"

const PayloadCleanupRequestBytes = 80

var payloadCleanupMagic = [4]byte{'V', 'R', 'L', 'D'}

type PayloadCleanupRequest struct {
	BuildDigest Digest
	MaxRows     uint16
	MaxBytes    uint32
}
type PayloadCleanupChunk struct {
	FirstOrdinal, ChunkCount, ReclaimedBytes uint64
	Final                                    bool
}

func NewPayloadCleanupRequest(head HeadRecord, maxRows uint16, maxBytes uint32) (PayloadCleanupRequest, error) {
	r := PayloadCleanupRequest{BuildDigest: head.CleanupBuildDigest, MaxRows: maxRows, MaxBytes: maxBytes}
	if err := validatePayloadCleanupRequest(r); err != nil || !nonzeroDigest(head.CleanupBuildDigest) {
		return PayloadCleanupRequest{}, ErrInvalidState
	}
	return r, nil
}
func AppendPayloadCleanupRequest(dst []byte, r PayloadCleanupRequest) ([]byte, error) {
	if err := validatePayloadCleanupRequest(r); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, PayloadCleanupRequestBytes-checksumBytes)...)
	out := dst[start:]
	copy(out[:4], payloadCleanupMagic[:])
	binary.LittleEndian.PutUint16(out[8:10], r.MaxRows)
	binary.LittleEndian.PutUint32(out[12:16], r.MaxBytes)
	putDigest(out[16:48], r.BuildDigest)
	dst = appendChecksum(dst, start)
	return dst, nil
}
func OpenPayloadCleanupRequest(raw []byte) (PayloadCleanupRequest, error) {
	if len(raw) != PayloadCleanupRequestBytes || !magicOK(raw, payloadCleanupMagic) || !zeroBytes(raw[4:8]) || !zeroBytes(raw[10:12]) || !zeroBytes(raw[48:76]) || !checksumOK(raw) {
		return PayloadCleanupRequest{}, ErrCorrupt
	}
	r := PayloadCleanupRequest{MaxRows: binary.LittleEndian.Uint16(raw[8:10]), MaxBytes: binary.LittleEndian.Uint32(raw[12:16]), BuildDigest: readDigest(raw[16:48])}
	if err := validatePayloadCleanupRequest(r); err != nil {
		return PayloadCleanupRequest{}, ErrCorrupt
	}
	return r, nil
}
func validatePayloadCleanupRequest(r PayloadCleanupRequest) error {
	if !nonzeroDigest(r.BuildDigest) || r.MaxRows == 0 || r.MaxRows > MaxAckGCDeleteRows || r.MaxBytes == 0 {
		return ErrCorrupt
	}
	return nil
}

// PlanPayloadCleanup derives the only legal next delete chunk. The caller may
// choose budgets, never cursor or reclaimed accounting.
func PlanPayloadCleanup(head HeadRecord, request PayloadCleanupRequest) (PayloadCleanupChunk, error) {
	if err := validateHead(head); err != nil || errOrNil(validatePayloadCleanupRequest(request)) != nil ||
		request.BuildDigest != head.CleanupBuildDigest || nonzeroDigest(head.OutstandingRoutePinDigest) {
		return PayloadCleanupChunk{}, ErrInvalidState
	}
	chunk := PayloadCleanupChunk{FirstOrdinal: head.CleanupNextChunk}
	remainingRows := uint64(request.MaxRows)
	remainingBytes := uint64(request.MaxBytes)
	for ordinal := head.CleanupNextChunk; ordinal < head.CleanupChunkCount && remainingRows > 0; ordinal++ {
		offset := ordinal * MaxPlanPageBytes
		dataBytes := min(uint64(MaxPlanPageBytes), head.CleanupTotalDataBytes-offset)
		rowBytes := uint64(PayloadStorageKeyBytes+payloadChunkHeaderBytes+checksumBytes) + dataBytes
		if rowBytes > remainingBytes {
			break
		}
		chunk.ChunkCount++
		chunk.ReclaimedBytes += rowBytes
		remainingRows--
		remainingBytes -= rowBytes
	}
	if chunk.ChunkCount == 0 {
		return PayloadCleanupChunk{}, ErrIncomplete
	}
	if head.CleanupNextChunk+chunk.ChunkCount == head.CleanupChunkCount {
		buildBytes := uint64(FixedStorageKeyBytes + payloadBuildBytes)
		if buildBytes <= remainingBytes && remainingRows > 0 {
			chunk.ReclaimedBytes += buildBytes
			chunk.Final = true
		}
	}
	return chunk, nil
}

func AdvancePayloadCleanup(head HeadRecord, request PayloadCleanupRequest, chunk PayloadCleanupChunk, revision uint64) (HeadRecord, error) {
	expected, err := PlanPayloadCleanup(head, request)
	if err != nil || expected != chunk || !nextRevision(head.Revision, revision) || chunk.ReclaimedBytes > head.CleanupPayloadBytes {
		return HeadRecord{}, ErrInvalidState
	}
	head.Revision = revision
	head.CleanupNextChunk += chunk.ChunkCount
	head.CleanupPayloadBytes -= chunk.ReclaimedBytes
	if chunk.Final {
		if head.CleanupNextChunk != head.CleanupChunkCount || head.CleanupPayloadBytes != 0 {
			return HeadRecord{}, ErrIncomplete
		}
		head.CleanupBuildDigest = Digest{}
		head.CleanupNextChunk = 0
		head.CleanupChunkCount = 0
		head.CleanupTotalDataBytes = 0
	}
	return head, validateHead(head)
}
