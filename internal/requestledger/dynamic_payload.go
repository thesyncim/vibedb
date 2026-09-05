package requestledger

import (
	"crypto/sha256"
	"encoding/binary"
)

const (
	// These are per-physical-wave byte/chunk bounds, never target caps.
	MaxDynamicWavePayloadBytes  = uint64(MaxPendingWaveSteps) * uint64(MaxTargetBytes+MaxCommandBytes)
	MaxDynamicWavePayloadChunks = uint64(MaxPendingWaveSteps) * 2 *
		uint64((MaxCommandBytes+MaxPlanPageBytes-1)/MaxPlanPageBytes)
	payloadBuildBytes          = 392
	payloadChunkHeaderBytes    = 240
	PayloadBuildRecordBytes    = payloadBuildBytes
	MaxPayloadChunkRecordBytes = payloadChunkHeaderBytes + MaxPlanPageBytes + checksumBytes
)

var (
	payloadBuildMagic        = [4]byte{'V', 'R', 'L', 'Z'}
	payloadChunkMagic        = [4]byte{'V', 'R', 'L', 'Y'}
	payloadBuildDigestDomain = []byte("vibedb/request-ledger/payload-build\x00")
)

type PayloadBuildPhase uint8

const (
	PayloadBuildInvalid PayloadBuildPhase = iota
	PayloadBuildStaging
	PayloadBuildSealed
)

type PayloadRootAccumulator struct {
	key                           Digest
	total, count, offset, ordinal uint64
	chain                         Digest
	failed                        bool
}

func NewPayloadRootAccumulator(key Digest, total uint64) (PayloadRootAccumulator, error) {
	if !nonzeroDigest(key) || total == 0 || total > MaxDynamicWavePayloadBytes {
		return PayloadRootAccumulator{}, ErrCorrupt
	}
	return PayloadRootAccumulator{key: key, total: total, count: (total + MaxPlanPageBytes - 1) / MaxPlanPageBytes}, nil
}
func (a *PayloadRootAccumulator) Append(data []byte) error {
	if a.failed || a.offset >= a.total {
		return ErrInvalidState
	}
	want := min(uint64(MaxPlanPageBytes), a.total-a.offset)
	if uint64(len(data)) != want {
		a.failed = true
		return ErrCorrupt
	}
	a.chain = PlanPageChain(a.key, a.ordinal, a.count, a.offset, a.total, a.chain, data)
	a.ordinal++
	a.offset += uint64(len(data))
	return nil
}
func (a *PayloadRootAccumulator) Root() (Digest, error) {
	if a.failed || a.offset != a.total || a.ordinal != a.count {
		return Digest{}, ErrIncomplete
	}
	return a.chain, nil
}

// PayloadBuildRecord is the sole per-current-wave CAS winner. Dynamic target
// and command bytes form one content-addressed aggregate; StepRefs are iovecs
// into it, so alternate roots cannot upload unbounded orphan content.
type PayloadBuildRecord struct {
	KeyDigest, RequestDigest, PlanRoot                       Digest
	PriorContinuationDigest, ContentRoot, BuildDigest, Chain Digest
	WaveOrdinal, Revision, TotalBytes, ChunkCount            uint64
	NextChunkOrdinal, StagedBytes                            uint64
	// CommandEpoch freezes the producer epoch before the first payload chunk.
	// A recovered controller must reconstruct this exact command, not rebind
	// its bytes to the newer lease used to admit the retry.
	CommandEpoch uint64
	Phase        PayloadBuildPhase
}

func NewPayloadBuild(head HeadRecord, contentRoot Digest, totalBytes, chunkCount, commandEpoch uint64) (PayloadBuildRecord, error) {
	if err := validateHead(head); err != nil || head.Phase != PhaseSealed ||
		nonzeroDigest(head.CleanupBuildDigest) ||
		head.MaxActivePayloadBytes == 0 || !nonzeroDigest(contentRoot) || totalBytes == 0 ||
		totalBytes > head.MaxActivePayloadBytes || chunkCount == 0 || chunkCount > head.MaxActivePayloadChunks ||
		chunkCount != (totalBytes+MaxPlanPageBytes-1)/MaxPlanPageBytes || commandEpoch == 0 {
		return PayloadBuildRecord{}, ErrInvalidState
	}
	record := PayloadBuildRecord{KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest,
		PlanRoot: head.PlanRoot, PriorContinuationDigest: head.ContinuationDigest,
		ContentRoot: contentRoot, WaveOrdinal: head.NextStepOrdinal, Revision: 1,
		TotalBytes: totalBytes, ChunkCount: chunkCount, CommandEpoch: commandEpoch, Phase: PayloadBuildStaging}
	record.BuildDigest = payloadBuildDigest(record)
	return record, validatePayloadBuild(record)
}

func AppendPayloadBuild(dst []byte, record PayloadBuildRecord) ([]byte, error) {
	if err := validatePayloadBuild(record); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, payloadBuildBytes-checksumBytes)...)
	out := dst[start:]
	copy(out[:4], payloadBuildMagic[:])
	out[8] = byte(record.Phase)
	binary.LittleEndian.PutUint64(out[16:24], record.WaveOrdinal)
	binary.LittleEndian.PutUint64(out[24:32], record.Revision)
	binary.LittleEndian.PutUint64(out[32:40], record.TotalBytes)
	binary.LittleEndian.PutUint64(out[40:48], record.ChunkCount)
	binary.LittleEndian.PutUint64(out[48:56], record.NextChunkOrdinal)
	binary.LittleEndian.PutUint64(out[56:64], record.StagedBytes)
	putDigest(out[64:96], record.KeyDigest)
	putDigest(out[96:128], record.RequestDigest)
	putDigest(out[128:160], record.PlanRoot)
	putDigest(out[160:192], record.PriorContinuationDigest)
	putDigest(out[192:224], record.ContentRoot)
	putDigest(out[224:256], record.BuildDigest)
	putDigest(out[256:288], record.Chain)
	binary.LittleEndian.PutUint64(out[288:296], record.CommandEpoch)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenPayloadBuild(raw []byte) (PayloadBuildRecord, error) {
	if len(raw) != payloadBuildBytes || !magicOK(raw, payloadBuildMagic) || !zeroBytes(raw[4:8]) || !zeroBytes(raw[9:16]) || !zeroBytes(raw[296:388]) || !checksumOK(raw) {
		return PayloadBuildRecord{}, ErrCorrupt
	}
	r := PayloadBuildRecord{Phase: PayloadBuildPhase(raw[8]), WaveOrdinal: binary.LittleEndian.Uint64(raw[16:24]), Revision: binary.LittleEndian.Uint64(raw[24:32]), TotalBytes: binary.LittleEndian.Uint64(raw[32:40]), ChunkCount: binary.LittleEndian.Uint64(raw[40:48]), NextChunkOrdinal: binary.LittleEndian.Uint64(raw[48:56]), StagedBytes: binary.LittleEndian.Uint64(raw[56:64]), KeyDigest: readDigest(raw[64:96]), RequestDigest: readDigest(raw[96:128]), PlanRoot: readDigest(raw[128:160]), PriorContinuationDigest: readDigest(raw[160:192]), ContentRoot: readDigest(raw[192:224]), BuildDigest: readDigest(raw[224:256]), Chain: readDigest(raw[256:288])}
	r.CommandEpoch = binary.LittleEndian.Uint64(raw[288:296])
	if err := validatePayloadBuild(r); err != nil {
		return PayloadBuildRecord{}, ErrCorrupt
	}
	return r, nil
}

type PayloadChunkRecord struct {
	KeyDigest, PlanRoot, BuildDigest, ContentRoot Digest
	Ordinal, Count, Offset, TotalBytes            uint64
	PreviousChain, Chain                          Digest
	Data                                          []byte
}

func NewPayloadChunk(build PayloadBuildRecord, data []byte) (PayloadChunkRecord, error) {
	if err := validatePayloadBuild(build); err != nil || build.Phase != PayloadBuildStaging || build.NextChunkOrdinal >= build.ChunkCount {
		return PayloadChunkRecord{}, ErrInvalidState
	}
	r := PayloadChunkRecord{KeyDigest: build.KeyDigest, PlanRoot: build.PlanRoot, BuildDigest: build.BuildDigest, ContentRoot: build.ContentRoot, Ordinal: build.NextChunkOrdinal, Count: build.ChunkCount, Offset: build.StagedBytes, TotalBytes: build.TotalBytes, PreviousChain: build.Chain, Data: data[:len(data):len(data)]}
	r.Chain = PlanPageChain(r.KeyDigest, r.Ordinal, r.Count, r.Offset, r.TotalBytes, r.PreviousChain, r.Data)
	if err := validatePayloadChunk(r); err != nil {
		return PayloadChunkRecord{}, err
	}
	return r, nil
}

func AdvancePayloadBuild(build PayloadBuildRecord, chunk PayloadChunkRecord, revision uint64) (PayloadBuildRecord, error) {
	if err := validatePayloadBuild(build); err != nil || errOrNil(validatePayloadChunk(chunk)) != nil || build.Phase != PayloadBuildStaging || !nextRevision(build.Revision, revision) || chunk.KeyDigest != build.KeyDigest || chunk.PlanRoot != build.PlanRoot || chunk.BuildDigest != build.BuildDigest || chunk.ContentRoot != build.ContentRoot || chunk.Ordinal != build.NextChunkOrdinal || chunk.Offset != build.StagedBytes || chunk.Count != build.ChunkCount || chunk.TotalBytes != build.TotalBytes || chunk.PreviousChain != build.Chain {
		return PayloadBuildRecord{}, ErrInvalidState
	}
	build.Revision = revision
	build.NextChunkOrdinal++
	build.StagedBytes += uint64(len(chunk.Data))
	build.Chain = chunk.Chain
	return build, validatePayloadBuild(build)
}

func SealPayloadBuild(build PayloadBuildRecord, revision uint64) (PayloadBuildRecord, error) {
	if err := validatePayloadBuild(build); err != nil || build.Phase != PayloadBuildStaging || build.NextChunkOrdinal != build.ChunkCount || build.StagedBytes != build.TotalBytes || build.Chain != build.ContentRoot || !nextRevision(build.Revision, revision) {
		return PayloadBuildRecord{}, ErrIncomplete
	}
	build.Phase = PayloadBuildSealed
	build.Revision = revision
	return build, validatePayloadBuild(build)
}

func AppendPayloadChunk(dst []byte, r PayloadChunkRecord) ([]byte, error) {
	if err := validatePayloadChunk(r); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, payloadChunkHeaderBytes)...)
	out := dst[start:]
	copy(out[:4], payloadChunkMagic[:])
	binary.LittleEndian.PutUint64(out[8:16], r.Ordinal)
	binary.LittleEndian.PutUint64(out[16:24], r.Count)
	binary.LittleEndian.PutUint64(out[24:32], r.Offset)
	binary.LittleEndian.PutUint64(out[32:40], r.TotalBytes)
	binary.LittleEndian.PutUint64(out[40:48], uint64(len(r.Data)))
	putDigest(out[48:80], r.KeyDigest)
	putDigest(out[80:112], r.PlanRoot)
	putDigest(out[112:144], r.BuildDigest)
	putDigest(out[144:176], r.ContentRoot)
	putDigest(out[176:208], r.PreviousChain)
	putDigest(out[208:240], r.Chain)
	dst = append(dst, r.Data...)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenPayloadChunk(raw []byte) (PayloadChunkRecord, error) {
	if len(raw) < payloadChunkHeaderBytes+1+checksumBytes || len(raw) > payloadChunkHeaderBytes+MaxPlanPageBytes+checksumBytes || !magicOK(raw, payloadChunkMagic) || !zeroBytes(raw[4:8]) || !checksumOK(raw) {
		return PayloadChunkRecord{}, ErrCorrupt
	}
	n := binary.LittleEndian.Uint64(raw[40:48])
	want, ok := exactLength(payloadChunkHeaderBytes+checksumBytes, n)
	if !ok || want != len(raw) {
		return PayloadChunkRecord{}, ErrCorrupt
	}
	r := PayloadChunkRecord{Ordinal: binary.LittleEndian.Uint64(raw[8:16]), Count: binary.LittleEndian.Uint64(raw[16:24]), Offset: binary.LittleEndian.Uint64(raw[24:32]), TotalBytes: binary.LittleEndian.Uint64(raw[32:40]), KeyDigest: readDigest(raw[48:80]), PlanRoot: readDigest(raw[80:112]), BuildDigest: readDigest(raw[112:144]), ContentRoot: readDigest(raw[144:176]), PreviousChain: readDigest(raw[176:208]), Chain: readDigest(raw[208:240]), Data: raw[payloadChunkHeaderBytes : len(raw)-checksumBytes : len(raw)-checksumBytes]}
	if err := validatePayloadChunk(r); err != nil {
		return PayloadChunkRecord{}, ErrCorrupt
	}
	return r, nil
}

func validatePayloadBuild(r PayloadBuildRecord) error {
	if r.CommandEpoch == 0 {
		return ErrCorrupt
	}
	if !nonzeroDigest(r.KeyDigest) || !nonzeroDigest(r.RequestDigest) || !nonzeroDigest(r.PlanRoot) || !nonzeroDigest(r.ContentRoot) || r.BuildDigest != payloadBuildDigest(r) || r.Revision == 0 || r.TotalBytes == 0 || r.TotalBytes > MaxDynamicWavePayloadBytes || r.ChunkCount == 0 || r.ChunkCount > MaxDynamicWavePayloadChunks || r.ChunkCount != (r.TotalBytes+MaxPlanPageBytes-1)/MaxPlanPageBytes || r.NextChunkOrdinal > r.ChunkCount || r.StagedBytes > r.TotalBytes || (r.WaveOrdinal == 0) != !nonzeroDigest(r.PriorContinuationDigest) || r.Phase < PayloadBuildStaging || r.Phase > PayloadBuildSealed || (r.NextChunkOrdinal == 0) != !nonzeroDigest(r.Chain) || (r.Phase == PayloadBuildSealed && (r.NextChunkOrdinal != r.ChunkCount || r.StagedBytes != r.TotalBytes || r.Chain != r.ContentRoot)) {
		return ErrCorrupt
	}
	return nil
}

func validatePayloadChunk(r PayloadChunkRecord) error {
	if !nonzeroDigest(r.KeyDigest) || !nonzeroDigest(r.PlanRoot) || !nonzeroDigest(r.BuildDigest) || !nonzeroDigest(r.ContentRoot) || r.TotalBytes == 0 || r.TotalBytes > MaxDynamicWavePayloadBytes || r.Count == 0 || r.Count > MaxDynamicWavePayloadChunks || r.Count != (r.TotalBytes+MaxPlanPageBytes-1)/MaxPlanPageBytes || r.Ordinal >= r.Count || r.Offset != r.Ordinal*MaxPlanPageBytes || len(r.Data) == 0 || len(r.Data) > MaxPlanPageBytes || r.Offset >= r.TotalBytes || uint64(len(r.Data)) > r.TotalBytes-r.Offset || (r.Ordinal == 0) != !nonzeroDigest(r.PreviousChain) || (r.Ordinal+1 == r.Count) != (r.Offset+uint64(len(r.Data)) == r.TotalBytes) || r.Chain != PlanPageChain(r.KeyDigest, r.Ordinal, r.Count, r.Offset, r.TotalBytes, r.PreviousChain, r.Data) {
		return ErrCorrupt
	}
	return nil
}

func payloadBuildDigest(r PayloadBuildRecord) Digest {
	const domain = "vibedb/request-ledger/payload-build\x00"
	var framed [len(domain) + 5*sha256.Size + 32]byte
	at := copy(framed[:], payloadBuildDigestDomain)
	for _, d := range [...]Digest{r.KeyDigest, r.RequestDigest, r.PlanRoot, r.PriorContinuationDigest, r.ContentRoot} {
		at += copy(framed[at:], d[:])
	}
	binary.LittleEndian.PutUint64(framed[at:at+8], r.WaveOrdinal)
	binary.LittleEndian.PutUint64(framed[at+8:at+16], r.TotalBytes)
	binary.LittleEndian.PutUint64(framed[at+16:at+24], r.ChunkCount)
	binary.LittleEndian.PutUint64(framed[at+24:at+32], r.CommandEpoch)
	return Digest(sha256.Sum256(framed[:]))
}
