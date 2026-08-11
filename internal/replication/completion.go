package replication

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"unicode/utf8"
)

var completionMagic = [8]byte{'V', 'D', 'B', 'C', 'M', 'P', 0, 0}

var completionResultDomain = [...]byte{
	'v', 'i', 'b', 'e', 'd', 'b', '/', 'r', 'e', 'p', 'l', 'i', 'c', 'a', 't', 'i', 'o', 'n', '/',
	'c', 'o', 'm', 'p', 'l', 'e', 't', 'i', 'o', 'n', '-', 'r', 'e', 's', 'u', 'l', 't', '/', 'v', '1', 0,
}

// CompletionV1 is the exact retained outcome of one retryable command. The
// origin lineage remains attached when completion state moves to another range.
// ResultFormat is an application-owned, nonzero payload format identifier.
type CompletionV1 struct {
	ClusterID             ID128
	ClusterIncarnation    ID128
	TopologyRecoveryEpoch uint64

	Distribution           string
	Shard                  string
	AllocationGeneration   uint64
	ShardIncarnation       ID128
	GroupID                ID128
	ReplicaSetVersion      uint64
	ActivePolicyGeneration uint64
	ProtectionEpoch        uint64
	RoutingVersion         uint64
	RouteGeneration        uint64

	Tenant          []byte
	ClientID        ID128
	ClientEpoch     uint64
	ClientSequence  uint64
	Fingerprint     Digest
	RetryHome       RetryHome
	AppliedSequence uint64

	ResultCode   uint32
	ResultFormat uint16
	Storage      CompletionStorage
	ResultLength uint64
	ResultDigest Digest
	InlineResult []byte
}

// CompletionViewV1 is a borrowed checksum-, digest-, and semantics-validated
// completion. Tenant, Distribution, Shard, InlineResult, and Bytes alias the
// OpenCompletionV1 input. Copying or retaining the view retains the complete
// bounded input envelope; all borrowed slices are capacity-clamped and
// read-only.
type CompletionViewV1 struct {
	ClusterID              ID128
	ClusterIncarnation     ID128
	TopologyRecoveryEpoch  uint64
	Distribution           []byte
	Shard                  []byte
	AllocationGeneration   uint64
	ShardIncarnation       ID128
	GroupID                ID128
	ReplicaSetVersion      uint64
	ActivePolicyGeneration uint64
	ProtectionEpoch        uint64
	RoutingVersion         uint64
	RouteGeneration        uint64

	Tenant          []byte
	ClientID        ID128
	ClientEpoch     uint64
	ClientSequence  uint64
	Fingerprint     Digest
	RetryHome       RetryHome
	AppliedSequence uint64

	ResultCode   uint32
	ResultFormat uint16
	Storage      CompletionStorage
	ResultLength uint64
	ResultDigest Digest
	InlineResult []byte

	raw []byte
}

// Bytes returns the exact validated envelope, borrowing the decoder input.
func (v CompletionViewV1) Bytes() []byte {
	return v.raw[:len(v.raw):len(v.raw)]
}

// CompletionResultDigestV1 computes the frozen, domain-separated SHA-256
// digest of one exact application result.
func CompletionResultDigestV1(
	resultCode uint32,
	resultFormat uint16,
	result []byte,
) Digest {
	return completionResultDigestV1(resultCode, resultFormat, uint64(len(result)), result)
}

func completionResultDigestV1(
	resultCode uint32,
	resultFormat uint16,
	resultLength uint64,
	result []byte,
) Digest {
	var metadata [14]byte
	binary.LittleEndian.PutUint32(metadata[0:4], resultCode)
	binary.LittleEndian.PutUint16(metadata[4:6], resultFormat)
	binary.LittleEndian.PutUint64(metadata[6:14], resultLength)
	var digest Digest
	hasher := sha256.New()
	_, _ = hasher.Write(completionResultDomain[:])
	_, _ = hasher.Write(metadata[:])
	_, _ = hasher.Write(result)
	_ = hasher.Sum(digest[:0])
	return digest
}

// AppendCompletionV1 appends one canonical completion envelope. On validation
// failure dst is unchanged. With sufficient capacity it allocates zero. Input
// slices must not overlap the writable append region in dst's current backing
// array; such aliases are rejected before dst is modified.
func AppendCompletionV1(dst []byte, completion CompletionV1) ([]byte, error) {
	total, err := measureCompletionV1(completion)
	if err != nil {
		return dst, err
	}
	if completionOverlapsAppendRegion(dst, total, completion) {
		return dst, semantic("completion input aliases destination append region")
	}
	start := len(dst)
	dst = extendZeroed(dst, total)
	frame := dst[start:]
	copy(frame[0:8], completionMagic[:])
	appendU16(frame, 8, CompletionFormatV1)
	frame[10] = byte(completion.Storage)
	appendU16(frame, 12, completionHeaderBytes)
	appendU16(frame, 14, completion.ResultFormat)
	appendU32(frame, 16, uint32(total))
	appendU32(frame, 20, uint32(len(completion.InlineResult)))
	appendU32(frame, 24, completion.ResultCode)
	appendU32(frame, 28, uint32(total-completionHeaderBytes-envelopeChecksumBytes))
	copy(frame[32:48], completion.ClusterID[:])
	copy(frame[48:64], completion.ClusterIncarnation[:])
	appendU64(frame, 64, completion.TopologyRecoveryEpoch)
	copy(frame[72:88], completion.ShardIncarnation[:])
	copy(frame[88:104], completion.GroupID[:])
	appendU64(frame, 104, completion.AllocationGeneration)
	appendU64(frame, 112, completion.ReplicaSetVersion)
	appendU64(frame, 120, completion.ActivePolicyGeneration)
	appendU64(frame, 128, completion.ProtectionEpoch)
	appendU64(frame, 136, completion.RoutingVersion)
	appendU64(frame, 144, completion.RouteGeneration)
	copy(frame[152:168], completion.ClientID[:])
	appendU64(frame, 168, completion.ClientEpoch)
	appendU64(frame, 176, completion.ClientSequence)
	appendU64(frame, 184, completion.AppliedSequence)
	copy(frame[192:224], completion.Fingerprint[:])
	copy(frame[224:256], completion.ResultDigest[:])
	copy(frame[256:264], completion.RetryHome[:])
	appendU64(frame, 264, completion.ResultLength)
	appendU16(frame, 272, uint16(len(completion.Tenant)))
	appendU16(frame, 274, uint16(len(completion.Distribution)))
	appendU16(frame, 276, uint16(len(completion.Shard)))

	cursor := completionHeaderBytes
	cursor += copy(frame[cursor:], completion.Tenant)
	cursor += copy(frame[cursor:], completion.Distribution)
	cursor += copy(frame[cursor:], completion.Shard)
	cursor += copy(frame[cursor:], completion.InlineResult)
	if cursor != total-envelopeChecksumBytes {
		panic("replication: measured completion size diverged during encode")
	}
	sealEnvelope(frame)
	return dst, nil
}

func completionOverlapsAppendRegion(dst []byte, total int, completion CompletionV1) bool {
	region := writableAppendRegion(dst, total)
	return len(region) != 0 && (byteSlicesOverlap(region, completion.Tenant) ||
		byteSliceStringOverlap(region, completion.Distribution) ||
		byteSliceStringOverlap(region, completion.Shard) ||
		byteSlicesOverlap(region, completion.InlineResult))
}

func measureCompletionV1(completion CompletionV1) (int, error) {
	if err := validateCompletionV1(completion); err != nil {
		return 0, err
	}
	total := uint64(completionHeaderBytes + envelopeChecksumBytes)
	for _, size := range [...]int{
		len(completion.Tenant), len(completion.Distribution),
		len(completion.Shard), len(completion.InlineResult),
	} {
		var ok bool
		total, ok = checkedAdd(total, uint64(size), MaxCompletionEnvelopeBytes)
		if !ok {
			return 0, ErrEnvelopeTooLarge
		}
	}
	return int(total), nil
}

func validateCompletionV1(completion CompletionV1) error {
	if !nonzero128(completion.ClusterID) || !nonzero128(completion.ClusterIncarnation) ||
		!nonzero128(completion.ShardIncarnation) || !nonzero128(completion.GroupID) ||
		!nonzero128(completion.ClientID) || !nonzeroDigest(completion.Fingerprint) ||
		!nonzeroDigest(completion.ResultDigest) {
		return semantic("completion contains a zero identity or digest")
	}
	if completion.TopologyRecoveryEpoch == 0 || completion.AllocationGeneration == 0 ||
		completion.ReplicaSetVersion == 0 || completion.ActivePolicyGeneration == 0 ||
		completion.ProtectionEpoch == 0 || completion.RoutingVersion == 0 ||
		completion.RouteGeneration == 0 || completion.ClientEpoch == 0 ||
		completion.ClientSequence == 0 || completion.AppliedSequence == 0 {
		return semantic("completion contains a zero generation, epoch, or sequence")
	}
	if completion.ResultFormat == 0 {
		return semantic("completion result format is zero")
	}
	if len(completion.Tenant) == 0 || len(completion.Tenant) > MaxIdentityBytes {
		return semantic("tenant identity length")
	}
	if err := validateTextIdentity("distribution", completion.Distribution, MaxIdentityBytes); err != nil {
		return err
	}
	if err := validateTextIdentity("shard", completion.Shard, MaxIdentityBytes); err != nil {
		return err
	}
	if completion.ResultLength > MaxCompletionResultBytes {
		return ErrEnvelopeTooLarge
	}
	switch completion.Storage {
	case CompletionInline:
		if completion.ResultLength > MaxInlineCompletionBytes ||
			completion.ResultLength != uint64(len(completion.InlineResult)) {
			return semantic("inline completion length")
		}
		want := completionResultDigestV1(
			completion.ResultCode, completion.ResultFormat,
			completion.ResultLength, completion.InlineResult,
		)
		if completion.ResultDigest != want {
			return semantic("inline completion result digest")
		}
	case CompletionDigestReference:
		if completion.ResultLength <= MaxInlineCompletionBytes ||
			len(completion.InlineResult) != 0 {
			return semantic("referenced completion length or inline bytes")
		}
	default:
		return semantic("unknown completion storage kind")
	}
	return nil
}

// OpenCompletionV1 validates one exact completion and returns a borrowed view.
func OpenCompletionV1(src []byte) (CompletionViewV1, error) {
	if len(src) < completionHeaderBytes+envelopeChecksumBytes {
		return CompletionViewV1{}, corrupt("short completion")
	}
	if len(src) > MaxCompletionEnvelopeBytes {
		return CompletionViewV1{}, ErrEnvelopeTooLarge
	}
	if !bytes.Equal(src[:8], completionMagic[:]) {
		return CompletionViewV1{}, corrupt("completion magic")
	}
	version := binary.LittleEndian.Uint16(src[8:10])
	if version != CompletionFormatV1 {
		return CompletionViewV1{}, unsupported("completion", version)
	}
	if binary.LittleEndian.Uint16(src[12:14]) != completionHeaderBytes {
		return CompletionViewV1{}, corrupt("completion header size")
	}
	total := binary.LittleEndian.Uint32(src[16:20])
	bodyBytes := binary.LittleEndian.Uint32(src[28:32])
	if uint64(total) != uint64(len(src)) ||
		uint64(bodyBytes)+completionHeaderBytes+envelopeChecksumBytes != uint64(total) {
		return CompletionViewV1{}, corrupt("completion total or body length")
	}
	if err := verifyEnvelopeChecksum(src); err != nil {
		return CompletionViewV1{}, err
	}
	if src[11] != 0 || !allZero(src[278:288]) {
		return CompletionViewV1{}, semantic("completion flags or reserved bytes")
	}

	var view CompletionViewV1
	view.Storage = CompletionStorage(src[10])
	view.ResultFormat = binary.LittleEndian.Uint16(src[14:16])
	inlineBytes := int(binary.LittleEndian.Uint32(src[20:24]))
	view.ResultCode = binary.LittleEndian.Uint32(src[24:28])
	copy(view.ClusterID[:], src[32:48])
	copy(view.ClusterIncarnation[:], src[48:64])
	view.TopologyRecoveryEpoch = binary.LittleEndian.Uint64(src[64:72])
	copy(view.ShardIncarnation[:], src[72:88])
	copy(view.GroupID[:], src[88:104])
	view.AllocationGeneration = binary.LittleEndian.Uint64(src[104:112])
	view.ReplicaSetVersion = binary.LittleEndian.Uint64(src[112:120])
	view.ActivePolicyGeneration = binary.LittleEndian.Uint64(src[120:128])
	view.ProtectionEpoch = binary.LittleEndian.Uint64(src[128:136])
	view.RoutingVersion = binary.LittleEndian.Uint64(src[136:144])
	view.RouteGeneration = binary.LittleEndian.Uint64(src[144:152])
	copy(view.ClientID[:], src[152:168])
	view.ClientEpoch = binary.LittleEndian.Uint64(src[168:176])
	view.ClientSequence = binary.LittleEndian.Uint64(src[176:184])
	view.AppliedSequence = binary.LittleEndian.Uint64(src[184:192])
	copy(view.Fingerprint[:], src[192:224])
	copy(view.ResultDigest[:], src[224:256])
	copy(view.RetryHome[:], src[256:264])
	view.ResultLength = binary.LittleEndian.Uint64(src[264:272])

	tenantLen := int(binary.LittleEndian.Uint16(src[272:274]))
	distributionLen := int(binary.LittleEndian.Uint16(src[274:276]))
	shardLen := int(binary.LittleEndian.Uint16(src[276:278]))
	identityBytes := tenantLen + distributionLen + shardLen
	bodyEnd := len(src) - envelopeChecksumBytes
	if tenantLen == 0 || tenantLen > MaxIdentityBytes ||
		distributionLen == 0 || distributionLen > MaxIdentityBytes ||
		shardLen == 0 || shardLen > MaxIdentityBytes || inlineBytes < 0 ||
		identityBytes < 0 || identityBytes > bodyEnd-completionHeaderBytes ||
		inlineBytes != bodyEnd-completionHeaderBytes-identityBytes {
		return CompletionViewV1{}, semantic("completion body lengths")
	}
	cursor := completionHeaderBytes
	end := cursor + tenantLen
	view.Tenant = src[cursor:end:end]
	cursor = end
	end = cursor + distributionLen
	view.Distribution = src[cursor:end:end]
	cursor = end
	end = cursor + shardLen
	view.Shard = src[cursor:end:end]
	cursor = end
	view.InlineResult = src[cursor:bodyEnd:bodyEnd]
	if !utf8.Valid(view.Distribution) || !utf8.Valid(view.Shard) {
		return CompletionViewV1{}, semantic("completion text identity is not valid UTF-8")
	}
	if err := validateCompletionViewV1(view); err != nil {
		return CompletionViewV1{}, err
	}
	view.raw = src[:len(src):len(src)]
	return view, nil
}

func validateCompletionViewV1(view CompletionViewV1) error {
	if !nonzero128(view.ClusterID) || !nonzero128(view.ClusterIncarnation) ||
		!nonzero128(view.ShardIncarnation) || !nonzero128(view.GroupID) ||
		!nonzero128(view.ClientID) || !nonzeroDigest(view.Fingerprint) ||
		!nonzeroDigest(view.ResultDigest) {
		return semantic("completion contains a zero identity or digest")
	}
	if view.TopologyRecoveryEpoch == 0 || view.AllocationGeneration == 0 ||
		view.ReplicaSetVersion == 0 || view.ActivePolicyGeneration == 0 ||
		view.ProtectionEpoch == 0 || view.RoutingVersion == 0 ||
		view.RouteGeneration == 0 || view.ClientEpoch == 0 ||
		view.ClientSequence == 0 || view.AppliedSequence == 0 ||
		view.ResultFormat == 0 || view.ResultLength > MaxCompletionResultBytes {
		return semantic("completion contains an invalid scalar")
	}
	switch view.Storage {
	case CompletionInline:
		if view.ResultLength > MaxInlineCompletionBytes ||
			view.ResultLength != uint64(len(view.InlineResult)) {
			return semantic("inline completion length")
		}
		want := completionResultDigestV1(
			view.ResultCode, view.ResultFormat, view.ResultLength, view.InlineResult,
		)
		if view.ResultDigest != want {
			return semantic("inline completion result digest")
		}
	case CompletionDigestReference:
		if view.ResultLength <= MaxInlineCompletionBytes || len(view.InlineResult) != 0 {
			return semantic("referenced completion length or inline bytes")
		}
	default:
		return semantic("unknown completion storage kind")
	}
	return nil
}
