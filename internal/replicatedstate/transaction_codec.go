package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
)

const (
	transactionCodecSentinel = uint16(0)

	transactionControlHeaderBytes     = 360
	transactionPayloadHeaderBytes     = 96
	transactionManifestHeaderBytes    = 112
	transactionMutationHeaderBytes    = 160
	transactionIntentHeaderBytes      = 80
	transactionControlStorageKeyBytes = 18
	transactionPayloadStorageKeyBytes = 17
	transactionManifestKeyBytes       = 21
	transactionMutationKeyBytes       = 23
	transactionIntentKeyBytes         = 35

	MaxTransactionControlRecordBytes = transactionControlHeaderBytes +
		distributedtxn.MaxIntentScopes*8 + recordChecksumLen
	MaxTransactionCoordinatorPayloadRecordBytes = transactionPayloadHeaderBytes +
		distributedtxn.MaxCoordinatorRecordBytes + recordChecksumLen
	MaxTransactionManifestPageRecordBytes = transactionManifestHeaderBytes +
		distributedtxn.ManifestSegmentBytes + recordChecksumLen
	MaxTransactionNativeMutationRecordBytes = transactionMutationHeaderBytes +
		replication.MaxMutationKeyBytes + replication.MaxMutationValueBytes + recordChecksumLen
	MaxTransactionIntentRecordBytes = transactionIntentHeaderBytes +
		replication.MaxMutationKeyBytes + recordChecksumLen

	// MaxTransactionResidentBytes is a per-transaction byte admission ceiling,
	// not a target or mutation-count contract. It includes exact encoded
	// system keys and values retained by the shard state machine.
	MaxTransactionResidentBytes = 128 << 20
)

const (
	transactionControlPrefix byte = 0x10 + iota
	transactionPayloadPrefix
	transactionManifestPrefix
	transactionMutationPrefix
	transactionIntentPrefix
)

var (
	ErrTransactionStateCorrupt = errors.New("replicatedstate: corrupt transaction state")

	transactionControlMagic  = [8]byte{'V', 'D', 'B', 'T', 'C', 'T', 'L', 0}
	transactionPayloadMagic  = [8]byte{'V', 'D', 'B', 'T', 'P', 'A', 'Y', 0}
	transactionManifestMagic = [8]byte{'V', 'D', 'B', 'T', 'P', 'A', 'G', 0}
	transactionMutationMagic = [8]byte{'V', 'D', 'B', 'T', 'M', 'U', 'T', 0}
	transactionIntentMagic   = [8]byte{'V', 'D', 'B', 'T', 'I', 'N', 'T', 0}

	transactionControlChecksumDomain = []byte(
		"vibedb/replicated-state/transaction-control-checksum\x00",
	)
	transactionPayloadChecksumDomain = []byte(
		"vibedb/replicated-state/transaction-payload-checksum\x00",
	)
	transactionManifestChecksumDomain = []byte(
		"vibedb/replicated-state/transaction-manifest-checksum\x00",
	)
	transactionMutationChecksumDomain = []byte(
		"vibedb/replicated-state/transaction-mutation-checksum\x00",
	)
	transactionMutationDigestDomain = []byte(
		"vibedb/replicated-state/transaction-mutation-digest\x00",
	)
	transactionIntentChecksumDomain = []byte(
		"vibedb/replicated-state/transaction-intent-checksum\x00",
	)
	transactionManifestChainDomain = [8]byte{'V', 'T', 'M', 'C', 'H', 'N', '1', 0}
	transactionManifestRootDomain  = [8]byte{'V', 'T', 'M', 'R', 'O', 'T', '1', 0}
	transactionManifestCRC         = crc32.MakeTable(crc32.Castagnoli)
)

// TransactionControl is the compact durable authority for one coordinator or
// target. Creation identity survives retirement/release so delayed stage
// retries cannot become new. Last* retains the exact applied transition needed
// to settle a response-lost retry after the ordinary session ring is gone.
type TransactionControl struct {
	ID    distributedtxn.ID
	Role  distributedtxn.ReplicatedRole
	State uint8

	Revision           uint64
	ControllerEpoch    uint64
	ExecutionPinDigest distributedtxn.Digest
	PayloadKind        distributedtxn.ReplicatedPayloadKind
	PayloadDigest      distributedtxn.Digest
	PayloadBytes       uint64
	PayloadCount       uint64
	// PayloadRelationCount is zero for coordinators and the exact number of
	// packed native relation rows for a target stage.
	PayloadRelationCount uint16

	CoordinatorGroup            replication.ID128
	CoordinatorShardIncarnation replication.ID128
	CoordinatorAllocation       uint64
	MutationDigest              distributedtxn.Digest
	BucketBits                  uint8
	// RecoveryPulse role-overlays BucketBits for coordinators. It is advanced
	// only by replicated recovery-pulse commands and never by local time.
	RecoveryPulse uint8
	IntentScopes  []distributedtxn.IntentScope

	AffectedRows      int64
	AffectedRowsValid bool
	// CoordinatorTargetOrdinal occupies the same durable scalar as
	// AffectedRows. Active coordinators never publish affected rows, while
	// targets never own a coordinator ordinal, so the role overlay keeps
	// the fixed control record compact.
	CoordinatorTargetOrdinal uint64
	// TargetOrdinal reuses the affected-row scalar only for compact
	// cancellation witnesses. It binds a missing target's exact manifest
	// position without growing every durable control.
	TargetOrdinal uint32
	// PrepareResultCode is the immutable vote produced by an atomic
	// stage+prepare. Zero denotes a legacy split-stage control which has not yet
	// recorded a vote; the durable nonzero values are ResultApplied,
	// ResultIndexConflict, and ResultWrongShard.
	PrepareResultCode uint32
	// FusedPath permanently distinguishes controls created by atomic prepare
	// operations from legacy split controls. The bit survives finish/retire so
	// historical retries cannot reinterpret one protocol as the other.
	FusedPath bool
	// CancellationWitness is the compact abort fence for a target whose
	// stage+prepare outcome was never observed. It carries no mutation image or
	// vote and permanently prevents a delayed creation command from installing
	// intents under the same transaction identity.
	CancellationWitness bool
	// CoordinatorDecision is invalid while staging, matches the live terminal
	// decision, and survives Retired so delayed Commit/Abort retries remain
	// distinguishable after child-row reclamation.
	CoordinatorDecision distributedtxn.CoordinatorState

	ResidentControlBytes  uint64
	ResidentPayloadBytes  uint64
	ResidentManifestBytes uint64
	ResidentMutationBytes uint64
	ResidentIntentBytes   uint64

	LastOperation        distributedtxn.ReplicatedOperation
	LastExpectedRevision uint64
	LastCommandDigest    replication.Digest
	LastResultCode       uint32
	LastAppliedIndex     uint64

	// Manifest* is the O(1) incremental seal witness for a segmented
	// coordinator. NextPage and NextTarget identify the only canonical
	// successor; EncodedBytes and ChainDigest bind every accepted page without
	// rescanning retained page rows. The tuple is zero before the first page and
	// for every non-manifest transaction.
	ManifestNextPage     uint32
	ManifestNextTarget   uint64
	ManifestEncodedBytes uint64
	ManifestChainDigest  distributedtxn.Digest
	// PrepareCommandDigest role-overlays ManifestChainDigest for targets.
	// Target controls never own manifest progress; retaining the original
	// prepare command digest makes response-lost vote lookup exact even after a
	// finish operation overwrites LastCommandDigest.
	PrepareCommandDigest replication.Digest
}

// TransactionControlView borrows the encoded row and uses caller-owned scope
// storage. Bytes and all borrowed slices are capacity-clamped.
type TransactionControlView struct {
	TransactionControl
	raw []byte
}

func (view TransactionControlView) Bytes() []byte {
	return view.raw[:len(view.raw):len(view.raw)]
}

func (view TransactionControlView) StorageKey() ([transactionControlStorageKeyBytes]byte, error) {
	return TransactionControlStorageKey(view.Role, view.ID)
}

func TransactionControlStorageKey(
	role distributedtxn.ReplicatedRole,
	id distributedtxn.ID,
) ([transactionControlStorageKeyBytes]byte, error) {
	var key [transactionControlStorageKeyBytes]byte
	if !transactionRoleValid(role) || id.IsZero() {
		return key, ErrTransactionStateCorrupt
	}
	key[0] = transactionControlPrefix
	key[1] = byte(role)
	copy(key[2:], id[:])
	return key, nil
}

func TransactionCoordinatorPayloadStorageKey(
	id distributedtxn.ID,
) ([transactionPayloadStorageKeyBytes]byte, error) {
	var key [transactionPayloadStorageKeyBytes]byte
	if id.IsZero() {
		return key, ErrTransactionStateCorrupt
	}
	key[0] = transactionPayloadPrefix
	copy(key[1:], id[:])
	return key, nil
}

func TransactionManifestPageStorageKey(
	id distributedtxn.ID,
	index uint32,
) ([transactionManifestKeyBytes]byte, error) {
	var key [transactionManifestKeyBytes]byte
	if id.IsZero() {
		return key, ErrTransactionStateCorrupt
	}
	key[0] = transactionManifestPrefix
	copy(key[1:17], id[:])
	binary.BigEndian.PutUint32(key[17:21], index)
	return key, nil
}

func TransactionNativeMutationStorageKey(
	id distributedtxn.ID,
	relation replication.RelationID,
	ordinal uint32,
) ([transactionMutationKeyBytes]byte, error) {
	var key [transactionMutationKeyBytes]byte
	if id.IsZero() || !transactionRelationValid(relation) || ordinal >= replication.MaxMutations {
		return key, ErrTransactionStateCorrupt
	}
	key[0] = transactionMutationPrefix
	copy(key[1:17], id[:])
	binary.BigEndian.PutUint16(key[17:19], uint16(relation))
	binary.BigEndian.PutUint32(key[19:23], ordinal)
	return key, nil
}

// TransactionIntentStorageKey is {intent-prefix, relation, SHA-256(raw-key)}.
// The corresponding value repeats the digest and raw key, so a hash collision
// is detected before an intent can block or authorize another key.
func TransactionIntentStorageKey(
	relation replication.RelationID,
	rawKey []byte,
) ([transactionIntentKeyBytes]byte, error) {
	var key [transactionIntentKeyBytes]byte
	if !transactionRelationValid(relation) || len(rawKey) == 0 ||
		len(rawKey) > replication.MaxMutationKeyBytes {
		return key, ErrTransactionStateCorrupt
	}
	key[0] = transactionIntentPrefix
	binary.BigEndian.PutUint16(key[1:3], uint16(relation))
	digest := sha256.Sum256(rawKey)
	copy(key[3:], digest[:])
	return key, nil
}

func TransactionControlResidentBytes(scopeCount int) (uint64, error) {
	if scopeCount < 0 || scopeCount > distributedtxn.MaxIntentScopes {
		return 0, ErrTransactionStateCorrupt
	}
	return transactionControlStorageKeyBytes + transactionControlHeaderBytes +
		uint64(scopeCount*8+recordChecksumLen), nil
}

func TransactionCoordinatorPayloadResidentBytes(payloadBytes int) (uint64, error) {
	if payloadBytes <= 0 || payloadBytes > distributedtxn.MaxCoordinatorRecordBytes {
		return 0, ErrTransactionStateCorrupt
	}
	return transactionPayloadStorageKeyBytes + transactionPayloadHeaderBytes +
		uint64(payloadBytes+recordChecksumLen), nil
}

func TransactionManifestPageResidentBytes(rawBytes int) (uint64, error) {
	if rawBytes <= 0 || rawBytes > distributedtxn.ManifestSegmentBytes {
		return 0, ErrTransactionStateCorrupt
	}
	return transactionManifestKeyBytes + transactionManifestHeaderBytes +
		uint64(rawBytes+recordChecksumLen), nil
}

func TransactionNativeMutationResidentBytes(mutation replication.Mutation) (uint64, error) {
	if !transactionMutationValid(mutation) {
		return 0, ErrTransactionStateCorrupt
	}
	return transactionMutationKeyBytes + transactionMutationHeaderBytes +
		uint64(len(mutation.Key)+len(mutation.Value)+recordChecksumLen), nil
}

func TransactionIntentResidentBytes(rawKeyBytes int) (uint64, error) {
	if rawKeyBytes <= 0 || rawKeyBytes > replication.MaxMutationKeyBytes {
		return 0, ErrTransactionStateCorrupt
	}
	return transactionIntentKeyBytes + transactionIntentHeaderBytes +
		uint64(rawKeyBytes+recordChecksumLen), nil
}

func AppendTransactionControl(dst []byte, control TransactionControl) ([]byte, error) {
	if !transactionControlValid(control) {
		return dst, ErrTransactionStateCorrupt
	}
	total := transactionControlHeaderBytes + len(control.IntentScopes)*8 + recordChecksumLen
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	frame := dst[start:]
	copy(frame[0:8], transactionControlMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], transactionCodecSentinel)
	frame[10] = byte(control.Role)
	frame[11] = control.State
	frame[12] = byte(control.PayloadKind)
	frame[13] = byte(control.LastOperation)
	if control.Role == distributedtxn.ReplicatedRoleCoordinator {
		frame[14] = control.RecoveryPulse
	} else {
		frame[14] = control.BucketBits
	}
	frame[15] = byte(control.CoordinatorDecision) << 1
	if control.AffectedRowsValid {
		frame[15] |= 1
	}
	if control.FusedPath {
		frame[15] |= 1 << 6
	}
	if control.CancellationWitness {
		frame[15] |= 1 << 7
	}
	switch control.PrepareResultCode {
	case 0:
	case ResultApplied:
		frame[15] |= 1 << 4
	case ResultIndexConflict:
		frame[15] |= 2 << 4
	case ResultWrongShard:
		frame[15] |= 3 << 4
	default:
		return dst[:start], ErrTransactionStateCorrupt
	}
	binary.LittleEndian.PutUint16(frame[16:18], transactionControlHeaderBytes)
	binary.LittleEndian.PutUint16(frame[18:20], uint16(len(control.IntentScopes)))
	binary.LittleEndian.PutUint32(frame[20:24], uint32(total))
	copy(frame[24:40], control.ID[:])
	binary.LittleEndian.PutUint64(frame[40:48], control.Revision)
	copy(frame[48:80], control.PayloadDigest[:])
	binary.LittleEndian.PutUint64(frame[80:88], control.PayloadBytes)
	// PayloadCount is admission-bounded below 2^48; pack the relation-row count
	// into the otherwise unreachable high 16 bits without growing every control.
	binary.LittleEndian.PutUint64(
		frame[88:96], control.PayloadCount|uint64(control.PayloadRelationCount)<<48,
	)
	copy(frame[96:112], control.CoordinatorGroup[:])
	copy(frame[112:128], control.CoordinatorShardIncarnation[:])
	binary.LittleEndian.PutUint64(frame[128:136], control.CoordinatorAllocation)
	copy(frame[136:168], control.MutationDigest[:])
	if control.Role == distributedtxn.ReplicatedRoleCoordinator &&
		distributedtxn.CoordinatorState(control.State) != distributedtxn.CoordinatorRetired {
		binary.LittleEndian.PutUint64(frame[168:176], control.CoordinatorTargetOrdinal)
	} else if control.CancellationWitness {
		binary.LittleEndian.PutUint64(frame[168:176], uint64(control.TargetOrdinal))
	} else {
		binary.LittleEndian.PutUint64(frame[168:176], uint64(control.AffectedRows))
	}
	binary.LittleEndian.PutUint64(frame[176:184], control.ResidentControlBytes)
	binary.LittleEndian.PutUint64(frame[184:192], control.ResidentPayloadBytes)
	binary.LittleEndian.PutUint64(frame[192:200], control.ResidentManifestBytes)
	binary.LittleEndian.PutUint64(frame[200:208], control.ResidentMutationBytes)
	binary.LittleEndian.PutUint64(frame[208:216], control.ResidentIntentBytes)
	binary.LittleEndian.PutUint64(frame[216:224], control.LastExpectedRevision)
	binary.LittleEndian.PutUint64(frame[224:232], control.LastAppliedIndex)
	copy(frame[232:264], control.LastCommandDigest[:])
	binary.LittleEndian.PutUint32(frame[264:268], control.LastResultCode)
	binary.LittleEndian.PutUint32(frame[268:272], control.ManifestNextPage)
	binary.LittleEndian.PutUint64(frame[272:280], control.ManifestNextTarget)
	binary.LittleEndian.PutUint64(frame[280:288], control.ManifestEncodedBytes)
	if control.Role == distributedtxn.ReplicatedRoleCoordinator {
		copy(frame[288:320], control.ManifestChainDigest[:])
	} else {
		copy(frame[288:320], control.PrepareCommandDigest[:])
	}
	binary.LittleEndian.PutUint64(frame[320:328], control.ControllerEpoch)
	copy(frame[328:360], control.ExecutionPinDigest[:])
	cursor := transactionControlHeaderBytes
	for _, scope := range control.IntentScopes {
		binary.LittleEndian.PutUint32(frame[cursor:cursor+4], scope.Start)
		binary.LittleEndian.PutUint32(frame[cursor+4:cursor+8], scope.End)
		cursor += 8
	}
	sealRecord(frame, transactionControlChecksumDomain)
	return dst, nil
}

func OpenTransactionControl(src []byte) (TransactionControlView, error) {
	if len(src) < transactionControlHeaderBytes+recordChecksumLen {
		return TransactionControlView{}, ErrTransactionStateCorrupt
	}
	count := int(binary.LittleEndian.Uint16(src[18:20]))
	if count > distributedtxn.MaxIntentScopes {
		return TransactionControlView{}, ErrTransactionStateCorrupt
	}
	return OpenTransactionControlInto(src, make([]distributedtxn.IntentScope, count))
}

func OpenTransactionControlInto(
	src []byte,
	scopeScratch []distributedtxn.IntentScope,
) (TransactionControlView, error) {
	if len(src) < transactionControlHeaderBytes+recordChecksumLen ||
		len(src) > MaxTransactionControlRecordBytes ||
		!bytes.Equal(src[0:8], transactionControlMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != transactionCodecSentinel ||
		binary.LittleEndian.Uint16(src[16:18]) != transactionControlHeaderBytes ||
		binary.LittleEndian.Uint32(src[20:24]) != uint32(len(src)) ||
		!verifyRecord(src, transactionControlChecksumDomain) {
		return TransactionControlView{}, ErrTransactionStateCorrupt
	}
	count := int(binary.LittleEndian.Uint16(src[18:20]))
	if count > distributedtxn.MaxIntentScopes || cap(scopeScratch) < count ||
		transactionControlHeaderBytes+count*8+recordChecksumLen != len(src) {
		return TransactionControlView{}, ErrTransactionStateCorrupt
	}
	view := TransactionControlView{raw: src[:len(src):len(src)]}
	view.Role = distributedtxn.ReplicatedRole(src[10])
	view.State = src[11]
	view.PayloadKind = distributedtxn.ReplicatedPayloadKind(src[12])
	view.LastOperation = distributedtxn.ReplicatedOperation(src[13])
	if view.Role == distributedtxn.ReplicatedRoleCoordinator {
		view.RecoveryPulse = src[14]
	} else {
		view.BucketBits = src[14]
	}
	view.AffectedRowsValid = src[15]&1 != 0
	view.FusedPath = src[15]&(1<<6) != 0
	view.CancellationWitness = src[15]&(1<<7) != 0
	view.CoordinatorDecision = distributedtxn.CoordinatorState((src[15] >> 1) & 7)
	switch (src[15] >> 4) & 3 {
	case 0:
	case 1:
		view.PrepareResultCode = ResultApplied
	case 2:
		view.PrepareResultCode = ResultIndexConflict
	case 3:
		view.PrepareResultCode = ResultWrongShard
	}
	copy(view.ID[:], src[24:40])
	view.Revision = binary.LittleEndian.Uint64(src[40:48])
	copy(view.PayloadDigest[:], src[48:80])
	view.PayloadBytes = binary.LittleEndian.Uint64(src[80:88])
	packedPayloadCount := binary.LittleEndian.Uint64(src[88:96])
	view.PayloadCount = packedPayloadCount & (uint64(1)<<48 - 1)
	view.PayloadRelationCount = uint16(packedPayloadCount >> 48)
	copy(view.CoordinatorGroup[:], src[96:112])
	copy(view.CoordinatorShardIncarnation[:], src[112:128])
	view.CoordinatorAllocation = binary.LittleEndian.Uint64(src[128:136])
	copy(view.MutationDigest[:], src[136:168])
	if view.Role == distributedtxn.ReplicatedRoleCoordinator &&
		distributedtxn.CoordinatorState(view.State) != distributedtxn.CoordinatorRetired {
		view.CoordinatorTargetOrdinal = binary.LittleEndian.Uint64(src[168:176])
	} else if view.CancellationWitness {
		ordinal := binary.LittleEndian.Uint64(src[168:176])
		if ordinal > math.MaxUint32 {
			return TransactionControlView{}, ErrTransactionStateCorrupt
		}
		view.TargetOrdinal = uint32(ordinal)
	} else {
		view.AffectedRows = int64(binary.LittleEndian.Uint64(src[168:176]))
	}
	view.ResidentControlBytes = binary.LittleEndian.Uint64(src[176:184])
	view.ResidentPayloadBytes = binary.LittleEndian.Uint64(src[184:192])
	view.ResidentManifestBytes = binary.LittleEndian.Uint64(src[192:200])
	view.ResidentMutationBytes = binary.LittleEndian.Uint64(src[200:208])
	view.ResidentIntentBytes = binary.LittleEndian.Uint64(src[208:216])
	view.LastExpectedRevision = binary.LittleEndian.Uint64(src[216:224])
	view.LastAppliedIndex = binary.LittleEndian.Uint64(src[224:232])
	copy(view.LastCommandDigest[:], src[232:264])
	view.LastResultCode = binary.LittleEndian.Uint32(src[264:268])
	view.ManifestNextPage = binary.LittleEndian.Uint32(src[268:272])
	view.ManifestNextTarget = binary.LittleEndian.Uint64(src[272:280])
	view.ManifestEncodedBytes = binary.LittleEndian.Uint64(src[280:288])
	view.ControllerEpoch = binary.LittleEndian.Uint64(src[320:328])
	copy(view.ExecutionPinDigest[:], src[328:360])
	if view.Role == distributedtxn.ReplicatedRoleCoordinator {
		copy(view.ManifestChainDigest[:], src[288:320])
	} else {
		copy(view.PrepareCommandDigest[:], src[288:320])
	}
	if count != 0 {
		view.IntentScopes = scopeScratch[:count:count]
		cursor := transactionControlHeaderBytes
		for index := range view.IntentScopes {
			view.IntentScopes[index] = distributedtxn.IntentScope{
				Start: binary.LittleEndian.Uint32(src[cursor : cursor+4]),
				End:   binary.LittleEndian.Uint32(src[cursor+4 : cursor+8]),
			}
			cursor += 8
		}
	}
	if !transactionControlValid(view.TransactionControl) {
		return TransactionControlView{}, ErrTransactionStateCorrupt
	}
	return view, nil
}

// TransactionCoordinatorPayloadView is one borrowed inline VTC1 or segmented
// VTCM coordinator creation payload.
type TransactionCoordinatorPayloadView struct {
	ID      distributedtxn.ID
	Kind    distributedtxn.ReplicatedPayloadKind
	Digest  distributedtxn.Digest
	Payload []byte
	raw     []byte
}

func (view TransactionCoordinatorPayloadView) Bytes() []byte {
	return view.raw[:len(view.raw):len(view.raw)]
}

func (view TransactionCoordinatorPayloadView) StorageKey() ([transactionPayloadStorageKeyBytes]byte, error) {
	return TransactionCoordinatorPayloadStorageKey(view.ID)
}

func AppendTransactionCoordinatorPayload(
	dst []byte,
	id distributedtxn.ID,
	kind distributedtxn.ReplicatedPayloadKind,
	payload []byte,
) ([]byte, error) {
	if !transactionCoordinatorPayloadValid(id, kind, payload) {
		return dst, ErrTransactionStateCorrupt
	}
	total := transactionPayloadHeaderBytes + len(payload) + recordChecksumLen
	if byteSlicesOverlap(writableAppendRegion(dst, total), payload) {
		return dst, ErrCodecAlias
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	frame := dst[start:]
	copy(frame[0:8], transactionPayloadMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], transactionCodecSentinel)
	frame[10] = byte(kind)
	binary.LittleEndian.PutUint16(frame[12:14], transactionPayloadHeaderBytes)
	binary.LittleEndian.PutUint32(frame[16:20], uint32(total))
	binary.LittleEndian.PutUint32(frame[20:24], uint32(len(payload)))
	copy(frame[24:40], id[:])
	digest := sha256.Sum256(payload)
	copy(frame[40:72], digest[:])
	copy(frame[transactionPayloadHeaderBytes:], payload)
	sealRecord(frame, transactionPayloadChecksumDomain)
	return dst, nil
}

func OpenTransactionCoordinatorPayload(src []byte) (TransactionCoordinatorPayloadView, error) {
	if len(src) < transactionPayloadHeaderBytes+1+recordChecksumLen ||
		len(src) > MaxTransactionCoordinatorPayloadRecordBytes ||
		!bytes.Equal(src[0:8], transactionPayloadMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != transactionCodecSentinel || src[11] != 0 ||
		binary.LittleEndian.Uint16(src[12:14]) != transactionPayloadHeaderBytes ||
		binary.LittleEndian.Uint16(src[14:16]) != 0 ||
		binary.LittleEndian.Uint32(src[16:20]) != uint32(len(src)) ||
		!allZero(src[72:transactionPayloadHeaderBytes]) ||
		!verifyRecord(src, transactionPayloadChecksumDomain) {
		return TransactionCoordinatorPayloadView{}, ErrTransactionStateCorrupt
	}
	payloadBytes64 := uint64(binary.LittleEndian.Uint32(src[20:24]))
	if payloadBytes64 > distributedtxn.MaxCoordinatorRecordBytes ||
		uint64(transactionPayloadHeaderBytes+recordChecksumLen)+payloadBytes64 != uint64(len(src)) {
		return TransactionCoordinatorPayloadView{}, ErrTransactionStateCorrupt
	}
	payloadBytes := int(payloadBytes64)
	view := TransactionCoordinatorPayloadView{raw: src[:len(src):len(src)]}
	view.Kind = distributedtxn.ReplicatedPayloadKind(src[10])
	copy(view.ID[:], src[24:40])
	copy(view.Digest[:], src[40:72])
	end := transactionPayloadHeaderBytes + payloadBytes
	view.Payload = src[transactionPayloadHeaderBytes:end:end]
	if distributedtxn.Digest(sha256.Sum256(view.Payload)) != view.Digest ||
		!transactionCoordinatorPayloadValid(view.ID, view.Kind, view.Payload) {
		return TransactionCoordinatorPayloadView{}, ErrTransactionStateCorrupt
	}
	return view, nil
}

type TransactionManifestPageView struct {
	ID          distributedtxn.ID
	Index       uint32
	FirstTarget uint64
	TargetCount uint32
	Digest      distributedtxn.Digest
	Raw         []byte
	raw         []byte
}

func (view TransactionManifestPageView) Bytes() []byte {
	return view.raw[:len(view.raw):len(view.raw)]
}

func (view TransactionManifestPageView) StorageKey() ([transactionManifestKeyBytes]byte, error) {
	return TransactionManifestPageStorageKey(view.ID, view.Index)
}

func AppendTransactionManifestPage(
	dst []byte,
	id distributedtxn.ID,
	segment distributedtxn.ManifestSegment,
) ([]byte, error) {
	if id.IsZero() || segment.TargetCount == 0 || len(segment.Raw) == 0 ||
		len(segment.Raw) > distributedtxn.ManifestSegmentBytes {
		return dst, ErrTransactionStateCorrupt
	}
	meta, ok := openTransactionManifestSegmentMeta(segment.Raw)
	if !ok || meta.Index != segment.Index || meta.FirstTarget != segment.FirstTarget ||
		meta.TargetCount != segment.TargetCount || meta.Digest != segment.Digest {
		return dst, ErrTransactionStateCorrupt
	}
	total := transactionManifestHeaderBytes + len(segment.Raw) + recordChecksumLen
	if byteSlicesOverlap(writableAppendRegion(dst, total), segment.Raw) {
		return dst, ErrCodecAlias
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	frame := dst[start:]
	copy(frame[0:8], transactionManifestMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], transactionCodecSentinel)
	binary.LittleEndian.PutUint16(frame[10:12], transactionManifestHeaderBytes)
	binary.LittleEndian.PutUint32(frame[12:16], uint32(total))
	binary.LittleEndian.PutUint32(frame[16:20], uint32(len(segment.Raw)))
	binary.LittleEndian.PutUint32(frame[20:24], segment.Index)
	binary.LittleEndian.PutUint64(frame[24:32], segment.FirstTarget)
	binary.LittleEndian.PutUint32(frame[32:36], segment.TargetCount)
	copy(frame[40:56], id[:])
	copy(frame[56:88], segment.Digest[:])
	copy(frame[transactionManifestHeaderBytes:], segment.Raw)
	sealRecord(frame, transactionManifestChecksumDomain)
	return dst, nil
}

// OpenTransactionManifestPageInto validates both the storage row and the
// nested canonical VTM1 page using caller-owned decode arenas.
func OpenTransactionManifestPageInto(
	src []byte,
	targetScratch []distributedtxn.TransactionTargetRef, identityScratch []byte,
) (TransactionManifestPageView, error) {
	if len(src) < transactionManifestHeaderBytes+1+recordChecksumLen ||
		len(src) > MaxTransactionManifestPageRecordBytes ||
		!bytes.Equal(src[0:8], transactionManifestMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != transactionCodecSentinel ||
		binary.LittleEndian.Uint16(src[10:12]) != transactionManifestHeaderBytes ||
		binary.LittleEndian.Uint32(src[12:16]) != uint32(len(src)) ||
		binary.LittleEndian.Uint32(src[36:40]) != 0 ||
		!allZero(src[88:transactionManifestHeaderBytes]) ||
		!verifyRecord(src, transactionManifestChecksumDomain) {
		return TransactionManifestPageView{}, ErrTransactionStateCorrupt
	}
	rawBytes64 := uint64(binary.LittleEndian.Uint32(src[16:20]))
	if rawBytes64 > distributedtxn.ManifestSegmentBytes ||
		uint64(transactionManifestHeaderBytes+recordChecksumLen)+rawBytes64 != uint64(len(src)) {
		return TransactionManifestPageView{}, ErrTransactionStateCorrupt
	}
	rawBytes := int(rawBytes64)
	view := TransactionManifestPageView{raw: src[:len(src):len(src)]}
	view.Index = binary.LittleEndian.Uint32(src[20:24])
	view.FirstTarget = binary.LittleEndian.Uint64(src[24:32])
	view.TargetCount = binary.LittleEndian.Uint32(src[32:36])
	copy(view.ID[:], src[40:56])
	copy(view.Digest[:], src[56:88])
	end := transactionManifestHeaderBytes + rawBytes
	view.Raw = src[transactionManifestHeaderBytes:end:end]
	page, err := distributedtxn.OpenManifestSegment(view.Raw, targetScratch, identityScratch)
	if err != nil || view.ID.IsZero() || page.Segment.Index != view.Index ||
		page.Segment.FirstTarget != view.FirstTarget ||
		page.Segment.TargetCount != view.TargetCount ||
		page.Segment.Digest != view.Digest {
		return TransactionManifestPageView{}, ErrTransactionStateCorrupt
	}
	return view, nil
}

type TransactionNativeMutationView struct {
	ID       distributedtxn.ID
	Relation replication.RelationID
	Ordinal  uint32
	Digest   distributedtxn.Digest
	Mutation replication.Mutation
	raw      []byte
}

func (view TransactionNativeMutationView) Bytes() []byte {
	return view.raw[:len(view.raw):len(view.raw)]
}

func (view TransactionNativeMutationView) StorageKey() ([transactionMutationKeyBytes]byte, error) {
	return TransactionNativeMutationStorageKey(view.ID, view.Relation, view.Ordinal)
}

func TransactionNativeMutationDigest(
	id distributedtxn.ID,
	relation replication.RelationID,
	ordinal uint32,
	mutation replication.Mutation,
) distributedtxn.Digest {
	h := sha256.New()
	_, _ = h.Write(transactionMutationDigestDomain)
	_, _ = h.Write(id[:])
	var fixed [48]byte
	binary.LittleEndian.PutUint16(fixed[0:2], uint16(relation))
	binary.LittleEndian.PutUint32(fixed[4:8], ordinal)
	fixed[8] = byte(mutation.Kind)
	binary.LittleEndian.PutUint32(fixed[12:16], uint32(len(mutation.Key)))
	binary.LittleEndian.PutUint32(fixed[16:20], uint32(len(mutation.Value)))
	binary.LittleEndian.PutUint64(fixed[20:28], mutation.ExpectedValueLength)
	_, _ = h.Write(fixed[:28])
	_, _ = h.Write(mutation.ExpectedValueDigest[:])
	_, _ = h.Write(mutation.Key)
	_, _ = h.Write(mutation.Value)
	var digest distributedtxn.Digest
	_ = h.Sum(digest[:0])
	return digest
}

func AppendTransactionNativeMutation(
	dst []byte,
	id distributedtxn.ID,
	relation replication.RelationID,
	ordinal uint32,
	mutation replication.Mutation,
) ([]byte, error) {
	if id.IsZero() || !transactionRelationValid(relation) ||
		ordinal >= replication.MaxMutations || !transactionMutationValid(mutation) {
		return dst, ErrTransactionStateCorrupt
	}
	total := transactionMutationHeaderBytes + len(mutation.Key) + len(mutation.Value) + recordChecksumLen
	region := writableAppendRegion(dst, total)
	if byteSlicesOverlap(region, mutation.Key) || byteSlicesOverlap(region, mutation.Value) {
		return dst, ErrCodecAlias
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	frame := dst[start:]
	copy(frame[0:8], transactionMutationMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], transactionCodecSentinel)
	frame[10] = byte(mutation.Kind)
	binary.LittleEndian.PutUint16(frame[12:14], transactionMutationHeaderBytes)
	binary.LittleEndian.PutUint32(frame[16:20], uint32(total))
	binary.LittleEndian.PutUint16(frame[20:22], uint16(len(mutation.Key)))
	binary.LittleEndian.PutUint16(frame[22:24], uint16(relation))
	binary.LittleEndian.PutUint32(frame[24:28], ordinal)
	binary.LittleEndian.PutUint32(frame[28:32], uint32(len(mutation.Value)))
	copy(frame[32:48], id[:])
	binary.LittleEndian.PutUint64(frame[48:56], mutation.ExpectedValueLength)
	copy(frame[56:88], mutation.ExpectedValueDigest[:])
	digest := TransactionNativeMutationDigest(id, relation, ordinal, mutation)
	copy(frame[88:120], digest[:])
	cursor := transactionMutationHeaderBytes
	copy(frame[cursor:], mutation.Key)
	cursor += len(mutation.Key)
	copy(frame[cursor:], mutation.Value)
	sealRecord(frame, transactionMutationChecksumDomain)
	return dst, nil
}

func OpenTransactionNativeMutation(src []byte) (TransactionNativeMutationView, error) {
	if len(src) < transactionMutationHeaderBytes+1+recordChecksumLen ||
		len(src) > MaxTransactionNativeMutationRecordBytes ||
		!bytes.Equal(src[0:8], transactionMutationMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != transactionCodecSentinel || src[11] != 0 ||
		binary.LittleEndian.Uint16(src[12:14]) != transactionMutationHeaderBytes ||
		binary.LittleEndian.Uint16(src[14:16]) != 0 ||
		binary.LittleEndian.Uint32(src[16:20]) != uint32(len(src)) ||
		!allZero(src[120:transactionMutationHeaderBytes]) ||
		!verifyRecord(src, transactionMutationChecksumDomain) {
		return TransactionNativeMutationView{}, ErrTransactionStateCorrupt
	}
	keyBytes64 := uint64(binary.LittleEndian.Uint16(src[20:22]))
	valueBytes64 := uint64(binary.LittleEndian.Uint32(src[28:32]))
	if keyBytes64 > replication.MaxMutationKeyBytes || valueBytes64 > replication.MaxMutationValueBytes ||
		uint64(transactionMutationHeaderBytes+recordChecksumLen)+keyBytes64+valueBytes64 != uint64(len(src)) {
		return TransactionNativeMutationView{}, ErrTransactionStateCorrupt
	}
	keyBytes, valueBytes := int(keyBytes64), int(valueBytes64)
	view := TransactionNativeMutationView{raw: src[:len(src):len(src)]}
	view.Relation = replication.RelationID(binary.LittleEndian.Uint16(src[22:24]))
	view.Ordinal = binary.LittleEndian.Uint32(src[24:28])
	copy(view.ID[:], src[32:48])
	view.Mutation.Kind = replication.MutationKind(src[10])
	view.Mutation.ExpectedValueLength = binary.LittleEndian.Uint64(src[48:56])
	copy(view.Mutation.ExpectedValueDigest[:], src[56:88])
	copy(view.Digest[:], src[88:120])
	keyEnd := transactionMutationHeaderBytes + keyBytes
	valueEnd := keyEnd + valueBytes
	view.Mutation.Key = src[transactionMutationHeaderBytes:keyEnd:keyEnd]
	view.Mutation.Value = src[keyEnd:valueEnd:valueEnd]
	if view.ID.IsZero() || !transactionRelationValid(view.Relation) ||
		view.Ordinal >= replication.MaxMutations || !transactionMutationValid(view.Mutation) ||
		TransactionNativeMutationDigest(view.ID, view.Relation, view.Ordinal, view.Mutation) != view.Digest {
		return TransactionNativeMutationView{}, ErrTransactionStateCorrupt
	}
	return view, nil
}

type TransactionIntentView struct {
	ID       distributedtxn.ID
	Relation replication.RelationID
	KeyHash  [sha256.Size]byte
	RawKey   []byte
	raw      []byte
}

func (view TransactionIntentView) Bytes() []byte {
	return view.raw[:len(view.raw):len(view.raw)]
}

func (view TransactionIntentView) StorageKey() ([transactionIntentKeyBytes]byte, error) {
	return TransactionIntentStorageKey(view.Relation, view.RawKey)
}

func (view TransactionIntentView) MatchesKey(
	relation replication.RelationID,
	rawKey []byte,
) bool {
	return view.Relation == relation && len(rawKey) == len(view.RawKey) &&
		sha256.Sum256(rawKey) == view.KeyHash && bytes.Equal(rawKey, view.RawKey)
}

func AppendTransactionIntent(
	dst []byte,
	id distributedtxn.ID,
	relation replication.RelationID,
	rawKey []byte,
) ([]byte, error) {
	if id.IsZero() || !transactionRelationValid(relation) || len(rawKey) == 0 ||
		len(rawKey) > replication.MaxMutationKeyBytes {
		return dst, ErrTransactionStateCorrupt
	}
	total := transactionIntentHeaderBytes + len(rawKey) + recordChecksumLen
	if byteSlicesOverlap(writableAppendRegion(dst, total), rawKey) {
		return dst, ErrCodecAlias
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	frame := dst[start:]
	copy(frame[0:8], transactionIntentMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], transactionCodecSentinel)
	binary.LittleEndian.PutUint16(frame[10:12], transactionIntentHeaderBytes)
	binary.LittleEndian.PutUint32(frame[12:16], uint32(total))
	binary.LittleEndian.PutUint16(frame[16:18], uint16(relation))
	binary.LittleEndian.PutUint16(frame[18:20], uint16(len(rawKey)))
	copy(frame[24:40], id[:])
	digest := sha256.Sum256(rawKey)
	copy(frame[40:72], digest[:])
	copy(frame[transactionIntentHeaderBytes:], rawKey)
	sealRecord(frame, transactionIntentChecksumDomain)
	return dst, nil
}

func OpenTransactionIntent(src []byte) (TransactionIntentView, error) {
	if len(src) < transactionIntentHeaderBytes+1+recordChecksumLen ||
		len(src) > MaxTransactionIntentRecordBytes ||
		!bytes.Equal(src[0:8], transactionIntentMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != transactionCodecSentinel ||
		binary.LittleEndian.Uint16(src[10:12]) != transactionIntentHeaderBytes ||
		binary.LittleEndian.Uint32(src[12:16]) != uint32(len(src)) ||
		binary.LittleEndian.Uint32(src[20:24]) != 0 ||
		!allZero(src[72:transactionIntentHeaderBytes]) ||
		!verifyRecord(src, transactionIntentChecksumDomain) {
		return TransactionIntentView{}, ErrTransactionStateCorrupt
	}
	keyBytes := int(binary.LittleEndian.Uint16(src[18:20]))
	if transactionIntentHeaderBytes+keyBytes+recordChecksumLen != len(src) {
		return TransactionIntentView{}, ErrTransactionStateCorrupt
	}
	view := TransactionIntentView{raw: src[:len(src):len(src)]}
	view.Relation = replication.RelationID(binary.LittleEndian.Uint16(src[16:18]))
	copy(view.ID[:], src[24:40])
	copy(view.KeyHash[:], src[40:72])
	end := transactionIntentHeaderBytes + keyBytes
	view.RawKey = src[transactionIntentHeaderBytes:end:end]
	if view.ID.IsZero() || !transactionRelationValid(view.Relation) ||
		keyBytes == 0 || keyBytes > replication.MaxMutationKeyBytes ||
		sha256.Sum256(view.RawKey) != view.KeyHash {
		return TransactionIntentView{}, ErrTransactionStateCorrupt
	}
	return view, nil
}

// OpenTransactionIntentForKey turns the collision check into one mandatory
// operation for lookup callers: both SHA-256 and the retained raw key must
// match the requested relation/key pair.
func OpenTransactionIntentForKey(
	src []byte,
	relation replication.RelationID,
	rawKey []byte,
) (TransactionIntentView, error) {
	view, err := OpenTransactionIntent(src)
	if err != nil || !view.MatchesKey(relation, rawKey) {
		return TransactionIntentView{}, ErrTransactionStateCorrupt
	}
	return view, nil
}

func transactionControlValid(control TransactionControl) bool {
	cancellation := transactionCancellationWitnessValid(control)
	if control.ID.IsZero() || !transactionRoleValid(control.Role) || control.Revision == 0 ||
		control.ControllerEpoch == 0 || control.ExecutionPinDigest == (distributedtxn.Digest{}) ||
		control.Role == distributedtxn.ReplicatedRoleTarget && control.RecoveryPulse != 0 ||
		control.PayloadDigest == (distributedtxn.Digest{}) ||
		(!cancellation && (control.PayloadBytes == 0 || control.PayloadCount == 0)) ||
		control.CoordinatorGroup == (replication.ID128{}) ||
		control.CoordinatorShardIncarnation == (replication.ID128{}) ||
		control.CoordinatorAllocation == 0 || control.MutationDigest == (distributedtxn.Digest{}) ||
		control.LastCommandDigest == (replication.Digest{}) || control.LastResultCode == 0 ||
		control.LastAppliedIndex == 0 || !transactionOperationRole(control.LastOperation, control.Role) ||
		(!transactionStateOperationCompatible(control.Role, control.State, control.LastOperation) &&
			!transactionFailedPrepareWitness(control) &&
			!transactionFailedFusedPrepareWitness(control)) {
		return false
	}
	if operationUsesFusedTransactionPath(control.LastOperation) != control.FusedPath &&
		operationHasExclusiveTransactionPath(control.LastOperation) {
		return false
	}
	creation := control.LastOperation == distributedtxn.ReplicatedStageCoordinator ||
		control.LastOperation == distributedtxn.ReplicatedStageManifestCoordinator ||
		control.LastOperation == distributedtxn.ReplicatedStageTarget ||
		control.LastOperation == distributedtxn.ReplicatedBeginPrepareCoordinator ||
		control.LastOperation == distributedtxn.ReplicatedBeginPrepareManifestCoordinator ||
		control.LastOperation == distributedtxn.ReplicatedStagePrepareTarget ||
		control.CancellationWitness
	if creation != (control.LastExpectedRevision == 0) {
		return false
	}
	wantControl, err := TransactionControlResidentBytes(len(control.IntentScopes))
	if err != nil || control.ResidentControlBytes != wantControl ||
		!transactionResidentCountersValid(control) {
		return false
	}
	if control.Role == distributedtxn.ReplicatedRoleCoordinator {
		if control.CancellationWitness {
			return false
		}
		if control.PayloadBytes > distributedtxn.MaxManifestBytes ||
			control.PayloadCount > distributedtxn.MaxManifestBytes {
			return false
		}
		active := distributedtxn.CoordinatorState(control.State) != distributedtxn.CoordinatorRetired
		if active != (control.ResidentPayloadBytes != 0) ||
			control.ResidentMutationBytes != 0 || control.ResidentIntentBytes != 0 {
			return false
		}
		if control.PayloadKind == distributedtxn.ReplicatedPayloadCoordinator &&
			(control.ResidentManifestBytes != 0 ||
				control.PayloadBytes > distributedtxn.MaxCoordinatorRecordBytes ||
				control.PayloadCount > distributedtxn.MaxInlineTargets ||
				!transactionManifestProgressZero(control)) {
			return false
		}
		if control.PayloadKind == distributedtxn.ReplicatedPayloadManifestCoordinator &&
			(active != (control.ResidentManifestBytes != 0) ||
				!transactionManifestProgressValid(control)) {
			return false
		}
		if control.PrepareResultCode != 0 &&
			control.PrepareResultCode != ResultApplied &&
			control.PrepareResultCode != ResultIndexConflict &&
			control.PrepareResultCode != ResultWrongShard {
			return false
		}
		if !control.FusedPath && control.PrepareResultCode != 0 {
			return false
		}
		if control.FusedPath && control.PrepareResultCode == 0 {
			return false
		}
		if control.PrepareResultCode == 0 && control.CoordinatorTargetOrdinal != 0 {
			return false
		}
		if control.PrepareResultCode != 0 &&
			control.CoordinatorTargetOrdinal >= control.PayloadCount {
			return false
		}
		return (control.PayloadKind == distributedtxn.ReplicatedPayloadCoordinator ||
			control.PayloadKind == distributedtxn.ReplicatedPayloadManifestCoordinator) &&
			control.PayloadRelationCount == 0 && control.BucketBits == 0 && len(control.IntentScopes) == 0 &&
			transactionCoordinatorAffectedRowsValid(control) &&
			transactionCoordinatorDecisionValid(control) &&
			control.State >= uint8(distributedtxn.CoordinatorStaging) &&
			control.State <= uint8(distributedtxn.CoordinatorRetired)
	}
	if control.CancellationWitness {
		return cancellation
	}
	if control.TargetOrdinal != 0 {
		return false
	}
	if control.PayloadKind != distributedtxn.ReplicatedPayloadTargetStage ||
		control.PayloadBytes > replication.MaxCommandBytes ||
		control.PayloadCount > replication.MaxMutations ||
		control.PayloadRelationCount == 0 ||
		control.PayloadRelationCount > replication.MaxRelationsPerBundle ||
		uint64(control.PayloadRelationCount) > control.PayloadCount ||
		!distributedtxn.ValidateIntentScopes(control.IntentScopes, control.BucketBits) ||
		control.State < uint8(distributedtxn.TargetStaged) ||
		control.State > uint8(distributedtxn.TargetReleased) || control.AffectedRows < 0 ||
		control.ResidentPayloadBytes != 0 || control.ResidentManifestBytes != 0 ||
		!transactionManifestProgressZero(control) ||
		control.CoordinatorDecision != distributedtxn.CoordinatorInvalid ||
		control.CoordinatorTargetOrdinal != 0 ||
		(control.PrepareResultCode != 0 && control.PrepareResultCode != ResultApplied &&
			control.PrepareResultCode != ResultIndexConflict &&
			control.PrepareResultCode != ResultWrongShard) {
		return false
	}
	if (control.PrepareResultCode != 0) !=
		(control.PrepareCommandDigest != (replication.Digest{})) {
		return false
	}
	if control.FusedPath && control.PrepareResultCode == 0 {
		return false
	}
	active := distributedtxn.TargetState(control.State) != distributedtxn.TargetReleased
	if active != (control.ResidentMutationBytes != 0) || active != (control.ResidentIntentBytes != 0) {
		return false
	}
	switch distributedtxn.TargetState(control.State) {
	case distributedtxn.TargetApplied:
		return control.AffectedRowsValid
	case distributedtxn.TargetAborted:
		return !control.AffectedRowsValid && control.AffectedRows == 0
	case distributedtxn.TargetReleased:
		return !control.AffectedRowsValid && control.AffectedRows == 0 || control.AffectedRowsValid
	default:
		return !control.AffectedRowsValid && control.AffectedRows == 0
	}
}

func transactionCoordinatorAffectedRowsValid(control TransactionControl) bool {
	if distributedtxn.CoordinatorState(control.State) != distributedtxn.CoordinatorRetired {
		return !control.AffectedRowsValid && control.AffectedRows == 0
	}
	if control.CoordinatorDecision == distributedtxn.CoordinatorCommitted {
		return control.AffectedRowsValid && control.AffectedRows >= 0
	}
	return control.CoordinatorDecision == distributedtxn.CoordinatorAborted &&
		!control.AffectedRowsValid && control.AffectedRows == 0
}

func transactionCancellationWitnessValid(control TransactionControl) bool {
	return control.CancellationWitness && control.FusedPath &&
		control.Role == distributedtxn.ReplicatedRoleTarget &&
		distributedtxn.TargetState(control.State) == distributedtxn.TargetReleased &&
		control.Revision == 1 &&
		control.PayloadKind == distributedtxn.ReplicatedPayloadTargetStage &&
		control.PayloadDigest == control.MutationDigest && control.PayloadBytes == 0 &&
		control.PayloadCount == 0 && control.PayloadRelationCount == 0 &&
		control.BucketBits == 0 && len(control.IntentScopes) == 0 &&
		!control.AffectedRowsValid && control.AffectedRows == 0 &&
		control.PrepareResultCode == 0 && control.PrepareCommandDigest == (replication.Digest{}) &&
		control.CoordinatorDecision == distributedtxn.CoordinatorInvalid &&
		control.CoordinatorTargetOrdinal == 0 &&
		control.ResidentPayloadBytes == 0 && control.ResidentManifestBytes == 0 &&
		control.ResidentMutationBytes == 0 && control.ResidentIntentBytes == 0 &&
		transactionManifestProgressZero(control) &&
		control.LastOperation == distributedtxn.ReplicatedAbortReleaseTarget &&
		control.LastExpectedRevision == 0 && control.LastResultCode == ResultApplied
}

func operationHasExclusiveTransactionPath(operation distributedtxn.ReplicatedOperation) bool {
	switch operation {
	case distributedtxn.ReplicatedStageCoordinator,
		distributedtxn.ReplicatedStageManifestCoordinator,
		distributedtxn.ReplicatedStageManifestSegment,
		distributedtxn.ReplicatedStageTarget,
		distributedtxn.ReplicatedPrepareTarget,
		distributedtxn.ReplicatedApplyTarget,
		distributedtxn.ReplicatedAbortTarget,
		distributedtxn.ReplicatedReleaseTarget,
		distributedtxn.ReplicatedBeginPrepareCoordinator,
		distributedtxn.ReplicatedBeginPrepareManifestCoordinator,
		distributedtxn.ReplicatedAppendManifestSegments,
		distributedtxn.ReplicatedStagePrepareTarget,
		distributedtxn.ReplicatedApplySingleTarget,
		distributedtxn.ReplicatedApplyReleaseTarget,
		distributedtxn.ReplicatedAbortReleaseTarget:
		return true
	default:
		return false
	}
}

func operationUsesFusedTransactionPath(operation distributedtxn.ReplicatedOperation) bool {
	switch operation {
	case distributedtxn.ReplicatedBeginPrepareCoordinator,
		distributedtxn.ReplicatedBeginPrepareManifestCoordinator,
		distributedtxn.ReplicatedAppendManifestSegments,
		distributedtxn.ReplicatedStagePrepareTarget,
		distributedtxn.ReplicatedApplySingleTarget,
		distributedtxn.ReplicatedApplyReleaseTarget,
		distributedtxn.ReplicatedAbortReleaseTarget:
		return true
	default:
		return false
	}
}

func transactionFailedPrepareWitness(control TransactionControl) bool {
	return control.Role == distributedtxn.ReplicatedRoleTarget &&
		distributedtxn.TargetState(control.State) == distributedtxn.TargetStaged &&
		control.LastOperation == distributedtxn.ReplicatedPrepareTarget &&
		control.LastExpectedRevision == control.Revision &&
		(control.LastResultCode == ResultIndexConflict ||
			control.LastResultCode == ResultWrongShard)
}

func transactionFailedFusedPrepareWitness(control TransactionControl) bool {
	return control.Role == distributedtxn.ReplicatedRoleTarget &&
		distributedtxn.TargetState(control.State) == distributedtxn.TargetReleased &&
		control.LastOperation == distributedtxn.ReplicatedStagePrepareTarget &&
		control.LastExpectedRevision == 0 &&
		(control.LastResultCode == ResultIndexConflict ||
			control.LastResultCode == ResultWrongShard) &&
		control.PrepareResultCode == control.LastResultCode &&
		!control.AffectedRowsValid && control.AffectedRows == 0 &&
		control.ResidentMutationBytes == 0 && control.ResidentIntentBytes == 0
}

func transactionCoordinatorDecisionValid(control TransactionControl) bool {
	switch distributedtxn.CoordinatorState(control.State) {
	case distributedtxn.CoordinatorStaging:
		return control.CoordinatorDecision == distributedtxn.CoordinatorInvalid
	case distributedtxn.CoordinatorCommitted:
		return control.CoordinatorDecision == distributedtxn.CoordinatorCommitted
	case distributedtxn.CoordinatorAborted:
		return control.CoordinatorDecision == distributedtxn.CoordinatorAborted
	case distributedtxn.CoordinatorRetired:
		return control.CoordinatorDecision == distributedtxn.CoordinatorCommitted ||
			control.CoordinatorDecision == distributedtxn.CoordinatorAborted
	default:
		return false
	}
}

func transactionManifestProgressZero(control TransactionControl) bool {
	return control.ManifestNextPage == 0 && control.ManifestNextTarget == 0 &&
		control.ManifestEncodedBytes == 0 &&
		control.ManifestChainDigest == (distributedtxn.Digest{})
}

func transactionManifestProgressValid(control TransactionControl) bool {
	zero := transactionManifestProgressZero(control)
	if control.ManifestNextPage == 0 || control.ManifestNextTarget == 0 ||
		control.ManifestEncodedBytes == 0 ||
		control.ManifestChainDigest == (distributedtxn.Digest{}) {
		return zero
	}
	return uint64(control.ManifestNextPage) <= control.PayloadCount &&
		control.ManifestNextTarget <= control.PayloadCount &&
		control.ManifestEncodedBytes <= control.PayloadBytes
}

// advanceTransactionManifestChain and finishTransactionManifestRoot mirror
// the canonical distributedtxn manifest construction. Keeping the O(1)
// transition here lets apply validate a final VTCM descriptor without opening
// any previously retained page row.
func advanceTransactionManifestChain(
	chain distributedtxn.Digest,
	index uint32,
	page distributedtxn.Digest,
) distributedtxn.Digest {
	var encoded [8 + 32 + 4 + 32]byte
	copy(encoded[0:8], transactionManifestChainDomain[:])
	copy(encoded[8:40], chain[:])
	binary.LittleEndian.PutUint32(encoded[40:44], index)
	copy(encoded[44:76], page[:])
	return sha256.Sum256(encoded[:])
}

func finishTransactionManifestRoot(
	chain distributedtxn.Digest,
	targetCount uint64,
	encodedBytes uint64,
	segmentCount uint32,
) distributedtxn.Digest {
	var encoded [8 + 32 + 8 + 8 + 4]byte
	copy(encoded[0:8], transactionManifestRootDomain[:])
	copy(encoded[8:40], chain[:])
	binary.LittleEndian.PutUint64(encoded[40:48], targetCount)
	binary.LittleEndian.PutUint64(encoded[48:56], encodedBytes)
	binary.LittleEndian.PutUint32(encoded[56:60], segmentCount)
	return sha256.Sum256(encoded[:])
}

func transactionResidentCountersValid(control TransactionControl) bool {
	total := control.ResidentControlBytes
	if total > MaxTransactionResidentBytes {
		return false
	}
	for _, value := range [...]uint64{
		control.ResidentPayloadBytes, control.ResidentManifestBytes,
		control.ResidentMutationBytes, control.ResidentIntentBytes,
	} {
		if value > MaxTransactionResidentBytes-total {
			return false
		}
		total += value
	}
	if total > MaxTransactionResidentBytes {
		return false
	}
	terminal := control.Role == distributedtxn.ReplicatedRoleCoordinator &&
		distributedtxn.CoordinatorState(control.State) == distributedtxn.CoordinatorRetired ||
		control.Role == distributedtxn.ReplicatedRoleTarget &&
			distributedtxn.TargetState(control.State) == distributedtxn.TargetReleased
	return !terminal || control.ResidentPayloadBytes == 0 &&
		control.ResidentManifestBytes == 0 && control.ResidentMutationBytes == 0 &&
		control.ResidentIntentBytes == 0
}

func transactionCoordinatorPayloadValid(
	id distributedtxn.ID,
	kind distributedtxn.ReplicatedPayloadKind,
	payload []byte,
) bool {
	if id.IsZero() || len(payload) == 0 || len(payload) > distributedtxn.MaxCoordinatorRecordBytes {
		return false
	}
	switch kind {
	case distributedtxn.ReplicatedPayloadCoordinator:
		var scratch [distributedtxn.MaxInlineTargets]distributedtxn.TransactionTargetRef
		record, err := distributedtxn.OpenCoordinatorInto(payload, scratch[:])
		return err == nil && record.ID == id && record.State == distributedtxn.CoordinatorStaging &&
			transactionInlineCoordinatorCanonical(payload)
	case distributedtxn.ReplicatedPayloadManifestCoordinator:
		record, err := distributedtxn.OpenManifestCoordinator(payload)
		return err == nil && record.ID == id && record.State == distributedtxn.CoordinatorStaging
	default:
		return false
	}
}

func transactionInlineCoordinatorCanonical(raw []byte) bool {
	const (
		headerBytes = 48
		entryBytes  = 76
		checksum    = 4
	)
	if len(raw) < headerBytes+entryBytes+checksum {
		return false
	}
	count := int(binary.LittleEndian.Uint16(raw[6:8]))
	cursor, end := headerBytes, len(raw)-checksum
	for index := 0; index < count; index++ {
		if end-cursor < entryBytes || raw[cursor+3] != 0 {
			return false
		}
		cursor += entryBytes + int(raw[cursor]) + int(raw[cursor+1])
		if cursor > end {
			return false
		}
	}
	return cursor == end
}

func transactionRoleValid(role distributedtxn.ReplicatedRole) bool {
	return role == distributedtxn.ReplicatedRoleCoordinator ||
		role == distributedtxn.ReplicatedRoleTarget
}

func transactionRelationValid(relation replication.RelationID) bool {
	return relation > 0 && relation <= replication.MaxRelationID
}

func transactionOperationRole(
	operation distributedtxn.ReplicatedOperation,
	role distributedtxn.ReplicatedRole,
) bool {
	switch operation {
	case distributedtxn.ReplicatedStageCoordinator,
		distributedtxn.ReplicatedStageManifestCoordinator,
		distributedtxn.ReplicatedBeginPrepareCoordinator,
		distributedtxn.ReplicatedBeginPrepareManifestCoordinator,
		distributedtxn.ReplicatedStageManifestSegment,
		distributedtxn.ReplicatedAppendManifestSegments,
		distributedtxn.ReplicatedCommitCoordinator,
		distributedtxn.ReplicatedAbortCoordinator,
		distributedtxn.ReplicatedRetireCoordinator:
		return role == distributedtxn.ReplicatedRoleCoordinator
	case distributedtxn.ReplicatedPulseCoordinator:
		return role == distributedtxn.ReplicatedRoleCoordinator
	case distributedtxn.ReplicatedStageTarget,
		distributedtxn.ReplicatedStagePrepareTarget,
		distributedtxn.ReplicatedApplySingleTarget,
		distributedtxn.ReplicatedPrepareTarget,
		distributedtxn.ReplicatedApplyTarget,
		distributedtxn.ReplicatedApplyReleaseTarget,
		distributedtxn.ReplicatedAbortTarget,
		distributedtxn.ReplicatedAbortReleaseTarget,
		distributedtxn.ReplicatedReleaseTarget:
		return role == distributedtxn.ReplicatedRoleTarget
	default:
		return false
	}
}

func transactionStateOperationCompatible(
	role distributedtxn.ReplicatedRole,
	state uint8,
	operation distributedtxn.ReplicatedOperation,
) bool {
	if role == distributedtxn.ReplicatedRoleCoordinator {
		switch distributedtxn.CoordinatorState(state) {
		case distributedtxn.CoordinatorStaging:
			return operation == distributedtxn.ReplicatedStageCoordinator ||
				operation == distributedtxn.ReplicatedStageManifestCoordinator ||
				operation == distributedtxn.ReplicatedBeginPrepareCoordinator ||
				operation == distributedtxn.ReplicatedBeginPrepareManifestCoordinator ||
				operation == distributedtxn.ReplicatedStageManifestSegment ||
				operation == distributedtxn.ReplicatedAppendManifestSegments ||
				operation == distributedtxn.ReplicatedPulseCoordinator
		case distributedtxn.CoordinatorCommitted:
			return operation == distributedtxn.ReplicatedCommitCoordinator
		case distributedtxn.CoordinatorAborted:
			return operation == distributedtxn.ReplicatedAbortCoordinator
		case distributedtxn.CoordinatorRetired:
			return operation == distributedtxn.ReplicatedRetireCoordinator
		}
		return false
	}
	switch distributedtxn.TargetState(state) {
	case distributedtxn.TargetStaged:
		return operation == distributedtxn.ReplicatedStageTarget
	case distributedtxn.TargetPrepared:
		return operation == distributedtxn.ReplicatedPrepareTarget ||
			operation == distributedtxn.ReplicatedStagePrepareTarget
	case distributedtxn.TargetApplied:
		return operation == distributedtxn.ReplicatedApplyTarget
	case distributedtxn.TargetAborted:
		return operation == distributedtxn.ReplicatedAbortTarget
	case distributedtxn.TargetReleased:
		return operation == distributedtxn.ReplicatedReleaseTarget ||
			operation == distributedtxn.ReplicatedApplyReleaseTarget ||
			operation == distributedtxn.ReplicatedApplySingleTarget ||
			operation == distributedtxn.ReplicatedAbortReleaseTarget
	default:
		return false
	}
}

func transactionMutationValid(mutation replication.Mutation) bool {
	if len(mutation.Key) == 0 || len(mutation.Key) > replication.MaxMutationKeyBytes {
		return false
	}
	zeroExpected := mutation.ExpectedValueLength == 0 &&
		mutation.ExpectedValueDigest == (replication.Digest{})
	switch mutation.Kind {
	case replication.MutationPut, replication.MutationPutAbsentOrEqual,
		replication.MutationPutAbsent, replication.MutationPutPresent:
		return len(mutation.Value) > 0 && len(mutation.Value) <= replication.MaxMutationValueBytes &&
			zeroExpected
	case replication.MutationDelete:
		return len(mutation.Value) == 0 && zeroExpected
	case replication.MutationDeleteDigestEqual:
		return len(mutation.Value) == 0 && mutation.ExpectedValueLength > 0 &&
			mutation.ExpectedValueLength <= replication.MaxMutationValueBytes &&
			mutation.ExpectedValueDigest != (replication.Digest{})
	case replication.MutationPutDigestEqual:
		return len(mutation.Value) > 0 && len(mutation.Value) <= replication.MaxMutationValueBytes &&
			mutation.ExpectedValueLength > 0 &&
			mutation.ExpectedValueLength <= replication.MaxMutationValueBytes &&
			mutation.ExpectedValueDigest != (replication.Digest{})
	default:
		return false
	}
}

type transactionManifestSegmentMeta struct {
	Index       uint32
	FirstTarget uint64
	TargetCount uint32
	Digest      distributedtxn.Digest
}

// openTransactionManifestSegmentMeta validates the exact VTM1 grammar without
// materializing target slices. Full recovery decoding still uses
// distributedtxn.OpenManifestSegment with caller scratch.
func openTransactionManifestSegmentMeta(raw []byte) (transactionManifestSegmentMeta, bool) {
	const (
		headerBytes = 32
		entryBytes  = 80
		checksum    = 4
	)
	if len(raw) < headerBytes+entryBytes+checksum ||
		len(raw) > distributedtxn.ManifestSegmentBytes ||
		!bytes.Equal(raw[:4], []byte{'V', 'T', 'M', '1'}) || raw[4] != distributedtxn.FormatVersion ||
		raw[5] != 0 || raw[6] != 0 || raw[7] != 0 ||
		binary.LittleEndian.Uint32(raw[len(raw)-checksum:]) !=
			crc32.Checksum(raw[:len(raw)-checksum], transactionManifestCRC) {
		return transactionManifestSegmentMeta{}, false
	}
	index := binary.LittleEndian.Uint32(raw[8:12])
	count := binary.LittleEndian.Uint32(raw[12:16])
	first := binary.LittleEndian.Uint64(raw[16:24])
	payloadBytes := binary.LittleEndian.Uint32(raw[24:28])
	if count == 0 || count > distributedtxn.MaxManifestPageTargets ||
		binary.LittleEndian.Uint32(raw[28:32]) != 0 ||
		uint64(headerBytes)+uint64(payloadBytes)+checksum != uint64(len(raw)) {
		return transactionManifestSegmentMeta{}, false
	}
	cursor, end := headerBytes, len(raw)-checksum
	var priorDistribution, priorShard [distributedtxn.MaxShardIdentityBytes]byte
	priorDistributionBytes, priorShardBytes := 0, 0
	for target := uint32(0); target < count; target++ {
		if end-cursor < entryBytes {
			return transactionManifestSegmentMeta{}, false
		}
		entry := raw[cursor:]
		distributionPrefix, distributionSuffix := int(entry[0]), int(entry[1])
		shardPrefix, shardSuffix := int(entry[2]), int(entry[3])
		distributionBytes := distributionPrefix + distributionSuffix
		shardBytes := shardPrefix + shardSuffix
		state := distributedtxn.TargetState(entry[4])
		if entry[5] != 0 || entry[6] != 0 || entry[7] != 0 ||
			(target == 0 && (distributionPrefix != 0 || shardPrefix != 0)) ||
			distributionPrefix > priorDistributionBytes || shardPrefix > priorShardBytes ||
			distributionBytes == 0 || distributionBytes > distributedtxn.MaxShardIdentityBytes ||
			shardBytes == 0 || shardBytes > distributedtxn.MaxShardIdentityBytes ||
			end-cursor-entryBytes < distributionSuffix+shardSuffix ||
			state < distributedtxn.TargetStaged || state > distributedtxn.TargetReleased ||
			binary.LittleEndian.Uint64(entry[8:16]) == 0 ||
			binary.LittleEndian.Uint64(entry[16:24]) == 0 ||
			binary.LittleEndian.Uint64(entry[24:32]) == 0 || allZero(entry[32:64]) {
			return transactionManifestSegmentMeta{}, false
		}
		cursor += entryBytes
		var distribution, shard [distributedtxn.MaxShardIdentityBytes]byte
		copy(distribution[:distributionPrefix], priorDistribution[:distributionPrefix])
		copy(distribution[distributionPrefix:distributionBytes], raw[cursor:cursor+distributionSuffix])
		cursor += distributionSuffix
		copy(shard[:shardPrefix], priorShard[:shardPrefix])
		copy(shard[shardPrefix:shardBytes], raw[cursor:cursor+shardSuffix])
		cursor += shardSuffix
		currentDistribution := distribution[:distributionBytes]
		currentShard := shard[:shardBytes]
		priorDistributionView := priorDistribution[:priorDistributionBytes]
		priorShardView := priorShard[:priorShardBytes]
		if !utf8.Valid(currentDistribution) || !utf8.Valid(currentShard) ||
			(target != 0 && (transactionIdentityCompare(
				priorDistributionView, priorShardView, currentDistribution, currentShard,
			) >= 0 || transactionCommonPrefix(priorDistributionView, currentDistribution) != distributionPrefix ||
				transactionCommonPrefix(priorShardView, currentShard) != shardPrefix)) {
			return transactionManifestSegmentMeta{}, false
		}
		priorDistribution, priorShard = distribution, shard
		priorDistributionBytes, priorShardBytes = distributionBytes, shardBytes
	}
	if cursor != end {
		return transactionManifestSegmentMeta{}, false
	}
	return transactionManifestSegmentMeta{
		Index: index, FirstTarget: first, TargetCount: count,
		Digest: distributedtxn.Digest(sha256.Sum256(raw)),
	}, true
}

func transactionIdentityCompare(
	leftDistribution, leftShard, rightDistribution, rightShard []byte,
) int {
	if order := bytes.Compare(leftDistribution, rightDistribution); order != 0 {
		return order
	}
	return bytes.Compare(leftShard, rightShard)
}

func transactionCommonPrefix(left, right []byte) int {
	limit := min(len(left), len(right))
	index := 0
	for index < limit && left[index] == right[index] {
		index++
	}
	return index
}
