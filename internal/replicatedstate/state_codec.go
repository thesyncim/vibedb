package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	stateCodecFormat                  = uint16(2)
	stateHeaderBytes                  = 400
	stateTransactionHeaderBytes       = 440
	stateRequestLedgerHeaderBytes     = 480
	stateExecutionPinHeaderBytes      = 504
	stateRelationPlacementHeaderBytes = 536
	recordChecksumLen                 = sha256.Size
)

var (
	stateMagic          = [8]byte{'V', 'D', 'B', 'R', 'S', 'M', 0, 0}
	stateChecksumDomain = []byte("vibedb/replicated-state/state-checksum\x00")
)

// RecordKind identifies what produced the most recent persisted publication.
type RecordKind uint8

const (
	RecordStaticSnapshot RecordKind = 1
	RecordNormal         RecordKind = 2
	RecordConfiguration  RecordKind = 3
	RecordOwnership      RecordKind = 4
	// RecordImportedSnapshot anchors an independently bootstrapped shard at a
	// certified staged image. Its synthetic entry digest becomes the prefix for
	// every later local Raft apply.
	RecordImportedSnapshot RecordKind = 5
	// RecordSchema retires one live relation bundle after durably binding the
	// exact prepared replacement and catalog authorization.
	RecordSchema RecordKind = 6
)

// State is the exact durable publication stored at the fixed state key.
type State struct {
	Binding         Binding
	Applied         uint64
	LastTerm        uint64
	LastKind        RecordKind
	LastEntryType   pb.EntryType
	LastEntryDigest [32]byte
	// DataChainDigest is the history-sensitive logical data transition chain.
	// It advances from exact changed rows and is intentionally distinct from
	// the canonical full-image digest produced at certification boundaries.
	DataChainDigest     [32]byte
	ApplyContractDigest [32]byte
	ConfState           *pb.ConfState
	ReplicaSetVersion   uint64
	BootstrapDigest     [32]byte
	// SnapshotBaseDigest binds the exact Raft snapshot certificate that most
	// recently established this state-machine base. Normal ordered applies
	// preserve it; installing a newer certified base replaces it atomically.
	SnapshotBaseDigest [32]byte
	SessionCount       uint64
	SessionSlotCount   uint64
	// SessionEpochHighWater is the greatest Raft apply index assigned by a
	// SessionOpen. It is the durable shard-wide anti-resurrection fence retained
	// after an individual session header and retry ring are reclaimed.
	SessionEpochHighWater uint64
	// AuthorityBindingCount is the bounded number of class-independent stable
	// identities whose authority survives session release.
	AuthorityBindingCount uint64
	// Transaction counters authenticate the complete hidden transaction image.
	// The zero tuple retains the original compact state-envelope geometry.
	TransactionControlCount  uint64
	ActiveTransactionCount   uint64
	TransactionPayloadRows   uint64
	TransactionIntentRows    uint64
	TransactionResidentBytes uint64
	// RequestLedgerRows and RequestLedgerResidentBytes authenticate every
	// request-ledger row in the hidden collection. Resident bytes are exact
	// key+value bytes, independent of allocator or storage-page accounting.
	RequestLedgerRows          uint64
	RequestLedgerResidentBytes uint64
	RequestLedgerReservedBytes uint64
	RequestLedgerAckRows       uint64
	RequestLedgerAckBytes      uint64
	// Execution-pin counters authenticate both the durable lifecycle rows and
	// the active logical-scope index. Terminal tombstones remain retained to
	// fence delayed acquire commands; only the active index is reclaimed.
	ExecutionPinRecordCount   uint64
	ActiveExecutionPinCount   uint64
	ExecutionPinResidentBytes uint64
	// RelationPlacementDigest authenticates the fixed-size incremental image
	// accumulators for every placement-capable global-index relation. Zero means
	// the opened bundle exposes no such relation contract.
	RelationPlacementDigest [sha256.Size]byte
}

// AppendState appends one strict binary State envelope. On error dst is
// unchanged. Input strings must not overlap the writable append region in
// dst's current backing array; such aliases are rejected before dst is
// modified. Aliases into an old backing array are safe when append relocates.
func AppendState(dst []byte, state State) ([]byte, error) {
	if err := validateState(state); err != nil {
		return dst, err
	}
	conf, err := proto.MarshalOptions{Deterministic: true}.Marshal(state.ConfState)
	if err != nil {
		return dst, fmt.Errorf("%w: encode ConfState: %v", ErrStateCorrupt, err)
	}
	headerBytes := stateHeaderBytes
	if stateHasRelationPlacement(state) {
		headerBytes = stateRelationPlacementHeaderBytes
	} else if stateHasExecutionPins(state) {
		headerBytes = stateExecutionPinHeaderBytes
	} else if stateHasRequestLedger(state) {
		headerBytes = stateRequestLedgerHeaderBytes
	} else if stateHasTransactions(state) {
		headerBytes = stateTransactionHeaderBytes
	}
	total := headerBytes + len(state.Binding.Distribution) +
		len(state.Binding.Shard) + len(conf) + recordChecksumLen
	if total > MaxStateEnvelopeBytes {
		return dst, fmt.Errorf("%w: state envelope %d", ErrAdmissionBound, total)
	}
	region := writableAppendRegion(dst, total)
	if byteStringOverlap(region, state.Binding.Distribution) ||
		byteStringOverlap(region, state.Binding.Shard) {
		return dst, ErrCodecAlias
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	frame := dst[start:]
	copy(frame[0:8], stateMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], stateCodecFormat)
	frame[10] = byte(state.LastKind)
	frame[11] = byte(state.LastEntryType)
	binary.LittleEndian.PutUint16(frame[12:14], uint16(headerBytes))
	binary.LittleEndian.PutUint32(frame[16:20], uint32(total))
	binary.LittleEndian.PutUint32(frame[20:24], uint32(total-headerBytes-recordChecksumLen))
	binary.LittleEndian.PutUint64(frame[24:32], state.Applied)
	binary.LittleEndian.PutUint64(frame[32:40], state.LastTerm)
	binary.LittleEndian.PutUint64(frame[40:48], state.ReplicaSetVersion)
	binary.LittleEndian.PutUint64(frame[48:56], state.SessionCount)
	binary.LittleEndian.PutUint64(frame[56:64], state.Binding.TopologyRecoveryEpoch)
	binary.LittleEndian.PutUint64(frame[64:72], state.Binding.AllocationGeneration)
	binary.LittleEndian.PutUint64(frame[72:80], state.Binding.ActivePolicyGeneration)
	binary.LittleEndian.PutUint64(frame[80:88], state.Binding.ProtectionEpoch)
	binary.LittleEndian.PutUint64(frame[88:96], state.Binding.OwnershipEpoch)
	binary.LittleEndian.PutUint64(frame[96:104], state.Binding.SchemaGeneration)
	binary.LittleEndian.PutUint64(frame[104:112], state.Binding.RoutingVersion)
	binary.LittleEndian.PutUint64(frame[112:120], state.Binding.RouteGeneration)
	copy(frame[120:136], state.Binding.ClusterID[:])
	copy(frame[136:152], state.Binding.ClusterIncarnation[:])
	copy(frame[152:168], state.Binding.ShardIncarnation[:])
	copy(frame[168:184], state.Binding.GroupID[:])
	copy(frame[184:216], state.LastEntryDigest[:])
	copy(frame[216:248], state.DataChainDigest[:])
	copy(frame[248:280], state.ApplyContractDigest[:])
	copy(frame[280:312], state.BootstrapDigest[:])
	copy(frame[312:344], state.SnapshotBaseDigest[:])
	binary.LittleEndian.PutUint16(frame[344:346], uint16(len(state.Binding.Distribution)))
	binary.LittleEndian.PutUint16(frame[346:348], uint16(len(state.Binding.Shard)))
	binary.LittleEndian.PutUint32(frame[348:352], uint32(len(conf)))
	binary.LittleEndian.PutUint64(frame[352:360], state.SessionSlotCount)
	binary.LittleEndian.PutUint64(frame[360:368], state.SessionEpochHighWater)
	binary.LittleEndian.PutUint64(frame[368:376], state.AuthorityBindingCount)
	copy(frame[376:384], state.Binding.OwnedRange.Start[:])
	copy(frame[384:392], state.Binding.OwnedRange.End.Point[:])
	if state.Binding.OwnedRange.End.Max {
		frame[392] = 1
	}
	if headerBytes >= stateTransactionHeaderBytes {
		binary.LittleEndian.PutUint64(frame[400:408], state.TransactionControlCount)
		binary.LittleEndian.PutUint64(frame[408:416], state.ActiveTransactionCount)
		binary.LittleEndian.PutUint64(frame[416:424], state.TransactionPayloadRows)
		binary.LittleEndian.PutUint64(frame[424:432], state.TransactionIntentRows)
		binary.LittleEndian.PutUint64(frame[432:440], state.TransactionResidentBytes)
	}
	if headerBytes >= stateRequestLedgerHeaderBytes {
		binary.LittleEndian.PutUint64(frame[440:448], state.RequestLedgerRows)
		binary.LittleEndian.PutUint64(frame[448:456], state.RequestLedgerResidentBytes)
		binary.LittleEndian.PutUint64(frame[456:464], state.RequestLedgerReservedBytes)
		binary.LittleEndian.PutUint64(frame[464:472], state.RequestLedgerAckRows)
		binary.LittleEndian.PutUint64(frame[472:480], state.RequestLedgerAckBytes)
	}
	if headerBytes >= stateExecutionPinHeaderBytes {
		binary.LittleEndian.PutUint64(frame[480:488], state.ExecutionPinRecordCount)
		binary.LittleEndian.PutUint64(frame[488:496], state.ActiveExecutionPinCount)
		binary.LittleEndian.PutUint64(frame[496:504], state.ExecutionPinResidentBytes)
	}
	if headerBytes == stateRelationPlacementHeaderBytes {
		copy(frame[504:536], state.RelationPlacementDigest[:])
	}
	cursor := headerBytes
	cursor += copy(frame[cursor:], state.Binding.Distribution)
	cursor += copy(frame[cursor:], state.Binding.Shard)
	cursor += copy(frame[cursor:], conf)
	sealRecord(frame, stateChecksumDomain)
	return dst, nil
}

// OpenState strictly decodes one complete State envelope.
func OpenState(src []byte) (State, error) {
	if len(src) < stateHeaderBytes+recordChecksumLen || len(src) > MaxStateEnvelopeBytes {
		return State{}, fmt.Errorf("%w: state length", ErrStateCorrupt)
	}
	headerBytes := int(binary.LittleEndian.Uint16(src[12:14]))
	if !bytes.Equal(src[0:8], stateMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != stateCodecFormat ||
		(headerBytes != stateHeaderBytes && headerBytes != stateTransactionHeaderBytes &&
			headerBytes != stateRequestLedgerHeaderBytes &&
			headerBytes != stateExecutionPinHeaderBytes &&
			headerBytes != stateRelationPlacementHeaderBytes) ||
		binary.LittleEndian.Uint16(src[14:16]) != 0 {
		return State{}, fmt.Errorf("%w: state header", ErrStateCorrupt)
	}
	total64 := uint64(binary.LittleEndian.Uint32(src[16:20]))
	body64 := uint64(binary.LittleEndian.Uint32(src[20:24]))
	if total64 != uint64(len(src)) ||
		body64 != uint64(len(src)-headerBytes-recordChecksumLen) ||
		!verifyRecord(src, stateChecksumDomain) {
		return State{}, fmt.Errorf("%w: state size or checksum", ErrStateCorrupt)
	}
	distributionLen64 := uint64(binary.LittleEndian.Uint16(src[344:346]))
	shardLen64 := uint64(binary.LittleEndian.Uint16(src[346:348]))
	confLen64 := uint64(binary.LittleEndian.Uint32(src[348:352]))
	if distributionLen64+shardLen64+confLen64 != body64 || confLen64 == 0 ||
		distributionLen64 > uint64(len(src)) || shardLen64 > uint64(len(src)) ||
		confLen64 > uint64(len(src)) {
		return State{}, fmt.Errorf("%w: state body lengths", ErrStateCorrupt)
	}
	distributionLen, shardLen, confLen := int(distributionLen64), int(shardLen64), int(confLen64)
	var state State
	state.LastKind = RecordKind(src[10])
	state.LastEntryType = pb.EntryType(src[11])
	state.Applied = binary.LittleEndian.Uint64(src[24:32])
	state.LastTerm = binary.LittleEndian.Uint64(src[32:40])
	state.ReplicaSetVersion = binary.LittleEndian.Uint64(src[40:48])
	state.SessionCount = binary.LittleEndian.Uint64(src[48:56])
	state.Binding.TopologyRecoveryEpoch = binary.LittleEndian.Uint64(src[56:64])
	state.Binding.AllocationGeneration = binary.LittleEndian.Uint64(src[64:72])
	state.Binding.ActivePolicyGeneration = binary.LittleEndian.Uint64(src[72:80])
	state.Binding.ProtectionEpoch = binary.LittleEndian.Uint64(src[80:88])
	state.Binding.OwnershipEpoch = binary.LittleEndian.Uint64(src[88:96])
	state.Binding.SchemaGeneration = binary.LittleEndian.Uint64(src[96:104])
	state.Binding.RoutingVersion = binary.LittleEndian.Uint64(src[104:112])
	state.Binding.RouteGeneration = binary.LittleEndian.Uint64(src[112:120])
	copy(state.Binding.ClusterID[:], src[120:136])
	copy(state.Binding.ClusterIncarnation[:], src[136:152])
	copy(state.Binding.ShardIncarnation[:], src[152:168])
	copy(state.Binding.GroupID[:], src[168:184])
	copy(state.LastEntryDigest[:], src[184:216])
	copy(state.DataChainDigest[:], src[216:248])
	copy(state.ApplyContractDigest[:], src[248:280])
	copy(state.BootstrapDigest[:], src[280:312])
	copy(state.SnapshotBaseDigest[:], src[312:344])
	state.SessionSlotCount = binary.LittleEndian.Uint64(src[352:360])
	state.SessionEpochHighWater = binary.LittleEndian.Uint64(src[360:368])
	state.AuthorityBindingCount = binary.LittleEndian.Uint64(src[368:376])
	copy(state.Binding.OwnedRange.Start[:], src[376:384])
	copy(state.Binding.OwnedRange.End.Point[:], src[384:392])
	if src[392] > 1 || !zeroBytes(src[393:400]) {
		return State{}, fmt.Errorf("%w: ownership range", ErrStateCorrupt)
	}
	state.Binding.OwnedRange.End.Max = src[392] == 1
	if headerBytes >= stateTransactionHeaderBytes {
		state.TransactionControlCount = binary.LittleEndian.Uint64(src[400:408])
		state.ActiveTransactionCount = binary.LittleEndian.Uint64(src[408:416])
		state.TransactionPayloadRows = binary.LittleEndian.Uint64(src[416:424])
		state.TransactionIntentRows = binary.LittleEndian.Uint64(src[424:432])
		state.TransactionResidentBytes = binary.LittleEndian.Uint64(src[432:440])
		if headerBytes == stateTransactionHeaderBytes && !stateHasTransactions(state) {
			return State{}, fmt.Errorf("%w: noncanonical empty transaction extension", ErrStateCorrupt)
		}
	}
	if headerBytes >= stateRequestLedgerHeaderBytes {
		state.RequestLedgerRows = binary.LittleEndian.Uint64(src[440:448])
		state.RequestLedgerResidentBytes = binary.LittleEndian.Uint64(src[448:456])
		state.RequestLedgerReservedBytes = binary.LittleEndian.Uint64(src[456:464])
		state.RequestLedgerAckRows = binary.LittleEndian.Uint64(src[464:472])
		state.RequestLedgerAckBytes = binary.LittleEndian.Uint64(src[472:480])
		if headerBytes == stateRequestLedgerHeaderBytes && !stateHasRequestLedger(state) {
			return State{}, fmt.Errorf("%w: noncanonical empty request-ledger extension", ErrStateCorrupt)
		}
	}
	if headerBytes >= stateExecutionPinHeaderBytes {
		state.ExecutionPinRecordCount = binary.LittleEndian.Uint64(src[480:488])
		state.ActiveExecutionPinCount = binary.LittleEndian.Uint64(src[488:496])
		state.ExecutionPinResidentBytes = binary.LittleEndian.Uint64(src[496:504])
		if headerBytes == stateExecutionPinHeaderBytes && !stateHasExecutionPins(state) {
			return State{}, fmt.Errorf("%w: noncanonical empty execution-pin extension", ErrStateCorrupt)
		}
	}
	if headerBytes == stateRelationPlacementHeaderBytes {
		copy(state.RelationPlacementDigest[:], src[504:536])
		if !stateHasRelationPlacement(state) {
			return State{}, fmt.Errorf("%w: empty relation placement extension", ErrStateCorrupt)
		}
	}
	cursor := headerBytes
	state.Binding.Distribution = string(src[cursor : cursor+distributionLen])
	cursor += distributionLen
	state.Binding.Shard = string(src[cursor : cursor+shardLen])
	cursor += shardLen
	confBytes := src[cursor : cursor+confLen]
	conf := new(pb.ConfState)
	if err := proto.Unmarshal(confBytes, conf); err != nil || len(conf.ProtoReflect().GetUnknown()) != 0 {
		return State{}, fmt.Errorf("%w: ConfState", ErrStateCorrupt)
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(conf)
	if err != nil || !bytes.Equal(canonical, confBytes) {
		return State{}, fmt.Errorf("%w: noncanonical ConfState", ErrStateCorrupt)
	}
	state.ConfState = conf
	if err := validateState(state); err != nil {
		return State{}, err
	}
	return state, nil
}

func validateState(state State) error {
	if err := state.Binding.validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrStateCorrupt, err)
	}
	if state.Applied == 0 || state.Applied == math.MaxUint64 ||
		state.LastTerm == 0 || state.LastTerm == math.MaxUint64 || state.ConfState == nil ||
		state.ReplicaSetVersion == 0 || state.ReplicaSetVersion > state.Applied ||
		state.ReplicaSetVersion == math.MaxUint64 || state.SessionCount > state.Applied-1 ||
		state.SessionSlotCount > state.Applied-1 ||
		state.SessionEpochHighWater > state.Applied ||
		state.AuthorityBindingCount > state.Applied-1 ||
		state.SessionCount > MaxRetainedSessions ||
		state.AuthorityBindingCount > MaxRetainedSessions ||
		state.SessionCount > state.AuthorityBindingCount ||
		state.SessionSlotCount > state.SessionCount*MaxSessionRetryWindow ||
		state.DataChainDigest == ([32]byte{}) || state.ApplyContractDigest == ([32]byte{}) ||
		state.SnapshotBaseDigest == ([32]byte{}) {
		return fmt.Errorf("%w: invalid state scalar", ErrStateCorrupt)
	}
	if !validStateTransactionCounters(state) {
		return fmt.Errorf("%w: invalid transaction counters", ErrStateCorrupt)
	}
	if !validStateRequestLedgerCounters(state) {
		return fmt.Errorf("%w: invalid request-ledger counters", ErrStateCorrupt)
	}
	if !validStateExecutionPinCounters(state) {
		return fmt.Errorf("%w: invalid execution-pin counters", ErrStateCorrupt)
	}
	if len(state.ConfState.ProtoReflect().GetUnknown()) != 0 {
		return fmt.Errorf("%w: unknown ConfState fields", ErrStateCorrupt)
	}
	if err := raftmodel.ValidateConfState(state.ConfState, state.Applied); err != nil {
		return fmt.Errorf("%w: invalid ConfState: %v", ErrStateCorrupt, err)
	}
	switch state.LastKind {
	case RecordStaticSnapshot:
		if state.LastEntryType != pb.EntryNormal || state.Applied != 1 ||
			state.LastTerm != 1 || state.ReplicaSetVersion != 1 || state.SessionCount != 0 ||
			state.SessionSlotCount != 0 || state.SessionEpochHighWater != 0 ||
			state.AuthorityBindingCount != 0 || stateHasTransactions(state) ||
			stateHasRequestLedger(state) ||
			stateHasExecutionPins(state) ||
			state.LastEntryDigest != state.BootstrapDigest {
			return fmt.Errorf("%w: invalid static snapshot state", ErrStateCorrupt)
		}
	case RecordNormal:
		if state.LastEntryType != pb.EntryNormal || state.Applied <= 1 ||
			state.ReplicaSetVersion >= state.Applied {
			return fmt.Errorf("%w: normal entry type", ErrStateCorrupt)
		}
	case RecordConfiguration:
		if state.Applied <= 1 ||
			(state.LastEntryType != pb.EntryConfChange && state.LastEntryType != pb.EntryConfChangeV2) ||
			state.ReplicaSetVersion != state.Applied {
			return fmt.Errorf("%w: configuration entry type", ErrStateCorrupt)
		}
		want, err := configurationEntryDigest(raftmodel.ApplyMeta{
			Index: state.Applied, Term: state.LastTerm, Type: state.LastEntryType,
		}, state.ConfState)
		if err != nil || state.LastEntryDigest != want {
			return fmt.Errorf("%w: configuration entry digest", ErrStateCorrupt)
		}
	case RecordOwnership:
		if state.LastEntryType != pb.EntryNormal || state.Applied <= 1 ||
			state.ReplicaSetVersion >= state.Applied {
			return fmt.Errorf("%w: ownership entry type", ErrStateCorrupt)
		}
	case RecordSchema:
		if state.LastEntryType != pb.EntryNormal || state.Applied <= 1 ||
			state.ReplicaSetVersion >= state.Applied {
			return fmt.Errorf("%w: schema entry type", ErrStateCorrupt)
		}
	case RecordImportedSnapshot:
		if state.LastEntryType != pb.EntryNormal || state.Applied <= 1 ||
			state.ReplicaSetVersion >= state.Applied || state.SessionCount != 0 ||
			state.SessionSlotCount != 0 || state.SessionEpochHighWater != state.Applied ||
			state.AuthorityBindingCount != 0 ||
			state.LastEntryDigest == ([sha256.Size]byte{}) {
			return fmt.Errorf("%w: imported snapshot state", ErrStateCorrupt)
		}
	default:
		return fmt.Errorf("%w: record kind", ErrStateCorrupt)
	}
	return nil
}

func sealRecord(frame, domain []byte) {
	h := sha256.New()
	_, _ = h.Write(domain)
	_, _ = h.Write(frame[:len(frame)-recordChecksumLen])
	_ = h.Sum(frame[len(frame)-recordChecksumLen : len(frame)-recordChecksumLen])
}

func verifyRecord(frame, domain []byte) bool {
	if len(frame) < recordChecksumLen {
		return false
	}
	h := sha256.New()
	_, _ = h.Write(domain)
	_, _ = h.Write(frame[:len(frame)-recordChecksumLen])
	var want [sha256.Size]byte
	_ = h.Sum(want[:0])
	return bytes.Equal(want[:], frame[len(frame)-recordChecksumLen:])
}

func allZero(src []byte) bool {
	for _, b := range src {
		if b != 0 {
			return false
		}
	}
	return true
}

func cloneConfState(state *pb.ConfState) *pb.ConfState {
	if state == nil {
		return nil
	}
	return proto.Clone(state).(*pb.ConfState)
}

func equalStatePublication(state State, applied uint64, digest [32]byte, conf *pb.ConfState, version uint64) bool {
	return state.Applied == applied && state.DataChainDigest == digest &&
		state.ReplicaSetVersion == version && proto.Equal(state.ConfState, conf)
}

func cloneState(state State) State {
	state.ConfState = cloneConfState(state.ConfState)
	return state
}

func equalState(left, right State) bool {
	return left.Binding == right.Binding && left.Applied == right.Applied &&
		left.LastTerm == right.LastTerm && left.LastKind == right.LastKind &&
		left.LastEntryType == right.LastEntryType &&
		left.LastEntryDigest == right.LastEntryDigest &&
		left.DataChainDigest == right.DataChainDigest &&
		left.ApplyContractDigest == right.ApplyContractDigest &&
		left.ReplicaSetVersion == right.ReplicaSetVersion &&
		left.BootstrapDigest == right.BootstrapDigest &&
		left.SnapshotBaseDigest == right.SnapshotBaseDigest &&
		left.SessionCount == right.SessionCount &&
		left.SessionSlotCount == right.SessionSlotCount &&
		left.SessionEpochHighWater == right.SessionEpochHighWater &&
		left.AuthorityBindingCount == right.AuthorityBindingCount &&
		left.TransactionControlCount == right.TransactionControlCount &&
		left.ActiveTransactionCount == right.ActiveTransactionCount &&
		left.TransactionPayloadRows == right.TransactionPayloadRows &&
		left.TransactionIntentRows == right.TransactionIntentRows &&
		left.TransactionResidentBytes == right.TransactionResidentBytes &&
		left.RequestLedgerRows == right.RequestLedgerRows &&
		left.RequestLedgerResidentBytes == right.RequestLedgerResidentBytes &&
		left.RequestLedgerReservedBytes == right.RequestLedgerReservedBytes &&
		left.RequestLedgerAckRows == right.RequestLedgerAckRows &&
		left.RequestLedgerAckBytes == right.RequestLedgerAckBytes &&
		left.ExecutionPinRecordCount == right.ExecutionPinRecordCount &&
		left.ActiveExecutionPinCount == right.ActiveExecutionPinCount &&
		left.ExecutionPinResidentBytes == right.ExecutionPinResidentBytes &&
		left.RelationPlacementDigest == right.RelationPlacementDigest &&
		proto.Equal(left.ConfState, right.ConfState)
}

func stateHasRelationPlacement(state State) bool {
	return state.RelationPlacementDigest != ([sha256.Size]byte{})
}

func stateHasRequestLedger(state State) bool {
	return state.RequestLedgerRows != 0 || state.RequestLedgerResidentBytes != 0 ||
		state.RequestLedgerReservedBytes != 0 || state.RequestLedgerAckRows != 0 ||
		state.RequestLedgerAckBytes != 0
}

func validStateRequestLedgerCounters(state State) bool {
	if !stateHasRequestLedger(state) {
		return true
	}
	// Every row has at least the fixed 34-byte request-ledger key and a
	// checksummed value. Exact key+value accounting is verified at reopen.
	const minimumResidentBytes = uint64(34 + 4)
	if state.RequestLedgerRows == 0 || state.RequestLedgerResidentBytes == 0 ||
		state.RequestLedgerRows > math.MaxUint64/minimumResidentBytes ||
		state.RequestLedgerResidentBytes < state.RequestLedgerRows*minimumResidentBytes ||
		state.RequestLedgerResidentBytes > math.MaxUint64-state.RequestLedgerReservedBytes {
		return false
	}
	if state.RequestLedgerAckRows > state.RequestLedgerRows ||
		state.RequestLedgerAckBytes > state.RequestLedgerResidentBytes ||
		(state.RequestLedgerAckRows == 0) != (state.RequestLedgerAckBytes == 0) {
		return false
	}
	return state.RequestLedgerRows != 0 && state.RequestLedgerResidentBytes != 0 &&
		state.RequestLedgerRows <= math.MaxUint64/minimumResidentBytes &&
		state.RequestLedgerResidentBytes >= state.RequestLedgerRows*minimumResidentBytes
}

func stateHasTransactions(state State) bool {
	return state.TransactionControlCount != 0 || state.ActiveTransactionCount != 0 ||
		state.TransactionPayloadRows != 0 || state.TransactionIntentRows != 0 ||
		state.TransactionResidentBytes != 0
}

func validStateTransactionCounters(state State) bool {
	if !stateHasTransactions(state) {
		return true
	}
	if state.TransactionControlCount == 0 || state.TransactionControlCount > state.Applied-1 ||
		state.TransactionControlCount > MaxRetainedTransactions ||
		state.ActiveTransactionCount > state.TransactionControlCount ||
		state.TransactionResidentBytes == 0 ||
		state.TransactionPayloadRows > state.TransactionResidentBytes ||
		state.TransactionIntentRows > state.TransactionResidentBytes ||
		state.TransactionPayloadRows > math.MaxUint64-state.TransactionIntentRows {
		return false
	}
	minimumControlBytes := uint64(transactionControlStorageKeyBytes +
		transactionControlHeaderBytes + recordChecksumLen)
	if state.TransactionControlCount > math.MaxUint64/minimumControlBytes ||
		state.TransactionResidentBytes < state.TransactionControlCount*minimumControlBytes {
		return false
	}
	return state.TransactionControlCount <= math.MaxUint64/MaxTransactionResidentBytes &&
		state.TransactionResidentBytes <= state.TransactionControlCount*MaxTransactionResidentBytes
}

func stateHasExecutionPins(state State) bool {
	return state.ExecutionPinRecordCount != 0 || state.ActiveExecutionPinCount != 0 ||
		state.ExecutionPinResidentBytes != 0
}

func validStateExecutionPinCounters(state State) bool {
	if !stateHasExecutionPins(state) {
		return true
	}
	if state.ExecutionPinRecordCount == 0 ||
		state.ExecutionPinRecordCount > state.Applied-1 ||
		state.ExecutionPinRecordCount > MaxRetainedExecutionPins ||
		state.ActiveExecutionPinCount > state.ExecutionPinRecordCount {
		return false
	}
	const recordResident = uint64(executionPinRecordStorageKeyBytes + executionpin.RecordBytes)
	const activeResident = uint64(executionPinActiveStorageKeyBytes + executionPinActiveValueBytes)
	if state.ExecutionPinRecordCount > math.MaxUint64/recordResident ||
		state.ActiveExecutionPinCount > math.MaxUint64/activeResident {
		return false
	}
	recordBytes := state.ExecutionPinRecordCount * recordResident
	activeBytes := state.ActiveExecutionPinCount * activeResident
	return recordBytes <= math.MaxUint64-activeBytes &&
		state.ExecutionPinResidentBytes == recordBytes+activeBytes
}

func stateSystemRowCount(state State) (uint64, bool) {
	values := [...]uint64{
		1, state.SessionCount, state.SessionSlotCount, state.AuthorityBindingCount,
		state.TransactionControlCount, state.TransactionPayloadRows, state.TransactionIntentRows,
		state.RequestLedgerRows,
		state.ExecutionPinRecordCount, state.ActiveExecutionPinCount,
	}
	var total uint64
	for _, value := range values {
		if total > math.MaxUint64-value {
			return 0, false
		}
		total += value
	}
	return total, true
}

func equalStateExceptSnapshotBaseDigest(left, right State) bool {
	left.SnapshotBaseDigest = right.SnapshotBaseDigest
	return equalState(left, right)
}
