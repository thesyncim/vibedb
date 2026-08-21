package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	stateCodecFormat  = uint16(1)
	stateHeaderBytes  = 328
	recordChecksumLen = sha256.Size
)

var (
	stateMagic          = [8]byte{'V', 'D', 'B', 'R', 'S', 'M', 0, 0}
	stateChecksumDomain = []byte("vibedb/replicated-state/state-checksum\x00")
	jsonHex             = []byte("0123456789abcdef")
)

// RecordKind identifies what produced the most recent persisted publication.
type RecordKind uint8

const (
	RecordStaticSnapshot RecordKind = 1
	RecordNormal         RecordKind = 2
	RecordConfiguration  RecordKind = 3
)

// State is the exact durable publication stored at the fixed state key.
type State struct {
	Binding           Binding
	Applied           uint64
	LastTerm          uint64
	LastKind          RecordKind
	LastEntryType     pb.EntryType
	LastEntryDigest   [32]byte
	LogicalDigest     [32]byte
	ConfState         *pb.ConfState
	ReplicaSetVersion uint64
	BootstrapDigest   [32]byte
	// SnapshotBaseDigest binds the exact Raft snapshot certificate that most
	// recently established this state-machine base. Normal ordered applies
	// preserve it; installing a newer certified base replaces it atomically.
	SnapshotBaseDigest [32]byte
	CompletionCount    uint64
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
	total := stateHeaderBytes + len(state.Binding.Distribution) +
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
	binary.LittleEndian.PutUint16(frame[12:14], stateHeaderBytes)
	binary.LittleEndian.PutUint32(frame[16:20], uint32(total))
	binary.LittleEndian.PutUint32(frame[20:24], uint32(total-stateHeaderBytes-recordChecksumLen))
	binary.LittleEndian.PutUint64(frame[24:32], state.Applied)
	binary.LittleEndian.PutUint64(frame[32:40], state.LastTerm)
	binary.LittleEndian.PutUint64(frame[40:48], state.ReplicaSetVersion)
	binary.LittleEndian.PutUint64(frame[48:56], state.CompletionCount)
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
	copy(frame[216:248], state.LogicalDigest[:])
	copy(frame[248:280], state.BootstrapDigest[:])
	copy(frame[280:312], state.SnapshotBaseDigest[:])
	binary.LittleEndian.PutUint16(frame[312:314], uint16(len(state.Binding.Distribution)))
	binary.LittleEndian.PutUint16(frame[314:316], uint16(len(state.Binding.Shard)))
	binary.LittleEndian.PutUint32(frame[316:320], uint32(len(conf)))
	cursor := stateHeaderBytes
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
	if !bytes.Equal(src[0:8], stateMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != stateCodecFormat ||
		binary.LittleEndian.Uint16(src[12:14]) != stateHeaderBytes ||
		binary.LittleEndian.Uint16(src[14:16]) != 0 || !allZero(src[320:328]) {
		return State{}, fmt.Errorf("%w: state header", ErrStateCorrupt)
	}
	total64 := uint64(binary.LittleEndian.Uint32(src[16:20]))
	body64 := uint64(binary.LittleEndian.Uint32(src[20:24]))
	if total64 != uint64(len(src)) ||
		body64 != uint64(len(src)-stateHeaderBytes-recordChecksumLen) ||
		!verifyRecord(src, stateChecksumDomain) {
		return State{}, fmt.Errorf("%w: state size or checksum", ErrStateCorrupt)
	}
	distributionLen64 := uint64(binary.LittleEndian.Uint16(src[312:314]))
	shardLen64 := uint64(binary.LittleEndian.Uint16(src[314:316]))
	confLen64 := uint64(binary.LittleEndian.Uint32(src[316:320]))
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
	state.CompletionCount = binary.LittleEndian.Uint64(src[48:56])
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
	copy(state.LogicalDigest[:], src[216:248])
	copy(state.BootstrapDigest[:], src[248:280])
	copy(state.SnapshotBaseDigest[:], src[280:312])
	cursor := stateHeaderBytes
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
		state.ReplicaSetVersion == math.MaxUint64 || state.CompletionCount > state.Applied-1 ||
		state.SnapshotBaseDigest == ([32]byte{}) {
		return fmt.Errorf("%w: invalid state scalar", ErrStateCorrupt)
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
			state.LastTerm != 1 || state.ReplicaSetVersion != 1 || state.CompletionCount != 0 ||
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
	want := h.Sum(nil)
	return bytes.Equal(want, frame[len(frame)-recordChecksumLen:])
}

func allZero(src []byte) bool {
	for _, b := range src {
		if b != 0 {
			return false
		}
	}
	return true
}

func wrapJSONHex(dst, raw []byte) []byte {
	dst = append(dst, '"')
	for _, b := range raw {
		dst = append(dst, jsonHex[b>>4], jsonHex[b&15])
	}
	dst = append(dst, '"')
	return dst
}

func unwrapJSONHex(src []byte, maxRaw int, category error) ([]byte, error) {
	if len(src) < 2 || src[0] != '"' || src[len(src)-1] != '"' ||
		(len(src)-2)&1 != 0 || (len(src)-2)/2 > maxRaw {
		return nil, fmt.Errorf("%w: noncanonical JSON wrapper", category)
	}
	raw := make([]byte, (len(src)-2)/2)
	for i := range raw {
		hi, ok := lowerHex(src[1+2*i])
		if !ok {
			return nil, fmt.Errorf("%w: noncanonical JSON wrapper", category)
		}
		lo, ok := lowerHex(src[2+2*i])
		if !ok {
			return nil, fmt.Errorf("%w: noncanonical JSON wrapper", category)
		}
		raw[i] = hi<<4 | lo
	}
	return raw, nil
}

func lowerHex(b byte) (byte, bool) {
	switch {
	case b >= '0' && b <= '9':
		return b - '0', true
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10, true
	default:
		return 0, false
	}
}

func cloneConfState(state *pb.ConfState) *pb.ConfState {
	if state == nil {
		return nil
	}
	return proto.Clone(state).(*pb.ConfState)
}

func equalStatePublication(state State, applied uint64, digest [32]byte, conf *pb.ConfState, version uint64) bool {
	return state.Applied == applied && state.LogicalDigest == digest &&
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
		left.LogicalDigest == right.LogicalDigest &&
		left.ReplicaSetVersion == right.ReplicaSetVersion &&
		left.BootstrapDigest == right.BootstrapDigest &&
		left.SnapshotBaseDigest == right.SnapshotBaseDigest &&
		left.CompletionCount == right.CompletionCount &&
		proto.Equal(left.ConfState, right.ConfState)
}

func equalStateExceptSnapshotBaseDigest(left, right State) bool {
	left.SnapshotBaseDigest = right.SnapshotBaseDigest
	return equalState(left, right)
}
