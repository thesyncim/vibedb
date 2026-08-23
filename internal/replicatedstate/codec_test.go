package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	_resultAppliedAtLeastExact           = ResultApplied - 1
	_resultAppliedAtMostExact            = uint32(1) - ResultApplied
	_resultStaleFenceAtLeastExact        = ResultStaleFence - 2
	_resultStaleFenceAtMostExact         = uint32(2) - ResultStaleFence
	_resultUnknownCollectionAtLeastExact = ResultUnknownCollection - 3
	_resultUnknownCollectionAtMostExact  = uint32(3) - ResultUnknownCollection
	_resultInvalidDocumentAtLeastExact   = ResultInvalidDocument - 4
	_resultInvalidDocumentAtMostExact    = uint32(4) - ResultInvalidDocument
	_resultTargetBoundAtLeastExact       = ResultTargetBound - 5
	_resultTargetBoundAtMostExact        = uint32(5) - ResultTargetBound
	_resultWrongShardAtLeastExact        = ResultWrongShard - 6
	_resultWrongShardAtMostExact         = uint32(6) - ResultWrongShard
	_resultSessionRetiredAtLeastExact    = ResultSessionRetired - 7
	_resultSessionRetiredAtMostExact     = uint32(7) - ResultSessionRetired
)

func codecState() State {
	dataChain := sha256.Sum256([]byte("data-chain"))
	contract := sha256.Sum256([]byte("apply-contract"))
	bootstrap := sha256.Sum256([]byte("bootstrap"))
	return State{
		Binding: testBinding(), Applied: 1, LastTerm: 1,
		LastKind: RecordStaticSnapshot, LastEntryType: pb.EntryNormal,
		LastEntryDigest: bootstrap, DataChainDigest: dataChain,
		ApplyContractDigest: contract,
		ConfState:           &pb.ConfState{Voters: []uint64{1}}, ReplicaSetVersion: 1,
		BootstrapDigest: bootstrap, SnapshotBaseDigest: bootstrap,
	}
}

func TestStateRoundTripGoldenAndStrictness(t *testing.T) {
	state := codecState()
	encoded, err := AppendState(nil, state)
	if err != nil {
		t.Fatal(err)
	}
	const wantDigest = "1455b8b87530344c39f7ccf0104e287e1f28752c4e1515d01c249ddf8e2717d2"
	gotDigest := sha256.Sum256(encoded)
	if hex.EncodeToString(gotDigest[:]) != wantDigest {
		t.Fatalf("state golden digest = %x, want %s", gotDigest, wantDigest)
	}
	decoded, err := OpenState(encoded)
	if err != nil || !equalState(decoded, state) {
		t.Fatalf("OpenState = %+v,%v", decoded, err)
	}
	corrupt := bytes.Clone(encoded)
	corrupt[216] ^= 1
	if _, err := OpenState(corrupt); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("corrupt state error = %v", err)
	}
	unknown := codecState()
	unknown.ConfState.ProtoReflect().SetUnknown([]byte{0x78, 0})
	if _, err := AppendState(nil, unknown); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("unknown ConfState error = %v", err)
	}
	duplicate := codecState()
	duplicate.ConfState.Voters = []uint64{1, 1}
	if _, err := AppendState(nil, duplicate); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("duplicate voter error = %v", err)
	}
}

func TestStateSessionEpochHighWaterRoundTripAndBounds(t *testing.T) {
	state := codecState()
	state.Applied = 3
	state.LastTerm = 2
	state.LastKind = RecordNormal
	state.LastEntryDigest = sha256.Sum256([]byte("normal-entry"))
	state.SessionEpochHighWater = state.Applied

	encoded, err := AppendState(nil, state)
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint64(encoded[360:368]); got != state.SessionEpochHighWater {
		t.Fatalf("encoded session epoch high-water = %d, want %d", got, state.SessionEpochHighWater)
	}
	decoded, err := OpenState(encoded)
	if err != nil || !equalState(decoded, state) {
		t.Fatalf("OpenState = %+v,%v", decoded, err)
	}

	tooHigh := state
	tooHigh.SessionEpochHighWater = tooHigh.Applied + 1
	if _, err := AppendState(nil, tooHigh); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("high-water beyond Applied error = %v", err)
	}
	corruptHighWater := bytes.Clone(encoded)
	binary.LittleEndian.PutUint64(corruptHighWater[360:368], state.Applied+1)
	sealRecord(corruptHighWater, stateChecksumDomain)
	if _, err := OpenState(corruptHighWater); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("decoded high-water beyond Applied error = %v", err)
	}

	static := codecState()
	static.SessionEpochHighWater = 1
	if _, err := AppendState(nil, static); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("static high-water error = %v", err)
	}
}

func TestStateEnvelopeBoundCoversMaximumConfiguration(t *testing.T) {
	state := codecState()
	const firstMember = uint64(math.MaxUint64 - 1024)
	state.ConfState = &pb.ConfState{Voters: []uint64{firstMember}}
	state.ConfState.Learners = make([]uint64, raftmodel.MaxConfStateMembers-1)
	for index := range state.ConfState.Learners {
		state.ConfState.Learners[index] = firstMember + uint64(index) + 1
	}
	encoded, err := AppendState(nil, state)
	if err != nil {
		t.Fatalf("maximum configuration state: %v", err)
	}
	if len(encoded) > MaxStateEnvelopeBytes {
		t.Fatalf("state envelope = %d, bound = %d", len(encoded), MaxStateEnvelopeBytes)
	}
}

func TestRawCodecLengthFieldsCannotOverflowInt(t *testing.T) {
	state := make([]byte, stateHeaderBytes+recordChecksumLen)
	copy(state[:8], stateMagic[:])
	binary.LittleEndian.PutUint16(state[8:10], stateCodecFormat)
	binary.LittleEndian.PutUint16(state[12:14], stateHeaderBytes)
	binary.LittleEndian.PutUint32(state[16:20], uint32(len(state)))
	binary.LittleEndian.PutUint32(state[20:24], 0)
	binary.LittleEndian.PutUint32(state[348:352], math.MaxUint32)
	sealRecord(state, stateChecksumDomain)
	if _, err := OpenState(state); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("maximum ConfState length error = %v", err)
	}

	session := make([]byte, sessionRecordHeaderBytes+1+recordChecksumLen)
	copy(session[:8], sessionRecordMagic[:])
	binary.LittleEndian.PutUint16(session[8:10], sessionRecordCodecFormat)
	binary.LittleEndian.PutUint16(session[10:12], sessionRecordHeaderBytes)
	binary.LittleEndian.PutUint32(session[12:16], uint32(len(session)))
	binary.LittleEndian.PutUint32(session[16:20], math.MaxUint32)
	binary.LittleEndian.PutUint16(session[26:28], 1)
	sealRecord(session, sessionRecordChecksumDomain)
	if _, err := OpenSessionRecord(session); !errors.Is(err, ErrSessionCorrupt) {
		t.Fatalf("maximum session body length error = %v", err)
	}

}

func TestStateRejectsResealedReconstructibleDigestMismatch(t *testing.T) {
	static := codecState()
	configuration := codecState()
	configuration.Applied = 2
	configuration.LastTerm = 2
	configuration.LastKind = RecordConfiguration
	configuration.LastEntryType = pb.EntryConfChange
	configuration.ReplicaSetVersion = 2
	var err error
	configuration.LastEntryDigest, err = configurationEntryDigest(raftmodel.ApplyMeta{
		Index: 2, Term: 2, Type: pb.EntryConfChange,
	}, configuration.ConfState)
	if err != nil {
		t.Fatal(err)
	}

	for name, state := range map[string]State{
		"static": static, "configuration": configuration,
	} {
		t.Run(name, func(t *testing.T) {
			encoded, err := AppendState(nil, state)
			if err != nil {
				t.Fatal(err)
			}
			encoded[184] ^= 1
			sealRecord(encoded, stateChecksumDomain)
			if _, err := OpenState(encoded); !errors.Is(err, ErrStateCorrupt) {
				t.Fatalf("OpenState error = %v", err)
			}
		})
	}
}

func TestStateRejectsResealedUnsortedConfStateMembers(t *testing.T) {
	state := codecState()
	state.ConfState.Voters = []uint64{1, 2}
	encoded, err := AppendState(nil, state)
	if err != nil {
		t.Fatal(err)
	}
	unsorted := &pb.ConfState{Voters: []uint64{2, 1}}
	conf, err := proto.MarshalOptions{Deterministic: true}.Marshal(unsorted)
	if err != nil {
		t.Fatal(err)
	}
	distributionLen := int(binary.LittleEndian.Uint16(encoded[344:346]))
	shardLen := int(binary.LittleEndian.Uint16(encoded[346:348]))
	confLen := int(binary.LittleEndian.Uint32(encoded[348:352]))
	if len(conf) != confLen {
		t.Fatalf("unsorted ConfState length = %d, want %d", len(conf), confLen)
	}
	start := stateHeaderBytes + distributionLen + shardLen
	copy(encoded[start:start+confLen], conf)
	sealRecord(encoded, stateChecksumDomain)
	if _, err := OpenState(encoded); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("OpenState error = %v", err)
	}
}

func codecLogicalCommand() replication.Command {
	binding := testBinding()
	return replication.Command{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: binding.ActivePolicyGeneration,
		ProtectionEpoch: binding.ProtectionEpoch, OwnershipEpoch: binding.OwnershipEpoch,
		SchemaGeneration: binding.SchemaGeneration, RoutingVersion: binding.RoutingVersion,
		RouteGeneration: binding.RouteGeneration, Tenant: []byte("tenant"),
		ClientID: id128(44), ClientEpoch: 2, ClientSequence: 3, AckThrough: 1,
		Fingerprint: sha256.Sum256([]byte("logical-fingerprint")),
		RetryHome:   replication.RetryHome{7, 6, 5, 4, 3, 2, 1, 0},
		Collection:  "docs",
		Mutations: []replication.Mutation{
			{Kind: replication.MutationPut, Key: []byte("k"), Value: []byte(`{"n":1}`)},
			{Kind: replication.MutationDelete, Key: []byte("z")},
		},
	}
}

func openCodecLogicalCommand(t testing.TB, command replication.Command) replication.CommandView {
	t.Helper()
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatalf("AppendCommand: %v", err)
	}
	view, err := replication.OpenCommand(encoded)
	if err != nil {
		t.Fatalf("OpenCommand: %v", err)
	}
	return view
}

func TestSessionCodecRoundTripAndFixedGrammar(t *testing.T) {
	if ValidationOpaqueBinary != 1 || ValidationDeterministicMutation != 2 ||
		ResultFormatMutation != 1 ||
		ResultApplied != 1 || ResultStaleFence != 2 || ResultUnknownCollection != 3 ||
		ResultInvalidDocument != 4 || ResultTargetBound != 5 || ResultWrongShard != 6 ||
		ResultSessionRetired != 7 || ResultSessionOpened != 8 {
		t.Fatalf("durable validation/result grammar drifted: profiles=%d,%d format=%d codes=%d,%d,%d,%d,%d,%d,%d,%d",
			ValidationOpaqueBinary, ValidationDeterministicMutation,
			ResultFormatMutation,
			ResultApplied, ResultStaleFence, ResultUnknownCollection,
			ResultInvalidDocument, ResultTargetBound, ResultWrongShard,
			ResultSessionRetired, ResultSessionOpened)
	}
	record := sessionCodecRecord()
	encoded, err := AppendSessionRecord(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := OpenSessionRecord(encoded)
	if err != nil || decoded.Digest != SessionKey(record.Tenant, record.ClientID) ||
		!bytes.Equal(decoded.Tenant, record.Tenant) || decoded.ClientID != record.ClientID ||
		decoded.Bytes()[0] != encoded[0] {
		t.Fatalf("OpenSessionRecord = %+v,%v", decoded, err)
	}

	slot := sessionCodecSlot(t)
	encoded, err = AppendSessionSlot(nil, slot)
	if err != nil {
		t.Fatal(err)
	}
	decodedSlot, err := OpenSessionSlot(encoded)
	if err != nil || decodedSlot.SessionDigest != slot.SessionDigest ||
		decodedSlot.LogicalCommandDigest != slot.LogicalCommandDigest ||
		decodedSlot.AppliedSequence != slot.AppliedSequence ||
		decodedSlot.ResultCode != slot.ResultCode || decodedSlot.Bytes()[0] != encoded[0] {
		t.Fatalf("OpenSessionSlot = %+v,%v", decodedSlot, err)
	}
}

func TestSessionKeyAndLogicalCommandDigestGolden(t *testing.T) {
	key := SessionKey([]byte("tenant"), id128(9))
	const wantKey = "5dc75da7b53d9acda39520cbf89cb7a71bdfaba3a76a84a41e8dee27eb8bfefe"
	if got := hex.EncodeToString(key[:]); got != wantKey {
		t.Fatalf("SessionKey = %s, want %s", got, wantKey)
	}

	command := codecLogicalCommand()
	digest := LogicalCommandDigest(openCodecLogicalCommand(t, command))
	const wantLogical = "e18f0764424a1e06df37d6a99af5308b6b0397cc113f04a35be2094bb1c7235a"
	if got := hex.EncodeToString(digest[:]); got != wantLogical {
		t.Fatalf("LogicalCommandDigest = %s, want %s", got, wantLogical)
	}
	refreshed := command
	refreshed.AckThrough = 2
	refreshed.AllocationGeneration++
	refreshed.RoutingVersion++
	refreshed.RouteGeneration++
	if got := LogicalCommandDigest(openCodecLogicalCommand(t, refreshed)); got != digest {
		t.Fatalf("logical digest changed across acknowledgement/fence refresh: %x != %x", got, digest)
	}
	changed := command
	changed.Mutations = append([]replication.Mutation(nil), command.Mutations...)
	changed.Mutations[0].Key = []byte("other")
	if got := LogicalCommandDigest(openCodecLogicalCommand(t, changed)); got == digest {
		t.Fatal("logical digest did not bind exact mutation bytes")
	}
}

func TestCodecAppendErrorsLeaveDestinationUnchanged(t *testing.T) {
	dst := make([]byte, 3, 1<<10)
	copy(dst, "pre")
	before := bytes.Clone(dst)
	invalidState := codecState()
	invalidState.Applied = 0
	got, err := AppendState(dst, invalidState)
	if !errors.Is(err, ErrStateCorrupt) || !bytes.Equal(got, before) {
		t.Fatalf("AppendState = %q,%v", got, err)
	}
	invalidSession := sessionCodecRecord()
	invalidSession.RetryWindow = 0
	got, err = AppendSessionRecord(dst, invalidSession)
	if !errors.Is(err, ErrSessionCorrupt) || !bytes.Equal(got, before) {
		t.Fatalf("AppendSessionRecord = %q,%v", got, err)
	}
	invalidSlot := sessionCodecSlot(t)
	invalidSlot.LogicalCommandDigest = [sha256.Size]byte{}
	got, err = AppendSessionSlot(dst, invalidSlot)
	if !errors.Is(err, ErrSessionCorrupt) || !bytes.Equal(got, before) {
		t.Fatalf("AppendSessionSlot = %q,%v", got, err)
	}
}

func TestCodecWritableAliasesAreRejectedAndRelocationAliasesAreAllowed(t *testing.T) {
	t.Run("state writable string", func(t *testing.T) {
		state := codecState()
		safe, err := AppendState(nil, state)
		if err != nil {
			t.Fatal(err)
		}
		dst := make([]byte, 3, 3+len(safe))
		copy(dst, "pre")
		full := dst[:cap(dst)]
		copy(full[3:7], "dist")
		state.Binding.Distribution = unsafe.String(unsafe.SliceData(full[3:7]), 4)
		before := bytes.Clone(dst)
		got, err := AppendState(dst, state)
		if !errors.Is(err, ErrCodecAlias) || !bytes.Equal(got, before) {
			t.Fatalf("AppendState = %q,%v", got, err)
		}
	})
	t.Run("session writable tenant", func(t *testing.T) {
		record := sessionCodecRecord()
		safe, err := AppendSessionRecord(nil, record)
		if err != nil {
			t.Fatal(err)
		}
		dst := make([]byte, 3, 3+len(safe)+len(record.Tenant))
		copy(dst, "pre")
		full := dst[:cap(dst)]
		copy(full[3:3+len(record.Tenant)], record.Tenant)
		record.Tenant = full[3 : 3+len(record.Tenant)]
		before := bytes.Clone(dst)
		got, err := AppendSessionRecord(dst, record)
		if !errors.Is(err, ErrCodecAlias) || !bytes.Equal(got, before) {
			t.Fatalf("AppendSessionRecord = %q,%v", got, err)
		}
	})
	t.Run("relocation", func(t *testing.T) {
		state := codecState()
		statePrefix := make([]byte, 4, 4)
		copy(statePrefix, "dist")
		state.Binding.Distribution = unsafe.String(unsafe.SliceData(statePrefix), len(statePrefix))
		encoded, err := AppendState(statePrefix, state)
		if err != nil || !bytes.Equal(encoded[:4], []byte("dist")) {
			t.Fatalf("relocated state = %q,%v", encoded, err)
		}
		if _, err := OpenState(encoded[4:]); err != nil {
			t.Fatalf("relocated state decode: %v", err)
		}

		record := sessionCodecRecord()
		tenantPrefix := make([]byte, len(record.Tenant), len(record.Tenant))
		copy(tenantPrefix, record.Tenant)
		record.Tenant = tenantPrefix
		encoded, err = AppendSessionRecord(tenantPrefix, record)
		if err != nil || !bytes.Equal(encoded[:len(tenantPrefix)], []byte("tenant-a")) {
			t.Fatalf("relocated session = %q,%v", encoded, err)
		}
		if _, err := OpenSessionRecord(encoded[len(tenantPrefix):]); err != nil {
			t.Fatalf("relocated session decode: %v", err)
		}

		slot := sessionCodecSlot(t)
		slotPrefix := []byte("slot")
		encoded, err = AppendSessionSlot(slotPrefix, slot)
		if err != nil || !bytes.Equal(encoded[:len(slotPrefix)], slotPrefix) {
			t.Fatalf("relocated slot = %q,%v", encoded, err)
		}
		if _, err := OpenSessionSlot(encoded[len(slotPrefix):]); err != nil {
			t.Fatalf("relocated slot decode: %v", err)
		}
	})
}

func FuzzOpenState(f *testing.F) {
	seed, err := AppendState(nil, codecState())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxStateEnvelopeBytes+1 {
			data = data[:MaxStateEnvelopeBytes+1]
		}
		_, _ = OpenState(data)
	})
}

func FuzzOpenSessionRecord(f *testing.F) {
	seed, err := AppendSessionRecord(nil, sessionCodecRecord())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxSessionRecordBytes+1 {
			data = data[:MaxSessionRecordBytes+1]
		}
		_, _ = OpenSessionRecord(data)
	})
}

func FuzzOpenSessionSlot(f *testing.F) {
	seed, err := AppendSessionSlot(nil, sessionCodecSlot(f))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxSessionSlotRecordBytes+1 {
			data = data[:MaxSessionSlotRecordBytes+1]
		}
		_, _ = OpenSessionSlot(data)
	})
}

func FuzzOpenStateResealed(f *testing.F) {
	seed, err := AppendState(nil, codecState())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(uint32(0), byte(0))
	f.Add(uint32(284), byte(0xff))
	f.Fuzz(func(t *testing.T, offset uint32, value byte) {
		candidate := bytes.Clone(seed)
		at := int(offset % uint32(len(candidate)-recordChecksumLen))
		candidate[at] = value
		sealRecord(candidate, stateChecksumDomain)
		_, _ = OpenState(candidate)
	})
}

func FuzzOpenSessionRecordResealed(f *testing.F) {
	seed, err := AppendSessionRecord(nil, sessionCodecRecord())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(uint32(0), byte(0))
	f.Add(uint32(104), byte(0xff))
	f.Fuzz(func(t *testing.T, offset uint32, value byte) {
		candidate := bytes.Clone(seed)
		at := int(offset % uint32(len(candidate)-recordChecksumLen))
		candidate[at] = value
		sealRecord(candidate, sessionRecordChecksumDomain)
		_, _ = OpenSessionRecord(candidate)
	})
}

func FuzzOpenSessionSlotResealed(f *testing.F) {
	seed, err := AppendSessionSlot(nil, sessionCodecSlot(f))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(uint32(0), byte(0))
	f.Add(uint32(104), byte(0xff))
	f.Fuzz(func(t *testing.T, offset uint32, value byte) {
		candidate := bytes.Clone(seed)
		at := int(offset % uint32(len(candidate)-recordChecksumLen))
		candidate[at] = value
		sealRecord(candidate, sessionSlotChecksumDomain)
		_, _ = OpenSessionSlot(candidate)
	})
}
