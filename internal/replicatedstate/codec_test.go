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
	const wantDigest = "073d8d565e6dfe0dd89b98261d5266762a67a0631c7ed62c33714a88b5c60786"
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
	wrapper := wrapJSONHex(nil, encoded)
	wrapper[1] = 'A'
	if _, err := unwrapJSONHex(wrapper, MaxStateEnvelopeBytes, ErrStateCorrupt); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("uppercase JSON hex error = %v", err)
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

func TestStateAndCompletionLengthFieldsCannotOverflowInt(t *testing.T) {
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

	completion := make([]byte, completionRecordHeaderBytes+recordChecksumLen)
	copy(completion[:8], completionRecordMagic[:])
	binary.LittleEndian.PutUint16(completion[8:10], completionRecordCodecFormat)
	binary.LittleEndian.PutUint16(completion[12:14], completionRecordHeaderBytes)
	binary.LittleEndian.PutUint32(completion[16:20], uint32(len(completion)))
	binary.LittleEndian.PutUint32(completion[20:24], 0)
	binary.LittleEndian.PutUint32(completion[132:136], math.MaxUint32)
	sealRecord(completion, completionRecordChecksumDomain)
	if _, err := OpenCompletionRecord(completion); !errors.Is(err, ErrCompletionCorrupt) {
		t.Fatalf("maximum completion length error = %v", err)
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

func codecCompletion(code uint32) ([]byte, CompletionRecord) {
	binding := testBinding()
	fingerprint := sha256.Sum256([]byte("fingerprint"))
	resultDigest := replication.CompletionResultDigest(code, ResultFormatMutation, nil)
	completion, err := replication.AppendCompletion(nil, replication.Completion{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: binding.ActivePolicyGeneration,
		ProtectionEpoch: binding.ProtectionEpoch, RoutingVersion: binding.RoutingVersion,
		RouteGeneration: binding.RouteGeneration, Tenant: []byte("tenant"),
		ClientID: id128(44), ClientEpoch: 2, ClientSequence: 3,
		Fingerprint: fingerprint, AppliedSequence: 4, ResultCode: code,
		ResultFormat: ResultFormatMutation, Storage: replication.CompletionInline,
		ResultDigest: resultDigest,
	})
	if err != nil {
		panic(err)
	}
	record := CompletionRecord{
		Tenant: []byte("tenant"), ClientID: id128(44), ClientEpoch: 2,
		ClientSequence: 3, Fingerprint: fingerprint,
		CommandDigest: sha256.Sum256([]byte("command")), Collection: "docs",
		Completion: completion,
	}
	return completion, record
}

func TestCompletionRecordRoundTripAndFixedGrammar(t *testing.T) {
	if ValidationSchemaFreeJSON != 1 || ValidationDeterministicMutation != 2 ||
		ResultFormatMutation != 1 ||
		ResultApplied != 1 || ResultStaleFence != 2 || ResultUnknownCollection != 3 ||
		ResultInvalidDocument != 4 || ResultTargetBound != 5 || ResultWrongShard != 6 {
		t.Fatalf("durable validation/result grammar drifted: profiles=%d,%d format=%d codes=%d,%d,%d,%d,%d,%d",
			ValidationSchemaFreeJSON, ValidationDeterministicMutation,
			ResultFormatMutation,
			ResultApplied, ResultStaleFence, ResultUnknownCollection,
			ResultInvalidDocument, ResultTargetBound, ResultWrongShard)
	}
	_, record := codecCompletion(ResultApplied)
	encoded, err := AppendCompletionRecord(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	const wantDigest = "d21396c86062885f7c5efc6f0686f3c164602d47c41acd2a8a629b9402ef5d05"
	if got := sha256.Sum256(encoded); hex.EncodeToString(got[:]) != wantDigest {
		t.Fatalf("completion record golden digest = %x, want %s", got, wantDigest)
	}
	decoded, err := OpenCompletionRecord(encoded)
	if err != nil || !bytes.Equal(decoded.Completion, record.Completion) ||
		!bytes.Equal(decoded.Tenant, record.Tenant) || decoded.Collection != record.Collection {
		t.Fatalf("OpenCompletionRecord = %+v,%v", decoded, err)
	}
	_, zeroCode := codecCompletion(0)
	if _, err := AppendCompletionRecord(nil, zeroCode); !errors.Is(err, ErrCompletionCorrupt) {
		t.Fatalf("zero result code error = %v", err)
	}
	wrongShardCompletion, wrongShard := codecCompletion(ResultWrongShard)
	wrongShardEncoded, err := AppendCompletionRecord(nil, wrongShard)
	if err != nil {
		t.Fatalf("wrong-shard grammar: %v", err)
	}
	const wantWrongShardCompletionDigest = "3f3696e22cc412db43999af1decc6311caf8b117f17263e98e71154ec033e788"
	if got := sha256.Sum256(wrongShardCompletion); hex.EncodeToString(got[:]) != wantWrongShardCompletionDigest {
		t.Fatalf("wrong-shard completion envelope golden digest = %x, want %s", got, wantWrongShardCompletionDigest)
	}
	const wantWrongShardRecordDigest = "d12136d0d8f1727e6f0b38f2c2e4b8c8d9da4a8876cefcb0e68bd2b64a1c7e55"
	if got := sha256.Sum256(wrongShardEncoded); hex.EncodeToString(got[:]) != wantWrongShardRecordDigest {
		t.Fatalf("wrong-shard completion record golden digest = %x, want %s", got, wantWrongShardRecordDigest)
	}
	_, unknown := codecCompletion(ResultWrongShard + 1)
	if _, err := AppendCompletionRecord(nil, unknown); !errors.Is(err, ErrCompletionCorrupt) {
		t.Fatalf("unknown result grammar error = %v", err)
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
	_, invalidCompletion := codecCompletion(0)
	got, err = AppendCompletionRecord(dst, invalidCompletion)
	if !errors.Is(err, ErrCompletionCorrupt) || !bytes.Equal(got, before) {
		t.Fatalf("AppendCompletionRecord = %q,%v", got, err)
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
	t.Run("completion writable slice", func(t *testing.T) {
		_, record := codecCompletion(ResultApplied)
		safe, err := AppendCompletionRecord(nil, record)
		if err != nil {
			t.Fatal(err)
		}
		dst := make([]byte, 3, 3+len(safe)+len(record.Completion))
		copy(dst, "pre")
		full := dst[:cap(dst)]
		copy(full[3:3+len(record.Completion)], record.Completion)
		record.Completion = full[3 : 3+len(record.Completion)]
		before := bytes.Clone(dst)
		got, err := AppendCompletionRecord(dst, record)
		if !errors.Is(err, ErrCodecAlias) || !bytes.Equal(got, before) {
			t.Fatalf("AppendCompletionRecord = %q,%v", got, err)
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

		_, record := codecCompletion(ResultApplied)
		completionPrefix := make([]byte, len(record.Tenant), len(record.Tenant))
		copy(completionPrefix, record.Tenant)
		record.Tenant = completionPrefix
		encoded, err = AppendCompletionRecord(completionPrefix, record)
		if err != nil || !bytes.Equal(encoded[:len(completionPrefix)], []byte("tenant")) {
			t.Fatalf("relocated completion = %q,%v", encoded, err)
		}
		if _, err := OpenCompletionRecord(encoded[len(completionPrefix):]); err != nil {
			t.Fatalf("relocated completion decode: %v", err)
		}
	})
}

func TestCompletionKeyGolden(t *testing.T) {
	key := CompletionKey([]byte("tenant"), id128(9), 7, 11)
	const want = "539b735a2ca83fe658e130c674cc1acd89e2c0e999320166f41cb3708e447e02"
	if hex.EncodeToString(key[:]) != want {
		t.Fatalf("CompletionKey = %x, want %s", key, want)
	}
	other := CompletionKey([]byte("tenant-2"), id128(9), 7, 11)
	if key == other {
		t.Fatal("different tuple produced the same test key")
	}
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

func FuzzOpenCompletionRecord(f *testing.F) {
	_, record := codecCompletion(ResultApplied)
	seed, err := AppendCompletionRecord(nil, record)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxCompletionRecordBytes+1 {
			data = data[:MaxCompletionRecordBytes+1]
		}
		_, _ = OpenCompletionRecord(data)
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

func FuzzOpenCompletionRecordResealed(f *testing.F) {
	_, record := codecCompletion(ResultApplied)
	seed, err := AppendCompletionRecord(nil, record)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(uint32(0), byte(0))
	f.Add(uint32(132), byte(0xff))
	f.Fuzz(func(t *testing.T, offset uint32, value byte) {
		candidate := bytes.Clone(seed)
		at := int(offset % uint32(len(candidate)-recordChecksumLen))
		candidate[at] = value
		sealRecord(candidate, completionRecordChecksumDomain)
		_, _ = OpenCompletionRecord(candidate)
	})
}
