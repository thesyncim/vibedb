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
	_resultAppliedAtLeastFrozen           = ResultApplied - 1
	_resultAppliedAtMostFrozen            = uint32(1) - ResultApplied
	_resultStaleFenceAtLeastFrozen        = ResultStaleFence - 2
	_resultStaleFenceAtMostFrozen         = uint32(2) - ResultStaleFence
	_resultUnknownCollectionAtLeastFrozen = ResultUnknownCollection - 3
	_resultUnknownCollectionAtMostFrozen  = uint32(3) - ResultUnknownCollection
	_resultInvalidDocumentAtLeastFrozen   = ResultInvalidDocument - 4
	_resultInvalidDocumentAtMostFrozen    = uint32(4) - ResultInvalidDocument
	_resultTargetBoundAtLeastFrozen       = ResultTargetBound - 5
	_resultTargetBoundAtMostFrozen        = uint32(5) - ResultTargetBound
)

func codecStateV1() StateV1 {
	logical := sha256.Sum256([]byte("logical"))
	bootstrap := sha256.Sum256([]byte("bootstrap"))
	return StateV1{
		Binding: testBinding(), Applied: 1, LastTerm: 1,
		LastKind: RecordStaticSnapshot, LastEntryType: pb.EntryNormal,
		LastEntryDigest: bootstrap, LogicalDigest: logical,
		ConfState: &pb.ConfState{Voters: []uint64{1}}, ReplicaSetVersion: 1,
		BootstrapDigest: bootstrap,
	}
}

func TestStateV1RoundTripGoldenAndStrictness(t *testing.T) {
	state := codecStateV1()
	encoded, err := AppendStateV1(nil, state)
	if err != nil {
		t.Fatal(err)
	}
	const wantDigest = "3243e0a6ed78a44df8dcb2ac928d1a90721c9d17dce8f5c352ea114b753da89a"
	gotDigest := sha256.Sum256(encoded)
	if hex.EncodeToString(gotDigest[:]) != wantDigest {
		t.Fatalf("state golden digest = %x, want %s", gotDigest, wantDigest)
	}
	decoded, err := OpenStateV1(encoded)
	if err != nil || !equalState(decoded, state) {
		t.Fatalf("OpenStateV1 = %+v,%v", decoded, err)
	}
	corrupt := bytes.Clone(encoded)
	corrupt[216] ^= 1
	if _, err := OpenStateV1(corrupt); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("corrupt state error = %v", err)
	}
	wrapper := wrapJSONHex(nil, encoded)
	wrapper[1] = 'A'
	if _, err := unwrapJSONHex(wrapper, MaxStateEnvelopeBytes, ErrStateCorrupt); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("uppercase JSON hex error = %v", err)
	}
	unknown := codecStateV1()
	unknown.ConfState.ProtoReflect().SetUnknown([]byte{0x78, 0})
	if _, err := AppendStateV1(nil, unknown); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("unknown ConfState error = %v", err)
	}
	duplicate := codecStateV1()
	duplicate.ConfState.Voters = []uint64{1, 1}
	if _, err := AppendStateV1(nil, duplicate); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("duplicate voter error = %v", err)
	}
}

func TestStateAndCompletionLengthFieldsCannotOverflowInt(t *testing.T) {
	state := make([]byte, stateHeaderBytes+recordChecksumLen)
	copy(state[:8], stateMagic[:])
	binary.LittleEndian.PutUint16(state[8:10], stateFormatV1)
	binary.LittleEndian.PutUint16(state[12:14], stateHeaderBytes)
	binary.LittleEndian.PutUint32(state[16:20], uint32(len(state)))
	binary.LittleEndian.PutUint32(state[20:24], 0)
	binary.LittleEndian.PutUint32(state[284:288], math.MaxUint32)
	sealRecord(state, stateChecksumDomain)
	if _, err := OpenStateV1(state); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("maximum ConfState length error = %v", err)
	}

	completion := make([]byte, completionRecordHeaderBytes+recordChecksumLen)
	copy(completion[:8], completionRecordMagic[:])
	binary.LittleEndian.PutUint16(completion[8:10], completionRecordFormatV1)
	binary.LittleEndian.PutUint16(completion[12:14], completionRecordHeaderBytes)
	binary.LittleEndian.PutUint32(completion[16:20], uint32(len(completion)))
	binary.LittleEndian.PutUint32(completion[20:24], 0)
	binary.LittleEndian.PutUint32(completion[132:136], math.MaxUint32)
	sealRecord(completion, completionRecordChecksumDomain)
	if _, err := OpenCompletionRecordV1(completion); !errors.Is(err, ErrCompletionCorrupt) {
		t.Fatalf("maximum completion length error = %v", err)
	}
}

func TestStateV1RejectsResealedReconstructibleDigestMismatch(t *testing.T) {
	static := codecStateV1()
	configuration := codecStateV1()
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

	for name, state := range map[string]StateV1{
		"static": static, "configuration": configuration,
	} {
		t.Run(name, func(t *testing.T) {
			encoded, err := AppendStateV1(nil, state)
			if err != nil {
				t.Fatal(err)
			}
			encoded[184] ^= 1
			sealRecord(encoded, stateChecksumDomain)
			if _, err := OpenStateV1(encoded); !errors.Is(err, ErrStateCorrupt) {
				t.Fatalf("OpenStateV1 error = %v", err)
			}
		})
	}
}

func TestStateV1RejectsResealedUnsortedConfStateMembers(t *testing.T) {
	state := codecStateV1()
	state.ConfState.Voters = []uint64{1, 2}
	encoded, err := AppendStateV1(nil, state)
	if err != nil {
		t.Fatal(err)
	}
	unsorted := &pb.ConfState{Voters: []uint64{2, 1}}
	conf, err := proto.MarshalOptions{Deterministic: true}.Marshal(unsorted)
	if err != nil {
		t.Fatal(err)
	}
	distributionLen := int(binary.LittleEndian.Uint16(encoded[280:282]))
	shardLen := int(binary.LittleEndian.Uint16(encoded[282:284]))
	confLen := int(binary.LittleEndian.Uint32(encoded[284:288]))
	if len(conf) != confLen {
		t.Fatalf("unsorted ConfState length = %d, want %d", len(conf), confLen)
	}
	start := stateHeaderBytes + distributionLen + shardLen
	copy(encoded[start:start+confLen], conf)
	sealRecord(encoded, stateChecksumDomain)
	if _, err := OpenStateV1(encoded); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("OpenStateV1 error = %v", err)
	}
}

func codecCompletionV1(code uint32) ([]byte, CompletionRecordV1) {
	return codecCompletion(ResultFormatMutationV1, code)
}

func codecCompletion(format uint16, code uint32) ([]byte, CompletionRecordV1) {
	binding := testBinding()
	fingerprint := sha256.Sum256([]byte("fingerprint"))
	resultDigest := replication.CompletionResultDigestV1(code, format, nil)
	completion, err := replication.AppendCompletionV1(nil, replication.CompletionV1{
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
		ResultFormat: format, Storage: replication.CompletionInline,
		ResultDigest: resultDigest,
	})
	if err != nil {
		panic(err)
	}
	record := CompletionRecordV1{
		Tenant: []byte("tenant"), ClientID: id128(44), ClientEpoch: 2,
		ClientSequence: 3, Fingerprint: fingerprint,
		CommandDigest: sha256.Sum256([]byte("command")), Collection: "docs",
		Completion: completion,
	}
	return completion, record
}

func TestCompletionRecordV1RoundTripAndFixedGrammar(t *testing.T) {
	if ValidationSchemaFreeJSONV1 != 1 || ValidationDeterministicMutationV1 != 2 ||
		ValidationDeterministicMutationV2 != 3 ||
		ResultFormatMutationV1 != 1 || ResultFormatMutationV2 != 2 ||
		ResultApplied != 1 || ResultStaleFence != 2 || ResultUnknownCollection != 3 ||
		ResultInvalidDocument != 4 || ResultTargetBound != 5 || ResultWrongShard != 6 {
		t.Fatalf("durable validation/result grammar drifted: profiles=%d,%d,%d formats=%d,%d codes=%d,%d,%d,%d,%d,%d",
			ValidationSchemaFreeJSONV1, ValidationDeterministicMutationV1,
			ValidationDeterministicMutationV2, ResultFormatMutationV1, ResultFormatMutationV2,
			ResultApplied, ResultStaleFence, ResultUnknownCollection,
			ResultInvalidDocument, ResultTargetBound, ResultWrongShard)
	}
	_, record := codecCompletionV1(ResultApplied)
	encoded, err := AppendCompletionRecordV1(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	const wantV1Digest = "6bd0b3a314c6cc4c7db668b768f1f7749d31872663fb6761fb58cb81e6bf2a13"
	if got := sha256.Sum256(encoded); hex.EncodeToString(got[:]) != wantV1Digest {
		t.Fatalf("v1 completion record golden digest = %x, want %s", got, wantV1Digest)
	}
	decoded, err := OpenCompletionRecordV1(encoded)
	if err != nil || !bytes.Equal(decoded.Completion, record.Completion) ||
		!bytes.Equal(decoded.Tenant, record.Tenant) || decoded.Collection != record.Collection {
		t.Fatalf("OpenCompletionRecordV1 = %+v,%v", decoded, err)
	}
	_, zeroCode := codecCompletionV1(0)
	if _, err := AppendCompletionRecordV1(nil, zeroCode); !errors.Is(err, ErrCompletionCorrupt) {
		t.Fatalf("zero result code error = %v", err)
	}
	v2Completion, v2 := codecCompletion(ResultFormatMutationV2, ResultWrongShard)
	v2Encoded, err := AppendCompletionRecordV1(nil, v2)
	if err != nil {
		t.Fatalf("v2 wrong-shard grammar: %v", err)
	}
	const wantV2CompletionDigest = "8182bf0ca7a18a8f4c4e2e9dbcc373362d156b2c9c8a96e823c62c462070bd74"
	if got := sha256.Sum256(v2Completion); hex.EncodeToString(got[:]) != wantV2CompletionDigest {
		t.Fatalf("v2 completion envelope golden digest = %x, want %s", got, wantV2CompletionDigest)
	}
	const wantV2RecordDigest = "4e3b146aa9405864a852b427f8c960daeed1f33fe9e4995d1ec5483e9aa09851"
	if got := sha256.Sum256(v2Encoded); hex.EncodeToString(got[:]) != wantV2RecordDigest {
		t.Fatalf("v2 completion record golden digest = %x, want %s", got, wantV2RecordDigest)
	}
	_, v1WrongShard := codecCompletion(ResultFormatMutationV1, ResultWrongShard)
	if _, err := AppendCompletionRecordV1(nil, v1WrongShard); !errors.Is(err, ErrCompletionCorrupt) {
		t.Fatalf("v1 wrong-shard grammar error = %v", err)
	}
	_, v2Unknown := codecCompletion(ResultFormatMutationV2, ResultWrongShard+1)
	if _, err := AppendCompletionRecordV1(nil, v2Unknown); !errors.Is(err, ErrCompletionCorrupt) {
		t.Fatalf("v2 unknown result grammar error = %v", err)
	}
}

func TestCodecAppendErrorsLeaveDestinationUnchanged(t *testing.T) {
	dst := make([]byte, 3, 1<<10)
	copy(dst, "pre")
	before := bytes.Clone(dst)
	invalidState := codecStateV1()
	invalidState.Applied = 0
	got, err := AppendStateV1(dst, invalidState)
	if !errors.Is(err, ErrStateCorrupt) || !bytes.Equal(got, before) {
		t.Fatalf("AppendStateV1 = %q,%v", got, err)
	}
	_, invalidCompletion := codecCompletionV1(0)
	got, err = AppendCompletionRecordV1(dst, invalidCompletion)
	if !errors.Is(err, ErrCompletionCorrupt) || !bytes.Equal(got, before) {
		t.Fatalf("AppendCompletionRecordV1 = %q,%v", got, err)
	}
}

func TestCodecWritableAliasesAreRejectedAndRelocationAliasesAreAllowed(t *testing.T) {
	t.Run("state writable string", func(t *testing.T) {
		state := codecStateV1()
		safe, err := AppendStateV1(nil, state)
		if err != nil {
			t.Fatal(err)
		}
		dst := make([]byte, 3, 3+len(safe))
		copy(dst, "pre")
		full := dst[:cap(dst)]
		copy(full[3:7], "dist")
		state.Binding.Distribution = unsafe.String(unsafe.SliceData(full[3:7]), 4)
		before := bytes.Clone(dst)
		got, err := AppendStateV1(dst, state)
		if !errors.Is(err, ErrCodecAlias) || !bytes.Equal(got, before) {
			t.Fatalf("AppendStateV1 = %q,%v", got, err)
		}
	})
	t.Run("completion writable slice", func(t *testing.T) {
		_, record := codecCompletionV1(ResultApplied)
		safe, err := AppendCompletionRecordV1(nil, record)
		if err != nil {
			t.Fatal(err)
		}
		dst := make([]byte, 3, 3+len(safe)+len(record.Completion))
		copy(dst, "pre")
		full := dst[:cap(dst)]
		copy(full[3:3+len(record.Completion)], record.Completion)
		record.Completion = full[3 : 3+len(record.Completion)]
		before := bytes.Clone(dst)
		got, err := AppendCompletionRecordV1(dst, record)
		if !errors.Is(err, ErrCodecAlias) || !bytes.Equal(got, before) {
			t.Fatalf("AppendCompletionRecordV1 = %q,%v", got, err)
		}
	})
	t.Run("relocation", func(t *testing.T) {
		state := codecStateV1()
		statePrefix := make([]byte, 4, 4)
		copy(statePrefix, "dist")
		state.Binding.Distribution = unsafe.String(unsafe.SliceData(statePrefix), len(statePrefix))
		encoded, err := AppendStateV1(statePrefix, state)
		if err != nil || !bytes.Equal(encoded[:4], []byte("dist")) {
			t.Fatalf("relocated state = %q,%v", encoded, err)
		}
		if _, err := OpenStateV1(encoded[4:]); err != nil {
			t.Fatalf("relocated state decode: %v", err)
		}

		_, record := codecCompletionV1(ResultApplied)
		completionPrefix := make([]byte, len(record.Tenant), len(record.Tenant))
		copy(completionPrefix, record.Tenant)
		record.Tenant = completionPrefix
		encoded, err = AppendCompletionRecordV1(completionPrefix, record)
		if err != nil || !bytes.Equal(encoded[:len(completionPrefix)], []byte("tenant")) {
			t.Fatalf("relocated completion = %q,%v", encoded, err)
		}
		if _, err := OpenCompletionRecordV1(encoded[len(completionPrefix):]); err != nil {
			t.Fatalf("relocated completion decode: %v", err)
		}
	})
}

func TestCompletionKeyV1Golden(t *testing.T) {
	key := CompletionKeyV1([]byte("tenant"), id128(9), 7, 11)
	const want = "022db6ad71532d0863a7837a4243d5b501f4d08568dc2badef5f51da5df186cd"
	if hex.EncodeToString(key[:]) != want {
		t.Fatalf("CompletionKeyV1 = %x, want %s", key, want)
	}
	other := CompletionKeyV1([]byte("tenant-2"), id128(9), 7, 11)
	if key == other {
		t.Fatal("different tuple produced the same test key")
	}
}

func FuzzOpenStateV1(f *testing.F) {
	seed, err := AppendStateV1(nil, codecStateV1())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxStateEnvelopeBytes+1 {
			data = data[:MaxStateEnvelopeBytes+1]
		}
		_, _ = OpenStateV1(data)
	})
}

func FuzzOpenCompletionRecordV1(f *testing.F) {
	_, record := codecCompletionV1(ResultApplied)
	seed, err := AppendCompletionRecordV1(nil, record)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxCompletionRecordBytes+1 {
			data = data[:MaxCompletionRecordBytes+1]
		}
		_, _ = OpenCompletionRecordV1(data)
	})
}

func FuzzOpenStateV1Resealed(f *testing.F) {
	seed, err := AppendStateV1(nil, codecStateV1())
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
		_, _ = OpenStateV1(candidate)
	})
}

func FuzzOpenCompletionRecordV1Resealed(f *testing.F) {
	_, record := codecCompletionV1(ResultApplied)
	seed, err := AppendCompletionRecordV1(nil, record)
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
		_, _ = OpenCompletionRecordV1(candidate)
	})
}
