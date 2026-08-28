package replicatedstate

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func stateSizeFixture(extensions uint8) State {
	state := codecState()
	state.Applied = 100
	state.LastKind = RecordNormal
	if extensions&1 != 0 {
		state.TransactionControlCount, state.ActiveTransactionCount = 1, 1
		state.TransactionResidentBytes = 4096
	}
	if extensions&2 != 0 {
		state.RequestLedgerRows, state.RequestLedgerResidentBytes = 2, 100
	}
	if extensions&4 != 0 {
		state.ExecutionPinRecordCount, state.ActiveExecutionPinCount = 1, 1
		state.ExecutionPinResidentBytes = executionPinRecordStorageKeyBytes + executionpin.RecordBytes +
			executionPinActiveStorageKeyBytes + executionPinActiveValueBytes
	}
	if extensions&8 != 0 {
		state.RelationPlacementDigest[0] = 1
	}
	if extensions&16 != 0 {
		state.FenceOriginDigest[0], state.FenceApplied = 1, 50
		state.SessionCount, state.AuthorityBindingCount, state.SessionSlotCount = 1, 1, 4
		state.HistoricalFenceCount, state.HistoricalFenceSlots, state.UnfencedSessionSlots = 2, 3, 1
	}
	return state
}

func assertStateSizeParity(t testing.TB, state State) {
	t.Helper()
	want, appendErr := AppendState(nil, state)
	size, sizeErr := validatedStateSize(state)
	if appendErr == nil {
		if sizeErr != nil || size != len(want) {
			t.Fatalf("size = %d, %v; append length = %d", size, sizeErr, len(want))
		}
		return
	}
	if size != 0 || sizeErr == nil ||
		errors.Is(sizeErr, ErrStateCorrupt) != errors.Is(appendErr, ErrStateCorrupt) ||
		errors.Is(sizeErr, ErrAdmissionBound) != errors.Is(appendErr, ErrAdmissionBound) {
		t.Fatalf("size = %d, %v; append error = %v", size, sizeErr, appendErr)
	}
}

func TestStateEncodedSizeExtensionsAndConfigurationParity(t *testing.T) {
	falseValue := false
	configs := []*pb.ConfState{
		{Voters: []uint64{1}},
		{Voters: []uint64{127, 128, 16384}, Learners: []uint64{math.MaxUint64 - 1024}},
		{Voters: []uint64{1}, AutoLeave: &falseValue},
		{Voters: []uint64{2, 3}, VotersOutgoing: []uint64{1, 2}, Learners: []uint64{4}, LearnersNext: []uint64{1}},
	}
	for extensions := uint8(0); extensions < 32; extensions++ {
		for config, conf := range configs {
			t.Run(fmt.Sprintf("extensions_%02x/config_%d", extensions, config), func(t *testing.T) {
				state := stateSizeFixture(extensions)
				state.ConfState = conf
				assertStateSizeParity(t, state)
				if _, err := validatedStateSize(state); err != nil {
					t.Fatalf("valid fixture rejected: %v", err)
				}
			})
		}
	}
	assertStateSizeParity(t, codecState())
	for _, kind := range []RecordKind{RecordConfiguration, RecordOwnership, RecordSchema, RecordImportedSnapshot} {
		t.Run(fmt.Sprintf("record_%d", kind), func(t *testing.T) {
			state := stateSizeFixture(0)
			state.LastKind = kind
			if kind == RecordConfiguration {
				state.LastEntryType, state.ReplicaSetVersion = pb.EntryConfChangeV2, state.Applied
				var err error
				state.LastEntryDigest, err = configurationEntryDigest(raftmodel.ApplyMeta{
					Index: state.Applied, Term: state.LastTerm, Type: state.LastEntryType,
				}, state.ConfState)
				if err != nil {
					t.Fatal(err)
				}
			}
			if kind == RecordImportedSnapshot {
				state.SessionEpochHighWater = state.Applied
			}
			assertStateSizeParity(t, state)
			if _, err := validatedStateSize(state); err != nil {
				t.Fatalf("valid record rejected: %v", err)
			}
		})
	}
	maximum := stateSizeFixture(31)
	maximum.Binding.Distribution = strings.Repeat("d", replication.MaxIdentityBytes)
	maximum.Binding.Shard = strings.Repeat("s", replication.MaxIdentityBytes)
	maximum.ConfState.Voters = []uint64{math.MaxUint64 - 1024}
	maximum.ConfState.Learners = make([]uint64, raftmodel.MaxConfStateMembers-1)
	for i := range maximum.ConfState.Learners {
		maximum.ConfState.Learners[i] = math.MaxUint64 - 1023 + uint64(i)
	}
	assertStateSizeParity(t, maximum)
	if size, err := validatedStateSize(maximum); err != nil || size > MaxStateEnvelopeBytes {
		t.Fatalf("maximum supported state = %d, %v", size, err)
	}
}

func TestStateEncodedSizeRejectsFullValidationFailures(t *testing.T) {
	for name, mutate := range map[string]func(*State){
		"binding":              func(s *State) { s.Binding.Distribution = "" },
		"oversize_identity":    func(s *State) { s.Binding.Shard = strings.Repeat("s", replication.MaxIdentityBytes+1) },
		"invalid_utf8":         func(s *State) { s.Binding.Shard = "\xff" },
		"applied":              func(s *State) { s.Applied = 0 },
		"term":                 func(s *State) { s.LastTerm = math.MaxUint64 },
		"session_count":        func(s *State) { s.SessionCount = 2 },
		"transaction_counters": func(s *State) { s.TransactionResidentBytes = 0 },
		"ledger_counters":      func(s *State) { s.RequestLedgerResidentBytes = 0 },
		"pin_counters":         func(s *State) { s.ExecutionPinResidentBytes = 0 },
		"fence_counters":       func(s *State) { s.HistoricalFenceSlots = math.MaxUint64 },
		"nil_conf":             func(s *State) { s.ConfState = nil },
		"unknown_conf":         func(s *State) { s.ConfState.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"no_voters":            func(s *State) { s.ConfState.Voters = nil },
		"duplicate_voter":      func(s *State) { s.ConfState.Voters = []uint64{1, 1} },
		"unsorted_voters":      func(s *State) { s.ConfState.Voters = []uint64{2, 1} },
		"zero_voter":           func(s *State) { s.ConfState.Voters = []uint64{0} },
		"automatic_leave":      func(s *State) { s.ConfState.AutoLeave = proto.Bool(true) },
		"too_many_members": func(s *State) {
			s.ConfState.Voters = make([]uint64, raftmodel.MaxConfStateMembers+1)
			for i := range s.ConfState.Voters {
				s.ConfState.Voters[i] = uint64(i + 1)
			}
		},
		"record_kind": func(s *State) { s.LastKind = 0 },
		"entry_type":  func(s *State) { s.LastEntryType = pb.EntryConfChange },
		"configuration_digest": func(s *State) {
			s.LastKind, s.LastEntryType, s.ReplicaSetVersion = RecordConfiguration, pb.EntryConfChangeV2, s.Applied
		},
	} {
		t.Run(name, func(t *testing.T) {
			state := stateSizeFixture(31)
			mutate(&state)
			assertStateSizeParity(t, state)
			if _, err := validatedStateSize(state); !errors.Is(err, ErrStateCorrupt) {
				t.Fatalf("invalid state accepted: %v", err)
			}
		})
	}
}

func TestStateEncodedSizeAddsNoValidationAllocations(t *testing.T) {
	for _, extensions := range []uint8{0, 31} {
		state := stateSizeFixture(extensions)
		validationAllocs := testing.AllocsPerRun(100, func() {
			if err := validateState(state); err != nil {
				t.Fatal(err)
			}
		})
		sizeAllocs := testing.AllocsPerRun(100, func() {
			if _, err := validatedStateSize(state); err != nil {
				t.Fatal(err)
			}
		})
		appendAllocs := testing.AllocsPerRun(100, func() {
			if _, err := AppendState(nil, state); err != nil {
				t.Fatal(err)
			}
		})
		if sizeAllocs != validationAllocs || appendAllocs < sizeAllocs+2 {
			t.Fatalf("extensions %02x allocations: size=%g validation=%g append=%g", extensions, sizeAllocs, validationAllocs, appendAllocs)
		}
	}
}

func TestStateEncodingSizeRejectsOverflow(t *testing.T) {
	state := stateSizeFixture(31)
	for _, confBytes := range []int{-1, math.MaxInt, MaxStateEnvelopeBytes} {
		if size, err := stateEncodingSize(state, confBytes); size != 0 || !errors.Is(err, ErrAdmissionBound) {
			t.Fatalf("configuration bytes %d: size=%d err=%v", confBytes, size, err)
		}
	}
	maximum := MaxStateEnvelopeBytes - stateEncodingHeader(state) - recordChecksumLen -
		len(state.Binding.Distribution) - len(state.Binding.Shard)
	if size, err := stateEncodingSize(state, maximum); err != nil || size != MaxStateEnvelopeBytes {
		t.Fatalf("exact bound: size=%d err=%v", size, err)
	}
	if size, err := stateEncodingSize(state, maximum+1); size != 0 || !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("one byte above bound: size=%d err=%v", size, err)
	}
	state.Binding.Shard = strings.Repeat("s", MaxStateEnvelopeBytes)
	if size, err := stateEncodingSize(state, 1); size != 0 || !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("oversized identity: size=%d err=%v", size, err)
	}
}

func FuzzStateEncodedSizeParity(f *testing.F) {
	for _, extensions := range []uint8{0, 1, 2, 4, 8, 16, 31} {
		f.Add(extensions, uint64(1), uint64(100), "distribution", "shard")
	}
	f.Add(uint8(31), uint64(math.MaxUint64-1024), uint64(100), "d", "s")
	f.Add(uint8(31), uint64(0), uint64(0), "", "")
	f.Fuzz(func(t *testing.T, extensions uint8, member, applied uint64, distribution, shard string) {
		state := stateSizeFixture(extensions)
		state.Applied = applied
		state.ConfState.Voters = []uint64{member}
		state.Binding.Distribution, state.Binding.Shard = distribution, shard
		assertStateSizeParity(t, state)
	})
}

func BenchmarkStateEncodedSize(b *testing.B) {
	for _, extensions := range []uint8{0, 31} {
		state := stateSizeFixture(extensions)
		for _, encode := range []bool{false, true} {
			b.Run(fmt.Sprintf("extensions_%02x/encode_%t", extensions, encode), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if encode {
						if _, err := AppendState(nil, state); err != nil {
							b.Fatal(err)
						}
					} else if _, err := validatedStateSize(state); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
