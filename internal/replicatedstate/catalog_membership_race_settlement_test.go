package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	pb "go.etcd.io/raft/v3/raftpb"
)

// TestCatalogMutationPreparedBeforeMembershipApply exercises the ordering that
// left native catalog retries unknown in CI. The command is encoded while its
// old membership fence is current; membership is then applied before the
// command reaches Machine.AdmitCommand. A catalog topology mutation may enter
// Raft only to publish ResultStaleFence, leaving its user mutation untouched.
func TestCatalogMutationPreparedBeforeMembershipApply(t *testing.T) {
	binding := testBinding()
	binding.Distribution, binding.Shard = "catalog", "controlplane"
	fixture := newMachineFixtureWithBinding(t, binding)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	prototype := commandValue(binding, 1)
	prototype.AuthorityClass = replication.CommandAuthorityTopology
	_, _, epoch := applySessionOpen(t, fixture.machine, 2, prototype)

	seed := commandValue(binding, 1)
	seed.AuthorityClass = replication.CommandAuthorityTopology
	seed.ClientEpoch = epoch
	seedEncoded := encodeCommand(t, seed)
	if err := fixture.machine.AdmitCommand(seedEncoded); err != nil {
		t.Fatalf("admit seed: %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), seedEncoded); err != nil {
		t.Fatalf("apply seed: %v", err)
	}
	before, found, err := fixture.user.Collection.AppendRaw(nil, []byte("k1"))
	if err != nil || !found {
		t.Fatalf("seed value=%q found=%t err=%v", before, found, err)
	}
	before = bytes.Clone(before)

	stale := commandValue(binding, 2)
	stale.AuthorityClass = replication.CommandAuthorityTopology
	stale.ClientEpoch = epoch
	stale.Batches[0].Mutations[0].Key = []byte("k1")
	stale.Batches[0].Mutations[0].Value = []byte(`{"n":2}`)
	stale.Fingerprint = sha256.Sum256([]byte("catalog-membership-race-stale"))
	encoded := encodeCommand(t, stale)
	exact := bytes.Clone(encoded)

	// This is the race boundary: the prepared bytes still carry membership
	// version one, while the AddLearner configuration is now committed.
	if _, err := fixture.machine.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: 4, Term: 2, Type: pb.EntryConfChange,
	}, &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}); err != nil {
		t.Fatalf("apply membership: %v", err)
	}
	if err := fixture.machine.AdmitCommand(encoded); err != nil {
		t.Fatalf("admit stale catalog command: %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(5), encoded); err != nil {
		t.Fatalf("apply stale catalog command: %v", err)
	}
	first, err := fixture.machine.LookupCompletion(encoded)
	if err != nil {
		t.Fatalf("lookup first completion: %v", err)
	}
	completion, err := replication.OpenCompletion(first.Bytes)
	if err != nil || completion.ResultCode != ResultStaleFence || first.AppliedSequence == 0 {
		t.Fatalf("stale completion=%+v applied=%d err=%v", completion, first.AppliedSequence, err)
	}
	after, found, err := fixture.user.Collection.AppendRaw(nil, []byte("k1"))
	if err != nil || !found || !bytes.Equal(after, before) {
		t.Fatalf("stale apply changed user value=%q found=%t err=%v want=%q", after, found, err, before)
	}
	if !bytes.Equal(encoded, exact) {
		t.Fatal("stale retry bytes changed during admission or apply")
	}

	reopened, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
	if err != nil {
		t.Fatalf("reopen after stale settlement: %v", err)
	}
	if err := reopened.AdmitCommand(encoded); err != nil {
		t.Fatalf("admit exact stale retry: %v", err)
	}
	if _, err := reopened.ApplyNormal(normalMeta(6), encoded); err != nil {
		t.Fatalf("apply exact stale retry: %v", err)
	}
	retry, err := reopened.LookupCompletion(encoded)
	if err != nil || !bytes.Equal(retry.Bytes, first.Bytes) || retry.AppliedSequence == 0 {
		t.Fatalf("retry completion=%+v err=%v first=%+v", retry, err, first)
	}
	after, found, err = fixture.user.Collection.AppendRaw(nil, []byte("k1"))
	if err != nil || !found || !bytes.Equal(after, before) {
		t.Fatalf("stale retry changed user value=%q found=%t err=%v want=%q", after, found, err, before)
	}
}

func TestCatalogMutationMembershipRaceRejectsUnrelatedStaleCommand(t *testing.T) {
	for _, tc := range []struct {
		name   string
		want   error
		change func(*replication.Command)
	}{
		{name: "ordinary data authority", want: ErrStaleCommand, change: func(command *replication.Command) {
			command.AuthorityClass = replication.CommandAuthorityData
		}},
		{name: "wrong shard", want: ErrWrongBinding, change: func(command *replication.Command) {
			command.Shard = "other"
		}},
		{name: "changed protection", want: ErrStaleCommand, change: func(command *replication.Command) {
			command.ProtectionEpoch++
		}},
		{name: "changed policy", want: ErrStaleCommand, change: func(command *replication.Command) {
			command.ActivePolicyGeneration++
		}},
		{name: "changed schema", want: ErrStaleCommand, change: func(command *replication.Command) {
			command.SchemaGeneration++
		}},
		{name: "future membership", want: ErrStaleCommand, change: func(command *replication.Command) {
			command.ReplicaSetVersion = 4
		}},
		{name: "future ownership", want: ErrStaleCommand, change: func(command *replication.Command) {
			command.OwnershipEpoch++
		}},
		{name: "future routing", want: ErrStaleCommand, change: func(command *replication.Command) {
			command.RoutingVersion++
		}},
		{name: "future route generation", want: ErrStaleCommand, change: func(command *replication.Command) {
			command.RouteGeneration++
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding := testBinding()
			binding.Distribution, binding.Shard = "catalog", "controlplane"
			fixture := newMachineFixtureWithBinding(t, binding)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			command := commandValue(binding, 1)
			command.AuthorityClass = replication.CommandAuthorityTopology
			tc.change(&command)
			prototype := commandValue(binding, 1)
			prototype.AuthorityClass = command.AuthorityClass
			_, _, epoch := applySessionOpen(t, fixture.machine, 2, prototype)
			command.ClientEpoch = epoch
			encoded := encodeCommand(t, command)
			if _, err := fixture.machine.ApplyConfiguration(raftmodel.ApplyMeta{
				Index: 3, Term: 2, Type: pb.EntryConfChange,
			}, &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}); err != nil {
				t.Fatal(err)
			}
			if err := fixture.machine.AdmitCommand(encoded); !errors.Is(err, tc.want) {
				t.Fatalf("unsafe stale command admission=%v want=%v", err, tc.want)
			}
		})
	}
}
