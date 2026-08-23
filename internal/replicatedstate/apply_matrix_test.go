package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	pb "go.etcd.io/raft/v3/raftpb"
)

func commandValue(binding Binding, sequence uint64) replication.Command {
	fingerprint := sha256.Sum256([]byte(fmt.Sprintf("request-%d", sequence)))
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
		// Test fixtures open their initial session at Raft index 2. Callers pass
		// a zero-based user-request ordinal: the open owns sequence 1, so the
		// first mutation is sequence 2.
		ClientID: id128(77), ClientEpoch: 2, ClientSequence: sequence + 1,
		Fingerprint: fingerprint, Collection: "docs",
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: []byte(fmt.Sprintf("k%d", sequence)),
			Value: []byte(`{"n":1}`),
		}},
	}
}

func encodeCommand(t testing.TB, command replication.Command) []byte {
	t.Helper()
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatalf("AppendCommand: %v", err)
	}
	return encoded
}

func TestEveryImmutableBindingMismatchIsTerminal(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*replication.Command)
	}{
		{"ClusterID", func(c *replication.Command) { c.ClusterID = id128(101) }},
		{"ClusterIncarnation", func(c *replication.Command) { c.ClusterIncarnation = id128(102) }},
		{"TopologyRecoveryEpoch", func(c *replication.Command) { c.TopologyRecoveryEpoch++ }},
		{"Distribution", func(c *replication.Command) { c.Distribution += "-other" }},
		{"Shard", func(c *replication.Command) { c.Shard += "-other" }},
		{"AllocationGeneration", func(c *replication.Command) { c.AllocationGeneration++ }},
		{"ShardIncarnation", func(c *replication.Command) { c.ShardIncarnation = id128(103) }},
		{"GroupID", func(c *replication.Command) { c.GroupID = id128(104) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newMachineFixture(t)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			command := commandValue(fixture.binding, 1)
			test.mutate(&command)
			if _, err := fixture.machine.ApplyNormal(normalMeta(2), encodeCommand(t, command)); !errors.Is(err, ErrWrongBinding) {
				t.Fatalf("ApplyNormal error = %v", err)
			}
			if fixture.machine.Applied() != 1 || fixture.user.Collection.Len() != 0 {
				t.Fatal("terminal binding mismatch changed state")
			}
		})
	}
}

func TestEveryMutableFenceMismatchPersistsStaleCompletion(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*replication.Command)
	}{
		{"ReplicaSetVersion", func(c *replication.Command) { c.ReplicaSetVersion++ }},
		{"ActivePolicyGeneration", func(c *replication.Command) { c.ActivePolicyGeneration++ }},
		{"ProtectionEpoch", func(c *replication.Command) { c.ProtectionEpoch++ }},
		{"OwnershipEpoch", func(c *replication.Command) { c.OwnershipEpoch++ }},
		{"SchemaGeneration", func(c *replication.Command) { c.SchemaGeneration++ }},
		{"RoutingVersion", func(c *replication.Command) { c.RoutingVersion++ }},
		{"RouteGeneration", func(c *replication.Command) { c.RouteGeneration++ }},
	}
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sequence := uint64(i + 1)
			command := commandValue(fixture.binding, sequence)
			test.mutate(&command)
			encoded := encodeCommand(t, command)
			publication, err := fixture.machine.ApplyNormal(normalMeta(sequence+2), encoded)
			if err != nil {
				t.Fatal(err)
			}
			if publication.Applied != sequence+2 || fixture.user.Collection.Len() != 0 {
				t.Fatalf("stale publication=%+v rows=%d", publication, fixture.user.Collection.Len())
			}
			lookup, err := fixture.machine.LookupCompletion(encoded)
			if err != nil {
				t.Fatal(err)
			}
			completion, err := replication.OpenCompletion(lookup.Bytes)
			if err != nil || completion.ResultCode != ResultStaleFence {
				t.Fatalf("completion=%+v err=%v", completion, err)
			}
		})
	}
}

func TestDeterministicResultCodeMatrix(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	tests := []struct {
		name string
		code uint32
		edit func(*replication.Command)
	}{
		{"applied", ResultApplied, func(*replication.Command) {}},
		{"unknown collection", ResultUnknownCollection, func(c *replication.Command) { c.Collection = "missing" }},
		{"invalid document", ResultInvalidDocument, func(c *replication.Command) { c.Mutations[0].Value = []byte("{") }},
		{"target bound", ResultTargetBound, func(c *replication.Command) {
			c.Mutations = make([]replication.Mutation, MaxDistinctMutations+1)
			for i := range c.Mutations {
				c.Mutations[i] = replication.Mutation{Kind: replication.MutationPut, Key: []byte{byte(i + 1)}, Value: []byte("null")}
			}
		}},
	}
	for i, test := range tests {
		sequence := uint64(i + 1)
		command := commandValue(fixture.binding, sequence)
		test.edit(&command)
		encoded := encodeCommand(t, command)
		if _, err := fixture.machine.ApplyNormal(normalMeta(sequence+2), encoded); err != nil {
			t.Fatalf("%s apply: %v", test.name, err)
		}
		lookup, err := fixture.machine.LookupCompletion(encoded)
		if err != nil {
			t.Fatalf("%s lookup: %v", test.name, err)
		}
		completion, err := replication.OpenCompletion(lookup.Bytes)
		if err != nil || completion.ResultCode != test.code {
			t.Fatalf("%s completion=%+v err=%v", test.name, completion, err)
		}
	}
}

func TestApplySequenceMatrix(t *testing.T) {
	tests := []struct {
		name  string
		prime bool
		meta  raftmodel.ApplyMeta
		data  []byte
	}{
		{"gap", false, raftmodel.ApplyMeta{Index: 3, Term: 2, Type: pb.EntryNormal}, nil},
		{"term regression", true, raftmodel.ApplyMeta{Index: 3, Term: 1, Type: pb.EntryNormal}, nil},
		{"terminal index", false, raftmodel.ApplyMeta{Index: math.MaxUint64, Term: 2, Type: pb.EntryNormal}, nil},
		{"wrong type", false, raftmodel.ApplyMeta{Index: 2, Term: 2, Type: pb.EntryConfChange}, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newMachineFixture(t)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			if test.prime {
				if _, err := fixture.machine.ApplyNormal(normalMeta(2), nil); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := fixture.machine.ApplyNormal(test.meta, test.data); !errors.Is(err, ErrApplySequence) {
				t.Fatalf("error = %v", err)
			}
		})
	}

	t.Run("same-index exact and different data", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
		command := encodeCommand(t, commandValue(fixture.binding, 1))
		meta := normalMeta(3)
		if _, err := fixture.machine.ApplyNormal(meta, command); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.machine.ApplyNormal(meta, command); err != nil {
			t.Fatalf("exact replay: %v", err)
		}
		different := bytes.Clone(command)
		different[len(different)-1] ^= 1
		if _, err := fixture.machine.ApplyNormal(meta, different); !errors.Is(err, ErrApplySequence) {
			t.Fatalf("different replay error = %v", err)
		}
	})
}

func TestApplyNormalRejectsOversizedDirectInputBeforeHashing(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	oversized := make([]byte, replication.MaxCommandBytes+1)
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), oversized); !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("oversized direct ApplyNormal error = %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), nil); !errors.Is(err, ErrApplyPoisoned) {
		t.Fatalf("ApplyNormal after oversized input error = %v", err)
	}
}
