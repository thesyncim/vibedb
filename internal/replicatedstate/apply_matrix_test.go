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

func commandValue(binding Binding, sequence uint64) replication.CommandV1 {
	fingerprint := sha256.Sum256([]byte(fmt.Sprintf("request-%d", sequence)))
	return replication.CommandV1{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: binding.ActivePolicyGeneration,
		ProtectionEpoch: binding.ProtectionEpoch, OwnershipEpoch: binding.OwnershipEpoch,
		SchemaGeneration: binding.SchemaGeneration, RoutingVersion: binding.RoutingVersion,
		RouteGeneration: binding.RouteGeneration, Tenant: []byte("tenant"),
		ClientID: id128(77), ClientEpoch: 1, ClientSequence: sequence,
		Fingerprint: fingerprint, Collection: "docs",
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: []byte(fmt.Sprintf("k%d", sequence)),
			Value: []byte(`{"n":1}`),
		}},
	}
}

func encodeCommand(t testing.TB, command replication.CommandV1) []byte {
	t.Helper()
	encoded, err := replication.AppendCommandV1(nil, command)
	if err != nil {
		t.Fatalf("AppendCommandV1: %v", err)
	}
	return encoded
}

func TestEveryImmutableBindingMismatchIsTerminal(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*replication.CommandV1)
	}{
		{"ClusterID", func(c *replication.CommandV1) { c.ClusterID = id128(101) }},
		{"ClusterIncarnation", func(c *replication.CommandV1) { c.ClusterIncarnation = id128(102) }},
		{"TopologyRecoveryEpoch", func(c *replication.CommandV1) { c.TopologyRecoveryEpoch++ }},
		{"Distribution", func(c *replication.CommandV1) { c.Distribution += "-other" }},
		{"Shard", func(c *replication.CommandV1) { c.Shard += "-other" }},
		{"AllocationGeneration", func(c *replication.CommandV1) { c.AllocationGeneration++ }},
		{"ShardIncarnation", func(c *replication.CommandV1) { c.ShardIncarnation = id128(103) }},
		{"GroupID", func(c *replication.CommandV1) { c.GroupID = id128(104) }},
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
		mutate func(*replication.CommandV1)
	}{
		{"ReplicaSetVersion", func(c *replication.CommandV1) { c.ReplicaSetVersion++ }},
		{"ActivePolicyGeneration", func(c *replication.CommandV1) { c.ActivePolicyGeneration++ }},
		{"ProtectionEpoch", func(c *replication.CommandV1) { c.ProtectionEpoch++ }},
		{"OwnershipEpoch", func(c *replication.CommandV1) { c.OwnershipEpoch++ }},
		{"SchemaGeneration", func(c *replication.CommandV1) { c.SchemaGeneration++ }},
		{"RoutingVersion", func(c *replication.CommandV1) { c.RoutingVersion++ }},
		{"RouteGeneration", func(c *replication.CommandV1) { c.RouteGeneration++ }},
	}
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sequence := uint64(i + 1)
			command := commandValue(fixture.binding, sequence)
			test.mutate(&command)
			encoded := encodeCommand(t, command)
			publication, err := fixture.machine.ApplyNormal(normalMeta(sequence+1), encoded)
			if err != nil {
				t.Fatal(err)
			}
			if publication.Applied != sequence+1 || fixture.user.Collection.Len() != 0 {
				t.Fatalf("stale publication=%+v rows=%d", publication, fixture.user.Collection.Len())
			}
			lookup, err := fixture.machine.LookupCompletion(encoded)
			if err != nil {
				t.Fatal(err)
			}
			completion, err := replication.OpenCompletionV1(lookup.Bytes)
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
	tests := []struct {
		name string
		code uint32
		edit func(*replication.CommandV1)
	}{
		{"applied", ResultApplied, func(*replication.CommandV1) {}},
		{"unknown collection", ResultUnknownCollection, func(c *replication.CommandV1) { c.Collection = "missing" }},
		{"invalid document", ResultInvalidDocument, func(c *replication.CommandV1) { c.Mutations[0].Value = []byte("{") }},
		{"target bound", ResultTargetBound, func(c *replication.CommandV1) {
			c.Mutations = make([]replication.Mutation, MaxDistinctMutationsV1+1)
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
		if _, err := fixture.machine.ApplyNormal(normalMeta(sequence+1), encoded); err != nil {
			t.Fatalf("%s apply: %v", test.name, err)
		}
		lookup, err := fixture.machine.LookupCompletion(encoded)
		if err != nil {
			t.Fatalf("%s lookup: %v", test.name, err)
		}
		completion, err := replication.OpenCompletionV1(lookup.Bytes)
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
		command := encodeCommand(t, commandValue(fixture.binding, 1))
		meta := normalMeta(2)
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
