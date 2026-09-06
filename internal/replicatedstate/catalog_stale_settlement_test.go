package replicatedstate

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestCatalogStaleMutationSettlesWithoutApplying(t *testing.T) {
	for _, tc := range []struct {
		name, distribution string
		class              replication.CommandAuthorityClass
		change             func(*replication.Command)
		settle             bool
	}{
		{"catalog-topology", "catalog", replication.CommandAuthorityTopology, nil, true},
		{"ordinary-topology", "data", replication.CommandAuthorityTopology, nil, false},
		{"catalog-data", "catalog", replication.CommandAuthorityData, nil, false},
		{"future-membership", "catalog", replication.CommandAuthorityTopology, func(c *replication.Command) { c.ReplicaSetVersion = 4 }, false},
		{"changed-policy", "catalog", replication.CommandAuthorityTopology, func(c *replication.Command) { c.ActivePolicyGeneration++ }, false},
		{"changed-schema", "catalog", replication.CommandAuthorityTopology, func(c *replication.Command) { c.SchemaGeneration++ }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding := testBinding()
			binding.Distribution, binding.Shard = tc.distribution, "controlplane"
			f := newMachineFixtureWithBinding(t, binding)
			if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
				t.Fatal(err)
			}
			command := commandValue(binding, 1)
			command.AuthorityClass = tc.class
			_, _, epoch := applySessionOpen(t, f.machine, 2, command)
			command.ClientEpoch = epoch
			if tc.change != nil {
				tc.change(&command)
			}
			encoded := encodeCommand(t, command)
			exact := bytes.Clone(encoded)
			if _, err := f.machine.ApplyConfiguration(raftmodel.ApplyMeta{Index: 3, Term: 2, Type: pb.EntryConfChange}, &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}); err != nil {
				t.Fatal(err)
			}
			err := f.machine.AdmitCommand(encoded)
			if !tc.settle {
				if !errors.Is(err, ErrStaleCommand) {
					t.Fatalf("non-settlement admission=%v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("stale catalog settlement admission=%v", err)
			}
			before := f.user.Collection.Len()
			if _, err = f.machine.ApplyNormal(normalMeta(4), encoded); err != nil {
				t.Fatal(err)
			}
			completion, err := f.machine.LookupCompletion(encoded)
			if err != nil {
				t.Fatal(err)
			}
			result, err := replication.OpenCompletion(completion.Bytes)
			if err != nil || result.ResultCode != ResultStaleFence || f.user.Collection.Len() != before {
				t.Fatalf("result=%d rows=%d err=%v", result.ResultCode, f.user.Collection.Len(), err)
			}
			reopened, err := Open(f.binding, f.bootstrap, f.system, UserCollection{Name: "docs", Target: f.user}, f.log, f.machine.options)
			if err != nil {
				t.Fatal(err)
			}
			if err = reopened.AdmitCommand(encoded); err != nil {
				t.Fatal(err)
			}
			if _, err = reopened.ApplyNormal(normalMeta(5), encoded); err != nil {
				t.Fatal(err)
			}
			retry, err := reopened.LookupCompletion(encoded)
			if err != nil || !bytes.Equal(retry.Bytes, completion.Bytes) || !bytes.Equal(encoded, exact) || f.user.Collection.Len() != before {
				t.Fatalf("settlement replay changed outcome or data: %v", err)
			}
		})
	}
}
