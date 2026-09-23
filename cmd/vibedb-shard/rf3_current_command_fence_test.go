package main

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestCommandFenceFromSnapshotFenceUsesCurrentAuthority(t *testing.T) {
	const (
		distributionName = "test-distribution"
		shardName        = "test-shard"
	)
	expected := sqldriver.ReplicatedShardStoreBinding{
		ClusterID:             [16]byte{1},
		ClusterIncarnation:    [16]byte{2},
		TopologyRecoveryEpoch: 3,
		Distribution:          distributionName,
		Shard:                 shardName,
		AllocationGeneration:  4,
		ShardIncarnation:      [16]byte{5},
		GroupID:               [16]byte{6},
		MemberID:              7,
		StoreID:               [16]byte{8},
		Authority: sqldriver.ReplicatedAuthorityProfile{
			ActivePolicyGeneration: 1,
			ProtectionEpoch:        1,
			OwnershipEpoch:         1,
			SchemaGeneration:       1,
			RoutingVersion:         1,
			RouteGeneration:        1,
		},
	}
	digest := [32]byte{9}
	identity := raftmember.RuntimeIdentity{
		Group: raftmember.GroupKey{
			ClusterID:             expected.ClusterID,
			ClusterIncarnation:    expected.ClusterIncarnation,
			TopologyRecoveryEpoch: expected.TopologyRecoveryEpoch,
			ShardIncarnation:      expected.ShardIncarnation,
			GroupID:               expected.GroupID,
		},
		Distribution:           distributionName,
		Shard:                  shardName,
		AllocationGeneration:   expected.AllocationGeneration,
		MemberID:               expected.MemberID,
		StoreID:                expected.StoreID,
		NodeIncarnation:        2,
		RelationManifestDigest: digest,
	}
	profile := sqldriver.ReplicatedApplyCapacityProfile{Binding: expected, RelationManifestDigest: digest}
	current := replicatedstate.Binding{
		ClusterID:              replication.ID128(expected.ClusterID),
		ClusterIncarnation:     replication.ID128(expected.ClusterIncarnation),
		TopologyRecoveryEpoch:  expected.TopologyRecoveryEpoch,
		Distribution:           distributionName,
		Shard:                  shardName,
		AllocationGeneration:   expected.AllocationGeneration,
		ShardIncarnation:       replication.ID128(expected.ShardIncarnation),
		GroupID:                replication.ID128(expected.GroupID),
		ActivePolicyGeneration: 1,
		ProtectionEpoch:        1,
		OwnershipEpoch:         2,
		SchemaGeneration:       1,
		RoutingVersion:         2,
		RouteGeneration:        2,
	}
	fence := replicatedstate.SnapshotFence{Binding: current, RelationManifestDigest: digest, ReplicaSetVersion: 42}
	publication := raftmodel.Publication{ReplicaSetVersion: 42}

	got, err := commandFenceFromSnapshotFence(profile, fence, identity, publication)
	if err != nil {
		t.Fatalf("current fence rejected: %v", err)
	}
	want := commandFenceFromPublication(sqldriver.ReplicatedAuthorityProfile{
		ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 2,
		SchemaGeneration: 1, RoutingVersion: 2, RouteGeneration: 2,
	}, identity, 42)
	if got != want {
		t.Fatalf("current command = %+v, want %+v", got, want)
	}

	stale := fence
	stale.Binding.OwnershipEpoch = 1
	stale.Binding.RoutingVersion = 1
	stale.Binding.RouteGeneration = 1
	staleCommand, err := commandFenceFromSnapshotFence(profile, stale, identity, publication)
	if err != nil {
		t.Fatalf("stale authenticated fence failed before comparison: %v", err)
	}
	if staleCommand == got {
		t.Fatal("stale authority produced the current serving command")
	}

	foreign := fence
	foreign.Binding.GroupID[0]++
	if _, err := commandFenceFromSnapshotFence(profile, foreign, identity, publication); err == nil {
		t.Fatal("foreign immutable binding accepted")
	}
}
