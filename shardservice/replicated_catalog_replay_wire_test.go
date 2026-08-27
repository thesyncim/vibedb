package shardservice

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestReplicatedWireRetainsExactCatalogCommandAcrossPlacement(t *testing.T) {
	fence := testReplicatedFence()
	command := replication.Command{
		AuthorityClass: replication.CommandAuthorityTopology,
		ClusterID:      fence.Group.ClusterID, ClusterIncarnation: fence.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: fence.Group.TopologyRecoveryEpoch, ShardIncarnation: fence.Group.ShardIncarnation,
		GroupID: fence.Group.GroupID, AllocationGeneration: fence.AllocationGeneration,
		Distribution: "catalog", Shard: "controlplane", ReplicaSetVersion: 1,
		ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1,
		RoutingVersion: 1, RouteGeneration: 1, Tenant: []byte("tenant"),
		ClientID: replication.ID128{1}, ClientEpoch: 2, ClientSequence: 1,
		Fingerprint: sha256.Sum256([]byte("retained-catalog-command")),
		Batches: []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{
			{Kind: replication.MutationPut, Key: []byte{1}, Value: []byte(`{"id":1}`)},
		}}},
	}
	fence.Command.ReplicaSetVersion++
	fence.Command.OwnershipEpoch++
	fence.Command.RoutingVersion++
	fence.Command.RouteGeneration++
	encode := func(c replication.Command) ([]byte, error) {
		t.Helper()
		raw, err := replication.AppendCommand(nil, c)
		if err != nil {
			t.Fatal(err)
		}
		var encoded bytes.Buffer
		err = EncodeReplicatedRequest(&encoded, &ReplicatedRequest{Operation: ReplicatedPropose,
			Capability: serviceauthz.CapabilityTopology, Authority: serviceauthz.Authority{Node: [16]byte{9}, Generation: 1},
			Fence: fence, Command: raw})
		if err != nil {
			return nil, err
		}
		decoded, err := DecodeReplicatedRequest(&encoded)
		if err == nil && (!bytes.Equal(decoded.Command, raw) || decoded.Fence != fence) {
			t.Fatal("wire changed retained command bytes or current fence")
		}
		return raw, err
	}
	if _, err := encode(command); err != nil {
		t.Fatalf("exact catalog completion cannot be recovered after placement change: %v", err)
	}
	for name, mutate := range map[string]func(*replication.Command){
		"data":              func(c *replication.Command) { c.AuthorityClass = replication.CommandAuthorityData },
		"distribution":      func(c *replication.Command) { c.Distribution = "orders" },
		"shard":             func(c *replication.Command) { c.Shard = "other" },
		"group":             func(c *replication.Command) { c.GroupID[0]++ },
		"allocation":        func(c *replication.Command) { c.AllocationGeneration++ },
		"schema":            func(c *replication.Command) { c.SchemaGeneration++ },
		"policy":            func(c *replication.Command) { c.ActivePolicyGeneration++ },
		"protection":        func(c *replication.Command) { c.ProtectionEpoch++ },
		"future rsv":        func(c *replication.Command) { c.ReplicaSetVersion = 3 },
		"future owner":      func(c *replication.Command) { c.OwnershipEpoch = 3 },
		"future route":      func(c *replication.Command) { c.RoutingVersion = 3 },
		"future generation": func(c *replication.Command) { c.RouteGeneration = 3 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := command
			mutate(&changed)
			if _, err := encode(changed); err == nil {
				t.Fatal("foreign or future authority admitted as retained catalog replay")
			}
		})
	}
}
