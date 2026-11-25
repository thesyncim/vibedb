package shardservice

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
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

func TestReplicatedWireRetainsExactCatalogCompletionAcrossPlacement(t *testing.T) {
	fence := testReplicatedFence()
	result, err := replicatedstate.AppendMutationCompletionResult(nil, replicatedstate.ResultApplied, 1)
	if err != nil {
		t.Fatal(err)
	}
	baseCompletion := replication.CompletionBytes{
		ClusterID:              replication.ID128(fence.Group.ClusterID),
		ClusterIncarnation:     replication.ID128(fence.Group.ClusterIncarnation),
		TopologyRecoveryEpoch:  fence.Group.TopologyRecoveryEpoch,
		Distribution:           []byte("catalog"),
		Shard:                  []byte("controlplane"),
		AllocationGeneration:   fence.AllocationGeneration,
		ShardIncarnation:       replication.ID128(fence.Group.ShardIncarnation),
		GroupID:                replication.ID128(fence.Group.GroupID),
		ReplicaSetVersion:      1,
		ActivePolicyGeneration: 1,
		ProtectionEpoch:        1,
		RoutingVersion:         1,
		RouteGeneration:        1,
		Tenant:                 []byte("tenant"),
		ClientID:               replication.ID128{1},
		ClientEpoch:            2,
		ClientSequence:         1,
		Fingerprint:            sha256.Sum256([]byte("retained-catalog-completion")),
		AppliedSequence:        9,
		ResultCode:             replicatedstate.ResultApplied,
		ResultFormat:           replicatedstate.ResultFormatMutation,
		Storage:                replication.CompletionInline,
		ResultLength:           uint64(len(result)),
		ResultDigest: replication.CompletionResultDigest(
			replicatedstate.ResultApplied, replicatedstate.ResultFormatMutation, result,
		),
		InlineResult: result,
	}
	fence.Command.ReplicaSetVersion++
	fence.Command.OwnershipEpoch++
	fence.Command.RoutingVersion++
	fence.Command.RouteGeneration++
	state := ReplicatedMemberState{
		Fence: fence, LeaderID: fence.MemberID,
		Commit: 9, Applied: 9, CheckpointApplied: 8,
	}
	roundTrip := func(c replication.CompletionBytes) error {
		t.Helper()
		raw, err := replication.AppendCompletionBytes(nil, c)
		if err != nil {
			t.Fatal(err)
		}
		response := &ReplicatedResponse{
			Kind: ReplicatedCompletion, HasState: true, State: state,
			RequestDigest: sha256.Sum256([]byte("retained-catalog-completion")),
			Completion:    raw,
			Outcome: raftserve.Outcome{
				Code: raftserve.OutcomeCompletion, AppliedIndex: 9,
				CompletionAppliedSequence: 9, CompletionBytes: len(raw),
			},
		}
		var encoded bytes.Buffer
		if err := EncodeReplicatedResponse(&encoded, response); err != nil {
			return err
		}
		decoded, err := DecodeReplicatedResponse(&encoded)
		if err == nil && !bytes.Equal(decoded.Completion, raw) {
			t.Fatal("wire changed retained catalog completion bytes")
		}
		return err
	}
	if err := roundTrip(baseCompletion); err != nil {
		t.Fatalf("retained catalog completion rejected after placement advance: %v", err)
	}
	for name, mutate := range map[string]func(*replication.CompletionBytes){
		"non-catalog distribution": func(c *replication.CompletionBytes) { c.Distribution = []byte("orders") },
		"non-controlplane shard":   func(c *replication.CompletionBytes) { c.Shard = []byte("all") },
		"future rsv":               func(c *replication.CompletionBytes) { c.ReplicaSetVersion = 3 },
		"future routing version":   func(c *replication.CompletionBytes) { c.RoutingVersion = 3 },
		"future route generation":  func(c *replication.CompletionBytes) { c.RouteGeneration = 3 },
		"policy mismatch":          func(c *replication.CompletionBytes) { c.ActivePolicyGeneration = 2 },
		"protection mismatch":      func(c *replication.CompletionBytes) { c.ProtectionEpoch = 2 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := baseCompletion
			mutate(&changed)
			if err := roundTrip(changed); err == nil {
				t.Fatal("invalid or non-catalog stale completion accepted")
			}
		})
	}
}
