package shardservice

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestReplicatedNativeWireRoundTripAndCanonicalFences(t *testing.T) {
	fence := testReplicatedFence()
	command := testReplicatedCommand(t, fence)
	for _, request := range []*ReplicatedRequest{
		{Operation: ReplicatedProbe, Fence: ReplicatedFence{
			Group: fence.Group, AllocationGeneration: fence.AllocationGeneration,
		}},
		{Operation: ReplicatedPropose, Fence: fence, Command: command},
	} {
		var encoded bytes.Buffer
		if err := EncodeReplicatedRequest(&encoded, request); err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeReplicatedRequest(&encoded)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Operation != request.Operation || decoded.Fence != request.Fence ||
			!bytes.Equal(decoded.Command, request.Command) {
			t.Fatalf("request round trip = %+v", decoded)
		}
		if len(decoded.Command) != 0 && cap(decoded.Command) != len(decoded.Command) {
			t.Fatal("decoded command retained writable trailing frame capacity")
		}
	}

	completion := testReplicatedCompletion(t, fence, 2)
	state := ReplicatedMemberState{
		Fence: fence, LeaderID: fence.MemberID, Commit: 9, Applied: 8,
		CheckpointApplied: 7,
	}
	responses := []*ReplicatedResponse{
		{Kind: ReplicatedHandshake, HasState: true, State: state},
		{Kind: ReplicatedCompletion, HasState: true, State: state,
			Outcome: raftserve.Outcome{Code: raftserve.OutcomeCompletion,
				AppliedIndex: 8, CompletionAppliedSequence: 2,
				CompletionBytes: len(completion)}, Completion: completion},
		{Kind: ReplicatedNotLeader, HasState: true, State: state},
		{Kind: ReplicatedOutcomeUnknown, HasState: true, State: state},
		{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalDeterministic,
			HasState: true, State: state, Outcome: raftserve.Outcome{Code: raftserve.OutcomeSessionEpoch}},
	}
	for _, response := range responses {
		var encoded bytes.Buffer
		if err := EncodeReplicatedResponse(&encoded, response); err != nil {
			t.Fatalf("encode %+v: %v", response, err)
		}
		decoded, err := DecodeReplicatedResponse(&encoded)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Kind != response.Kind || decoded.Refusal != response.Refusal ||
			decoded.HasState != response.HasState ||
			decoded.State != response.State || decoded.Outcome != response.Outcome ||
			!bytes.Equal(decoded.Completion, response.Completion) {
			t.Fatalf("response round trip = %+v, want %+v", decoded, response)
		}
	}
}

func TestReplicatedNativeWireRejectsSQLShapedAndCrossGroupPayloads(t *testing.T) {
	fence := testReplicatedFence()
	command := testReplicatedCommand(t, fence)
	invalid := []*ReplicatedRequest{
		{Operation: ReplicatedProbe, Fence: fence},
		{Operation: ReplicatedPropose, Fence: fence},
		{Operation: ReplicatedPropose, Fence: fence, Command: []byte("INSERT INTO docs")},
	}
	changed := fence
	changed.Group.GroupID[0]++
	invalid = append(invalid, &ReplicatedRequest{
		Operation: ReplicatedPropose, Fence: changed, Command: command,
	})
	for _, request := range invalid {
		var encoded bytes.Buffer
		if err := EncodeReplicatedRequest(&encoded, request); err == nil {
			t.Fatalf("invalid request encoded: %+v", request)
		}
	}
}

func testReplicatedFence() ReplicatedFence {
	group := raftmember.GroupKey{TopologyRecoveryEpoch: 3}
	for index := range group.ClusterID {
		group.ClusterID[index] = byte(index + 1)
		group.ClusterIncarnation[index] = byte(index + 21)
		group.ShardIncarnation[index] = byte(index + 41)
		group.GroupID[index] = byte(index + 61)
	}
	fence := ReplicatedFence{
		Group: group, AllocationGeneration: 5, MemberID: 7,
		Command: raftservice.CommandFence{
			ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
			OwnershipEpoch: 1, SchemaGeneration: 1,
			RelationManifestDigest: [32]byte{1},
			RoutingVersion:         1, RouteGeneration: 1,
		},
		NodeIncarnation: 11, Term: 13,
	}
	fence.StoreID[0] = 9
	return fence
}

func testReplicatedCommand(t testing.TB, fence ReplicatedFence) []byte {
	t.Helper()
	command := replication.Command{
		ClusterID:             fence.Group.ClusterID,
		ClusterIncarnation:    fence.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: fence.Group.TopologyRecoveryEpoch,
		Distribution:          "orders", Shard: "0000-ffff",
		AllocationGeneration: fence.AllocationGeneration,
		ShardIncarnation:     fence.Group.ShardIncarnation, GroupID: fence.Group.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
		OwnershipEpoch: 1, SchemaGeneration: 1, RoutingVersion: 1,
		RouteGeneration: 1, Tenant: []byte("tenant"),
		ClientID: replication.ID128{1}, ClientEpoch: 2, ClientSequence: 1,
		Fingerprint: sha256.Sum256([]byte("native-wire")),
		Batches: []replication.RelationMutationBatch{{Relation: 1,
			Mutations: []replication.Mutation{{Kind: replication.MutationPut,
				Key: []byte{1}, Value: []byte(`{"id":1}`)}}}},
	}
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func testReplicatedCompletion(
	t testing.TB,
	fence ReplicatedFence,
	applied uint64,
) []byte {
	t.Helper()
	resultDigest := replication.CompletionResultDigest(1, 1, nil)
	encoded, err := replication.AppendCompletion(nil, replication.Completion{
		ClusterID:             fence.Group.ClusterID,
		ClusterIncarnation:    fence.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: fence.Group.TopologyRecoveryEpoch,
		Distribution:          "orders", Shard: "0000-ffff",
		AllocationGeneration: fence.AllocationGeneration,
		ShardIncarnation:     fence.Group.ShardIncarnation, GroupID: fence.Group.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
		RoutingVersion: 1, RouteGeneration: 1, Tenant: []byte("tenant"),
		ClientID: replication.ID128{1}, ClientEpoch: 2, ClientSequence: 1,
		Fingerprint: sha256.Sum256([]byte("native-wire")), AppliedSequence: applied,
		ResultCode: 1, ResultFormat: 1, Storage: replication.CompletionInline,
		ResultDigest: resultDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
