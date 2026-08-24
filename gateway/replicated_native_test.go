package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
)

type scriptedReplicatedClient struct {
	states    map[string]shardservice.ReplicatedMemberState
	proposals int
	commands  [][]byte
}

func (client *scriptedReplicatedClient) DoReplicated(
	_ context.Context,
	address string,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	state := client.states[address]
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
		}, nil
	}
	client.proposals++
	client.commands = append(client.commands, append([]byte(nil), request.Command...))
	if client.proposals == 1 {
		for address, changed := range client.states {
			changed.LeaderID = 3
			changed.Fence.Term++
			client.states[address] = changed
		}
		state = client.states[address]
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedOutcomeUnknown, HasState: true, State: state,
		}, nil
	}
	command, err := replication.OpenCommand(request.Command)
	if err != nil {
		return nil, err
	}
	completion, err := appendNativeSessionCompletion(
		nil, command, command.ClientEpoch, 9, replicatedstate.ResultApplied,
	)
	if err != nil {
		return nil, err
	}
	state.Applied, state.Commit = 9, 9
	client.states[address] = state
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedCompletion, HasState: true, State: state,
		Outcome: raftserve.Outcome{Code: raftserve.OutcomeCompletion,
			AppliedIndex: 9, CompletionAppliedSequence: 9, CompletionBytes: len(completion)},
		Completion: completion,
	}, nil
}

func TestReplicatedExecutorFollowsLeaderAndRetriesExactBytesAfterUnknown(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	client := &scriptedReplicatedClient{states: states}
	executor, err := NewReplicatedExecutor(client, 2)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Propose(context.Background(), route, command)
	if err != nil {
		t.Fatal(err)
	}
	completion, completionErr := replication.OpenCompletion(result.Completion)
	if completionErr != nil || completion.ResultCode != replicatedstate.ResultApplied ||
		result.Retries != 1 ||
		client.proposals != 2 || len(client.commands) != 2 ||
		!bytes.Equal(client.commands[0], command) ||
		!bytes.Equal(client.commands[1], command) {
		t.Fatalf("result=%+v proposals=%d commands=%d", result, client.proposals, len(client.commands))
	}
}

type failingReplicatedClient struct {
	state   shardservice.ReplicatedMemberState
	command []byte
}

func (client *failingReplicatedClient) DoReplicated(
	_ context.Context,
	_ string,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: client.state,
		}, nil
	}
	client.command = append(client.command[:0], request.Command...)
	return nil, errors.New("connection lost after write")
}

func TestReplicatedExecutorExhaustedUnknownOwnsExactRetryCommand(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	client := &failingReplicatedClient{state: states["m2"]}
	// Route directly to the state this one-endpoint fake reports.
	route.Replicas = []ReplicatedEndpoint{{Member: 2, Address: "m2"}}
	executor, err := NewReplicatedExecutor(client, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Propose(context.Background(), route, command)
	var unknown *raftservice.UnknownOutcomeError
	if !errors.As(err, &unknown) || !bytes.Equal(unknown.Command, command) ||
		!bytes.Equal(client.command, command) {
		t.Fatalf("error = %T %v", err, err)
	}
}

type staleFenceReplicatedClient struct {
	oldState  shardservice.ReplicatedMemberState
	newState  shardservice.ReplicatedMemberState
	proposals int
}

func (client *staleFenceReplicatedClient) DoReplicated(
	_ context.Context,
	_ string,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: client.oldState,
		}, nil
	}
	client.proposals++
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedRefusal, HasState: true, State: client.newState,
		Refusal: shardservice.ReplicatedRefusalStaleFence,
	}, nil
}

func TestReplicatedExecutorTreatsChangedStaleFenceAsDefinite(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	oldState := states["m2"]
	newState := oldState
	newState.Fence.Command.SchemaGeneration++
	newState.Fence.Command.RelationManifestDigest[0]++
	client := &staleFenceReplicatedClient{oldState: oldState, newState: newState}
	executor, err := NewReplicatedExecutor(client, 2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Propose(context.Background(), route, command)
	if !errors.Is(err, raftservice.ErrServingFence) ||
		errors.Is(err, raftservice.ErrOutcomeUnknown) || client.proposals != 1 {
		t.Fatalf("error=%T %v proposals=%d", err, err, client.proposals)
	}
}

func testReplicatedRouteCommand(
	t testing.TB,
) (ReplicatedRoute, []byte, map[string]shardservice.ReplicatedMemberState) {
	t.Helper()
	group := raftmember.GroupKey{TopologyRecoveryEpoch: 3}
	for index := range group.ClusterID {
		group.ClusterID[index] = byte(index + 1)
		group.ClusterIncarnation[index] = byte(index + 21)
		group.ShardIncarnation[index] = byte(index + 41)
		group.GroupID[index] = byte(index + 61)
	}
	route := ReplicatedRoute{
		Group: group, AllocationGeneration: 5,
		Command: raftservice.CommandFence{
			ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
			OwnershipEpoch: 1, SchemaGeneration: 1,
			RelationManifestDigest: [32]byte{1},
			RoutingVersion:         1, RouteGeneration: 1,
		},
		Replicas: []ReplicatedEndpoint{{Member: 1, Address: "m1"},
			{Member: 2, Address: "m2"}, {Member: 3, Address: "m3"}},
	}
	states := make(map[string]shardservice.ReplicatedMemberState, 3)
	for index, endpoint := range route.Replicas {
		fence := shardservice.ReplicatedFence{
			Group: group, AllocationGeneration: route.AllocationGeneration,
			Command:  route.Command,
			MemberID: endpoint.Member, NodeIncarnation: 10 + endpoint.Member, Term: 7,
		}
		fence.StoreID[0] = byte(index + 1)
		states[endpoint.Address] = shardservice.ReplicatedMemberState{
			Fence: fence, LeaderID: 2, Commit: 8, Applied: 8, CheckpointApplied: 8,
		}
	}
	command := replication.Command{
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
		Distribution:          "orders", Shard: "0000-ffff",
		AllocationGeneration: route.AllocationGeneration,
		ShardIncarnation:     group.ShardIncarnation, GroupID: group.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
		OwnershipEpoch: 1, SchemaGeneration: 1, RoutingVersion: 1,
		RouteGeneration: 1, Tenant: []byte("tenant"),
		ClientID: replication.ID128{1}, ClientEpoch: 2, ClientSequence: 2,
		Fingerprint: sha256.Sum256([]byte("gateway-rf3")),
		Batches: []replication.RelationMutationBatch{{Relation: 1,
			Mutations: []replication.Mutation{{Kind: replication.MutationPut,
				Key: []byte{1}, Value: []byte(`{"id":1}`)}}}},
	}
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return route, encoded, states
}
