package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
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
	executor, err := NewReplicatedExecutor(client, 2, time.Second)
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

type membershipReplicatedClient struct {
	state      shardservice.ReplicatedMemberState
	response   *shardservice.ReplicatedResponse
	err        error
	membership shardservice.ReplicatedMembershipRequest
}

func (client *membershipReplicatedClient) DoReplicated(
	_ context.Context,
	_ string,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake,
			HasState: true, State: client.state}, nil
	}
	client.membership = request.Membership
	return client.response, client.err
}

func TestReplicatedExecutorMembershipAcceptedAndUnknownAreDistinct(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	route.Replicas = []ReplicatedEndpoint{{Member: 2, Address: "m2"}}
	state := states["m2"]
	membership := shardservice.ReplicatedMembershipRequest{
		Kind: raftservice.MembershipAddLearner, TransitionID: [16]byte{8},
		MetadataEpoch: 9, CatalogGeneration: 10,
		ExpectedReplicaSetVersion: route.Command.ReplicaSetVersion,
		SourceMember:              2, TargetMember: 3,
	}
	client := &membershipReplicatedClient{state: state,
		response: &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedMembershipAccepted, HasState: true, State: state}}
	executor, err := NewReplicatedExecutor(client, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.ApplyMembership(context.Background(), route, membership)
	if err != nil || result.State != state || client.membership != membership {
		t.Fatalf("accepted result=%+v err=%v sent=%+v", result, err, client.membership)
	}
	client.err = errors.New("lost after write")
	client.response = nil
	_, err = executor.ApplyMembership(context.Background(), route, membership)
	if !errors.Is(err, raftservice.ErrOutcomeUnknown) {
		t.Fatalf("transport outcome = %v", err)
	}
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
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
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

type sequenceReplicatedClient struct {
	state     shardservice.ReplicatedMemberState
	responses []*shardservice.ReplicatedResponse
	proposals int
}

func (client *sequenceReplicatedClient) DoReplicated(
	_ context.Context,
	_ string,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: client.state,
		}, nil
	}
	index := client.proposals
	client.proposals++
	if index >= len(client.responses) {
		return nil, errors.New("unexpected proposal")
	}
	return client.responses[index], nil
}

func TestReplicatedExecutorPreservesPriorUnknownUntilAppliedProof(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	state := states["m2"]
	route.Replicas = []ReplicatedEndpoint{{Member: 2, Address: "m2"}}
	unknown := &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedOutcomeUnknown, HasState: true, State: state,
	}
	changedFence := state
	changedFence.Fence.Command.SchemaGeneration++
	changedFence.Fence.Command.RelationManifestDigest[0]++
	wrongMember := state
	wrongMember.Fence.MemberID++
	tests := []struct {
		name     string
		response *shardservice.ReplicatedResponse
	}{
		{"changed stale fence", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalStaleFence,
			HasState: true, State: changedFence,
		}},
		{"admission bound", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalAdmissionBound,
			HasState: true, State: state,
		}},
		{"proposal refused", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalProposalRefused,
			HasState: true, State: state,
		}},
		{"unavailable", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalUnavailable,
			HasState: true, State: state,
		}},
		{"unavailable without state", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalUnavailable,
		}},
		{"wrong member", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedNotLeader, HasState: true, State: wrongMember,
		}},
		{"malformed kind", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
		}},
		{"unproven deterministic refusal", &shardservice.ReplicatedResponse{
			Kind:     shardservice.ReplicatedRefusal,
			Refusal:  shardservice.ReplicatedRefusalDeterministic,
			HasState: true, State: state,
			Outcome: raftserve.Outcome{Code: raftserve.OutcomeSessionReleased},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &sequenceReplicatedClient{
				state: state, responses: []*shardservice.ReplicatedResponse{unknown, test.response},
			}
			executor, err := NewReplicatedExecutor(client, 2, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_, err = executor.Propose(context.Background(), route, command)
			var outcome *raftservice.UnknownOutcomeError
			if !errors.As(err, &outcome) || !bytes.Equal(outcome.Command, command) ||
				client.proposals != 2 {
				t.Fatalf("error=%T %v proposals=%d", err, err, client.proposals)
			}
		})
	}
}

func TestReplicatedExecutorAppliedDeterministicRefusalResolvesUnknown(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	state := states["m2"]
	route.Replicas = []ReplicatedEndpoint{{Member: 2, Address: "m2"}}
	state.Commit, state.Applied = 12, 12
	client := &sequenceReplicatedClient{
		state: state,
		responses: []*shardservice.ReplicatedResponse{
			{Kind: shardservice.ReplicatedOutcomeUnknown, HasState: true, State: state},
			{Kind: shardservice.ReplicatedRefusal,
				Refusal:  shardservice.ReplicatedRefusalDeterministic,
				HasState: true, State: state,
				Outcome: raftserve.Outcome{
					Code: raftserve.OutcomeSessionReleased, AppliedIndex: 12,
				}},
		},
	}
	executor, err := NewReplicatedExecutor(client, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Propose(context.Background(), route, command)
	if !errors.Is(err, replicatedstate.ErrSessionReleased) ||
		errors.Is(err, raftservice.ErrOutcomeUnknown) || client.proposals != 2 {
		t.Fatalf("error=%T %v proposals=%d", err, err, client.proposals)
	}
}

func TestReplicatedExecutorPreAdmissionRefusalIsDefiniteWithoutPriorUnknown(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	state := states["m2"]
	route.Replicas = []ReplicatedEndpoint{{Member: 2, Address: "m2"}}
	tests := []struct {
		name string
		code shardservice.ReplicatedRefusalCode
		want error
	}{
		{"admission bound", shardservice.ReplicatedRefusalAdmissionBound, raftmodel.ErrAdmissionBound},
		{"proposal refused", shardservice.ReplicatedRefusalProposalRefused, raftserve.ErrProposalRefused},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &sequenceReplicatedClient{
				state: state,
				responses: []*shardservice.ReplicatedResponse{{
					Kind:     shardservice.ReplicatedRefusal,
					Refusal:  test.code,
					HasState: true, State: state,
				}},
			}
			executor, err := NewReplicatedExecutor(client, 1, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_, err = executor.Propose(context.Background(), route, command)
			if !errors.Is(err, test.want) || errors.Is(err, raftservice.ErrOutcomeUnknown) {
				t.Fatalf("error=%T %v", err, err)
			}
		})
	}
}

type deadlineReplicatedClient struct {
	state shardservice.ReplicatedMemberState
}

func (client deadlineReplicatedClient) DoReplicated(
	ctx context.Context,
	_ string,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: client.state,
		}, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestReplicatedExecutorRequiresAndEnforcesPerAttemptTimeout(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	route.Replicas = []ReplicatedEndpoint{{Member: 2, Address: "m2"}}
	if _, err := NewReplicatedExecutor(deadlineReplicatedClient{}, 1, 0); !errors.Is(err, ErrReplicatedRoute) {
		t.Fatalf("zero attempt timeout = %v", err)
	}
	if _, err := NewReplicatedExecutor(
		deadlineReplicatedClient{}, 1, AbsoluteMaxReplicatedAttemptTimeout+time.Nanosecond,
	); !errors.Is(err, ErrReplicatedRoute) {
		t.Fatalf("over-max attempt timeout = %v", err)
	}
	executor, err := NewReplicatedExecutor(
		deadlineReplicatedClient{state: states["m2"]}, 1, 20*time.Millisecond,
	)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err = executor.Propose(context.Background(), route, command)
	if !errors.Is(err, raftservice.ErrOutcomeUnknown) || time.Since(started) > time.Second {
		t.Fatalf("bounded proposal = %T %v after %s", err, err, time.Since(started))
	}
}

func TestTCPReplicatedClientRequiresExplicitDial(t *testing.T) {
	_, err := (TCPReplicatedClient{}).DoReplicated(
		context.Background(), "127.0.0.1:1", &shardservice.ReplicatedRequest{},
	)
	if !errors.Is(err, ErrReplicatedDial) {
		t.Fatalf("nil Dial = %v", err)
	}
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
	executor, err := NewReplicatedExecutor(client, 2, time.Second)
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
