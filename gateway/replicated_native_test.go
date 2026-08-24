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
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	address := endpoint.Address
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

type pointReadClient struct {
	states        map[string]shardservice.ReplicatedMemberState
	readAddress   string
	readOperation shardservice.ReplicatedOperation
	readRefusal   shardservice.ReplicatedRefusalCode
}

type fixedReplicatedResponseClient struct {
	state         shardservice.ReplicatedMemberState
	probeResponse *shardservice.ReplicatedResponse
	response      *shardservice.ReplicatedResponse
	operation     shardservice.ReplicatedOperation
	probes        int
	requests      int
	command       []byte
}

func (client *fixedReplicatedResponseClient) DoReplicated(
	_ context.Context,
	_ ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request.Operation == shardservice.ReplicatedProbe {
		client.probes++
		if client.probeResponse != nil {
			return client.probeResponse, nil
		}
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: client.state,
		}, nil
	}
	client.operation = request.Operation
	client.requests++
	client.command = append(client.command[:0], request.Command...)
	return client.response, nil
}

func (client *pointReadClient) DoReplicated(
	_ context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	address := endpoint.Address
	state := client.states[address]
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake,
			HasState: true, State: state}, nil
	}
	client.readAddress, client.readOperation = address, request.Operation
	refusal := client.readRefusal
	if refusal == shardservice.ReplicatedRefusalNone && request.MinimumApplied > state.Applied {
		refusal = shardservice.ReplicatedRefusalReadBehind
	}
	if refusal != shardservice.ReplicatedRefusalNone {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal,
			Refusal: refusal, HasState: true, State: state}, nil
	}
	return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedReadFound,
		HasState: true, State: state, ReadApplied: state.Applied,
		Value: []byte{}}, nil
}

func TestReplicatedPointReadReturnsTypedBoundsWithoutLeaderMisclassification(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	for address, state := range states {
		state.Applied, state.Commit, state.CheckpointApplied = 10, 10, 9
		states[address] = state
	}
	for _, test := range []struct {
		name      string
		read      ReplicatedPointRead
		refusal   shardservice.ReplicatedRefusalCode
		want      error
		operation shardservice.ReplicatedOperation
	}{{name: "future-applied-floor", read: ReplicatedPointRead{
		Relation: 1, Key: []byte{0, 1}, MinimumApplied: 11,
		MaxValueBytes: 1024, Linearizable: true,
	}, want: ErrReplicatedReadBehind, operation: shardservice.ReplicatedReadLeader},
		{name: "response-buffer", read: ReplicatedPointRead{
			Relation: 1, Key: []byte{0, 1}, MinimumApplied: 9, MaxValueBytes: 8,
		}, refusal: shardservice.ReplicatedRefusalReadBufferBound,
			want: ErrReplicatedReadBufferBound, operation: shardservice.ReplicatedReadFollower}} {
		t.Run(test.name, func(t *testing.T) {
			client := &pointReadClient{states: states, readRefusal: test.refusal}
			executor, err := NewReplicatedExecutor(client, 3, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_, err = executor.ReadPoint(context.Background(), route, test.read)
			if !errors.Is(err, test.want) || errors.Is(err, ErrReplicatedLeader) ||
				errors.Is(err, ErrReplicatedRoute) {
				t.Fatalf("bound error=%T %v", err, err)
			}
			if client.readOperation != test.operation {
				t.Fatalf("operation=%d", client.readOperation)
			}
		})
	}
}

func TestReplicatedPointReadRejectsNonCanonicalCustomResponses(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	state := states["m2"]
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
	completion := testReplicatedCompletionResponse(t, command, state)
	tests := []struct {
		name     string
		response *shardservice.ReplicatedResponse
	}{
		{"nonterminal refusal", &shardservice.ReplicatedResponse{
			Kind:     shardservice.ReplicatedNotLeader,
			Refusal:  shardservice.ReplicatedRefusalAdmissionBound,
			HasState: true, State: state,
		}},
		{"nonterminal read result", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedNotLeader, HasState: true, State: state,
			ReadApplied: state.Applied,
		}},
		{"read refusal outcome", &shardservice.ReplicatedResponse{
			Kind:     shardservice.ReplicatedRefusal,
			Refusal:  shardservice.ReplicatedRefusalReadBehind,
			HasState: true, State: state,
			Outcome: raftserve.Outcome{Code: raftserve.OutcomeSessionReleased},
		}},
		{"read refusal value", &shardservice.ReplicatedResponse{
			Kind:     shardservice.ReplicatedRefusal,
			Refusal:  shardservice.ReplicatedRefusalReadBufferBound,
			HasState: true, State: state, Value: []byte{1},
		}},
		{"write-only refusal", &shardservice.ReplicatedResponse{
			Kind:     shardservice.ReplicatedRefusal,
			Refusal:  shardservice.ReplicatedRefusalProposalRefused,
			HasState: true, State: state,
		}},
		{"applied refusal", &shardservice.ReplicatedResponse{
			Kind:     shardservice.ReplicatedRefusal,
			Refusal:  shardservice.ReplicatedRefusalDeterministic,
			HasState: true, State: state,
			Outcome: raftserve.Outcome{
				Code: raftserve.OutcomeSessionReleased, AppliedIndex: state.Applied,
			},
		}},
		{"write completion", completion},
		{"read found completion", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedReadFound, HasState: true, State: state,
			ReadApplied: state.Applied, Completion: []byte{1},
		}},
		{"read missing value", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedReadMissing, HasState: true, State: state,
			ReadApplied: state.Applied, Value: []byte{1},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fixedReplicatedResponseClient{state: state, response: test.response}
			executor, err := NewReplicatedExecutor(client, 1, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_, err = executor.ReadPoint(context.Background(), route, ReplicatedPointRead{
				Relation: 1, Key: []byte("k"), MinimumApplied: state.Applied,
				MaxValueBytes: 1024, Linearizable: true,
			})
			if !errors.Is(err, ErrReplicatedRoute) ||
				errors.Is(err, ErrReplicatedReadBehind) ||
				errors.Is(err, ErrReplicatedReadBufferBound) {
				t.Fatalf("error=%T %v", err, err)
			}
			if client.requests != 1 || client.operation != shardservice.ReplicatedReadLeader {
				t.Fatalf("requests=%d operation=%d", client.requests, client.operation)
			}
		})
	}
}

func TestReplicatedPointReadTreatsChangedStaleFenceAsDefinite(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	state := states["m2"]
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
	refreshed := state
	refreshed.Fence.Command.SchemaGeneration++
	refreshed.Fence.Command.RelationManifestDigest[0]++
	client := &fixedReplicatedResponseClient{state: state,
		response: &shardservice.ReplicatedResponse{
			Kind:     shardservice.ReplicatedRefusal,
			Refusal:  shardservice.ReplicatedRefusalStaleFence,
			HasState: true, State: refreshed,
		},
	}
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.ReadPoint(context.Background(), route, ReplicatedPointRead{
		Relation: 1, Key: []byte("k"), MinimumApplied: state.Applied,
		MaxValueBytes: 1024, Linearizable: true,
	})
	if !errors.Is(err, raftservice.ErrServingFence) ||
		errors.Is(err, ErrReplicatedRoute) || errors.Is(err, ErrReplicatedLeader) ||
		client.requests != 1 {
		t.Fatalf("error=%T %v requests=%d", err, err, client.requests)
	}
}

func TestReplicatedPointReadPrefersAppliedFollowerAndKeepsLeaderReadStrict(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	for address, state := range states {
		state.Applied, state.Commit, state.CheckpointApplied = 10, 10, 9
		states[address] = state
	}
	executor, err := NewReplicatedExecutor(&pointReadClient{states: states}, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	followerClient := executor.client.(*pointReadClient)
	result, err := executor.ReadPoint(context.Background(), route, ReplicatedPointRead{
		Relation: 1, Key: []byte{0, 1}, MinimumApplied: 9,
		MaxValueBytes: 1024,
	})
	if err != nil || !result.Found || result.Applied != 10 ||
		followerClient.readAddress != "m1" ||
		followerClient.readOperation != shardservice.ReplicatedReadFollower {
		t.Fatalf("follower result=%+v address=%q op=%d err=%v",
			result, followerClient.readAddress, followerClient.readOperation, err)
	}
	leaderClient := &pointReadClient{states: states}
	executor.client = leaderClient
	result, err = executor.ReadPoint(context.Background(), route, ReplicatedPointRead{
		Relation: 1, Key: []byte{0, 1}, MinimumApplied: 9,
		MaxValueBytes: 1024, Linearizable: true,
	})
	if err != nil || leaderClient.readAddress != "m2" ||
		leaderClient.readOperation != shardservice.ReplicatedReadLeader {
		t.Fatalf("leader result=%+v address=%q op=%d err=%v",
			result, leaderClient.readAddress, leaderClient.readOperation, err)
	}
}

func BenchmarkReplicatedPointReadAppliedFollower(b *testing.B) {
	route, _, states := testReplicatedRouteCommand(b)
	for address, state := range states {
		state.Applied, state.Commit, state.CheckpointApplied = 10, 10, 9
		states[address] = state
	}
	client := &pointReadClient{states: states}
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		b.Fatal(err)
	}
	read := ReplicatedPointRead{Relation: 1, Key: []byte{0, 1}, MinimumApplied: 9,
		MaxValueBytes: 1024}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := executor.ReadPoint(context.Background(), route, read); err != nil {
			b.Fatal(err)
		}
	}
}

type failingReplicatedClient struct {
	state   shardservice.ReplicatedMemberState
	command []byte
}

func (client *failingReplicatedClient) DoReplicated(
	_ context.Context,
	_ ReplicatedEndpoint,
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
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
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
	_ ReplicatedEndpoint,
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
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
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
		{"read behind", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalReadBehind,
			HasState: true, State: state,
		}},
		{"read buffer bound", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalReadBufferBound,
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
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
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
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
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

func TestReplicatedExecutorTreatsReadOnlyProposalRefusalsAsUnknown(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	state := states["m2"]
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
	for _, test := range []struct {
		name string
		code shardservice.ReplicatedRefusalCode
	}{
		{"read behind", shardservice.ReplicatedRefusalReadBehind},
		{"read buffer bound", shardservice.ReplicatedRefusalReadBufferBound},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := &shardservice.ReplicatedResponse{
				Kind: shardservice.ReplicatedRefusal, Refusal: test.code,
				HasState: true, State: state,
			}
			client := &fixedReplicatedResponseClient{state: state, response: response}
			executor, err := NewReplicatedExecutor(client, 1, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			sent := append([]byte(nil), command...)
			want := append([]byte(nil), sent...)
			_, err = executor.Propose(context.Background(), route, sent)
			sent[0] ^= 0xff
			var unknown *raftservice.UnknownOutcomeError
			if !errors.As(err, &unknown) || !bytes.Equal(unknown.Command, want) ||
				!bytes.Equal(client.command, want) ||
				errors.Is(err, ErrReplicatedReadBehind) ||
				errors.Is(err, ErrReplicatedReadBufferBound) {
				t.Fatalf("error=%T %v unknown=%+v sent=%t", err, err,
					unknown, bytes.Equal(client.command, want))
			}
			if client.requests != 1 || client.operation != shardservice.ReplicatedPropose {
				t.Fatalf("requests=%d operation=%d", client.requests, client.operation)
			}
		})
	}
}

func TestReplicatedExecutorRejectsNonCanonicalCustomWriteResponses(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	state := states["m2"]
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
	completion := testReplicatedCompletionResponse(t, command, state)
	tests := []struct {
		name     string
		response *shardservice.ReplicatedResponse
	}{
		{"completion read applied", &shardservice.ReplicatedResponse{
			Kind: completion.Kind, HasState: true, State: state,
			Outcome: completion.Outcome, Completion: completion.Completion,
			ReadApplied: state.Applied,
		}},
		{"completion value", &shardservice.ReplicatedResponse{
			Kind: completion.Kind, HasState: true, State: state,
			Outcome: completion.Outcome, Completion: completion.Completion, Value: []byte{1},
		}},
		{"completion refusal", &shardservice.ReplicatedResponse{
			Kind: completion.Kind, Refusal: shardservice.ReplicatedRefusalAdmissionBound,
			HasState: true, State: state,
			Outcome: completion.Outcome, Completion: completion.Completion,
		}},
		{"not leader value", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedNotLeader, HasState: true, State: state,
			Value: []byte{1},
		}},
		{"not leader outcome", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedNotLeader, HasState: true, State: state,
			Outcome: raftserve.Outcome{Code: raftserve.OutcomeSessionReleased},
		}},
		{"outcome unknown read applied", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedOutcomeUnknown, HasState: true, State: state,
			ReadApplied: state.Applied,
		}},
		{"refusal value", &shardservice.ReplicatedResponse{
			Kind:     shardservice.ReplicatedRefusal,
			Refusal:  shardservice.ReplicatedRefusalAdmissionBound,
			HasState: true, State: state, Value: []byte{1},
		}},
		{"refusal completion", &shardservice.ReplicatedResponse{
			Kind:     shardservice.ReplicatedRefusal,
			Refusal:  shardservice.ReplicatedRefusalAdmissionBound,
			HasState: true, State: state, Completion: []byte{1},
		}},
		{"deterministic read applied", &shardservice.ReplicatedResponse{
			Kind:     shardservice.ReplicatedRefusal,
			Refusal:  shardservice.ReplicatedRefusalDeterministic,
			HasState: true, State: state,
			Outcome: raftserve.Outcome{
				Code: raftserve.OutcomeSessionReleased, AppliedIndex: state.Applied,
			},
			ReadApplied: state.Applied,
		}},
		{"stateless unavailable value", &shardservice.ReplicatedResponse{
			Kind:    shardservice.ReplicatedRefusal,
			Refusal: shardservice.ReplicatedRefusalUnavailable, Value: []byte{1},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fixedReplicatedResponseClient{state: state, response: test.response}
			executor, err := NewReplicatedExecutor(client, 1, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_, err = executor.Propose(context.Background(), route, command)
			var unknown *raftservice.UnknownOutcomeError
			if !errors.As(err, &unknown) || !bytes.Equal(unknown.Command, command) ||
				!bytes.Equal(client.command, command) {
				t.Fatalf("error=%T %v", err, err)
			}
			if client.requests != 1 || client.operation != shardservice.ReplicatedPropose {
				t.Fatalf("requests=%d operation=%d", client.requests, client.operation)
			}
		})
	}
}

type deadlineReplicatedClient struct {
	state shardservice.ReplicatedMemberState
}

func (client deadlineReplicatedClient) DoReplicated(
	ctx context.Context,
	_ ReplicatedEndpoint,
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
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
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
		context.Background(), ReplicatedEndpoint{Address: "127.0.0.1:1"}, &shardservice.ReplicatedRequest{},
	)
	if !errors.Is(err, ErrReplicatedDial) {
		t.Fatalf("nil Dial = %v", err)
	}
}

func TestReplicatedExecutorRejectsNonCanonicalCustomHandshakes(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	state := states["m2"]
	followerState := states["m1"]
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
	tests := []struct {
		name     string
		response *shardservice.ReplicatedResponse
	}{
		{"refusal", &shardservice.ReplicatedResponse{
			Kind:     shardservice.ReplicatedHandshake,
			Refusal:  shardservice.ReplicatedRefusalAdmissionBound,
			HasState: true, State: state,
		}},
		{"outcome", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
			Outcome: raftserve.Outcome{Code: raftserve.OutcomeSessionReleased},
		}},
		{"completion", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
			Completion: []byte{1},
		}},
		{"read applied", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
			ReadApplied: state.Applied,
		}},
		{"value", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
			Value: []byte{1},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, operation := range []struct {
				name string
				read bool
			}{{"leader discovery", false}, {"follower selection", true}} {
				t.Run(operation.name, func(t *testing.T) {
					selectedRoute, selectedState := route, state
					response := *test.response
					if operation.read {
						selectedRoute.Replicas = []ReplicatedEndpoint{route.Replicas[0]}
						selectedState = followerState
						response.State = followerState
					}
					client := &fixedReplicatedResponseClient{
						state: selectedState, probeResponse: &response,
					}
					executor, err := NewReplicatedExecutor(client, 1, time.Second)
					if err != nil {
						t.Fatal(err)
					}
					if operation.read {
						_, err = executor.ReadPoint(context.Background(), selectedRoute, ReplicatedPointRead{
							Relation: 1, Key: []byte("k"), MinimumApplied: selectedState.Applied,
							MaxValueBytes: 1024,
						})
					} else {
						_, err = executor.Propose(context.Background(), selectedRoute, command)
					}
					if !errors.Is(err, ErrReplicatedRoute) || client.requests != 0 ||
						client.probes == 0 {
						t.Fatalf("error=%T %v probes=%d requests=%d",
							err, err, client.probes, client.requests)
					}
				})
			}
		})
	}
}

func (client *staleFenceReplicatedClient) DoReplicated(
	_ context.Context,
	_ ReplicatedEndpoint,
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

func testReplicatedCompletionResponse(
	t testing.TB,
	commandBytes []byte,
	state shardservice.ReplicatedMemberState,
) *shardservice.ReplicatedResponse {
	t.Helper()
	command, err := replication.OpenCommand(commandBytes)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := appendNativeSessionCompletion(
		nil, command, command.ClientEpoch, state.Applied, replicatedstate.ResultApplied,
	)
	if err != nil {
		t.Fatal(err)
	}
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedCompletion, HasState: true, State: state,
		Outcome: raftserve.Outcome{
			Code: raftserve.OutcomeCompletion, AppliedIndex: state.Applied,
			CompletionAppliedSequence: state.Applied, CompletionBytes: len(completion),
		},
		Completion: completion,
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
		Replicas: []ReplicatedEndpoint{
			{Member: 1, Node: [16]byte{1}, StoreID: [16]byte{1}, NodeIncarnation: 11, Address: "m1"},
			{Member: 2, Node: [16]byte{2}, StoreID: [16]byte{2}, NodeIncarnation: 12, Address: "m2"},
			{Member: 3, Node: [16]byte{3}, StoreID: [16]byte{3}, NodeIncarnation: 13, Address: "m3"},
		},
	}
	states := make(map[string]shardservice.ReplicatedMemberState, 3)
	for _, endpoint := range route.Replicas {
		fence := shardservice.ReplicatedFence{
			Group: group, AllocationGeneration: route.AllocationGeneration,
			Command:  route.Command,
			MemberID: endpoint.Member, NodeIncarnation: 10 + endpoint.Member, Term: 7,
		}
		fence.StoreID = endpoint.StoreID
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
