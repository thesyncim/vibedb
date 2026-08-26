package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"
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

type staleSelfLeaderReplicatedClient struct {
	states    map[string]shardservice.ReplicatedMemberState
	addresses []string
	commands  [][]byte
}

type leaderlessElectionReplicatedClient struct {
	states           map[string]shardservice.ReplicatedMemberState
	unknown          bool
	leaderlessProbes int
	addresses        []string
	commands         [][]byte
}

func (client *leaderlessElectionReplicatedClient) DoReplicated(
	_ context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	state := client.states[endpoint.Address]
	if request.Operation == shardservice.ReplicatedProbe {
		if client.unknown && client.leaderlessProbes < len(client.states) {
			client.leaderlessProbes++
			state.LeaderID = 0
		}
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
		}, nil
	}
	client.addresses = append(client.addresses, endpoint.Address)
	client.commands = append(client.commands, append([]byte(nil), request.Command...))
	if !client.unknown {
		client.unknown = true
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedOutcomeUnknown, HasState: true, State: state,
		}, nil
	}
	if endpoint.Member != 3 {
		return nil, errors.New("proposal did not wait for replacement leader")
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
	state.Commit, state.Applied = 9, 9
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedCompletion, HasState: true, State: state,
		RequestDigest: replicatedRequestDigest(request.Command),
		Outcome: raftserve.Outcome{Code: raftserve.OutcomeCompletion,
			AppliedIndex: 9, CompletionAppliedSequence: 9, CompletionBytes: len(completion)},
		Completion: completion,
	}, nil
}

func (client *staleSelfLeaderReplicatedClient) DoReplicated(
	_ context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	state := client.states[endpoint.Address]
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
		}, nil
	}
	client.addresses = append(client.addresses, endpoint.Address)
	client.commands = append(client.commands, append([]byte(nil), request.Command...))
	if endpoint.Member == 1 {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedOutcomeUnknown, HasState: true, State: state,
		}, nil
	}
	if endpoint.Member != 3 {
		return nil, errors.New("proposal did not rotate to replacement leader")
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
	state.Commit, state.Applied = 9, 9
	client.states[endpoint.Address] = state
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedCompletion, HasState: true, State: state,
		RequestDigest: replicatedRequestDigest(request.Command),
		Outcome: raftserve.Outcome{Code: raftserve.OutcomeCompletion,
			AppliedIndex: 9, CompletionAppliedSequence: 9, CompletionBytes: len(completion)},
		Completion: completion,
	}, nil
}

var replicatedRequestDigestSink [sha256.Size]byte

func TestReplicatedRequestDigestIsExactAndAllocationFree(t *testing.T) {
	command := make([]byte, 4<<10)
	for index := range command {
		command[index] = byte(index)
	}
	want := replicatedRequestDigest(command)
	if allocations := testing.AllocsPerRun(1000, func() {
		replicatedRequestDigestSink = replicatedRequestDigest(command)
	}); allocations != 0 {
		t.Fatalf("request digest allocations = %v", allocations)
	}
	command[len(command)-1]++
	if got := replicatedRequestDigest(command); got == want {
		t.Fatal("request digest did not bind the exact command bytes")
	}
}

func BenchmarkReplicatedRequestDigest(b *testing.B) {
	for _, test := range []struct {
		name string
		size int
	}{{"4KiB", 4 << 10}, {"1MiB", 1 << 20}} {
		b.Run(test.name, func(b *testing.B) {
			command := make([]byte, test.size)
			b.SetBytes(int64(len(command)))
			b.ReportAllocs()
			for b.Loop() {
				replicatedRequestDigestSink = replicatedRequestDigest(command)
			}
		})
	}
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
		RequestDigest: replicatedRequestDigest(request.Command),
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

func TestReplicatedExecutorRotatesAwayFromStaleSelfLeaderAfterUnknown(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	stale := states["m1"]
	stale.LeaderID, stale.Fence.Term = 1, 7
	states["m1"] = stale
	for _, address := range []string{"m2", "m3"} {
		current := states[address]
		current.LeaderID, current.Fence.Term = 3, 8
		states[address] = current
	}
	client := &staleSelfLeaderReplicatedClient{states: states}
	executor, err := NewReplicatedExecutor(client, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Propose(context.Background(), route, command)
	if err != nil {
		t.Fatal(err)
	}
	completion, completionErr := replication.OpenCompletion(result.Completion)
	if completionErr != nil || completion.ResultCode != replicatedstate.ResultApplied ||
		result.Retries != 1 || !slices.Equal(client.addresses, []string{"m1", "m3"}) ||
		len(client.commands) != 2 || !bytes.Equal(client.commands[0], command) ||
		!bytes.Equal(client.commands[1], command) {
		t.Fatalf("result=%+v addresses=%v commands=%d", result, client.addresses, len(client.commands))
	}
}

func TestReplicatedExecutorKeepsBoundedRetryAliveThroughLeaderlessElection(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	stale := states["m1"]
	stale.LeaderID, stale.Fence.Term = 1, 7
	states["m1"] = stale
	for _, address := range []string{"m2", "m3"} {
		current := states[address]
		current.LeaderID, current.Fence.Term = 3, 8
		states[address] = current
	}
	client := &leaderlessElectionReplicatedClient{states: states}
	executor, err := NewReplicatedExecutor(client, 4, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Propose(context.Background(), route, command)
	if err != nil {
		t.Fatal(err)
	}
	completion, completionErr := replication.OpenCompletion(result.Completion)
	if completionErr != nil || completion.ResultCode != replicatedstate.ResultApplied ||
		result.Retries != 2 || client.leaderlessProbes != len(states) ||
		!slices.Equal(client.addresses, []string{"m1", "m3"}) ||
		len(client.commands) != 2 || !bytes.Equal(client.commands[0], command) ||
		!bytes.Equal(client.commands[1], command) {
		t.Fatalf("result=%+v probes=%d addresses=%v commands=%d", result,
			client.leaderlessProbes, client.addresses, len(client.commands))
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
			RequestDigest: [32]byte{0xff},
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

func TestReplicatedPointReadRetriesSameCommandFenceIncarnationRace(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	state := states["m2"]
	state.Applied, state.Commit = 10, 10
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
	refreshed := state
	refreshed.Fence.Term++
	client := &sequenceReplicatedClient{state: refreshed, responses: []*shardservice.ReplicatedResponse{
		{Kind: shardservice.ReplicatedRefusal,
			Refusal: shardservice.ReplicatedRefusalStaleFence, HasState: true, State: refreshed},
		{Kind: shardservice.ReplicatedReadFound,
			HasState: true, State: refreshed, ReadApplied: refreshed.Applied, Value: []byte("value")},
	}}
	executor, err := NewReplicatedExecutor(client, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.ReadPoint(context.Background(), route, ReplicatedPointRead{
		Relation: 1, Key: []byte("k"), MinimumApplied: 10,
		MaxValueBytes: 1024, Linearizable: true,
	})
	if err != nil || !result.Found || result.Retries != 1 ||
		!bytes.Equal(result.Value, []byte("value")) || client.proposals != 2 {
		t.Fatalf("result=%+v requests=%d err=%v", result, client.proposals, err)
	}
}

func TestReplicatedPointReadStopsServingFenceBackoffOnCancellation(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	state := states["m2"]
	state.Applied, state.Commit = 10, 10
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
	client := &sequenceReplicatedClient{state: state, responses: []*shardservice.ReplicatedResponse{{
		Kind:    shardservice.ReplicatedRefusal,
		Refusal: shardservice.ReplicatedRefusalStaleFence, HasState: true, State: state,
	}}}
	executor, err := NewReplicatedExecutor(client, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = executor.ReadPoint(ctx, route, ReplicatedPointRead{
		Relation: 1, Key: []byte("k"), MinimumApplied: 10,
		MaxValueBytes: 1024, Linearizable: true,
	})
	if !errors.Is(err, context.Canceled) || client.proposals != 1 {
		t.Fatalf("error=%T %v requests=%d", err, err, client.proposals)
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

type membershipReplicatedClient struct {
	state      shardservice.ReplicatedMemberState
	response   *shardservice.ReplicatedResponse
	err        error
	membership shardservice.ReplicatedMembershipRequest
}

type countingMembershipClient struct{ calls int }

func (client *countingMembershipClient) DoReplicated(
	context.Context,
	ReplicatedEndpoint,
	*shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	client.calls++
	return nil, errors.New("membership client must not be called")
}

func (client *membershipReplicatedClient) DoReplicated(
	_ context.Context,
	_ ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake,
			HasState: true, State: client.state}, nil
	}
	client.membership = request.Membership
	return client.response, client.err
}

func TestReplicatedExecutorRejectsMalformedMembershipBeforeDiscovery(t *testing.T) {
	route, _, _ := testReplicatedRouteCommand(t)
	valid := shardservice.ReplicatedMembershipRequest{
		Kind: raftservice.MembershipAddLearner, TransitionID: [16]byte{1},
		MetadataEpoch: 2, CatalogGeneration: 3,
		ExpectedReplicaSetVersion: route.Command.ReplicaSetVersion,
		SourceMember:              2, TargetMember: 4,
	}
	tests := []struct {
		name string
		edit func(*shardservice.ReplicatedMembershipRequest)
		want error
	}{
		{"zero_kind", func(request *shardservice.ReplicatedMembershipRequest) { request.Kind = 0 }, raftservice.ErrMembershipMalformed},
		{"unknown_kind", func(request *shardservice.ReplicatedMembershipRequest) {
			request.Kind = raftservice.MembershipTransferLeader + 1
		}, raftservice.ErrMembershipMalformed},
		{"zero_transition", func(request *shardservice.ReplicatedMembershipRequest) { request.TransitionID = [16]byte{} }, raftservice.ErrMembershipMalformed},
		{"zero_metadata_epoch", func(request *shardservice.ReplicatedMembershipRequest) { request.MetadataEpoch = 0 }, raftservice.ErrMembershipMalformed},
		{"zero_catalog_generation", func(request *shardservice.ReplicatedMembershipRequest) { request.CatalogGeneration = 0 }, raftservice.ErrMembershipMalformed},
		{"zero_replica_version", func(request *shardservice.ReplicatedMembershipRequest) { request.ExpectedReplicaSetVersion = 0 }, raftservice.ErrMembershipMalformed},
		{"stale_replica_version", func(request *shardservice.ReplicatedMembershipRequest) { request.ExpectedReplicaSetVersion++ }, ErrReplicatedRoute},
		{"zero_source", func(request *shardservice.ReplicatedMembershipRequest) { request.SourceMember = 0 }, raftservice.ErrMembershipMalformed},
		{"zero_target", func(request *shardservice.ReplicatedMembershipRequest) { request.TargetMember = 0 }, raftservice.ErrMembershipMalformed},
		{"same_members", func(request *shardservice.ReplicatedMembershipRequest) { request.TargetMember = request.SourceMember }, raftservice.ErrMembershipMalformed},
		{"term_without_remove", func(request *shardservice.ReplicatedMembershipRequest) { request.TransferTerm = 9 }, raftservice.ErrMembershipMalformed},
		{"remove_without_term", func(request *shardservice.ReplicatedMembershipRequest) {
			request.Kind = raftservice.MembershipRemoveVoter
		}, raftservice.ErrMembershipMalformed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid
			test.edit(&request)
			client := new(countingMembershipClient)
			executor, err := NewReplicatedExecutor(client, 3, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_, err = executor.ApplyMembership(context.Background(), route, request)
			if !errors.Is(err, test.want) || client.calls != 0 {
				t.Fatalf("err=%v calls=%d", err, client.calls)
			}
		})
	}
}

type transferReplicatedClient struct {
	states        map[string]shardservice.ReplicatedMemberState
	moved         bool
	failAfterMove bool
}

func (client *transferReplicatedClient) DoReplicated(
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
	if request.Operation != shardservice.ReplicatedMembership {
		return nil, errors.New("unexpected operation")
	}
	for endpoint, moved := range client.states {
		moved.LeaderID = request.Membership.TargetMember
		moved.Fence.Term++
		client.states[endpoint] = moved
	}
	client.moved = true
	if client.failAfterMove {
		return nil, errors.New("lost transfer response")
	}
	return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedMembershipAccepted,
		HasState: true, State: state}, nil
}

func TestReplicatedExecutorTransferReturnsConsumableLeaderTermWitness(t *testing.T) {
	for _, failAfterMove := range []bool{false, true} {
		route, _, states := testReplicatedRouteCommand(t)
		client := &transferReplicatedClient{states: states, failAfterMove: failAfterMove}
		executor, err := NewReplicatedExecutor(client, 3, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		source := states["m2"].LeaderID
		beforeTerm := states["m2"].Fence.Term
		target := uint64(3)
		membership := shardservice.ReplicatedMembershipRequest{
			Kind: raftservice.MembershipTransferLeader, TransitionID: [16]byte{9},
			MetadataEpoch: 10, CatalogGeneration: 11,
			ExpectedReplicaSetVersion: route.Command.ReplicaSetVersion,
			SourceMember:              source, TargetMember: target,
		}
		result, err := executor.ApplyMembership(context.Background(), route, membership)
		if err != nil || !client.moved || result.TransferWitness.TargetMember != target ||
			result.TransferWitness.Term <= beforeTerm ||
			result.State.Fence.MemberID != target || result.State.LeaderID != target {
			t.Fatalf("fail=%t transfer result=%+v err=%v", failAfterMove, result, err)
		}
		remove := membership
		remove.Kind = raftservice.MembershipRemoveVoter
		remove.TransferTerm = result.TransferWitness.Term
		if remove.TransferTerm == 0 {
			t.Fatal("removal did not consume a transfer term")
		}
	}
}

func TestReplicatedExecutorObservesTransferAfterUnknownWithoutResend(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	target := uint64(3)
	afterTerm := states["m2"].Fence.Term
	for address, state := range states {
		state.LeaderID = target
		state.Fence.Term = afterTerm + 1
		states[address] = state
	}
	client := &transferReplicatedClient{states: states}
	executor, err := NewReplicatedExecutor(client, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.ObserveMembershipTransfer(context.Background(), route,
		target, afterTerm)
	if err != nil || client.moved || result.TransferWitness.TargetMember != target ||
		result.TransferWitness.Term != afterTerm+1 || result.State.Fence.MemberID != target {
		t.Fatalf("observation result=%+v moved=%t err=%v", result, client.moved, err)
	}
}

func TestReplicatedExecutorMembershipAcceptedAndUnknownAreDistinct(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
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
	client.err = nil
	client.response = &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalUnauthorized,
	}
	_, err = executor.ApplyMembership(context.Background(), route, membership)
	if !errors.Is(err, ErrReplicatedUnauthorized) || errors.Is(err, raftservice.ErrOutcomeUnknown) {
		t.Fatalf("stateless authorization refusal = %v", err)
	}
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

func TestReplicatedExecutorInternalUnknownOwnershipModesDoNotClone(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}

	owned := make([]byte, len(command), len(command)+4096)
	copy(owned, command)
	ownedClient := &failingReplicatedClient{state: states["m2"]}
	ownedExecutor, err := NewReplicatedExecutor(ownedClient, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ownedExecutor.proposeOwned(context.Background(), route, owned)
	var transferred *raftservice.UnknownOutcomeError
	if !errors.As(err, &transferred) || len(transferred.Command) != len(owned) ||
		&transferred.Command[0] != &owned[0] || cap(transferred.Command) != len(owned) {
		t.Fatalf("owned outcome=%T %+v", err, transferred)
	}

	borrowedClient := &failingReplicatedClient{state: states["m2"]}
	borrowedExecutor, err := NewReplicatedExecutor(borrowedClient, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = borrowedExecutor.retryUnknownBorrowed(
		context.Background(), route, transferred.Command,
	)
	var borrowed *raftservice.UnknownOutcomeError
	if !errors.As(err, &borrowed) || borrowed.Command != nil ||
		!errors.Is(err, raftservice.ErrOutcomeUnknown) {
		t.Fatalf("borrowed outcome=%T %+v", err, borrowed)
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

type cancelFailoverRetryClient struct {
	state     shardservice.ReplicatedMemberState
	cancel    context.CancelFunc
	proposals int
}

func (client *cancelFailoverRetryClient) DoReplicated(
	_ context.Context,
	_ ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: client.state,
		}, nil
	}
	client.proposals++
	client.cancel()
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedOutcomeUnknown, HasState: true, State: client.state,
	}, nil
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

func TestReplicatedExecutorCancellationStopsOutcomeUnknownRetry(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
	ctx, cancel := context.WithCancel(context.Background())
	client := &cancelFailoverRetryClient{state: states["m2"], cancel: cancel}
	executor, err := NewReplicatedExecutor(client, 8, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Propose(ctx, route, command)
	var unknown *raftservice.UnknownOutcomeError
	if !errors.As(err, &unknown) || !errors.Is(err, context.Canceled) ||
		!bytes.Equal(unknown.Command, command) || client.proposals != 1 {
		t.Fatalf("error=%T %v proposals=%d", err, err, client.proposals)
	}
}

func TestReplicatedFailoverRetryBudgetSpansElectionBound(t *testing.T) {
	var total time.Duration
	for attempt := 0; attempt < 7; attempt++ {
		total += replicatedFailoverRetryDelay(attempt)
	}
	if total != 1260*time.Millisecond ||
		replicatedFailoverRetryDelay(-1) != 20*time.Millisecond ||
		replicatedFailoverRetryDelay(7) != 320*time.Millisecond {
		t.Fatalf("retry budget=%v base=%v cap=%v", total,
			replicatedFailoverRetryDelay(-1), replicatedFailoverRetryDelay(7))
	}
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
	wrongDigestCompletion := testReplicatedCompletionResponse(t, command, state)
	wrongDigestCompletion.RequestDigest[0] ^= 0xff
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
		{"wrong request completion", wrongDigestCompletion},
		{"wrong request applied refusal", &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalDeterministic,
			HasState: true, State: state, RequestDigest: [32]byte{0xff},
			Outcome: raftserve.Outcome{Code: raftserve.OutcomeSessionReleased,
				AppliedIndex: state.Applied},
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
				RequestDigest: replicatedRequestDigest(command),
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
	if _, err := NewReplicatedExecutor(
		deadlineReplicatedClient{}, AbsoluteMaxReplicatedAttempts+1, time.Second,
	); !errors.Is(err, ErrReplicatedRoute) {
		t.Fatalf("over-max attempts = %v", err)
	}
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

func TestReplicatedExecutorRetriesSameCommandFenceIncarnationRace(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	state := states["m2"]
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
	refreshed := state
	refreshed.Fence.Term++
	completion := testReplicatedCompletionResponse(t, command, refreshed)
	client := &sequenceReplicatedClient{state: refreshed, responses: []*shardservice.ReplicatedResponse{
		{Kind: shardservice.ReplicatedRefusal,
			Refusal: shardservice.ReplicatedRefusalStaleFence, HasState: true, State: refreshed},
		completion,
	}}
	executor, err := NewReplicatedExecutor(client, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Propose(context.Background(), route, command)
	if err != nil || result.Retries != 1 || client.proposals != 2 ||
		!bytes.Equal(result.Completion, completion.Completion) {
		t.Fatalf("result=%+v proposals=%d err=%v", result, client.proposals, err)
	}
}

func TestReplicatedExecutorStopsServingFenceBackoffOnCancellation(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	state := states["m2"]
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
	client := &sequenceReplicatedClient{state: state, responses: []*shardservice.ReplicatedResponse{{
		Kind:    shardservice.ReplicatedRefusal,
		Refusal: shardservice.ReplicatedRefusalStaleFence, HasState: true, State: state,
	}}}
	executor, err := NewReplicatedExecutor(client, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = executor.Propose(ctx, route, command)
	if !errors.Is(err, context.Canceled) || client.proposals != 1 {
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
		RequestDigest: replicatedRequestDigest(commandBytes),
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
		Distribution: "orders", Shard: "0000-ffff",
		Group: group, AllocationGeneration: 5,
		Command: raftservice.CommandFence{
			ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
			OwnershipEpoch: 1, SchemaGeneration: 1,
			RelationManifestDigest: [32]byte{1},
			RoutingVersion:         1, RouteGeneration: 1,
		},
		Replicas: []ReplicatedEndpoint{
			{Member: 1, Node: [16]byte{1}, StoreID: [16]byte{1}, NodeIncarnation: 11, NativeEndpoint: "n1", Address: "m1"},
			{Member: 2, Node: [16]byte{2}, StoreID: [16]byte{2}, NodeIncarnation: 12, NativeEndpoint: "n2", Address: "m2"},
			{Member: 3, Node: [16]byte{3}, StoreID: [16]byte{3}, NodeIncarnation: 13, NativeEndpoint: "n3", Address: "m3"},
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
