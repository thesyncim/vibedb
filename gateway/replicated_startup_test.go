package gateway

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type startupElectionClient struct {
	states           map[string]shardservice.ReplicatedMemberState
	leaderlessSweeps int
	probes           int
	commands         [][]byte
	capabilities     []serviceauthz.Capability
	probeError       error
	unauthorized     bool
	wrongFence       bool
	afterFirstSweep  func()
}

func (client *startupElectionClient) DoReplicated(_ context.Context, endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	state := client.states[endpoint.Address]
	if request.Operation == shardservice.ReplicatedProbe {
		client.probes++
		if client.probeError != nil {
			return nil, client.probeError
		}
		if client.unauthorized {
			return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal,
				Refusal: shardservice.ReplicatedRefusalUnauthorized}, nil
		}
		if client.probes <= client.leaderlessSweeps*len(client.states) {
			state.LeaderID = 0
		}
		if client.wrongFence {
			state.Fence.Command.SchemaGeneration++
		}
		if client.probes == len(client.states) && client.afterFirstSweep != nil {
			client.afterFirstSweep()
		}
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake,
			HasState: true, State: state}, nil
	}
	client.commands = append(client.commands, bytes.Clone(request.Command))
	client.capabilities = append(client.capabilities, request.Capability)
	command, err := replication.OpenCommand(request.Command)
	if err != nil {
		return nil, err
	}
	epoch := command.ClientEpoch
	resultCode := uint32(replicatedstate.ResultApplied)
	if command.Kind() == replication.CommandSessionOpen {
		epoch = state.Applied
		resultCode = replicatedstate.ResultSessionOpened
	}
	completion, err := appendNativeSessionCompletion(nil, command, epoch,
		state.Applied, resultCode)
	if err != nil {
		return nil, err
	}
	return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedCompletion,
		HasState: true, State: state, RequestDigest: replicatedRequestDigest(request.Command),
		Outcome: raftserve.Outcome{Code: raftserve.OutcomeCompletion, AppliedIndex: state.Applied,
			CompletionAppliedSequence: state.Applied, CompletionBytes: len(completion)},
		Completion: completion}, nil
}

func TestReplicatedExecutorRetriesInitialLeaderlessDiscovery(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	client := &startupElectionClient{states: states, leaderlessSweeps: 2}
	executor, err := NewReplicatedExecutor(client, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Propose(t.Context(), route, command)
	if err != nil || result.Retries != 2 || client.probes != 8 || len(client.commands) != 1 ||
		!bytes.Equal(client.commands[0], command) {
		t.Fatalf("initial election: result=%+v probes=%d proposals=%d error=%v",
			result, client.probes, len(client.commands), err)
	}
}

func TestNativeTopologySessionOpenWaitsForInitialElection(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	client := &startupElectionClient{states: states, leaderlessSweeps: 1}
	executor, err := NewReplicatedExecutor(client, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewNativeSession(NativeSessionOptions{Executor: executor, Route: route,
		Distribution: string(route.Distribution), Shard: string(route.Shard), Tenant: []byte{1},
		ClientID: replication.ID128{1}, RetryHome: replication.RetryHome{1},
		Resolver: BaseRelationResolver{Relation: 1}, ProposalCapability: serviceauthz.CapabilityTopology})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = session.Open(t.Context(), 1000); err != nil || !session.Status().Active ||
		session.Status().Pending || client.probes != 5 || len(client.commands) != 1 ||
		client.capabilities[0] != serviceauthz.CapabilityTopology {
		t.Fatalf("catalog session election: status=%+v probes=%d proposals=%d error=%v",
			session.Status(), client.probes, len(client.commands), err)
	}
	command, err := replication.OpenCommand(client.commands[0])
	if err != nil || command.Kind() != replication.CommandSessionOpen ||
		command.AuthorityClass != replication.CommandAuthorityTopology || command.ClientSequence != 1 {
		t.Fatalf("startup changed command identity or authority: %v", err)
	}
}

func TestReplicatedExecutorInitialElectionExhaustionIsBoundedAndDiagnosable(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	client := &startupElectionClient{states: states, leaderlessSweeps: 99}
	executor, err := NewReplicatedExecutor(client, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Propose(t.Context(), route, command)
	if !errors.Is(err, ErrReplicatedLeader) || errors.Is(err, raftservice.ErrOutcomeUnknown) ||
		client.probes != 6 || len(client.commands) != 0 ||
		!strings.Contains(err.Error(), "no authenticated replica reported itself as leader") {
		t.Fatalf("bounded election: probes=%d proposals=%d error=%v", client.probes, len(client.commands), err)
	}
}

func TestReplicatedExecutorInitialElectionHonorsCancellation(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client := &startupElectionClient{states: states, leaderlessSweeps: 99, afterFirstSweep: cancel}
	executor, err := NewReplicatedExecutor(client, 8, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Propose(ctx, route, command)
	if !errors.Is(err, context.Canceled) || errors.Is(err, raftservice.ErrOutcomeUnknown) ||
		client.probes != 3 || len(client.commands) != 0 {
		t.Fatalf("canceled election: probes=%d proposals=%d error=%v", client.probes, len(client.commands), err)
	}
}

func TestReplicatedExecutorInitialDiscoveryDoesNotRetryTerminalFailures(t *testing.T) {
	transportError := errors.New("test transport failure")
	for _, test := range []struct {
		name   string
		change func(*startupElectionClient)
		want   error
		probes int
	}{
		{"unauthorized", func(c *startupElectionClient) { c.unauthorized = true }, ErrReplicatedUnauthorized, 1},
		{"command fence", func(c *startupElectionClient) { c.wrongFence = true }, ErrReplicatedRoute, 3},
		{"transport", func(c *startupElectionClient) { c.probeError = transportError }, transportError, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			route, command, states := testReplicatedRouteCommand(t)
			client := &startupElectionClient{states: states, leaderlessSweeps: 99}
			test.change(client)
			executor, err := NewReplicatedExecutor(client, 8, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_, err = executor.Propose(t.Context(), route, command)
			if !errors.Is(err, test.want) || errors.Is(err, raftservice.ErrOutcomeUnknown) ||
				client.probes != test.probes || len(client.commands) != 0 {
				t.Fatalf("terminal discovery: probes=%d proposals=%d error=%v", client.probes, len(client.commands), err)
			}
		})
	}
}
