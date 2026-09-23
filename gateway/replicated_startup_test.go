package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
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
	leaderlessUntil  time.Time
}

type unavailableProposalClient struct {
	*startupElectionClient
	readyAt   time.Time
	withState bool
	refused   [][]byte
}

type ownershipDiscoveryClient struct {
	*startupElectionClient
	proposed int
}

func (client *ownershipDiscoveryClient) ProbeReplicated(ctx context.Context, route ReplicatedRoute, endpoint ReplicatedEndpoint,
	capability serviceauthz.Capability,
) (*shardservice.ReplicatedResponse, error) {
	response, err := client.startupElectionClient.DoReplicated(ctx, endpoint, &shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedProbe})
	if err == nil {
		_, err = bindReplicatedObservation(route, endpoint, response)
	}
	return response, err
}

func (client *ownershipDiscoveryClient) DoReplicated(ctx context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	client.proposed++
	state := client.states[endpoint.Address]
	for address, state := range client.states {
		state.Fence.Command.OwnershipEpoch++
		client.states[address] = state
	}
	return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedOutcomeUnknown, HasState: true, State: state}, nil
}

func TestReplicatedOwnershipDiscoveryPreservesAdmissionKnowledge(t *testing.T) {
	for _, priorProposal := range []bool{false, true} {
		t.Run(fmt.Sprint(priorProposal), func(t *testing.T) {
			route, command, states := testReplicatedRouteCommand(t)
			if !priorProposal {
				for address, state := range states {
					state.Fence.Command.OwnershipEpoch++
					states[address] = state
				}
			}
			client := &ownershipDiscoveryClient{startupElectionClient: &startupElectionClient{states: states}}
			executor, err := NewReplicatedExecutor(client, 2, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_, err = executor.Propose(t.Context(), route, command)
			if !errors.Is(err, raftservice.ErrServingFence) || errors.Is(err, errReplicatedNotAdmitted) == priorProposal ||
				errors.Is(err, raftservice.ErrOutcomeUnknown) != priorProposal {
				t.Fatalf("prior=%t proposed=%d err=%v", priorProposal, client.proposed, err)
			}
			if priorProposal {
				var unknown *raftservice.UnknownOutcomeError
				if client.proposed != 1 || !errors.As(err, &unknown) || !bytes.Equal(unknown.Command, command) {
					t.Fatal("ambiguous recipe changed")
				}
			} else if client.proposed != 0 {
				t.Fatal("stale route reached proposal")
			}
		})
	}
}

func (client *unavailableProposalClient) DoReplicated(ctx context.Context, endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request.Operation == shardservice.ReplicatedPropose && time.Now().Before(client.readyAt) {
		client.refused = append(client.refused, bytes.Clone(request.Command))
		response := &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal,
			Refusal: shardservice.ReplicatedRefusalUnavailable}
		if client.withState {
			response.HasState, response.State = true, client.states[endpoint.Address]
		}
		return response, nil
	}
	return client.startupElectionClient.DoReplicated(ctx, endpoint, request)
}

func TestReplicatedExecutorWaitsForDefiniteUnavailableWithoutChangingCommand(t *testing.T) {
	for _, withState := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			route, command, states := testReplicatedRouteCommand(t)
			client := &unavailableProposalClient{startupElectionClient: &startupElectionClient{states: states},
				readyAt: time.Now().Add(1500 * time.Millisecond), withState: withState}
			executor, err := NewReplicatedExecutor(client, 1, 3*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			result, err := executor.Propose(t.Context(), route, command)
			if err != nil || len(client.commands) != 1 || len(client.refused) < 2 || result.Retries != len(client.refused) {
				t.Fatalf("state=%t result=%+v admitted=%d refused=%d err=%v", withState, result, len(client.commands), len(client.refused), err)
			}
			for _, sent := range append(client.refused, client.commands...) {
				if !bytes.Equal(sent, command) {
					t.Fatal("readiness retry changed immutable command")
				}
			}
		})
	}
}

func TestReplicatedExecutorUnavailableDeadlineIsDefinite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		route, command, states := testReplicatedRouteCommand(t)
		client := &unavailableProposalClient{startupElectionClient: &startupElectionClient{states: states},
			readyAt: time.Now().Add(time.Minute), withState: true}
		executor, err := NewReplicatedExecutor(client, 1, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		_, err = executor.Propose(t.Context(), route, command)
		if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, raftservice.ErrOutcomeUnknown) ||
			time.Since(started) != time.Second || len(client.commands) != 0 {
			t.Fatalf("elapsed=%s admitted=%d err=%v", time.Since(started), len(client.commands), err)
		}
	})
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
		if client.probes <= client.leaderlessSweeps*len(client.states) || time.Now().Before(client.leaderlessUntil) {
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

func TestReplicatedExecutorElectionWaitDoesNotSpendProposalAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		route, command, states := testReplicatedRouteCommand(t)
		client := &startupElectionClient{states: states, leaderlessUntil: time.Now().Add(1500 * time.Millisecond)}
		executor, err := NewReplicatedExecutor(client, 1, 3*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		result, err := executor.Propose(t.Context(), route, command)
		if err != nil || result.Retries != 0 || len(client.commands) != 1 || !bytes.Equal(client.commands[0], command) {
			t.Fatalf("election consumed proposal allowance: result=%+v proposals=%d err=%v", result, len(client.commands), err)
		}
	})
}

func TestReplicatedExecutorRetriesInitialLeaderlessDiscovery(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	client := &startupElectionClient{states: states, leaderlessSweeps: 2}
	executor, err := NewReplicatedExecutor(client, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Propose(t.Context(), route, command)
	if err != nil || result.Retries != 0 || client.probes != 8 || len(client.commands) != 1 ||
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
	ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
	defer cancel()
	_, err = executor.Propose(ctx, route, command)
	if !errors.Is(err, ErrReplicatedLeader) || errors.Is(err, raftservice.ErrOutcomeUnknown) ||
		!errors.Is(err, context.DeadlineExceeded) || client.probes < 3 || len(client.commands) != 0 ||
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
