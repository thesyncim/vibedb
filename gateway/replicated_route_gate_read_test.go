package gateway

import (
	"context"
	"errors"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

type routeGateReadClient struct {
	states    map[string]shardservice.ReplicatedMemberState
	status    routegate.Status
	response  func(*shardservice.ReplicatedResponse)
	calls     int
	authority serviceauthz.Authority
}

type stalledRouteGateClient struct{ *routeGateReadClient }

type electingRouteGateClient struct {
	*routeGateReadClient
	electionAt time.Time
	probes     atomic.Int64
}

func (*electingRouteGateClient) parallelReplicatedDiscovery() {}

func (client *electingRouteGateClient) DoReplicated(ctx context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	if endpoint.Member == 1 {
		return nil, io.EOF
	}
	if request.Operation == shardservice.ReplicatedProbe {
		client.probes.Add(1)
		state := client.states[endpoint.Address]
		if time.Now().Before(client.electionAt) {
			state.LeaderID = 0
		}
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake, HasState: true, State: state}, nil
	}
	return client.routeGateReadClient.DoReplicated(ctx, endpoint, request)
}

func TestReplicatedRouteGateWaitsForReplacementElection(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	for address, state := range states {
		state.Applied, state.Commit = 11, 11
		states[address] = state
	}
	client := &electingRouteGateClient{routeGateReadClient: &routeGateReadClient{states: states, status: routegate.Status{Epoch: 29}}, electionAt: time.Now().Add(80 * time.Millisecond)}
	executor, err := NewReplicatedExecutor(client, 5, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.ReadRouteGate(t.Context(), route, 7)
	if err != nil || result.Status.Epoch != 29 || client.calls != 1 || result.Retries == 0 || client.probes.Load() > 10 {
		t.Fatalf("gate read burned retries before election: result=%+v probes=%d calls=%d err=%v", result, client.probes.Load(), client.calls, err)
	}
}

func (*stalledRouteGateClient) parallelReplicatedDiscovery() {}

func (client *stalledRouteGateClient) DoReplicated(ctx context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	if endpoint.Member == 1 {
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}
	return client.routeGateReadClient.DoReplicated(ctx, endpoint, request)
}

func TestReplicatedRouteGateRefreshesWarmHintBeforeAcquiringWave(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	for address, state := range states {
		state.Applied, state.Commit = 11, 11
		states[address] = state
	}
	client := &stalledRouteGateClient{&routeGateReadClient{states: states, status: routegate.Status{Epoch: 29}}}
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	old := states["m1"]
	old.LeaderID = 1
	executor.leaderHints.publish(route, route.Replicas[0], old)
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	result, err := executor.ReadRouteGate(ctx, route, 7)
	if err != nil || result.State.Fence.MemberID != 2 || context.Cause(ctx) != nil {
		t.Fatalf("stale route-gate hint consumed wave deadline: result=%+v err=%v", result, err)
	}
}

func (c *routeGateReadClient) DoReplicated(_ context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	state := c.states[endpoint.Address]
	if request.Capability != serviceauthz.CapabilityDataWrite || request.Authority != c.authority {
		return nil, errors.New("wrong authority")
	}
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake, HasState: true, State: state}, nil
	}
	if request.Operation != shardservice.ReplicatedRouteGateRead || endpoint.Member != state.LeaderID || request.Fence != state.Fence || request.MinimumApplied != 7 {
		return nil, errors.New("not exact leader status request")
	}
	c.calls++
	raw, err := shardservice.AppendReplicatedRouteGateReadValue(nil, c.status)
	if err != nil {
		return nil, err
	}
	response := &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRouteGateReadResult, HasState: true, State: state, ReadApplied: 9, Value: raw}
	if c.response != nil {
		c.response(response)
	}
	return response, nil
}
func TestReplicatedReadRouteGateUsesActualEpochAndRejectsForeignResults(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	for address, state := range states {
		state.Applied = 11
		state.Commit = 11
		state.CheckpointApplied = 10
		states[address] = state
	}
	authority := serviceauthz.Authority{Node: [16]byte{9}, Generation: 1}
	ctx, err := serviceauthz.WithAuthority(t.Context(), authority)
	if err != nil {
		t.Fatal(err)
	}
	status := routegate.Status{Epoch: 29, Revision: 7, ActivePins: 2, ReleasedPins: 3, RetainedRecords: 5}
	client := &routeGateReadClient{states: states, status: status, authority: authority}
	executor, err := NewReplicatedExecutor(client, 3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.ReadRouteGate(ctx, route, 7)
	if err != nil || result.Status != status || result.Applied != 9 || result.State.Fence.MemberID != result.State.LeaderID || client.calls != 1 {
		t.Fatalf("result %+v calls %d %v", result, client.calls, err)
	}
	for _, mutate := range []func(*shardservice.ReplicatedResponse){
		func(r *shardservice.ReplicatedResponse) { r.ReadApplied = 6 },
		func(r *shardservice.ReplicatedResponse) { r.ReadApplied = 12 },
		func(r *shardservice.ReplicatedResponse) { r.State.Fence.Group.GroupID[0] ^= 1 },
		func(r *shardservice.ReplicatedResponse) { r.State.Fence.Command.SchemaGeneration++ },
		func(r *shardservice.ReplicatedResponse) { r.State.Fence.StoreID[0] ^= 1 },
		func(r *shardservice.ReplicatedResponse) { r.Value[0] ^= 1 },
		func(r *shardservice.ReplicatedResponse) { r.Completion = []byte{1} },
		func(r *shardservice.ReplicatedResponse) {
			r.Outcome = raftserve.Outcome{Code: raftserve.OutcomeSessionReleased}
		},
		func(r *shardservice.ReplicatedResponse) { r.Refusal = shardservice.ReplicatedRefusalReadBehind },
		func(r *shardservice.ReplicatedResponse) { r.Kind = shardservice.ReplicatedReadFound },
	} {
		client.response = mutate
		client.calls = 0
		executor, _ = NewReplicatedExecutor(client, 2, time.Second)
		if result, err := executor.ReadRouteGate(ctx, route, 7); err == nil || result != (ReplicatedRouteGateReadResult{}) || client.calls > 2 {
			t.Fatalf("foreign result %+v calls %d err %v", result, client.calls, err)
		}
	}
}
