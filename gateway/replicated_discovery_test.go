package gateway

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestAuthenticatedReplicatedColdDiscoveryCancelsStalledProbe(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	client, _ := testCapacityClient(t, 8, 1)
	var servers sync.WaitGroup
	client.dial = func(_ context.Context, address string) (net.Conn, error) {
		local, server := net.Pipe()
		servers.Add(1)
		go func() {
			defer servers.Done()
			defer server.Close()
			if address == "m1" {
				_, _ = io.Copy(io.Discard, server) // A stopped voter never replies.
				return
			}
			for {
				if _, err := shardservice.DecodeReplicatedRequest(server); err != nil {
					return
				}
				if err := shardservice.EncodeReplicatedResponse(server, &shardservice.ReplicatedResponse{
					Kind: shardservice.ReplicatedHandshake, HasState: true, State: states[address],
				}); err != nil {
					return
				}
			}
		}()
		return local, nil
	}
	defer func() { _ = client.Close(); servers.Wait() }()
	ctx, err := serviceauthz.WithAuthority(t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 5})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	executor, err := NewReplicatedExecutor(client, 1, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, _, err := executor.discoverLeaderFresh(ctx, route, 1, serviceauthz.CapabilityDataWrite)
	if err != nil || endpoint.Member != 2 || context.Cause(ctx) != nil {
		t.Fatalf("authenticated cold discovery leader=%d err=%v", endpoint.Member, err)
	}
	if stats := client.Stats(); stats.Connections != stats.Idle || stats.Handshakes != 0 || stats.Waiters != 0 || stats.Poisoned == 0 {
		t.Fatalf("losing probe did not release and poison its stalled stream: %+v", stats)
	}
}

type stalledFirstDiscoveryClient struct {
	states  map[string]shardservice.ReplicatedMemberState
	active  atomic.Int64
	started atomic.Int64
}

func (*stalledFirstDiscoveryClient) parallelReplicatedDiscovery() {}

func (client *stalledFirstDiscoveryClient) DoReplicated(ctx context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	client.active.Add(1)
	defer client.active.Add(-1)
	client.started.Add(1)
	if endpoint.Member == 1 {
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake, HasState: true, State: client.states[endpoint.Address]}, nil
}

func TestReplicatedColdDiscoveryDoesNotWaitForStalledFirstVoter(t *testing.T) {
	for _, catalog := range []bool{false, true} {
		t.Run(map[bool]string{false: "data", true: "catalog"}[catalog], func(t *testing.T) {
			route, _, states := testReplicatedRouteCommand(t)
			client := &stalledFirstDiscoveryClient{states: states}
			executor, err := NewReplicatedExecutor(client, 1, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
			defer cancel()
			if catalog {
				route.Distribution, route.Shard = ReplicatedCatalogDistribution, ReplicatedCatalogShard
				_, err = executor.catalogOperationalRoute(ctx, route, nil)
			} else {
				var endpoint ReplicatedEndpoint
				endpoint, _, err = executor.discoverLeaderFresh(ctx, route, 1, serviceauthz.CapabilityDataWrite)
				if err == nil && endpoint.Member != 2 {
					t.Fatalf("leader=%d", endpoint.Member)
				}
			}
			if err != nil || context.Cause(ctx) != nil {
				t.Fatalf("cold discovery exhausted request deadline behind one stopped voter: %v", err)
			}
			if client.active.Load() != 0 || client.started.Load() > 3 {
				t.Fatalf("discovery leaked/exceeded one bounded RF3 sweep: active=%d started=%d", client.active.Load(), client.started.Load())
			}
		})
	}
}
