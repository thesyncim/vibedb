package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type catalogDisconnectedSweepClient struct {
	states   map[string]shardservice.ReplicatedMemberState
	probes   [ServingReplicaCount]atomic.Int64
	parallel bool
	failure  error
	always   bool
}

// catalogTypedRefusalSweepClient models the physical catalog receiver while
// its canonical service gate is being installed.  An unavailable response is
// deliberately state-less and retryable; an unauthorized response is also
// state-less but terminal.  Keeping these as wire refusals catches accidental
// conversion of a missing gate into a transport error (or, worse, a trusted
// static handshake).
type catalogTypedRefusalSweepClient struct {
	states   map[string]shardservice.ReplicatedMemberState
	probes   [ServingReplicaCount]atomic.Int64
	parallel bool
	refusal  shardservice.ReplicatedRefusalCode
}

func (client *catalogTypedRefusalSweepClient) parallelReplicatedDiscoveryEnabled() bool {
	return client.parallel
}

func (client *catalogTypedRefusalSweepClient) DoReplicated(_ context.Context, endpoint ReplicatedEndpoint,
	_ *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	attempt := client.probes[endpoint.Member-1].Add(1)
	if client.refusal == shardservice.ReplicatedRefusalUnavailable && attempt == 1 {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal,
			Refusal: shardservice.ReplicatedRefusalUnavailable}, nil
	}
	if client.refusal == shardservice.ReplicatedRefusalUnauthorized {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal,
			Refusal: shardservice.ReplicatedRefusalUnauthorized}, nil
	}
	return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake, HasState: true,
		State: client.states[endpoint.Address]}, nil
}

func (client *catalogDisconnectedSweepClient) parallelReplicatedDiscoveryEnabled() bool {
	return client.parallel
}

func (client *catalogDisconnectedSweepClient) DoReplicated(_ context.Context, endpoint ReplicatedEndpoint,
	_ *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if client.probes[endpoint.Member-1].Add(1) == 1 || client.always {
		return nil, client.failure
	}
	return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake, HasState: true,
		State: client.states[endpoint.Address]}, nil
}

func TestCatalogDiscoveryRetriesDisconnectedSweep(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		name := map[bool]string{false: "serial", true: "parallel"}[parallel]
		t.Run(name, func(t *testing.T) {
			route, _, states := testReplicatedRouteCommand(t)
			route.Distribution, route.Shard = ReplicatedCatalogDistribution, ReplicatedCatalogShard
			client := &catalogDisconnectedSweepClient{states: states, parallel: parallel, failure: io.EOF}
			executor, err := NewReplicatedExecutor(client, 2, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			observed, err := executor.catalogOperationalRoute(t.Context(), route, nil)
			if err != nil || observed.Command != route.Command || client.probes[1].Load() != 2 {
				t.Fatalf("catalog did not recover from all three expired streams: route=%+v err=%v", observed, err)
			}
			for index := range client.probes {
				client.probes[index].Store(0)
			}
			client.always = true
			if _, err = executor.catalogOperationalRoute(t.Context(), route, nil); !errors.Is(err, io.EOF) || !errors.Is(err, ErrReplicatedLeader) {
				t.Fatalf("disconnected catalog lost its transport cause: %v", err)
			}
			for index := range client.probes {
				if probes := client.probes[index].Load(); probes != 2 {
					t.Fatalf("member %d probes=%d, want bounded two sweeps", index+1, probes)
				}
			}
		})
	}
}

func TestCatalogDiscoveryRejectsTerminalFailureBesideDisconnect(t *testing.T) {
	for name, failure := range map[string]error{
		"identity":      ErrReplicatedRoute,
		"authorization": ErrReplicatedUnauthorized,
		"canceled":      context.Canceled,
	} {
		t.Run(name, func(t *testing.T) {
			route, _, states := testReplicatedRouteCommand(t)
			route.Distribution, route.Shard = ReplicatedCatalogDistribution, ReplicatedCatalogShard
			client := &catalogDisconnectedSweepClient{states: states, failure: errors.Join(io.EOF, failure)}
			executor, err := NewReplicatedExecutor(client, 2, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = executor.catalogOperationalRoute(t.Context(), route, nil); !errors.Is(err, failure) {
				t.Fatalf("catalog retried a terminal failure: %v", err)
			}
			for index := range client.probes {
				if probes := client.probes[index].Load(); probes != 1 {
					t.Fatalf("member %d probes=%d, want one terminal sweep", index+1, probes)
				}
			}
		})
	}
}

func TestCatalogDiscoveryHandlesTypedGateRefusals(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		name := map[bool]string{false: "serial", true: "hedged"}[parallel]
		t.Run(name, func(t *testing.T) {
			route, _, states := testReplicatedRouteCommand(t)
			route.Distribution, route.Shard = ReplicatedCatalogDistribution, ReplicatedCatalogShard
			client := &catalogTypedRefusalSweepClient{
				states: states, parallel: parallel, refusal: shardservice.ReplicatedRefusalUnavailable,
			}
			executor, err := NewReplicatedExecutor(client, 2, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			observed, err := executor.catalogOperationalRoute(t.Context(), route, nil)
			if err != nil || observed.Command != route.Command {
				t.Fatalf("typed unavailable refusal did not recover: route=%+v err=%v", observed, err)
			}
			var retried bool
			for index := range client.probes {
				if probes := client.probes[index].Load(); probes > 1 {
					retried = true
				}
			}
			if !retried {
				t.Fatalf("typed unavailable refusal did not trigger a bounded second sweep: probes=%v", client.probes)
			}

			client = &catalogTypedRefusalSweepClient{
				states: states, parallel: parallel, refusal: shardservice.ReplicatedRefusalUnauthorized,
			}
			executor, err = NewReplicatedExecutor(client, 2, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = executor.catalogOperationalRoute(t.Context(), route, nil); err == nil {
				t.Fatal("typed unauthorized refusal unexpectedly recovered")
			} else {
				var refusal *ReplicatedRefusalError
				if !errors.As(err, &refusal) || refusal.Code != shardservice.ReplicatedRefusalUnauthorized {
					t.Fatalf("typed unauthorized refusal=%v, want refusal code %d", err,
						shardservice.ReplicatedRefusalUnauthorized)
				}
			}
			for index := range client.probes {
				if probes := client.probes[index].Load(); probes > 1 {
					t.Fatalf("typed unauthorized refusal retried member %d %d times", index+1, probes)
				}
			}
		})
	}
}

func TestCatalogDiscoveryReopensExpiredPooledConnections(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	route.Distribution, route.Shard = ReplicatedCatalogDistribution, ReplicatedCatalogShard
	client, _ := testCapacityClient(t, 8, 1)
	var servers sync.WaitGroup
	var peersMu sync.Mutex
	var peers []net.Conn
	client.dial = func(_ context.Context, address string) (net.Conn, error) {
		local, peer := net.Pipe()
		peersMu.Lock()
		peers = append(peers, peer)
		peersMu.Unlock()
		servers.Add(1)
		go func() {
			defer servers.Done()
			defer peer.Close()
			for {
				if _, err := shardservice.DecodeReplicatedRequest(peer); err != nil {
					return
				}
				if err := shardservice.EncodeReplicatedResponse(peer, &shardservice.ReplicatedResponse{
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
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for _, endpoint := range route.Replicas {
		if _, err := client.probeCatalog(ctx, route, endpoint); err != nil {
			t.Fatal(err)
		}
	}
	// The server expires every retained idle stream while the observer is busy
	// elsewhere (for example, waiting for a live DDL operation to complete).
	peersMu.Lock()
	for _, peer := range peers {
		_ = peer.Close()
	}
	peersMu.Unlock()
	executor, err := NewReplicatedExecutor(client, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.catalogOperationalRoute(ctx, route, nil); err != nil {
		t.Fatalf("catalog did not reopen expired pooled connections: %v", err)
	}
	if stats := client.Stats(); stats.Poisoned < ServingReplicaCount || stats.Dials <= ServingReplicaCount ||
		stats.Connections != stats.Idle || stats.Handshakes != 0 || stats.Waiters != 0 {
		t.Fatalf("catalog recovery did not discard expired streams and release checkouts: %+v", stats)
	}
}
