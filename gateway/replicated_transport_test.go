package gateway

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardservice"
)

type testAuthenticatedConnection struct {
	net.Conn
	identity rafttransport.PeerIdentity
}

func TestAuthenticatedReplicatedClientRejectsMissingTLSCapability(t *testing.T) {
	if _, err := NewAuthenticatedReplicatedClient(AuthenticatedReplicatedClientOptions{
		Dial:              func(context.Context, string) (net.Conn, error) { return nil, nil },
		HandshakeDeadline: func() time.Time { return time.Now().Add(time.Second) },
		MaxConnections:    1, MaxPerEndpoint: 1, MaxIdlePerEndpoint: 1,
		MaxWaiters: 1, MaxIdleAge: time.Second, MaxLifetime: time.Minute,
	}); !errors.Is(err, ErrReplicatedTLSProfile) {
		t.Fatalf("missing TLS error = %v", err)
	}
}

func (connection *testAuthenticatedConnection) PeerIdentity() rafttransport.PeerIdentity {
	return connection.identity
}
func (*testAuthenticatedConnection) TrafficClass() rafttransport.TrafficClass {
	return rafttransport.TrafficShardNative
}

func testPooledClient(t *testing.T, endpoint ReplicatedEndpoint, serve func(net.Conn)) (*AuthenticatedReplicatedClient, *atomic.Uint64) {
	t.Helper()
	var dials atomic.Uint64
	client := &AuthenticatedReplicatedClient{
		dial: func(context.Context, string) (net.Conn, error) {
			dials.Add(1)
			client, server := net.Pipe()
			go serve(server)
			return client, nil
		},
		authenticate: func(_ context.Context, raw net.Conn, node rafttransport.NodeID, class rafttransport.TrafficClass, _ rafttransport.DeadlineFunc) (rafttransport.PeerConnection, error) {
			if node != endpoint.Node || class != rafttransport.TrafficShardNative {
				return nil, errors.New("wrong capability")
			}
			return &testAuthenticatedConnection{Conn: raw, identity: rafttransport.PeerIdentity{Node: endpoint.Node}}, nil
		},
		handshakeDeadline: func() time.Time { return time.Now().Add(time.Second) },
		maxConnections:    1, maxPerEndpoint: 1, maxIdlePerEndpoint: 1, maxWaiters: 1,
		maxIdleAge: time.Minute, maxLifetime: time.Hour, generation: 1,
		perEndpoint: make(map[rafttransport.NodeID]int), idle: make(map[rafttransport.NodeID][]*pooledReplicatedConn), wake: make(chan struct{}),
	}
	return client, &dials
}

func TestAuthenticatedReplicatedClientReusesExclusiveStreamAndPoisonsIdentityMismatch(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	endpoint := route.Replicas[0]
	state := states[endpoint.Address]
	var responses atomic.Uint64
	client, rawDials := testPooledClient(t, endpoint, func(server net.Conn) {
		defer server.Close()
		for {
			if _, err := shardservice.DecodeReplicatedRequest(server); err != nil {
				return
			}
			current := state
			if responses.Add(1) == 3 {
				current.Fence.NodeIncarnation++
			}
			if err := shardservice.EncodeReplicatedResponse(server, &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake, HasState: true, State: current}); err != nil {
				return
			}
		}
	})
	request := &shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedProbe, Fence: shardservice.ReplicatedFence{Group: route.Group, AllocationGeneration: route.AllocationGeneration}}
	for attempt := 0; attempt < 2; attempt++ {
		requestCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		if _, err := client.DoReplicated(requestCtx, endpoint, request); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
	}
	// A canceled AfterFunc from either completed request must not install a
	// stale deadline after the connection was returned to the pool.
	time.Sleep(25 * time.Millisecond)
	if rawDials.Load() != 1 || client.Stats().Reuses != 1 {
		t.Fatalf("dials=%d stats=%+v", rawDials.Load(), client.Stats())
	}
	if _, err := client.DoReplicated(context.Background(), endpoint, request); !errors.Is(err, ErrReplicatedRoute) {
		t.Fatalf("identity mismatch error = %v", err)
	}
	stats := client.Stats()
	if stats.Connections != 0 || stats.Idle != 0 || stats.Poisoned != 1 {
		t.Fatalf("poison stats = %+v", stats)
	}
}

func TestAuthenticatedReplicatedClientHardWaiterBound(t *testing.T) {
	route, _, _ := testReplicatedRouteCommand(t)
	endpoint := route.Replicas[0]
	block := make(chan struct{})
	client, _ := testPooledClient(t, endpoint, func(server net.Conn) { <-block; _ = server.Close() })
	first, err := client.acquire(context.Background(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithCancel(context.Background())
	waiting := make(chan error, 1)
	go func() { _, err := client.acquire(waitCtx, endpoint); waiting <- err }()
	for client.Stats().Waiters != 1 {
		time.Sleep(time.Millisecond)
	}
	if _, err := client.acquire(context.Background(), endpoint); !errors.Is(err, ErrReplicatedTransportBound) {
		t.Fatalf("bound error = %v", err)
	}
	cancel()
	if err := <-waiting; !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v", err)
	}
	client.release(first, false)
	close(block)
}

func TestAuthenticatedReplicatedClientCloseWakesWaiterAndRefusesReuse(t *testing.T) {
	route, _, _ := testReplicatedRouteCommand(t)
	endpoint := route.Replicas[0]
	block := make(chan struct{})
	client, _ := testPooledClient(t, endpoint, func(server net.Conn) { <-block; _ = server.Close() })
	checkedOut, err := client.acquire(context.Background(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	waiting := make(chan error, 1)
	go func() { _, err := client.acquire(context.Background(), endpoint); waiting <- err }()
	for client.Stats().Waiters != 1 {
		time.Sleep(time.Millisecond)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-waiting; !errors.Is(err, ErrReplicatedTLSProfile) {
		t.Fatalf("waiter close error = %v", err)
	}
	client.release(checkedOut, true)
	close(block)
	if stats := client.Stats(); stats.Connections != 0 || stats.Idle != 0 || stats.Waiters != 0 {
		t.Fatalf("closed stats = %+v", stats)
	}
}

func BenchmarkAuthenticatedReplicatedPoolAcquireRelease(b *testing.B) {
	endpoint := ReplicatedEndpoint{Member: 1, Node: [16]byte{1}, StoreID: [16]byte{2}, NodeIncarnation: 3, Address: "fixed"}
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	connection := &pooledReplicatedConn{conn: &testAuthenticatedConnection{Conn: left}, endpoint: endpoint, created: time.Now(), lastUsed: time.Now(), generation: 1}
	client := &AuthenticatedReplicatedClient{maxConnections: 1, maxPerEndpoint: 1, maxIdlePerEndpoint: 1, maxIdleAge: time.Hour, maxLifetime: time.Hour, generation: 1, total: 1, perEndpoint: map[rafttransport.NodeID]int{endpoint.Node: 1}, idle: map[rafttransport.NodeID][]*pooledReplicatedConn{endpoint.Node: {connection}}, wake: make(chan struct{})}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		acquired, _ := client.acquire(context.Background(), endpoint)
		client.release(acquired, true)
	}
}
