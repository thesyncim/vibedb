package gateway

import (
	"context"
	"errors"
	"io"
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
		maxHandshakes: 1,
		maxIdleAge:    time.Minute, maxLifetime: time.Hour, generation: 1,
		perEndpoint: make(map[replicatedTransportEndpoint]int), idle: make(map[replicatedTransportEndpoint][]*pooledReplicatedConn), active: make(map[*pooledReplicatedConn]struct{}), wake: make(chan struct{}),
	}
	return client, &dials
}

func TestAuthenticatedReplicatedClientHardHandshakeBoundAndStats(t *testing.T) {
	route, _, _ := testReplicatedRouteCommand(t)
	entered := make(chan struct{}, 2)
	unblock := make(chan struct{})
	client := &AuthenticatedReplicatedClient{
		dial: func(context.Context, string) (net.Conn, error) {
			local, peer := net.Pipe()
			go func() { <-unblock; _ = peer.Close() }()
			return local, nil
		},
		authenticate: func(_ context.Context, raw net.Conn, node rafttransport.NodeID, _ rafttransport.TrafficClass, _ rafttransport.DeadlineFunc) (rafttransport.PeerConnection, error) {
			entered <- struct{}{}
			<-unblock
			return &testAuthenticatedConnection{Conn: raw,
				identity: rafttransport.PeerIdentity{Node: node}}, nil
		},
		handshakeDeadline: func() time.Time { return time.Now().Add(time.Second) },
		maxConnections:    2, maxPerEndpoint: 1, maxIdlePerEndpoint: 0,
		maxHandshakes: 1, maxWaiters: 1, maxIdleAge: time.Minute,
		maxLifetime: time.Hour, generation: 1,
		perEndpoint: make(map[replicatedTransportEndpoint]int),
		idle:        make(map[replicatedTransportEndpoint][]*pooledReplicatedConn),
		active:      make(map[*pooledReplicatedConn]struct{}), wake: make(chan struct{}),
	}
	results := make(chan *pooledReplicatedConn, 2)
	errors := make(chan error, 2)
	for _, endpoint := range route.Replicas[:2] {
		endpoint := endpoint
		go func() {
			connection, err := client.acquire(context.Background(), endpoint)
			results <- connection
			errors <- err
		}()
		if endpoint.Member == route.Replicas[0].Member {
			<-entered
		}
	}
	deadline := time.Now().Add(time.Second)
	for client.Stats().Waiters != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stats := client.Stats()
	if stats.Handshakes != 1 || stats.PeakHandshakes != 1 || stats.Waiters != 1 {
		t.Fatalf("bounded handshake stats = %+v", stats)
	}
	close(unblock)
	for range 2 {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
		client.release(<-results, false)
	}
	stats = client.Stats()
	if stats.Handshakes != 0 || stats.PeakHandshakes != 1 || stats.Connections != 0 {
		t.Fatalf("completed handshake stats = %+v", stats)
	}
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

func TestAuthenticatedReplicatedClientSharesPhysicalNodeAcrossShardFences(t *testing.T) {
	route, _, _ := testReplicatedRouteCommand(t)
	first := route.Replicas[0]
	second := first
	second.Member++
	second.StoreID[0]++
	second.NodeIncarnation++
	block := make(chan struct{})
	client, dials := testPooledClient(t, first, func(server net.Conn) { <-block; _ = server.Close() })
	connection, err := client.acquire(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	client.release(connection, true)
	reused, err := client.acquire(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if reused != connection || dials.Load() != 1 {
		t.Fatalf("connection=%p reused=%p dials=%d", connection, reused, dials.Load())
	}
	client.release(reused, false)
	close(block)
}

func TestAuthenticatedReplicatedClientCloseRejectsHandshakeCompletedAfterReturn(t *testing.T) {
	testBlockedAuthenticatedPublish(t, false)
}

func TestAuthenticatedReplicatedClientRotateRejectsOldHandshakeCompletedAfterReturn(t *testing.T) {
	testBlockedAuthenticatedPublish(t, true)
}

func TestAuthenticatedReplicatedClientRotateClosesCheckedOutOldGeneration(t *testing.T) {
	authority := newGatewayTLSAuthority(t)
	identity := gatewayPeerIdentity(13, 51)
	profile := authority.profile(t, identity)
	replacement := authority.profile(t, identity)
	peerDone := make(chan struct{})
	client, err := NewAuthenticatedReplicatedClient(AuthenticatedReplicatedClientOptions{
		TLS: profile,
		Dial: func(context.Context, string) (net.Conn, error) {
			local, peer := net.Pipe()
			go func() { defer close(peerDone); defer peer.Close(); _, _ = io.Copy(io.Discard, peer) }()
			return local, nil
		},
		HandshakeDeadline: func() time.Time { return time.Now().Add(time.Second) },
		MaxConnections:    1, MaxPerEndpoint: 1, MaxIdlePerEndpoint: 1,
		MaxWaiters: 1, MaxIdleAge: time.Minute, MaxLifetime: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	client.authenticate = func(_ context.Context, raw net.Conn, node rafttransport.NodeID, _ rafttransport.TrafficClass, _ rafttransport.DeadlineFunc) (rafttransport.PeerConnection, error) {
		return &testAuthenticatedConnection{Conn: raw, identity: rafttransport.PeerIdentity{Node: node}}, nil
	}
	endpoint := ReplicatedEndpoint{Member: 1, Node: identity.Node, StoreID: [16]byte{1}, NodeIncarnation: 1, Address: "active"}
	connection, err := client.acquire(context.Background(), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.RotateTLS(replacement); err != nil {
		t.Fatal(err)
	}
	select {
	case <-peerDone:
	case <-time.After(time.Second):
		t.Fatal("rotation returned before old active stream closed")
	}
	if _, err := connection.conn.Write([]byte{1}); err == nil {
		t.Fatal("old active stream remained writable after rotation")
	}
	client.release(connection, false)
}

func testBlockedAuthenticatedPublish(t *testing.T, rotate bool) {
	t.Helper()
	authority := newGatewayTLSAuthority(t)
	identity := gatewayPeerIdentity(11, 31)
	profile := authority.profile(t, identity)
	replacement := authority.profile(t, identity)
	peerDone := make(chan struct{})
	client, err := NewAuthenticatedReplicatedClient(AuthenticatedReplicatedClientOptions{
		TLS: profile,
		Dial: func(context.Context, string) (net.Conn, error) {
			local, peer := net.Pipe()
			go func() { defer close(peerDone); defer peer.Close(); var one [1]byte; _, _ = peer.Read(one[:]) }()
			return local, nil
		},
		HandshakeDeadline: func() time.Time { return time.Now().Add(time.Second) },
		MaxConnections:    1, MaxPerEndpoint: 1, MaxIdlePerEndpoint: 1,
		MaxWaiters: 1, MaxIdleAge: time.Minute, MaxLifetime: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	unblock := make(chan struct{})
	client.authenticate = func(_ context.Context, raw net.Conn, node rafttransport.NodeID, class rafttransport.TrafficClass, _ rafttransport.DeadlineFunc) (rafttransport.PeerConnection, error) {
		close(entered)
		<-unblock
		return &testAuthenticatedConnection{Conn: raw, identity: rafttransport.PeerIdentity{Node: node}}, nil
	}
	endpoint := ReplicatedEndpoint{Member: 1, Node: identity.Node, StoreID: [16]byte{1}, NodeIncarnation: 1, Address: "blocked"}
	result := make(chan error, 1)
	go func() { _, err := client.acquire(context.Background(), endpoint); result <- err }()
	<-entered
	if rotate {
		if err := client.RotateTLS(replacement); err != nil {
			t.Fatal(err)
		}
	} else if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	close(unblock)
	if err := <-result; !errors.Is(err, ErrReplicatedTLSProfile) {
		t.Fatalf("late authentication error = %v", err)
	}
	if stats := client.Stats(); stats.Connections != 0 || stats.Idle != 0 {
		t.Fatalf("late authentication published: %+v", stats)
	}
	select {
	case <-peerDone:
	case <-time.After(time.Second):
		t.Fatal("rejected authenticated connection remained open")
	}
}

func BenchmarkAuthenticatedReplicatedPoolAcquireRelease(b *testing.B) {
	endpoint := ReplicatedEndpoint{Member: 1, Node: [16]byte{1}, StoreID: [16]byte{2}, NodeIncarnation: 3, Address: "fixed"}
	physical := replicatedTransportEndpoint{node: endpoint.Node, address: endpoint.Address}
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	connection := &pooledReplicatedConn{conn: &testAuthenticatedConnection{Conn: left}, endpoint: physical, created: time.Now(), lastUsed: time.Now(), generation: 1}
	client := &AuthenticatedReplicatedClient{maxConnections: 1, maxPerEndpoint: 1, maxIdlePerEndpoint: 1, maxIdleAge: time.Hour, maxLifetime: time.Hour, generation: 1, total: 1, perEndpoint: map[replicatedTransportEndpoint]int{physical: 1}, idle: map[replicatedTransportEndpoint][]*pooledReplicatedConn{physical: {connection}}, active: make(map[*pooledReplicatedConn]struct{}), wake: make(chan struct{})}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		acquired, _ := client.acquire(context.Background(), endpoint)
		client.release(acquired, true)
	}
}
