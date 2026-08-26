package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type testAuthenticatedConnection struct {
	net.Conn
	identity rafttransport.PeerIdentity
}

type idleTestAddr string

func (address idleTestAddr) Network() string { return string(address) }
func (address idleTestAddr) String() string  { return string(address) }

type idleTestConn struct{}

func (idleTestConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (idleTestConn) Write(value []byte) (int, error)  { return len(value), nil }
func (idleTestConn) Close() error                     { return nil }
func (idleTestConn) LocalAddr() net.Addr              { return idleTestAddr("local") }
func (idleTestConn) RemoteAddr() net.Addr             { return idleTestAddr("remote") }
func (idleTestConn) SetDeadline(time.Time) error      { return nil }
func (idleTestConn) SetReadDeadline(time.Time) error  { return nil }
func (idleTestConn) SetWriteDeadline(time.Time) error { return nil }

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

func TestAuthenticatedReplicatedClientDefaultsAndBoundsControlReserve(t *testing.T) {
	authority := newGatewayTLSAuthority(t)
	profile := authority.profile(t, gatewayPeerIdentity(9, 17))
	options := AuthenticatedReplicatedClientOptions{
		TLS: profile, Dial: func(context.Context, string) (net.Conn, error) { return nil, errors.New("unused") },
		HandshakeDeadline: func() time.Time { return time.Now().Add(time.Second) },
		MaxConnections:    2, MaxPerEndpoint: 2, MaxIdlePerEndpoint: 1, MaxHandshakes: 2,
		MaxWaiters: 2, MaxIdleAge: time.Second, MaxLifetime: time.Minute,
	}
	client, err := NewAuthenticatedReplicatedClient(options)
	if err != nil {
		t.Fatal(err)
	}
	if stats := client.Stats(); stats.ReservedControlConnections != 1 || stats.ReservedControlHandshakes != 1 ||
		stats.ReservedControlWaiters != 1 {
		t.Fatalf("default reserves = %+v", stats)
	}
	_ = client.Close()
	options.ReservedControlConnections = options.MaxConnections
	if _, err = NewAuthenticatedReplicatedClient(options); !errors.Is(err, ErrReplicatedTLSProfile) {
		t.Fatalf("connection reserve bound = %v", err)
	}
	options.ReservedControlConnections = 1
	options.ReservedControlHandshakes = options.MaxHandshakes
	if _, err = NewAuthenticatedReplicatedClient(options); !errors.Is(err, ErrReplicatedTLSProfile) {
		t.Fatalf("handshake reserve bound = %v", err)
	}
	options.ReservedControlHandshakes = 1
	options.ReservedControlWaiters = options.MaxWaiters
	if _, err = NewAuthenticatedReplicatedClient(options); !errors.Is(err, ErrReplicatedTLSProfile) {
		t.Fatalf("waiter reserve bound = %v", err)
	}
}

func TestReplicatedCapabilityControlReserveClassification(t *testing.T) {
	for _, capability := range []serviceauthz.Capability{
		serviceauthz.CapabilitySchema,
		serviceauthz.CapabilityMembership,
		serviceauthz.CapabilityTopology,
		serviceauthz.CapabilityTransactionRecovery,
		serviceauthz.CapabilityDataRead | serviceauthz.CapabilityTransactionRecovery,
	} {
		if !replicatedCapabilityUsesControlReserve(capability) {
			t.Fatalf("capability %d did not use the control reserve", capability)
		}
	}
	for _, capability := range []serviceauthz.Capability{
		0, serviceauthz.CapabilityDataRead, serviceauthz.CapabilityDataWrite,
		serviceauthz.CapabilityDataRead | serviceauthz.CapabilityDataWrite,
	} {
		if replicatedCapabilityUsesControlReserve(capability) {
			t.Fatalf("data capability %d consumed the control reserve", capability)
		}
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
		perEndpoint: make(map[replicatedTransportEndpoint]int), idle: make(map[replicatedTransportEndpoint]replicatedIdleEndpoint), active: make(map[*pooledReplicatedConn]struct{}), wake: make(chan struct{}),
	}
	return client, &dials
}

func testCapacityClient(t *testing.T, maxConnections, controlReserve int) (*AuthenticatedReplicatedClient, *atomic.Uint64) {
	t.Helper()
	var dials atomic.Uint64
	client := &AuthenticatedReplicatedClient{
		dial: func(context.Context, string) (net.Conn, error) {
			dials.Add(1)
			local, peer := net.Pipe()
			go func() {
				_, _ = io.Copy(io.Discard, peer)
				_ = peer.Close()
			}()
			return local, nil
		},
		authenticate: func(_ context.Context, raw net.Conn, node rafttransport.NodeID, class rafttransport.TrafficClass, _ rafttransport.DeadlineFunc) (rafttransport.PeerConnection, error) {
			if class != rafttransport.TrafficShardNative {
				return nil, errors.New("wrong capability")
			}
			return &testAuthenticatedConnection{Conn: raw, identity: rafttransport.PeerIdentity{Node: node}}, nil
		},
		handshakeDeadline: func() time.Time { return time.Now().Add(time.Second) },
		maxConnections:    maxConnections, maxPerEndpoint: maxConnections, maxIdlePerEndpoint: 1,
		maxHandshakes: maxConnections, handshakeReserve: controlReserve,
		controlReserve: controlReserve, maxWaiters: maxConnections,
		maxIdleAge: time.Minute, maxLifetime: time.Hour, generation: 1,
		perEndpoint: make(map[replicatedTransportEndpoint]int),
		idle:        make(map[replicatedTransportEndpoint]replicatedIdleEndpoint),
		active:      make(map[*pooledReplicatedConn]struct{}), wake: make(chan struct{}),
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, &dials
}

func testCapacityEndpoint(index byte) ReplicatedEndpoint {
	return ReplicatedEndpoint{
		Member: uint64(index) + 1, Node: rafttransport.NodeID{index + 1},
		StoreID: [16]byte{index + 1}, NodeIncarnation: 1,
		Address: "endpoint-" + strconv.Itoa(int(index)),
	}
}

func testLargeCapacityEndpoint(index int) ReplicatedEndpoint {
	value := uint64(index + 1)
	var node rafttransport.NodeID
	var store [16]byte
	for offset := 0; offset < 8; offset++ {
		node[offset] = byte(value >> (offset * 8))
		store[offset] = byte(value >> (offset * 8))
	}
	return ReplicatedEndpoint{
		Member: value, Node: node, StoreID: store, NodeIncarnation: 1,
		Address: "large-endpoint-" + strconv.Itoa(index),
	}
}

func testFullIdleClient(capacity int) (*AuthenticatedReplicatedClient, []ReplicatedEndpoint) {
	client := &AuthenticatedReplicatedClient{
		maxConnections: capacity, maxPerEndpoint: 1, maxIdlePerEndpoint: 1,
		maxHandshakes: capacity, maxWaiters: capacity,
		maxIdleAge: time.Hour, maxLifetime: time.Hour, generation: 1,
		perEndpoint: make(map[replicatedTransportEndpoint]int, capacity),
		idle:        make(map[replicatedTransportEndpoint]replicatedIdleEndpoint, capacity),
		active:      make(map[*pooledReplicatedConn]struct{}), wake: make(chan struct{}),
	}
	endpoints := make([]ReplicatedEndpoint, capacity*2)
	for index := range endpoints {
		endpoints[index] = testLargeCapacityEndpoint(index)
		if index >= capacity {
			continue
		}
		physical := replicatedTransportEndpoint{node: endpoints[index].Node, address: endpoints[index].Address}
		connection := &pooledReplicatedConn{
			conn:     &testAuthenticatedConnection{Conn: idleTestConn{}, identity: rafttransport.PeerIdentity{Node: physical.node}},
			endpoint: physical, created: time.Now(), lastUsed: time.Now(), generation: 1,
		}
		client.total++
		client.perEndpoint[physical] = 1
		client.linkIdleLocked(connection)
	}
	return client, endpoints
}

func TestAuthenticatedReplicatedClientEvictsGlobalOldestIdleOnEndpointChurn(t *testing.T) {
	client, dials := testCapacityClient(t, 4, 1)
	for index := byte(0); index < 64; index++ {
		connection, err := client.acquire(context.Background(), testCapacityEndpoint(index))
		if err != nil {
			t.Fatalf("acquire endpoint %d: %v", index, err)
		}
		client.release(connection, true)
	}
	stats := client.Stats()
	if dials.Load() != 64 || stats.Connections != 4 || stats.Idle != 4 || stats.IdleEvictions != 60 {
		t.Fatalf("churn stats = %+v dials=%d", stats, dials.Load())
	}
	connection, err := client.acquire(context.Background(), testCapacityEndpoint(63))
	if err != nil {
		t.Fatal(err)
	}
	client.release(connection, true)
	if dials.Load() != 64 || client.Stats().Reuses != 1 {
		t.Fatalf("hot endpoint was not reused: stats=%+v dials=%d", client.Stats(), dials.Load())
	}
}

func TestAuthenticatedReplicatedClientConstantTimeLRUAt4096Capacity(t *testing.T) {
	const capacity = 4096
	client, endpoints := testFullIdleClient(capacity)
	t.Cleanup(func() { _ = client.Close() })
	if client.idleCount != capacity || len(client.idle) != capacity || client.total != capacity {
		t.Fatalf("initial capacity: idle=%d endpoints=%d total=%d", client.idleCount, len(client.idle), client.total)
	}
	for index := 0; index < capacity; index++ {
		oldest := client.idleOldest
		if oldest == nil || !client.evictOldestIdleLocked() || oldest.idle ||
			oldest.endpointOlder != nil || oldest.endpointNewer != nil || oldest.globalOlder != nil || oldest.globalNewer != nil {
			t.Fatalf("eviction %d left intrusive links", index)
		}
		endpoint := endpoints[capacity+index]
		physical := replicatedTransportEndpoint{node: endpoint.Node, address: endpoint.Address}
		connection := &pooledReplicatedConn{
			conn:     &testAuthenticatedConnection{Conn: idleTestConn{}, identity: rafttransport.PeerIdentity{Node: physical.node}},
			endpoint: physical, created: time.Now(), lastUsed: time.Now(), generation: 1,
		}
		client.total++
		client.perEndpoint[physical] = 1
		client.linkIdleLocked(connection)
	}
	firstNew := replicatedTransportEndpoint{node: endpoints[capacity].Node, address: endpoints[capacity].Address}
	lastNew := replicatedTransportEndpoint{node: endpoints[2*capacity-1].Node, address: endpoints[2*capacity-1].Address}
	stats := client.Stats()
	if stats.IdleEvictions != capacity || stats.Connections != capacity || stats.Idle != capacity ||
		len(client.idle) != capacity || client.idleOldest.endpoint != firstNew || client.idleNewest.endpoint != lastNew {
		t.Fatalf("4096-capacity churn: stats=%+v endpoints=%d", stats, len(client.idle))
	}
}

func TestAuthenticatedReplicatedClientControlProgressWhileDataSaturates(t *testing.T) {
	client, _ := testCapacityClient(t, 3, 1)
	first, err := client.acquire(context.Background(), testCapacityEndpoint(1))
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.acquire(context.Background(), testCapacityEndpoint(2))
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancelWait := context.WithCancel(context.Background())
	waiting := make(chan error, 1)
	go func() {
		_, acquireErr := client.acquire(waitCtx, testCapacityEndpoint(3))
		waiting <- acquireErr
	}()
	deadline := time.Now().Add(time.Second)
	for client.Stats().DataWaiters != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if stats := client.Stats(); stats.DataInUse != 2 || stats.DataWaiters != 1 ||
		stats.ReservedControlConnections != 1 || stats.ReservedControlHandshakes != 1 {
		t.Fatalf("data saturation stats = %+v", stats)
	}
	control, err := client.acquireClass(context.Background(), testCapacityEndpoint(4), true)
	if err != nil {
		t.Fatalf("control acquire behind saturated data: %v", err)
	}
	if stats := client.Stats(); stats.Connections != 3 || stats.DataInUse != 2 {
		t.Fatalf("control progress stats = %+v", stats)
	}
	client.release(control, true)
	cancelWait()
	if err := <-waiting; !errors.Is(err, context.Canceled) {
		t.Fatalf("data waiter cancellation = %v", err)
	}
	client.release(first, true)
	client.release(second, true)
	if stats := client.Stats(); stats.Waiters != 0 || stats.DataWaiters != 0 || stats.ControlWaiters != 0 || stats.DataInUse != 0 {
		t.Fatalf("settled stats = %+v", stats)
	}
}

func TestAuthenticatedReplicatedClientReservesControlHandshake(t *testing.T) {
	client, _ := testCapacityClient(t, 3, 1)
	entered := make(chan rafttransport.NodeID, 3)
	unblock := make(chan struct{})
	client.authenticate = func(_ context.Context, raw net.Conn, node rafttransport.NodeID, _ rafttransport.TrafficClass, _ rafttransport.DeadlineFunc) (rafttransport.PeerConnection, error) {
		entered <- node
		<-unblock
		return &testAuthenticatedConnection{Conn: raw, identity: rafttransport.PeerIdentity{Node: node}}, nil
	}
	type acquireResult struct {
		connection *pooledReplicatedConn
		err        error
	}
	results := make(chan acquireResult, 4)
	for _, index := range []byte{1, 2, 3} {
		endpoint := testCapacityEndpoint(index)
		go func() {
			connection, err := client.acquire(context.Background(), endpoint)
			results <- acquireResult{connection: connection, err: err}
		}()
	}
	for range 2 {
		<-entered
	}
	select {
	case node := <-entered:
		t.Fatalf("third data handshake consumed reserved slot: node=%x", node)
	case <-time.After(10 * time.Millisecond):
	}
	deadline := time.Now().Add(time.Second)
	for client.Stats().DataWaiters != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	controlEndpoint := testCapacityEndpoint(4)
	go func() {
		connection, err := client.acquireClass(context.Background(), controlEndpoint, true)
		results <- acquireResult{connection: connection, err: err}
	}()
	select {
	case node := <-entered:
		if node != controlEndpoint.Node {
			t.Fatalf("reserved handshake went to node=%x want=%x", node, controlEndpoint.Node)
		}
	case <-time.After(time.Second):
		t.Fatal("control handshake did not enter reserved slot")
	}
	if stats := client.Stats(); stats.Handshakes != 3 || stats.DataInUse != 2 || stats.Waiters != 1 {
		t.Fatalf("reserved handshake stats = %+v", stats)
	}
	close(unblock)
	for range 4 {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		client.release(result.connection, true)
	}
	if stats := client.Stats(); stats.Handshakes != 0 || stats.Waiters != 0 || stats.DataInUse != 0 {
		t.Fatalf("completed reserved handshake stats = %+v", stats)
	}
}

func TestAuthenticatedReplicatedClientControlWaitCancellationStats(t *testing.T) {
	client, _ := testCapacityClient(t, 2, 1)
	data, err := client.acquire(context.Background(), testCapacityEndpoint(1))
	if err != nil {
		t.Fatal(err)
	}
	control, err := client.acquireClass(context.Background(), testCapacityEndpoint(2), true)
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, acquireErr := client.acquireClass(waitCtx, testCapacityEndpoint(3), true)
		result <- acquireErr
	}()
	deadline := time.Now().Add(time.Second)
	for client.Stats().ControlWaiters != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if stats := client.Stats(); stats.ControlWaiters != 1 || stats.DataWaiters != 0 {
		t.Fatalf("control waiter stats = %+v", stats)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("control waiter cancellation = %v", err)
	}
	client.release(control, true)
	client.release(data, true)
	if stats := client.Stats(); stats.Waiters != 0 || stats.ControlWaiters != 0 || stats.DataWaiters != 0 {
		t.Fatalf("canceled waiter remained: %+v", stats)
	}
}

func TestAuthenticatedReplicatedClientReservesControlWaiter(t *testing.T) {
	client, _ := testCapacityClient(t, 2, 1)
	client.waiterReserve = 1
	data, err := client.acquire(context.Background(), testCapacityEndpoint(1))
	if err != nil {
		t.Fatal(err)
	}
	control, err := client.acquireClass(context.Background(), testCapacityEndpoint(2), true)
	if err != nil {
		t.Fatal(err)
	}

	dataWaitCtx, cancelDataWait := context.WithCancel(context.Background())
	dataWait := make(chan error, 1)
	go func() {
		_, acquireErr := client.acquire(dataWaitCtx, testCapacityEndpoint(3))
		dataWait <- acquireErr
	}()
	deadline := time.Now().Add(time.Second)
	for client.Stats().DataWaiters != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if stats := client.Stats(); stats.Waiters != 1 || stats.DataWaiters != 1 ||
		stats.ReservedControlWaiters != 1 {
		t.Fatalf("reserved waiter setup = %+v", stats)
	}
	rejectedCtx, cancelRejected := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelRejected()
	if _, err = client.acquire(rejectedCtx, testCapacityEndpoint(4)); !errors.Is(err, ErrReplicatedTransportBound) {
		t.Fatalf("data consumed reserved control waiter: %v", err)
	}

	type acquireResult struct {
		connection *pooledReplicatedConn
		err        error
	}
	recoveryWait := make(chan acquireResult, 1)
	go func() {
		connection, acquireErr := client.acquireClass(context.Background(), testCapacityEndpoint(5), true)
		recoveryWait <- acquireResult{connection: connection, err: acquireErr}
	}()
	deadline = time.Now().Add(time.Second)
	for client.Stats().ControlWaiters != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if stats := client.Stats(); stats.Waiters != 2 || stats.DataWaiters != 1 || stats.ControlWaiters != 1 {
		t.Fatalf("recovery waiter did not enter reserve: %+v", stats)
	}

	client.release(control, true)
	select {
	case result := <-recoveryWait:
		if result.err != nil {
			t.Fatalf("recovery waiter failed after capacity release: %v", result.err)
		}
		client.release(result.connection, true)
	case <-time.After(time.Second):
		t.Fatal("recovery waiter did not progress through reserved admission")
	}
	cancelDataWait()
	if err := <-dataWait; !errors.Is(err, context.Canceled) {
		t.Fatalf("data waiter cancellation = %v", err)
	}
	client.release(data, true)
	if stats := client.Stats(); stats.Waiters != 0 || stats.ControlWaiters != 0 || stats.DataWaiters != 0 || stats.DataInUse != 0 {
		t.Fatalf("settled stats = %+v", stats)
	}
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
		idle:        make(map[replicatedTransportEndpoint]replicatedIdleEndpoint),
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
	client := &AuthenticatedReplicatedClient{maxConnections: 1, maxPerEndpoint: 1, maxIdlePerEndpoint: 1, maxIdleAge: time.Hour, maxLifetime: time.Hour, generation: 1, total: 1, perEndpoint: map[replicatedTransportEndpoint]int{physical: 1}, idle: make(map[replicatedTransportEndpoint]replicatedIdleEndpoint), active: make(map[*pooledReplicatedConn]struct{}), wake: make(chan struct{})}
	client.linkIdleLocked(connection)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		acquired, _ := client.acquire(context.Background(), endpoint)
		client.release(acquired, true)
	}
}

func BenchmarkAuthenticatedReplicatedPoolChurnAt4096Capacity(b *testing.B) {
	const capacity = 4096
	client, endpoints := testFullIdleClient(capacity)
	b.Cleanup(func() { _ = client.Close() })
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		client.mu.Lock()
		connection := client.idleOldest
		client.evictOldestIdleLocked()
		endpoint := endpoints[capacity+(index&(capacity-1))]
		connection.endpoint = replicatedTransportEndpoint{node: endpoint.Node, address: endpoint.Address}
		client.total++
		client.perEndpoint[connection.endpoint] = 1
		client.linkIdleLocked(connection)
		client.mu.Unlock()
	}
}
