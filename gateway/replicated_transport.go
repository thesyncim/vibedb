package gateway

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

var (
	ErrReplicatedTransportBound = errors.New("gateway: replicated transport bound exceeded")
	ErrReplicatedTLSProfile     = errors.New("gateway: invalid replicated TLS profile")
)

const (
	AbsoluteMaxReplicatedPoolConnections = 65536
	AbsoluteMaxReplicatedPoolWaiters     = 65536
	AbsoluteMaxReplicatedHandshakes      = 65536
	AbsoluteMaxReplicatedConnectionAge   = 24 * time.Hour
)

// RawReplicatedDial opens one owned TCP-like stream. Authentication is always
// performed by AuthenticatedReplicatedClient before the stream enters its pool.
type RawReplicatedDial func(context.Context, string) (net.Conn, error)

type AuthenticatedReplicatedClientOptions struct {
	TLS                *rafttransport.PeerTLS
	Dial               RawReplicatedDial
	HandshakeDeadline  rafttransport.DeadlineFunc
	MaxConnections     int
	MaxPerEndpoint     int
	MaxIdlePerEndpoint int
	// MaxHandshakes independently bounds concurrent dial-plus-TLS work. Zero
	// preserves source compatibility by selecting MaxConnections; production
	// callers should set the smaller admission budget they intend to own.
	MaxHandshakes int
	// ReservedControlConnections keeps this many checkout slots available to
	// schema, membership, and topology traffic. Zero selects one when the pool
	// has more than one connection. The reservation limits active data
	// checkouts, not physical stream reuse, so an idle stream remains reusable
	// by every capability.
	ReservedControlConnections int
	// ReservedControlHandshakes similarly prevents data dial/TLS work from
	// consuming every handshake slot. Zero selects one when MaxHandshakes is
	// greater than one.
	ReservedControlHandshakes int
	MaxWaiters                int
	MaxIdleAge                time.Duration
	MaxLifetime               time.Duration
}

type pooledReplicatedConn struct {
	conn                         rafttransport.PeerConnection
	endpoint                     replicatedTransportEndpoint
	created                      time.Time
	lastUsed                     time.Time
	generation                   uint64
	control                      bool
	idle                         bool
	endpointOlder, endpointNewer *pooledReplicatedConn
	globalOlder, globalNewer     *pooledReplicatedConn
}

type replicatedIdleEndpoint struct {
	oldest *pooledReplicatedConn
	newest *pooledReplicatedConn
	count  int
}

// replicatedTransportEndpoint is the physical authenticated pool key. Shard
// membership and store-incarnation fences are checked per response and do not
// fragment transport reuse across shards served by the same node endpoint.
type replicatedTransportEndpoint struct {
	node    rafttransport.NodeID
	address string
}

// AuthenticatedReplicatedClient owns a bounded exclusive LIFO connection pool.
// One checked-out stream carries exactly one request at a time. Any transport,
// framing, deadline, cancellation, identity, or deadline-reset error poisons it.
type AuthenticatedReplicatedClient struct {
	mu                 sync.Mutex
	tls                *rafttransport.PeerTLS
	dial               RawReplicatedDial
	authenticate       func(context.Context, net.Conn, rafttransport.NodeID, rafttransport.TrafficClass, rafttransport.DeadlineFunc) (rafttransport.PeerConnection, error)
	handshakeDeadline  rafttransport.DeadlineFunc
	maxConnections     int
	maxPerEndpoint     int
	maxIdlePerEndpoint int
	maxHandshakes      int
	maxWaiters         int
	maxIdleAge         time.Duration
	maxLifetime        time.Duration
	generation         uint64
	closed             bool
	total              int
	handshakes         int
	dataHandshakes     int
	peakHandshakes     int
	waiters            int
	controlWaiters     int
	dataWaiters        int
	dataInUse          int
	controlReserve     int
	handshakeReserve   int
	perEndpoint        map[replicatedTransportEndpoint]int
	idle               map[replicatedTransportEndpoint]replicatedIdleEndpoint
	idleOldest         *pooledReplicatedConn
	idleNewest         *pooledReplicatedConn
	idleCount          int
	active             map[*pooledReplicatedConn]struct{}
	wake               chan struct{}

	dials             atomic.Uint64
	reuses            atomic.Uint64
	poisoned          atomic.Uint64
	rejected          atomic.Uint64
	handshakeFailures atomic.Uint64
	idleEvictions     atomic.Uint64
}

type AuthenticatedReplicatedClientStats struct {
	Dials, Reuses, Poisoned, Rejected         uint64
	HandshakeFailures                         uint64
	IdleEvictions                             uint64
	Connections, Idle, Waiters                int
	ControlWaiters, DataWaiters, DataInUse    int
	ReservedControlConnections                int
	ReservedControlHandshakes                 int
	Handshakes, PeakHandshakes, MaxHandshakes int
	Generation                                uint64
}

func NewAuthenticatedReplicatedClient(options AuthenticatedReplicatedClientOptions) (*AuthenticatedReplicatedClient, error) {
	maxHandshakes := options.MaxHandshakes
	if maxHandshakes == 0 {
		maxHandshakes = options.MaxConnections
	}
	controlReserve := options.ReservedControlConnections
	if controlReserve == 0 && options.MaxConnections > 1 {
		controlReserve = 1
	}
	handshakeReserve := options.ReservedControlHandshakes
	if handshakeReserve == 0 && maxHandshakes > 1 {
		handshakeReserve = 1
	}
	if options.TLS == nil || options.TLS.LocalIdentity().Node == (rafttransport.NodeID{}) ||
		options.Dial == nil || options.HandshakeDeadline == nil ||
		options.MaxConnections <= 0 || options.MaxConnections > AbsoluteMaxReplicatedPoolConnections ||
		options.MaxPerEndpoint <= 0 || options.MaxPerEndpoint > options.MaxConnections ||
		options.MaxIdlePerEndpoint < 0 || options.MaxIdlePerEndpoint > options.MaxPerEndpoint ||
		maxHandshakes <= 0 || maxHandshakes > options.MaxConnections ||
		maxHandshakes > AbsoluteMaxReplicatedHandshakes ||
		options.ReservedControlConnections < 0 || controlReserve >= options.MaxConnections ||
		options.ReservedControlHandshakes < 0 || handshakeReserve >= maxHandshakes ||
		options.MaxWaiters < 0 || options.MaxWaiters > AbsoluteMaxReplicatedPoolWaiters ||
		options.MaxIdleAge <= 0 || options.MaxIdleAge > AbsoluteMaxReplicatedConnectionAge ||
		options.MaxLifetime <= 0 || options.MaxLifetime > AbsoluteMaxReplicatedConnectionAge ||
		options.MaxIdleAge > options.MaxLifetime {
		return nil, ErrReplicatedTLSProfile
	}
	return &AuthenticatedReplicatedClient{
		tls: options.TLS, dial: options.Dial, handshakeDeadline: options.HandshakeDeadline,
		authenticate:   options.TLS.Client,
		maxConnections: options.MaxConnections, maxPerEndpoint: options.MaxPerEndpoint,
		maxIdlePerEndpoint: options.MaxIdlePerEndpoint, maxHandshakes: maxHandshakes,
		controlReserve: controlReserve, handshakeReserve: handshakeReserve,
		maxWaiters: options.MaxWaiters,
		maxIdleAge: options.MaxIdleAge, maxLifetime: options.MaxLifetime, generation: 1,
		perEndpoint: make(map[replicatedTransportEndpoint]int),
		idle:        make(map[replicatedTransportEndpoint]replicatedIdleEndpoint), wake: make(chan struct{}),
		active: make(map[*pooledReplicatedConn]struct{}),
	}, nil
}

func validAuthenticatedEndpoint(endpoint ReplicatedEndpoint) bool {
	return endpoint.Member != 0 && endpoint.Node != (rafttransport.NodeID{}) &&
		endpoint.StoreID != ([16]byte{}) && endpoint.NodeIncarnation != 0 && endpoint.Address != ""
}

func (client *AuthenticatedReplicatedClient) signalLocked() {
	if client.waiters == 0 {
		return
	}
	close(client.wake)
	client.wake = make(chan struct{})
}

func (client *AuthenticatedReplicatedClient) closeLocked(connection *pooledReplicatedConn) {
	if connection == nil {
		return
	}
	client.unlinkIdleLocked(connection)
	_ = connection.conn.Close()
	client.total--
	client.perEndpoint[connection.endpoint]--
	if client.perEndpoint[connection.endpoint] == 0 {
		delete(client.perEndpoint, connection.endpoint)
	}
	client.signalLocked()
}

func (client *AuthenticatedReplicatedClient) releaseHandshakeReservationLocked(
	physical replicatedTransportEndpoint, control bool,
) {
	client.handshakes--
	if !control {
		client.dataHandshakes--
		client.dataInUse--
	}
	client.total--
	client.perEndpoint[physical]--
	if client.perEndpoint[physical] == 0 {
		delete(client.perEndpoint, physical)
	}
	client.signalLocked()
}

func (client *AuthenticatedReplicatedClient) acquire(ctx context.Context, endpoint ReplicatedEndpoint) (*pooledReplicatedConn, error) {
	return client.acquireClass(ctx, endpoint, false)
}

func (client *AuthenticatedReplicatedClient) linkIdleLocked(connection *pooledReplicatedConn) {
	endpoint := client.idle[connection.endpoint]
	connection.endpointOlder = endpoint.newest
	if endpoint.newest == nil {
		endpoint.oldest = connection
	} else {
		endpoint.newest.endpointNewer = connection
	}
	endpoint.newest = connection
	endpoint.count++
	client.idle[connection.endpoint] = endpoint

	connection.globalOlder = client.idleNewest
	if client.idleNewest == nil {
		client.idleOldest = connection
	} else {
		client.idleNewest.globalNewer = connection
	}
	client.idleNewest = connection
	client.idleCount++
	connection.idle = true
}

func (client *AuthenticatedReplicatedClient) unlinkIdleLocked(connection *pooledReplicatedConn) {
	if connection == nil || !connection.idle {
		return
	}
	endpoint := client.idle[connection.endpoint]
	if connection.endpointOlder == nil {
		endpoint.oldest = connection.endpointNewer
	} else {
		connection.endpointOlder.endpointNewer = connection.endpointNewer
	}
	if connection.endpointNewer == nil {
		endpoint.newest = connection.endpointOlder
	} else {
		connection.endpointNewer.endpointOlder = connection.endpointOlder
	}
	endpoint.count--
	if endpoint.count == 0 {
		delete(client.idle, connection.endpoint)
	} else {
		client.idle[connection.endpoint] = endpoint
	}

	if connection.globalOlder == nil {
		client.idleOldest = connection.globalNewer
	} else {
		connection.globalOlder.globalNewer = connection.globalNewer
	}
	if connection.globalNewer == nil {
		client.idleNewest = connection.globalOlder
	} else {
		connection.globalNewer.globalOlder = connection.globalOlder
	}
	connection.endpointOlder, connection.endpointNewer = nil, nil
	connection.globalOlder, connection.globalNewer = nil, nil
	connection.idle = false
	client.idleCount--
}

func (client *AuthenticatedReplicatedClient) evictOldestIdleLocked() bool {
	oldest := client.idleOldest
	if oldest == nil {
		return false
	}
	client.closeLocked(oldest)
	client.idleEvictions.Add(1)
	return true
}

func (client *AuthenticatedReplicatedClient) acquireClass(
	ctx context.Context, endpoint ReplicatedEndpoint, control bool,
) (*pooledReplicatedConn, error) {
	if client == nil || ctx == nil || !validAuthenticatedEndpoint(endpoint) {
		return nil, ErrReplicatedTLSProfile
	}
	physical := replicatedTransportEndpoint{node: endpoint.Node, address: endpoint.Address}
	for {
		now := time.Now()
		client.mu.Lock()
		if client.closed {
			client.mu.Unlock()
			return nil, ErrReplicatedTLSProfile
		}
		dataCheckoutAvailable := control || client.dataInUse < client.maxConnections-client.controlReserve
		endpointIdle := client.idle[physical]
		for dataCheckoutAvailable && endpointIdle.newest != nil {
			connection := endpointIdle.newest
			client.unlinkIdleLocked(connection)
			if connection.generation != client.generation ||
				now.Sub(connection.lastUsed) > client.maxIdleAge || now.Sub(connection.created) > client.maxLifetime {
				client.closeLocked(connection)
				endpointIdle = client.idle[physical]
				continue
			}
			client.active[connection] = struct{}{}
			connection.control = control
			if !control {
				client.dataInUse++
			}
			client.reuses.Add(1)
			client.mu.Unlock()
			return connection, nil
		}
		dataHandshakeAvailable := control || client.dataHandshakes < client.maxHandshakes-client.handshakeReserve
		canCreate := dataCheckoutAvailable && dataHandshakeAvailable &&
			client.perEndpoint[physical] < client.maxPerEndpoint && client.handshakes < client.maxHandshakes
		if canCreate && client.total >= client.maxConnections {
			client.evictOldestIdleLocked()
		}
		if canCreate && client.total < client.maxConnections {
			client.total++
			client.perEndpoint[physical]++
			client.handshakes++
			if !control {
				client.dataHandshakes++
				client.dataInUse++
			}
			if client.handshakes > client.peakHandshakes {
				client.peakHandshakes = client.handshakes
			}
			authenticate, generation := client.authenticate, client.generation
			client.mu.Unlock()
			raw, err := client.dial(ctx, endpoint.Address)
			if err == nil && raw == nil {
				err = ErrReplicatedTLSProfile
			}
			if err != nil {
				if raw != nil {
					_ = raw.Close()
				}
				client.mu.Lock()
				client.releaseHandshakeReservationLocked(physical, control)
				client.mu.Unlock()
				client.handshakeFailures.Add(1)
				return nil, err
			}
			authenticated, err := authenticate(ctx, raw, endpoint.Node, rafttransport.TrafficShardNative, client.handshakeDeadline)
			if err == nil && (authenticated == nil || authenticated.PeerIdentity().Node != endpoint.Node ||
				authenticated.TrafficClass() != rafttransport.TrafficShardNative) {
				if authenticated != nil {
					_ = authenticated.Close()
				}
				err = ErrReplicatedTLSProfile
			}
			if err != nil {
				client.mu.Lock()
				client.releaseHandshakeReservationLocked(physical, control)
				client.mu.Unlock()
				client.handshakeFailures.Add(1)
				return nil, err
			}
			client.mu.Lock()
			client.handshakes--
			if !control {
				client.dataHandshakes--
			}
			client.signalLocked()
			if client.closed || generation != client.generation {
				_ = authenticated.Close()
				client.total--
				if !control {
					client.dataInUse--
				}
				client.perEndpoint[physical]--
				if client.perEndpoint[physical] == 0 {
					delete(client.perEndpoint, physical)
				}
				client.signalLocked()
				client.mu.Unlock()
				return nil, ErrReplicatedTLSProfile
			}
			connection := &pooledReplicatedConn{conn: authenticated, endpoint: physical, created: now, lastUsed: now, generation: generation, control: control}
			client.active[connection] = struct{}{}
			client.mu.Unlock()
			client.dials.Add(1)
			return connection, nil
		}
		if client.waiters >= client.maxWaiters {
			client.rejected.Add(1)
			client.mu.Unlock()
			return nil, ErrReplicatedTransportBound
		}
		wake := client.wake
		client.waiters++
		if control {
			client.controlWaiters++
		} else {
			client.dataWaiters++
		}
		client.mu.Unlock()
		select {
		case <-ctx.Done():
			client.mu.Lock()
			client.waiters--
			if control {
				client.controlWaiters--
			} else {
				client.dataWaiters--
			}
			client.mu.Unlock()
			return nil, context.Cause(ctx)
		case <-wake:
			client.mu.Lock()
			client.waiters--
			if control {
				client.controlWaiters--
			} else {
				client.dataWaiters--
			}
			client.mu.Unlock()
		}
	}
}

func (client *AuthenticatedReplicatedClient) release(connection *pooledReplicatedConn, healthy bool) {
	if connection == nil {
		return
	}
	if healthy {
		healthy = connection.conn.SetDeadline(time.Time{}) == nil
	}
	now := time.Now()
	client.mu.Lock()
	delete(client.active, connection)
	if !connection.control {
		client.dataInUse--
	}
	connection.control = false
	endpointIdle := client.idle[connection.endpoint]
	idleAtEndpoint := endpointIdle.count
	if !healthy || connection.generation != client.generation || now.Sub(connection.created) > client.maxLifetime ||
		idleAtEndpoint >= client.maxIdlePerEndpoint {
		client.closeLocked(connection)
		client.mu.Unlock()
		if !healthy {
			client.poisoned.Add(1)
		}
		return
	}
	connection.lastUsed = now
	client.linkIdleLocked(connection)
	client.signalLocked()
	client.mu.Unlock()
}

func (client *AuthenticatedReplicatedClient) DoReplicated(ctx context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	control := request != nil && request.Capability&(serviceauthz.CapabilitySchema|serviceauthz.CapabilityMembership|serviceauthz.CapabilityTopology) != 0
	connection, err := client.acquireClass(ctx, endpoint, control)
	if err != nil {
		return nil, err
	}
	response, err := shardservice.RoundTripReplicated(ctx, connection.conn, request)
	identityMismatch := err == nil && response != nil && response.HasState &&
		(response.State.Fence.MemberID != endpoint.Member || response.State.Fence.StoreID != endpoint.StoreID ||
			response.State.Fence.NodeIncarnation != endpoint.NodeIncarnation)
	healthy := err == nil && !identityMismatch && context.Cause(ctx) == nil
	client.release(connection, healthy)
	if identityMismatch {
		return nil, ErrReplicatedRoute
	}
	return response, err
}

// RotateTLS atomically publishes a new profile and drains every idle stream.
// Checked-out old-generation streams are closed when returned.
func (client *AuthenticatedReplicatedClient) RotateTLS(profile *rafttransport.PeerTLS) error {
	if client == nil || profile == nil || profile.LocalIdentity().Node == (rafttransport.NodeID{}) {
		return ErrReplicatedTLSProfile
	}
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return ErrReplicatedTLSProfile
	}
	if profile.LocalIdentity() != client.tls.LocalIdentity() {
		client.mu.Unlock()
		return ErrReplicatedTLSProfile
	}
	client.tls = profile
	client.authenticate = profile.Client
	client.generation++
	for connection := range client.active {
		_ = connection.conn.Close()
	}
	for client.idleOldest != nil {
		client.closeLocked(client.idleOldest)
	}
	client.signalLocked()
	client.mu.Unlock()
	return nil
}

// Close refuses new checkouts, wakes bounded waiters, and closes every idle
// connection. Checked-out streams are poisoned when their owner returns them.
func (client *AuthenticatedReplicatedClient) Close() error {
	if client == nil {
		return ErrReplicatedTLSProfile
	}
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return nil
	}
	client.closed = true
	client.generation++
	for connection := range client.active {
		_ = connection.conn.Close()
	}
	for client.idleOldest != nil {
		client.closeLocked(client.idleOldest)
	}
	client.signalLocked()
	client.mu.Unlock()
	return nil
}

func (client *AuthenticatedReplicatedClient) Stats() AuthenticatedReplicatedClientStats {
	if client == nil {
		return AuthenticatedReplicatedClientStats{}
	}
	client.mu.Lock()
	stats := AuthenticatedReplicatedClientStats{Connections: client.total, Idle: client.idleCount,
		Waiters: client.waiters, Handshakes: client.handshakes,
		ControlWaiters: client.controlWaiters, DataWaiters: client.dataWaiters,
		DataInUse: client.dataInUse, ReservedControlConnections: client.controlReserve,
		ReservedControlHandshakes: client.handshakeReserve,
		PeakHandshakes:            client.peakHandshakes, MaxHandshakes: client.maxHandshakes,
		Generation: client.generation}
	client.mu.Unlock()
	stats.Dials, stats.Reuses = client.dials.Load(), client.reuses.Load()
	stats.Poisoned, stats.Rejected = client.poisoned.Load(), client.rejected.Load()
	stats.HandshakeFailures = client.handshakeFailures.Load()
	stats.IdleEvictions = client.idleEvictions.Load()
	return stats
}
