package gateway

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardservice"
)

var (
	ErrReplicatedTransportBound = errors.New("gateway: replicated transport bound exceeded")
	ErrReplicatedTLSProfile     = errors.New("gateway: invalid replicated TLS profile")
)

const (
	AbsoluteMaxReplicatedPoolConnections = 65536
	AbsoluteMaxReplicatedPoolWaiters     = 65536
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
	MaxWaiters         int
	MaxIdleAge         time.Duration
	MaxLifetime        time.Duration
}

type pooledReplicatedConn struct {
	conn       rafttransport.PeerConnection
	endpoint   ReplicatedEndpoint
	created    time.Time
	lastUsed   time.Time
	generation uint64
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
	maxWaiters         int
	maxIdleAge         time.Duration
	maxLifetime        time.Duration
	generation         uint64
	closed             bool
	total              int
	waiters            int
	perEndpoint        map[rafttransport.NodeID]int
	idle               map[rafttransport.NodeID][]*pooledReplicatedConn
	wake               chan struct{}

	dials    atomic.Uint64
	reuses   atomic.Uint64
	poisoned atomic.Uint64
	rejected atomic.Uint64
}

type AuthenticatedReplicatedClientStats struct {
	Dials, Reuses, Poisoned, Rejected uint64
	Connections, Idle, Waiters        int
	Generation                        uint64
}

func NewAuthenticatedReplicatedClient(options AuthenticatedReplicatedClientOptions) (*AuthenticatedReplicatedClient, error) {
	if options.TLS == nil || options.TLS.LocalIdentity().Node == (rafttransport.NodeID{}) ||
		options.Dial == nil || options.HandshakeDeadline == nil ||
		options.MaxConnections <= 0 || options.MaxConnections > AbsoluteMaxReplicatedPoolConnections ||
		options.MaxPerEndpoint <= 0 || options.MaxPerEndpoint > options.MaxConnections ||
		options.MaxIdlePerEndpoint < 0 || options.MaxIdlePerEndpoint > options.MaxPerEndpoint ||
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
		maxIdlePerEndpoint: options.MaxIdlePerEndpoint, maxWaiters: options.MaxWaiters,
		maxIdleAge: options.MaxIdleAge, maxLifetime: options.MaxLifetime, generation: 1,
		perEndpoint: make(map[rafttransport.NodeID]int),
		idle:        make(map[rafttransport.NodeID][]*pooledReplicatedConn), wake: make(chan struct{}),
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
	_ = connection.conn.Close()
	client.total--
	client.perEndpoint[connection.endpoint.Node]--
	if client.perEndpoint[connection.endpoint.Node] == 0 {
		delete(client.perEndpoint, connection.endpoint.Node)
	}
	client.signalLocked()
}

func (client *AuthenticatedReplicatedClient) acquire(ctx context.Context, endpoint ReplicatedEndpoint) (*pooledReplicatedConn, error) {
	if client == nil || ctx == nil || !validAuthenticatedEndpoint(endpoint) {
		return nil, ErrReplicatedTLSProfile
	}
	for {
		now := time.Now()
		client.mu.Lock()
		if client.closed {
			client.mu.Unlock()
			return nil, ErrReplicatedTLSProfile
		}
		stack := client.idle[endpoint.Node]
		for len(stack) != 0 {
			last := len(stack) - 1
			connection := stack[last]
			stack = stack[:last]
			if connection.endpoint != endpoint || connection.generation != client.generation ||
				now.Sub(connection.lastUsed) > client.maxIdleAge || now.Sub(connection.created) > client.maxLifetime {
				client.closeLocked(connection)
				continue
			}
			client.idle[endpoint.Node] = stack
			client.reuses.Add(1)
			client.mu.Unlock()
			return connection, nil
		}
		client.idle[endpoint.Node] = stack
		if client.total < client.maxConnections && client.perEndpoint[endpoint.Node] < client.maxPerEndpoint {
			client.total++
			client.perEndpoint[endpoint.Node]++
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
				client.total--
				client.perEndpoint[endpoint.Node]--
				if client.perEndpoint[endpoint.Node] == 0 {
					delete(client.perEndpoint, endpoint.Node)
				}
				client.signalLocked()
				client.mu.Unlock()
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
				client.total--
				client.perEndpoint[endpoint.Node]--
				if client.perEndpoint[endpoint.Node] == 0 {
					delete(client.perEndpoint, endpoint.Node)
				}
				client.signalLocked()
				client.mu.Unlock()
				return nil, err
			}
			client.dials.Add(1)
			return &pooledReplicatedConn{conn: authenticated, endpoint: endpoint, created: now, lastUsed: now, generation: generation}, nil
		}
		if client.waiters >= client.maxWaiters {
			client.rejected.Add(1)
			client.mu.Unlock()
			return nil, ErrReplicatedTransportBound
		}
		wake := client.wake
		client.waiters++
		client.mu.Unlock()
		select {
		case <-ctx.Done():
			client.mu.Lock()
			client.waiters--
			client.mu.Unlock()
			return nil, context.Cause(ctx)
		case <-wake:
			client.mu.Lock()
			client.waiters--
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
	if !healthy || connection.generation != client.generation || now.Sub(connection.created) > client.maxLifetime ||
		len(client.idle[connection.endpoint.Node]) >= client.maxIdlePerEndpoint {
		client.closeLocked(connection)
		client.mu.Unlock()
		if !healthy {
			client.poisoned.Add(1)
		}
		return
	}
	connection.lastUsed = now
	client.idle[connection.endpoint.Node] = append(client.idle[connection.endpoint.Node], connection)
	client.signalLocked()
	client.mu.Unlock()
}

func (client *AuthenticatedReplicatedClient) DoReplicated(ctx context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	connection, err := client.acquire(ctx, endpoint)
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
	for node, stack := range client.idle {
		for _, connection := range stack {
			client.closeLocked(connection)
		}
		delete(client.idle, node)
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
	for node, stack := range client.idle {
		for _, connection := range stack {
			client.closeLocked(connection)
		}
		delete(client.idle, node)
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
	idle := 0
	for _, stack := range client.idle {
		idle += len(stack)
	}
	stats := AuthenticatedReplicatedClientStats{Connections: client.total, Idle: idle, Waiters: client.waiters, Generation: client.generation}
	client.mu.Unlock()
	stats.Dials, stats.Reuses = client.dials.Load(), client.reuses.Load()
	stats.Poisoned, stats.Rejected = client.poisoned.Load(), client.rejected.Load()
	return stats
}
