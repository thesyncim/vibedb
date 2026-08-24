package shardservice

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

var ErrReplicatedAuthentication = errors.New("shardservice: replicated peer authentication failed")

// ReplicatedServerTLS is the mandatory mTLS capability for a native RF3
// listener. Its allowlist is an immutable set of certificate-bound gateway
// NodeIDs; no subject strings or unauthenticated connection fallback exists.
type ReplicatedServerTLS struct {
	mu                sync.Mutex
	tls               *rafttransport.PeerTLS
	allow             map[rafttransport.NodeID]struct{}
	generation        uint64
	active            map[net.Conn]uint64
	authenticated     atomic.Uint64
	authRejected      atomic.Uint64
	handshakeRejected atomic.Uint64
}

type ReplicatedServerTLSStats struct {
	Authenticated, AuthenticationRejected, HandshakeRejected uint64
	Generation                                               uint64
	Active                                                   int
}

func NewReplicatedServerTLS(profile *rafttransport.PeerTLS, allowed []rafttransport.NodeID) (*ReplicatedServerTLS, error) {
	allow, err := replicatedGatewayAllowlist(allowed)
	if profile == nil || profile.LocalIdentity().Node == (rafttransport.NodeID{}) || err != nil {
		return nil, ErrReplicatedAuthentication
	}
	return &ReplicatedServerTLS{tls: profile, allow: allow, generation: 1, active: make(map[net.Conn]uint64)}, nil
}

func replicatedGatewayAllowlist(nodes []rafttransport.NodeID) (map[rafttransport.NodeID]struct{}, error) {
	if len(nodes) == 0 || len(nodes) > AbsoluteMaxReplicatedConnections {
		return nil, ErrReplicatedAuthentication
	}
	result := make(map[rafttransport.NodeID]struct{}, len(nodes))
	for _, node := range nodes {
		if node == (rafttransport.NodeID{}) {
			return nil, ErrReplicatedAuthentication
		}
		if _, exists := result[node]; exists {
			return nil, ErrReplicatedAuthentication
		}
		result[node] = struct{}{}
	}
	return result, nil
}

func (capability *ReplicatedServerTLS) snapshot() (*rafttransport.PeerTLS, uint64) {
	capability.mu.Lock()
	defer capability.mu.Unlock()
	return capability.tls, capability.generation
}

func (capability *ReplicatedServerTLS) admit(connection rafttransport.PeerConnection, generation uint64) bool {
	if connection == nil || connection.TrafficClass() != rafttransport.TrafficShardNative {
		return false
	}
	identity := connection.PeerIdentity()
	capability.mu.Lock()
	defer capability.mu.Unlock()
	if generation != capability.generation {
		return false
	}
	if _, allowed := capability.allow[identity.Node]; !allowed {
		return false
	}
	capability.active[connection] = generation
	return true
}

func (capability *ReplicatedServerTLS) release(connection net.Conn) {
	capability.mu.Lock()
	delete(capability.active, connection)
	capability.mu.Unlock()
}

func (capability *ReplicatedServerTLS) closeActive() {
	capability.mu.Lock()
	for connection := range capability.active {
		_ = connection.Close()
		delete(capability.active, connection)
	}
	capability.mu.Unlock()
}

// Rotate publishes a new TLS profile and allowlist, then closes every stream
// authenticated under the old generation. In-flight proposals consequently
// retain the protocol's conservative outcome-unknown semantics.
func (capability *ReplicatedServerTLS) Rotate(profile *rafttransport.PeerTLS, allowed []rafttransport.NodeID) error {
	allow, err := replicatedGatewayAllowlist(allowed)
	if capability == nil || profile == nil || profile.LocalIdentity().Node == (rafttransport.NodeID{}) || err != nil {
		return ErrReplicatedAuthentication
	}
	capability.mu.Lock()
	if profile.LocalIdentity() != capability.tls.LocalIdentity() {
		capability.mu.Unlock()
		return ErrReplicatedAuthentication
	}
	capability.tls, capability.allow = profile, allow
	capability.generation++
	for connection := range capability.active {
		_ = connection.Close()
		delete(capability.active, connection)
	}
	capability.mu.Unlock()
	return nil
}

func (capability *ReplicatedServerTLS) Stats() ReplicatedServerTLSStats {
	if capability == nil {
		return ReplicatedServerTLSStats{}
	}
	capability.mu.Lock()
	generation, active := capability.generation, len(capability.active)
	capability.mu.Unlock()
	return ReplicatedServerTLSStats{Authenticated: capability.authenticated.Load(), AuthenticationRejected: capability.authRejected.Load(), HandshakeRejected: capability.handshakeRejected.Load(), Generation: generation, Active: active}
}

// ServeAuthenticated is the production RF3 listener. Raw connection and TLS
// handshake slots are both immediate hard bounds; excess sockets are closed.
func (server *ReplicatedServer) ServeAuthenticated(ctx context.Context, listener net.Listener, capability *ReplicatedServerTLS, handshakeDeadline rafttransport.DeadlineFunc, maxConnections, maxHandshakes int) error {
	if server == nil || server.owner == nil || ctx == nil || listener == nil || capability == nil || handshakeDeadline == nil ||
		maxConnections <= 0 || maxConnections > AbsoluteMaxReplicatedConnections || maxHandshakes <= 0 || maxHandshakes > maxConnections ||
		!server.state.CompareAndSwap(replicatedServerReady, replicatedServerRunning) {
		return ErrReplicatedWire
	}
	stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stop()
	defer server.state.Store(replicatedServerClosed)
	connectionSlots := make(chan struct{}, maxConnections)
	handshakeSlots := make(chan struct{}, maxHandshakes)
	var workers sync.WaitGroup
	defer func() { _ = listener.Close(); capability.closeActive(); workers.Wait() }()
	for {
		raw, err := listener.Accept()
		if err != nil {
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
			return err
		}
		select {
		case connectionSlots <- struct{}{}:
		default:
			server.rejected.Add(1)
			_ = raw.Close()
			continue
		}
		select {
		case handshakeSlots <- struct{}{}:
		default:
			<-connectionSlots
			capability.handshakeRejected.Add(1)
			_ = raw.Close()
			continue
		}
		server.accepted.Add(1)
		server.active.Add(1)
		workers.Add(1)
		go func(raw net.Conn) {
			defer workers.Done()
			defer func() { <-connectionSlots; server.active.Add(^uint64(0)) }()
			profile, generation := capability.snapshot()
			connection, err := profile.Server(ctx, raw, rafttransport.TrafficShardNative, handshakeDeadline)
			<-handshakeSlots
			if err != nil {
				capability.authRejected.Add(1)
				server.failed.Add(1)
				return
			}
			if !capability.admit(connection, generation) {
				capability.authRejected.Add(1)
				_ = connection.Close()
				return
			}
			capability.authenticated.Add(1)
			defer capability.release(connection)
			defer connection.Close()
			if err := server.ServeReplicatedConn(ctx, connection); err != nil && context.Cause(ctx) == nil {
				server.failed.Add(1)
			}
		}(raw)
	}
}
