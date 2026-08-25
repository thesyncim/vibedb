// Package servicetls turns the certificate-bound rafttransport identity
// foundation into a bounded application-service listener. It deliberately
// carries only fixed binary principals; protocol payloads and authorization
// metadata never enter this layer.
package servicetls

import (
	"bytes"
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

var (
	ErrInvalidProfile = errors.New("servicetls: invalid profile")
	ErrUnauthorized   = errors.New("servicetls: peer is not authorized")
	ErrBound          = errors.New("servicetls: connection bound exceeded")
)

const (
	AbsoluteMaxConnections = 65536
	AbsoluteMaxIdentities  = 65536
)

// NodeAuthorizer is an immutable, compact allowlist of exact certificate Node
// identities. Sorting makes lookup allocation-free and avoids a hash table's
// per-entry space overhead. The trust domain is already verified by PeerTLS.
type NodeAuthorizer struct {
	nodes []rafttransport.NodeID
}

func NewNodeAuthorizer(nodes []rafttransport.NodeID) (*NodeAuthorizer, error) {
	if len(nodes) == 0 || len(nodes) > AbsoluteMaxIdentities {
		return nil, ErrInvalidProfile
	}
	owned := slices.Clone(nodes)
	for _, node := range owned {
		if node == (rafttransport.NodeID{}) {
			return nil, ErrInvalidProfile
		}
	}
	slices.SortFunc(owned, compareNode)
	for index := 1; index < len(owned); index++ {
		if owned[index] == owned[index-1] {
			return nil, ErrInvalidProfile
		}
	}
	return &NodeAuthorizer{nodes: owned}, nil
}

func compareNode(left, right rafttransport.NodeID) int {
	return bytes.Compare(left[:], right[:])
}

func (authorizer *NodeAuthorizer) allows(identity rafttransport.PeerIdentity) bool {
	if authorizer == nil {
		return false
	}
	_, found := slices.BinarySearchFunc(authorizer.nodes, identity.Node, compareNode)
	return found
}

// Limits bound accepted sockets before a goroutine or TLS handshake can be
// retained. HandshakeDeadline must return a nonzero absolute deadline.
type Limits struct {
	MaxConnections    int
	MaxHandshakes     int
	HandshakeDeadline rafttransport.DeadlineFunc
}

func (limits Limits) valid() bool {
	return limits.MaxConnections > 0 && limits.MaxConnections <= AbsoluteMaxConnections &&
		limits.MaxHandshakes > 0 && limits.MaxHandshakes <= limits.MaxConnections &&
		limits.HandshakeDeadline != nil
}

// Handler synchronously owns one authenticated stream for the duration of the
// call. Serve closes it after return, including on rotation or shutdown.
type Handler func(context.Context, rafttransport.PeerConnection)

type trackedConnection struct {
	rafttransport.PeerConnection
	generation uint64
}

type admissionGenerationKey struct{}

// AdmissionGeneration returns the immutable TLS/allowlist generation that
// admitted this stream. Higher layers use it to bind adjacent policy state to
// the same publication and reject a stream retired between admission and handoff.
func AdmissionGeneration(ctx context.Context) (uint64, bool) {
	if ctx == nil {
		return 0, false
	}
	generation, ok := ctx.Value(admissionGenerationKey{}).(uint64)
	return generation, ok && generation != 0
}

// Server is a rotation-safe TLS capability. Rotate publishes credentials and
// authorization together and closes every stream from the retired generation.
type Server struct {
	mu         sync.Mutex
	tls        *rafttransport.PeerTLS
	class      rafttransport.TrafficClass
	authorizer *NodeAuthorizer
	generation uint64
	active     map[*trackedConnection]struct{}

	authenticated atomic.Uint64
	authRejected  atomic.Uint64
	overloaded    atomic.Uint64
}

type Stats struct {
	Authenticated, AuthenticationRejected, Overloaded uint64
	Generation                                        uint64
	Active                                            int
}

func NewServer(profile *rafttransport.PeerTLS, class rafttransport.TrafficClass, authorizer *NodeAuthorizer) (*Server, error) {
	if profile == nil || profile.LocalIdentity().Node == (rafttransport.NodeID{}) || authorizer == nil {
		return nil, ErrInvalidProfile
	}
	// Constructing the config proves class is a supported, isolated ALPN before
	// the capability can be published. The detached config is then discarded.
	if _, err := profile.ServerConfig(class); err != nil {
		return nil, errors.Join(ErrInvalidProfile, err)
	}
	return &Server{tls: profile, class: class, authorizer: authorizer, generation: 1,
		active: make(map[*trackedConnection]struct{})}, nil
}

func (server *Server) snapshot() (*rafttransport.PeerTLS, *NodeAuthorizer, uint64, error) {
	if server == nil {
		return nil, nil, 0, ErrInvalidProfile
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.tls, server.authorizer, server.generation, nil
}

func (server *Server) admit(connection rafttransport.PeerConnection, authorizer *NodeAuthorizer, generation uint64) (*trackedConnection, error) {
	if connection == nil || connection.TrafficClass() != server.class || !authorizer.allows(connection.PeerIdentity()) {
		return nil, ErrUnauthorized
	}
	tracked := &trackedConnection{PeerConnection: connection, generation: generation}
	server.mu.Lock()
	defer server.mu.Unlock()
	if generation != server.generation || authorizer != server.authorizer {
		return nil, ErrUnauthorized
	}
	server.active[tracked] = struct{}{}
	return tracked, nil
}

func (server *Server) release(connection *trackedConnection) {
	server.mu.Lock()
	delete(server.active, connection)
	server.mu.Unlock()
}

// Rotate atomically changes both credentials and authorization. The local
// binary identity is stable across certificate renewal; changing it requires a
// new listener capability. Existing generation streams are revoked eagerly.
func (server *Server) Rotate(profile *rafttransport.PeerTLS, authorizer *NodeAuthorizer) error {
	if server == nil || profile == nil || authorizer == nil ||
		profile.LocalIdentity().Node == (rafttransport.NodeID{}) {
		return ErrInvalidProfile
	}
	if _, err := profile.ServerConfig(server.class); err != nil {
		return errors.Join(ErrInvalidProfile, err)
	}
	server.mu.Lock()
	if profile.LocalIdentity() != server.tls.LocalIdentity() {
		server.mu.Unlock()
		return ErrInvalidProfile
	}
	server.tls, server.authorizer = profile, authorizer
	server.generation++
	retired := make([]*trackedConnection, 0, len(server.active))
	for connection := range server.active {
		retired = append(retired, connection)
		delete(server.active, connection)
	}
	server.mu.Unlock()
	for _, connection := range retired {
		_ = connection.Close()
	}
	return nil
}

func (server *Server) Stats() Stats {
	if server == nil {
		return Stats{}
	}
	server.mu.Lock()
	generation, active := server.generation, len(server.active)
	server.mu.Unlock()
	return Stats{Authenticated: server.authenticated.Load(),
		AuthenticationRejected: server.authRejected.Load(), Overloaded: server.overloaded.Load(),
		Generation: generation, Active: active}
}

// Serve accepts, authenticates, authorizes, and synchronously hands bounded
// streams to handler. Raw sockets are bounded before goroutine creation, and
// TLS work has an independent smaller bound.
func (server *Server) Serve(ctx context.Context, listener net.Listener, limits Limits, handler Handler) error {
	if server == nil || ctx == nil || listener == nil || !limits.valid() || handler == nil {
		if listener != nil {
			_ = listener.Close()
		}
		return ErrInvalidProfile
	}
	stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stop()
	connectionSlots := make(chan struct{}, limits.MaxConnections)
	handshakeSlots := make(chan struct{}, limits.MaxHandshakes)
	var workers sync.WaitGroup
	defer func() {
		_ = listener.Close()
		workers.Wait()
	}()
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
			server.overloaded.Add(1)
			_ = raw.Close()
			continue
		}
		select {
		case handshakeSlots <- struct{}{}:
		default:
			<-connectionSlots
			server.overloaded.Add(1)
			_ = raw.Close()
			continue
		}
		workers.Add(1)
		go func(raw net.Conn) {
			defer workers.Done()
			defer func() { <-connectionSlots }()
			profile, authorizer, generation, err := server.snapshot()
			if err != nil {
				<-handshakeSlots
				_ = raw.Close()
				return
			}
			connection, err := profile.Server(ctx, raw, server.class, limits.HandshakeDeadline)
			<-handshakeSlots
			if err != nil {
				server.authRejected.Add(1)
				return
			}
			tracked, err := server.admit(connection, authorizer, generation)
			if err != nil {
				server.authRejected.Add(1)
				_ = connection.Close()
				return
			}
			server.authenticated.Add(1)
			defer server.release(tracked)
			defer tracked.Close()
			stopConnection := context.AfterFunc(ctx, func() { _ = tracked.Close() })
			defer stopConnection()
			handler(context.WithValue(ctx, admissionGenerationKey{}, generation), tracked)
		}(raw)
	}
}

// FixedDeadline returns a nonzero absolute handshake deadline without putting
// duration arithmetic into callers' accept loops.
func FixedDeadline(timeout time.Duration) rafttransport.DeadlineFunc {
	if timeout <= 0 {
		return nil
	}
	return func() time.Time { return time.Now().Add(timeout) }
}
