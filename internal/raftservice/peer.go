package raftservice

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

var (
	// ErrInvalidPeerServer reports an incomplete or unbounded authenticated
	// listener/runtime composition.
	ErrInvalidPeerServer = errors.New("raftservice: invalid authenticated peer server")
	// ErrPeerServerClosed reports retirement of the authenticated peer listener.
	ErrPeerServerClosed = errors.New("raftservice: authenticated peer server is closed")
)

const AbsoluteMaxInboundPeerStreams = rafttransport.AbsoluteMaxTransportPeers * 2

const (
	peerServerReady uint32 = iota
	peerServerRunning
	peerServerClosed
)

// PeerServerOptions configures a TLS-only ordinary-Raft ingress listener.
// MaxStreams is an exact concurrency bound. Each stream owns at most one
// receiver frame at a time, so its frame memory is additionally bounded by
// MaxStreams*rafttransport.MaxFrameBytes.
type PeerServerOptions struct {
	Listener          net.Listener
	TLS               *rafttransport.PeerTLS
	Receiver          *rafttransport.OrdinaryReceiver
	HandshakeDeadline rafttransport.DeadlineFunc
	MaxStreams        int
}

// PeerServerStats is a detached snapshot of listener activity. Rejected is
// incremented when the exact active-stream bound is full. Failed counts only
// accepted streams that fail authentication, framing, admission, or handling.
type PeerServerStats struct {
	Accepted uint64
	Rejected uint64
	Failed   uint64
	Active   uint64
}

// PeerServer accepts only mutually authenticated ordinary-Raft streams. It
// does not queue connections in user space: an accepted connection either
// acquires one fixed slot or is closed immediately.
type PeerServer struct {
	listener          net.Listener
	peerTLS           *rafttransport.PeerTLS
	receiver          *rafttransport.OrdinaryReceiver
	handshakeDeadline rafttransport.DeadlineFunc
	slots             chan struct{}

	state   atomic.Uint32
	started chan struct{}
	ctx     context.Context
	cancel  context.CancelCauseFunc

	accepted atomic.Uint64
	rejected atomic.Uint64
	failed   atomic.Uint64
	active   atomic.Uint64
}

// NewPeerServer validates one owned authenticated listener. Ownership of the
// listener transfers only on success.
func NewPeerServer(options PeerServerOptions) (*PeerServer, error) {
	if options.Listener == nil || options.TLS == nil || options.Receiver == nil ||
		options.HandshakeDeadline == nil || options.MaxStreams <= 0 ||
		options.MaxStreams > AbsoluteMaxInboundPeerStreams {
		return nil, ErrInvalidPeerServer
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	return &PeerServer{
		listener: options.Listener, peerTLS: options.TLS, receiver: options.Receiver,
		handshakeDeadline: options.HandshakeDeadline,
		slots:             make(chan struct{}, options.MaxStreams), started: make(chan struct{}),
		ctx: ctx, cancel: cancel,
	}, nil
}

// Run owns the accept loop until parent cancellation, Close, or a terminal
// listener failure. A bad authenticated stream is isolated to that stream.
func (server *PeerServer) Run(parent context.Context) error {
	if server == nil || parent == nil ||
		!server.state.CompareAndSwap(peerServerReady, peerServerRunning) {
		return ErrPeerServerClosed
	}
	close(server.started)
	stopParent := context.AfterFunc(parent, func() {
		cause := context.Cause(parent)
		if cause == nil {
			cause = context.Canceled
		}
		server.cancel(cause)
		_ = server.listener.Close()
	})
	defer stopParent()
	if cause := context.Cause(parent); cause != nil {
		server.cancel(cause)
		_ = server.listener.Close()
	}

	var streams sync.WaitGroup
	defer func() {
		server.state.Store(peerServerClosed)
		server.cancel(ErrPeerServerClosed)
		_ = server.listener.Close()
		streams.Wait()
	}()

	for {
		raw, err := server.listener.Accept()
		if err != nil {
			if cause := context.Cause(server.ctx); cause != nil {
				return cause
			}
			server.cancel(err)
			return err
		}
		select {
		case server.slots <- struct{}{}:
			server.accepted.Add(1)
			server.active.Add(1)
			streams.Add(1)
			go func(connection net.Conn) {
				defer streams.Done()
				defer func() {
					<-server.slots
					server.active.Add(^uint64(0))
				}()
				if serveErr := server.receiver.ServeTLS(
					server.ctx, connection, server.peerTLS, server.handshakeDeadline,
				); serveErr != nil && context.Cause(server.ctx) == nil {
					server.failed.Add(1)
				}
			}(raw)
		default:
			server.rejected.Add(1)
			_ = raw.Close()
		}
	}
}

// Close retires the listener and every active authenticated stream. It is
// idempotent; Run performs the bounded join of stream goroutines.
func (server *PeerServer) Close() error {
	if server == nil {
		return nil
	}
	for {
		state := server.state.Load()
		if state == peerServerClosed {
			return nil
		}
		if server.state.CompareAndSwap(state, peerServerClosed) {
			server.cancel(ErrPeerServerClosed)
			if state == peerServerReady {
				close(server.started)
			}
			return server.listener.Close()
		}
	}
}

// Started closes after Run acquires the listener or Close retires it first.
func (server *PeerServer) Started() <-chan struct{} {
	if server == nil {
		return nil
	}
	return server.started
}

// Running reports whether the listener currently owns its accept loop.
func (server *PeerServer) Running() bool {
	return server != nil && server.state.Load() == peerServerRunning &&
		context.Cause(server.ctx) == nil
}

// Stats returns an allocation-free detached counter snapshot.
func (server *PeerServer) Stats() PeerServerStats {
	if server == nil {
		return PeerServerStats{}
	}
	return PeerServerStats{
		Accepted: server.accepted.Load(), Rejected: server.rejected.Load(),
		Failed: server.failed.Load(), Active: server.active.Load(),
	}
}

const (
	peerRuntimeReady uint32 = iota
	peerRuntimeRunning
	peerRuntimeClosed
)

// AuthenticatedPeerOptions composes the only serving owner lane with both
// directions of the authenticated ordinary-message transport. Registry and
// handlers are injected by the constructor and therefore must be nil in the
// nested transport profiles.
type AuthenticatedPeerOptions struct {
	Registry          *rafttransport.StaticRegistry
	TLS               *rafttransport.PeerTLS
	Dial              rafttransport.RawPeerDialFunc
	Listener          net.Listener
	Owner             Options
	Transport         rafttransport.OrdinaryTransportOptions
	Receiver          rafttransport.OrdinaryReceiverOptions
	HandshakeDeadline rafttransport.DeadlineFunc
	MaxInboundStreams int
}

// AuthenticatedPeerRuntime owns one Host lane, a bounded authenticated outbox,
// and a bounded authenticated listener. It deliberately has no snapshot or
// learner path; those require a separately budgeted traffic capability.
type AuthenticatedPeerRuntime struct {
	owner     *Owner
	transport *rafttransport.OrdinaryTransport
	server    *PeerServer

	state   atomic.Uint32
	started chan struct{}
	done    chan struct{}
}

// NewAuthenticatedPeerRuntime constructs one fail-closed serving composition.
func NewAuthenticatedPeerRuntime(options AuthenticatedPeerOptions) (*AuthenticatedPeerRuntime, error) {
	identity := options.TLS.LocalIdentity()
	if options.Registry == nil || options.TLS == nil || options.Dial == nil ||
		options.Listener == nil || options.HandshakeDeadline == nil ||
		options.Owner.Outbound != nil || options.Transport.Registry != nil ||
		options.Owner.MembershipAuthority != nil ||
		options.Transport.Dialer != nil || options.Receiver.Registry != nil ||
		options.Receiver.Handle != nil || identity.Node != options.Registry.LocalNode() ||
		identity.TrustDomain != options.Registry.TrustDomain() {
		return nil, ErrInvalidPeerServer
	}
	transportOptions := options.Transport
	transportOptions.Registry = options.Registry
	transportOptions.Dialer = rafttransport.TLSOrdinaryDialer{
		TLS: options.TLS, Dial: options.Dial,
		HandshakeDeadline: options.HandshakeDeadline,
	}
	transport, err := rafttransport.NewOrdinaryTransport(transportOptions)
	if err != nil {
		return nil, err
	}

	ownerOptions := options.Owner
	ownerOptions.Outbound = transport
	ownerOptions.MembershipAuthority = options.Registry
	owner, err := NewOwner(ownerOptions)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}

	receiverOptions := options.Receiver
	receiverOptions.Registry = options.Registry
	receiverOptions.Handle = owner.HandleInbound
	receiver, err := rafttransport.NewOrdinaryReceiver(receiverOptions)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	server, err := NewPeerServer(PeerServerOptions{
		Listener: options.Listener, TLS: options.TLS, Receiver: receiver,
		HandshakeDeadline: options.HandshakeDeadline,
		MaxStreams:        options.MaxInboundStreams,
	})
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	return &AuthenticatedPeerRuntime{
		owner: owner, transport: transport, server: server,
		started: make(chan struct{}), done: make(chan struct{}),
	}, nil
}

type peerComponentResult struct {
	kind uint8
	err  error
}

// Run starts the outbound workers before publishing the Owner and listener.
// The first component exit cancels and joins the complete composition.
func (runtime *AuthenticatedPeerRuntime) Run(parent context.Context) error {
	if runtime == nil || parent == nil ||
		!runtime.state.CompareAndSwap(peerRuntimeReady, peerRuntimeRunning) {
		return ErrPeerServerClosed
	}
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(ErrPeerServerClosed)
	defer close(runtime.done)
	defer runtime.state.Store(peerRuntimeClosed)

	results := make(chan peerComponentResult, 3)
	go func() {
		results <- peerComponentResult{kind: 1, err: runtime.transport.Run(ctx)}
	}()
	select {
	case <-runtime.transport.Started():
	case <-ctx.Done():
	}
	if !runtime.transport.Running() {
		cause := context.Cause(ctx)
		if cause == nil {
			cause = ErrPeerServerClosed
		}
		cancel(cause)
	}

	// Owner and listener are always started, even if outbound startup failed.
	// Their canceled startup path deterministically closes the adopted Host and
	// listener; returning early here would leak both owned capabilities.
	go func() { results <- peerComponentResult{kind: 2, err: runtime.owner.Run(ctx)} }()
	go func() { results <- peerComponentResult{kind: 3, err: runtime.server.Run(ctx)} }()
	select {
	case <-runtime.owner.Started():
	case <-ctx.Done():
	}
	select {
	case <-runtime.server.Started():
	case <-ctx.Done():
	}
	close(runtime.started)
	if !runtime.owner.Running() || !runtime.server.Running() {
		cancel(ErrPeerServerClosed)
	}

	first := <-results
	cause := first.err
	if cause == nil {
		cause = ErrPeerServerClosed
	}
	cancel(cause)
	// Both components close their resources when ctx is canceled. Calling
	// Close as well races that cause with their independent "closed" sentinel
	// and can turn an ordinary requested shutdown into a spurious failure.

	joined := cause
	for completed := 1; completed < 3; completed++ {
		result := <-results
		if result.err != nil && !errors.Is(result.err, cause) {
			joined = errors.Join(joined, result.err)
		}
	}
	return joined
}

// Owner returns the sole serialized serving capability. Request listeners may
// call its queueing methods; they never receive the underlying Host.
func (runtime *AuthenticatedPeerRuntime) Owner() *Owner {
	if runtime == nil {
		return nil
	}
	return runtime.owner
}

// TransportStats returns one detached outbound-peer counter snapshot. It is an
// observability capability only: the caller cannot enqueue, drain, or otherwise
// influence transport work through the returned value. SentBytes counts exact
// encoded Raft frame bytes and excludes the four-byte stream record prefix.
func (runtime *AuthenticatedPeerRuntime) TransportStats(
	node rafttransport.NodeID,
) (rafttransport.PeerStats, error) {
	if runtime == nil || runtime.transport == nil {
		return rafttransport.PeerStats{}, rafttransport.ErrNodeNotFound
	}
	return runtime.transport.Stats(node)
}

// InboundStats returns a detached snapshot of the authenticated peer listener.
// It exposes no listener or receiver authority.
func (runtime *AuthenticatedPeerRuntime) InboundStats() PeerServerStats {
	if runtime == nil || runtime.server == nil {
		return PeerServerStats{}
	}
	return runtime.server.Stats()
}

// Started closes when the complete runtime has either published all three
// lanes or failed before publication. Running distinguishes those states.
func (runtime *AuthenticatedPeerRuntime) Started() <-chan struct{} {
	if runtime == nil {
		return nil
	}
	return runtime.started
}

// Running reports whether the complete runtime is currently serving.
func (runtime *AuthenticatedPeerRuntime) Running() bool {
	return runtime != nil && runtime.state.Load() == peerRuntimeRunning &&
		runtime.transport.Running() && runtime.server.Running()
}

// Done closes after all three lanes and active peer streams are joined.
func (runtime *AuthenticatedPeerRuntime) Done() <-chan struct{} {
	if runtime == nil {
		return nil
	}
	return runtime.done
}
