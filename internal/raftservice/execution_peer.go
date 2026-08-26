package raftservice

import (
	"context"
	"errors"
	"net"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

// AuthenticatedExecutionPeerOptions composes all execution-lane owners with a
// single per-peer transport and authenticated listener. Nested transport
// injection points must be nil; this constructor owns the complete wiring.
type AuthenticatedExecutionPeerOptions struct {
	Registry          *rafttransport.StaticRegistry
	TLS               *rafttransport.PeerTLS
	Dial              rafttransport.RawPeerDialFunc
	Listener          net.Listener
	Execution         ExecutionOptions
	Transport         rafttransport.OrdinaryTransportOptions
	Receiver          rafttransport.OrdinaryReceiverOptions
	HandshakeDeadline rafttransport.DeadlineFunc
	MaxInboundStreams int
}

type AuthenticatedExecutionPeerRuntime struct {
	owners    *ExecutionOwners
	transport *rafttransport.OrdinaryTransport
	server    *PeerServer
	state     atomic.Uint32
	started   chan struct{}
	done      chan struct{}
}

func NewAuthenticatedExecutionPeerRuntime(options AuthenticatedExecutionPeerOptions) (*AuthenticatedExecutionPeerRuntime, error) {
	if options.TLS == nil {
		return nil, ErrInvalidPeerServer
	}
	identity := options.TLS.LocalIdentity()
	if options.Registry == nil || options.Dial == nil || options.Listener == nil ||
		options.HandshakeDeadline == nil || options.Execution.Outbound != nil ||
		options.Execution.MembershipAuthority != nil || options.Transport.Registry != nil ||
		options.Transport.Dialer != nil || options.Receiver.Registry != nil ||
		options.Receiver.Handle != nil || identity.Node != options.Registry.LocalNode() ||
		identity.TrustDomain != options.Registry.TrustDomain() {
		return nil, ErrInvalidPeerServer
	}
	for index, authorization := range options.Execution.MembershipAuthorizations {
		if index >= len(options.Execution.Members) {
			return nil, ErrInvalidPeerServer
		}
		if err := options.Registry.AuthorizeTransition(rafttransport.TransitionGrant{
			Group: options.Execution.Members[index].Group, TransitionID: authorization.TransitionID,
			MetadataEpoch: authorization.MetadataEpoch, CatalogGeneration: authorization.CatalogGeneration,
			SourceMember: authorization.SourceMember, TargetMember: authorization.TargetMember,
		}); err != nil {
			return nil, err
		}
	}
	transportOptions := options.Transport
	transportOptions.Registry = options.Registry
	transportOptions.Dialer = rafttransport.TLSOrdinaryDialer{TLS: options.TLS, Dial: options.Dial, HandshakeDeadline: options.HandshakeDeadline}
	transport, err := rafttransport.NewOrdinaryTransport(transportOptions)
	if err != nil {
		return nil, err
	}
	executionOptions := options.Execution
	executionOptions.Outbound = transport
	if len(executionOptions.MembershipAuthorizations) != 0 {
		executionOptions.MembershipAuthority = options.Registry
	}
	owners, err := NewExecutionOwners(executionOptions)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	receiverOptions := options.Receiver
	receiverOptions.Registry = options.Registry
	receiverOptions.Handle = owners.HandleInbound
	receiver, err := rafttransport.NewOrdinaryReceiver(receiverOptions)
	if err != nil {
		_ = transport.Close()
		_ = executionOptions.Lanes.Close()
		return nil, err
	}
	server, err := NewPeerServer(PeerServerOptions{
		Listener: options.Listener, TLS: options.TLS, Receiver: receiver,
		HandshakeDeadline: options.HandshakeDeadline, MaxStreams: options.MaxInboundStreams,
	})
	if err != nil {
		_ = transport.Close()
		_ = executionOptions.Lanes.Close()
		return nil, err
	}
	return &AuthenticatedExecutionPeerRuntime{
		owners: owners, transport: transport, server: server,
		started: make(chan struct{}), done: make(chan struct{}),
	}, nil
}

// Run starts the one shared transport before every owner loop and the shared
// listener. Any component failure cancels and joins the whole composition.
func (runtime *AuthenticatedExecutionPeerRuntime) Run(parent context.Context) error {
	if runtime == nil || parent == nil || !runtime.state.CompareAndSwap(peerRuntimeReady, peerRuntimeRunning) {
		return ErrPeerServerClosed
	}
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(ErrPeerServerClosed)
	defer close(runtime.done)
	defer runtime.state.Store(peerRuntimeClosed)
	results := make(chan peerComponentResult, 3)
	go func() { results <- peerComponentResult{kind: 1, err: runtime.transport.Run(ctx)} }()
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
	go func() { results <- peerComponentResult{kind: 2, err: runtime.owners.Run(ctx)} }()
	go func() { results <- peerComponentResult{kind: 3, err: runtime.server.Run(ctx)} }()
	select {
	case <-runtime.owners.Started():
	case <-ctx.Done():
	}
	select {
	case <-runtime.server.Started():
	case <-ctx.Done():
	}
	close(runtime.started)
	if !runtime.owners.Running() || !runtime.server.Running() {
		cancel(ErrPeerServerClosed)
	}
	first := <-results
	cause := first.err
	if cause == nil {
		cause = ErrPeerServerClosed
	}
	cancel(cause)
	_ = runtime.server.Close()
	_ = runtime.transport.Close()
	joined := cause
	for completed := 1; completed < 3; completed++ {
		result := <-results
		if result.err != nil && !errors.Is(result.err, cause) {
			joined = errors.Join(joined, result.err)
		}
	}
	return joined
}

func (runtime *AuthenticatedExecutionPeerRuntime) Owners() *ExecutionOwners {
	if runtime == nil {
		return nil
	}
	return runtime.owners
}
func (runtime *AuthenticatedExecutionPeerRuntime) Started() <-chan struct{} {
	if runtime == nil {
		return nil
	}
	return runtime.started
}
func (runtime *AuthenticatedExecutionPeerRuntime) Done() <-chan struct{} {
	if runtime == nil {
		return nil
	}
	return runtime.done
}
func (runtime *AuthenticatedExecutionPeerRuntime) Running() bool {
	return runtime != nil && runtime.state.Load() == peerRuntimeRunning && runtime.transport.Running() && runtime.server.Running() && runtime.owners.Running()
}
