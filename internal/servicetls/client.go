package servicetls

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type RawDial func(context.Context, string) (net.Conn, error)

type Endpoint struct {
	Address string
	Node    rafttransport.NodeID
}

type ClientOptions struct {
	TLS               *rafttransport.PeerTLS
	Class             rafttransport.TrafficClass
	Endpoints         []Endpoint
	Dial              RawDial
	HandshakeDeadline rafttransport.DeadlineFunc
	MaxConnections    int
	MaxHandshakes     int
}

type clientConnection struct {
	rafttransport.PeerConnection
	owner *Client
	once  sync.Once
}

func (connection *clientConnection) Close() error {
	if connection == nil {
		return nil
	}
	err := connection.PeerConnection.Close()
	connection.once.Do(func() { connection.owner.release(connection) })
	return err
}

// Client is a bounded, rotation-safe authenticated dial capability. Returned
// streams retain a connection slot until Close, so an outer protocol pool may
// reuse them without weakening the global bound.
type Client struct {
	mu          sync.Mutex
	tls         *rafttransport.PeerTLS
	class       rafttransport.TrafficClass
	endpoints   map[string]rafttransport.NodeID
	dial        RawDial
	deadline    rafttransport.DeadlineFunc
	generation  uint64
	closed      bool
	active      map[*clientConnection]struct{}
	connections chan struct{}
	handshakes  chan struct{}

	authenticated atomic.Uint64
	rejected      atomic.Uint64
}

type ClientStats struct {
	Authenticated, Rejected uint64
	Generation              uint64
	Active                  int
}

func endpointMap(endpoints []Endpoint) (map[string]rafttransport.NodeID, error) {
	if len(endpoints) == 0 || len(endpoints) > AbsoluteMaxIdentities {
		return nil, ErrInvalidProfile
	}
	result := make(map[string]rafttransport.NodeID, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint.Address == "" || endpoint.Node == (rafttransport.NodeID{}) {
			return nil, ErrInvalidProfile
		}
		address := strings.Clone(endpoint.Address)
		if _, exists := result[address]; exists {
			return nil, ErrInvalidProfile
		}
		result[address] = endpoint.Node
	}
	return result, nil
}

func NewClient(options ClientOptions) (*Client, error) {
	endpoints, err := endpointMap(options.Endpoints)
	if options.TLS == nil || options.TLS.LocalIdentity().Node == (rafttransport.NodeID{}) ||
		options.Dial == nil || options.HandshakeDeadline == nil ||
		options.MaxConnections <= 0 || options.MaxConnections > AbsoluteMaxConnections ||
		options.MaxHandshakes <= 0 || options.MaxHandshakes > options.MaxConnections || err != nil {
		return nil, errors.Join(ErrInvalidProfile, err)
	}
	// An arbitrary/unsupported ALPN is rejected before publication.
	if _, err = options.TLS.ClientConfig(options.Endpoints[0].Node, options.Class); err != nil {
		return nil, errors.Join(ErrInvalidProfile, err)
	}
	return &Client{tls: options.TLS, class: options.Class, endpoints: endpoints,
		dial: options.Dial, deadline: options.HandshakeDeadline, generation: 1,
		active: make(map[*clientConnection]struct{}), connections: make(chan struct{}, options.MaxConnections),
		handshakes: make(chan struct{}, options.MaxHandshakes)}, nil
}

func (client *Client) release(connection *clientConnection) {
	client.mu.Lock()
	if _, exists := client.active[connection]; exists {
		delete(client.active, connection)
		<-client.connections
	}
	client.mu.Unlock()
}

// Dial authenticates exactly the configured endpoint identity. Endpoint lookup
// happens only on a cold physical dial; pooled protocol round trips retain no
// identity strings.
func (client *Client) Dial(ctx context.Context, address string) (net.Conn, error) {
	if client == nil || ctx == nil {
		return nil, ErrInvalidProfile
	}
	client.mu.Lock()
	expected, found := client.endpoints[address]
	if client.closed || !found {
		client.mu.Unlock()
		return nil, ErrUnauthorized
	}
	profile, generation := client.tls, client.generation
	client.mu.Unlock()
	select {
	case client.connections <- struct{}{}:
	default:
		client.rejected.Add(1)
		return nil, ErrBound
	}
	owned := false
	defer func() {
		if !owned {
			<-client.connections
		}
	}()
	select {
	case client.handshakes <- struct{}{}:
	default:
		client.rejected.Add(1)
		return nil, ErrBound
	}
	raw, err := client.dial(ctx, address)
	if err == nil && raw == nil {
		err = ErrInvalidProfile
	}
	if err != nil {
		<-client.handshakes
		return nil, err
	}
	connection, err := profile.Client(ctx, raw, expected, client.class, client.deadline)
	<-client.handshakes
	if err != nil {
		return nil, err
	}
	tracked := &clientConnection{PeerConnection: connection, owner: client}
	client.mu.Lock()
	if client.closed || client.generation != generation || client.endpoints[address] != expected {
		client.mu.Unlock()
		_ = connection.Close()
		return nil, ErrUnauthorized
	}
	client.active[tracked] = struct{}{}
	client.authenticated.Add(1)
	client.mu.Unlock()
	owned = true
	return tracked, nil
}

// Rotate publishes credentials and endpoint identities as one generation and
// revokes every pooled or checked-out old-generation stream.
func (client *Client) Rotate(profile *rafttransport.PeerTLS, endpoints []Endpoint) error {
	next, err := endpointMap(endpoints)
	if client == nil || profile == nil || err != nil {
		return ErrInvalidProfile
	}
	if _, err = profile.ClientConfig(endpoints[0].Node, client.class); err != nil {
		return errors.Join(ErrInvalidProfile, err)
	}
	client.mu.Lock()
	if client.closed || profile.LocalIdentity() != client.tls.LocalIdentity() {
		client.mu.Unlock()
		return ErrInvalidProfile
	}
	client.tls, client.endpoints = profile, next
	client.generation++
	retired := make([]*clientConnection, 0, len(client.active))
	for connection := range client.active {
		retired = append(retired, connection)
	}
	client.mu.Unlock()
	for _, connection := range retired {
		_ = connection.Close()
	}
	return nil
}

func (client *Client) Close() error {
	if client == nil {
		return ErrInvalidProfile
	}
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return nil
	}
	client.closed = true
	client.generation++
	retired := make([]*clientConnection, 0, len(client.active))
	for connection := range client.active {
		retired = append(retired, connection)
	}
	client.mu.Unlock()
	for _, connection := range retired {
		_ = connection.Close()
	}
	return nil
}

func (client *Client) Stats() ClientStats {
	if client == nil {
		return ClientStats{}
	}
	client.mu.Lock()
	generation, active := client.generation, len(client.active)
	client.mu.Unlock()
	return ClientStats{Authenticated: client.authenticated.Load(), Rejected: client.rejected.Load(),
		Generation: generation, Active: active}
}
