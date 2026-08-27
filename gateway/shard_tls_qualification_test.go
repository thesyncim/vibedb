package gateway

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
)

const authenticatedShardQualificationOperations = 1024

func TestAuthenticatedGatewayShardHotAuthorizationAllocationFree(t *testing.T) {
	gatewayIdentity := gatewayPeerIdentity(59, 10)
	userIdentity := gatewayPeerIdentity(59, 30)
	policy := newShardQualificationPolicy(t, 7, gatewayIdentity.Node, userIdentity.Node)
	gate, err := serviceauthz.NewGate(policy)
	if err != nil {
		t.Fatal(err)
	}
	check := func() {
		if gate.Check(gatewayIdentity.Node, 7, serviceauthz.CapabilityDelegate) != serviceauthz.DecisionAllow ||
			gate.Check(userIdentity.Node, 7, serviceauthz.CapabilityDataRead) != serviceauthz.DecisionAllow {
			panic("authenticated gateway-to-shard authority changed")
		}
	}
	if allocations := testing.AllocsPerRun(authenticatedShardQualificationOperations, check); allocations != 0 {
		t.Fatalf("delegate plus forwarded-principal authorization allocations=%v", allocations)
	}
}

// TestAuthenticatedShardBoundaryRotationAndConfusedDeputyFault proves the
// shipped gateway-to-shard boundary authenticates the service principal and
// independently authorizes the forwarded principal. Policy and certificate
// rotation both revoke the old authority rather than silently upgrading it.
func TestAuthenticatedShardBoundaryRotationAndConfusedDeputyFault(t *testing.T) {
	authority := newGatewayTLSAuthority(t)
	shardIdentity := gatewayPeerIdentity(61, 10)
	gatewayIdentity := gatewayPeerIdentity(61, 30)
	rogueGatewayIdentity := gatewayPeerIdentity(61, 50)
	userIdentity := gatewayPeerIdentity(61, 70)

	policy := newShardQualificationPolicy(t, 1, gatewayIdentity.Node, userIdentity.Node)
	gate, err := serviceauthz.NewGate(policy)
	if err != nil {
		t.Fatal(err)
	}
	serverAuthorizer, err := servicetls.NewNodeAuthorizer([]rafttransport.NodeID{gatewayIdentity.Node})
	if err != nil {
		t.Fatal(err)
	}
	tlsServer, err := servicetls.NewServer(authority.profile(t, shardIdentity),
		rafttransport.TrafficShardSQL, serverAuthorizer)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	shard := newShardServer(t)
	deadline := servicetls.FixedDeadline(5 * time.Second)
	go func() {
		served <- tlsServer.Serve(ctx, listener, servicetls.Limits{
			MaxConnections: 8, MaxHandshakes: 2, HandshakeDeadline: deadline,
		}, func(_ context.Context, connection rafttransport.PeerConnection) {
			shard.ServeAuthorizedConn(connection, gate, nil)
		})
	}()

	client := newShardQualificationClient(t, authority.profile(t, gatewayIdentity),
		listener.Addr().String(), shardIdentity.Node, deadline)
	defer client.Close()
	connection, err := client.Dial(ctx, listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	request := ownedReq("SELECT 1")
	request.Authority = serviceauthz.Authority{Node: userIdentity.Node, Generation: 1}
	response, err := RoundTrip(ctx, connection, request)
	if err != nil || response.Kind != shardservice.ResponseRows {
		t.Fatalf("authorized response=%+v err=%v", response, err)
	}

	request.Authority.Node = rogueGatewayIdentity.Node
	response, err = RoundTrip(ctx, connection, request)
	if !errors.Is(err, ErrUnauthorized) || response != nil {
		t.Fatalf("confused-deputy response=%+v err=%v", response, err)
	}

	next := newShardQualificationPolicy(t, 2, gatewayIdentity.Node, userIdentity.Node)
	if err = gate.Rotate(next); err != nil {
		t.Fatal(err)
	}
	request.Authority = serviceauthz.Authority{Node: userIdentity.Node, Generation: 1}
	response, err = RoundTrip(ctx, connection, request)
	if !errors.Is(err, ErrUnauthorized) || response != nil {
		t.Fatalf("stale-generation response=%+v err=%v", response, err)
	}

	// Certificate publication closes the previously authenticated stream. A
	// retry must establish a new stream and carry the new policy generation.
	if err = tlsServer.Rotate(authority.profile(t, shardIdentity), serverAuthorizer); err != nil {
		t.Fatal(err)
	}
	request.Authority.Generation = 2
	if _, err = RoundTrip(ctx, connection, request); err == nil {
		t.Fatal("retired shard TLS generation remained usable")
	}
	_ = connection.Close()
	connection, err = client.Dial(ctx, listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	response, err = RoundTrip(ctx, connection, request)
	if err != nil || response.Kind != shardservice.ResponseRows {
		t.Fatalf("rotated response=%+v err=%v", response, err)
	}
	_ = connection.Close()

	rogue := newShardQualificationClient(t, authority.profile(t, rogueGatewayIdentity),
		listener.Addr().String(), shardIdentity.Node, deadline)
	rogueConnection, rogueErr := rogue.Dial(ctx, listener.Addr().String())
	if rogueErr == nil {
		_ = rogueConnection.SetDeadline(time.Now().Add(time.Second))
		_, rogueErr = RoundTrip(ctx, rogueConnection, request)
		_ = rogueConnection.Close()
	}
	_ = rogue.Close()
	if rogueErr == nil {
		t.Fatal("unlisted gateway certificate reached the shard protocol")
	}

	cancel()
	_ = listener.Close()
	if err = <-served; !errors.Is(err, context.Canceled) {
		t.Fatalf("serve error=%v", err)
	}
	stats := tlsServer.Stats()
	if stats.Generation != 2 || stats.Authenticated != 2 || stats.AuthenticationRejected == 0 || stats.Active != 0 {
		t.Fatalf("TLS stats=%+v", stats)
	}
}

// BenchmarkAuthenticatedGatewayShardStream measures the steady authenticated
// stream separately from cold TLS handshakes. Every operation performs both
// service-delegate and forwarded-user authorization checks.
func BenchmarkAuthenticatedGatewayShardStream(b *testing.B) {
	authority := newGatewayTLSAuthority(b)
	shardIdentity := gatewayPeerIdentity(63, 10)
	gatewayIdentity := gatewayPeerIdentity(63, 30)
	userIdentity := gatewayPeerIdentity(63, 50)
	policy := newShardQualificationPolicy(b, 1, gatewayIdentity.Node, userIdentity.Node)
	gate, err := serviceauthz.NewGate(policy)
	if err != nil {
		b.Fatal(err)
	}
	serverAuthorizer, err := servicetls.NewNodeAuthorizer([]rafttransport.NodeID{gatewayIdentity.Node})
	if err != nil {
		b.Fatal(err)
	}
	server, err := servicetls.NewServer(authority.profile(b, shardIdentity),
		rafttransport.TrafficShardSQL, serverAuthorizer)
	if err != nil {
		b.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	deadline := servicetls.FixedDeadline(5 * time.Second)
	var allowed atomic.Uint64
	go func() {
		served <- server.Serve(ctx, listener, servicetls.Limits{
			MaxConnections: 2, MaxHandshakes: 1, HandshakeDeadline: deadline,
		}, func(ctx context.Context, connection rafttransport.PeerConnection) {
			generation, ok := servicetls.AdmissionGeneration(ctx)
			var request [8]byte
			for ok {
				if _, readErr := io.ReadFull(connection, request[:]); readErr != nil {
					return
				}
				if gate.Check(connection.PeerIdentity().Node, generation,
					serviceauthz.CapabilityDelegate) != serviceauthz.DecisionAllow ||
					gate.Check(userIdentity.Node, generation,
						serviceauthz.CapabilityDataRead) != serviceauthz.DecisionAllow {
					return
				}
				allowed.Add(1)
				if _, writeErr := connection.Write(request[:]); writeErr != nil {
					return
				}
			}
		})
	}()
	client := newShardQualificationClient(b, authority.profile(b, gatewayIdentity),
		listener.Addr().String(), shardIdentity.Node, deadline)
	connection, err := client.Dial(ctx, listener.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		_ = connection.Close()
		_ = client.Close()
		cancel()
		_ = listener.Close()
		if serveErr := <-served; !errors.Is(serveErr, context.Canceled) {
			b.Errorf("serve error=%v", serveErr)
		}
	}()
	var request, response [8]byte
	b.ReportAllocs()
	b.SetBytes(8)
	b.ResetTimer()
	for operation := 0; operation < b.N; operation++ {
		binary.BigEndian.PutUint64(request[:], uint64(operation+1))
		if _, err = connection.Write(request[:]); err != nil {
			b.Fatal(err)
		}
		if _, err = io.ReadFull(connection, response[:]); err != nil || response != request {
			b.Fatalf("response=%x request=%x err=%v", response, request, err)
		}
	}
	b.StopTimer()
	if got := allowed.Load(); got != uint64(b.N) {
		b.Fatalf("authorized operations=%d want=%d", got, b.N)
	}
}

func newShardQualificationPolicy(t testing.TB, generation uint64,
	gateway, user rafttransport.NodeID) *serviceauthz.Policy {
	t.Helper()
	policy, err := serviceauthz.NewPolicy(generation, []serviceauthz.Entry{
		{Node: gateway, Capabilities: serviceauthz.CapabilityDelegate},
		{Node: user, Capabilities: serviceauthz.CapabilityDataRead},
	})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func newShardQualificationClient(t testing.TB, profile *rafttransport.PeerTLS,
	address string, shard rafttransport.NodeID,
	deadline rafttransport.DeadlineFunc) *servicetls.Client {
	t.Helper()
	client, err := servicetls.NewClient(servicetls.ClientOptions{
		TLS: profile, Class: rafttransport.TrafficShardSQL,
		Endpoints: []servicetls.Endpoint{{Address: address, Node: shard}},
		Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		},
		HandshakeDeadline: deadline, MaxConnections: 2, MaxHandshakes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
