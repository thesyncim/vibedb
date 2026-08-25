package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestAuthorizedClientTLSRotationIsOneMonotonicPublication(t *testing.T) {
	authority := newGatewayTLSAuthority(t)
	serverIdentity := gatewayPeerIdentity(7, 10)
	clientIdentity := gatewayPeerIdentity(7, 30)
	first, err := serviceauthz.NewPolicy(4, []serviceauthz.Entry{{
		Node: clientIdentity.Node, Capabilities: serviceauthz.CapabilityDataRead,
	}})
	if err != nil {
		t.Fatal(err)
	}
	capability, err := NewAuthorizedClientTLS(authority.profile(t, serverIdentity), first)
	if err != nil {
		t.Fatal(err)
	}
	before := capability.Stats().Generation
	stale, _ := serviceauthz.NewPolicy(4, []serviceauthz.Entry{{
		Node: clientIdentity.Node, Capabilities: serviceauthz.CapabilityDataWrite,
	}})
	if err = capability.RotateAuthorization(authority.profile(t, serverIdentity), stale); err == nil {
		t.Fatal("equal policy generation rotated TLS")
	}
	if got := capability.Stats().Generation; got != before {
		t.Fatalf("failed policy publication changed TLS generation: %d -> %d", before, got)
	}
	if err = capability.RotateClientTLS(authority.profile(t, serverIdentity),
		[]rafttransport.NodeID{clientIdentity.Node}); err == nil {
		t.Fatal("authorized capability admitted an unbound TLS-only rotation")
	}
	next, _ := serviceauthz.NewPolicy(5, []serviceauthz.Entry{{
		Node: clientIdentity.Node, Capabilities: serviceauthz.CapabilityDataWrite,
	}})
	if err = capability.RotateAuthorization(authority.profile(t, serverIdentity), next); err != nil {
		t.Fatal(err)
	}
	if capability.gate.Generation() != 5 || capability.Stats().Generation != before+1 {
		t.Fatalf("publication gate=%d TLS=%d", capability.gate.Generation(), capability.Stats().Generation)
	}
}

func TestClientTLSAuthenticatesAuthorizesRotatesAndSeparatesALPN(t *testing.T) {
	authority := newGatewayTLSAuthority(t)
	serverIdentity := gatewayPeerIdentity(9, 10)
	firstIdentity := gatewayPeerIdentity(9, 30)
	secondIdentity := gatewayPeerIdentity(9, 50)
	serverProfile := authority.profile(t, serverIdentity)
	firstProfile := authority.profile(t, firstIdentity)
	secondProfile := authority.profile(t, secondIdentity)
	capability, err := NewClientTLS(serverProfile, []rafttransport.NodeID{firstIdentity.Node})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	go func() {
		served <- capability.ServeAuthenticatedClients(ctx, listener, ClientTLSLimits{
			MaxConnections: 4, MaxHandshakes: 2, HandshakeDeadline: deadline,
		}, func(_ context.Context, connection net.Conn) {
			var request [1]byte
			if _, readErr := io.ReadFull(connection, request[:]); readErr == nil {
				_, _ = connection.Write(request[:])
			}
		})
	}()
	dial := func(profile *rafttransport.PeerTLS, class rafttransport.TrafficClass) (rafttransport.PeerConnection, error) {
		raw, dialErr := (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
		if dialErr != nil {
			return nil, dialErr
		}
		return profile.Client(ctx, raw, serverIdentity.Node, class, deadline)
	}

	first, err := dial(firstProfile, rafttransport.TrafficGatewayClient)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = first.Write([]byte{7}); err != nil {
		t.Fatal(err)
	}
	var response [1]byte
	if _, err = io.ReadFull(first, response[:]); err != nil || response[0] != 7 {
		t.Fatalf("first response=%d err=%v", response[0], err)
	}

	wrongClass, err := dial(firstProfile, rafttransport.TrafficShardNative)
	if err == nil {
		_ = wrongClass.Close()
		t.Fatal("gateway accepted a shard-native ALPN")
	}

	denied, err := dial(secondProfile, rafttransport.TrafficGatewayClient)
	if err == nil {
		_ = denied.SetDeadline(time.Now().Add(time.Second))
		_, err = denied.Write([]byte{8})
		if err == nil {
			_, err = io.ReadFull(denied, response[:])
		}
		_ = denied.Close()
	}
	if err == nil {
		t.Fatal("non-allowlisted client remained usable")
	}

	replacement := authority.profile(t, serverIdentity)
	if err = capability.RotateClientTLS(replacement, []rafttransport.NodeID{secondIdentity.Node}); err != nil {
		t.Fatal(err)
	}
	_ = first.SetDeadline(time.Now().Add(time.Second))
	if _, err = first.Write([]byte{9}); err == nil {
		_, err = io.ReadFull(first, response[:])
	}
	if err == nil {
		t.Fatal("retired TLS generation remained usable")
	}
	_ = first.Close()

	second, err := dial(secondProfile, rafttransport.TrafficGatewayClient)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = second.Write([]byte{10}); err != nil {
		t.Fatal(err)
	}
	if _, err = io.ReadFull(second, response[:]); err != nil || response[0] != 10 {
		t.Fatalf("second response=%d err=%v", response[0], err)
	}
	_ = second.Close()
	stats := capability.Stats()
	if stats.Generation != 2 || stats.Authenticated != 2 || stats.AuthenticationRejected == 0 {
		t.Fatalf("stats=%+v", stats)
	}
	cancel()
	_ = listener.Close()
	if err = <-served; !errors.Is(err, context.Canceled) {
		t.Fatalf("serve error=%v", err)
	}
}

func TestClientTLSRejectsExcessHandshakeBeforeWorkerGrowth(t *testing.T) {
	authority := newGatewayTLSAuthority(t)
	serverIdentity := gatewayPeerIdentity(11, 10)
	clientIdentity := gatewayPeerIdentity(11, 30)
	capability, err := NewClientTLS(authority.profile(t, serverIdentity), []rafttransport.NodeID{clientIdentity.Node})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() {
		served <- capability.ServeAuthenticatedClients(ctx, listener, ClientTLSLimits{
			MaxConnections: 2, MaxHandshakes: 1,
			HandshakeDeadline: func() time.Time { return time.Now().Add(5 * time.Second) },
		}, func(context.Context, net.Conn) {})
	}()
	first, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	_ = second.SetReadDeadline(time.Now().Add(2 * time.Second))
	var one [1]byte
	if _, err = second.Read(one[:]); err == nil {
		t.Fatal("excess raw handshake was not rejected")
	}
	deadline := time.Now().Add(2 * time.Second)
	for capability.Stats().Overloaded == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if stats := capability.Stats(); stats.Overloaded == 0 || stats.Active != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	cancel()
	_ = listener.Close()
	_ = first.Close()
	if err = <-served; !errors.Is(err, context.Canceled) {
		t.Fatalf("serve error=%v", err)
	}
}

func TestClientTLSStatsZeroAllocations(t *testing.T) {
	authority := newGatewayTLSAuthority(t)
	serverIdentity := gatewayPeerIdentity(13, 10)
	clientIdentity := gatewayPeerIdentity(13, 30)
	capability, err := NewClientTLS(authority.profile(t, serverIdentity), []rafttransport.NodeID{clientIdentity.Node})
	if err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1000, func() { _ = capability.Stats() }); allocations != 0 {
		t.Fatalf("Stats allocations=%v", allocations)
	}
}

func TestAuthenticatedShardDialIsBoundedExactAndRotationSafe(t *testing.T) {
	authority := newGatewayTLSAuthority(t)
	serverIdentity := gatewayPeerIdentity(15, 10)
	clientIdentity := gatewayPeerIdentity(15, 30)
	serverProfile := authority.profile(t, serverIdentity)
	clientProfile := authority.profile(t, clientIdentity)
	authorizer, err := servicetls.NewNodeAuthorizer([]rafttransport.NodeID{clientIdentity.Node})
	if err != nil {
		t.Fatal(err)
	}
	server, err := servicetls.NewServer(serverProfile, rafttransport.TrafficShardSQL, authorizer)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	served := make(chan error, 1)
	shard := newShardServer(t)
	go func() {
		served <- server.Serve(ctx, listener, servicetls.Limits{
			MaxConnections: 2, MaxHandshakes: 1, HandshakeDeadline: deadline,
		}, func(_ context.Context, connection rafttransport.PeerConnection) { shard.ServeConn(connection) })
	}()
	client, err := servicetls.NewClient(servicetls.ClientOptions{
		TLS: clientProfile, Class: rafttransport.TrafficShardSQL,
		Endpoints: []servicetls.Endpoint{{Address: listener.Addr().String(), Node: serverIdentity.Node}},
		Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		}, HandshakeDeadline: deadline, MaxConnections: 1, MaxHandshakes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	first, err := client.Dial(ctx, listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Dial(ctx, listener.Addr().String()); !errors.Is(err, servicetls.ErrBound) {
		t.Fatalf("second physical connection err=%v", err)
	}
	response, err := RoundTrip(ctx, first, ownedReq("SELECT 1"))
	if err != nil || response.Kind != shardservice.ResponseRows || len(response.Rows) != 1 {
		t.Fatalf("authenticated shard response=%+v err=%v", response, err)
	}
	replacement := authority.profile(t, clientIdentity)
	if err = client.Rotate(replacement, []servicetls.Endpoint{{
		Address: listener.Addr().String(), Node: serverIdentity.Node,
	}}); err != nil {
		t.Fatal(err)
	}
	_, err = RoundTrip(ctx, first, ownedReq("SELECT 1"))
	if err == nil {
		t.Fatal("rotated client left an old pooled connection usable")
	}
	_ = first.Close()
	second, err := client.Dial(ctx, listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	response, err = RoundTrip(ctx, second, ownedReq("SELECT 1"))
	if err != nil || response.Kind != shardservice.ResponseRows || len(response.Rows) != 1 {
		t.Fatalf("rotated shard response=%+v err=%v", response, err)
	}
	_ = second.Close()
	cancel()
	_ = listener.Close()
	if err = <-served; !errors.Is(err, context.Canceled) {
		t.Fatalf("serve error=%v", err)
	}
}
