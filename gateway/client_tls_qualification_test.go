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
)

const (
	authorizedTransportQualificationOperations = 1024
	authorizedTransportMaximumOperation        = 2 * time.Second
	authorizedTransportMaximumPhase            = 20 * time.Second
)

// TestAuthorizedClientTLSNetworkChaosAndThroughputGate keeps an authorized
// stream busy while unrelated sockets send truncated TLS records and reset.
// It then rotates the exact policy generation, proves that the retired stream
// is unusable, and repeats the measured request phase on the new generation.
// The generous hard timing limits are a deadlock/stall gate, not a published
// host-independent performance claim.
func TestAuthorizedClientTLSNetworkChaosAndThroughputGate(t *testing.T) {
	harness := newAuthorizedTransportQualification(t)
	defer harness.close(t)

	chaosDone := make(chan error, 1)
	go func() {
		for range 64 {
			connection, err := net.DialTimeout("tcp", harness.listener.Addr().String(), time.Second)
			if err != nil {
				chaosDone <- err
				return
			}
			_ = connection.SetDeadline(time.Now().Add(time.Second))
			_, _ = connection.Write([]byte{0x16, 0x03, 0x03, 0, 32, 1, 2, 3})
			_ = connection.Close()
		}
		chaosDone <- nil
	}()

	first := harness.dial(t)
	firstElapsed, firstMaximum := runAuthorizedTransportPhase(t, first,
		authorizedTransportQualificationOperations)

	next, err := serviceauthz.NewPolicy(2, []serviceauthz.Entry{{
		Node: harness.clientIdentity.Node, Capabilities: serviceauthz.CapabilityDataRead,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = harness.capability.RotateAuthorization(
		harness.authority.profile(t, harness.serverIdentity), next,
	); err != nil {
		t.Fatal(err)
	}
	_ = first.SetDeadline(time.Now().Add(time.Second))
	if _, err = first.Write(make([]byte, 8)); err == nil {
		var response [8]byte
		_, err = io.ReadFull(first, response[:])
	}
	_ = first.Close()
	if err == nil {
		t.Fatal("retired authorization generation remained usable")
	}

	second := harness.dial(t)
	secondElapsed, secondMaximum := runAuthorizedTransportPhase(t, second,
		authorizedTransportQualificationOperations)
	_ = second.Close()
	if err = <-chaosDone; err != nil {
		t.Fatal(err)
	}

	want := uint64(2 * authorizedTransportQualificationOperations)
	if got := harness.allowed.Load(); got != want {
		t.Fatalf("authorized requests=%d want=%d", got, want)
	}
	deadline := time.Now().Add(2 * time.Second)
	for stats := harness.capability.Stats(); (stats.AuthenticationRejected == 0 || stats.Active != 0) && time.Now().Before(deadline); stats = harness.capability.Stats() {
		time.Sleep(time.Millisecond)
	}
	stats := harness.capability.Stats()
	if stats.Generation != 2 || stats.Authenticated != 2 || stats.AuthenticationRejected == 0 || stats.Active != 0 {
		t.Fatalf("transport stats=%+v", stats)
	}
	if firstElapsed > authorizedTransportMaximumPhase || secondElapsed > authorizedTransportMaximumPhase ||
		firstMaximum > authorizedTransportMaximumOperation || secondMaximum > authorizedTransportMaximumOperation {
		t.Fatalf("stalled authorized transport: phase=%s/%s max=%s/%s", firstElapsed,
			secondElapsed, firstMaximum, secondMaximum)
	}
	t.Logf("authorized_transport\toperations\t%d\tphase_1_ns\t%d\tphase_2_ns\t%d\tmax_operation_ns\t%d\trejected_handshakes\t%d",
		want, firstElapsed.Nanoseconds(), secondElapsed.Nanoseconds(),
		max(firstMaximum, secondMaximum).Nanoseconds(), stats.AuthenticationRejected)
}

func BenchmarkAuthorizedClientTLSRequest(b *testing.B) {
	harness := newAuthorizedTransportQualification(b)
	defer harness.close(b)
	connection := harness.dial(b)
	defer connection.Close()
	var request, response [8]byte
	b.ReportAllocs()
	b.SetBytes(8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(request[:], uint64(i+1))
		if _, err := connection.Write(request[:]); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(connection, response[:]); err != nil || response != request {
			b.Fatalf("response=%x request=%x err=%v", response, request, err)
		}
	}
}

type authorizedTransportQualification struct {
	authority               *gatewayTLSAuthority
	serverIdentity          rafttransport.PeerIdentity
	clientIdentity          rafttransport.PeerIdentity
	capability              *ClientTLS
	listener                net.Listener
	cancel                  context.CancelFunc
	served                  chan error
	allowed                 atomic.Uint64
	clientProfile           *rafttransport.PeerTLS
	clientHandshakeDeadline rafttransport.DeadlineFunc
}

func newAuthorizedTransportQualification(t testing.TB) *authorizedTransportQualification {
	t.Helper()
	authority := newGatewayTLSAuthority(t)
	serverIdentity := gatewayPeerIdentity(41, 10)
	clientIdentity := gatewayPeerIdentity(41, 30)
	policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{{
		Node: clientIdentity.Node, Capabilities: serviceauthz.CapabilityDataRead,
	}})
	if err != nil {
		t.Fatal(err)
	}
	capability, err := NewAuthorizedClientTLS(authority.profile(t, serverIdentity), policy)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	harness := &authorizedTransportQualification{
		authority: authority, serverIdentity: serverIdentity, clientIdentity: clientIdentity,
		capability: capability, listener: listener, cancel: cancel, served: make(chan error, 1),
		clientProfile:           authority.profile(t, clientIdentity),
		clientHandshakeDeadline: func() time.Time { return time.Now().Add(5 * time.Second) },
	}
	go func() {
		harness.served <- capability.ServeAuthorizedClients(ctx, listener, ClientTLSLimits{
			MaxConnections: 128, MaxHandshakes: 16,
			HandshakeDeadline: harness.clientHandshakeDeadline,
		}, func(ctx context.Context, connection net.Conn) {
			var request [8]byte
			for {
				if _, readErr := io.ReadFull(connection, request[:]); readErr != nil {
					return
				}
				if capability.Authorize(ctx, serviceauthz.CapabilityDataRead, nil) != serviceauthz.DecisionAllow {
					return
				}
				harness.allowed.Add(1)
				if _, writeErr := connection.Write(request[:]); writeErr != nil {
					return
				}
			}
		})
	}()
	return harness
}

func (harness *authorizedTransportQualification) dial(t testing.TB) rafttransport.PeerConnection {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", harness.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	connection, err := harness.clientProfile.Client(ctx, raw, harness.serverIdentity.Node,
		rafttransport.TrafficGatewayClient, harness.clientHandshakeDeadline)
	if err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	return connection
}

func (harness *authorizedTransportQualification) close(t testing.TB) {
	t.Helper()
	harness.cancel()
	_ = harness.listener.Close()
	if err := <-harness.served; !errors.Is(err, context.Canceled) {
		t.Errorf("serve error=%v", err)
	}
}

func runAuthorizedTransportPhase(t testing.TB, connection net.Conn, operations int) (time.Duration, time.Duration) {
	t.Helper()
	started := time.Now()
	maximum := time.Duration(0)
	var request, response [8]byte
	for operation := 0; operation < operations; operation++ {
		binary.BigEndian.PutUint64(request[:], uint64(operation+1))
		begin := time.Now()
		_ = connection.SetDeadline(begin.Add(authorizedTransportMaximumOperation))
		if _, err := connection.Write(request[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(connection, response[:]); err != nil || response != request {
			t.Fatalf("response=%x request=%x err=%v", response, request, err)
		}
		maximum = max(maximum, time.Since(begin))
	}
	_ = connection.SetDeadline(time.Time{})
	return time.Since(started), maximum
}
