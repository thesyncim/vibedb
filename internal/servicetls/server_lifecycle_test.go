package servicetls

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"errors"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestServerListenerClosureCancelsAndClosesEstablishedConnection(t *testing.T) {
	serverTLS, clientTLS := serverTestTLSProfiles(t)
	authorizer, err := NewNodeAuthorizer([]rafttransport.NodeID{clientTLS.LocalIdentity().Node})
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(serverTLS, rafttransport.TrafficGatewayClient, authorizer)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan context.Context, 1)
	handlerRead := make(chan error, 1)
	served := make(chan error, 1)
	deadline := FixedDeadline(5 * time.Second)
	go func() {
		served <- server.Serve(ctx, listener, Limits{
			MaxConnections: 1, MaxHandshakes: 1, HandshakeDeadline: deadline,
		}, func(ctx context.Context, connection rafttransport.PeerConnection) {
			entered <- ctx
			<-ctx.Done()
			var data [1]byte
			_, err := connection.Read(data[:])
			handlerRead <- err
		})
	}()
	raw, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	connection, err := clientTLS.Client(ctx, raw, serverTLS.LocalIdentity().Node,
		rafttransport.TrafficGatewayClient, deadline)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	var handlerContext context.Context
	select {
	case handlerContext = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("authenticated connection did not reach handler")
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-served:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Serve() = %v, want listener closure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("listener closure left Serve waiting for an established handler")
	}
	if !errors.Is(context.Cause(handlerContext), context.Canceled) {
		t.Fatalf("handler cancellation = %v", context.Cause(handlerContext))
	}
	if err := <-handlerRead; err == nil {
		t.Fatal("handler connection remained readable after listener shutdown")
	}
	if active := server.Stats().Active; active != 0 {
		t.Fatalf("shutdown retained %d active connections", active)
	}
}

func serverTestTLSProfiles(t *testing.T) (*rafttransport.PeerTLS, *rafttransport.PeerTLS) {
	t.Helper()
	now := time.Now()
	public, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root := &x509.Certificate{SerialNumber: big.NewInt(1),
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, root, root, public, key)
	if err != nil {
		t.Fatal(err)
	}
	root, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(root)
	oid := asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}
	profile := func(node byte) *rafttransport.PeerTLS {
		leafPublic, leafKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		identity := rafttransport.PeerIdentity{Node: rafttransport.NodeID{node},
			TrustDomain: rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{1}}}
		extension, err := rafttransport.PeerIdentityExtension(oid, identity)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(node) + 1),
			NotBefore: root.NotBefore, NotAfter: root.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature,
			ExtKeyUsage:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
			ExtraExtensions: []pkix.Extension{extension}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, root, leafPublic, key)
		if err != nil {
			t.Fatal(err)
		}
		result, err := rafttransport.NewPeerTLS(rafttransport.PeerTLSOptions{
			IdentityOID: oid, Identity: identity, Roots: roots,
			Certificate: tls.Certificate{Certificate: [][]byte{der, root.Raw}, PrivateKey: leafKey},
			Now:         func() time.Time { return now },
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	return profile(1), profile(2)
}
