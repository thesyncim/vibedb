package rafttransport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
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
)

var peerTLSTestNow = time.Date(2031, 4, 5, 6, 7, 8, 0, time.UTC)

// PEN 32473 is reserved for examples and documentation.
var peerTLSTestIdentityOID = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}

type peerTLSTestAuthority struct {
	certificate *x509.Certificate
	key         *ecdsa.PrivateKey
	roots       *x509.CertPool
	serial      int64
}

func newPeerTLSTestAuthority(t testing.TB, seed byte) *peerTLSTestAuthority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(int64(seed) + 1),
		Subject:               pkix.Name{CommonName: "untrusted-name-ca"},
		NotBefore:             peerTLSTestNow.Add(-24 * time.Hour),
		NotAfter:              peerTLSTestNow.Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	encoded, err := x509.CreateCertificate(
		rand.Reader, template, template, &key.PublicKey, key,
	)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(encoded)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	return &peerTLSTestAuthority{
		certificate: certificate, key: key, roots: roots, serial: int64(seed) + 10,
	}
}

func (authority *peerTLSTestAuthority) issue(
	t testing.TB,
	identity PeerIdentity,
	notBefore, notAfter time.Time,
) tls.Certificate {
	return authority.issueWithOID(
		t, identity, peerTLSTestIdentityOID, notBefore, notAfter,
	)
}

func (authority *peerTLSTestAuthority) issueWithOID(
	t testing.TB,
	identity PeerIdentity,
	oid asn1.ObjectIdentifier,
	notBefore, notAfter time.Time,
) tls.Certificate {
	return authority.issueWithProfile(
		t, identity, oid, notBefore, notAfter,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	)
}

func (authority *peerTLSTestAuthority) issueWithProfile(
	t testing.TB,
	identity PeerIdentity,
	oid asn1.ObjectIdentifier,
	notBefore, notAfter time.Time,
	usages []x509.ExtKeyUsage,
	extraExtensions ...pkix.Extension,
) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	extension, err := PeerIdentityExtension(peerTLSTestIdentityOID, identity)
	if err != nil {
		t.Fatal(err)
	}
	extension.Id = append(asn1.ObjectIdentifier(nil), oid...)
	authority.serial++
	template := &x509.Certificate{
		SerialNumber:    big.NewInt(authority.serial),
		Subject:         pkix.Name{CommonName: "must-not-be-a-node-identity"},
		DNSNames:        []string{"must-not-be-a-node-identity.invalid"},
		NotBefore:       notBefore,
		NotAfter:        notAfter,
		KeyUsage:        x509.KeyUsageDigitalSignature,
		ExtKeyUsage:     append([]x509.ExtKeyUsage(nil), usages...),
		ExtraExtensions: append([]pkix.Extension{extension}, extraExtensions...),
	}
	encoded, err := x509.CreateCertificate(
		rand.Reader, template, authority.certificate, &key.PublicKey, authority.key,
	)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{
		Certificate: [][]byte{encoded, authority.certificate.Raw}, PrivateKey: key,
	}
}

func peerTLSTestIdentity(domainSeed, nodeSeed byte) PeerIdentity {
	var identity PeerIdentity
	for index := range identity.TrustDomain.ClusterID {
		identity.TrustDomain.ClusterID[index] = domainSeed + byte(index)
		identity.TrustDomain.ClusterIncarnation[index] = domainSeed + 32 + byte(index)
	}
	for index := range identity.Node {
		identity.Node[index] = nodeSeed + byte(index)
	}
	return identity
}

func newPeerTLSTestProfile(
	t testing.TB,
	authority *peerTLSTestAuthority,
	identity PeerIdentity,
) *PeerTLS {
	t.Helper()
	profile, err := NewPeerTLS(PeerTLSOptions{
		IdentityOID: peerTLSTestIdentityOID,
		Identity:    identity,
		Certificate: authority.issue(
			t, identity, peerTLSTestNow.Add(-time.Hour), peerTLSTestNow.Add(time.Hour),
		),
		Roots: authority.roots,
		Now:   func() time.Time { return peerTLSTestNow },
	})
	if err != nil {
		t.Fatalf("NewPeerTLS: %v", err)
	}
	return profile
}

func TestPeerIdentityExtensionIsExactCriticalAndDuplicateClosed(t *testing.T) {
	identity := peerTLSTestIdentity(1, 91)
	extension, err := PeerIdentityExtension(peerTLSTestIdentityOID, identity)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{Extensions: []pkix.Extension{extension}}
	got, err := ParsePeerIdentity(peerTLSTestIdentityOID, certificate)
	if err != nil || got != identity {
		t.Fatalf("ParsePeerIdentity = %+v, %v", got, err)
	}

	mutations := []struct {
		name   string
		change func(*x509.Certificate)
	}{
		{name: "missing", change: func(c *x509.Certificate) { c.Extensions = nil }},
		{name: "duplicate", change: func(c *x509.Certificate) {
			c.Extensions = append(c.Extensions, extension)
		}},
		{name: "noncritical", change: func(c *x509.Certificate) {
			c.Extensions[0].Critical = false
		}},
		{name: "trailing byte", change: func(c *x509.Certificate) {
			c.Extensions[0].Value = append(c.Extensions[0].Value, 0)
		}},
		{name: "zero cluster ID", change: func(c *x509.Certificate) {
			clear(c.Extensions[0].Value[:16])
		}},
		{name: "zero cluster incarnation", change: func(c *x509.Certificate) {
			clear(c.Extensions[0].Value[16:32])
		}},
		{name: "zero node", change: func(c *x509.Certificate) {
			clear(c.Extensions[0].Value[32:])
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			candidate := &x509.Certificate{Extensions: []pkix.Extension{{
				Id: append([]int(nil), extension.Id...), Critical: extension.Critical,
				Value: append([]byte(nil), extension.Value...),
			}}}
			mutation.change(candidate)
			if _, err := ParsePeerIdentity(peerTLSTestIdentityOID, candidate); !errors.Is(err, ErrInvalidPeerIdentity) {
				t.Fatalf("error = %v, want ErrInvalidPeerIdentity", err)
			}
		})
	}
}

func TestPeerIdentityOIDMustBeExactOperatorPrivateEnterpriseLeaf(t *testing.T) {
	identity := peerTLSTestIdentity(2, 92)
	invalid := []struct {
		name string
		oid  asn1.ObjectIdentifier
	}{
		{name: "nil"},
		{name: "standard extension", oid: asn1.ObjectIdentifier{2, 5, 29, 17}},
		{name: "zero PEN", oid: asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 0, 1, 1}},
		{name: "missing type arc", oid: asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1}},
		{name: "wrong VibeDB arc", oid: asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 2, 1}},
		{name: "wrong type arc", oid: asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 2}},
		{name: "unknown trailing arc", oid: asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1, 1}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			if _, err := PeerIdentityExtension(test.oid, identity); !errors.Is(err, ErrInvalidPeerIdentity) {
				t.Fatalf("error = %v, want ErrInvalidPeerIdentity", err)
			}
		})
	}
}

func TestPeerTLSPinsIdentityOIDAndRejectsCertificateAtAnotherOID(t *testing.T) {
	authority := newPeerTLSTestAuthority(t, 2)
	identity := peerTLSTestIdentity(4, 24)
	oid := append(asn1.ObjectIdentifier(nil), peerTLSTestIdentityOID...)
	profile, err := NewPeerTLS(PeerTLSOptions{
		IdentityOID: oid,
		Identity:    identity,
		Certificate: authority.issue(
			t, identity, peerTLSTestNow.Add(-time.Hour), peerTLSTestNow.Add(time.Hour),
		),
		Roots: authority.roots,
		Now:   func() time.Time { return peerTLSTestNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	oid[6] = 1
	if !profile.identityOID.Equal(peerTLSTestIdentityOID) {
		t.Fatalf("profile identity OID changed through caller alias: %v", profile.identityOID)
	}

	otherOID := append(asn1.ObjectIdentifier(nil), peerTLSTestIdentityOID...)
	otherOID[8] = 2
	certificate := authority.issueWithOID(
		t, identity, otherOID,
		peerTLSTestNow.Add(-time.Hour), peerTLSTestNow.Add(time.Hour),
	)
	if _, err := NewPeerTLS(PeerTLSOptions{
		IdentityOID: peerTLSTestIdentityOID,
		Identity:    identity,
		Certificate: certificate,
		Roots:       authority.roots,
		Now:         func() time.Time { return peerTLSTestNow },
	}); !errors.Is(err, ErrInvalidPeerIdentity) {
		t.Fatalf("different extension OID error = %v, want ErrInvalidPeerIdentity", err)
	}
}

func TestPeerTLSRejectsCertificateChainBoundsBeforeCopy(t *testing.T) {
	authority := newPeerTLSTestAuthority(t, 5)
	identity := peerTLSTestIdentity(8, 28)
	base := authority.issue(
		t, identity, peerTLSTestNow.Add(-time.Hour), peerTLSTestNow.Add(time.Hour),
	)
	options := PeerTLSOptions{
		IdentityOID: peerTLSTestIdentityOID,
		Identity:    identity,
		Certificate: base,
		Roots:       authority.roots,
		Now:         func() time.Time { return peerTLSTestNow },
	}
	tooMany := options
	tooMany.Certificate.Certificate = make([][]byte, AbsoluteMaxPeerCertificateChain+1)
	for index := range tooMany.Certificate.Certificate {
		tooMany.Certificate.Certificate[index] = []byte{1}
	}
	if _, err := NewPeerTLS(tooMany); !errors.Is(err, ErrInvalidPeerIdentity) {
		t.Fatalf("oversized chain-count error = %v", err)
	}

	tooLarge := options
	tooLarge.Certificate.Certificate = [][]byte{
		make([]byte, AbsoluteMaxPeerCertificateDERBytes+1),
	}
	if _, err := NewPeerTLS(tooLarge); !errors.Is(err, ErrInvalidPeerIdentity) {
		t.Fatalf("oversized chain-bytes error = %v", err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		_, _ = NewPeerTLS(tooLarge)
	}); got != 0 {
		t.Fatalf("oversized certificate rejection allocations = %v, want 0", got)
	}
}

func TestPeerTLSMutualAuthenticationDerivesExactNode(t *testing.T) {
	authority := newPeerTLSTestAuthority(t, 1)
	clientIdentity := peerTLSTestIdentity(7, 21)
	serverIdentity := peerTLSTestIdentity(7, 41)
	clientTLS := newPeerTLSTestProfile(t, authority, clientIdentity)
	serverTLS := newPeerTLSTestProfile(t, authority, serverIdentity)

	client, server, clientErr, serverErr := peerTLSTestHandshake(
		t, clientTLS, serverTLS, serverIdentity.Node,
		TrafficOrdinary, TrafficOrdinary,
	)
	if clientErr != nil || serverErr != nil {
		t.Fatalf("handshake errors = client %v, server %v", clientErr, serverErr)
	}
	defer client.Close()
	defer server.Close()
	if client.PeerNode() != serverIdentity.Node || server.PeerNode() != clientIdentity.Node ||
		client.TrafficClass() != TrafficOrdinary || server.TrafficClass() != TrafficOrdinary {
		t.Fatalf("derived peers = client %x/%d server %x/%d",
			client.PeerNode(), client.TrafficClass(), server.PeerNode(), server.TrafficClass())
	}
}

func TestPeerTLSServerRejectsLocalNodeCertificate(t *testing.T) {
	authority := newPeerTLSTestAuthority(t, 6)
	identity := peerTLSTestIdentity(17, 61)
	profile := newPeerTLSTestProfile(t, authority, identity)
	client, server, clientErr, serverErr := peerTLSTestHandshake(
		t, profile, profile, identity.Node,
		TrafficOrdinary, TrafficOrdinary,
	)
	if client != nil {
		_ = client.Close()
	}
	if server != nil {
		_ = server.Close()
	}
	if !errors.Is(clientErr, ErrWrongPeer) && !errors.Is(serverErr, ErrWrongPeer) {
		t.Fatalf("self-authentication errors = client %v, server %v", clientErr, serverErr)
	}
}

func TestPeerTLSRechecksCertificateTimeAtHandshake(t *testing.T) {
	authority := newPeerTLSTestAuthority(t, 8)
	clientIdentity := peerTLSTestIdentity(23, 31)
	serverIdentity := peerTLSTestIdentity(23, 51)
	current := peerTLSTestNow
	newProfile := func(identity PeerIdentity) *PeerTLS {
		profile, err := NewPeerTLS(PeerTLSOptions{
			IdentityOID: peerTLSTestIdentityOID,
			Identity:    identity,
			Certificate: authority.issue(
				t, identity, peerTLSTestNow.Add(-time.Hour), peerTLSTestNow.Add(time.Hour),
			),
			Roots: authority.roots,
			Now:   func() time.Time { return current },
		})
		if err != nil {
			t.Fatal(err)
		}
		return profile
	}
	clientTLS := newProfile(clientIdentity)
	serverTLS := newProfile(serverIdentity)
	current = peerTLSTestNow.Add(2 * time.Hour)
	client, server, clientErr, serverErr := peerTLSTestHandshake(
		t, clientTLS, serverTLS, serverIdentity.Node,
		TrafficOrdinary, TrafficOrdinary,
	)
	if client != nil {
		_ = client.Close()
	}
	if server != nil {
		_ = server.Close()
	}
	if !errors.Is(clientErr, ErrPeerAuthentication) &&
		!errors.Is(serverErr, ErrPeerAuthentication) {
		t.Fatalf("expired handshake errors = client %v, server %v", clientErr, serverErr)
	}
}

func TestPeerTLSRejectsRogueCAWrongNodeWrongClusterAndTrafficClass(t *testing.T) {
	authority := newPeerTLSTestAuthority(t, 3)
	rogueAuthority := newPeerTLSTestAuthority(t, 4)
	clientIdentity := peerTLSTestIdentity(11, 31)
	serverIdentity := peerTLSTestIdentity(11, 51)
	clientTLS := newPeerTLSTestProfile(t, authority, clientIdentity)
	serverTLS := newPeerTLSTestProfile(t, authority, serverIdentity)

	tests := []struct {
		name        string
		client      *PeerTLS
		server      *PeerTLS
		expected    NodeID
		clientClass TrafficClass
		serverClass TrafficClass
		want        error
	}{
		{
			name: "rogue CA", client: clientTLS,
			server:   newPeerTLSTestProfile(t, rogueAuthority, serverIdentity),
			expected: serverIdentity.Node, clientClass: TrafficOrdinary,
			serverClass: TrafficOrdinary, want: ErrPeerAuthentication,
		},
		{
			name: "wrong node", client: clientTLS, server: serverTLS,
			expected:    peerTLSTestIdentity(11, 71).Node,
			clientClass: TrafficOrdinary, serverClass: TrafficOrdinary,
			want: ErrWrongPeer,
		},
		{
			name: "wrong cluster with reused CA", client: clientTLS,
			server: newPeerTLSTestProfile(
				t, authority, peerTLSTestIdentity(12, 51),
			),
			expected: serverIdentity.Node, clientClass: TrafficOrdinary,
			serverClass: TrafficOrdinary, want: ErrWrongTrustDomain,
		},
		{
			name: "snapshot on ordinary stream", client: clientTLS, server: serverTLS,
			expected: serverIdentity.Node, clientClass: TrafficSnapshot,
			serverClass: TrafficOrdinary, want: ErrPeerAuthentication,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, server, clientErr, serverErr := peerTLSTestHandshake(
				t, test.client, test.server, test.expected,
				test.clientClass, test.serverClass,
			)
			if client != nil {
				_ = client.Close()
			}
			if server != nil {
				_ = server.Close()
			}
			if !errors.Is(clientErr, test.want) && !errors.Is(serverErr, test.want) {
				t.Fatalf("errors = client %v, server %v, want %v",
					clientErr, serverErr, test.want)
			}
		})
	}
}

func TestPeerTLSRejectsExpiredLocalCertificateAndSubjectOnlyIdentity(t *testing.T) {
	authority := newPeerTLSTestAuthority(t, 7)
	identity := peerTLSTestIdentity(19, 81)
	expired := authority.issue(
		t, identity, peerTLSTestNow.Add(-2*time.Hour), peerTLSTestNow.Add(-time.Hour),
	)
	if _, err := NewPeerTLS(PeerTLSOptions{
		IdentityOID: peerTLSTestIdentityOID,
		Identity:    identity, Certificate: expired, Roots: authority.roots,
		Now: func() time.Time { return peerTLSTestNow },
	}); !errors.Is(err, ErrPeerAuthentication) {
		t.Fatalf("expired certificate error = %v", err)
	}
	serverOnly := authority.issueWithProfile(
		t, identity, peerTLSTestIdentityOID,
		peerTLSTestNow.Add(-time.Hour), peerTLSTestNow.Add(time.Hour),
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	)
	if _, err := NewPeerTLS(PeerTLSOptions{
		IdentityOID: peerTLSTestIdentityOID,
		Identity:    identity,
		Certificate: serverOnly,
		Roots:       authority.roots,
		Now:         func() time.Time { return peerTLSTestNow },
	}); !errors.Is(err, ErrInvalidPeerIdentity) {
		t.Fatalf("single-EKU certificate error = %v", err)
	}
	unknownCritical := authority.issueWithProfile(
		t, identity, peerTLSTestIdentityOID,
		peerTLSTestNow.Add(-time.Hour), peerTLSTestNow.Add(time.Hour),
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		pkix.Extension{
			Id:       asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 9, 9},
			Critical: true, Value: []byte{1},
		},
	)
	if _, err := NewPeerTLS(PeerTLSOptions{
		IdentityOID: peerTLSTestIdentityOID,
		Identity:    identity,
		Certificate: unknownCritical,
		Roots:       authority.roots,
		Now:         func() time.Time { return peerTLSTestNow },
	}); !errors.Is(err, ErrPeerAuthentication) {
		t.Fatalf("unknown critical extension error = %v", err)
	}
	if _, err := NewPeerTLS(PeerTLSOptions{
		IdentityOID: peerTLSTestIdentityOID,
		Identity:    identity,
		Certificate: authority.issue(
			t, identity, peerTLSTestNow.Add(-time.Hour), peerTLSTestNow.Add(time.Hour),
		),
		Roots: authority.roots,
	}); !errors.Is(err, ErrInvalidPeerIdentity) {
		t.Fatalf("missing explicit clock error = %v", err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(999),
		Subject:      pkix.Name{CommonName: string(identity.Node[:])},
		NotBefore:    peerTLSTestNow.Add(-time.Hour),
		NotAfter:     peerTLSTestNow.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth,
		},
	}
	encoded, err := x509.CreateCertificate(
		rand.Reader, template, authority.certificate, &key.PublicKey, authority.key,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewPeerTLS(PeerTLSOptions{
		IdentityOID: peerTLSTestIdentityOID,
		Identity:    identity,
		Certificate: tls.Certificate{
			Certificate: [][]byte{encoded, authority.certificate.Raw}, PrivateKey: key,
		},
		Roots: authority.roots, Now: func() time.Time { return peerTLSTestNow },
	}); !errors.Is(err, ErrInvalidPeerIdentity) {
		t.Fatalf("subject-only identity error = %v", err)
	}
}

func peerTLSTestHandshake(
	t testing.TB,
	clientTLS, serverTLS *PeerTLS,
	expected NodeID,
	clientClass, serverClass TrafficClass,
) (PeerConnection, PeerConnection, error, error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	acceptErrors := make(chan error, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			acceptErrors <- acceptErr
			return
		}
		accepted <- connection
	}()
	dialer := net.Dialer{Timeout: 5 * time.Second}
	clientRaw, err := dialer.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	var serverRaw net.Conn
	select {
	case serverRaw = <-accepted:
	case err := <-acceptErrors:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("TCP accept leaked")
	}
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type result struct {
		connection PeerConnection
		err        error
	}
	serverResult := make(chan result, 1)
	go func() {
		connection, err := serverTLS.Server(
			ctx, serverRaw, serverClass, deadline,
		)
		serverResult <- result{connection: connection, err: err}
	}()
	client, clientErr := clientTLS.Client(
		ctx, clientRaw, expected, clientClass, deadline,
	)
	var server result
	select {
	case server = <-serverResult:
	case <-ctx.Done():
		_ = clientRaw.Close()
		_ = serverRaw.Close()
		t.Fatalf("TLS handshake leaked: %v", context.Cause(ctx))
	}
	return client, server.connection, clientErr, server.err
}

func BenchmarkPeerTLSMutualHandshakeTCP(b *testing.B) {
	authority := newPeerTLSTestAuthority(b, 30)
	clientIdentity := peerTLSTestIdentity(31, 41)
	serverIdentity := peerTLSTestIdentity(31, 61)
	clientTLS := newPeerTLSTestProfile(b, authority, clientIdentity)
	serverTLS := newPeerTLSTestProfile(b, authority, serverIdentity)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	type result struct {
		connection PeerConnection
		err        error
	}
	serverResults := make(chan result, 1)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		for {
			raw, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			connection, serveErr := serverTLS.Server(
				context.Background(), raw, TrafficOrdinary, deadline,
			)
			serverResults <- result{connection: connection, err: serveErr}
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		raw, dialErr := net.Dial("tcp", listener.Addr().String())
		if dialErr != nil {
			b.Fatal(dialErr)
		}
		client, clientErr := clientTLS.Client(
			context.Background(), raw, serverIdentity.Node,
			TrafficOrdinary, deadline,
		)
		server := <-serverResults
		if clientErr != nil || server.err != nil {
			b.Fatalf("handshake errors = client %v, server %v", clientErr, server.err)
		}
		_ = client.Close()
		_ = server.connection.Close()
	}
	b.StopTimer()
	_ = listener.Close()
	<-serverDone
}
