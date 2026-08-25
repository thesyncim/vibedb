package snapshottransfer

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"math/big"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

var snapshotTestOID = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}
var snapshotTestNow = time.Date(2032, 1, 2, 3, 4, 5, 0, time.UTC)

type snapshotTestCA struct {
	certificate *x509.Certificate
	key         *ecdsa.PrivateKey
	roots       *x509.CertPool
	serial      int64
}

func newSnapshotTestCA(t testing.TB) *snapshotTestCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "snapshot-test-ca"},
		NotBefore: snapshotTestNow.Add(-time.Hour), NotAfter: snapshotTestNow.Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	encoded, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(encoded)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	return &snapshotTestCA{certificate: certificate, key: key, roots: roots, serial: 10}
}

func (ca *snapshotTestCA) profile(t testing.TB, identity rafttransport.PeerIdentity) *rafttransport.PeerTLS {
	t.Helper()
	ca.serial++
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	extension, err := rafttransport.PeerIdentityExtension(snapshotTestOID, identity)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(ca.serial), Subject: pkix.Name{CommonName: "ignored"},
		NotBefore: snapshotTestNow.Add(-time.Hour), NotAfter: snapshotTestNow.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, ExtraExtensions: []pkix.Extension{extension}}
	encoded, err := x509.CreateCertificate(rand.Reader, template, ca.certificate, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := rafttransport.NewPeerTLS(rafttransport.PeerTLSOptions{IdentityOID: snapshotTestOID, Identity: identity,
		Certificate: tls.Certificate{Certificate: [][]byte{encoded, ca.certificate.Raw}, PrivateKey: key}, Roots: ca.roots,
		Now: func() time.Time { return snapshotTestNow }})
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

type tlsPipeOpener struct {
	service              *Service
	serverTLS, clientTLS *rafttransport.PeerTLS
	deadline             rafttransport.DeadlineFunc
	errors               chan error
}

func (o *tlsPipeOpener) OpenSnapshot(ctx context.Context, node rafttransport.NodeID) (rafttransport.PeerConnection, error) {
	client, server := net.Pipe()
	go func() { o.errors <- o.service.ServeTLS(ctx, server, o.serverTLS, o.deadline) }()
	return (rafttransport.TLSSnapshotStreamOpener{TLS: o.clientTLS, Open: func(context.Context, rafttransport.NodeID) (net.Conn, error) { return client, nil }, HandshakeDeadline: o.deadline}).OpenSnapshot(ctx, node)
}

func TestSnapshotTLSCertificateRotationPreservesExactIdentity(t *testing.T) {
	payload := make([]byte, MinChunkBytes+317)
	for i := range payload {
		payload[i] = byte(i * 31)
	}
	d := testDescriptor(payload)
	sourceRepo := openTestRepository(t, filepath.Join(t.TempDir(), "source"))
	appendAll(t, sourceRepo, d, payload, 0)
	registry, source, target := testRegistry(t)
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	service, err := NewService(ServiceOptions{Repository: sourceRepo, Registry: registry, Authorize: func(got Descriptor) bool { return got == d }, ReadDeadline: deadline, WriteDeadline: deadline, MaxConnections: 2, MaxChunkBytes: MinChunkBytes, MaxInflightBytes: 2 * MinChunkBytes})
	if err != nil {
		t.Fatal(err)
	}
	ca := newSnapshotTestCA(t)
	serverTLS := ca.profile(t, source)
	oldClientTLS := ca.profile(t, target)
	rotatedClientTLS := ca.profile(t, target)
	for name, clientTLS := range map[string]*rafttransport.PeerTLS{"old-certificate": oldClientTLS, "rotated-certificate": rotatedClientTLS} {
		t.Run(name, func(t *testing.T) {
			targetRepo := openTestRepository(t, filepath.Join(t.TempDir(), "target"))
			opener := &tlsPipeOpener{service: service, serverTLS: serverTLS, clientTLS: clientTLS, deadline: deadline, errors: make(chan error, 3)}
			receiver := Receiver{Repository: targetRepo, Opener: opener, ReadDeadline: deadline, WriteDeadline: deadline, Workspace: make([]byte, MinChunkBytes)}
			if err := receiver.Receive(context.Background(), source.Node, d); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := <-opener.errors; err != nil {
					t.Fatalf("server=%v", err)
				}
			}
		})
	}
}
