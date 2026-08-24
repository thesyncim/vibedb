package gateway

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"math/big"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

var gatewayTLSNow = time.Date(2034, 2, 3, 4, 5, 6, 0, time.UTC)
var gatewayTLSOID = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}

type gatewayTLSAuthority struct {
	certificate *x509.Certificate
	key         *ecdsa.PrivateKey
	roots       *x509.CertPool
	serial      int64
}

func newGatewayTLSAuthority(t testing.TB) *gatewayTLSAuthority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true,
		NotBefore: gatewayTLSNow.Add(-time.Hour), NotAfter: gatewayTLSNow.Add(time.Hour),
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
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
	return &gatewayTLSAuthority{certificate: certificate, key: key, roots: roots, serial: 1}
}

func gatewayPeerIdentity(domain, node byte) (identity rafttransport.PeerIdentity) {
	for index := range identity.TrustDomain.ClusterID {
		identity.TrustDomain.ClusterID[index] = domain + byte(index)
		identity.TrustDomain.ClusterIncarnation[index] = domain + 32 + byte(index)
		identity.Node[index] = node + byte(index)
	}
	return identity
}

func (authority *gatewayTLSAuthority) profile(t testing.TB, identity rafttransport.PeerIdentity) *rafttransport.PeerTLS {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	extension, err := rafttransport.PeerIdentityExtension(gatewayTLSOID, identity)
	if err != nil {
		t.Fatal(err)
	}
	authority.serial++
	template := &x509.Certificate{SerialNumber: big.NewInt(authority.serial), Subject: pkix.Name{CommonName: "ignored"},
		NotBefore: gatewayTLSNow.Add(-time.Hour), NotAfter: gatewayTLSNow.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, ExtraExtensions: []pkix.Extension{extension}}
	encoded, err := x509.CreateCertificate(rand.Reader, template, authority.certificate, &key.PublicKey, authority.key)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := rafttransport.NewPeerTLS(rafttransport.PeerTLSOptions{IdentityOID: gatewayTLSOID, Identity: identity,
		Certificate: tls.Certificate{Certificate: [][]byte{encoded, authority.certificate.Raw}, PrivateKey: key}, Roots: authority.roots,
		Now: func() time.Time { return gatewayTLSNow }})
	if err != nil {
		t.Fatal(err)
	}
	return profile
}
