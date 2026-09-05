package shardservice

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

var shardTLSNow = time.Date(2034, 2, 3, 4, 5, 6, 0, time.UTC)
var shardTLSOID = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}

type shardTLSAuthority struct {
	certificate *x509.Certificate
	key         *ecdsa.PrivateKey
	roots       *x509.CertPool
	serial      int64
}

func newShardTLSAuthority(t testing.TB) *shardTLSAuthority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true,
		NotBefore: shardTLSNow.Add(-time.Hour), NotAfter: shardTLSNow.Add(time.Hour), KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
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
	return &shardTLSAuthority{certificate: certificate, key: key, roots: roots, serial: 1}
}

func shardPeerIdentity(domain, node byte) (identity rafttransport.PeerIdentity) {
	for index := range identity.TrustDomain.ClusterID {
		identity.TrustDomain.ClusterID[index] = domain + byte(index)
		identity.TrustDomain.ClusterIncarnation[index] = domain + 32 + byte(index)
		identity.Node[index] = node + byte(index)
	}
	return identity
}

func (authority *shardTLSAuthority) profile(t testing.TB, identity rafttransport.PeerIdentity) *rafttransport.PeerTLS {
	t.Helper()
	return authority.profileClock(t, identity, func() time.Time { return shardTLSNow })
}

func (authority *shardTLSAuthority) profileClock(t testing.TB, identity rafttransport.PeerIdentity, now func() time.Time) *rafttransport.PeerTLS {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	extension, err := rafttransport.PeerIdentityExtension(shardTLSOID, identity)
	if err != nil {
		t.Fatal(err)
	}
	authority.serial++
	template := &x509.Certificate{SerialNumber: big.NewInt(authority.serial), Subject: pkix.Name{CommonName: "ignored"},
		NotBefore: shardTLSNow.Add(-time.Hour), NotAfter: shardTLSNow.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, ExtraExtensions: []pkix.Extension{extension}}
	encoded, err := x509.CreateCertificate(rand.Reader, template, authority.certificate, &key.PublicKey, authority.key)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := rafttransport.NewPeerTLS(rafttransport.PeerTLSOptions{IdentityOID: shardTLSOID, Identity: identity,
		Certificate: tls.Certificate{Certificate: [][]byte{encoded, authority.certificate.Raw}, PrivateKey: key}, Roots: authority.roots,
		Now: now})
	if err != nil {
		t.Fatal(err)
	}
	return profile
}
