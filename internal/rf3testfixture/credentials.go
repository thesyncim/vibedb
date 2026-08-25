package rf3testfixture

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

// Credential is one certificate/key pair signed by Roots.
type Credential struct {
	Certificate string
	Key         string
}

// WriteCredentials creates exact certificate-bound peer identities under one
// trust domain. Every resulting profile supports both peer and native traffic.
func WriteCredentials(
	root string,
	oid asn1.ObjectIdentifier,
	domain rafttransport.TrustDomain,
	nodes []rafttransport.NodeID,
) ([]Credential, string, error) {
	credentials := make([]Credential, len(nodes))
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, "", err
	}
	now := time.Now().UTC()
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "rf3-command-test-ca"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(2 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(
		rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey,
	)
	if err != nil {
		return nil, "", err
	}
	caCertificate, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, "", err
	}
	roots := filepath.Join(root, "cluster-roots.pem")
	if err := os.WriteFile(
		roots, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600,
	); err != nil {
		return nil, "", err
	}
	for index, node := range nodes {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, "", err
		}
		extension, err := rafttransport.PeerIdentityExtension(
			oid, rafttransport.PeerIdentity{TrustDomain: domain, Node: node},
		)
		if err != nil {
			return nil, "", err
		}
		leaf := &x509.Certificate{
			SerialNumber: big.NewInt(int64(index + 2)),
			Subject:      pkix.Name{CommonName: "rf3-command-test-member"},
			NotBefore:    now.Add(-time.Hour), NotAfter: now.Add(2 * time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{
				x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth,
			},
			ExtraExtensions: []pkix.Extension{extension},
		}
		leafDER, err := x509.CreateCertificate(
			rand.Reader, leaf, caCertificate, &key.PublicKey, caKey,
		)
		if err != nil {
			return nil, "", err
		}
		certificatePath := filepath.Join(root, "member-"+big.NewInt(int64(index+1)).String()+"-cert.pem")
		certificatePEM := append(
			pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
			pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})...,
		)
		if err := os.WriteFile(certificatePath, certificatePEM, 0o600); err != nil {
			return nil, "", err
		}
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			return nil, "", err
		}
		keyPath := filepath.Join(root, "member-"+big.NewInt(int64(index+1)).String()+"-key.pem")
		if err := os.WriteFile(
			keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600,
		); err != nil {
			return nil, "", err
		}
		credentials[index] = Credential{Certificate: certificatePath, Key: keyPath}
	}
	return credentials, roots, nil
}
