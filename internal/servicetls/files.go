package servicetls

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/asn1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

const AbsoluteMaxCredentialPEMBytes = 1 << 20

// LoadProfile loads one bounded PEM credential set and derives the local
// binary identity from the critical certificate extension. Subjects and DNS
// names are never treated as identities.
func LoadProfile(certificatePath, keyPath, rootsPath, identityOID string, now func() time.Time) (*rafttransport.PeerTLS, error) {
	if certificatePath == "" || keyPath == "" || rootsPath == "" || now == nil {
		return nil, ErrInvalidProfile
	}
	oid, err := parseOID(identityOID)
	if err != nil {
		return nil, err
	}
	certificatePEM, err := readBounded(certificatePath)
	if err != nil {
		return nil, err
	}
	keyPEM, err := readBounded(keyPath)
	if err != nil {
		return nil, err
	}
	rootsPEM, err := readBounded(rootsPath)
	if err != nil {
		return nil, err
	}
	certificate, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil || len(certificate.Certificate) == 0 {
		return nil, errors.Join(ErrInvalidProfile, err)
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return nil, errors.Join(ErrInvalidProfile, err)
	}
	identity, err := rafttransport.ParsePeerIdentity(oid, leaf)
	if err != nil {
		return nil, errors.Join(ErrInvalidProfile, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(rootsPEM) {
		return nil, ErrInvalidProfile
	}
	return rafttransport.NewPeerTLS(rafttransport.PeerTLSOptions{
		IdentityOID: oid, Identity: identity, Certificate: certificate, Roots: roots, Now: now,
	})
}

func readBounded(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Join(ErrInvalidProfile, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() <= 0 || info.Size() > AbsoluteMaxCredentialPEMBytes {
		return nil, errors.Join(ErrInvalidProfile, err)
	}
	value := make([]byte, int(info.Size()))
	if _, err = io.ReadFull(file, value); err != nil {
		return nil, errors.Join(ErrInvalidProfile, err)
	}
	var trailing [1]byte
	if count, readErr := file.Read(trailing[:]); count != 0 || !errors.Is(readErr, io.EOF) {
		return nil, ErrInvalidProfile
	}
	return value, nil
}

func parseOID(value string) (asn1.ObjectIdentifier, error) {
	parts := strings.Split(value, ".")
	if len(parts) != 9 {
		return nil, ErrInvalidProfile
	}
	result := make(asn1.ObjectIdentifier, len(parts))
	for index, part := range parts {
		coordinate, err := strconv.ParseUint(part, 10, 31)
		if err != nil {
			return nil, errors.Join(ErrInvalidProfile, err)
		}
		result[index] = int(coordinate)
	}
	return result, nil
}

// ParseNodeID parses the sole operator-facing representation of a certificate
// NodeID. The hot path retains only the resulting 16 bytes.
func ParseNodeID(value string) (rafttransport.NodeID, error) {
	var result rafttransport.NodeID
	if len(value) != hex.EncodedLen(len(result)) {
		return result, ErrInvalidProfile
	}
	if _, err := hex.Decode(result[:], []byte(value)); err != nil {
		return rafttransport.NodeID{}, fmt.Errorf("%w: node identity: %v", ErrInvalidProfile, err)
	}
	if result == (rafttransport.NodeID{}) {
		return rafttransport.NodeID{}, ErrInvalidProfile
	}
	return result, nil
}
