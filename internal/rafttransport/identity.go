package rafttransport

import (
	"bytes"
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"errors"
	"fmt"
	"math"
	"net"
	"slices"
	"time"
)

var (
	ErrInvalidPeerIdentity = errors.New("rafttransport: invalid peer certificate identity")
	ErrPeerAuthentication  = errors.New("rafttransport: peer authentication failed")
	ErrWrongTrustDomain    = errors.New("rafttransport: peer trust domain differs")
	ErrWrongPeer           = errors.New("rafttransport: authenticated peer differs")
	ErrWrongTrafficClass   = errors.New("rafttransport: peer traffic class differs")
)

const peerIdentityExtensionBytes = 48

const (
	// AbsoluteMaxPeerCertificateChain bounds one presented leaf and its chain.
	AbsoluteMaxPeerCertificateChain = 8
	// AbsoluteMaxPeerCertificateDERBytes bounds the complete presented chain.
	AbsoluteMaxPeerCertificateDERBytes = 1 << 20
)

var privateEnterpriseOIDPrefix = [...]int{1, 3, 6, 1, 4, 1}

const (
	peerIdentityOIDVibeDBArc = 1
	peerIdentityOIDTypeArc   = 1
)

// TrustDomain identifies one cluster certificate domain. It is intentionally
// two fixed binary coordinates rather than a DNS name or a certificate subject.
type TrustDomain struct {
	ClusterID          [16]byte
	ClusterIncarnation [16]byte
}

// PeerIdentity is the sole transport principal accepted from a peer leaf
// certificate.
type PeerIdentity struct {
	TrustDomain TrustDomain
	Node        NodeID
}

// TrafficClass keeps ordinary Raft messages and snapshot transfer on separate
// authenticated streams. The ordinary transport in this package never opens or
// consumes snapshot streams.
type TrafficClass uint8

const (
	TrafficOrdinary TrafficClass = 1
	TrafficSnapshot TrafficClass = 2
)

const (
	ordinaryALPN = "vibedb-raft-ordinary"
	snapshotALPN = "vibedb-raft-snapshot"
)

func (class TrafficClass) alpn() (string, error) {
	switch class {
	case TrafficOrdinary:
		return ordinaryALPN, nil
	case TrafficSnapshot:
		return snapshotALPN, nil
	default:
		return "", ErrWrongTrafficClass
	}
}

func validPeerIdentity(identity PeerIdentity) bool {
	return identity.TrustDomain.ClusterID != ([16]byte{}) &&
		identity.TrustDomain.ClusterIncarnation != ([16]byte{}) &&
		identity.Node != (NodeID{})
}

// validPeerIdentityOID accepts exactly the VibeDB peer-identity leaf beneath
// one operator-owned IANA Private Enterprise Number. The operator is
// responsible for assigning the fixed .1.1 sub-arcs within that PEN.
func validPeerIdentityOID(oid asn1.ObjectIdentifier) bool {
	if len(oid) != len(privateEnterpriseOIDPrefix)+3 {
		return false
	}
	for index := range privateEnterpriseOIDPrefix {
		if oid[index] != privateEnterpriseOIDPrefix[index] {
			return false
		}
	}
	pen := int64(oid[len(privateEnterpriseOIDPrefix)])
	return pen > 0 && pen < math.MaxUint32 &&
		oid[len(privateEnterpriseOIDPrefix)+1] == peerIdentityOIDVibeDBArc &&
		oid[len(privateEnterpriseOIDPrefix)+2] == peerIdentityOIDTypeArc
}

// PeerIdentityExtension returns the canonical critical X.509 extension for an
// enrolled peer identity. oid must be the fixed .1.1 identity leaf beneath an
// operator-owned IANA Private Enterprise Number. Certificate issuance is
// outside the hot path.
func PeerIdentityExtension(
	oid asn1.ObjectIdentifier,
	identity PeerIdentity,
) (pkix.Extension, error) {
	if !validPeerIdentityOID(oid) || !validPeerIdentity(identity) {
		return pkix.Extension{}, ErrInvalidPeerIdentity
	}
	value := make([]byte, peerIdentityExtensionBytes)
	copy(value[:16], identity.TrustDomain.ClusterID[:])
	copy(value[16:32], identity.TrustDomain.ClusterIncarnation[:])
	copy(value[32:], identity.Node[:])
	return pkix.Extension{
		Id:       slices.Clone(oid),
		Critical: true,
		Value:    value,
	}, nil
}

// ParsePeerIdentity derives the exact transport principal from one leaf
// certificate. It does not consult Subject, CommonName, DNS names, IP names, or
// URI names. The leaf must contain exactly one canonical VibeDB extension.
func ParsePeerIdentity(
	oid asn1.ObjectIdentifier,
	certificate *x509.Certificate,
) (PeerIdentity, error) {
	if !validPeerIdentityOID(oid) || certificate == nil {
		return PeerIdentity{}, ErrInvalidPeerIdentity
	}
	var value []byte
	count := 0
	for _, extension := range certificate.Extensions {
		if !extension.Id.Equal(oid) {
			continue
		}
		count++
		if !extension.Critical || len(extension.Value) != peerIdentityExtensionBytes {
			return PeerIdentity{}, ErrInvalidPeerIdentity
		}
		value = extension.Value
	}
	if count != 1 {
		return PeerIdentity{}, ErrInvalidPeerIdentity
	}
	var identity PeerIdentity
	copy(identity.TrustDomain.ClusterID[:], value[:16])
	copy(identity.TrustDomain.ClusterIncarnation[:], value[16:32])
	copy(identity.Node[:], value[32:])
	if !validPeerIdentity(identity) {
		return PeerIdentity{}, ErrInvalidPeerIdentity
	}
	return identity, nil
}

// PeerTLSOptions owns the trust and local credential inputs for one peer TLS
// foundation. Roots and certificate DER are copied by NewPeerTLS. PrivateKey
// must be a crypto.Signer whose public key matches the leaf, and it must remain
// immutable for the returned object's lifetime. IdentityOID must belong to the
// operator. Now is required so certificate-time verification is explicit even
// though DNS-name verification is deliberately disabled.
type PeerTLSOptions struct {
	IdentityOID asn1.ObjectIdentifier
	Identity    PeerIdentity
	Certificate tls.Certificate
	Roots       *x509.CertPool
	Now         func() time.Time
}

// PeerTLS constructs mutually authenticated TLS configurations and derives the
// complete peer identity after a completed handshake.
type PeerTLS struct {
	identityOID asn1.ObjectIdentifier
	identity    PeerIdentity
	certificate tls.Certificate
	roots       *x509.CertPool
	now         func() time.Time
}

// LocalIdentity returns the exact certificate-bound identity authenticated by
// this profile. The value contains no credential material and is safe to use
// when composing TLS with a static transport registry.
func (peerTLS *PeerTLS) LocalIdentity() PeerIdentity {
	if peerTLS == nil {
		return PeerIdentity{}
	}
	return peerTLS.identity
}

// NewPeerTLS validates and detaches the local current-format TLS profile.
func NewPeerTLS(options PeerTLSOptions) (*PeerTLS, error) {
	if !validPeerIdentityOID(options.IdentityOID) || !validPeerIdentity(options.Identity) ||
		options.Roots == nil || options.Now == nil ||
		!validEncodedCertificateChainBounds(options.Certificate.Certificate) ||
		options.Certificate.PrivateKey == nil {
		return nil, ErrInvalidPeerIdentity
	}
	certificate := cloneTLSCertificate(options.Certificate)
	chain, err := parseCertificateChain(certificate.Certificate)
	if err != nil {
		return nil, err
	}
	identity, err := ParsePeerIdentity(options.IdentityOID, chain[0])
	if err != nil || identity != options.Identity {
		return nil, errors.Join(ErrInvalidPeerIdentity, err)
	}
	if !validPeerLeaf(chain[0]) {
		return nil, fmt.Errorf("%w: local leaf profile", ErrInvalidPeerIdentity)
	}
	if err := validatePeerTLS13Signer(certificate, chain[0]); err != nil {
		return nil, err
	}
	peerTLS := &PeerTLS{
		identityOID: slices.Clone(options.IdentityOID),
		identity:    options.Identity, certificate: certificate,
		roots: options.Roots.Clone(), now: options.Now,
	}
	for _, usage := range []x509.ExtKeyUsage{
		x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth,
	} {
		if _, err := peerTLS.verifyCertificateChain(chain, usage); err != nil {
			return nil, fmt.Errorf("%w: local certificate: %w", ErrPeerAuthentication, err)
		}
	}
	return peerTLS, nil
}

var peerTLS13SignatureSchemes = []tls.SignatureScheme{
	tls.Ed25519,
	tls.ECDSAWithP256AndSHA256,
	tls.ECDSAWithP384AndSHA384,
	tls.ECDSAWithP521AndSHA512,
	tls.PSSWithSHA256,
	tls.PSSWithSHA384,
	tls.PSSWithSHA512,
}

// validatePeerTLS13Signer rejects an unusable local credential before any
// listener or dialer can publish it. The public-key comparison is independent
// of the concrete private-key type, so hardware-backed crypto.Signer values
// remain supported.
func validatePeerTLS13Signer(certificate tls.Certificate, leaf *x509.Certificate) error {
	signer, ok := certificate.PrivateKey.(crypto.Signer)
	if !ok || signer.Public() == nil || leaf == nil {
		return fmt.Errorf("%w: local private key is not a signer", ErrInvalidPeerIdentity)
	}
	publicDER, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil || !bytes.Equal(publicDER, leaf.RawSubjectPublicKeyInfo) {
		return fmt.Errorf("%w: local private key differs from leaf", ErrInvalidPeerIdentity)
	}
	candidate := certificate
	candidate.Leaf = leaf
	hello := tls.ClientHelloInfo{
		SupportedVersions: []uint16{tls.VersionTLS13},
		SignatureSchemes:  peerTLS13SignatureSchemes,
	}
	if err := hello.SupportsCertificate(&candidate); err != nil {
		return fmt.Errorf("%w: local private key cannot sign TLS 1.3: %v", ErrInvalidPeerIdentity, err)
	}
	return nil
}

func cloneTLSCertificate(source tls.Certificate) tls.Certificate {
	result := source
	result.Certificate = make([][]byte, len(source.Certificate))
	for index := range source.Certificate {
		result.Certificate[index] = slices.Clone(source.Certificate[index])
	}
	result.OCSPStaple = slices.Clone(source.OCSPStaple)
	result.SignedCertificateTimestamps = make([][]byte, len(source.SignedCertificateTimestamps))
	for index := range source.SignedCertificateTimestamps {
		result.SignedCertificateTimestamps[index] = slices.Clone(source.SignedCertificateTimestamps[index])
	}
	result.Leaf = nil
	return result
}

func parseCertificateChain(encoded [][]byte) ([]*x509.Certificate, error) {
	if !validEncodedCertificateChainBounds(encoded) {
		return nil, ErrInvalidPeerIdentity
	}
	result := make([]*x509.Certificate, len(encoded))
	for index := range encoded {
		certificate, err := x509.ParseCertificate(encoded[index])
		if err != nil {
			return nil, fmt.Errorf("%w: parse certificate %d: %w", ErrInvalidPeerIdentity, index, err)
		}
		result[index] = certificate
	}
	return result, nil
}

func validEncodedCertificateChainBounds(encoded [][]byte) bool {
	if len(encoded) == 0 || len(encoded) > AbsoluteMaxPeerCertificateChain {
		return false
	}
	total := 0
	for _, certificate := range encoded {
		if len(certificate) == 0 ||
			len(certificate) > AbsoluteMaxPeerCertificateDERBytes-total {
			return false
		}
		total += len(certificate)
	}
	return true
}

func hasExplicitExtendedKeyUsage(certificate *x509.Certificate, usage x509.ExtKeyUsage) bool {
	for _, candidate := range certificate.ExtKeyUsage {
		if candidate == usage {
			return true
		}
	}
	return false
}

func validPeerLeaf(certificate *x509.Certificate) bool {
	return certificate != nil && !certificate.IsCA &&
		certificate.KeyUsage&x509.KeyUsageDigitalSignature != 0 &&
		hasExplicitExtendedKeyUsage(certificate, x509.ExtKeyUsageClientAuth) &&
		hasExplicitExtendedKeyUsage(certificate, x509.ExtKeyUsageServerAuth)
}

func handledPeerIdentityLeaf(
	source *x509.Certificate,
	oid asn1.ObjectIdentifier,
) *x509.Certificate {
	leaf := *source
	leaf.UnhandledCriticalExtensions = slices.DeleteFunc(
		slices.Clone(source.UnhandledCriticalExtensions),
		func(candidate asn1.ObjectIdentifier) bool { return candidate.Equal(oid) },
	)
	return &leaf
}

func (peerTLS *PeerTLS) verifyCertificateChain(
	chain []*x509.Certificate,
	usage x509.ExtKeyUsage,
) (PeerIdentity, error) {
	if peerTLS == nil || peerTLS.roots == nil || len(chain) == 0 ||
		len(chain) > AbsoluteMaxPeerCertificateChain {
		return PeerIdentity{}, ErrPeerAuthentication
	}
	total := 0
	for _, certificate := range chain {
		if certificate == nil || len(certificate.Raw) == 0 ||
			len(certificate.Raw) > AbsoluteMaxPeerCertificateDERBytes-total {
			return PeerIdentity{}, ErrPeerAuthentication
		}
		total += len(certificate.Raw)
	}
	identity, err := ParsePeerIdentity(peerTLS.identityOID, chain[0])
	if err != nil {
		return PeerIdentity{}, errors.Join(ErrPeerAuthentication, err)
	}
	if !validPeerLeaf(chain[0]) {
		return PeerIdentity{}, fmt.Errorf("%w: peer leaf profile", ErrPeerAuthentication)
	}
	intermediates := x509.NewCertPool()
	for _, certificate := range chain[1:] {
		intermediates.AddCert(certificate)
	}
	verifyOptions := x509.VerifyOptions{
		Roots: peerTLS.roots, Intermediates: intermediates,
		KeyUsages: []x509.ExtKeyUsage{usage},
	}
	now := peerTLS.now()
	if now.IsZero() {
		return PeerIdentity{}, fmt.Errorf("%w: zero certificate time", ErrPeerAuthentication)
	}
	verifyOptions.CurrentTime = now
	if _, err := handledPeerIdentityLeaf(chain[0], peerTLS.identityOID).Verify(verifyOptions); err != nil {
		return PeerIdentity{}, errors.Join(ErrPeerAuthentication, err)
	}
	if identity.TrustDomain != peerTLS.identity.TrustDomain {
		return PeerIdentity{}, ErrWrongTrustDomain
	}
	return identity, nil
}

func (peerTLS *PeerTLS) verifyConnection(
	state tls.ConnectionState,
	usage x509.ExtKeyUsage,
	expected NodeID,
	class TrafficClass,
) error {
	protocol, err := class.alpn()
	if err != nil || state.Version < tls.VersionTLS13 ||
		state.NegotiatedProtocol != protocol {
		return errors.Join(ErrWrongTrafficClass, err)
	}
	identity, err := peerTLS.verifyCertificateChain(state.PeerCertificates, usage)
	if err != nil {
		return err
	}
	if identity.Node == peerTLS.identity.Node {
		return ErrWrongPeer
	}
	if expected != (NodeID{}) && identity.Node != expected {
		return ErrWrongPeer
	}
	return nil
}

// ClientConfig returns a detached TLS 1.3 configuration that verifies the
// normal server certificate chain without DNS identity and then requires the
// exact trust domain, NodeID, private extension, and traffic class.
func (peerTLS *PeerTLS) ClientConfig(expected NodeID, class TrafficClass) (*tls.Config, error) {
	protocol, err := class.alpn()
	if peerTLS == nil || expected == (NodeID{}) || err != nil {
		return nil, errors.Join(ErrInvalidPeerIdentity, err)
	}
	config := &tls.Config{
		Certificates:           []tls.Certificate{cloneTLSCertificate(peerTLS.certificate)},
		MinVersion:             tls.VersionTLS13,
		NextProtos:             []string{protocol},
		InsecureSkipVerify:     true,
		SessionTicketsDisabled: true,
		Time:                   peerTLS.now,
	}
	config.VerifyConnection = func(state tls.ConnectionState) error {
		return peerTLS.verifyConnection(state, x509.ExtKeyUsageServerAuth, expected, class)
	}
	return config, nil
}

// ServerConfig returns a detached TLS 1.3 configuration that requires and
// verifies one client certificate under the exact trust domain and traffic
// class. StaticRegistry frame admission later binds that complete identity to
// each source member and frame group.
func (peerTLS *PeerTLS) ServerConfig(class TrafficClass) (*tls.Config, error) {
	protocol, err := class.alpn()
	if peerTLS == nil || err != nil {
		return nil, errors.Join(ErrInvalidPeerIdentity, err)
	}
	config := &tls.Config{
		Certificates:           []tls.Certificate{cloneTLSCertificate(peerTLS.certificate)},
		MinVersion:             tls.VersionTLS13,
		NextProtos:             []string{protocol},
		ClientAuth:             tls.RequireAnyClientCert,
		SessionTicketsDisabled: true,
		Time:                   peerTLS.now,
	}
	config.VerifyConnection = func(state tls.ConnectionState) error {
		return peerTLS.verifyConnection(state, x509.ExtKeyUsageClientAuth, NodeID{}, class)
	}
	return config, nil
}

// DeadlineFunc returns an absolute I/O deadline. The transport rejects nil or
// zero results instead of installing an unbounded hidden timeout.
type DeadlineFunc func() time.Time

// PeerConnection is a completed mutually authenticated stream. Production
// implementations returned by PeerTLS derive both fields from TLS state.
// Tests can implement this interface with bounded in-memory connections.
type PeerConnection interface {
	net.Conn
	PeerIdentity() PeerIdentity
	TrafficClass() TrafficClass
}

type authenticatedPeerConnection struct {
	net.Conn
	identity PeerIdentity
	class    TrafficClass
}

func (connection *authenticatedPeerConnection) PeerIdentity() PeerIdentity {
	if connection == nil {
		return PeerIdentity{}
	}
	return connection.identity
}

func (connection *authenticatedPeerConnection) TrafficClass() TrafficClass {
	if connection == nil {
		return 0
	}
	return connection.class
}

// Client authenticates an owned raw connection as expected. The method closes
// raw on every error and returns a TLS-derived PeerConnection on success.
func (peerTLS *PeerTLS) Client(
	ctx context.Context,
	raw net.Conn,
	expected NodeID,
	class TrafficClass,
	handshakeDeadline DeadlineFunc,
) (PeerConnection, error) {
	if ctx == nil {
		if raw != nil {
			_ = raw.Close()
		}
		return nil, ErrPeerAuthentication
	}
	config, err := peerTLS.ClientConfig(expected, class)
	if err != nil {
		if raw != nil {
			_ = raw.Close()
		}
		return nil, err
	}
	return peerTLS.handshake(ctx, raw, tls.Client, config, expected, class, handshakeDeadline)
}

// Server authenticates an owned raw connection as one peer in the local trust
// domain. Static frame admission decides whether that identity can send a given
// group member's traffic.
func (peerTLS *PeerTLS) Server(
	ctx context.Context,
	raw net.Conn,
	class TrafficClass,
	handshakeDeadline DeadlineFunc,
) (PeerConnection, error) {
	if ctx == nil {
		if raw != nil {
			_ = raw.Close()
		}
		return nil, ErrPeerAuthentication
	}
	config, err := peerTLS.ServerConfig(class)
	if err != nil {
		if raw != nil {
			_ = raw.Close()
		}
		return nil, err
	}
	return peerTLS.handshake(ctx, raw, tls.Server, config, NodeID{}, class, handshakeDeadline)
}

type tlsConstructor func(net.Conn, *tls.Config) *tls.Conn

func (peerTLS *PeerTLS) handshake(
	ctx context.Context,
	raw net.Conn,
	constructor tlsConstructor,
	config *tls.Config,
	expected NodeID,
	class TrafficClass,
	handshakeDeadline DeadlineFunc,
) (PeerConnection, error) {
	if raw == nil || handshakeDeadline == nil {
		if raw != nil {
			_ = raw.Close()
		}
		return nil, ErrPeerAuthentication
	}
	deadline := handshakeDeadline()
	if deadline.IsZero() {
		_ = raw.Close()
		return nil, ErrPeerAuthentication
	}
	if err := raw.SetDeadline(deadline); err != nil {
		_ = raw.Close()
		return nil, errors.Join(ErrPeerAuthentication, err)
	}
	connection := constructor(raw, config)
	if err := connection.HandshakeContext(ctx); err != nil {
		_ = connection.Close()
		return nil, errors.Join(ErrPeerAuthentication, err)
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		_ = connection.Close()
		return nil, errors.Join(ErrPeerAuthentication, err)
	}
	state := connection.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		_ = connection.Close()
		return nil, ErrPeerAuthentication
	}
	identity, err := ParsePeerIdentity(peerTLS.identityOID, state.PeerCertificates[0])
	if err != nil || identity.TrustDomain != peerTLS.identity.TrustDomain ||
		(expected != (NodeID{}) && identity.Node != expected) {
		_ = connection.Close()
		return nil, errors.Join(ErrPeerAuthentication, err)
	}
	return &authenticatedPeerConnection{
		Conn: connection, identity: identity, class: class,
	}, nil
}
