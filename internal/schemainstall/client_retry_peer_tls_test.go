package schemainstall

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestClientMarksPeerTLSOpenEOFTransientAndRetriesExactPrepare(t *testing.T) {
	request, _, _, bundle := schemaFixture(91)
	profiles := newRetryPeerTLSProfiles(t, 91)
	opener := newPeerTLSRetryOpener(profiles, peerTLSCloseAfterClientHello)
	client := newPeerTLSRetryClient(t, opener)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.Prepare(ctx, profiles.serverPeer.Node, request, bundle); err == nil ||
		!errors.Is(err, ErrTransientControlOpen) || !errors.Is(err, rafttransport.ErrPeerAuthentication) ||
		(!errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF)) ||
		errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("close during peer TLS open error=%v, want transient pre-request EOF preserving authentication cause", err)
	}
	if opener.firstConnectionReturned {
		t.Fatal("failed PeerTLS.Client returned a connection")
	}
	if err := <-opener.firstServerDone; err != nil {
		t.Fatalf("consume complete ClientHello and close: %v", err)
	}

	receipt, err := client.Prepare(ctx, profiles.serverPeer.Node, request, bundle)
	if err != nil {
		t.Fatalf("retry exact prepare over authenticated connection: %v", err)
	}
	if receipt.Group != request.Group || receipt.ToSchemaGeneration != request.ToSchemaGeneration ||
		receipt.ToRelationManifestDigest != request.ToRelationManifestDigest ||
		receipt.ContractDigest != ContractDigest() || receipt.InstallationDigest != ([32]byte{0x91}) {
		t.Fatalf("retry receipt=%+v, does not match exact prepared request", receipt)
	}
	observation := <-opener.observations
	if observation.command != CommandPrepare || observation.request != request ||
		observation.authorization != (Authorization{}) || observation.proof != (DrainProof{}) ||
		!bytes.Equal(observation.bundle, bundle) {
		t.Fatalf("authenticated retry command=%d request-match=%t bundle-match=%t",
			observation.command, observation.request == request, bytes.Equal(observation.bundle, bundle))
	}
	if err := <-opener.serverDone; err != nil {
		t.Fatalf("serve authenticated retry: %v", err)
	}
	if got := opener.schemaRequests.Load(); got != 1 {
		t.Fatalf("schema requests sent=%d, want only the authenticated retry", got)
	}
	if got := opener.calls.Load(); got != 2 {
		t.Fatalf("control opens=%d, want failed TLS open plus one exact retry", got)
	}
}

func TestClientMarksPeerTLSOpenUnexpectedEOFTransient(t *testing.T) {
	request, _, _, bundle := schemaFixture(92)
	profiles := newRetryPeerTLSProfiles(t, 92)
	opener := newPeerTLSRetryOpener(profiles, peerTLSTruncateServerHelloRecord)
	client := newPeerTLSRetryClient(t, opener)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Prepare(ctx, profiles.serverPeer.Node, request, bundle)
	if err == nil || !errors.Is(err, ErrTransientControlOpen) ||
		!errors.Is(err, rafttransport.ErrPeerAuthentication) || !errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("truncated peer TLS response error=%v, want transient unexpected EOF before request", err)
	}
	if opener.firstConnectionReturned {
		t.Fatal("truncated PeerTLS.Client returned a connection")
	}
	if err := <-opener.firstServerDone; err != nil {
		t.Fatalf("consume ClientHello and send truncated TLS record: %v", err)
	}
	if got := opener.schemaRequests.Load(); got != 0 {
		t.Fatalf("truncated handshake sent %d schema requests, want none", got)
	}
}

func TestClientDoesNotClassifyPostCommandEOFAsTransientOpen(t *testing.T) {
	request, authorization, _, _ := schemaFixture(93)
	profiles := newRetryPeerTLSProfiles(t, 93)
	opener := newPeerTLSRetryOpener(profiles, 0)
	opener.closeAfterRequest = true
	client := newPeerTLSRetryClient(t, opener)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Commit(ctx, profiles.serverPeer.Node, request, authorization)
	if !errors.Is(err, ErrOutcomeUnknown) || errors.Is(err, ErrTransientControlOpen) || !errors.Is(err, io.EOF) {
		t.Fatalf("EOF after accepted commit command=%v, want outcome unknown without open-retry marker", err)
	}
	observation := <-opener.observations
	if observation.command != CommandCommit || observation.request != request ||
		observation.authorization != authorization || observation.proof != (DrainProof{}) {
		t.Fatalf("server accepted different commit command: %+v", observation)
	}
	if err := <-opener.serverDone; err != nil {
		t.Fatalf("close authenticated server after command: %v", err)
	}
	if got := opener.schemaRequests.Load(); got != 1 {
		t.Fatalf("schema requests accepted=%d, want one complete commit", got)
	}
}

type retryPeerTLSProfiles struct {
	client     *rafttransport.PeerTLS
	server     *rafttransport.PeerTLS
	serverPeer rafttransport.PeerIdentity
}

func newRetryPeerTLSProfiles(t testing.TB, seed byte) retryPeerTLSProfiles {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	rootKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rootTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "schemainstall-test-root"},
		NotBefore: now.Add(-24 * time.Hour), NotAfter: now.Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	rootDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate,
		&rootKey.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	root, err := x509.ParseCertificate(rootDER)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(root)
	trust := rafttransport.TrustDomain{ClusterID: [16]byte{seed}, ClusterIncarnation: [16]byte{seed + 1}}
	clientPeer := rafttransport.PeerIdentity{TrustDomain: trust, Node: rafttransport.NodeID{0x93}}
	serverPeer := rafttransport.PeerIdentity{TrustDomain: trust, Node: rafttransport.NodeID{0x94}}
	oid := asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}
	newProfile := func(identity rafttransport.PeerIdentity, serial int64) *rafttransport.PeerTLS {
		t.Helper()
		key, keyErr := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		extension, extensionErr := rafttransport.PeerIdentityExtension(oid, identity)
		if extensionErr != nil {
			t.Fatal(extensionErr)
		}
		leafTemplate := &x509.Certificate{
			SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "ignored-peer-name"},
			NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
			KeyUsage:        x509.KeyUsageDigitalSignature,
			ExtKeyUsage:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
			ExtraExtensions: []pkix.Extension{extension},
		}
		leafDER, certErr := x509.CreateCertificate(rand.Reader, leafTemplate, root,
			&key.PublicKey, rootKey)
		if certErr != nil {
			t.Fatal(certErr)
		}
		profile, profileErr := rafttransport.NewPeerTLS(rafttransport.PeerTLSOptions{
			IdentityOID: oid, Identity: identity,
			Certificate: tls.Certificate{Certificate: [][]byte{leafDER, rootDER}, PrivateKey: key},
			Roots:       roots, Now: func() time.Time { return now },
		})
		if profileErr != nil {
			t.Fatalf("create test peer TLS profile: %v", profileErr)
		}
		return profile
	}
	return retryPeerTLSProfiles{client: newProfile(clientPeer, 2), server: newProfile(serverPeer, 3),
		serverPeer: serverPeer}
}

type peerTLSOpenFailure uint8

const (
	peerTLSCloseAfterClientHello peerTLSOpenFailure = iota + 1
	peerTLSTruncateServerHelloRecord
)

type peerTLSRetryObservation struct {
	command       Command
	request       Request
	authorization Authorization
	proof         DrainProof
	bundle        []byte
}

type peerTLSRetryOpener struct {
	profiles                retryPeerTLSProfiles
	firstFailure            peerTLSOpenFailure
	closeAfterRequest       bool
	calls                   atomic.Int32
	schemaRequests          atomic.Int32
	firstConnectionReturned bool
	firstServerDone         chan error
	serverDone              chan error
	observations            chan peerTLSRetryObservation
}

func newPeerTLSRetryOpener(profiles retryPeerTLSProfiles, firstFailure peerTLSOpenFailure) *peerTLSRetryOpener {
	return &peerTLSRetryOpener{profiles: profiles, firstFailure: firstFailure,
		firstServerDone: make(chan error, 1), serverDone: make(chan error, 1),
		observations: make(chan peerTLSRetryObservation, 1)}
}

func newPeerTLSRetryClient(t testing.TB, opener *peerTLSRetryOpener) *Client {
	t.Helper()
	deadline := func() time.Time { return time.Now().Add(3 * time.Second) }
	client, err := NewClient(ClientOptions{Opener: opener, ReadDeadline: deadline, WriteDeadline: deadline})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func (opener *peerTLSRetryOpener) OpenShardControl(
	ctx context.Context, node rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	call := opener.calls.Add(1)
	if node != opener.profiles.serverPeer.Node {
		return nil, rafttransport.ErrWrongPeer
	}
	clientRaw, serverRaw := net.Pipe()
	deadline := func() time.Time { return time.Now().Add(3 * time.Second) }
	if call == 1 && opener.firstFailure != 0 {
		go func() {
			var result error
			defer func() {
				_ = serverRaw.Close()
				opener.firstServerDone <- result
			}()
			if err := serverRaw.SetDeadline(deadline()); err != nil {
				result = err
				return
			}
			result = readCompleteTLSClientHello(serverRaw)
			if result == nil && opener.firstFailure == peerTLSTruncateServerHelloRecord {
				_, result = serverRaw.Write([]byte{22, 3, 3, 0, 8})
				if result == nil {
					_, result = serverRaw.Write([]byte{2, 0, 0})
				}
			}
		}()
		connection, err := opener.profiles.client.Client(ctx, clientRaw, node,
			rafttransport.TrafficShardControl, deadline)
		opener.firstConnectionReturned = connection != nil
		if connection != nil {
			_ = connection.Close()
		}
		return connection, err
	}
	go opener.serveAuthenticated(ctx, serverRaw, deadline)
	return opener.profiles.client.Client(ctx, clientRaw, node,
		rafttransport.TrafficShardControl, deadline)
}

func (opener *peerTLSRetryOpener) serveAuthenticated(
	ctx context.Context, raw net.Conn, deadline rafttransport.DeadlineFunc,
) {
	var peerConnection rafttransport.PeerConnection
	var result error
	defer func() {
		_ = raw.Close()
		if peerConnection != nil {
			_ = peerConnection.Close()
		}
		opener.serverDone <- result
	}()
	peerConnection, result = opener.profiles.server.Server(ctx, raw,
		rafttransport.TrafficShardControl, deadline)
	if result != nil {
		return
	}
	if result = peerConnection.SetReadDeadline(deadline()); result != nil {
		return
	}
	command, request, authorization, proof, err := ReadControlRequest(peerConnection)
	if err != nil {
		result = err
		return
	}
	var bundle []byte
	if command == CommandPrepare {
		bundle = make([]byte, int(request.BundleBytes))
		if _, result = io.ReadFull(peerConnection, bundle); result != nil {
			return
		}
	}
	opener.schemaRequests.Add(1)
	opener.observations <- peerTLSRetryObservation{command: command, request: request,
		authorization: authorization, proof: proof, bundle: bundle}
	if opener.closeAfterRequest {
		result = peerConnection.Close()
		peerConnection = nil
		return
	}
	record := Record{Request: request, Revision: 1, State: StatePrepared, Installation: [32]byte{0x91}}
	if command != CommandPrepare {
		record.Revision, record.State, record.Authorization = 2, StateAuthorized, authorization
	}
	result = (&ControlService{}).writeResponse(peerConnection, ResponseOK, record)
}

func readCompleteTLSClientHello(connection net.Conn) error {
	var handshake []byte
	for {
		if len(handshake) >= 4 {
			messageBytes := int(handshake[1])<<16 | int(handshake[2])<<8 | int(handshake[3])
			if len(handshake) >= 4+messageBytes {
				if handshake[0] != 1 {
					return fmt.Errorf("first TLS handshake message type=%d, want ClientHello", handshake[0])
				}
				return nil
			}
		}
		var header [5]byte
		if _, err := io.ReadFull(connection, header[:]); err != nil {
			return err
		}
		if header[0] != 22 {
			return fmt.Errorf("TLS record type=%d before complete ClientHello", header[0])
		}
		length := int(header[3])<<8 | int(header[4])
		if length == 0 || length > 1<<14 {
			return fmt.Errorf("invalid TLS handshake record length %d", length)
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(connection, payload); err != nil {
			return err
		}
		handshake = append(handshake, payload...)
	}
}

func TestControlOpenTransientEOFPrecedence(t *testing.T) {
	eofCauses := []struct {
		name string
		err  error
	}{{name: "EOF", err: io.EOF}, {name: "unexpected EOF", err: io.ErrUnexpectedEOF}}
	rejections := []struct {
		name string
		err  error
	}{
		{name: "certificate verification", err: &tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}},
		{name: "TLS alert", err: tls.AlertError(42)},
		{name: "identity", err: rafttransport.ErrWrongPeer},
		{name: "key", err: rafttransport.ErrPeerKeyMismatch},
		{name: "enrollment", err: rafttransport.ErrPeerUnauthorized},
		{name: "build", err: rafttransport.ErrPeerBuild},
		{name: "authorization", err: rafttransport.ErrUnauthorized},
	}
	for _, eof := range eofCauses {
		t.Run(eof.name, func(t *testing.T) {
			if !transientControlOpenFailure(errors.Join(rafttransport.ErrPeerAuthentication, eof.err)) {
				t.Fatal("peer-authenticated transport EOF was not transient")
			}
			for _, rejection := range rejections {
				t.Run(rejection.name, func(t *testing.T) {
					joined := errors.Join(rafttransport.ErrPeerAuthentication, eof.err, rejection.err)
					if transientControlOpenFailure(joined) {
						t.Fatalf("hard rejection was masked by opening EOF: %v", joined)
					}
				})
			}
		})
	}
	if transientControlOpenFailure(rafttransport.ErrPeerAuthentication) {
		t.Fatal("bare peer-authentication rejection became transient")
	}
	customCause := errors.New("caller stopped open")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	client := newTestRetryClientWithOpener(t, controlRetryTestOpener(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
		cancel(customCause)
		return nil, errors.Join(rafttransport.ErrPeerAuthentication, io.EOF)
	}))
	request, _, _, bundle := schemaFixture(94)
	_, err := client.Prepare(ctx, rafttransport.NodeID{9}, request, bundle)
	if !errors.Is(err, customCause) || !errors.Is(err, io.EOF) || errors.Is(err, ErrTransientControlOpen) {
		t.Fatalf("canceled opening EOF=%v, want cause and EOF without retry marker", err)
	}
}

func newTestRetryClientWithOpener(t testing.TB, opener StreamOpener) *Client {
	t.Helper()
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	client, err := NewClient(ClientOptions{Opener: opener, ReadDeadline: deadline, WriteDeadline: deadline})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

var _ StreamOpener = (*peerTLSRetryOpener)(nil)
