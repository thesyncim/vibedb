package gatewayruntime

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type frontendDrainTestListener struct {
	accepted chan net.Conn
	closed   chan struct{}
	once     sync.Once
}

func newFrontendDrainTestListener() *frontendDrainTestListener {
	return &frontendDrainTestListener{accepted: make(chan net.Conn, 1), closed: make(chan struct{})}
}

func (listener *frontendDrainTestListener) Accept() (net.Conn, error) {
	select {
	case conn := <-listener.accepted:
		return conn, nil
	case <-listener.closed:
		return nil, net.ErrClosed
	}
}

func (listener *frontendDrainTestListener) Close() error {
	listener.once.Do(func() { close(listener.closed) })
	return nil
}

func (listener *frontendDrainTestListener) Addr() net.Addr { return frontendDrainTestAddr{} }

type frontendDrainTestAddr struct{}

func (frontendDrainTestAddr) Network() string { return "frontend-drain-test" }
func (frontendDrainTestAddr) String() string  { return "frontend-drain-test" }

func testFrontendDrainIdentity() FrontendDrainIdentity {
	var identity FrontendDrainIdentity
	identity.NodeID[0] = 1
	identity.Incarnation = 2
	identity.SessionID[0] = 3
	identity.SessionRevision = 4
	identity.NodeRevision = 5
	identity.CatalogGeneration = 6
	identity.DirectoryRevision = 7
	return identity
}

func TestFrontendDrainKeepsAcceptedNativeConnection(t *testing.T) {
	identity := testFrontendDrainIdentity()
	frontend := newFrontendAdmission(identity, false, false)
	listener := newFrontendDrainTestListener()
	wrapped := &frontendAdmissionListener{Listener: listener, frontend: frontend}

	client, peer := net.Pipe()
	listener.accepted <- peer
	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := wrapped.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()
	var conn net.Conn
	select {
	case err := <-acceptErr:
		t.Fatalf("accepted connection rejected before drain: %v", err)
	case conn = <-accepted:
	case <-time.After(time.Second):
		t.Fatal("listener did not accept the native connection")
	}
	defer client.Close()

	runtime := &Runtime{listener: listener, frontend: frontend}
	ack := runtime.BeginFrontendDrain()
	if !ack.AdmissionDrained || ack.ActiveNativeConnections != 1 || ack.SafeToStop {
		t.Fatalf("drain acknowledgement = %+v", ack)
	}
	if !frontend.isDraining() {
		t.Fatal("frontend did not retain the admission-drained state")
	}

	// Closing the public listener does not close an already accepted stream.
	if err := conn.Close(); err != nil {
		t.Fatalf("close accepted connection: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		ack = runtime.FrontendDrainStatus()
		if ack.ActiveNativeConnections == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("accepted connection remained counted: %+v", ack)
		}
		time.Sleep(time.Millisecond)
	}
	if !ack.SafeToStop {
		t.Fatalf("zero-count, identity-bound drain was not safe: %+v", ack)
	}

	afterDrain := make(chan error, 1)
	go func() {
		_, err := wrapped.Accept()
		afterDrain <- err
	}()
	select {
	case err := <-afterDrain:
		if !errors.Is(err, errFrontendAdmissionDrained) {
			t.Fatalf("Accept after drain = %v, want frontend drain sentinel", err)
		}
	case <-time.After(time.Second):
		t.Fatal("closed listener did not acknowledge admission drain")
	}
}

func TestFrontendAdmissionDrainIsIdempotent(t *testing.T) {
	frontend := newFrontendAdmission(testFrontendDrainIdentity(), false, false)
	listener := newFrontendDrainTestListener()
	runtime := &Runtime{listener: listener, frontend: frontend}
	first := runtime.BeginFrontendDrain()
	second := runtime.BeginFrontendDrain()
	if first.Revision == 0 || second.Revision != first.Revision {
		t.Fatalf("drain revision changed across retry: first=%+v second=%+v", first, second)
	}
	if !second.AdmissionDrained || !second.Identity.Valid() {
		t.Fatalf("idempotent drain lost its fence: %+v", second)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFrontendContinuationCredentialSnapshotsOpenNativeSocket(t *testing.T) {
	frontend := newFrontendAdmission(testFrontendDrainIdentity(), false, false)
	listener := newFrontendDrainTestListener()
	wrapped := &frontendAdmissionListener{Listener: listener, frontend: frontend}
	pgToken, ok := frontend.admitPG()
	if !ok {
		t.Fatal("failed to admit PostgreSQL token fixture")
	}
	client, peer := net.Pipe()
	defer client.Close()
	listener.accepted <- peer
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := wrapped.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()
	var conn net.Conn
	select {
	case conn = <-accepted:
	case <-time.After(time.Second):
		t.Fatal("listener did not accept native socket")
	}
	defer conn.Close()

	base := serviceauthz.FrontendConnectionContextFromConn(context.Background(), conn)
	if _, ok := serviceauthz.FrontendContinuationFromContext(base); ok {
		t.Fatal("active socket exposed a continuation before drain publication")
	}
	frontend.begin(listener)
	var digest [32]byte
	digest[0] = 9
	if !frontend.installGrant(digest, serviceauthz.FrontendScopeNative) {
		t.Fatal("continuation grant was not installed for an open socket")
	}
	credential, ok := serviceauthz.FrontendContinuationFromContext(base)
	if !ok || credential.GrantDigest != digest || credential.Protocol != serviceauthz.FrontendScopeNative {
		t.Fatalf("credential = %+v, ok=%v", credential, ok)
	}
	if _, ok := serviceauthz.FrontendContinuationFromContext(
		serviceauthz.FrontendConnectionContextFromConn(context.Background(), &frontendTrackedConn{
			Conn: conn, token: credential.ConnToken, scope: serviceauthz.FrontendScopePostgreSQL, frontend: frontend,
		}),
	); ok {
		t.Fatal("native token was replayable under PostgreSQL scope")
	}
	if !frontend.installGrant(digest, serviceauthz.FrontendScopePostgreSQL) {
		t.Fatal("second protocol grant should be independently installable")
	}
	pgContextConn := &frontendTrackedConn{Conn: conn, token: pgToken,
		scope: serviceauthz.FrontendScopePostgreSQL, frontend: frontend}
	pgContext := serviceauthz.FrontendConnectionContextFromConn(context.Background(), pgContextConn)
	pgCredential, ok := serviceauthz.FrontendContinuationFromContext(pgContext)
	if !ok || pgCredential.ConnToken != pgToken || pgCredential.Protocol != serviceauthz.FrontendScopePostgreSQL {
		t.Fatalf("PostgreSQL credential = %+v, ok=%v", pgCredential, ok)
	}
	frontend.releasePG(pgToken)

	// A restarted draining frontend carries only the durable fence state. It
	// must not mint or revive a token for a socket that was never accepted by
	// this process.
	restarted := newFrontendAdmission(testFrontendDrainIdentity(), true, false)
	if !restarted.installGrant(digest, serviceauthz.FrontendScopeNative) {
		t.Fatal("restarted drain rejected durable grant publication")
	}
	if _, ok := restarted.FrontendContinuationCredential(credential.ConnToken, serviceauthz.FrontendScopeNative); ok {
		t.Fatal("restarted drain revived an old accepted-socket token")
	}
}
