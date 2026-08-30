package schemainstall

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type schemaPeerConnection struct {
	net.Conn
	identity rafttransport.PeerIdentity
}

func (connection *schemaPeerConnection) PeerIdentity() rafttransport.PeerIdentity {
	return connection.identity
}
func (*schemaPeerConnection) TrafficClass() rafttransport.TrafficClass {
	return rafttransport.TrafficShardControl
}

func TestControlCodecCanonicalStrictAndBound(t *testing.T) {
	request, authorization, proof, _ := schemaFixture(31)
	for _, fixture := range []struct {
		command       Command
		authorization Authorization
		proof         DrainProof
	}{
		{CommandPrepare, Authorization{}, DrainProof{}}, {CommandAuthorize, authorization, DrainProof{}},
		{CommandActivate, authorization, DrainProof{}}, {CommandDrain, authorization, proof},
	} {
		raw, err := AppendControlRequest(nil, fixture.command, request, fixture.authorization, fixture.proof)
		if err != nil || len(raw) != ControlRequestBytes {
			t.Fatalf("append = %d, %v", len(raw), err)
		}
		command, opened, auth, openedProof, err := ReadControlRequest(bytes.NewReader(raw))
		if err != nil || command != fixture.command || opened != request || auth != fixture.authorization || openedProof != fixture.proof {
			t.Fatalf("open = %d %#v %#v %#v %v", command, opened, auth, openedProof, err)
		}
		for length := 0; length < len(raw); length++ {
			if _, _, _, _, err = ReadControlRequest(bytes.NewReader(raw[:length])); err == nil {
				t.Fatalf("accepted prefix %d", length)
			}
		}
		mutated := append([]byte(nil), raw...)
		mutated[9] = 1
		if _, _, _, _, err = ReadControlRequest(bytes.NewReader(mutated)); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	record := Record{Request: request, Revision: 1, State: StatePrepared,
		Installation: InstallationDigest(request, [32]byte{1})}
	var buffer bytes.Buffer
	service := new(ControlService)
	if err := service.writeResponse(&buffer, ResponseOK, record); err != nil {
		t.Fatal(err)
	}
	opened, err := ReadControlResponse(&buffer)
	if err != nil || opened != record {
		t.Fatalf("response = %#v, %v", opened, err)
	}
}

func TestAuthenticatedControlServiceLifecycle(t *testing.T) {
	activator := newTestActivator()
	installer, journal, backend := openTestInstaller(t, t.TempDir(), activator, 4)
	defer journal.Close()
	defer backend.Close()
	request, authorization, proof, bundle := schemaFixture(41)
	peer := rafttransport.PeerIdentity{Node: [16]byte{9}, TrustDomain: rafttransport.TrustDomain{
		ClusterID: request.Group.ClusterID, ClusterIncarnation: request.Group.ClusterIncarnation}}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewControlService(ControlOptions{Installer: installer,
		Authorize: func(identity rafttransport.PeerIdentity, got Request, _ Command) bool {
			return identity == peer && got.Group == request.Group
		}, ReadDeadline: deadline, WriteDeadline: deadline, MaxBundleBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	invoke := func(command Command, auth Authorization, drain DrainProof, payload []byte) Record {
		t.Helper()
		client, server := net.Pipe()
		done := make(chan error, 1)
		go func() {
			done <- service.Serve(context.Background(), &schemaPeerConnection{Conn: server, identity: peer})
		}()
		raw, appendErr := AppendControlRequest(nil, command, request, auth, drain)
		if appendErr != nil {
			t.Fatal(appendErr)
		}
		if _, appendErr = client.Write(append(raw, payload...)); appendErr != nil {
			t.Fatal(appendErr)
		}
		record, readErr := ReadControlResponse(client)
		if readErr != nil {
			t.Fatal(readErr)
		}
		_ = client.Close()
		if serveErr := <-done; serveErr != nil {
			t.Fatal(serveErr)
		}
		return record
	}
	if record := invoke(CommandPrepare, Authorization{}, DrainProof{}, bundle); record.State != StatePrepared {
		t.Fatal(record.State)
	}
	if record := invoke(CommandAuthorize, authorization, DrainProof{}, nil); record.State != StateAuthorized {
		t.Fatal(record.State)
	}
	if record := invoke(CommandActivate, authorization, DrainProof{}, nil); record.State != StateActive {
		t.Fatal(record.State)
	}
	if record := invoke(CommandDrain, authorization, proof, nil); record.State != StateDrained {
		t.Fatal(record.State)
	}
}

func TestClientDrainRetryAcceptsAlreadyDrainedExactOperation(t *testing.T) {
	installer, journal, backend := openTestInstaller(t, t.TempDir(), newTestActivator(), 4)
	defer journal.Close()
	defer backend.Close()
	request, authorization, proof, bundle := schemaFixture(42)
	peer := rafttransport.PeerIdentity{Node: [16]byte{9}, TrustDomain: rafttransport.TrustDomain{
		ClusterID: request.Group.ClusterID, ClusterIncarnation: request.Group.ClusterIncarnation}}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewControlService(ControlOptions{Installer: installer,
		Authorize: func(identity rafttransport.PeerIdentity, got Request, _ Command) bool {
			return identity == peer && got == request
		}, ReadDeadline: deadline, WriteDeadline: deadline, MaxBundleBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	opener := buildTestOpener(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
		client, server := net.Pipe()
		go func() { _ = service.Serve(context.Background(), &schemaPeerConnection{Conn: server, identity: peer}) }()
		return &schemaPeerConnection{Conn: client, identity: peer}, nil
	})
	client, err := NewClient(ClientOptions{Opener: opener, ReadDeadline: deadline, WriteDeadline: deadline})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Prepare(t.Context(), peer.Node, request, bundle); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Authorize(t.Context(), peer.Node, request, authorization); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Activate(t.Context(), peer.Node, request, authorization); err != nil {
		t.Fatal(err)
	}
	first, err := client.Drain(t.Context(), peer.Node, request, authorization, proof)
	if err != nil || first.State != StateDrained || first.DrainProof != proof {
		t.Fatalf("first drain=%+v err=%v", first, err)
	}
	later := proof
	later.ReleasedExecutionPinRoot[0]++
	replayed, err := client.Drain(t.Context(), peer.Node, request, authorization, later)
	if err != nil || replayed != first {
		t.Fatalf("terminal retry=%+v want=%+v err=%v", replayed, first, err)
	}
}

func TestClientClassifiesTransientOpenFailureForExactRetry(t *testing.T) {
	request, _, _, bundle := schemaFixture(43)
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	client, err := NewClient(ClientOptions{ReadDeadline: deadline, WriteDeadline: deadline,
		Opener: buildTestOpener(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
		})})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Prepare(t.Context(), [16]byte{9}, request, bundle); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("transient open was not retryable: %v", err)
	}
}

func TestControlServiceRejectsWrongTrustDomainBeforeWork(t *testing.T) {
	installer, journal, backend := openTestInstaller(t, t.TempDir(), newTestActivator(), 2)
	defer journal.Close()
	defer backend.Close()
	request, _, _, _ := schemaFixture(51)
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewControlService(ControlOptions{Installer: installer,
		Authorize:    func(rafttransport.PeerIdentity, Request, Command) bool { return true },
		ReadDeadline: deadline, WriteDeadline: deadline, MaxBundleBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	client, server := net.Pipe()
	peer := rafttransport.PeerIdentity{Node: [16]byte{1}}
	done := make(chan error, 1)
	go func() {
		done <- service.Serve(context.Background(), &schemaPeerConnection{Conn: server, identity: peer})
	}()
	raw, err := AppendControlRequest(nil, CommandPrepare, request, Authorization{}, DrainProof{})
	if err != nil {
		t.Fatal(err)
	}
	// Authorization is decided from the fixed header before the service accepts
	// any attacker-controlled bundle bytes.
	if _, err = client.Write(raw); err != nil {
		t.Fatal(err)
	}
	if _, err = ReadControlResponse(client); !errors.Is(err, rafttransport.ErrUnauthorized) {
		t.Fatal(err)
	}
	_ = client.Close()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if _, err = installer.Read(context.Background(), request.Operation); !errors.Is(err, ErrMissing) {
		t.Fatal(err)
	}
}
