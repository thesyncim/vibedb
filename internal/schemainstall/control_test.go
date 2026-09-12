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
func (*schemaPeerConnection) PeerKeyDigest() [32]byte { return [32]byte{} }
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
