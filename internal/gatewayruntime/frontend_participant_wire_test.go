package gatewayruntime

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type frontendParticipantWirePeer struct {
	net.Conn
	peer  rafttransport.PeerIdentity
	key   [32]byte
	class rafttransport.TrafficClass
}

func (peer *frontendParticipantWirePeer) PeerIdentity() rafttransport.PeerIdentity { return peer.peer }
func (peer *frontendParticipantWirePeer) PeerKeyDigest() [32]byte                  { return peer.key }
func (peer *frontendParticipantWirePeer) TrafficClass() rafttransport.TrafficClass { return peer.class }

type frontendParticipantWireOpener struct {
	connection rafttransport.PeerConnection
}

func (opener frontendParticipantWireOpener) OpenGatewayControlMember(
	context.Context, gateway.ClusterCatalogDrainMember,
) (rafttransport.PeerConnection, error) {
	return opener.connection, nil
}

type frontendParticipantCancelOpener struct {
	connection rafttransport.PeerConnection
	opened     chan struct{}
}

func (opener frontendParticipantCancelOpener) OpenGatewayControlMember(
	context.Context, gateway.ClusterCatalogDrainMember,
) (rafttransport.PeerConnection, error) {
	close(opener.opened)
	return opener.connection, nil
}

func frontendParticipantWireRecord(target *rafttransport.PeerTLS) gateway.NodeRecord {
	return gateway.NodeRecord{
		NodeID: target.LocalIdentity().Node, Incarnation: 3,
		ServiceKeyDigest: replication.Digest{1},
		DataEndpoint:     "data", NativeEndpoint: "native", ControlEndpoint: "control",
		GatewayEndpoint: "gateway", DataAddress: "data-address", NativeAddress: "native-address",
		ControlAddress: "control-address", GatewayAddress: "gateway-control",
		FailureDomain: "worker", Roles: gateway.NodeRoleStorage | gateway.NodeRoleGateway,
		Lifecycle: gateway.NodeDraining, Revision: 2, CatalogGeneration: 7,
		Gateway: gateway.GatewayIdentity{
			NodeID: target.LocalIdentity().Node, Incarnation: 9,
			ServiceKeyDigest: replication.Digest(target.LocalServiceKeyDigest()),
			ServiceID:        [16]byte{2}, SessionID: [16]byte{3}, SessionRevision: 4,
			ParticipantDigest: replication.Digest{5},
		},
	}
}

func frontendParticipantWireEvidence(record gateway.NodeRecord, active bool) gateway.GatewayParticipantEvidence {
	return gateway.GatewayParticipantEvidence{
		NodeID: record.NodeID, Incarnation: record.Incarnation, ServiceKeyDigest: record.ServiceKeyDigest,
		NodeRevision: record.Revision, CatalogGeneration: record.CatalogGeneration,
		GatewayNodeID: record.Gateway.NodeID, GatewayIncarnation: record.Gateway.Incarnation,
		GatewayServiceKeyDigest: record.Gateway.ServiceKeyDigest, ServiceID: record.Gateway.ServiceID,
		SessionID: record.Gateway.SessionID, SessionRevision: record.Gateway.SessionRevision,
		ParticipantDigest: record.Gateway.ParticipantDigest, DirectoryRevision: 8,
		Active: active, Digest: replication.Digest{6},
	}
}

func runFrontendParticipantWireRPC(
	t *testing.T, caller, target *rafttransport.PeerTLS, record gateway.NodeRecord,
	opener func(rafttransport.PeerConnection) frontendParticipantMemberOpener,
	addressOf frontendParticipantMemberAddress,
	scan func(context.Context, gateway.NodeRecord) (gateway.GatewayParticipantEvidence, error),
) (gateway.GatewayParticipantEvidence, error) {
	t.Helper()
	clientRaw, serverRaw := net.Pipe()
	client := &frontendParticipantWirePeer{Conn: clientRaw,
		peer: target.LocalIdentity(), key: target.LocalServiceKeyDigest(), class: rafttransport.TrafficGatewayControl}
	server := &frontendParticipantWirePeer{Conn: serverRaw,
		peer: caller.LocalIdentity(), key: caller.LocalServiceKeyDigest(), class: rafttransport.TrafficGatewayControl}
	serverErr := make(chan error, 1)
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	go func() {
		serverErr <- serveFrontendParticipantConnectionWith(t.Context(), server,
			func(rafttransport.PeerConnection) bool { return true },
			func(context.Context, rafttransport.NodeID, uint64) (gateway.NodeRecord, error) { return record, nil },
			scan, deadline, deadline)
	}()
	evidence, err := scanRemoteGatewayParticipantOverControl(t.Context(), record, caller, opener(client), addressOf, deadline, deadline)
	_ = client.Close()
	_ = server.Close()
	if serverErr := <-serverErr; err == nil && serverErr != nil {
		t.Fatalf("participant server: %v", serverErr)
	}
	return evidence, err
}

func TestRemoteGatewayParticipantScanUsesTargetControlIdentity(t *testing.T) {
	profiles, _ := runtimeControlTLSFixture(t, []serviceauthz.Entry{
		{Node: rafttransport.NodeID{11}, Capabilities: serviceauthz.AllCapabilities},
		{Node: rafttransport.NodeID{12}, Capabilities: serviceauthz.AllCapabilities},
	})
	caller, target := profiles[0], profiles[1]
	record := frontendParticipantWireRecord(target)
	if !record.Valid() {
		t.Fatal("invalid participant wire record")
	}
	active := true
	addressOf := func(member gateway.ClusterCatalogDrainMember) (string, bool) {
		return record.GatewayAddress, member.Node == record.Gateway.NodeID && member.Incarnation == record.Gateway.Incarnation
	}
	firstClientRaw, firstServerRaw := net.Pipe()
	firstClient := &frontendParticipantWirePeer{Conn: firstClientRaw,
		peer: target.LocalIdentity(), key: target.LocalServiceKeyDigest(), class: rafttransport.TrafficGatewayControl}
	firstServer := &frontendParticipantWirePeer{Conn: firstServerRaw,
		peer: caller.LocalIdentity(), key: caller.LocalServiceKeyDigest(), class: rafttransport.TrafficGatewayControl}
	firstServerErr := make(chan error, 1)
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	go func() {
		firstServerErr <- serveFrontendParticipantConnectionWith(t.Context(), firstServer,
			func(rafttransport.PeerConnection) bool { return true },
			func(context.Context, rafttransport.NodeID, uint64) (gateway.NodeRecord, error) { return record, nil },
			func(ctx context.Context, got gateway.NodeRecord) (gateway.GatewayParticipantEvidence, error) {
				if !got.Valid() {
					return gateway.GatewayParticipantEvidence{}, gateway.ErrInvalidScalingMetadata
				}
				return frontendParticipantWireEvidence(record, active), nil
			}, deadline, deadline)
	}()
	first, err := scanRemoteGatewayParticipantOverControl(t.Context(), record, caller,
		frontendParticipantWireOpener{connection: firstClient}, addressOf, deadline, deadline)
	_ = firstClient.Close()
	_ = firstServer.Close()
	if err != nil || !first.Active || !first.ValidFor(record) {
		t.Fatalf("active participant scan = %+v, err=%v", first, err)
	}
	if serverErr := <-firstServerErr; serverErr != nil {
		t.Fatalf("active participant server: %v", serverErr)
	}

	active = false
	second, err := runFrontendParticipantWireRPC(t, caller, target, record,
		func(connection rafttransport.PeerConnection) frontendParticipantMemberOpener {
			return frontendParticipantWireOpener{connection: connection}
		}, addressOf,
		func(context.Context, gateway.NodeRecord) (gateway.GatewayParticipantEvidence, error) {
			return frontendParticipantWireEvidence(record, active), nil
		})
	if err != nil || second.Active || !second.ValidFor(record) {
		t.Fatalf("released participant scan = %+v, err=%v", second, err)
	}
}

func TestRemoteGatewayParticipantScanRejectsForeignEndpointAndPeer(t *testing.T) {
	profiles, _ := runtimeControlTLSFixture(t, []serviceauthz.Entry{
		{Node: rafttransport.NodeID{21}, Capabilities: serviceauthz.AllCapabilities},
		{Node: rafttransport.NodeID{22}, Capabilities: serviceauthz.AllCapabilities},
	})
	record := frontendParticipantWireRecord(profiles[1])
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	foreignAddress := func(gateway.ClusterCatalogDrainMember) (string, bool) { return "foreign-control", true }
	if _, err := scanRemoteGatewayParticipantOverControl(t.Context(), record, profiles[0],
		frontendParticipantWireOpener{}, foreignAddress, deadline, deadline); !errors.Is(err, gateway.ErrScalingRevision) {
		t.Fatalf("foreign endpoint error = %v, want scaling revision", err)
	}

	clientRaw, serverRaw := net.Pipe()
	wrongKey := [32]byte{99}
	client := &frontendParticipantWirePeer{Conn: clientRaw,
		peer: profiles[1].LocalIdentity(), key: wrongKey, class: rafttransport.TrafficGatewayControl}
	server := &frontendParticipantWirePeer{Conn: serverRaw,
		peer: profiles[0].LocalIdentity(), key: profiles[0].LocalServiceKeyDigest(), class: rafttransport.TrafficGatewayControl}
	if _, err := scanRemoteGatewayParticipantOverControl(t.Context(), record, profiles[0],
		frontendParticipantWireOpener{connection: client}, func(gateway.ClusterCatalogDrainMember) (string, bool) {
			return record.GatewayAddress, true
		}, deadline, deadline); !errors.Is(err, gateway.ErrScalingIdentity) {
		t.Fatalf("foreign peer error = %v, want scaling identity", err)
	}
	_ = client.Close()
	_ = server.Close()
}

func TestFrontendParticipantWireRejectsRecordRevisionChange(t *testing.T) {
	profiles, _ := runtimeControlTLSFixture(t, []serviceauthz.Entry{
		{Node: rafttransport.NodeID{31}, Capabilities: serviceauthz.AllCapabilities},
		{Node: rafttransport.NodeID{32}, Capabilities: serviceauthz.AllCapabilities},
	})
	record := frontendParticipantWireRecord(profiles[1])
	mutated := record
	mutated.Revision++
	clientRaw, serverRaw := net.Pipe()
	client := &frontendParticipantWirePeer{Conn: clientRaw,
		peer: profiles[1].LocalIdentity(), key: profiles[1].LocalServiceKeyDigest(), class: rafttransport.TrafficGatewayControl}
	server := &frontendParticipantWirePeer{Conn: serverRaw,
		peer: profiles[0].LocalIdentity(), key: profiles[0].LocalServiceKeyDigest(), class: rafttransport.TrafficGatewayControl}
	serverErr := make(chan error, 1)
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	go func() {
		serverErr <- serveFrontendParticipantConnectionWith(t.Context(), server,
			func(rafttransport.PeerConnection) bool { return true },
			func(context.Context, rafttransport.NodeID, uint64) (gateway.NodeRecord, error) { return record, nil },
			func(context.Context, gateway.NodeRecord) (gateway.GatewayParticipantEvidence, error) {
				return frontendParticipantWireEvidence(record, false), nil
			}, deadline, deadline)
	}()
	if _, err := scanRemoteGatewayParticipantOverControl(t.Context(), mutated, profiles[0],
		frontendParticipantWireOpener{connection: client}, func(gateway.ClusterCatalogDrainMember) (string, bool) {
			return record.GatewayAddress, true
		}, deadline, deadline); !errors.Is(err, gateway.ErrScalingRevision) {
		t.Fatalf("revision mismatch error = %v, want scaling revision", err)
	}
	_ = client.Close()
	_ = server.Close()
	if serverErr := <-serverErr; !errors.Is(serverErr, gateway.ErrScalingIdentity) {
		t.Fatalf("server revision mismatch = %v, want scaling identity", serverErr)
	}
}

func TestRemoteGatewayParticipantScanCancellationClosesConnection(t *testing.T) {
	profiles, _ := runtimeControlTLSFixture(t, []serviceauthz.Entry{
		{Node: rafttransport.NodeID{41}, Capabilities: serviceauthz.AllCapabilities},
		{Node: rafttransport.NodeID{42}, Capabilities: serviceauthz.AllCapabilities},
	})
	record := frontendParticipantWireRecord(profiles[1])
	clientRaw, serverRaw := net.Pipe()
	client := &frontendParticipantWirePeer{Conn: clientRaw,
		peer: profiles[1].LocalIdentity(), key: profiles[1].LocalServiceKeyDigest(), class: rafttransport.TrafficGatewayControl}
	server := &frontendParticipantWirePeer{Conn: serverRaw,
		peer: profiles[0].LocalIdentity(), key: profiles[0].LocalServiceKeyDigest(), class: rafttransport.TrafficGatewayControl}
	opened := make(chan struct{})
	opener := frontendParticipantCancelOpener{connection: client, opened: opened}
	addressOf := func(gateway.ClusterCatalogDrainMember) (string, bool) { return record.GatewayAddress, true }
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	go func() {
		_, err := scanRemoteGatewayParticipantOverControl(ctx, record, profiles[0], opener, addressOf, deadline, deadline)
		result <- err
	}()
	select {
	case <-opened:
	case <-time.After(time.Second):
		t.Fatal("participant opener was not called")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled participant scan = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled participant scan retained the connection")
	}
	_ = server.Close()
}
