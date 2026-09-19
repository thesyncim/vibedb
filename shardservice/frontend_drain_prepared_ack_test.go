package shardservice

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type frontendDrainPreparedAckTestConnection struct {
	net.Conn
	identity rafttransport.PeerIdentity
	key      [32]byte
	class    rafttransport.TrafficClass
}

func (connection *frontendDrainPreparedAckTestConnection) PeerIdentity() rafttransport.PeerIdentity {
	return connection.identity
}
func (connection *frontendDrainPreparedAckTestConnection) PeerKeyDigest() [32]byte {
	return connection.key
}
func (connection *frontendDrainPreparedAckTestConnection) TrafficClass() rafttransport.TrafficClass {
	return connection.class
}

type frontendDrainPreparedAckTestInstaller struct {
	gate           *serviceauthz.ServiceDirectoryGate
	coordinates    frontenddrain.PreparedAckCutReadFloor
	coordinatesSet bool
	calls          int
}

type frontendDrainPreparedAckTestCutReader struct {
	cuts []frontenddrain.PreparedAckCut
}

func (reader *frontendDrainPreparedAckTestCutReader) ReadFrontendDrainPreparedAckCut(
	_ context.Context, _ frontenddrain.PreparedAckRequest,
) (frontenddrain.PreparedAckCut, error) {
	if reader == nil || len(reader.cuts) == 0 {
		return frontenddrain.PreparedAckCut{}, frontenddrain.ErrPreparedAckState
	}
	cut := reader.cuts[0]
	reader.cuts = reader.cuts[1:]
	return cut, nil
}

func (installer *frontendDrainPreparedAckTestInstaller) InstallFrontendDrainServiceCut(
	_ context.Context, cut frontenddrain.PreparedAckCut,
) (uint64, error) {
	if !cut.Valid() {
		return 0, errors.New("invalid service cut")
	}
	floor := cut.ReadFloor()
	if !floor.Valid() || floor == (frontenddrain.PreparedAckCutReadFloor{}) {
		return 0, errors.New("invalid service cut floor")
	}
	if installer.coordinatesSet && !cut.AtLeastFloor(installer.coordinates) {
		return 0, serviceauthz.ErrServiceDirectoryStale
	}
	if installer.gate == nil {
		gate, err := serviceauthz.NewServiceDirectoryGate(cut.ServiceDirectory)
		if err != nil {
			return 0, err
		}
		installer.gate = gate
	} else if err := installer.gate.ApplyCommittedCut(cut.ServiceDirectory); err != nil {
		return 0, err
	}
	installer.coordinates = floor
	installer.coordinatesSet = true
	installer.calls++
	return cut.ServiceDirectoryRevision, nil
}

func (installer *frontendDrainPreparedAckTestInstaller) ServiceCutCoordinates() (frontenddrain.PreparedAckCutReadFloor, bool) {
	if installer == nil {
		return frontenddrain.PreparedAckCutReadFloor{}, false
	}
	return installer.coordinates, installer.coordinatesSet
}

func TestFrontendDrainPreparedAckInstallsCanonicalGateBeforeResponse(t *testing.T) {
	request := frontendDrainPreparedAckTestRequest(t)
	controller := rafttransport.PeerIdentity{TrustDomain: request.SourceCut.ServiceDirectory.TrustDomain, Node: request.SourcePrincipal}
	installer := new(frontendDrainPreparedAckTestInstaller)
	reader := &frontendDrainPreparedAckTestCutReader{cuts: []frontenddrain.PreparedAckCut{request.SourceCut}}
	service, err := NewFrontendDrainPreparedAckService(FrontendDrainPreparedAckServiceOptions{
		Reader: reader, Installer: installer,
		TrustDomain: controller.TrustDomain,
		Authorize: func(peer rafttransport.PeerIdentity, got frontenddrain.PreparedAckRequest) bool {
			return peer == controller && got.SourcePrincipal == controller.Node
		}, ReadDeadline: func() time.Time { return time.Now().Add(time.Second) },
		WriteDeadline: func() time.Time { return time.Now().Add(time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- service.Serve(t.Context(), &frontendDrainPreparedAckTestConnection{
			Conn: server, identity: controller, key: request.SourcePrincipalKeyDigest,
			class: rafttransport.TrafficShardControl,
		})
	}()
	encoded, err := request.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFrontendDrainPreparedAckFrame(client, encoded); err != nil {
		t.Fatal(err)
	}
	responseRaw := make([]byte, frontenddrain.PreparedAckResponseBytes)
	if _, err := io.ReadFull(client, responseRaw); err != nil {
		t.Fatal(err)
	}
	response, err := frontenddrain.OpenPreparedAckResponse(responseRaw, request)
	if err != nil || response.AppliedRevision != request.SourceCut.ServiceDirectoryRevision {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if installer.calls != 1 || installer.gate == nil || installer.gate.Revision() != request.SourceCut.ServiceDirectoryRevision {
		t.Fatalf("installer calls=%d gate=%v revision=%d", installer.calls, installer.gate != nil, installer.gate.Revision())
	}
}

func TestFrontendDrainPreparedAckRejectsWrongTLSKeyAndReceiverShape(t *testing.T) {
	request := frontendDrainPreparedAckTestRequest(t)
	controller := rafttransport.PeerIdentity{TrustDomain: request.SourceCut.ServiceDirectory.TrustDomain, Node: request.SourcePrincipal}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	for name, mutate := range map[string]func(*frontenddrain.PreparedAckRequest){
		"wrong key":      func(got *frontenddrain.PreparedAckRequest) { got.SourcePrincipalKeyDigest[0]++ },
		"wrong receiver": func(got *frontenddrain.PreparedAckRequest) { got.ReceiverIncarnation++ },
		"prepared grant missing": func(got *frontenddrain.PreparedAckRequest) {
			got.GrantDigest = [32]byte{99}
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := request
			mutate(&got)
			installer := new(frontendDrainPreparedAckTestInstaller)
			reader := &frontendDrainPreparedAckTestCutReader{cuts: []frontenddrain.PreparedAckCut{request.SourceCut}}
			service, err := NewFrontendDrainPreparedAckService(FrontendDrainPreparedAckServiceOptions{
				Reader: reader, Installer: installer,
				TrustDomain:  controller.TrustDomain,
				Authorize:    func(rafttransport.PeerIdentity, frontenddrain.PreparedAckRequest) bool { return true },
				ReadDeadline: deadline, WriteDeadline: deadline,
			})
			if err != nil {
				t.Fatal(err)
			}
			client, server := net.Pipe()
			done := make(chan error, 1)
			go func() {
				done <- service.Serve(t.Context(), &frontendDrainPreparedAckTestConnection{
					Conn: server, identity: controller, key: request.SourcePrincipalKeyDigest,
					class: rafttransport.TrafficShardControl,
				})
			}()
			encoded, marshalErr := got.Marshal()
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			if err := writeFrontendDrainPreparedAckFrame(client, encoded); err != nil {
				t.Fatal(err)
			}
			_ = client.Close()
			serveErr := <-done
			if name == "wrong key" {
				if !errors.Is(serveErr, frontenddrain.ErrPreparedAckAuth) {
					t.Fatalf("wrong key error=%v", serveErr)
				}
			} else if !errors.Is(serveErr, frontenddrain.ErrPreparedAckState) {
				t.Fatalf("rejected request error=%v", serveErr)
			}
			if installer.calls != 0 {
				t.Fatalf("rejected request installed gate %d times", installer.calls)
			}
		})
	}
}

func TestFrontendDrainPreparedAckUsesNativeServerGateMonotonically(t *testing.T) {
	request := frontendDrainPreparedAckTestRequest(t)
	controller := rafttransport.PeerIdentity{TrustDomain: request.SourceCut.ServiceDirectory.TrustDomain, Node: request.SourcePrincipal}
	server, err := NewReplicatedServer(&fakeReplicatedOwner{state: testReplicatedServingState()}, DefaultReplicatedInFlightFrameBytes, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	reader := &frontendDrainPreparedAckTestCutReader{cuts: []frontenddrain.PreparedAckCut{request.SourceCut}}
	service, err := NewFrontendDrainPreparedAckService(FrontendDrainPreparedAckServiceOptions{
		Reader: reader, Installer: server,
		TrustDomain: controller.TrustDomain,
		Authorize: func(peer rafttransport.PeerIdentity, got frontenddrain.PreparedAckRequest) bool {
			return peer == controller && got.SourcePrincipal == controller.Node
		}, ReadDeadline: deadline, WriteDeadline: deadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	serve := func(got frontenddrain.PreparedAckRequest) frontenddrain.PreparedAckResponse {
		t.Helper()
		client, peer := net.Pipe()
		done := make(chan error, 1)
		go func() {
			done <- service.Serve(t.Context(), &frontendDrainPreparedAckTestConnection{
				Conn: peer, identity: controller, key: got.SourcePrincipalKeyDigest,
				class: rafttransport.TrafficShardControl,
			})
		}()
		encoded, marshalErr := got.Marshal()
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if err := writeFrontendDrainPreparedAckFrame(client, encoded); err != nil {
			t.Fatal(err)
		}
		responseRaw := make([]byte, frontenddrain.PreparedAckResponseBytes)
		if _, err := io.ReadFull(client, responseRaw); err != nil {
			t.Fatal(err)
		}
		response, openErr := frontenddrain.OpenPreparedAckResponse(responseRaw, got)
		if openErr != nil {
			t.Fatal(openErr)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		_ = client.Close()
		return response
	}
	first := serve(request)
	if first.AppliedRevision != request.SourceCut.ServiceDirectoryRevision {
		t.Fatalf("first applied revision=%d", first.AppliedRevision)
	}
	firstGate := server.ServiceDirectoryGate()
	if firstGate == nil || firstGate.Revision() != request.SourceCut.ServiceDirectoryRevision {
		t.Fatalf("first native gate=%v revision=%d", firstGate != nil, server.ServiceDirectoryRevision())
	}
	next := request
	next.SourceCut.ServiceDirectoryRevision++
	next.SourceCut.ServiceDirectory.Revision++
	reader.cuts = append(reader.cuts, next.SourceCut)
	second := serve(next)
	if second.AppliedRevision != next.SourceCut.ServiceDirectoryRevision || server.ServiceDirectoryGate() != firstGate {
		t.Fatalf("second applied=%d gate_replaced=%t", second.AppliedRevision, server.ServiceDirectoryGate() != firstGate)
	}
	staleCut := request.SourceCut
	staleCut.DirectoryRevision--
	staleCut.DirectoryDigest = [32]byte{19}
	if _, err := server.InstallFrontendDrainServiceCut(t.Context(), staleCut); !errors.Is(err, serviceauthz.ErrServiceDirectoryStale) {
		t.Fatalf("stale full-cut install error=%v", err)
	}
}

func frontendDrainPreparedAckTestRequest(t *testing.T) frontenddrain.PreparedAckRequest {
	t.Helper()
	trust := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	storage := rafttransport.NodeID{3}
	gateway := rafttransport.NodeID{4}
	group := raftmember.GroupKey{ClusterID: [16]byte{5}, ClusterIncarnation: [16]byte{6}, ShardIncarnation: [16]byte{7}, GroupID: [16]byte{8}}
	drainID := [32]byte{9}
	scope := serviceauthz.FrontendContinuationScopeRecord{Protocol: serviceauthz.FrontendScopeNative,
		Action: serviceauthz.FrontendActionForwardedData, Capability: serviceauthz.CapabilityDataRead,
		Operation: serviceauthz.ServiceOperationForwardedRead, Group: group}
	grant, err := serviceauthz.NewCommittedFrontendContinuationGrant(serviceauthz.CommittedFrontendContinuationGrant{
		TrustDomain: trust, PhysicalNode: storage, PhysicalIncarnation: 2, PeerKeyDigest: [32]byte{11},
		GatewayServiceID: gateway, GatewaySessionID: [16]byte{12}, GatewaySessionRevision: 13, DrainID: drainID,
		AdmissionEpoch: 14, AcceptedConnectionTokens: []serviceauthz.FrontendConnToken{{15}},
		AcceptedConnectionProtocols: []serviceauthz.FrontendContinuationScope{serviceauthz.FrontendScopeNative},
		AdmissionClosedProofDigest:  [32]byte{16},
		Revision:                    17, State: serviceauthz.ContinuationGrantPrepared,
	})
	if err != nil {
		t.Fatal(err)
	}
	cut := frontenddrain.PreparedAckCut{
		DirectoryRevision: 19, DirectoryDigest: [32]byte{20}, CatalogGeneration: 21,
		CatalogHeadDigest: [32]byte{22}, ServiceDirectoryRevision: 23,
		ServiceDirectory: serviceauthz.ServiceDirectoryCut{
			CatalogGeneration: 21, Revision: 23, TrustDomain: trust, PolicyGeneration: 24,
			Bindings: []serviceauthz.ServiceBinding{
				{Principal: storage, PhysicalNode: storage, PhysicalIncarnation: 2, KeyDigest: [32]byte{10}, Roles: serviceauthz.ServiceRoleStorage, Lifecycle: serviceauthz.ServiceActive},
				{Principal: gateway, PhysicalNode: storage, PhysicalIncarnation: 2, KeyDigest: [32]byte{11}, Roles: serviceauthz.ServiceRoleGateway, Lifecycle: serviceauthz.ServiceActive, GatewayIncarnation: 3, SessionID: [16]byte{12}, SessionRevision: 13, ParticipantDigest: [32]byte{18}},
			}, ForwardedScopes: []serviceauthz.FrontendContinuationScopeRecord{scope},
			ContinuationGrants: []serviceauthz.CommittedFrontendContinuationGrant{grant},
		},
	}
	return frontenddrain.PreparedAckRequest{Nonce: [16]byte{25}, DrainID: drainID, GrantDigest: grant.GrantDigest,
		SourcePrincipal: rafttransport.NodeID{26}, SourcePrincipalKeyDigest: [32]byte{27}, ReceiverNode: storage,
		ReceiverIncarnation: 2, ReceiverServiceKeyDigest: [32]byte{10}, ReceiverNodeRevision: 28, SourceCut: cut}
}
