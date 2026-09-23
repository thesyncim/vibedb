package main

import (
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type rf3PreparedAckReaderTestConnection struct {
	net.Conn
	identity rafttransport.PeerIdentity
	key      [32]byte
	class    rafttransport.TrafficClass
}

func (connection *rf3PreparedAckReaderTestConnection) PeerIdentity() rafttransport.PeerIdentity {
	return connection.identity
}

func (connection *rf3PreparedAckReaderTestConnection) PeerKeyDigest() [32]byte {
	return connection.key
}

func (connection *rf3PreparedAckReaderTestConnection) TrafficClass() rafttransport.TrafficClass {
	return connection.class
}

func TestRF3FrontendDrainPreparedAckReaderReadsCanonicalSourceOverGatewayControl(t *testing.T) {
	for _, name := range []string{"creator", "current-controller", "current-controller-empty-drain"} {
		t.Run(name, func(t *testing.T) {
			request, seed, cut := rf3PreparedAckReaderTestRequest(t)
			if name == "current-controller-empty-drain" {
				grant := cut.ServiceDirectory.ContinuationGrants[0]
				cut.Subjects = []frontenddrain.PreparedAckSubject{{
					DrainID: grant.DrainID, PhysicalNode: grant.PhysicalNode, PhysicalIncarnation: grant.PhysicalIncarnation,
					GatewayServiceID: grant.GatewayServiceID, GatewayIncarnation: seed.Incarnation,
					GatewaySessionID: grant.GatewaySessionID, GatewaySessionRevision: grant.GatewaySessionRevision,
					NodeRevision: 1, Lifecycle: 1, DrainFenceDigest: [32]byte{33},
				}}
				cut.ServiceDirectory.ContinuationGrants = nil
				request.GrantDigest = [32]byte{}
			}
			if name != "creator" {
				// The current controller publishes an exact durable cut. The
				// continuation credential still belongs to its original gateway.
				publisher := cut.ServiceDirectory.Bindings[1]
				publisher.Principal = rafttransport.NodeID{31}
				publisher.KeyDigest = [32]byte{32}
				cut.ServiceDirectory.Bindings = append(cut.ServiceDirectory.Bindings, publisher)
				request.SourcePrincipal, request.SourcePrincipalKeyDigest = publisher.Principal, publisher.KeyDigest
				request.SourceCut = cut
				seed.NodeID, seed.SPKIPinDigest = publisher.Principal, publisher.KeyDigest
			}
			testRF3PreparedAckGatewaySourceRead(t, request, seed, cut)
		})
	}
}

func TestRF3FrontendDrainPreparedAckSourceRequiresCurrentPublisherAndExactSubject(t *testing.T) {
	for _, name := range []string{"principal", "key", "incarnation", "retired", "drain", "grant", "prepared"} {
		t.Run(name, func(t *testing.T) {
			request, seed, cut := rf3PreparedAckReaderTestRequest(t)
			query := frontenddrain.PreparedAckCutReadRequest{
				Operation: frontenddrain.CutOperationInstallExact, Nonce: request.Nonce,
				DrainID: request.DrainID, GrantDigest: request.GrantDigest,
				ReceiverNode: request.ReceiverNode, ReceiverIncarnation: request.ReceiverIncarnation,
				ReceiverServiceKeyDigest: request.ReceiverServiceKeyDigest, ReceiverNodeRevision: request.ReceiverNodeRevision,
				SourceFloor: cut.ReadFloor(), SourceCutDigest: cut.Digest(),
			}
			switch name {
			case "principal":
				seed.NodeID[0]++
			case "key":
				seed.SPKIPinDigest[0]++
			case "incarnation":
				seed.Incarnation++
			case "retired":
				cut.ServiceDirectory.Bindings[1].Lifecycle = serviceauthz.ServiceDecommissioned
			case "drain":
				query.DrainID[0]++
			case "grant":
				query.GrantDigest[0]++
			case "prepared":
				query.RequirePrepared = true
				grant := cut.ServiceDirectory.ContinuationGrants[0]
				grant.State = serviceauthz.ContinuationGrantEnforcing
				var err error
				grant, err = serviceauthz.NewCommittedFrontendContinuationGrant(grant)
				if err != nil {
					t.Fatal(err)
				}
				cut.ServiceDirectory.ContinuationGrants[0] = grant
				query.GrantDigest = grant.GrantDigest
			}
			if rf3FrontendDrainPreparedAckSourceCutMatchesQuery(cut, query, seed) {
				t.Fatal("accepted mismatched current publisher or drain subject")
			}
		})
	}
}

func testRF3PreparedAckGatewaySourceRead(t *testing.T, request frontenddrain.PreparedAckRequest,
	seed nodecontrol.BootstrapGatewaySeed, cut frontenddrain.PreparedAckCut,
) {
	t.Helper()
	trust := cut.ServiceDirectory.TrustDomain
	reader := &rf3FrontendDrainPreparedAckCutReader{
		trust: trust, localNode: request.ReceiverNode, localIncarnation: request.ReceiverIncarnation,
		localServiceKey: request.ReceiverServiceKeyDigest,
		readDeadline:    func() time.Time { return time.Now().Add(time.Second) },
		writeDeadline:   func() time.Time { return time.Now().Add(time.Second) },
		seeds:           map[rafttransport.NodeID]nodecontrol.BootstrapGatewaySeed{seed.NodeID: seed},
	}
	client, source := net.Pipe()
	clientConn := &rf3PreparedAckReaderTestConnection{Conn: client,
		identity: rafttransport.PeerIdentity{TrustDomain: trust, Node: seed.NodeID},
		key:      [32]byte(seed.SPKIPinDigest), class: rafttransport.TrafficGatewayControl}
	sourceConn := &rf3PreparedAckReaderTestConnection{Conn: source,
		identity: rafttransport.PeerIdentity{TrustDomain: trust, Node: request.ReceiverNode},
		key:      request.ReceiverServiceKeyDigest, class: rafttransport.TrafficGatewayControl}
	done := make(chan error, 1)
	go func() {
		defer sourceConn.Close()
		queryRaw := make([]byte, frontenddrain.PreparedAckCutReadRequestBytes)
		if _, err := io.ReadFull(sourceConn, queryRaw); err != nil {
			done <- err
			return
		}
		query, err := frontenddrain.OpenPreparedAckCutReadRequest(queryRaw)
		if err != nil {
			done <- err
			return
		}
		response := frontenddrain.PreparedAckCutReadResponse{
			Operation: frontenddrain.CutOperationInstallExact,
			Nonce:     request.Nonce, DrainID: request.DrainID, GrantDigest: request.GrantDigest,
			ReceiverNode: request.ReceiverNode, ReceiverIncarnation: request.ReceiverIncarnation,
			ReceiverServiceKeyDigest: request.ReceiverServiceKeyDigest,
			ReceiverNodeRevision:     request.ReceiverNodeRevision,
			DirectoryRevision:        cut.DirectoryRevision, DirectoryDigest: cut.DirectoryDigest,
			CatalogGeneration: cut.CatalogGeneration, CatalogHeadDigest: cut.CatalogHeadDigest,
			ServiceDirectoryRevision: cut.ServiceDirectoryRevision,
			ServiceDirectoryDigest:   cut.ServiceDirectoryDigestValue(), SourceCutDigest: query.SourceCutDigest, Cut: cut,
		}
		raw, err := response.Marshal()
		if err == nil {
			err = writeRF3FrontendDrainPreparedAckFrame(sourceConn, raw)
		}
		done <- err
	}()
	got, err := reader.readFromConnection(t.Context(), clientConn, seed, request)
	_ = clientConn.Close()
	if err != nil || got.Digest() != cut.Digest() {
		t.Fatalf("source cut=%x err=%v want=%x", got.Digest(), err, cut.Digest())
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRF3FrontendDrainPreparedAckReaderRetriesAuthenticatedMovedSourceCut(t *testing.T) {
	request, seed, cut := rf3PreparedAckReaderTestRequest(t)
	advanced := cut
	advanced.DirectoryRevision++
	advanced.DirectoryDigest[0]++
	if !advanced.Valid() || !advanced.AtLeastFloor(request.SourceCut.ReadFloor()) || advanced.Digest() == request.SourceCutDigest() {
		t.Fatal("invalid advanced source cut fixture")
	}
	trust := cut.ServiceDirectory.TrustDomain
	reader := &rf3FrontendDrainPreparedAckCutReader{
		trust: trust, localNode: request.ReceiverNode, localIncarnation: request.ReceiverIncarnation,
		localServiceKey: request.ReceiverServiceKeyDigest,
		readDeadline:    func() time.Time { return time.Now().Add(time.Second) },
		writeDeadline:   func() time.Time { return time.Now().Add(time.Second) },
	}
	writeResponse := func(connection *rf3PreparedAckReaderTestConnection, moved bool) error {
		queryRaw := make([]byte, frontenddrain.PreparedAckCutReadRequestBytes)
		if _, err := io.ReadFull(connection, queryRaw); err != nil {
			return err
		}
		query, err := frontenddrain.OpenPreparedAckCutReadRequest(queryRaw)
		if err != nil {
			return err
		}
		if moved {
			response := frontenddrain.PreparedAckCutReadMovedResponse{
				Operation: query.Operation, RequirePrepared: query.RequirePrepared,
				Nonce: query.Nonce, DrainID: query.DrainID, GrantDigest: query.GrantDigest,
				ReceiverNode: query.ReceiverNode, ReceiverIncarnation: query.ReceiverIncarnation,
				ReceiverServiceKeyDigest: query.ReceiverServiceKeyDigest,
				ReceiverNodeRevision:     query.ReceiverNodeRevision,
				SourceFloor:              advanced.ReadFloor(), SourceCutDigest: advanced.Digest(),
			}
			raw, err := response.Marshal()
			if err != nil {
				return err
			}
			return writeRF3FrontendDrainPreparedAckFrame(connection, raw)
		}
		response := frontenddrain.PreparedAckCutReadResponse{
			Operation: query.Operation, RequirePrepared: query.RequirePrepared,
			Nonce: query.Nonce, DrainID: query.DrainID, GrantDigest: query.GrantDigest,
			ReceiverNode: query.ReceiverNode, ReceiverIncarnation: query.ReceiverIncarnation,
			ReceiverServiceKeyDigest: query.ReceiverServiceKeyDigest,
			ReceiverNodeRevision:     query.ReceiverNodeRevision,
			DirectoryRevision:        advanced.DirectoryRevision, DirectoryDigest: advanced.DirectoryDigest,
			CatalogGeneration: advanced.CatalogGeneration, CatalogHeadDigest: advanced.CatalogHeadDigest,
			ServiceDirectoryRevision: advanced.ServiceDirectoryRevision,
			ServiceDirectoryDigest:   advanced.ServiceDirectoryDigestValue(), SourceCutDigest: advanced.Digest(), Cut: advanced,
		}
		raw, err := response.Marshal()
		if err != nil {
			return err
		}
		return writeRF3FrontendDrainPreparedAckFrame(connection, raw)
	}
	firstClient, firstSource := net.Pipe()
	firstClientConn := &rf3PreparedAckReaderTestConnection{Conn: firstClient,
		identity: rafttransport.PeerIdentity{TrustDomain: trust, Node: seed.NodeID},
		key:      [32]byte(seed.SPKIPinDigest), class: rafttransport.TrafficGatewayControl}
	firstSourceConn := &rf3PreparedAckReaderTestConnection{Conn: firstSource,
		identity: rafttransport.PeerIdentity{TrustDomain: trust, Node: request.ReceiverNode},
		key:      request.ReceiverServiceKeyDigest, class: rafttransport.TrafficGatewayControl}
	firstDone := make(chan error, 1)
	go func() {
		defer firstSourceConn.Close()
		firstDone <- writeResponse(firstSourceConn, true)
	}()
	_, err := reader.readFromConnection(t.Context(), firstClientConn, seed, request)
	_ = firstClientConn.Close()
	if !errors.Is(err, frontenddrain.ErrPreparedAckCutMoved) || !errors.Is(err, errRF3FrontendDrainPreparedAckReaderState) {
		t.Fatalf("moved source response error=%v, want authenticated retryable movement", err)
	}
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}

	// The retry uses the rederived advanced cut in the request. A normal
	// response at that exact digest completes the read without another spin.
	retry := request
	retry.SourceCut = advanced
	secondClient, secondSource := net.Pipe()
	secondClientConn := &rf3PreparedAckReaderTestConnection{Conn: secondClient,
		identity: rafttransport.PeerIdentity{TrustDomain: trust, Node: seed.NodeID},
		key:      [32]byte(seed.SPKIPinDigest), class: rafttransport.TrafficGatewayControl}
	secondSourceConn := &rf3PreparedAckReaderTestConnection{Conn: secondSource,
		identity: rafttransport.PeerIdentity{TrustDomain: trust, Node: request.ReceiverNode},
		key:      request.ReceiverServiceKeyDigest, class: rafttransport.TrafficGatewayControl}
	secondDone := make(chan error, 1)
	go func() {
		defer secondSourceConn.Close()
		secondDone <- writeResponse(secondSourceConn, false)
	}()
	got, err := reader.readFromConnection(t.Context(), secondClientConn, seed, retry)
	_ = secondClientConn.Close()
	if err != nil || got.Digest() != advanced.Digest() {
		t.Fatalf("rederived moved retry cut=%x err=%v want=%x", got.Digest(), err, advanced.Digest())
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
}

func TestRF3FrontendDrainPreparedAckReaderReadsCanonicalSourceOverShardControl(t *testing.T) {
	request, _, cut := rf3PreparedAckReaderTestRequest(t)
	trust := cut.ServiceDirectory.TrustDomain
	seed := nodecontrol.BootstrapGatewaySeed{
		NodeID: request.ReceiverNode, Incarnation: request.ReceiverIncarnation,
		ControlAddress: "127.0.0.1:20002", SPKIPinDigest: [32]byte(request.ReceiverServiceKeyDigest),
	}
	reader := &rf3FrontendDrainPreparedAckCutReader{
		trust: trust, localNode: request.ReceiverNode, localIncarnation: request.ReceiverIncarnation,
		localServiceKey: request.ReceiverServiceKeyDigest,
		readDeadline:    func() time.Time { return time.Now().Add(time.Second) },
		writeDeadline:   func() time.Time { return time.Now().Add(time.Second) },
	}
	client, source := net.Pipe()
	clientConn := &rf3PreparedAckReaderTestConnection{Conn: client,
		identity: rafttransport.PeerIdentity{TrustDomain: trust, Node: seed.NodeID},
		key:      [32]byte(seed.SPKIPinDigest), class: rafttransport.TrafficShardControl}
	sourceConn := &rf3PreparedAckReaderTestConnection{Conn: source,
		identity: rafttransport.PeerIdentity{TrustDomain: trust, Node: request.ReceiverNode},
		key:      request.ReceiverServiceKeyDigest, class: rafttransport.TrafficShardControl}
	done := make(chan error, 1)
	go func() {
		defer sourceConn.Close()
		queryRaw := make([]byte, frontenddrain.PreparedAckCutReadRequestBytes)
		if _, err := io.ReadFull(sourceConn, queryRaw); err != nil {
			done <- err
			return
		}
		query, err := frontenddrain.OpenPreparedAckCutReadRequest(queryRaw)
		if err != nil {
			done <- err
			return
		}
		response := frontenddrain.PreparedAckCutReadResponse{
			Operation:                frontenddrain.CutOperationReadLatest,
			Nonce:                    query.Nonce,
			ReceiverNode:             query.ReceiverNode,
			ReceiverIncarnation:      query.ReceiverIncarnation,
			ReceiverServiceKeyDigest: query.ReceiverServiceKeyDigest,
			ReceiverNodeRevision:     1,
			DirectoryRevision:        cut.DirectoryRevision, DirectoryDigest: cut.DirectoryDigest,
			CatalogGeneration: cut.CatalogGeneration, CatalogHeadDigest: cut.CatalogHeadDigest,
			ServiceDirectoryRevision: cut.ServiceDirectoryRevision,
			ServiceDirectoryDigest:   cut.ServiceDirectoryDigestValue(), Cut: cut,
		}
		raw, err := response.Marshal()
		if err == nil {
			err = writeRF3FrontendDrainPreparedAckFrame(sourceConn, raw)
		}
		done <- err
	}()
	query := frontenddrain.PreparedAckCutReadRequest{
		Operation: frontenddrain.CutOperationReadLatest, Nonce: [16]byte{17},
		ReceiverNode: request.ReceiverNode, ReceiverIncarnation: request.ReceiverIncarnation,
		ReceiverServiceKeyDigest: request.ReceiverServiceKeyDigest,
	}
	got, err := reader.readQueryFromConnectionClass(t.Context(), clientConn, seed, query, rafttransport.TrafficShardControl, true)
	_ = clientConn.Close()
	if err != nil || got.Digest() != cut.Digest() {
		t.Fatalf("physical source cut=%x err=%v want=%x", got.Digest(), err, cut.Digest())
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRF3FrontendDrainPreparedAckReaderRejectsPeerKeyAndSourceBinding(t *testing.T) {
	request, seed, cut := rf3PreparedAckReaderTestRequest(t)
	reader := &rf3FrontendDrainPreparedAckCutReader{
		trust: cut.ServiceDirectory.TrustDomain, localNode: request.ReceiverNode,
		localIncarnation: request.ReceiverIncarnation, localServiceKey: request.ReceiverServiceKeyDigest,
		readDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		writeDeadline: func() time.Time { return time.Now().Add(time.Second) },
	}
	client, remote := net.Pipe()
	defer client.Close()
	defer remote.Close()
	wrongPeer := &rf3PreparedAckReaderTestConnection{Conn: client,
		identity: rafttransport.PeerIdentity{TrustDomain: reader.trust, Node: seed.NodeID},
		key:      [32]byte{99}, class: rafttransport.TrafficGatewayControl}
	if _, err := reader.readFromConnection(t.Context(), wrongPeer, seed, request); !errors.Is(err, errRF3FrontendDrainPreparedAckReaderAuth) {
		t.Fatalf("wrong peer key error=%v", err)
	}
	bad := request
	bad.SourcePrincipal = rafttransport.NodeID{77}
	if _, err := reader.readFromConnection(t.Context(), wrongPeer, seed, bad); !errors.Is(err, errRF3FrontendDrainPreparedAckReaderAuth) {
		t.Fatalf("wrong source principal error=%v", err)
	}
}

func TestRF3CanonicalSourceRosterPersistsAndRejectsEndpointTamper(t *testing.T) {
	path := t.TempDir() + "/frontend-drain-source-roster"
	roster := rf3CanonicalSourceRoster{
		Format: rf3CanonicalSourceRosterFormat,
		Floor: frontenddrain.PreparedAckCutReadFloor{
			DirectoryRevision: 7, DirectoryDigest: replication.Digest{1},
			CatalogGeneration: 3, CatalogHeadDigest: replication.Digest{2},
			ServiceDirectoryRevision: 11, ServiceDirectoryDigest: replication.Digest{3},
		},
		Seeds: []nodecontrol.BootstrapGatewaySeed{
			{NodeID: rafttransport.NodeID{1}, Incarnation: 4, ControlAddress: "source-a:9100", SPKIPinDigest: [32]byte{4}},
			{NodeID: rafttransport.NodeID{2}, Incarnation: 5, ControlAddress: "source-b:9100", SPKIPinDigest: [32]byte{5}},
		},
	}
	if err := persistRF3CanonicalSourceRoster(path, roster); err != nil {
		t.Fatalf("persist learned roster: %v", err)
	}
	loaded, err := loadRF3CanonicalSourceRoster(path)
	if err != nil || loaded == nil || loaded.Format != roster.Format || loaded.Floor != roster.Floor ||
		len(loaded.Seeds) != len(roster.Seeds) || loaded.Seeds[0] != roster.Seeds[0] || loaded.Seeds[1] != roster.Seeds[1] {
		t.Fatalf("reload learned roster=%+v err=%v, want=%+v", loaded, err, roster)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// An endpoint changed by a caller must fail the body digest check before it
	// can become a dial target. This keeps the learned source set tied to the
	// authenticated source cut that produced it.
	mutated := append([]byte(nil), raw...)
	mutated[10] ^= 1
	if err := os.WriteFile(path, mutated, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRF3CanonicalSourceRoster(path); !errors.Is(err, errRF3CanonicalSourceRoster) {
		t.Fatalf("tampered learned endpoint accepted: %v", err)
	}
}

func TestRF3PhysicalSourceInstallExactRequiresMatchingDrain(t *testing.T) {
	request, _, cut := rf3PreparedAckReaderTestRequest(t)
	seed := nodecontrol.BootstrapGatewaySeed{
		NodeID: request.ReceiverNode, Incarnation: request.ReceiverIncarnation,
		ControlAddress: "127.0.0.1:20002", SPKIPinDigest: [32]byte(request.ReceiverServiceKeyDigest),
	}
	query := frontenddrain.PreparedAckCutReadRequest{
		Operation: frontenddrain.CutOperationInstallExact, Nonce: [16]byte{44},
		DrainID: request.DrainID, GrantDigest: request.GrantDigest,
		ReceiverNode: request.ReceiverNode, ReceiverIncarnation: request.ReceiverIncarnation,
		ReceiverServiceKeyDigest: request.ReceiverServiceKeyDigest,
		ReceiverNodeRevision:     request.ReceiverNodeRevision, SourceFloor: cut.ReadFloor(), SourceCutDigest: cut.Digest(),
	}
	if !rf3FrontendDrainPreparedAckPhysicalSourceCutMatchesQuery(cut, query, seed) {
		t.Fatal("matching physical InstallExact query was rejected")
	}
	wrong := query
	wrong.GrantDigest = [32]byte{45}
	if rf3FrontendDrainPreparedAckPhysicalSourceCutMatchesQuery(cut, wrong, seed) {
		t.Fatal("physical source accepted InstallExact query for a different drain")
	}
}

func TestRF3FrontendDrainPreparedAckReaderInstallsAuthenticatedCutWithoutSeeds(t *testing.T) {
	request, seed, cut := rf3PreparedAckReaderTestRequest(t)
	reader := &rf3FrontendDrainPreparedAckCutReader{
		trust: cut.ServiceDirectory.TrustDomain, localNode: request.ReceiverNode,
		localIncarnation: request.ReceiverIncarnation, localServiceKey: request.ReceiverServiceKeyDigest,
		readDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		writeDeadline: func() time.Time { return time.Now().Add(time.Second) },
	}
	got, err := reader.ReadFrontendDrainPreparedAckCut(t.Context(), request)
	if err != nil {
		t.Fatalf("unmanaged install: %v", err)
	}
	if got.Digest() != cut.Digest() {
		t.Fatalf("unmanaged cut digest=%x, want %x", got.Digest(), cut.Digest())
	}

	wrongReceiver := request
	wrongReceiver.ReceiverNode = rafttransport.NodeID{99}
	if _, err = reader.ReadFrontendDrainPreparedAckCut(t.Context(), wrongReceiver); !errors.Is(err, errRF3FrontendDrainPreparedAckReaderAuth) {
		t.Fatalf("wrong receiver err=%v, want auth", err)
	}

	managed := &rf3FrontendDrainPreparedAckCutReader{
		trust: cut.ServiceDirectory.TrustDomain, localNode: request.ReceiverNode,
		localIncarnation: request.ReceiverIncarnation, localServiceKey: request.ReceiverServiceKeyDigest,
		readDeadline:  func() time.Time { return time.Now().Add(time.Second) },
		writeDeadline: func() time.Time { return time.Now().Add(time.Second) },
		seeds: map[rafttransport.NodeID]nodecontrol.BootstrapGatewaySeed{
			rafttransport.NodeID{9}: seed,
		},
	}
	if _, err = managed.ReadFrontendDrainPreparedAckCut(t.Context(), request); !errors.Is(err, errRF3FrontendDrainPreparedAckReaderUnavailable) {
		t.Fatalf("managed missing publisher err=%v, want unavailable", err)
	}
	applied, err := rf3NonmanagedPreparedAckInstaller{}.InstallFrontendDrainServiceCut(t.Context(), cut)
	if err != nil || applied != cut.ServiceDirectoryRevision {
		t.Fatalf("nonmanaged install applied=%d err=%v", applied, err)
	}
}

func rf3PreparedAckReaderTestRequest(t *testing.T) (
	frontenddrain.PreparedAckRequest, nodecontrol.BootstrapGatewaySeed, frontenddrain.PreparedAckCut,
) {
	t.Helper()
	trust := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	storage := rafttransport.NodeID{1}
	source := rafttransport.NodeID{2}
	storageKey := [32]byte{3}
	sourceKey := [32]byte{4}
	drainID := [32]byte{5}
	group := raftmember.GroupKey{ClusterID: [16]byte{6}, ClusterIncarnation: [16]byte{7}, ShardIncarnation: [16]byte{8}, GroupID: [16]byte{9}}
	scope := serviceauthz.FrontendContinuationScopeRecord{
		Protocol: serviceauthz.FrontendScopeNative, Action: serviceauthz.FrontendActionForwardedData,
		Capability: serviceauthz.CapabilityDataRead, Operation: serviceauthz.ServiceOperationForwardedRead,
		Group: group,
	}
	grant, err := serviceauthz.NewCommittedFrontendContinuationGrant(serviceauthz.CommittedFrontendContinuationGrant{
		TrustDomain: trust, PhysicalNode: storage, PhysicalIncarnation: 2, PeerKeyDigest: sourceKey,
		GatewayServiceID: source, GatewaySessionID: [16]byte{10}, GatewaySessionRevision: 1,
		DrainID: drainID, AdmissionEpoch: 1, AcceptedConnectionTokens: []serviceauthz.FrontendConnToken{{11}},
		AcceptedConnectionProtocols: []serviceauthz.FrontendContinuationScope{serviceauthz.FrontendScopeNative},
		AdmissionClosedProofDigest:  [32]byte{12},
		Revision:                    1, State: serviceauthz.ContinuationGrantPrepared,
	})
	if err != nil {
		t.Fatal(err)
	}
	serviceCut := serviceauthz.ServiceDirectoryCut{
		CatalogGeneration: 1, Revision: 1, TrustDomain: trust, PolicyGeneration: 1,
		Bindings: []serviceauthz.ServiceBinding{
			{Principal: storage, PhysicalNode: storage, PhysicalIncarnation: 2, KeyDigest: storageKey,
				Roles: serviceauthz.ServiceRoleStorage, Lifecycle: serviceauthz.ServiceActive},
			{Principal: source, PhysicalNode: storage, PhysicalIncarnation: 2, KeyDigest: sourceKey,
				Roles: serviceauthz.ServiceRoleGateway, Lifecycle: serviceauthz.ServiceActive,
				GatewayIncarnation: seedIncarnation, SessionID: [16]byte{10}, SessionRevision: 1, ParticipantDigest: [32]byte{13}},
		}, ForwardedScopes: []serviceauthz.FrontendContinuationScopeRecord{scope},
		ContinuationGrants: []serviceauthz.CommittedFrontendContinuationGrant{grant},
	}
	cut := frontenddrain.PreparedAckCut{
		DirectoryRevision: 1, DirectoryDigest: [32]byte{14}, CatalogGeneration: 1, CatalogHeadDigest: [32]byte{15},
		ServiceDirectoryRevision: 1, ServiceDirectory: serviceCut,
		SourceRoster: []frontenddrain.PreparedAckSource{{
			NodeID: storage, Incarnation: 2, ControlAddress: "127.0.0.1:20002",
			SPKIPinDigest: storageKey,
		}},
	}
	request := frontenddrain.PreparedAckRequest{
		Nonce: [16]byte{16}, DrainID: drainID, GrantDigest: grant.GrantDigest,
		SourcePrincipal: source, SourcePrincipalKeyDigest: sourceKey, ReceiverNode: storage,
		ReceiverIncarnation: 2, ReceiverServiceKeyDigest: storageKey, ReceiverNodeRevision: 1, SourceCut: cut,
	}
	seed := nodecontrol.BootstrapGatewaySeed{
		NodeID: source, Incarnation: seedIncarnation, ControlAddress: "127.0.0.1:20001",
		SPKIPinDigest: [32]byte(sourceKey),
	}
	return request, seed, cut
}

const seedIncarnation = uint64(3)
