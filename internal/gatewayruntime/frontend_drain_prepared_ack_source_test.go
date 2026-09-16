package gatewayruntime

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type frontendDrainSourceTestConnection struct {
	input  *bytes.Reader
	output bytes.Buffer
	peer   rafttransport.PeerIdentity
	key    [32]byte
	class  rafttransport.TrafficClass
	closed bool
}

func (connection *frontendDrainSourceTestConnection) Read(dst []byte) (int, error) {
	return connection.input.Read(dst)
}

func (connection *frontendDrainSourceTestConnection) Write(src []byte) (int, error) {
	return connection.output.Write(src)
}

func (connection *frontendDrainSourceTestConnection) Close() error {
	connection.closed = true
	return nil
}

func (*frontendDrainSourceTestConnection) LocalAddr() net.Addr              { return frontendDrainSourceTestAddr{} }
func (*frontendDrainSourceTestConnection) RemoteAddr() net.Addr             { return frontendDrainSourceTestAddr{} }
func (*frontendDrainSourceTestConnection) SetDeadline(time.Time) error      { return nil }
func (*frontendDrainSourceTestConnection) SetReadDeadline(time.Time) error  { return nil }
func (*frontendDrainSourceTestConnection) SetWriteDeadline(time.Time) error { return nil }
func (connection *frontendDrainSourceTestConnection) PeerIdentity() rafttransport.PeerIdentity {
	return connection.peer
}
func (connection *frontendDrainSourceTestConnection) PeerKeyDigest() [32]byte { return connection.key }
func (connection *frontendDrainSourceTestConnection) TrafficClass() rafttransport.TrafficClass {
	return connection.class
}

type frontendDrainSourceTestAddr struct{}

func (frontendDrainSourceTestAddr) Network() string { return "frontend-drain-source-test" }
func (frontendDrainSourceTestAddr) String() string  { return "frontend-drain-source-test" }

func frontendDrainSourceTestFixture(t *testing.T) (
	*rafttransport.PeerTLS, gateway.NodeRecord, gateway.FrontendDrainRuntimeCut,
	frontenddrain.PreparedAckCut, frontenddrain.PreparedAckCutReadRequest,
) {
	t.Helper()
	profiles, _ := runtimeControlTLSFixture(t, []serviceauthz.Entry{
		{Node: rafttransport.NodeID{3}, Capabilities: serviceauthz.AllCapabilities},
	})
	profile := profiles[0]
	trust := profile.LocalIdentity().TrustDomain
	physical := rafttransport.NodeID{4}
	gatewayNode := profile.LocalIdentity().Node
	gatewayKey := profile.LocalServiceKeyDigest()
	node := gateway.NodeRecord{
		NodeID: physical, Incarnation: 2, ServiceKeyDigest: replication.Digest{10},
		DataEndpoint: "source-data", NativeEndpoint: "source-native", ControlEndpoint: "source-control",
		GatewayEndpoint: "source-gateway", DataAddress: "127.0.0.1:8101", NativeAddress: "127.0.0.1:8102",
		ControlAddress: "127.0.0.1:8103", GatewayAddress: "127.0.0.1:8104", FailureDomain: "source-zone",
		Roles: gateway.NodeRoleStorage | gateway.NodeRoleGateway, Lifecycle: gateway.NodeActive,
		Revision: 7, CatalogGeneration: 1,
		Gateway: gateway.GatewayIdentity{NodeID: gatewayNode, Incarnation: 3,
			ServiceKeyDigest: replication.Digest(gatewayKey), ServiceID: [16]byte{12}, SessionID: [16]byte{13},
			SessionRevision: 14, ParticipantDigest: replication.Digest{15}},
	}
	if !node.Valid() {
		t.Fatal("source receiver fixture is invalid")
	}
	grant, err := serviceauthz.NewCommittedFrontendContinuationGrant(serviceauthz.CommittedFrontendContinuationGrant{
		TrustDomain: trust, PhysicalNode: physical, PhysicalIncarnation: node.Incarnation,
		PeerKeyDigest: gatewayKey, GatewayServiceID: gatewayNode, GatewaySessionID: [16]byte{13},
		GatewaySessionRevision: 14, DrainID: [32]byte{31}, AdmissionEpoch: 32,
		AcceptedConnectionTokens:    []serviceauthz.FrontendConnToken{{33}},
		AcceptedConnectionProtocols: []serviceauthz.FrontendContinuationScope{serviceauthz.FrontendScopeNative},
		AdmissionClosedProofDigest:  [32]byte{34}, Revision: 35,
		State: serviceauthz.ContinuationGrantPrepared,
	})
	if err != nil {
		t.Fatal(err)
	}
	catalog := catalogRouteSeedSnapshot(t, 1, "127.0.0.1:9101")
	source := gateway.FrontendDrainRuntimeCut{
		Nodes: gateway.NodeDirectoryCut{Revision: 7, Digest: replication.Digest{36},
			CatalogGeneration: catalog.Generation(), Nodes: []gateway.NodeRecord{node}},
		Catalog: catalog, CatalogHeadDigest: replication.Digest{37}, ServiceDirectoryRevision: 3,
		ContinuationGrants: []serviceauthz.CommittedFrontendContinuationGrant{grant},
	}
	serviceCut, err := runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut(t.Context(), source, profile, 1)
	if err != nil {
		t.Fatalf("project source service cut: %v", err)
	}
	cut := frontenddrain.PreparedAckCut{
		DirectoryRevision: source.Nodes.Revision, DirectoryDigest: source.Nodes.Digest,
		CatalogGeneration: source.Nodes.CatalogGeneration, CatalogHeadDigest: source.CatalogHeadDigest,
		ServiceDirectoryRevision: serviceCut.Revision, ServiceDirectory: serviceCut,
	}
	if !cut.Valid() {
		t.Fatal("projected source cut is invalid")
	}
	request := frontenddrain.PreparedAckCutReadRequest{
		Operation: frontenddrain.CutOperationInstallExact,
		Nonce:     [16]byte{38}, DrainID: grant.DrainID, GrantDigest: grant.GrantDigest,
		ReceiverNode: physical, ReceiverIncarnation: node.Incarnation,
		ReceiverServiceKeyDigest: [32]byte{10}, ReceiverNodeRevision: node.Revision,
		SourceFloor:     cut.ReadFloor(),
		SourceCutDigest: cut.Digest(),
	}
	return profile, node, source, cut, request
}

func TestFrontendDrainPreparedAckCutReadDispatchAndAuthorityProjection(t *testing.T) {
	profile, node, source, cut, request := frontendDrainSourceTestFixture(t)
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	authorize := func(connection rafttransport.PeerConnection) bool {
		return connection.TrafficClass() == rafttransport.TrafficGatewayControl &&
			connection.PeerIdentity().TrustDomain == profile.LocalIdentity().TrustDomain &&
			connection.PeerIdentity().Node == node.NodeID && connection.PeerKeyDigest() == [32]byte{10}
	}
	connection := &frontendDrainSourceTestConnection{
		input: bytes.NewReader(request.Marshal()),
		peer:  rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: node.NodeID},
		key:   [32]byte{10}, class: rafttransport.TrafficGatewayControl,
	}
	if err := serveFrontendDrainPreparedAckCutReadConnectionWith(t.Context(), connection,
		authorize,
		func(_ context.Context, got rafttransport.NodeID, incarnation uint64) (gateway.NodeRecord, error) {
			if got != node.NodeID || incarnation != node.Incarnation {
				return gateway.NodeRecord{}, gateway.ErrScalingIdentity
			}
			return node, nil
		}, func(context.Context) (gateway.FrontendDrainRuntimeCut, error) { return source, nil },
		profile, 1, deadline, deadline); err != nil {
		t.Fatalf("source-cut endpoint: %v", err)
	}
	responseBytes := connection.output.Bytes()
	if len(responseBytes) < frontenddrain.PreparedAckCutReadResponseHeaderBytes+32 {
		t.Fatalf("short source-cut response: %d", len(responseBytes))
	}
	cutBytes := binary.LittleEndian.Uint32(responseBytes[frontenddrain.PreparedAckCutReadResponseCutBytesOffset : frontenddrain.PreparedAckCutReadResponseCutBytesOffset+4])
	responseLength := frontenddrain.PreparedAckCutReadResponseHeaderBytes + int(cutBytes) + 32
	if responseLength != len(responseBytes) {
		t.Fatalf("source-cut response length=%d, actual=%d", responseLength, len(responseBytes))
	}
	response, err := frontenddrain.OpenPreparedAckCutReadResponse(responseBytes, request)
	if err != nil || response.Cut.Digest() != cut.Digest() {
		t.Fatalf("source-cut response=%+v err=%v", response, err)
	}

	// The top-level control dispatcher must select this endpoint before the
	// generic control service. A missing runtime authority reaches the selected
	// handler's explicit auth guard rather than falling through.
	discriminator := frontenddrain.PreparedAckCutReadDiscriminator
	dispatchConnection := &frontendDrainSourceTestConnection{
		input: bytes.NewReader(discriminator[:]), class: rafttransport.TrafficGatewayControl,
	}
	dispatchRuntime := &Runtime{controlReadDeadline: deadline}
	if err := dispatchRuntime.serveGatewayControlConnection(t.Context(), dispatchConnection); !errors.Is(err, errFrontendDrainPreparedAckSourceAuth) {
		t.Fatalf("source-cut discriminator did not dispatch to endpoint: %v", err)
	}
}

func TestFrontendDrainPreparedAckCutReadReturnsAuthenticatedMovedFloor(t *testing.T) {
	profile, node, source, _, request := frontendDrainSourceTestFixture(t)
	advanced := source
	advanced.Nodes = source.Nodes
	advanced.Nodes.Revision++
	advanced.Nodes.Digest = replication.Digest{39}
	serviceCut, err := runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut(
		t.Context(), advanced, profile, 1)
	if err != nil {
		t.Fatalf("project advanced source service cut: %v", err)
	}
	advancedCut, err := frontendDrainPreparedAckCutFromRuntimeCut(advanced, serviceCut)
	if err != nil {
		t.Fatalf("project advanced source cut: %v", err)
	}
	if !advancedCut.AtLeastFloor(request.SourceFloor) || advancedCut.Digest() == request.SourceCutDigest {
		t.Fatalf("advanced source cut did not move floor: floor=%+v digest=%x prior=%x", advancedCut.ReadFloor(), advancedCut.Digest(), request.SourceCutDigest)
	}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	authorize := func(connection rafttransport.PeerConnection) bool {
		return connection.TrafficClass() == rafttransport.TrafficGatewayControl &&
			connection.PeerIdentity().TrustDomain == profile.LocalIdentity().TrustDomain &&
			connection.PeerIdentity().Node == node.NodeID && connection.PeerKeyDigest() == [32]byte{10}
	}
	readNode := func(_ context.Context, got rafttransport.NodeID, incarnation uint64) (gateway.NodeRecord, error) {
		if got != node.NodeID || incarnation != node.Incarnation {
			return gateway.NodeRecord{}, gateway.ErrScalingIdentity
		}
		return node, nil
	}
	connection := &frontendDrainSourceTestConnection{
		input: bytes.NewReader(request.Marshal()),
		peer:  rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: node.NodeID},
		key:   [32]byte{10}, class: rafttransport.TrafficGatewayControl,
	}
	if err := serveFrontendDrainPreparedAckCutReadConnectionWith(t.Context(), connection,
		authorize, readNode, func(context.Context) (gateway.FrontendDrainRuntimeCut, error) {
			return advanced, nil
		}, profile, 1, deadline, deadline); err != nil {
		t.Fatalf("advanced source endpoint: %v", err)
	}
	moved, err := frontenddrain.OpenPreparedAckCutReadMovedResponse(connection.output.Bytes(), request)
	if err != nil {
		t.Fatalf("open authenticated moved response: %v", err)
	}
	if moved.SourceFloor != advancedCut.ReadFloor() || moved.SourceCutDigest != advancedCut.Digest() {
		t.Fatalf("moved response floor=%+v digest=%x, want floor=%+v digest=%x",
			moved.SourceFloor, moved.SourceCutDigest, advancedCut.ReadFloor(), advancedCut.Digest())
	}

	// A changed digest at the same scalar floor is an equivocation and must
	// remain a terminal source-state response; it is not a moved retry.
	equalFloor := source
	equalFloor.Nodes = source.Nodes
	equalFloor.Nodes.Digest = replication.Digest{40}
	equalConnection := &frontendDrainSourceTestConnection{
		input: bytes.NewReader(request.Marshal()),
		peer:  rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: node.NodeID},
		key:   [32]byte{10}, class: rafttransport.TrafficGatewayControl,
	}
	if err := serveFrontendDrainPreparedAckCutReadConnectionWith(t.Context(), equalConnection,
		authorize, readNode, func(context.Context) (gateway.FrontendDrainRuntimeCut, error) {
			return equalFloor, nil
		}, profile, 1, deadline, deadline); !errors.Is(err, errFrontendDrainPreparedAckSourceState) {
		t.Fatalf("equal-floor source mutation error=%v, want source state", err)
	}
	if equalConnection.output.Len() != 0 {
		t.Fatalf("equal-floor source mutation emitted %d bytes", equalConnection.output.Len())
	}
}

func TestFrontendDrainPreparedAckCutReadPhysicalShardSource(t *testing.T) {
	profile, node, source, _, request := frontendDrainSourceTestFixture(t)
	// The source process is a catalog-owning storage node with no gateway
	// frontend. Its identity is therefore proved by the physical node record,
	// while the requester's identity remains the exact receiver binding.
	node.Roles |= gateway.NodeRoleCatalog
	source.Nodes.Nodes[0] = node
	serviceCut, err := runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut(
		t.Context(), source, profile, 1)
	if err != nil {
		t.Fatalf("project physical source cut: %v", err)
	}
	expected, err := frontendDrainPreparedAckCutFromRuntimeCut(source, serviceCut)
	if err != nil {
		t.Fatalf("build physical source cut: %v", err)
	}
	request.SourceFloor, request.SourceCutDigest = expected.ReadFloor(), expected.Digest()
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	trust := profile.LocalIdentity().TrustDomain
	service, err := NewFrontendDrainPreparedAckCutReadService(
		FrontendDrainPreparedAckCutReadServiceOptions{
			Authorize: func(connection rafttransport.PeerConnection) bool {
				return connection.TrafficClass() == rafttransport.TrafficShardControl &&
					connection.PeerIdentity().TrustDomain == trust &&
					connection.PeerIdentity().Node == node.NodeID &&
					connection.PeerKeyDigest() == [32]byte{10}
			},
			ReadCut: func(context.Context) (gateway.FrontendDrainRuntimeCut, error) {
				return source, nil
			},
			Profile: profile, PolicyGeneration: 1,
			TrafficClass: rafttransport.TrafficShardControl,
			SourceNode:   node.NodeID, SourceIncarnation: node.Incarnation,
			SourceServiceKeyDigest: [32]byte{10},
			ReadDeadline:           deadline, WriteDeadline: deadline,
		},
	)
	if err != nil {
		t.Fatalf("new physical source service: %v", err)
	}
	connection := &frontendDrainSourceTestConnection{
		input: bytes.NewReader(request.Marshal()),
		peer:  rafttransport.PeerIdentity{TrustDomain: trust, Node: node.NodeID},
		key:   [32]byte{10}, class: rafttransport.TrafficShardControl,
	}
	if err := service.Serve(t.Context(), connection); err != nil {
		t.Fatalf("physical source service: %v", err)
	}
	response, err := frontenddrain.OpenPreparedAckCutReadResponse(connection.output.Bytes(), request)
	if err != nil || response.Cut.Digest() != expected.Digest() {
		t.Fatalf("physical source response=%+v err=%v", response, err)
	}

	wrongSource, err := NewFrontendDrainPreparedAckCutReadService(
		FrontendDrainPreparedAckCutReadServiceOptions{
			Authorize: func(rafttransport.PeerConnection) bool { return true },
			ReadCut:   func(context.Context) (gateway.FrontendDrainRuntimeCut, error) { return source, nil },
			Profile:   profile, PolicyGeneration: 1,
			TrafficClass: rafttransport.TrafficShardControl,
			SourceNode:   profile.LocalIdentity().Node, SourceIncarnation: 1,
			SourceServiceKeyDigest: profile.LocalServiceKeyDigest(),
			ReadDeadline:           deadline, WriteDeadline: deadline,
		},
	)
	if err != nil {
		t.Fatalf("new wrong-source service: %v", err)
	}
	rejected := &frontendDrainSourceTestConnection{
		input: bytes.NewReader(request.Marshal()),
		peer:  rafttransport.PeerIdentity{TrustDomain: trust, Node: node.NodeID},
		key:   [32]byte{10}, class: rafttransport.TrafficShardControl,
	}
	if err := wrongSource.Serve(t.Context(), rejected); !errors.Is(err, errFrontendDrainPreparedAckSourceAuth) {
		t.Fatalf("wrong physical source accepted: %v", err)
	}
}

func TestFrontendDrainPreparedAckCutReadLatestAllowsBootstrapAndFloor(t *testing.T) {
	profile, node, source, cut, install := frontendDrainSourceTestFixture(t)
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	query := install
	query.Operation = frontenddrain.CutOperationReadLatest
	query.DrainID = [32]byte{}
	query.GrantDigest = [32]byte{}
	query.ReceiverNodeRevision = 0
	query.SourceFloor = frontenddrain.PreparedAckCutReadFloor{}
	query.SourceCutDigest = [32]byte{}
	authorize := func(connection rafttransport.PeerConnection) bool {
		return connection.TrafficClass() == rafttransport.TrafficGatewayControl &&
			connection.PeerIdentity().TrustDomain == profile.LocalIdentity().TrustDomain &&
			connection.PeerIdentity().Node == node.NodeID && connection.PeerKeyDigest() == [32]byte{10}
	}
	readNode := func(_ context.Context, got rafttransport.NodeID, incarnation uint64) (gateway.NodeRecord, error) {
		if got != node.NodeID || incarnation != node.Incarnation {
			return gateway.NodeRecord{}, gateway.ErrScalingIdentity
		}
		return node, nil
	}
	connection := &frontendDrainSourceTestConnection{
		input: bytes.NewReader(query.Marshal()),
		peer:  rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: node.NodeID},
		key:   [32]byte{10}, class: rafttransport.TrafficGatewayControl,
	}
	if err := serveFrontendDrainPreparedAckCutReadConnectionWith(t.Context(), connection,
		authorize, readNode, func(context.Context) (gateway.FrontendDrainRuntimeCut, error) {
			return source, nil
		}, profile, 1, deadline, deadline); err != nil {
		t.Fatalf("ReadLatest endpoint: %v", err)
	}
	responseRaw := connection.output.Bytes()
	response, err := frontenddrain.OpenPreparedAckCutReadResponse(responseRaw, query)
	if err != nil || response.Cut.Digest() != cut.Digest() {
		t.Fatalf("ReadLatest response=%+v err=%v", response, err)
	}

	// A complete non-zero floor is enforced independently of the receiver's
	// physical node revision. Returning an older source directory is denied.
	floorQuery := query
	floorQuery.SourceFloor = cut.ReadFloor()
	stale := source
	stale.Nodes.Revision--
	stale.Nodes.Digest = replication.Digest{99}
	staleConnection := &frontendDrainSourceTestConnection{
		input: bytes.NewReader(floorQuery.Marshal()),
		peer:  rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: node.NodeID},
		key:   [32]byte{10}, class: rafttransport.TrafficGatewayControl,
	}
	if err := serveFrontendDrainPreparedAckCutReadConnectionWith(t.Context(), staleConnection,
		authorize, readNode, func(context.Context) (gateway.FrontendDrainRuntimeCut, error) {
			return stale, nil
		}, profile, 1, deadline, deadline); !errors.Is(err, errFrontendDrainPreparedAckSourceState) {
		t.Fatalf("stale ReadLatest source error=%v", err)
	}
}

func TestFrontendDrainPreparedAckCutReadRejectsPeerIncarnationGrantAndState(t *testing.T) {
	profile, node, source, _, request := frontendDrainSourceTestFixture(t)
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	baseAuthorize := func(connection rafttransport.PeerConnection) bool {
		return connection.TrafficClass() == rafttransport.TrafficGatewayControl &&
			connection.PeerIdentity().TrustDomain == profile.LocalIdentity().TrustDomain &&
			connection.PeerIdentity().Node == node.NodeID && connection.PeerKeyDigest() == [32]byte{10}
	}
	readNode := func(_ context.Context, got rafttransport.NodeID, incarnation uint64) (gateway.NodeRecord, error) {
		if got != node.NodeID || incarnation != node.Incarnation {
			return gateway.NodeRecord{}, gateway.ErrScalingIdentity
		}
		return node, nil
	}
	for _, test := range []struct {
		name      string
		peerKey   [32]byte
		mutateReq func(*frontenddrain.PreparedAckCutReadRequest)
		mutateCut func(*gateway.FrontendDrainRuntimeCut)
		want      error
	}{
		{name: "wrong peer key", peerKey: [32]byte{41}, want: errFrontendDrainPreparedAckSourceAuth},
		{name: "wrong incarnation", peerKey: [32]byte{10}, mutateReq: func(got *frontenddrain.PreparedAckCutReadRequest) {
			got.ReceiverIncarnation++
		}, want: errFrontendDrainPreparedAckSourceState},
		{name: "wrong grant", peerKey: [32]byte{10}, mutateReq: func(got *frontenddrain.PreparedAckCutReadRequest) {
			got.GrantDigest = [32]byte{42}
		}, want: errFrontendDrainPreparedAckSourceState},
		{name: "non-prepared grant", peerKey: [32]byte{10}, mutateCut: func(got *gateway.FrontendDrainRuntimeCut) {
			got.ContinuationGrants[0].State = serviceauthz.ContinuationGrantEnforcing
		}, want: errFrontendDrainPreparedAckSourceState},
	} {
		t.Run(test.name, func(t *testing.T) {
			gotRequest := request
			if test.mutateReq != nil {
				test.mutateReq(&gotRequest)
			}
			gotSource := source
			gotSource.ContinuationGrants = append([]serviceauthz.CommittedFrontendContinuationGrant(nil), source.ContinuationGrants...)
			if test.mutateCut != nil {
				test.mutateCut(&gotSource)
			}
			connection := &frontendDrainSourceTestConnection{
				input: bytes.NewReader(gotRequest.Marshal()),
				peer:  rafttransport.PeerIdentity{TrustDomain: profile.LocalIdentity().TrustDomain, Node: node.NodeID},
				key:   test.peerKey, class: rafttransport.TrafficGatewayControl,
			}
			err := serveFrontendDrainPreparedAckCutReadConnectionWith(t.Context(), connection,
				baseAuthorize, readNode,
				func(context.Context) (gateway.FrontendDrainRuntimeCut, error) { return gotSource, nil },
				profile, 1, deadline, deadline)
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
			if connection.output.Len() != 0 {
				t.Fatalf("rejected source-cut query emitted %d bytes", connection.output.Len())
			}
		})
	}
}

var _ io.ReadWriter = (*frontendDrainSourceTestConnection)(nil)
