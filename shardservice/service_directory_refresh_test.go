package shardservice

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type serviceDirectoryRefreshTestReader struct {
	cut     frontenddrain.PreparedAckCut
	entered chan struct{}
	release chan struct{}
	calls   atomic.Uint64
	mu      sync.Mutex
	query   frontenddrain.ServiceCutReadLatestRequest
}

func (reader *serviceDirectoryRefreshTestReader) ReadLatestServiceCut(
	ctx context.Context, query frontenddrain.ServiceCutReadLatestRequest,
) (frontenddrain.ServiceCut, error) {
	if reader == nil || ctx == nil || query.Operation != frontenddrain.ReadLatest {
		return frontenddrain.ServiceCut{}, frontenddrain.ErrPreparedAckWire
	}
	reader.mu.Lock()
	reader.query = query
	reader.mu.Unlock()
	if reader.calls.Add(1) == 1 && reader.entered != nil {
		close(reader.entered)
	}
	if reader.release != nil {
		select {
		case <-reader.release:
		case <-ctx.Done():
			if cause := context.Cause(ctx); cause != nil {
				return frontenddrain.ServiceCut{}, cause
			}
			return frontenddrain.ServiceCut{}, context.Canceled
		}
	}
	return reader.cut, nil
}

func TestReplicatedServerServiceDirectoryMissRefreshesBeforeLocalExecution(t *testing.T) {
	fixture, initial, next, request := newServiceDirectoryRefreshFixture(t)
	if err := ValidateReplicatedRequest(&request); err != nil {
		t.Fatalf("request invalid: %+v continuation=%+v: %v", request, request.Continuation, err)
	}
	if _, err := fixture.server.InstallFrontendDrainServiceCut(t.Context(), initial); err != nil {
		t.Fatal(err)
	}
	reader := &serviceDirectoryRefreshTestReader{cut: next}
	if err := fixture.server.BindServiceDirectoryRefresh(reader, fixture.storage.LocalIdentity().Node,
		7, fixture.storage.LocalServiceKeyDigest()); err != nil {
		t.Fatal(err)
	}
	lease, err := fixture.server.DispatchReplicated(t.Context(), ReplicatedCall{Request: request})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := DetachReplicatedReply(lease)
	if err != nil || reply.Response.Kind != ReplicatedReadFound {
		t.Fatalf("refreshed local reply=%+v err=%v", reply, err)
	}
	if got := reader.calls.Load(); got != 1 {
		t.Fatalf("canonical refresh calls=%d, want 1", got)
	}
	if fixture.owner.readCalled == nil {
		t.Fatal("owner read witness was not configured")
	}
	select {
	case <-fixture.owner.readCalled:
	default:
		t.Fatal("owner read did not execute after refresh")
	}
	floor, ok := fixture.server.ServiceCutCoordinates()
	if !ok || floor != next.ReadFloor() {
		t.Fatalf("retained floor=%+v/%t, want %+v", floor, ok, next.ReadFloor())
	}

	unknown := request
	unknown.Continuation = cloneFrontendContinuation(request.Continuation)
	unknown.Continuation.ConnToken[0]++
	lease, err = fixture.server.DispatchReplicated(t.Context(), ReplicatedCall{Request: unknown})
	if err != nil {
		t.Fatal(err)
	}
	reply, err = DetachReplicatedReply(lease)
	if err != nil || reply.Response.Kind != ReplicatedRefusal || reply.Response.Refusal != ReplicatedRefusalUnauthorized {
		t.Fatalf("unknown token reply=%+v err=%v", reply, err)
	}
	if got := reader.calls.Load(); got != 1 {
		t.Fatalf("unknown token triggered refresh calls=%d", got)
	}

	unknownResource := request
	unknownResource.Continuation = cloneFrontendContinuation(request.Continuation)
	unknownResource.Relation = 3
	unknownResource.Continuation.Scope.Relation = [16]byte{15: 3}
	if err := unknownResource.SetFrontendContinuation(*unknownResource.Continuation); err != nil {
		t.Fatal(err)
	}
	lease, err = fixture.server.DispatchReplicated(t.Context(), ReplicatedCall{Request: unknownResource})
	if err != nil {
		t.Fatal(err)
	}
	reply, err = DetachReplicatedReply(lease)
	if err != nil || reply.Response.Kind != ReplicatedRefusal || reply.Response.Refusal != ReplicatedRefusalUnauthorized {
		t.Fatalf("unknown resource reply=%+v err=%v", reply, err)
	}
	if got := reader.calls.Load(); got != 2 {
		t.Fatalf("unknown resource refresh calls=%d, want one additional canonical read", got)
	}

	wrongPeer := serviceauthz.AuthenticatedPeer{Identity: fixture.gateway.LocalIdentity(), KeyDigest: [32]byte{99}}
	if fixture.server.authorizeReplicatedPeerWithDirectoryRefresh(t.Context(), wrongPeer, &request) {
		t.Fatal("wrong physical peer authorized after refresh")
	}
	wrongProtocol := request
	wrongProtocol.Continuation = cloneFrontendContinuation(request.Continuation)
	wrongProtocol.Continuation.Scope.Protocol = serviceauthz.FrontendScopePostgreSQL
	if fixture.server.authorizeReplicatedPeerWithDirectoryRefresh(t.Context(),
		serviceauthz.AuthenticatedPeer{Identity: fixture.gateway.LocalIdentity(), KeyDigest: fixture.gateway.LocalPeerKeyDigest()}, &wrongProtocol) {
		t.Fatal("cross-protocol continuation authorized")
	}
	unknownAuthority := request
	unknownAuthority.Authority.Node = rafttransport.NodeID{94}
	if fixture.server.authorizeReplicatedPeerWithDirectoryRefresh(t.Context(),
		serviceauthz.AuthenticatedPeer{Identity: fixture.gateway.LocalIdentity(), KeyDigest: fixture.gateway.LocalPeerKeyDigest()}, &unknownAuthority) {
		t.Fatal("unknown forwarded authority authorized")
	}
	if got := reader.calls.Load(); got != 2 {
		t.Fatalf("invalid identity/protocol triggered refresh calls=%d", got)
	}
}

func TestReplicatedServerServiceDirectoryMissCoalescesSocketRefresh(t *testing.T) {
	fixture, initial, next, request := newServiceDirectoryRefreshFixture(t)
	if _, err := fixture.server.InstallFrontendDrainServiceCut(t.Context(), initial); err != nil {
		t.Fatal(err)
	}
	reader := &serviceDirectoryRefreshTestReader{cut: next, entered: make(chan struct{}), release: make(chan struct{})}
	if err := fixture.server.BindServiceDirectoryRefresh(reader, fixture.storage.LocalIdentity().Node,
		7, fixture.storage.LocalServiceKeyDigest()); err != nil {
		t.Fatal(err)
	}
	peer := serviceauthz.AuthenticatedPeer{Identity: fixture.gateway.LocalIdentity(), KeyDigest: fixture.gateway.LocalPeerKeyDigest()}
	serveOne := func() (*ReplicatedResponse, error) {
		client, serverConn := net.Pipe()
		done := make(chan error, 1)
		go func() {
			done <- fixture.server.serveReplicatedRequestAuthorized(
				t.Context(), serverConn, peer.Identity.Node, true, peer, &FrameEncoder{},
			)
		}()
		if err := EncodeReplicatedRequest(client, &request); err != nil {
			_ = client.Close()
			return nil, err
		}
		response, err := DecodeReplicatedResponse(client)
		_ = client.Close()
		if serveErr := <-done; err == nil {
			err = serveErr
		}
		return response, err
	}
	responses := make(chan *ReplicatedResponse, 2)
	errorsCh := make(chan error, 2)
	go func() {
		response, err := serveOne()
		responses <- response
		errorsCh <- err
	}()
	go func() {
		response, err := serveOne()
		responses <- response
		errorsCh <- err
	}()
	select {
	case <-reader.entered:
	case <-time.After(time.Second):
		t.Fatal("socket refresh did not reach canonical source")
	}
	close(reader.release)
	for range 2 {
		if err := <-errorsCh; err != nil {
			t.Fatal(err)
		}
		response := <-responses
		if response == nil || response.Kind != ReplicatedReadFound {
			t.Fatalf("socket response=%+v", response)
		}
	}
	if got := reader.calls.Load(); got != 1 {
		t.Fatalf("coalesced socket refresh calls=%d, want 1", got)
	}
	if fixture.owner.readCalled == nil {
		t.Fatal("socket owner read witness was not configured")
	}
}

func TestReplicatedServerServiceDirectoryRefreshCancellationDeniesWithoutExecution(t *testing.T) {
	fixture, initial, next, request := newServiceDirectoryRefreshFixture(t)
	if _, err := fixture.server.InstallFrontendDrainServiceCut(t.Context(), initial); err != nil {
		t.Fatal(err)
	}
	reader := &serviceDirectoryRefreshTestReader{cut: next, entered: make(chan struct{}), release: make(chan struct{})}
	if err := fixture.server.BindServiceDirectoryRefresh(reader, fixture.storage.LocalIdentity().Node,
		7, fixture.storage.LocalServiceKeyDigest()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	lease, err := fixture.server.DispatchReplicated(ctx, ReplicatedCall{Request: request})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := DetachReplicatedReply(lease)
	if err != nil || reply.Response.Kind != ReplicatedRefusal || reply.Response.Refusal != ReplicatedRefusalUnauthorized {
		t.Fatalf("canceled refresh reply=%+v err=%v", reply, err)
	}
	if got := reader.calls.Load(); got != 1 {
		t.Fatalf("canceled refresh calls=%d, want 1", got)
	}
	if fixture.owner.readCalled != nil {
		select {
		case <-fixture.owner.readCalled:
			t.Fatal("canceled refresh reached owner read")
		default:
		}
	}
}

func TestReplicatedServerServiceDirectoryInternalScopeMissRefreshesBeforeProbe(t *testing.T) {
	state := testReplicatedServingState()
	owner := &fakeReplicatedOwner{state: state}
	fixture := bindSemanticServer(t, owner, time.Second)
	policy, err := serviceauthz.NewPolicy(2, []serviceauthz.Entry{
		{Node: fixture.gateway.LocalIdentity().Node, Capabilities: serviceauthz.CapabilityDelegate | serviceauthz.CapabilityTopology},
		{Node: fixture.actor.Node, Capabilities: serviceauthz.CapabilityDataRead},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.gate.Rotate(policy); err != nil {
		t.Fatal(err)
	}
	trust := fixture.gateway.LocalIdentity().TrustDomain
	group := testReplicatedFence().Group
	digest := testReplicatedFence().Command.RelationManifestDigest
	var relation [16]byte
	copy(relation[:], digest[:])
	storage := serviceauthz.ServiceBinding{Principal: fixture.storage.LocalIdentity().Node,
		PhysicalNode: fixture.storage.LocalIdentity().Node, PhysicalIncarnation: 7,
		KeyDigest: fixture.storage.LocalServiceKeyDigest(), Roles: serviceauthz.ServiceRoleStorage,
		Lifecycle: serviceauthz.ServiceActive}
	gateway := serviceauthz.ServiceBinding{Principal: fixture.gateway.LocalIdentity().Node,
		PhysicalNode: fixture.storage.LocalIdentity().Node, PhysicalIncarnation: 7,
		KeyDigest: fixture.gateway.LocalServiceKeyDigest(), Roles: serviceauthz.ServiceRoleGateway,
		Lifecycle: serviceauthz.ServiceActive, GatewayIncarnation: 1, SessionID: [16]byte{2},
		SessionRevision: 3, ParticipantDigest: [32]byte{4}}
	fence := serviceauthz.ServiceFence{Action: serviceauthz.ServiceActionGatewayCatalogRead,
		Operation: serviceauthz.ServiceOperationCatalogRead, Group: group, Relation: relation,
		SessionID: gateway.SessionID, SessionRevision: gateway.SessionRevision,
		IntentID: digest, FenceDigest: digest}
	gateway.InternalFences = []serviceauthz.ServiceFence{fence}
	initialDirectory := serviceauthz.ServiceDirectoryCut{CatalogGeneration: 1, Revision: 1,
		TrustDomain: trust, PolicyGeneration: 2,
		Bindings: []serviceauthz.ServiceBinding{storage, gateway}}
	advancedGateway := gateway
	advancedGateway.InternalFences = []serviceauthz.ServiceFence{fence}
	advancedDirectory := initialDirectory
	advancedDirectory.CatalogGeneration = 2
	advancedDirectory.Bindings = []serviceauthz.ServiceBinding{storage, advancedGateway}
	initial := frontenddrain.PreparedAckCut{DirectoryRevision: 10, DirectoryDigest: [32]byte{10},
		CatalogGeneration: 1, CatalogHeadDigest: [32]byte{11}, ServiceDirectoryRevision: 1,
		ServiceDirectory: initialDirectory}
	next := frontenddrain.PreparedAckCut{DirectoryRevision: 10, DirectoryDigest: [32]byte{10},
		CatalogGeneration: 2, CatalogHeadDigest: [32]byte{12}, ServiceDirectoryRevision: 1,
		ServiceDirectory: advancedDirectory}
	// Build the old cut without the exact new resource, then publish the new
	// fence only in the source response. The request uses the catalog probe
	// grammar, so its scope is derived from the serving command rather than a
	// caller supplied internal tuple.
	initialDirectory.Bindings[1].InternalFences = nil
	initial.ServiceDirectory = initialDirectory
	if !initial.Valid() || !next.Valid() || !next.AtLeastFloor(initial.ReadFloor()) {
		t.Fatalf("invalid internal refresh fixture initial=%t next=%t floor=%t", initial.Valid(), next.Valid(), next.AtLeastFloor(initial.ReadFloor()))
	}
	fenceWire := testReplicatedFence()
	request := ReplicatedRequest{Operation: ReplicatedProbe,
		Authority:  serviceauthz.Authority{Node: fixture.gateway.LocalIdentity().Node, Generation: 2},
		Capability: serviceauthz.CapabilityTopology,
		Fence: ReplicatedFence{Group: fenceWire.Group, AllocationGeneration: fenceWire.AllocationGeneration,
			Command: fenceWire.Command}}
	if err := ValidateReplicatedRequest(&request); err != nil {
		t.Fatalf("internal probe invalid: %v", err)
	}
	if _, err := fixture.server.InstallFrontendDrainServiceCut(t.Context(), initial); err != nil {
		t.Fatal(err)
	}
	reader := &serviceDirectoryRefreshTestReader{cut: next}
	if err := fixture.server.BindServiceDirectoryRefresh(reader, fixture.storage.LocalIdentity().Node,
		7, fixture.storage.LocalServiceKeyDigest()); err != nil {
		t.Fatal(err)
	}
	lease, err := fixture.server.DispatchReplicated(t.Context(), ReplicatedCall{Request: request})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := DetachReplicatedReply(lease)
	if err != nil || reply.Response.Kind != ReplicatedHandshake {
		t.Fatalf("internal refresh reply=%+v err=%v", reply, err)
	}
	if got := reader.calls.Load(); got != 1 {
		t.Fatalf("internal refresh calls=%d, want 1", got)
	}
	if got := owner.probeCalls.Load(); got != 1 {
		t.Fatalf("internal owner probes=%d, want 1", got)
	}
}

type serviceDirectoryRefreshFixture struct {
	server  *ReplicatedServer
	owner   *fakeReplicatedOwner
	storage *rafttransport.PeerTLS
	gateway *rafttransport.PeerTLS
}

func newServiceDirectoryRefreshFixture(
	t *testing.T,
) (serviceDirectoryRefreshFixture, frontenddrain.PreparedAckCut, frontenddrain.PreparedAckCut, ReplicatedRequest) {
	t.Helper()
	state := testReplicatedServingState()
	owner := &fakeReplicatedOwner{state: state, readResult: raftservice.PointReadResult{
		Applied: state.Status.Applied, Found: true, Value: []byte("value"),
	}, readCalled: make(chan struct{})}
	fixture := bindSemanticServer(t, owner, time.Second)
	trust := fixture.gateway.LocalIdentity().TrustDomain
	group := testReplicatedFence().Group
	storageNode := fixture.storage.LocalIdentity().Node
	gatewayNode := fixture.gateway.LocalIdentity().Node
	storage := serviceauthz.ServiceBinding{Principal: storageNode, PhysicalNode: storageNode,
		PhysicalIncarnation: 7, KeyDigest: fixture.storage.LocalServiceKeyDigest(),
		Roles: serviceauthz.ServiceRoleStorage, Lifecycle: serviceauthz.ServiceActive}
	gateway := serviceauthz.ServiceBinding{Principal: gatewayNode, PhysicalNode: storageNode,
		PhysicalIncarnation: 7, KeyDigest: fixture.gateway.LocalServiceKeyDigest(),
		Roles: serviceauthz.ServiceRoleGateway, Lifecycle: serviceauthz.ServiceActive,
		GatewayIncarnation: 1, SessionID: [16]byte{2}, SessionRevision: 3,
		ParticipantDigest: [32]byte{4}}
	token := serviceauthz.FrontendConnToken{5}
	grant, err := serviceauthz.NewCommittedFrontendContinuationGrant(serviceauthz.CommittedFrontendContinuationGrant{
		TrustDomain: trust, PhysicalNode: storageNode, PhysicalIncarnation: 7,
		PeerKeyDigest: fixture.gateway.LocalServiceKeyDigest(), GatewayServiceID: gatewayNode,
		GatewaySessionID: gateway.SessionID, GatewaySessionRevision: gateway.SessionRevision,
		DrainID: [32]byte{6}, AdmissionEpoch: 1, AcceptedConnectionTokens: []serviceauthz.FrontendConnToken{token},
		AcceptedConnectionProtocols: []serviceauthz.FrontendContinuationScope{serviceauthz.FrontendScopeNative},
		AdmissionClosedProofDigest:  [32]byte{7}, Revision: 1,
		State: serviceauthz.ContinuationGrantPrepared,
	})
	if err != nil {
		t.Fatal(err)
	}
	oldScope := serviceauthz.FrontendContinuationScopeRecord{Protocol: serviceauthz.FrontendScopeNative,
		Action: serviceauthz.FrontendActionForwardedData, Capability: serviceauthz.CapabilityDataRead,
		Operation: serviceauthz.ServiceOperationForwardedRead, Group: group, Relation: [16]byte{15: 1}}
	newScope := oldScope
	newScope.Relation = [16]byte{15: 2}
	directory := func(catalogGeneration uint64, scopes []serviceauthz.FrontendContinuationScopeRecord) serviceauthz.ServiceDirectoryCut {
		return serviceauthz.ServiceDirectoryCut{CatalogGeneration: catalogGeneration, Revision: 1,
			TrustDomain: trust, PolicyGeneration: fixture.gate.Generation(),
			Bindings: []serviceauthz.ServiceBinding{storage, gateway}, ForwardedScopes: scopes,
			ContinuationGrants: []serviceauthz.CommittedFrontendContinuationGrant{grant}}
	}
	initialDirectory := directory(1, []serviceauthz.FrontendContinuationScopeRecord{oldScope})
	nextDirectory := directory(2, []serviceauthz.FrontendContinuationScopeRecord{oldScope, newScope})
	initial := frontenddrain.PreparedAckCut{DirectoryRevision: 10, DirectoryDigest: [32]byte{10},
		CatalogGeneration: 1, CatalogHeadDigest: [32]byte{11}, ServiceDirectoryRevision: 1,
		ServiceDirectory: initialDirectory}
	next := frontenddrain.PreparedAckCut{DirectoryRevision: 10, DirectoryDigest: [32]byte{10},
		CatalogGeneration: 2, CatalogHeadDigest: [32]byte{12}, ServiceDirectoryRevision: 1,
		ServiceDirectory: nextDirectory}
	if !initial.Valid() || !next.Valid() || !next.AtLeastFloor(initial.ReadFloor()) {
		t.Fatalf("invalid refresh fixture initial=%t next=%t floor=%t", initial.Valid(), next.Valid(), next.AtLeastFloor(initial.ReadFloor()))
	}
	fence := testReplicatedFence()
	request := ReplicatedRequest{Operation: ReplicatedReadLeader,
		Authority:  serviceauthz.Authority{Node: fixture.actor.Node, Generation: fixture.gate.Generation()},
		Capability: serviceauthz.CapabilityDataRead,
		Fence:      fence, Relation: 2, Key: []byte("key"),
		MinimumApplied: state.Status.Applied, MaxValueBytes: 1024}
	envelope := serviceauthz.FrontendContinuationEnvelope{GrantDigest: grant.GrantDigest,
		ConnToken: token, Scope: newScope}
	if err := request.SetFrontendContinuation(envelope); err != nil {
		t.Fatal(err)
	}
	return serviceDirectoryRefreshFixture{server: fixture.server, owner: owner, storage: fixture.storage, gateway: fixture.gateway}, initial, next, request
}

func cloneFrontendContinuation(envelope *serviceauthz.FrontendContinuationEnvelope) *serviceauthz.FrontendContinuationEnvelope {
	if envelope == nil {
		return nil
	}
	copy := *envelope
	return &copy
}
