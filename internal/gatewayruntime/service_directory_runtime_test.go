package gatewayruntime

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type runtimeCanonicalCutReaderTest struct {
	gateway.DirectoryReader
	cut gateway.FrontendDrainRuntimeCut
}

func (reader *runtimeCanonicalCutReaderTest) ReadNodeDirectoryCut(context.Context) (gateway.NodeDirectoryCut, error) {
	return reader.cut.Nodes, nil
}

func (reader *runtimeCanonicalCutReaderTest) ReadFrontendDrainRuntimeCut(context.Context) (gateway.FrontendDrainRuntimeCut, error) {
	return reader.cut, nil
}

type runtimeFullServiceCutTransportTest struct {
	gateway.ReplicatedRoundTripper
	gate        *serviceauthz.ServiceDirectoryGate
	coordinates frontenddrain.PreparedAckCutReadFloor
	set         bool
}

func (transport *runtimeFullServiceCutTransportTest) InstallFrontendDrainServiceCut(
	_ context.Context, cut frontenddrain.PreparedAckCut,
) (uint64, error) {
	if !cut.Valid() {
		return 0, frontenddrain.ErrPreparedAckState
	}
	floor := cut.ReadFloor()
	if !floor.Valid() || floor == (frontenddrain.PreparedAckCutReadFloor{}) ||
		transport.set && !cut.AtLeastFloor(transport.coordinates) {
		return 0, serviceauthz.ErrServiceDirectoryStale
	}
	if transport.gate == nil {
		gate, err := serviceauthz.NewServiceDirectoryGate(cut.ServiceDirectory)
		if err != nil {
			return 0, err
		}
		transport.gate = gate
	} else if err := transport.gate.ApplyCommittedCut(cut.ServiceDirectory); err != nil {
		return 0, err
	}
	transport.coordinates, transport.set = floor, true
	return cut.ServiceDirectoryRevision, nil
}

func (transport *runtimeFullServiceCutTransportTest) ServiceDirectoryGate() *serviceauthz.ServiceDirectoryGate {
	if transport == nil {
		return nil
	}
	return transport.gate
}

func (transport *runtimeFullServiceCutTransportTest) ServiceCutCoordinates() (frontenddrain.PreparedAckCutReadFloor, bool) {
	if transport == nil {
		return frontenddrain.PreparedAckCutReadFloor{}, false
	}
	return transport.coordinates, transport.set
}

func TestRuntimeControlDirectoryUpdatesRetainedFullServiceCut(t *testing.T) {
	profile, _, source, _, _ := frontendDrainSourceTestFixture(t)
	_, policy := runtimeControlTLSFixture(t, []serviceauthz.Entry{{
		Node: profile.LocalIdentity().Node, Capabilities: serviceauthz.AllCapabilities,
	}})
	reader := &runtimeCanonicalCutReaderTest{cut: source}
	transport := new(runtimeFullServiceCutTransportTest)
	runtime := &Runtime{config: Config{
		ControlDirectory:               reader,
		TLSProfile:                     profile,
		Authorization:                  policy,
		Transport:                      transport,
		RequireServiceDirectoryBinding: true,
	}, ctx: t.Context()}
	if err := runtime.openControlDirectory(); err != nil {
		t.Fatalf("open control directory: %v", err)
	}
	retained := transport.gate
	if retained == nil || runtime.serviceDirectory != retained {
		t.Fatalf("initial service gate was not shared: runtime=%p transport=%p", runtime.serviceDirectory, retained)
	}
	initialFloor, ok := transport.ServiceCutCoordinates()
	initialServiceCut, err := runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut(
		t.Context(), source, profile, policy.Generation())
	if err != nil {
		t.Fatal(err)
	}
	initialFullCut, err := frontendDrainPreparedAckCutFromRuntimeCut(source, initialServiceCut)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || initialFloor != initialFullCut.ReadFloor() {
		t.Fatalf("initial full floor=%+v set=%v want=%+v", initialFloor, ok, initialFullCut.ReadFloor())
	}

	next := source
	next.Nodes.Revision++
	next.Nodes.Digest[0]++
	next.ServiceDirectoryRevision++
	reader.cut = next
	nextSnapshot := gateway.ReplicatedControlDirectorySnapshot{
		Revision: next.Nodes.Revision, CatalogGeneration: next.Nodes.CatalogGeneration,
		Nodes: next.Nodes.CurrentNodes(),
	}
	if err := runtime.applyLiveControlDirectory(t.Context(), nextSnapshot); err != nil {
		t.Fatalf("apply live full service cut: %v", err)
	}
	if runtime.serviceDirectory != retained || transport.gate != retained {
		t.Fatalf("live publication replaced retained gate: runtime=%p transport=%p retained=%p", runtime.serviceDirectory, transport.gate, retained)
	}
	nextServiceCut, err := runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut(
		t.Context(), next, profile, policy.Generation())
	if err != nil {
		t.Fatal(err)
	}
	nextFullCut, err := frontendDrainPreparedAckCutFromRuntimeCut(next, nextServiceCut)
	if err != nil {
		t.Fatal(err)
	}
	gotFloor, ok := transport.ServiceCutCoordinates()
	if !ok || gotFloor != nextFullCut.ReadFloor() {
		t.Fatalf("live full floor=%+v set=%v want=%+v", gotFloor, ok, nextFullCut.ReadFloor())
	}
}

func TestRuntimeControlDirectoryPropagatesLifecycleCutsToRetainedReceiver(t *testing.T) {
	profile, subject, source, _, _ := frontendDrainSourceTestFixture(t)
	_, policy := runtimeControlTLSFixture(t, []serviceauthz.Entry{{
		Node: profile.LocalIdentity().Node, Capabilities: serviceauthz.AllCapabilities,
	}})
	survivor := gateway.NodeRecord{
		NodeID: rafttransport.NodeID{5}, Incarnation: 1, ServiceKeyDigest: replication.Digest{60},
		DataEndpoint: "survivor-data", NativeEndpoint: "survivor-native", ControlEndpoint: "survivor-control",
		DataAddress: "127.0.0.1:8201", NativeAddress: "127.0.0.1:8202", ControlAddress: "127.0.0.1:8203",
		FailureDomain: "survivor-zone", Roles: gateway.NodeRoleStorage, Lifecycle: gateway.NodeActive,
		Revision: 1, CatalogGeneration: 1,
	}
	if !survivor.Valid() {
		t.Fatal("survivor fixture is invalid")
	}
	source.Nodes.Nodes = append(source.Nodes.Nodes, survivor)
	if !source.Nodes.Valid() {
		t.Fatal("source lifecycle directory is invalid")
	}
	reader := &runtimeCanonicalCutReaderTest{cut: source}
	transport := new(runtimeFullServiceCutTransportTest)
	runtime := &Runtime{config: Config{
		ControlDirectory:               reader,
		TLSProfile:                     profile,
		Authorization:                  policy,
		Transport:                      transport,
		RequireServiceDirectoryBinding: true,
	}, ctx: t.Context()}
	if err := runtime.openControlDirectory(); err != nil {
		t.Fatalf("open lifecycle control directory: %v", err)
	}
	retained := transport.gate
	if retained == nil || runtime.serviceDirectory != retained {
		t.Fatalf("initial lifecycle gate was not shared: runtime=%p transport=%p", runtime.serviceDirectory, retained)
	}

	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}}
	serviceFence := serviceauthz.ServiceFence{
		Action: serviceauthz.ServiceActionGatewayCatalogRead, Operation: serviceauthz.ServiceOperationCatalogRead,
		Group: group, Relation: [16]byte{61}, SessionID: subject.Gateway.SessionID,
		SessionRevision: subject.Gateway.SessionRevision, IntentID: [32]byte{62}, FenceDigest: [32]byte{63},
	}
	drainFence := serviceauthz.CommittedFrontendDrainFence{
		TrustDomain: source.ContinuationGrants[0].TrustDomain, PhysicalNode: subject.NodeID,
		PhysicalIncarnation: subject.Incarnation, PeerKeyDigest: source.ContinuationGrants[0].PeerKeyDigest,
		GatewayServiceID: subject.Gateway.NodeID, GatewaySessionID: subject.Gateway.SessionID,
		GatewaySessionRevision: subject.Gateway.SessionRevision, DrainID: source.ContinuationGrants[0].DrainID,
		Revision: subject.Revision + 1, Fence: serviceFence,
	}
	if !drainFence.Valid() {
		t.Fatal("enforcing drain fence is invalid")
	}

	enforcing := source
	enforcingNode := subject
	enforcingNode.Lifecycle, enforcingNode.Revision = gateway.NodeDraining, subject.Revision+1
	enforcing.Nodes.Nodes[0] = enforcingNode
	enforcing.Nodes.Revision = source.Nodes.Revision + 1
	enforcing.ContinuationGrants = append([]serviceauthz.CommittedFrontendContinuationGrant(nil), source.ContinuationGrants...)
	enforcing.ContinuationGrants[0].State = serviceauthz.ContinuationGrantEnforcing
	enforcing.ServiceDirectoryRevision = source.ServiceDirectoryRevision + 1
	enforcing.DrainFences = []serviceauthz.CommittedFrontendDrainFence{drainFence}
	reader.cut = enforcing
	enforcingSnapshot := gateway.ReplicatedControlDirectorySnapshot{
		Revision: enforcing.Nodes.Revision, CatalogGeneration: enforcing.Nodes.CatalogGeneration,
		Nodes: enforcing.Nodes.CurrentNodes(),
	}
	if err := runtime.applyLiveControlDirectory(t.Context(), enforcingSnapshot); err != nil {
		t.Fatalf("apply enforcing lifecycle cut: %v", err)
	}

	catalogOnly := enforcing
	catalogOnly.Catalog = catalogRouteSeedSnapshot(t, 2, "127.0.0.1:9101")
	catalogOnly.CatalogHeadDigest[0]++
	catalogOnly.Nodes.Revision++
	catalogOnly.Nodes.Digest[0]++
	catalogOnly.Nodes.CatalogGeneration = 2
	for index := range catalogOnly.Nodes.Nodes {
		catalogOnly.Nodes.Nodes[index].CatalogGeneration = 2
	}
	reader.cut = catalogOnly
	catalogSnapshot := gateway.ReplicatedControlDirectorySnapshot{
		Revision: catalogOnly.Nodes.Revision, CatalogGeneration: catalogOnly.Nodes.CatalogGeneration,
		Nodes: catalogOnly.Nodes.CurrentNodes(),
	}
	if err := runtime.applyLiveControlDirectory(t.Context(), catalogSnapshot); err != nil {
		t.Fatalf("apply catalog-only lifecycle cut: %v", err)
	}

	retired := catalogOnly
	retiredNode := enforcingNode
	retiredNode.Lifecycle, retiredNode.Revision = gateway.NodeDecommissioned, enforcingNode.Revision+1
	retiredNode.RetirementScanDigest = replication.Digest{64}
	retiredNode.RetirementScanDirectoryRevision, retiredNode.RetirementScanCutRevision = enforcingNode.Revision, 1
	retired.Nodes.Nodes[0] = retiredNode
	retired.Nodes.Revision++
	retired.Nodes.Digest[0]++
	retired.ContinuationGrants = append([]serviceauthz.CommittedFrontendContinuationGrant(nil), catalogOnly.ContinuationGrants...)
	retired.ContinuationGrants[0].State = serviceauthz.ContinuationGrantRetired
	retired.ServiceDirectoryRevision++
	reader.cut = retired
	retiredSnapshot := gateway.ReplicatedControlDirectorySnapshot{
		Revision: retired.Nodes.Revision, CatalogGeneration: retired.Nodes.CatalogGeneration,
		Nodes: retired.Nodes.CurrentNodes(),
	}
	if err := runtime.applyLiveControlDirectory(t.Context(), retiredSnapshot); err != nil {
		t.Fatalf("apply retired lifecycle cut: %v", err)
	}
	if runtime.serviceDirectory != retained || transport.gate != retained {
		t.Fatalf("lifecycle publication replaced retained gate: runtime=%p transport=%p retained=%p", runtime.serviceDirectory, transport.gate, retained)
	}
	finalCut, err := runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut(
		t.Context(), retired, profile, policy.Generation())
	if err != nil {
		t.Fatal(err)
	}
	finalFullCut, err := frontendDrainPreparedAckCutFromRuntimeCut(retired, finalCut)
	if err != nil {
		t.Fatal(err)
	}
	gotFloor, ok := transport.ServiceCutCoordinates()
	if !ok || gotFloor != finalFullCut.ReadFloor() {
		t.Fatalf("retired full floor=%+v set=%v want=%+v", gotFloor, ok, finalFullCut.ReadFloor())
	}
	if gateCut, ok := retained.Cut(); !ok || len(gateCut.Bindings) != 3 {
		t.Fatalf("retained gate lost survivor or terminal binding: %+v", gateCut)
	}
}

type runtimeCatalogFenceReader struct {
	gateway.DirectoryReader
	fences        []serviceauthz.ServiceFence
	scopes        []serviceauthz.FrontendContinuationScopeRecord
	grants        []serviceauthz.CommittedFrontendContinuationGrant
	drainFences   []serviceauthz.CommittedFrontendDrainFence
	grantRevision uint64
}

// ReadCompleteServiceDirectoryCut is deliberately one call: this fixture
// supplies all projection material as one coherent test cut. Production code
// uses gateway.FrontendDrainRuntimeCutReader, whose catalog/head/node reads
// are verified against one authoritative source epoch.
func (reader runtimeCatalogFenceReader) ReadCompleteServiceDirectoryCut(context.Context) (serviceDirectoryCompleteCut, error) {
	revision := reader.grantRevision
	if revision == 0 {
		revision = 1
	}
	return serviceDirectoryCompleteCut{
		Revision:           revision,
		CatalogGeneration:  1,
		ContinuationGrants: reader.grants,
		DrainFences:        reader.drainFences,
		ForwardedScopes:    reader.scopes,
		ScopesGeneration:   1,
		CatalogFences:      reader.fences,
	}, nil
}

func TestRuntimeServiceDirectoryKeepsColocatedGatewayCatalogGrants(t *testing.T) {
	profiles, policy := runtimeControlTLSFixture(t, []serviceauthz.Entry{
		{Node: rafttransport.NodeID{1}, Capabilities: serviceauthz.AllCapabilities},
	})
	profile := profiles[0]
	key := replication.Digest{2}
	node := gateway.NodeRecord{
		NodeID: profile.LocalIdentity().Node, Incarnation: 1, ServiceKeyDigest: key,
		DataEndpoint: "peer", NativeEndpoint: "native", ControlEndpoint: "control", GatewayEndpoint: "gateway",
		DataAddress: "localhost:1", NativeAddress: "localhost:2", ControlAddress: "localhost:3", GatewayAddress: "localhost:4",
		FailureDomain: "worker", Roles: gateway.NodeRoleStorage | gateway.NodeRoleGateway | gateway.NodeRoleControl,
		Lifecycle: gateway.NodeActive, Revision: 1, CatalogGeneration: 1,
		Gateway: gateway.GatewayIdentity{
			NodeID: profile.LocalIdentity().Node, Incarnation: 3, ServiceKeyDigest: key,
			ServiceID: [16]byte{4}, SessionID: [16]byte{5}, SessionRevision: 6, ParticipantDigest: replication.Digest{7},
		},
	}
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}}
	reader := runtimeCatalogFenceReader{fences: []serviceauthz.ServiceFence{
		{Action: serviceauthz.ServiceActionGatewayCatalogRead, Operation: serviceauthz.ServiceOperationCatalogRead,
			Group: group, Relation: [16]byte{1}, IntentID: [32]byte{2}, FenceDigest: [32]byte{3}},
		{Action: serviceauthz.ServiceActionGatewayCatalogWrite, Operation: serviceauthz.ServiceOperationCatalogWrite,
			Group: group, Relation: [16]byte{1}, IntentID: [32]byte{4}, FenceDigest: [32]byte{5}},
	}}
	snapshot := gateway.ReplicatedControlDirectorySnapshot{Revision: 1, CatalogGeneration: 1, Nodes: []gateway.NodeRecord{node}}
	cut, err := runtimeServiceDirectoryCut(t.Context(), reader, snapshot, profile, policy.Generation())
	if err != nil {
		t.Fatal(err)
	}
	gate, err := serviceauthz.NewServiceDirectoryGate(cut)
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Bindings) != 1 || cut.Bindings[0].Roles != serviceauthz.ServiceRoleStorage|serviceauthz.ServiceRoleGateway|serviceauthz.ServiceRoleController {
		t.Fatalf("physical and gateway identities did not merge: %+v", cut.Bindings)
	}
	peer := serviceauthz.AuthenticatedPeer{Identity: profile.LocalIdentity(), KeyDigest: [32]byte(key)}
	authority := serviceauthz.Authority{Node: node.NodeID, Generation: policy.Generation()}
	for _, fence := range reader.fences {
		if fence.SessionID != ([16]byte{}) || fence.SessionRevision != 0 {
			t.Fatal("runtime changed shared catalog grant input")
		}
		request := serviceauthz.ServiceRequest{
			Action: fence.Action, Capability: serviceauthz.CapabilityTopology, Operation: fence.Operation,
			Group: fence.Group, Relation: fence.Relation, SessionID: node.Gateway.SessionID,
			SessionRevision: node.Gateway.SessionRevision, IntentID: fence.IntentID, FenceDigest: fence.FenceDigest,
		}
		if got := gate.CheckInternal(peer, authority, request); got != serviceauthz.DecisionAllow {
			t.Fatalf("co-located gateway lost exact catalog action %d: %v", fence.Action, got)
		}
		request.FenceDigest[0]++
		if got := gate.CheckInternal(peer, authority, request); got == serviceauthz.DecisionAllow {
			t.Fatal("merged gateway admitted an ungranted catalog fence")
		}
	}
}

func TestRuntimeServiceDirectoryProjectsCertifiedPhysicalReplacement(t *testing.T) {
	profiles, policy := runtimeControlTLSFixture(t, []serviceauthz.Entry{
		{Node: rafttransport.NodeID{0xef}, Capabilities: serviceauthz.AllCapabilities},
	})
	profile := profiles[0]
	nodeID := rafttransport.NodeID{0xd1}
	oldKey, newKey := replication.Digest{0xd2}, replication.Digest{0xd3}
	oldGateway := gateway.GatewayIdentity{
		NodeID: nodeID, Incarnation: 5, ServiceKeyDigest: oldKey,
		ServiceID: [16]byte{0xd4}, SessionID: [16]byte{0xd5}, SessionRevision: 6,
		ParticipantDigest: replication.Digest{0xd6},
	}
	predecessor := gateway.NodeRecord{
		NodeID: nodeID, Incarnation: 1, ServiceKeyDigest: oldKey,
		DataEndpoint: "replacement-old-data", NativeEndpoint: "replacement-old-native",
		ControlEndpoint: "replacement-old-control", GatewayEndpoint: "replacement-old-gateway",
		DataAddress: "127.0.0.1:9101", NativeAddress: "127.0.0.1:9102", ControlAddress: "127.0.0.1:9103",
		GatewayAddress: "127.0.0.1:9104", FailureDomain: "replacement-zone",
		Roles: gateway.NodeRoleStorage | gateway.NodeRoleGateway, Lifecycle: gateway.NodeDecommissioned,
		Revision: 2, CatalogGeneration: 1, Gateway: oldGateway,
		RetirementScanDigest: replication.Digest{0xd7}, RetirementScanDirectoryRevision: 1,
		RetirementScanCutRevision: 1,
	}
	if !predecessor.Valid() {
		t.Fatal("terminal predecessor fixture is invalid")
	}
	request := gateway.ScalingIntentRequest{Kind: gateway.ScalingDecommission, RequestID: [32]byte{0xd8},
		Drain: gateway.NodeReference{NodeID: nodeID, Incarnation: 1}, MaxMoves: 1, MaxMigrationBytes: 1 << 20}
	intent := gateway.ScalingIntent{ID: request.ID(), Request: request, CatalogGeneration: 1,
		Revision: 1, DirectoryRevision: 1, State: gateway.ScalingComplete}
	if !intent.Valid() {
		t.Fatal("replacement intent fixture is invalid")
	}
	successor := gateway.NodeRecord{
		NodeID: nodeID, Incarnation: 2, ServiceKeyDigest: newKey,
		DataEndpoint: "replacement-new-data", NativeEndpoint: "replacement-new-native",
		ControlEndpoint: "replacement-new-control", GatewayEndpoint: "replacement-new-gateway",
		DataAddress: "127.0.0.1:9201", NativeAddress: "127.0.0.1:9202", ControlAddress: "127.0.0.1:9203",
		GatewayAddress: "127.0.0.1:9204", FailureDomain: "replacement-zone",
		Roles: gateway.NodeRoleStorage | gateway.NodeRoleGateway, Lifecycle: gateway.NodeJoining,
		Revision: 1, CatalogGeneration: 1,
		Gateway: gateway.GatewayIdentity{
			NodeID: nodeID, Incarnation: 6, ServiceKeyDigest: newKey,
			ServiceID: [16]byte{0xd9}, SessionID: [16]byte{0xda}, SessionRevision: 7,
			ParticipantDigest: replication.Digest{0xdb},
		},
		Replacement: &gateway.NodeReplacementProof{
			PredecessorIncarnation: 1, PredecessorServiceKeyDigest: oldKey,
			PredecessorGateway: oldGateway, IntentID: intent.ID, ProofDigest: replication.Digest{0xdc},
		},
	}
	digest, err := gateway.CertifiedNodeReplacementDigest(predecessor, intent, successor)
	if err != nil {
		t.Fatalf("replacement proof digest: %v", err)
	}
	successor.Replacement.ProofDigest = digest
	if !successor.Valid() {
		t.Fatal("joining successor fixture is invalid")
	}

	oldSnapshot := gateway.ReplicatedControlDirectorySnapshot{Revision: 2, CatalogGeneration: 1,
		Nodes: []gateway.NodeRecord{predecessor}}
	oldCut, err := runtimeServiceDirectoryCut(t.Context(), runtimeCatalogFenceReader{grantRevision: 1},
		oldSnapshot, profile, policy.Generation())
	if err != nil {
		t.Fatalf("project terminal predecessor: %v", err)
	}
	if len(oldCut.Bindings) != 1 || len(oldCut.Replacements) != 0 {
		t.Fatalf("terminal predecessor projection bindings=%+v replacements=%+v", oldCut.Bindings, oldCut.Replacements)
	}
	active := successor
	active.Lifecycle, active.Revision = gateway.NodeActive, 2
	activeSnapshot := gateway.ReplicatedControlDirectorySnapshot{Revision: 3, CatalogGeneration: 1,
		Nodes: []gateway.NodeRecord{active}}
	activeCut, err := runtimeServiceDirectoryCut(t.Context(), runtimeCatalogFenceReader{grantRevision: 1},
		activeSnapshot, profile, policy.Generation())
	if err != nil {
		t.Fatalf("project certified successor: %v", err)
	}
	if len(activeCut.Bindings) != 1 || len(activeCut.Replacements) != 1 ||
		activeCut.Replacements[0].Prior.KeyDigest != [32]byte(oldKey) ||
		activeCut.Replacements[0].Next.KeyDigest != [32]byte(newKey) {
		t.Fatalf("replacement projection bindings=%+v replacements=%+v", activeCut.Bindings, activeCut.Replacements)
	}
	retained, err := serviceauthz.NewServiceDirectoryGate(oldCut)
	if err != nil {
		t.Fatal(err)
	}
	if err := retained.ApplyCommittedCut(activeCut); err != nil {
		t.Fatalf("retained gate rejected certified replacement: %v", err)
	}
	oldPeerIdentity := profile.LocalIdentity()
	oldPeerIdentity.Node = nodeID
	oldPeer := serviceauthz.AuthenticatedPeer{Identity: oldPeerIdentity, KeyDigest: [32]byte(oldKey)}
	newPeer := oldPeer
	newPeer.KeyDigest = [32]byte(newKey)
	if got := retained.CheckGatewayPeer(oldPeer); got == serviceauthz.DecisionAllow {
		t.Fatalf("retained gate accepted predecessor key: %v", got)
	}
	if got := retained.CheckGatewayPeer(newPeer); got != serviceauthz.DecisionAllow {
		t.Fatalf("retained gate rejected rotated successor key: %v", got)
	}
	fresh, err := serviceauthz.NewServiceDirectoryGate(activeCut)
	if err != nil {
		t.Fatal(err)
	}
	if got := fresh.CheckGatewayPeer(oldPeer); got == serviceauthz.DecisionAllow {
		t.Fatalf("fresh gate accepted predecessor key: %v", got)
	}
	if got := fresh.CheckGatewayPeer(newPeer); got != serviceauthz.DecisionAllow {
		t.Fatalf("fresh gate rejected rotated successor key: %v", got)
	}
}

func TestRuntimeServiceDirectoryProjectsAuthenticatedContinuationGrant(t *testing.T) {
	profiles, policy := runtimeControlTLSFixture(t, []serviceauthz.Entry{
		{Node: rafttransport.NodeID{9}, Capabilities: serviceauthz.AllCapabilities},
	})
	profile := profiles[0]
	key := replication.Digest{0x21}
	node := gateway.NodeRecord{
		NodeID: profile.LocalIdentity().Node, Incarnation: 4, ServiceKeyDigest: key,
		DataEndpoint: "peer", NativeEndpoint: "native", ControlEndpoint: "control", GatewayEndpoint: "gateway",
		DataAddress: "localhost:11", NativeAddress: "localhost:12", ControlAddress: "localhost:13", GatewayAddress: "localhost:14",
		FailureDomain: "worker", Roles: gateway.NodeRoleStorage | gateway.NodeRoleGateway | gateway.NodeRoleControl,
		Lifecycle: gateway.NodeDraining, Revision: 2, CatalogGeneration: 1,
		Gateway: gateway.GatewayIdentity{
			NodeID: profile.LocalIdentity().Node, Incarnation: 8, ServiceKeyDigest: key,
			ServiceID: [16]byte{0x22}, SessionID: [16]byte{0x23}, SessionRevision: 3, ParticipantDigest: replication.Digest{0x24},
		},
	}
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}}
	scope := serviceauthz.FrontendContinuationScopeRecord{
		Protocol: serviceauthz.FrontendScopePostgreSQL, Action: serviceauthz.FrontendActionForwardedData,
		Capability: serviceauthz.CapabilityDataRead, Operation: serviceauthz.ServiceOperationForwardedRead, Group: group,
	}
	grant, err := serviceauthz.NewCommittedFrontendContinuationGrant(serviceauthz.CommittedFrontendContinuationGrant{
		TrustDomain: profile.LocalIdentity().TrustDomain, PhysicalNode: node.NodeID, PhysicalIncarnation: node.Incarnation,
		PeerKeyDigest: [32]byte(key), GatewayServiceID: node.Gateway.NodeID, GatewaySessionID: node.Gateway.SessionID,
		GatewaySessionRevision: node.Gateway.SessionRevision, DrainID: [32]byte{0x25}, AdmissionEpoch: 1,
		AcceptedConnectionTokens:    []serviceauthz.FrontendConnToken{{0x26}},
		AcceptedConnectionProtocols: []serviceauthz.FrontendContinuationScope{serviceauthz.FrontendScopePostgreSQL},
		AdmissionClosedProofDigest:  [32]byte{0x27},
		Revision:                    node.Revision, State: serviceauthz.ContinuationGrantEnforcing,
	})
	if err != nil {
		t.Fatal(err)
	}
	drainFence := serviceauthz.CommittedFrontendDrainFence{
		TrustDomain: grant.TrustDomain, PhysicalNode: grant.PhysicalNode, PhysicalIncarnation: grant.PhysicalIncarnation,
		PeerKeyDigest: grant.PeerKeyDigest, GatewayServiceID: grant.GatewayServiceID,
		GatewaySessionID: grant.GatewaySessionID, GatewaySessionRevision: grant.GatewaySessionRevision,
		DrainID: grant.DrainID, Revision: node.Revision,
		Fence: serviceauthz.ServiceFence{Action: serviceauthz.ServiceActionGatewayCatalogRead,
			Operation: serviceauthz.ServiceOperationCatalogRead, Group: group, Relation: [16]byte{0x28},
			SessionID: node.Gateway.SessionID, SessionRevision: node.Gateway.SessionRevision,
			IntentID: [32]byte{0x29}, FenceDigest: [32]byte{0x2a}},
	}
	if !drainFence.Valid() {
		t.Fatal("drain fence fixture is invalid")
	}
	reader := runtimeCatalogFenceReader{grants: []serviceauthz.CommittedFrontendContinuationGrant{grant}, scopes: []serviceauthz.FrontendContinuationScopeRecord{scope}, drainFences: []serviceauthz.CommittedFrontendDrainFence{drainFence}}
	snapshot := gateway.ReplicatedControlDirectorySnapshot{Revision: node.Revision, CatalogGeneration: 1, Nodes: []gateway.NodeRecord{node}}
	cut, err := runtimeServiceDirectoryCut(t.Context(), reader, snapshot, profile, policy.Generation())
	if err != nil {
		t.Fatalf("project enforcing grant: %v", err)
	}
	if len(cut.ContinuationGrants) != 1 || cut.ContinuationGrants[0].GrantDigest != grant.GrantDigest {
		t.Fatalf("continuation grant was not retained in receiver cut: %+v", cut.ContinuationGrants)
	}
	if len(cut.Bindings) != 1 || cut.Bindings[0].DrainFenceDigest != grant.GrantDigest {
		t.Fatalf("draining binding lacks exact grant fence: %+v", cut.Bindings)
	}
	gate, err := serviceauthz.NewServiceDirectoryGate(cut)
	if err != nil {
		t.Fatal(err)
	}
	peer := serviceauthz.AuthenticatedPeer{Identity: profile.LocalIdentity(), KeyDigest: [32]byte(key)}
	envelope := serviceauthz.FrontendContinuationEnvelope{GrantDigest: grant.GrantDigest, ConnToken: grant.AcceptedConnectionTokens[0], Scope: scope}
	if got := gate.CheckFrontendContinuation(peer, policy.Generation(), envelope, scope); got != serviceauthz.DecisionAllow {
		t.Fatalf("receiver rejected authenticated continuation: %v", got)
	}
	foreign := grant
	foreign.PhysicalNode[0]++
	foreign.GrantDigest = foreign.Digest()
	foreignReader := runtimeCatalogFenceReader{grants: []serviceauthz.CommittedFrontendContinuationGrant{foreign}, scopes: []serviceauthz.FrontendContinuationScopeRecord{scope}}
	if _, err = runtimeServiceDirectoryCut(t.Context(), foreignReader, snapshot, profile, policy.Generation()); err == nil {
		t.Fatal("receiver accepted a grant for a foreign physical node")
	}
}

func TestRuntimeServiceDirectoryProjectsPreparedContinuationGrant(t *testing.T) {
	profiles, policy := runtimeControlTLSFixture(t, []serviceauthz.Entry{
		{Node: rafttransport.NodeID{11}, Capabilities: serviceauthz.AllCapabilities},
	})
	profile := profiles[0]
	key := replication.Digest{0x41}
	node := gateway.NodeRecord{
		NodeID: profile.LocalIdentity().Node, Incarnation: 6, ServiceKeyDigest: key,
		DataEndpoint: "peer-prepared", NativeEndpoint: "native-prepared", ControlEndpoint: "control-prepared", GatewayEndpoint: "gateway-prepared",
		DataAddress: "localhost:31", NativeAddress: "localhost:32", ControlAddress: "localhost:33", GatewayAddress: "localhost:34",
		FailureDomain: "worker", Roles: gateway.NodeRoleStorage | gateway.NodeRoleGateway | gateway.NodeRoleControl,
		Lifecycle: gateway.NodeActive, Revision: 3, CatalogGeneration: 1,
		Gateway: gateway.GatewayIdentity{
			NodeID: profile.LocalIdentity().Node, Incarnation: 10, ServiceKeyDigest: key,
			ServiceID: [16]byte{0x42}, SessionID: [16]byte{0x43}, SessionRevision: 5, ParticipantDigest: replication.Digest{0x44},
		},
	}
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{6}}
	scope := serviceauthz.FrontendContinuationScopeRecord{
		Protocol: serviceauthz.FrontendScopeNative, Action: serviceauthz.FrontendActionForwardedData,
		Capability: serviceauthz.CapabilityDataRead, Operation: serviceauthz.ServiceOperationForwardedRead, Group: group,
	}
	grant, err := serviceauthz.NewCommittedFrontendContinuationGrant(serviceauthz.CommittedFrontendContinuationGrant{
		TrustDomain: profile.LocalIdentity().TrustDomain, PhysicalNode: node.NodeID, PhysicalIncarnation: node.Incarnation,
		PeerKeyDigest: [32]byte(key), GatewayServiceID: node.Gateway.NodeID, GatewaySessionID: node.Gateway.SessionID,
		GatewaySessionRevision: node.Gateway.SessionRevision, DrainID: [32]byte{0x45}, AdmissionEpoch: 2,
		AcceptedConnectionTokens: []serviceauthz.FrontendConnToken{{0x46}}, AcceptedConnectionProtocols: []serviceauthz.FrontendContinuationScope{serviceauthz.FrontendScopeNative},
		AdmissionClosedProofDigest: [32]byte{0x47},
		Revision:                   node.Revision, State: serviceauthz.ContinuationGrantPrepared,
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes := []gateway.NodeRecord{node}
	snapshot := gateway.ReplicatedControlDirectorySnapshot{Revision: node.Revision, CatalogGeneration: 1, Nodes: nodes}
	// Install the no-drain source first. The Prepared commit gets its own
	// service-directory revision, so a receiver can apply the exact returned
	// Prepared cut and immediately replay that coherent cut without requiring a
	// further unrelated authority write to escape a poisoned same revision.
	prior, err := runtimeServiceDirectoryCut(t.Context(), runtimeCatalogFenceReader{grantRevision: 1},
		snapshot, profile, policy.Generation())
	if err != nil {
		t.Fatalf("project prior service cut: %v", err)
	}
	gate, err := serviceauthz.NewServiceDirectoryGate(prior)
	if err != nil {
		t.Fatalf("install prior service cut: %v", err)
	}
	reader := runtimeCatalogFenceReader{
		grants: []serviceauthz.CommittedFrontendContinuationGrant{grant}, scopes: []serviceauthz.FrontendContinuationScopeRecord{scope}, grantRevision: 2,
	}
	cut, err := runtimeServiceDirectoryCut(t.Context(), reader, snapshot, profile, policy.Generation())
	if err != nil {
		t.Fatalf("project prepared grant: %v", err)
	}
	if len(cut.ContinuationGrants) != 1 || cut.ContinuationGrants[0].GrantDigest != grant.GrantDigest || nodes[0] != node {
		t.Fatalf("prepared grant projection=%+v snapshot nodes=%+v", cut.ContinuationGrants, nodes)
	}
	if cut.Revision <= prior.Revision {
		t.Fatalf("prepared cut revision=%d did not advance prior=%d", cut.Revision, prior.Revision)
	}
	if err := gate.ApplyCommittedCut(cut); err != nil {
		t.Fatalf("receiver installed prepared cut revision=%d prior=%d err=%v", cut.Revision, prior.Revision, err)
	}
	following, err := runtimeServiceDirectoryCut(t.Context(), reader, snapshot, profile, policy.Generation())
	if err != nil || following.Revision != cut.Revision ||
		len(following.ContinuationGrants) != 1 || following.ContinuationGrants[0].GrantDigest != grant.GrantDigest ||
		gate.ApplyCommittedCut(following) != nil {
		t.Fatalf("receiver rejected immediate coherent prepared replay cut=%+v err=%v", following, err)
	}
}

func TestRuntimeServiceDirectoryAppliesPreparedEnforcingRetiredCuts(t *testing.T) {
	profiles, policy := runtimeControlTLSFixture(t, []serviceauthz.Entry{
		{Node: rafttransport.NodeID{12}, Capabilities: serviceauthz.AllCapabilities},
	})
	profile := profiles[0]
	key := replication.Digest{0x51}
	node := gateway.NodeRecord{
		NodeID: profile.LocalIdentity().Node, Incarnation: 7, ServiceKeyDigest: key,
		DataEndpoint: "peer-lifecycle", NativeEndpoint: "native-lifecycle", ControlEndpoint: "control-lifecycle", GatewayEndpoint: "gateway-lifecycle",
		DataAddress: "localhost:41", NativeAddress: "localhost:42", ControlAddress: "localhost:43", GatewayAddress: "localhost:44",
		FailureDomain: "worker", Roles: gateway.NodeRoleStorage | gateway.NodeRoleGateway | gateway.NodeRoleControl,
		Lifecycle: gateway.NodeActive, Revision: 1, CatalogGeneration: 1,
		Gateway: gateway.GatewayIdentity{
			NodeID: profile.LocalIdentity().Node, Incarnation: 11, ServiceKeyDigest: key,
			ServiceID: [16]byte{0x52}, SessionID: [16]byte{0x53}, SessionRevision: 6, ParticipantDigest: replication.Digest{0x54},
		},
	}
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{7}}
	scope := serviceauthz.FrontendContinuationScopeRecord{
		Protocol: serviceauthz.FrontendScopePostgreSQL, Action: serviceauthz.FrontendActionForwardedData,
		Capability: serviceauthz.CapabilityDataRead, Operation: serviceauthz.ServiceOperationForwardedRead, Group: group,
	}
	grant, err := serviceauthz.NewCommittedFrontendContinuationGrant(serviceauthz.CommittedFrontendContinuationGrant{
		TrustDomain: profile.LocalIdentity().TrustDomain, PhysicalNode: node.NodeID, PhysicalIncarnation: node.Incarnation,
		PeerKeyDigest: [32]byte(key), GatewayServiceID: node.Gateway.NodeID, GatewaySessionID: node.Gateway.SessionID,
		GatewaySessionRevision: node.Gateway.SessionRevision, DrainID: [32]byte{0x55}, AdmissionEpoch: 1,
		AcceptedConnectionTokens: []serviceauthz.FrontendConnToken{{0x56}}, AcceptedConnectionProtocols: []serviceauthz.FrontendContinuationScope{serviceauthz.FrontendScopePostgreSQL},
		AdmissionClosedProofDigest: [32]byte{0x57},
		Revision:                   node.Revision, State: serviceauthz.ContinuationGrantPrepared,
	})
	if err != nil {
		t.Fatal(err)
	}
	drainFence := serviceauthz.CommittedFrontendDrainFence{
		TrustDomain: grant.TrustDomain, PhysicalNode: grant.PhysicalNode, PhysicalIncarnation: grant.PhysicalIncarnation,
		PeerKeyDigest: grant.PeerKeyDigest, GatewayServiceID: grant.GatewayServiceID,
		GatewaySessionID: grant.GatewaySessionID, GatewaySessionRevision: grant.GatewaySessionRevision,
		DrainID: grant.DrainID, Revision: node.Revision,
		Fence: serviceauthz.ServiceFence{Action: serviceauthz.ServiceActionGatewayCatalogRead,
			Operation: serviceauthz.ServiceOperationCatalogRead, Group: group, Relation: [16]byte{0x48},
			SessionID: node.Gateway.SessionID, SessionRevision: node.Gateway.SessionRevision,
			IntentID: [32]byte{0x49}, FenceDigest: [32]byte{0x4a}},
	}
	if !drainFence.Valid() {
		t.Fatal("drain fence fixture is invalid")
	}
	reader := runtimeCatalogFenceReader{grants: []serviceauthz.CommittedFrontendContinuationGrant{grant}, scopes: []serviceauthz.FrontendContinuationScopeRecord{scope}, drainFences: []serviceauthz.CommittedFrontendDrainFence{drainFence}, grantRevision: 1}
	preparedCut, err := runtimeServiceDirectoryCut(t.Context(), reader,
		gateway.ReplicatedControlDirectorySnapshot{Revision: node.Revision, CatalogGeneration: 1, Nodes: []gateway.NodeRecord{node}},
		profile, policy.Generation())
	if err != nil {
		t.Fatalf("project prepared lifecycle cut: %v", err)
	}
	gate, err := serviceauthz.NewServiceDirectoryGate(preparedCut)
	if err != nil {
		t.Fatal(err)
	}

	draining := node
	draining.Lifecycle, draining.Revision = gateway.NodeDraining, node.Revision+1
	enforcing := grant
	enforcing.State = serviceauthz.ContinuationGrantEnforcing
	reader.grants = []serviceauthz.CommittedFrontendContinuationGrant{enforcing}
	reader.grantRevision = 2
	drainingCut, err := runtimeServiceDirectoryCut(t.Context(), reader,
		gateway.ReplicatedControlDirectorySnapshot{Revision: draining.Revision, CatalogGeneration: 1, Nodes: []gateway.NodeRecord{draining}},
		profile, policy.Generation())
	if err != nil {
		t.Fatalf("project enforcing lifecycle cut: %v", err)
	}
	if err := gate.ApplyCommittedCut(drainingCut); err != nil {
		t.Fatalf("apply enforcing lifecycle cut: %v", err)
	}

	terminal := draining
	terminal.Lifecycle, terminal.Revision = gateway.NodeDecommissioned, draining.Revision+1
	terminal.RetirementScanDigest = replication.Digest{0x58}
	terminal.RetirementScanDirectoryRevision, terminal.RetirementScanCutRevision = 1, 1
	retired := enforcing
	retired.State = serviceauthz.ContinuationGrantRetired
	reader.grants = []serviceauthz.CommittedFrontendContinuationGrant{retired}
	reader.grantRevision = 3
	terminalCut, err := runtimeServiceDirectoryCut(t.Context(), reader,
		gateway.ReplicatedControlDirectorySnapshot{Revision: terminal.Revision, CatalogGeneration: 1, Nodes: []gateway.NodeRecord{terminal}},
		profile, policy.Generation())
	if err != nil {
		t.Fatalf("project retired lifecycle cut: %v", err)
	}
	if err := gate.ApplyCommittedCut(terminalCut); err != nil {
		t.Fatalf("apply retired lifecycle cut: %v", err)
	}
	if cut, ok := gate.Cut(); !ok || len(cut.ContinuationGrants) != 1 || cut.ContinuationGrants[0].State != serviceauthz.ContinuationGrantRetired {
		t.Fatalf("retired continuation was not retained: %+v", cut)
	}
	peer := serviceauthz.AuthenticatedPeer{Identity: profile.LocalIdentity(), KeyDigest: [32]byte(key)}
	envelope := serviceauthz.FrontendContinuationEnvelope{GrantDigest: retired.GrantDigest,
		ConnToken: retired.AcceptedConnectionTokens[0], Scope: scope}
	if got := gate.CheckFrontendContinuation(peer, policy.Generation(), envelope, scope); got == serviceauthz.DecisionAllow {
		t.Fatalf("retired continuation admitted before compaction: %v", got)
	}
	compactedCut := terminalCut
	compactedCut.Revision++
	compactedCut.ContinuationGrants = nil
	missedRetired, err := serviceauthz.NewServiceDirectoryGate(drainingCut)
	if err != nil {
		t.Fatal(err)
	}
	if err := missedRetired.ApplyCommittedCut(compactedCut); err != nil {
		t.Fatalf("enforcing receiver rejected exact compacted terminal proof: %v", err)
	}
	if got := missedRetired.CheckFrontendContinuation(peer, policy.Generation(), envelope, scope); got == serviceauthz.DecisionAllow {
		t.Fatalf("enforcing receiver admitted saved credential after missed retired cut: %v", got)
	}
	if err := gate.ApplyCommittedCut(compactedCut); err != nil {
		t.Fatalf("apply terminal proof compaction: %v", err)
	}
	if got := gate.CheckFrontendContinuation(peer, policy.Generation(), envelope, scope); got == serviceauthz.DecisionAllow {
		t.Fatalf("existing gate admitted compacted retired credential: %v", got)
	}
	restarted, err := serviceauthz.NewServiceDirectoryGate(compactedCut)
	if err != nil {
		t.Fatalf("restart compacted gate: %v", err)
	}
	if got := restarted.CheckFrontendContinuation(peer, policy.Generation(), envelope, scope); got == serviceauthz.DecisionAllow {
		t.Fatalf("restarted gate admitted compacted retired credential: %v", got)
	}
}

func TestRuntimeServiceDirectoryProjectsDurableEmptyDrainFence(t *testing.T) {
	profiles, policy := runtimeControlTLSFixture(t, []serviceauthz.Entry{
		{Node: rafttransport.NodeID{10}, Capabilities: serviceauthz.AllCapabilities},
	})
	profile := profiles[0]
	key := replication.Digest{0x31}
	node := gateway.NodeRecord{
		NodeID: profile.LocalIdentity().Node, Incarnation: 5, ServiceKeyDigest: key,
		DataEndpoint: "peer-empty", NativeEndpoint: "native-empty", ControlEndpoint: "control-empty", GatewayEndpoint: "gateway-empty",
		DataAddress: "localhost:21", NativeAddress: "localhost:22", ControlAddress: "localhost:23", GatewayAddress: "localhost:24",
		FailureDomain: "worker", Roles: gateway.NodeRoleStorage | gateway.NodeRoleGateway | gateway.NodeRoleControl,
		Lifecycle: gateway.NodeDraining, Revision: 2, CatalogGeneration: 1,
		Gateway: gateway.GatewayIdentity{
			NodeID: profile.LocalIdentity().Node, Incarnation: 9, ServiceKeyDigest: key,
			ServiceID: [16]byte{0x32}, SessionID: [16]byte{0x33}, SessionRevision: 4, ParticipantDigest: replication.Digest{0x34},
		},
	}
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{5}}
	fence := serviceauthz.ServiceFence{Action: serviceauthz.ServiceActionGatewayCatalogRead,
		Operation: serviceauthz.ServiceOperationCatalogRead, Group: group, Relation: [16]byte{6},
		SessionID: node.Gateway.SessionID, SessionRevision: node.Gateway.SessionRevision,
		IntentID: [32]byte{7}, FenceDigest: [32]byte{8}}
	drainFence := serviceauthz.CommittedFrontendDrainFence{
		TrustDomain: profile.LocalIdentity().TrustDomain, PhysicalNode: node.NodeID, PhysicalIncarnation: node.Incarnation,
		PeerKeyDigest: [32]byte(key), GatewayServiceID: node.Gateway.NodeID, GatewaySessionID: node.Gateway.SessionID,
		GatewaySessionRevision: node.Gateway.SessionRevision, DrainID: frontendDrainID(FrontendDrainIdentity{
			NodeID: node.NodeID, Incarnation: node.Incarnation, GatewayNodeID: node.Gateway.NodeID,
			GatewayIncarnation: node.Gateway.Incarnation, SessionID: node.Gateway.SessionID, SessionRevision: node.Gateway.SessionRevision,
		}), Revision: node.Revision, Fence: fence,
	}
	if !drainFence.Valid() {
		t.Fatal("empty drain fence fixture is invalid")
	}
	reader := runtimeCatalogFenceReader{fences: []serviceauthz.ServiceFence{fence}, drainFences: []serviceauthz.CommittedFrontendDrainFence{drainFence}}
	snapshot := gateway.ReplicatedControlDirectorySnapshot{Revision: node.Revision, CatalogGeneration: 1, Nodes: []gateway.NodeRecord{node}}
	cut, err := runtimeServiceDirectoryCut(t.Context(), reader, snapshot, profile, policy.Generation())
	if err != nil {
		t.Fatalf("project empty drain fence: %v", err)
	}
	if len(cut.ContinuationGrants) != 0 || len(cut.Bindings) != 1 || cut.Bindings[0].DrainFenceDigest != fence.FenceDigest {
		t.Fatalf("empty drain projection = grants=%d bindings=%+v", len(cut.ContinuationGrants), cut.Bindings)
	}
	if _, err = serviceauthz.NewServiceDirectoryGate(cut); err != nil {
		t.Fatalf("empty drain cut rejected: %v", err)
	}
	foreign := drainFence
	foreign.PhysicalNode[0]++
	foreign.DrainID[0]++
	foreignReader := runtimeCatalogFenceReader{fences: []serviceauthz.ServiceFence{fence}, drainFences: []serviceauthz.CommittedFrontendDrainFence{foreign}}
	if _, err = runtimeServiceDirectoryCut(t.Context(), foreignReader, snapshot, profile, policy.Generation()); err == nil {
		t.Fatal("receiver accepted a foreign empty drain fence")
	}
}
