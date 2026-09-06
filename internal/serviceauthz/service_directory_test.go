package serviceauthz

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func serviceDirectoryGroup(seed byte) raftmember.GroupKey {
	return raftmember.GroupKey{ClusterID: [16]byte{seed}, ClusterIncarnation: [16]byte{seed + 1},
		TopologyRecoveryEpoch: 2, ShardIncarnation: [16]byte{seed + 2}, GroupID: [16]byte{seed + 3}}
}

func serviceDirectoryPeer(seed byte, key byte) AuthenticatedPeer {
	return AuthenticatedPeer{Identity: rafttransport.PeerIdentity{
		TrustDomain: rafttransport.TrustDomain{ClusterID: [16]byte{7}, ClusterIncarnation: [16]byte{8}},
		Node:        rafttransport.NodeID{seed},
	}, KeyDigest: [32]byte{key}}
}

func serviceDirectoryBinding(peer AuthenticatedPeer, roles ServiceRoleMask, lifecycle ServiceLifecycle) ServiceBinding {
	binding := ServiceBinding{Principal: peer.Identity.Node, PhysicalNode: peer.Identity.Node,
		PhysicalIncarnation: 11, KeyDigest: peer.KeyDigest, Roles: roles, Lifecycle: lifecycle}
	if roles&ServiceRoleGateway != 0 {
		binding.GatewayIncarnation = 13
		binding.SessionID = [16]byte{14}
		binding.SessionRevision = 15
		binding.ParticipantDigest = [32]byte{16}
	}
	if lifecycle == ServiceDraining && roles&ServiceRoleGateway != 0 {
		binding.DrainFenceDigest = [32]byte{17}
		binding.DrainFence = ServiceFence{Action: ServiceActionGatewayCatalogRead,
			Operation: ServiceOperationCatalogRead, Group: serviceDirectoryGroup(12),
			SessionID: binding.SessionID, SessionRevision: binding.SessionRevision,
			IntentID: [32]byte{18}, FenceDigest: binding.DrainFenceDigest}
		binding.InternalFences = []ServiceFence{binding.DrainFence}
	}
	if roles&ServiceRoleStorage != 0 {
		binding.InternalFences = append(binding.InternalFences, ServiceFence{
			Action: ServiceActionStorageBootstrapRead, Operation: ServiceOperationBootstrapMetadata,
			Group: serviceDirectoryGroup(4), IntentID: [32]byte{5}, FenceDigest: [32]byte{6},
		})
	}
	return binding
}

func serviceDirectoryCut(revision uint64, bindings ...ServiceBinding) ServiceDirectoryCut {
	return ServiceDirectoryCut{Revision: revision, TrustDomain: rafttransport.TrustDomain{
		ClusterID: [16]byte{7}, ClusterIncarnation: [16]byte{8}}, PolicyGeneration: 21, Bindings: bindings}
}

func serviceDirectoryContinuationGrant(peer AuthenticatedPeer, binding ServiceBinding,
	state ContinuationGrantState,
) CommittedFrontendContinuationGrant {
	scope := FrontendContinuationScopeRecord{Protocol: FrontendScopeNative,
		Action: FrontendActionForwardedData, Capability: CapabilityDataRead,
		Operation: ServiceOperationForwardedRead, Group: serviceDirectoryGroup(31), Relation: [16]byte{37}}
	grant, err := NewCommittedFrontendContinuationGrant(CommittedFrontendContinuationGrant{
		TrustDomain: peer.Identity.TrustDomain, PhysicalNode: binding.PhysicalNode,
		PhysicalIncarnation: binding.PhysicalIncarnation, PeerKeyDigest: binding.KeyDigest,
		GatewayServiceID: binding.Principal, GatewaySessionID: binding.SessionID,
		GatewaySessionRevision: binding.SessionRevision, DrainID: [32]byte{32}, AdmissionEpoch: 33,
		AcceptedConnectionTokens:    []FrontendConnToken{{34}},
		AcceptedConnectionProtocols: []FrontendContinuationScope{FrontendScopeNative},
		AllowedScopes:               []FrontendContinuationScopeRecord{scope},
		AdmissionClosedProofDigest:  [32]byte{35}, Revision: 36, State: state,
	})
	if err != nil {
		panic(err)
	}
	return grant
}

func serviceDirectoryCutWithContinuations(revision uint64, grants []CommittedFrontendContinuationGrant,
	bindings ...ServiceBinding,
) ServiceDirectoryCut {
	cut := serviceDirectoryCut(revision, bindings...)
	cut.ContinuationGrants = grants
	return cut
}

func TestServiceDirectoryDelegateBindsLifecycleAndVerifiedKey(t *testing.T) {
	peer := serviceDirectoryPeer(1, 2)
	storage := serviceDirectoryPeer(2, 3)
	cut := serviceDirectoryCut(1,
		serviceDirectoryBinding(peer, ServiceRoleGateway, ServiceActive),
		serviceDirectoryBinding(storage, ServiceRoleStorage, ServiceActive))
	gate, err := NewServiceDirectoryGate(cut)
	if err != nil {
		t.Fatal(err)
	}
	if got := gate.CheckDelegate(peer, 21, ServiceFence{}); got != DecisionAllow {
		t.Fatalf("active gateway decision=%d", got)
	}
	wrongKey := peer
	wrongKey.KeyDigest[0]++
	if got := gate.CheckDelegate(wrongKey, 21, ServiceFence{}); got == DecisionAllow {
		t.Fatal("wrong verified leaf key reused a gateway principal")
	}
	if got := gate.CheckDelegate(peer, 22, ServiceFence{}); got == DecisionAllow {
		t.Fatal("changed operator policy generation bypassed service cut")
	}
	if got := gate.CheckDelegate(storage, 21, ServiceFence{}); got == DecisionAllow {
		t.Fatal("storage principal acquired Delegate implicitly")
	}
	unknown := serviceDirectoryPeer(99, 100)
	if got := gate.CheckDelegate(unknown, 21, ServiceFence{}); got == DecisionAllow {
		t.Fatal("unknown CA-valid principal bypassed the committed directory")
	}
}

func TestServiceDirectoryInternalBootstrapIsClosedAndSelfOnly(t *testing.T) {
	peer := serviceDirectoryPeer(3, 4)
	gate, err := NewServiceDirectoryGate(serviceDirectoryCut(1,
		serviceDirectoryBinding(peer, ServiceRoleStorage, ServiceJoining)))
	if err != nil {
		t.Fatal(err)
	}
	authority := Authority{Node: peer.Identity.Node, Generation: 21}
	request := ServiceRequest{Action: ServiceActionStorageBootstrapRead,
		Capability: CapabilityDataRead, Operation: ServiceOperationBootstrapMetadata,
		Group: serviceDirectoryGroup(4), IntentID: [32]byte{5}, FenceDigest: [32]byte{6}}
	if got := gate.CheckInternal(peer, authority, request); got != DecisionAllow {
		t.Fatalf("joining storage bootstrap decision=%d", got)
	}
	request.Capability = CapabilityTopology
	if got := gate.CheckInternal(peer, authority, request); got == DecisionAllow {
		t.Fatal("bootstrap adapter accepted a forged topology capability")
	}
	request.Capability = CapabilityDataRead
	request.SessionID = [16]byte{7}
	if got := gate.CheckInternal(peer, authority, request); got == DecisionAllow {
		t.Fatal("bootstrap accepted a gateway session field")
	}
	request.SessionID = [16]byte{}
	forwarded := authority
	forwarded.Node[0]++
	if got := gate.CheckInternal(peer, forwarded, request); got == DecisionAllow {
		t.Fatal("internal action accepted a forwarded user authority")
	}
	request.Action = ServiceActionGatewayCatalogRead
	request.Operation = ServiceOperationCatalogRead
	request.Capability = CapabilityTopology
	request.SessionID = [16]byte{8}
	request.SessionRevision = 9
	if got := gate.CheckInternal(peer, authority, request); got == DecisionAllow {
		t.Fatal("joining storage principal gained gateway catalog authority")
	}
}

func TestServiceDirectoryGatewaySessionAndDrainingFence(t *testing.T) {
	peer := serviceDirectoryPeer(10, 11)
	binding := serviceDirectoryBinding(peer, ServiceRoleGateway, ServiceDraining)
	gate, err := NewServiceDirectoryGate(serviceDirectoryCut(3, binding))
	if err != nil {
		t.Fatal(err)
	}
	group := serviceDirectoryGroup(12)
	fence := ServiceFence{Action: ServiceActionGatewayCatalogRead, Operation: ServiceOperationCatalogRead,
		Group: group, SessionID: binding.SessionID, SessionRevision: binding.SessionRevision,
		IntentID: [32]byte{18}, FenceDigest: binding.DrainFenceDigest}
	binding.DrainFence = fence
	binding.InternalFences = []ServiceFence{fence}
	gate, err = NewServiceDirectoryGate(serviceDirectoryCut(3, binding))
	if err != nil {
		t.Fatal(err)
	}
	if got := gate.CheckDelegate(peer, 21, fence); got != DecisionAllow {
		t.Fatalf("exact draining continuation decision=%d", got)
	}
	wrong := fence
	wrong.SessionRevision++
	if got := gate.CheckDelegate(peer, 21, wrong); got == DecisionAllow {
		t.Fatal("stale session revision continued a draining gateway")
	}
	request := ServiceRequest{Action: fence.Action, Capability: CapabilityTopology, Operation: fence.Operation,
		Group: group, SessionID: fence.SessionID, SessionRevision: fence.SessionRevision,
		IntentID: fence.IntentID, FenceDigest: fence.FenceDigest}
	if got := gate.CheckInternal(peer, Authority{Node: peer.Identity.Node, Generation: 21}, request); got != DecisionAllow {
		t.Fatalf("exact draining internal continuation decision=%d", got)
	}
	binding.Lifecycle = ServiceDecommissioned
	binding.DrainFenceDigest = [32]byte{}
	if err := gate.ApplyCommittedCut(serviceDirectoryCut(4, binding)); err != nil {
		t.Fatal(err)
	}
	if got := gate.CheckDelegate(peer, 21, fence); got == DecisionAllow {
		t.Fatal("decommissioned tombstone retained an old service grant")
	}
}

func TestServiceDirectoryRevisionConflictAndTombstoneRetention(t *testing.T) {
	peer := serviceDirectoryPeer(20, 21)
	cut := serviceDirectoryCut(1, serviceDirectoryBinding(peer, ServiceRoleGateway, ServiceActive))
	gate, err := NewServiceDirectoryGate(cut)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.ApplyCommittedCut(cut); err != nil {
		t.Fatalf("equal exact replay: %v", err)
	}
	conflict := cut
	conflict.Bindings = append([]ServiceBinding(nil), cut.Bindings...)
	conflict.Bindings[0].SessionRevision++
	if err := gate.ApplyCommittedCut(conflict); !errors.Is(err, ErrServiceDirectoryStale) {
		t.Fatalf("equal conflicting cut error=%v", err)
	}
	rollback := cut
	rollback.Revision = 0
	if err := gate.ApplyCommittedCut(rollback); !errors.Is(err, ErrInvalidServiceDirectory) {
		t.Fatalf("invalid rollback error=%v", err)
	}
	higher := serviceDirectoryCut(2)
	if err := gate.ApplyCommittedCut(higher); !errors.Is(err, ErrInvalidServiceDirectory) {
		t.Fatalf("omitted managed principal error=%v", err)
	}
}

func TestServiceDirectoryContinuationGrantBindsConnectionAndLifecycle(t *testing.T) {
	peer := serviceDirectoryPeer(40, 41)
	active := serviceDirectoryBinding(peer, ServiceRoleGateway, ServiceActive)
	prepared := serviceDirectoryContinuationGrant(peer, active, ContinuationGrantPrepared)
	cut := serviceDirectoryCutWithContinuations(1, []CommittedFrontendContinuationGrant{prepared}, active)
	gate, err := NewServiceDirectoryGate(cut)
	if err != nil {
		t.Fatal(err)
	}
	scope := prepared.AllowedScopes[0]
	envelope := FrontendContinuationEnvelope{GrantDigest: prepared.GrantDigest,
		ConnToken: prepared.AcceptedConnectionTokens[0], Scope: scope}
	if got := gate.CheckFrontendContinuation(peer, 21, envelope, scope); got != DecisionAllow {
		t.Fatalf("prepared active continuation decision=%d", got)
	}
	postgresGrant := serviceDirectoryContinuationGrant(peer, active, ContinuationGrantPrepared)
	postgresGrant.AcceptedConnectionProtocols[0] = FrontendScopePostgreSQL
	postgresGrant.AllowedScopes[0].Protocol = FrontendScopePostgreSQL
	postgresGrant, err = NewCommittedFrontendContinuationGrant(postgresGrant)
	if err != nil {
		t.Fatal(err)
	}
	postgresGate, err := NewServiceDirectoryGate(serviceDirectoryCutWithContinuations(1,
		[]CommittedFrontendContinuationGrant{postgresGrant}, active))
	if err != nil {
		t.Fatal(err)
	}
	postgresScope := scope
	postgresScope.Protocol = FrontendScopePostgreSQL
	postgresEnvelope := FrontendContinuationEnvelope{GrantDigest: postgresGrant.GrantDigest,
		ConnToken: postgresGrant.AcceptedConnectionTokens[0], Scope: postgresScope}
	if got := postgresGate.CheckFrontendContinuation(peer, 21, postgresEnvelope, postgresScope); got != DecisionAllow {
		t.Fatalf("prepared PostgreSQL continuation decision=%d", got)
	}
	nativeReplay := postgresEnvelope
	nativeReplay.Scope.Protocol = FrontendScopeNative
	if got := postgresGate.CheckFrontendContinuation(peer, 21, nativeReplay, nativeReplay.Scope); got == DecisionAllow {
		t.Fatal("PostgreSQL connection token replayed as native")
	}
	wrongToken := envelope
	wrongToken.ConnToken[0]++
	if got := gate.CheckFrontendContinuation(peer, 21, wrongToken, scope); got == DecisionAllow {
		t.Fatal("uncommitted connection token bypassed continuation")
	}
	wrongScope := scope
	wrongScope.Group = serviceDirectoryGroup(32)
	if got := gate.CheckFrontendContinuation(peer, 21, envelope, wrongScope); got == DecisionAllow {
		t.Fatal("request for an uncommitted resource bypassed continuation")
	}

	draining := serviceDirectoryBinding(peer, ServiceRoleGateway, ServiceDraining)
	enforcing := prepared
	enforcing.State = ContinuationGrantEnforcing
	enforcing, err = NewCommittedFrontendContinuationGrant(enforcing)
	if err != nil {
		t.Fatal(err)
	}
	if enforcing.GrantDigest != prepared.GrantDigest {
		t.Fatal("grant identity changed across state transition")
	}
	if err := gate.ApplyCommittedCut(serviceDirectoryCutWithContinuations(2,
		[]CommittedFrontendContinuationGrant{enforcing}, draining)); err != nil {
		t.Fatal(err)
	}
	envelope.GrantDigest = enforcing.GrantDigest
	if got := gate.CheckFrontendContinuation(peer, 21, envelope, scope); got != DecisionAllow {
		t.Fatalf("enforcing draining continuation decision=%d", got)
	}
	wrongKey := peer
	wrongKey.KeyDigest[0]++
	if got := gate.CheckFrontendContinuation(wrongKey, 21, envelope, scope); got == DecisionAllow {
		t.Fatal("old or unrelated TLS key bypassed continuation")
	}

	retiredBinding := draining
	retiredBinding.Lifecycle = ServiceDecommissioned
	retiredBinding.DrainFenceDigest = [32]byte{}
	retiredBinding.DrainFence = ServiceFence{}
	retired := enforcing
	retired.State = ContinuationGrantRetired
	retired, err = NewCommittedFrontendContinuationGrant(retired)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.ApplyCommittedCut(serviceDirectoryCutWithContinuations(3,
		[]CommittedFrontendContinuationGrant{retired}, retiredBinding)); err != nil {
		t.Fatal(err)
	}
	if got := gate.CheckFrontendContinuation(peer, 21, envelope, scope); got == DecisionAllow {
		t.Fatal("retired continuation remained usable")
	}
	if got := gate.CheckDelegate(peer, 21, ServiceFence{}); got == DecisionAllow {
		t.Fatal("decommissioned principal retained Delegate")
	}
}

func TestServiceDirectoryContinuationGrantRejectsMutationAndRollback(t *testing.T) {
	peer := serviceDirectoryPeer(50, 51)
	active := serviceDirectoryBinding(peer, ServiceRoleGateway, ServiceActive)
	prepared := serviceDirectoryContinuationGrant(peer, active, ContinuationGrantPrepared)
	gate, err := NewServiceDirectoryGate(serviceDirectoryCutWithContinuations(1,
		[]CommittedFrontendContinuationGrant{prepared}, active))
	if err != nil {
		t.Fatal(err)
	}
	mutated := prepared
	mutated.AcceptedConnectionTokens = []FrontendConnToken{{52}}
	mutated, err = NewCommittedFrontendContinuationGrant(mutated)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.ApplyCommittedCut(serviceDirectoryCutWithContinuations(2,
		[]CommittedFrontendContinuationGrant{mutated}, active)); !errors.Is(err, ErrInvalidServiceDirectory) {
		t.Fatalf("mutated grant transition error=%v", err)
	}
	rollback := prepared
	rollback.State = ContinuationGrantRetired
	rollback, err = NewCommittedFrontendContinuationGrant(rollback)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.ApplyCommittedCut(serviceDirectoryCutWithContinuations(2,
		[]CommittedFrontendContinuationGrant{rollback}, active)); !errors.Is(err, ErrInvalidServiceDirectory) {
		t.Fatalf("retired active grant transition error=%v", err)
	}
}

func TestServiceDirectoryCatalogAndNodeRevisionsAdvanceIndependently(t *testing.T) {
	peer := serviceDirectoryPeer(20, 21)
	cut := serviceDirectoryCut(1, serviceDirectoryBinding(peer, ServiceRoleGateway, ServiceActive))
	cut.CatalogGeneration = 3
	gate, err := NewServiceDirectoryGate(cut)
	if err != nil {
		t.Fatal(err)
	}
	advanced := cut
	advanced.CatalogGeneration++
	if err := gate.ApplyCommittedCut(advanced); err != nil {
		t.Fatal(err)
	}
	if err := gate.ApplyCommittedCut(cut); !errors.Is(err, ErrServiceDirectoryStale) {
		t.Fatalf("catalog rollback: %v", err)
	}
	changedSession := advanced
	changedSession.CatalogGeneration++
	changedSession.Bindings = append([]ServiceBinding(nil), advanced.Bindings...)
	changedSession.Bindings[0].SessionID[0] ^= 0x80
	changedSession.Bindings[0].SessionRevision++
	if err := gate.ApplyCommittedCut(changedSession); !errors.Is(err, ErrInvalidServiceDirectory) {
		t.Fatalf("catalog advance changed session: %v", err)
	}

	advanced.Revision++
	if err := gate.ApplyCommittedCut(advanced); err != nil {
		t.Fatal(err)
	}
	rollback := advanced
	rollback.Revision--
	rollback.CatalogGeneration++
	if err := gate.ApplyCommittedCut(rollback); !errors.Is(err, ErrServiceDirectoryStale) {
		t.Fatalf("node rollback: %v", err)
	}
	conflict := advanced
	conflict.Bindings = append([]ServiceBinding(nil), advanced.Bindings...)
	conflict.Bindings[0].SessionRevision++
	if err := gate.ApplyCommittedCut(conflict); !errors.Is(err, ErrServiceDirectoryStale) {
		t.Fatalf("same cut conflict: %v", err)
	}
}

func TestBootstrapPeerRemainsReadableAcrossEnrollmentLifecycle(t *testing.T) {
	peer := serviceDirectoryPeer(3, 4)
	for _, state := range []ServiceLifecycle{ServiceJoining, ServiceActive, ServiceDraining, ServiceDecommissioned} {
		gate, err := NewServiceDirectoryGate(serviceDirectoryCut(1, serviceDirectoryBinding(peer, ServiceRoleStorage, state)))
		if err != nil {
			t.Fatal(err)
		}
		allowed := gate.CheckBootstrapPeer(peer) == DecisionAllow
		if allowed != (state != ServiceDecommissioned) {
			t.Fatalf("lifecycle %d allowed=%t", state, allowed)
		}
		forged := peer
		forged.KeyDigest[0] ^= 1
		if gate.CheckBootstrapPeer(forged) == DecisionAllow {
			t.Fatal("substituted certificate admitted")
		}
		if gate.CheckGatewayPeer(peer) == DecisionAllow {
			t.Fatal("storage bootstrap identity admitted as gateway")
		}
	}
}
