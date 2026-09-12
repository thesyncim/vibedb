package serviceauthz

import (
	"errors"
	"runtime"
	"slices"
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

func TestServiceDirectoryDrainingRequiresExactCommittedFence(t *testing.T) {
	peer := serviceDirectoryPeer(10, 11)
	binding := serviceDirectoryBinding(peer, ServiceRoleGateway, ServiceDraining)
	mutations := []struct {
		name string
		edit func(*ServiceFence)
	}{
		{"action", func(f *ServiceFence) { f.Action = ServiceActionGatewayCatalogWrite }},
		{"operation", func(f *ServiceFence) { f.Operation = ServiceOperationCatalogWrite }},
		{"group", func(f *ServiceFence) { f.Group.GroupID[0]++ }},
		{"relation", func(f *ServiceFence) { f.Relation[0]++ }},
		{"session", func(f *ServiceFence) { f.SessionID[0]++ }},
		{"session revision", func(f *ServiceFence) { f.SessionRevision++ }},
		{"intent", func(f *ServiceFence) { f.IntentID[0]++ }},
		{"digest", func(f *ServiceFence) { f.FenceDigest[0]++ }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			changed := binding.DrainFence
			mutation.edit(&changed)
			// Even an additional committed internal grant must not widen the
			// one exact continuation selected by the drain fence.
			cutBinding := binding
			cutBinding.InternalFences = []ServiceFence{binding.DrainFence, changed}
			slices.SortFunc(cutBinding.InternalFences, CompareServiceFences)
			gate, err := NewServiceDirectoryGate(serviceDirectoryCut(1, cutBinding))
			if err != nil {
				t.Fatal(err)
			}
			if gate.CheckDelegate(peer, 21, changed) == DecisionAllow {
				t.Error("draining delegate admitted a different continuation scope")
			}
			request := ServiceRequest{Action: changed.Action, Capability: CapabilityTopology,
				Operation: changed.Operation, Group: changed.Group, Relation: changed.Relation,
				SessionID: changed.SessionID, SessionRevision: changed.SessionRevision,
				IntentID: changed.IntentID, FenceDigest: changed.FenceDigest}
			if gate.CheckInternal(peer, Authority{Node: peer.Identity.Node, Generation: 21}, request) == DecisionAllow {
				t.Fatal("draining internal action admitted a different continuation scope")
			}
		})
	}
}

func TestServiceDirectoryInternalContinuationUsesOneCut(t *testing.T) {
	peer := serviceDirectoryPeer(40, 41)
	active := serviceDirectoryBinding(peer, ServiceRoleGateway, ServiceActive)
	prepared := serviceDirectoryContinuationGrant(peer, active, ContinuationGrantPrepared)
	scope := FrontendContinuationScopeRecord{Protocol: FrontendScopeNative,
		Action: FrontendActionGatewayCatalog, Capability: CapabilityTopology,
		Operation: ServiceOperationCatalogRead, Group: serviceDirectoryGroup(31),
		Relation: [16]byte{37}, IntentID: [32]byte{38}, FenceDigest: [32]byte{39}}
	prepared.AllowedScopes = []FrontendContinuationScopeRecord{scope}
	prepared, err := NewCommittedFrontendContinuationGrant(prepared)
	if err != nil {
		t.Fatal(err)
	}
	activeState, err := newDirectoryState(serviceDirectoryCutWithContinuations(1,
		[]CommittedFrontendContinuationGrant{prepared}, active))
	if err != nil {
		t.Fatal(err)
	}
	draining := serviceDirectoryBinding(peer, ServiceRoleGateway, ServiceDraining)
	retired := prepared
	retired.State = ContinuationGrantRetired
	retiredState, err := newDirectoryState(serviceDirectoryCutWithContinuations(2,
		[]CommittedFrontendContinuationGrant{retired}, draining))
	if err != nil {
		t.Fatal(err)
	}
	envelope := FrontendContinuationEnvelope{GrantDigest: prepared.GrantDigest,
		ConnToken: prepared.AcceptedConnectionTokens[0], Scope: scope}
	authority := Authority{Node: peer.Identity.Node, Generation: 21}
	gate := new(ServiceDirectoryGate)
	activeWithFence := active
	activeWithFence.InternalFences = []ServiceFence{{Action: ServiceActionGatewayCatalogRead,
		Operation: scope.Operation, Group: scope.Group, Relation: scope.Relation,
		SessionID: active.SessionID, SessionRevision: active.SessionRevision,
		IntentID: scope.IntentID, FenceDigest: scope.FenceDigest}}
	allowedActive, err := newDirectoryState(serviceDirectoryCutWithContinuations(1,
		[]CommittedFrontendContinuationGrant{prepared}, activeWithFence))
	if err != nil {
		t.Fatal(err)
	}
	enforcing := prepared
	enforcing.State = ContinuationGrantEnforcing
	allowedDraining, err := newDirectoryState(serviceDirectoryCutWithContinuations(2,
		[]CommittedFrontendContinuationGrant{enforcing}, draining))
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []*directoryState{allowedActive, allowedDraining} {
		gate.current.Store(state)
		if gate.CheckInternalFrontendContinuation(peer, authority, envelope, scope) != DecisionAllow {
			t.Fatal("exact internal continuation denied")
		}
		forwarded := authority
		forwarded.Node[0]++
		if gate.CheckInternalFrontendContinuation(peer, forwarded, envelope, scope) == DecisionAllow {
			t.Fatal("internal continuation admitted a forwarded authority")
		}
		stale := authority
		stale.Generation++
		if gate.CheckInternalFrontendContinuation(peer, stale, envelope, scope) == DecisionAllow {
			t.Fatal("internal continuation admitted a stale policy generation")
		}
	}
	// Neither publication permits this request: Active lacks an InternalFence,
	// and Draining has a retired grant. Alternate the immutable publications
	// directly to amplify a mixed-snapshot read without adding timing hooks to
	// the production gate. Transition validation is tested separately.
	for _, state := range []*directoryState{activeState, retiredState} {
		gate.current.Store(state)
		if gate.CheckInternalFrontendContinuation(peer, authority, envelope, scope) == DecisionAllow {
			t.Fatal("individual publication authorized the test request")
		}
	}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			gate.current.Store(activeState)
			runtime.Gosched()
			gate.current.Store(retiredState)
			runtime.Gosched()
		}
	}()
	defer func() { close(stop); <-done }()
	for attempt := 0; attempt < 100000; attempt++ {
		if gate.CheckInternalFrontendContinuation(peer, authority, envelope, scope) == DecisionAllow {
			t.Fatal("mixed two denying publications into an allowed request")
		}
	}
}

func TestServiceDirectoryContinuationAdmissionDoesNotRehashGrant(t *testing.T) {
	peer := serviceDirectoryPeer(40, 41)
	active := serviceDirectoryBinding(peer, ServiceRoleGateway, ServiceActive)
	prepared := serviceDirectoryContinuationGrant(peer, active, ContinuationGrantPrepared)
	gate, err := NewServiceDirectoryGate(serviceDirectoryCutWithContinuations(1,
		[]CommittedFrontendContinuationGrant{prepared}, active))
	if err != nil {
		t.Fatal(err)
	}
	scope := prepared.AllowedScopes[0]
	envelope := FrontendContinuationEnvelope{GrantDigest: prepared.GrantDigest,
		ConnToken: prepared.AcceptedConnectionTokens[0], Scope: scope}
	if allocations := testing.AllocsPerRun(100, func() {
		if gate.CheckFrontendContinuation(peer, 21, envelope, scope) != DecisionAllow {
			t.Fatal("committed continuation denied")
		}
	}); allocations != 0 {
		t.Fatalf("continuation admission allocated %g times", allocations)
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
