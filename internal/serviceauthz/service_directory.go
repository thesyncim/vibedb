package serviceauthz

// This file contains the committed service-identity directory.  It is kept
// separate from Policy/Gate on purpose: Policy remains the operator/user
// authority and this directory is the catalog-derived lifecycle and key
// fence for physical services.  A certificate that is valid for the cluster
// is therefore not, by itself, a service capability.

import (
	"bytes"
	"errors"
	"slices"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

var (
	ErrInvalidServiceDirectory = errors.New("serviceauthz: invalid service directory cut")
	ErrServiceDirectoryBound   = errors.New("serviceauthz: service directory bound exceeded")
	ErrServiceDirectoryStale   = errors.New("serviceauthz: stale service directory cut")
	ErrServiceDirectoryDenied  = errors.New("serviceauthz: service directory denied")
)

const (
	AbsoluteMaxServiceBindings = 4096
	AbsoluteMaxServiceActions  = 32
)

// ServiceRoleMask is a physical service role.  Roles are deliberately not
// aliases for the user/operator Capability set; a storage process never gains
// delegation or topology authority merely because it is enrolled.
type ServiceRoleMask uint8

const (
	ServiceRoleStorage ServiceRoleMask = 1 << iota
	ServiceRoleGateway
	ServiceRoleController
)

const serviceRoles = ServiceRoleStorage | ServiceRoleGateway | ServiceRoleController

func (roles ServiceRoleMask) Valid() bool {
	return roles != 0 && roles&^serviceRoles == 0
}

// ServiceLifecycle is the catalog-derived physical/service lifecycle.  The
// values intentionally mirror the durable node lifecycle without importing
// gateway metadata into this low-level package.
type ServiceLifecycle uint8

const (
	ServiceJoining ServiceLifecycle = iota + 1
	ServiceActive
	ServiceDraining
	ServiceDecommissioned
)

func (lifecycle ServiceLifecycle) Valid() bool {
	return lifecycle >= ServiceJoining && lifecycle <= ServiceDecommissioned
}

func (lifecycle ServiceLifecycle) Allows(next ServiceLifecycle) bool {
	if !lifecycle.Valid() || !next.Valid() {
		return false
	}
	if lifecycle == next {
		return true
	}
	switch lifecycle {
	case ServiceJoining:
		return next == ServiceActive
	case ServiceActive:
		return next == ServiceDraining
	case ServiceDraining:
		return next == ServiceDecommissioned
	default:
		return false
	}
}

// ServiceBinding is one exact authenticated principal in a complete
// directory cut. Principal is the TLS identity; PhysicalNode identifies the
// node record it belongs to and may differ for a gateway process. A
// Decommissioned binding is a durable tombstone and must remain present in
// subsequent complete cuts until the catalog's durable history can retire it.
type ServiceBinding struct {
	Principal           rafttransport.NodeID
	PhysicalNode        rafttransport.NodeID
	PhysicalIncarnation uint64
	KeyDigest           [32]byte
	Roles               ServiceRoleMask
	Lifecycle           ServiceLifecycle
	GatewayIncarnation  uint64
	SessionID           [16]byte
	SessionRevision     uint64
	ParticipantDigest   [32]byte
	// DrainFenceDigest is the exact committed continuation fence for an
	// admitted gateway session.  It is required for Draining bindings and is
	// compared byte-for-byte by CheckDelegate/CheckInternal.
	DrainFenceDigest [32]byte
	// InternalFences is the bounded set of exact catalog-authorized internal
	// actions/resources this principal may perform. A caller cannot turn a
	// nonzero relation or digest into authority without a matching cut entry.
	InternalFences []ServiceFence
	// DrainFence carries the full tuple behind DrainFenceDigest. The digest
	// alone is insufficient because it cannot prove the request's scope.
	DrainFence ServiceFence
}

func (binding ServiceBinding) Valid() bool {
	if binding.Principal == (rafttransport.NodeID{}) ||
		binding.PhysicalNode == (rafttransport.NodeID{}) || binding.PhysicalIncarnation == 0 ||
		binding.KeyDigest == ([32]byte{}) || !binding.Roles.Valid() || !binding.Lifecycle.Valid() {
		return false
	}
	if binding.Roles&ServiceRoleGateway != 0 {
		if binding.GatewayIncarnation == 0 || binding.SessionID == ([16]byte{}) ||
			binding.SessionRevision == 0 || binding.ParticipantDigest == ([32]byte{}) {
			return false
		}
	} else if binding.GatewayIncarnation != 0 || binding.SessionID != ([16]byte{}) ||
		binding.SessionRevision != 0 || binding.ParticipantDigest != ([32]byte{}) ||
		binding.DrainFenceDigest != ([32]byte{}) {
		return false
	}
	if binding.Lifecycle == ServiceDraining {
		if binding.Roles&ServiceRoleGateway != 0 && binding.DrainFenceDigest == ([32]byte{}) {
			return false
		}
		if binding.Roles&ServiceRoleGateway != 0 &&
			(!binding.DrainFence.validShape() || binding.DrainFence.FenceDigest != binding.DrainFenceDigest) {
			return false
		}
	} else if binding.DrainFenceDigest != ([32]byte{}) {
		return false
	}
	if len(binding.InternalFences) > AbsoluteMaxServiceActions {
		return false
	}
	for index, fence := range binding.InternalFences {
		if !validInternalFenceShape(fence) || index > 0 && compareServiceFences(binding.InternalFences[index-1], fence) >= 0 {
			return false
		}
	}
	return true
}

// ServiceDirectoryCut is one catalog-authorized immutable participant cut.
// PolicyGeneration is copied from the existing operator policy and never
// rotated by this gate.
type ServiceDirectoryCut struct {
	Revision           uint64
	TrustDomain        rafttransport.TrustDomain
	PolicyGeneration   uint64
	Bindings           []ServiceBinding
	ContinuationGrants []CommittedFrontendContinuationGrant
}

func (cut ServiceDirectoryCut) Valid() bool {
	if cut.Revision == 0 || cut.PolicyGeneration == 0 ||
		cut.TrustDomain.ClusterID == ([16]byte{}) ||
		cut.TrustDomain.ClusterIncarnation == ([16]byte{}) ||
		len(cut.Bindings) == 0 || len(cut.Bindings) > AbsoluteMaxServiceBindings {
		return false
	}
	for index, binding := range cut.Bindings {
		if !binding.Valid() || index > 0 && bytes.Compare(cut.Bindings[index-1].Principal[:], binding.Principal[:]) >= 0 {
			return false
		}
	}
	for index, grant := range cut.ContinuationGrants {
		if !grant.Valid() || index > 0 && bytes.Compare(cut.ContinuationGrants[index-1].GrantDigest[:], grant.GrantDigest[:]) >= 0 {
			return false
		}
	}
	return true
}

// AuthenticatedPeer is constructed only after TLS has verified the leaf and
// its key binding. KeyDigest is never accepted from a request payload.
type AuthenticatedPeer struct {
	Identity  rafttransport.PeerIdentity
	KeyDigest [32]byte
}

func (peer AuthenticatedPeer) Valid() bool {
	return peer.Identity.TrustDomain.ClusterID != ([16]byte{}) &&
		peer.Identity.TrustDomain.ClusterIncarnation != ([16]byte{}) &&
		peer.Identity.Node != (rafttransport.NodeID{}) && peer.KeyDigest != ([32]byte{})
}

// ServiceAction is a closed internal action grammar.  User data/schema
// requests continue through Gate and are intentionally absent here.
type ServiceAction uint8

const (
	ServiceActionStorageBootstrapRead ServiceAction = iota + 1
	ServiceActionGatewayCatalogRead
	ServiceActionGatewayCatalogWrite
	ServiceActionGatewayRequestLedger
	ServiceActionGatewayExecutionPin
	ServiceActionGatewayTransactionRecovery
	ServiceActionControllerMembership
)

func (action ServiceAction) Valid() bool {
	return action >= ServiceActionStorageBootstrapRead && action <= ServiceActionControllerMembership
}

// ServiceOperation identifies the typed operation inside an internal action.
// It is not a wire operation number from another package; adapters map their
// own closed operation enums to these values before calling CheckInternal.
type ServiceOperation uint8

const (
	ServiceOperationBootstrapMetadata ServiceOperation = iota + 1
	ServiceOperationCatalogRead
	ServiceOperationCatalogWrite
	ServiceOperationRequestLedger
	ServiceOperationExecutionPin
	ServiceOperationTransactionRecovery
	ServiceOperationMembership
	// Forwarded operations describe the authenticated gateway boundary around
	// an end-user request. They do not grant data access; the existing Gate
	// still checks the forwarded authority and capability.
	ServiceOperationForwardedRead
	ServiceOperationForwardedWrite
)

func (operation ServiceOperation) Valid() bool {
	return operation >= ServiceOperationBootstrapMetadata && operation <= ServiceOperationMembership
}

// ServiceFence carries the exact continuation coordinates for a request that
// was admitted before a gateway entered Draining. The digest is a committed
// fence, not a caller-selected capability.
type ServiceFence struct {
	Action          ServiceAction
	Operation       ServiceOperation
	Group           raftmember.GroupKey
	Relation        [16]byte
	SessionID       [16]byte
	SessionRevision uint64
	IntentID        [32]byte
	FenceDigest     [32]byte
}

func (fence ServiceFence) validShape() bool {
	return fence.validScopeShape() && fence.SessionID != ([16]byte{}) && fence.SessionRevision != 0
}

// validScopeShape is shared by all internal fences. Storage bootstrap and
// controller membership are self-scoped actions and consequently do not have
// a gateway session; gateway and drain continuations use validShape below.
func (fence ServiceFence) validScopeShape() bool {
	return fence.Action.Valid() && fence.Operation.Valid() &&
		fence.Group != (raftmember.GroupKey{}) && fence.IntentID != ([32]byte{}) &&
		fence.FenceDigest != ([32]byte{})
}

func validInternalFenceShape(fence ServiceFence) bool {
	if !fence.validScopeShape() {
		return false
	}
	switch fence.Action {
	case ServiceActionStorageBootstrapRead, ServiceActionControllerMembership:
		return fence.SessionID == ([16]byte{}) && fence.SessionRevision == 0
	default:
		return fence.SessionID != ([16]byte{}) && fence.SessionRevision != 0
	}
}

func compareServiceFences(left, right ServiceFence) int {
	if left.Action != right.Action {
		if left.Action < right.Action {
			return -1
		}
		return 1
	}
	if left.Operation != right.Operation {
		if left.Operation < right.Operation {
			return -1
		}
		return 1
	}
	if result := compareGroupKeys(left.Group, right.Group); result != 0 {
		return result
	}
	if result := bytes.Compare(left.Relation[:], right.Relation[:]); result != 0 {
		return result
	}
	if result := bytes.Compare(left.SessionID[:], right.SessionID[:]); result != 0 {
		return result
	}
	if left.SessionRevision != right.SessionRevision {
		if left.SessionRevision < right.SessionRevision {
			return -1
		}
		return 1
	}
	if result := bytes.Compare(left.IntentID[:], right.IntentID[:]); result != 0 {
		return result
	}
	return bytes.Compare(left.FenceDigest[:], right.FenceDigest[:])
}

func compareGroupKeys(left, right raftmember.GroupKey) int {
	if result := bytes.Compare(left.ClusterID[:], right.ClusterID[:]); result != 0 {
		return result
	}
	if result := bytes.Compare(left.ClusterIncarnation[:], right.ClusterIncarnation[:]); result != 0 {
		return result
	}
	if left.TopologyRecoveryEpoch != right.TopologyRecoveryEpoch {
		if left.TopologyRecoveryEpoch < right.TopologyRecoveryEpoch {
			return -1
		}
		return 1
	}
	if result := bytes.Compare(left.ShardIncarnation[:], right.ShardIncarnation[:]); result != 0 {
		return result
	}
	return bytes.Compare(left.GroupID[:], right.GroupID[:])
}

func sameServiceFence(left, right ServiceFence) bool {
	return left.Action == right.Action && left.Operation == right.Operation &&
		left.Group == right.Group && left.Relation == right.Relation &&
		left.SessionID == right.SessionID && left.SessionRevision == right.SessionRevision &&
		left.IntentID == right.IntentID && left.FenceDigest == right.FenceDigest
}

func sameServiceBinding(left, right ServiceBinding) bool {
	return left.Principal == right.Principal && left.PhysicalNode == right.PhysicalNode &&
		left.PhysicalIncarnation == right.PhysicalIncarnation && left.KeyDigest == right.KeyDigest &&
		left.Roles == right.Roles && left.Lifecycle == right.Lifecycle &&
		left.GatewayIncarnation == right.GatewayIncarnation && left.SessionID == right.SessionID &&
		left.SessionRevision == right.SessionRevision && left.ParticipantDigest == right.ParticipantDigest &&
		left.DrainFenceDigest == right.DrainFenceDigest && sameServiceFence(left.DrainFence, right.DrainFence) &&
		slices.Equal(left.InternalFences, right.InternalFences)
}

func sameDirectoryCut(left, right ServiceDirectoryCut) bool {
	if left.Revision != right.Revision || left.TrustDomain != right.TrustDomain ||
		left.PolicyGeneration != right.PolicyGeneration || len(left.Bindings) != len(right.Bindings) {
		return false
	}
	for index := range left.Bindings {
		if !sameServiceBinding(left.Bindings[index], right.Bindings[index]) {
			return false
		}
	}
	if len(left.ContinuationGrants) != len(right.ContinuationGrants) {
		return false
	}
	for index := range left.ContinuationGrants {
		if !sameContinuationGrant(left.ContinuationGrants[index], right.ContinuationGrants[index]) {
			return false
		}
	}
	return true
}

func cloneServiceBinding(binding ServiceBinding) ServiceBinding {
	binding.InternalFences = slices.Clone(binding.InternalFences)
	return binding
}

// ServiceRequest is the closed, typed request passed to CheckInternal.  The
// adapter must fill every coordinate required by the selected action; unknown
// action/capability combinations are denied even if Policy would allow them.
type ServiceRequest struct {
	Action          ServiceAction
	Capability      Capability
	Operation       ServiceOperation
	Group           raftmember.GroupKey
	Relation        [16]byte
	SessionID       [16]byte
	SessionRevision uint64
	IntentID        [32]byte
	FenceDigest     [32]byte
}

func (request ServiceRequest) fence() ServiceFence {
	return ServiceFence{Action: request.Action, Operation: request.Operation, Group: request.Group,
		Relation: request.Relation, SessionID: request.SessionID, SessionRevision: request.SessionRevision,
		IntentID: request.IntentID, FenceDigest: request.FenceDigest}
}

type directoryState struct {
	cut           ServiceDirectoryCut
	bindings      map[rafttransport.NodeID]ServiceBinding
	continuations map[[32]byte]CommittedFrontendContinuationGrant
}

// ServiceDirectoryGate atomically publishes complete committed cuts. It does
// not replace Gate and never mutates the operator/user Policy.
type ServiceDirectoryGate struct {
	current atomic.Pointer[directoryState]
}

func NewServiceDirectoryGate(cut ServiceDirectoryCut) (*ServiceDirectoryGate, error) {
	state, err := newDirectoryState(cut)
	if err != nil {
		return nil, err
	}
	gate := new(ServiceDirectoryGate)
	gate.current.Store(state)
	return gate, nil
}

func newDirectoryState(cut ServiceDirectoryCut) (*directoryState, error) {
	owned := slices.Clone(cut.Bindings)
	// Cuts are canonicalized here so readers never observe caller-owned slices.
	for index := range owned {
		owned[index] = cloneServiceBinding(owned[index])
	}
	slices.SortFunc(owned, func(left, right ServiceBinding) int {
		return bytes.Compare(left.Principal[:], right.Principal[:])
	})
	cut.Bindings = owned
	grants := slices.Clone(cut.ContinuationGrants)
	for index := range grants {
		grants[index] = cloneContinuationGrant(grants[index])
	}
	slices.SortFunc(grants, func(left, right CommittedFrontendContinuationGrant) int {
		return bytes.Compare(left.GrantDigest[:], right.GrantDigest[:])
	})
	cut.ContinuationGrants = grants
	if !cut.Valid() {
		return nil, ErrInvalidServiceDirectory
	}
	bindings := make(map[rafttransport.NodeID]ServiceBinding, len(cut.Bindings))
	for index, binding := range owned {
		if index > 0 && binding.Principal == owned[index-1].Principal {
			return nil, ErrInvalidServiceDirectory
		}
		bindings[binding.Principal] = cloneServiceBinding(binding)
	}
	continuations := make(map[[32]byte]CommittedFrontendContinuationGrant, len(cut.ContinuationGrants))
	for _, grant := range cut.ContinuationGrants {
		binding, found := bindings[grant.GatewayServiceID]
		if !found || grant.TrustDomain != cut.TrustDomain || !grantMatchesBinding(grant, binding) {
			return nil, ErrInvalidServiceDirectory
		}
		if _, duplicate := continuations[grant.GrantDigest]; duplicate {
			return nil, ErrInvalidServiceDirectory
		}
		continuations[grant.GrantDigest] = cloneContinuationGrant(grant)
	}
	return &directoryState{cut: cut, bindings: bindings, continuations: continuations}, nil
}

func (gate *ServiceDirectoryGate) Revision() uint64 {
	if gate == nil {
		return 0
	}
	state := gate.current.Load()
	if state == nil {
		return 0
	}
	return state.cut.Revision
}

func (gate *ServiceDirectoryGate) PolicyGeneration() uint64 {
	if gate == nil {
		return 0
	}
	state := gate.current.Load()
	if state == nil {
		return 0
	}
	return state.cut.PolicyGeneration
}

// ManagedPrincipal reports whether a NodeID is present in the committed
// directory, including a Decommissioned tombstone. Callers use this to stop
// the legacy Policy from resurrecting a retired service principal when it is
// named as a forwarded authority.
func (gate *ServiceDirectoryGate) ManagedPrincipal(node rafttransport.NodeID) bool {
	if gate == nil || node == (rafttransport.NodeID{}) {
		return false
	}
	state := gate.current.Load()
	if state == nil {
		return false
	}
	_, found := state.bindings[node]
	return found
}

func (gate *ServiceDirectoryGate) Cut() (ServiceDirectoryCut, bool) {
	if gate == nil {
		return ServiceDirectoryCut{}, false
	}
	state := gate.current.Load()
	if state == nil {
		return ServiceDirectoryCut{}, false
	}
	cut := state.cut
	cut.Bindings = slices.Clone(cut.Bindings)
	for index := range cut.Bindings {
		cut.Bindings[index] = cloneServiceBinding(cut.Bindings[index])
	}
	cut.ContinuationGrants = slices.Clone(cut.ContinuationGrants)
	for index := range cut.ContinuationGrants {
		cut.ContinuationGrants[index] = cloneContinuationGrant(cut.ContinuationGrants[index])
	}
	return cut, true
}

// ApplyCommittedCut publishes one complete, newer cut. A same-revision
// replay is accepted only when every byte of the immutable cut matches. A
// prior managed principal may not disappear from a later cut; callers must
// publish its Decommissioned tombstone instead.
func (gate *ServiceDirectoryGate) ApplyCommittedCut(cut ServiceDirectoryCut) error {
	if gate == nil {
		return ErrInvalidServiceDirectory
	}
	next, err := newDirectoryState(cut)
	if err != nil {
		return err
	}
	for {
		prior := gate.current.Load()
		if prior == nil {
			if gate.current.CompareAndSwap(nil, next) {
				return nil
			}
			continue
		}
		if next.cut.TrustDomain != prior.cut.TrustDomain ||
			next.cut.PolicyGeneration != prior.cut.PolicyGeneration ||
			next.cut.Revision < prior.cut.Revision {
			return ErrServiceDirectoryStale
		}
		if next.cut.Revision == prior.cut.Revision {
			if sameDirectoryCut(prior.cut, next.cut) {
				return nil
			}
			return ErrServiceDirectoryStale
		}
		for principal, priorBinding := range prior.bindings {
			nextBinding, found := next.bindings[principal]
			if !found {
				return ErrInvalidServiceDirectory
			}
			if !validBindingTransition(priorBinding, nextBinding) {
				return ErrInvalidServiceDirectory
			}
		}
		for digest, priorGrant := range prior.continuations {
			nextGrant, found := next.continuations[digest]
			if !found || !validContinuationTransition(priorGrant, nextGrant) {
				return ErrInvalidServiceDirectory
			}
		}
		if gate.current.CompareAndSwap(prior, next) {
			return nil
		}
	}
}

func validBindingTransition(prior, next ServiceBinding) bool {
	if prior.Principal != next.Principal || prior.PhysicalNode != next.PhysicalNode ||
		prior.PhysicalIncarnation != next.PhysicalIncarnation || prior.KeyDigest != next.KeyDigest ||
		prior.Roles != next.Roles || prior.GatewayIncarnation != next.GatewayIncarnation {
		return false
	}
	if !prior.Lifecycle.Allows(next.Lifecycle) {
		return false
	}
	if prior.Roles&ServiceRoleGateway != 0 {
		if prior.SessionID == next.SessionID {
			if prior.SessionRevision != next.SessionRevision || prior.ParticipantDigest != next.ParticipantDigest {
				return false
			}
		} else if next.SessionRevision <= prior.SessionRevision {
			return false
		}
	}
	return true
}

func (gate *ServiceDirectoryGate) lookup(peer AuthenticatedPeer) (directoryState, ServiceBinding, bool) {
	if gate == nil || !peer.Valid() {
		return directoryState{}, ServiceBinding{}, false
	}
	state := gate.current.Load()
	if state == nil || state.cut.TrustDomain != peer.Identity.TrustDomain {
		return directoryState{}, ServiceBinding{}, false
	}
	binding, found := state.bindings[peer.Identity.Node]
	if !found || binding.KeyDigest != peer.KeyDigest {
		return directoryState{}, ServiceBinding{}, false
	}
	return *state, binding, true
}

// CheckDelegate admits a TLS service principal to the forwarding boundary.
// Storage and controller principals never acquire delegation implicitly.
func (gate *ServiceDirectoryGate) CheckDelegate(
	peer AuthenticatedPeer, policyGeneration uint64, continuation ServiceFence,
) DecisionCode {
	state, binding, ok := gate.lookup(peer)
	if !ok || policyGeneration == 0 || policyGeneration != state.cut.PolicyGeneration {
		return DecisionDenyNoPrincipal
	}
	if binding.Roles&ServiceRoleGateway == 0 {
		return DecisionDenyCapability
	}
	switch binding.Lifecycle {
	case ServiceActive:
		return DecisionAllow
	case ServiceDraining:
		if continuation.validShape() && continuation.SessionID == binding.SessionID &&
			continuation.SessionRevision == binding.SessionRevision &&
			continuation.FenceDigest == binding.DrainFenceDigest {
			return DecisionAllow
		}
		return DecisionDenyCapability
	default:
		return DecisionDenyCapability
	}
}

// CheckGatewayPeer admits an authenticated gateway-control stream against the
// current committed service identity. It deliberately does not grant data or
// forwarding authority: request handlers still validate their closed request
// grammar and exact drain fence. A Draining gateway may keep an already
// captured control stream alive so the immutable drain protocol can settle,
// while new data delegation remains denied by CheckDelegate.
func (gate *ServiceDirectoryGate) CheckGatewayPeer(peer AuthenticatedPeer) DecisionCode {
	_, binding, ok := gate.lookup(peer)
	if !ok || binding.Roles&ServiceRoleGateway == 0 {
		return DecisionDenyNoPrincipal
	}
	switch binding.Lifecycle {
	case ServiceActive, ServiceDraining:
		return DecisionAllow
	default:
		return DecisionDenyCapability
	}
}

// CheckBootstrapPeer admits only the target physical storage identity during
// the empty-node enrollment read. It is intentionally narrower than
// CheckGatewayPeer and never accepts a gateway, controller, or decommissioned
// binding on this listener.
func (gate *ServiceDirectoryGate) CheckBootstrapPeer(peer AuthenticatedPeer) DecisionCode {
	_, binding, ok := gate.lookup(peer)
	if !ok || binding.Roles&ServiceRoleStorage == 0 || binding.PhysicalNode != peer.Identity.Node {
		return DecisionDenyNoPrincipal
	}
	if binding.Lifecycle != ServiceJoining {
		return DecisionDenyCapability
	}
	return DecisionAllow
}

// CheckInternal admits one closed internal service action. It requires a
// self-authority: forwarded end-user principals must continue through Gate.
func (gate *ServiceDirectoryGate) CheckInternal(
	peer AuthenticatedPeer, authority Authority, request ServiceRequest,
) DecisionCode {
	state, binding, ok := gate.lookup(peer)
	if !ok || !authority.Valid() || authority.Node != peer.Identity.Node ||
		authority.Generation != state.cut.PolicyGeneration {
		return DecisionDenyNoPrincipal
	}
	if binding.Lifecycle == ServiceDecommissioned || binding.Lifecycle == ServiceJoining &&
		request.Action != ServiceActionStorageBootstrapRead {
		return DecisionDenyCapability
	}
	if !validInternalRequest(request) || !roleAllowsAction(binding, request.Action) {
		return DecisionDenyCapability
	}
	if !binding.allowsInternalFence(request.fence()) {
		return DecisionDenyCapability
	}
	if request.Action == ServiceActionStorageBootstrapRead {
		if binding.PhysicalNode != peer.Identity.Node ||
			(binding.Lifecycle != ServiceJoining && binding.Lifecycle != ServiceActive) {
			return DecisionDenyCapability
		}
	} else if request.Action == ServiceActionControllerMembership {
		if binding.Roles&ServiceRoleController == 0 || binding.Lifecycle != ServiceActive {
			return DecisionDenyCapability
		}
	} else {
		if binding.Roles&ServiceRoleGateway == 0 || binding.Lifecycle != ServiceActive && binding.Lifecycle != ServiceDraining {
			return DecisionDenyCapability
		}
		if request.SessionID != binding.SessionID || request.SessionRevision != binding.SessionRevision {
			return DecisionDenyCapability
		}
		if binding.Lifecycle == ServiceDraining && request.FenceDigest != binding.DrainFenceDigest {
			return DecisionDenyCapability
		}
	}
	return DecisionAllow
}

func (binding ServiceBinding) allowsInternalFence(request ServiceFence) bool {
	for _, fence := range binding.InternalFences {
		if sameServiceFence(fence, request) {
			return true
		}
	}
	return false
}

func roleAllowsAction(binding ServiceBinding, action ServiceAction) bool {
	switch action {
	case ServiceActionStorageBootstrapRead:
		return binding.Roles&ServiceRoleStorage != 0
	case ServiceActionControllerMembership:
		return binding.Roles&ServiceRoleController != 0
	case ServiceActionGatewayCatalogRead, ServiceActionGatewayCatalogWrite,
		ServiceActionGatewayRequestLedger, ServiceActionGatewayExecutionPin,
		ServiceActionGatewayTransactionRecovery:
		return binding.Roles&ServiceRoleGateway != 0
	default:
		return false
	}
}

func validInternalRequest(request ServiceRequest) bool {
	if !request.Action.Valid() || !request.Operation.Valid() || request.Group == (raftmember.GroupKey{}) ||
		request.IntentID == ([32]byte{}) || request.FenceDigest == ([32]byte{}) {
		return false
	}
	var wantOperation ServiceOperation
	var wantCapability Capability
	switch request.Action {
	case ServiceActionStorageBootstrapRead:
		wantOperation, wantCapability = ServiceOperationBootstrapMetadata, CapabilityDataRead
	case ServiceActionGatewayCatalogRead:
		wantOperation, wantCapability = ServiceOperationCatalogRead, CapabilityTopology
	case ServiceActionGatewayCatalogWrite:
		wantOperation, wantCapability = ServiceOperationCatalogWrite, CapabilityTopology
	case ServiceActionGatewayRequestLedger:
		wantOperation, wantCapability = ServiceOperationRequestLedger, CapabilityRequestLedger
	case ServiceActionGatewayExecutionPin:
		wantOperation, wantCapability = ServiceOperationExecutionPin, CapabilityExecutionPin
	case ServiceActionGatewayTransactionRecovery:
		wantOperation, wantCapability = ServiceOperationTransactionRecovery, CapabilityTransactionRecovery
	case ServiceActionControllerMembership:
		wantOperation, wantCapability = ServiceOperationMembership, CapabilityMembership
	default:
		return false
	}
	if request.Operation != wantOperation || request.Capability != wantCapability {
		return false
	}
	if request.Action == ServiceActionStorageBootstrapRead || request.Action == ServiceActionControllerMembership {
		return request.SessionID == ([16]byte{}) && request.SessionRevision == 0
	}
	return request.SessionID != ([16]byte{}) && request.SessionRevision != 0
}
