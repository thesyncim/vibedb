package serviceauthz

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

// ContinuationGrantState is the committed lifecycle of a frontend drain
// proof. Prepared permits installation while the gateway is still Active;
// Enforcing is the only state that admits a wrapped request after the
// directory enters Draining; Retired is a durable tombstone.
type ContinuationGrantState uint8

const (
	ContinuationGrantPrepared ContinuationGrantState = iota + 1
	ContinuationGrantEnforcing
	ContinuationGrantRetired
)

func (state ContinuationGrantState) Valid() bool {
	return state >= ContinuationGrantPrepared && state <= ContinuationGrantRetired
}

func (state ContinuationGrantState) Allows(next ContinuationGrantState) bool {
	if !state.Valid() || !next.Valid() {
		return false
	}
	if state == next {
		return true
	}
	switch state {
	case ContinuationGrantPrepared:
		return next == ContinuationGrantEnforcing || next == ContinuationGrantRetired
	case ContinuationGrantEnforcing:
		return next == ContinuationGrantRetired
	default:
		return false
	}
}

// FrontendContinuationAction is the bounded action grammar for a wrapped
// request. ForwardedData is checked by the existing user/operator Gate after
// this proof; the service actions are checked against their typed operation
// and committed resource tuple.
type FrontendContinuationAction uint8

const (
	FrontendActionForwardedData FrontendContinuationAction = iota + 1
	FrontendActionGatewayCatalog
	FrontendActionGatewayRequestLedger
	FrontendActionGatewayExecutionPin
	FrontendActionGatewayTransactionRecovery
	FrontendActionControllerMembership
)

func (action FrontendContinuationAction) Valid() bool {
	return action >= FrontendActionForwardedData && action <= FrontendActionControllerMembership
}

// FrontendContinuationScopeRecord is the exact scope committed for a
// connection token. Relation is deliberately retained even for group-level
// service actions so a copied grant cannot be replayed against another
// catalog relation.
type FrontendContinuationScopeRecord struct {
	Protocol    FrontendContinuationScope
	Action      FrontendContinuationAction
	Capability  Capability
	Operation   ServiceOperation
	Group       raftmember.GroupKey
	Relation    [16]byte
	IntentID    [32]byte
	FenceDigest [32]byte
}

func (scope FrontendContinuationScopeRecord) Valid() bool {
	if !scope.Protocol.Valid() || !scope.Action.Valid() || !scope.Capability.Valid() ||
		scope.Group == (raftmember.GroupKey{}) {
		return false
	}
	switch scope.Action {
	case FrontendActionForwardedData:
		return (scope.Operation == ServiceOperationForwardedRead && scope.Capability == CapabilityDataRead ||
			scope.Operation == ServiceOperationForwardedWrite && scope.Capability == CapabilityDataWrite) &&
			// Query and mutation forwarding can cover the exact routed group
			// without naming a single relation. Point reads still carry their
			// relation in this field; an internal action may never omit it.
			scope.IntentID == ([32]byte{}) && scope.FenceDigest == ([32]byte{})
	case FrontendActionGatewayCatalog:
		return scope.Capability == CapabilityTopology &&
			(scope.Operation == ServiceOperationCatalogRead || scope.Operation == ServiceOperationCatalogWrite) &&
			scope.Relation != ([16]byte{}) && scope.IntentID != ([32]byte{}) && scope.FenceDigest != ([32]byte{})
	case FrontendActionGatewayRequestLedger:
		return scope.Operation == ServiceOperationRequestLedger && scope.Capability == CapabilityRequestLedger &&
			scope.Relation != ([16]byte{}) && scope.IntentID != ([32]byte{}) && scope.FenceDigest != ([32]byte{})
	case FrontendActionGatewayExecutionPin:
		return scope.Operation == ServiceOperationExecutionPin && scope.Capability == CapabilityExecutionPin &&
			scope.Relation != ([16]byte{}) && scope.IntentID != ([32]byte{}) && scope.FenceDigest != ([32]byte{})
	case FrontendActionGatewayTransactionRecovery:
		return scope.Operation == ServiceOperationTransactionRecovery && scope.Capability == CapabilityTransactionRecovery &&
			scope.Relation != ([16]byte{}) && scope.IntentID != ([32]byte{}) && scope.FenceDigest != ([32]byte{})
	case FrontendActionControllerMembership:
		return scope.Operation == ServiceOperationMembership && scope.Capability == CapabilityMembership &&
			scope.Relation != ([16]byte{}) && scope.IntentID != ([32]byte{}) && scope.FenceDigest != ([32]byte{})
	default:
		return false
	}
}

func compareContinuationScopes(left, right FrontendContinuationScopeRecord) int {
	if left.Protocol != right.Protocol {
		if left.Protocol < right.Protocol {
			return -1
		}
		return 1
	}
	if left.Action != right.Action {
		if left.Action < right.Action {
			return -1
		}
		return 1
	}
	if left.Capability != right.Capability {
		if left.Capability < right.Capability {
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
	if result := compareContinuationGroups(left.Group, right.Group); result != 0 {
		return result
	}
	if result := bytes.Compare(left.Relation[:], right.Relation[:]); result != 0 {
		return result
	}
	if result := bytes.Compare(left.IntentID[:], right.IntentID[:]); result != 0 {
		return result
	}
	return bytes.Compare(left.FenceDigest[:], right.FenceDigest[:])
}

func sameContinuationScope(left, right FrontendContinuationScopeRecord) bool {
	return compareContinuationScopes(left, right) == 0
}

func compareContinuationGroups(left, right raftmember.GroupKey) int {
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

// FrontendContinuationEnvelope is supplied by a connection owner and carries
// no identity authority. The receiver compares every field with its
// committed grant and then decodes the actual request into the same scope
// record before admitting it.
type FrontendContinuationEnvelope struct {
	GrantDigest [32]byte
	ConnToken   FrontendConnToken
	Scope       FrontendContinuationScopeRecord
}

func (envelope FrontendContinuationEnvelope) Valid() bool {
	return envelope.GrantDigest != ([32]byte{}) && envelope.ConnToken != (FrontendConnToken{}) && envelope.Scope.Valid()
}

// CommittedFrontendContinuationGrant is an immutable catalog cut. The
// accepted-token and scope sets are bounded and canonicalized; a grant never
// accepts a caller populated token merely because its digest is nonzero.
type CommittedFrontendContinuationGrant struct {
	GrantDigest              [32]byte
	TrustDomain              rafttransport.TrustDomain
	PhysicalNode             rafttransport.NodeID
	PhysicalIncarnation      uint64
	PeerKeyDigest            [32]byte
	GatewayServiceID         rafttransport.NodeID
	GatewaySessionID         [16]byte
	GatewaySessionRevision   uint64
	DrainID                  [32]byte
	AdmissionEpoch           uint64
	AcceptedConnectionTokens []FrontendConnToken
	// AcceptedConnectionProtocols binds each token to the frontend origin
	// that admitted it. Parallel slices are kept fixed-width so the token
	// itself remains unmodified and protocol cross-replay is impossible.
	AcceptedConnectionProtocols []FrontendContinuationScope
	AllowedScopes               []FrontendContinuationScopeRecord
	AdmissionClosedProofDigest  [32]byte
	Revision                    uint64
	State                       ContinuationGrantState
}

var ErrInvalidContinuationGrant = errors.New("serviceauthz: invalid committed continuation grant")

const (
	AbsoluteMaxContinuationTokens = 65536
	AbsoluteMaxContinuationScopes = 65536
)

func (grant CommittedFrontendContinuationGrant) Valid() bool {
	if grant.GrantDigest == ([32]byte{}) || grant.TrustDomain.ClusterID == ([16]byte{}) ||
		grant.TrustDomain.ClusterIncarnation == ([16]byte{}) || grant.PhysicalNode == (rafttransport.NodeID{}) ||
		grant.PhysicalIncarnation == 0 || grant.PeerKeyDigest == ([32]byte{}) ||
		grant.GatewayServiceID == (rafttransport.NodeID{}) || grant.GatewaySessionID == ([16]byte{}) ||
		grant.GatewaySessionRevision == 0 || grant.DrainID == ([32]byte{}) || grant.AdmissionEpoch == 0 ||
		grant.AdmissionClosedProofDigest == ([32]byte{}) || grant.Revision == 0 || !grant.State.Valid() ||
		len(grant.AcceptedConnectionTokens) == 0 || len(grant.AcceptedConnectionTokens) > AbsoluteMaxContinuationTokens ||
		len(grant.AcceptedConnectionProtocols) != len(grant.AcceptedConnectionTokens) ||
		len(grant.AllowedScopes) == 0 || len(grant.AllowedScopes) > AbsoluteMaxContinuationScopes {
		return false
	}
	for index, token := range grant.AcceptedConnectionTokens {
		if token == (FrontendConnToken{}) || index > 0 && bytes.Compare(grant.AcceptedConnectionTokens[index-1][:], token[:]) >= 0 {
			return false
		}
		if !grant.AcceptedConnectionProtocols[index].Valid() {
			return false
		}
	}
	for index, scope := range grant.AllowedScopes {
		if !scope.Valid() || index > 0 && compareContinuationScopes(grant.AllowedScopes[index-1], scope) >= 0 {
			return false
		}
	}
	return grant.GrantDigest == grant.Digest()
}

// Digest computes the canonical committed grant identity. GrantDigest itself
// is excluded so NewCommittedFrontendContinuationGrant can fill it once.
func (grant CommittedFrontendContinuationGrant) Digest() [32]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/serviceauthz/frontend-continuation/v1\x00"))
	_, _ = hash.Write(grant.TrustDomain.ClusterID[:])
	_, _ = hash.Write(grant.TrustDomain.ClusterIncarnation[:])
	_, _ = hash.Write(grant.PhysicalNode[:])
	writeU64(hash, grant.PhysicalIncarnation)
	_, _ = hash.Write(grant.PeerKeyDigest[:])
	_, _ = hash.Write(grant.GatewayServiceID[:])
	_, _ = hash.Write(grant.GatewaySessionID[:])
	writeU64(hash, grant.GatewaySessionRevision)
	_, _ = hash.Write(grant.DrainID[:])
	writeU64(hash, grant.AdmissionEpoch)
	_, _ = hash.Write(grant.AdmissionClosedProofDigest[:])
	writeU64(hash, grant.Revision)
	// State is the lifecycle of one committed grant identity.  It is
	// deliberately excluded from the digest so Prepared -> Enforcing ->
	// Retired transitions retain the same proof identity; the directory cut
	// carries the authenticated lifecycle transition separately.
	writeU64(hash, uint64(len(grant.AcceptedConnectionTokens)))
	for _, token := range grant.AcceptedConnectionTokens {
		_, _ = hash.Write(token[:])
	}
	for _, protocol := range grant.AcceptedConnectionProtocols {
		_, _ = hash.Write([]byte{byte(protocol)})
	}
	writeU64(hash, uint64(len(grant.AllowedScopes)))
	for _, scope := range grant.AllowedScopes {
		_, _ = hash.Write([]byte{byte(scope.Protocol), byte(scope.Action)})
		writeU64(hash, uint64(scope.Capability))
		_, _ = hash.Write([]byte{byte(scope.Operation)})
		writeGroup(hash, scope.Group)
		_, _ = hash.Write(scope.Relation[:])
		_, _ = hash.Write(scope.IntentID[:])
		_, _ = hash.Write(scope.FenceDigest[:])
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func NewCommittedFrontendContinuationGrant(grant CommittedFrontendContinuationGrant) (CommittedFrontendContinuationGrant, error) {
	grant.GrantDigest = grant.Digest()
	if !grant.Valid() {
		return CommittedFrontendContinuationGrant{}, ErrInvalidContinuationGrant
	}
	return grant, nil
}

func writeU64(hash interface{ Write([]byte) (int, error) }, value uint64) {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], value)
	_, _ = hash.Write(raw[:])
}

func writeGroup(hash interface{ Write([]byte) (int, error) }, group raftmember.GroupKey) {
	_, _ = hash.Write(group.ClusterID[:])
	_, _ = hash.Write(group.ClusterIncarnation[:])
	writeU64(hash, group.TopologyRecoveryEpoch)
	_, _ = hash.Write(group.ShardIncarnation[:])
	_, _ = hash.Write(group.GroupID[:])
}

func cloneContinuationGrant(grant CommittedFrontendContinuationGrant) CommittedFrontendContinuationGrant {
	grant.AcceptedConnectionTokens = slices.Clone(grant.AcceptedConnectionTokens)
	grant.AcceptedConnectionProtocols = slices.Clone(grant.AcceptedConnectionProtocols)
	grant.AllowedScopes = slices.Clone(grant.AllowedScopes)
	return grant
}

func sameContinuationGrant(left, right CommittedFrontendContinuationGrant) bool {
	return left.GrantDigest == right.GrantDigest && left.TrustDomain == right.TrustDomain &&
		left.PhysicalNode == right.PhysicalNode && left.PhysicalIncarnation == right.PhysicalIncarnation &&
		left.PeerKeyDigest == right.PeerKeyDigest && left.GatewayServiceID == right.GatewayServiceID &&
		left.GatewaySessionID == right.GatewaySessionID && left.GatewaySessionRevision == right.GatewaySessionRevision &&
		left.DrainID == right.DrainID && left.AdmissionEpoch == right.AdmissionEpoch &&
		left.AdmissionClosedProofDigest == right.AdmissionClosedProofDigest && left.Revision == right.Revision &&
		left.State == right.State && slices.Equal(left.AcceptedConnectionTokens, right.AcceptedConnectionTokens) &&
		slices.Equal(left.AcceptedConnectionProtocols, right.AcceptedConnectionProtocols) &&
		slices.Equal(left.AllowedScopes, right.AllowedScopes)
}

func containsContinuationToken(tokens []FrontendConnToken, want FrontendConnToken) bool {
	_, found := continuationTokenIndex(tokens, want)
	return found
}

func continuationTokenIndex(tokens []FrontendConnToken, want FrontendConnToken) (int, bool) {
	index, found := slices.BinarySearchFunc(tokens, want, func(left FrontendConnToken, right FrontendConnToken) int {
		return bytes.Compare(left[:], right[:])
	})
	return index, found && index >= 0
}

func containsContinuationScope(scopes []FrontendContinuationScopeRecord, want FrontendContinuationScopeRecord) bool {
	index, found := slices.BinarySearchFunc(scopes, want, func(left, right FrontendContinuationScopeRecord) int {
		return compareContinuationScopes(left, right)
	})
	return found && index >= 0 && sameContinuationScope(scopes[index], want)
}

// grantMatchesBinding ties a continuation grant to the exact directory
// binding that owns the accepted frontend connections.  The grant is never a
// bearer token: every identity and lifecycle coordinate is checked against
// the current committed binding before a request can use it.
func grantMatchesBinding(grant CommittedFrontendContinuationGrant, binding ServiceBinding) bool {
	if !grant.Valid() || !binding.Valid() {
		return false
	}
	return grantMatchesBindingFields(grant, binding)
}

func grantMatchesBindingFields(grant CommittedFrontendContinuationGrant, binding ServiceBinding) bool {
	if !binding.Valid() ||
		grant.GatewayServiceID != binding.Principal ||
		grant.PhysicalNode != binding.PhysicalNode ||
		grant.PhysicalIncarnation != binding.PhysicalIncarnation ||
		grant.PeerKeyDigest != binding.KeyDigest ||
		grant.GatewaySessionID != binding.SessionID ||
		grant.GatewaySessionRevision != binding.SessionRevision ||
		binding.Roles&ServiceRoleGateway == 0 {
		return false
	}
	switch grant.State {
	case ContinuationGrantPrepared:
		return binding.Lifecycle == ServiceActive
	case ContinuationGrantEnforcing:
		return binding.Lifecycle == ServiceDraining
	case ContinuationGrantRetired:
		return binding.Lifecycle == ServiceDraining || binding.Lifecycle == ServiceDecommissioned
	default:
		return false
	}
}

func validContinuationTransition(prior, next CommittedFrontendContinuationGrant) bool {
	if !prior.Valid() || !next.Valid() || prior.GrantDigest != next.GrantDigest ||
		prior.TrustDomain != next.TrustDomain || prior.PhysicalNode != next.PhysicalNode ||
		prior.PhysicalIncarnation != next.PhysicalIncarnation || prior.PeerKeyDigest != next.PeerKeyDigest ||
		prior.GatewayServiceID != next.GatewayServiceID || prior.GatewaySessionID != next.GatewaySessionID ||
		prior.GatewaySessionRevision != next.GatewaySessionRevision || prior.DrainID != next.DrainID ||
		prior.AdmissionEpoch != next.AdmissionEpoch ||
		prior.AdmissionClosedProofDigest != next.AdmissionClosedProofDigest || prior.Revision != next.Revision ||
		!slices.Equal(prior.AcceptedConnectionTokens, next.AcceptedConnectionTokens) ||
		!slices.Equal(prior.AcceptedConnectionProtocols, next.AcceptedConnectionProtocols) ||
		!slices.Equal(prior.AllowedScopes, next.AllowedScopes) {
		return false
	}
	return prior.State.Allows(next.State)
}

// CheckFrontendContinuation validates a wrapped request against the current
// committed grant.  ForwardedData only proves the service-to-service drain
// boundary; the caller must still run the existing user/operator Gate for the
// forwarded authority.  Service actions remain constrained by their exact
// committed scope tuple.
func (gate *ServiceDirectoryGate) CheckFrontendContinuation(
	peer AuthenticatedPeer, policyGeneration uint64, envelope FrontendContinuationEnvelope,
	actual FrontendContinuationScopeRecord,
) DecisionCode {
	if gate == nil || !envelope.Valid() || !actual.Valid() ||
		!sameContinuationScope(envelope.Scope, actual) {
		return DecisionDenyCapability
	}
	state, binding, ok := gate.lookup(peer)
	if !ok || policyGeneration == 0 || policyGeneration != state.cut.PolicyGeneration ||
		binding.Roles&ServiceRoleGateway == 0 {
		return DecisionDenyNoPrincipal
	}
	grant, found := state.continuations[envelope.GrantDigest]
	tokenIndex, tokenFound := continuationTokenIndex(grant.AcceptedConnectionTokens, envelope.ConnToken)
	if !found || !grant.Valid() || grant.State == ContinuationGrantRetired ||
		grant.TrustDomain != peer.Identity.TrustDomain ||
		grant.GatewayServiceID != peer.Identity.Node || grant.PeerKeyDigest != peer.KeyDigest ||
		grant.GatewaySessionID != binding.SessionID || grant.GatewaySessionRevision != binding.SessionRevision ||
		!tokenFound || tokenIndex >= len(grant.AcceptedConnectionProtocols) ||
		grant.AcceptedConnectionProtocols[tokenIndex] != envelope.Scope.Protocol ||
		!containsContinuationScope(grant.AllowedScopes, actual) {
		return DecisionDenyCapability
	}
	if !grantMatchesBindingFields(grant, binding) {
		return DecisionDenyCapability
	}
	return DecisionAllow
}

// IsActiveGateway reports the lifecycle state already checked by the
// directory. It lets adapters require the narrower InternalFences for a
// Prepared grant while leaving an Enforcing Draining grant to its exact
// committed continuation scope.
func (gate *ServiceDirectoryGate) IsActiveGateway(peer AuthenticatedPeer, policyGeneration uint64) bool {
	state, binding, ok := gate.lookup(peer)
	return ok && policyGeneration != 0 && policyGeneration == state.cut.PolicyGeneration &&
		binding.Roles&ServiceRoleGateway != 0 && binding.Lifecycle == ServiceActive
}

// CheckInternalFrontendContinuation applies the complete internal-service
// boundary for one wrapped request. Active Prepared grants additionally need
// the binding's exact InternalFence; Draining Enforcing grants use the
// committed continuation scope after the frontend barrier has closed.
func (gate *ServiceDirectoryGate) CheckInternalFrontendContinuation(
	peer AuthenticatedPeer, authority Authority, envelope FrontendContinuationEnvelope,
	actual FrontendContinuationScopeRecord,
) DecisionCode {
	if actual.Action == FrontendActionForwardedData || authority.Node != peer.Identity.Node {
		return DecisionDenyCapability
	}
	if decision := gate.CheckFrontendContinuation(peer, authority.Generation, envelope, actual); decision != DecisionAllow {
		return decision
	}
	if gate.IsActiveGateway(peer, authority.Generation) &&
		gate.CheckInternalScope(peer, authority, actual) != DecisionAllow {
		return DecisionDenyCapability
	}
	return DecisionAllow
}

// CheckInternalScope adapts a trusted decoded continuation scope to the
// closed ServiceRequest grammar. It fills the current gateway session from
// the committed binding instead of accepting session fields from the wire.
// ForwardedData is intentionally rejected here; its user authority remains
// the existing Gate check after CheckFrontendContinuation succeeds.
func (gate *ServiceDirectoryGate) CheckInternalScope(
	peer AuthenticatedPeer, authority Authority, scope FrontendContinuationScopeRecord,
) DecisionCode {
	if gate == nil || !scope.Valid() || scope.Action == FrontendActionForwardedData {
		return DecisionDenyCapability
	}
	state, binding, ok := gate.lookup(peer)
	if !ok || authority.Generation != state.cut.PolicyGeneration || authority.Node != peer.Identity.Node {
		return DecisionDenyNoPrincipal
	}
	action, operation, ok := internalScopeAction(scope)
	if !ok {
		return DecisionDenyCapability
	}
	request := ServiceRequest{Action: action, Capability: scope.Capability, Operation: operation,
		Group: scope.Group, Relation: scope.Relation, IntentID: scope.IntentID, FenceDigest: scope.FenceDigest}
	if binding.Roles&ServiceRoleGateway != 0 {
		request.SessionID, request.SessionRevision = binding.SessionID, binding.SessionRevision
	}
	return gate.CheckInternal(peer, authority, request)
}

func internalScopeAction(scope FrontendContinuationScopeRecord) (ServiceAction, ServiceOperation, bool) {
	switch scope.Action {
	case FrontendActionGatewayCatalog:
		if scope.Operation == ServiceOperationCatalogRead {
			return ServiceActionGatewayCatalogRead, ServiceOperationCatalogRead, true
		}
		if scope.Operation == ServiceOperationCatalogWrite {
			return ServiceActionGatewayCatalogWrite, ServiceOperationCatalogWrite, true
		}
	case FrontendActionGatewayRequestLedger:
		return ServiceActionGatewayRequestLedger, ServiceOperationRequestLedger, true
	case FrontendActionGatewayExecutionPin:
		return ServiceActionGatewayExecutionPin, ServiceOperationExecutionPin, true
	case FrontendActionGatewayTransactionRecovery:
		return ServiceActionGatewayTransactionRecovery, ServiceOperationTransactionRecovery, true
	case FrontendActionControllerMembership:
		return ServiceActionControllerMembership, ServiceOperationMembership, true
	}
	return 0, 0, false
}
