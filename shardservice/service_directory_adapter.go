package shardservice

import (
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

const (
	continuationWireTag       = 0xd1
	continuationEnvelopeBytes = 1 + 32 + 32 + 1 + 1 + 8 + 1 + 72 + 16 + 32 + 32
)

var ErrInvalidFrontendContinuationEnvelope = errors.New("shardservice: invalid frontend continuation envelope")

// authenticatedServicePeer is intentionally a secondary interface. Older
// test and loopback connections can still implement PeerConnection without a
// certificate binding, while every production PeerTLS connection implements
// this verified-leaf method. A nonzero directory gate rejects the former.
type authenticatedServicePeer interface {
	PeerIdentity() rafttransport.PeerIdentity
	PeerKeyDigest() [32]byte
}

func servicePeerFromConnection(connection any) serviceauthz.AuthenticatedPeer {
	peer, ok := connection.(authenticatedServicePeer)
	if !ok || peer == nil {
		return serviceauthz.AuthenticatedPeer{}
	}
	return serviceauthz.AuthenticatedPeer{Identity: peer.PeerIdentity(), KeyDigest: peer.PeerKeyDigest()}
}

// SetFrontendContinuation installs only the outer connection proof. The
// request's closed operation union is independently decoded by the receiver
// and must produce the same exact scope before this envelope is accepted.
func (request *ReplicatedRequest) SetFrontendContinuation(
	envelope serviceauthz.FrontendContinuationEnvelope,
) error {
	if request == nil || !envelope.Valid() {
		return ErrInvalidFrontendContinuationEnvelope
	}
	scope, ok := FrontendContinuationScopeForReplicatedRequestWithProtocol(request, envelope.Scope.Protocol)
	if !ok || !sameFrontendContinuationScope(scope, envelope.Scope) {
		return ErrInvalidFrontendContinuationEnvelope
	}
	copy := envelope
	request.Continuation = &copy
	return nil
}

func sameFrontendContinuationScope(left, right serviceauthz.FrontendContinuationScopeRecord) bool {
	return left.Protocol == right.Protocol && left.Action == right.Action && left.Capability == right.Capability &&
		left.Operation == right.Operation && left.Group == right.Group && left.Relation == right.Relation &&
		left.IntentID == right.IntentID && left.FenceDigest == right.FenceDigest
}

// FrontendContinuationScopeForReplicatedRequest derives the closed outer
// scope from the already decoded native request. It is shared by gateway
// senders and shard receivers so a caller cannot choose a different action,
// group, relation, or internal resource tuple in the envelope.
func FrontendContinuationScopeForReplicatedRequest(
	request *ReplicatedRequest,
) (serviceauthz.FrontendContinuationScopeRecord, bool) {
	return FrontendContinuationScopeForReplicatedRequestWithProtocol(request, serviceauthz.FrontendScopeNative)
}

// FrontendContinuationScopeForReplicatedRequestWithProtocol derives the same
// closed native operation/resource scope while retaining the authenticated
// frontend origin (Native or PostgreSQL) in the committed envelope.
func FrontendContinuationScopeForReplicatedRequestWithProtocol(
	request *ReplicatedRequest, protocol serviceauthz.FrontendContinuationScope,
) (serviceauthz.FrontendContinuationScopeRecord, bool) {
	if request == nil || request.Fence.Group == (raftmember.GroupKey{}) || !request.Capability.Valid() {
		return serviceauthz.FrontendContinuationScopeRecord{}, false
	}
	scope := serviceauthz.FrontendContinuationScopeRecord{
		Protocol: protocol, Capability: request.Capability,
		Group: request.Fence.Group,
	}
	// A point read has one relation. Batch/query/propose requests are scoped to
	// their exact group and typed operation, with no fabricated relation
	// wildcard; the catalog commits that group-level forwarding boundary.
	binary.BigEndian.PutUint16(scope.Relation[len(scope.Relation)-2:], uint16(request.Relation))
	switch request.Operation {
	case ReplicatedProbe:
		switch request.Capability {
		case serviceauthz.CapabilityDataRead:
			scope.Action, scope.Operation = serviceauthz.FrontendActionForwardedData,
				serviceauthz.ServiceOperationForwardedRead
		case serviceauthz.CapabilityDataWrite:
			scope.Action, scope.Operation = serviceauthz.FrontendActionForwardedData,
				serviceauthz.ServiceOperationForwardedWrite
		case serviceauthz.CapabilityTopology:
			// A probe has no relation ordinal. Catalog probes are scoped to
			// the committed relation manifest carried by the serving fence.
			scope.Action, scope.Operation = serviceauthz.FrontendActionGatewayCatalog,
				serviceauthz.ServiceOperationCatalogRead
		default:
			return serviceauthz.FrontendContinuationScopeRecord{}, false
		}
	case ReplicatedPropose:
		switch request.Capability {
		case serviceauthz.CapabilityDataWrite:
			scope.Action, scope.Operation = serviceauthz.FrontendActionForwardedData,
				serviceauthz.ServiceOperationForwardedWrite
		case serviceauthz.CapabilityTopology:
			scope.Action, scope.Operation = serviceauthz.FrontendActionGatewayCatalog, serviceauthz.ServiceOperationCatalogWrite
		default:
			return serviceauthz.FrontendContinuationScopeRecord{}, false
		}
	case ReplicatedReadLeader, ReplicatedReadFollower, ReplicatedReadBatchLeader, ReplicatedQueryLeader:
		if request.Capability == serviceauthz.CapabilityDataRead {
			scope.Action, scope.Operation = serviceauthz.FrontendActionForwardedData,
				serviceauthz.ServiceOperationForwardedRead
		} else if request.Capability == serviceauthz.CapabilityTopology && request.Operation == ReplicatedReadLeader {
			scope.Action, scope.Operation = serviceauthz.FrontendActionGatewayCatalog, serviceauthz.ServiceOperationCatalogRead
		} else {
			return serviceauthz.FrontendContinuationScopeRecord{}, false
		}
	default:
		return serviceauthz.FrontendContinuationScopeRecord{}, false
	}
	if scope.Action != serviceauthz.FrontendActionForwardedData {
		if scope.Action != serviceauthz.FrontendActionGatewayCatalog ||
			request.Fence.Command.RelationManifestDigest == ([32]byte{}) {
			return serviceauthz.FrontendContinuationScopeRecord{}, false
		}
		if request.Operation != ReplicatedReadLeader {
			copy(scope.Relation[:], request.Fence.Command.RelationManifestDigest[:len(scope.Relation)])
		}
		scope.IntentID = request.Fence.Command.RelationManifestDigest
		scope.FenceDigest = request.Fence.Command.RelationManifestDigest
	}
	if !scope.Valid() {
		return serviceauthz.FrontendContinuationScopeRecord{}, false
	}
	return scope, true
}

func replicatedRequestRequiresServiceScope(request *ReplicatedRequest) bool {
	if request == nil {
		return false
	}
	switch request.Operation {
	case ReplicatedRequestLedgerRead, ReplicatedExecutionPinRead, ReplicatedTransactionRead, ReplicatedMembership:
		return true
	case ReplicatedPropose:
		return request.Capability != serviceauthz.CapabilityDataWrite
	case ReplicatedReadLeader, ReplicatedReadFollower, ReplicatedReadBatchLeader, ReplicatedQueryLeader:
		return request.Capability == serviceauthz.CapabilityTopology
	default:
		return false
	}
}

func encodeFrontendContinuation(e *encbuf, envelope *serviceauthz.FrontendContinuationEnvelope) {
	e.u8(continuationWireTag)
	e.b = append(e.b, envelope.GrantDigest[:]...)
	e.b = append(e.b, envelope.ConnToken[:]...)
	scope := envelope.Scope
	e.u8(uint8(scope.Protocol))
	e.u8(uint8(scope.Action))
	e.u64(uint64(scope.Capability))
	e.u8(uint8(scope.Operation))
	e.b = append(e.b, scope.Group.ClusterID[:]...)
	e.b = append(e.b, scope.Group.ClusterIncarnation[:]...)
	e.u64(scope.Group.TopologyRecoveryEpoch)
	e.b = append(e.b, scope.Group.ShardIncarnation[:]...)
	e.b = append(e.b, scope.Group.GroupID[:]...)
	e.b = append(e.b, scope.Relation[:]...)
	e.b = append(e.b, scope.IntentID[:]...)
	e.b = append(e.b, scope.FenceDigest[:]...)
}

func decodeFrontendContinuation(d *deccur) *serviceauthz.FrontendContinuationEnvelope {
	if len(d.b) != continuationEnvelopeBytes {
		d.fail(ErrInvalidFrontendContinuationEnvelope)
		return nil
	}
	if d.u8() != continuationWireTag {
		d.fail(ErrInvalidFrontendContinuationEnvelope)
		return nil
	}
	envelope := &serviceauthz.FrontendContinuationEnvelope{}
	envelope.GrantDigest = d.fixed32()
	envelope.ConnToken = serviceauthz.FrontendConnToken(d.fixed32())
	envelope.Scope = serviceauthz.FrontendContinuationScopeRecord{
		Protocol:   serviceauthz.FrontendContinuationScope(d.u8()),
		Action:     serviceauthz.FrontendContinuationAction(d.u8()),
		Capability: serviceauthz.Capability(d.u64()),
		Operation:  serviceauthz.ServiceOperation(d.u8()),
		Group: raftmember.GroupKey{
			ClusterID: d.fixed16(), ClusterIncarnation: d.fixed16(),
			TopologyRecoveryEpoch: d.u64(), ShardIncarnation: d.fixed16(), GroupID: d.fixed16(),
		},
		Relation: d.fixed16(), IntentID: d.fixed32(), FenceDigest: d.fixed32(),
	}
	if d.bad() || !envelope.Valid() {
		d.fail(ErrInvalidFrontendContinuationEnvelope)
		return nil
	}
	return envelope
}

func frontendContinuationTailBytes(request *ReplicatedRequest) int {
	if request != nil && request.Continuation != nil {
		return continuationEnvelopeBytes
	}
	return 0
}

// serviceFenceForReplicatedRequest is deliberately empty until a request has
// an explicit committed continuation fence. Active gateway principals need no
// continuation; a draining principal is therefore denied by the directory
// gate rather than being allowed to smuggle session fields through the old
// native wire grammar.
func serviceFenceForReplicatedRequest(*ReplicatedRequest) serviceauthz.ServiceFence {
	return serviceauthz.ServiceFence{}
}
