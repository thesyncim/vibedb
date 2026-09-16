package gateway

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// FrontendDrainLifecycle is the durable lifecycle of one physical gateway
// drain.  The node directory and this record advance together at the
// Prepared -> Enforcing and Enforcing -> Retired boundaries.
type FrontendDrainLifecycle uint8

const (
	FrontendDrainPrepared FrontendDrainLifecycle = iota + 1
	FrontendDrainEnforcing
	FrontendDrainRetired
)

func (lifecycle FrontendDrainLifecycle) Valid() bool {
	return lifecycle >= FrontendDrainPrepared && lifecycle <= FrontendDrainRetired
}

func (lifecycle FrontendDrainLifecycle) Allows(next FrontendDrainLifecycle) bool {
	if !lifecycle.Valid() || !next.Valid() {
		return false
	}
	if lifecycle == next {
		return true
	}
	switch lifecycle {
	case FrontendDrainPrepared:
		return next == FrontendDrainEnforcing
	case FrontendDrainEnforcing:
		return next == FrontendDrainRetired
	default:
		return false
	}
}

// FrontendDrainRecord is the bounded durable proof for one decommissioning
// gateway.  IntentID and DrainID are separate: the former identifies the
// operator request, while the latter identifies the exact physical and
// gateway session fence.  ContinuationGrant is nil when admission captured no
// accepted socket; an empty drain is represented by DrainFence instead of a
// fabricated bearer token.
type FrontendDrainRecord struct {
	IntentID [32]byte
	// DecommissionIntentID is an input-compatible alias for IntentID. Writers
	// canonicalize it before persistence and readers return both fields equal.
	DecommissionIntentID [32]byte `json:"decommission_intent_id,omitempty"`
	DrainID              [32]byte
	TrustDomain          rafttransport.TrustDomain
	PhysicalNode         rafttransport.NodeID
	PhysicalIncarnation  uint64
	GatewayServiceID     rafttransport.NodeID
	GatewayIncarnation   uint64
	PeerKeyDigest        replication.Digest
	// GatewayServiceKeyDigest is retained as a descriptive alias for callers
	// that construct records from GatewayIdentity.
	GatewayServiceKeyDigest    replication.Digest `json:"gateway_service_key_digest,omitempty"`
	GatewayIdentityServiceID   [16]byte
	GatewaySessionID           [16]byte
	GatewaySessionRevision     uint64
	NodeRevision               uint64
	AdmissionEpoch             uint64
	AdmissionClosedProofDigest replication.Digest
	// ReceiverDirectory* binds the Prepared acknowledgement roster to one
	// complete physical-node cut. EnforceFrontendDrain rechecks this fence so a
	// newly enrolled receiver cannot be omitted between acknowledgement and the
	// Active -> Draining catalog CAS. Zero values are accepted only for
	// compatibility with drain rows written before receiver acknowledgements.
	ReceiverDirectoryRevision uint64
	ReceiverDirectoryDigest   replication.Digest
	ReceiverCatalogGeneration uint64
	ReceiverCatalogHeadDigest replication.Digest
	DrainFence                serviceauthz.ServiceFence
	ContinuationGrant         *serviceauthz.CommittedFrontendContinuationGrant
	Lifecycle                 FrontendDrainLifecycle
	Revision                  uint64
}

// NewFrontendDrainID deterministically binds an operator intent to one exact
// physical incarnation. It is stable across process/controller retries.
func NewFrontendDrainID(intentID [32]byte, node NodeReference) [32]byte {
	if intentID == ([32]byte{}) || !node.Valid() {
		return [32]byte{}
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/frontend-drain/id/v2\x00"))
	_, _ = hash.Write(intentID[:])
	_, _ = hash.Write(node.NodeID[:])
	var scalar [8]byte
	binary.BigEndian.PutUint64(scalar[:], node.Incarnation)
	_, _ = hash.Write(scalar[:])
	return sha256.Sum256(hash.Sum(nil))
}

func (record FrontendDrainRecord) normalized() FrontendDrainRecord {
	if record.IntentID == ([32]byte{}) {
		record.IntentID = record.DecommissionIntentID
	}
	if record.DecommissionIntentID == ([32]byte{}) {
		record.DecommissionIntentID = record.IntentID
	}
	if record.PeerKeyDigest == (replication.Digest{}) {
		record.PeerKeyDigest = record.GatewayServiceKeyDigest
	}
	if record.GatewayServiceKeyDigest == (replication.Digest{}) {
		record.GatewayServiceKeyDigest = record.PeerKeyDigest
	}
	return record
}

func (record FrontendDrainRecord) keyDigest() replication.Digest {
	if record.PeerKeyDigest != (replication.Digest{}) {
		return record.PeerKeyDigest
	}
	return record.GatewayServiceKeyDigest
}

func (record FrontendDrainRecord) validGrantIdentity(grant serviceauthz.CommittedFrontendContinuationGrant) bool {
	return grant.TrustDomain == record.TrustDomain && grant.PhysicalNode == record.PhysicalNode &&
		grant.PhysicalIncarnation == record.PhysicalIncarnation && grant.GatewayServiceID == record.GatewayServiceID &&
		grant.PeerKeyDigest == [32]byte(record.keyDigest()) && grant.GatewaySessionID == record.GatewaySessionID &&
		grant.GatewaySessionRevision == record.GatewaySessionRevision && grant.DrainID == record.DrainID &&
		grant.AdmissionEpoch == record.AdmissionEpoch &&
		grant.AdmissionClosedProofDigest == [32]byte(record.AdmissionClosedProofDigest) && grant.Revision <= record.NodeRevision
}

func (record FrontendDrainRecord) Valid() bool {
	record = record.normalized()
	if record.IntentID == ([32]byte{}) || record.DecommissionIntentID != record.IntentID ||
		record.DrainID == ([32]byte{}) || record.TrustDomain.ClusterID == ([16]byte{}) ||
		record.TrustDomain.ClusterIncarnation == ([16]byte{}) ||
		record.PhysicalNode == (rafttransport.NodeID{}) || record.PhysicalIncarnation == 0 ||
		record.GatewayServiceID == (rafttransport.NodeID{}) || record.GatewayIncarnation == 0 ||
		record.keyDigest() == (replication.Digest{}) || record.GatewaySessionID == ([16]byte{}) ||
		record.GatewayIdentityServiceID == ([16]byte{}) ||
		record.GatewaySessionRevision == 0 || record.NodeRevision == 0 || record.AdmissionEpoch == 0 ||
		record.AdmissionClosedProofDigest == (replication.Digest{}) || !record.Lifecycle.Valid() || record.Revision == 0 {
		return false
	}
	if record.PeerKeyDigest != (replication.Digest{}) &&
		record.GatewayServiceKeyDigest != (replication.Digest{}) &&
		record.PeerKeyDigest != record.GatewayServiceKeyDigest {
		return false
	}
	if (record.ReceiverDirectoryRevision == 0) != (record.ReceiverDirectoryDigest == (replication.Digest{})) ||
		(record.ReceiverDirectoryRevision == 0) != (record.ReceiverCatalogGeneration == 0) ||
		(record.ReceiverDirectoryRevision == 0) != (record.ReceiverCatalogHeadDigest == (replication.Digest{})) {
		return false
	}
	if record.DrainID != NewFrontendDrainID(record.IntentID, NodeReference{
		NodeID: record.PhysicalNode, Incarnation: record.PhysicalIncarnation,
	}) {
		return false
	}
	if record.ContinuationGrant != nil {
		grant := *record.ContinuationGrant
		if !grant.Valid() || !record.validGrantIdentity(grant) ||
			!validFrontendDrainFence(record.DrainFence, record.GatewaySessionID, record.GatewaySessionRevision) ||
			(grant.State == serviceauthz.ContinuationGrantPrepared && record.Lifecycle != FrontendDrainPrepared) ||
			(grant.State == serviceauthz.ContinuationGrantEnforcing && record.Lifecycle != FrontendDrainEnforcing) ||
			(grant.State == serviceauthz.ContinuationGrantRetired && record.Lifecycle != FrontendDrainRetired) {
			return false
		}
		if record.DrainFence != (serviceauthz.ServiceFence{}) &&
			!validFrontendDrainFence(record.DrainFence, record.GatewaySessionID, record.GatewaySessionRevision) {
			return false
		}
		return true
	}
	if !validFrontendDrainFence(record.DrainFence, record.GatewaySessionID, record.GatewaySessionRevision) {
		return false
	}
	return true
}

func validFrontendDrainFence(fence serviceauthz.ServiceFence, sessionID [16]byte, sessionRevision uint64) bool {
	return fence.Action.Valid() && fence.Operation.Valid() && fence.Group != (raftmember.GroupKey{}) &&
		fence.IntentID != ([32]byte{}) && fence.FenceDigest != ([32]byte{}) &&
		fence.SessionID == sessionID && fence.SessionRevision == sessionRevision
}

// ValidForNode binds a record to the immutable physical and gateway identity
// in a catalog NodeRecord. The caller still supplies the expected node
// revision to the authority CAS; this predicate does not authorize a state
// transition by itself.
func (record FrontendDrainRecord) ValidForNode(node NodeRecord) bool {
	record = record.normalized()
	return record.Valid() && node.Valid() && record.PhysicalNode == node.NodeID &&
		record.PhysicalIncarnation == node.Incarnation && record.GatewayServiceID == node.Gateway.NodeID &&
		record.GatewayIncarnation == node.Gateway.Incarnation && record.keyDigest() == node.Gateway.ServiceKeyDigest &&
		record.GatewayIdentityServiceID == node.Gateway.ServiceID && record.GatewaySessionID == node.Gateway.SessionID &&
		record.GatewaySessionRevision == node.Gateway.SessionRevision && record.NodeRevision == node.Revision
}
