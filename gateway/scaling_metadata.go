package gateway

// This file contains the durable scaling control-plane contract.  The
// contract deliberately lives beside the catalog because node lifecycle and
// group enrollment are catalog-authorized facts; process-local schedulers and
// provisioning code consume these types but do not define another metadata
// authority.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	// These bounds apply before a value is encoded into the replicated
	// catalog.  A bounded directory is part of the safety property: a
	// controller must be able to scan it completely before claiming that a
	// node is safe to stop.
	MaxScalingNodes            = 4096
	MaxScalingIntents          = 256
	MaxEnrollmentIntents       = 4096
	MaxScalingTargetsPerIntent = 256
	MaxScalingMovesPerIntent   = 4096
	MaxScalingBlockers         = 256
	MaxScalingStringBytes      = 4096
)

var (
	ErrInvalidScalingMetadata = errors.New("gateway: invalid scaling metadata")
	ErrScalingMetadataBound   = errors.New("gateway: scaling metadata bound exceeded")
	ErrScalingRevision        = errors.New("gateway: scaling metadata revision conflict")
	ErrScalingState           = errors.New("gateway: invalid scaling state transition")
	ErrScalingIdentity        = errors.New("gateway: scaling identity mismatch")
)

// NodeLifecycle is the physical-node state machine.  Enrollment and
// preparation are group-scoped and therefore are intentionally absent here.
// A draining node remains ineligible for all new placement even when a
// controller records a blocker for it.
type NodeLifecycle uint8

const (
	NodeJoining NodeLifecycle = iota + 1
	NodeActive
	NodeDraining
	NodeDecommissioned
)

func (state NodeLifecycle) Valid() bool {
	return state >= NodeJoining && state <= NodeDecommissioned
}

func (state NodeLifecycle) Allows(next NodeLifecycle) bool {
	if !state.Valid() || !next.Valid() {
		return false
	}
	if state == next {
		return true
	}
	switch state {
	case NodeJoining:
		return next == NodeActive
	case NodeActive:
		return next == NodeDraining
	case NodeDraining:
		return next == NodeDecommissioned
	default:
		return false
	}
}

// NodeRole is a physical capability.  It is never interpreted as Raft
// membership or as authority to publish a catalog.
type NodeRole uint8

const (
	NodeRoleStorage NodeRole = 1 << iota
	NodeRoleCatalog
	NodeRoleGateway
	NodeRoleControl
)

const validNodeRoles = NodeRoleStorage | NodeRoleCatalog | NodeRoleGateway | NodeRoleControl

// GatewayIdentity keeps the gateway participant and its session incarnation
// separate from the physical store identity carried by group replicas.
type GatewayIdentity struct {
	NodeID            rafttransport.NodeID
	Incarnation       uint64
	ServiceKeyDigest  replication.Digest
	ServiceID         [16]byte
	SessionID         [16]byte
	SessionRevision   uint64
	ParticipantDigest replication.Digest
}

// NodeRecord is one replicated physical-node lifecycle record.  Endpoint IDs
// are catalog handles; their addresses are included in the same record so a
// directory read is a complete, revision-fenced participant cut.
type NodeRecord struct {
	NodeID                          rafttransport.NodeID
	Incarnation                     uint64
	ServiceKeyDigest                replication.Digest
	DataEndpoint                    distribution.EndpointID
	NativeEndpoint                  distribution.EndpointID
	ControlEndpoint                 distribution.EndpointID
	GatewayEndpoint                 distribution.EndpointID
	DataAddress                     string
	NativeAddress                   string
	ControlAddress                  string
	GatewayAddress                  string
	FailureDomain                   string
	Roles                           NodeRole
	Capacity                        autosplit.CapacityVector
	Used                            autosplit.CapacityVector
	MigrationCapacity               uint64
	MigrationUsed                   uint64
	MaxReceives                     uint32
	ActiveReceives                  uint32
	Lifecycle                       NodeLifecycle
	Revision                        uint64
	CatalogGeneration               uint64
	Gateway                         GatewayIdentity
	RetirementScanDigest            replication.Digest
	RetirementScanDirectoryRevision uint64
	RetirementScanCutRevision       uint64
}

func (record NodeRecord) Valid() bool {
	if record.NodeID == (rafttransport.NodeID{}) || record.Incarnation == 0 ||
		record.ServiceKeyDigest == (replication.Digest{}) ||
		record.Lifecycle == 0 || !record.Lifecycle.Valid() || record.Roles == 0 || record.Roles&^validNodeRoles != 0 ||
		record.Revision == 0 || record.CatalogGeneration == 0 ||
		len(record.FailureDomain) == 0 || len(record.FailureDomain) > MaxScalingStringBytes {
		return false
	}
	for _, value := range []string{
		string(record.DataEndpoint), string(record.NativeEndpoint), string(record.ControlEndpoint),
		string(record.GatewayEndpoint), record.DataAddress, record.NativeAddress,
		record.ControlAddress, record.GatewayAddress,
	} {
		if len(value) > MaxScalingStringBytes {
			return false
		}
	}
	if record.DataEndpoint == "" || record.NativeEndpoint == "" || record.ControlEndpoint == "" ||
		record.DataAddress == "" || record.NativeAddress == "" || record.ControlAddress == "" ||
		record.DataEndpoint == record.NativeEndpoint || record.DataEndpoint == record.ControlEndpoint ||
		record.NativeEndpoint == record.ControlEndpoint || record.DataAddress == record.NativeAddress ||
		record.DataAddress == record.ControlAddress || record.NativeAddress == record.ControlAddress {
		return false
	}
	if record.Roles&NodeRoleGateway != 0 {
		if record.GatewayEndpoint == "" || record.GatewayAddress == "" ||
			record.GatewayEndpoint == record.DataEndpoint || record.GatewayEndpoint == record.NativeEndpoint ||
			record.GatewayEndpoint == record.ControlEndpoint || record.GatewayAddress == record.DataAddress ||
			record.GatewayAddress == record.NativeAddress || record.GatewayAddress == record.ControlAddress ||
			record.Gateway.NodeID != record.NodeID || record.Gateway.Incarnation != record.Incarnation ||
			record.Gateway.ServiceKeyDigest == (replication.Digest{}) || record.Gateway.ServiceID == ([16]byte{}) ||
			record.Gateway.SessionID == ([16]byte{}) || record.Gateway.SessionRevision == 0 ||
			record.Gateway.ParticipantDigest == (replication.Digest{}) {
			return false
		}
	} else if record.GatewayEndpoint != "" || record.GatewayAddress != "" ||
		record.Gateway != (GatewayIdentity{}) {
		return false
	}
	if record.Lifecycle == NodeDecommissioned {
		if record.RetirementScanDigest == (replication.Digest{}) || record.RetirementScanDirectoryRevision == 0 || record.RetirementScanCutRevision == 0 {
			return false
		}
	} else if record.RetirementScanDigest != (replication.Digest{}) || record.RetirementScanDirectoryRevision != 0 || record.RetirementScanCutRevision != 0 {
		return false
	}
	return true
}

// UsedExceedsCapacity is kept separate so admission code can return a useful
// blocker while the record validator remains a simple fail-closed predicate.
func (record NodeRecord) UsedExceedsCapacity() bool {
	for resource := range autosplit.ResourceCount {
		if record.Used[resource] > record.Capacity[resource] {
			return true
		}
	}
	return false
}

func (record NodeRecord) PlacementEligible() bool {
	return record.Valid() && record.Lifecycle == NodeActive && record.Roles&NodeRoleStorage != 0 &&
		!record.UsedExceedsCapacity() && record.ActiveReceives <= record.MaxReceives &&
		record.MigrationUsed <= record.MigrationCapacity
}

func (record NodeRecord) CanReceive() bool {
	return record.PlacementEligible() && record.ActiveReceives < record.MaxReceives
}

// NodeReferenceEvidence is the complete reference scan used by safe-to-stop.
// Counts are authoritative only when the scan generation and directory
// revision still match the subsequent retirement CAS.
type NodeReferenceEvidence struct {
	NodeID                    rafttransport.NodeID
	Incarnation               uint64
	CatalogGeneration         uint64
	DirectoryRevision         uint64
	DirectoryCutRevision      uint64
	DirectoryCutDigest        replication.Digest
	CatalogHeadDigest         replication.Digest
	ScalingDirectoryDigest    replication.Digest
	EnrollmentDirectoryDigest replication.Digest
	OperationDirectoryDigest  replication.Digest
	GatewayDirectoryRevision  uint64
	GatewayDirectoryDigest    replication.Digest
	ServingReplicas           uint32
	LearnerReplicas           uint32
	EnrolledTargets           uint32
	OutstandingMoves          uint32
	CatalogVoterReferences    uint32
	ControlVoterReferences    uint32
	GatewayParticipantRefs    uint32
	RetirementDrainGeneration uint64
	Digest                    replication.Digest
}

// GatewayParticipantEvidence is supplied by the live gateway participant
// directory. A physical NodeRecord's Gateway role is a capability, not proof
// that a frontend session is still serving; the scanner must report the
// current session identity and its own revision before retirement can pass.
type GatewayParticipantEvidence struct {
	NodeID            rafttransport.NodeID
	Incarnation       uint64
	ServiceKeyDigest  replication.Digest
	ServiceID         [16]byte
	SessionID         [16]byte
	SessionRevision   uint64
	ParticipantDigest replication.Digest
	DirectoryRevision uint64
	Active            bool
	Digest            replication.Digest
}

func (evidence GatewayParticipantEvidence) ValidFor(record NodeRecord) bool {
	if record.NodeID != evidence.NodeID || record.Incarnation != evidence.Incarnation ||
		record.ServiceKeyDigest != evidence.ServiceKeyDigest ||
		record.Roles&NodeRoleGateway == 0 || record.Gateway.NodeID != evidence.NodeID ||
		record.Gateway.Incarnation != evidence.Incarnation || record.Gateway.ServiceKeyDigest != evidence.ServiceKeyDigest ||
		record.Gateway.ServiceID != evidence.ServiceID || record.Gateway.SessionID != evidence.SessionID ||
		record.Gateway.SessionRevision != evidence.SessionRevision || record.Gateway.ParticipantDigest != evidence.ParticipantDigest ||
		evidence.ParticipantDigest == (replication.Digest{}) || evidence.DirectoryRevision == 0 ||
		evidence.Digest == (replication.Digest{}) {
		return false
	}
	return true
}

// NodeDirectoryCut is the complete physical-node directory plus the global
// CAS revision for that directory. The revision is distinct from any one
// NodeRecord revision and therefore remains a valid fence when an older node
// has a larger per-record revision than a newly joined node.
type NodeDirectoryCut struct {
	Revision          uint64
	Digest            replication.Digest
	CatalogGeneration uint64
	Nodes             []NodeRecord
}

func (cut NodeDirectoryCut) Valid() bool {
	if cut.Revision == 0 || cut.Digest == (replication.Digest{}) ||
		cut.CatalogGeneration == 0 || len(cut.Nodes) == 0 || len(cut.Nodes) > MaxScalingNodes {
		return false
	}
	for index, node := range cut.Nodes {
		if !node.Valid() || node.CatalogGeneration > cut.CatalogGeneration ||
			index > 0 && bytesCompareNodeIdentity(cut.Nodes[index-1], node) >= 0 {
			return false
		}
	}
	return true
}

func bytesCompareNodeID(left, right rafttransport.NodeID) int {
	for index := range left {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	return 0
}

func bytesCompareNodeIdentity(left, right NodeRecord) int {
	if compared := bytesCompareNodeID(left.NodeID, right.NodeID); compared != 0 {
		return compared
	}
	if left.Incarnation < right.Incarnation {
		return -1
	}
	if left.Incarnation > right.Incarnation {
		return 1
	}
	return 0
}

// CurrentNodes projects a complete cut onto the newest incarnation of each
// physical NodeID. Historical decommissioned incarnations remain in Nodes for
// retirement scans, while runtime participant directories use this projection.
func (cut NodeDirectoryCut) CurrentNodes() []NodeRecord {
	if len(cut.Nodes) == 0 {
		return nil
	}
	latest := make(map[rafttransport.NodeID]NodeRecord, len(cut.Nodes))
	for _, node := range cut.Nodes {
		prior, found := latest[node.NodeID]
		if !found || node.Incarnation > prior.Incarnation {
			latest[node.NodeID] = node
		}
	}
	result := make([]NodeRecord, 0, len(latest))
	for _, node := range latest {
		// A decommissioned incarnation remains in the durable cut for
		// reference scans and historical drain addresses, but it must not
		// re-enter the live participant directory after retirement.
		if node.Lifecycle == NodeDecommissioned {
			continue
		}
		result = append(result, node)
	}
	slices.SortFunc(result, func(left, right NodeRecord) int {
		return bytesCompareNodeID(left.NodeID, right.NodeID)
	})
	return result
}

func (e NodeReferenceEvidence) ZeroDataReferences() bool {
	return e.ServingReplicas == 0 && e.LearnerReplicas == 0 && e.EnrolledTargets == 0 &&
		e.OutstandingMoves == 0
}

func (e NodeReferenceEvidence) ZeroAllReferences() bool {
	return e.ZeroDataReferences() && e.CatalogVoterReferences == 0 &&
		e.ControlVoterReferences == 0 && e.GatewayParticipantRefs == 0
}

// ReplicaIdentity is the exact per-group endpoint identity.  StoreID here is
// the raftstore/group identity; it is never a substitute for NodeRecord's
// physical node identity.
type ReplicaIdentity struct {
	Member          uint64
	Node            rafttransport.NodeID
	NodeIncarnation uint64
	StoreID         [16]byte
	Endpoint        distribution.EndpointID
	NativeEndpoint  distribution.EndpointID
	ControlEndpoint distribution.EndpointID
}

func (identity ReplicaIdentity) Valid() bool {
	for _, endpoint := range []string{string(identity.Endpoint), string(identity.NativeEndpoint), string(identity.ControlEndpoint)} {
		if len(endpoint) == 0 || len(endpoint) > MaxScalingStringBytes {
			return false
		}
	}
	return identity.Member != 0 && identity.Node != (rafttransport.NodeID{}) &&
		identity.NodeIncarnation != 0 && identity.StoreID != ([16]byte{}) &&
		identity.Endpoint != identity.NativeEndpoint && identity.Endpoint != identity.ControlEndpoint &&
		identity.NativeEndpoint != identity.ControlEndpoint
}

// EnrollmentState is group-scoped.  Reserved is durable admission before a
// provisioner performs side effects; Prepared certifies a target artifact;
// Enrolled certifies the membership grant; Moving means the Raft saga is
// journaled; Complete means the source and all transient references are gone.
type EnrollmentState uint8

const (
	EnrollmentReserved EnrollmentState = iota + 1
	EnrollmentPrepared
	EnrollmentEnrolled
	EnrollmentMoving
	EnrollmentComplete
	// EnrollmentCancelled is a terminal tombstone for a reservation that
	// never crossed the Prepared side-effect boundary. It is deliberately not
	// a generic abort for an enrolled or moving replica.
	EnrollmentCancelled
)

func (state EnrollmentState) Valid() bool {
	return state >= EnrollmentReserved && state <= EnrollmentCancelled
}

func (state EnrollmentState) Allows(next EnrollmentState) bool {
	if !state.Valid() || !next.Valid() {
		return false
	}
	if state == next {
		return true
	}
	if next == EnrollmentCancelled {
		// Cancellation is safe only while the row is still a pure reservation;
		// Prepared may already have created an authenticated local artifact.
		return state == EnrollmentReserved
	}
	return next == state+1
}

// PreparedReplicaProof is produced by an authenticated nodecontrol
// provisioner. It binds the target member/store and the exact command
// contract used by the eventual membership grant. Preparation happens before
// the target is a Raft learner, so AppliedIndex and ReplicaSetVersion may
// both be zero for the pre-membership phase. Once either is populated, the
// installed-proof fence below remains mandatory.
type PreparedReplicaProof struct {
	IntentID                   [32]byte
	Group                      raftmember.GroupKey
	Distribution               distribution.DistributionName
	Shard                      distribution.ShardID
	ReplicaOrdinal             uint8
	TargetMember               uint64
	TargetNode                 rafttransport.NodeID
	TargetNodeIncarnation      uint64
	TargetStoreID              [16]byte
	TargetEndpoint             distribution.EndpointID
	TargetNativeEndpoint       distribution.EndpointID
	TargetControlEndpoint      distribution.EndpointID
	ExpectedRosterDigest       replication.Digest
	ExpectedDescriptorDigest   replication.Digest
	ExpectedManifestDigest     replication.Digest
	RelationManifestDigest     replication.Digest
	DescriptorDigest           replication.Digest
	ManifestDigest             replication.Digest
	EnrollmentDigest           replication.Digest
	Command                    raftservice.CommandFence
	AllocationGeneration       distribution.ShardAllocationGeneration
	CatalogGeneration          uint64
	AppliedIndex               uint64
	ReplicaSetVersion          uint64
	CertifiedDirectoryRevision uint64
}

func (proof PreparedReplicaProof) Valid() bool {
	return proof.IntentID != ([32]byte{}) && !invalidGroupKey(proof.Group) &&
		proof.Distribution != "" && proof.Shard != "" && proof.TargetMember != 0 &&
		proof.TargetNode != (rafttransport.NodeID{}) && proof.TargetNodeIncarnation != 0 &&
		proof.TargetStoreID != ([16]byte{}) && proof.TargetEndpoint != "" &&
		proof.TargetNativeEndpoint != "" && proof.TargetControlEndpoint != "" &&
		proof.TargetEndpoint != proof.TargetNativeEndpoint &&
		proof.TargetEndpoint != proof.TargetControlEndpoint &&
		proof.TargetNativeEndpoint != proof.TargetControlEndpoint &&
		proof.ExpectedRosterDigest != (replication.Digest{}) &&
		proof.ExpectedDescriptorDigest != (replication.Digest{}) &&
		proof.ExpectedManifestDigest != (replication.Digest{}) &&
		proof.RelationManifestDigest != (replication.Digest{}) &&
		proof.DescriptorDigest != (replication.Digest{}) && proof.ManifestDigest != (replication.Digest{}) &&
		proof.EnrollmentDigest != (replication.Digest{}) && proof.Command.Valid() &&
		proof.AllocationGeneration != 0 && proof.CatalogGeneration != 0 &&
		((proof.AppliedIndex == 0 && proof.ReplicaSetVersion == 0) ||
			(proof.AppliedIndex != 0 && proof.ReplicaSetVersion != 0 &&
				proof.ReplicaSetVersion == proof.Command.ReplicaSetVersion)) &&
		proof.ExpectedDescriptorDigest == proof.DescriptorDigest &&
		proof.ExpectedManifestDigest == proof.ManifestDigest &&
		proof.CertifiedDirectoryRevision != 0 && proof.EnrollmentDigest == proof.ComputedEnrollmentDigest()
}

// InstallationFenceValid distinguishes pre-membership preparation from the
// later learner-installation observation. A prepared target has no applied
// index yet; once either observation field is present, both fields and the
// exact command replica-set version are required.
func (proof PreparedReplicaProof) InstallationFenceValid() bool {
	return (proof.AppliedIndex == 0 && proof.ReplicaSetVersion == 0) ||
		(proof.AppliedIndex != 0 && proof.ReplicaSetVersion != 0 &&
			proof.ReplicaSetVersion == proof.Command.ReplicaSetVersion)
}

// CertifiedEnrollmentReceipt records the catalog publication that made a
// prepared target an enrolled member. It is intentionally separate from the
// immutable Reserved intent: publishing the target advances the catalog
// generation, so later grant and move commands must use the enrolled cut.
type CertifiedEnrollmentReceipt struct {
	IntentID                         [32]byte
	IntentDigest                     replication.Digest
	BaseCatalogGeneration            uint64
	BaseCatalogHeadDigest            replication.Digest
	BaseDescriptorDigest             replication.Digest
	PublicationPredecessorGeneration uint64
	PublicationPredecessorHeadDigest replication.Digest
	EnrolledCatalogGeneration        uint64
	EnrolledCatalogHeadDigest        replication.Digest
	EnrolledDescriptorDigest         replication.Digest
	Target                           ReplicaIdentity
	InitialReplicaSetVersion         uint64
	GrantDigest                      replication.Digest
	TransitionID                     [32]byte
}

func (receipt CertifiedEnrollmentReceipt) Valid() bool {
	return receipt.IntentID != ([32]byte{}) &&
		receipt.IntentDigest != (replication.Digest{}) &&
		receipt.BaseCatalogGeneration != 0 &&
		receipt.BaseCatalogHeadDigest != (replication.Digest{}) &&
		receipt.BaseDescriptorDigest != (replication.Digest{}) &&
		receipt.PublicationPredecessorGeneration != 0 &&
		receipt.PublicationPredecessorGeneration >= receipt.BaseCatalogGeneration &&
		receipt.PublicationPredecessorHeadDigest != (replication.Digest{}) &&
		receipt.EnrolledCatalogGeneration > receipt.BaseCatalogGeneration &&
		receipt.EnrolledCatalogGeneration == receipt.PublicationPredecessorGeneration+1 &&
		receipt.EnrolledCatalogHeadDigest != (replication.Digest{}) &&
		receipt.EnrolledDescriptorDigest != (replication.Digest{}) &&
		receipt.Target.Valid() && receipt.InitialReplicaSetVersion != 0 &&
		receipt.GrantDigest != (replication.Digest{}) && receipt.TransitionID != ([32]byte{})
}

// ComputedEnrollmentDigest is the immutable proof digest consumed by the
// membership grant boundary.  It is calculated over the complete proof with
// the digest field cleared, so an arbitrary nonzero witness cannot pass.
func (proof PreparedReplicaProof) ComputedEnrollmentDigest() replication.Digest {
	copyOfProof := proof
	copyOfProof.EnrollmentDigest = replication.Digest{}
	raw, err := vibejson.Marshal(&copyOfProof)
	if err != nil {
		return replication.Digest{}
	}
	return sha256.Sum256(raw)
}

// GroupEnrollmentIntent is the single active transition for one group.  It
// carries both source and target identities so restarts cannot rediscover a
// different process behind a reused endpoint or member ID.
type GroupEnrollmentIntent struct {
	IntentID                  [32]byte
	Group                     raftmember.GroupKey
	Distribution              distribution.DistributionName
	Shard                     distribution.ShardID
	AllocationGeneration      distribution.ShardAllocationGeneration
	CatalogGeneration         uint64
	ExpectedCatalogHeadDigest replication.Digest
	ReplicaOrdinal            uint8
	Source                    ReplicaIdentity
	SnapshotSourceMember      uint64
	Target                    ReplicaIdentity
	ExpectedRosterDigest      replication.Digest
	ExpectedDescriptorDigest  replication.Digest
	ExpectedManifestDigest    replication.Digest
	ExpectedCommand           raftservice.CommandFence
	TargetNodeRevision        uint64
	// PreparationClaim is a durable side-effect fence.  A controller must
	// claim the Reserved row before invoking node-control PrepareReplica;
	// cancellation is rejected while this claim is present.
	PreparationClaim [32]byte
	State            EnrollmentState
	Revision         uint64
	Proof            *PreparedReplicaProof
	Receipt          *CertifiedEnrollmentReceipt
	MoveOperationID  [32]byte
}

func (intent GroupEnrollmentIntent) Valid() bool {
	if intent.IntentID == ([32]byte{}) || invalidGroupKey(intent.Group) || intent.Distribution == "" ||
		intent.Shard == "" || intent.AllocationGeneration == 0 || intent.CatalogGeneration == 0 ||
		intent.Source.Member == 0 || !intent.Source.Valid() || intent.SnapshotSourceMember == 0 ||
		intent.Target.Member == 0 || !intent.Target.Valid() || intent.Source.Member == intent.Target.Member ||
		intent.Source.Node == intent.Target.Node || intent.ReplicaOrdinal >= ServingReplicaCount ||
		intent.ExpectedRosterDigest == (replication.Digest{}) ||
		intent.ExpectedDescriptorDigest == (replication.Digest{}) ||
		intent.ExpectedManifestDigest == (replication.Digest{}) || !intent.ExpectedCommand.Valid() ||
		intent.TargetNodeRevision == 0 || !intent.State.Valid() || intent.Revision == 0 {
		return false
	}
	if intent.PreparationClaim != ([32]byte{}) &&
		(intent.State != EnrollmentReserved || intent.PreparationClaim != EnrollmentPreparationClaim(intent)) {
		return false
	}
	if intent.Proof != nil && (!intent.Proof.Valid() || intent.Proof.IntentID != intent.IntentID ||
		intent.Proof.Group != intent.Group || intent.Proof.Distribution != intent.Distribution ||
		intent.Proof.Shard != intent.Shard || intent.Proof.ReplicaOrdinal != intent.ReplicaOrdinal ||
		intent.Proof.AllocationGeneration != intent.AllocationGeneration ||
		intent.Proof.CatalogGeneration != intent.CatalogGeneration ||
		intent.Proof.TargetMember != intent.Target.Member ||
		intent.Proof.TargetNode != intent.Target.Node ||
		intent.Proof.TargetNodeIncarnation != intent.Target.NodeIncarnation ||
		intent.Proof.TargetStoreID != intent.Target.StoreID ||
		intent.Proof.TargetEndpoint != intent.Target.Endpoint ||
		intent.Proof.TargetNativeEndpoint != intent.Target.NativeEndpoint ||
		intent.Proof.TargetControlEndpoint != intent.Target.ControlEndpoint ||
		intent.Proof.ExpectedRosterDigest != intent.ExpectedRosterDigest ||
		intent.Proof.ExpectedDescriptorDigest != intent.ExpectedDescriptorDigest ||
		intent.Proof.ExpectedManifestDigest != intent.ExpectedManifestDigest ||
		intent.Proof.DescriptorDigest != intent.ExpectedDescriptorDigest ||
		intent.Proof.ManifestDigest != intent.ExpectedManifestDigest ||
		intent.Proof.RelationManifestDigest != intent.ExpectedCommand.RelationManifestDigest ||
		intent.Proof.Command != intent.ExpectedCommand ||
		intent.Proof.CertifiedDirectoryRevision != intent.TargetNodeRevision) {
		return false
	}
	if (intent.State >= EnrollmentPrepared && intent.State <= EnrollmentComplete) && intent.Proof == nil {
		return false
	}
	if intent.Receipt != nil && (!intent.Receipt.Valid() ||
		intent.Receipt.IntentID != intent.IntentID ||
		intent.Receipt.IntentDigest != intent.Digest() ||
		intent.Receipt.Target != intent.Target ||
		intent.Receipt.BaseCatalogGeneration != intent.CatalogGeneration ||
		intent.ExpectedCatalogHeadDigest == (replication.Digest{}) ||
		intent.Receipt.BaseCatalogHeadDigest != intent.ExpectedCatalogHeadDigest ||
		intent.CatalogGeneration == ^uint64(0) ||
		intent.Receipt.EnrolledCatalogGeneration <= intent.CatalogGeneration ||
		intent.Receipt.BaseDescriptorDigest != intent.ExpectedDescriptorDigest ||
		intent.Receipt.InitialReplicaSetVersion != intent.ExpectedCommand.ReplicaSetVersion ||
		intent.Receipt.TransitionID != EnrollmentTransitionDigest(intent)) {
		return false
	}
	if intent.State < EnrollmentEnrolled && intent.Receipt != nil {
		return false
	}
	if intent.State == EnrollmentCancelled && (intent.Proof != nil || intent.Receipt != nil || intent.MoveOperationID != ([32]byte{})) {
		return false
	}
	if intent.State >= EnrollmentEnrolled && intent.State <= EnrollmentComplete && intent.Receipt == nil {
		return false
	}
	if intent.State >= EnrollmentMoving && intent.State <= EnrollmentComplete && intent.MoveOperationID == ([32]byte{}) {
		return false
	}
	return true
}

// Digest returns the immutable enrollment tuple digest.  State, record
// revision, proof, and move operation are deliberately excluded because they
// advance during the one durable transition.  Every other field is frozen
// across CAS, including both source and target identities and every catalog
// fence.
func (intent GroupEnrollmentIntent) Digest() replication.Digest {
	copyOfIntent := intent
	copyOfIntent.State = EnrollmentReserved
	copyOfIntent.Revision = 0
	copyOfIntent.Proof = nil
	copyOfIntent.Receipt = nil
	copyOfIntent.MoveOperationID = [32]byte{}
	copyOfIntent.PreparationClaim = [32]byte{}
	raw, err := vibejson.Marshal(&copyOfIntent)
	if err != nil {
		return replication.Digest{}
	}
	return sha256.Sum256(raw)
}

// EnrollmentPreparationClaim is deterministic for the immutable intent and
// therefore survives controller restart.  The row CAS, rather than this
// digest, chooses the sole claimant.
func EnrollmentPreparationClaim(intent GroupEnrollmentIntent) [32]byte {
	var claim [32]byte
	if intent.IntentID == ([32]byte{}) {
		return claim
	}
	raw := append([]byte("vibedb/enrollment-preparation/1\x00"), intent.IntentID[:]...)
	return sha256.Sum256(raw)
}

// ScalingKind identifies one operator intent.  ScaleIn and Decommission are
// separate so a data evacuation can finish without claiming physical-node
// retirement.
type ScalingKind uint8

const (
	ScalingScaleIn ScalingKind = iota + 1
	ScalingScaleOut
	ScalingRebalance
	ScalingDecommission
)

func (kind ScalingKind) Valid() bool { return kind >= ScalingScaleIn && kind <= ScalingDecommission }

type NodeReference struct {
	NodeID      rafttransport.NodeID
	Incarnation uint64
}

func (reference NodeReference) Valid() bool {
	return reference.NodeID != (rafttransport.NodeID{}) && reference.Incarnation != 0
}

// ScalingIntentRequest is the immutable operator input from which a stable
// intent ID is derived.  RequestID is supplied by the authenticated caller;
// it is never generated from wall-clock time by the server.
type ScalingIntentRequest struct {
	Kind              ScalingKind
	RequestID         [32]byte
	Drain             NodeReference
	Targets           []NodeReference
	DesiredNodeCount  uint16
	MaxMoves          uint16
	MaxMigrationBytes uint64
	HysteresisPPM     uint64
}

func (request ScalingIntentRequest) Valid() bool {
	if !request.Kind.Valid() || request.RequestID == ([32]byte{}) ||
		(request.Kind == ScalingScaleIn || request.Kind == ScalingDecommission) && !request.Drain.Valid() ||
		len(request.Targets) > MaxScalingTargetsPerIntent || request.MaxMoves == 0 ||
		request.HysteresisPPM > 1_000_000 {
		return false
	}
	if request.Kind == ScalingScaleOut && request.DesiredNodeCount == 0 && len(request.Targets) == 0 {
		return false
	}
	for index, target := range request.Targets {
		if !target.Valid() || (request.Drain.Valid() && target.NodeID == request.Drain.NodeID &&
			target.Incarnation == request.Drain.Incarnation) || index > 0 &&
			bytesCompareNode(target.NodeID, request.Targets[index-1].NodeID) <= 0 {
			return false
		}
	}
	return true
}

func (request ScalingIntentRequest) ID() [32]byte {
	var digest [32]byte
	if !request.Valid() {
		return digest
	}
	hash := sha256.New()
	hash.Write([]byte("vibedb/scaling-intent/v1\x00"))
	hash.Write(request.RequestID[:])
	hash.Write([]byte{byte(request.Kind)})
	appendNodeReference(hash, request.Drain)
	var scalar [16]byte
	binary.LittleEndian.PutUint16(scalar[:2], request.DesiredNodeCount)
	binary.LittleEndian.PutUint16(scalar[2:4], request.MaxMoves)
	binary.LittleEndian.PutUint64(scalar[4:], request.MaxMigrationBytes)
	hash.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:8], request.HysteresisPPM)
	hash.Write(scalar[:8])
	for _, target := range request.Targets {
		appendNodeReference(hash, target)
	}
	copy(digest[:], hash.Sum(nil))
	return digest
}

func appendNodeReference(hash interface{ Write([]byte) (int, error) }, reference NodeReference) {
	hash.Write(reference.NodeID[:])
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], reference.Incarnation)
	hash.Write(scalar[:])
}

func bytesCompareNode(left, right rafttransport.NodeID) int {
	for index := range left {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	return 0
}

// ScalingIntent is the durable bounded controller state.  Blockers and the
// evidence are retained alongside progress, so a status read never has to
// infer safety from a missing process-local queue.
type ScalingIntent struct {
	ID                [32]byte
	Request           ScalingIntentRequest
	CatalogGeneration uint64
	Revision          uint64
	DirectoryRevision uint64
	State             ScalingIntentState
	PlannedReplicas   uint32
	CompletedReplicas uint32
	OutstandingMoves  [][32]byte
	Blockers          []ScalingBlocker
	Evidence          SafeToStopEvidence
}

type ScalingIntentState uint8

const (
	ScalingReserved ScalingIntentState = iota + 1
	ScalingRunning
	ScalingComplete
	ScalingCancelled
)

func (state ScalingIntentState) Valid() bool {
	return state >= ScalingReserved && state <= ScalingCancelled
}

func (state ScalingIntentState) Allows(next ScalingIntentState) bool {
	if !state.Valid() || !next.Valid() {
		return false
	}
	if state == next {
		return true
	}
	switch state {
	case ScalingReserved:
		return next == ScalingRunning || next == ScalingCancelled
	case ScalingRunning:
		return next == ScalingComplete || next == ScalingCancelled
	default:
		return false
	}
}

// ValidateEnrollmentReceipt binds the two head digests observed by the
// catalog publisher to the immutable intent tuple. The intent intentionally
// stores the base descriptor and generation; the publisher supplies the
// linearizable head bytes it actually read so an arbitrary nonzero receipt
// digest cannot authorize a target publication.
func ValidateEnrollmentReceipt(intent GroupEnrollmentIntent, receipt CertifiedEnrollmentReceipt, baseHeadDigest, enrolledHeadDigest replication.Digest) bool {
	if !intent.Valid() || intent.State > EnrollmentPrepared ||
		baseHeadDigest == (replication.Digest{}) || enrolledHeadDigest == (replication.Digest{}) ||
		intent.ExpectedCatalogHeadDigest == (replication.Digest{}) ||
		receipt.BaseCatalogHeadDigest != intent.ExpectedCatalogHeadDigest ||
		receipt.PublicationPredecessorHeadDigest != baseHeadDigest ||
		receipt.EnrolledCatalogHeadDigest != enrolledHeadDigest {
		return false
	}
	copyOfIntent := intent
	copyOfIntent.State = EnrollmentEnrolled
	copyOfIntent.Revision++
	copyOfIntent.Receipt = &receipt
	return copyOfIntent.Valid()
}

type ScalingBlocker struct {
	Code           string
	Detail         string
	Group          raftmember.GroupKey
	Shard          distribution.ShardID
	ReplicaOrdinal uint8
	Node           rafttransport.NodeID
	Revision       uint64
}

func (blocker ScalingBlocker) Valid() bool {
	return len(blocker.Code) > 0 && len(blocker.Code) <= 128 && len(blocker.Detail) <= MaxScalingStringBytes &&
		(blocker.Node == (rafttransport.NodeID{}) || blocker.Revision != 0)
}

// SafeToStopEvidence is a proof summary, not an operator assertion.  The
// controller sets it only after a complete catalog/control directory scan and
// a drain acknowledgement at the same or newer generation.
type SafeToStopEvidence struct {
	NodeID                    rafttransport.NodeID
	NodeIncarnation           uint64
	ScanCatalogGeneration     uint64
	ScanDirectoryRevision     uint64
	ScanDirectoryDigest       replication.Digest
	CatalogHeadDigest         replication.Digest
	ScalingDirectoryDigest    replication.Digest
	EnrollmentDirectoryDigest replication.Digest
	OperationDirectoryDigest  replication.Digest
	GatewayDirectoryRevision  uint64
	GatewayDirectoryDigest    replication.Digest
	ServingReplicas           uint32
	LearnerReplicas           uint32
	EnrolledTargets           uint32
	OutstandingMoves          uint32
	CatalogVoters             uint32
	ControlVoters             uint32
	GatewayParticipants       uint32
	DrainAcknowledged         bool
	RetiredAcknowledged       bool
	CatalogControlMigrated    bool
	Digest                    replication.Digest
}

func (evidence SafeToStopEvidence) SafeForDataEvacuation() bool {
	return evidence.NodeID != (rafttransport.NodeID{}) && evidence.NodeIncarnation != 0 &&
		evidence.ScanCatalogGeneration != 0 && evidence.ScanDirectoryRevision != 0 &&
		evidence.ScanDirectoryDigest != (replication.Digest{}) && evidence.CatalogHeadDigest != (replication.Digest{}) &&
		evidence.ServingReplicas == 0 && evidence.LearnerReplicas == 0 &&
		evidence.EnrolledTargets == 0 && evidence.OutstandingMoves == 0 && evidence.DrainAcknowledged &&
		evidence.Digest != (replication.Digest{})
}

func (evidence SafeToStopEvidence) SafeToStop() bool {
	return evidence.SafeForDataEvacuation() && evidence.CatalogVoters == 0 &&
		evidence.ControlVoters == 0 && evidence.GatewayParticipants == 0 &&
		evidence.RetiredAcknowledged && evidence.CatalogControlMigrated
}

// MatchesReference binds an operator-visible completion claim to the exact
// authoritative scan performed by the replicated authority. A nonzero digest
// alone is never sufficient evidence.
func (evidence SafeToStopEvidence) MatchesReference(reference NodeReferenceEvidence) bool {
	return evidence.NodeID == reference.NodeID && evidence.NodeIncarnation == reference.Incarnation &&
		evidence.ScanCatalogGeneration == reference.CatalogGeneration &&
		evidence.ScanDirectoryRevision == reference.DirectoryCutRevision &&
		evidence.ScanDirectoryDigest == reference.DirectoryCutDigest &&
		evidence.CatalogHeadDigest == reference.CatalogHeadDigest &&
		evidence.ScalingDirectoryDigest == reference.ScalingDirectoryDigest &&
		evidence.EnrollmentDirectoryDigest == reference.EnrollmentDirectoryDigest &&
		evidence.OperationDirectoryDigest == reference.OperationDirectoryDigest &&
		evidence.GatewayDirectoryRevision == reference.GatewayDirectoryRevision &&
		evidence.GatewayDirectoryDigest == reference.GatewayDirectoryDigest &&
		evidence.ServingReplicas == reference.ServingReplicas &&
		evidence.LearnerReplicas == reference.LearnerReplicas &&
		evidence.EnrolledTargets == reference.EnrolledTargets &&
		evidence.OutstandingMoves == reference.OutstandingMoves &&
		evidence.CatalogVoters == reference.CatalogVoterReferences &&
		evidence.ControlVoters == reference.ControlVoterReferences &&
		evidence.GatewayParticipants == reference.GatewayParticipantRefs &&
		evidence.Digest == reference.Digest
}

// SafeToStopEvidenceFromReference copies an authority scan into the durable
// operator status shape. The caller must still set DrainAcknowledged and the
// retirement flags from their corresponding authenticated lifecycle steps.
func SafeToStopEvidenceFromReference(reference NodeReferenceEvidence) SafeToStopEvidence {
	return SafeToStopEvidence{
		NodeID: reference.NodeID, NodeIncarnation: reference.Incarnation,
		ScanCatalogGeneration: reference.CatalogGeneration, ScanDirectoryRevision: reference.DirectoryCutRevision,
		ScanDirectoryDigest: reference.DirectoryCutDigest, CatalogHeadDigest: reference.CatalogHeadDigest,
		ScalingDirectoryDigest: reference.ScalingDirectoryDigest, EnrollmentDirectoryDigest: reference.EnrollmentDirectoryDigest,
		OperationDirectoryDigest: reference.OperationDirectoryDigest, GatewayDirectoryRevision: reference.GatewayDirectoryRevision,
		GatewayDirectoryDigest: reference.GatewayDirectoryDigest, ServingReplicas: reference.ServingReplicas,
		LearnerReplicas: reference.LearnerReplicas, EnrolledTargets: reference.EnrolledTargets,
		OutstandingMoves: reference.OutstandingMoves, CatalogVoters: reference.CatalogVoterReferences,
		ControlVoters: reference.ControlVoterReferences, GatewayParticipants: reference.GatewayParticipantRefs,
		Digest: reference.Digest,
	}
}

func (intent ScalingIntent) Valid() bool {
	if intent.ID == ([32]byte{}) || !intent.Request.Valid() || intent.ID != intent.Request.ID() ||
		intent.CatalogGeneration == 0 || intent.Revision == 0 || intent.DirectoryRevision == 0 ||
		intent.DirectoryRevision != intent.Revision || !intent.State.Valid() ||
		len(intent.OutstandingMoves) > MaxScalingMovesPerIntent || len(intent.Blockers) > MaxScalingBlockers ||
		intent.CompletedReplicas > intent.PlannedReplicas {
		return false
	}
	for _, move := range intent.OutstandingMoves {
		if move == ([32]byte{}) {
			return false
		}
	}
	for _, blocker := range intent.Blockers {
		if !blocker.Valid() {
			return false
		}
	}
	return true
}

func invalidGroupKey(group raftmember.GroupKey) bool {
	return group.ClusterID == ([16]byte{}) || group.ClusterIncarnation == ([16]byte{}) ||
		group.TopologyRecoveryEpoch == 0 || group.ShardIncarnation == ([16]byte{}) ||
		group.GroupID == ([16]byte{})
}

// DirectoryReader is the only read authority consumed by scaling.  Concrete
// catalog implementations must return a complete bounded cut for each call.
type DirectoryReader interface {
	ListNodes(context.Context) ([]NodeRecord, error)
	ReadNode(context.Context, rafttransport.NodeID, uint64) (NodeRecord, error)
	ListScalingIntents(context.Context) ([]ScalingIntent, error)
	ReadScalingIntent(context.Context, [32]byte) (ScalingIntent, error)
	ReadScalingIntentAt(context.Context, [32]byte, uint64, replication.Digest) (ScalingIntent, error)
	ListEnrollmentIntents(context.Context, raftmember.GroupKey) ([]GroupEnrollmentIntent, error)
	ReadEnrollmentIntent(context.Context, [32]byte) (GroupEnrollmentIntent, error)
	ReadEnrollmentIntentAt(context.Context, [32]byte, uint64, replication.Digest) (GroupEnrollmentIntent, error)
	ScanNodeReferences(context.Context, rafttransport.NodeID, uint64) (NodeReferenceEvidence, error)
}

// NodeDirectoryCutReader is implemented by the replicated authority when a
// consumer needs the global directory CAS revision rather than the maximum
// per-record revision. It is optional so existing read-only adapters can
// continue serving group placement while the physical directory is upgraded.
type NodeDirectoryCutReader interface {
	ReadNodeDirectoryCut(context.Context) (NodeDirectoryCut, error)
}

// GatewayParticipantScanner is an optional authoritative live-session reader.
// A nil scanner deliberately leaves gateway nodes blocked from retirement;
// callers may never infer that the gateway has drained from capacity or role
// bits alone.
type GatewayParticipantScanner interface {
	ScanGatewayParticipant(context.Context, NodeRecord) (GatewayParticipantEvidence, error)
}

// DirectoryWriter performs all transitions through replicated CAS.  An
// expected revision of zero means create-only; nonzero means replace exactly
// that prior record.  Implementations must reject a target that changed from
// Active at the same transaction as enrollment reservation.
type DirectoryWriter interface {
	PutNode(context.Context, NodeRecord, uint64) error
	PutScalingIntent(context.Context, ScalingIntent, uint64) error
	PutEnrollmentIntent(context.Context, GroupEnrollmentIntent, uint64) error
}

// EnrollmentPreparationClaimer is the side-effect admission fence used by
// restartable controllers.  Implementations atomically mark a Reserved row
// before any node-control request is sent; cancellation cannot race that mark.
type EnrollmentPreparationClaimer interface {
	ClaimEnrollmentPreparation(context.Context, [32]byte, uint64) (GroupEnrollmentIntent, error)
}

// EnrollmentReceiptPublisher is the catalog side of the Reserved -> Prepared
// -> Enrolled boundary.  Implementations must publish the target descriptor
// and return the same intent with one immutable CertifiedEnrollmentReceipt;
// the receipt binds the base catalog cut to the enrolled G+1 cut.  A caller
// must never synthesize an Enrolled row when this interface is unavailable.
type EnrollmentReceiptPublisher interface {
	PublishEnrollmentReceipt(context.Context, GroupEnrollmentIntent) (GroupEnrollmentIntent, error)
}

// NodeProvisioner is implemented by nodecontrol.  It receives only the
// catalog-derived intent and must return a proof with matching identities and
// digests; caller-provided endpoint descriptors are never authority.
type NodeProvisioner interface {
	PrepareReplica(context.Context, GroupEnrollmentIntent) (PreparedReplicaProof, error)
	EnrollReplica(context.Context, GroupEnrollmentIntent, PreparedReplicaProof) error
	AbortPreparedReplica(context.Context, GroupEnrollmentIntent) error
	VerifyReplica(context.Context, GroupEnrollmentIntent) (PreparedReplicaProof, error)
}

func validateNodeTransition(previous, next NodeRecord) error {
	if !previous.Valid() || !next.Valid() || previous.NodeID != next.NodeID ||
		previous.Incarnation != next.Incarnation || next.Revision != previous.Revision+1 ||
		!previous.Lifecycle.Allows(next.Lifecycle) {
		return ErrScalingState
	}
	if !sameNodeIdentity(previous, next) {
		return ErrScalingIdentity
	}
	return nil
}

func sameNodeIdentity(left, right NodeRecord) bool {
	return left.NodeID == right.NodeID && left.Incarnation == right.Incarnation &&
		left.ServiceKeyDigest == right.ServiceKeyDigest &&
		left.DataEndpoint == right.DataEndpoint && left.NativeEndpoint == right.NativeEndpoint &&
		left.ControlEndpoint == right.ControlEndpoint && left.GatewayEndpoint == right.GatewayEndpoint &&
		left.DataAddress == right.DataAddress && left.NativeAddress == right.NativeAddress &&
		left.ControlAddress == right.ControlAddress && left.GatewayAddress == right.GatewayAddress &&
		left.FailureDomain == right.FailureDomain && left.Roles == right.Roles && left.Gateway == right.Gateway
}

func cloneScalingIntent(intent ScalingIntent) ScalingIntent {
	intent.Request.Targets = slices.Clone(intent.Request.Targets)
	intent.OutstandingMoves = slices.Clone(intent.OutstandingMoves)
	intent.Blockers = slices.Clone(intent.Blockers)
	if intent.Evidence.Digest != (replication.Digest{}) {
		intent.Evidence.Digest = intent.Evidence.Digest
	}
	return intent
}

func metadataError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidScalingMetadata, fmt.Sprintf(format, args...))
}
