package gateway

import (
	"context"
	"crypto/sha256"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
)

// ReplicatedCatalogHeadDigest returns the digest of the exact control-plane
// head envelope that the catalog authority commits. It is distinct from the
// nested SnapshotDocument digest and is suitable for
// GroupEnrollmentIntent.ExpectedCatalogHeadDigest.
func ReplicatedCatalogHeadDigest(snapshot *Snapshot) (replication.Digest, error) {
	raw, err := appendReplicatedCatalogDocument(nil, snapshot, maxReplicatedCatalogBytes)
	if err != nil {
		return replication.Digest{}, err
	}
	return replication.Digest(sha256.Sum256(raw)), nil
}

// EnrollmentReceiptMatchesSnapshot verifies that the certified enrolled group
// is unchanged in an authoritative catalog snapshot. Unrelated publications
// may advance the global generation without invalidating the group's receipt.
func EnrollmentReceiptMatchesSnapshot(intent GroupEnrollmentIntent, snapshot *Snapshot) bool {
	return enrollmentReceiptMatchesCatalog(intent, replicatedCatalogCut{snapshot: snapshot})
}

// EnrollmentMoveMatchesSnapshot is kept as a fail-closed compatibility
// wrapper. A collected move cannot be proved from an enrollment row and a
// catalog snapshot alone: ReplicaSetVersion is an applied log index and may
// advance for unrelated entries. Callers must use
// EnrollmentMoveMatchesSnapshotWithReceipt with the retained, operation-bound
// transition receipt.
func EnrollmentMoveMatchesSnapshot(intent GroupEnrollmentIntent, snapshot *Snapshot) bool {
	return false
}

// EnrollmentMoveMatchesSnapshotWithReceipt verifies the post-remove catalog
// cut for an enrolled group whose move operation may already have been
// collected. The transition intent and receipt are both retained by the
// authority under the group-scoped move-rec record. Matching their operation,
// source, target, and enrolled source cut prevents a different move or a
// later replay from certifying completion.
func EnrollmentMoveMatchesSnapshotWithReceipt(
	intent GroupEnrollmentIntent, snapshot *Snapshot,
	transition GroupTransitionIntent, publication GroupPublicationReceipt,
) bool {
	if !intent.Valid() || intent.State != EnrollmentMoving ||
		intent.MoveOperationID == ([32]byte{}) || intent.Receipt == nil || snapshot == nil ||
		!transition.Valid() || !publication.Valid() || publication.Key != transition.Key ||
		publication.Phase < TransitionPhasePostRemove {
		return false
	}
	enrollment := *intent.Receipt
	if !enrollment.Valid() || enrollment.IntentID != intent.IntentID ||
		enrollment.IntentDigest != intent.Digest() || enrollment.Target != intent.Target ||
		enrollment.EnrolledCatalogGeneration > ^uint64(0)-2 ||
		snapshot.Generation() < enrollment.EnrolledCatalogGeneration+2 {
		return false
	}
	if transition.Key.OperationID != intent.MoveOperationID ||
		transition.Key.Distribution != intent.Distribution ||
		transition.Key.Shard != intent.Shard || transition.Key.Group != intent.Group ||
		transition.Key.SourceAllocationGeneration != uint64(intent.AllocationGeneration) ||
		transition.SourceMember != intent.Source.Member ||
		transition.TargetMember != intent.Target.Member ||
		transition.SourceHeadGeneration < enrollment.EnrolledCatalogGeneration ||
		transition.SourceDescriptor.Distribution != intent.Distribution ||
		transition.SourceDescriptor.Shard != intent.Shard ||
		transition.SourceDescriptor.Group != intent.Group ||
		transition.SourceDescriptor.AllocationGeneration != intent.AllocationGeneration ||
		transition.SourceDescriptor.Command != intent.ExpectedCommand ||
		transition.Key.SourceCommandFenceDigest != DigestCommandFence(intent.ExpectedCommand) ||
		transition.Replacement != enrollmentTargetDescriptor(intent.Target) ||
		transition.SourceDescriptor.EnrolledTarget == nil ||
		*transition.SourceDescriptor.EnrolledTarget != enrollmentTargetDescriptor(intent.Target) ||
		publication.Key.OperationID != intent.MoveOperationID ||
		publication.Key.Distribution != intent.Distribution ||
		publication.Key.Shard != intent.Shard || publication.Key.Group != intent.Group ||
		publication.CommittedDistributionVersion != transition.TargetDistributionVersion ||
		publication.CommittedHeadGeneration < transition.SourceHeadGeneration ||
		transition.SourceHeadGeneration > ^uint64(0)-2 ||
		publication.CommittedHeadGeneration < transition.SourceHeadGeneration+2 {
		return false
	}
	targetFound := false
	route, found := snapshot.ResolveReplicatedMembershipRoute(intent.Distribution, intent.Shard, nil)
	if !found || route.HasEnrolledTarget || route.Serving.Group != intent.Group ||
		route.Serving.Distribution != intent.Distribution || route.Serving.Shard != intent.Shard ||
		route.Serving.AllocationGeneration != uint64(intent.AllocationGeneration) {
		return false
	}
	descriptor, descriptorFound := enrollmentMoveDescriptor(snapshot, intent.Group)
	if !descriptorFound {
		return false
	}
	command := route.Serving.Command
	expected := intent.ExpectedCommand
	if command.ActivePolicyGeneration != expected.ActivePolicyGeneration ||
		command.ProtectionEpoch != expected.ProtectionEpoch ||
		command.SchemaGeneration != expected.SchemaGeneration ||
		command.RelationManifestDigest != expected.RelationManifestDigest ||
		DigestCommandFence(command) != publication.CommittedCommandFenceDigest ||
		DigestReplicatedShardDescriptor(descriptor) != publication.CommittedGroupDigest ||
		DigestReplicaRoster(descriptor.Replicas) != publication.CommittedRosterDigest ||
		DigestRouteFor(snapshot, intent.Distribution, intent.Shard) != publication.CommittedRouteDigest ||
		expected.OwnershipEpoch == ^uint64(0) || expected.RoutingVersion == ^uint64(0) ||
		expected.RouteGeneration == ^uint64(0) ||
		command.OwnershipEpoch != expected.OwnershipEpoch+1 ||
		command.RoutingVersion != expected.RoutingVersion+1 ||
		command.RouteGeneration != expected.RouteGeneration+1 {
		return false
	}
	manifest, manifestFound := snapshot.Manifest(intent.Distribution)
	if !manifestFound || manifest.Version() != publication.CommittedDistributionVersion {
		return false
	}
	for _, replica := range route.Serving.Replicas {
		if intent.Target.Member == replica.Member && intent.Target.Node == replica.Node &&
			intent.Target.NodeIncarnation == replica.NodeIncarnation && intent.Target.StoreID == replica.StoreID &&
			intent.Target.Endpoint == distribution.EndpointID(replica.Endpoint) &&
			intent.Target.NativeEndpoint == distribution.EndpointID(replica.NativeEndpoint) &&
			intent.Target.ControlEndpoint == distribution.EndpointID(replica.ControlEndpoint) {
			if targetFound {
				return false
			}
			targetFound = true
		}
		// Member, node, and store identities are independently unique in a
		// serving route. Reject every source identity, including a reused
		// member with a substituted physical incarnation.
		if replica.Member == intent.Source.Member || replica.Node == intent.Source.Node ||
			replica.StoreID == intent.Source.StoreID {
			return false
		}
	}
	for _, replica := range transition.SourceDescriptor.Replicas {
		if replica.Member == intent.Source.Member &&
			replica.Node == intent.Source.Node && replica.StoreID == intent.Source.StoreID &&
			replica.NodeIncarnation == intent.Source.NodeIncarnation &&
			replica.Endpoint == intent.Source.Endpoint &&
			replica.NativeEndpoint == intent.Source.NativeEndpoint &&
			replica.ControlEndpoint == intent.Source.ControlEndpoint {
			return targetFound
		}
	}
	return false
}

func enrollmentMoveDescriptor(snapshot *Snapshot, group raftmember.GroupKey) (ReplicatedShardDescriptor, bool) {
	if snapshot == nil {
		return ReplicatedShardDescriptor{}, false
	}
	for _, descriptor := range snapshot.ReplicatedShardDescriptors() {
		if descriptor.Group == group {
			return descriptor, true
		}
	}
	return ReplicatedShardDescriptor{}, false
}

func enrollmentTargetDescriptor(identity ReplicaIdentity) ReplicatedReplicaDescriptor {
	return ReplicatedReplicaDescriptor{Member: identity.Member, Node: identity.Node,
		StoreID: identity.StoreID, NodeIncarnation: identity.NodeIncarnation,
		Endpoint: identity.Endpoint, NativeEndpoint: identity.NativeEndpoint,
		ControlEndpoint: identity.ControlEndpoint}
}

// ReadReplicatedCatalogHead reads one authoritative snapshot and returns the
// digest of the same canonical head bytes. The digest is derived only after
// the authority has validated its authenticated head/witness cut; callers
// must not hash an arbitrary local catalog file as a control-plane witness.
func (authority *ReplicatedCatalogAuthority) ReadReplicatedCatalogHead(ctx context.Context) (*Snapshot, replication.Digest, error) {
	if authority == nil || ctx == nil {
		return nil, replication.Digest{}, ErrReplicatedCatalog
	}
	snapshot, err := authority.Read(ctx)
	if err != nil {
		return nil, replication.Digest{}, err
	}
	digest, err := ReplicatedCatalogHeadDigest(snapshot)
	if err != nil {
		return nil, replication.Digest{}, err
	}
	return snapshot, digest, nil
}

// EnrollmentCatalogCut fences enrollment reads against the two durable
// publications that can change their meaning. It contains no live process or
// drain acknowledgement: recovering a replica cannot depend on its own gateway.
type EnrollmentCatalogCut struct {
	CatalogGeneration         uint64
	CatalogHeadDigest         replication.Digest
	EnrollmentDirectoryDigest replication.Digest
}

// ReadEnrollmentCatalogCut is a bounded metadata read. Callers recheck this
// cut after reading child rows and independently fence the physical directory.
func (authority *ReplicatedCatalogAuthority) ReadEnrollmentCatalogCut(ctx context.Context) (EnrollmentCatalogCut, error) {
	snapshot, digest, err := authority.ReadReplicatedCatalogHead(ctx)
	if err != nil {
		return EnrollmentCatalogCut{}, err
	}
	directory, err := authority.readRaw(ctx, enrollmentDirectoryKey, maxEnrollmentDirectoryBytes)
	if err != nil {
		return EnrollmentCatalogCut{}, err
	}
	if !directory.Found {
		return EnrollmentCatalogCut{}, ErrEnrollmentIntentMissing
	}
	return EnrollmentCatalogCut{CatalogGeneration: snapshot.Generation(), CatalogHeadDigest: digest,
		EnrollmentDirectoryDigest: scalingDigest(directory.Value)}, nil
}

// ReplicatedInitialMembershipDigests returns the catalog-certified serving
// RF3 roster and complete descriptor witnesses for one group. A missing or
// malformed group returns ok=false; callers must never substitute a made-up
// nonzero digest.
func ReplicatedInitialMembershipDigests(
	snapshot *Snapshot, group raftmember.GroupKey,
) (roster, descriptor replication.Digest, ok bool) {
	if snapshot == nil || group == (raftmember.GroupKey{}) {
		return replication.Digest{}, replication.Digest{}, false
	}
	for index, entry := range snapshot.replicatedShards {
		if entry.group != group || int(entry.replicaCount) != ServingReplicaCount {
			continue
		}
		roster = replication.Digest(replicatedCatalogInitialRosterDigest(snapshot, index))
		descriptor = replication.Digest(replicatedCatalogInitialDescriptorDigest(snapshot, index))
		if roster == (replication.Digest{}) || descriptor == (replication.Digest{}) {
			return replication.Digest{}, replication.Digest{}, false
		}
		return roster, descriptor, true
	}
	return replication.Digest{}, replication.Digest{}, false
}
