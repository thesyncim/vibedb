package driver

import (
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"io"
)

// ReplicatedRelationManifestForBinding computes the exact state-machine schema
// digest from opened SQL/index definitions at an explicitly supplied binding.
// It does not acquire serving authority, prepare apply storage, or mutate rows.
func (d *Database) ReplicatedRelationManifestForBinding(identity ReplicatedShardStoreIdentity, placement ReplicatedPlacementProfile, binding ReplicatedShardStoreBinding) ([32]byte, error) {
	if d == nil || d.connector == nil {
		return [32]byte{}, ErrDatabaseClosed
	}
	connector := d.connector
	connector.mu.Lock()
	defer connector.mu.Unlock()
	if connector.closed || connector.db == nil {
		return [32]byte{}, ErrDatabaseClosed
	}
	core := connector.db
	core.mu.RLock()
	defer core.mu.RUnlock()
	if core.closed || core.catalog.ReplicatedShardStore == nil || !core.catalog.ReplicatedShardStore.Equal(identity) {
		return [32]byte{}, ErrReplicatedShardStoreIdentityMismatch
	}
	return replicatedRestoreManifestAt(core, identity, placement, binding)
}

func replicatedRestoreManifestAt(core *database, identity ReplicatedShardStoreIdentity, placement ReplicatedPlacementProfile, binding ReplicatedShardStoreBinding) ([32]byte, error) {
	projected := ownedReplicatedShardStoreIdentity(identity)
	projected.Binding = binding
	if projected.RelationSchemaGeneration != binding.Authority.SchemaGeneration {
		return [32]byte{}, ErrReplicatedRestoreStageProof
	}
	apply := ReplicatedApplyIdentity{ValidationProfile: uint8(replicatedstate.ValidationDeterministicMutation), ValidationDigest: replicatedApplyProfileDigest(projected, placement), Placement: placement}
	relations, err := replicatedApplyRelations(projected, apply, core, &ReplicatedApply{})
	if err != nil {
		return [32]byte{}, err
	}
	return replicatedstate.RelationCollectionManifestDigest(replicatedStateBindingAt(projected, placement.Range), relations)
}

func restoreSourceSchemaBinding(target ReplicatedShardStoreBinding, source replicatedstate.Binding) ReplicatedShardStoreBinding {
	target.ClusterID = [16]byte(source.ClusterID)
	target.ClusterIncarnation = [16]byte(source.ClusterIncarnation)
	target.TopologyRecoveryEpoch = source.TopologyRecoveryEpoch
	target.Distribution = source.Distribution
	target.Shard = source.Shard
	target.AllocationGeneration = source.AllocationGeneration
	target.ShardIncarnation = [16]byte(source.ShardIncarnation)
	target.GroupID = [16]byte(source.GroupID)
	target.Authority = ReplicatedAuthorityProfile{ActivePolicyGeneration: source.ActivePolicyGeneration, ProtectionEpoch: source.ProtectionEpoch, OwnershipEpoch: source.OwnershipEpoch, SchemaGeneration: source.SchemaGeneration, RoutingVersion: source.RoutingVersion, RouteGeneration: source.RouteGeneration}
	return target
}

// VerifyReplicatedRestoreImage authenticates source bytes and computes the
// unchanged relation image under the destination validation domain. This can
// verify sealed receipts without opening live SQL/WAL files.
func VerifyReplicatedRestoreImage(identity ReplicatedShardStoreIdentity, apply ReplicatedApplyIdentity, reader io.Reader) (replicatedstate.SnapshotArtifactManifest, [32]byte, error) {
	if err := validateReplicatedShardStoreIdentity(identity); err != nil {
		return replicatedstate.SnapshotArtifactManifest{}, [32]byte{}, err
	}
	if apply.ValidationDigest != replicatedApplyProfileDigest(identity, apply.Placement) {
		return replicatedstate.SnapshotArtifactManifest{}, [32]byte{}, ErrReplicatedRestoreStageProof
	}
	profiles := make([]replicatedstate.RestoreImageProfile, len(identity.Relations))
	logical := replicatedRelationApplyManifestDigest(identity)
	for i, r := range identity.Relations {
		profiles[i].Name = r.Table
		profiles[i].ValidationDigest = apply.ValidationDigest
		if r.Kind == ReplicatedShardRelationGlobalIndex {
			profiles[i].ValidationDigest = replicatedGlobalIndexValidationDigest(identity, r, logical)
		}
	}
	return replicatedstate.RehashSnapshotArtifact(reader, profiles)
}
