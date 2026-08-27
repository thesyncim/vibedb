package main

import (
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
)

type rf3SnapshotFenceSource interface {
	SnapshotAuthorizationFence() (replicatedstate.SnapshotFence, error)
}

func rf3SnapshotDataAuthorizer(source rf3SnapshotFenceSource, identity raftmember.RuntimeIdentity,
	target rf3ManifestEnrolledTarget,
) snapshottransfer.AuthorizeFunc {
	return func(descriptor snapshottransfer.Descriptor) bool {
		if source == nil || descriptor.Group != identity.Group ||
			descriptor.SourceMember != identity.MemberID || descriptor.TargetMember != target.MemberID ||
			descriptor.TargetStore != target.StoreID || descriptor.TargetIncarnation != target.NodeIncarnation {
			return false
		}
		fence, err := source.SnapshotAuthorizationFence()
		if err != nil || fence.Applied == 0 || fence.RelationManifestDigest == ([32]byte{}) {
			return false
		}
		binding := fence.Binding
		group := raftmember.GroupKey{ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
			TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch, ShardIncarnation: binding.ShardIncarnation,
			GroupID: binding.GroupID}
		return group == descriptor.Group && descriptor.SchemaGeneration == binding.SchemaGeneration &&
			descriptor.ReplicaSetVersion == fence.ReplicaSetVersion
	}
}
