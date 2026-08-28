package gateway

import "github.com/thesyncim/vibedb/internal/raftmember"

// ReplicatedSplitOrigin is a compact live-allocation checkpoint, not an
// operation history. It survives terminal operation GC and identifies the
// original provisioned template without walking an ancestry chain.
type ReplicatedSplitOrigin struct {
	RootGroup, ParentGroup               raftmember.GroupKey
	Operation, PlanDigest, CutoverDigest [32]byte
	SchemaGeneration                     uint64
	RelationManifestDigest               [32]byte
	Child                                uint8
}

type persistedReplicatedSplitGroup struct {
	ClusterID             [16]byte `json:"cluster_id"`
	ClusterIncarnation    [16]byte `json:"cluster_incarnation"`
	TopologyRecoveryEpoch uint64   `json:"topology_recovery_epoch"`
	ShardIncarnation      [16]byte `json:"shard_incarnation"`
	GroupID               [16]byte `json:"group_id"`
}

type persistedReplicatedSplitOrigin struct {
	RootGroup              persistedReplicatedSplitGroup `json:"root_group"`
	ParentGroup            persistedReplicatedSplitGroup `json:"parent_group"`
	Operation              [32]byte                      `json:"operation"`
	PlanDigest             [32]byte                      `json:"plan_digest"`
	CutoverDigest          [32]byte                      `json:"cutover_digest"`
	SchemaGeneration       uint64                        `json:"schema_generation"`
	RelationManifestDigest [32]byte                      `json:"relation_manifest_digest"`
	Child                  uint8                         `json:"child"`
}

func persistReplicatedSplitOrigin(origin *ReplicatedSplitOrigin) *persistedReplicatedSplitOrigin {
	if origin == nil {
		return nil
	}
	return &persistedReplicatedSplitOrigin{RootGroup: persistedReplicatedSplitGroup(origin.RootGroup), ParentGroup: persistedReplicatedSplitGroup(origin.ParentGroup),
		Operation: origin.Operation, PlanDigest: origin.PlanDigest, CutoverDigest: origin.CutoverDigest, SchemaGeneration: origin.SchemaGeneration,
		RelationManifestDigest: origin.RelationManifestDigest, Child: origin.Child}
}

func openReplicatedSplitOrigin(origin *persistedReplicatedSplitOrigin) *ReplicatedSplitOrigin {
	if origin == nil {
		return nil
	}
	return &ReplicatedSplitOrigin{RootGroup: raftmember.GroupKey(origin.RootGroup), ParentGroup: raftmember.GroupKey(origin.ParentGroup),
		Operation: origin.Operation, PlanDigest: origin.PlanDigest, CutoverDigest: origin.CutoverDigest, SchemaGeneration: origin.SchemaGeneration,
		RelationManifestDigest: origin.RelationManifestDigest, Child: origin.Child}
}

func cloneReplicatedSplitOrigin(origin *ReplicatedSplitOrigin) *ReplicatedSplitOrigin {
	if origin == nil {
		return nil
	}
	copy := *origin
	return &copy
}

func validReplicatedSplitOrigin(origin *ReplicatedSplitOrigin, descriptor ReplicatedShardDescriptor) bool {
	if origin == nil {
		return true
	}
	return validReplicatedCatalogGroup(origin.RootGroup) && validReplicatedCatalogGroup(origin.ParentGroup) &&
		origin.RootGroup.ClusterID == descriptor.Group.ClusterID && origin.RootGroup.ClusterIncarnation == descriptor.Group.ClusterIncarnation &&
		origin.RootGroup.TopologyRecoveryEpoch == descriptor.Group.TopologyRecoveryEpoch &&
		origin.ParentGroup.ClusterID == descriptor.Group.ClusterID && origin.ParentGroup.ClusterIncarnation == descriptor.Group.ClusterIncarnation &&
		origin.ParentGroup.TopologyRecoveryEpoch == descriptor.Group.TopologyRecoveryEpoch &&
		origin.RootGroup != descriptor.Group && origin.ParentGroup != descriptor.Group &&
		origin.Operation != ([32]byte{}) && origin.PlanDigest != ([32]byte{}) && origin.CutoverDigest != ([32]byte{}) &&
		origin.SchemaGeneration != 0 && origin.SchemaGeneration <= descriptor.Command.SchemaGeneration &&
		origin.RelationManifestDigest != ([32]byte{}) && origin.Child < 3
}

func sameReplicatedSplitOrigin(left, right *ReplicatedSplitOrigin) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}
