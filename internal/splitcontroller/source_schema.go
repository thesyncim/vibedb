package splitcontroller

import (
	"bytes"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store"
)

func cloneSplitLocalIndexes(indexes []store.IndexDefinition) []store.IndexDefinition {
	if indexes == nil {
		return nil
	}
	result := make([]store.IndexDefinition, len(indexes))
	for i, index := range indexes {
		result[i] = store.IndexDefinition{
			Name: index.Name, Paths: slices.Clone(index.Paths), Unique: index.Unique,
		}
	}
	return result
}

func clonePlanSourceSchema(schema PlanSourceSchema) PlanSourceSchema {
	schema.SQL = schema.SQL.Clone()
	schema.LocalIndexes = cloneSplitLocalIndexes(schema.LocalIndexes)
	return schema
}

func sameSplitLocalIndexes(left, right []store.IndexDefinition) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Name != right[i].Name || left[i].Unique != right[i].Unique ||
			!slices.Equal(left[i].Paths, right[i].Paths) {
			return false
		}
	}
	return true
}

func samePlanSourceAuthority(left, right PlanSourceAuthority) bool {
	return left.Group == right.Group && left.Command == right.Command && left.LogicalSchemaDigest == right.LogicalSchemaDigest && left.Schema.SQL.Equal(right.Schema.SQL) &&
		left.Schema.Placement == right.Schema.Placement && sameSplitLocalIndexes(left.Schema.LocalIndexes, right.Schema.LocalIndexes)
}

func (p *Plan) validateReplicatedSourceSchema() error {
	authority := p.sourceAuthority
	if authority == nil {
		return ErrInvalidPlan
	}
	schema := authority.Schema
	binding := schema.SQL.Binding
	group := raftmember.GroupKey{ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch, ShardIncarnation: binding.ShardIncarnation, GroupID: binding.GroupID}
	if group != authority.Group || binding.Distribution != string(p.source.Distribution) || binding.Shard != string(p.source.Shard) ||
		binding.AllocationGeneration != uint64(p.source.AllocationGeneration) || binding.Authority.SchemaGeneration != authority.Command.SchemaGeneration ||
		schema.SQL.UserTable != p.partitioner.CollectionName() || !schema.Placement.Range.Contains(p.source.Range.Start) ||
		!schema.Placement.Range.End.Max && (p.source.Range.End.Max || bytes.Compare(schema.Placement.Range.End.Point[:], p.source.Range.End.Point[:]) < 0) {
		return ErrInvalidPlan
	}
	digest, err := sqldriver.ReplicatedSchemaManifest(schema.SQL, schema.Placement, schema.LocalIndexes)
	if err != nil || digest != authority.Command.RelationManifestDigest {
		return errors.Join(ErrInvalidPlan, err)
	}
	logical, err := sqldriver.ReplicatedRelationManifestDigest(schema.SQL)
	if err != nil || logical != authority.LogicalSchemaDigest {
		return errors.Join(ErrInvalidPlan, err)
	}
	return nil
}

func (p *Plan) validateReplicatedChildSchema(target ChildTarget) error {
	source := p.sourceAuthority.Schema
	sourceDigest, err := sqldriver.ReplicatedRelationManifestDigest(source.SQL)
	if err != nil {
		return errors.Join(ErrInvalidPlan, err)
	}
	if !sameSplitLocalIndexes(source.LocalIndexes, target.LocalIndexes) {
		return ErrInvalidPlan
	}
	for _, replica := range target.Replicas {
		digest, err := sqldriver.ReplicatedSchemaManifest(replica.SQL, replica.Apply.Placement, target.LocalIndexes)
		if err != nil || digest != target.RelationManifestDigest {
			return errors.Join(ErrInvalidPlan, err)
		}
		logical, err := sqldriver.ReplicatedRelationManifestDigest(replica.SQL)
		if err != nil || logical != sourceDigest || replica.Apply.Placement.Format != source.Placement.Format ||
			replica.Apply.Placement.ShardKey != source.Placement.ShardKey || replica.Apply.Placement.TupleVersion != source.Placement.TupleVersion ||
			replica.Apply.Placement.MapperVersion != source.Placement.MapperVersion {
			return errors.Join(ErrInvalidPlan, err)
		}
	}
	return nil
}

func (p *Plan) sourceBindingAuthorityMatches(binding replicatedstate.Binding) bool {
	authority := p.sourceAuthority
	return authority != nil && binding.ClusterID == authority.Group.ClusterID && binding.ClusterIncarnation == authority.Group.ClusterIncarnation &&
		binding.TopologyRecoveryEpoch == authority.Group.TopologyRecoveryEpoch && binding.ShardIncarnation == authority.Group.ShardIncarnation && binding.GroupID == authority.Group.GroupID &&
		binding.ActivePolicyGeneration == authority.Command.ActivePolicyGeneration && binding.ProtectionEpoch == authority.Command.ProtectionEpoch && binding.SchemaGeneration == authority.Command.SchemaGeneration
}

func (p *Plan) validRelationCollection(index int, relation sqldriver.ReplicatedShardRelationIdentity) bool {
	if p == nil || p.partitioner == nil || index < 0 || relation.Relation == 0 ||
		int(relation.Relation) != index+1 {
		return false
	}
	if index == 0 {
		return relation.Kind == sqldriver.ReplicatedShardRelationJSON && relation.Table == p.partitioner.CollectionName()
	}
	if relation.Kind != sqldriver.ReplicatedShardRelationGlobalIndex {
		return false
	}
	if p.sourceAuthority == nil {
		// Preserve the preexisting non-replicated restriction: no unrelated
		// collection gains authority without an authenticated source schema.
		return relation.Table == p.partitioner.CollectionName()
	}
	// NewPlan has already verified the source machine/logical digests and each
	// child's matching logical schema. Global slots own their own collections;
	// bind to that exact source slot, not the base table's collection name.
	relations := p.sourceAuthority.Schema.SQL.Relations
	return index < len(relations) && relations[index].Relation == relation.Relation &&
		relations[index].Kind == relation.Kind && relations[index].Table == relation.Table
}
