package driver

import (
	"fmt"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store"
)

func rejectReplicatedLocalUniqueIndexes(indexes []store.IndexDefinition) error {
	for i := range indexes {
		if indexes[i].Unique {
			return fmt.Errorf(
				"%w: replicated local unique index %q is not supported",
				ErrReplicatedShardStoreProfile, indexes[i].Name,
			)
		}
	}
	return nil
}

// ReplicatedSchemaManifest computes the exact serving-machine schema for a
// retained SQL identity and explicit local indexes without opening storage.
// Local index definitions must match the authenticated SQL manifest. The result
// is distinct from the replica-local SQL digest and changes with placement.
func ReplicatedSchemaManifest(identity ReplicatedShardStoreIdentity, placement ReplicatedPlacementProfile,
	indexes []store.IndexDefinition,
) ([32]byte, error) {
	if err := validateReplicatedShardStoreIdentity(identity); err != nil {
		return [32]byte{}, err
	}
	if err := validateReplicatedPlacementProfile(placement, identity); err != nil {
		return [32]byte{}, err
	}
	apply := ReplicatedApplyIdentity{ValidationProfile: uint8(replicatedstate.ValidationDeterministicMutation),
		ValidationDigest: replicatedApplyProfileDigest(identity, placement), Placement: placement}
	return replicatedSchemaManifestValidated(identity, apply, indexes)
}

// replicatedSchemaManifestValidated builds the serving manifest after the
// caller has authenticated the shard-store identity and apply profile. It is
// intentionally private: callers must prove that identity and apply refer to
// the same live catalog state before using this bounded construction path.
func replicatedSchemaManifestValidated(identity ReplicatedShardStoreIdentity,
	apply ReplicatedApplyIdentity, indexes []store.IndexDefinition,
) ([32]byte, error) {
	if err := rejectReplicatedLocalUniqueIndexes(indexes); err != nil {
		return [32]byte{}, err
	}
	if len(indexes) > 4096 {
		return [32]byte{}, ErrReplicatedShardStoreProfile
	}
	meta := make([]indexMeta, len(indexes))
	for i, index := range indexes {
		compiled, err := store.CompileExactIndex(index)
		if err != nil {
			return [32]byte{}, err
		}
		meta[i] = indexMeta{Name: index.Name, Paths: compiled.Specs[:compiled.N]}
	}
	if replicatedLocalIndexDigest(meta) != identity.Relations[0].LocalIndexDigest {
		return [32]byte{}, ErrReplicatedShardStoreIdentityMismatch
	}
	relations, err := replicatedRelationSchemas(identity, apply, indexes)
	if err != nil {
		return [32]byte{}, err
	}
	return replicatedstate.InitialRelationManifest(identity.RelationSchemaGeneration, relations)
}

// Both cold manifest verification and live apply use these exact descriptors.
// Live collection handles and validators are attached only by replicatedApplyRelations.
func replicatedRelationSchemas(base ReplicatedShardStoreIdentity, apply ReplicatedApplyIdentity,
	indexes []store.IndexDefinition,
) ([]replicatedstate.RelationCollection, error) {
	logicalManifest := replicatedRelationApplyManifestDigest(base)
	result := make([]replicatedstate.RelationCollection, int(base.RelationCount))
	for ordinal := range result {
		relation := base.Relations[ordinal]
		spec := replicatedstate.RelationCollection{
			Relation: replication.RelationID(relation.Relation), Name: relation.Table,
			Target: replicatedstate.CollectionTarget{Validation: replicatedstate.ValidationDeterministicMutation,
				Limits: replicatedStateCollectionLimits(relation.Limits)},
		}
		switch relation.Kind {
		case ReplicatedShardRelationJSON:
			spec.Kind, spec.LocalIndexes = replicatedstate.RelationJSON, indexes
			spec.Target.Validation = replicatedstate.ValidationProfile(apply.ValidationProfile)
			spec.Target.ValidationDigest = apply.ValidationDigest
		case ReplicatedShardRelationGlobalIndex:
			spec.Kind = replicatedstate.RelationGlobalIndex
			spec.Target.ValidationDigest = replicatedGlobalIndexValidationDigest(base, relation, logicalManifest)
			spec.GlobalIndex = replicatedstate.GlobalIndexProfile{IndexID: relation.IndexID, Incarnation: relation.Incarnation,
				LocatorCount: relation.LocatorCount, Unique: relation.Unique, KeyEncoding: replicatedstate.GlobalIndexKeyEncoding(relation.KeyEncoding),
				KeyArity: relation.KeyArity, TupleVersion: relation.TupleVersion, MapperVersion: relation.MapperVersion, BucketBits: relation.BucketBits}
		default:
			return nil, ErrReplicatedApplyMismatch
		}
		result[ordinal] = spec
	}
	return result, nil
}
