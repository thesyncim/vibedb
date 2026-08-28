package driver

import (
	"crypto/sha256"
	"fmt"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/store"
)

// ReplicatedRelationManifestDigest returns the portable SQL relation descriptor
// digest. It excludes member-local storage identities, but is not the machine
// schema digest exposed by CapacityQualificationProfile: that also binds the
// mutation-validation and placement profiles.
func ReplicatedRelationManifestDigest(
	identity ReplicatedShardStoreIdentity,
) ([sha256.Size]byte, error) {
	if err := validateReplicatedShardStoreIdentity(identity); err != nil {
		return [sha256.Size]byte{}, err
	}
	return replicatedRelationApplyManifestDigest(identity), nil
}

// InitialReplicatedRelationSchema describes an unmaterialized singleton JSON
// relation. Zero Limits selects the engine's initial replicated limits.
type InitialReplicatedRelationSchema struct {
	Table        string
	PrimaryKey   string
	Limits       ReplicatedShardStoreLimits
	LocalIndexes []store.IndexDefinition
	Schema       *store.Schema
}

// InitialReplicatedRelationManifest computes the exact machine-schema digest
// exposed by a replica created with this binding, placement, and schema. It
// does not create storage or grant authority. Member/store identifiers are
// validated but excluded from the portable digest by the shared hash grammar.
func InitialReplicatedRelationManifest(
	binding ReplicatedShardStoreBinding,
	placement ReplicatedPlacementProfile,
	schema InitialReplicatedRelationSchema,
) ([sha256.Size]byte, ReplicatedShardStoreLimits, error) {
	identity, err := initialReplicatedSchemaIdentity(binding, placement, schema)
	if err != nil {
		return [sha256.Size]byte{}, ReplicatedShardStoreLimits{}, err
	}
	digest, err := replicatedstate.InitialJSONRelationManifest(binding.Authority.SchemaGeneration,
		schema.Table, replicatedStateCollectionLimits(identity.UserLimits), replicatedApplyProfileDigest(identity, placement), schema.LocalIndexes)
	if err != nil {
		return [sha256.Size]byte{}, ReplicatedShardStoreLimits{}, fmt.Errorf("initial replicated schema: %w", err)
	}
	return digest, identity.UserLimits, nil
}

// InitialReplicatedLogicalSchemaDigest computes the portable SQL schema
// commitment before storage exists. It shares the bound identity's exact hash
// grammar, but never invents log IDs, storage names, or serving authority.
func InitialReplicatedLogicalSchemaDigest(binding ReplicatedShardStoreBinding,
	placement ReplicatedPlacementProfile, schema InitialReplicatedRelationSchema,
) ([sha256.Size]byte, error) {
	identity, err := initialReplicatedSchemaIdentity(binding, placement, schema)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return replicatedRelationApplyManifestDigest(identity), nil
}

func initialReplicatedSchemaIdentity(binding ReplicatedShardStoreBinding,
	placement ReplicatedPlacementProfile, schema InitialReplicatedRelationSchema,
) (ReplicatedShardStoreIdentity, error) {
	if err := validateReplicatedShardStoreBinding(binding); err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	if err := validateReplicatedBundleRelationName(schema.Table); err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	limits := schema.Limits
	if limits == (ReplicatedShardStoreLimits{}) {
		limits = ReplicatedShardStoreLimits{
			MaxKeyBytes: replicatedMaxKeyBytes, MaxDocumentBytes: replicatedMaxDocumentBytes,
			MaxBatchDocuments: replicatedMaxDistinctMutations, MaxBatchBytes: replicatedMaxBatchBytes,
		}
	}
	if err := validateReplicatedShardStoreLimits(limits); err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	indexes := make([]indexMeta, len(schema.LocalIndexes))
	for i, index := range schema.LocalIndexes {
		compiled, err := store.CompileExactIndex(index)
		if err != nil {
			return ReplicatedShardStoreIdentity{}, err
		}
		indexes[i] = indexMeta{Name: index.Name, Paths: compiled.Specs[:compiled.N]}
	}
	// This value is only a hashing input. It deliberately has no fabricated
	// log/storage IDs and is never passed off as a bound durable identity.
	identity := ReplicatedShardStoreIdentity{
		Binding: binding, UserTable: schema.Table, UserPrimaryKey: schema.PrimaryKey,
		UserLimits: limits, RelationCount: 1, RelationSchemaGeneration: binding.Authority.SchemaGeneration,
		Relations: []ReplicatedShardRelationIdentity{{
			Relation: 1, Kind: ReplicatedShardRelationJSON, Table: schema.Table, Limits: limits,
			LocalIndexDigest: replicatedLocalIndexDigest(indexes),
			SchemaDigest:     replicatedSchemaDigest(schemaMetaFrom(schema.Schema)),
		}},
	}
	if err := validateReplicatedPlacementProfile(placement, identity); err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	return identity, nil
}
