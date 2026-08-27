package driver

import "crypto/sha256"

// ReplicatedRelationManifestDigest returns the portable logical relation
// manifest authenticated by Raft. Unlike the store identity's local digest,
// this witness deliberately excludes member-local storage identities.
func ReplicatedRelationManifestDigest(
	identity ReplicatedShardStoreIdentity,
) ([sha256.Size]byte, error) {
	if err := validateReplicatedShardStoreIdentity(identity); err != nil {
		return [sha256.Size]byte{}, err
	}
	return replicatedRelationApplyManifestDigest(identity), nil
}

// InitialReplicatedRelationManifest returns the portable manifest and fixed
// admission limits for a generation-one singleton JSON relation. It performs
// no storage allocation and is used before any replica owns a persistent
// volume.
func InitialReplicatedRelationManifest(
	table string,
) ([sha256.Size]byte, ReplicatedShardStoreLimits, error) {
	if err := validateReplicatedBundleRelationName(table); err != nil {
		return [sha256.Size]byte{}, ReplicatedShardStoreLimits{}, err
	}
	limits := ReplicatedShardStoreLimits{
		MaxKeyBytes: replicatedMaxKeyBytes, MaxDocumentBytes: replicatedMaxDocumentBytes,
		MaxBatchDocuments: replicatedMaxDistinctMutations, MaxBatchBytes: replicatedMaxBatchBytes,
	}
	identity := ReplicatedShardStoreIdentity{
		RelationCount: 1, RelationSchemaGeneration: 1,
		Relations: []ReplicatedShardRelationIdentity{{
			Relation: 1, Kind: ReplicatedShardRelationJSON, Table: table, Limits: limits,
		}},
	}
	return replicatedRelationApplyManifestDigest(identity), limits, nil
}
