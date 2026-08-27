package driver

import (
	"fmt"
	"slices"
	"strings"
)

// NewReplicatedChildShardStoreIdentity constructs the complete exact identity
// for a schema-free singleton JSON table before its destination root exists.
// It is the topology allocator boundary used by split preparation; all storage
// names and the SQL LogID are caller-issued, while sidecars and manifest bytes
// are derived by the same canonical code consumed by bind.
func NewReplicatedChildShardStoreIdentity(
	local ShardStoreIdentity,
	binding ReplicatedShardStoreBinding,
	userTable, userStorage, primaryKey string,
	limits ReplicatedShardStoreLimits,
) (ReplicatedShardStoreIdentity, error) {
	if err := validateShardStoreIdentity(local); err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	if err := validateReplicatedShardStoreBinding(binding); err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	if string(local.Distribution) != binding.Distribution || string(local.Shard) != binding.Shard ||
		uint64(local.AllocationGeneration) != binding.AllocationGeneration ||
		validateReplicatedUserTableName(userTable) != nil || validateStorageIdentity(userStorage) != nil ||
		validateReplicatedShardStoreLimits(limits) != nil {
		return ReplicatedShardStoreIdentity{}, ErrReplicatedShardStoreProfile
	}
	sidecars := canonicalReplicatedShardStoreSidecars()
	identity := ReplicatedShardStoreIdentity{
		Format: ReplicatedShardStoreFormat, Binding: ownedReplicatedShardStoreBinding(binding),
		LogID: local.LogID, UserTable: strings.Clone(userTable),
		UserStorage: strings.Clone(userStorage), UserPrimaryKey: strings.Clone(primaryKey),
		UserLimits: limits, Sidecars: sidecars, RelationCount: 1,
		RelationSchemaGeneration: binding.Authority.SchemaGeneration,
		Relations:                make([]ReplicatedShardRelationIdentity, 1),
	}
	identity.Relations[0] = ReplicatedShardRelationIdentity{
		Relation: 1, Kind: ReplicatedShardRelationJSON,
		Table: identity.UserTable, Storage: identity.UserStorage, Limits: identity.UserLimits,
		LocalIndexDigest: replicatedLocalIndexDigest(nil),
	}
	identity.RelationManifestDigest = replicatedRelationManifestDigest(identity)
	if err := validateReplicatedShardStoreIdentity(identity); err != nil {
		return ReplicatedShardStoreIdentity{}, fmt.Errorf(
			"%w: %v", ErrReplicatedShardStoreProfile, err,
		)
	}
	return identity, nil
}

// NewReplicatedChildShardStoreBundleIdentity copies only authenticated schema
// from a retained source. The topology allocator must issue every destination
// storage name explicitly; none is derived from a relation or collection name.
func NewReplicatedChildShardStoreBundleIdentity(local ShardStoreIdentity,
	binding ReplicatedShardStoreBinding, source ReplicatedShardStoreIdentity, storages []string,
) (ReplicatedShardStoreIdentity, error) {
	if err := validateReplicatedShardStoreIdentity(source); err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	if len(storages) != int(source.RelationCount) || binding.Authority.SchemaGeneration != source.RelationSchemaGeneration ||
		binding.ClusterID != source.Binding.ClusterID || binding.ClusterIncarnation != source.Binding.ClusterIncarnation ||
		binding.TopologyRecoveryEpoch != source.Binding.TopologyRecoveryEpoch || binding.Distribution != source.Binding.Distribution {
		return ReplicatedShardStoreIdentity{}, ErrReplicatedShardStoreProfile
	}
	identity, err := NewReplicatedChildShardStoreIdentity(local, binding, source.UserTable, storages[0], source.UserPrimaryKey, source.UserLimits)
	if err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	identity.Relations = slices.Clone(source.Relations)
	identity.RelationCount = source.RelationCount
	for i := range identity.Relations {
		if validateStorageIdentity(storages[i]) != nil {
			return ReplicatedShardStoreIdentity{}, ErrReplicatedShardStoreProfile
		}
		for j := 0; j < i; j++ {
			if storages[i] == storages[j] {
				return ReplicatedShardStoreIdentity{}, ErrReplicatedShardStoreProfile
			}
		}
		identity.Relations[i].Storage = strings.Clone(storages[i])
		identity.Relations[i].Table = strings.Clone(identity.Relations[i].Table)
	}
	identity.RelationManifestDigest = replicatedRelationManifestDigest(identity)
	if err := validateReplicatedShardStoreIdentity(identity); err != nil {
		return ReplicatedShardStoreIdentity{}, err
	}
	return identity, nil
}

// NewReplicatedChildApplyIdentity derives the exact hidden apply identity
// without creating files. Storage and captureStorage are allocator-issued and
// are validated against the complete base identity and options.
func NewReplicatedChildApplyIdentity(
	base ReplicatedShardStoreIdentity,
	storage, captureStorage string,
	options ReplicatedApplyOptions,
) (ReplicatedApplyIdentity, error) {
	if err := validateReplicatedShardStoreIdentity(base); err != nil {
		return ReplicatedApplyIdentity{}, err
	}
	if err := validateReplicatedApplyOptions(base, options); err != nil {
		return ReplicatedApplyIdentity{}, err
	}
	meta := newReplicatedApplyMeta(base, strings.Clone(storage), strings.Clone(captureStorage), options)
	if err := validateReplicatedApplyMeta(&meta, &base); err != nil {
		return ReplicatedApplyIdentity{}, err
	}
	return meta.identity(), nil
}
