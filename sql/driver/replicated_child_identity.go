package driver

import (
	"fmt"
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
