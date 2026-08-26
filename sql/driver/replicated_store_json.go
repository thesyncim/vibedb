package driver

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibejson"
)

// Replicated store metadata uses one bounded vibejson grammar. The fixed
// appenders avoid interface/map materialization, while the cursor decoders
// reject unknown, duplicate, missing, oversized, and trailing input before a
// decoded identity can reach catalog validation.

const (
	maxReplicatedAuthorityJSONBytes = 1 << 10
	maxReplicatedBindingJSONBytes   = 4 << 10
	maxReplicatedLimitsJSONBytes    = 512
	maxReplicatedSidecarsJSONBytes  = 512
	maxReplicatedRelationJSONBytes  = 4 << 10
	maxReplicatedStoreJSONBytes     = 256 << 10
)

type replicatedAuthorityVibe ReplicatedAuthorityProfile
type replicatedBindingVibe ReplicatedShardStoreBinding
type replicatedLimitsVibe ReplicatedShardStoreLimits
type replicatedRelationVibe ReplicatedShardRelationIdentity
type replicatedStoreIdentityVibe ReplicatedShardStoreIdentity

var (
	replicatedAuthorityFields = vibejson.MakeFieldSet(
		"active_policy_generation", "protection_epoch", "ownership_epoch",
		"schema_generation", "routing_version", "route_generation",
	)
	replicatedBindingFields = vibejson.MakeFieldSet(
		"cluster_id", "cluster_incarnation", "topology_recovery_epoch",
		"distribution", "shard", "allocation_generation", "shard_incarnation",
		"group_id", "member_id", "store_id", "authority",
	)
	replicatedRelationFields = vibejson.MakeFieldSet(
		"relation", "kind", "table", "storage", "limits", "local_index_digest",
		"index_id", "incarnation", "locator_count", "unique", "key_encoding",
		"key_arity", "tuple_version", "mapper_version", "bucket_bits",
	)
	replicatedStoreFields = vibejson.MakeFieldSet(
		"format", "binding", "log_id", "user_table", "user_storage",
		"user_primary_key", "user_limits", "sidecars",
		"relation_schema_generation", "relation_manifest_digest", "relations",
	)
)

var (
	replicatedAuthorityFieldNames = [...]string{
		"active_policy_generation", "protection_epoch", "ownership_epoch",
		"schema_generation", "routing_version", "route_generation",
	}
	replicatedBindingFieldNames = [...]string{
		"cluster_id", "cluster_incarnation", "topology_recovery_epoch",
		"distribution", "shard", "allocation_generation", "shard_incarnation",
		"group_id", "member_id", "store_id", "authority",
	}
	replicatedRelationFieldNames = [...]string{
		"relation", "kind", "table", "storage", "limits", "local_index_digest",
		"index_id", "incarnation", "locator_count", "unique", "key_encoding",
		"key_arity", "tuple_version", "mapper_version", "bucket_bits",
	}
	replicatedStoreFieldNames = [...]string{
		"format", "binding", "log_id", "user_table", "user_storage",
		"user_primary_key", "user_limits", "sidecars",
		"relation_schema_generation", "relation_manifest_digest", "relations",
	}
)

func validateReplicatedAuthorityGrammar(a ReplicatedAuthorityProfile) error {
	if a.ActivePolicyGeneration == 0 || a.ProtectionEpoch == 0 ||
		a.OwnershipEpoch == 0 || a.SchemaGeneration == 0 ||
		a.RoutingVersion == 0 || a.RouteGeneration == 0 {
		return errors.New("vibedb: replicated authority profile contains a zero generation")
	}
	return nil
}

func validateReplicatedRelationGrammar(r ReplicatedShardRelationIdentity) error {
	if r.Relation == 0 || r.Relation > replication.MaxRelationsPerBundle ||
		validateReplicatedBundleRelationName(r.Table) != nil ||
		r.Storage == "" || validateStorageIdentity(r.Storage) != nil ||
		validateReplicatedShardStoreLimits(r.Limits) != nil {
		return fmt.Errorf("%w: invalid relation identity", ErrReplicatedShardStoreProfile)
	}
	switch r.Kind {
	case ReplicatedShardRelationJSON:
		if r.IndexID != 0 || r.Incarnation != 0 || r.LocatorCount != 0 || r.Unique ||
			r.KeyEncoding != 0 || r.KeyArity != 0 || r.TupleVersion != 0 ||
			r.MapperVersion != 0 || r.BucketBits != 0 {
			return fmt.Errorf("%w: invalid JSON relation identity", ErrReplicatedShardStoreProfile)
		}
	case ReplicatedShardRelationGlobalIndex:
		if r.LocalIndexDigest != ([sha256.Size]byte{}) || r.IndexID == 0 ||
			r.Incarnation == 0 || r.LocatorCount == 0 || r.LocatorCount > 8 ||
			!validReplicatedGlobalIndexPlacement(
				r.KeyEncoding, r.KeyArity, r.TupleVersion, r.MapperVersion, r.BucketBits,
			) {
			return fmt.Errorf("%w: invalid global-index relation identity", ErrReplicatedShardStoreProfile)
		}
	default:
		return fmt.Errorf("%w: unknown relation kind", ErrReplicatedShardStoreProfile)
	}
	return nil
}

func (a ReplicatedAuthorityProfile) MarshalJSON() ([]byte, error) {
	if err := validateReplicatedAuthorityGrammar(a); err != nil {
		return nil, err
	}
	encoded := replicatedAuthorityVibe(a)
	return vibejson.Marshal(&encoded)
}

func (b ReplicatedShardStoreBinding) MarshalJSON() ([]byte, error) {
	if err := validateReplicatedShardStoreBinding(b); err != nil {
		return nil, err
	}
	encoded := replicatedBindingVibe(b)
	return vibejson.Marshal(&encoded)
}

func (l ReplicatedShardStoreLimits) MarshalJSON() ([]byte, error) {
	if err := validateReplicatedShardStoreLimits(l); err != nil {
		return nil, err
	}
	encoded := replicatedLimitsVibe(l)
	return vibejson.Marshal(&encoded)
}

func (r ReplicatedShardRelationIdentity) MarshalJSON() ([]byte, error) {
	if err := validateReplicatedRelationGrammar(r); err != nil {
		return nil, err
	}
	encoded := replicatedRelationVibe(r)
	return vibejson.Marshal(&encoded)
}

func (i ReplicatedShardStoreIdentity) MarshalJSON() ([]byte, error) {
	if err := validateReplicatedShardStoreIdentity(i); err != nil {
		return nil, err
	}
	encoded := replicatedStoreIdentityVibe(i)
	return vibejson.Marshal(&encoded)
}

func (a *ReplicatedAuthorityProfile) UnmarshalJSON(data []byte) error {
	if len(data) > maxReplicatedAuthorityJSONBytes {
		return errors.New("vibedb: replicated authority profile exceeds its byte bound")
	}
	var decoded replicatedAuthorityVibe
	if err := vibejson.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*a = ReplicatedAuthorityProfile(decoded)
	return nil
}

func (b *ReplicatedShardStoreBinding) UnmarshalJSON(data []byte) error {
	if len(data) > maxReplicatedBindingJSONBytes {
		return errors.New("vibedb: replicated shard store binding exceeds its byte bound")
	}
	var decoded replicatedBindingVibe
	if err := vibejson.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*b = ReplicatedShardStoreBinding(decoded)
	return nil
}

func (l *ReplicatedShardStoreLimits) UnmarshalJSON(data []byte) error {
	if len(data) > maxReplicatedLimitsJSONBytes {
		return errors.New("vibedb: replicated shard store limits exceed their byte bound")
	}
	var decoded replicatedLimitsVibe
	if err := vibejson.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*l = ReplicatedShardStoreLimits(decoded)
	return nil
}

func (r *ReplicatedShardRelationIdentity) UnmarshalJSON(data []byte) error {
	return decodeBoundedReplicatedRelation(data, r)
}

func (i *ReplicatedShardStoreIdentity) UnmarshalJSON(data []byte) error {
	if len(data) > maxReplicatedStoreJSONBytes {
		return errors.New("vibedb: replicated shard store identity exceeds its byte bound")
	}
	var decoded replicatedStoreIdentityVibe
	if err := vibejson.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*i = ReplicatedShardStoreIdentity(decoded)
	return nil
}

func (a *replicatedAuthorityVibe) MarshalVibeJSON(
	w vibejson.TrustedAppender,
) vibejson.TrustedAppender {
	return appendReplicatedAuthority(w, ReplicatedAuthorityProfile(*a))
}

func (b *replicatedBindingVibe) MarshalVibeJSON(
	w vibejson.TrustedAppender,
) vibejson.TrustedAppender {
	return appendReplicatedBinding(w, ReplicatedShardStoreBinding(*b))
}

func (l *replicatedLimitsVibe) MarshalVibeJSON(
	w vibejson.TrustedAppender,
) vibejson.TrustedAppender {
	return appendReplicatedLimits(w, ReplicatedShardStoreLimits(*l))
}

func (r *replicatedRelationVibe) MarshalVibeJSON(
	w vibejson.TrustedAppender,
) vibejson.TrustedAppender {
	return appendReplicatedRelation(w, ReplicatedShardRelationIdentity(*r))
}

func (i *replicatedStoreIdentityVibe) MarshalVibeJSON(
	w vibejson.TrustedAppender,
) vibejson.TrustedAppender {
	identity := ReplicatedShardStoreIdentity(*i)
	w = w.RawUnchecked(`{"format":`).Uint(uint64(identity.Format))
	w = w.RawUnchecked(`,"binding":`)
	w = appendReplicatedBinding(w, identity.Binding)
	w = w.RawUnchecked(`,"log_id":`)
	w = appendReplicatedHexString(w, identity.LogID[:])
	w = w.RawUnchecked(`,"user_table":`).String(identity.UserTable)
	w = w.RawUnchecked(`,"user_storage":`).String(identity.UserStorage)
	w = w.RawUnchecked(`,"user_primary_key":`).String(identity.UserPrimaryKey)
	w = w.RawUnchecked(`,"user_limits":`)
	w = appendReplicatedLimits(w, identity.UserLimits)
	w = w.RawUnchecked(`,"sidecars":`)
	w = appendReplicatedShardSidecars(w, identity.Sidecars)
	w = w.RawUnchecked(`,"relation_schema_generation":`).Uint(
		identity.RelationSchemaGeneration,
	)
	w = w.RawUnchecked(`,"relation_manifest_digest":`)
	w = appendReplicatedHexString(w, identity.RelationManifestDigest[:])
	w = w.RawUnchecked(`,"relations":[`)
	for ordinal := 0; ordinal < int(identity.RelationCount); ordinal++ {
		if ordinal != 0 {
			w = w.RawByteUnchecked(',')
		}
		w = appendReplicatedRelation(w, identity.Relations[ordinal])
	}
	return w.RawUnchecked(`]}`)
}

func appendReplicatedAuthority(
	w vibejson.TrustedAppender,
	a ReplicatedAuthorityProfile,
) vibejson.TrustedAppender {
	w = w.RawUnchecked(`{"active_policy_generation":`).Uint(a.ActivePolicyGeneration)
	w = w.RawUnchecked(`,"protection_epoch":`).Uint(a.ProtectionEpoch)
	w = w.RawUnchecked(`,"ownership_epoch":`).Uint(a.OwnershipEpoch)
	w = w.RawUnchecked(`,"schema_generation":`).Uint(a.SchemaGeneration)
	w = w.RawUnchecked(`,"routing_version":`).Uint(a.RoutingVersion)
	w = w.RawUnchecked(`,"route_generation":`).Uint(a.RouteGeneration)
	return w.RawByteUnchecked('}')
}

func appendReplicatedBinding(
	w vibejson.TrustedAppender,
	b ReplicatedShardStoreBinding,
) vibejson.TrustedAppender {
	w = w.RawUnchecked(`{"cluster_id":`)
	w = appendReplicatedHexString(w, b.ClusterID[:])
	w = w.RawUnchecked(`,"cluster_incarnation":`)
	w = appendReplicatedHexString(w, b.ClusterIncarnation[:])
	w = w.RawUnchecked(`,"topology_recovery_epoch":`).Uint(b.TopologyRecoveryEpoch)
	w = w.RawUnchecked(`,"distribution":`).String(b.Distribution)
	w = w.RawUnchecked(`,"shard":`).String(b.Shard)
	w = w.RawUnchecked(`,"allocation_generation":`).Uint(b.AllocationGeneration)
	w = w.RawUnchecked(`,"shard_incarnation":`)
	w = appendReplicatedHexString(w, b.ShardIncarnation[:])
	w = w.RawUnchecked(`,"group_id":`)
	w = appendReplicatedHexString(w, b.GroupID[:])
	w = w.RawUnchecked(`,"member_id":`).Uint(b.MemberID)
	w = w.RawUnchecked(`,"store_id":`)
	w = appendReplicatedHexString(w, b.StoreID[:])
	w = w.RawUnchecked(`,"authority":`)
	w = appendReplicatedAuthority(w, b.Authority)
	return w.RawByteUnchecked('}')
}

func appendReplicatedRelation(
	w vibejson.TrustedAppender,
	r ReplicatedShardRelationIdentity,
) vibejson.TrustedAppender {
	w = w.RawUnchecked(`{"relation":`).Uint(uint64(r.Relation))
	w = w.RawUnchecked(`,"kind":`).Uint(uint64(r.Kind))
	w = w.RawUnchecked(`,"table":`).String(r.Table)
	w = w.RawUnchecked(`,"storage":`).String(r.Storage)
	w = w.RawUnchecked(`,"limits":`)
	w = appendReplicatedLimits(w, r.Limits)
	w = w.RawUnchecked(`,"local_index_digest":`)
	w = appendReplicatedHexString(w, r.LocalIndexDigest[:])
	w = w.RawUnchecked(`,"index_id":`).Uint(r.IndexID)
	w = w.RawUnchecked(`,"incarnation":`).Uint(r.Incarnation)
	w = w.RawUnchecked(`,"locator_count":`).Uint(uint64(r.LocatorCount))
	w = w.RawUnchecked(`,"unique":`).Bool(r.Unique)
	w = w.RawUnchecked(`,"key_encoding":`).Uint(uint64(r.KeyEncoding))
	w = w.RawUnchecked(`,"key_arity":`).Uint(uint64(r.KeyArity))
	w = w.RawUnchecked(`,"tuple_version":`).Uint(uint64(r.TupleVersion))
	w = w.RawUnchecked(`,"mapper_version":`).Uint(uint64(r.MapperVersion))
	w = w.RawUnchecked(`,"bucket_bits":`).Uint(uint64(r.BucketBits))
	return w.RawByteUnchecked('}')
}

func appendReplicatedShardSidecars(
	w vibejson.TrustedAppender,
	p ReplicatedShardStoreSidecarProfile,
) vibejson.TrustedAppender {
	w = w.RawUnchecked(`{"user_recovery_journal_bytes":`).Uint(
		p.UserRecoveryJournalBytes,
	)
	w = w.RawUnchecked(`,"transaction_marker_bytes":`).Uint(p.TransactionMarkerBytes)
	return w.RawByteUnchecked('}')
}

func (a *replicatedAuthorityVibe) UnmarshalVibeJSON(
	c vibejson.DecodeCursor,
) (vibejson.DecodeCursor, error) {
	var decoded ReplicatedAuthorityProfile
	if err := decodeReplicatedAuthorityVibe(&c, &decoded); err != nil {
		return c, err
	}
	*a = replicatedAuthorityVibe(decoded)
	return c, nil
}

func (b *replicatedBindingVibe) UnmarshalVibeJSON(
	c vibejson.DecodeCursor,
) (vibejson.DecodeCursor, error) {
	var decoded ReplicatedShardStoreBinding
	if err := decodeReplicatedBindingVibe(&c, &decoded); err != nil {
		return c, err
	}
	*b = replicatedBindingVibe(decoded)
	return c, nil
}

func (l *replicatedLimitsVibe) UnmarshalVibeJSON(
	c vibejson.DecodeCursor,
) (vibejson.DecodeCursor, error) {
	var decoded ReplicatedShardStoreLimits
	if err := decodeReplicatedLimitsVibe(&c, &decoded); err != nil {
		return c, err
	}
	*l = replicatedLimitsVibe(decoded)
	return c, nil
}

func (r *replicatedRelationVibe) UnmarshalVibeJSON(
	c vibejson.DecodeCursor,
) (vibejson.DecodeCursor, error) {
	var decoded ReplicatedShardRelationIdentity
	if err := decodeReplicatedRelationVibe(&c, &decoded); err != nil {
		return c, err
	}
	*r = replicatedRelationVibe(decoded)
	return c, nil
}

func (i *replicatedStoreIdentityVibe) UnmarshalVibeJSON(
	c vibejson.DecodeCursor,
) (vibejson.DecodeCursor, error) {
	var decoded ReplicatedShardStoreIdentity
	if err := decodeReplicatedStoreIdentityVibe(&c, &decoded); err != nil {
		return c, err
	}
	*i = replicatedStoreIdentityVibe(decoded)
	return c, nil
}

func decodeReplicatedAuthorityVibe(
	c *vibejson.DecodeCursor,
	dst *ReplicatedAuthorityProfile,
) error {
	if err := c.BeginObject("replicated authority profile"); err != nil {
		return errors.New("vibedb: replicated authority profile must be a JSON object")
	}
	var decoded ReplicatedAuthorityProfile
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		index, known := replicatedAuthorityFields.Lookup(name, true)
		if !known {
			return unknownCatalogMember("replicated authority profile", name)
		}
		if err := markReplicatedCatalogField(&seen, index, "replicated authority profile", name); err != nil {
			return err
		}
		var decodeErr error
		switch index {
		case 0:
			decodeErr = c.Uint64(&decoded.ActivePolicyGeneration)
		case 1:
			decodeErr = c.Uint64(&decoded.ProtectionEpoch)
		case 2:
			decodeErr = c.Uint64(&decoded.OwnershipEpoch)
		case 3:
			decodeErr = c.Uint64(&decoded.SchemaGeneration)
		case 4:
			decodeErr = c.Uint64(&decoded.RoutingVersion)
		case 5:
			decodeErr = c.Uint64(&decoded.RouteGeneration)
		}
		if decodeErr != nil {
			return decodeErr
		}
	}
	if err := requireReplicatedFields(seen, replicatedAuthorityFieldNames[:], "replicated authority profile"); err != nil {
		return err
	}
	if err := validateReplicatedAuthorityGrammar(decoded); err != nil {
		return err
	}
	*dst = decoded
	return nil
}

func decodeReplicatedBindingVibe(
	c *vibejson.DecodeCursor,
	dst *ReplicatedShardStoreBinding,
) error {
	if err := c.BeginObject("replicated shard store binding"); err != nil {
		return errors.New("vibedb: replicated shard store binding must be a JSON object")
	}
	var decoded ReplicatedShardStoreBinding
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		index, known := replicatedBindingFields.Lookup(name, true)
		if !known {
			return unknownCatalogMember("replicated shard store binding", name)
		}
		if err := markReplicatedCatalogField(&seen, index, "replicated shard store binding", name); err != nil {
			return err
		}
		switch index {
		case 0:
			err = decodeReplicatedLowerHex(c, decoded.ClusterID[:],
				"vibedb: replicated cluster_id must be 128-bit lowercase hexadecimal",
				"vibedb: replicated cluster_id")
		case 1:
			err = decodeReplicatedLowerHex(c, decoded.ClusterIncarnation[:],
				"vibedb: replicated cluster_incarnation must be 128-bit lowercase hexadecimal",
				"vibedb: replicated cluster_incarnation")
		case 2:
			err = c.Uint64(&decoded.TopologyRecoveryEpoch)
		case 3:
			err = c.String(&decoded.Distribution)
		case 4:
			err = c.String(&decoded.Shard)
		case 5:
			err = c.Uint64(&decoded.AllocationGeneration)
		case 6:
			err = decodeReplicatedLowerHex(c, decoded.ShardIncarnation[:],
				"vibedb: replicated shard_incarnation must be 128-bit lowercase hexadecimal",
				"vibedb: replicated shard_incarnation")
		case 7:
			err = decodeReplicatedLowerHex(c, decoded.GroupID[:],
				"vibedb: replicated group_id must be 128-bit lowercase hexadecimal",
				"vibedb: replicated group_id")
		case 8:
			err = c.Uint64(&decoded.MemberID)
		case 9:
			err = decodeReplicatedLowerHex(c, decoded.StoreID[:],
				"vibedb: replicated store_id must be 128-bit lowercase hexadecimal",
				"vibedb: replicated store_id")
		case 10:
			err = checkReplicatedValueByteBound(
				c, maxReplicatedAuthorityJSONBytes,
				"vibedb: replicated authority profile exceeds its byte bound",
			)
			if err == nil {
				err = decodeReplicatedAuthorityVibe(c, &decoded.Authority)
			}
		}
		if err != nil {
			return err
		}
	}
	if err := requireReplicatedFields(seen, replicatedBindingFieldNames[:], "replicated shard store binding"); err != nil {
		return err
	}
	if err := validateReplicatedShardStoreBinding(decoded); err != nil {
		return err
	}
	*dst = decoded
	return nil
}

func decodeReplicatedLimitsVibe(
	c *vibejson.DecodeCursor,
	dst *ReplicatedShardStoreLimits,
) error {
	var decoded ReplicatedShardStoreLimits
	if err := decodeReplicatedSystemLimitsVibe(c, &decoded); err != nil {
		return err
	}
	if err := validateReplicatedShardStoreLimits(decoded); err != nil {
		return err
	}
	*dst = decoded
	return nil
}

func decodeReplicatedRelationVibe(
	c *vibejson.DecodeCursor,
	dst *ReplicatedShardRelationIdentity,
) error {
	if err := c.BeginObject("replicated shard relation"); err != nil {
		return errors.New("vibedb: replicated shard relation must be a JSON object")
	}
	var decoded ReplicatedShardRelationIdentity
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		index, known := replicatedRelationFields.Lookup(name, true)
		if !known {
			return unknownCatalogMember("replicated shard relation", name)
		}
		if err := markReplicatedCatalogField(&seen, index, "replicated shard relation", name); err != nil {
			return err
		}
		switch index {
		case 0:
			err = c.Uint16(&decoded.Relation)
		case 1:
			var kind uint8
			err = c.Uint8(&kind)
			decoded.Kind = ReplicatedShardRelationKind(kind)
		case 2:
			err = c.String(&decoded.Table)
		case 3:
			err = c.String(&decoded.Storage)
		case 4:
			err = checkReplicatedValueByteBound(
				c, maxReplicatedLimitsJSONBytes,
				"vibedb: replicated shard store limits exceed their byte bound",
			)
			if err == nil {
				err = decodeReplicatedLimitsVibe(c, &decoded.Limits)
			}
		case 5:
			err = decodeReplicatedLowerHex(c, decoded.LocalIndexDigest[:],
				"vibedb: replicated local_index_digest must be lowercase SHA-256 hexadecimal",
				"vibedb: replicated local_index_digest")
		case 6:
			err = c.Uint64(&decoded.IndexID)
		case 7:
			err = c.Uint64(&decoded.Incarnation)
		case 8:
			err = c.Uint8(&decoded.LocatorCount)
		case 9:
			err = c.Bool(&decoded.Unique)
		case 10:
			var encoding uint8
			err = c.Uint8(&encoding)
			decoded.KeyEncoding = ReplicatedRelationKeyEncoding(encoding)
		case 11:
			err = c.Uint8(&decoded.KeyArity)
		case 12:
			var version uint32
			err = c.Uint32(&version)
			decoded.TupleVersion = distribution.TupleVersion(version)
		case 13:
			var version uint32
			err = c.Uint32(&version)
			decoded.MapperVersion = distribution.MapperVersion(version)
		case 14:
			err = c.Uint8(&decoded.BucketBits)
		}
		if err != nil {
			return err
		}
	}
	if err := requireReplicatedFields(seen, replicatedRelationFieldNames[:], "replicated shard relation"); err != nil {
		return err
	}
	if err := validateReplicatedRelationGrammar(decoded); err != nil {
		return err
	}
	*dst = decoded
	return nil
}

func decodeReplicatedStoreIdentityVibe(
	c *vibejson.DecodeCursor,
	dst *ReplicatedShardStoreIdentity,
) error {
	if err := c.BeginObject("replicated shard store identity"); err != nil {
		return errors.New("vibedb: replicated shard store identity must be a JSON object")
	}
	var decoded ReplicatedShardStoreIdentity
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		index, known := replicatedStoreFields.Lookup(name, true)
		if !known {
			return unknownCatalogMember("replicated shard store identity", name)
		}
		if err := markReplicatedCatalogField(&seen, index, "replicated shard store identity", name); err != nil {
			return err
		}
		switch index {
		case 0:
			err = decodeRequiredReplicatedUint16(c, "replicated shard store format", &decoded.Format)
		case 1:
			err = checkReplicatedValueByteBound(
				c, maxReplicatedBindingJSONBytes,
				"vibedb: replicated shard store binding exceeds its byte bound",
			)
			if err == nil {
				err = decodeReplicatedBindingVibe(c, &decoded.Binding)
			}
		case 2:
			err = decodeReplicatedLowerHex(c, decoded.LogID[:],
				"vibedb: replicated log_id must be 128-bit lowercase hexadecimal",
				"vibedb: replicated log_id")
		case 3:
			err = c.String(&decoded.UserTable)
		case 4:
			err = c.String(&decoded.UserStorage)
		case 5:
			err = c.String(&decoded.UserPrimaryKey)
		case 6:
			err = checkReplicatedValueByteBound(
				c, maxReplicatedLimitsJSONBytes,
				"vibedb: replicated shard store limits exceed their byte bound",
			)
			if err == nil {
				err = decodeReplicatedLimitsVibe(c, &decoded.UserLimits)
			}
		case 7:
			err = checkReplicatedValueByteBound(
				c, maxReplicatedSidecarsJSONBytes,
				"vibedb: replicated shard store sidecars exceed their byte bound",
			)
			if err == nil {
				err = decodeReplicatedShardSidecarsVibe(c, &decoded.Sidecars)
			}
		case 8:
			err = c.Uint64(&decoded.RelationSchemaGeneration)
		case 9:
			err = decodeReplicatedLowerHex(c, decoded.RelationManifestDigest[:],
				"vibedb: replicated relation_manifest_digest must be lowercase SHA-256 hexadecimal",
				"vibedb: replicated relation_manifest_digest")
		case 10:
			err = decodeReplicatedRelationArrayVibe(c, &decoded)
		}
		if err != nil {
			return err
		}
	}
	if err := requireReplicatedFields(seen, replicatedStoreFieldNames[:], "replicated shard store identity"); err != nil {
		return err
	}
	if err := validateReplicatedShardStoreIdentity(decoded); err != nil {
		return err
	}
	*dst = decoded
	return nil
}

func decodeReplicatedRelationArrayVibe(
	c *vibejson.DecodeCursor,
	dst *ReplicatedShardStoreIdentity,
) error {
	if err := c.BeginArray("replicated relations"); err != nil {
		return errors.New("vibedb: replicated relations must be an array")
	}
	var relations [replication.MaxRelationsPerBundle]ReplicatedShardRelationIdentity
	count := 0
	for first := true; ; first = false {
		more, err := c.NextElement(first)
		if err != nil {
			return err
		}
		if !more {
			break
		}
		if count >= len(relations) {
			return fmt.Errorf(
				"vibedb: replicated relation manifest exceeds %d slots", len(relations),
			)
		}
		// Probe and bound the encoded element before any escaped table or storage
		// text is materialized. DecodeCursor copies are detached parser state, so
		// Raw validates the exact element without advancing the live cursor or
		// retaining decoded strings. The live decode then keeps one arena for the
		// entire catalog instead of allocating one decoder per relation.
		if err := checkReplicatedValueByteBound(
			c, maxReplicatedRelationJSONBytes,
			"vibedb: replicated shard relation exceeds its byte bound",
		); err != nil {
			return err
		}
		if err := decodeReplicatedRelationVibe(c, &relations[count]); err != nil {
			return err
		}
		count++
	}
	dst.RelationCount = uint16(count)
	dst.Relations = make([]ReplicatedShardRelationIdentity, count)
	copy(dst.Relations, relations[:count])
	return nil
}

func checkReplicatedValueByteBound(
	c *vibejson.DecodeCursor,
	maximum int,
	message string,
) error {
	if c == nil || maximum <= 0 {
		return errors.New(message)
	}
	probe := *c
	raw, err := probe.Raw()
	if err != nil {
		return err
	}
	if len(raw.Bytes()) > maximum {
		return errors.New(message)
	}
	return nil
}

func decodeBoundedReplicatedRelation(
	data []byte,
	dst *ReplicatedShardRelationIdentity,
) error {
	if len(data) > maxReplicatedRelationJSONBytes {
		return errors.New("vibedb: replicated shard relation exceeds its byte bound")
	}
	var decoded replicatedRelationVibe
	if err := vibejson.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*dst = ReplicatedShardRelationIdentity(decoded)
	return nil
}

func decodeReplicatedShardSidecarsVibe(
	c *vibejson.DecodeCursor,
	dst *ReplicatedShardStoreSidecarProfile,
) error {
	var decoded replicatedShardStoreSidecarVibe
	next, err := decoded.UnmarshalVibeJSON(*c)
	*c = next
	if err != nil {
		return err
	}
	*dst = ReplicatedShardStoreSidecarProfile(decoded)
	return nil
}

func requireReplicatedFields(seen uint64, names []string, kind string) error {
	for index, name := range names {
		if seen&(uint64(1)<<index) == 0 {
			return fmt.Errorf("vibedb: %s is missing member %q", kind, name)
		}
	}
	return nil
}
