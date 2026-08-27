package driver

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"unicode/utf8"
)

// ErrReplicatedSchemaCatalogImage identifies a schema-rollout bundle that is
// not the one canonical, fully validated SQL catalog image for a replicated
// shard. Callers must not repair or normalize such an image: its exact bytes
// are authenticated by the rollout receipt.
var ErrReplicatedSchemaCatalogImage = errors.New("vibedb: invalid replicated schema catalog image")

// ReplicatedSchemaCatalogImage is the bounded public witness returned for an
// opaque rollout bundle. It deliberately exposes no mutable catalog metadata.
// RelationManifestDigest is the exact serving-machine schema used by Raft,
// including placement and authenticated index validation; the
// LocalRelationManifestDigest additionally authenticates this replica's exact
// storage identities.
type ReplicatedSchemaCatalogImage struct {
	Bytes                       uint64
	Digest                      [sha256.Size]byte
	SchemaGeneration            uint64
	RelationManifestDigest      [sha256.Size]byte
	LocalRelationManifestDigest [sha256.Size]byte
	ApplyProfileDigest          [sha256.Size]byte
}

// MatchesRolloutTarget compares the fields a shard installer receives outside
// the opaque catalog bytes. Apply-contract matching is intentionally separate:
// it must use SchemaApplyContractDigest from the materialized target machine,
// never the catalog's validation-profile digest.
func (image ReplicatedSchemaCatalogImage) MatchesRolloutTarget(
	bytes uint64,
	digest [sha256.Size]byte,
	schemaGeneration uint64,
	relationManifest [sha256.Size]byte,
) bool {
	return image.Bytes != 0 && image.Bytes == bytes &&
		image.Digest != ([sha256.Size]byte{}) && image.Digest == digest &&
		image.SchemaGeneration != 0 && image.SchemaGeneration == schemaGeneration &&
		image.RelationManifestDigest != ([sha256.Size]byte{}) &&
		image.RelationManifestDigest == relationManifest
}

// ValidateReplicatedSchemaCatalogImage admits only a byte-unique canonical
// vibejson catalog. Decode performs the existing allocation, depth, key,
// collection-count, descriptor, and replicated-sidecar bounds; re-encoding
// proves that whitespace, alternate escapes, reordered members, and other
// equivalent-but-noncanonical byte strings cannot acquire a receipt.
func ValidateReplicatedSchemaCatalogImage(raw []byte) (ReplicatedSchemaCatalogImage, error) {
	_, image, err := openReplicatedSchemaCatalogImage(raw)
	return image, err
}

func openReplicatedSchemaCatalogImage(
	raw []byte,
) (catalogFile, ReplicatedSchemaCatalogImage, error) {
	if len(raw) == 0 || len(raw) > maxCatalogBytes || !utf8.Valid(raw) {
		return catalogFile{}, ReplicatedSchemaCatalogImage{}, ErrReplicatedSchemaCatalogImage
	}
	var decoded catalogFileVibe
	if err := decodeCatalogJSON(raw, &decoded); err != nil {
		return catalogFile{}, ReplicatedSchemaCatalogImage{}, errors.Join(ErrReplicatedSchemaCatalogImage, err)
	}
	catalog := catalogFile(decoded)
	if catalog.Version != catalogVersion || catalog.Tables == nil ||
		catalog.ReplicatedShardStore == nil || catalog.ReplicatedApply == nil {
		return catalogFile{}, ReplicatedSchemaCatalogImage{}, ErrReplicatedSchemaCatalogImage
	}
	bound, err := catalogSizeUpperBound(catalog)
	if err != nil {
		return catalogFile{}, ReplicatedSchemaCatalogImage{}, errors.Join(ErrReplicatedSchemaCatalogImage, err)
	}
	canonical, err := appendCatalogJSON(make([]byte, 0, bound), catalog)
	if err != nil {
		return catalogFile{}, ReplicatedSchemaCatalogImage{}, errors.Join(ErrReplicatedSchemaCatalogImage, err)
	}
	if !bytes.Equal(raw, canonical) {
		return catalogFile{}, ReplicatedSchemaCatalogImage{}, fmt.Errorf(
			"%w: catalog bytes are not canonical", ErrReplicatedSchemaCatalogImage,
		)
	}
	identity := *catalog.ReplicatedShardStore
	manifest, err := ReplicatedSchemaManifest(identity, catalog.ReplicatedApply.Placement,
		replicatedApplyLocalIndexes(&table{meta: catalog.Tables[identity.UserTable]}))
	if err != nil {
		return catalogFile{}, ReplicatedSchemaCatalogImage{}, errors.Join(ErrReplicatedSchemaCatalogImage, err)
	}
	return catalog, ReplicatedSchemaCatalogImage{
		Bytes: uint64(len(raw)), Digest: sha256.Sum256(raw),
		SchemaGeneration:            identity.RelationSchemaGeneration,
		RelationManifestDigest:      manifest,
		LocalRelationManifestDigest: identity.RelationManifestDigest,
		ApplyProfileDigest:          catalog.ReplicatedApply.ValidationDigest,
	}, nil
}

// ValidateReplicatedSchemaCatalogTarget additionally proves that raw is the
// immediate schema successor of this live shard, without allowing a schema
// rollout to smuggle a topology, ownership, WAL, member, store, or shard-root
// replacement through the data-plane catalog bundle.
func (d *Database) ValidateReplicatedSchemaCatalogTarget(
	raw []byte,
) (ReplicatedSchemaCatalogImage, error) {
	target, image, err := openReplicatedSchemaCatalogImage(raw)
	if err != nil {
		return ReplicatedSchemaCatalogImage{}, err
	}
	if d == nil || d.connector == nil {
		return ReplicatedSchemaCatalogImage{}, ErrDatabaseClosed
	}
	d.connector.mu.Lock()
	if d.connector.closed || d.connector.db == nil {
		d.connector.mu.Unlock()
		return ReplicatedSchemaCatalogImage{}, ErrDatabaseClosed
	}
	core := d.connector.db
	core.mu.RLock()
	d.connector.mu.Unlock()
	defer core.mu.RUnlock()
	if core.closed {
		return ReplicatedSchemaCatalogImage{}, ErrDatabaseClosed
	}
	current := core.catalog.ReplicatedShardStore
	if current == nil || core.catalog.ReplicatedApply == nil ||
		current.RelationSchemaGeneration == ^uint64(0) {
		return ReplicatedSchemaCatalogImage{}, ErrReplicatedSchemaCatalogImage
	}
	wantBinding := current.Binding
	wantBinding.Authority.SchemaGeneration++
	next := target.ReplicatedShardStore
	if next.Binding != wantBinding || next.LogID != current.LogID ||
		next.RelationSchemaGeneration != current.RelationSchemaGeneration+1 ||
		next.UserTable != current.UserTable ||
		target.ShardStore == nil || core.catalog.ShardStore == nil ||
		*target.ShardStore != *core.catalog.ShardStore {
		return ReplicatedSchemaCatalogImage{}, ErrReplicatedSchemaCatalogImage
	}
	return image, nil
}
