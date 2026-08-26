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
// RelationManifestDigest is the portable logical manifest used by Raft; the
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

// ValidateReplicatedSchemaCatalogImage admits only a byte-unique canonical
// vibejson catalog. Decode performs the existing allocation, depth, key,
// collection-count, descriptor, and replicated-sidecar bounds; re-encoding
// proves that whitespace, alternate escapes, reordered members, and other
// equivalent-but-noncanonical byte strings cannot acquire a receipt.
func ValidateReplicatedSchemaCatalogImage(raw []byte) (ReplicatedSchemaCatalogImage, error) {
	if len(raw) == 0 || len(raw) > maxCatalogBytes || !utf8.Valid(raw) {
		return ReplicatedSchemaCatalogImage{}, ErrReplicatedSchemaCatalogImage
	}
	var decoded catalogFileVibe
	if err := decodeCatalogJSON(raw, &decoded); err != nil {
		return ReplicatedSchemaCatalogImage{}, errors.Join(ErrReplicatedSchemaCatalogImage, err)
	}
	catalog := catalogFile(decoded)
	if catalog.Version != catalogVersion || catalog.Tables == nil ||
		catalog.ReplicatedShardStore == nil || catalog.ReplicatedApply == nil {
		return ReplicatedSchemaCatalogImage{}, ErrReplicatedSchemaCatalogImage
	}
	bound, err := catalogSizeUpperBound(catalog)
	if err != nil {
		return ReplicatedSchemaCatalogImage{}, errors.Join(ErrReplicatedSchemaCatalogImage, err)
	}
	canonical, err := appendCatalogJSON(make([]byte, 0, bound), catalog)
	if err != nil {
		return ReplicatedSchemaCatalogImage{}, errors.Join(ErrReplicatedSchemaCatalogImage, err)
	}
	if !bytes.Equal(raw, canonical) {
		return ReplicatedSchemaCatalogImage{}, fmt.Errorf(
			"%w: catalog bytes are not canonical", ErrReplicatedSchemaCatalogImage,
		)
	}
	identity := *catalog.ReplicatedShardStore
	return ReplicatedSchemaCatalogImage{
		Bytes: uint64(len(raw)), Digest: sha256.Sum256(raw),
		SchemaGeneration:            identity.RelationSchemaGeneration,
		RelationManifestDigest:      replicatedRelationApplyManifestDigest(identity),
		LocalRelationManifestDigest: identity.RelationManifestDigest,
		ApplyProfileDigest:          catalog.ReplicatedApply.ValidationDigest,
	}, nil
}
