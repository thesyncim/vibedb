package replicatedstate

import (
	"crypto/sha256"
	"github.com/thesyncim/vibedb/store"
)

// InitialJSONRelationManifest hashes a singleton schema with the same grammar
// as OpenBundle. It validates schema inputs only: no collection is opened and
// the result does not certify storage, durability, membership, or serving.
func InitialJSONRelationManifest(schemaGeneration uint64, name string,
	limits CollectionLimits, validationDigest [sha256.Size]byte,
	indexes []store.IndexDefinition,
) ([sha256.Size]byte, error) {
	relations := [1]RelationCollection{{Relation: 1, Kind: RelationJSON, Name: name,
		Target: CollectionTarget{Validation: ValidationDeterministicMutation,
			ValidationDigest: validationDigest, Limits: limits},
		LocalIndexes: indexes,
	}}
	return InitialRelationManifest(schemaGeneration, relations[:])
}

// InitialRelationManifest computes a complete bundle's serving schema before
// destination storage exists. It shares canonicalization and the digest grammar
// with OpenBundle but grants no storage, durability or serving authority.
func InitialRelationManifest(schemaGeneration uint64, input []RelationCollection) ([sha256.Size]byte, error) {
	if schemaGeneration == 0 {
		return [sha256.Size]byte{}, ErrInvalidCollection
	}
	relations, err := prepareRelationSchemas(input)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return relationManifestDigest(schemaGeneration, relations), nil
}
