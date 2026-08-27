package replicatedstate

import (
	"crypto/sha256"
	"strings"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store"
)

// InitialJSONRelationManifest hashes a singleton schema with the same grammar
// as OpenBundle. It validates schema inputs only: no collection is opened and
// the result does not certify storage, durability, membership, or serving.
func InitialJSONRelationManifest(schemaGeneration uint64, name string,
	limits CollectionLimits, validationDigest [sha256.Size]byte,
	indexes []store.IndexDefinition,
) ([sha256.Size]byte, error) {
	if schemaGeneration == 0 || name == "" || len(name) > replication.MaxIdentityBytes ||
		!utf8.ValidString(name) || strings.IndexByte(name, 0) >= 0 || name == systemCollectionName ||
		validationDigest == ([sha256.Size]byte{}) || limits.MaxKeyBytes <= 0 ||
		limits.MaxKeyBytes > replication.MaxMutationKeyBytes || limits.MaxDocumentBytes <= 0 ||
		limits.MaxDocumentBytes > replication.MaxMutationValueBytes || limits.MaxDistinctMutations <= 0 ||
		limits.MaxDistinctMutations > MaxDistinctMutations || limits.MaxBatchBytes <= 0 {
		return [sha256.Size]byte{}, ErrInvalidCollection
	}
	canonical, err := canonicalLocalIndexes(indexes)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	relations := [1]relationCollection{{id: 1, kind: RelationJSON, name: name,
		target: CollectionTarget{Validation: ValidationDeterministicMutation,
			ValidationDigest: validationDigest, Limits: limits},
		localIndexes: canonical,
	}}
	return relationManifestDigest(schemaGeneration, relations[:]), nil
}
