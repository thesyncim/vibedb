package replicatedstate

import (
	"github.com/thesyncim/vibedb/internal/replication"
	"io"
)

// RelationCollectionManifestDigest uses the same validated relation grammar as
// OpenBundle without initializing a machine or changing any collection.
func RelationCollectionManifestDigest(binding Binding, relations []RelationCollection) ([32]byte, error) {
	_, digest, err := prepareRelationCollections(binding, relations)
	return digest, err
}

// RestoreImageProfile supplies only the new image hash domain. It grants no
// authority; the enclosing restore operation authenticates the source artifact
// and target schema independently.
type RestoreImageProfile struct {
	Name             string
	ValidationDigest [32]byte
}

// RehashSnapshotArtifact verifies the complete source artifact while hashing
// its unchanged rows under fresh target validation domains. Memory is bounded
// by the verifier chunk plus the fixed relation directory, never row count.
func RehashSnapshotArtifact(reader io.Reader, profiles []RestoreImageProfile) (SnapshotArtifactManifest, [32]byte, error) {
	if len(profiles) == 0 || len(profiles) > replication.MaxRelationsPerBundle {
		return SnapshotArtifactManifest{}, [32]byte{}, ErrInvalidCollection
	}
	hashers := make([]*canonicalImageHasher, len(profiles))
	for i, p := range profiles {
		var err error
		hashers[i], err = newCanonicalImageHasher(p.Name, ValidationDeterministicMutation, p.ValidationDigest, projectionDigestValidator{})
		if err != nil {
			return SnapshotArtifactManifest{}, [32]byte{}, err
		}
	}
	manifest, err := VerifySnapshotArtifact(reader, SnapshotArtifactCallbacks{Rows: func(_ SnapshotArtifactCheckpoint, rows SnapshotArtifactRows) error {
		if rows.Collection() != SnapshotArtifactUser {
			return nil
		}
		i := int(rows.Relation()) - 1
		if len(profiles) == 1 && rows.Relation() == 0 {
			i = 0
		}
		if i < 0 || i >= len(hashers) {
			return ErrInvalidCollection
		}
		iterator := rows.Iterator()
		for {
			key, value, ok := iterator.Next()
			if !ok {
				break
			}
			if err := hashers[i].add(key, value); err != nil {
				return err
			}
		}
		return nil
	}})
	if err != nil {
		return manifest, [32]byte{}, err
	}
	if manifest.Bundle && len(manifest.Relations) != len(profiles) || !manifest.Bundle && len(profiles) != 1 {
		return manifest, [32]byte{}, ErrInvalidCollection
	}
	relations := make([]SnapshotArtifactRelation, len(profiles))
	for i, p := range profiles {
		if manifest.Bundle && string(manifest.Relations[i].Collection) != p.Name || !manifest.Bundle && string(manifest.UserCollection) != p.Name {
			return manifest, [32]byte{}, ErrInvalidCollection
		}
		relations[i].Relation = replication.RelationID(i + 1)
		relations[i].ImageDigest = hashers[i].sum()
	}
	return manifest, canonicalBundleImageDigest(relations), nil
}
