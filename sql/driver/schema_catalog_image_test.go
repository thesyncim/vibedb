package driver

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func TestReplicatedSchemaCatalogImageCanonicalWitness(t *testing.T) {
	path, database, identity := bindReplicatedApplyTestRoot(t, "schema-catalog-image")
	claim, _, err := database.OpenReplicatedApply(
		identity, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	witness, err := ValidateReplicatedSchemaCatalogImage(raw)
	if err != nil {
		t.Fatal(err)
	}
	if witness.Bytes != uint64(len(raw)) || witness.Digest == ([32]byte{}) ||
		witness.SchemaGeneration != identity.RelationSchemaGeneration ||
		witness.RelationManifestDigest != replicatedRelationApplyManifestDigest(identity) ||
		witness.LocalRelationManifestDigest != identity.RelationManifestDigest ||
		witness.ApplyProfileDigest == ([32]byte{}) {
		t.Fatalf("catalog image witness = %+v", witness)
	}

	// The decoder accepts insignificant whitespace, but a rollout bundle must
	// have exactly one authenticated representation.
	noncanonical := append([]byte{' ', '\n'}, raw...)
	if _, err = ValidateReplicatedSchemaCatalogImage(noncanonical); !errors.Is(err, ErrReplicatedSchemaCatalogImage) {
		t.Fatalf("noncanonical image error = %v", err)
	}
	changed := bytes.Clone(raw)
	changed[len(changed)-1] = ' '
	if _, err = ValidateReplicatedSchemaCatalogImage(changed); !errors.Is(err, ErrReplicatedSchemaCatalogImage) {
		t.Fatalf("truncated image error = %v", err)
	}
	if err = claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedSchemaCatalogImageRejectsLocalCatalog(t *testing.T) {
	raw := testCanonicalCatalogImage(t)
	if _, err := ValidateReplicatedSchemaCatalogImage(raw); !errors.Is(err, ErrReplicatedSchemaCatalogImage) {
		t.Fatalf("local catalog error = %v", err)
	}
}
