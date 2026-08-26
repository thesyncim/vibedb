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
	if !witness.MatchesRolloutTarget(
		uint64(len(raw)), witness.Digest, identity.RelationSchemaGeneration,
		replicatedRelationApplyManifestDigest(identity),
	) {
		t.Fatal("exact rollout target did not match")
	}
	wrong := witness.RelationManifestDigest
	wrong[0] ^= 1
	if witness.MatchesRolloutTarget(
		uint64(len(raw)), witness.Digest, identity.RelationSchemaGeneration, wrong,
	) {
		t.Fatal("substituted rollout target matched")
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

func TestReplicatedSchemaCatalogTargetFencesNonSchemaLineageChanges(t *testing.T) {
	_, database, identity := bindReplicatedApplyTestRoot(t, "schema-catalog-target")
	claim, _, err := database.OpenReplicatedApply(
		identity, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	core := database.connector.db
	core.mu.RLock()
	raw, err := appendCatalogJSON(nil, core.catalog)
	core.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	var decoded catalogFileVibe
	if err = decodeCatalogJSON(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	target := catalogFile(decoded)
	target.ReplicatedShardStore.Binding.Authority.SchemaGeneration++
	target.ReplicatedShardStore.RelationSchemaGeneration++
	target.ReplicatedShardStore.RelationManifestDigest =
		replicatedRelationManifestDigest(*target.ReplicatedShardStore)
	target.ReplicatedApply.ValidationDigest = replicatedApplyProfileDigest(
		*target.ReplicatedShardStore, target.ReplicatedApply.Placement,
	)
	targetRaw, err := appendCatalogJSON(nil, target)
	if err != nil {
		t.Fatal(err)
	}
	witness, err := database.ValidateReplicatedSchemaCatalogTarget(targetRaw)
	if err != nil || witness.SchemaGeneration != identity.RelationSchemaGeneration+1 ||
		witness.RelationManifestDigest == ([32]byte{}) {
		t.Fatalf("target witness = %+v, %v", witness, err)
	}

	target.ReplicatedShardStore.Binding.Authority.RoutingVersion++
	target.ReplicatedShardStore.RelationManifestDigest =
		replicatedRelationManifestDigest(*target.ReplicatedShardStore)
	target.ReplicatedApply.ValidationDigest = replicatedApplyProfileDigest(
		*target.ReplicatedShardStore, target.ReplicatedApply.Placement,
	)
	targetRaw, err = appendCatalogJSON(nil, target)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = database.ValidateReplicatedSchemaCatalogTarget(targetRaw); !errors.Is(err, ErrReplicatedSchemaCatalogImage) {
		t.Fatalf("topology substitution error = %v", err)
	}
	if err = claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
}
