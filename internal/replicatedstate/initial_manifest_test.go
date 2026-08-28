package replicatedstate

import (
	"crypto/sha256"
	"testing"
)

func TestInitialJSONRelationManifestMatchesPreparedCollection(t *testing.T) {
	fixture := newRelationBundleFixture(t, false)
	spec := relationBundleCollections(fixture.base, fixture.global, fixture.index, RelationGlobalIndex)[0]
	_, want, err := prepareRelationCollections(fixture.binding, []RelationCollection{spec})
	if err != nil {
		t.Fatal(err)
	}
	got, err := InitialJSONRelationManifest(fixture.binding.SchemaGeneration, spec.Name, spec.Target.Limits, spec.Target.ValidationDigest, spec.LocalIndexes)
	if err != nil || got == ([sha256.Size]byte{}) || got != want {
		t.Fatalf("initial=%x opened=%x err=%v", got, want, err)
	}
	if _, err = InitialJSONRelationManifest(0, spec.Name, spec.Target.Limits, spec.Target.ValidationDigest, spec.LocalIndexes); err == nil {
		t.Fatal("accepted missing schema generation")
	}
	if _, err = InitialJSONRelationManifest(fixture.binding.SchemaGeneration, spec.Name, spec.Target.Limits, [sha256.Size]byte{}, spec.LocalIndexes); err == nil {
		t.Fatal("accepted missing validation digest")
	}
}
