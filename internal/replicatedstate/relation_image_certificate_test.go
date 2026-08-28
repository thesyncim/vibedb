package replicatedstate

import (
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

func TestRelationImageCertificateBindsCardinalityAndCanonicalRoots(t *testing.T) {
	fixture := newMachineFixture(t)
	specs := []RelationCollection{{
		Relation: 1, Kind: RelationJSON, Name: "docs", Target: fixture.user,
	}}
	first, err := CertifyRelationImages(fixture.binding, specs)
	if err != nil {
		t.Fatal(err)
	}
	if first.SchemaGeneration != fixture.binding.SchemaGeneration ||
		first.RelationCount != 1 || first.TotalRows != 0 || first.NonEmptyRelations != 0 ||
		first.ManifestDigest == ([32]byte{}) || first.ImageRoot == ([32]byte{}) ||
		first.CardinalityRoot == ([32]byte{}) || first.Witness == ([32]byte{}) {
		t.Fatalf("empty image certificate = %+v", first)
	}
	if err = fixture.user.Collection.Update(func(write *durable.WriteBatch) error {
		return write.Put([]byte("k"), []byte(`{"id":"k"}`))
	}); err != nil {
		t.Fatal(err)
	}
	second, err := CertifyRelationImages(fixture.binding, specs)
	if err != nil {
		t.Fatal(err)
	}
	if second.TotalRows != 1 || second.NonEmptyRelations != 1 ||
		second.ManifestDigest != first.ManifestDigest ||
		second.ImageRoot == first.ImageRoot || second.CardinalityRoot == first.CardinalityRoot ||
		second.Witness == first.Witness {
		t.Fatalf("changed image certificate: before=%+v after=%+v", first, second)
	}
}
