package clusterrestore

import (
	"bytes"
	"testing"
)

func TestServingGrantCanOnlyBeMintedForExactActivatedReplica(t *testing.T) {
	operation := restoreOperationFixture(t, 2)
	roots := servingRootsFixture(operation)
	catalog := makeCatalogWitness(operation, roots)
	authority, err := NewServingAuthority(operation, roots, catalog, makeServingPermit(operation, catalog))
	if err != nil {
		t.Fatal(err)
	}
	grant, err := authority.Grant(operation.Targets[1].Group, 2)
	if err != nil {
		t.Fatal(err)
	}
	want := operation.Targets[1].Replicas[1]
	if grant.Operation() != operation.Digest || grant.CatalogWitness() != catalog.CatalogDigest ||
		grant.Group() != operation.Targets[1].Group || grant.Member() != want.Member ||
		grant.Node() != want.Node || grant.Store() != want.Store ||
		grant.NodeIncarnation() != want.NodeIncarnation {
		t.Fatalf("grant mismatch")
	}
	raw, err := AppendServingGrant(nil, grant)
	opened, openErr := OpenServingGrant(raw)
	canonical, canonicalErr := AppendServingGrant(nil, opened)
	if err != nil || openErr != nil || canonicalErr != nil || !bytes.Equal(raw, canonical) ||
		opened.Digest() != grant.Digest() {
		t.Fatalf("canonical=%t append=%v open=%v reappend=%v", bytes.Equal(raw, canonical), err, openErr, canonicalErr)
	}
	if _, err := authority.Grant(operation.Targets[1].Group, 4); err == nil {
		t.Fatal("minted grant for absent member")
	}
	corrupt := bytes.Clone(raw)
	corrupt[len(corrupt)/2] ^= 1
	if _, err := OpenServingGrant(corrupt); err == nil {
		t.Fatal("accepted corrupt grant")
	}
}
