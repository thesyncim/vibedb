package clusterrestore

import "testing"

func TestServingAuthorityRequiresCompleteCatalogObservedActivation(t *testing.T) {
	operation := restoreOperationFixture(t, 3)
	roots := servingRootsFixture(operation)
	catalog := makeCatalogWitness(operation, roots)
	permit := makeServingPermit(operation, catalog)
	authority, err := NewServingAuthority(operation, roots, catalog, permit)
	if err != nil {
		t.Fatal(err)
	}
	first := operation.Targets[0].Replicas[0]
	if !authority.AllowsReplica(operation.Targets[0].Group, first.Member, first.Store,
		first.NodeIncarnation) {
		t.Fatal("fresh active replica denied")
	}
	if authority.AllowsReplica(operation.Targets[0].Group, first.Member, first.Store, 2) ||
		authority.AllowsReplica(operation.Certificate.Groups[0].Group, first.Member, first.Store, 1) {
		t.Fatal("stale incarnation or source group admitted")
	}
	grants, err := authority.Grants()
	if err != nil || len(grants) != len(operation.Targets)*3 {
		t.Fatalf("grants=%d err=%v", len(grants), err)
	}
	for index, grant := range grants {
		group, replica := index/3, index%3
		want := operation.Targets[group]
		if grant.Group() != want.Group || grant.Member() != uint64(replica+1) ||
			grant.Node() != want.Replicas[replica].Node || grant.Store() != want.Replicas[replica].Store {
			t.Fatalf("grant %d does not preserve canonical target order", index)
		}
	}

	for name, mutate := range map[string]func(*[]RootWitness, *CatalogWitness, *ServingPermit){
		"partial": func(roots *[]RootWitness, _ *CatalogWitness, _ *ServingPermit) {
			*roots = (*roots)[:len(*roots)-1]
		},
		"forged-root": func(roots *[]RootWitness, _ *CatalogWitness, _ *ServingPermit) {
			(*roots)[1].GenesisProof[0] ^= 1
		},
		"stale-catalog": func(_ *[]RootWitness, catalog *CatalogWitness, _ *ServingPermit) {
			catalog.CatalogDigest[0] ^= 1
		},
		"local-permit-only": func(_ *[]RootWitness, _ *CatalogWitness, permit *ServingPermit) {
			permit.CatalogWitness[0] ^= 1
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidateRoots := append([]RootWitness(nil), roots...)
			candidateCatalog, candidatePermit := catalog, permit
			mutate(&candidateRoots, &candidateCatalog, &candidatePermit)
			if _, err := NewServingAuthority(operation, candidateRoots, candidateCatalog, candidatePermit); err == nil {
				t.Fatal("accepted incomplete or forged activation")
			}
		})
	}
}
