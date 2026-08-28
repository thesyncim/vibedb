package replicatedstate

import (
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestInitialRelationManifestDoesNotBypassLiveCollectionValidation(t *testing.T) {
	limits := CollectionLimits{MaxKeyBytes: 256, MaxDocumentBytes: 1024, MaxDistinctMutations: 8, MaxBatchBytes: 16384}
	spec := RelationCollection{Relation: 1, Kind: RelationJSON, Name: "docs",
		Target:       CollectionTarget{Validation: ValidationDeterministicMutation, ValidationDigest: [32]byte{7}, Limits: limits},
		LocalIndexes: []store.IndexDefinition{{Name: "by_kind", Paths: []string{"/kind"}}}}
	got, err := InitialRelationManifest(11, []RelationCollection{spec})
	if err != nil {
		t.Fatal(err)
	}
	want, err := InitialJSONRelationManifest(11, spec.Name, limits, spec.Target.ValidationDigest, spec.LocalIndexes)
	if err != nil || got != want {
		t.Fatalf("singleton grammar drift: %v", err)
	}
	if _, _, err := prepareRelationCollections(Binding{SchemaGeneration: 11}, []RelationCollection{spec}); err == nil {
		t.Fatal("cold schema inputs were accepted as live durable handles")
	}
	for _, mutate := range []func(*RelationCollection){
		func(s *RelationCollection) { s.Relation++ }, func(s *RelationCollection) { s.Target.ValidationDigest = [32]byte{} },
		func(s *RelationCollection) { s.Target.Limits.MaxBatchBytes = 0 }, func(s *RelationCollection) { s.Kind = RelationGlobalIndex },
	} {
		bad := spec
		mutate(&bad)
		if _, err := InitialRelationManifest(11, []RelationCollection{bad}); err == nil {
			t.Fatal("invalid cold schema accepted")
		}
	}
}
