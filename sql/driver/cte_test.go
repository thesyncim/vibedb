package driver

import (
	"slices"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestCTEPhysicalDependenciesAreRecursiveStableAndUnique(t *testing.T) {
	statement := `WITH
		base AS (
			SELECT id FROM docs
			WHERE id IN (SELECT id FROM permitted)
		),
		reused AS (
			SELECT id FROM base
			WHERE EXISTS (SELECT id FROM docs)
		)
	SELECT id FROM reused
	WHERE EXISTS (SELECT id FROM audit)`
	tree, err := sqlast.Parse(statement)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := selectPhysicalDependencies(tree)
	wantNames := []string{"docs", "permitted", "audit"}
	gotNames := make([]string, len(dependencies))
	for i := range dependencies {
		gotNames[i] = dependencies[i].name
	}
	if !slices.Equal(gotNames, wantNames) {
		t.Fatalf("CTE dependencies = %v, want %v", gotNames, wantNames)
	}
	for i := range dependencies {
		wantPos := strings.Index(statement, dependencies[i].name)
		if dependencies[i].pos != wantPos {
			t.Fatalf(
				"dependency %q position = %d, want first physical reference %d",
				dependencies[i].name, dependencies[i].pos, wantPos,
			)
		}
	}
}
