package driver

import (
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
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

func TestNestedSelfJoinRequiresCatalogDespiteOnePhysicalDependency(t *testing.T) {
	tree, err := sqlast.Parse(`
		SELECT d.id
		FROM (
			SELECT left_docs.id
			FROM docs AS left_docs
			JOIN docs AS right_docs ON left_docs.id = right_docs.id
		) AS d`)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := selectPhysicalDependencies(tree)
	if len(dependencies) != 1 || dependencies[0].name != "docs" {
		t.Fatalf("self-join dependencies = %+v, want one docs dependency", dependencies)
	}
	if !selectContainsJoin(tree) {
		t.Fatal("nested self-join was classified as direct single-source execution")
	}
}

func TestSinglePhysicalDerivedDependencyKeepsDirectSource(t *testing.T) {
	const text = `SELECT d.id FROM (` +
		`SELECT id FROM docs WHERE id = ?` +
		`) AS d`
	tree, err := sqlast.ParseStatement(text)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := query.PrepareParsedStatement(text, tree.Select)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Release()
	statement := stmt{
		tree:         tree,
		query:        prepared,
		dependencies: selectPhysicalDependencies(tree.Select),
		catalogJoin:  selectContainsJoin(tree.Select),
	}
	if !prepared.RequiresCatalog() {
		t.Fatal("derived statement unexpectedly stopped reporting nested execution")
	}
	if got := prepared.Collection(); got != "docs" {
		t.Fatalf("derived physical driving collection = %q, want docs", got)
	}
	if got := len(statement.dependencies); got != 1 {
		t.Fatalf("derived physical dependency count = %d, want 1", got)
	}
	if statement.requiresCatalogSource() {
		t.Fatal("single physical derived dependency selected heap catalog materialization")
	}
}

func TestAbsentCTECatalogClassificationIsAllocationFree(t *testing.T) {
	statement := prepareAbsentCTEClassificationStatement(t)
	defer statement.query.Release()
	if statement.tree.Select.With != nil || statement.dependencies != nil {
		t.Fatal("ordinary statement retained CTE/dependency state")
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if statement.requiresCatalogSource() {
			panic("ordinary statement requires a catalog")
		}
	}); allocs != 0 {
		t.Fatalf("absent-CTE classification allocated %.2f times, want zero", allocs)
	}
}

func BenchmarkAbsentCTECatalogClassification(b *testing.B) {
	statement := prepareAbsentCTEClassificationStatement(b)
	defer statement.query.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if statement.requiresCatalogSource() {
			b.Fatal("ordinary statement requires a catalog")
		}
	}
}

type testOrBenchmark interface {
	Helper()
	Fatal(args ...any)
}

func prepareAbsentCTEClassificationStatement(tb testOrBenchmark) *stmt {
	tb.Helper()
	const text = `SELECT id FROM docs WHERE id = ?`
	tree, err := sqlast.ParseStatement(text)
	if err != nil {
		tb.Fatal(err)
	}
	prepared, err := query.PrepareParsedStatement(text, tree.Select)
	if err != nil {
		tb.Fatal(err)
	}
	return &stmt{tree: tree, query: prepared}
}
