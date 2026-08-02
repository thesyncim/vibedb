package query

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
)

type sqlViewMap map[string]SQLViewDefinition

func (m sqlViewMap) ResolveSQLView(name string) (SQLViewDefinition, bool, error) {
	definition, ok := m[name]
	return definition, ok, nil
}

func TestSQLViewExpansionNestedAliasesExecuteDifferentially(t *testing.T) {
	views := sqlViewMap{
		"active": {
			Name:  "active",
			Query: `SELECT id, score FROM docs WHERE active = true`,
		},
		"ranked": {
			Name:    "ranked",
			Query:   `SELECT id, score FROM active WHERE score >= 2`,
			Columns: []string{"doc_id", "points"},
		},
	}
	const source = `SELECT r.doc_id, r.points FROM ranked AS r ORDER BY r.doc_id`
	tree, err := sqlast.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	expansion, err := ExpandSQLViews(source, tree, views, SQLViewExpansionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := expansion.Dependencies, []SQLViewDependency{
		{Name: "ranked", Pos: strings.Index(source, "ranked")},
		{Name: "active", Pos: strings.Index(source, "ranked")},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("dependencies = %+v, want %+v", got, want)
	}
	outer := &tree.From[0]
	if outer.Kind != sqlast.RelationDerived || outer.Name != "" ||
		outer.Alias != "r" || outer.Query == nil {
		t.Fatalf("outer expansion = %+v", *outer)
	}
	inner := &outer.Query.From[0]
	if inner.Kind != sqlast.RelationDerived || inner.Query == nil ||
		outer.Query.Columns[0].Alias != "doc_id" ||
		outer.Query.Columns[1].Alias != "points" {
		t.Fatalf("nested expansion = ref %+v columns %+v", *inner, outer.Query.Columns)
	}

	expanded, err := PrepareParsedStatement(source, tree)
	if err != nil {
		t.Fatal(err)
	}
	defer expanded.Release()
	direct, err := PrepareStatement(
		`SELECT r.doc_id, r.points FROM (` +
			`SELECT id AS doc_id, score AS points FROM (` +
			`SELECT id, score FROM docs WHERE active = true` +
			`) AS active WHERE score >= 2` +
			`) AS r ORDER BY r.doc_id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Release()

	var database store.Database
	docs, err := database.CreateCollection("docs", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for key, document := range map[string]string{
		"a": `{"id":"a","score":1,"active":true}`,
		"b": `{"id":"b","score":2,"active":true}`,
		"c": `{"id":"c","score":3,"active":false}`,
		"d": `{"id":"d","score":4,"active":true}`,
	} {
		if _, err := docs.Put(key, []byte(document)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := database.Snapshot()
	got := runStatement(t, expanded, FromDatabase(snapshot, expanded.Collection()))
	want := runStatement(t, direct, FromDatabase(snapshot, direct.Collection()))
	if got != want {
		t.Fatalf("expanded result = %q, direct = %q", got, want)
	}
}

func TestSQLViewExpansionFailureNeverPublishesPartialAST(t *testing.T) {
	views := sqlViewMap{
		"good": {Name: "good", Query: `SELECT id FROM docs`},
		"bad":  {Name: "bad", Query: `SELECT id FROM bad`},
	}
	const source = `SELECT g.id FROM good AS g JOIN bad AS b ON g.id = b.id`
	tree, err := sqlast.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	good, bad := tree.From[0], tree.From[1]
	_, err = ExpandSQLViews(source, tree, views, SQLViewExpansionOptions{})
	if !errors.Is(err, ErrSQLViewCycle) {
		t.Fatalf("error = %v, want ErrSQLViewCycle", err)
	}
	if !reflect.DeepEqual(tree.From[0], good) || !reflect.DeepEqual(tree.From[1], bad) {
		t.Fatalf("failed expansion mutated root: %+v", tree.From)
	}

	canceled := errors.New("view expansion canceled")
	checks := 0
	_, err = ExpandSQLViews(source, tree, sqlViewMap{
		"good": {Name: "good", Query: `SELECT id FROM docs`},
		"bad":  {Name: "bad", Query: `SELECT id FROM docs`},
	}, SQLViewExpansionOptions{Check: func() error {
		checks++
		if checks >= 5 {
			return canceled
		}
		return nil
	}})
	if !errors.Is(err, canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
	if !reflect.DeepEqual(tree.From[0], good) || !reflect.DeepEqual(tree.From[1], bad) {
		t.Fatalf("canceled expansion mutated root: %+v", tree.From)
	}

	delete(views, "bad")
	expansion, err := ExpandSQLViews(source, tree, views, SQLViewExpansionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(expansion.Dependencies) != 1 || tree.From[0].Kind != sqlast.RelationDerived ||
		tree.From[1].Kind != sqlast.RelationCollection {
		t.Fatalf("recovery expansion = %+v refs %+v", expansion, tree.From)
	}
}

func TestSQLViewExpansionCoversCTESetsAndPredicateSubqueries(t *testing.T) {
	views := sqlViewMap{
		"v1": {Name: "v1", Query: `SELECT id FROM one`},
		"v2": {Name: "v2", Query: `SELECT id FROM two`},
		"v3": {Name: "v3", Query: `SELECT id FROM three`},
	}
	const source = `WITH local AS (SELECT id FROM v1 UNION ALL SELECT id FROM v2)
		SELECT id FROM local WHERE id IN (SELECT id FROM v3)`
	tree, err := sqlast.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	expansion, err := ExpandSQLViews(source, tree, views, SQLViewExpansionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{
		expansion.Dependencies[0].Name,
		expansion.Dependencies[1].Name,
		expansion.Dependencies[2].Name,
	}; !reflect.DeepEqual(got, []string{"v1", "v2", "v3"}) {
		t.Fatalf("dependency order = %v", got)
	}
	definition := tree.With.CTEs[0].Query.Set.Root
	if definition.Left.Select.From[0].Kind != sqlast.RelationDerived ||
		definition.Right.Select.From[0].Kind != sqlast.RelationDerived ||
		tree.Where.Subquery.From[0].Kind != sqlast.RelationDerived {
		t.Fatalf("not all nested positions expanded")
	}
}

func TestSQLViewExpansionAppliesAliasesToCompoundOutputOrdinally(t *testing.T) {
	const source = `SELECT renamed FROM combined`
	tree, err := sqlast.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ExpandSQLViews(source, tree, sqlViewMap{
		"combined": {
			Name:    "combined",
			Query:   `SELECT id FROM one UNION ALL SELECT id FROM two`,
			Columns: []string{"renamed"},
		},
	}, SQLViewExpansionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	definition := tree.From[0].Query
	if definition == nil || definition.Set == nil ||
		definition.Set.Outputs[0].Name != "renamed" ||
		definition.Set.First.Columns[0].Alias != "renamed" {
		t.Fatalf("compound aliases = %+v", definition)
	}
	prepared, err := PrepareParsedStatement(source, tree)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Release()
	if !reflect.DeepEqual(prepared.Columns(), []string{"renamed"}) {
		t.Fatalf("outer columns = %q", prepared.Columns())
	}
}

func TestSQLViewExpansionValidatesStableSchemaAndResolverIdentity(t *testing.T) {
	tests := []struct {
		name  string
		views sqlViewMap
		root  string
		is    error
		want  string
	}{
		{
			name: "wildcard",
			views: sqlViewMap{"v": {
				Name: "v", Query: `SELECT * FROM docs`,
			}},
			root: `SELECT v.id FROM v`,
			want: "wildcard-dependent",
		},
		{
			name: "alias arity",
			views: sqlViewMap{"v": {
				Name: "v", Query: `SELECT id, n FROM docs`, Columns: []string{"only"},
			}},
			root: `SELECT only FROM v`,
			want: "declares 1 output names for 2",
		},
		{
			name: "cycle from unpublished root",
			views: sqlViewMap{"v": {
				Name: "v", Query: `SELECT id FROM root_view`,
			}},
			root: `SELECT id FROM v`,
			is:   ErrSQLViewCycle,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree, err := sqlast.Parse(test.root)
			if err != nil {
				t.Fatal(err)
			}
			options := SQLViewExpansionOptions{}
			if test.name == "cycle from unpublished root" {
				options.RootName = "root_view"
			}
			_, err = ExpandSQLViews(test.root, tree, test.views, options)
			if test.is != nil && !errors.Is(err, test.is) {
				t.Fatalf("error = %v, want %v", err, test.is)
			}
			if test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}

	tree, err := sqlast.Parse(`SELECT id FROM v`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ExpandSQLViews(`SELECT id FROM v`, tree, mismatchedSQLViewResolver{}, SQLViewExpansionOptions{})
	if err == nil || !strings.Contains(err.Error(), `associated reference "v"`) {
		t.Fatalf("resolver mismatch = %v", err)
	}
}

type mismatchedSQLViewResolver struct{}

func (mismatchedSQLViewResolver) ResolveSQLView(string) (SQLViewDefinition, bool, error) {
	return SQLViewDefinition{Name: "other", Query: `SELECT id FROM docs`}, true, nil
}

func TestSQLViewExpansionFiniteDepthAndConcurrentIndependence(t *testing.T) {
	views := make(sqlViewMap, maxSQLViewExpansionDepth+2)
	for i := 0; i < maxSQLViewExpansionDepth+2; i++ {
		name := fmt.Sprintf("v%d", i)
		next := "docs"
		if i+1 < maxSQLViewExpansionDepth+2 {
			next = fmt.Sprintf("v%d", i+1)
		}
		views[name] = SQLViewDefinition{
			Name: name, Query: fmt.Sprintf("SELECT id FROM %s", next),
		}
	}
	tree, err := sqlast.Parse(`SELECT id FROM v0`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ExpandSQLViews(`SELECT id FROM v0`, tree, views, SQLViewExpansionOptions{})
	if !errors.Is(err, ErrSQLViewExpansionLimit) {
		t.Fatalf("depth error = %v, want ErrSQLViewExpansionLimit", err)
	}
	if tree.From[0].Kind != sqlast.RelationCollection {
		t.Fatal("depth failure partially expanded root")
	}

	const workers = 8
	const iterations = 40
	stable := sqlViewMap{"v": {Name: "v", Query: `SELECT id FROM docs`}}
	var wait sync.WaitGroup
	errorsOut := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				local, parseErr := sqlast.Parse(`SELECT id FROM v`)
				if parseErr != nil {
					errorsOut <- parseErr
					return
				}
				if _, expandErr := ExpandSQLViews(
					`SELECT id FROM v`, local, stable, SQLViewExpansionOptions{},
				); expandErr != nil {
					errorsOut <- expandErr
					return
				}
				if local.From[0].Kind != sqlast.RelationDerived {
					errorsOut <- errors.New("view reference was not expanded")
					return
				}
			}
		}()
	}
	wait.Wait()
	close(errorsOut)
	for err := range errorsOut {
		t.Fatal(err)
	}
}

func TestSQLViewDefinitionDepthAdmissionMatchesTopLevelExpansion(t *testing.T) {
	views := make(sqlViewMap, maxSQLViewExpansionDepth)
	for depth := 1; depth <= maxSQLViewExpansionDepth; depth++ {
		name := fmt.Sprintf("v%d", depth)
		next := "docs"
		if depth < maxSQLViewExpansionDepth {
			next = fmt.Sprintf("v%d", depth+1)
		}
		views[name] = SQLViewDefinition{
			Name: name, Query: fmt.Sprintf("SELECT id FROM %s", next),
		}
	}

	boundary := SQLViewDefinition{
		Name: "boundary", Query: `SELECT id FROM v2`,
	}
	if _, _, err := ExpandSQLViewDefinition(
		boundary, views, SQLViewExpansionOptions{},
	); err != nil {
		t.Fatalf("boundary definition rejected: %v", err)
	}
	views[boundary.Name] = boundary
	tree, err := sqlast.Parse(`SELECT id FROM boundary`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ExpandSQLViews(
		`SELECT id FROM boundary`, tree, views, SQLViewExpansionOptions{},
	); err != nil {
		t.Fatalf("accepted boundary definition was not executable: %v", err)
	}

	overLimit := SQLViewDefinition{
		Name: "over_limit", Query: `SELECT id FROM v1`,
	}
	_, _, err = ExpandSQLViewDefinition(
		overLimit, views, SQLViewExpansionOptions{},
	)
	if !errors.Is(err, ErrSQLViewExpansionLimit) {
		t.Fatalf("over-limit definition error = %v, want ErrSQLViewExpansionLimit", err)
	}
	var positioned interface{ Position() int }
	if !errors.As(err, &positioned) {
		t.Fatalf("over-limit definition error has no position: %T %v", err, err)
	}
	if want := strings.Index(overLimit.Query, "v1"); positioned.Position() != want {
		t.Fatalf("over-limit definition position = %d, want %d",
			positioned.Position(), want)
	}
}
