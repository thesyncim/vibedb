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

func TestSQLViewExpansionSuppressesDefinitionOwnedComparisonPositions(t *testing.T) {
	const definitionSource = `SELECT CASE WHEN a != b THEN 1 ELSE 0 END AS changed
		FROM docs WHERE a <= b`
	definition := SQLViewDefinition{Name: "compared", Query: definitionSource}
	const outerSource = `SELECT changed FROM compared`
	outer, err := sqlast.Parse(outerSource)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ExpandSQLViews(outerSource, outer, sqlViewMap{
		"compared": definition,
	}, SQLViewExpansionOptions{}); err != nil {
		t.Fatal(err)
	}
	expanded := outer.From[0].Query
	if expanded == nil || expanded.Where == nil || expanded.Where.Value.Pos != -1 {
		t.Fatalf("expanded WHERE comparison position = %+v, want unavailable", expanded)
	}
	searched := expanded.Columns[0].Scalar.Whens[0].Predicate
	if searched == nil || searched.Value.Pos != -1 {
		t.Fatalf("expanded CASE comparison = %+v, want unavailable position", searched)
	}

	// CREATE VIEW validation owns definitionSource and must keep using its
	// exact offsets. Only a catalog definition rewritten into another source
	// loses a meaningful client position.
	direct, _, err := ExpandSQLViewDefinition(
		definition, sqlViewMap{}, SQLViewExpansionOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := direct.Where.Value.Pos, strings.Index(definitionSource, "<="); got != want {
		t.Fatalf("direct definition WHERE position = %d, want %d", got, want)
	}
	directSearched := direct.Columns[0].Scalar.Whens[0].Predicate
	if got, want := directSearched.Value.Pos, strings.Index(definitionSource, "!="); got != want {
		t.Fatalf("direct definition CASE position = %d, want %d", got, want)
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

func TestSQLViewExpansionLeadingAliasListPreservesTrailingNames(t *testing.T) {
	tests := []struct {
		name       string
		definition string
		set        bool
	}{
		{
			name:       "select",
			definition: `SELECT id, n FROM docs`,
		},
		{
			name: "compound",
			definition: `SELECT id, n FROM one ` +
				`UNION ALL SELECT id, n FROM two`,
			set: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const source = `SELECT doc_id, n FROM v`
			tree, err := sqlast.Parse(source)
			if err != nil {
				t.Fatal(err)
			}
			_, err = ExpandSQLViews(source, tree, sqlViewMap{
				"v": {
					Name: "v", Query: test.definition,
					Columns: []string{"doc_id"},
				},
			}, SQLViewExpansionOptions{})
			if err != nil {
				t.Fatal(err)
			}
			definition := tree.From[0].Query
			if definition == nil || definition.Columns[0].Alias != "doc_id" ||
				definition.Columns[1].Alias != "" {
				t.Fatalf("leading alias application = %+v", definition)
			}
			if test.set && (definition.Set == nil ||
				definition.Set.Outputs[0].Name != "doc_id" ||
				definition.Set.Outputs[1].Name != "n") {
				t.Fatalf("compound effective outputs = %+v", definition.Set)
			}
			prepared, err := PrepareParsedStatement(source, tree)
			if err != nil {
				t.Fatal(err)
			}
			defer prepared.Release()
			if got := prepared.Columns(); !reflect.DeepEqual(got, []string{"doc_id", "n"}) {
				t.Fatalf("effective columns = %q, want [doc_id n]", got)
			}
		})
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
			name: "excess alias arity",
			views: sqlViewMap{"v": {
				Name: "v", Query: `SELECT id, n FROM docs`,
				Columns: []string{"first", "second", "excess"},
			}},
			root: `SELECT first FROM v`,
			is:   ErrSQLViewColumnArity,
			want: "3 column aliases but its query has 2 outputs",
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

func TestSQLViewExpansionMemoizedDAGAdmitsLongestPath(t *testing.T) {
	const overLimitSource = `SELECT s.id FROM shallow AS s ` +
		`JOIN deep_0 AS d ON s.id = d.id`
	overLimit, err := sqlast.Parse(overLimitSource)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ExpandSQLViews(
		overLimitSource, overLimit, sqlViewMemoizedDepthDAG(28, 5),
		SQLViewExpansionOptions{},
	)
	if !errors.Is(err, ErrSQLViewExpansionLimit) {
		t.Fatalf("shallow-first cached DAG error = %v, want depth limit", err)
	}
	var limit *SQLViewExpansionLimitError
	if !errors.As(err, &limit) || limit.Name != "shared_0" ||
		limit.Pos != strings.Index(overLimitSource, "deep_0") {
		t.Fatalf("shallow-first cached DAG detail = %+v", limit)
	}
	for i := range overLimit.From {
		if overLimit.From[i].Kind != sqlast.RelationCollection ||
			overLimit.From[i].Query != nil {
			t.Fatalf("depth failure partially published relation %d: %+v",
				i, overLimit.From[i])
		}
	}

	const boundarySource = `SELECT s.id FROM shallow AS s ` +
		`JOIN deep_0 AS d ON s.id = d.id`
	boundary, err := sqlast.Parse(boundarySource)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ExpandSQLViews(
		boundarySource, boundary, sqlViewMemoizedDepthDAG(27, 5),
		SQLViewExpansionOptions{},
	); err != nil {
		t.Fatalf("32-level cached DAG boundary rejected: %v", err)
	}
	for i := range boundary.From {
		if boundary.From[i].Kind != sqlast.RelationDerived ||
			boundary.From[i].Query == nil {
			t.Fatalf("boundary relation %d was not published: %+v",
				i, boundary.From[i])
		}
	}
}

func sqlViewMemoizedDepthDAG(sharedHeight, deepHeight int) sqlViewMap {
	views := make(sqlViewMap, sharedHeight+deepHeight+1)
	for depth := 0; depth < sharedHeight; depth++ {
		name := fmt.Sprintf("shared_%d", depth)
		next := "docs"
		if depth+1 < sharedHeight {
			next = fmt.Sprintf("shared_%d", depth+1)
		}
		views[name] = SQLViewDefinition{
			Name: name, Query: fmt.Sprintf("SELECT id FROM %s", next),
		}
	}
	views["shallow"] = SQLViewDefinition{
		Name: "shallow", Query: `SELECT id FROM shared_0`,
	}
	for depth := 0; depth < deepHeight; depth++ {
		name := fmt.Sprintf("deep_%d", depth)
		next := "shared_0"
		if depth+1 < deepHeight {
			next = fmt.Sprintf("deep_%d", depth+1)
		}
		views[name] = SQLViewDefinition{
			Name: name, Query: fmt.Sprintf("SELECT id FROM %s", next),
		}
	}
	return views
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
