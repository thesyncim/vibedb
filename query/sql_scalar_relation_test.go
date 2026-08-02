package query

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestSQLScalarDerivedColumnResolutionIsPositioned(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		column  string
		matches int
		is      error
		marker  string
	}{
		{
			name:   "projection undefined",
			sql:    `SELECT "δ".missing + 1 AS value FROM (SELECT id FROM docs) AS "δ"`,
			column: "missing", is: ErrUndefinedColumn, marker: "missing",
		},
		{
			name: "predicate undefined",
			sql: `SELECT "δ".id + 1 AS value FROM (SELECT id FROM docs) AS "δ" ` +
				`WHERE "δ".missing + 1 > 0`,
			column: "missing", is: ErrUndefinedColumn, marker: "missing",
		},
		{
			name: "projection duplicate alias ambiguous",
			sql: `SELECT "δ".id + 1 AS value FROM (` +
				`SELECT left_id AS id, right_id AS id FROM docs` +
				`) AS "δ"`,
			column: "id", matches: 2, is: ErrAmbiguousColumn, marker: `"δ".id`,
		},
		{
			name: "predicate duplicate alias ambiguous",
			sql: `SELECT 1 AS value FROM (` +
				`SELECT left_id AS id, right_id AS id FROM docs` +
				`) AS "δ" WHERE "δ".id + 1 > 0`,
			column: "id", matches: 2, is: ErrAmbiguousColumn, marker: `"δ".id`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := PrepareStatement(test.sql)
			if !errors.Is(err, test.is) {
				t.Fatalf("PrepareStatement error = %#v, want errors.Is(%v)", err, test.is)
			}
			var columnErr *RelationColumnError
			if !errors.As(err, &columnErr) {
				t.Fatalf("PrepareStatement error = %T, want *RelationColumnError", err)
			}
			if columnErr.Column != test.column || columnErr.Matches != test.matches {
				t.Fatalf("column error = %+v, want column %q matches %d", columnErr, test.column, test.matches)
			}
			wantPos := strings.Index(test.sql, test.marker)
			if columnErr.Position() != wantPos {
				t.Fatalf("error position = %d, want UTF-8 byte offset %d", columnErr.Position(), wantPos)
			}
		})
	}
}

type scalarRelationViewResolver map[string]SQLViewDefinition

func (r scalarRelationViewResolver) ResolveSQLView(name string) (SQLViewDefinition, bool, error) {
	definition, ok := r[name]
	return definition, ok, nil
}

func TestSQLScalarExpandedViewColumnResolution(t *testing.T) {
	const source = `SELECT "δ".missing + 1 AS value FROM ordinary AS "δ"`
	tree, err := sqlast.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	_, err = ExpandSQLViews(source, tree, scalarRelationViewResolver{
		"ordinary": {Name: "ordinary", Query: `SELECT id FROM docs`},
	}, SQLViewExpansionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = PrepareParsedStatement(source, tree)
	if !errors.Is(err, ErrUndefinedColumn) {
		t.Fatalf("PrepareParsedStatement error = %#v, want ErrUndefinedColumn", err)
	}
	var columnErr *RelationColumnError
	if !errors.As(err, &columnErr) {
		t.Fatalf("PrepareParsedStatement error = %T, want *RelationColumnError", err)
	}
	if want := strings.Index(source, "missing"); columnErr.Position() != want {
		t.Fatalf("error position = %d, want UTF-8 byte offset %d", columnErr.Position(), want)
	}
}

func TestSQLScalarDerivedDependenciesAreResolvedAndReusable(t *testing.T) {
	const source = `SELECT d.n + 1 AS value FROM (SELECT n FROM docs) AS d`
	statement, err := PrepareStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	runtime := statement.scalarStatement()
	if runtime == nil || len(runtime.deps) != 1 || runtime.deps[0].spec != "/0" {
		t.Fatalf("scalar dependencies = %+v, want one resolved ordinal /0", runtime)
	}

	segment := mustSegment(t, `{"n":2}`, `{"n":null}`, `{}`)
	var exec Exec
	defer exec.Release()
	for run := 0; run < 3; run++ {
		cursor, runErr := statement.RunInto(&exec, FromSegment(segment), nil)
		if runErr != nil {
			t.Fatalf("run %d: %v", run, runErr)
		}
		want := []string{"3", "null", "null"}
		for row := range want {
			if !cursor.Next() {
				t.Fatalf("run %d missing row %d", run, row)
			}
			if got := string(cursor.Cell(0).JSON()); got != want[row] {
				t.Fatalf("run %d row %d = %q, want %q", run, row, got, want[row])
			}
		}
		if cursor.Next() {
			t.Fatalf("run %d returned an extra row", run)
		}
	}
}

func TestSQLScalarDerivedIndependentPreparedStatementsRace(t *testing.T) {
	const source = `SELECT d.n + 1 AS value FROM (SELECT n FROM docs) AS d`
	segment := mustSegment(t, `{"n":2}`, `{"n":4}`)
	const workers = 4
	statements := make([]*Statement, workers)
	for i := range statements {
		var err error
		statements[i], err = PrepareStatement(source)
		if err != nil {
			t.Fatal(err)
		}
		defer statements[i].Release()
	}

	errs := make(chan error, workers)
	var group sync.WaitGroup
	for worker := range statements {
		group.Add(1)
		go func(statement *Statement) {
			defer group.Done()
			var exec Exec
			defer exec.Release()
			for run := 0; run < 50; run++ {
				cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
				if err != nil {
					errs <- err
					return
				}
				if !cursor.Next() || string(cursor.Cell(0).JSON()) != "3" ||
					!cursor.Next() || string(cursor.Cell(0).JSON()) != "5" || cursor.Next() {
					errs <- fmt.Errorf("run %d returned an unexpected scalar-derived result", run)
					return
				}
			}
		}(statements[worker])
	}
	group.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestSQLScalarRelationResolutionAbsentPathIsZeroCost(t *testing.T) {
	statement, err := PrepareStatement(`SELECT id FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if statement.scalarStatement() != nil || statement.derived() != nil || statement.nested != nil {
		t.Fatalf("ordinary statement retained absent relation/scalar state: %+v", statement.nested)
	}
	segment := mustSegment(t, `{"id":"a"}`, `{"id":"b"}`)
	var exec Exec
	defer exec.Release()
	run := func() {
		cursor, runErr := statement.RunInto(&exec, FromSegment(segment), nil)
		if runErr != nil {
			t.Fatal(runErr)
		}
		for cursor.Next() {
			sqlSink += len(cursor.Cell(0).Payload())
		}
	}
	run()
	run()
	if got := testing.AllocsPerRun(100, run); got != 0 {
		t.Fatalf("ordinary warmed statement allocated %.2f/run, want zero", got)
	}
}
