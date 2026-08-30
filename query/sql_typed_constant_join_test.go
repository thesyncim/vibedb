package query

import (
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func TestSQLPostgreSQLTypedConstantJoinComparisons(t *testing.T) {
	db := relationJoinDatabase(t)
	wantTrue := []string{
		`"a2","b2x"`,
		`"a2","b2y"`,
		`"a2","b4"`,
	}
	for _, on := range []string{`a.enabled = BOOL 't'`, `BOOL 't' = a.enabled`} {
		statement, _, got := runRelationJoinSQL(t, db, `
			SELECT a.label, b.label FROM a JOIN b ON `+on+`
			ORDER BY a.label, b.label`)
		if statement.tree.From[1].On.Expr.Kind != sqlast.ExprCompare ||
			strings.Join(got, "\n") != strings.Join(wantTrue, "\n") {
			t.Fatalf("ON %s rows:\n got %q\nwant %q", on, got, wantTrue)
		}
	}

	_, _, got := runRelationJoinSQL(t, db, `
		SELECT a.label, b.label FROM a JOIN b ON BOOL 'f' < a.enabled
		ORDER BY a.label, b.label`)
	if strings.Join(got, "\n") != strings.Join(wantTrue, "\n") {
		t.Fatalf("reversed ordered comparison rows:\n got %q\nwant %q", got, wantTrue)
	}
}

func TestSQLPostgreSQLTypedConstantJoinCastChainComparison(t *testing.T) {
	db := relationJoinDatabase(t)
	wantTrue := []string{
		`"a2","b2x"`,
		`"a2","b2y"`,
		`"a2","b4"`,
	}
	for _, on := range []string{
		`a.enabled = TEXT 't'::BOOL`,
		`TEXT 't'::BOOL = a.enabled`,
	} {
		_, _, got := runRelationJoinSQL(t, db, `
			SELECT a.label, b.label FROM a JOIN b ON `+on+`
			ORDER BY a.label, b.label`)
		if strings.Join(got, "\n") != strings.Join(wantTrue, "\n") {
			t.Fatalf("ON %s rows:\n got %q\nwant %q", on, got, wantTrue)
		}
	}
}

func TestSQLPostgreSQLTypedConstantJoinWarmExecutionIsAllocationFree(t *testing.T) {
	db := relationJoinDatabase(t)
	statement, err := PrepareStatement(`
		SELECT a.label, b.label FROM a JOIN b ON BOOL 'f' < a.enabled
		ORDER BY a.label, b.label`)
	if err != nil {
		t.Fatal(err)
	}
	source := FromDatabase(db.Snapshot(), statement.Collection())
	exec := Exec{Options: ExecOptions{IntermediateBytes: -1, JoinPairBytes: -1}}
	run := func() {
		if _, runErr := statement.RunInto(&exec, source, nil); runErr != nil {
			panic(runErr)
		}
	}
	run()
	if allocs := testing.AllocsPerRun(50, run); allocs != 0 {
		t.Fatalf("warmed typed-constant JOIN allocated %.2f/run", allocs)
	}
}

func TestSQLPostgreSQLTypedDerivedJoinSchemaPropagation(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		types []ValueType
		reps  []OutputRepresentation
	}{
		{
			name:  "direct",
			sql:   `SELECT q.v FROM a CROSS JOIN (SELECT BOOL 't' AS v) AS q`,
			types: []ValueType{TypeBool},
			reps:  []OutputRepresentation{OutputSQLBool},
		},
		{
			name: "joined CTE",
			sql: `WITH q AS (SELECT BOOL 't' AS v)
				SELECT q.v FROM a CROSS JOIN q`,
			types: []ValueType{TypeBool},
			reps:  []OutputRepresentation{OutputSQLBool},
		},
		{
			name: "derived wildcard",
			sql: `SELECT q.* FROM a CROSS JOIN
				(SELECT BOOL 't' AS v, TEXT 'x' AS s) AS q`,
			types: []ValueType{TypeBool, TypeString},
			reps:  []OutputRepresentation{OutputSQLBool, OutputSQLText},
		},
		{
			name: "mixed identity projection",
			sql: `SELECT a.*, q.v, q.s FROM a CROSS JOIN
				(SELECT BOOL 't' AS v, TEXT 'x' AS s) AS q`,
			types: []ValueType{TypeAny, TypeBool, TypeString},
			reps:  []OutputRepresentation{OutputJSON, OutputSQLBool, OutputSQLText},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement, err := PrepareStatement(test.sql)
			if err != nil {
				t.Fatal(err)
			}
			schema := statement.AppendSchema(nil)
			if len(schema) != len(test.types) {
				t.Fatalf("schema = %+v, want %d columns", schema, len(test.types))
			}
			for column := range schema {
				if schema[column].Type != test.types[column] ||
					schema[column].Representation != test.reps[column] {
					t.Fatalf(
						"schema[%d] = %+v, want type=%d representation=%d",
						column, schema[column], test.types[column], test.reps[column],
					)
				}
			}
		})
	}
}

func TestSQLPostgreSQLTypedDerivedJoinWarmExecutionIsAllocationFree(t *testing.T) {
	db := relationJoinDatabase(t)
	statement, err := PrepareStatement(
		`SELECT q.v FROM a CROSS JOIN (SELECT BOOL 't' AS v) AS q`,
	)
	if err != nil {
		t.Fatal(err)
	}
	source := FromDatabase(db.Snapshot(), statement.Collection())
	exec := Exec{Options: ExecOptions{IntermediateBytes: -1, JoinPairBytes: -1}}
	run := func() {
		cursor, runErr := statement.RunInto(&exec, source, nil)
		if runErr != nil {
			panic(runErr)
		}
		rows := 0
		for cursor.Next() {
			value, ok := cursor.Cell(0).Bool()
			if !ok || !value {
				panic("derived BOOL did not retain its native cell kind")
			}
			rows++
		}
		if rows == 0 {
			panic("derived join returned no rows")
		}
	}
	run()
	if allocs := testing.AllocsPerRun(50, run); allocs != 0 {
		t.Fatalf("warmed typed-derived JOIN allocated %.2f/run", allocs)
	}
}
