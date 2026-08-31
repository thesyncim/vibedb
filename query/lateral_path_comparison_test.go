package query

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
)

func lateralPathComparisonHeap(t testing.TB) *store.Database {
	t.Helper()
	db := new(store.Database)
	outer := lateralCorrelationSlotsHeapCollection(t, db, "lateral_path_outer")
	inner := lateralCorrelationSlotsHeapCollection(t, db, "lateral_path_inner")
	if _, err := outer.Put("outer", []byte(
		`{"id":"outer","n":1,"s":"1","nullv":null,"obj":{"x":1}}`,
	)); err != nil {
		t.Fatal(err)
	}
	if _, err := inner.Put("inner", []byte(
		`{"id":"inner","owner":"outer","n":1,"s":"1","obj":{"x":1}}`,
	)); err != nil {
		t.Fatal(err)
	}
	return db
}

func lateralPathComparisonRun(
	t testing.TB,
	db *store.Database,
	source string,
) (*Statement, *Exec, error) {
	t.Helper()
	statement, err := PrepareStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	exec := new(Exec)
	_, err = statement.RunInto(
		exec, FromDatabase(db.Snapshot(), statement.Collection()), nil,
	)
	return statement, exec, err
}

func assertLateralUndefinedOperator(
	t testing.TB,
	err error,
	left, operator, right string,
	position int,
) *sqlast.UndefinedOperatorError {
	t.Helper()
	var undefined *sqlast.UndefinedOperatorError
	if !errors.As(err, &undefined) {
		t.Fatalf("error = %T %v, want UndefinedOperatorError", err, err)
	}
	if undefined.Left != left || undefined.Operator != operator ||
		undefined.Right != right || undefined.Pos != position {
		t.Fatalf("undefined operator = %+v, want %s %s %s at %d",
			undefined, left, operator, right, position)
	}
	return undefined
}

func TestSQLLateralPathComparisonRejectsEveryIncompatibleOperatorInAuthoredOrder(t *testing.T) {
	db := lateralPathComparisonHeap(t)
	for _, orientation := range []struct {
		name        string
		left, right string
	}{
		{name: "local-left", left: "i.n", right: "o.s"},
		{name: "outer-left", left: "o.n", right: "i.s"},
	} {
		orientation := orientation
		t.Run(orientation.name, func(t *testing.T) {
			for _, operator := range []struct {
				authored, canonical string
			}{
				{authored: "=", canonical: "="},
				{authored: "<>", canonical: "<>"},
				{authored: "!=", canonical: "<>"},
				{authored: "<", canonical: "<"},
				{authored: "<=", canonical: "<="},
				{authored: ">", canonical: ">"},
				{authored: ">=", canonical: ">="},
			} {
				operator := operator
				t.Run(operator.authored, func(t *testing.T) {
					predicate := orientation.left + " " + operator.authored + " " + orientation.right
					source := `SELECT o.id, d.id FROM lateral_path_outer o CROSS JOIN LATERAL (` +
						`SELECT i.id FROM lateral_path_inner i WHERE ` + predicate + `) d`
					statement, exec, err := lateralPathComparisonRun(t, db, source)
					defer statement.Release()
					defer exec.Release()
					position := strings.LastIndex(source, predicate) + len(orientation.left) + 1
					assertLateralUndefinedOperator(
						t, err, "numeric", operator.canonical, "text", position,
					)
					if exec.Result.RowCount != 0 {
						t.Fatalf("failed LATERAL comparison published %d rows", exec.Result.RowCount)
					}
				})
			}
		})
	}
}

func TestSQLLateralPathComparisonContainersReachUndefinedOperator(t *testing.T) {
	db := lateralPathComparisonHeap(t)
	for _, test := range []struct {
		name            string
		predicate       string
		left, op, right string
	}{
		{
			name: "outer container authored left", predicate: "o.obj = i.n",
			left: "json", op: "=", right: "numeric",
		},
		{
			name: "containers on both sides", predicate: "i.obj != o.obj",
			left: "json", op: "<>", right: "json",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := `SELECT o.id FROM lateral_path_outer o CROSS JOIN LATERAL (` +
				`SELECT i.id FROM lateral_path_inner i WHERE ` + test.predicate + `) d`
			statement, exec, err := lateralPathComparisonRun(t, db, source)
			defer statement.Release()
			defer exec.Release()
			operatorAt := strings.Index(test.predicate, " ") + 1
			operatorAt += strings.LastIndex(source, test.predicate)
			assertLateralUndefinedOperator(
				t, err, test.left, test.op, test.right, operatorAt,
			)
			var binding *LateralBindingValueError
			if errors.As(err, &binding) {
				t.Fatalf("container was rejected by obsolete argument coercion: %+v", binding)
			}
		})
	}
}

func TestSQLLateralOuterOnlyPathGateUsesStrictSQLDomains(t *testing.T) {
	db := lateralPathComparisonHeap(t)
	for _, test := range []struct {
		predicate       string
		left, op, right string
	}{
		{predicate: "o.n <= o.s", left: "numeric", op: "<=", right: "text"},
		{predicate: "o.obj = o.obj", left: "json", op: "=", right: "json"},
	} {
		t.Run(test.predicate, func(t *testing.T) {
			source := `SELECT o.id, d.id FROM lateral_path_outer o LEFT JOIN LATERAL (` +
				`SELECT i.id FROM lateral_path_inner i ` +
				`WHERE i.owner = o.id AND ` + test.predicate + `) d ON TRUE`
			statement, exec, err := lateralPathComparisonRun(t, db, source)
			defer statement.Release()
			defer exec.Release()
			position := strings.LastIndex(source, test.predicate) +
				strings.Index(test.predicate, " ") + 1
			assertLateralUndefinedOperator(t, err, test.left, test.op, test.right, position)
			if exec.Result.RowCount != 0 {
				t.Fatalf("failed outer-only gate published %d rows", exec.Result.RowCount)
			}
		})
	}
}

func TestSQLLateralOuterOnlyPathGateNullIsUnknown(t *testing.T) {
	db := lateralPathComparisonHeap(t)
	statement, exec, got := runLateralStatement(t, db, `
		SELECT o.id, d.id FROM lateral_path_outer o LEFT JOIN LATERAL (
			SELECT i.id FROM lateral_path_inner i
			WHERE i.owner = o.id AND o.nullv = o.s
		) d ON TRUE`)
	defer statement.Release()
	defer exec.Release()
	if want := []string{`"outer",null`}; strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("NULL-gated LATERAL rows = %q, want %q", got, want)
	}
	if evaluations := statement.relationJoin().operands[1].lateral.evaluations; evaluations != 0 {
		t.Fatalf("UNKNOWN outer-only gate evaluated its child %d times", evaluations)
	}
}

func TestSQLLateralONResolvesPathOperatorBeforeFalseConjunct(t *testing.T) {
	db := lateralPathComparisonHeap(t)
	source := `SELECT o.id FROM lateral_path_outer o LEFT JOIN LATERAL (` +
		`SELECT i.s FROM lateral_path_inner i WHERE i.owner = o.id` +
		`) d ON FALSE AND o.n < d.s`
	statement, exec, err := lateralPathComparisonRun(t, db, source)
	defer statement.Release()
	defer exec.Release()
	assertLateralUndefinedOperator(
		t, err, "numeric", "<", "text", strings.LastIndex(source, "<"),
	)
	if exec.Result.RowCount != 0 {
		t.Fatalf("failed LATERAL ON comparison published %d rows", exec.Result.RowCount)
	}
}

func TestSQLLateralPathComparisonCannotBePrunedByExactIndex(t *testing.T) {
	db := new(store.Database)
	outer := lateralCorrelationSlotsHeapCollection(t, db, "lateral_index_outer")
	inner := lateralCorrelationSlotsHeapCollection(t, db, "lateral_index_inner")
	if _, err := outer.Put("outer", []byte(`{"id":"outer","k":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := inner.Put("inner", []byte(`{"id":"inner","k":"1"}`)); err != nil {
		t.Fatal(err)
	}
	definition := store.IndexDefinition{Name: "by_k", Paths: []string{"/k"}}
	if _, err := inner.CreateIndex(definition); err != nil {
		t.Fatal(err)
	}
	if _, err := inner.BackfillIndex(definition.Name, 0); err != nil {
		t.Fatal(err)
	}
	source := `SELECT o.id FROM lateral_index_outer o CROSS JOIN LATERAL (` +
		`SELECT i.id FROM lateral_index_inner i WHERE i.k = o.k) d`
	statement, exec, err := lateralPathComparisonRun(t, db, source)
	defer statement.Release()
	defer exec.Release()
	assertLateralUndefinedOperator(
		t, err, "text", "=", "numeric", strings.LastIndex(source, "="),
	)
	childExec := &statement.relationJoin().operands[1].exec
	if childExec.Workspace.storeIndexProbes != 0 || childExec.Stats.IndexBounded ||
		childExec.Stats.IndexLookups != 0 {
		t.Fatalf("strict correlation was index-pruned: probes=%d stats=%+v",
			childExec.Workspace.storeIndexProbes, childExec.Stats)
	}
}

func TestSQLLateralPathComparisonExplainDeclaresFullRecheck(t *testing.T) {
	statement, err := PrepareStatement(
		`SELECT o.id FROM lateral_path_outer o CROSS JOIN LATERAL (` +
			`SELECT i.id FROM lateral_path_inner i WHERE i.n = o.n) d`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	child := statement.relationJoin().operands[1].stmt
	plan, err := child.Explain()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"access_path":"full-scan"`,
		`"kind":"correlation-comparison","path":"n","operator":"="`,
	} {
		if !strings.Contains(plan, want) {
			t.Fatalf("correlated path EXPLAIN missing %s: %s", want, plan)
		}
	}
}

func TestSQLLateralStoredViewSentinelSurvivesCorrelationClone(t *testing.T) {
	db := lateralPathComparisonHeap(t)
	predicate := "o.n < i.s"
	source := `SELECT o.id FROM lateral_path_outer o CROSS JOIN LATERAL (` +
		`SELECT i.id FROM lateral_path_inner i WHERE ` + predicate + `) d`
	parser := new(sqlast.Parser)
	tree := new(sqlast.SelectStmt)
	if err := parser.Parse(tree, source); err != nil {
		t.Fatal(err)
	}
	suppressSQLViewRuntimeComparisonPositions(tree)
	statement, err := prepareTree(source, tree)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	var exec Exec
	defer exec.Release()
	_, err = statement.RunInto(
		&exec, FromDatabase(db.Snapshot(), statement.Collection()), nil,
	)
	var undefined *sqlast.UndefinedOperatorError
	if !errors.As(err, &undefined) || !undefined.Unpositioned ||
		undefined.Left != "numeric" || undefined.Operator != "<" ||
		undefined.Right != "text" {
		t.Fatalf("stored-view LATERAL error = %T %+v", err, undefined)
	}
	var positioned *sqlast.ParseError
	if errors.As(err, &positioned) {
		t.Fatalf("stored-view LATERAL error leaked a definition offset: %+v", positioned)
	}
}

func TestSQLLateralPathComparisonExactDomainsStayZeroAllocation(t *testing.T) {
	db := lateralPathComparisonHeap(t)
	source := `SELECT o.id, d.id FROM lateral_path_outer o CROSS JOIN LATERAL (` +
		`SELECT i.id FROM lateral_path_inner i WHERE i.n = o.n) d`
	statement, err := PrepareStatement(source)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	input := FromDatabase(db.Snapshot(), statement.Collection())
	var exec Exec
	defer exec.Release()
	run := func() {
		cursor, runErr := statement.RunInto(&exec, input, nil)
		if runErr != nil {
			panic(runErr)
		}
		for cursor.Next() {
			lateralCorrelationSlotsSink += len(cursor.Cell(0).Payload())
		}
	}
	run()
	run()
	if got := testing.AllocsPerRun(50, run); got != 0 {
		t.Fatalf("warmed strict LATERAL path comparison allocates %.2f times", got)
	}
}

func TestSQLLateralPathComparisonErrorMessageIsCanonical(t *testing.T) {
	db := lateralPathComparisonHeap(t)
	source := `SELECT o.id FROM lateral_path_outer o CROSS JOIN LATERAL (` +
		`SELECT i.id FROM lateral_path_inner i WHERE o.n != i.s) d`
	statement, exec, err := lateralPathComparisonRun(t, db, source)
	defer statement.Release()
	defer exec.Release()
	undefined := assertLateralUndefinedOperator(
		t, err, "numeric", "<>", "text", strings.LastIndex(source, "!="),
	)
	if want := "operator does not exist: numeric <> text"; undefined.Msg != want {
		t.Fatalf("message = %q, want %q", undefined.Msg, want)
	}
	if got := fmt.Sprint(undefined.Left, " ", undefined.Operator, " ", undefined.Right); got != "numeric <> text" {
		t.Fatalf("typed diagnostic = %q", got)
	}
}
