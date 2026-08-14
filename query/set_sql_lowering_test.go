package query

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestSQLSetLoweringAllSixModesStableOrderAndFirstSchema(t *testing.T) {
	database := setStatementDatabase(t)
	tests := []struct {
		syntax string
		want   []string
	}{
		{"UNION ALL", []string{"1", "2", "2", "4", "2", "2", "3", "4"}},
		{"UNION DISTINCT", []string{"1", "2", "4", "3"}},
		{"INTERSECT ALL", []string{"2", "2", "4"}},
		{"INTERSECT DISTINCT", []string{"2", "4"}},
		{"EXCEPT ALL", []string{"1"}},
		{"EXCEPT DISTINCT", []string{"1"}},
	}
	for _, test := range tests {
		t.Run(test.syntax, func(t *testing.T) {
			statement, err := PrepareStatement(
				`SELECT v AS first_name FROM set_left ` + test.syntax +
					` SELECT v AS ignored_name FROM set_right`,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			if got := statement.Columns(); !reflect.DeepEqual(got, []string{"first_name"}) {
				t.Fatalf("columns = %v", got)
			}
			var execution Exec
			cursor, err := statement.RunInto(
				&execution,
				FromDatabase(database.Snapshot(), statement.Collection()), nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			if got := setStatementCursorJSON(cursor); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("rows = %v, want %v", got, test.want)
			}
			if execution.Result.Columns[0].Header != "first_name" {
				t.Fatalf("header = %q", execution.Result.Columns[0].Header)
			}
		})
	}
}

func TestSQLSetLoweringScopedTailsAndAbsoluteParameters(t *testing.T) {
	database := setStatementDatabase(t)
	statement, err := PrepareStatement(
		`(SELECT v AS value FROM set_left WHERE v >= ? ORDER BY value DESC LIMIT ?) ` +
			`UNION ALL SELECT v FROM set_right WHERE v <= ? ` +
			`ORDER BY 1 DESC LIMIT ? OFFSET ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if statement.NumParams() != 5 {
		t.Fatalf("parameters = %d, want 5", statement.NumParams())
	}
	args := []any{int64(2), int64(2), int64(3), int64(3), int64(1)}
	var execution Exec
	cursor, err := statement.RunInto(
		&execution,
		FromDatabase(database.Snapshot(), statement.Collection()), args,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := setStatementCursorJSON(cursor), []string{"3", "2", "2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}

	explained, err := statement.ExplainBound(args)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		`"node":"set"`, `"operation":"union all"`,
		`"order_by":["value DESC"]`, `"limit":3`, `"offset":1`,
	} {
		if !strings.Contains(explained, fragment) {
			t.Fatalf("EXPLAIN %s does not contain %s", explained, fragment)
		}
	}
}

func TestSQLSetLoweringDeferredWildcardArityAndUTF8Position(t *testing.T) {
	valid := `SELECT d.* FROM (SELECT v, tag FROM set_left) d ` +
		`UNION ALL SELECT v, tag FROM set_right`
	statement, err := PrepareStatement(valid)
	if err != nil {
		t.Fatal(err)
	}
	if got := statement.Columns(); !reflect.DeepEqual(got, []string{"v", "tag"}) {
		statement.Release()
		t.Fatalf("expanded columns = %v", got)
	}
	statement.Release()

	invalid := `SELECT d.* FROM (SELECT v, tag FROM "café") d UNION SELECT v FROM set_right`
	_, err = PrepareStatement(invalid)
	var arity *SetSQLArityError
	if !errors.As(err, &arity) || !errors.Is(err, ErrSetTreeArity) {
		t.Fatalf("error = %T %v, want positioned set arity", err, err)
	}
	if want := strings.Index(invalid, "UNION"); arity.Position() != want {
		t.Fatalf("position = %d, want %d", arity.Position(), want)
	}
}

func TestSQLSetLoweringCatalogClassificationAndCTEReuse(t *testing.T) {
	single, err := PrepareStatement(
		`SELECT v FROM set_left WHERE v >= ? UNION ALL ` +
			`SELECT v FROM set_left WHERE v <= ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if single.RequiresCatalog() || single.Collection() != "set_left" {
		single.Release()
		t.Fatalf("single dependency classified as catalog: %v/%q",
			single.RequiresCatalog(), single.Collection())
	}
	single.Release()

	multi, err := PrepareStatement(
		`WITH c AS (SELECT v FROM set_left WHERE v >= ?) ` +
			`SELECT v FROM c UNION ALL SELECT v FROM set_right WHERE v <= ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer multi.Release()
	if !multi.RequiresCatalog() || !multi.UsesDirectCatalogExecution() {
		t.Fatal("multi-dependency set did not request direct coherent catalog execution")
	}
	database := setStatementDatabase(t)
	var execution Exec
	cursor, err := multi.RunInto(
		&execution,
		FromDatabase(database.Snapshot(), multi.Collection()),
		[]any{int64(1), int64(4)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := setStatementCursorJSON(cursor),
		[]string{"1", "2", "2", "4", "2", "2", "3", "4"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("CTE rows = %v, want %v", got, want)
	}

	shared, err := PrepareStatement(
		`WITH c AS (SELECT v FROM set_left WHERE v >= ?) ` +
			`SELECT v FROM c UNION ALL SELECT v FROM c`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer shared.Release()
	catalog := shared.cteCatalog()
	if catalog == nil || len(catalog.defs) != 1 || catalog.defs[0].references != 2 {
		t.Fatalf("shared CTE catalog/reference count = %+v", catalog)
	}
	var sharedExecution Exec
	sharedCursor, err := shared.RunInto(
		&sharedExecution,
		FromDatabase(database.Snapshot(), shared.Collection()),
		[]any{int64(1)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := setStatementCursorJSON(sharedCursor),
		[]string{"1", "2", "2", "4", "1", "2", "2", "4"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("shared CTE rows = %v, want %v", got, want)
	}
	if evaluations := catalog.defs[0].runEvaluations; evaluations != 1 {
		t.Fatalf("shared CTE evaluations = %d, want exactly 1", evaluations)
	}
	if catalog.defs[0].activeBytes != 0 || catalog.defs[0].spool.rows != 0 {
		t.Fatal("shared CTE publication survived completed set statement")
	}
}

func TestSQLSetLoweringGroupedTailUsesIntermediateLimits(t *testing.T) {
	database := setStatementDatabase(t)
	statement, err := PrepareStatement(
		`(SELECT v AS value FROM set_left ` +
			`UNION ALL SELECT v FROM set_right ORDER BY value) ` +
			`INTERSECT SELECT v FROM set_left WHERE v = 1`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	var execution Exec
	execution.Options.ResultRows = 1
	execution.Options.ResultBytes = 256
	execution.Options.IntermediateBytes = 1 << 20
	cursor, err := statement.RunInto(
		&execution,
		FromDatabase(database.Snapshot(), statement.Collection()),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := setStatementCursorJSON(cursor), []string{"1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	if execution.Result.RowCount != 1 {
		t.Fatalf("published rows = %d, want 1", execution.Result.RowCount)
	}
	if used := statement.nested.frame.intermediate.used; used != 0 {
		t.Fatalf("statement retained %d intermediate bytes", used)
	}
}

func TestSQLSetLoweringCancellationNoPartialAndWarmZeroAlloc(t *testing.T) {
	database := setStatementDatabase(t)
	statement, err := PrepareStatement(
		`SELECT v AS value FROM set_left WHERE v >= ? ` +
			`UNION DISTINCT SELECT v FROM set_right WHERE v <= ? ` +
			`ORDER BY value DESC LIMIT ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	low, high, limit := int64(1), int64(4), int64(3)
	args := []any{&low, &high, &limit}
	source := FromDatabase(database.Snapshot(), statement.Collection())
	var cancel CancelFlag
	var execution Exec
	execution.Options.Cancel = &cancel
	run := func() {
		_, err = statement.RunInto(&execution, source, args)
	}
	run()
	if err != nil || execution.Result.RowCount != 3 {
		t.Fatalf("warm run rows/error = %d/%v", execution.Result.RowCount, err)
	}
	if allocations := testing.AllocsPerRun(100, run); allocations != 0 {
		t.Fatalf("warmed SQL set allocations = %.2f, want 0", allocations)
	}
	if err != nil {
		t.Fatal(err)
	}

	cancel.Cancel()
	run()
	if !errors.Is(err, ErrCanceled) || execution.Result.RowCount != 0 {
		t.Fatalf("canceled run rows/error = %d/%v", execution.Result.RowCount, err)
	}
	cancel.Reset()
	run()
	if err != nil || execution.Result.RowCount != 3 {
		t.Fatalf("recovery rows/error = %d/%v", execution.Result.RowCount, err)
	}
}
