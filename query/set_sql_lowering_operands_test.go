package query

import (
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestSQLSetValuesOperandsAllModesRootTailsAndParameters(t *testing.T) {
	tests := []struct {
		operation string
		want      []string
	}{
		{"UNION ALL", []string{"1", "2", "2", "2", "3"}},
		{"UNION DISTINCT", []string{"1", "2", "3"}},
		{"INTERSECT ALL", []string{"2"}},
		{"INTERSECT DISTINCT", []string{"2"}},
		{"EXCEPT ALL", []string{"1", "2"}},
		{"EXCEPT DISTINCT", []string{"1"}},
	}
	for _, test := range tests {
		t.Run(test.operation, func(t *testing.T) {
			statement, err := PrepareStatement(
				`VALUES (?), (2), (2) ` + test.operation +
					` VALUES (2), (?) ORDER BY column1`,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			if statement.Collection() != "" || statement.RequiresCatalog() {
				t.Fatalf("source-independent classification = %q/%v",
					statement.Collection(), statement.RequiresCatalog())
			}
			if got := statement.Columns(); !reflect.DeepEqual(got, []string{"column1"}) {
				t.Fatalf("columns = %v", got)
			}
			var execution Exec
			cursor, err := statement.RunInto(
				&execution, Source{}, []any{int64(1), int64(3)},
			)
			if err != nil {
				t.Fatal(err)
			}
			if got := setStatementCursorJSON(cursor); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("rows = %v, want %v", got, test.want)
			}
		})
	}

	grouped, err := PrepareStatement(
		`(VALUES (3, 'c'), (1, 'a'), (2, 'b') ` +
			`ORDER BY column1 LIMIT ? OFFSET ?) ` +
			`UNION ALL VALUES (4, NULL) ORDER BY column1 DESC LIMIT ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer grouped.Release()
	var execution Exec
	cursor, err := grouped.RunInto(
		&execution, Source{}, []any{int64(2), int64(1), int64(2)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := setStatementCursorJSON(cursor), []string{`4,null`, `3,"c"`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("grouped VALUES rows = %v, want %v", got, want)
	}
	plan, err := grouped.ExplainBound([]any{int64(2), int64(1), int64(2)})
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		`"kind":"values"`, `"access_path":"constructed-rows"`, `"rows":3`,
	} {
		if !strings.Contains(plan, fragment) {
			t.Fatalf("VALUES EXPLAIN %s lacks %s", plan, fragment)
		}
	}
	typed, err := PrepareStatement(
		`VALUES (1, 'a', NULL), (2, 'b', NULL) UNION ALL ` +
			`VALUES (3, 'c', NULL)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	schema := typed.AppendSchema(nil)
	typed.Release()
	if len(schema) != 3 || schema[0].Type != TypeNumber ||
		schema[1].Type != TypeString || schema[2].Type != TypeNull {
		t.Fatalf("VALUES schema types = %+v", schema)
	}
}

func TestSQLSetTableRootCTESharingAndFirstMetadata(t *testing.T) {
	database := setStatementDatabase(t)

	root, err := PrepareStatement(`TABLE set_left`)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Release()
	if root.Collection() != "set_left" || root.RequiresCatalog() {
		t.Fatalf("TABLE root classification = %q/%v",
			root.Collection(), root.RequiresCatalog())
	}
	var rootExecution Exec
	rootCursor, err := root.RunInto(
		&rootExecution, FromDatabase(database.Snapshot(), root.Collection()), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := setStatementCursorJSON(rootCursor); len(got) != 4 ||
		got[0] != `{"v":1,"tag":"l1"}` {
		t.Fatalf("TABLE root rows = %v", got)
	}
	grouped, err := PrepareStatement(
		`(TABLE set_left ORDER BY "*" LIMIT ?) ` +
			`UNION ALL TABLE set_left LIMIT ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	var groupedExecution Exec
	groupedCursor, err := grouped.RunInto(
		&groupedExecution,
		FromDatabase(database.Snapshot(), grouped.Collection()),
		[]any{int64(2), int64(3)},
	)
	if err != nil {
		grouped.Release()
		t.Fatal(err)
	}
	if got := len(setStatementCursorJSON(groupedCursor)); got != 3 {
		grouped.Release()
		t.Fatalf("group-tailed TABLE rows = %d, want 3", got)
	}
	grouped.Release()

	statement, err := PrepareStatement(
		`WITH c AS (SELECT v AS value FROM set_left WHERE v <= ?) ` +
			`SELECT value AS stable FROM c UNION ALL TABLE c`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if got := statement.Columns(); !reflect.DeepEqual(got, []string{"stable"}) {
		t.Fatalf("first-operand metadata = %v", got)
	}
	catalog := statement.cteCatalog()
	if catalog == nil || len(catalog.defs) != 1 || catalog.defs[0].references != 2 {
		t.Fatalf("TABLE CTE catalog = %+v", catalog)
	}
	var execution Exec
	cursor, err := statement.RunInto(
		&execution,
		FromDatabase(database.Snapshot(), statement.Collection()),
		[]any{int64(2)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := setStatementCursorJSON(cursor),
		[]string{"1", "2", "2", "1", "2", "2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("TABLE CTE rows = %v, want %v", got, want)
	}
	if catalog.defs[0].runEvaluations != 1 {
		t.Fatalf("CTE evaluations = %d, want one", catalog.defs[0].runEvaluations)
	}
	invalid := `WITH c AS (SELECT v, tag FROM set_left) ` +
		`SELECT v FROM c UNION TABLE c`
	_, err = PrepareStatement(invalid)
	var arity *SetSQLArityError
	if !errors.As(err, &arity) || arity.Position() != strings.Index(invalid, "UNION") ||
		arity.Left != 1 || arity.Right != 2 {
		t.Fatalf("TABLE deferred arity error = %T %+v", err, arity)
	}

	for _, test := range []struct {
		operation string
		rows      int
	}{
		{"UNION ALL", 8},
		{"UNION DISTINCT", 4},
		{"INTERSECT ALL", 4},
		{"INTERSECT DISTINCT", 4},
		{"EXCEPT ALL", 0},
		{"EXCEPT DISTINCT", 0},
	} {
		chain, prepareErr := PrepareStatement(
			`TABLE set_left ` + test.operation + ` TABLE set_left`,
		)
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		var chainExecution Exec
		chainCursor, runErr := chain.RunInto(
			&chainExecution,
			FromDatabase(database.Snapshot(), chain.Collection()), nil,
		)
		if runErr != nil {
			chain.Release()
			t.Fatal(runErr)
		}
		got := len(setStatementCursorJSON(chainCursor))
		chain.Release()
		if got != test.rows {
			t.Fatalf("TABLE %s rows = %d, want %d", test.operation, got, test.rows)
		}
	}
}

func TestSQLSetValuesWarmZeroAllocBudgetCancellationAndRecovery(t *testing.T) {
	statement, err := PrepareStatement(
		`VALUES (?, ?), (?, NULL) UNION DISTINCT VALUES (?, ?)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	a, b, c, d, e := int64(1), "one", int64(2), int64(1), "one"
	args := []any{&a, &b, &c, &d, &e}
	var cancel CancelFlag
	var execution Exec
	execution.Options.Cancel = &cancel
	run := func() {
		_, err = statement.RunInto(&execution, Source{}, args)
	}
	run()
	if err != nil || execution.Result.RowCount != 2 {
		t.Fatalf("warm VALUES result = %d/%v", execution.Result.RowCount, err)
	}
	if allocations := testing.AllocsPerRun(100, run); allocations != 0 {
		t.Fatalf("warmed VALUES set allocated %.2f/run, want 0", allocations)
	}

	cancel.Cancel()
	run()
	if !errors.Is(err, ErrCanceled) || execution.Result.RowCount != 0 {
		t.Fatalf("canceled VALUES result = %d/%v", execution.Result.RowCount, err)
	}
	cancel.Reset()
	execution.Options.ResultRows = 1
	run()
	if !errors.Is(err, ErrResultBudget) || execution.Result.RowCount != 0 {
		t.Fatalf("row-bounded VALUES result = %d/%T %v",
			execution.Result.RowCount, err, err)
	}
	execution.Options.ResultRows = -1
	execution.Options.IntermediateBytes = 1
	run()
	var budget *IntermediateBudgetError
	if !errors.As(err, &budget) || execution.Result.RowCount != 0 {
		t.Fatalf("bounded VALUES result = %d/%T %v",
			execution.Result.RowCount, err, err)
	}
	execution.Options.IntermediateBytes = -1
	run()
	if err != nil || execution.Result.RowCount != 2 {
		t.Fatalf("VALUES recovery = %d/%v", execution.Result.RowCount, err)
	}
}

func TestSQLSetValuesExactIdentityAndIndependentStatementRace(t *testing.T) {
	identity, err := PrepareStatement(
		`VALUES (1), (1.0), (NULL), ('1') ` +
			`UNION DISTINCT VALUES (1e0), (NULL), ('1')`,
	)
	if err != nil {
		t.Fatal(err)
	}
	var identityExecution Exec
	cursor, err := identity.RunInto(&identityExecution, Source{}, nil)
	if err != nil {
		identity.Release()
		t.Fatal(err)
	}
	if got, want := setStatementCursorJSON(cursor), []string{"1", "null", `"1"`}; !reflect.DeepEqual(got, want) {
		identity.Release()
		t.Fatalf("exact VALUES identity = %v, want %v", got, want)
	}
	identity.Release()

	const workers = 8
	var wait sync.WaitGroup
	errorsFound := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			statement, prepareErr := PrepareStatement(
				`VALUES (?), (?) UNION DISTINCT VALUES (?)`,
			)
			if prepareErr != nil {
				errorsFound <- prepareErr
				return
			}
			defer statement.Release()
			value, other := int64(worker), int64(worker+1)
			args := []any{&value, &other, &value}
			var execution Exec
			for run := 0; run < 50; run++ {
				cursor, runErr := statement.RunInto(&execution, Source{}, args)
				if runErr != nil {
					errorsFound <- runErr
					return
				}
				if execution.Result.RowCount != 2 || !cursor.Next() {
					errorsFound <- errors.New("independent VALUES statement returned wrong shape")
					return
				}
			}
		}(worker)
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Fatal(err)
	}
}
