package query

import (
	"errors"
	"fmt"
	"testing"
)

func TestStatementScalarOrderedStableMergeAllLengths(t *testing.T) {
	for rows := 0; rows <= 129; rows++ {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			ordered := statementScalarOrdered{
				order:  []statementScalarOrder{{}},
				rows:   make([]statementScalarOrderRow, rows),
				values: make([]scalar, rows),
			}
			for row := range rows {
				ordered.rows[row] = statementScalarOrderRow{input: row, keyBase: row}
				ordered.values[row] = scalar{
					kind: kindNumber, isInt: true, ival: int64((row * 17) % 11),
				}
			}
			if err := ordered.sort(nil); err != nil {
				t.Fatal(err)
			}
			for i := 1; i < rows; i++ {
				left, right := ordered.rows[i-1], ordered.rows[i]
				leftValue := ordered.values[left.keyBase].ival
				rightValue := ordered.values[right.keyBase].ival
				if leftValue > rightValue || leftValue == rightValue && left.input > right.input {
					t.Fatalf("unstable order at %d: (%d,%d) then (%d,%d)",
						i, leftValue, left.input, rightValue, right.input)
				}
			}
		})
	}
	var cancel CancelFlag
	cancel.Cancel()
	ordered := statementScalarOrdered{
		order:  []statementScalarOrder{{}},
		rows:   []statementScalarOrderRow{{input: 1, keyBase: 0}, {input: 0, keyBase: 1}},
		values: []scalar{{kind: kindNumber, isInt: true, ival: 2}, {kind: kindNumber, isInt: true, ival: 1}},
	}
	if err := ordered.sort(&cancel); !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled stable sort = %v, want ErrCanceled", err)
	}
}

func TestSQLScalarOrderByComputedAliasMixedKeysStableAndTail(t *testing.T) {
	segment := mustSegment(t,
		`{"id":"a","n":2}`,
		`{"id":"b","n":1}`,
		`{"id":"c","n":2}`,
		`{"id":"d","n":null}`,
	)
	statement, err := PrepareStatement(`
		SELECT id, n * 2 AS score
		FROM docs
		ORDER BY score DESC, id DESC
		OFFSET 1 LIMIT 2`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	for row, want := range [][2]string{{`"a"`, `4`}, {`"b"`, `2`}} {
		if !cursor.Next() {
			t.Fatalf("missing row %d", row)
		}
		if got := string(cursor.Cell(0).JSON()); got != want[0] {
			t.Fatalf("row %d id = %s, want %s", row, got, want[0])
		}
		if got := string(cursor.Cell(1).JSON()); got != want[1] {
			t.Fatalf("row %d score = %s, want %s", row, got, want[1])
		}
	}
	if cursor.Next() {
		t.Fatal("ordered tail returned an extra row")
	}

	stable, err := PrepareStatement(`SELECT id, n + 0 AS score FROM docs ORDER BY score`)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err = stable.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	for row, want := range []string{`"d"`, `"b"`, `"a"`, `"c"`} {
		if !cursor.Next() || string(cursor.Cell(0).JSON()) != want {
			t.Fatalf("stable row %d != %s", row, want)
		}
	}
}

func TestSQLOrderByOutputPositionsAcrossPathScalarAndAggregate(t *testing.T) {
	segment := mustSegment(t,
		`{"id":"a","team":"x","n":2}`,
		`{"id":"b","team":"y","n":4}`,
		`{"id":"c","team":"x","n":3}`,
	)
	var exec Exec

	path, err := PrepareStatement(`SELECT id, n FROM docs ORDER BY 2 DESC, 1`)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := path.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	for row, want := range []string{`"b"`, `"c"`, `"a"`} {
		if !cursor.Next() || string(cursor.Cell(0).JSON()) != want {
			t.Fatalf("path ordinal row %d != %s", row, want)
		}
	}
	if cursor.Next() {
		t.Fatal("path ordinal returned an extra row")
	}

	scalarStatement, err := PrepareStatement(`
		SELECT id, n * 2 FROM docs ORDER BY 2 DESC, 1`)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err = scalarStatement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	for row, want := range [][2]string{{`"b"`, `8`}, {`"c"`, `6`}, {`"a"`, `4`}} {
		if !cursor.Next() || string(cursor.Cell(0).JSON()) != want[0] ||
			string(cursor.Cell(1).JSON()) != want[1] {
			t.Fatalf("scalar ordinal row %d != %v", row, want)
		}
	}

	aggregate, err := PrepareStatement(`
		SELECT team, SUM(n) FROM docs GROUP BY team ORDER BY 2 DESC, 1`)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err = aggregate.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	for row, want := range [][2]string{{`"x"`, `5`}, {`"y"`, `4`}} {
		if !cursor.Next() || string(cursor.Cell(0).JSON()) != want[0] ||
			string(cursor.Cell(1).JSON()) != want[1] {
			t.Fatalf("aggregate ordinal row %d != %v", row, want)
		}
	}
	if cursor.Next() {
		t.Fatal("aggregate ordinal returned an extra row")
	}
	runAggregate := func() {
		ordered, runErr := aggregate.RunInto(&exec, FromSegment(segment), nil)
		if runErr != nil {
			panic(runErr)
		}
		for ordered.Next() {
		}
	}
	runAggregate()
	if allocs := testing.AllocsPerRun(100, runAggregate); allocs != 0 {
		t.Fatalf("warmed aggregate ordinal allocated %.2f/run", allocs)
	}

	const havingSource = `SELECT team, SUM(n) FROM docs GROUP BY team ` +
		`HAVING team = 'x' ORDER BY 2 DESC LIMIT 1`
	having, err := PrepareStatement(havingSource)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err = having.RunInto(&exec, FromSegment(segment), nil)
	if err != nil || !cursor.Next() || string(cursor.Cell(0).JSON()) != `"x"` ||
		string(cursor.Cell(1).JSON()) != `5` || cursor.Next() {
		t.Fatalf("aggregate ordinal/HAVING cursor=%+v err=%v", cursor, err)
	}

	counted, err := PrepareStatement(`SELECT DISTINCT COUNT(*) FROM docs ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err = counted.RunInto(&exec, FromSegment(segment), nil)
	if err != nil || !cursor.Next() || string(cursor.Cell(0).JSON()) != `3` || cursor.Next() {
		t.Fatalf("single aggregate ordinal cursor=%+v err=%v", cursor, err)
	}
}

func TestSQLScalarOrderByHavingFiltersBeforeKeysAndTail(t *testing.T) {
	segment := mustSegment(t,
		`{"team":"x","n":2,"v":"2"}`,
		`{"team":"y","n":100,"v":"bad"}`,
		`{"team":"z","n":7,"v":"7"}`,
		`{"team":"x","n":3,"v":"3"}`,
	)
	var exec Exec

	// The rejected high group must not occupy LIMIT's only slot. The group
	// key is intentionally hidden from the output, which also proves Cursor
	// does not try to apply HAVING again after publication truncates hidden
	// dependency columns.
	statement, err := PrepareStatement(`
		SELECT SUM(n) AS total FROM docs GROUP BY team
		HAVING team <> 'y' ORDER BY 1 DESC LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil || !cursor.Next() {
		t.Fatalf("HAVING-before-tail cursor=%+v err=%v", cursor, err)
	}
	if got := string(cursor.Cell(0).JSON()); got != `7` || cursor.Next() {
		t.Fatalf("HAVING-before-tail got=%s cursor=%+v", got, cursor)
	}

	// Scalar dependencies are deduplicated in first-use order, which need not
	// equal authored SELECT ordinals. HAVING's direct aggregate must be mapped
	// into that dependency namespace before it filters.
	reordered, err := PrepareStatement(`
		SELECT SUM(n) + 1 AS score, SUM(n) AS total, team
		FROM docs GROUP BY team HAVING SUM(n) >= 5
		ORDER BY score DESC OFFSET 1 LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err = reordered.RunInto(&exec, FromSegment(segment), nil)
	if err != nil || !cursor.Next() || string(cursor.Cell(2).JSON()) != `"z"` ||
		string(cursor.Cell(0).JSON()) != `8` || cursor.Next() {
		t.Fatalf("reordered HAVING dependency cursor=%+v err=%v", cursor, err)
	}

	// Sort keys are eager only for groups that survive HAVING. The rejected y
	// group carries a division-by-zero arm and must never evaluate it.
	lazyKey, err := PrepareStatement(`
		SELECT team, SUM(n), CASE WHEN team = 'y' THEN 1 / 0 ELSE SUM(n) END AS score
		FROM docs GROUP BY team HAVING team <> 'y' ORDER BY score DESC`)
	if err != nil {
		t.Fatal(err)
	}
	run := func() {
		ordered, runErr := lazyKey.RunInto(&exec, FromSegment(segment), nil)
		if runErr != nil {
			t.Fatal(runErr)
		}
		for row, want := range []string{`"z"`, `"x"`} {
			if !ordered.Next() {
				t.Fatalf("HAVING lazy key missing row %d", row)
			}
			if got := string(ordered.Cell(0).JSON()); got != want {
				t.Fatalf("HAVING lazy key row %d = %s, want %s", row, got, want)
			}
		}
		if ordered.Next() {
			t.Fatal("HAVING lazy key returned an extra row")
		}
	}
	run()
	warm := func() {
		ordered, runErr := statement.RunInto(&exec, FromSegment(segment), nil)
		if runErr != nil {
			t.Fatal(runErr)
		}
		for ordered.Next() {
		}
	}
	warm()
	if allocs := testing.AllocsPerRun(100, warm); allocs != 0 {
		t.Fatalf("warmed HAVING ordered stage allocated %.2f/run", allocs)
	}

	// A path retained only to validate a pruned CASE arm is promoted in place
	// when HAVING needs it. The base result therefore carries one aggregate and
	// one hidden key, never a duplicate semantic-only copy.
	pruned, err := PrepareStatement(`
		SELECT CASE WHEN TRUE THEN 1 ELSE team END AS key, SUM(n) AS total
		FROM docs GROUP BY team HAVING team <> 'y' ORDER BY key, 2 DESC`)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(pruned.q.columns); got != 2 {
		t.Fatalf("pruned CASE/HAVING base columns = %d, want 2", got)
	}
	cursor, err = pruned.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	for row, want := range []string{`7`, `5`} {
		if !cursor.Next() || string(cursor.Cell(1).JSON()) != want {
			t.Fatalf("pruned CASE/HAVING row %d != %s", row, want)
		}
	}

	constant, err := PrepareStatement(`
		SELECT 1 AS key FROM docs GROUP BY team HAVING team <> 'y' ORDER BY key`)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err = constant.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for cursor.Next() {
		rows++
	}
	if rows != 2 {
		t.Fatalf("cardinality-only HAVING kept %d groups, want 2", rows)
	}

	parameterized, err := PrepareStatement(`
		SELECT SUM(n) FROM docs GROUP BY team HAVING SUM(n) > ? ORDER BY 1 DESC`)
	if err != nil {
		t.Fatal(err)
	}
	args := []any{int64(5)}
	runParameterized := func() {
		ordered, runErr := parameterized.RunInto(&exec, FromSegment(segment), args)
		if runErr != nil {
			t.Fatal(runErr)
		}
		for ordered.Next() {
		}
	}
	runParameterized()
	if allocs := testing.AllocsPerRun(100, runParameterized); allocs != 0 {
		t.Fatalf("warmed parameterized HAVING order allocated %.2f/run", allocs)
	}
	exec.Options.IntermediateBytes = 1
	if _, err := parameterized.RunInto(&exec, FromSegment(segment), args); !errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("tiny HAVING ordered workspace = %T %v, want ErrIntermediateBudget", err, err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("HAVING workspace failure published %d rows", exec.Result.RowCount)
	}
	exec.Options.IntermediateBytes = 0
	exec.Options.ResultBytes = 1
	if _, err := parameterized.RunInto(&exec, FromSegment(segment), args); !errors.Is(err, ErrResultBudget) {
		t.Fatalf("tiny HAVING ordered result = %T %v, want ErrResultBudget", err, err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("HAVING result failure published %d rows", exec.Result.RowCount)
	}
	exec.Options.ResultBytes = 0
	runParameterized()

	limitZero, err := PrepareStatement(`
		SELECT SUM(n) FROM docs GROUP BY team HAVING SUM(n) > 0 ORDER BY 1 LIMIT 0`)
	if err != nil {
		t.Fatal(err)
	}
	var cancel CancelFlag
	cancel.Cancel()
	exec.Options.Cancel = &cancel
	if _, err := limitZero.RunInto(&exec, FromSegment(segment), nil); !errors.Is(err, ErrCanceled) {
		t.Fatalf("prearmed HAVING/LIMIT 0 cancellation = %T %v, want ErrCanceled", err, err)
	}
	exec.Options.Cancel = nil
}

func TestSQLScalarOrderByEvaluatesKeysBeforeTailButProjectsLazily(t *testing.T) {
	badKey, err := PrepareStatement(`
		SELECT id, CAST(v AS NUMERIC) AS score
		FROM docs ORDER BY score LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	segment := mustSegment(t, `{"id":"a","v":"1"}`, `{"id":"b","v":"bad"}`)
	var exec Exec
	if _, err := badKey.RunInto(&exec, FromSegment(segment), nil); !errors.Is(err, ErrScalarInvalidText) {
		t.Fatalf("ORDER key error = %T %v, want ErrScalarInvalidText", err, err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("failed ordered key evaluation published %d rows", exec.Result.RowCount)
	}

	lazyProjection, err := PrepareStatement(`
		SELECT id, CAST(payload AS NUMERIC) AS projected, n + 0 AS score
		FROM docs ORDER BY score LIMIT 1`)
	if err != nil {
		t.Fatal(err)
	}
	segment = mustSegment(t,
		`{"id":"selected","payload":"7","n":1}`,
		`{"id":"not-selected","payload":"bad","n":2}`,
	)
	cursor, err := lazyProjection.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatalf("unselected projection was evaluated: %v", err)
	}
	if !cursor.Next() || string(cursor.Cell(0).JSON()) != `"selected"` ||
		string(cursor.Cell(1).JSON()) != `7` || cursor.Next() {
		t.Fatal("lazy ordered projection returned the wrong row")
	}

	limitZero, err := PrepareStatement(`
		SELECT CAST(v AS NUMERIC) AS score FROM docs ORDER BY score LIMIT 0`)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err = limitZero.RunInto(&exec, FromSegment(segment), nil)
	if err != nil || cursor.Next() {
		t.Fatalf("LIMIT 0 evaluated ordered keys or returned a row: cursor=%+v err=%v", cursor, err)
	}
}

func TestSQLScalarOrderByCaseAliasUsesCompleteProgram(t *testing.T) {
	segment := mustSegment(t,
		`{"id":"a","n":1}`,
		`{"id":"b","n":3}`,
		`{"id":"c","n":2}`,
	)
	statement, err := PrepareStatement(`
		SELECT id, CASE WHEN n >= 2 THEN n * 10 ELSE n END AS score
		FROM docs ORDER BY score DESC`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	for row, want := range [][2]string{{`"b"`, `3e1`}, {`"c"`, `2e1`}, {`"a"`, `1`}} {
		if !cursor.Next() || string(cursor.Cell(0).JSON()) != want[0] ||
			string(cursor.Cell(1).JSON()) != want[1] {
			t.Fatalf("CASE ordered row %d != %v", row, want)
		}
	}
}

func TestSQLScalarOrderByGroupedAggregateAlias(t *testing.T) {
	segment := mustSegment(t,
		`{"team":"a","n":1}`,
		`{"team":"b","n":4}`,
		`{"team":"a","n":2}`,
	)
	statement, err := PrepareStatement(`
		SELECT team, SUM(n) + 1 AS score
		FROM docs GROUP BY team ORDER BY score DESC, team`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	cursor, err := statement.RunInto(&exec, FromSegment(segment), nil)
	if err != nil {
		t.Fatal(err)
	}
	for row, want := range [][2]string{{`"b"`, `5`}, {`"a"`, `4`}} {
		if !cursor.Next() || string(cursor.Cell(0).JSON()) != want[0] ||
			string(cursor.Cell(1).JSON()) != want[1] {
			t.Fatalf("grouped ordered row %d != %v", row, want)
		}
	}
}

func TestSQLScalarOrderByLargeLimitCannotOverflowTail(t *testing.T) {
	segment := mustSegment(t, `{"n":3}`, `{"n":1}`, `{"n":2}`)
	statement, err := PrepareStatement(`
		SELECT n + 0 AS score FROM docs ORDER BY score OFFSET 1 LIMIT ?`)
	if err != nil {
		t.Fatal(err)
	}
	limit := int64(int(^uint(0) >> 1))
	var exec Exec
	cursor, err := statement.RunInto(&exec, FromSegment(segment), []any{limit})
	if err != nil {
		t.Fatal(err)
	}
	for row, want := range []string{"2", "3"} {
		if !cursor.Next() || string(cursor.Cell(0).JSON()) != want {
			t.Fatalf("large LIMIT row %d != %s", row, want)
		}
	}
	if cursor.Next() {
		t.Fatal("large LIMIT returned an extra row")
	}
}

func TestSQLScalarOrderByParameterRebindAndResultBudgetRecovery(t *testing.T) {
	segment := mustSegment(t, `{"id":"a","n":1}`, `{"id":"b","n":3}`, `{"id":"c","n":2}`)
	statement, err := PrepareStatement(`
		SELECT id, n * ? AS score FROM docs ORDER BY score`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	check := func(multiplier int64, want []string) {
		t.Helper()
		cursor, runErr := statement.RunInto(&exec, FromSegment(segment), []any{multiplier})
		if runErr != nil {
			t.Fatal(runErr)
		}
		for row := range want {
			if !cursor.Next() || string(cursor.Cell(0).JSON()) != want[row] {
				t.Fatalf("multiplier %d row %d != %s", multiplier, row, want[row])
			}
		}
	}
	check(1, []string{`"a"`, `"c"`, `"b"`})
	check(-1, []string{`"b"`, `"c"`, `"a"`})

	exec.Options.ResultBytes = 1
	if _, err := statement.RunInto(&exec, FromSegment(segment), []any{int64(1)}); !errors.Is(err, ErrResultBudget) {
		t.Fatalf("tiny ordered result budget = %T %v, want ErrResultBudget", err, err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("ordered result budget failure published %d rows", exec.Result.RowCount)
	}
	exec.Options.ResultBytes = 0
	check(1, []string{`"a"`, `"c"`, `"b"`})
}

func TestSQLScalarOrderByBudgetCancellationRecoveryAndWarmZeroAlloc(t *testing.T) {
	segment := mustSegment(t,
		`{"id":"a","s":"ccc"}`,
		`{"id":"b","s":"a"}`,
		`{"id":"c","s":"bb"}`,
	)
	statement, err := PrepareStatement(`SELECT id, s || '!' AS key FROM docs ORDER BY key`)
	if err != nil {
		t.Fatal(err)
	}
	var exec Exec
	exec.Options.IntermediateBytes = 1
	if _, err := statement.RunInto(&exec, FromSegment(segment), nil); !errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("tiny ordered workspace = %T %v, want ErrIntermediateBudget", err, err)
	}
	if exec.Result.RowCount != 0 {
		t.Fatalf("budget failure published %d rows", exec.Result.RowCount)
	}

	exec.Options.IntermediateBytes = 0
	run := func() {
		cursor, runErr := statement.RunInto(&exec, FromSegment(segment), nil)
		if runErr != nil {
			t.Fatal(runErr)
		}
		for row, want := range []string{`"b"`, `"c"`, `"a"`} {
			if !cursor.Next() || string(cursor.Cell(0).JSON()) != want {
				t.Fatalf("warm row %d != %s", row, want)
			}
		}
		if cursor.Next() {
			t.Fatal("warm ordered result returned extra row")
		}
	}
	run()
	if allocs := testing.AllocsPerRun(100, run); allocs != 0 {
		t.Fatalf("warmed scalar ORDER BY allocated %.2f/run", allocs)
	}

	var cancel CancelFlag
	cancel.Cancel()
	exec.Options.Cancel = &cancel
	if _, err := statement.RunInto(&exec, FromSegment(segment), nil); !errors.Is(err, ErrCanceled) {
		t.Fatalf("ordered cancellation = %T %v, want ErrCanceled", err, err)
	}
	exec.Options.Cancel = nil
	run()
}
