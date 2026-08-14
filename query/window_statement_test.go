package query

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
)

func windowStatementDatabase(t testing.TB) *store.Database {
	t.Helper()
	database := &store.Database{}
	collection, err := database.CreateCollection("events", store.Options{ChunkDocuments: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i, document := range []string{
		`{"id":"a","team":"x","score":1,"value":10}`,
		`{"id":"b","team":"x","score":1.0,"value":20}`,
		`{"id":"c","team":"x","score":2,"value":5}`,
		`{"id":"d","team":"y","score":1,"value":7}`,
		`{"id":"e","team":"y","score":2,"value":3}`,
	} {
		if _, err := collection.Put(fmt.Sprintf("event-%d", i), []byte(document)); err != nil {
			t.Fatal(err)
		}
	}
	return database
}

func TestWindowFunctionLoweringIsExhaustive(t *testing.T) {
	tests := [...]struct {
		sql      sqlast.WindowFunctionKind
		physical windowFunctionKind
	}{
		{sqlast.WindowRowNumber, windowRowNumber},
		{sqlast.WindowRank, windowRank},
		{sqlast.WindowDenseRank, windowDenseRank},
		{sqlast.WindowLag, windowLag},
		{sqlast.WindowLead, windowLead},
		{sqlast.WindowCount, windowCount},
		{sqlast.WindowSum, windowSum},
		{sqlast.WindowAvg, windowAvg},
		{sqlast.WindowMin, windowMin},
		{sqlast.WindowMax, windowMax},
		{sqlast.WindowNTile, windowNTile},
		{sqlast.WindowPercentRank, windowPercentRank},
		{sqlast.WindowCumeDist, windowCumeDist},
		{sqlast.WindowFirstValue, windowFirstValue},
		{sqlast.WindowLastValue, windowLastValue},
		{sqlast.WindowNthValue, windowNthValue},
	}
	for _, test := range tests {
		got, err := lowerWindowFunctionKind(test.sql)
		if err != nil || got != test.physical {
			t.Fatalf("lowerWindowFunctionKind(%s) = %d, %v; want %d", test.sql, got, err, test.physical)
		}
	}
	if _, err := lowerWindowFunctionKind(sqlast.WindowFunctionKind(255)); err == nil {
		t.Fatal("invalid SQL window function kind was accepted")
	}

	for _, kind := range []sqlast.WindowFunctionKind{
		sqlast.WindowCount, sqlast.WindowSum, sqlast.WindowAvg,
		sqlast.WindowMin, sqlast.WindowMax, sqlast.WindowFirstValue,
		sqlast.WindowLastValue, sqlast.WindowNthValue,
	} {
		if !sqlWindowFunctionUsesFrame(kind) {
			t.Fatalf("%s does not use a frame", kind)
		}
	}
	for _, kind := range []sqlast.WindowFunctionKind{
		sqlast.WindowRowNumber, sqlast.WindowRank, sqlast.WindowDenseRank,
		sqlast.WindowLag, sqlast.WindowLead, sqlast.WindowNTile,
		sqlast.WindowPercentRank, sqlast.WindowCumeDist,
	} {
		if sqlWindowFunctionUsesFrame(kind) {
			t.Fatalf("%s unexpectedly uses a frame", kind)
		}
	}
}

func TestSQLWindowDefaultFrameIncludesPeers(t *testing.T) {
	database := windowStatementDatabase(t)
	statement, err := PrepareStatement(`
		SELECT id,
			SUM(value) OVER (PARTITION BY team ORDER BY score) AS peer_running,
			SUM(value) OVER (PARTITION BY team) AS partition_total,
			SUM(value) OVER (
				PARTITION BY team ORDER BY score
				ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			) AS row_running,
			SUM(value) OVER (
				PARTITION BY team ORDER BY score
				GROUPS BETWEEN CURRENT ROW AND CURRENT ROW
			) AS peer_total
		FROM events ORDER BY 2 DESC, 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	got := runStatement(t, statement, FromDatabase(database.Snapshot(), "events"))
	wantRows := strings.Join([]string{
		`4:"c"|3:35|3:35|3:35|3:5|`,
		`4:"a"|3:30|3:35|3:10|3:30|`,
		`4:"b"|3:30|3:35|3:30|3:30|`,
		`4:"e"|3:10|3:10|3:10|3:3|`,
		`4:"d"|3:7|3:10|3:7|3:7|`,
	}, "\n")
	if rows := strings.TrimSpace(strings.SplitN(got, "\n", 2)[1]); rows != wantRows {
		t.Fatalf("rows:\n%s\nwant:\n%s", rows, wantRows)
	}
	window := statement.window()
	if len(window.stages) != 2 || window.stages[0].plan.functions[0].frame.unit != windowFrameGroups {
		t.Fatalf("default frame/stages = unit %d, stages %d; want GROUPS, 2",
			window.stages[0].plan.functions[0].frame.unit, len(window.stages))
	}
}

func TestSQLWindowExactRangeExclusionsAndNamedReuse(t *testing.T) {
	database := windowStatementDatabase(t)
	statement, err := PrepareStatement(`
		SELECT id,
			SUM(value) OVER (ordered RANGE BETWEEN 0.50 PRECEDING AND 5e-1 FOLLOWING EXCLUDE NO OTHERS) AS all_rows,
			SUM(value) OVER (ordered RANGE BETWEEN 0.5 PRECEDING AND 0.5 FOLLOWING EXCLUDE CURRENT ROW) AS no_current,
			SUM(value) OVER (ordered RANGE BETWEEN 0.5 PRECEDING AND 0.5 FOLLOWING EXCLUDE GROUP) AS no_group,
			SUM(value) OVER (ordered RANGE BETWEEN 0.5 PRECEDING AND 0.5 FOLLOWING EXCLUDE TIES) AS no_ties
		FROM events
		WINDOW partitioned AS (PARTITION BY team),
		       ordered AS (partitioned ORDER BY score)
		ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	got := runStatement(t, statement, FromDatabase(database.Snapshot(), "events"))
	wantRows := strings.Join([]string{
		`4:"a"|3:30|3:20|1:null|3:10|`,
		`4:"b"|3:30|3:10|1:null|3:20|`,
		`4:"c"|3:5|1:null|1:null|3:5|`,
		`4:"d"|3:7|1:null|1:null|3:7|`,
		`4:"e"|3:3|1:null|1:null|3:3|`,
	}, "\n")
	if rows := strings.TrimSpace(strings.SplitN(got, "\n", 2)[1]); rows != wantRows {
		t.Fatalf("rows:\n%s\nwant:\n%s", rows, wantRows)
	}
	window := statement.window()
	if len(window.stages) != 1 || len(window.stages[0].plan.functions) != 4 {
		t.Fatalf("named/equivalent sort reuse = %d stages, %d functions",
			len(window.stages), len(window.stages[0].plan.functions))
	}
	for i := range window.stages[0].plan.functions {
		frame := window.stages[0].plan.functions[i].frame
		if frame.unit != windowFrameRange || frame.exclusion != windowFrameExclusion(i) {
			t.Fatalf("function %d frame = %+v", i, frame)
		}
	}
}

func TestSQLWindowRangePeerCurrentAndUnboundedBounds(t *testing.T) {
	database := windowStatementDatabase(t)
	statement, err := PrepareStatement(`
		SELECT id,
			SUM(value) OVER (ordered RANGE CURRENT ROW) AS peers,
			SUM(value) OVER (
				ordered RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
			) AS prefix,
			SUM(value) OVER (
				ordered RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
			) AS suffix
		FROM events WINDOW ordered AS (PARTITION BY team ORDER BY score)
		ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	got := runStatement(t, statement, FromDatabase(database.Snapshot(), "events"))
	wantRows := strings.Join([]string{
		`4:"a"|3:30|3:30|3:35|`,
		`4:"b"|3:30|3:30|3:35|`,
		`4:"c"|3:5|3:35|3:5|`,
		`4:"d"|3:7|3:7|3:10|`,
		`4:"e"|3:3|3:10|3:3|`,
	}, "\n")
	if rows := strings.TrimSpace(strings.SplitN(got, "\n", 2)[1]); rows != wantRows {
		t.Fatalf("rows:\n%s\nwant:\n%s", rows, wantRows)
	}
}

func TestSQLWindowRangeOffsetBindRecoveryAndExactness(t *testing.T) {
	database := windowStatementDatabase(t)
	statement, err := PrepareStatement(`
		SELECT id, SUM(value) OVER (
			PARTITION BY team ORDER BY score
			RANGE BETWEEN ? PRECEDING AND ? FOLLOWING
		) AS ranged
		FROM events ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(database.Snapshot(), "events")
	var execution Exec
	defer execution.Release()

	for _, args := range [][]any{
		{nil, Number("0.5")},
		{"0.5", Number("0.5")},
		{Number("-0.0001"), Number("0.5")},
	} {
		if _, err := statement.RunInto(&execution, source, args); !errors.Is(err, ErrWindowArgument) ||
			execution.Result.RowCount != 0 {
			t.Fatalf("invalid RANGE bind %v = %v, rows %d", args, err, execution.Result.RowCount)
		}
	}
	args := []any{Number("5e-1"), Number("0.5000")}
	cursor, err := statement.RunInto(&execution, source, args)
	if err != nil {
		t.Fatal(err)
	}
	if got := cursorKey(statement, cursor); !strings.Contains(got, `4:"a"|3:30|`) ||
		!strings.Contains(got, `4:"c"|3:5|`) {
		t.Fatalf("exact RANGE result:\n%s", got)
	}
	args[0], args[1] = Number("1"), Number("0")
	if _, err := statement.RunInto(&execution, source, args); err != nil {
		t.Fatalf("reuse after invalid binds: %v", err)
	}
}

func TestSQLWindowParameterizedZeroRangeBoundNormalizesToCurrentRow(t *testing.T) {
	database := windowStatementDatabase(t)
	statement, err := PrepareStatement(`
		SELECT id, COUNT(*) OVER (
			PARTITION BY team ORDER BY score
			RANGE BETWEEN ? FOLLOWING AND CURRENT ROW
		) AS peers FROM events ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	var execution Exec
	defer execution.Release()
	source := FromDatabase(database.Snapshot(), "events")
	if _, err := statement.RunInto(&execution, source, []any{Number("0e100")}); err != nil {
		t.Fatalf("zero-equivalent RANGE bind: %v", err)
	}
	if execution.Result.RowCount != 5 {
		t.Fatalf("zero-equivalent RANGE rows = %d, want 5", execution.Result.RowCount)
	}
	if _, err := statement.RunInto(&execution, source, []any{Number("0.1")}); !errors.Is(err, ErrWindowArgument) || execution.Result.RowCount != 0 {
		t.Fatalf("positive FOLLOWING to CURRENT = %v, rows %d",
			err, execution.Result.RowCount)
	}
}

func TestSQLWindowRangeRejectsNonnumericOrderWithoutRows(t *testing.T) {
	database := windowStatementDatabase(t)
	statement, err := PrepareStatement(`
		SELECT id, SUM(value) OVER (
			ORDER BY team RANGE BETWEEN 1 PRECEDING AND CURRENT ROW
		) AS ranged FROM events`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	var execution Exec
	defer execution.Release()
	_, err = statement.RunInto(
		&execution, FromDatabase(database.Snapshot(), "events"), nil,
	)
	if !errors.Is(err, ErrWindowArgument) || execution.Result.RowCount != 0 {
		t.Fatalf("nonnumeric RANGE order = %v, rows %d", err, execution.Result.RowCount)
	}
	window := statement.window()
	if window.activeBytes != 0 || window.inputSpool.rows != 0 ||
		window.stages[0].active != 0 {
		t.Fatalf("nonnumeric RANGE retained state = %d/%d/%d",
			window.activeBytes, window.inputSpool.rows, window.stages[0].active)
	}
}

func TestSQLWindowRangeOffsetsTreatNullAndMissingAsPeers(t *testing.T) {
	database := windowStatementDatabase(t)
	collection, ok := database.Collection("events")
	if !ok {
		t.Fatal("events collection is missing")
	}
	for key, document := range map[string]string{
		"event-null":    `{"id":"n","team":"z","score":null,"value":100}`,
		"event-missing": `{"id":"m","team":"z","value":200}`,
	} {
		if _, err := collection.Put(key, []byte(document)); err != nil {
			t.Fatal(err)
		}
	}
	statement, err := PrepareStatement(`
		SELECT id,
			COUNT(*) OVER ranged AS peer_count,
			SUM(value) OVER ranged AS peer_sum
		FROM events WHERE team = 'z'
		WINDOW ranged AS (
			PARTITION BY team ORDER BY score ASC NULLS FIRST
			RANGE BETWEEN 0.5 PRECEDING AND 1 FOLLOWING
		)
		ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	got := runStatement(t, statement, FromDatabase(database.Snapshot(), "events"))
	for _, row := range []string{
		`4:"m"|3:2|3:300|`,
		`4:"n"|3:2|3:300|`,
	} {
		if !strings.Contains(got, row) {
			t.Fatalf("NULL/missing RANGE peers lack %q:\n%s", row, got)
		}
	}
}

func TestSQLWindowAdvancedFunctionsPreparedReuse(t *testing.T) {
	database := windowStatementDatabase(t)
	statement, err := PrepareStatement(`
		SELECT id,
			ROW_NUMBER() OVER (PARTITION BY team ORDER BY score) AS row_number,
			RANK() OVER (PARTITION BY team ORDER BY score) AS rank,
			DENSE_RANK() OVER (PARTITION BY team ORDER BY score) AS dense_rank,
			NTILE(?) OVER (PARTITION BY team ORDER BY score) AS tile,
			PERCENT_RANK() OVER (PARTITION BY team ORDER BY score) AS percent_rank,
			CUME_DIST() OVER (PARTITION BY team ORDER BY score) AS cume_dist,
			FIRST_VALUE(value) OVER (PARTITION BY team ORDER BY score) AS first_value,
			LAST_VALUE(value) OVER (PARTITION BY team ORDER BY score) AS last_value,
			NTH_VALUE(value, ?) OVER (PARTITION BY team ORDER BY score) AS nth_value
		FROM events ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(database.Snapshot(), "events")
	var execution Exec
	defer execution.Release()
	args := []any{int64(2), int64(2)}
	run := func() string {
		cursor, err := statement.RunInto(&execution, source, args)
		if err != nil {
			t.Fatal(err)
		}
		return cursorKey(statement, cursor)
	}
	first := run()
	if second := run(); second != first {
		t.Fatalf("prepared reuse changed output:\n%s\nthen:\n%s", first, second)
	}
	if len(execution.Result.Columns) != len(statement.Columns()) {
		t.Fatalf("Result schema width = %d, Statement schema width = %d",
			len(execution.Result.Columns), len(statement.Columns()))
	}
	for i, name := range statement.Columns() {
		if execution.Result.Columns[i].Header != name {
			t.Fatalf("Result column %d = %q, Statement column = %q",
				i, execution.Result.Columns[i].Header, name)
		}
	}
	for _, fragment := range []string{
		`4:"a"|3:1|3:1|3:1|3:1|3:0|3:0.6666666666666666666666666666666667|3:10|3:20|3:20|`,
		`4:"b"|3:2|3:1|3:1|3:1|3:0|3:0.6666666666666666666666666666666667|3:10|3:20|3:20|`,
		`4:"c"|3:3|3:3|3:2|3:2|3:1|3:1|3:10|3:5|3:20|`,
	} {
		if !strings.Contains(first, fragment) {
			t.Fatalf("advanced window output lacks %q:\n%s", fragment, first)
		}
	}
	if len(statement.window().stages) != 1 || len(statement.window().stages[0].plan.functions) != 9 {
		t.Fatalf("sort reuse stages/functions = %d/%d, want 1/9",
			len(statement.window().stages), len(statement.window().stages[0].plan.functions))
	}

	args[0] = int64(0)
	if _, err := statement.RunInto(&execution, source, args); err == nil ||
		!strings.Contains(err.Error(), "greater than zero") || execution.Result.RowCount != 0 {
		t.Fatalf("zero NTILE bind = %v, rows %d", err, execution.Result.RowCount)
	}
	args[0], args[1] = int64(2), int64(0)
	if _, err := statement.RunInto(&execution, source, args); err == nil ||
		!strings.Contains(err.Error(), "greater than zero") || execution.Result.RowCount != 0 {
		t.Fatalf("zero NTH_VALUE bind = %v, rows %d", err, execution.Result.RowCount)
	}
}

func TestSQLWindowBudgetCancellationCleanupAndReuse(t *testing.T) {
	database := windowStatementDatabase(t)
	statement, err := PrepareStatement(`
		SELECT id, SUM(value) OVER ranged AS running FROM events
		WINDOW ranged AS (PARTITION BY team ORDER BY score
			RANGE BETWEEN 0.5 PRECEDING AND CURRENT ROW EXCLUDE TIES)
		ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(database.Snapshot(), "events")
	var execution Exec
	execution.Options.IntermediateBytes = 1
	_, err = statement.RunInto(&execution, source, nil)
	var budget *IntermediateBudgetError
	if !errors.As(err, &budget) || execution.Result.RowCount != 0 {
		t.Fatalf("budget failure = %v, rows %d", err, execution.Result.RowCount)
	}
	window := statement.window()
	if window.activeBytes != 0 || window.inputSpool.rows != 0 || window.stages[0].active != 0 {
		t.Fatalf("budget failure retained input/stage = %d/%d/%d",
			window.activeBytes, window.inputSpool.rows, window.stages[0].active)
	}

	var cancel CancelFlag
	execution.Options = ExecOptions{IntermediateBytes: -1, Cancel: &cancel}
	cancel.Cancel()
	if _, err := statement.RunInto(&execution, source, nil); !errors.Is(err, ErrCanceled) {
		t.Fatalf("cancellation error = %v", err)
	}
	cancel.Reset()
	cursor, err := statement.RunInto(&execution, source, nil)
	next := cursor.Next()
	if err != nil || !next {
		t.Fatalf("reuse after cancellation = row %t, error %v", next, err)
	}
}

func TestSQLWindowExplainTruthfulSortReuseAndAnalyze(t *testing.T) {
	database := windowStatementDatabase(t)
	statement, err := PrepareStatement(`
		SELECT id,
			SUM(value) OVER (
				PARTITION BY team ORDER BY score DESC NULLS LAST
			) AS running,
			LAG(value, 1, NULL) OVER (
				PARTITION BY team ORDER BY score DESC NULLS LAST
			) AS previous
		FROM events WHERE value >= 3 ORDER BY id LIMIT 4`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	plan, err := statement.Explain()
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		`"node":"window"`,
		`"access_path":"full-scan"`,
		`"predicate":{"kind":"comparison","path":"value","operator":"\u003e="}`,
		`"windows":[{"algorithm":"stable-merge-sort"`,
		`"partition_by":["team"]`,
		`"order_by":[{"path":"score","direction":"desc","nulls":"last"}]`,
		`"sort_reused":true`,
		`"name":"sum","argument":"value","default":false,"frame":{"mode":"default-peer-prefix","unit":"groups","start":"unbounded preceding","end":"current row","exclusion":"no others"}`,
		`"name":"lag","argument":"value","offset":1,"default":true`,
	} {
		if !strings.Contains(plan, fragment) {
			t.Errorf("EXPLAIN lacks %s: %s", fragment, plan)
		}
	}
	if strings.Contains(plan, `"rows":`) {
		t.Fatalf("plain EXPLAIN contains measured rows: %s", plan)
	}

	var execution Exec
	cursor, err := statement.RunInto(
		&execution, FromDatabase(database.Snapshot(), "events"), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for cursor.Next() {
		rows++
	}
	actual, err := statement.ExplainAnalyze(ExplainOptions{}, ExplainAnalysis{
		Rows: rows, Stats: execution.Stats,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(actual, `"rows":5`) ||
		!strings.Contains(actual, `"analyze":{"elapsed_ns":0,"rows":4`) {
		t.Fatalf("EXPLAIN ANALYZE lacks input/final rows: %s", actual)
	}
}

func TestSQLWindowExplainNamedRangeAndExclusion(t *testing.T) {
	statement, err := PrepareStatement(`
		SELECT SUM(value) OVER (
			ordered RANGE BETWEEN 1.25 PRECEDING AND CURRENT ROW EXCLUDE TIES
		) AS ranged,
		COUNT(*) OVER equivalent AS peer_count
		FROM events
		WINDOW ordered AS (PARTITION BY team ORDER BY score),
		       equivalent AS (PARTITION BY team ORDER BY score)`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	plan, err := statement.Explain()
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		`"window":"ordered"`,
		`"window":"equivalent"`,
		`"sort_reused":true`,
		`"mode":"explicit","unit":"range"`,
		`"start":"1.25 preceding"`,
		`"end":"current row"`,
		`"exclusion":"ties"`,
	} {
		if !strings.Contains(plan, fragment) {
			t.Fatalf("EXPLAIN lacks %s: %s", fragment, plan)
		}
	}
}

func TestSQLWindowReleaseDropsRetainedState(t *testing.T) {
	database := windowStatementDatabase(t)
	statement, err := PrepareStatement(`
		SELECT id, SUM(value) OVER (PARTITION BY team ORDER BY score) AS running
		FROM events`)
	if err != nil {
		t.Fatal(err)
	}
	window := statement.window()
	var execution Exec
	if _, err := statement.RunInto(
		&execution, FromDatabase(database.Snapshot(), "events"), nil,
	); err != nil {
		t.Fatal(err)
	}
	statement.Release()
	execution.Release()
	if statement.tree != nil || window.input != nil || window.stages != nil ||
		window.inputSpool.columns != nil || window.viewColumns != nil {
		t.Fatalf("Release retained statement/window state: statement=%+v window=%+v",
			statement.tree, window)
	}
	statement.Release()
}

func TestSQLWindowWarmExecutionIsAllocationFree(t *testing.T) {
	database := windowStatementDatabase(t)
	statement, err := PrepareStatement(`
		SELECT id, SUM(value) OVER (PARTITION BY team ORDER BY score) AS running,
			LAG(value, 1, NULL) OVER (PARTITION BY team ORDER BY score) AS previous
		FROM events ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(database.Snapshot(), "events")
	var execution Exec
	defer execution.Release()
	run := func() {
		cursor, err := statement.RunInto(&execution, source, nil)
		if err != nil {
			t.Fatal(err)
		}
		for cursor.Next() {
			sqlSink += len(cursor.Cell(0).Payload())
		}
	}
	run()
	run()
	if allocations := testing.AllocsPerRun(50, run); allocations != 0 {
		t.Fatalf("warmed window execution allocated %.1f times per run", allocations)
	}
}

func TestSQLWindowNamedRangeWarmExecutionIsAllocationFree(t *testing.T) {
	database := windowStatementDatabase(t)
	statement, err := PrepareStatement(`
		SELECT id, SUM(value) OVER (
			ordered RANGE BETWEEN ? PRECEDING AND ? FOLLOWING EXCLUDE TIES
		) AS ranged
		FROM events WINDOW ordered AS (PARTITION BY team ORDER BY score)
		ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(database.Snapshot(), "events")
	args := []any{Number("0.5"), Number("5e-1")}
	var execution Exec
	defer execution.Release()
	run := func() {
		cursor, err := statement.RunInto(&execution, source, args)
		if err != nil {
			t.Fatal(err)
		}
		for cursor.Next() {
			sqlSink += len(cursor.Cell(0).Payload())
		}
	}
	run()
	run()
	if allocations := testing.AllocsPerRun(50, run); allocations != 0 {
		t.Fatalf("warmed named RANGE execution allocated %.1f times per run", allocations)
	}
}

func BenchmarkSQLWindowPrepared(b *testing.B) {
	database := windowStatementDatabase(b)
	statement, err := PrepareStatement(`
		SELECT id, SUM(value) OVER (PARTITION BY team ORDER BY score) AS running,
			LAG(value, 1, NULL) OVER (PARTITION BY team ORDER BY score) AS previous
		FROM events ORDER BY id`)
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(database.Snapshot(), "events")
	var execution Exec
	defer execution.Release()
	run := func() {
		cursor, err := statement.RunInto(&execution, source, nil)
		if err != nil {
			b.Fatal(err)
		}
		for cursor.Next() {
			sqlSink += len(cursor.Cell(0).Payload())
		}
	}
	run()
	run()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		run()
	}
}

func BenchmarkSQLWindowNamedRangePrepared(b *testing.B) {
	database := windowStatementDatabase(b)
	statement, err := PrepareStatement(`
		SELECT id, SUM(value) OVER (
			ordered RANGE BETWEEN ? PRECEDING AND ? FOLLOWING EXCLUDE TIES
		) AS ranged
		FROM events WINDOW ordered AS (PARTITION BY team ORDER BY score)
		ORDER BY id`)
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(database.Snapshot(), "events")
	args := []any{Number("0.5"), Number("5e-1")}
	var execution Exec
	defer execution.Release()
	run := func() {
		cursor, err := statement.RunInto(&execution, source, args)
		if err != nil {
			b.Fatal(err)
		}
		for cursor.Next() {
			sqlSink += len(cursor.Cell(0).Payload())
		}
	}
	run()
	run()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		run()
	}
}
