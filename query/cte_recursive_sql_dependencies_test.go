package query

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

const recursiveSQLPrecedingCTEs = `WITH RECURSIVE
enabled_edges(src, dst) AS MATERIALIZED (
	SELECT src, dst FROM edges WHERE enabled = ?
),
roots(node) AS MATERIALIZED (
	SELECT node FROM seeds WHERE node = ?
),
reachable(node) AS (
	SELECT node FROM roots
	UNION
	SELECT e.dst AS node
	FROM reachable AS r
	JOIN enabled_edges AS e ON r.node = e.src
)
SELECT node FROM reachable WHERE node <= ? ORDER BY node`

const recursiveSQLSequentialDefinitions = `WITH RECURSIVE
forward(node) AS MATERIALIZED (
	SELECT node FROM seeds WHERE node = ?
	UNION
	SELECT e.dst AS node
	FROM forward AS f JOIN edges AS e ON f.node = e.src
	WHERE e.enabled = ?
),
continued(node) AS (
	SELECT node FROM forward WHERE node = ?
	UNION
	SELECT e.dst AS node
	FROM continued AS c
	JOIN edges AS e ON c.node = e.src
	JOIN forward AS f ON e.dst = f.node
	WHERE e.enabled = ?
)
SELECT node FROM continued WHERE node <= ? ORDER BY node`

func TestRecursiveSQLBridgePrecedingCTEsShareOwnerFrameAndSnapshot(t *testing.T) {
	edges := [][2]int{{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {4, 5}}
	liveEdges, snapshot := recursiveStatementDatabase(t, edges)
	if _, err := liveEdges.Put(
		"late", []byte(`{"src":5,"dst":7,"enabled":true}`),
	); err != nil {
		t.Fatal(err)
	}
	statement, err := PrepareStatement(recursiveSQLPrecedingCTEs)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if statement.NumParams() != 3 || !statement.RequiresCatalog() {
		t.Fatalf("recursive dependency params/catalog = %d/%v, want 3/true",
			statement.NumParams(), statement.RequiresCatalog())
	}
	catalog := statement.cteCatalog()
	if catalog == nil || len(catalog.defs) != 3 {
		t.Fatalf("recursive dependency catalog = %+v", catalog)
	}
	recursive := catalog.defs[2].recursiveDefinition
	if recursive == nil || recursive.anchorStmt.NumParams() != 3 ||
		recursive.recursiveStmt.NumParams() != 3 ||
		recursive.anchor.paramBase != 0 || recursive.recursive.paramBase != 0 {
		t.Fatalf("recursive dependency full-frame terms = %+v", recursive)
	}

	var execution Exec
	got := recursiveSQLIntRows(
		t, statement, &execution,
		FromDatabase(snapshot, statement.Collection()),
		[]any{true, int64(0), int64(5)},
	)
	if want := []int{0, 1, 2, 3, 4, 5}; !reflect.DeepEqual(got, want) {
		t.Fatalf("recursive dependency rows = %v, want snapshot rows %v", got, want)
	}
	if catalog.defs[0].runEvaluations != 1 || catalog.defs[1].runEvaluations != 1 ||
		catalog.defs[2].runEvaluations != 1 {
		t.Fatalf("recursive dependency evaluations = %d/%d/%d, want 1/1/1",
			catalog.defs[0].runEvaluations, catalog.defs[1].runEvaluations,
			catalog.defs[2].runEvaluations)
	}
	childFrame := &recursive.execution
	childBytes := int64(0)
	for i := 0; i < 2; i++ {
		definition := catalog.defs[i]
		if definition.activeFrame != childFrame {
			t.Fatalf("preceding dependency %d publication frame = %p, want recursive child %p",
				i, definition.activeFrame, childFrame)
		}
		childBytes += definition.activeBytes
	}
	if got := childFrame.intermediate.used; got != childBytes {
		t.Fatalf("preceding dependency child account = %d, want exact publications %d",
			got, childBytes)
	}
	if catalog.defs[2].activeFrame != &statement.nested.frame {
		t.Fatalf("recursive publication frame = %p, want outer owner %p",
			catalog.defs[2].activeFrame, &statement.nested.frame)
	}
	// A successful top-level CTE cursor retains its source relation until the
	// next execution/discard, matching the ordinary CTE lifecycle. Explicitly
	// cross that boundary before checking exact publication teardown.
	statement.discardRelations()
	for i := range catalog.defs {
		if catalog.defs[i].state != cteIdle || catalog.defs[i].activeBytes != 0 ||
			catalog.defs[i].activeFrame != nil || catalog.defs[i].spool.rows != 0 {
			t.Fatalf("recursive dependency %d retained publication state", i)
		}
	}
	if statement.nested.frame.intermediate.used != 0 {
		t.Fatalf("recursive dependency retained %d intermediate bytes after success teardown",
			statement.nested.frame.intermediate.used)
	}
	execution.Release()
}

func TestRecursiveSQLBridgeSequentialDefinitionsDependOnPriorFixpoint(t *testing.T) {
	edges := [][2]int{{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {4, 5}}
	_, snapshot := recursiveStatementDatabase(t, edges)
	statement, err := PrepareStatement(recursiveSQLSequentialDefinitions)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	catalog := statement.cteCatalog()
	if catalog == nil || len(catalog.defs) != 2 ||
		catalog.defs[0].recursiveDefinition == nil ||
		catalog.defs[1].recursiveDefinition == nil {
		t.Fatalf("sequential recursive catalog = %+v", catalog)
	}
	if catalog.defs[0].references != 3 || catalog.defs[1].references != 1 {
		t.Fatalf("sequential recursive references = %d/%d, want 3/1",
			catalog.defs[0].references, catalog.defs[1].references)
	}
	for i := range catalog.defs {
		prepared := catalog.defs[i].recursiveDefinition
		if prepared.references != catalog.defs[i].references ||
			prepared.anchorStmt.NumParams() != statement.NumParams() ||
			prepared.recursiveStmt.NumParams() != statement.NumParams() {
			t.Fatalf("sequential recursive definition %d captured stale frame/references", i)
		}
	}

	var execution Exec
	got := recursiveSQLIntRows(
		t, statement, &execution,
		FromDatabase(snapshot, statement.Collection()),
		[]any{int64(0), true, int64(2), true, int64(5)},
	)
	if want := []int{2, 3, 4, 5}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sequential recursive rows = %v, want %v", got, want)
	}
	if catalog.defs[0].runEvaluations != 1 || catalog.defs[1].runEvaluations != 1 ||
		catalog.defs[0].recursiveDefinition.runtime.Evaluations() != 1 ||
		catalog.defs[1].recursiveDefinition.runtime.Evaluations() != 1 {
		t.Fatalf("sequential recursive evaluations = %d/%d runtime=%d/%d",
			catalog.defs[0].runEvaluations, catalog.defs[1].runEvaluations,
			catalog.defs[0].recursiveDefinition.runtime.Evaluations(),
			catalog.defs[1].recursiveDefinition.runtime.Evaluations())
	}
	continuedFrame := &catalog.defs[1].recursiveDefinition.execution
	if catalog.defs[0].activeFrame != continuedFrame ||
		continuedFrame.intermediate.used != catalog.defs[0].activeBytes {
		t.Fatalf("prior fixpoint publication owner/account = %p/%d, want %p/%d",
			catalog.defs[0].activeFrame, continuedFrame.intermediate.used,
			continuedFrame, catalog.defs[0].activeBytes)
	}
	if catalog.defs[1].activeFrame != &statement.nested.frame {
		t.Fatalf("continued publication frame = %p, want outer owner %p",
			catalog.defs[1].activeFrame, &statement.nested.frame)
	}
	execution.Release()
}

func TestRecursiveSQLBridgeDependencyCancellationBudgetsAtomicAndReusable(t *testing.T) {
	edges := [][2]int{{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {4, 5}}
	_, snapshot := recursiveStatementDatabase(t, edges)
	statement, err := PrepareStatement(recursiveSQLSequentialDefinitions)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			statement.Release()
		}
	}()
	source := FromDatabase(snapshot, statement.Collection())
	args := []any{int64(0), true, int64(2), true, int64(5)}
	var cancel CancelFlag
	var execution Exec
	execution.Options = ExecOptions{IntermediateBytes: -1, Cancel: &cancel}
	if got := recursiveSQLIntRows(t, statement, &execution, source, args); len(got) != 4 {
		t.Fatalf("warm sequential rows = %v", got)
	}
	statement.discardRelations()
	assertRecursiveSQLDependencyDefinitionsIdle(t, statement)
	if got := recursiveSQLIntRows(t, statement, &execution, source, args); len(got) != 4 {
		t.Fatalf("post-discard warm sequential rows = %v", got)
	}

	cancel.Cancel()
	_, err = statement.RunInto(&execution, source, args)
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("dependency cancellation error = %v", err)
	}
	assertRecursiveSQLDependencyFailureAtomic(t, statement, &execution)

	cancel.Reset()
	execution.Options = ExecOptions{IntermediateBytes: 1, Cancel: &cancel}
	_, err = statement.RunInto(&execution, source, args)
	if !errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("dependency intermediate error = %v", err)
	}
	assertRecursiveSQLDependencyFailureAtomic(t, statement, &execution)

	execution.Options = ExecOptions{
		IntermediateBytes: -1, ResultRows: 2, ResultBytes: -1, Cancel: &cancel,
	}
	_, err = statement.RunInto(&execution, source, args)
	if !errors.Is(err, ErrResultBudget) {
		t.Fatalf("dependency result-row error = %v", err)
	}
	assertRecursiveSQLDependencyFailureAtomic(t, statement, &execution)

	execution.Options = ExecOptions{
		IntermediateBytes: -1, ResultRows: -1, ResultBytes: -1, Cancel: &cancel,
	}
	if got := recursiveSQLIntRows(t, statement, &execution, source, args); !reflect.DeepEqual(got, []int{2, 3, 4, 5}) {
		t.Fatalf("dependency reuse rows = %v", got)
	}
	statement.discardRelations()
	assertRecursiveSQLDependencyDefinitionsIdle(t, statement)
	if got := recursiveSQLIntRows(t, statement, &execution, source, args); !reflect.DeepEqual(got, []int{2, 3, 4, 5}) {
		t.Fatalf("dependency post-discard reuse rows = %v", got)
	}
	definitions := append([]*statementCTE(nil), statement.cteCatalog().defs...)
	live := false
	for _, definition := range definitions {
		live = live || definition.activeFrame != nil
	}
	if !live {
		t.Fatal("successful recursive dependency run did not establish an owned publication frame")
	}
	statement.Release()
	released = true
	for i, definition := range definitions {
		if definition.activeFrame != nil || definition.activeBytes != 0 ||
			definition.state != cteIdle {
			t.Fatalf("released recursive dependency %d retained frame/bytes/state = %p/%d/%d",
				i, definition.activeFrame, definition.activeBytes, definition.state)
		}
	}
	execution.Release()
}

func TestRecursiveSQLBridgeDependencyWarmedExecutionZeroAlloc(t *testing.T) {
	edges := [][2]int{{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {4, 5}}
	_, snapshot := recursiveStatementDatabase(t, edges)
	statement, err := PrepareStatement(recursiveSQLSequentialDefinitions)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	var execution Exec
	execution.Options = ExecOptions{
		IntermediateBytes: -1, ResultRows: -1, ResultBytes: -1,
	}
	source := FromDatabase(snapshot, statement.Collection())
	args := []any{int64(0), true, int64(2), true, int64(5)}
	run := func() {
		cursor, runErr := statement.RunInto(&execution, source, args)
		if runErr != nil {
			panic(runErr)
		}
		rows := 0
		for cursor.Next() {
			if _, ok := cursor.Cell(0).Int64(); !ok {
				panic("recursive dependency returned a non-integer")
			}
			rows++
		}
		if rows != 4 {
			panic("unexpected recursive dependency row count")
		}
	}
	run()
	run()
	if got := testing.AllocsPerRun(100, run); got != 0 {
		t.Fatalf("warmed recursive dependency execution allocated %.1f times, want 0", got)
	}
	statement.discardRelations()
	assertRecursiveSQLDependencyDefinitionsIdle(t, statement)
	execution.Release()
}

func TestRecursiveSQLBridgeDependencyIndependentStatementsRaceSafe(t *testing.T) {
	edges := [][2]int{{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {4, 5}}
	_, snapshot := recursiveStatementDatabase(t, edges)
	const workers = 4
	statements := make([]*Statement, workers)
	for i := range statements {
		statement, err := PrepareStatement(recursiveSQLSequentialDefinitions)
		if err != nil {
			for j := 0; j < i; j++ {
				statements[j].Release()
			}
			t.Fatal(err)
		}
		statements[i] = statement
	}
	defer func() {
		for _, statement := range statements {
			statement.Release()
		}
	}()

	start := make(chan struct{})
	errors := make(chan error, workers)
	var wait sync.WaitGroup
	wait.Add(workers)
	for worker, statement := range statements {
		worker, statement := worker, statement
		go func() {
			defer wait.Done()
			<-start
			var execution Exec
			execution.Options = ExecOptions{
				IntermediateBytes: -1, ResultRows: -1, ResultBytes: -1,
			}
			source := FromDatabase(snapshot, statement.Collection())
			args := []any{int64(0), true, int64(2), true, int64(5)}
			for run := 0; run < 25; run++ {
				cursor, err := statement.RunInto(&execution, source, args)
				if err != nil {
					errors <- fmt.Errorf("worker %d run %d: %w", worker, run, err)
					execution.Release()
					return
				}
				rows := 0
				for cursor.Next() {
					value, ok := cursor.Cell(0).Int64()
					if !ok || value != int64(rows+2) {
						errors <- fmt.Errorf(
							"worker %d run %d row %d = %d/%t",
							worker, run, rows, value, ok,
						)
						execution.Release()
						return
					}
					rows++
				}
				if rows != 4 {
					errors <- fmt.Errorf(
						"worker %d run %d returned %d rows", worker, run, rows,
					)
					execution.Release()
					return
				}
			}
			statement.discardRelations()
			for i, definition := range statement.cteCatalog().defs {
				if definition.activeFrame != nil || definition.activeBytes != 0 ||
					definition.state != cteIdle {
					errors <- fmt.Errorf(
						"worker %d definition %d retained frame/bytes/state",
						worker, i,
					)
					execution.Release()
					return
				}
			}
			execution.Release()
		}()
	}
	close(start)
	wait.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

func assertRecursiveSQLDependencyFailureAtomic(
	tb testing.TB,
	statement *Statement,
	execution *Exec,
) {
	tb.Helper()
	if execution.Result.RowCount != 0 ||
		statement.nested.frame.intermediate.used != 0 {
		tb.Fatalf("failed recursive dependency rows/budget = %d/%d",
			execution.Result.RowCount, statement.nested.frame.intermediate.used)
	}
	for column := range execution.Result.Columns {
		if len(execution.Result.Columns[column].Cells) != 0 {
			tb.Fatalf("failed recursive dependency published %d cells in column %d",
				len(execution.Result.Columns[column].Cells), column)
		}
	}
	assertRecursiveSQLDependencyDefinitionsIdle(tb, statement)
}

func assertRecursiveSQLDependencyDefinitionsIdle(
	tb testing.TB,
	statement *Statement,
) {
	tb.Helper()
	if statement.nested.frame.intermediate.used != 0 {
		tb.Fatalf("failed recursive dependency retained %d outer intermediate bytes",
			statement.nested.frame.intermediate.used)
	}
	for i, definition := range statement.cteCatalog().defs {
		if definition.state != cteIdle || definition.activeBytes != 0 ||
			definition.activeFrame != nil || definition.spool.rows != 0 {
			tb.Fatalf("recursive dependency %d retained CTE publication/frame = %d/%d/%p/%d",
				i, definition.state, definition.activeBytes,
				definition.activeFrame, definition.spool.rows)
		}
		prepared := definition.recursiveDefinition
		if prepared == nil {
			continue
		}
		if prepared.active.Load() || prepared.runtime.frame != nil ||
			prepared.execution.intermediate.used != 0 ||
			prepared.recursive.target.recursiveBinding != nil {
			tb.Fatalf("failed recursive dependency %d retained active/frame/budget/binding = %v/%p/%d/%p",
				i, prepared.active.Load(), prepared.runtime.frame,
				prepared.execution.intermediate.used,
				prepared.recursive.target.recursiveBinding)
		}
	}
}

func recursiveSQLIntRows(
	tb testing.TB,
	statement *Statement,
	execution *Exec,
	source Source,
	args []any,
) []int {
	tb.Helper()
	cursor, err := statement.RunInto(execution, source, args)
	if err != nil {
		tb.Fatal(err)
	}
	rows := make([]int, 0, execution.Result.RowCount)
	for cursor.Next() {
		value, ok := cursor.Cell(0).Int64()
		if !ok {
			tb.Fatal(fmt.Errorf("recursive SQL result is not an integer"))
		}
		rows = append(rows, int(value))
	}
	return rows
}
