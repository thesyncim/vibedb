package query

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

type recursiveDefinitionFixture struct {
	owner      *Statement
	definition *statementCTE
	prepared   *statementRecursiveDefinition
	anchor     *RecursiveCTEStatementTerm
	recursive  *RecursiveCTEStatementTerm
}

func prepareRecursiveDefinitionFixture(
	tb testing.TB,
	materialization RecursiveCTEMaterialization,
	limits RecursiveCTELimits,
) *recursiveDefinitionFixture {
	tb.Helper()
	hint := "MATERIALIZED"
	if materialization == RecursiveCTEReferenceLocal {
		hint = "NOT MATERIALIZED"
	}
	owner, err := PrepareStatement(fmt.Sprintf(`
		WITH reachable(node) AS %s (
			SELECT node FROM seeds WHERE node = ? OR node = ?
		)
		SELECT left_side.node AS left_node, right_side.node AS right_node
		FROM reachable left_side
		JOIN reachable right_side ON left_side.node = right_side.node
		ORDER BY left_side.node`, hint))
	if err != nil {
		tb.Fatal(err)
	}
	graph := prepareRecursiveStatementGraph(tb, 0, 1)
	descriptor, err := PrepareRecursiveCTEDescriptor(
		"reachable", []string{"node"}, graph.anchor, graph.recursive,
		RecursiveUnionDistinct, materialization, limits,
	)
	if err != nil {
		graph.release()
		owner.Release()
		tb.Fatal(err)
	}
	definition := owner.cteCatalog().defs[0]
	prepared, err := installStatementRecursiveDefinition(
		owner, definition, descriptor, "seeds",
	)
	if err != nil {
		graph.release()
		owner.Release()
		tb.Fatal(err)
	}
	return &recursiveDefinitionFixture{
		owner: owner, definition: definition, prepared: prepared,
		anchor: graph.anchor, recursive: graph.recursive,
	}
}

func (f *recursiveDefinitionFixture) release() {
	if f != nil && f.owner != nil {
		f.owner.Release()
	}
}

func runRecursiveDefinitionFixture(
	tb testing.TB,
	fixture *recursiveDefinitionFixture,
	exec *Exec,
	snapshot store.DatabaseSnapshot,
	start int,
) []int {
	tb.Helper()
	cursor, err := fixture.owner.RunInto(
		exec,
		FromDatabase(snapshot, fixture.owner.Collection()),
		[]any{int64(start), true},
	)
	if err != nil {
		tb.Fatal(err)
	}
	rows := make([]int, 0, exec.Result.RowCount)
	for cursor.Next() {
		left, leftOK := cursor.Cell(0).Int64()
		right, rightOK := cursor.Cell(1).Int64()
		if !leftOK || !rightOK || left != right {
			tb.Fatalf("recursive definition row = %d/%v, %d/%v",
				left, leftOK, right, rightOK)
		}
		rows = append(rows, int(left))
	}
	return rows
}

func TestRecursiveCTEDefinitionPublishesSharedAndReferenceLocalRelations(t *testing.T) {
	edges := [][2]int{{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {4, 5}}
	liveEdges, snapshot := recursiveStatementDatabase(t, edges)
	if _, err := liveEdges.Put(
		"late", []byte(`{"src":5,"dst":7,"enabled":true}`),
	); err != nil {
		t.Fatal(err)
	}
	want := recursiveStatementGraphOracle(0, edges)
	sort.Ints(want)
	for _, test := range []struct {
		name            string
		materialization RecursiveCTEMaterialization
		evaluations     uint64
	}{
		{name: "shared", materialization: RecursiveCTEShared, evaluations: 1},
		{name: "reference_local", materialization: RecursiveCTEReferenceLocal, evaluations: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := prepareRecursiveDefinitionFixture(
				t, test.materialization,
				RecursiveCTELimits{MaxIterations: 32, MaxRows: 256, MaxBytes: -1},
			)
			defer fixture.release()
			if fixture.definition.references != 2 || fixture.prepared.references != 2 {
				t.Fatalf("reference count = %d/%d, want 2",
					fixture.definition.references, fixture.prepared.references)
			}
			var exec Exec
			got := runRecursiveDefinitionFixture(t, fixture, &exec, snapshot, 0)
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("published rows = %v, want snapshot rows %v", got, want)
			}
			if fixture.definition.runEvaluations != test.evaluations ||
				fixture.prepared.runtime.Evaluations() != test.evaluations {
				t.Fatalf("evaluations = definition %d/runtime %d, want %d",
					fixture.definition.runEvaluations,
					fixture.prepared.runtime.Evaluations(), test.evaluations)
			}
			if fixture.prepared.runtime.frame != nil ||
				fixture.recursive.target.recursiveBinding != nil {
				t.Fatal("published definition retained a runtime frame or delta binding")
			}
			if test.materialization == RecursiveCTEShared {
				if fixture.definition.state != cteReady ||
					fixture.definition.spool.rows != len(want) {
					t.Fatal("shared definition did not retain one ready publication")
				}
			} else {
				join := fixture.owner.relationJoin()
				if fixture.definition.state != cteIdle || join == nil ||
					len(join.operands) != 2 || len(join.operands[0].spool.data) == 0 ||
					len(join.operands[1].spool.data) == 0 ||
					&join.operands[0].spool.data[0] == &join.operands[1].spool.data[0] {
					t.Fatal("reference-local publications do not own independent payloads")
				}
			}
			exec.Release()
		})
	}
}

func TestRecursiveCTEDefinitionCancellationBudgetAndBoundsAreAtomic(t *testing.T) {
	edges := [][2]int{{0, 1}, {0, 2}, {1, 3}, {2, 4}, {4, 5}}
	_, snapshot := recursiveStatementDatabase(t, edges)
	fixture := prepareRecursiveDefinitionFixture(
		t, RecursiveCTEShared,
		RecursiveCTELimits{MaxIterations: 32, MaxRows: 256, MaxBytes: -1},
	)
	defer fixture.release()
	var exec Exec
	if got := runRecursiveDefinitionFixture(t, fixture, &exec, snapshot, 0); len(got) != 6 {
		t.Fatalf("initial rows = %d, want 6", len(got))
	}

	var cancel CancelFlag
	cancel.Cancel()
	exec.Options = ExecOptions{IntermediateBytes: -1, Cancel: &cancel}
	_, err := fixture.owner.RunInto(
		&exec, FromDatabase(snapshot, fixture.owner.Collection()),
		[]any{int64(0), true},
	)
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled definition error = %v", err)
	}
	assertRecursiveDefinitionFailureAtomic(t, fixture, &exec)

	cancel.Reset()
	exec.Options = ExecOptions{IntermediateBytes: 1, Cancel: &cancel}
	_, err = fixture.owner.RunInto(
		&exec, FromDatabase(snapshot, fixture.owner.Collection()),
		[]any{int64(0), true},
	)
	if !errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("bounded definition error = %v", err)
	}
	assertRecursiveDefinitionFailureAtomic(t, fixture, &exec)

	exec.Options = ExecOptions{IntermediateBytes: -1, Cancel: &cancel}
	if got := runRecursiveDefinitionFixture(t, fixture, &exec, snapshot, 0); len(got) != 6 {
		t.Fatalf("reuse after failures = %d rows, want 6", len(got))
	}

	bounded := prepareRecursiveDefinitionFixture(
		t, RecursiveCTEShared,
		RecursiveCTELimits{MaxIterations: 32, MaxRows: 2, MaxBytes: -1},
	)
	defer bounded.release()
	var boundedExec Exec
	_, err = bounded.owner.RunInto(
		&boundedExec, FromDatabase(snapshot, bounded.owner.Collection()),
		[]any{int64(0), true},
	)
	if !errors.Is(err, ErrRecursiveRows) {
		t.Fatalf("recursive definition row bound error = %v", err)
	}
	assertRecursiveDefinitionFailureAtomic(t, bounded, &boundedExec)
}

func assertRecursiveDefinitionFailureAtomic(
	tb testing.TB,
	fixture *recursiveDefinitionFixture,
	exec *Exec,
) {
	tb.Helper()
	if exec.Result.RowCount != 0 || fixture.definition.state != cteIdle ||
		fixture.definition.activeBytes != 0 || fixture.definition.spool.rows != 0 ||
		fixture.prepared.runtime.frame != nil ||
		fixture.recursive.target.recursiveBinding != nil {
		tb.Fatalf("failed definition retained rows/state/bytes = %d/%d/%d/%d",
			exec.Result.RowCount, fixture.definition.state,
			fixture.definition.activeBytes, fixture.definition.spool.rows)
	}
	for column := range exec.Result.Columns {
		if len(exec.Result.Columns[column].Cells) != 0 {
			tb.Fatalf("failed definition retained stale result column %d", column)
		}
	}
}

func TestRecursiveCTEDefinitionInstallationAndStatementReleaseLifecycle(t *testing.T) {
	fixture := prepareRecursiveDefinitionFixture(
		t, RecursiveCTEShared,
		RecursiveCTELimits{MaxIterations: 32, MaxRows: 256, MaxBytes: -1},
	)
	if _, err := installStatementRecursiveDefinition(
		fixture.owner, fixture.definition, fixture.prepared.descriptor, "seeds",
	); !errors.Is(err, errStatementRecursiveDefinition) {
		t.Fatalf("duplicate installation error = %v", err)
	}
	if fixture.definition.recursiveDefinition != fixture.prepared {
		t.Fatal("duplicate installation replaced the original sidecar")
	}
	edges := [][2]int{{0, 1}, {1, 2}}
	_, snapshot := recursiveStatementDatabase(t, edges)
	var exec Exec
	_ = runRecursiveDefinitionFixture(t, fixture, &exec, snapshot, 0)
	fixpoint := &fixture.prepared.runtime.fixpoint
	if cap(fixpoint.result.cells)+cap(fixpoint.result.data)+cap(fixpoint.result.ends) == 0 ||
		cap(fixture.anchor.owned[0])+cap(fixture.anchor.owned[1]) == 0 ||
		cap(fixture.recursive.owned[0])+cap(fixture.recursive.owned[1]) == 0 {
		t.Fatal("test did not establish recursive definition high-water storage")
	}
	prepared := fixture.prepared
	anchor, recursive := fixture.anchor, fixture.recursive
	anchorStmt, recursiveStmt := prepared.anchorStmt, prepared.recursiveStmt
	recursiveTarget := recursive.target
	exec.Release()
	fixture.owner.Release()
	fixture.owner.Release()
	if prepared.descriptor != nil || prepared.runtime.descriptor != nil ||
		prepared.anchor != nil || prepared.recursive != nil ||
		anchor.statement != nil || recursive.statement != nil ||
		anchorStmt.tree != nil || recursiveStmt.tree != nil ||
		recursiveTarget.recursiveOwner != nil {
		t.Fatal("owning Statement Release retained recursive definition lifecycle state")
	}
}

func TestRecursiveCTEDefinitionDefusesSingleReferenceBeforePublication(t *testing.T) {
	owner, err := PrepareStatement(`
		WITH reachable AS (
			SELECT node FROM seeds WHERE node >= ? AND node <= ?
		)
		SELECT * FROM reachable`)
	if err != nil {
		t.Fatal(err)
	}
	graph := prepareRecursiveStatementGraph(t, 0, 1)
	descriptor, err := PrepareRecursiveCTEDescriptor(
		"reachable", []string{"node"}, graph.anchor, graph.recursive,
		RecursiveUnionDistinct, RecursiveCTEShared,
		RecursiveCTELimits{MaxIterations: 32, MaxRows: 256, MaxBytes: -1},
	)
	if err != nil {
		graph.release()
		owner.Release()
		t.Fatal(err)
	}
	definition := owner.cteCatalog().defs[0]
	if mode := owner.cteReference().mode(); mode != cteFused {
		graph.release()
		owner.Release()
		t.Fatalf("pre-installation mode = %s, want fused", mode)
	}
	prepared, err := installStatementRecursiveDefinition(
		owner, definition, descriptor, "seeds",
	)
	if err != nil {
		graph.release()
		owner.Release()
		t.Fatal(err)
	}
	defer owner.Release()
	if mode := owner.cteReference().mode(); mode != cteSharedMaterialized {
		t.Fatalf("installed single-reference mode = %s, want materialized", mode)
	}
	edges := [][2]int{{0, 1}, {1, 2}, {2, 3}}
	_, snapshot := recursiveStatementDatabase(t, edges)
	var exec Exec
	cursor, err := owner.RunInto(
		&exec, FromDatabase(snapshot, owner.Collection()),
		[]any{int64(0), true},
	)
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for cursor.Next() {
		rows++
	}
	if rows != 4 || definition.runEvaluations != 1 ||
		prepared.runtime.Evaluations() != 1 {
		t.Fatalf("single-reference publication = rows %d, evaluations %d/%d",
			rows, definition.runEvaluations, prepared.runtime.Evaluations())
	}
	exec.Release()
}

func TestRecursiveCTEDefinitionIndependentStatementsRaceSafe(t *testing.T) {
	edges := [][2]int{{0, 1}, {0, 2}, {1, 3}, {2, 4}, {4, 5}}
	_, snapshot := recursiveStatementDatabase(t, edges)
	const workers = 6
	fixtures := make([]*recursiveDefinitionFixture, workers)
	for worker := range fixtures {
		fixtures[worker] = prepareRecursiveDefinitionFixture(
			t, RecursiveCTEShared,
			RecursiveCTELimits{MaxIterations: 32, MaxRows: 256, MaxBytes: -1},
		)
		defer fixtures[worker].release()
	}
	var wait sync.WaitGroup
	errorsOut := make(chan error, workers)
	for worker := range fixtures {
		wait.Add(1)
		go func(fixture *recursiveDefinitionFixture) {
			defer wait.Done()
			var exec Exec
			cursor, err := fixture.owner.RunInto(
				&exec, FromDatabase(snapshot, fixture.owner.Collection()),
				[]any{int64(0), true},
			)
			rows := 0
			if err == nil {
				for cursor.Next() {
					rows++
				}
				if rows != 6 {
					err = fmt.Errorf("rows = %d, want 6", rows)
				}
			}
			errorsOut <- err
		}(fixtures[worker])
	}
	wait.Wait()
	close(errorsOut)
	for err := range errorsOut {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestRecursiveCTEDefinitionValidationRejectsSchemaMismatch(t *testing.T) {
	owner, err := PrepareStatement(`
		WITH reachable(other) AS MATERIALIZED (
			SELECT node FROM seeds WHERE node = ? OR node = ?
		)
		SELECT other FROM reachable`)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Release()
	graph := prepareRecursiveStatementGraph(t, 0, 1)
	defer graph.release()
	descriptor, err := PrepareRecursiveCTEDescriptor(
		"reachable", []string{"node"}, graph.anchor, graph.recursive,
		RecursiveUnionDistinct, RecursiveCTEShared, RecursiveCTELimits{},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = installStatementRecursiveDefinition(
		owner, owner.cteCatalog().defs[0], descriptor, "seeds",
	)
	if !errors.Is(err, errStatementRecursiveDefinition) ||
		!strings.Contains(err.Error(), "column 0") {
		t.Fatalf("schema mismatch error = %v", err)
	}
	if owner.cteCatalog().defs[0].recursiveDefinition != nil {
		t.Fatal("failed installation published a sidecar")
	}
	_, snapshot := recursiveStatementDatabase(t, nil)
	var exec Exec
	cursor, runErr := owner.RunInto(
		&exec, FromDatabase(snapshot, owner.Collection()),
		[]any{int64(0), int64(7)},
	)
	rows := 0
	for cursor.Next() {
		rows++
	}
	if runErr != nil || rows != 2 {
		t.Fatalf("Statement reuse after rejected installation = %d rows, %v",
			rows, runErr)
	}
	exec.Release()
}

func TestRecursiveCTEDefinitionPublicationRejectsAliasWithoutMutation(t *testing.T) {
	relation := buildRelationSpoolForTest(t, [][]string{{"1"}, {`"exact"`}})
	defer relation.release()
	frame := beginRecursiveCTEFrame(t, ExecOptions{IntermediateBytes: -1})
	charge, err := materializeRecursiveDefinitionResult(
		&relation, &relation, frame, nil, "alias publication",
	)
	if !errors.Is(err, errStatementRecursiveDefinition) || charge != 0 ||
		frame.intermediate.used != 0 || relation.rows != 2 ||
		string(cellFromScalar(relation.columns[0][1]).JSON()) != `"exact"` {
		t.Fatalf("alias publication mutated source: charge=%d used=%d rows=%d err=%v",
			charge, frame.intermediate.used, relation.rows, err)
	}
}
