package query

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

const recursiveStatementStepSQL = `
WITH delta(node) AS (
	SELECT node FROM seeds WHERE node = -1
)
SELECT e.dst AS node
FROM delta d JOIN edges e ON d.node = e.src
WHERE e.enabled = ?
ORDER BY e.dst`

type recursiveStatementGraph struct {
	anchorStmt    *Statement
	recursiveStmt *Statement
	anchor        *RecursiveCTEStatementTerm
	recursive     *RecursiveCTEStatementTerm
	descriptor    *RecursiveCTEDescriptor
}

func prepareRecursiveStatementGraph(
	tb testing.TB,
	anchorBase, recursiveBase int,
) *recursiveStatementGraph {
	tb.Helper()
	anchorStmt, err := PrepareStatement(
		`SELECT node FROM seeds WHERE node = ?`,
	)
	if err != nil {
		tb.Fatal(err)
	}
	recursiveStmt, err := PrepareStatement(recursiveStatementStepSQL)
	if err != nil {
		anchorStmt.Release()
		tb.Fatal(err)
	}
	if !recursiveStmt.UsesGeneralizedRelationJoin() {
		anchorStmt.Release()
		recursiveStmt.Release()
		tb.Fatal("recursive Statement term did not select the generalized relation join")
	}
	anchor, err := PrepareRecursiveCTEStatementTerm(
		anchorStmt,
		RecursiveCTEStatementTermOptions{ParamBase: anchorBase},
	)
	if err != nil {
		anchorStmt.Release()
		recursiveStmt.Release()
		tb.Fatal(err)
	}
	recursive, err := PrepareRecursiveCTEStatementTerm(
		recursiveStmt,
		RecursiveCTEStatementTermOptions{
			ParamBase: recursiveBase, RecursiveRelation: "delta",
		},
	)
	if err != nil {
		anchor.Release()
		anchorStmt.Release()
		recursiveStmt.Release()
		tb.Fatal(err)
	}
	descriptor, err := PrepareRecursiveCTEDescriptor(
		"reachable", []string{"node"}, anchor, recursive,
		RecursiveUnionDistinct, RecursiveCTEReferenceLocal,
		RecursiveCTELimits{MaxIterations: 32, MaxRows: 256, MaxBytes: -1},
	)
	if err != nil {
		anchor.Release()
		recursive.Release()
		anchorStmt.Release()
		recursiveStmt.Release()
		tb.Fatal(err)
	}
	return &recursiveStatementGraph{
		anchorStmt: anchorStmt, recursiveStmt: recursiveStmt,
		anchor: anchor, recursive: recursive, descriptor: descriptor,
	}
}

func (g *recursiveStatementGraph) release() {
	if g == nil {
		return
	}
	g.anchor.Release()
	g.recursive.Release()
	g.anchorStmt.Release()
	g.recursiveStmt.Release()
}

func recursiveStatementDatabase(
	tb testing.TB,
	edges [][2]int,
) (*store.Collection, store.DatabaseSnapshot) {
	tb.Helper()
	var database store.Database
	seeds, err := database.CreateCollection("seeds", store.Options{})
	if err != nil {
		tb.Fatal(err)
	}
	edgeCollection, err := database.CreateCollection("edges", store.Options{})
	if err != nil {
		tb.Fatal(err)
	}
	for node := 0; node < 8; node++ {
		document := []byte(fmt.Sprintf(`{"node":%d,"label":"node-%d"}`, node, node))
		if _, err := seeds.Put(strconv.Itoa(node), document); err != nil {
			tb.Fatal(err)
		}
	}
	for ordinal, edge := range edges {
		document := []byte(fmt.Sprintf(
			`{"src":%d,"dst":%d,"enabled":true}`,
			edge[0], edge[1],
		))
		if _, err := edgeCollection.Put(strconv.Itoa(ordinal), document); err != nil {
			tb.Fatal(err)
		}
	}
	return edgeCollection, database.Snapshot()
}

func executeRecursiveStatementGraph(
	tb testing.TB,
	graph *recursiveStatementGraph,
	snapshot store.DatabaseSnapshot,
	start int,
) ([]int, int) {
	tb.Helper()
	options := ExecOptions{IntermediateBytes: -1}
	frame := beginRecursiveCTEFrame(tb, options)
	frame.args = []any{int64(start), true}
	var runtime RecursiveCTERuntime
	result, err := runtime.executeStatementTerms(
		graph.descriptor, FromDatabase(snapshot, "seeds"), frame, options,
	)
	if err != nil {
		tb.Fatal(err)
	}
	values := make([]int, result.Rows())
	for row := range values {
		value, ok := result.Cell(row, 0).Int64()
		if !ok {
			tb.Fatalf("recursive Statement row %d is not an integer", row)
		}
		values[row] = int(value)
	}
	iterations := result.Iterations()
	runtime.releaseExecution(frame)
	if frame.intermediate.used != 0 {
		tb.Fatalf("recursive Statement retained %d intermediate bytes", frame.intermediate.used)
	}
	return values, iterations
}

func TestRecursiveCTEStatementGraphClosureUsesBaseAndDeltaSnapshot(t *testing.T) {
	edges := [][2]int{{1, 2}, {1, 3}, {2, 4}, {3, 4}, {4, 5}, {7, 6}}
	liveEdges, snapshot := recursiveStatementDatabase(t, edges)
	graph := prepareRecursiveStatementGraph(t, 0, 1)
	defer graph.release()

	// The adapter must retain input.Base as the one coherent cut. A hidden
	// resnapshot by the recursive term would make this late edge visible.
	if _, err := liveEdges.Put(
		"late", []byte(`{"src":5,"dst":7,"enabled":true}`),
	); err != nil {
		t.Fatal(err)
	}
	got, iterations := executeRecursiveStatementGraph(t, graph, snapshot, 1)
	want := recursiveGraphOracle(1, edges)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("Statement closure = %v, want snapshot oracle %v", got, want)
	}
	if iterations != 4 {
		t.Fatalf("Statement closure iterations = %d, want 4", iterations)
	}
}

func TestRecursiveCTEStatementParamBaseAndFrameAreExact(t *testing.T) {
	edges := [][2]int{{0, 1}, {1, 2}, {2, 3}}
	_, snapshot := recursiveStatementDatabase(t, edges)
	graph := prepareRecursiveStatementGraph(t, 1, 2)
	defer graph.release()
	options := ExecOptions{IntermediateBytes: -1}
	frame := beginRecursiveCTEFrame(t, options)
	frame.args = []any{"unrelated", int64(0), true}
	outerArgs := frame.args
	var runtime RecursiveCTERuntime
	result, err := runtime.executeStatementTerms(
		graph.descriptor, FromDatabase(snapshot, "seeds"), frame, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(frame.args) != len(outerArgs) || &frame.args[0] != &outerArgs[0] {
		t.Fatal("term execution did not restore the owning frame argument view")
	}
	if result.Rows() != 4 {
		t.Fatalf("ParamBase closure rows = %d, want 4", result.Rows())
	}
	runtime.releaseExecution(frame)

	short := beginRecursiveCTEFrame(t, options)
	short.args = []any{"unrelated", int64(0)}
	_, err = runtime.executeStatementTerms(
		graph.descriptor, FromDatabase(snapshot, "seeds"), short, options,
	)
	var parameter *RecursiveCTEStatementParameterError
	if !errors.As(err, &parameter) ||
		!errors.Is(err, ErrRecursiveCTEStatement) ||
		!errors.Is(err, ErrRecursiveCTEConfig) ||
		parameter.ParamBase != 2 || parameter.Params != 1 || parameter.Bound != 2 {
		t.Fatalf("ParamBase error = %#v (%v)", parameter, err)
	}
	if short.intermediate.used != 0 || runtime.currentResult().Rows() != 0 {
		t.Fatalf("failed ParamBase published result/charge = %d/%d",
			runtime.currentResult().Rows(), short.intermediate.used)
	}

	// A nested CTE reads its absolute range through statementFrame.args rather
	// than the args slice passed to the root bind. This catches an adapter that
	// slices the root correctly but forgets to rebase nested placeholder bases.
	nested, err := PrepareStatement(
		`WITH picked AS MATERIALIZED (` +
			`SELECT node FROM seeds WHERE node = ?` +
			`) SELECT node FROM picked`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nested.Release()
	nestedAnchor, err := PrepareRecursiveCTEStatementTerm(
		nested, RecursiveCTEStatementTermOptions{ParamBase: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nestedAnchor.Release()
	stop, err := PrepareRecursiveCTECallbackTerm(
		[]string{"node"},
		func(exec *Exec, _ RecursiveCTETermInput) error {
			return prepareRecursiveCTETestResult(exec, []string{"node"}, 0)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	nestedDescriptor, err := PrepareRecursiveCTEDescriptor(
		"nested_param", nil, nestedAnchor, stop, RecursiveUnionDistinct,
		RecursiveCTEReferenceLocal, RecursiveCTELimits{},
	)
	if err != nil {
		t.Fatal(err)
	}
	nestedFrame := beginRecursiveCTEFrame(t, options)
	nestedFrame.args = []any{"not-a-number", int64(2)}
	var nestedRuntime RecursiveCTERuntime
	nestedResult, err := nestedRuntime.executeStatementTerms(
		nestedDescriptor, FromDatabase(snapshot, "seeds"), nestedFrame, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	value, ok := nestedResult.Cell(0, 0).Int64()
	if nestedResult.Rows() != 1 || !ok || value != 2 {
		t.Fatalf("nested ParamBase result = %d rows, %d/%v, want node 2",
			nestedResult.Rows(), value, ok)
	}
	nestedRuntime.releaseExecution(nestedFrame)
}

func TestRecursiveCTEStatementStrictResultArityIsAtomicAndReusable(t *testing.T) {
	_, snapshot := recursiveStatementDatabase(t, nil)
	statement, err := PrepareStatement(`SELECT node, label FROM seeds WHERE node = 0`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	anchor, err := PrepareRecursiveCTEStatementTerm(
		statement, RecursiveCTEStatementTermOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer anchor.Release()
	allNames := anchor.names
	anchor.names = anchor.names[:1]
	stopOne, err := PrepareRecursiveCTECallbackTerm(
		[]string{"node"},
		func(exec *Exec, _ RecursiveCTETermInput) error {
			return prepareRecursiveCTETestResult(exec, []string{"node"}, 0)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	bad, err := PrepareRecursiveCTEDescriptor(
		"bad_width", nil, anchor, stopOne, RecursiveUnionDistinct,
		RecursiveCTEReferenceLocal, RecursiveCTELimits{},
	)
	if err != nil {
		t.Fatal(err)
	}
	options := ExecOptions{IntermediateBytes: -1}
	frame := beginRecursiveCTEFrame(t, options)
	var runtime RecursiveCTERuntime
	_, err = runtime.executeStatementTerms(
		bad, FromDatabase(snapshot, "seeds"), frame, options,
	)
	var arity *RecursiveCTEArityError
	if !errors.As(err, &arity) || arity.Expected != 1 || arity.Actual != 2 ||
		arity.Term != "Statement result" {
		t.Fatalf("strict Statement arity error = %#v (%v)", arity, err)
	}
	if runtime.currentResult().Rows() != 0 || frame.intermediate.used != 0 ||
		runtime.anchorExec.Result.RowCount != 0 {
		t.Fatalf("arity failure left rows/charge = %d/%d/%d",
			runtime.currentResult().Rows(), runtime.anchorExec.Result.RowCount,
			frame.intermediate.used)
	}
	for column := range runtime.anchorExec.Result.Columns {
		if len(runtime.anchorExec.Result.Columns[column].Cells) != 0 {
			t.Fatalf("arity failure left stale cells in column %d", column)
		}
	}

	anchor.names = allNames
	stopTwo, err := PrepareRecursiveCTECallbackTerm(
		[]string{"node", "label"},
		func(exec *Exec, _ RecursiveCTETermInput) error {
			return prepareRecursiveCTETestResult(exec, []string{"node", "label"}, 0)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	good, err := PrepareRecursiveCTEDescriptor(
		"good_width", nil, anchor, stopTwo, RecursiveUnionDistinct,
		RecursiveCTEReferenceLocal, RecursiveCTELimits{},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.executeStatementTerms(
		good, FromDatabase(snapshot, "seeds"), frame, options,
	)
	if err != nil || result.Rows() != 1 || len(result.Columns()) != 2 {
		t.Fatalf("reuse after arity failure = %d rows/%d columns, %v",
			result.Rows(), len(result.Columns()), err)
	}
	runtime.releaseExecution(frame)
}

func TestRecursiveCTEStatementTargetLifecycleAndDuplicateAdapter(t *testing.T) {
	_, snapshot := recursiveStatementDatabase(t, nil)
	statement, err := PrepareStatement(
		`WITH delta AS (SELECT node FROM seeds WHERE node = -1) SELECT * FROM delta`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	if mode := statement.cteReference().mode(); mode != cteFused {
		t.Fatalf("initial direct CTE mode = %s, want fused", mode)
	}
	term, err := PrepareRecursiveCTEStatementTerm(
		statement,
		RecursiveCTEStatementTermOptions{RecursiveRelation: "delta"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if mode := statement.cteReference().mode(); mode == cteFused {
		t.Fatal("recursive target remained fused")
	}
	if _, err := PrepareRecursiveCTEStatementTerm(
		statement,
		RecursiveCTEStatementTermOptions{RecursiveRelation: "delta"},
	); !errors.Is(err, ErrRecursiveCTEStatement) {
		t.Fatalf("duplicate adapter error = %v", err)
	}

	frame := beginRecursiveCTEFrame(t, ExecOptions{IntermediateBytes: -1})
	if err := term.begin(frame, []string{"node"}); err != nil {
		t.Fatal(err)
	}
	term.Release()
	if term.statement == nil || term.target == nil ||
		term.target.recursiveOwner != term {
		t.Fatal("Release detached an active Statement adapter")
	}
	term.end()
	term.Release()
	term.Release()
	if statement.cteReference().def.recursiveOwner != nil {
		t.Fatal("released adapter retained target ownership")
	}
	if mode := statement.cteReference().mode(); mode != cteFused {
		t.Fatalf("post-Release direct CTE mode = %s, want fused", mode)
	}
	var exec Exec
	cursor, err := statement.RunInto(
		&exec, FromDatabase(snapshot, "seeds"), nil,
	)
	if err != nil || cursor.Next() {
		t.Fatalf("borrowed Statement reuse after adapter Release = row %v, %v",
			cursor.Row(), err)
	}
}

func TestRecursiveCTEStatementCancellationAndUnscopedExecutionAreAtomic(t *testing.T) {
	edges := [][2]int{{0, 1}, {1, 2}}
	_, snapshot := recursiveStatementDatabase(t, edges)
	graph := prepareRecursiveStatementGraph(t, 0, 1)
	defer graph.release()
	var unscoped Exec
	if err := graph.anchor.RunRecursiveCTETerm(
		&unscoped,
		RecursiveCTETermInput{Base: FromDatabase(snapshot, "seeds")},
	); !errors.Is(err, ErrRecursiveCTEStatement) {
		t.Fatalf("unscoped execution error = %v", err)
	}

	var cancel CancelFlag
	cancel.Cancel()
	options := ExecOptions{IntermediateBytes: -1, Cancel: &cancel}
	frame := beginRecursiveCTEFrame(t, options)
	frame.args = []any{int64(0), true}
	var runtime RecursiveCTERuntime
	_, err := runtime.executeStatementTerms(
		graph.descriptor, FromDatabase(snapshot, "seeds"), frame, options,
	)
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("pre-canceled Statement recursion error = %v", err)
	}
	if frame.intermediate.used != 0 || runtime.currentResult().Rows() != 0 ||
		graph.recursive.target.recursiveBinding != nil {
		t.Fatalf("canceled Statement recursion leaked result/charge/binding")
	}
	cancel.Reset()
	result, err := runtime.executeStatementTerms(
		graph.descriptor, FromDatabase(snapshot, "seeds"), frame, options,
	)
	if err != nil || result.Rows() != 3 {
		t.Fatalf("reuse after cancellation = %d rows, %v", result.Rows(), err)
	}
	runtime.releaseExecution(frame)
}

func TestRecursiveCTEStatementBudgetsAreAtomicAndReleaseDropsHighWater(t *testing.T) {
	edges := [][2]int{{0, 1}, {0, 2}, {1, 3}, {2, 4}, {4, 5}}
	_, snapshot := recursiveStatementDatabase(t, edges)
	graph := prepareRecursiveStatementGraph(t, 0, 1)
	defer graph.anchorStmt.Release()
	defer graph.recursiveStmt.Release()
	args := []any{int64(0), true}
	limited := ExecOptions{IntermediateBytes: 1}
	limitedFrame := beginRecursiveCTEFrame(t, limited)
	limitedFrame.args = args
	var runtime RecursiveCTERuntime
	_, err := runtime.executeStatementTerms(
		graph.descriptor, FromDatabase(snapshot, "seeds"), limitedFrame, limited,
	)
	if !errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("bounded Statement recursion error = %v, want intermediate budget", err)
	}
	if limitedFrame.intermediate.used != 0 || runtime.currentResult().Rows() != 0 ||
		graph.recursive.target.recursiveBinding != nil {
		t.Fatal("bounded Statement recursion published partial state")
	}

	// Caller-visible result limits do not constrain a relation-valued recursive
	// term. Its exact result and every nested join share IntermediateBytes.
	unlimited := ExecOptions{
		ResultRows: 1, ResultBytes: 1, IntermediateBytes: -1,
	}
	frame := beginRecursiveCTEFrame(t, unlimited)
	frame.args = args
	result, err := runtime.executeStatementTerms(
		graph.descriptor, FromDatabase(snapshot, "seeds"), frame, unlimited,
	)
	if err != nil || result.Rows() != 6 {
		t.Fatalf("reuse after budget error = %d rows, %v", result.Rows(), err)
	}
	runtime.releaseExecution(frame)
	if frame.intermediate.used != 0 {
		t.Fatalf("successful cleanup retained %d bytes", frame.intermediate.used)
	}
	if cap(graph.anchor.owned[0])+cap(graph.anchor.owned[1]) == 0 ||
		cap(graph.recursive.owned[0])+cap(graph.recursive.owned[1]) == 0 {
		t.Fatal("test did not establish adapter result high-water storage")
	}
	runtime.Release()
	graph.anchor.Release()
	graph.recursive.Release()
	graph.anchor.Release()
	graph.recursive.Release()
	if graph.anchor.owned[0] != nil || graph.anchor.owned[1] != nil ||
		graph.recursive.owned[0] != nil || graph.recursive.owned[1] != nil ||
		graph.recursiveStmt.cteCatalog().defs[0].recursiveOwner != nil {
		t.Fatal("adapter Release retained high-water storage or target ownership")
	}
}

func TestRecursiveCTEStatementIndependentExecutorsRaceSafe(t *testing.T) {
	edges := [][2]int{{0, 1}, {0, 2}, {1, 3}, {2, 4}, {4, 5}}
	_, snapshot := recursiveStatementDatabase(t, edges)
	const workers = 6
	graphs := make([]*recursiveStatementGraph, workers)
	for worker := range graphs {
		graphs[worker] = prepareRecursiveStatementGraph(t, 0, 1)
		defer graphs[worker].release()
	}
	var wait sync.WaitGroup
	errorsOut := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func(graph *recursiveStatementGraph) {
			defer wait.Done()
			options := ExecOptions{IntermediateBytes: -1}
			var frame statementFrame
			err := frame.begin(options)
			frame.args = []any{int64(0), true}
			var runtime RecursiveCTERuntime
			var result RecursiveCTEResult
			if err == nil {
				result, err = runtime.executeStatementTerms(
					graph.descriptor, FromDatabase(snapshot, "seeds"), &frame, options,
				)
			}
			if err == nil && result.Rows() != 6 {
				err = fmt.Errorf("rows = %d, want 6", result.Rows())
			}
			if err == nil {
				runtime.releaseExecution(&frame)
			}
			errorsOut <- err
		}(graphs[worker])
	}
	wait.Wait()
	close(errorsOut)
	for err := range errorsOut {
		if err != nil {
			t.Fatal(err)
		}
	}
}
