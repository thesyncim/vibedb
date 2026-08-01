package query

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/store"
)

func prepareRecursiveEchoDescriptor(
	tb testing.TB,
	union RecursiveUnionMode,
	materialization RecursiveCTEMaterialization,
) *RecursiveCTEDescriptor {
	tb.Helper()
	anchor, err := PrepareRecursiveCTETerm(Select(Path("/0")))
	if err != nil {
		tb.Fatal(err)
	}
	recursive, err := PrepareRecursiveCTETerm(Select(Path("/0")))
	if err != nil {
		tb.Fatal(err)
	}
	descriptor, err := PrepareRecursiveCTEDescriptor(
		"walk", []string{"node"}, anchor, recursive, union,
		materialization,
		RecursiveCTELimits{MaxIterations: 32, MaxRows: 1_024, MaxBytes: -1},
	)
	if err != nil {
		tb.Fatal(err)
	}
	return descriptor
}

func beginRecursiveCTEFrame(tb testing.TB, options ExecOptions) *statementFrame {
	tb.Helper()
	frame := new(statementFrame)
	if err := frame.begin(options); err != nil {
		tb.Fatal(err)
	}
	return frame
}

func TestRecursiveCTEDescriptorClonesNamesAndChecksArity(t *testing.T) {
	anchor, err := PrepareRecursiveCTETerm(Select(Path("/0"), Path("/1")))
	if err != nil {
		t.Fatal(err)
	}
	recursive, err := PrepareRecursiveCTETerm(Select(Path("/0"), Path("/1")))
	if err != nil {
		t.Fatal(err)
	}
	nameStorage := []byte("closure")
	leftStorage := []byte("node")
	rightStorage := []byte("node")
	name := unsafe.String(&nameStorage[0], len(nameStorage))
	aliases := []string{
		unsafe.String(&leftStorage[0], len(leftStorage)),
		unsafe.String(&rightStorage[0], len(rightStorage)),
	}
	descriptor, err := PrepareRecursiveCTEDescriptor(
		name, aliases, anchor, recursive, RecursiveUnionDistinct,
		RecursiveCTEShared, RecursiveCTELimits{},
	)
	if err != nil {
		t.Fatal(err)
	}
	copy(nameStorage, "mutated")
	copy(leftStorage, "xxxx")
	copy(rightStorage, "yyyy")
	if descriptor.Name() != "closure" {
		t.Fatalf("stable name = %q, want closure", descriptor.Name())
	}
	if got := descriptor.Columns(); len(got) != 2 || got[0] != "node" || got[1] != "node" {
		t.Fatalf("stable duplicate aliases = %q, want [node node]", got)
	}

	one, err := PrepareRecursiveCTETerm(Select(Path("/0")))
	if err != nil {
		t.Fatal(err)
	}
	_, err = PrepareRecursiveCTEDescriptor(
		"bad", nil, anchor, one, RecursiveUnionAll,
		RecursiveCTEReferenceLocal, RecursiveCTELimits{},
	)
	var arity *RecursiveCTEArityError
	if !errors.As(err, &arity) || !errors.Is(err, ErrRecursiveCTEArity) ||
		arity.Expected != 2 || arity.Actual != 1 || arity.Term != "recursive" {
		t.Fatalf("term arity error = %#v (%v)", arity, err)
	}
	_, err = PrepareRecursiveCTEDescriptor(
		"bad", []string{"only"}, anchor, recursive, RecursiveUnionAll,
		RecursiveCTEReferenceLocal, RecursiveCTELimits{},
	)
	if !errors.As(err, &arity) || arity.Term != "aliases" {
		t.Fatalf("alias arity error = %#v (%v)", arity, err)
	}
	_, err = PrepareRecursiveCTEDescriptor(
		"bad", nil, nil, recursive, RecursiveUnionAll,
		RecursiveCTEReferenceLocal, RecursiveCTELimits{},
	)
	if !errors.Is(err, ErrRecursiveCTEConfig) {
		t.Fatalf("nil term error = %v, want ErrRecursiveCTEConfig", err)
	}
	_, err = PrepareRecursiveCTEDescriptor(
		"bad", nil, one, one, RecursiveUnionAll,
		RecursiveCTEReferenceLocal, RecursiveCTELimits{MaxRows: -2},
	)
	if !errors.Is(err, ErrRecursiveCTEConfig) || !errors.Is(err, ErrRecursiveConfig) {
		t.Fatalf("invalid limits error = %v, want CTE and fixpoint config classes", err)
	}
}

func TestRecursiveCTEUnionDistinctExactIdentityAndOwnedSnapshot(t *testing.T) {
	input := buildRelationSpoolForTest(t, [][]string{
		{"1"}, {"1.0"}, {"10e-1"},
		{"null"}, {"null"},
		{`"x"`}, {`"\u0078"`},
		{`{"a":1}`}, {`{"a":1}`}, {`{"a":1 }`},
	})
	// The first NULL is the engine's missing marker. DISTINCT must treat it as
	// not distinct from explicit NULL while preserving the first spelling.
	input.columns[0][3] = scalar{kind: kindNull}
	descriptor := prepareRecursiveEchoDescriptor(
		t, RecursiveUnionDistinct, RecursiveCTEReferenceLocal,
	)
	frame := beginRecursiveCTEFrame(t, ExecOptions{IntermediateBytes: -1})
	var runtime RecursiveCTERuntime
	result, err := runtime.execute(
		descriptor, fromRelationSpool(&input), frame,
		ExecOptions{IntermediateBytes: -1},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows() != 5 || result.Iterations() != 1 {
		t.Fatalf("shape/iterations = %d/%d, want 5/1", result.Rows(), result.Iterations())
	}
	if !result.Missing(1, 0) {
		t.Fatal("first NULL identity did not retain the missing marker")
	}
	want := []string{"1", "null", `"x"`, `{"a":1}`, `{"a":1 }`}
	for row := range want {
		if got := string(result.Cell(row, 0).AppendJSON(nil)); got != want[row] {
			t.Fatalf("row %d = %q, want %q", row, got, want[row])
		}
	}
	// The fixpoint owns exact values; neither the source spool nor term Result
	// remains part of the published snapshot lifetime.
	input.release()
	if got := string(result.Cell(4, 0).AppendJSON(nil)); got != `{"a":1 }` {
		t.Fatalf("owned snapshot after source release = %q", got)
	}
	runtime.releaseExecution(frame)
	if frame.intermediate.used != 0 {
		t.Fatalf("released frame still charges %d bytes", frame.intermediate.used)
	}
}

func TestRecursiveCTEUnionAllEmptyAndDuplicateOrder(t *testing.T) {
	anchor, err := PrepareRecursiveCTETerm(Select(Path("/0")))
	if err != nil {
		t.Fatal(err)
	}
	stop, err := PrepareRecursiveCTECallbackTerm(
		[]string{"/0"},
		func(exec *Exec, _ RecursiveCTETermInput) error {
			return prepareRecursiveCTETestResult(exec, []string{"/0"}, 0)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := PrepareRecursiveCTEDescriptor(
		"all_rows", []string{"value"}, anchor, stop, RecursiveUnionAll,
		RecursiveCTEReferenceLocal,
		RecursiveCTELimits{MaxIterations: 4, MaxRows: 16, MaxBytes: -1},
	)
	if err != nil {
		t.Fatal(err)
	}
	options := ExecOptions{IntermediateBytes: -1}
	for _, test := range []struct {
		name string
		rows [][]string
		want []int64
		iter int
	}{
		{name: "empty", rows: nil, iter: 0},
		{name: "one", rows: [][]string{{"7"}}, want: []int64{7}, iter: 1},
		{name: "duplicates", rows: [][]string{{"2"}, {"1"}, {"2"}}, want: []int64{2, 1, 2}, iter: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var input relationSpool
			if len(test.rows) == 0 {
				if err := input.begin(0, 1, 0); err != nil {
					t.Fatal(err)
				}
			} else {
				input = buildRelationSpoolForTest(t, test.rows)
			}
			defer input.release()
			frame := beginRecursiveCTEFrame(t, options)
			var runtime RecursiveCTERuntime
			result, err := runtime.execute(
				descriptor, fromRelationSpool(&input), frame, options,
			)
			if err != nil {
				t.Fatal(err)
			}
			if result.Rows() != len(test.want) || result.Iterations() != test.iter {
				t.Fatalf("shape = %d/%d, want %d/%d",
					result.Rows(), result.Iterations(), len(test.want), test.iter)
			}
			for row, want := range test.want {
				got, ok := result.Cell(row, 0).Int64()
				if !ok || got != want {
					t.Fatalf("row %d = %d/%v, want %d", row, got, ok, want)
				}
			}
			runtime.releaseExecution(frame)
		})
	}
}

func TestRecursiveCTESharedAndReferenceLocalSemantics(t *testing.T) {
	input := buildRelationSpoolForTest(t, [][]string{{"1"}, {"2"}})
	defer input.release()
	options := ExecOptions{IntermediateBytes: -1}

	shared := prepareRecursiveEchoDescriptor(t, RecursiveUnionDistinct, RecursiveCTEShared)
	sharedFrame := beginRecursiveCTEFrame(t, options)
	var sharedRuntime RecursiveCTERuntime
	first, err := sharedRuntime.execute(shared, fromRelationSpool(&input), sharedFrame, options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := sharedRuntime.execute(shared, fromRelationSpool(&input), sharedFrame, options)
	if err != nil {
		t.Fatal(err)
	}
	if first.relation != second.relation || first.Evaluation() != second.Evaluation() ||
		sharedRuntime.Evaluations() != 1 {
		t.Fatalf("shared evaluations/pointers = %d/%d %p/%p",
			first.Evaluation(), second.Evaluation(), first.relation, second.relation)
	}
	otherFrame := beginRecursiveCTEFrame(t, options)
	if _, err := sharedRuntime.execute(shared, fromRelationSpool(&input), otherFrame, options); !errors.Is(err, ErrRecursiveCTEConfig) {
		t.Fatalf("cross-frame shared error = %v, want config error", err)
	}
	if first.Rows() != 2 {
		t.Fatal("cross-frame rejection disturbed the existing shared snapshot")
	}
	sharedRuntime.releaseExecution(sharedFrame)

	local := prepareRecursiveEchoDescriptor(t, RecursiveUnionDistinct, RecursiveCTEReferenceLocal)
	localFrame := beginRecursiveCTEFrame(t, options)
	var localRuntime RecursiveCTERuntime
	one, err := localRuntime.execute(local, fromRelationSpool(&input), localFrame, options)
	if err != nil {
		t.Fatal(err)
	}
	two, err := localRuntime.execute(local, fromRelationSpool(&input), localFrame, options)
	if err != nil {
		t.Fatal(err)
	}
	if one.Evaluation() != 1 || two.Evaluation() != 2 || localRuntime.Evaluations() != 2 {
		t.Fatalf("reference-local evaluations = %d/%d total=%d",
			one.Evaluation(), two.Evaluation(), localRuntime.Evaluations())
	}
	localRuntime.releaseExecution(localFrame)
}

type recursiveGraphJoinTerm struct {
	edges *Query
}

func (t *recursiveGraphJoinTerm) RecursiveCTEColumns() []string {
	return []string{"node"}
}

func (t *recursiveGraphJoinTerm) RunRecursiveCTETerm(
	exec *Exec,
	input RecursiveCTETermInput,
) error {
	if !input.Delta.Valid() || input.Iteration != input.Delta.Iteration()+1 {
		return fmt.Errorf("invalid recursive graph delta metadata")
	}
	edges, err := input.Collection("edges")
	if err != nil {
		return err
	}
	if err := t.edges.RunInto(exec, edges); err != nil {
		return err
	}
	if len(exec.Result.Columns) != 2 {
		return fmt.Errorf("edge plan returned %d columns", len(exec.Result.Columns))
	}
	write := 0
	payload := int64(0)
	for edge := 0; edge < exec.Result.RowCount; edge++ {
		from := exec.Result.Columns[0].Cells[edge]
		matched := false
		for row := 0; row < input.Delta.Rows(); row++ {
			if compareScalar(
				recursiveCTEScalarFromCell(from),
				recursiveCTEScalarFromCell(input.Delta.Cell(row, 0)),
			) == 0 {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		to := exec.Result.Columns[1].Cells[edge]
		exec.Result.Columns[0].Cells[write] = to
		payload = saturatedBytes(payload, resultCellPayloadBytes(to))
		write++
	}
	clear(exec.Result.Columns[0].Cells[write:])
	clear(exec.Result.Columns[1].Cells)
	exec.Result.Columns[0].Cells = exec.Result.Columns[0].Cells[:write]
	exec.Result.Columns[0].Header = "node"
	exec.Result.Columns = exec.Result.Columns[:1]
	exec.Result.RowCount = write
	required, err := exec.Result.checkResultBudget(1, write, payload)
	if err != nil {
		exec.Result.abortResult()
		return err
	}
	exec.Result.resultBytesUsed = required
	return nil
}

func recursiveCTEScalarFromCell(cell Cell) scalar {
	switch cell.kind {
	case TypeNull:
		if cell.flag&cellMissing != 0 {
			return scalar{kind: kindNull}
		}
		return scalar{kind: kindNull, raw: cell.raw}
	case TypeBool:
		value, _ := cell.Bool()
		return scalar{kind: kindBool, bval: value, raw: cell.raw}
	case TypeNumber:
		value := scalar{kind: kindNumber, num: cell.raw, raw: cell.raw}
		if integer, ok := cell.Int64(); ok {
			value.isInt, value.ival = true, integer
		}
		return value
	case TypeString:
		return scalar{kind: kindString, sval: cell.text, raw: cell.raw}
	default:
		return scalar{kind: kindContainer, raw: cell.raw}
	}
}

func TestRecursiveCTEGraphClosureUsesBaseAndDeltaAtOneSnapshot(t *testing.T) {
	var database store.Database
	seeds, err := database.CreateCollection("seeds", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	edges, err := database.CreateCollection("edges", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seeds.Put("seed", []byte(`{"node":1}`)); err != nil {
		t.Fatal(err)
	}
	graph := [][2]int{{1, 2}, {1, 3}, {2, 4}, {3, 4}, {4, 5}}
	for i, edge := range graph {
		doc := []byte(fmt.Sprintf(`{"from":%d,"to":%d}`, edge[0], edge[1]))
		if _, err := edges.Put(strconv.Itoa(i), doc); err != nil {
			t.Fatal(err)
		}
	}
	coherent := database.Snapshot()
	// This edge exists in the live database but not in the source snapshot.
	// A recursive term that resnapshots base collections would incorrectly
	// include node 99 in the closure.
	if _, err := edges.Put("late", []byte(`{"from":5,"to":99}`)); err != nil {
		t.Fatal(err)
	}

	anchor, err := PrepareRecursiveCTETerm(Select(Path("node")))
	if err != nil {
		t.Fatal(err)
	}
	recursive := &recursiveGraphJoinTerm{
		edges: Select(Path("from"), Path("to")),
	}
	if err := recursive.edges.Prepare(); err != nil {
		t.Fatal(err)
	}
	descriptor, err := PrepareRecursiveCTEDescriptor(
		"reachable", []string{"node"}, anchor, recursive,
		RecursiveUnionDistinct, RecursiveCTEReferenceLocal,
		RecursiveCTELimits{MaxIterations: 16, MaxRows: 128, MaxBytes: -1},
	)
	if err != nil {
		t.Fatal(err)
	}
	options := ExecOptions{IntermediateBytes: -1}
	frame := beginRecursiveCTEFrame(t, options)
	var runtime RecursiveCTERuntime
	result, err := runtime.execute(
		descriptor, FromDatabase(coherent, "seeds"), frame, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{1, 2, 3, 4, 5}
	if result.Rows() != len(want) || result.Iterations() != 4 {
		t.Fatalf("closure shape = %d rows/%d iterations, want %d/4",
			result.Rows(), result.Iterations(), len(want))
	}
	for row, expected := range want {
		got, ok := result.Cell(row, 0).Int64()
		if !ok || got != expected {
			t.Fatalf("closure row %d = %d/%v, want %d", row, got, ok, expected)
		}
	}
	runtime.releaseExecution(frame)
	if frame.intermediate.used != 0 {
		t.Fatalf("closure cleanup retained %d statement bytes", frame.intermediate.used)
	}
}

func TestRecursiveCTEBudgetsCancellationNoPartialPublication(t *testing.T) {
	input := buildRelationSpoolForTest(t, [][]string{{"1"}, {"2"}})
	defer input.release()
	descriptor := prepareRecursiveEchoDescriptor(
		t, RecursiveUnionDistinct, RecursiveCTEReferenceLocal,
	)
	var runtime RecursiveCTERuntime

	termBytes := resultColumnBytes + 2*resultCellBytes + 2
	fixpointBytes := recursiveRowRetainedBytes(1, 1)
	limit := termBytes + fixpointBytes - 1
	limitedOptions := ExecOptions{IntermediateBytes: limit}
	limitedFrame := beginRecursiveCTEFrame(t, limitedOptions)
	result, err := runtime.execute(
		descriptor, fromRelationSpool(&input), limitedFrame, limitedOptions,
	)
	var intermediate *RecursiveCTEIntermediateError
	if !errors.As(err, &intermediate) || !errors.Is(err, ErrIntermediateBudget) ||
		!errors.Is(err, ErrRecursiveBytes) {
		t.Fatalf("combined budget error = %#v (%v)", intermediate, err)
	}
	if intermediate.Bytes <= intermediate.Limit || intermediate.Limit != limit {
		t.Fatalf("combined budget diagnostic = %d/%d, want required > limit %d",
			intermediate.Bytes, intermediate.Limit, limit)
	}
	if result.Rows() != 0 || runtime.currentResult().Rows() != 0 ||
		limitedFrame.intermediate.used != 0 {
		t.Fatalf("failed budget published rows or charge: result=%d current=%d used=%d",
			result.Rows(), runtime.currentResult().Rows(), limitedFrame.intermediate.used)
	}

	var cancel CancelFlag
	cancel.Cancel()
	cancelOptions := ExecOptions{IntermediateBytes: -1, Cancel: &cancel}
	cancelFrame := beginRecursiveCTEFrame(t, cancelOptions)
	if _, err := runtime.execute(
		descriptor, fromRelationSpool(&input), cancelFrame, cancelOptions,
	); !errors.Is(err, ErrCanceled) {
		t.Fatalf("pre-canceled execution error = %v, want ErrCanceled", err)
	}
	if cancelFrame.intermediate.used != 0 || runtime.currentResult().Rows() != 0 {
		t.Fatalf("canceled execution leaked charge/result: %d/%d",
			cancelFrame.intermediate.used, runtime.currentResult().Rows())
	}
	cancel.Reset()
	result, err = runtime.execute(
		descriptor, fromRelationSpool(&input), cancelFrame, cancelOptions,
	)
	if err != nil || result.Rows() != 2 {
		t.Fatalf("reuse after cancellation = %d rows, %v", result.Rows(), err)
	}
	runtime.releaseExecution(cancelFrame)

	// Outer statement result limits do not silently truncate an intermediate
	// recursive term; the statement-wide intermediate account is authoritative.
	outerOptions := ExecOptions{
		ResultRows: 1, ResultBytes: 1, IntermediateBytes: -1,
	}
	outerFrame := beginRecursiveCTEFrame(t, outerOptions)
	result, err = runtime.execute(
		descriptor, fromRelationSpool(&input), outerFrame, outerOptions,
	)
	if err != nil || result.Rows() != 2 {
		t.Fatalf("outer result limits constrained CTE = %d rows, %v", result.Rows(), err)
	}
	runtime.releaseExecution(outerFrame)
}

func TestRecursiveCTEPartialDeltaBindCancellationIsAtomic(t *testing.T) {
	var runtime RecursiveCTERuntime
	var destination relationSpool
	var cancel CancelFlag
	cancel.Cancel()
	frame := beginRecursiveCTEFrame(t, ExecOptions{IntermediateBytes: -1})
	view := recursiveView{
		spool: &recursiveSpool{columns: 1}, rows: 1 << 12,
	}
	charge, err := runtime.bindRecursiveView(
		view, &destination, frame, &cancel, "canceled delta test",
	)
	if !errors.Is(err, ErrCanceled) || charge != 0 || destination.rows != 0 ||
		frame.intermediate.used != 0 {
		t.Fatalf("partial bind = charge %d rows %d used %d err %v",
			charge, destination.rows, frame.intermediate.used, err)
	}
	if cap(destination.columns) == 0 {
		t.Fatal("test did not reach partial destination acquisition")
	}
}

func TestRecursiveCTEReentryIsTypedAndReusable(t *testing.T) {
	input := buildRelationSpoolForTest(t, [][]string{{"1"}})
	defer input.release()
	options := ExecOptions{IntermediateBytes: -1}
	frame := beginRecursiveCTEFrame(t, options)
	var runtime RecursiveCTERuntime
	var descriptor *RecursiveCTEDescriptor
	anchor, err := PrepareRecursiveCTECallbackTerm(
		[]string{"node"},
		func(_ *Exec, input RecursiveCTETermInput) error {
			_, nestedErr := runtime.execute(descriptor, input.Base, frame, options)
			return nestedErr
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	recursive, err := PrepareRecursiveCTETerm(Select(Path("/0")))
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err = PrepareRecursiveCTEDescriptor(
		"cycle", nil, anchor, recursive, RecursiveUnionDistinct,
		RecursiveCTEReferenceLocal,
		RecursiveCTELimits{MaxIterations: 8, MaxRows: 8, MaxBytes: -1},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = runtime.execute(descriptor, fromRelationSpool(&input), frame, options)
	var reentry *RecursiveCTEReentryError
	if !errors.As(err, &reentry) || !errors.Is(err, ErrRecursiveCTEReentry) ||
		!errors.Is(err, ErrRecursiveInUse) || reentry.Name != "cycle" {
		t.Fatalf("reentry error = %#v (%v)", reentry, err)
	}
	if frame.intermediate.used != 0 || runtime.currentResult().Rows() != 0 {
		t.Fatalf("reentry leaked frame/result: %d/%d",
			frame.intermediate.used, runtime.currentResult().Rows())
	}
	recovery := prepareRecursiveEchoDescriptor(
		t, RecursiveUnionDistinct, RecursiveCTEReferenceLocal,
	)
	result, err := runtime.execute(
		recovery, fromRelationSpool(&input), frame, options,
	)
	if err != nil || result.Rows() != 1 {
		t.Fatalf("runtime reuse after reentry = %d rows, %v", result.Rows(), err)
	}
	runtime.releaseExecution(frame)
}

func TestRecursiveCTEExecutionArityFailureIsAtomic(t *testing.T) {
	input := buildRelationSpoolForTest(t, [][]string{{"1"}})
	defer input.release()
	bad, err := PrepareRecursiveCTECallbackTerm(
		[]string{"node"},
		func(exec *Exec, _ RecursiveCTETermInput) error {
			return prepareRecursiveCTETestResult(exec, []string{"a", "b"}, 1)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	recursive, err := PrepareRecursiveCTETerm(Select(Path("/0")))
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := PrepareRecursiveCTEDescriptor(
		"bad_runtime", nil, bad, recursive, RecursiveUnionDistinct,
		RecursiveCTEReferenceLocal, RecursiveCTELimits{},
	)
	if err != nil {
		t.Fatal(err)
	}
	frame := beginRecursiveCTEFrame(t, ExecOptions{IntermediateBytes: -1})
	var runtime RecursiveCTERuntime
	_, err = runtime.execute(
		descriptor, fromRelationSpool(&input), frame,
		ExecOptions{IntermediateBytes: -1},
	)
	var arity *RecursiveCTEArityError
	if !errors.As(err, &arity) || arity.Term != "execution" {
		t.Fatalf("runtime arity error = %#v (%v)", arity, err)
	}
	if frame.intermediate.used != 0 || runtime.currentResult().Rows() != 0 {
		t.Fatalf("arity failure leaked frame/result: %d/%d",
			frame.intermediate.used, runtime.currentResult().Rows())
	}
}

func TestRecursiveCTEReleaseDropsHighWaterAndIsIdempotent(t *testing.T) {
	input := buildRelationSpoolForTest(t, [][]string{
		{"1"}, {"2"}, {"3"}, {"4"}, {"5"}, {"6"}, {"7"}, {"8"},
	})
	defer input.release()
	descriptor := prepareRecursiveEchoDescriptor(
		t, RecursiveUnionDistinct, RecursiveCTEReferenceLocal,
	)
	options := ExecOptions{IntermediateBytes: -1}
	frame := beginRecursiveCTEFrame(t, options)
	var runtime RecursiveCTERuntime
	if _, err := runtime.execute(
		descriptor, fromRelationSpool(&input), frame, options,
	); err != nil {
		t.Fatal(err)
	}
	runtime.releaseExecution(frame)
	if cap(runtime.fixpoint.result.cells) == 0 || cap(runtime.snapshot.columns) == 0 ||
		cap(runtime.row) == 0 {
		t.Fatal("statement cleanup unexpectedly dropped warmed high-water storage")
	}
	runtime.Release()
	runtime.Release()
	if cap(runtime.fixpoint.result.cells) != 0 || cap(runtime.fixpoint.working.cells) != 0 ||
		cap(runtime.fixpoint.identity.slots) != 0 || cap(runtime.delta.columns) != 0 ||
		cap(runtime.snapshot.columns) != 0 || cap(runtime.row) != 0 ||
		runtime.Evaluations() != 0 {
		t.Fatalf(
			"Release retained capacities result=%d working=%d identity=%d delta=%d snapshot=%d row=%d evaluations=%d",
			cap(runtime.fixpoint.result.cells), cap(runtime.fixpoint.working.cells),
			cap(runtime.fixpoint.identity.slots), cap(runtime.delta.columns),
			cap(runtime.snapshot.columns), cap(runtime.row), runtime.Evaluations(),
		)
	}
}

func prepareRecursiveCTETestResult(
	exec *Exec,
	names []string,
	rows int,
) error {
	rowLimit, byteLimit, err := normalizeResultBudget(exec.Options)
	if err != nil {
		return err
	}
	exec.Result.beginResultBudget(rowLimit, byteLimit)
	if err := exec.Result.admitResultShape(len(names), rows); err != nil {
		return err
	}
	if cap(exec.Result.Columns) < len(names) {
		exec.Result.Columns = make([]ResultColumn, len(names))
	} else {
		exec.Result.Columns = exec.Result.Columns[:len(names)]
	}
	for column := range names {
		exec.Result.Columns[column].Header = names[column]
		exec.Result.Columns[column].Cells = resize(
			exec.Result.Columns[column].Cells, rows,
		)
	}
	exec.Result.RowCount = rows
	return nil
}

func TestRecursiveCTEIndependentRuntimesRaceSafe(t *testing.T) {
	input := buildRelationSpoolForTest(t, [][]string{{"1"}, {"2"}, {"3"}})
	defer input.release()
	descriptor := prepareRecursiveEchoDescriptor(
		t, RecursiveUnionDistinct, RecursiveCTEReferenceLocal,
	)
	const workers = 4
	var wait sync.WaitGroup
	errorsByWorker := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			var runtime RecursiveCTERuntime
			options := ExecOptions{IntermediateBytes: -1}
			for run := 0; run < 64; run++ {
				var frame statementFrame
				if err := frame.begin(options); err != nil {
					errorsByWorker <- err
					return
				}
				result, err := runtime.execute(
					descriptor, fromRelationSpool(&input), &frame, options,
				)
				if err != nil || result.Rows() != 3 {
					errorsByWorker <- fmt.Errorf("rows=%d: %w", result.Rows(), err)
					return
				}
				runtime.releaseExecution(&frame)
				if frame.intermediate.used != 0 {
					errorsByWorker <- fmt.Errorf("frame retained %d", frame.intermediate.used)
					return
				}
			}
			runtime.Release()
		}()
	}
	wait.Wait()
	close(errorsByWorker)
	for err := range errorsByWorker {
		t.Error(err)
	}
}
