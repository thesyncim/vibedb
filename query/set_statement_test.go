package query

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func setStatementDatabase(t testing.TB) *store.Database {
	t.Helper()
	db := &store.Database{}
	put := func(name string, docs ...string) {
		t.Helper()
		collection, err := db.CreateCollection(name, store.Options{ChunkDocuments: 2})
		if err != nil {
			t.Fatalf("CreateCollection(%q): %v", name, err)
		}
		for row, doc := range docs {
			if _, err := collection.Put(
				fmt.Sprintf("%s-%02d", name, row), []byte(doc),
			); err != nil {
				t.Fatalf("Put(%q, %d): %v", name, row, err)
			}
		}
	}
	put("set_left",
		`{"v":1,"tag":"l1"}`,
		`{"v":2,"tag":"l2a"}`,
		`{"v":2,"tag":"l2b"}`,
		`{"v":4,"tag":"l4"}`,
	)
	put("set_right",
		`{"v":2,"tag":"r2a"}`,
		`{"v":2,"tag":"r2b"}`,
		`{"v":3,"tag":"r3"}`,
		`{"v":4,"tag":"r4"}`,
	)
	return db
}

func prepareSetStatementLeaves(t testing.TB) (*Statement, *Statement) {
	t.Helper()
	left, err := PrepareStatement(
		`SELECT v AS first_value FROM set_left WHERE v >= ? ORDER BY v`,
	)
	if err != nil {
		t.Fatal(err)
	}
	right, err := PrepareStatement(
		`SELECT v AS second_value FROM set_right WHERE v <= ? ORDER BY v`,
	)
	if err != nil {
		left.Release()
		t.Fatal(err)
	}
	return left, right
}

func binarySetStatementPlan(operation SetTreeOperation, columns int) SetTreePlan {
	return SetTreePlan{Nodes: []SetTreeNode{
		NewSetTreeLeaf(0, columns),
		NewSetTreeLeaf(1, columns),
		NewSetTreeBinary(operation, 0, 1),
	}, Root: 2}
}

func prepareBinarySetStatement(
	t testing.TB,
	left, right setStatementRunner,
	operation SetTreeOperation,
	leftBase, rightBase, params int,
) *setStatementDescriptor {
	t.Helper()
	descriptor, err := prepareSetStatementDescriptor(
		binarySetStatementPlan(operation, len(left.Columns())),
		[]setStatementLeaf{
			{runner: left, paramBase: leftBase},
			{runner: right, paramBase: rightBase},
		},
		0,
		params,
	)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func runSetStatementRuntime(
	t testing.TB,
	runtime *setStatementRuntime,
	parent *Exec,
	source Source,
	args []any,
) Cursor {
	t.Helper()
	var frame statementFrame
	if err := frame.begin(parent.Options); err != nil {
		t.Fatal(err)
	}
	frame.args = args
	cursor, err := runtime.runIntoFrame(parent, source, args, &frame)
	if err != nil {
		t.Fatal(err)
	}
	if frame.intermediate.used != 0 {
		t.Fatalf("set statement retained %d intermediate bytes", frame.intermediate.used)
	}
	return cursor
}

func setStatementCursorJSON(cursor Cursor) []string {
	rows := make([]string, 0)
	for cursor.Next() {
		row := ""
		for column := 0; column < len(cursor.res.Columns); column++ {
			if column != 0 {
				row += ","
			}
			row += string(cursor.Cell(column).JSON())
		}
		rows = append(rows, row)
	}
	return rows
}

func statementSetTreeSource(
	t testing.TB,
	statement *Statement,
	source Source,
	args []any,
) SetTreeSource {
	t.Helper()
	var exec Exec
	cursor, err := statement.RunInto(&exec, source, args)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([][]Cell, 0, exec.Result.RowCount)
	for cursor.Next() {
		row := make([]Cell, len(statement.Columns()))
		for column := range row {
			row[column] = cursor.Cell(column)
		}
		rows = append(rows, row)
	}
	return &setTreeTestSource{rows: rows, columns: len(statement.Columns())}
}

func TestSetStatementAllModesMatchDirectTreeAndStableOrder(t *testing.T) {
	db := setStatementDatabase(t)
	left, right := prepareSetStatementLeaves(t)
	defer left.Release()
	defer right.Release()
	catalog := db.Snapshot()
	args := []any{int64(4), int64(1)}
	tests := []struct {
		name string
		op   SetTreeOperation
		want []string
	}{
		{"union all", SetTreeUnionAll, []string{"1", "2", "2", "4", "2", "2", "3", "4"}},
		{"union distinct", SetTreeUnionDistinct, []string{"1", "2", "4", "3"}},
		{"intersect all", SetTreeIntersectAll, []string{"2", "2", "4"}},
		{"intersect distinct", SetTreeIntersectDistinct, []string{"2", "4"}},
		{"except all", SetTreeExceptAll, []string{"1"}},
		{"except distinct", SetTreeExceptDistinct, []string{"1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Deliberately reverse parameter bases: the right operand owns arg 0
			// and the syntactic first/left operand owns arg 1.
			descriptor := prepareBinarySetStatement(
				t, left, right, test.op, 1, 0, len(args),
			)
			var runtime setStatementRuntime
			if err := runtime.prepare(descriptor); err != nil {
				t.Fatal(err)
			}
			defer runtime.Release()
			var parent Exec
			cursor := runSetStatementRuntime(
				t, &runtime, &parent,
				FromDatabase(catalog, left.Collection()), args,
			)
			got := setStatementCursorJSON(cursor)
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("stable rows = %v, want %v", got, test.want)
			}
			if names := descriptor.Columns(); !reflect.DeepEqual(names, []string{"first_value"}) {
				t.Fatalf("descriptor names = %v", names)
			}
			if len(parent.Result.Columns) != 1 ||
				parent.Result.Columns[0].Header != "first_value" {
				t.Fatalf("result schema = %+v", parent.Result.Columns)
			}

			directProgram := &setTreeTestProgram{sources: []SetTreeSource{
				statementSetTreeSource(
					t, left, FromDatabase(catalog, left.Collection()), args[1:2],
				),
				statementSetTreeSource(
					t, right, FromDatabase(catalog, right.Collection()), args[0:1],
				),
			}}
			var direct SetTreeExecutor
			result, err := direct.Run(descriptor.plan, directProgram, SetTreeOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if result.Rows() != parent.Result.RowCount {
				t.Fatalf("direct/runtime rows = %d/%d", result.Rows(), parent.Result.RowCount)
			}
			for row := 0; row < result.Rows(); row++ {
				if string(result.Cell(row, 0).JSON()) !=
					string(parent.Result.Columns[0].Cells[row].JSON()) {
					t.Fatalf("row %d direct/runtime mismatch", row)
				}
			}
		})
	}
}

func TestSetStatementArbitraryChainReusesPreparedRunnerWithDistinctParamBase(t *testing.T) {
	db := setStatementDatabase(t)
	left, right := prepareSetStatementLeaves(t)
	defer left.Release()
	defer right.Release()
	var builder SetTreeBuilder
	plan, err := builder.BuildChain(
		[]SetTreeLeafSpec{
			{Source: 0, Columns: 1},
			{Source: 1, Columns: 1},
			{Source: 2, Columns: 1},
		},
		[]SetTreeOperation{SetTreeUnionAll, SetTreeExceptDistinct},
	)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := prepareSetStatementDescriptor(
		plan,
		[]setStatementLeaf{
			{runner: left, paramBase: 0},
			{runner: right, paramBase: 1},
			// The same immutable prepared statement is a distinct leaf with its
			// own Exec and binding range.
			{runner: right, paramBase: 2},
		},
		0, 3,
	)
	if err != nil {
		t.Fatal(err)
	}
	var runtime setStatementRuntime
	if err := runtime.prepare(descriptor); err != nil {
		t.Fatal(err)
	}
	defer runtime.Release()
	var parent Exec
	got := setStatementCursorJSON(runSetStatementRuntime(
		t, &runtime, &parent,
		FromDatabase(db.Snapshot(), left.Collection()),
		[]any{int64(1), int64(4), int64(2)},
	))
	if want := []string{"1", "4", "3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("chain rows = %v, want %v", got, want)
	}
}

func TestSetStatementUsesOneSnapshotAndFreshPreparedArguments(t *testing.T) {
	db := setStatementDatabase(t)
	left, right := prepareSetStatementLeaves(t)
	defer left.Release()
	defer right.Release()
	descriptor := prepareBinarySetStatement(t, left, right, SetTreeUnionAll, 0, 1, 2)
	var runtime setStatementRuntime
	if err := runtime.prepare(descriptor); err != nil {
		t.Fatal(err)
	}
	defer runtime.Release()
	old := db.Snapshot()
	leftCollection, _ := db.Collection("set_left")
	rightCollection, _ := db.Collection("set_right")
	if _, err := leftCollection.Put("set_left-new", []byte(`{"v":8,"tag":"new-left"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := rightCollection.Put("set_right-new", []byte(`{"v":9,"tag":"new-right"}`)); err != nil {
		t.Fatal(err)
	}

	var parent Exec
	oldRows := setStatementCursorJSON(runSetStatementRuntime(
		t, &runtime, &parent, FromDatabase(old, left.Collection()),
		[]any{int64(4), int64(4)},
	))
	if !reflect.DeepEqual(oldRows, []string{"4", "2", "2", "3", "4"}) {
		t.Fatalf("old coherent snapshot rows = %v", oldRows)
	}
	newRows := setStatementCursorJSON(runSetStatementRuntime(
		t, &runtime, &parent, FromDatabase(db.Snapshot(), left.Collection()),
		[]any{int64(8), int64(9)},
	))
	if !reflect.DeepEqual(newRows, []string{"8", "2", "2", "3", "4", "9"}) {
		t.Fatalf("new coherent snapshot/fresh args rows = %v", newRows)
	}
	if runtime.PeakIntermediateBytes() == 0 {
		t.Fatal("successful set statement did not report an intermediate peak")
	}
}

func TestSetStatementLeafSourcePreservesSameCollectionTransactionOverlay(t *testing.T) {
	holder := NewFileOverlaySource(nil)
	source := FromFileOverlay(nil, &holder)
	got, err := setStatementLeafSource(source, "docs", "docs")
	if err != nil || got.kind != source.kind || got.payload != source.payload ||
		got.file != source.file {
		t.Fatalf("same-collection overlay source changed: got=%+v err=%v", got, err)
	}
	if _, err := setStatementLeafSource(source, "docs", "other"); err == nil {
		t.Fatal("cross-collection overlay was accepted without a coherent catalog")
	}
}

func TestSetStatementFirstOperandOwnsDuplicateNamesAndSchema(t *testing.T) {
	first, err := PrepareStatement(
		`SELECT v AS id, tag AS id FROM set_left ORDER BY v`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	second, err := PrepareStatement(
		`SELECT v AS other_v, tag AS other_tag FROM set_right ORDER BY v`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	descriptor := prepareBinarySetStatement(
		t, first, second, SetTreeUnionAll, 0, 0, 0,
	)
	if got := descriptor.Columns(); !reflect.DeepEqual(got, []string{"id", "id"}) {
		t.Fatalf("duplicate names = %v", got)
	}
	schema := descriptor.AppendSchema(nil)
	if len(schema) != 2 || schema[0].Header != "id" || schema[1].Header != "id" ||
		schema[0].Ordinal != 0 || schema[1].Ordinal != 1 {
		t.Fatalf("first-operand schema = %+v", schema)
	}

	var runtime setStatementRuntime
	if err := runtime.prepare(descriptor); err != nil {
		t.Fatal(err)
	}
	defer runtime.Release()
	var parent Exec
	runSetStatementRuntime(
		t, &runtime, &parent,
		FromDatabase(setStatementDatabase(t).Snapshot(), first.Collection()), nil,
	)
	if parent.Result.Columns[0].Header != "id" || parent.Result.Columns[1].Header != "id" {
		t.Fatalf("duplicate Result headers = %+v", parent.Result.Columns)
	}
}

func TestSetStatementDescriptorRejectsArityAndParameterRangesBeforeExecution(t *testing.T) {
	left, right := prepareSetStatementLeaves(t)
	defer left.Release()
	defer right.Release()
	_, err := prepareSetStatementDescriptor(
		binarySetStatementPlan(SetTreeUnionAll, 2),
		[]setStatementLeaf{{runner: left}, {runner: right}}, 0, 2,
	)
	if !errors.Is(err, ErrSetTreeArity) {
		t.Fatalf("prepared arity error = %v", err)
	}
	_, err = prepareSetStatementDescriptor(
		binarySetStatementPlan(SetTreeUnionAll, 1),
		[]setStatementLeaf{
			{runner: left, paramBase: 0},
			{runner: right, paramBase: 2},
		}, 0, 2,
	)
	if !errors.Is(err, errSetStatementConfig) {
		t.Fatalf("prepared parameter error = %v", err)
	}
}

func TestSetStatementSharedIntermediateLimitIsExactAndRollsBack(t *testing.T) {
	db := setStatementDatabase(t)
	left, right := prepareSetStatementLeaves(t)
	defer left.Release()
	defer right.Release()
	descriptor := prepareBinarySetStatement(t, left, right, SetTreeUnionDistinct, 0, 1, 2)
	var runtime setStatementRuntime
	if err := runtime.prepare(descriptor); err != nil {
		t.Fatal(err)
	}
	defer runtime.Release()
	args := []any{int64(1), int64(4)}
	source := FromDatabase(db.Snapshot(), left.Collection())
	const prior int64 = 37

	run := func(limit int64) (*Exec, *statementFrame, error) {
		parent := &Exec{}
		parent.Options.IntermediateBytes = limit
		frame := &statementFrame{}
		if err := frame.begin(parent.Options); err != nil {
			return parent, frame, err
		}
		frame.args = args
		if err := frame.intermediate.reserve("earlier sibling", prior); err != nil {
			return parent, frame, err
		}
		_, err := runtime.runIntoFrame(parent, source, args, frame)
		return parent, frame, err
	}
	parent, frame, err := run(-1)
	if err != nil || parent.Result.RowCount != 4 || frame.intermediate.used != prior {
		t.Fatalf("unlimited run = rows %d used %d err %v",
			parent.Result.RowCount, frame.intermediate.used, err)
	}
	peak := runtime.PeakIntermediateBytes()
	if peak <= prior {
		t.Fatalf("peak = %d, want above prior %d", peak, prior)
	}
	parent, frame, err = run(peak)
	if err != nil || parent.Result.RowCount != 4 || frame.intermediate.used != prior {
		t.Fatalf("exact-peak run = rows %d used %d err %v",
			parent.Result.RowCount, frame.intermediate.used, err)
	}
	parent, frame, err = run(peak - 1)
	if !errors.Is(err, ErrIntermediateBudget) || !errors.Is(err, ErrSetTreeBytes) {
		t.Fatalf("under-peak error = %T %v", err, err)
	}
	if parent.Result.RowCount != 0 || frame.intermediate.used != prior ||
		runtime.PeakIntermediateBytes() != 0 {
		t.Fatalf("failed run published/retained rows=%d used=%d peak=%d",
			parent.Result.RowCount, frame.intermediate.used,
			runtime.PeakIntermediateBytes())
	}
}

func TestSetStatementResultBudgetRejectsBeforePublicationAndRecovers(t *testing.T) {
	db := setStatementDatabase(t)
	left, right := prepareSetStatementLeaves(t)
	defer left.Release()
	defer right.Release()
	descriptor := prepareBinarySetStatement(t, left, right, SetTreeUnionAll, 0, 1, 2)
	var runtime setStatementRuntime
	if err := runtime.prepare(descriptor); err != nil {
		t.Fatal(err)
	}
	defer runtime.Release()
	args := []any{int64(1), int64(4)}
	source := FromDatabase(db.Snapshot(), left.Collection())
	var parent Exec
	parent.Options.ResultRows = 7
	var frame statementFrame
	if err := frame.begin(parent.Options); err != nil {
		t.Fatal(err)
	}
	frame.args = args
	_, err := runtime.runIntoFrame(&parent, source, args, &frame)
	if !errors.Is(err, ErrResultBudget) || parent.Result.RowCount != 0 ||
		parent.Result.resultBytesUsed != 0 || frame.intermediate.used != 0 {
		t.Fatalf("result rejection = rows %d result bytes %d intermediate %d err %v",
			parent.Result.RowCount, parent.Result.resultBytesUsed,
			frame.intermediate.used, err)
	}
	parent.Options.ResultRows = 8
	if err := frame.begin(parent.Options); err != nil {
		t.Fatal(err)
	}
	frame.args = args
	cursor, err := runtime.runIntoFrame(&parent, source, args, &frame)
	if err != nil || len(setStatementCursorJSON(cursor)) != 8 ||
		frame.intermediate.used != 0 {
		t.Fatalf("result-budget recovery = rows %d bytes %d err %v",
			parent.Result.RowCount, frame.intermediate.used, err)
	}
}

type setStatementErrorRunner struct {
	names      []string
	collection string
	err        error
	entered    chan struct{}
	release    chan struct{}
}

func (r *setStatementErrorRunner) Columns() []string { return r.names }
func (r *setStatementErrorRunner) NumParams() int    { return 0 }
func (r *setStatementErrorRunner) Collection() string {
	return r.collection
}
func (r *setStatementErrorRunner) AppendSchema(dst []OutputColumn) []OutputColumn {
	for column, name := range r.names {
		dst = append(dst, OutputColumn{Header: name, Ordinal: uint32(column)})
	}
	return dst
}
func (r *setStatementErrorRunner) runIntoFrame(
	_ *Exec, _ Source, _ []any, _ *statementFrame, _ string,
) (Cursor, error) {
	if r.entered != nil {
		close(r.entered)
		<-r.release
	}
	return Cursor{}, r.err
}
func (r *setStatementErrorRunner) releaseRelations(*statementFrame) {}

type cancelingSetStatementRunner struct {
	setStatementRunner
	flag    *CancelFlag
	enabled bool
}

func (r *cancelingSetStatementRunner) runIntoFrame(
	exec *Exec,
	source Source,
	args []any,
	frame *statementFrame,
	resource string,
) (Cursor, error) {
	cursor, err := r.setStatementRunner.runIntoFrame(exec, source, args, frame, resource)
	if err == nil && r.enabled {
		r.flag.Cancel()
	}
	return cursor, err
}

func TestSetStatementExactLeafErrorCancellationRecoveryAndNoPartialResult(t *testing.T) {
	db := setStatementDatabase(t)
	left, right := prepareSetStatementLeaves(t)
	defer left.Release()
	defer right.Release()
	sentinel := errors.New("leaf failed exactly")
	failing := &setStatementErrorRunner{
		names: []string{"second_value"}, collection: right.Collection(), err: sentinel,
	}
	descriptor := prepareBinarySetStatement(t, left, failing, SetTreeUnionAll, 0, 0, 1)
	var runtime setStatementRuntime
	if err := runtime.prepare(descriptor); err != nil {
		t.Fatal(err)
	}
	var parent Exec
	var frame statementFrame
	if err := frame.begin(parent.Options); err != nil {
		t.Fatal(err)
	}
	args := []any{int64(1)}
	frame.args = args
	_, err := runtime.runIntoFrame(
		&parent, FromDatabase(db.Snapshot(), left.Collection()), args, &frame,
	)
	if err != sentinel {
		t.Fatalf("leaf error = %T %v, want exact sentinel", err, err)
	}
	if parent.Result.RowCount != 0 || frame.intermediate.used != 0 {
		t.Fatalf("leaf failure published/retained rows=%d bytes=%d",
			parent.Result.RowCount, frame.intermediate.used)
	}

	flag := &CancelFlag{}
	canceling := &cancelingSetStatementRunner{
		setStatementRunner: right, flag: flag, enabled: true,
	}
	descriptor = prepareBinarySetStatement(t, left, canceling, SetTreeUnionAll, 0, 1, 2)
	if err := runtime.prepare(descriptor); err != nil {
		t.Fatal(err)
	}
	parent.Options.Cancel = flag
	args = []any{int64(1), int64(4)}
	if err := frame.begin(parent.Options); err != nil {
		t.Fatal(err)
	}
	frame.args = args
	_, err = runtime.runIntoFrame(
		&parent, FromDatabase(db.Snapshot(), left.Collection()), args, &frame,
	)
	if !errors.Is(err, ErrCanceled) || parent.Result.RowCount != 0 ||
		frame.intermediate.used != 0 {
		t.Fatalf("canceled run = rows %d bytes %d err %v",
			parent.Result.RowCount, frame.intermediate.used, err)
	}
	flag.Reset()
	canceling.enabled = false
	if err := frame.begin(parent.Options); err != nil {
		t.Fatal(err)
	}
	frame.args = args
	cursor, err := runtime.runIntoFrame(
		&parent, FromDatabase(db.Snapshot(), left.Collection()), args, &frame,
	)
	if err != nil || len(setStatementCursorJSON(cursor)) != 8 || frame.intermediate.used != 0 {
		t.Fatalf("recovery = rows %d bytes %d err %v",
			parent.Result.RowCount, frame.intermediate.used, err)
	}
	runtime.Release()
}

func TestSetStatementConcurrentUseRejectedAndIndependentRuntimesRaceSafe(t *testing.T) {
	blocking := &setStatementErrorRunner{
		names: []string{"v"}, collection: "set_left",
		err: errors.New("released"), entered: make(chan struct{}), release: make(chan struct{}),
	}
	descriptor := prepareBinarySetStatement(
		t, blocking, blocking, SetTreeUnionAll, 0, 0, 0,
	)
	var runtime setStatementRuntime
	if err := runtime.prepare(descriptor); err != nil {
		t.Fatal(err)
	}
	blockingSource := FromSegment(&store.Segment{})
	firstDone := make(chan error, 1)
	go func() {
		var parent Exec
		var frame statementFrame
		_ = frame.begin(parent.Options)
		_, err := runtime.runIntoFrame(&parent, blockingSource, nil, &frame)
		firstDone <- err
	}()
	<-blocking.entered
	var secondParent Exec
	var secondFrame statementFrame
	if err := secondFrame.begin(secondParent.Options); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.runIntoFrame(
		&secondParent, blockingSource, nil, &secondFrame,
	); !errors.Is(err, ErrSetTreeInUse) {
		t.Fatalf("concurrent error = %v", err)
	}
	close(blocking.release)
	<-firstDone
	runtime.Release()

	db := setStatementDatabase(t)
	catalog := db.Snapshot()
	const workers = 4
	runtimes := make([]setStatementRuntime, workers)
	statements := make([][2]*Statement, workers)
	for worker := 0; worker < workers; worker++ {
		left, right := prepareSetStatementLeaves(t)
		statements[worker] = [2]*Statement{left, right}
		descriptor := prepareBinarySetStatement(
			t, left, right, SetTreeUnionDistinct, 0, 1, 2,
		)
		if err := runtimes[worker].prepare(descriptor); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errorsOut := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var parent Exec
			args := []any{int64(1), int64(4)}
			for run := 0; run < 25; run++ {
				var frame statementFrame
				if err := frame.begin(parent.Options); err != nil {
					errorsOut <- err
					return
				}
				frame.args = args
				cursor, err := runtimes[worker].runIntoFrame(
					&parent,
					FromDatabase(catalog, statements[worker][0].Collection()),
					args, &frame,
				)
				if err != nil {
					errorsOut <- err
					return
				}
				rows := 0
				for cursor.Next() {
					rows++
				}
				if rows != 4 || frame.intermediate.used != 0 {
					errorsOut <- fmt.Errorf("rows=%d bytes=%d", rows, frame.intermediate.used)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errorsOut)
	for err := range errorsOut {
		t.Error(err)
	}
	for worker := range workers {
		runtimes[worker].Release()
		statements[worker][0].Release()
		statements[worker][1].Release()
	}
}
