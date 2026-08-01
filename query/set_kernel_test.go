package query

import (
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/thesyncim/vibejson"
)

const setTestMissing = "<missing>"

func TestSetKernelMultisetSemanticsAndStableOrder(t *testing.T) {
	leftRows := [][]string{
		{`1`}, {`1.0`}, {`2`}, {`null`}, {setTestMissing},
		{`{"x":1}`}, {`{"x":1}`},
	}
	rightRows := [][]string{
		{`1e0`}, {`3`}, {`null`}, {`{"x":1}`},
	}
	left := buildSetTestSpool(t, leftRows)
	right := buildSetTestSpool(t, rightRows)
	tests := []struct {
		name string
		op   setOperation
		want [][]string
	}{
		{
			name: "union all", op: setUnionAll,
			want: append(slices.Clone(leftRows), rightRows...),
		},
		{
			name: "union distinct", op: setUnionDistinct,
			want: [][]string{{`1`}, {`2`}, {`null`}, {`{"x":1}`}, {`3`}},
		},
		{
			name: "intersect all", op: setIntersectAll,
			want: [][]string{{`1`}, {`null`}, {`{"x":1}`}},
		},
		{
			name: "intersect distinct", op: setIntersectDistinct,
			want: [][]string{{`1`}, {`null`}, {`{"x":1}`}},
		},
		{
			name: "except all", op: setExceptAll,
			want: [][]string{{`1.0`}, {`2`}, {setTestMissing}, {`{"x":1}`}},
		},
		{
			name: "except distinct", op: setExceptDistinct,
			want: [][]string{{`2`}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := runSetTest(t, test.op, &left, &right)
			assertSetRows(t, got, test.want)
		})
	}
}

func TestSetKernelNullAndMissingShareHashAndEqualityClass(t *testing.T) {
	explicitNull := buildSetTestSpool(t, [][]string{{`null`}})
	missing := buildSetTestSpool(t, [][]string{{setTestMissing}})
	seed := maphash.MakeSeed()
	nullHash, err := hashSetRow(seed, &explicitNull, 0, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	missingHash, err := hashSetRow(seed, &missing, 0, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if nullHash != missingHash {
		t.Fatalf("explicit null hash %x differs from missing hash %x",
			nullHash, missingHash)
	}
	equal, err := setRowsEqual(&explicitNull, 0, &missing, 0, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !equal {
		t.Fatal("explicit null and missing are not one duplicate class")
	}
	result := runSetTest(t, setUnionDistinct, &explicitNull, &missing)
	assertSetRows(t, result, [][]string{{`null`}})
}

func TestSetKernelOrdinalRowsExactValuesAndContainerSpellings(t *testing.T) {
	left := buildSetTestSpool(t, [][]string{
		{`1`, `"a"`},
		{`1.00`, `"a"`},
		{`1`, `"b"`},
		{`9007199254740992`, `{"x":1}`},
		{`9007199254740993`, `{ "x" : 1 }`},
		{`1e999999999999999999999`, `[1,2]`},
	})
	right := buildSetTestSpool(t, [][]string{
		{`1e0`, `"a"`},
		{`9007199254740992.0`, `{"x":1}`},
		{`1.0e999999999999999999999`, `[1,2]`},
	})
	result := runSetTest(t, setUnionDistinct, &left, &right)
	assertSetRows(t, result, [][]string{
		{`1`, `"a"`},
		{`1`, `"b"`},
		{`9007199254740992`, `{"x":1}`},
		{`9007199254740993`, `{ "x" : 1 }`},
		{`1e999999999999999999999`, `[1,2]`},
	})
}

func TestSetKernelZeroColumnRows(t *testing.T) {
	tests := []struct {
		op          setOperation
		left, right int
		want        int
	}{
		{setUnionAll, 3, 2, 5},
		{setUnionDistinct, 3, 2, 1},
		{setIntersectAll, 3, 2, 2},
		{setIntersectDistinct, 3, 2, 1},
		{setExceptAll, 3, 2, 1},
		{setExceptDistinct, 3, 2, 0},
		{setExceptAll, 3, 0, 3},
		{setExceptDistinct, 3, 0, 1},
		{setIntersectAll, 3, 0, 0},
	}
	for _, test := range tests {
		left := relationSpool{rows: test.left}
		right := relationSpool{rows: test.right}
		got := runSetTest(t, test.op, &left, &right)
		if got.rows != test.want || len(got.columns) != 0 {
			t.Fatalf("op %d over %d/%d zero-column rows = %dx%d, want %dx0",
				test.op, test.left, test.right, got.rows, len(got.columns),
				test.want)
		}
	}
}

func TestSetKernelArityAndMalformedInputsPublishNothing(t *testing.T) {
	one := buildSetTestSpool(t, [][]string{{`1`}})
	two := buildSetTestSpool(t, [][]string{{`1`, `2`}})
	var executor setExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	charge, err := executor.execute(setUnionAll, &one, &one, &frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	frame.intermediate.release(charge)

	if _, err := executor.execute(setUnionAll, &one, &two, &frame, nil); !errors.Is(err, errSetArity) {
		t.Fatalf("arity error = %v, want %v", err, errSetArity)
	} else {
		var arity *setArityError
		if !errors.As(err, &arity) || arity.Left != 1 || arity.Right != 2 {
			t.Fatalf("typed arity = %#v", arity)
		}
	}
	if executor.result.rows != 0 {
		t.Fatalf("arity failure retained %d stale rows", executor.result.rows)
	}

	malformed := buildSetTestSpool(t, [][]string{{`1`}, {`2`}})
	malformed.columns[0] = malformed.columns[0][:1]
	if _, err := executor.execute(
		setUnionAll, &one, &malformed, &frame, nil,
	); !errors.Is(err, errSetInput) {
		t.Fatalf("malformed input error = %v, want %v", err, errSetInput)
	}
	if _, err := executor.execute(
		setOperation(255), &one, &one, &frame, nil,
	); !errors.Is(err, errSetMode) {
		t.Fatalf("invalid mode error = %v, want %v", err, errSetMode)
	}
	if _, err := executor.execute(
		setUnionAll, nil, &one, &frame, nil,
	); !errors.Is(err, errSetInput) {
		t.Fatalf("nil input error = %v, want %v", err, errSetInput)
	}
}

func TestSetKernelAliasRejectionPreservesAliasedResult(t *testing.T) {
	left := buildSetTestSpool(t, [][]string{{`1`, `"alpha"`}, {`2`, `"beta"`}})
	right := buildSetTestSpool(t, [][]string{{`3`, `"gamma"`}})
	var executor setExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	charge, err := executor.execute(setUnionAll, &left, &right, &frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	frame.intermediate.release(charge)
	want := setTestRows(&executor.result)
	wantCapacities := setExecutorCapacities(&executor)
	wantPlanned := executor.result.plannedData

	for _, inputs := range [][2]*relationSpool{
		{&executor.result, &right},
		{&left, &executor.result},
	} {
		if _, err := executor.execute(
			setUnionAll, inputs[0], inputs[1], &frame, nil,
		); !errors.Is(err, errSetAlias) {
			t.Fatalf("alias error = %v, want %v", err, errSetAlias)
		}
		if got := setTestRows(&executor.result); !equalSetTestRows(got, want) {
			t.Fatalf("alias rejection mutated result: got %v, want %v", got, want)
		}
		if got := setExecutorCapacities(&executor); got != wantCapacities {
			t.Fatalf("alias rejection changed capacities: got %+v, want %+v",
				got, wantCapacities)
		}
		if executor.result.plannedData != wantPlanned || frame.intermediate.used != 0 {
			t.Fatalf("alias rejection changed planned data/budget: got %d/%d, want %d/0",
				executor.result.plannedData, frame.intermediate.used, wantPlanned)
		}
	}
}

func TestSetKernelNonUnionBudgetExcludesRightPayload(t *testing.T) {
	left := buildSetTestSpool(t, [][]string{{`1`, `"small"`}})
	right := buildSetTestSpool(t, [][]string{
		{`2`, fmt.Sprintf("%q", strings.Repeat("r", 64<<10))},
	})
	for _, op := range []setOperation{
		setIntersectAll, setIntersectDistinct, setExceptAll, setExceptDistinct,
	} {
		t.Run(fmt.Sprintf("op=%d", op), func(t *testing.T) {
			shape, err := measureSetExecution(op, &left, &right, len(left.columns))
			if err != nil {
				t.Fatal(err)
			}
			wantOutputCharge := relationSpoolRetainedBytes(
				shape.maxOutput, len(left.columns), int64(len(left.data)),
			)
			if shape.outputCharge != wantOutputCharge {
				t.Fatalf("output bound = %d, want left-only %d",
					shape.outputCharge, wantOutputCharge)
			}
			var frame statementFrame
			if err := frame.begin(ExecOptions{IntermediateBytes: shape.totalCharge}); err != nil {
				t.Fatal(err)
			}
			var executor setExecutor
			charge, err := executor.execute(op, &left, &right, &frame, nil)
			if err != nil {
				t.Fatalf("left-only tight budget rejected: %v", err)
			}
			frame.intermediate.release(charge)
		})
	}
}

func TestSetKernelUnionBudgetIncludesBothPayloads(t *testing.T) {
	left := buildSetTestSpool(t, [][]string{{`1`, `"left"`}})
	right := buildSetTestSpool(t, [][]string{{`2`, `"right"`}})
	for _, op := range []setOperation{setUnionAll, setUnionDistinct} {
		shape, err := measureSetExecution(op, &left, &right, len(left.columns))
		if err != nil {
			t.Fatal(err)
		}
		want := relationSpoolRetainedBytes(
			left.rows+right.rows,
			len(left.columns),
			int64(len(left.data)+len(right.data)),
		)
		if shape.outputCharge != want {
			t.Fatalf("op %d output bound = %d, want two-input %d",
				op, shape.outputCharge, want)
		}
	}
}

func TestSetKernelIntermediateBudgetFailsBeforeGrowthAndIsShared(t *testing.T) {
	left := buildSetTestSpool(t, [][]string{
		{`1`, `"alpha"`}, {`2`, `"beta"`}, {`3`, `"gamma"`},
	})
	right := buildSetTestSpool(t, [][]string{
		{`2.0`, `"beta"`}, {`4`, `"delta"`},
	})
	shape, err := measureSetExecution(
		setUnionDistinct, &left, &right, len(left.columns),
	)
	if err != nil {
		t.Fatal(err)
	}
	var executor setExecutor
	before := setExecutorCapacities(&executor)
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: shape.totalCharge - 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.execute(
		setUnionDistinct, &left, &right, &frame, nil,
	); !errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("budget error = %v, want %v", err, ErrIntermediateBudget)
	}
	if after := setExecutorCapacities(&executor); after != before {
		t.Fatalf("failed admission grew executor from %+v to %+v", before, after)
	}
	if executor.result.rows != 0 || frame.intermediate.used != 0 {
		t.Fatalf("failed admission published rows/bytes = %d/%d",
			executor.result.rows, frame.intermediate.used)
	}

	if err := frame.begin(ExecOptions{IntermediateBytes: shape.totalCharge}); err != nil {
		t.Fatal(err)
	}
	if err := frame.intermediate.reserve("earlier relation", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.execute(
		setUnionDistinct, &left, &right, &frame, nil,
	); !errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("shared budget error = %v, want %v", err, ErrIntermediateBudget)
	}
	if frame.intermediate.used != 1 {
		t.Fatalf("set failure changed earlier reservation to %d", frame.intermediate.used)
	}
	frame.intermediate.release(1)

	if err := frame.begin(ExecOptions{IntermediateBytes: shape.totalCharge}); err != nil {
		t.Fatal(err)
	}
	charge, err := executor.execute(
		setUnionDistinct, &left, &right, &frame, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if frame.intermediate.used != charge || charge > shape.outputCharge {
		t.Fatalf("successful retained charge = frame %d return %d bound %d",
			frame.intermediate.used, charge, shape.outputCharge)
	}
	frame.intermediate.release(charge)
}

func TestSetKernelOverflowGuards(t *testing.T) {
	left := relationSpool{rows: math.MaxInt}
	right := relationSpool{rows: 1}
	if _, err := measureSetExecution(setUnionAll, &left, &right, 0); !errors.Is(err, errSetSize) {
		t.Fatalf("row overflow = %v, want %v", err, errSetSize)
	}
	if _, err := setTableCapacity(math.MaxInt); !errors.Is(err, errSetSize) {
		t.Fatalf("table overflow = %v, want %v", err, errSetSize)
	}
	if got := setWorkspaceRetainedBytes(math.MaxInt, math.MaxInt, math.MaxInt); got != math.MaxInt64 {
		t.Fatalf("workspace overflow = %d, want MaxInt64", got)
	}
}

func TestSetKernelCancellationPublishesNothingAndRecovers(t *testing.T) {
	left := buildSetTestSpool(t, [][]string{{`1`}, {`2`}, {`3`}})
	right := buildSetTestSpool(t, [][]string{{`2`}, {`4`}})
	var executor setExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	var cancel CancelFlag
	cancel.Cancel()
	before := setExecutorCapacities(&executor)
	if _, err := executor.execute(
		setUnionDistinct, &left, &right, &frame, &cancel,
	); !errors.Is(err, ErrCanceled) {
		t.Fatalf("cancellation error = %v, want %v", err, ErrCanceled)
	}
	if executor.result.rows != 0 || frame.intermediate.used != 0 {
		t.Fatalf("cancellation published rows/bytes = %d/%d",
			executor.result.rows, frame.intermediate.used)
	}
	if after := setExecutorCapacities(&executor); after != before {
		t.Fatalf("pre-cancellation grew executor from %+v to %+v", before, after)
	}
	cancel.Reset()
	charge, err := executor.execute(
		setUnionDistinct, &left, &right, &frame, &cancel,
	)
	if err != nil {
		t.Fatalf("reuse after cancellation: %v", err)
	}
	frame.intermediate.release(charge)
	assertSetRows(t, &executor.result, [][]string{{`1`}, {`2`}, {`3`}, {`4`}})
}

func TestSetKernelPreparedReuseIsAllocationFree(t *testing.T) {
	left := buildSetTestSpool(t, [][]string{
		{`1`, `"a"`}, {`1.0`, `"a"`}, {`2`, `"b"`},
		{`null`, `{"x":1}`}, {setTestMissing, `{"x":1}`},
	})
	right := buildSetTestSpool(t, [][]string{
		{`1e0`, `"a"`}, {`3`, `"c"`}, {`null`, `{"x":1}`},
	})
	for op := setUnionAll; op <= setExceptDistinct; op++ {
		t.Run(fmt.Sprintf("op=%d", op), func(t *testing.T) {
			var executor setExecutor
			run := func() {
				var frame statementFrame
				if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
					panic(err)
				}
				charge, err := executor.execute(op, &left, &right, &frame, nil)
				if err != nil {
					panic(err)
				}
				setSink += executor.result.rows
				frame.intermediate.release(charge)
			}
			run()
			run()
			if allocations := testing.AllocsPerRun(500, run); allocations != 0 {
				t.Fatalf("warmed set execution allocated %.2f times, want 0",
					allocations)
			}
		})
	}
}

func TestSetKernelReleaseDropsHighWaterStorage(t *testing.T) {
	left := buildSetTestSpool(t, [][]string{{`1`}, {`2`}, {`3`}})
	right := buildSetTestSpool(t, [][]string{{`2`}, {`4`}})
	var executor setExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	charge, err := executor.execute(
		setUnionDistinct, &left, &right, &frame, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	frame.intermediate.release(charge)
	if capacities := setExecutorCapacities(&executor); capacities == (setTestCapacities{}) {
		t.Fatal("test did not establish retained high-water storage")
	}
	executor.release()
	executor.release()
	if capacities := setExecutorCapacities(&executor); capacities != (setTestCapacities{}) {
		t.Fatalf("Release retained capacities: %+v", capacities)
	}
	if executor.relation() == nil || executor.relation().rows != 0 {
		t.Fatal("released executor is not a reusable empty relation")
	}
	charge, err = executor.execute(setUnionAll, &left, &right, &frame, nil)
	if err != nil {
		t.Fatalf("execute after Release: %v", err)
	}
	frame.intermediate.release(charge)
}

func TestSetKernelDifferentialRandomMultisets(t *testing.T) {
	random := rand.New(rand.NewSource(0x5e7))
	values := []string{
		setTestMissing, `null`, `false`, `true`, `0`, `-0.0`, `1`, `1e0`,
		`9007199254740992`, `9007199254740993`, `"a"`, `"a\n"`,
		`[]`, `[1]`, `{"x":1}`, `{ "x" : 1 }`,
	}
	for iteration := range 500 {
		columns := 1 + random.Intn(3)
		leftRows := randomSetRows(random, values, random.Intn(18), columns)
		rightRows := randomSetRows(random, values, random.Intn(18), columns)
		left := buildSetTestSpoolColumns(t, leftRows, columns)
		right := buildSetTestSpoolColumns(t, rightRows, columns)
		for op := setUnionAll; op <= setExceptDistinct; op++ {
			var executor setExecutor
			var frame statementFrame
			if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
				t.Fatal(err)
			}
			charge, err := executor.execute(op, &left, &right, &frame, nil)
			if err != nil {
				t.Fatalf("iteration %d op %d: %v", iteration, op, err)
			}
			frame.intermediate.release(charge)
			wantRefs := referenceSetRows(op, &left, &right)
			want := rowsForSetRefs(wantRefs, &left, &right)
			if got := setTestRows(&executor.result); !equalSetTestRows(got, want) {
				t.Fatalf("iteration %d op %d\nleft=%v\nright=%v\ngot=%v\nwant=%v",
					iteration, op, leftRows, rightRows, got, want)
			}
		}
	}
}

func TestSetKernelRaceSafeIndependentExecutorsAndCancellation(t *testing.T) {
	rows := make([][]string, 4096)
	for row := range rows {
		rows[row] = []string{
			fmt.Sprintf("%d", row%97), fmt.Sprintf("%q", fmt.Sprintf("g%d", row%31)),
		}
	}
	left := buildSetTestSpool(t, rows)
	right := buildSetTestSpool(t, rows[:2048])
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			var executor setExecutor
			for range 10 {
				var frame statementFrame
				if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
					t.Error(err)
					return
				}
				charge, err := executor.execute(
					setIntersectAll, &left, &right, &frame, nil,
				)
				if err != nil {
					t.Error(err)
					return
				}
				frame.intermediate.release(charge)
			}
		})
	}
	workers.Wait()

	var executor setExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	var cancel CancelFlag
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		charge, err := executor.execute(
			setUnionDistinct, &left, &right, &frame, &cancel,
		)
		if err == nil {
			frame.intermediate.release(charge)
		}
		done <- err
	}()
	<-started
	cancel.Cancel()
	err := <-done
	if err != nil && !errors.Is(err, ErrCanceled) {
		t.Fatalf("concurrent cancellation = %v", err)
	}
	if errors.Is(err, ErrCanceled) &&
		(executor.result.rows != 0 || frame.intermediate.used != 0) {
		t.Fatalf("canceled execution published rows/bytes = %d/%d",
			executor.result.rows, frame.intermediate.used)
	}
	cancel.Reset()
	charge, err := executor.execute(setUnionAll, &left, &right, &frame, &cancel)
	if err != nil {
		t.Fatalf("reuse after concurrent cancellation: %v", err)
	}
	frame.intermediate.release(charge)
}

var setSink int

type setTestCapacities struct {
	slots, entries, selected int
	columns, data            int
}

func setExecutorCapacities(e *setExecutor) setTestCapacities {
	return setTestCapacities{
		slots: cap(e.workspace.slots), entries: cap(e.workspace.entries),
		selected: cap(e.workspace.selected), columns: cap(e.result.columns),
		data: cap(e.result.data),
	}
}

func runSetTest(
	t testing.TB,
	op setOperation,
	left, right *relationSpool,
) *relationSpool {
	t.Helper()
	var executor setExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	charge, err := executor.execute(op, left, right, &frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	frame.intermediate.release(charge)
	return &executor.result
}

func buildSetTestSpool(t testing.TB, rows [][]string) relationSpool {
	t.Helper()
	columns := 0
	if len(rows) != 0 {
		columns = len(rows[0])
	}
	return buildSetTestSpoolColumns(t, rows, columns)
}

func buildSetTestSpoolColumns(
	t testing.TB,
	rows [][]string,
	columns int,
) relationSpool {
	t.Helper()
	payload := int64(0)
	for row := range rows {
		if len(rows[row]) != columns {
			t.Fatalf("row %d has %d columns, want %d", row, len(rows[row]), columns)
		}
		for column := range rows[row] {
			payload = saturatedBytes(
				payload,
				int64(relationCellOwnedBytes(setTestCell(rows[row][column]))),
			)
		}
	}
	var spool relationSpool
	if err := spool.begin(len(rows), columns, payload); err != nil {
		t.Fatal(err)
	}
	for row := range rows {
		for column := range rows[row] {
			owned, err := spool.ownCell(setTestCell(rows[row][column]), nil)
			if err != nil {
				t.Fatal(err)
			}
			spool.columns[column][row] = owned
		}
	}
	if len(spool.data) != spool.plannedData {
		t.Fatalf("test spool data = %d, planned %d", len(spool.data), spool.plannedData)
	}
	return spool
}

func setTestCell(src string) Cell {
	if src == setTestMissing {
		return Cell{kind: TypeNull, flag: cellMissing, raw: nullBytes}
	}
	var decoded []byte
	return cellFromScalar(classifyRawInto(
		vibejson.RawValue{Src: []byte(src)}, &decoded,
	))
}

func setTestRows(spool *relationSpool) [][]string {
	rows := make([][]string, spool.rows)
	for row := 0; row < spool.rows; row++ {
		rows[row] = make([]string, len(spool.columns))
		for column := range spool.columns {
			value := spool.columns[column][row]
			if value.kind == kindNull && value.raw == nil {
				rows[row][column] = setTestMissing
				continue
			}
			rows[row][column] = string(value.raw)
		}
	}
	return rows
}

func assertSetRows(t testing.TB, spool *relationSpool, want [][]string) {
	t.Helper()
	got := setTestRows(spool)
	if !equalSetTestRows(got, want) {
		t.Fatalf("set rows = %v, want %v", got, want)
	}
}

func equalSetTestRows(left, right [][]string) bool {
	if len(left) != len(right) {
		return false
	}
	for row := range left {
		if !slices.Equal(left[row], right[row]) {
			return false
		}
	}
	return true
}

func randomSetRows(
	random *rand.Rand,
	values []string,
	rows, columns int,
) [][]string {
	result := make([][]string, rows)
	for row := range result {
		result[row] = make([]string, columns)
		for column := range result[row] {
			result[row][column] = values[random.Intn(len(values))]
		}
	}
	return result
}

func referenceSetRows(
	op setOperation,
	left, right *relationSpool,
) []setRowRef {
	key := func(relation *relationSpool, row int) string {
		var encoded []byte
		for column := range relation.columns {
			encoded = appendGroupKey(encoded, relation.columns[column][row])
		}
		return string(encoded)
	}
	selected := make([]setRowRef, 0, left.rows+right.rows)
	switch op {
	case setUnionAll:
		for row := 0; row < left.rows; row++ {
			selected = append(selected, setRowRef{row: row, side: setRowLeft})
		}
		for row := 0; row < right.rows; row++ {
			selected = append(selected, setRowRef{row: row, side: setRowRight})
		}
	case setUnionDistinct:
		seen := make(map[string]struct{})
		for _, side := range []setRowSide{setRowLeft, setRowRight} {
			relation := left
			if side == setRowRight {
				relation = right
			}
			for row := 0; row < relation.rows; row++ {
				encoded := key(relation, row)
				if _, ok := seen[encoded]; ok {
					continue
				}
				seen[encoded] = struct{}{}
				selected = append(selected, setRowRef{row: row, side: side})
			}
		}
	case setIntersectAll, setIntersectDistinct:
		counts := make(map[string]int)
		for row := 0; row < right.rows; row++ {
			counts[key(right, row)]++
		}
		emitted := make(map[string]bool)
		for row := 0; row < left.rows; row++ {
			encoded := key(left, row)
			if op == setIntersectDistinct {
				if counts[encoded] != 0 && !emitted[encoded] {
					emitted[encoded] = true
					selected = append(selected, setRowRef{row: row, side: setRowLeft})
				}
				continue
			}
			if counts[encoded] != 0 {
				counts[encoded]--
				selected = append(selected, setRowRef{row: row, side: setRowLeft})
			}
		}
	case setExceptAll, setExceptDistinct:
		counts := make(map[string]int)
		for row := 0; row < right.rows; row++ {
			counts[key(right, row)]++
		}
		emitted := make(map[string]bool)
		for row := 0; row < left.rows; row++ {
			encoded := key(left, row)
			if op == setExceptDistinct {
				if counts[encoded] == 0 && !emitted[encoded] {
					emitted[encoded] = true
					selected = append(selected, setRowRef{row: row, side: setRowLeft})
				}
				continue
			}
			if counts[encoded] != 0 {
				counts[encoded]--
				continue
			}
			selected = append(selected, setRowRef{row: row, side: setRowLeft})
		}
	}
	return selected
}

func rowsForSetRefs(
	refs []setRowRef,
	left, right *relationSpool,
) [][]string {
	rows := make([][]string, len(refs))
	for at, ref := range refs {
		relation := left
		if ref.side == setRowRight {
			relation = right
		}
		row := setTestRowsAt(relation, ref.row)
		rows[at] = row
	}
	return rows
}

func setTestRowsAt(spool *relationSpool, row int) []string {
	result := make([]string, len(spool.columns))
	for column := range spool.columns {
		value := spool.columns[column][row]
		if value.kind == kindNull && value.raw == nil {
			result[column] = setTestMissing
		} else {
			result[column] = string(value.raw)
		}
	}
	return result
}
