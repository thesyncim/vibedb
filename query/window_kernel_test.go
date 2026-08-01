package query

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sync"
	"testing"
)

func TestWindowKernelRankingOffsetsStableOrderAndPartitions(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`"b"`, `2`, `"x"`, `20`},
		{`"a"`, `1`, `"z"`, `10`},
		{`"a"`, `1.0`, `"z"`, `15`},
		{`"a"`, `null`, `"n"`, `5`},
		{`"b"`, `1`, `"x"`, `null`},
		{`"a"`, `2`, `"a"`, `25`},
		{`"a"`, `2.00`, `"b"`, `30`},
	})
	defaultValue := windowTestScalar(t, `-1`)
	plan := windowPlan{
		partition: []int{0},
		order: []windowOrderKey{
			{column: 1, nulls: windowNullsLast},
			{column: 2, descending: true, nulls: windowNullsFirst},
		},
		functions: []windowFunctionSpec{
			{kind: windowRowNumber, column: -1},
			{kind: windowRank, column: -1},
			{kind: windowDenseRank, column: -1},
			{kind: windowLag, column: 3, offset: 1, defaultVal: defaultValue, hasDefault: true},
			{kind: windowLead, column: 3, offset: 1, defaultVal: defaultValue, hasDefault: true},
		},
	}
	result := runWindowTest(t, &input, &plan)
	assertSetRows(t, result, [][]string{
		{`2`, `2`, `2`, `null`, `-1`},
		{`1`, `1`, `1`, `-1`, `15`},
		{`2`, `1`, `1`, `10`, `30`},
		{`5`, `5`, `4`, `25`, `-1`},
		{`1`, `1`, `1`, `-1`, `20`},
		{`4`, `4`, `3`, `30`, `5`},
		{`3`, `3`, `2`, `15`, `25`},
	})
}

func TestWindowKernelExplicitNullOrderingAndStablePeers(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{setTestMissing, `10`},
		{`null`, `20`},
		{`1`, `30`},
	})
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, descending: true, nulls: windowNullsLast}},
		functions: []windowFunctionSpec{
			{kind: windowRowNumber, column: -1},
			{kind: windowRank, column: -1},
			{kind: windowDenseRank, column: -1},
			{kind: windowLag, column: 1, offset: 1},
		},
	}
	result := runWindowTest(t, &input, &plan)
	assertSetRows(t, result, [][]string{
		{`2`, `2`, `2`, `30`},
		{`3`, `2`, `2`, `10`},
		{`1`, `1`, `1`, `null`},
	})
}

func TestWindowKernelSlidingRowsAggregates(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`"p"`, `1`, `1`},
		{`"p"`, `2`, `2.00`},
		{`"p"`, `3`, `null`},
		{`"p"`, `4`, `4`},
		{`"p"`, `5`, `8`},
	})
	frame := windowRowsFrame{
		start: windowFrameBound{kind: windowPreceding, offset: 1},
		end:   windowFrameBound{kind: windowFollowing, offset: 1},
	}
	plan := windowPlan{
		partition: []int{0},
		order:     []windowOrderKey{{column: 1, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowCount, column: -1, frame: frame},
			{kind: windowCount, column: 2, frame: frame},
			{kind: windowSum, column: 2, frame: frame},
			{kind: windowAvg, column: 2, frame: frame},
			{kind: windowMin, column: 2, frame: frame},
			{kind: windowMax, column: 2, frame: frame},
		},
	}
	result := runWindowTest(t, &input, &plan)
	assertSetRows(t, result, [][]string{
		{`2`, `2`, `3`, `1.5`, `1`, `2.00`},
		{`3`, `2`, `3`, `1.5`, `1`, `2.00`},
		{`3`, `2`, `6`, `3`, `2.00`, `4`},
		{`3`, `2`, `12`, `6`, `4`, `8`},
		{`2`, `2`, `12`, `6`, `4`, `8`},
	})
}

func TestWindowKernelUnboundedFramesExactDecimalsAndEmptyFrame(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`1`, `9007199254740992`},
		{`2`, `2.0`},
	})
	full := windowRowsFrame{
		start: windowFrameBound{kind: windowUnboundedPreceding},
		end:   windowFrameBound{kind: windowUnboundedFollowing},
	}
	future := windowRowsFrame{
		start: windowFrameBound{kind: windowFollowing, offset: 1},
		end:   windowFrameBound{kind: windowUnboundedFollowing},
	}
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowSum, column: 1, frame: full},
			{kind: windowAvg, column: 1, frame: full},
			{kind: windowMin, column: 1, frame: full},
			{kind: windowMax, column: 1, frame: full},
			{kind: windowSum, column: 1, frame: future},
			{kind: windowCount, column: -1, frame: future},
		},
	}
	result := runWindowTest(t, &input, &plan)
	assertSetRows(t, result, [][]string{
		{`9007199254740994`, `4503599627370497`, `2.0`, `9007199254740992`, `2`, `1`},
		{`9007199254740994`, `4503599627370497`, `2.0`, `9007199254740992`, `null`, `0`},
	})
}

func TestWindowKernelLagLeadPreserveMissingAndOwnDefault(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`1`, setTestMissing},
		{`2`, `"value"`},
	})
	defaultValue := windowTestScalar(t, `"fallback"`)
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowLag, column: 1, offset: 1, defaultVal: defaultValue, hasDefault: true},
			{kind: windowLead, column: 1, offset: 1, defaultVal: defaultValue, hasDefault: true},
		},
	}
	result := runWindowTest(t, &input, &plan)
	if result.columns[0][1].kind != kindNull || result.columns[0][1].raw != nil {
		t.Fatal("LAG did not preserve the engine's missing marker")
	}
	assertSetRows(t, result, [][]string{
		{`"fallback"`, `"value"`},
		{setTestMissing, `"fallback"`},
	})
}

func TestWindowKernelValidationAndFrames(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{{`1`}})
	validFrame := windowRowsFrame{
		start: windowFrameBound{kind: windowPreceding, offset: 1},
		end:   windowFrameBound{kind: windowCurrentRow},
	}
	tests := []windowPlan{
		{partition: []int{1}},
		{order: []windowOrderKey{{column: 0, nulls: windowNullOrder(9)}}},
		{functions: []windowFunctionSpec{{kind: windowFunctionKind(99), column: -1}}},
		{functions: []windowFunctionSpec{{kind: windowRowNumber, column: 0}}},
		{functions: []windowFunctionSpec{{kind: windowLag, column: 1}}},
		{functions: []windowFunctionSpec{{kind: windowSum, column: 0, frame: windowRowsFrame{
			start: windowFrameBound{kind: windowFollowing, offset: 2},
			end:   windowFrameBound{kind: windowFollowing, offset: 1},
		}}}},
		{functions: []windowFunctionSpec{{kind: windowCount, column: -1, offset: 1, frame: validFrame}}},
	}
	for at := range tests {
		var executor windowExecutor
		var frame statementFrame
		if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
			t.Fatal(err)
		}
		if _, err := executor.execute(&input, &tests[at], &frame, nil); !errors.Is(err, errWindowPlan) &&
			!errors.Is(err, errWindowFrame) {
			t.Fatalf("invalid plan %d error = %v", at, err)
		}
		if executor.result.rows != 0 || frame.intermediate.used != 0 {
			t.Fatalf("invalid plan %d published rows/bytes %d/%d",
				at, executor.result.rows, frame.intermediate.used)
		}
	}

	malformed := input
	malformed.columns[0] = nil
	plan := windowPlan{functions: []windowFunctionSpec{{kind: windowRowNumber, column: -1}}}
	var executor windowExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.execute(&malformed, &plan, &frame, nil); !errors.Is(err, errWindowInput) {
		t.Fatalf("malformed input error = %v", err)
	}
}

func TestWindowKernelAliasRejectionPreservesInput(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{{`2`}, {`1`}})
	plan := windowPlan{
		order:     []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{{kind: windowRowNumber, column: -1}},
	}
	var executor windowExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	charge, err := executor.execute(&input, &plan, &frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	frame.intermediate.release(charge)
	want := setTestRows(&executor.result)
	wantCaps := windowExecutorCapacities(&executor)
	if _, err := executor.execute(&executor.result, &plan, &frame, nil); !errors.Is(err, errWindowAlias) {
		t.Fatalf("alias error = %v", err)
	}
	if got := setTestRows(&executor.result); !equalSetTestRows(got, want) {
		t.Fatalf("alias rejection mutated input: got %v, want %v", got, want)
	}
	if got := windowExecutorCapacities(&executor); got != wantCaps {
		t.Fatalf("alias rejection changed high-water: got %+v, want %+v", got, wantCaps)
	}
}

func TestWindowKernelBudgetAdmissionPrecedesGrowth(t *testing.T) {
	input := buildWindowRows(t, 128)
	frameSpec := windowRowsFrame{
		start: windowFrameBound{kind: windowPreceding, offset: 4},
		end:   windowFrameBound{kind: windowFollowing, offset: 3},
	}
	plan := windowPlan{
		partition: []int{0},
		order:     []windowOrderKey{{column: 1, nulls: windowNullsLast}},
		functions: []windowFunctionSpec{
			{kind: windowSum, column: 2, frame: frameSpec},
			{kind: windowMin, column: 2, frame: frameSpec},
		},
	}
	shape, err := measureWindowExecution(&input, &plan, nil)
	if err != nil {
		t.Fatal(err)
	}
	var executor windowExecutor
	before := windowExecutorCapacities(&executor)
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: shape.workCharge - 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.execute(&input, &plan, &frame, nil); !errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("workspace budget error = %v, want %v", err, ErrIntermediateBudget)
	}
	if got := windowExecutorCapacities(&executor); got != before {
		t.Fatalf("failed workspace admission grew %+v to %+v", before, got)
	}
	if frame.intermediate.used != 0 || executor.result.rows != 0 {
		t.Fatalf("workspace failure retained rows/bytes %d/%d",
			executor.result.rows, frame.intermediate.used)
	}

	if err := frame.begin(ExecOptions{IntermediateBytes: shape.workCharge}); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.execute(&input, &plan, &frame, nil); !errors.Is(err, ErrIntermediateBudget) {
		t.Fatalf("result budget error = %v, want %v", err, ErrIntermediateBudget)
	}
	if cap(executor.result.columns) != 0 || cap(executor.result.data) != 0 {
		t.Fatal("failed result admission grew result storage")
	}
	if frame.intermediate.used != 0 {
		t.Fatalf("result failure retained %d budget bytes", frame.intermediate.used)
	}
}

func TestWindowKernelCancellationNoPartialAndReuse(t *testing.T) {
	input := buildWindowRows(t, 4096)
	plan := windowStressPlan()
	var executor windowExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	var cancel CancelFlag
	cancel.Cancel()
	before := windowExecutorCapacities(&executor)
	if _, err := executor.execute(&input, &plan, &frame, &cancel); !errors.Is(err, ErrCanceled) {
		t.Fatalf("pre-cancel error = %v", err)
	}
	if got := windowExecutorCapacities(&executor); got != before {
		t.Fatalf("pre-cancel grew %+v to %+v", before, got)
	}
	cancel.Reset()
	charge, err := executor.execute(&input, &plan, &frame, &cancel)
	if err != nil {
		t.Fatalf("reuse after cancellation: %v", err)
	}
	frame.intermediate.release(charge)
}

func TestWindowKernelPreparedReuseZeroAllocAndRelease(t *testing.T) {
	input := buildWindowRows(t, 256)
	plan := windowStressPlan()
	var executor windowExecutor
	run := func() {
		var frame statementFrame
		if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
			panic(err)
		}
		charge, err := executor.execute(&input, &plan, &frame, nil)
		if err != nil {
			panic(err)
		}
		windowSink += executor.result.rows
		frame.intermediate.release(charge)
	}
	run()
	run()
	if allocations := testing.AllocsPerRun(300, run); allocations != 0 {
		t.Fatalf("warmed window execution allocated %.2f times, want 0", allocations)
	}
	if windowExecutorCapacities(&executor) == (windowTestCapacities{}) {
		t.Fatal("test did not establish high-water storage")
	}
	executor.release()
	executor.release()
	if got := windowExecutorCapacities(&executor); got != (windowTestCapacities{}) {
		t.Fatalf("Release retained high-water: %+v", got)
	}
	run()
}

func TestWindowKernelWideDecimalAggregation(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{
		{`1`, `123456789012345678901234567890.125`},
		{`2`, `-99999999999999999999999999999.875`},
	})
	frameSpec := windowRowsFrame{
		start: windowFrameBound{kind: windowUnboundedPreceding},
		end:   windowFrameBound{kind: windowUnboundedFollowing},
	}
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowSum, column: 1, frame: frameSpec},
			{kind: windowAvg, column: 1, frame: frameSpec},
		},
	}
	result := runWindowTest(t, &input, &plan)
	assertSetRows(t, result, [][]string{
		{`23456789012345678901234567890.25`, `11728394506172839450617283945.125`},
		{`23456789012345678901234567890.25`, `11728394506172839450617283945.125`},
	})
}

func TestWindowKernelDecimalOutputNormalization(t *testing.T) {
	input := buildSetTestSpool(t, [][]string{{`1`, `1.2`}, {`2`, `1.8`}})
	frameSpec := windowRowsFrame{
		start: windowFrameBound{kind: windowUnboundedPreceding},
		end:   windowFrameBound{kind: windowUnboundedFollowing},
	}
	plan := windowPlan{
		order: []windowOrderKey{{column: 0, nulls: windowNullsFirst}},
		functions: []windowFunctionSpec{
			{kind: windowSum, column: 1, frame: frameSpec},
			{kind: windowAvg, column: 1, frame: frameSpec},
		},
	}
	assertSetRows(t, runWindowTest(t, &input, &plan), [][]string{
		{`3`, `1.5`}, {`3`, `1.5`},
	})
}

func TestWindowKernelAverageFastPathExactAndTiesToEven(t *testing.T) {
	tests := []struct {
		coefficient int64
		scale       int64
		count       int64
		want        string
	}{
		{1, 0, 2, `0.5`},
		{1, 0, 3, `0.3333333333333333333333333333333333`},
		{2, 0, 3, `0.6666666666666666666666666666666667`},
		{-1, 0, 8, `-0.125`},
		{506612146037681, 9, 84, `6031096976639059523809.52380952381`},
		{1, 0, 562949953421312, `0.000000000000001776356839400250464677810668945312`},
		{3, 0, 281474976710656, `0.00000000000001065814103640150278806686401367188`},
	}
	for iteration, test := range tests {
		if test.count > int64(math.MaxInt) {
			continue
		}
		var executor windowExecutor
		executor.numberOut = make([]byte, 0, 512)
		executor.negative = make([]byte, 0, averageDigits+2)
		executor.aggregate.n = int(test.count)
		executor.aggregate.sum = decimalSum{
			set: true, smallCoeff: test.coefficient, smallScale: test.scale,
			digits: intDigits64(test.coefficient),
		}
		got, ok, err := executor.windowAverageCellFast()
		if err != nil || !ok {
			t.Fatalf("iteration %d fast average = ok %v err %v", iteration, ok, err)
		}
		if string(got.raw) != test.want {
			t.Fatalf("iteration %d (%d*10^%d)/%d = %s, want %s",
				iteration, test.coefficient, test.scale, test.count, got.raw, test.want)
		}
	}
}

func TestWindowKernelDifferentialRandomIntegerFrames(t *testing.T) {
	random := rand.New(rand.NewSource(0x71ad0))
	for iteration := range 300 {
		rows := 1 + random.Intn(24)
		data := make([][]string, rows)
		for row := range data {
			value := fmt.Sprintf("%d", random.Intn(21)-10)
			if random.Intn(5) == 0 {
				value = `null`
			}
			order := fmt.Sprintf("%d", random.Intn(7)-3)
			if random.Intn(9) == 0 {
				order = setTestMissing
			}
			data[row] = []string{fmt.Sprintf("%d", random.Intn(3)), order, value}
		}
		input := buildSetTestSpool(t, data)
		preceding, following := random.Intn(4), random.Intn(4)
		frameSpec := windowRowsFrame{
			start: windowFrameBound{kind: windowPreceding, offset: preceding},
			end:   windowFrameBound{kind: windowFollowing, offset: following},
		}
		plan := windowPlan{
			partition: []int{0},
			order: []windowOrderKey{{
				column: 1, descending: random.Intn(2) == 0,
				nulls: windowNullOrder(random.Intn(2)),
			}},
			functions: []windowFunctionSpec{
				{kind: windowRowNumber, column: -1},
				{kind: windowRank, column: -1},
				{kind: windowDenseRank, column: -1},
				{kind: windowLag, column: 2, offset: random.Intn(4)},
				{kind: windowLead, column: 2, offset: random.Intn(4)},
				{kind: windowCount, column: -1, frame: frameSpec},
				{kind: windowCount, column: 2, frame: frameSpec},
				{kind: windowSum, column: 2, frame: frameSpec},
				{kind: windowMin, column: 2, frame: frameSpec},
				{kind: windowMax, column: 2, frame: frameSpec},
			},
		}
		result := runWindowTest(t, &input, &plan)
		want := referenceWindowIntegerRows(t, &input, &plan)
		if got := setTestRows(result); !equalSetTestRows(got, want) {
			t.Fatalf("iteration %d\ndata=%v\ngot=%v\nwant=%v", iteration, data, got, want)
		}
	}
}

func TestWindowKernelRaceIndependentExecutorsAndConcurrentCancellation(t *testing.T) {
	input := buildWindowRows(t, 2048)
	plan := windowStressPlan()
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			var executor windowExecutor
			for range 8 {
				var frame statementFrame
				if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
					t.Error(err)
					return
				}
				charge, err := executor.execute(&input, &plan, &frame, nil)
				if err != nil {
					t.Error(err)
					return
				}
				frame.intermediate.release(charge)
			}
		})
	}
	workers.Wait()

	var executor windowExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	var cancel CancelFlag
	done := make(chan error, 1)
	go func() {
		charge, err := executor.execute(&input, &plan, &frame, &cancel)
		if err == nil {
			frame.intermediate.release(charge)
		}
		done <- err
	}()
	cancel.Cancel()
	err := <-done
	if err != nil && !errors.Is(err, ErrCanceled) {
		t.Fatalf("concurrent cancellation error = %v", err)
	}
	if errors.Is(err, ErrCanceled) &&
		(executor.result.rows != 0 || frame.intermediate.used != 0) {
		t.Fatalf("canceled execution retained rows/bytes %d/%d",
			executor.result.rows, frame.intermediate.used)
	}
}

func TestWindowKernelOverflowGuards(t *testing.T) {
	input := relationSpool{rows: math.MaxInt}
	plan := windowPlan{functions: []windowFunctionSpec{{kind: windowRowNumber, column: -1}}}
	if _, err := measureWindowExecution(&input, &plan, nil); !errors.Is(err, errWindowSize) {
		t.Fatalf("row workspace overflow = %v, want %v", err, errWindowSize)
	}
	invalidFrames := []windowRowsFrame{
		{start: windowFrameBound{kind: windowUnboundedFollowing}, end: windowFrameBound{kind: windowUnboundedFollowing}},
		{start: windowFrameBound{kind: windowCurrentRow}, end: windowFrameBound{kind: windowUnboundedPreceding}},
		{start: windowFrameBound{kind: windowCurrentRow, offset: 1}, end: windowFrameBound{kind: windowUnboundedFollowing}},
	}
	for _, frame := range invalidFrames {
		if err := validateWindowFrame(frame); !errors.Is(err, errWindowFrame) {
			t.Fatalf("invalid frame %+v error = %v", frame, err)
		}
	}
}

var windowSink int

type windowTestCapacities struct {
	order, scratch, deque int
	number, negative      int
	columns, data         int
}

func windowExecutorCapacities(e *windowExecutor) windowTestCapacities {
	return windowTestCapacities{
		order: cap(e.order), scratch: cap(e.sortScratch), deque: cap(e.deque),
		number: cap(e.numberOut), negative: cap(e.negative),
		columns: cap(e.result.columns), data: cap(e.result.data),
	}
}

func runWindowTest(
	t testing.TB,
	input *relationSpool,
	plan *windowPlan,
) *relationSpool {
	t.Helper()
	var executor windowExecutor
	var frame statementFrame
	if err := frame.begin(ExecOptions{IntermediateBytes: -1}); err != nil {
		t.Fatal(err)
	}
	charge, err := executor.execute(input, plan, &frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	frame.intermediate.release(charge)
	return &executor.result
}

func windowTestScalar(t testing.TB, value string) scalar {
	t.Helper()
	spool := buildSetTestSpool(t, [][]string{{value}})
	return spool.columns[0][0]
}

func buildWindowRows(t testing.TB, rows int) relationSpool {
	t.Helper()
	data := make([][]string, rows)
	for row := range data {
		data[row] = []string{
			fmt.Sprintf("%d", row%7),
			fmt.Sprintf("%d.0", row%97),
			fmt.Sprintf("%d", row%31-15),
		}
	}
	return buildSetTestSpool(t, data)
}

func windowStressPlan() windowPlan {
	frame := windowRowsFrame{
		start: windowFrameBound{kind: windowPreceding, offset: 8},
		end:   windowFrameBound{kind: windowFollowing, offset: 4},
	}
	return windowPlan{
		partition: []int{0},
		order: []windowOrderKey{{
			column: 1, descending: true, nulls: windowNullsLast,
		}},
		functions: []windowFunctionSpec{
			{kind: windowRowNumber, column: -1},
			{kind: windowRank, column: -1},
			{kind: windowDenseRank, column: -1},
			{kind: windowLag, column: 2, offset: 3},
			{kind: windowLead, column: 2, offset: 2},
			{kind: windowCount, column: -1, frame: frame},
			{kind: windowCount, column: 2, frame: frame},
			{kind: windowSum, column: 2, frame: frame},
			{kind: windowAvg, column: 2, frame: frame},
			{kind: windowMin, column: 2, frame: frame},
			{kind: windowMax, column: 2, frame: frame},
		},
	}
}

func referenceWindowIntegerRows(
	t testing.TB,
	input *relationSpool,
	plan *windowPlan,
) [][]string {
	t.Helper()
	order := make([]int, input.rows)
	for row := range order {
		order[row] = row
	}
	slices.SortStableFunc(order, func(left, right int) int {
		comparison, err := compareWindowRows(input, plan, left, right, nil)
		if err != nil {
			panic(err)
		}
		return comparison
	})
	result := make([][]string, input.rows)
	for row := range result {
		result[row] = make([]string, len(plan.functions))
	}
	for partitionStart := 0; partitionStart < len(order); {
		partitionEnd := partitionStart + 1
		for partitionEnd < len(order) {
			equal, err := windowPartitionEqual(
				input, plan.partition, order[partitionStart], order[partitionEnd], nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !equal {
				break
			}
			partitionEnd++
		}
		for position := partitionStart; position < partitionEnd; position++ {
			row := order[position]
			local := position - partitionStart
			rows := partitionEnd - partitionStart
			for column, function := range plan.functions {
				switch function.kind {
				case windowRowNumber:
					result[row][column] = fmt.Sprintf("%d", local+1)
				case windowRank, windowDenseRank:
					rank, dense := 1, 1
					for at := 1; at <= local; at++ {
						peer, err := windowPeers(
							input, plan.order, order[partitionStart+at-1], order[partitionStart+at], nil,
						)
						if err != nil {
							t.Fatal(err)
						}
						if !peer {
							rank = at + 1
							dense++
						}
					}
					if function.kind == windowDenseRank {
						rank = dense
					}
					result[row][column] = fmt.Sprintf("%d", rank)
				case windowLag, windowLead:
					target, ok := windowOffsetPosition(
						local, rows, function.offset, function.kind == windowLead,
					)
					if !ok {
						result[row][column] = `null`
					} else {
						result[row][column] = windowScalarString(
							input.columns[function.column][order[partitionStart+target]],
						)
					}
				case windowCount, windowSum, windowMin, windowMax:
					lo, hi := resolveWindowFrame(function.frame, local, rows)
					count, sum := 0, int64(0)
					var extreme scalar
					for at := lo; at < hi; at++ {
						if function.kind == windowCount && function.column < 0 {
							count++
							continue
						}
						value := input.columns[function.column][order[partitionStart+at]]
						if value.kind == kindNull {
							continue
						}
						if function.kind == windowCount {
							count++
							continue
						}
						if value.kind != kindNumber || !value.isInt {
							continue
						}
						if count == 0 {
							extreme = value
						} else if function.kind == windowMin && compareScalar(value, extreme) < 0 ||
							function.kind == windowMax && compareScalar(value, extreme) > 0 {
							extreme = value
						}
						count++
						sum += value.ival
					}
					switch function.kind {
					case windowCount:
						result[row][column] = fmt.Sprintf("%d", count)
					case windowSum:
						if count == 0 {
							result[row][column] = `null`
						} else {
							result[row][column] = fmt.Sprintf("%d", sum)
						}
					default:
						if count == 0 {
							result[row][column] = `null`
						} else {
							result[row][column] = windowScalarString(extreme)
						}
					}
				}
			}
		}
		partitionStart = partitionEnd
	}
	return result
}

func windowScalarString(value scalar) string {
	if value.kind == kindNull && value.raw == nil {
		return setTestMissing
	}
	return string(value.raw)
}
