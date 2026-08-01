package query

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"runtime"
	"sync"
	"testing"

	"github.com/thesyncim/vibejson"
)

type applyTestSource struct {
	rows    [][]Cell
	columns int
}

func (s *applyTestSource) Rows() int { return len(s.rows) }

func (s *applyTestSource) Columns() int {
	if s.columns != 0 || len(s.rows) == 0 {
		return s.columns
	}
	return len(s.rows[0])
}

func (s *applyTestSource) Cell(row, column int) Cell { return s.rows[row][column] }

type applyTestProgram struct {
	bind  func(ApplyLeftRow, *ApplyParameterBinder) error
	right func(ApplyParameters, *ApplyRightAppender) error
}

func (p *applyTestProgram) Bind(
	left ApplyLeftRow,
	parameters *ApplyParameterBinder,
) error {
	if p.bind == nil {
		return nil
	}
	return p.bind(left, parameters)
}

func (p *applyTestProgram) Right(
	parameters ApplyParameters,
	rows *ApplyRightAppender,
) error {
	if p.right == nil {
		return nil
	}
	return p.right(parameters, rows)
}

func applyTestJSONCell(src string) Cell {
	if src == "<missing>" {
		return Cell{kind: TypeNull, flag: cellMissing, raw: nullBytes}
	}
	var decoded []byte
	return cellFromScalar(classifyRawInto(
		vibejson.RawValue{Src: []byte(src)}, &decoded,
	))
}

func applyTestInt(value int64) Cell {
	return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(value)}
}

func applyTestRows(rows ...[]string) *applyTestSource {
	source := &applyTestSource{rows: make([][]Cell, len(rows))}
	for row := range rows {
		source.rows[row] = make([]Cell, len(rows[row]))
		for column, value := range rows[row] {
			source.rows[row][column] = applyTestJSONCell(value)
		}
	}
	return source
}

func applyResultJSON(result ApplyResult) [][]string {
	rows := make([][]string, result.Rows())
	for row := range rows {
		rows[row] = make([]string, result.Columns())
		for column := range rows[row] {
			rows[row][column] = string(result.Cell(row, column).JSON())
		}
	}
	return rows
}

func TestApplyKernelCrossAndLeftAreDeterministicLeftMajor(t *testing.T) {
	source := applyTestRows(
		[]string{`1`, `"a"`},
		[]string{`2`, `"b"`},
		[]string{`3`, `"c"`},
	)
	rightRows := map[int64][][]Cell{
		1: {
			{applyTestInt(10), applyTestJSONCell(`"x"`)},
			{applyTestInt(11), applyTestJSONCell(`"y"`)},
		},
		3: {{applyTestInt(30), applyTestJSONCell(`"z"`)}},
	}
	program := &applyTestProgram{
		bind: func(left ApplyLeftRow, parameters *ApplyParameterBinder) error {
			return parameters.Append(left.Cell(0))
		},
		right: func(parameters ApplyParameters, out *ApplyRightAppender) error {
			key, _ := parameters.Cell(0).Int64()
			for _, row := range rightRows[key] {
				if err := out.AppendRow(row); err != nil {
					return err
				}
			}
			return nil
		},
	}
	options := ApplyOptions{
		Kind: ApplyCross, RightColumns: 2, ParameterColumns: 1,
	}
	var kernel ApplyKernel
	result, err := kernel.Run(source, program, options)
	if err != nil {
		t.Fatal(err)
	}
	wantCross := [][]string{
		{`1`, `"a"`, `10`, `"x"`},
		{`1`, `"a"`, `11`, `"y"`},
		{`3`, `"c"`, `30`, `"z"`},
	}
	if got := applyResultJSON(result); !reflect.DeepEqual(got, wantCross) {
		t.Fatalf("CROSS APPLY = %v, want %v", got, wantCross)
	}

	options.Kind = ApplyLeft
	result, err = kernel.Run(source, program, options)
	if err != nil {
		t.Fatal(err)
	}
	wantLeft := [][]string{
		{`1`, `"a"`, `10`, `"x"`},
		{`1`, `"a"`, `11`, `"y"`},
		{`2`, `"b"`, `null`, `null`},
		{`3`, `"c"`, `30`, `"z"`},
	}
	if got := applyResultJSON(result); !reflect.DeepEqual(got, wantLeft) {
		t.Fatalf("LEFT APPLY = %v, want %v", got, wantLeft)
	}
	if result.Missing(2, 2) || result.Missing(2, 3) {
		t.Fatal("null extension used missing markers instead of explicit SQL NULLs")
	}
}

func TestApplyKernelExactMemoizationIdentityAndFirstRepresentative(t *testing.T) {
	source := applyTestRows(
		[]string{`1`}, []string{`1.0`}, []string{`10e-1`},
		[]string{`<missing>`}, []string{`null`},
		[]string{`9007199254740992`}, []string{`9007199254740993`},
		[]string{`"a"`}, []string{`"\u0061"`},
	)
	calls := 0
	row := [1]Cell{}
	program := &applyTestProgram{
		bind: func(left ApplyLeftRow, parameters *ApplyParameterBinder) error {
			return parameters.Append(left.Cell(0))
		},
		right: func(parameters ApplyParameters, out *ApplyRightAppender) error {
			calls++
			row[0] = parameters.Cell(0)
			return out.AppendRow(row[:])
		},
	}
	var kernel ApplyKernel
	result, err := kernel.Run(source, program, ApplyOptions{
		Kind: ApplyCross, RightColumns: 1, ParameterColumns: 1,
		Memoization: ApplyMemoizationExact,
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 5 {
		t.Fatalf("right evaluations = %d, want 5 exact identity classes", calls)
	}
	wantRight := []string{
		`1`, `1`, `1`, `null`, `null`,
		`9007199254740992`, `9007199254740993`, `"a"`, `"a"`,
	}
	for index, want := range wantRight {
		if got := result.Cell(index, 1).JSON(); !bytes.Equal(got, []byte(want)) {
			t.Fatalf("row %d cached right = %q, want %q", index, got, want)
		}
	}
	if !result.Missing(3, 1) || !result.Missing(4, 1) {
		t.Fatal("NULL/missing cache hit did not preserve its first missing representative")
	}
}

func TestApplyKernelCompositeMemoKey(t *testing.T) {
	source := applyTestRows(
		[]string{`1`, `null`},
		[]string{`1.0`, `<missing>`},
		[]string{`1`, `"x"`},
		[]string{`1.00`, `"\u0078"`},
		[]string{`1`, `"y"`},
	)
	calls := 0
	row := [1]Cell{}
	program := &applyTestProgram{
		bind: func(left ApplyLeftRow, parameters *ApplyParameterBinder) error {
			if err := parameters.Append(left.Cell(0)); err != nil {
				return err
			}
			return parameters.Append(left.Cell(1))
		},
		right: func(_ ApplyParameters, out *ApplyRightAppender) error {
			calls++
			row[0] = applyTestInt(int64(calls))
			return out.AppendRow(row[:])
		},
	}
	var kernel ApplyKernel
	result, err := kernel.Run(source, program, ApplyOptions{
		Kind: ApplyCross, RightColumns: 1, ParameterColumns: 2,
		Memoization: ApplyMemoizationExact,
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("right evaluations = %d, want 3 composite identities", calls)
	}
	want := []int64{1, 1, 2, 2, 3}
	for row, value := range want {
		got, ok := result.Cell(row, 2).Int64()
		if !ok || got != value {
			t.Fatalf("row %d memo class = %d/%v, want %d", row, got, ok, value)
		}
	}
}

func TestApplyKernelZeroParameterMemoizationDecorrelatesOnce(t *testing.T) {
	source := applyTestRows([]string{`1`}, []string{`2`}, []string{`3`})
	calls := 0
	program := &applyTestProgram{
		bind: func(ApplyLeftRow, *ApplyParameterBinder) error {
			return errors.New("zero-parameter Bind must be skipped")
		},
		right: func(parameters ApplyParameters, out *ApplyRightAppender) error {
			calls++
			if parameters.Columns() != 0 {
				return fmt.Errorf("parameter columns = %d", parameters.Columns())
			}
			for _, value := range []int64{7, 8} {
				row := [1]Cell{applyTestInt(value)}
				if err := out.AppendRow(row[:]); err != nil {
					return err
				}
			}
			return nil
		},
	}
	var kernel ApplyKernel
	result, err := kernel.Run(source, program, ApplyOptions{
		Kind: ApplyCross, RightColumns: 1,
		Memoization: ApplyMemoizationExact,
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("uncorrelated right evaluations = %d, want 1", calls)
	}
	want := [][]string{
		{`1`, `7`}, {`1`, `8`},
		{`2`, `7`}, {`2`, `8`},
		{`3`, `7`}, {`3`, `8`},
	}
	if got := applyResultJSON(result); !reflect.DeepEqual(got, want) {
		t.Fatalf("decorrelated output = %v, want %v", got, want)
	}
}

func TestApplyKernelMemoizedEmptyRelationReplaysLeftNullExtension(t *testing.T) {
	source := applyTestRows([]string{`1`}, []string{`2`}, []string{`3`})
	calls := 0
	program := &applyTestProgram{right: func(
		_ ApplyParameters,
		_ *ApplyRightAppender,
	) error {
		calls++
		return nil
	}}
	var kernel ApplyKernel
	result, err := kernel.Run(source, program, ApplyOptions{
		Kind: ApplyOuter, RightColumns: 2,
		Memoization: ApplyMemoizationExact,
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("empty right evaluations = %d, want 1", calls)
	}
	if result.Rows() != source.Rows() {
		t.Fatalf("null-extended rows = %d, want %d", result.Rows(), source.Rows())
	}
	for row := 0; row < result.Rows(); row++ {
		if !result.Cell(row, 1).IsNull() || !result.Cell(row, 2).IsNull() ||
			result.Missing(row, 1) || result.Missing(row, 2) {
			t.Fatalf("row %d does not contain explicit right NULLs", row)
		}
	}
}

func TestApplyKernelCacheDisabledEvaluatesEveryLeftWithoutCacheState(t *testing.T) {
	source := applyTestRows([]string{`1`}, []string{`1.0`}, []string{`1`})
	calls := 0
	row := [1]Cell{}
	program := &applyTestProgram{
		bind: func(left ApplyLeftRow, parameters *ApplyParameterBinder) error {
			return parameters.Append(left.Cell(0))
		},
		right: func(_ ApplyParameters, out *ApplyRightAppender) error {
			calls++
			row[0] = applyTestInt(int64(calls))
			return out.AppendRow(row[:])
		},
	}
	var kernel ApplyKernel
	if _, err := kernel.Run(source, program, ApplyOptions{
		Kind: ApplyCross, RightColumns: 1, ParameterColumns: 1,
		Memoization: ApplyMemoizationExact,
	}); err != nil {
		t.Fatal(err)
	}
	calls = 0
	result, err := kernel.Run(source, program, ApplyOptions{
		Kind: ApplyCross, RightColumns: 1, ParameterColumns: 1,
		Memoization: ApplyMemoizationNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != source.Rows() {
		t.Fatalf("cache-disabled evaluations = %d, want %d", calls, source.Rows())
	}
	if len(kernel.cache.entries) != 0 || len(kernel.cache.slots) != 0 ||
		kernel.cache.keys.rows != 0 || kernel.cache.values.rows != 0 {
		t.Fatal("cache-disabled run retained logical memoization state")
	}
	for row := 0; row < result.Rows(); row++ {
		value, _ := result.Cell(row, 1).Int64()
		if value != int64(row+1) {
			t.Fatalf("row %d evaluation = %d, want %d", row, value, row+1)
		}
	}
}

func TestApplyKernelPreparedReuseClearsMemoizedRelations(t *testing.T) {
	source := applyTestRows([]string{`1`}, []string{`1`})
	epoch := int64(10)
	row := [1]Cell{}
	program := &applyTestProgram{
		bind: func(left ApplyLeftRow, parameters *ApplyParameterBinder) error {
			return parameters.Append(left.Cell(0))
		},
		right: func(_ ApplyParameters, out *ApplyRightAppender) error {
			row[0] = applyTestInt(epoch)
			return out.AppendRow(row[:])
		},
	}
	options := ApplyOptions{
		Kind: ApplyCross, RightColumns: 1, ParameterColumns: 1,
		Memoization: ApplyMemoizationExact,
	}
	var kernel ApplyKernel
	first, err := kernel.Run(source, program, options)
	if err != nil {
		t.Fatal(err)
	}
	if value, _ := first.Cell(1, 1).Int64(); value != 10 {
		t.Fatalf("first prepared value = %d", value)
	}
	entryCap, slotCap := cap(kernel.cache.entries), cap(kernel.cache.slots)
	epoch = 20
	second, err := kernel.Run(source, program, options)
	if err != nil {
		t.Fatal(err)
	}
	for row := 0; row < second.Rows(); row++ {
		if value, _ := second.Cell(row, 1).Int64(); value != 20 {
			t.Fatalf("second prepared row %d = %d, want 20", row, value)
		}
	}
	if cap(kernel.cache.entries) != entryCap || cap(kernel.cache.slots) != slotCap {
		t.Fatalf("cache capacities changed: entries %d->%d slots %d->%d",
			entryCap, cap(kernel.cache.entries), slotCap, cap(kernel.cache.slots))
	}
}

func TestApplyKernelTypedBudgetsAndNoPartialPublication(t *testing.T) {
	source := applyTestRows([]string{`1`}, []string{`2`})
	bind := func(left ApplyLeftRow, parameters *ApplyParameterBinder) error {
		return parameters.Append(left.Cell(0))
	}
	tests := []struct {
		name    string
		options ApplyOptions
		program ApplyProgram
		is      error
	}{
		{
			name: "parameter count",
			options: ApplyOptions{
				Kind: ApplyCross, RightColumns: 1, ParameterColumns: 2,
			},
			program: &applyTestProgram{bind: bind},
			is:      ErrApplyParameterCount,
		},
		{
			name: "right arity",
			options: ApplyOptions{
				Kind: ApplyCross, RightColumns: 2, ParameterColumns: 1,
			},
			program: &applyTestProgram{
				bind: bind,
				right: func(_ ApplyParameters, out *ApplyRightAppender) error {
					return out.AppendRow([]Cell{applyTestInt(1)})
				},
			},
			is: ErrApplyRightArity,
		},
		{
			name: "rows",
			options: ApplyOptions{
				Kind: ApplyCross, RightColumns: 1, ParameterColumns: 1, MaxRows: 1,
			},
			program: &applyTestProgram{
				bind: bind,
				right: func(_ ApplyParameters, out *ApplyRightAppender) error {
					row := [1]Cell{applyTestInt(1)}
					return out.AppendRow(row[:])
				},
			},
			is: ErrApplyRows,
		},
		{
			name: "bytes",
			options: ApplyOptions{
				Kind: ApplyCross, RightColumns: 1, ParameterColumns: 1, MaxBytes: 1,
			},
			program: &applyTestProgram{bind: bind},
			is:      ErrApplyBytes,
		},
		{
			name: "cache entries",
			options: ApplyOptions{
				Kind: ApplyCross, RightColumns: 1, ParameterColumns: 1,
				Memoization: ApplyMemoizationExact, MaxCacheEntries: 1,
			},
			program: &applyTestProgram{bind: bind},
			is:      ErrApplyCacheBudget,
		},
		{
			name: "cache bytes",
			options: ApplyOptions{
				Kind: ApplyCross, RightColumns: 1, ParameterColumns: 1,
				Memoization: ApplyMemoizationExact, MaxCacheBytes: 1,
			},
			program: &applyTestProgram{bind: bind},
			is:      ErrApplyCacheBudget,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var kernel ApplyKernel
			result, err := kernel.Run(source, test.program, test.options)
			if !errors.Is(err, test.is) {
				t.Fatalf("error = %v, want errors.Is(_, %v)", err, test.is)
			}
			if result.Rows() != 0 || kernel.result.rows != 0 || kernel.inner.rows != 0 {
				t.Fatalf("partial output: returned=%d result=%d inner=%d",
					result.Rows(), kernel.result.rows, kernel.inner.rows)
			}
		})
	}
}

func TestApplyKernelLeftNullExtensionHonorsBudgets(t *testing.T) {
	source := applyTestRows([]string{`1`}, []string{`2`})
	program := &applyTestProgram{
		bind: func(left ApplyLeftRow, parameters *ApplyParameterBinder) error {
			return parameters.Append(left.Cell(0))
		},
	}
	var kernel ApplyKernel
	result, err := kernel.Run(source, program, ApplyOptions{
		Kind: ApplyLeft, RightColumns: 2, ParameterColumns: 1, MaxRows: 1,
	})
	if !errors.Is(err, ErrApplyRows) || result.Rows() != 0 || kernel.result.rows != 0 {
		t.Fatalf("null-extension budget = rows %d internal %d err %v",
			result.Rows(), kernel.result.rows, err)
	}
}

func TestApplyKernelCancellationAndCallbackFailureRecoverForReuse(t *testing.T) {
	source := applyTestRows([]string{`1`}, []string{`2`})
	var cancel CancelFlag
	callbackErr := errors.New("right relation failed")
	row := [1]Cell{applyTestInt(7)}
	program := &applyTestProgram{
		bind: func(left ApplyLeftRow, parameters *ApplyParameterBinder) error {
			return parameters.Append(left.Cell(0))
		},
		right: func(_ ApplyParameters, out *ApplyRightAppender) error {
			if err := out.AppendRow(row[:]); err != nil {
				return err
			}
			return callbackErr
		},
	}
	options := ApplyOptions{
		Kind: ApplyCross, RightColumns: 1, ParameterColumns: 1, Cancel: &cancel,
	}
	var kernel ApplyKernel
	result, err := kernel.Run(source, program, options)
	if !errors.Is(err, callbackErr) || result.Rows() != 0 || kernel.result.rows != 0 {
		t.Fatalf("callback failure = rows %d internal %d err %v",
			result.Rows(), kernel.result.rows, err)
	}

	entered := make(chan struct{})
	program.right = func(parameters ApplyParameters, out *ApplyRightAppender) error {
		key, _ := parameters.Cell(0).Int64()
		if key == 1 {
			return out.AppendRow(row[:])
		}
		close(entered)
		for {
			if err := out.Checkpoint(); err != nil {
				return err
			}
			runtime.Gosched()
		}
	}
	type outcome struct {
		result ApplyResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := kernel.Run(source, program, options)
		done <- outcome{result: result, err: err}
	}()
	<-entered
	cancel.Cancel()
	got := <-done
	if !errors.Is(got.err, ErrCanceled) || got.result.Rows() != 0 || kernel.result.rows != 0 {
		t.Fatalf("cancellation = rows %d internal %d err %v",
			got.result.Rows(), kernel.result.rows, got.err)
	}

	cancel.Reset()
	var retained *ApplyRightAppender
	program.right = func(_ ApplyParameters, out *ApplyRightAppender) error {
		retained = out
		return out.AppendRow(row[:])
	}
	result, err = kernel.Run(source, program, options)
	if err != nil || result.Rows() != 2 {
		t.Fatalf("recovery = rows %d err %v", result.Rows(), err)
	}
	if err := retained.AppendRow(row[:]); !errors.Is(err, ErrApplyAppenderInactive) {
		t.Fatalf("retained appender error = %v, want inactive", err)
	}
}

func TestApplyKernelIgnoredEmitterErrorsStillFail(t *testing.T) {
	source := applyTestRows([]string{`1`})
	tests := []struct {
		name    string
		options ApplyOptions
		program ApplyProgram
		is      error
	}{
		{
			name: "binder",
			options: ApplyOptions{
				Kind: ApplyCross, RightColumns: 1, ParameterColumns: 1,
			},
			program: &applyTestProgram{bind: func(_ ApplyLeftRow, parameters *ApplyParameterBinder) error {
				_ = parameters.Append(applyTestInt(1))
				_ = parameters.Append(applyTestInt(2))
				return nil
			}},
			is: ErrApplyParameterCount,
		},
		{
			name: "right",
			options: ApplyOptions{
				Kind: ApplyCross, RightColumns: 1,
			},
			program: &applyTestProgram{right: func(_ ApplyParameters, out *ApplyRightAppender) error {
				_ = out.AppendRow(nil)
				return nil
			}},
			is: ErrApplyRightArity,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var kernel ApplyKernel
			if _, err := kernel.Run(source, test.program, test.options); !errors.Is(err, test.is) {
				t.Fatalf("error = %v, want %v", err, test.is)
			}
		})
	}
}

func TestApplyKernelReleaseDropsHighWaterStorageAndReusesZeroValue(t *testing.T) {
	rows := make([][]Cell, 64)
	for row := range rows {
		rows[row] = []Cell{applyTestInt(int64(row % 8))}
	}
	source := &applyTestSource{rows: rows}
	program := &applyTestProgram{
		bind: func(left ApplyLeftRow, parameters *ApplyParameterBinder) error {
			return parameters.Append(left.Cell(0))
		},
		right: func(parameters ApplyParameters, out *ApplyRightAppender) error {
			row := [1]Cell{parameters.Cell(0)}
			return out.AppendRow(row[:])
		},
	}
	options := ApplyOptions{
		Kind: ApplyCross, RightColumns: 1, ParameterColumns: 1,
		Memoization: ApplyMemoizationExact,
	}
	var kernel ApplyKernel
	if _, err := kernel.Run(source, program, options); err != nil {
		t.Fatal(err)
	}
	if cap(kernel.result.cells) == 0 || cap(kernel.inner.cells) == 0 ||
		cap(kernel.cache.entries) == 0 || cap(kernel.outputCells) == 0 {
		t.Fatal("run did not retain reusable APPLY storage")
	}
	kernel.Release()
	if cap(kernel.result.cells) != 0 || cap(kernel.inner.cells) != 0 ||
		cap(kernel.cache.entries) != 0 || cap(kernel.cache.slots) != 0 ||
		cap(kernel.leftCells) != 0 || cap(kernel.outputCells) != 0 {
		t.Fatal("Release retained APPLY storage")
	}
	result, err := kernel.Run(source, program, options)
	if err != nil || result.Rows() != len(rows) {
		t.Fatalf("post-Release run = rows %d err %v", result.Rows(), err)
	}
}

func TestApplyKernelRejectsInvalidConfigurationAndOverflow(t *testing.T) {
	source := applyTestRows([]string{`1`})
	program := &applyTestProgram{}
	var kernel ApplyKernel
	tests := []ApplyOptions{
		{},
		{Kind: ApplyJoinKind(9), RightColumns: 1},
		{Kind: ApplyCross, RightColumns: -1},
		{Kind: ApplyCross, RightColumns: 1, ParameterColumns: -1},
		{Kind: ApplyCross, RightColumns: 1, Memoization: ApplyMemoization(9)},
		{Kind: ApplyCross, RightColumns: 1, MaxRows: -1, MaxBytes: -1},
		{
			Kind: ApplyCross, RightColumns: 1, Memoization: ApplyMemoizationExact,
			MaxCacheEntries: -1, MaxCacheBytes: -1,
		},
	}
	for _, options := range tests {
		if _, err := kernel.Run(source, program, options); !errors.Is(err, ErrApplyConfig) {
			t.Fatalf("options %+v error = %v, want config", options, err)
		}
	}
	if _, err := kernel.Run(nil, program, ApplyOptions{Kind: ApplyCross, RightColumns: 1}); !errors.Is(err, ErrApplySource) {
		t.Fatalf("nil source error = %v", err)
	}
	if _, err := kernel.Run(source, nil, ApplyOptions{Kind: ApplyCross, RightColumns: 1}); !errors.Is(err, ErrApplyProgram) {
		t.Fatalf("nil program error = %v", err)
	}

	overflow := &applyShapeSource{rows: 1, columns: math.MaxInt}
	if _, err := kernel.Run(overflow, program, ApplyOptions{
		Kind: ApplyCross, RightColumns: 1,
	}); !errors.Is(err, ErrApplySize) {
		t.Fatalf("overflow source error = %v, want size", err)
	}
}

type applyShapeSource struct{ rows, columns int }

func (s *applyShapeSource) Rows() int                 { return s.rows }
func (s *applyShapeSource) Columns() int              { return s.columns }
func (s *applyShapeSource) Cell(row, column int) Cell { return Cell{} }

func TestApplyKernelConcurrentUseAndIndependentStatementSafety(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	source := applyTestRows([]string{`1`})
	program := &applyTestProgram{right: func(_ ApplyParameters, out *ApplyRightAppender) error {
		close(entered)
		<-release
		row := [1]Cell{applyTestInt(1)}
		return out.AppendRow(row[:])
	}}
	options := ApplyOptions{Kind: ApplyCross, RightColumns: 1}
	var kernel ApplyKernel
	done := make(chan error, 1)
	go func() {
		_, err := kernel.Run(source, program, options)
		done <- err
	}()
	<-entered
	if result, err := kernel.Run(source, program, options); !errors.Is(err, ErrApplyInUse) || result.Rows() != 0 {
		t.Fatalf("overlapping run = rows %d err %v", result.Rows(), err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	const workers = 16
	var wait sync.WaitGroup
	wait.Add(workers)
	errs := make(chan error, workers)
	for worker := range workers {
		worker := worker
		go func() {
			defer wait.Done()
			workerSource := &applyTestSource{rows: [][]Cell{{applyTestInt(int64(worker))}}}
			row := [1]Cell{}
			workerProgram := &applyTestProgram{right: func(_ ApplyParameters, out *ApplyRightAppender) error {
				row[0] = applyTestInt(int64(worker + 1))
				return out.AppendRow(row[:])
			}}
			var workerKernel ApplyKernel
			result, err := workerKernel.Run(workerSource, workerProgram, options)
			if err == nil {
				value, ok := result.Cell(0, 1).Int64()
				if result.Rows() != 1 || !ok || value != int64(worker+1) {
					err = fmt.Errorf("worker %d got rows=%d value=%d ok=%v",
						worker, result.Rows(), value, ok)
				}
			}
			errs <- err
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestApplyKernelRandomizedDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(0xa9917))
	for trial := 0; trial < 200; trial++ {
		leftRows := 1 + rng.Intn(24)
		sourceRows := make([][]Cell, leftRows)
		keys := make([]int, leftRows)
		for row := range sourceRows {
			keys[row] = rng.Intn(9) - 3
			sourceRows[row] = []Cell{applyTestInt(int64(row)), applyTestInt(int64(keys[row]))}
		}
		source := &applyTestSource{rows: sourceRows}
		for _, kind := range []ApplyJoinKind{ApplyCross, ApplyLeft} {
			want := applyReferenceRows(keys, kind)
			without := runApplyRandomCase(t, source, kind, ApplyMemoizationNone)
			with := runApplyRandomCase(t, source, kind, ApplyMemoizationExact)
			if !reflect.DeepEqual(without, want) {
				t.Fatalf("trial %d kind %d uncached = %v, want %v", trial, kind, without, want)
			}
			if !reflect.DeepEqual(with, want) {
				t.Fatalf("trial %d kind %d cached = %v, want %v", trial, kind, with, want)
			}
		}
	}
}

func runApplyRandomCase(
	t testing.TB,
	source *applyTestSource,
	kind ApplyJoinKind,
	memoization ApplyMemoization,
) [][3]int {
	t.Helper()
	row := [1]Cell{}
	program := &applyTestProgram{
		bind: func(left ApplyLeftRow, parameters *ApplyParameterBinder) error {
			return parameters.Append(left.Cell(1))
		},
		right: func(parameters ApplyParameters, out *ApplyRightAppender) error {
			key, _ := parameters.Cell(0).Int64()
			count := int((key%3 + 3) % 3)
			for ordinal := 0; ordinal < count; ordinal++ {
				row[0] = applyTestInt(key*10 + int64(ordinal))
				if err := out.AppendRow(row[:]); err != nil {
					return err
				}
			}
			return nil
		},
	}
	var kernel ApplyKernel
	result, err := kernel.Run(source, program, ApplyOptions{
		Kind: kind, RightColumns: 1, ParameterColumns: 1,
		Memoization: memoization,
	})
	if err != nil {
		t.Fatal(err)
	}
	got := make([][3]int, result.Rows())
	for output := range got {
		left, _ := result.Cell(output, 0).Int64()
		key, _ := result.Cell(output, 1).Int64()
		right := math.MinInt
		if value, ok := result.Cell(output, 2).Int64(); ok {
			right = int(value)
		}
		got[output] = [3]int{int(left), int(key), right}
	}
	return got
}

func applyReferenceRows(keys []int, kind ApplyJoinKind) [][3]int {
	result := make([][3]int, 0)
	for left, key := range keys {
		count := (key%3 + 3) % 3
		if count == 0 && kind == ApplyLeft {
			result = append(result, [3]int{left, key, math.MinInt})
			continue
		}
		for ordinal := 0; ordinal < count; ordinal++ {
			result = append(result, [3]int{left, key, key*10 + ordinal})
		}
	}
	return result
}
