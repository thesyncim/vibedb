package query

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"sync"
	"testing"

	"github.com/thesyncim/vibejson"
)

type recursiveTestProgram struct {
	anchor func(*RecursiveAppender) error
	step   func(RecursiveDelta, *RecursiveAppender) error
}

func (p *recursiveTestProgram) Anchor(out *RecursiveAppender) error {
	if p.anchor == nil {
		return nil
	}
	return p.anchor(out)
}

func (p *recursiveTestProgram) Step(delta RecursiveDelta, out *RecursiveAppender) error {
	if p.step == nil {
		return nil
	}
	return p.step(delta, out)
}

func recursiveTestJSONCell(src string) Cell {
	if src == "<missing>" {
		return Cell{kind: TypeNull, flag: cellMissing, raw: nullBytes}
	}
	var decoded []byte
	return cellFromScalar(classifyRawInto(
		vibejson.RawValue{Src: []byte(src)}, &decoded,
	))
}

func recursiveTestInt(value int) Cell {
	return Cell{kind: TypeNumber, flag: cellInteger, word: uint64(int64(value))}
}

func recursiveResultInts(t testing.TB, result RecursiveResult) []int {
	t.Helper()
	values := make([]int, result.Rows())
	for row := range values {
		value, ok := result.Cell(row, 0).Int64()
		if !ok {
			t.Fatalf("row %d is %q, not an integer", row, result.Cell(row, 0).JSON())
		}
		values[row] = int(value)
	}
	return values
}

func recursiveResultJSON(result RecursiveResult) []string {
	rows := make([]string, result.Rows())
	for row := range rows {
		rows[row] = string(result.Cell(row, 0).JSON())
	}
	return rows
}

func TestRecursiveFixpointUnionAllBreadthFirstOrder(t *testing.T) {
	graph := [][]int{
		nil,
		{2, 3},
		{4, 5},
		{6},
		nil,
		{7},
		nil,
		nil,
	}
	row := [1]Cell{}
	program := &recursiveTestProgram{
		anchor: func(out *RecursiveAppender) error {
			row[0] = recursiveTestInt(1)
			return out.AppendRow(row[:])
		},
		step: func(delta RecursiveDelta, out *RecursiveAppender) error {
			for source := 0; source < delta.Rows(); source++ {
				value, _ := delta.Cell(source, 0).Int64()
				for _, child := range graph[value] {
					row[0] = recursiveTestInt(child)
					if err := out.AppendRow(row[:]); err != nil {
						return err
					}
				}
			}
			return nil
		},
	}

	var kernel RecursiveFixpoint
	result, err := kernel.Run(program, RecursiveFixpointOptions{
		Columns: 1, Union: RecursiveUnionAll,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := recursiveResultInts(t, result), []int{1, 2, 3, 4, 5, 6, 7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("breadth-first rows = %v, want %v", got, want)
	}
	if result.Iterations() != 4 {
		t.Fatalf("iterations = %d, want 4", result.Iterations())
	}
}

func TestRecursiveFixpointUnionDistinctCyclesConvergeInFirstProductionOrder(t *testing.T) {
	graph := [][]int{
		nil,
		{2, 3},
		{1, 4},
		{4, 5},
		{2},
		nil,
	}
	row := [1]Cell{}
	program := &recursiveTestProgram{
		anchor: func(out *RecursiveAppender) error {
			row[0] = recursiveTestInt(1)
			if err := out.AppendRow(row[:]); err != nil {
				return err
			}
			return out.AppendRow(row[:])
		},
		step: func(delta RecursiveDelta, out *RecursiveAppender) error {
			for source := 0; source < delta.Rows(); source++ {
				value, _ := delta.Cell(source, 0).Int64()
				for _, child := range graph[value] {
					row[0] = recursiveTestInt(child)
					if err := out.AppendRow(row[:]); err != nil {
						return err
					}
				}
			}
			return nil
		},
	}

	var kernel RecursiveFixpoint
	result, err := kernel.Run(program, RecursiveFixpointOptions{
		Columns: 1, Union: RecursiveUnionDistinct,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := recursiveResultInts(t, result), []int{1, 2, 3, 4, 5}; !reflect.DeepEqual(got, want) {
		t.Fatalf("distinct rows = %v, want %v", got, want)
	}
	if result.Iterations() != 3 {
		t.Fatalf("iterations = %d, want 3", result.Iterations())
	}
}

type recursiveReuseProgram struct {
	rows [][]Cell
	row  []Cell
}

func (p *recursiveReuseProgram) Anchor(out *RecursiveAppender) error {
	for _, row := range p.rows {
		if err := out.AppendRow(row); err != nil {
			return err
		}
	}
	return nil
}

func (p *recursiveReuseProgram) Step(delta RecursiveDelta, out *RecursiveAppender) error {
	if cap(p.row) < delta.Columns() {
		p.row = make([]Cell, delta.Columns())
	} else {
		p.row = p.row[:delta.Columns()]
	}
	for source := 0; source < delta.Rows(); source++ {
		for column := range p.row {
			p.row[column] = delta.Cell(source, column)
		}
		if err := out.AppendRow(p.row); err != nil {
			return err
		}
	}
	return nil
}

func recursiveTestRows(values ...string) [][]Cell {
	rows := make([][]Cell, len(values))
	for index, value := range values {
		rows[index] = []Cell{recursiveTestJSONCell(value)}
	}
	return rows
}

func TestRecursiveFixpointPreparedDistinctReuseStableExactIdentity(t *testing.T) {
	program := &recursiveReuseProgram{rows: recursiveTestRows(
		`1`, `1.0`, `10e-1`,
		`<missing>`, `null`,
		`9007199254740992`, `9007199254740993`,
		`"a"`, `"\u0061"`,
		`{"x":1}`, `{ "x":1}`,
	)}
	options := RecursiveFixpointOptions{Columns: 1, Union: RecursiveUnionDistinct}
	var kernel RecursiveFixpoint

	first, err := kernel.Run(program, options)
	if err != nil {
		t.Fatal(err)
	}
	wantFirst := []string{
		`1`, `null`, `9007199254740992`, `9007199254740993`,
		`"a"`, `{"x":1}`, `{ "x":1}`,
	}
	if got := recursiveResultJSON(first); !reflect.DeepEqual(got, wantFirst) {
		t.Fatalf("first prepared run = %v, want %v", got, wantFirst)
	}
	if !first.Missing(1, 0) {
		t.Fatal("first NULL identity did not retain its first-produced missing marker")
	}
	firstSlots, firstEntries := cap(kernel.identity.slots), cap(kernel.identity.entries)

	program.rows = recursiveTestRows(
		`1.00`, `1`,
		`null`, `<missing>`,
		`9007199254740993`, `9007199254740992`,
		`"\u0061"`, `"a"`,
		`{ "x":1}`, `{"x":1}`, `2`,
	)
	second, err := kernel.Run(program, options)
	if err != nil {
		t.Fatal(err)
	}
	wantSecond := []string{
		`1.00`, `null`, `9007199254740993`, `9007199254740992`,
		`"\u0061"`, `{ "x":1}`, `{"x":1}`, `2`,
	}
	if got := recursiveResultJSON(second); !reflect.DeepEqual(got, wantSecond) {
		t.Fatalf("second prepared run = %v, want %v", got, wantSecond)
	}
	if second.Missing(1, 0) {
		t.Fatal("second NULL identity did not retain its first-produced explicit NULL")
	}
	if cap(kernel.identity.slots) != firstSlots || cap(kernel.identity.entries) != firstEntries {
		t.Fatalf("prepared index capacities changed: slots %d->%d entries %d->%d",
			firstSlots, cap(kernel.identity.slots), firstEntries, cap(kernel.identity.entries))
	}

	third, err := kernel.Run(program, options)
	if err != nil {
		t.Fatal(err)
	}
	if got := recursiveResultJSON(third); !reflect.DeepEqual(got, wantSecond) {
		t.Fatalf("repeated prepared run = %v, want %v", got, wantSecond)
	}
}

func TestRecursiveFixpointCompositeDistinctIdentity(t *testing.T) {
	rows := [][]Cell{
		{recursiveTestJSONCell(`1`), recursiveTestJSONCell(`null`)},
		{recursiveTestJSONCell(`1.0`), recursiveTestJSONCell(`<missing>`)},
		{recursiveTestJSONCell(`1`), recursiveTestJSONCell(`"x"`)},
		{recursiveTestJSONCell(`1.00`), recursiveTestJSONCell(`"\u0078"`)},
	}
	program := &recursiveTestProgram{anchor: func(out *RecursiveAppender) error {
		for _, row := range rows {
			if err := out.AppendRow(row); err != nil {
				return err
			}
		}
		return nil
	}}
	var kernel RecursiveFixpoint
	result, err := kernel.Run(program, RecursiveFixpointOptions{
		Columns: 2, Union: RecursiveUnionDistinct,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows() != 2 {
		t.Fatalf("rows = %d, want 2", result.Rows())
	}
	if !bytes.Equal(result.Cell(0, 0).JSON(), []byte(`1`)) ||
		!bytes.Equal(result.Cell(1, 1).JSON(), []byte(`"x"`)) {
		t.Fatalf("first composite representatives = %q / %q",
			result.Cell(0, 0).JSON(), result.Cell(1, 1).JSON())
	}
}

func TestRecursiveFixpointUnionAllPreservesNullAndMissing(t *testing.T) {
	program := &recursiveTestProgram{anchor: func(out *RecursiveAppender) error {
		for _, value := range []string{"<missing>", `null`} {
			row := [1]Cell{recursiveTestJSONCell(value)}
			if err := out.AppendRow(row[:]); err != nil {
				return err
			}
		}
		return nil
	}}
	var kernel RecursiveFixpoint
	result, err := kernel.Run(program, RecursiveFixpointOptions{
		Columns: 1, Union: RecursiveUnionAll,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows() != 2 || !result.Missing(0, 0) || result.Missing(1, 0) {
		t.Fatalf("missing markers = (%v, %v), rows = %d",
			result.Missing(0, 0), result.Missing(1, 0), result.Rows())
	}
}

func TestRecursiveFixpointLimitsAreTypedAndPublishNoPartialResult(t *testing.T) {
	tests := []struct {
		name    string
		options RecursiveFixpointOptions
		program RecursiveFixpointProgram
		is      error
	}{
		{
			name: "arity",
			options: RecursiveFixpointOptions{
				Columns: 2, Union: RecursiveUnionAll,
			},
			program: &recursiveTestProgram{anchor: func(out *RecursiveAppender) error {
				return out.AppendRow([]Cell{recursiveTestInt(1)})
			}},
			is: ErrRecursiveArity,
		},
		{
			name: "rows",
			options: RecursiveFixpointOptions{
				Columns: 1, Union: RecursiveUnionAll, MaxRows: 2,
			},
			program: &recursiveTestProgram{anchor: func(out *RecursiveAppender) error {
				row := [1]Cell{recursiveTestInt(1)}
				for range 3 {
					if err := out.AppendRow(row[:]); err != nil {
						return err
					}
				}
				return nil
			}},
			is: ErrRecursiveRows,
		},
		{
			name: "bytes",
			options: RecursiveFixpointOptions{
				Columns: 1, Union: RecursiveUnionAll, MaxBytes: 1,
			},
			program: &recursiveTestProgram{anchor: func(out *RecursiveAppender) error {
				row := [1]Cell{recursiveTestJSONCell(`"payload"`)}
				return out.AppendRow(row[:])
			}},
			is: ErrRecursiveBytes,
		},
		{
			name: "depth",
			options: RecursiveFixpointOptions{
				Columns: 1, Union: RecursiveUnionAll, MaxIterations: 2,
			},
			program: &recursiveTestProgram{
				anchor: func(out *RecursiveAppender) error {
					row := [1]Cell{recursiveTestInt(1)}
					return out.AppendRow(row[:])
				},
				step: func(delta RecursiveDelta, out *RecursiveAppender) error {
					row := [1]Cell{recursiveTestInt(delta.Iteration() + 2)}
					return out.AppendRow(row[:])
				},
			},
			is: ErrRecursiveDepth,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var kernel RecursiveFixpoint
			result, err := kernel.Run(test.program, test.options)
			if !errors.Is(err, test.is) {
				t.Fatalf("error = %v, want errors.Is(_, %v)", err, test.is)
			}
			if result.Rows() != 0 || kernel.result.rows != 0 || kernel.working.rows != 0 {
				t.Fatalf("partial result published: returned=%d result=%d working=%d",
					result.Rows(), kernel.result.rows, kernel.working.rows)
			}
		})
	}
}

func TestRecursiveFixpointCancellationAndCallbackFailureRecoverForReuse(t *testing.T) {
	var cancel CancelFlag
	row := [1]Cell{recursiveTestInt(1)}
	program := &recursiveTestProgram{anchor: func(out *RecursiveAppender) error {
		if err := out.AppendRow(row[:]); err != nil {
			return err
		}
		cancel.Cancel()
		return out.AppendRow(row[:])
	}}
	options := RecursiveFixpointOptions{
		Columns: 1, Union: RecursiveUnionDistinct, Cancel: &cancel,
	}
	var kernel RecursiveFixpoint
	result, err := kernel.Run(program, options)
	if !errors.Is(err, ErrCanceled) || result.Rows() != 0 || kernel.result.rows != 0 {
		t.Fatalf("canceled run = rows %d, internal %d, err %v",
			result.Rows(), kernel.result.rows, err)
	}

	cancel.Reset()
	callbackErr := errors.New("recursive callback failed")
	program.anchor = func(out *RecursiveAppender) error {
		if err := out.AppendRow(row[:]); err != nil {
			return err
		}
		return callbackErr
	}
	result, err = kernel.Run(program, options)
	if !errors.Is(err, callbackErr) || result.Rows() != 0 || kernel.result.rows != 0 {
		t.Fatalf("failed run = rows %d, internal %d, err %v",
			result.Rows(), kernel.result.rows, err)
	}

	var retained *RecursiveAppender
	program.anchor = func(out *RecursiveAppender) error {
		retained = out
		return out.AppendRow(row[:])
	}
	result, err = kernel.Run(program, options)
	if err != nil || result.Rows() != 1 {
		t.Fatalf("recovery run = rows %d, err %v", result.Rows(), err)
	}
	if err := retained.AppendRow(row[:]); !errors.Is(err, ErrRecursiveAppenderInactive) {
		t.Fatalf("retained appender error = %v, want inactive", err)
	}
}

func TestRecursiveFixpointConcurrentCancellationPublishesNoRows(t *testing.T) {
	var cancel CancelFlag
	entered := make(chan struct{})
	program := &recursiveTestProgram{
		anchor: func(out *RecursiveAppender) error {
			row := [1]Cell{recursiveTestInt(1)}
			return out.AppendRow(row[:])
		},
		step: func(_ RecursiveDelta, out *RecursiveAppender) error {
			close(entered)
			for {
				if err := out.Checkpoint(); err != nil {
					return err
				}
				runtime.Gosched()
			}
		},
	}
	options := RecursiveFixpointOptions{
		Columns: 1, Union: RecursiveUnionAll, Cancel: &cancel,
	}
	var kernel RecursiveFixpoint
	type outcome struct {
		result RecursiveResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := kernel.Run(program, options)
		done <- outcome{result: result, err: err}
	}()
	<-entered
	cancel.Cancel()
	got := <-done
	if !errors.Is(got.err, ErrCanceled) || got.result.Rows() != 0 || kernel.result.rows != 0 {
		t.Fatalf("concurrent cancellation = rows %d, internal %d, err %v",
			got.result.Rows(), kernel.result.rows, got.err)
	}
}

func TestRecursiveFixpointIgnoredAppenderErrorStillFails(t *testing.T) {
	program := &recursiveTestProgram{anchor: func(out *RecursiveAppender) error {
		_ = out.AppendRow(nil)
		return nil
	}}
	var kernel RecursiveFixpoint
	result, err := kernel.Run(program, RecursiveFixpointOptions{
		Columns: 1, Union: RecursiveUnionAll,
	})
	if !errors.Is(err, ErrRecursiveArity) || result.Rows() != 0 {
		t.Fatalf("ignored append error returned rows=%d err=%v", result.Rows(), err)
	}
}

func TestRecursiveFixpointReleaseDropsStorageAndAllowsReuse(t *testing.T) {
	program := &recursiveTestProgram{anchor: func(out *RecursiveAppender) error {
		for value := 0; value < 64; value++ {
			row := [1]Cell{recursiveTestInt(value)}
			if err := out.AppendRow(row[:]); err != nil {
				return err
			}
		}
		return nil
	}}
	options := RecursiveFixpointOptions{Columns: 1, Union: RecursiveUnionDistinct}
	var kernel RecursiveFixpoint
	if _, err := kernel.Run(program, options); err != nil {
		t.Fatal(err)
	}
	if cap(kernel.result.cells) == 0 || cap(kernel.working.cells) == 0 ||
		cap(kernel.identity.entries) == 0 {
		t.Fatal("run did not retain reusable storage")
	}
	kernel.Release()
	if cap(kernel.result.cells) != 0 || cap(kernel.working.cells) != 0 ||
		cap(kernel.identity.entries) != 0 || cap(kernel.identity.slots) != 0 {
		t.Fatal("Release retained recursive storage")
	}
	result, err := kernel.Run(program, options)
	if err != nil || result.Rows() != 64 {
		t.Fatalf("post-Release run = rows %d, err %v", result.Rows(), err)
	}
}

func TestRecursiveFixpointRejectsUnboundedOrInvalidConfiguration(t *testing.T) {
	program := &recursiveTestProgram{}
	var kernel RecursiveFixpoint
	for _, options := range []RecursiveFixpointOptions{
		{},
		{Columns: 1, Union: RecursiveUnionMode(99)},
		{Columns: 1, Union: RecursiveUnionAll, MaxRows: -2},
		{Columns: 1, Union: RecursiveUnionAll, MaxIterations: -1, MaxRows: -1, MaxBytes: -1},
	} {
		if _, err := kernel.Run(program, options); !errors.Is(err, ErrRecursiveConfig) {
			t.Fatalf("options %+v error = %v, want config error", options, err)
		}
	}
	if _, err := kernel.Run(nil, RecursiveFixpointOptions{Columns: 1}); !errors.Is(err, ErrRecursiveProgram) {
		t.Fatalf("nil program error = %v", err)
	}
}

func TestRecursiveFixpointConcurrentUseIsRejectedWithoutCorruptingOwner(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	program := &recursiveTestProgram{anchor: func(out *RecursiveAppender) error {
		close(entered)
		<-release
		row := [1]Cell{recursiveTestInt(7)}
		return out.AppendRow(row[:])
	}}
	options := RecursiveFixpointOptions{Columns: 1, Union: RecursiveUnionAll}
	var kernel RecursiveFixpoint
	resultCh := make(chan RecursiveResult, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := kernel.Run(program, options)
		resultCh <- result
		errCh <- err
	}()
	<-entered
	if result, err := kernel.Run(program, options); !errors.Is(err, ErrRecursiveInUse) || result.Rows() != 0 {
		t.Fatalf("overlapping run = rows %d, err %v", result.Rows(), err)
	}
	close(release)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if result := <-resultCh; !reflect.DeepEqual(recursiveResultInts(t, result), []int{7}) {
		t.Fatalf("owner result = %v", recursiveResultInts(t, result))
	}
}

func TestRecursiveFixpointIndependentKernelsRunConcurrently(t *testing.T) {
	const workers = 16
	var wait sync.WaitGroup
	wait.Add(workers)
	errs := make(chan error, workers)
	for worker := range workers {
		worker := worker
		go func() {
			defer wait.Done()
			row := [1]Cell{recursiveTestInt(worker)}
			program := &recursiveTestProgram{anchor: func(out *RecursiveAppender) error {
				return out.AppendRow(row[:])
			}}
			var kernel RecursiveFixpoint
			result, err := kernel.Run(program, RecursiveFixpointOptions{
				Columns: 1, Union: RecursiveUnionDistinct,
			})
			if err == nil {
				value, ok := result.Cell(0, 0).Int64()
				if result.Rows() != 1 || !ok || value != int64(worker) {
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

func TestRecursiveFixpointRandomizedGraphDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(0x5eed))
	for trial := 0; trial < 200; trial++ {
		nodes := 1 + rng.Intn(8)
		graph := make([][]int, nodes)
		for source := range graph {
			for target := range nodes {
				if source < target && rng.Intn(4) == 0 {
					graph[source] = append(graph[source], target)
				}
			}
		}
		got := runRecursiveTestGraph(t, graph, RecursiveUnionAll)
		want := recursiveReferenceGraph(graph, false)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("trial %d UNION ALL graph %v: got %v, want %v", trial, graph, got, want)
		}

		for source := range graph {
			graph[source] = graph[source][:0]
			for target := range nodes {
				if rng.Intn(4) == 0 {
					graph[source] = append(graph[source], target)
				}
			}
		}
		got = runRecursiveTestGraph(t, graph, RecursiveUnionDistinct)
		want = recursiveReferenceGraph(graph, true)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("trial %d DISTINCT graph %v: got %v, want %v", trial, graph, got, want)
		}
	}
}

func runRecursiveTestGraph(t testing.TB, graph [][]int, mode RecursiveUnionMode) []int {
	t.Helper()
	row := [1]Cell{}
	program := &recursiveTestProgram{
		anchor: func(out *RecursiveAppender) error {
			row[0] = recursiveTestInt(0)
			return out.AppendRow(row[:])
		},
		step: func(delta RecursiveDelta, out *RecursiveAppender) error {
			for source := 0; source < delta.Rows(); source++ {
				value, _ := delta.Cell(source, 0).Int64()
				for _, target := range graph[value] {
					row[0] = recursiveTestInt(target)
					if err := out.AppendRow(row[:]); err != nil {
						return err
					}
				}
			}
			return nil
		},
	}
	var kernel RecursiveFixpoint
	result, err := kernel.Run(program, RecursiveFixpointOptions{
		Columns: 1, Union: mode, MaxRows: 100_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	return recursiveResultInts(t, result)
}

func recursiveReferenceGraph(graph [][]int, distinct bool) []int {
	result := []int{0}
	delta := []int{0}
	seen := make([]bool, len(graph))
	seen[0] = true
	for len(delta) != 0 {
		next := make([]int, 0)
		for _, source := range delta {
			for _, target := range graph[source] {
				if distinct && seen[target] {
					continue
				}
				if distinct {
					seen[target] = true
				}
				next = append(next, target)
			}
		}
		result = append(result, next...)
		delta = next
	}
	return result
}
