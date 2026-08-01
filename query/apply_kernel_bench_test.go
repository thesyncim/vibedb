package query

import "testing"

type applyBenchmarkSource struct{ cells []Cell }

func (s *applyBenchmarkSource) Rows() int    { return len(s.cells) }
func (s *applyBenchmarkSource) Columns() int { return 1 }
func (s *applyBenchmarkSource) Cell(row, _ int) Cell {
	return s.cells[row]
}

type applyBenchmarkProgram struct {
	row [2]Cell
}

func (p *applyBenchmarkProgram) Bind(
	left ApplyLeftRow,
	parameters *ApplyParameterBinder,
) error {
	return parameters.Append(left.Cell(0))
}

func (p *applyBenchmarkProgram) Right(
	parameters ApplyParameters,
	out *ApplyRightAppender,
) error {
	key, _ := parameters.Cell(0).Int64()
	if key == 0 {
		return nil
	}
	p.row[0] = Cell{kind: TypeNumber, flag: cellInteger, word: uint64(key * 10)}
	p.row[1] = Cell{kind: TypeNumber, flag: cellInteger, word: uint64(key*10 + 1)}
	return out.AppendRow(p.row[:])
}

func newApplyBenchmarkSource(rows, identities int) *applyBenchmarkSource {
	source := &applyBenchmarkSource{cells: make([]Cell, rows)}
	for row := range source.cells {
		source.cells[row] = Cell{
			kind: TypeNumber, flag: cellInteger, word: uint64(row % identities),
		}
	}
	return source
}

func BenchmarkApplyKernelCrossCacheDisabledWarm(b *testing.B) {
	source := newApplyBenchmarkSource(64, 64)
	program := new(applyBenchmarkProgram)
	options := ApplyOptions{
		Kind: ApplyCross, RightColumns: 2, ParameterColumns: 1,
		Memoization: ApplyMemoizationNone,
	}
	benchmarkApplyKernelWarm(b, source, program, options, 63)
}

func BenchmarkApplyKernelLeftExactMemoizationWarm(b *testing.B) {
	source := newApplyBenchmarkSource(64, 8)
	program := new(applyBenchmarkProgram)
	options := ApplyOptions{
		Kind: ApplyLeft, RightColumns: 2, ParameterColumns: 1,
		Memoization: ApplyMemoizationExact,
	}
	benchmarkApplyKernelWarm(b, source, program, options, 64)
}

func benchmarkApplyKernelWarm(
	b *testing.B,
	source *applyBenchmarkSource,
	program *applyBenchmarkProgram,
	options ApplyOptions,
	wantRows int,
) {
	b.Helper()
	var kernel ApplyKernel
	result, err := kernel.Run(source, program, options)
	if err != nil || result.Rows() != wantRows {
		b.Fatalf("warm-up rows=%d err=%v, want %d", result.Rows(), err, wantRows)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err = kernel.Run(source, program, options)
		if err != nil || result.Rows() != wantRows {
			b.Fatalf("rows=%d err=%v, want %d", result.Rows(), err, wantRows)
		}
	}
}

func TestApplyKernelWarmExecutionAllocations(t *testing.T) {
	tests := []struct {
		name     string
		options  ApplyOptions
		wantRows int
	}{
		{
			name: "cache disabled",
			options: ApplyOptions{
				Kind: ApplyCross, RightColumns: 2, ParameterColumns: 1,
				Memoization: ApplyMemoizationNone,
			},
			wantRows: 63,
		},
		{
			name: "exact memoization",
			options: ApplyOptions{
				Kind: ApplyLeft, RightColumns: 2, ParameterColumns: 1,
				Memoization: ApplyMemoizationExact,
			},
			wantRows: 64,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			identities := 64
			if test.options.Memoization == ApplyMemoizationExact {
				identities = 8
			}
			source := newApplyBenchmarkSource(64, identities)
			program := new(applyBenchmarkProgram)
			var kernel ApplyKernel
			if _, err := kernel.Run(source, program, test.options); err != nil {
				t.Fatal(err)
			}
			var result ApplyResult
			var err error
			allocations := testing.AllocsPerRun(100, func() {
				result, err = kernel.Run(source, program, test.options)
			})
			if err != nil || result.Rows() != test.wantRows {
				t.Fatalf("rows=%d err=%v, want %d", result.Rows(), err, test.wantRows)
			}
			if allocations != 0 {
				t.Fatalf("warmed Run allocations = %.2f, want 0", allocations)
			}
		})
	}
}
