package query

import "testing"

type setTreeBenchmarkSource struct{ cells []Cell }

func (s *setTreeBenchmarkSource) Rows() int    { return len(s.cells) }
func (s *setTreeBenchmarkSource) Columns() int { return 1 }
func (s *setTreeBenchmarkSource) Cell(row, _ int) Cell {
	return s.cells[row]
}

type setTreeBenchmarkProgram struct{ sources []SetTreeSource }

func (p *setTreeBenchmarkProgram) Leaf(
	source int,
	_ *CancelFlag,
) (SetTreeSource, error) {
	return p.sources[source], nil
}

func newSetTreeBenchmark(
	leaves, rows int,
) (*SetTreeBuilder, SetTreePlan, *setTreeBenchmarkProgram) {
	specs := make([]SetTreeLeafSpec, leaves)
	operations := make([]SetTreeOperation, leaves-1)
	program := &setTreeBenchmarkProgram{sources: make([]SetTreeSource, leaves)}
	for leaf := range leaves {
		specs[leaf] = SetTreeLeafSpec{Source: leaf, Columns: 1}
		cells := make([]Cell, rows)
		for row := range cells {
			cells[row] = Cell{
				kind: TypeNumber, flag: cellInteger,
				word: uint64(int64((leaf*7 + row) % 31)),
			}
		}
		program.sources[leaf] = &setTreeBenchmarkSource{cells: cells}
		if leaf != leaves-1 {
			operations[leaf] = SetTreeOperation(leaf % 6)
		}
	}
	builder := new(SetTreeBuilder)
	plan, err := builder.BuildChain(specs, operations)
	if err != nil {
		panic(err)
	}
	return builder, plan, program
}

func BenchmarkSetTreeExecutorMixedChainWarm(b *testing.B) {
	builder, plan, program := newSetTreeBenchmark(16, 32)
	defer builder.Release()
	benchmarkSetTreeWarm(b, plan, program)
}

func BenchmarkSetTreeExecutorDeepUnionAllWarm(b *testing.B) {
	const leaves = 32
	specs := make([]SetTreeLeafSpec, leaves)
	operations := make([]SetTreeOperation, leaves-1)
	program := &setTreeBenchmarkProgram{sources: make([]SetTreeSource, leaves)}
	for leaf := range leaves {
		specs[leaf] = SetTreeLeafSpec{Source: leaf, Columns: 1}
		program.sources[leaf] = &setTreeBenchmarkSource{cells: []Cell{{
			kind: TypeNumber, flag: cellInteger, word: uint64(leaf),
		}}}
		if leaf != leaves-1 {
			operations[leaf] = SetTreeUnionAll
		}
	}
	var builder SetTreeBuilder
	plan, err := builder.BuildChain(specs, operations)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkSetTreeWarm(b, plan, program)
}

func BenchmarkSetTreeBuilderWarm(b *testing.B) {
	const leaves = 64
	specs := make([]SetTreeLeafSpec, leaves)
	operations := make([]SetTreeOperation, leaves-1)
	for leaf := range leaves {
		specs[leaf] = SetTreeLeafSpec{Source: leaf, Columns: 2}
		if leaf != leaves-1 {
			operations[leaf] = SetTreeOperation(leaf % 6)
		}
	}
	var builder SetTreeBuilder
	plan, err := builder.BuildChain(specs, operations)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		plan, err = builder.BuildChain(specs, operations)
		if err != nil || len(plan.Nodes) != leaves*2-1 {
			b.Fatalf("nodes=%d err=%v", len(plan.Nodes), err)
		}
	}
	setTreeBenchmarkSink += plan.Root
}

func benchmarkSetTreeWarm(
	b *testing.B,
	plan SetTreePlan,
	program SetTreeProgram,
) {
	b.Helper()
	var executor SetTreeExecutor
	result, err := executor.Run(plan, program, SetTreeOptions{})
	if err != nil {
		b.Fatal(err)
	}
	wantRows := result.Rows()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err = executor.Run(plan, program, SetTreeOptions{})
		if err != nil || result.Rows() != wantRows {
			b.Fatalf("rows=%d err=%v, want %d", result.Rows(), err, wantRows)
		}
	}
	setTreeBenchmarkSink += result.Rows()
}

func TestSetTreeExecutorWarmExecutionAllocations(t *testing.T) {
	builder, plan, program := newSetTreeBenchmark(16, 32)
	defer builder.Release()
	var executor SetTreeExecutor
	result, err := executor.Run(plan, program, SetTreeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wantRows := result.Rows()
	allocations := testing.AllocsPerRun(100, func() {
		result, err = executor.Run(plan, program, SetTreeOptions{})
	})
	if err != nil || result.Rows() != wantRows {
		t.Fatalf("rows=%d err=%v, want %d", result.Rows(), err, wantRows)
	}
	if allocations != 0 {
		t.Fatalf("warmed set-tree Run allocations = %.2f, want 0", allocations)
	}
}

func TestSetTreeBuilderAndIntegratedExecutionWarmAllocations(t *testing.T) {
	builder, plan, program := newSetTreeBenchmark(16, 32)
	defer builder.Release()
	specs := make([]SetTreeLeafSpec, 16)
	operations := make([]SetTreeOperation, 15)
	for leaf := range specs {
		specs[leaf] = SetTreeLeafSpec{Source: leaf, Columns: 1}
		if leaf != len(specs)-1 {
			operations[leaf] = SetTreeOperation(leaf % 6)
		}
	}
	var executor SetTreeExecutor
	result, err := executor.Run(plan, program, SetTreeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wantRows := result.Rows()

	builderAllocs := testing.AllocsPerRun(100, func() {
		plan, err = builder.BuildChain(specs, operations)
	})
	if err != nil {
		t.Fatal(err)
	}
	if builderAllocs != 0 {
		t.Fatalf("warmed BuildChain allocations = %.2f, want 0", builderAllocs)
	}

	integratedAllocs := testing.AllocsPerRun(100, func() {
		plan, err = builder.BuildChain(specs, operations)
		if err == nil {
			result, err = executor.Run(plan, program, SetTreeOptions{})
		}
	})
	if err != nil || result.Rows() != wantRows {
		t.Fatalf("integrated rows=%d err=%v, want %d", result.Rows(), err, wantRows)
	}
	if integratedAllocs != 0 {
		t.Fatalf("warmed BuildChain+Run allocations = %.2f, want 0", integratedAllocs)
	}
}

var setTreeBenchmarkSink int
