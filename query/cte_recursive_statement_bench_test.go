package query

import "testing"

func TestRecursiveCTEStatementWarmedExecutionZeroAlloc(t *testing.T) {
	edges := [][2]int{
		{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {4, 5}, {5, 0},
	}
	_, snapshot := recursiveStatementDatabase(t, edges)
	graph := prepareRecursiveStatementGraph(t, 0, 1)
	defer graph.release()
	options := ExecOptions{IntermediateBytes: -1}
	args := []any{int64(0), true}
	var frame statementFrame
	var runtime RecursiveCTERuntime
	run := func() {
		if err := frame.begin(options); err != nil {
			panic(err)
		}
		frame.args = args
		result, err := runtime.executeStatementTerms(
			graph.descriptor, FromDatabase(snapshot, "seeds"), &frame, options,
		)
		if err != nil {
			panic(err)
		}
		if result.Rows() != 6 || result.Iterations() != 5 {
			panic("unexpected prepared Statement recursive result")
		}
		if tail, ok := result.Cell(5, 0).Int64(); !ok || tail != 5 {
			panic("unexpected prepared Statement recursive tail")
		}
		runtime.releaseExecution(&frame)
		if frame.intermediate.used != 0 ||
			graph.recursive.target.recursiveBinding != nil {
			panic("prepared Statement recursive execution leaked state")
		}
	}
	// Both adapter result arenas and every Statement/fixpoint workspace need a
	// complete high-water cycle before the allocation contract is measured.
	run()
	run()
	run()
	if got := testing.AllocsPerRun(100, run); got != 0 {
		t.Fatalf("warmed prepared Statement recursion allocated %.1f times, want 0", got)
	}
	runtime.Release()
}

func BenchmarkRecursiveCTEStatementWarmedExecution(b *testing.B) {
	edges := [][2]int{
		{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {4, 5}, {5, 0},
	}
	_, snapshot := recursiveStatementDatabase(b, edges)
	graph := prepareRecursiveStatementGraph(b, 0, 1)
	defer graph.release()
	options := ExecOptions{IntermediateBytes: -1}
	args := []any{int64(0), true}
	var frame statementFrame
	var runtime RecursiveCTERuntime
	run := func() {
		if err := frame.begin(options); err != nil {
			b.Fatal(err)
		}
		frame.args = args
		result, err := runtime.executeStatementTerms(
			graph.descriptor, FromDatabase(snapshot, "seeds"), &frame, options,
		)
		if err != nil {
			b.Fatal(err)
		}
		if result.Rows() != 6 {
			b.Fatalf("rows = %d, want 6", result.Rows())
		}
		runtime.releaseExecution(&frame)
	}
	run()
	run()
	run()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		run()
	}
}
