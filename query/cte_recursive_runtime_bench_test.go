package query

import "testing"

func TestRecursiveCTEWarmedExecutionZeroAlloc(t *testing.T) {
	input := buildRelationSpoolForTest(t, [][]string{
		{"1"}, {"2"}, {"3"}, {"4"}, {"5"}, {"6"}, {"7"}, {"8"},
	})
	defer input.release()
	descriptor := prepareRecursiveEchoDescriptor(
		t, RecursiveUnionDistinct, RecursiveCTEReferenceLocal,
	)
	options := ExecOptions{IntermediateBytes: -1}
	var runtime RecursiveCTERuntime
	var frame statementFrame
	run := func() {
		if err := frame.begin(options); err != nil {
			panic(err)
		}
		result, err := runtime.execute(
			descriptor, fromRelationSpool(&input), &frame, options,
		)
		if err != nil {
			panic(err)
		}
		if result.Rows() != 8 || result.Iterations() != 1 {
			panic("unexpected recursive result")
		}
		if value, ok := result.Cell(7, 0).Int64(); !ok || value != 8 {
			panic("unexpected recursive tail")
		}
		runtime.releaseExecution(&frame)
		if frame.intermediate.used != 0 {
			panic("recursive frame charge leaked")
		}
	}
	run()
	if got := testing.AllocsPerRun(100, run); got != 0 {
		t.Fatalf("warmed recursive CTE execution allocated %.1f times, want 0", got)
	}
	runtime.Release()
}

func TestRecursiveCTEBaseDeltaWarmedExecutionZeroAlloc(t *testing.T) {
	edges := [][2]int{
		{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {4, 5}, {5, 0},
	}
	base := buildRecursiveGraphRelation(t, edges)
	defer base.release()
	descriptor, err := PrepareRecursiveCTEDescriptor(
		"graph", nil,
		&recursiveGraphAnchorPhysicalTerm{start: recursiveGraphIntegerCell(0)},
		&recursiveGraphStepPhysicalTerm{}, RecursiveUnionDistinct,
		RecursiveCTEReferenceLocal,
		RecursiveCTELimits{MaxIterations: 16, MaxRows: 16, MaxBytes: -1},
	)
	if err != nil {
		t.Fatal(err)
	}
	options := ExecOptions{IntermediateBytes: -1}
	var runtime RecursiveCTERuntime
	var frame statementFrame
	run := func() {
		if err := frame.begin(options); err != nil {
			panic(err)
		}
		result, err := runtime.execute(
			descriptor, fromRelationSpool(&base), &frame, options,
		)
		if err != nil {
			panic(err)
		}
		if result.Rows() != 6 || result.Iterations() != 5 {
			panic("unexpected base+delta graph result")
		}
		runtime.releaseExecution(&frame)
	}
	run()
	if got := testing.AllocsPerRun(100, run); got != 0 {
		t.Fatalf("warmed base+delta recursive CTE allocated %.1f times, want 0", got)
	}
	runtime.Release()
}

func BenchmarkRecursiveCTEWarmedExecution(b *testing.B) {
	input := buildRelationSpoolForTest(b, [][]string{
		{"1"}, {"2"}, {"3"}, {"4"}, {"5"}, {"6"}, {"7"}, {"8"},
	})
	defer input.release()
	descriptor := prepareRecursiveEchoDescriptor(
		b, RecursiveUnionDistinct, RecursiveCTEReferenceLocal,
	)
	options := ExecOptions{IntermediateBytes: -1}
	var runtime RecursiveCTERuntime
	var frame statementFrame
	run := func() {
		if err := frame.begin(options); err != nil {
			b.Fatal(err)
		}
		result, err := runtime.execute(
			descriptor, fromRelationSpool(&input), &frame, options,
		)
		if err != nil {
			b.Fatal(err)
		}
		if result.Rows() != 8 {
			b.Fatalf("rows = %d, want 8", result.Rows())
		}
		runtime.releaseExecution(&frame)
	}
	run()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		run()
	}
}
