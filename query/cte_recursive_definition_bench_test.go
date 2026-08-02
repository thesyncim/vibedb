package query

import "testing"

func TestRecursiveCTEDefinitionWarmedExecutionZeroAlloc(t *testing.T) {
	edges := [][2]int{
		{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {4, 5}, {5, 0},
	}
	_, snapshot := recursiveStatementDatabase(t, edges)
	fixture := prepareRecursiveDefinitionFixture(
		t, RecursiveCTEShared,
		RecursiveCTELimits{MaxIterations: 32, MaxRows: 256, MaxBytes: -1},
	)
	defer fixture.release()
	var exec Exec
	source := FromDatabase(snapshot, fixture.owner.Collection())
	args := []any{int64(0), true}
	run := func() {
		cursor, err := fixture.owner.RunInto(&exec, source, args)
		if err != nil {
			panic(err)
		}
		rows := 0
		for cursor.Next() {
			rows++
		}
		if rows != 6 || fixture.definition.runEvaluations != 1 {
			panic("unexpected recursive definition publication")
		}
	}
	run()
	run()
	run()
	if got := testing.AllocsPerRun(100, run); got != 0 {
		t.Fatalf("warmed owning recursive definition allocated %.1f times, want 0", got)
	}
	exec.Release()
}

func BenchmarkRecursiveCTEDefinitionWarmedExecution(b *testing.B) {
	edges := [][2]int{
		{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {4, 5}, {5, 0},
	}
	_, snapshot := recursiveStatementDatabase(b, edges)
	fixture := prepareRecursiveDefinitionFixture(
		b, RecursiveCTEShared,
		RecursiveCTELimits{MaxIterations: 32, MaxRows: 256, MaxBytes: -1},
	)
	defer fixture.release()
	var exec Exec
	source := FromDatabase(snapshot, fixture.owner.Collection())
	args := []any{int64(0), true}
	run := func() {
		cursor, err := fixture.owner.RunInto(&exec, source, args)
		if err != nil {
			b.Fatal(err)
		}
		rows := 0
		for cursor.Next() {
			rows++
		}
		if rows != 6 {
			b.Fatalf("rows = %d, want 6", rows)
		}
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
