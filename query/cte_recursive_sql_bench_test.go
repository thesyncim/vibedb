package query

import "testing"

func TestRecursiveSQLBridgeWarmedExecutionZeroAlloc(t *testing.T) {
	edges := [][2]int{{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {4, 5}, {5, 0}}
	_, snapshot := recursiveStatementDatabase(t, edges)
	statement := prepareRecursiveSQLGraph(t)
	defer statement.Release()
	var exec Exec
	source := FromDatabase(snapshot, statement.Collection())
	args := []any{int64(0), true}
	run := func() {
		cursor, err := statement.RunInto(&exec, source, args)
		if err != nil {
			panic(err)
		}
		rows := 0
		for cursor.Next() {
			rows++
		}
		if rows != 6 {
			panic("unexpected recursive SQL row count")
		}
	}
	run()
	run()
	if got := testing.AllocsPerRun(100, run); got != 0 {
		t.Fatalf("warmed recursive SQL execution allocated %.1f times, want 0", got)
	}
	exec.Release()
}

func BenchmarkRecursiveSQLBridgeWarmedExecution(b *testing.B) {
	edges := [][2]int{{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {4, 5}, {5, 0}}
	_, snapshot := recursiveStatementDatabase(b, edges)
	statement := prepareRecursiveSQLGraph(b)
	defer statement.Release()
	var exec Exec
	source := FromDatabase(snapshot, statement.Collection())
	args := []any{int64(0), true}
	run := func() {
		cursor, err := statement.RunInto(&exec, source, args)
		if err != nil {
			b.Fatal(err)
		}
		for cursor.Next() {
			_ = cursor.Cell(0)
		}
	}
	run()
	run()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		run()
	}
}
