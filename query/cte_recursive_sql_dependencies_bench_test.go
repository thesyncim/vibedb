package query

import "testing"

func BenchmarkRecursiveSQLBridgeDependencyWarmedExecution(b *testing.B) {
	edges := [][2]int{{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}, {4, 5}}
	_, snapshot := recursiveStatementDatabase(b, edges)
	statement, err := PrepareStatement(recursiveSQLSequentialDefinitions)
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Release()
	var execution Exec
	execution.Options = ExecOptions{
		IntermediateBytes: -1, ResultRows: -1, ResultBytes: -1,
	}
	source := FromDatabase(snapshot, statement.Collection())
	args := []any{int64(0), true, int64(2), true, int64(5)}
	run := func() {
		cursor, runErr := statement.RunInto(&execution, source, args)
		if runErr != nil {
			b.Fatal(runErr)
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
	b.StopTimer()
	statement.discardRelations()
	execution.Release()
}
