package query

import "testing"

func BenchmarkSQLLateralPreparedApply(b *testing.B) {
	db := lateralStatementDatabase(b)
	statement, err := PrepareStatement(correlatedLateralSQL)
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	exec := Exec{Options: ExecOptions{IntermediateBytes: -1, JoinPairBytes: -1}}
	defer exec.Release()
	if _, err := statement.RunInto(&exec, source, nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := statement.RunInto(&exec, source, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLLateralProjectedGatePreparedApply(b *testing.B) {
	db := lateralStatementDatabase(b)
	statement, err := PrepareStatement(projectedGatedLateralSQL)
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	exec := Exec{Options: ExecOptions{IntermediateBytes: -1, JoinPairBytes: -1}}
	defer exec.Release()
	if _, err := statement.RunInto(&exec, source, nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := statement.RunInto(&exec, source, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLLateralAllCorrelatedPreparedApply(b *testing.B) {
	db := lateralStatementDatabase(b)
	statement, err := PrepareStatement(allCorrelatedLateralSQL)
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	exec := Exec{Options: ExecOptions{IntermediateBytes: -1, JoinPairBytes: -1}}
	defer exec.Release()
	if _, err := statement.RunInto(&exec, source, nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := statement.RunInto(&exec, source, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLLateralGroupedAggregatePreparedApply(b *testing.B) {
	db := lateralStatementDatabase(b)
	statement, err := PrepareStatement(`
		SELECT a.id, d.total FROM accounts a LEFT JOIN LATERAL (
			SELECT SUM(a.id) AS total FROM items i WHERE i.owner = a.id
			GROUP BY a.id HAVING SUM(a.id) >= ?
		) d ON TRUE`)
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	exec := Exec{Options: ExecOptions{IntermediateBytes: -1, JoinPairBytes: -1}}
	defer exec.Release()
	threshold := Number("1")
	args := []any{&threshold}
	if _, err := statement.RunInto(&exec, source, args); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := statement.RunInto(&exec, source, args); err != nil {
			b.Fatal(err)
		}
	}
}
