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
