package query

import "testing"

func BenchmarkSQLCorrelatedExistsDecorrelated(b *testing.B) {
	db := correlatedExistsHeapDatabase(
		b, correlatedExistsOuterDocs, correlatedExistsInnerDocs, true,
	)
	statement, err := PrepareStatement(correlatedExistsSQL(false, true))
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	exec := Exec{Options: ExecOptions{IntermediateBytes: -1}}
	defer exec.Release()
	active := true
	args := []any{&active, &active}
	if _, err := statement.RunInto(&exec, source, args); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		cursor, err := statement.RunInto(&exec, source, args)
		if err != nil {
			b.Fatal(err)
		}
		for cursor.Next() {
			sqlSink += len(cursor.Cell(0).Payload())
		}
	}
}

func BenchmarkSQLCorrelatedNotExistsDecorrelated(b *testing.B) {
	db := correlatedExistsHeapDatabase(
		b, correlatedExistsOuterDocs, correlatedExistsInnerDocs, true,
	)
	statement, err := PrepareStatement(correlatedExistsSQL(true, false))
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Release()
	source := FromDatabase(db.Snapshot(), statement.Collection())
	exec := Exec{Options: ExecOptions{IntermediateBytes: -1}}
	defer exec.Release()
	if _, err := statement.RunInto(&exec, source, nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		cursor, err := statement.RunInto(&exec, source, nil)
		if err != nil {
			b.Fatal(err)
		}
		for cursor.Next() {
			sqlSink += len(cursor.Cell(0).Payload())
		}
	}
}
