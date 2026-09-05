package query

import "testing"

func TestFilePackedIntegerIntervalDeclinesNestedAndGrouped(t *testing.T) {
	const rows = filePackedCountTestRows
	snapshot := filePackedCountWideSnapshot(t, rows)
	e := Exec{Options: ExecOptions{Workers: 1}}
	defer e.Release()

	t.Run("nested-residual", func(t *testing.T) {
		q := Select(Count()).Where(And(
			And(Cmp("n", Ge, int64(0)), Cmp("n", Lt, int64(1))),
			Exists("n"),
		))
		if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
			t.Fatal(err)
		}
		if e.Result.RowCount != 1 || e.Stats.RowsScanned != rows ||
			e.Stats.TokenFilterRows != 0 || e.Stats.TokenFilterFallbackRows != 0 {
			t.Fatalf("nested interval unexpectedly used direct lane: result=%+v stats=%+v", e.Result, e.Stats)
		}
	})

	t.Run("grouped", func(t *testing.T) {
		q := Select(Path("n"), Count()).Where(And(
			Cmp("n", Ge, int64(-1)), Cmp("n", Lt, int64(1)),
		)).GroupBy("n")
		if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
			t.Fatal(err)
		}
		if e.Stats.RowsScanned != rows || e.Stats.TokenFilterRows != 0 ||
			e.Stats.TokenFilterFallbackRows != 0 {
			t.Fatalf("grouped interval unexpectedly used direct lane: result=%+v stats=%+v", e.Result, e.Stats)
		}
	})
}
