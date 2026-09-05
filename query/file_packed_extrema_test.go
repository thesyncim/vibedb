package query

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

func filePackedIntegerExtremaExpected(rows int, value func(int) int64) (int64, int64) {
	minimum, maximum := value(0), value(0)
	for row := 1; row < rows; row++ {
		current := value(row)
		if current < minimum {
			minimum = current
		}
		if current > maximum {
			maximum = current
		}
	}
	return minimum, maximum
}

func assertFilePackedIntegerExtrema(
	tb testing.TB, e *Exec, rows uint64, minimum, maximum int64,
) {
	tb.Helper()
	minColumn, minOK := e.Result.Column("min(n)")
	maxColumn, maxOK := e.Result.Column("max(n)")
	if e.Result.RowCount != 1 || !minOK || !maxOK || len(minColumn.Cells) != 1 || len(maxColumn.Cells) != 1 {
		tb.Fatalf("extrema result=%+v, want one row with min/max", e.Result)
	}
	gotMin, minInt := minColumn.Cells[0].Int64()
	gotMax, maxInt := maxColumn.Cells[0].Int64()
	if !minInt || !maxInt || gotMin != minimum || gotMax != maximum {
		tb.Fatalf("extrema=(%d,%d,%t,%t), want=(%d,%d,true,true)", gotMin, gotMax, minInt, maxInt, minimum, maximum)
	}
	stats := e.Stats
	if stats.Workers != 1 || stats.RowsTotal != rows || stats.RowsScanned != rows ||
		stats.Batches != 0 || stats.CoveringColumns != 1 ||
		stats.TokenFilterRows != 0 || stats.TokenFilterFallbackRows != 0 ||
		stats.PrimaryRangeBounded || stats.IndexBounded || stats.IndexLookups != 0 ||
		stats.IndexPostingPages != 0 || stats.IndexCertificateRows != 0 ||
		stats.IndexRecheckRows != 0 || stats.CandidateRows != 0 ||
		stats.CandidateChunks != 0 || stats.DataSkippedRows != 0 ||
		stats.DataSkippedStripes != 0 {
		tb.Fatalf("extrema stats=%+v, want one worker typed full scan", stats)
	}
}

func TestFilePackedIntegerExtrema(t *testing.T) {
	const rows = filePackedCountTestRows
	snapshot := filePackedCountSnapshot(t, rows)
	minimum, maximum := filePackedIntegerExtremaExpected(rows, func(row int) int64 {
		return int64(filePackedCountNumber(row))
	})
	e := Exec{Options: ExecOptions{Workers: 1}}
	defer e.Release()
	if err := Select(Min("n"), Max("n")).RunInto(&e, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	assertFilePackedIntegerExtrema(t, &e, rows, minimum, maximum)
}

func TestFilePackedIntegerExtremaWide(t *testing.T) {
	const rows = filePackedCountTestRows
	snapshot := filePackedCountWideSnapshot(t, rows)
	minimum, maximum := filePackedIntegerExtremaExpected(rows, filePackedCountWideNumber)
	e := Exec{Options: ExecOptions{Workers: 1}}
	defer e.Release()
	if err := Select(Min("n"), Max("n")).RunInto(&e, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	assertFilePackedIntegerExtrema(t, &e, rows, minimum, maximum)
}

func TestFilePackedIntegerExtremaAllAbsentAndMixedShapes(t *testing.T) {
	makeSnapshot := func(t *testing.T, withValues bool) *durable.Snapshot {
		t.Helper()
		file, err := os.CreateTemp(t.TempDir(), "query-file-packed-extrema-*")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		source := &store.Collection{}
		for row := 0; row < 128; row++ {
			doc := []byte(`{"other":1}`)
			if withValues && row%2 == 0 {
				value := int64(((row*73 + row*row*31) & 1023) - 512)
				doc = fmt.Appendf(nil, `{"n":%d}`, value)
			}
			if _, err := source.Put(fmt.Sprintf("row-%03d", row), doc); err != nil {
				t.Fatal(err)
			}
		}
		options := durable.Options{Collection: store.Options{ChunkDocuments: 64}}
		if _, err := durable.CreateFromPrimary(source, file, options); err != nil {
			t.Fatal(err)
		}
		collection, err := durable.Open(file, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collection.Close() })
		snapshot, err := collection.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = snapshot.Close() })
		return snapshot
	}

	t.Run("all-absent", func(t *testing.T) {
		snapshot := makeSnapshot(t, false)
		e := Exec{Options: ExecOptions{Workers: 1}}
		defer e.Release()
		if err := Select(Min("n"), Max("n")).RunInto(&e, FromFile(snapshot)); err != nil {
			t.Fatal(err)
		}
		minColumn, _ := e.Result.Column("min(n)")
		maxColumn, _ := e.Result.Column("max(n)")
		if !minColumn.Cells[0].IsNull() || !maxColumn.Cells[0].IsNull() {
			t.Fatalf("all-absent extrema=(%s,%s), want NULLs", minColumn.Cells[0], maxColumn.Cells[0])
		}
		if e.Stats.CoveringColumns != 1 || e.Stats.RowsScanned != 128 ||
			e.Stats.TokenFilterRows != 0 || e.Stats.TokenFilterFallbackRows != 0 {
			t.Fatalf("all-absent stats=%+v, want typed scan", e.Stats)
		}
	})

	t.Run("absent-plus-FOR", func(t *testing.T) {
		snapshot := makeSnapshot(t, true)
		e := Exec{Options: ExecOptions{Workers: 1}}
		defer e.Release()
		if err := Select(Min("n"), Max("n")).RunInto(&e, FromFile(snapshot)); err != nil {
			t.Fatal(err)
		}
		minimum, maximum := int64(1<<63-1), int64(-1<<63)
		for row := 0; row < 128; row += 2 {
			value := int64(((row*73 + row*row*31) & 1023) - 512)
			if value < minimum {
				minimum = value
			}
			if value > maximum {
				maximum = value
			}
		}
		assertFilePackedIntegerExtrema(t, &e, 128, minimum, maximum)
	})
}

func TestFilePackedIntegerExtremaDeclinesUnsafeShapes(t *testing.T) {
	const rows = filePackedCountTestRows
	snapshot := filePackedCountSnapshot(t, rows)
	cases := []struct {
		name  string
		query *Query
	}{
		{name: "where", query: Select(Min("n"), Max("n")).Where(Cmp("n", Eq, 1))},
		{name: "sum", query: Select(Sum("n"))},
		{name: "root", query: Select(Min(""), Max(""))},
		{name: "different-paths", query: Select(Min("n"), Max("label"))},
	}
	e := Exec{Options: ExecOptions{Workers: 1}}
	defer e.Release()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.query.RunInto(&e, FromFile(snapshot)); err != nil {
				t.Fatal(err)
			}
			if e.Stats.CoveringColumns != 0 || e.Stats.TokenFilterRows != 0 ||
				e.Stats.TokenFilterFallbackRows != 0 || e.Stats.RowsScanned != rows {
				t.Fatalf("declined %s stats=%+v, want generic full scan", tc.name, e.Stats)
			}
		})
	}
}

func TestFilePackedIntegerExtremaAggregateBudget(t *testing.T) {
	snapshot := filePackedCountSnapshot(t, filePackedCountTestRows)
	e := Exec{Options: ExecOptions{
		Workers: 1, AggregateBytes: aggregateAccBaseBytes,
	}}
	defer e.Release()
	err := Select(Min("n"), Max("n")).RunInto(&e, FromFile(snapshot))
	var budgetErr *AggregateBudgetError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("extrema budget error=%v, want AggregateBudgetError", err)
	}
}
