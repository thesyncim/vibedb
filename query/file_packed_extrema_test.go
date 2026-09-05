package query

import (
	"errors"
	"fmt"
	"os"
	"strings"
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

// filePackedIntegerExtremaSnapshot builds both sides of an oracle pair from
// one immutable input. The heap snapshot drives the authoritative generic
// executor while CreateFromPrimary supplies the compact durable image whose
// storage-native lane is under test.
func filePackedIntegerExtremaSnapshot(
	tb testing.TB, rows int, options durable.Options, document func(int) []byte,
) (*durable.Snapshot, store.Snapshot) {
	tb.Helper()
	file, err := os.CreateTemp(tb.TempDir(), "query-file-packed-extrema-pair-*")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = file.Close() })
	source := &store.Collection{}
	for row := range rows {
		if _, err := source.Put(fmt.Sprintf("row-%07d", row), document(row)); err != nil {
			tb.Fatalf("source row %d: %v", row, err)
		}
	}
	if options.Collection.ChunkDocuments == 0 {
		options.Collection.ChunkDocuments = 64
	}
	if _, err := durable.CreateFromPrimary(source, file, options); err != nil {
		tb.Fatalf("CreateFromPrimary: %v", err)
	}
	collection, err := durable.Open(file, options)
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}
	tb.Cleanup(func() { _ = collection.Close() })
	snapshot, err := collection.Snapshot()
	if err != nil {
		tb.Fatalf("Snapshot: %v", err)
	}
	tb.Cleanup(func() { _ = snapshot.Close() })
	heap, err := source.Snapshot()
	if err != nil {
		tb.Fatalf("heap Snapshot: %v", err)
	}
	return snapshot, heap
}

// filePackedIntegerExtremaOverflowSnapshot starts from a clean bulk image and
// appends one out-of-line row through the ordinary durable mutation path.
// CreateFromPrimary intentionally rejects overflow input, so the mutation is
// the production way to obtain an overflow-bearing compact leaf.
func filePackedIntegerExtremaOverflowSnapshot(
	tb testing.TB, rows int, document func(int) []byte,
) (*durable.Snapshot, store.Snapshot) {
	tb.Helper()
	file, err := os.CreateTemp(tb.TempDir(), "query-file-packed-extrema-overflow-*")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = file.Close() })
	options := durable.Options{
		Collection:       store.Options{ChunkDocuments: 64},
		InlineValueBytes: 256,
		MaxDocumentBytes: 8 << 10,
	}
	source := &store.Collection{}
	for row := 0; row < rows-1; row++ {
		if _, err := source.Put(fmt.Sprintf("row-%07d", row), document(row)); err != nil {
			tb.Fatalf("source row %d: %v", row, err)
		}
	}
	if _, err := durable.CreateFromPrimary(source, file, options); err != nil {
		tb.Fatalf("CreateFromPrimary: %v", err)
	}
	collection, err := durable.Open(file, options)
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}
	tb.Cleanup(func() { _ = collection.Close() })
	last := document(rows - 1)
	if _, err := source.Put(fmt.Sprintf("row-%07d", rows-1), last); err != nil {
		tb.Fatalf("source overflow row: %v", err)
	}
	if _, err := collection.Put([]byte(fmt.Sprintf("row-%07d", rows-1)), last); err != nil {
		tb.Fatalf("durable overflow row: %v", err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		tb.Fatalf("Snapshot: %v", err)
	}
	tb.Cleanup(func() { _ = snapshot.Close() })
	heap, err := source.Snapshot()
	if err != nil {
		tb.Fatalf("heap Snapshot: %v", err)
	}
	return snapshot, heap
}

func assertFilePackedIntegerExtremaResultEqual(
	tb testing.TB, got, want *Exec,
) {
	tb.Helper()
	for _, name := range []string{"min(n)", "max(n)"} {
		gotColumn, gotOK := got.Result.Column(name)
		wantColumn, wantOK := want.Result.Column(name)
		if !gotOK || !wantOK || len(gotColumn.Cells) != 1 || len(wantColumn.Cells) != 1 {
			tb.Fatalf("%s missing result cells: got=%+v want=%+v", name, gotColumn, wantColumn)
		}
		if string(gotColumn.Cells[0].JSON()) != string(wantColumn.Cells[0].JSON()) {
			tb.Fatalf("%s got=%s want=%s", name, gotColumn.Cells[0].JSON(), wantColumn.Cells[0].JSON())
		}
	}
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

func TestFilePackedIntegerExtremaDeclinesLateNonFORAndOverflowAtomically(t *testing.T) {
	const rows = 2 * (4 << 10)
	for _, tc := range []struct {
		name    string
		doc     func(int) []byte
		options durable.Options
	}{
		{
			name: "late-non-FOR",
			doc: func(row int) []byte {
				if row == rows-1 {
					return []byte(`{"n":"late"}`)
				}
				return fmt.Appendf(nil, `{"n":%d}`, filePackedCountNumber(row))
			},
			options: durable.Options{Collection: store.Options{ChunkDocuments: 64}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot, heap := filePackedIntegerExtremaSnapshot(t, rows, tc.options, tc.doc)
			if snapshot.Len() != rows {
				t.Fatalf("snapshot rows=%d, want %d", snapshot.Len(), rows)
			}
			query := Select(Min("n"), Max("n"))
			var oracle Exec
			oracle.Options.Workers = 1
			if err := query.RunInto(&oracle, FromSnapshot(heap)); err != nil {
				t.Fatalf("generic oracle: %v", err)
			}
			var execution Exec
			execution.Options.Workers = 1
			if err := query.RunInto(&execution, FromFile(snapshot)); err != nil {
				t.Fatalf("durable execution: %v", err)
			}
			assertFilePackedIntegerExtremaResultEqual(t, &execution, &oracle)
			if execution.Stats.CoveringColumns != 0 ||
				execution.Stats.RowsScanned != snapshot.Len() ||
				execution.Stats.Workers != 1 {
				t.Fatalf("declined %s stats=%+v, want generic full scan", tc.name, execution.Stats)
			}
		})
	}
	t.Run("late-overflow", func(t *testing.T) {
		snapshot, heap := filePackedIntegerExtremaOverflowSnapshot(t, rows, func(row int) []byte {
			if row == rows-1 {
				return fmt.Appendf(nil, `{"n":%d,"pad":%q}`, filePackedCountNumber(row), strings.Repeat("x", 1024))
			}
			return fmt.Appendf(nil, `{"n":%d}`, filePackedCountNumber(row))
		})
		if snapshot.Len() != rows {
			t.Fatalf("snapshot rows=%d, want %d", snapshot.Len(), rows)
		}
		query := Select(Min("n"), Max("n"))
		oracle := Exec{Options: ExecOptions{Workers: 1}}
		if err := query.RunInto(&oracle, FromSnapshot(heap)); err != nil {
			t.Fatalf("generic oracle: %v", err)
		}
		execution := Exec{Options: ExecOptions{Workers: 1}}
		if err := query.RunInto(&execution, FromFile(snapshot)); err != nil {
			t.Fatalf("durable execution: %v", err)
		}
		assertFilePackedIntegerExtremaResultEqual(t, &execution, &oracle)
		if execution.Stats.CoveringColumns != 0 ||
			execution.Stats.RowsScanned != snapshot.Len() || execution.Stats.Workers != 1 {
			t.Fatalf("declined late-overflow stats=%+v, want generic full scan", execution.Stats)
		}
	})
}

func TestFilePackedIntegerExtremaSingleAggregateEmptyFallbackAndWarmAllocations(t *testing.T) {
	const rows = 4096
	snapshot := filePackedCountSnapshot(t, rows)
	for _, tc := range []struct {
		name  string
		query *Query
		want  int64
	}{
		{name: "min-only", query: Select(Min("n")), want: -512},
		{name: "max-only", query: Select(Max("n")), want: 511},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var execution Exec
			if err := tc.query.RunInto(&execution, FromFile(snapshot)); err != nil {
				t.Fatal(err)
			}
			column, ok := execution.Result.Column(tc.name[:3] + "(n)")
			if !ok || len(column.Cells) != 1 {
				t.Fatalf("result=%+v", execution.Result)
			}
			value, isInt := column.Cells[0].Int64()
			if !isInt || value != tc.want || execution.Stats.CoveringColumns != 1 ||
				execution.Stats.RowsScanned != rows {
				t.Fatalf("value=%d/%t stats=%+v, want %d and typed full scan", value, isInt, execution.Stats, tc.want)
			}
		})
	}

	emptyFile, err := os.CreateTemp(t.TempDir(), "query-file-packed-extrema-empty-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = emptyFile.Close() })
	emptyOptions := durable.Options{Collection: store.Options{ChunkDocuments: 64}}
	emptySource := &store.Collection{}
	if _, err := emptySource.Put("seed", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.CreateFromPrimary(emptySource, emptyFile, emptyOptions); err != nil {
		t.Fatal(err)
	}
	emptyCollection, err := durable.Open(emptyFile, emptyOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = emptyCollection.Close() })
	if deleted, err := emptyCollection.Delete([]byte("seed")); err != nil || !deleted {
		t.Fatalf("delete empty seed=%t err=%v", deleted, err)
	}
	emptySnapshot, err := emptyCollection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = emptySnapshot.Close() })
	var empty Exec
	if err := Select(Min("n"), Max("n")).RunInto(&empty, FromFile(emptySnapshot)); err != nil {
		t.Fatal(err)
	}
	minColumn, _ := empty.Result.Column("min(n)")
	maxColumn, _ := empty.Result.Column("max(n)")
	if !minColumn.Cells[0].IsNull() || !maxColumn.Cells[0].IsNull() ||
		empty.Stats.CoveringColumns != 1 || empty.Stats.RowsScanned != 0 {
		t.Fatalf("empty result=%+v stats=%+v, want NULL typed empty scan", empty.Result, empty.Stats)
	}

	// Reuse the snapshot and executor so this exercises the warmed durable lane,
	// including its retained scan scratch and aggregate state, rather than fixture
	// setup or result construction.
	e := Exec{Options: ExecOptions{Workers: 1}}
	defer e.Release()
	query := Select(Min("n"), Max("n"))
	if err := query.RunInto(&e, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	warm := func() {
		if err := query.RunInto(&e, FromFile(snapshot)); err != nil {
			panic(err)
		}
		minColumn, _ := e.Result.Column("min(n)")
		maxColumn, _ := e.Result.Column("max(n)")
		if min, ok := minColumn.Cells[0].Int64(); !ok || min != -512 {
			panic("warm min changed")
		}
		if max, ok := maxColumn.Cells[0].Int64(); !ok || max != 511 {
			panic("warm max changed")
		}
	}
	warm()
	if allocs := testing.AllocsPerRun(100, warm); allocs != 0 {
		t.Fatalf("warmed extrema allocations=%v, want 0", allocs)
	}
}

func TestFilePackedIntegerExtremaFallsBackForNonCanonicalValues(t *testing.T) {
	snapshot, heap := filePackedIntegerExtremaSnapshot(
		t, 4, durable.Options{Collection: store.Options{ChunkDocuments: 64}},
		func(row int) []byte {
			switch row {
			case 0:
				return []byte(`{"n":1.0}`)
			case 1:
				return []byte(`{"n":null}`)
			case 2:
				return []byte(`{"n":"2"}`)
			default:
				return []byte(`{"n":3}`)
			}
		},
	)
	query := Select(Min("n"), Max("n"))
	var oracle Exec
	if err := query.RunInto(&oracle, FromSnapshot(heap)); err != nil {
		t.Fatal(err)
	}
	var execution Exec
	if err := query.RunInto(&execution, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	assertFilePackedIntegerExtremaResultEqual(t, &execution, &oracle)
	if execution.Stats.CoveringColumns != 0 || execution.Stats.RowsScanned != 4 {
		t.Fatalf("noncanonical stats=%+v, want generic full scan", execution.Stats)
	}
}
