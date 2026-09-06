package query

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

func TestRunFileCompactDataSkippingReopenAndMutation(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "query-file-skip-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	const documents = 2048
	source := &store.Collection{}
	padding := strings.Repeat("x", 512)
	for row := range documents {
		raw := fmt.Appendf(nil,
			`{"id":%d,"score":%d,"padding":%q}`,
			row, row, padding,
		)
		if _, err := source.Put(fmt.Sprintf("key-%05d", row), raw); err != nil {
			t.Fatal(err)
		}
	}
	options := durable.Options{
		SkipIndexes: []string{"/score"},
		Indexes: []store.IndexDefinition{{
			Name: "by_id", Paths: []string{"/id"},
		}},
		Durability:       durable.DurabilityAsyncVisible,
		InlineValueBytes: 2048,
		MaxDocumentBytes: 2048,
	}
	if _, err := durable.CreateFromPrimary(source, file, options); err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Open(file, durable.Options{
		Durability: durable.DurabilityAsyncVisible,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	run := func(
		snapshot *durable.Snapshot,
		op Op,
		needle, want int,
	) ExecStats {
		t.Helper()
		query := Select(Path("id")).Where(Cmp("score", op, needle))
		var execution Exec
		if err := query.RunInto(&execution, FromFile(snapshot)); err != nil {
			t.Fatal(err)
		}
		if execution.Result.RowCount != want {
			t.Fatalf("score op=%d needle=%d result = %+v, want %d", op, needle, execution.Result, want)
		}
		return execution.Stats
	}

	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	needleRaw := []byte("1900")
	needle, err := vibejson.BuildIndex(needleRaw, make([]vibejson.IndexEntry, 1))
	if err != nil {
		t.Fatal(err)
	}
	var direct durable.DataSkippingFilter
	if enabled, err := snapshot.CompileDataSkippingFilter(
		&direct, []durable.DataSkippingPredicate{{
			Path: "/score", Op: durable.DataSkippingGreaterEqual, Value: needle,
		}},
	); err != nil || !enabled {
		t.Fatalf("direct data-skipping compile = %v, %v", enabled, err)
	}
	visited := 0
	if _, err := snapshot.RangeDataSkippingRawBuffer(
		&direct, nil, func(_, _ []byte) error { visited++; return nil },
	); err != nil {
		t.Fatal(err)
	}
	if skipped := direct.Metrics(); skipped.Stripes == 0 {
		t.Fatalf("direct data-skipping scan visited=%d skipped=%+v", visited, skipped)
	}
	visit := func(_, _ []byte) error { return nil }
	var scanErr error
	var scanScratch []byte
	if allocs := testing.AllocsPerRun(5, func() {
		scanScratch, scanErr = snapshot.RangeDataSkippingRawBuffer(
			&direct, scanScratch, visit,
		)
	}); allocs != 0 || scanErr != nil {
		t.Fatalf("warm data-skipping scan allocations/error = %v/%v", allocs, scanErr)
	}
	if enabled, err := snapshot.CompileDataSkippingFilter(
		&direct, []durable.DataSkippingPredicate{{
			Path: "/score", Op: durable.DataSkippingOp(255), Value: needle,
		}},
	); err != nil || enabled {
		t.Fatalf("invalid data-skipping operator = %v, %v", enabled, err)
	}
	for _, comparison := range []struct {
		op     Op
		needle int
		want   int
	}{
		{Eq, 1900, 1},
		{Lt, 100, 100},
		{Le, 100, 101},
		{Gt, 1900, documents - 1901},
		{Ge, 1900, documents - 1900},
	} {
		stats := run(snapshot, comparison.op, comparison.needle, comparison.want)
		if stats.DataSkippedRows == 0 || stats.DataSkippedStripes == 0 ||
			stats.RowsScanned >= documents ||
			stats.RowsScanned+stats.DataSkippedRows != documents {
			t.Fatalf("data-skipping op=%d stats = %+v", comparison.op, stats)
		}
	}
	impossible := Select(Count()).Where(And(
		Cmp("score", Ge, 10), Cmp("score", Lt, 10),
	))
	var impossibleExecution Exec
	if err := impossible.RunInto(&impossibleExecution, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	impossibleCount, ok := impossibleExecution.Result.Columns[0].Cells[0].Int64()
	if !ok || impossibleCount != 0 || impossibleExecution.Stats.RowsScanned != 0 ||
		impossibleExecution.Stats.DataSkippedRows != documents {
		t.Fatalf(
			"impossible bounds result/stats = %+v / %+v",
			impossibleExecution.Result, impossibleExecution.Stats,
		)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}

	// Replacing an existing minimum-range row with a new outlier must widen or
	// rebuild its summary before publication. A stale maximum would skip the
	// only matching stripe and return a false negative.
	if _, err := collection.Put(
		[]byte("key-00000"),
		fmt.Appendf(nil, `{"id":0,"score":9999,"padding":%q}`, padding),
	); err != nil {
		t.Fatal(err)
	}
	mutated, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer mutated.Close()
	query := Select(Path("id")).Where(Cmp("score", Gt, 9000))
	var execution Exec
	if err := query.RunInto(&execution, FromFile(mutated)); err != nil {
		t.Fatal(err)
	}
	got, ok := execution.Result.Columns[0].Cells[0].Int64()
	if execution.Result.RowCount != 1 || !ok || got != 0 ||
		execution.Stats.DataSkippedStripes == 0 {
		t.Fatalf("mutated outlier result/stats = %+v / %+v", execution.Result, execution.Stats)
	}
}

func dataSkippingIntegerSnapshot(
	tb testing.TB,
	name string,
	rows int,
	field string,
	skipIndexes []string,
	value func(int) int64,
) *durable.Snapshot {
	tb.Helper()
	file, err := os.CreateTemp(tb.TempDir(), "query-file-skip-admission-"+name+"-*")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = file.Close() })
	source := &store.Collection{}
	padding := strings.Repeat("x", 512)
	for row := range rows {
		doc := fmt.Appendf(nil,
			`{"id":%d,%q:%d,"padding":%q}`,
			row, field, value(row), padding,
		)
		if _, err := source.Put(fmt.Sprintf("row-%07d", row), doc); err != nil {
			tb.Fatalf("source row %d: %v", row, err)
		}
	}
	options := durable.Options{
		SkipIndexes:      skipIndexes,
		Collection:       store.Options{ChunkDocuments: 64},
		Durability:       durable.DurabilityAsyncVisible,
		InlineValueBytes: 2048,
		MaxDocumentBytes: 2048,
	}
	if _, err := durable.CreateFromPrimary(source, file, options); err != nil {
		tb.Fatalf("CreateFromPrimary %s: %v", name, err)
	}
	collection, err := durable.Open(file, durable.Options{
		Durability: durable.DurabilityAsyncVisible,
	})
	if err != nil {
		tb.Fatalf("Open %s: %v", name, err)
	}
	tb.Cleanup(func() { _ = collection.Close() })
	snapshot, err := collection.Snapshot()
	if err != nil {
		tb.Fatalf("Snapshot %s: %v", name, err)
	}
	tb.Cleanup(func() { _ = snapshot.Close() })
	return snapshot
}

func runDataSkippingIntegerCount(
	t testing.TB,
	snapshot *durable.Snapshot,
	query *Query,
) (int64, ExecStats) {
	t.Helper()
	var execution Exec
	execution.Options.Workers = 1
	if err := query.RunInto(&execution, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	column, ok := execution.Result.Column("count(*)")
	if !ok || len(column.Cells) != 1 {
		t.Fatalf("integer skip result = %+v", execution.Result)
	}
	count, ok := column.Cells[0].Int64()
	if !ok {
		t.Fatalf("integer skip count is not int64: %+v", execution.Result)
	}
	stats := execution.Stats
	execution.Release()
	return count, stats
}

func TestRunFileCompactIntegerSkipAdmission(t *testing.T) {
	const prefixRows = 8192
	prefixValue := func(row int) int64 { return int64(row) }
	prefixInterval := func() *Query {
		return Select(Count()).Where(And(
			Cmp("score", Ge, int64(prefixRows-64)),
			Cmp("score", Lt, int64(prefixRows)),
		))
	}
	assertPrefixNative := func(t *testing.T, snapshot *durable.Snapshot) {
		t.Helper()
		count, stats := runDataSkippingIntegerCount(t, snapshot, prefixInterval())
		if count != 64 || stats.RowsScanned != prefixRows ||
			stats.TokenFilterRows != prefixRows || stats.TokenFilterFallbackRows != 0 ||
			stats.DataSkippedRows != 0 || stats.DataSkippedStripes != 0 ||
			stats.Batches != 0 {
			t.Fatalf("prefix native count/stats = %d/%+v", count, stats)
		}
		count, stats = runDataSkippingIntegerCount(t, snapshot,
			Select(Count()).Where(Cmp("score", Ge, int64(prefixRows-64))))
		if count != 64 || stats.RowsScanned != prefixRows ||
			stats.TokenFilterRows != prefixRows || stats.TokenFilterFallbackRows != 0 ||
			stats.DataSkippedRows != 0 || stats.DataSkippedStripes != 0 ||
			stats.Batches != 0 {
			t.Fatalf("prefix ordered native count/stats = %d/%+v", count, stats)
		}
	}

	t.Run("prefix-matching-skip-selective", func(t *testing.T) {
		snapshot := dataSkippingIntegerSnapshot(
			t, "prefix-matching", prefixRows, "score", []string{"/score"}, prefixValue,
		)
		count, stats := runDataSkippingIntegerCount(t, snapshot, prefixInterval())
		if count != 64 || stats.DataSkippedRows == 0 ||
			stats.DataSkippedStripes == 0 || stats.RowsScanned >= prefixRows ||
			stats.RowsScanned+stats.DataSkippedRows != prefixRows {
			t.Fatalf("prefix selective count/stats = %d/%+v", count, stats)
		}
		count, stats = runDataSkippingIntegerCount(t, snapshot,
			Select(Count()).Where(Cmp("score", Ge, int64(prefixRows-64))))
		if count != 64 || stats.DataSkippedRows == 0 ||
			stats.DataSkippedStripes == 0 || stats.RowsScanned >= prefixRows ||
			stats.RowsScanned+stats.DataSkippedRows != prefixRows {
			t.Fatalf("prefix ordered selective count/stats = %d/%+v", count, stats)
		}
	})

	t.Run("prefix-matching-skip-impossible", func(t *testing.T) {
		snapshot := dataSkippingIntegerSnapshot(
			t, "prefix-impossible", prefixRows, "score", []string{"/score"}, prefixValue,
		)
		query := Select(Count()).Where(And(
			Cmp("score", Ge, int64(10)), Cmp("score", Lt, int64(10)),
		))
		count, stats := runDataSkippingIntegerCount(t, snapshot, query)
		if count != 0 || stats.RowsScanned != 0 ||
			stats.DataSkippedRows != prefixRows || stats.DataSkippedStripes == 0 {
			t.Fatalf("prefix impossible count/stats = %d/%+v", count, stats)
		}
	})

	t.Run("prefix-no-skip-native", func(t *testing.T) {
		snapshot := dataSkippingIntegerSnapshot(
			t, "prefix-none", prefixRows, "score", nil, prefixValue,
		)
		assertPrefixNative(t, snapshot)
	})

	t.Run("prefix-unrelated-skip-native", func(t *testing.T) {
		snapshot := dataSkippingIntegerSnapshot(
			t, "prefix-unrelated", prefixRows, "score", []string{"/other"}, prefixValue,
		)
		assertPrefixNative(t, snapshot)
	})

	t.Run("for-matching-skip-stays-native", func(t *testing.T) {
		const rows = filePackedCountTestRows
		snapshot := dataSkippingIntegerSnapshot(
			t, "for-matching", rows, "n", []string{"/n"},
			func(row int) int64 { return int64(filePackedCountNumber(row)) },
		)
		cases := []struct {
			name  string
			query *Query
			want  int64
		}{
			{
				name:  "all",
				query: Select(Count()).Where(Cmp("n", Ge, int64(-1<<63))),
				want:  rows,
			},
			{
				name: "selective",
				query: Select(Count()).Where(And(
					Cmp("n", Ge, int64(-100)), Cmp("n", Lt, int64(100)),
				)),
				want: func() int64 {
					var want int64
					for row := range rows {
						value := int64(filePackedCountNumber(row))
						if value >= -100 && value < 100 {
							want++
						}
					}
					return want
				}(),
			},
			{
				name: "empty",
				query: Select(Count()).Where(And(
					Cmp("n", Ge, int64(10)), Cmp("n", Lt, int64(10)),
				)),
				want: 0,
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				count, stats := runDataSkippingIntegerCount(t, snapshot, tc.query)
				if count != tc.want || stats.RowsScanned != rows ||
					stats.TokenFilterRows != rows || stats.TokenFilterFallbackRows != 0 ||
					stats.DataSkippedRows != 0 || stats.DataSkippedStripes != 0 ||
					stats.Batches != 0 {
					t.Fatalf("FOR native count/stats = %d/%+v want %d", count, stats, tc.want)
				}
			})
		}
	})
}
