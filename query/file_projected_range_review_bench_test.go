package query

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// This benchmark deliberately uses only APIs available on the base revision,
// so the identical source can measure both implementations. It times complete
// bounded queries and checks their values, including a wide projection control.
func BenchmarkProjectedRangeReview(b *testing.B) {
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		b.Fatal(err)
	}
	for row := range 4096 {
		key := fmt.Sprintf("%05d", row)
		doc := fmt.Appendf(nil, `{"id":%q,"bucket":%d`, key, row%8)
		for field := range 12 {
			doc = fmt.Appendf(doc, `,"field%d":%q`, field,
				fmt.Sprintf("group-%02d-%s", (row+field)%37, strings.Repeat(string(rune('a'+field)), 80)))
		}
		doc = append(doc, '}')
		if err := builder.Append(key, doc); err != nil {
			b.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}
	file, err := os.CreateTemp(b.TempDir(), "projected-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	opts := durable.Options{InlineValueBytes: 4096}
	if _, err := durable.CreateFromPrimary(built, file, opts); err != nil {
		b.Fatal(err)
	}
	collection, err := durable.Open(file, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer collection.Close()
	snapshot, err := collection.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer snapshot.Close()
	for _, scenario := range []string{"covered-narrow", "filtered-narrow", "covered-wide", "filtered-wide"} {
		for _, rows := range []int{32, 256} {
			b.Run(fmt.Sprintf("%s/rows=%d", scenario, rows), func(b *testing.B) {
				columns := []Column{Path("/id"), Path("/bucket")}
				if strings.HasSuffix(scenario, "-wide") {
					for field := range 12 {
						columns = append(columns, Path(fmt.Sprintf("/field%d", field)))
					}
				}
				predicate := And(Cmp("/id", Ge, "01024"), Cmp("/id", Lt, "04096"))
				if strings.HasPrefix(scenario, "filtered-") {
					predicate = And(predicate, Cmp("/bucket", Eq, 5))
				}
				q := Select(columns...).Where(predicate).OrderBy("/id", Asc).Limit(rows)
				span := NewFileRangeSource([]byte("01024"), []byte("04096"), false)
				span.BindPrimaryOrder("/id")
				if !strings.HasPrefix(scenario, "filtered-") {
					span.BindPrimaryPredicate("/id")
				}
				var exec Exec
				defer exec.Release()
				run := func() {
					if err := q.RunInto(&exec, FromFileRange(snapshot, &span)); err != nil {
						b.Fatal(err)
					}
					if exec.Result.RowCount != rows {
						b.Fatalf("rows=%d want=%d", exec.Result.RowCount, rows)
					}
					last := 1024 + rows - 1
					if strings.HasPrefix(scenario, "filtered-") {
						last = 1029 + (rows-1)*8
					}
					value, ok := exec.Result.Columns[1].Cells[rows-1].Int64()
					if !ok || value != int64(last%8) {
						b.Fatalf("last bucket=%d ok=%t want=%d", value, ok, last%8)
					}
				}
				run()
				b.ReportAllocs()
				for b.Loop() {
					run()
				}
				b.ReportMetric(float64(exec.Stats.RowsScanned), "scanned/op")
				b.ReportMetric(float64(exec.Result.RetainedBytes()), "result-B/op")
				b.ReportMetric(float64(len(exec.Result.fileData)), "owned-B/op")
				b.ReportMetric(float64(exec.Stats.BufferedBytes), "scan-B/op")
			})
		}
	}
}
