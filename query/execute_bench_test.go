package query

import (
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson/document"
)

// The in-memory execution battery. Every case reports ns/doc rather than
// ns/op, because the question these answer is what one row costs on the
// scan-and-filter path: an ns/op number changes with the corpus size and
// therefore cannot be compared against the bare-scan floor, which is the only
// baseline that says whether a filter is expensive.
//
// The corpus is deliberately built without postings, a value dictionary, or a
// declared index. Those turn a predicate into a candidate bound, which is the
// path that already gathers only surviving rows; the unindexed scan is the one
// under measurement here.

const benchScanRows = 65536

// benchDoc renders one corpus row. sel partitions the corpus into thousandths
// so a predicate can name an exact selectivity, g gives grouping a moderate
// cardinality, and f0..f15 give projection width without changing the rest of
// the row.
func benchDoc(i int) []byte {
	doc := make([]byte, 0, 256)
	doc = fmt.Appendf(doc, `{"id":%d,"sel":%d,"g":%d,"name":"item-%06d","score":%d`,
		i, i%1000, i%64, i%4096, i*7%1000)
	for f := 0; f < 16; f++ {
		doc = fmt.Appendf(doc, `,"f%d":%d`, f, (i*(f+1))%9973)
	}
	doc = fmt.Appendf(doc, `,"obj":{"x":%d},"tags":["a","b%d"]}`, i%3, i%5)
	return doc
}

func benchScanSegment(tb testing.TB) *store.Segment {
	tb.Helper()
	set := &store.Segment{ShapeTapes: true, Options: document.IndexOptions{HashKeys: true}}
	for i := 0; i < benchScanRows; i++ {
		if _, err := set.Append(benchDoc(i)); err != nil {
			tb.Fatal(err)
		}
	}
	return set
}

func benchScanSnapshot(tb testing.TB) store.Snapshot {
	tb.Helper()
	builder, err := store.NewBuilder(store.Options{ChunkDocuments: 64, ShapeTapes: true})
	if err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < benchScanRows; i++ {
		if err := builder.Append(fmt.Sprintf("k%08d", i), benchDoc(i)); err != nil {
			tb.Fatal(err)
		}
	}
	db, err := builder.Build()
	if err != nil {
		tb.Fatal(err)
	}
	snapshot, _ := db.Snapshot()
	return snapshot
}

var benchResultRows int

// runQueryBench times one query over one source and reports ns/doc against the
// corpus size, not against the rows that survived: the cost being measured is
// what the scan pays per row it looked at.
func runQueryBench(b *testing.B, q *Query, src Source, rows int) {
	var e Exec
	if err := q.RunInto(&e, src); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := q.RunInto(&e, src); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchResultRows = e.Result.RowCount
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rows), "ns/doc")
	b.ReportMetric(float64(e.Result.RowCount), "outrows")
}

// benchShapes enumerates the query battery. Selectivity cases all read one
// column and project one, so the only thing that varies between them is how
// many rows survive the filter — which is what isolates the cost of
// materializing a row that is about to be discarded.
func benchShapes() []struct {
	name string
	q    *Query
} {
	wide4 := Select(Path("f0"), Path("f1"), Path("f2"), Path("f3"))
	wideCols := make([]Column, 0, 16)
	for f := 0; f < 16; f++ {
		wideCols = append(wideCols, Path(fmt.Sprintf("f%d", f)))
	}
	return []struct {
		name string
		q    *Query
	}{
		{"scan", Select(Path("id"))},
		{"filter/0.1pct", Select(Path("id")).Where(Cmp("sel", Lt, 1))},
		{"filter/1pct", Select(Path("id")).Where(Cmp("sel", Lt, 10))},
		{"filter/10pct", Select(Path("id")).Where(Cmp("sel", Lt, 100))},
		{"filter/25pct", Select(Path("id")).Where(Cmp("sel", Lt, 250))},
		{"filter/50pct", Select(Path("id")).Where(Cmp("sel", Lt, 500))},
		{"filter/75pct", Select(Path("id")).Where(Cmp("sel", Lt, 750))},
		{"filter/90pct", Select(Path("id")).Where(Cmp("sel", Lt, 900))},
		{"filter/100pct", Select(Path("id")).Where(Cmp("sel", Lt, 1000))},
		{"project/1", Select(Path("f0")).Where(Cmp("sel", Lt, 10))},
		{"project/4", wide4.Where(Cmp("sel", Lt, 10))},
		{"project/16", Select(wideCols...).Where(Cmp("sel", Lt, 10))},
		{"project/16-nofilter", Select(wideCols...)},
		{"order", Select(Path("id"), Path("name")).Where(Cmp("sel", Lt, 10)).OrderBy("name", Asc).Limit(100)},
		{"group", Select(Path("g"), Count(), Sum("score")).Where(Cmp("sel", Lt, 100)).GroupBy("g")},
		{"aggregate", Select(Count(), Sum("score")).Where(Cmp("sel", Lt, 100))},
		{"pred/eq", Select(Path("id")).Where(Cmp("sel", Eq, 7))},
		{"pred/range", Select(Path("id")).Where(And(Cmp("sel", Ge, 10), Cmp("sel", Lt, 20)))},
		{"pred/in", Select(Path("id")).Where(In("sel", 1, 3, 5, 7, 9, 11, 13, 15, 17, 19))},
		{"pred/contains", Select(Path("id")).Where(Contains("obj", `{"x":1}`))},
		{"pred/isnull", Select(Path("id")).Where(IsNull("missing"))},
		{"pred/exists", Select(Path("id")).Where(Exists("score"))},
		{"pred/string", Select(Path("id")).Where(Cmp("name", Ge, "item-004000"))},
	}
}

func BenchmarkSegmentShapes(b *testing.B) {
	set := benchScanSegment(b)
	for _, shape := range benchShapes() {
		b.Run(shape.name, func(b *testing.B) {
			runQueryBench(b, shape.q, FromSegment(set), benchScanRows)
		})
	}
}

func BenchmarkSnapshotShapes(b *testing.B) {
	snapshot := benchScanSnapshot(b)
	for _, shape := range benchShapes() {
		b.Run(shape.name, func(b *testing.B) {
			runQueryBench(b, shape.q, FromSnapshot(snapshot), benchScanRows)
		})
	}
}

// BenchmarkSegmentScale sweeps the corpus size for the shapes whose cost is
// expected to move with it, which is what a parallelism threshold has to be
// chosen against: a crossover picked at one size is not evidence for another.
func BenchmarkSegmentScale(b *testing.B) {
	for _, rows := range []int{256, 1024, 4096, 16384, 65536, 262144} {
		set := &store.Segment{ShapeTapes: true, Options: document.IndexOptions{}}
		for i := 0; i < rows; i++ {
			if _, err := set.Append(benchDoc(i)); err != nil {
				b.Fatal(err)
			}
		}
		src := FromSegment(set)
		for _, shape := range []struct {
			name string
			q    *Query
		}{
			{"scan", Select(Path("id"))},
			{"filter1pct", Select(Path("id")).Where(Cmp("sel", Lt, 10))},
			{"aggregate", Select(Count(), Sum("score")).Where(Cmp("sel", Lt, 100))},
		} {
			b.Run(fmt.Sprintf("%s/rows=%d", shape.name, rows), func(b *testing.B) {
				runQueryBench(b, shape.q, src, rows)
			})
		}
	}
}

// runParallelBench times one query at one explicit worker count. It is the
// evidence behind scanWorkerCount's default: the crossover between a split
// scan and a serial one is a function of the scan's size, and the only way to
// place it is to measure both at every size.
func runParallelBench(b *testing.B, q *Query, src Source, rows, workers int) {
	e := Exec{Options: ExecOptions{Workers: workers}}
	if err := q.RunInto(&e, src); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := q.RunInto(&e, src); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	benchResultRows = e.Result.RowCount
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rows), "ns/doc")
}

func BenchmarkSegmentParallel(b *testing.B) {
	for _, rows := range []int{256, 512, 1024, 4096, 32768, 131072} {
		set := &store.Segment{ShapeTapes: true, Options: document.IndexOptions{HashKeys: true}}
		for i := 0; i < rows; i++ {
			if _, err := set.Append(benchDoc(i)); err != nil {
				b.Fatal(err)
			}
		}
		src := FromSegment(set)
		q := Select(Path("id")).Where(Cmp("sel", Lt, 10))
		for _, workers := range []int{1, 2, 4, 8, 16} {
			b.Run(fmt.Sprintf("rows=%06d/w=%02d", rows, workers), func(b *testing.B) {
				runParallelBench(b, q, src, rows, workers)
			})
		}
	}
}

// BenchmarkNarrowRowParallel places the crossover against the cheapest possible
// row — two small fields, so the per-row work is at its floor and the fixed
// cost of splitting is at its most visible. A threshold chosen only against the
// wide corpus above would be too low for this one, which is the case the
// default has to survive.
func BenchmarkNarrowRowParallel(b *testing.B) {
	for _, rows := range []int{256, 512, 1024, 2048, 4096, 16384} {
		set := &store.Segment{ShapeTapes: true, Options: document.IndexOptions{HashKeys: true}}
		for i := 0; i < rows; i++ {
			if _, err := set.Append(fmt.Appendf(nil, `{"id":%d,"sel":%d}`, i, i%1000)); err != nil {
				b.Fatal(err)
			}
		}
		src := FromSegment(set)
		q := Select(Path("id")).Where(Cmp("sel", Lt, 10))
		for _, workers := range []int{1, 2, 4, 8} {
			b.Run(fmt.Sprintf("rows=%06d/w=%02d", rows, workers), func(b *testing.B) {
				runParallelBench(b, q, src, rows, workers)
			})
		}
	}
}

func BenchmarkSnapshotParallel(b *testing.B) {
	for _, rows := range []int{512, 2048, 8192, 32768, 131072} {
		builder, err := store.NewBuilder(store.Options{ChunkDocuments: 64, ShapeTapes: true})
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < rows; i++ {
			if err := builder.Append(fmt.Sprintf("k%08d", i), benchDoc(i)); err != nil {
				b.Fatal(err)
			}
		}
		db, err := builder.Build()
		if err != nil {
			b.Fatal(err)
		}
		snapshot, _ := db.Snapshot()
		src := FromSnapshot(snapshot)
		q := Select(Path("id")).Where(Cmp("sel", Lt, 10))
		for _, workers := range []int{1, 2, 4, 8, 16} {
			b.Run(fmt.Sprintf("rows=%06d/w=%02d", rows, workers), func(b *testing.B) {
				runParallelBench(b, q, src, rows, workers)
			})
		}
	}
}
