package query

import (
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// A million documents make the returned page a tiny fraction of the index.
// Probe positions vary across the corpus; setup and warming are not timed.
func BenchmarkFileIndexedPage(b *testing.B) {
	const count = 1_000_000
	if testing.Short() {
		b.Skip("million-row corpus")
	}
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		b.Fatal(err)
	}
	for i := range count {
		key := fmt.Sprintf("user-%09d", i)
		doc := fmt.Appendf(nil, `{"id":%q,"score":%d,"active":%t,"name":"Person %d","note":"A realistic document with some padding for indexed page queries and point reads."}`, key, i, i%3 == 0, i)
		if err := builder.Append(key, doc); err != nil {
			b.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}
	f, err := os.CreateTemp(b.TempDir(), "pages-*")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	opts := durable.Options{ResidentBytes: 512 << 20, Backend: durable.BackendPortable, Indexes: []store.IndexDefinition{{Name: "score", Paths: []string{"/score"}}}}
	if _, err = durable.CreateFromPrimary(built, f, opts); err != nil {
		b.Fatal(err)
	}
	fs, err := durable.Open(f, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer fs.Close()
	snap, err := fs.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer snap.Close()
	rng := rand.New(rand.NewPCG(7, 11))
	keys := make([][]byte, 1024)
	positions := make([]int, len(keys))
	for i := range keys {
		positions[i] = rng.IntN(count - 1024)
		keys[i] = fmt.Appendf(nil, "user-%09d", positions[i])
	}
	b.Run("Point", func(b *testing.B) {
		buf := make([]byte, 0, 256)
		for _, key := range keys {
			var ok bool
			buf, ok, err = snap.AppendRaw(buf[:0], key)
			if err != nil || !ok {
				b.Fatal(err)
			}
		}
		at := 0
		b.ReportAllocs()
		for b.Loop() {
			var ok bool
			buf, ok, err = snap.AppendRaw(buf[:0], keys[at&1023])
			if err != nil || !ok {
				b.Fatal(err)
			}
			at++
		}
	})
	for _, size := range []int{1, 32, 64, 256} {
		b.Run(fmt.Sprintf("Primary/rows=%d", size), func(b *testing.B) {
			q := Select(Path("/id"), Path("/score"), Path("/name")).OrderBy("/id", Asc).Limit(size)
			var e Exec
			defer e.Release()
			span := NewFileRangeSource(keys[0], nil, false)
			span.BindPrimaryOrder("/id")
			run := func(at int) {
				span.Bind(keys[at&1023], nil, false)
				span.BindPrimaryOrder("/id")
				if err := q.RunInto(&e, FromFileRange(snap, &span)); err != nil {
					b.Fatal(err)
				}
				if e.Result.RowCount != size {
					b.Fatalf("rows=%d", e.Result.RowCount)
				}
			}
			for i := range keys {
				run(i)
			}
			at := 0
			b.ReportAllocs()
			for b.Loop() {
				run(at)
				at++
			}
			b.ReportMetric(float64(e.Stats.RowsScanned), "scanned/op")
		})
	}
	for _, size := range []int{1, 32, 64, 256} {
		b.Run(fmt.Sprintf("Secondary/rows=%d", size), func(b *testing.B) {
			queries := make([]*Query, len(keys))
			for i, pos := range positions {
				queries[i] = Select(Path("/id"), Path("/score"), Path("/name")).Where(And(Cmp("/score", Ge, pos), Cmp("/score", Lt, pos+size))).OrderBy("/score", Asc).Limit(size)
			}
			var e Exec
			defer e.Release()
			run := func(at int) {
				if err := queries[at&1023].RunInto(&e, FromFile(snap)); err != nil {
					b.Fatal(err)
				}
				if e.Result.RowCount != size || !e.Stats.IndexBounded {
					b.Fatalf("rows=%d stats=%+v", e.Result.RowCount, e.Stats)
				}
			}
			for i := range keys {
				run(i)
			}
			at := 0
			b.ReportAllocs()
			for b.Loop() {
				run(at)
				at++
			}
			b.ReportMetric(float64(e.Stats.RowsScanned), "scanned/op")
		})
	}
}
