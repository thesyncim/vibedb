package query

import (
	"fmt"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

func durableGroupedCorpus(tb testing.TB, n int) *durable.Snapshot {
	tb.Helper()
	file, err := os.CreateTemp(tb.TempDir(), "query-file-grouped-*.vjc")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = file.Close() })
	source := &store.Collection{}
	for i := range n {
		doc := fmt.Appendf(nil,
			`{"id":%d,"low":"grp-%02d","med":"grp-%04d","high":"grp-%05d","amount":%d}`,
			i, i%16, i%512, i%10000, (i*17)%1000,
		)
		if _, err := source.Put(fmt.Sprintf("k-%07d", i), doc); err != nil {
			tb.Fatal(err)
		}
	}
	options := durable.Options{Collection: store.Options{ChunkDocuments: 64}}
	if _, err := durable.CreateFromPrimary(source, file, options); err != nil {
		tb.Fatal(err)
	}
	fs, err := durable.Open(file, options)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = fs.Close() })
	snapshot, err := fs.Snapshot()
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = snapshot.Close() })
	return snapshot
}

func BenchmarkFileGrouped(b *testing.B) {
	const numDocs = 50_000
	snapshot := durableGroupedCorpus(b, numDocs)
	cases := []struct {
		name     string
		groupCol string
		wantRows int
	}{
		{name: "low_16", groupCol: "low", wantRows: 16},
		{name: "med_512", groupCol: "med", wantRows: 512},
		{name: "high_10000", groupCol: "high", wantRows: 10000},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			q := Select(Path(tc.groupCol), Count()).
				GroupBy(tc.groupCol)
			e := Exec{Options: ExecOptions{
				Workers:   4,
				BatchRows: 128,
			}}
			defer e.Release()
			// Warm up the Exec, pool, arenas, slots, and hash tables.
			for range 4 {
				if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
					b.Fatal(err)
				}
				if e.Result.RowCount != tc.wantRows {
					b.Fatalf("got %d groups, want %d", e.Result.RowCount, tc.wantRows)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func durableIntegerGroupedCorpus(tb testing.TB, n int) *durable.Snapshot {
	tb.Helper()
	file, err := os.CreateTemp(tb.TempDir(), "query-file-int-grouped-*.vjc")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = file.Close() })
	source := &store.Collection{}
	for i := range n {
		doc := fmt.Appendf(nil,
			`{"id":%d,"low":%d,"med":%d,"high":%d,"amount":%d}`,
			i, i%16, i%512, i%10000, (i*17)%1000,
		)
		if _, err := source.Put(fmt.Sprintf("k-%07d", i), doc); err != nil {
			tb.Fatal(err)
		}
	}
	options := durable.Options{Collection: store.Options{ChunkDocuments: 64}}
	if _, err := durable.CreateFromPrimary(source, file, options); err != nil {
		tb.Fatal(err)
	}
	fs, err := durable.Open(file, options)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = fs.Close() })
	snapshot, err := fs.Snapshot()
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = snapshot.Close() })
	return snapshot
}

func BenchmarkFileGroupedInteger(b *testing.B) {
	const numDocs = 50_000
	snapshot := durableIntegerGroupedCorpus(b, numDocs)
	cases := []struct {
		name     string
		groupCol string
		wantRows int
	}{
		{name: "low_16", groupCol: "low", wantRows: 16},
		{name: "med_512", groupCol: "med", wantRows: 512},
		{name: "high_10000", groupCol: "high", wantRows: 10000},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			q := Select(Path(tc.groupCol), Count(), Sum("amount")).
				GroupBy(tc.groupCol)
			e := Exec{Options: ExecOptions{
				Workers:   4,
				BatchRows: 128,
			}}
			defer e.Release()
			// Warm up the Exec, integer group hash table, and result buffers.
			for range 4 {
				if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
					b.Fatal(err)
				}
				if e.Result.RowCount != tc.wantRows {
					b.Fatalf("got %d groups, want %d", e.Result.RowCount, tc.wantRows)
				}
				if e.Stats.GroupedIntegerRows != numDocs {
					b.Fatalf("got GroupedIntegerRows=%d, want %d", e.Stats.GroupedIntegerRows, numDocs)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := q.RunInto(&e, FromFile(snapshot)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
