package query

import (
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

var storeQueryBenchmarkRows int

func benchmarkIndexedCollection(b *testing.B, rows int) store.Snapshot {
	b.Helper()
	builder, err := store.NewBuilder(store.Options{
		ChunkDocuments: 64,
		ShapeTapes:     true,
	})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		doc := fmt.Appendf(nil, `{"id":%d,"bucket":%d,"payload":"value-%08d"}`, i, i%100, i)
		if err := builder.Append(fmt.Sprintf("key:%08d", i), doc); err != nil {
			b.Fatal(err)
		}
	}
	db, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}
	if _, err := db.CreateIndex(store.IndexDefinition{Name: "bucket", Paths: []string{"/bucket"}}); err != nil {
		b.Fatal(err)
	}
	for {
		info, err := db.BackfillIndex("bucket", 0)
		if err != nil {
			b.Fatal(err)
		}
		if info.State == store.IndexReady {
			break
		}
	}
	snapshot, _ := db.Snapshot()
	return snapshot
}

func BenchmarkStoreQueryIndexedProjection(b *testing.B) {
	snapshot := benchmarkIndexedCollection(b, 100_000)
	q := Select(Path("id")).Where(Cmp("bucket", Eq, 17))
	src := FromSnapshot(snapshot)
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
	storeQueryBenchmarkRows = e.Result.RowCount
}

func BenchmarkStoreQueryIndexedCount(b *testing.B) {
	snapshot := benchmarkIndexedCollection(b, 100_000)
	q := Select(Count()).Where(Cmp("bucket", Eq, 17))
	src := FromSnapshot(snapshot)
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
	storeQueryBenchmarkRows = e.Result.RowCount
}
