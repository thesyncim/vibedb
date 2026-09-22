package query

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// BenchmarkSQLMatchPrunedHeapScan measures ==> over a heap snapshot with
// postings pruning engaged: 4096 documents, 64 tin hits. The masks must
// keep extraction plus evaluation proportional to the hit set, not the
// corpus. Compare against the unpruned lane by stashing the predMatch case
// in candidates_mask.go.
func BenchmarkSQLMatchPrunedHeapScan(b *testing.B) {
	db := &store.Database{}
	coll, err := db.CreateCollection("docs", store.Options{})
	if err != nil {
		b.Fatal(err)
	}
	for i := range 4096 {
		body := "filler words padding out this document body"
		if i%64 == 0 {
			body = "luxury vintage watches " + body
		}
		if _, err := coll.Put(fmt.Sprintf("d%04d", i), []byte(`{"body":`+strconv.Quote(body)+`}`)); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := coll.CreateIndex(store.IndexDefinition{
		Name: "body_tin", Paths: []string{"/body"}, Kind: store.IndexTin,
	}); err != nil {
		b.Fatal(err)
	}
	if _, err := coll.BackfillIndex("body_tin", 0); err != nil {
		b.Fatal(err)
	}
	statement, err := PrepareStatement(
		`SELECT o.id FROM docs AS o WHERE o.body ==> 'luxury' ORDER BY o.id`,
	)
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Release()
	b.ResetTimer()
	for range b.N {
		exec := Exec{}
		cursor, err := statement.RunInto(&exec, FromDatabase(db.Snapshot(), "docs"), nil)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for cursor.Next() {
			n++
		}
		exec.Release()
		if n != 64 {
			b.Fatalf("hits = %d, want 64", n)
		}
	}
}

// BenchmarkSQLMatchFileScan measures ==> over a durable snapshot: 4096
// documents, 64 tin hits, no postings pruning yet. This is the baseline the
// durable ordinal-to-slot bridge must beat; the heap twin above shows what
// pruning buys once candidates reach the scan.
func BenchmarkSQLMatchFileScan(b *testing.B) {
	file, err := os.CreateTemp(b.TempDir(), "tin-file-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = file.Close() })
	collection, err := durable.Create(file, durable.Options{
		Indexes: []store.IndexDefinition{
			{Name: "body_tin", Paths: []string{"/body"}, Kind: store.IndexTin},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = collection.Close() })
	for i := range 4096 {
		body := "filler words padding out this document body"
		if i%64 == 0 {
			body = "luxury vintage watches " + body
		}
		doc := `{"id":"d` + fmt.Sprintf("%04d", i) + `","body":` + strconv.Quote(body) + `}`
		if _, err := collection.Put([]byte(fmt.Sprintf("d%04d", i)), []byte(doc)); err != nil {
			b.Fatal(err)
		}
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = snapshot.Close() })
	q := Select(Path("id")).Where(Match("body", "luxury")).OrderBy("id", Asc)
	b.ResetTimer()
	for range b.N {
		exec := Exec{}
		if err := q.RunInto(&exec, FromFile(snapshot)); err != nil {
			b.Fatal(err)
		}
		col, ok := exec.Result.Column("id")
		if !ok || len(col.Cells) != 64 {
			b.Fatalf("hits = %d, want 64", len(col.Cells))
		}
		exec.Release()
	}
}
