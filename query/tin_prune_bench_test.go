package query

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/store"
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
