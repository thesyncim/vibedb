package query

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// scoreBenchDatabase builds an 8k-doc heap corpus with a ubiquitous term
// (varying TF and length, so SCORE() spreads) and a skewed term for the
// selective lane. Both benches below pin the full SQL path: ==> match,
// per-row SCORE(), ORDER BY plus LIMIT.
func scoreBenchDatabase(b *testing.B) *store.Database {
	b.Helper()
	db := &store.Database{}
	coll, err := db.CreateCollection("docs", store.Options{})
	if err != nil {
		b.Fatal(err)
	}
	for i := range 8192 {
		body := "common"
		for k := 0; k < (i*2654435761)%64; k++ {
			body += " common"
		}
		for k := 0; k < (i*97)%240; k++ {
			body += " filler"
		}
		if i%5 == 0 {
			body += " zipf"
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
	return db
}

func runScoreBench(b *testing.B, db *store.Database, src string, want int) {
	b.Helper()
	statement, err := PrepareStatement(src)
	if err != nil {
		b.Fatal(err)
	}
	defer statement.Release()
	catalog := db.Snapshot()
	b.ResetTimer()
	for range b.N {
		exec := Exec{}
		cursor, err := statement.RunInto(&exec, FromDatabase(catalog, "docs"), nil)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for cursor.Next() {
			n++
		}
		exec.Release()
		if n != want {
			b.Fatalf("rows = %d, want %d", n, want)
		}
	}
}

func BenchmarkSQLScoreOrderLimitBroad(b *testing.B) {
	db := scoreBenchDatabase(b)
	runScoreBench(b, db, `SELECT o.id FROM docs AS o WHERE o.body ==> 'common' ORDER BY SCORE() DESC LIMIT 10`, 10)
}

func BenchmarkSQLScoreOrderLimitSelective(b *testing.B) {
	db := scoreBenchDatabase(b)
	runScoreBench(b, db, `SELECT o.id FROM docs AS o WHERE o.body ==> 'zipf AND common' ORDER BY SCORE() DESC LIMIT 10`, 10)
}
