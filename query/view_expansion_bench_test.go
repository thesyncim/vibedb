package query

import (
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
)

func BenchmarkSQLViewExpansionNested(b *testing.B) {
	views := sqlViewMap{
		"leaf": {Name: "leaf", Query: `SELECT id, n FROM docs WHERE n >= 1`},
		"top":  {Name: "top", Query: `SELECT id, n FROM leaf WHERE n <= 9`},
	}
	const source = `SELECT id FROM top ORDER BY id`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree, err := sqlast.Parse(source)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := ExpandSQLViews(
			source, tree, views, SQLViewExpansionOptions{},
		); err != nil {
			b.Fatal(err)
		}
	}
}
